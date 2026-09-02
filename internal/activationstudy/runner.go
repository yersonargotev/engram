package activationstudy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	maxCodexEventBytes         = 16 << 20
	maxFinalMessageBytes       = 256 << 10
	maxShimLogBytes            = 1 << 20
	workspaceCleanupAttempts   = 5
	workspaceCleanupRetryDelay = 10 * time.Millisecond
	studyProject               = "activation-study-fixture"
	preservationCanary         = "EVAL-PRESERVE-731"
)

type RunOptions struct {
	SourceRepo  string
	UserSkill   string
	AuthFile    string
	CodexBinary string
	TempRoot    string
	OutputPath  string
}

// Verify checks the frozen runtime and all fixture identities without invoking Codex.
func (study *Study) Verify(ctx context.Context, options RunOptions) (VerificationReport, error) {
	if err := study.Contract.validate(); err != nil {
		return VerificationReport{}, err
	}
	if strings.TrimSpace(options.CodexBinary) == "" {
		options.CodexBinary = "codex"
	}
	if err := study.verifyRuntime(ctx, options); err != nil {
		return VerificationReport{}, err
	}
	root, err := os.MkdirTemp(options.TempRoot, "engram-activation-verify-")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("create activation verification workspace: %w", err)
	}
	fixtures, err := study.PrepareFixtures(ctx, FixtureOptions{SourceRepo: options.SourceRepo, Root: filepath.Join(root, "fixtures"), UserSkill: options.UserSkill})
	if err != nil {
		_ = removeAllWithRetry(root, os.RemoveAll)
		return VerificationReport{}, err
	}
	for _, treatment := range []string{"engram-normal", "engram-ablated", "neutral"} {
		probeRoot := filepath.Join(root, "probes", treatment)
		inventory, probeErr := study.verifyCodexPromptInput(ctx, options.CodexBinary, fixtures.Path(treatment), treatment, probeRoot, options.AuthFile, options.UserSkill)
		if probeErr != nil {
			_ = removeAllWithRetry(root, os.RemoveAll)
			return VerificationReport{}, fmt.Errorf("verify %s Codex prompt input: %w", treatment, probeErr)
		}
		if len(fixtures.Report.CodexSkillInventory) == 0 {
			fixtures.Report.CodexSkillInventory = inventory
		} else if strings.Join(fixtures.Report.CodexSkillInventory, "\x00") != strings.Join(inventory, "\x00") {
			_ = removeAllWithRetry(root, os.RemoveAll)
			return VerificationReport{}, fmt.Errorf("verify %s Codex prompt input: skill inventory changed across treatments", treatment)
		}
	}
	fixtures.Report.CodexPromptInputVerified = true
	if err := removeAllWithRetry(root, os.RemoveAll); err != nil {
		return VerificationReport{}, fmt.Errorf("clean activation verification workspace: %w", err)
	}
	fixtures.Report.CleanupVerified = true
	return fixtures.Report, nil
}

// Run executes every frozen cell in fresh disposable state and returns only bounded evidence.
func (study *Study) Run(ctx context.Context, options RunOptions) (EventSet, error) {
	if err := study.Contract.validate(); err != nil {
		return EventSet{}, err
	}
	if strings.TrimSpace(options.CodexBinary) == "" {
		options.CodexBinary = "codex"
	}
	if err := study.verifyRuntime(ctx, options); err != nil {
		return EventSet{}, err
	}
	root, err := os.MkdirTemp(options.TempRoot, "engram-activation-study-")
	if err != nil {
		return EventSet{}, fmt.Errorf("create activation study workspace: %w", err)
	}
	cleanup := func() error { return removeAllWithRetry(root, os.RemoveAll) }
	defer cleanup()

	fixtures, err := study.PrepareFixtures(ctx, FixtureOptions{
		SourceRepo: options.SourceRepo,
		Root:       filepath.Join(root, "fixtures"),
		UserSkill:  options.UserSkill,
	})
	if err != nil {
		return EventSet{}, err
	}
	realEngram := filepath.Join(root, "bin", "engram-real")
	if err := buildFrozenEngram(ctx, fixtures.Path("engram-normal"), realEngram); err != nil {
		return EventSet{}, err
	}
	expectedManifests := make(map[string]string, len(fixtures.Report.Fixtures))
	for _, fixture := range fixtures.Report.Fixtures {
		expectedManifests[fixture.ID] = fixture.ManifestSHA256
	}

	eventSet := EventSet{
		SchemaVersion: "codex-activation-event-set-v1", StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash,
		Verification: fixtures.Report,
	}
	if strings.TrimSpace(options.OutputPath) != "" {
		if _, statErr := os.Stat(options.OutputPath); statErr == nil {
			existing, readErr := ReadEventSet(options.OutputPath)
			if readErr != nil {
				return EventSet{}, readErr
			}
			if existing.SchemaVersion != eventSet.SchemaVersion || existing.StudyID != eventSet.StudyID || existing.StudyVersion != eventSet.StudyVersion || existing.ContractSHA256 != eventSet.ContractSHA256 {
				return EventSet{}, fmt.Errorf("existing activation event set does not match the frozen contract")
			}
			if !sameVerificationIdentity(existing.Verification, eventSet.Verification) {
				return EventSet{}, fmt.Errorf("existing activation event set fixture identity changed")
			}
			records, validationErr := validatePartialEventRecords(study.Plan(), existing.Records, study.Contract.Codex.AvailableSkills)
			if validationErr != nil {
				return EventSet{}, validationErr
			}
			if len(records) > 0 && !existing.Verification.CodexPromptInputVerified {
				return EventSet{}, fmt.Errorf("existing activation event set lacks prompt-input verification")
			}
			if len(records) > 0 && len(existing.Verification.CodexSkillInventory) == 0 {
				return EventSet{}, fmt.Errorf("existing activation event set lacks its Codex skill inventory")
			}
			eventSet.Records = records
			eventSet.Verification.CodexPromptInputVerified = existing.Verification.CodexPromptInputVerified
			eventSet.Verification.CodexSkillInventory = existing.Verification.CodexSkillInventory
		} else if !os.IsNotExist(statErr) {
			return EventSet{}, fmt.Errorf("inspect activation event output: %w", statErr)
		}
	}
	completed := make(map[string]bool, len(eventSet.Records))
	for _, record := range eventSet.Records {
		completed[record.CellID] = true
	}
	for _, planned := range study.Plan() {
		if completed[planned.CellID] {
			continue
		}
		cellContext, cancel := context.WithTimeout(ctx, time.Duration(study.Contract.Codex.PerRunTimeoutSeconds)*time.Second)
		record, inventory, err := study.runCell(cellContext, planned, fixtures.Path(planned.Treatment), expectedManifests[planned.Treatment], realEngram, options)
		cancel()
		if err != nil {
			return EventSet{}, fmt.Errorf("run activation cell %s: %w", planned.CellID, err)
		}
		if len(eventSet.Verification.CodexSkillInventory) == 0 {
			eventSet.Verification.CodexSkillInventory = inventory
		} else if strings.Join(eventSet.Verification.CodexSkillInventory, "\x00") != strings.Join(inventory, "\x00") {
			return EventSet{}, fmt.Errorf("run activation cell %s: Codex skill inventory changed across cells", planned.CellID)
		}
		eventSet.Records = append(eventSet.Records, record)
		sort.Slice(eventSet.Records, func(i, j int) bool { return eventSet.Records[i].Sequence < eventSet.Records[j].Sequence })
		eventSet.Verification.CodexPromptInputVerified = true
		if strings.TrimSpace(options.OutputPath) != "" {
			if err := WritePrivateJSON(options.OutputPath, eventSet); err != nil {
				return EventSet{}, fmt.Errorf("persist activation study progress: %w", err)
			}
		}
	}

	if err := cleanup(); err != nil {
		return EventSet{}, fmt.Errorf("clean activation study workspace: %w", err)
	}
	eventSet.Verification.CodexPromptInputVerified = true
	eventSet.Verification.CleanupVerified = true
	if strings.TrimSpace(options.OutputPath) != "" {
		if err := WritePrivateJSON(options.OutputPath, eventSet); err != nil {
			return EventSet{}, fmt.Errorf("persist completed activation study: %w", err)
		}
	}
	return eventSet, nil
}

func sameVerificationIdentity(first, second VerificationReport) bool {
	if first.ContractSHA256 != second.ContractSHA256 || first.SourceRevision != second.SourceRevision || first.UserSkill != second.UserSkill || first.Ablation.RemovedGuidanceRows != second.Ablation.RemovedGuidanceRows || strings.Join(first.Ablation.ChangedFiles, "\x00") != strings.Join(second.Ablation.ChangedFiles, "\x00") {
		return false
	}
	if len(first.Fixtures) != len(second.Fixtures) {
		return false
	}
	for index := range first.Fixtures {
		if first.Fixtures[index] != second.Fixtures[index] {
			return false
		}
	}
	return true
}

func (study *Study) verifyRuntime(ctx context.Context, options RunOptions) error {
	if strings.TrimSpace(options.SourceRepo) == "" || strings.TrimSpace(options.UserSkill) == "" || strings.TrimSpace(options.AuthFile) == "" {
		return fmt.Errorf("activation study requires source repository, user skill, and Codex auth file")
	}
	info, err := os.Stat(options.AuthFile)
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("activation study Codex auth file is unavailable")
	}
	version := exec.CommandContext(ctx, options.CodexBinary, "--version")
	output, err := version.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect Codex version: %w", err)
	}
	wantCodex := "codex-cli " + study.Contract.Codex.Version
	if strings.TrimSpace(string(output)) != wantCodex {
		return fmt.Errorf("Codex version mismatch: got %q, want %q", strings.TrimSpace(string(output)), wantCodex)
	}
	goVersion := exec.CommandContext(ctx, "go", "version")
	output, err = goVersion.CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect Go version: %w", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 3 || fields[2] != study.Contract.Codex.GoVersion {
		return fmt.Errorf("Go version mismatch: got %q, want %q", strings.TrimSpace(string(output)), study.Contract.Codex.GoVersion)
	}
	return nil
}

func (study *Study) runCell(
	ctx context.Context,
	planned PlannedRun,
	templatePath, expectedManifest, realEngram string,
	options RunOptions,
) (RunRecord, []string, error) {
	cellRoot, err := os.MkdirTemp(filepath.Dir(filepath.Dir(templatePath)), "cell-")
	if err != nil {
		return RunRecord{}, nil, err
	}
	defer func() { _ = removeAllWithRetry(cellRoot, os.RemoveAll) }()
	workspace := filepath.Join(cellRoot, "workspace")
	if err := copyTree(templatePath, workspace); err != nil {
		return RunRecord{}, nil, fmt.Errorf("copy fixture: %w", err)
	}
	_, manifest, err := treeManifest(workspace)
	if err != nil {
		return RunRecord{}, nil, err
	}
	if manifest != expectedManifest {
		return RunRecord{}, nil, fmt.Errorf("pre-run fixture hash mismatch: got %s, want %s", manifest, expectedManifest)
	}
	if err := verifyFixtureIsolation(workspace, planned.Treatment); err != nil {
		return RunRecord{}, nil, err
	}

	state := filepath.Join(cellRoot, "state")
	home := filepath.Join(state, "home")
	codexHome := filepath.Join(home, ".codex")
	dataDir := filepath.Join(state, "engram-data")
	toolsDir := filepath.Join(state, "tools")
	for _, path := range []string{codexHome, dataDir, toolsDir, filepath.Join(state, "tmp"), filepath.Join(state, "xdg-config"), filepath.Join(state, "xdg-cache"), filepath.Join(state, "xdg-data")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return RunRecord{}, nil, err
		}
	}
	if err := copyFile(options.AuthFile, filepath.Join(codexHome, "auth.json"), 0o600); err != nil {
		return RunRecord{}, nil, fmt.Errorf("copy disposable Codex auth: %w", err)
	}
	if err := copyTree(options.UserSkill, filepath.Join(home, ".agents", "skills", study.Contract.UserSkill.Name)); err != nil {
		return RunRecord{}, nil, fmt.Errorf("copy disposable user skill: %w", err)
	}
	shimLog := filepath.Join(state, "shim.jsonl")
	if err := os.WriteFile(shimLog, nil, 0o600); err != nil {
		return RunRecord{}, nil, err
	}
	if err := writeShim(filepath.Join(toolsDir, "engram")); err != nil {
		return RunRecord{}, nil, err
	}
	environment := activationEnvironment(home, codexHome, dataDir, toolsDir, state, shimLog, realEngram)
	inventory, err := study.runCodexPromptInputProbe(ctx, options.CodexBinary, workspace, planned.Treatment, environment)
	if err != nil {
		return RunRecord{}, nil, fmt.Errorf("pre-run Codex prompt-input verification: %w", err)
	}
	if err := seedRecallCanary(ctx, realEngram, workspace, environment); err != nil {
		return RunRecord{}, nil, err
	}

	finalMessagePath := filepath.Join(state, "final-message.txt")
	arguments := []string{
		"exec", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--json",
		"--model", study.Contract.Codex.Model,
		"--sandbox", study.Contract.Codex.Sandbox,
		"--cd", workspace, "--add-dir", state,
		"--output-last-message", finalMessagePath,
		"--config", `approval_policy="never"`,
		"--config", fmt.Sprintf(`model_reasoning_effort=%q`, study.Contract.Codex.ReasoningEffort),
		"--config", `shell_environment_policy.inherit="all"`,
	}
	for _, feature := range study.Contract.Codex.DisabledFeatures {
		arguments = append(arguments, "--disable", feature)
	}
	arguments = append(arguments, "-")
	command := exec.CommandContext(ctx, options.CodexBinary, arguments...)
	command.Dir = workspace
	command.Env = environment
	command.Stdin = strings.NewReader(planned.PromptText)
	stdout := &boundedBuffer{maximum: maxCodexEventBytes}
	stderr := &boundedBuffer{maximum: maxCodexEventBytes}
	command.Stdout = stdout
	command.Stderr = stderr
	commandErr := command.Run()
	exitCode := commandExitCode(commandErr)
	if stdout.exceeded || stderr.exceeded {
		exitCode = 1
	}

	finalMessage, finalErr := readOptionalBoundedFile(finalMessagePath, maxFinalMessageBytes)
	shimEvents, shimErr := readShimEvents(shimLog)
	preservationVerified := false
	var preservationErr error
	if hasSuccessfulOperation(shimEvents, "save") {
		preservationVerified, preservationErr = verifyPreservation(ctx, realEngram, workspace, environment)
	}
	record := Classify(planned, RawEvidence{
		CodexJSONL: bytes.NewReader(stdout.Bytes()), CodexExitCode: exitCode,
		FinalMessage: finalMessage, AvailableSkills: inventory,
		ShimEvents: shimEvents, PreservationVerified: preservationVerified,
	})
	if preservationErr != nil {
		record.Omissions = append(record.Omissions, "preservation_verification_failed")
	}
	if commandErr != nil {
		record.Omissions = append(record.Omissions, "codex_process_failed")
	}
	if stdout.exceeded || stderr.exceeded {
		record.Omissions = append(record.Omissions, "codex_event_limit_exceeded")
	}
	if finalErr != nil {
		record.Omissions = append(record.Omissions, "final_message_unavailable")
	}
	if shimErr != nil {
		record.Omissions = append(record.Omissions, "shim_event_parse_failed")
		record.Events["integration_failure"] = true
	}
	_, afterManifest, err := treeManifest(workspace)
	if err != nil {
		return RunRecord{}, nil, err
	}
	if afterManifest != expectedManifest {
		record.Deviations = append(record.Deviations, "fixture_mutated")
	}
	record.Omissions = sortedUnique(record.Omissions)
	record.Deviations = sortedUnique(record.Deviations)
	return record, inventory, nil
}

func removeAllWithRetry(path string, removeAll func(string) error) error {
	var err error
	for attempt := 0; attempt < workspaceCleanupAttempts; attempt++ {
		err = removeAll(path)
		if err == nil {
			return nil
		}
		// RemoveAll reports a concurrently repopulated directory as ErrExist
		// (ENOTEMPTY on Unix). Give short-lived writers a bounded chance to exit.
		if !errors.Is(err, fs.ErrExist) || attempt == workspaceCleanupAttempts-1 {
			return err
		}
		time.Sleep(time.Duration(1<<attempt) * workspaceCleanupRetryDelay)
	}
	return err
}

func (study *Study) verifyCodexPromptInput(ctx context.Context, codexBinary, workspace, treatment, root, authFile, userSkill string) ([]string, error) {
	home := filepath.Join(root, "home")
	codexHome := filepath.Join(home, ".codex")
	state := filepath.Join(root, "state")
	for _, path := range []string{codexHome, filepath.Join(state, "tools"), filepath.Join(state, "tmp"), filepath.Join(state, "xdg-config"), filepath.Join(state, "xdg-cache"), filepath.Join(state, "xdg-data"), filepath.Join(state, "engram-data")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, err
		}
	}
	if err := copyFile(authFile, filepath.Join(codexHome, "auth.json"), 0o600); err != nil {
		return nil, err
	}
	if err := copyTree(userSkill, filepath.Join(home, ".agents", "skills", study.Contract.UserSkill.Name)); err != nil {
		return nil, err
	}
	environment := activationEnvironment(home, codexHome, filepath.Join(state, "engram-data"), filepath.Join(state, "tools"), state, filepath.Join(state, "unused.jsonl"), filepath.Join(state, "unused-engram"))
	return study.runCodexPromptInputProbe(ctx, codexBinary, workspace, treatment, environment)
}

func (study *Study) runCodexPromptInputProbe(ctx context.Context, codexBinary, workspace, treatment string, environment []string) ([]string, error) {
	arguments := []string{"-C", workspace}
	for _, feature := range study.Contract.Codex.DisabledFeatures {
		arguments = append(arguments, "--disable", feature)
	}
	arguments = append(arguments, "debug", "prompt-input", "Report the current Git branch without modifying files.")
	command := exec.CommandContext(ctx, codexBinary, arguments...)
	command.Dir = workspace
	command.Env = environment
	output := &boundedBuffer{maximum: maxCodexEventBytes}
	command.Stdout = output
	command.Stderr = &boundedBuffer{maximum: maxCodexEventBytes}
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("render Codex prompt input: %w", err)
	}
	if output.exceeded {
		return nil, fmt.Errorf("Codex prompt input exceeded the evidence bound")
	}
	inventory, promptText, err := parseCodexPromptInput(output.Bytes())
	if err != nil {
		return nil, err
	}
	expected := sortedUnique(study.Contract.Codex.AvailableSkills)
	if strings.Join(inventory, "\x00") != strings.Join(expected, "\x00") {
		return nil, fmt.Errorf("Codex skill inventory mismatch: got %v, want %v", inventory, expected)
	}
	protocolVisible := strings.Contains(promptText, "skills/memory-protocol/SKILL.md")
	if protocolVisible != (treatment == "engram-normal") {
		return nil, fmt.Errorf("Codex project Memory guidance visibility does not match treatment %s", treatment)
	}
	return inventory, nil
}

func parseCodexPromptInput(raw []byte) ([]string, string, error) {
	var messages []struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &messages); err != nil {
		return nil, "", fmt.Errorf("decode Codex prompt input: %w", err)
	}
	var prompt strings.Builder
	for _, message := range messages {
		for _, content := range message.Content {
			if content.Type == "input_text" {
				prompt.WriteString(content.Text)
				prompt.WriteByte('\n')
			}
		}
	}
	text := prompt.String()
	marker := "### Available skills\n"
	start := strings.Index(text, marker)
	if start < 0 {
		return nil, "", fmt.Errorf("Codex prompt input lacks the skill inventory")
	}
	section := text[start+len(marker):]
	if end := strings.Index(section, "</skills_instructions>"); end >= 0 {
		section = section[:end]
	}
	var skills []string
	for _, line := range strings.Split(section, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "- ") {
			continue
		}
		name, _, found := strings.Cut(strings.TrimPrefix(line, "- "), ":")
		if found && !strings.ContainsAny(name, " /\\=") {
			skills = append(skills, name)
		}
	}
	return sortedUnique(skills), text, nil
}

func buildFrozenEngram(ctx context.Context, source, output string) error {
	if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-o", output, "./cmd/engram")
	command.Dir = source
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local")
	if combined, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build frozen Engram binary: %w: %s", err, strings.TrimSpace(string(combined)))
	}
	return nil
}

func activationEnvironment(home, codexHome, dataDir, toolsDir, state, shimLog, realEngram string) []string {
	path := strings.Join([]string{toolsDir, "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}, string(os.PathListSeparator))
	return []string{
		"PATH=" + path, "HOME=" + home, "CODEX_HOME=" + codexHome,
		"XDG_CONFIG_HOME=" + filepath.Join(state, "xdg-config"),
		"XDG_CACHE_HOME=" + filepath.Join(state, "xdg-cache"),
		"XDG_DATA_HOME=" + filepath.Join(state, "xdg-data"),
		"TMPDIR=" + filepath.Join(state, "tmp"), "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "NO_COLOR=1",
		"ENGRAM_DATA_DIR=" + dataDir, "ENGRAM_PROJECT=" + studyProject,
		"ENGRAM_ACTIVATION_LOG=" + shimLog, "ENGRAM_ACTIVATION_REAL=" + realEngram,
	}
}

func seedRecallCanary(ctx context.Context, binary, workspace string, environment []string) error {
	command := exec.CommandContext(ctx, binary,
		"save", "--title", "Synthetic activation recall canary",
		"--content", "What: The stored evaluation color pair is "+syntheticRecallCanary+".\nWhy: It verifies useful recall without private evidence.\nWhere: disposable activation study store.\nLearned: The token is synthetic.",
		"--project", studyProject, "--json",
	)
	command.Dir = workspace
	command.Env = environmentWithoutShim(environment)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("seed disposable recall canary: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func verifyPreservation(ctx context.Context, binary, workspace string, environment []string) (bool, error) {
	command := exec.CommandContext(ctx, binary, "search", preservationCanary, "--project", studyProject, "--match-mode", "all", "--limit", "5", "--json")
	command.Dir = workspace
	command.Env = environmentWithoutShim(environment)
	output, err := command.Output()
	if err != nil {
		return false, err
	}
	return bytes.Contains(output, []byte(preservationCanary)), nil
}

func environmentWithoutShim(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, value := range environment {
		if strings.HasPrefix(value, "PATH=") {
			parts := strings.SplitN(value, "=", 2)
			paths := filepath.SplitList(parts[1])
			if len(paths) > 0 {
				paths = paths[1:]
			}
			filtered = append(filtered, "PATH="+strings.Join(paths, string(os.PathListSeparator)))
			continue
		}
		if strings.HasPrefix(value, "ENGRAM_ACTIVATION_") {
			continue
		}
		filtered = append(filtered, value)
	}
	return filtered
}

func writeShim(path string) error {
	const script = `#!/bin/sh
operation="other"
case "$1" in
  current-project) operation="current_project" ;;
  search) operation="targeted_search" ;;
  save) operation="save" ;;
  checkpoint) operation="checkpoint" ;;
esac
"$ENGRAM_ACTIVATION_REAL" "$@"
status=$?
printf '{"operation":"%s","exit_code":%d}\n' "$operation" "$status" >> "$ENGRAM_ACTIVATION_LOG"
exit "$status"
`
	return os.WriteFile(path, []byte(script), 0o700)
}

func readShimEvents(path string) ([]ShimEvent, error) {
	raw, err := readBoundedFile(path, maxShimLogBytes)
	if err != nil {
		return nil, err
	}
	var events []ShimEvent
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		var event ShimEvent
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return events, err
		}
		if canonicalOperation(event.Operation) == "" && event.Operation != "other" {
			return events, fmt.Errorf("invalid shim operation")
		}
		events = append(events, event)
	}
	return events, scanner.Err()
}

func verifyFixtureIsolation(workspace, treatment string) error {
	for _, forbidden := range []string{".codex/config.toml", ".codex/hooks.json", ".mcp.json"} {
		if _, err := os.Lstat(filepath.Join(workspace, filepath.FromSlash(forbidden))); !os.IsNotExist(err) {
			return fmt.Errorf("fixture %s inherited forbidden integration surface %s", treatment, forbidden)
		}
	}
	if treatment == "neutral" {
		for _, forbidden := range []string{"AGENTS.md", "CLAUDE.md", ".engram/config.json", "plugin"} {
			if _, err := os.Lstat(filepath.Join(workspace, filepath.FromSlash(forbidden))); !os.IsNotExist(err) {
				return fmt.Errorf("neutral fixture inherited forbidden surface %s", forbidden)
			}
		}
	}
	return nil
}

func copyTree(source, destination string) error {
	resolvedSource, err := filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	source = resolvedSource
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if entry.Type()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			return os.Symlink(link, target)
		}
		return copyFile(path, target, info.Mode().Perm())
	})
}

func copyFile(source, destination string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	return errors.Join(copyErr, closeErr)
}

func readOptionalBoundedFile(path string, maximum int64) (string, error) {
	raw, err := readBoundedFile(path, maximum)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func hasSuccessfulOperation(events []ShimEvent, operation string) bool {
	for _, event := range events {
		if event.Operation == operation && event.ExitCode == 0 {
			return true
		}
	}
	return false
}

func commandExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return 1
}

type boundedBuffer struct {
	buffer   bytes.Buffer
	maximum  int
	exceeded bool
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	if buffer.exceeded {
		return len(value), nil
	}
	remaining := buffer.maximum - buffer.buffer.Len()
	if remaining <= 0 {
		buffer.exceeded = true
		return len(value), nil
	}
	if len(value) > remaining {
		_, _ = buffer.buffer.Write(value[:remaining])
		buffer.exceeded = true
		return len(value), nil
	}
	return buffer.buffer.Write(value)
}

func (buffer *boundedBuffer) Bytes() []byte { return buffer.buffer.Bytes() }
