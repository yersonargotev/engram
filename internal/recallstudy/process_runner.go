package recallstudy

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/yersonargotev/engram/internal/codexlifecycle"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/store"
)

const (
	maxStudyProcessBytes = 8 << 20
	studyRunTimeout      = 900 * time.Second
)

type processCohortRunner struct {
	study         *Study
	manifest      *Manifest
	compatibility protocolcontract.CompatibilityReport
	runtime       ExecutionRuntime
	root          string
	snapshot      string
	binary        string
	baseHome      string
	activeCell    string
}

type studyInjectionEvidence struct {
	expectedUTF8Bytes int64
	broadResultKey    string
}

type studyRecallEvidence struct {
	searchObserved   bool
	resultKeys       []string
	assessments      []Assessment
	latencyMillis    float64
	falseEmptyReview string
}

func newProcessCohortRunner(
	ctx context.Context,
	study *Study,
	manifest *Manifest,
	compatibility protocolcontract.CompatibilityReport,
	runtime ExecutionRuntime,
) (*processCohortRunner, error) {
	if study == nil || manifest == nil {
		return nil, fmt.Errorf("Recall study process runner requires a frozen study and cohort")
	}
	if strings.TrimSpace(runtime.SourceRepo) == "" || strings.TrimSpace(runtime.CodexBinary) == "" || strings.TrimSpace(runtime.AuthFile) == "" {
		return nil, fmt.Errorf("Recall study process runner requires source repository, Codex binary, and auth file")
	}
	if info, err := os.Stat(runtime.AuthFile); err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("Recall study Codex auth file is unavailable")
	}
	version := exec.CommandContext(ctx, runtime.CodexBinary, "--version")
	versionOutput, err := version.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("inspect Recall study Codex version: %w", err)
	}
	wantVersion := "codex-cli " + study.Contract.Model.CodexVersion
	if strings.TrimSpace(string(versionOutput)) != wantVersion {
		return nil, fmt.Errorf("Recall study Codex version mismatch: got %q, want %q", strings.TrimSpace(string(versionOutput)), wantVersion)
	}
	commit := exec.CommandContext(ctx, "git", "-C", runtime.SourceRepo, "cat-file", "-e", study.Contract.SourceRevision+"^{commit}")
	if output, err := commit.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("verify Recall study source revision: %w: %s", err, strings.TrimSpace(string(output)))
	}

	root, err := os.MkdirTemp(runtime.TempRoot, "engram-recall-study-")
	if err != nil {
		return nil, fmt.Errorf("create Recall study runtime: %w", err)
	}
	runner := &processCohortRunner{
		study: study, manifest: manifest, compatibility: compatibility,
		runtime: runtime, root: root, snapshot: filepath.Join(root, "source"),
		binary: filepath.Join(root, "bin", "engram"), baseHome: filepath.Join(root, "base-home"),
	}
	cleanup := func(runErr error) (*processCohortRunner, error) {
		_ = runner.Close()
		return nil, runErr
	}
	if err := extractStudyRevision(ctx, runtime.SourceRepo, study.Contract.SourceRevision, runner.snapshot); err != nil {
		return cleanup(err)
	}
	if err := verifyStudyPlugin(runner.snapshot, study.Contract.Revisions.CodexPlugin.Version); err != nil {
		return cleanup(err)
	}
	if err := buildStudyBinary(ctx, runner.snapshot, runner.binary, study.Contract.Revisions.EngramBinary.Version, study.Contract.SourceRevision); err != nil {
		return cleanup(err)
	}
	if err := runner.prepareCodexHome(ctx); err != nil {
		return cleanup(err)
	}
	return runner, nil
}

func (runner *processCohortRunner) Close() error {
	if runner == nil || runner.root == "" {
		return nil
	}
	if runner.activeCell != "" {
		return fmt.Errorf("Recall study retained unpersisted cell evidence at %s", runner.activeCell)
	}
	err := os.RemoveAll(runner.root)
	runner.root = ""
	return err
}

func (runner *processCohortRunner) Run(ctx context.Context, planned PlannedRun, input TaskInput) (cohortRun, error) {
	if runner.activeCell != "" {
		return cohortRun{}, fmt.Errorf("Recall study runner still owns an unpersisted cell")
	}
	cellRoot, err := os.MkdirTemp(runner.root, "cell-")
	if err != nil {
		return cohortRun{}, err
	}
	runner.activeCell = cellRoot
	cleanup := func() error {
		if err := os.RemoveAll(cellRoot); err != nil {
			return err
		}
		if runner.activeCell == cellRoot {
			runner.activeCell = ""
		}
		return nil
	}
	failSetup := func(runErr error) (cohortRun, error) {
		return cohortRun{}, errors.Join(runErr, cleanup())
	}
	completed := func(row RunRow) (cohortRun, error) {
		return cohortRun{Row: row, Cleanup: cleanup}, nil
	}
	workspace := filepath.Join(cellRoot, "workspace")
	if err := copyStudyTree(runner.snapshot, workspace); err != nil {
		return failSetup(fmt.Errorf("copy Recall study source snapshot: %w", err))
	}
	if err := initializeStudyWorkspace(ctx, workspace, runner.study.Contract.Repository.URL); err != nil {
		return failSetup(err)
	}
	fixture := filepath.Join(workspace, filepath.FromSlash(input.FixturePath))
	if err := os.MkdirAll(filepath.Dir(fixture), 0o755); err != nil {
		return failSetup(err)
	}
	if err := os.WriteFile(fixture, []byte(input.FixtureUTF8), 0o600); err != nil {
		return failSetup(err)
	}
	fixtureBytes, err := os.ReadFile(fixture)
	if err != nil || !bytes.Equal(fixtureBytes, []byte(input.FixtureUTF8)) {
		return completed(operationalFailureRow(planned, "fixture_integrity_mismatch"))
	}
	if err := runner.study.VerifyTaskInput(runner.manifest, input); err != nil {
		return completed(operationalFailureRow(planned, "fixture_integrity_mismatch"))
	}

	harnessState := filepath.Join(cellRoot, "harness")
	modelState := filepath.Join(cellRoot, "model")
	home := filepath.Join(harnessState, "home")
	dataDir := filepath.Join(harnessState, "engram-data")
	toolsDir := filepath.Join(harnessState, "tools")
	for _, path := range []string{home, dataDir, toolsDir, filepath.Join(modelState, "tmp"), filepath.Join(modelState, "xdg-config"), filepath.Join(modelState, "xdg-cache"), filepath.Join(modelState, "xdg-data")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return failSetup(err)
		}
	}
	if err := copyStudyTree(runner.baseHome, home); err != nil {
		return failSetup(fmt.Errorf("copy Recall study Codex home: %w", err))
	}
	broadContext, err := runner.seedSyntheticMemory(ctx, workspace, dataDir, input)
	if err != nil {
		return failSetup(err)
	}
	injection, err := runner.injectionEvidence(planned, broadContext)
	if err != nil {
		return failSetup(err)
	}
	if err := writeStudyEngramWrapper(filepath.Join(toolsDir, "engram"), runner.binary, dataDir, planned.Treatment); err != nil {
		return failSetup(err)
	}

	finalMessagePath := filepath.Join(harnessState, "final-message.txt")
	arguments := runner.codexArguments(workspace, finalMessagePath)
	cellContext, cancel := context.WithTimeout(ctx, studyRunTimeout)
	defer cancel()
	command := exec.CommandContext(cellContext, runner.runtime.CodexBinary, arguments...)
	command.Dir = workspace
	command.Env = studyProcessEnvironment(home, toolsDir, modelState)
	command.Stdin = strings.NewReader(input.InstructionUTF8)
	stdout := &studyBoundedBuffer{maximum: maxStudyProcessBytes}
	stderr := &studyBoundedBuffer{maximum: maxStudyProcessBytes}
	command.Stdout, command.Stderr = stdout, stderr
	started := time.Now()
	commandErr := command.Run()
	elapsed := time.Since(started)
	if errors.Is(cellContext.Err(), context.DeadlineExceeded) {
		return completed(operationalFailureRow(planned, "runner_timeout"))
	}
	if commandErr != nil || stdout.exceeded || stderr.exceeded {
		return completed(operationalFailureRow(planned, "runner_process_failed"))
	}
	finalMessage, err := os.ReadFile(finalMessagePath)
	if err != nil || len(finalMessage) > maxStudyProcessBytes {
		return completed(operationalFailureRow(planned, "runner_process_failed"))
	}
	passed, err := verifyStudyTaskResult(workspace, input, finalMessage, stdout.Bytes())
	if err != nil {
		return cohortRun{Cleanup: cleanup}, err
	}
	row, err := runner.collectRow(planned, dataDir, elapsed, passed, injection)
	if err != nil {
		return cohortRun{Cleanup: cleanup}, fmt.Errorf("collect Recall study evidence: %w", err)
	}
	return completed(row)
}

func (runner *processCohortRunner) codexArguments(workspace, finalMessagePath string) []string {
	return []string{
		"exec", "--ephemeral", "--ignore-rules", "--json",
		"--model", runner.study.Contract.Model.Name,
		"--sandbox", "workspace-write", "--cd", workspace,
		"--output-last-message", finalMessagePath, "--dangerously-bypass-hook-trust",
		"--config", `approval_policy="never"`,
		"--config", fmt.Sprintf(`model_reasoning_effort=%q`, runner.study.Contract.Model.ReasoningEffort),
		"--config", `shell_environment_policy.inherit="all"`, "-",
	}
}

func (runner *processCohortRunner) prepareCodexHome(ctx context.Context) error {
	codexHome := filepath.Join(runner.baseHome, ".codex")
	for _, path := range []string{codexHome, filepath.Join(runner.root, "setup-tmp"), filepath.Join(runner.root, "setup-xdg-config"), filepath.Join(runner.root, "setup-xdg-cache"), filepath.Join(runner.root, "setup-xdg-data")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	if err := copyStudyFile(runner.runtime.AuthFile, filepath.Join(codexHome, "auth.json"), 0o600); err != nil {
		return fmt.Errorf("copy Recall study Codex auth: %w", err)
	}
	environment := []string{
		"HOME=" + runner.baseHome,
		"PATH=" + studySystemPath(),
		"TMPDIR=" + filepath.Join(runner.root, "setup-tmp"),
		"XDG_CONFIG_HOME=" + filepath.Join(runner.root, "setup-xdg-config"),
		"XDG_CACHE_HOME=" + filepath.Join(runner.root, "setup-xdg-cache"),
		"XDG_DATA_HOME=" + filepath.Join(runner.root, "setup-xdg-data"),
	}
	commands := [][]string{
		{"plugin", "marketplace", "add", runner.snapshot, "--json"},
		{"plugin", "add", "engram@engram", "--json"},
	}
	for _, arguments := range commands {
		command := exec.CommandContext(ctx, runner.runtime.CodexBinary, arguments...)
		command.Env = environment
		output, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("prepare Recall study Codex plugin: %w: %s", err, strings.TrimSpace(string(output)))
		}
	}
	return nil
}

func (runner *processCohortRunner) seedSyntheticMemory(ctx context.Context, workspace, dataDir string, input TaskInput) (string, error) {
	content := "What: Synthetic Recall study evidence for " + input.SamplingUnitID + " is " + strings.TrimSpace(input.ExpectedResultUTF8) +
		".\nWhy: It measures a consented disposable treatment without user or session content.\nWhere: recall study fixture " + input.FixturePath + ".\nLearned: This Memory is synthetic and must be deleted with the cell."
	command := exec.CommandContext(ctx, runner.binary, "save", "--title", "Synthetic Recall study "+input.SamplingUnitID, "--content", content, "--project", "engram", "--json")
	command.Dir = workspace
	command.Env = append(studyProcessEnvironment(filepath.Join(filepath.Dir(dataDir), "seed-home"), "", filepath.Dir(dataDir)), "ENGRAM_DATA_DIR="+dataDir, "ENGRAM_PROJECT=engram")
	if output, err := command.CombinedOutput(); err != nil {
		return "", fmt.Errorf("seed Recall study Memory: %w: %s", err, strings.TrimSpace(string(output)))
	}
	localStore, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		return "", err
	}
	context, contextErr := localStore.FormatContext("engram", "")
	closeErr := localStore.Close()
	if contextErr != nil || closeErr != nil {
		return "", errors.Join(contextErr, closeErr)
	}
	if strings.TrimSpace(context) == "" {
		return "", fmt.Errorf("synthetic Recall study Memory did not produce broad context")
	}
	return context, nil
}

func (runner *processCohortRunner) injectionEvidence(planned PlannedRun, broadContext string) (studyInjectionEvidence, error) {
	cue, err := codexlifecycle.ReadCanonicalCue(filepath.Join(runner.snapshot, "plugin", "codex"))
	if err != nil {
		return studyInjectionEvidence{}, err
	}
	if strings.TrimSpace(broadContext) == "" {
		return studyInjectionEvidence{}, fmt.Errorf("Recall study broad context evidence is unavailable")
	}
	extra := ""
	if planned.Treatment == "broad-chronological" {
		extra = broadContext
	}
	modelContext, truncated := codexlifecycle.BuildModelContext(cue, extra, codexlifecycle.MaxInjectedUTF8Bytes)
	if planned.Treatment == "broad-chronological" && truncated {
		return studyInjectionEvidence{}, fmt.Errorf("Recall study broad context exceeds the frozen injection budget")
	}
	return studyInjectionEvidence{
		expectedUTF8Bytes: int64(len(modelContext)),
		broadResultKey:    studyFileDigest("recall-study-broad-exposure-v1\x00" + broadContext),
	}, nil
}

func (runner *processCohortRunner) collectRow(planned PlannedRun, dataDir string, elapsed time.Duration, taskPassed bool, injection studyInjectionEvidence) (RunRow, error) {
	var lifecycle recallbaseline.LifecycleReport
	var start recallbaseline.OperationReport
	deadline := time.Now().Add(2 * time.Second)
	for {
		operation, observed, err := recallbaseline.InspectOperationReadOnly(recallbaseline.Config{DataDir: dataDir}, recallbaseline.SurfaceLifecycle, "session_start")
		if err != nil {
			return RunRow{}, err
		}
		if observed {
			start = operation
			ledger, err := recallbaseline.Open(recallbaseline.Config{DataDir: dataDir})
			if err != nil {
				return RunRow{}, err
			}
			report, reportErr := ledger.Report(runner.compatibility)
			closeErr := ledger.Close()
			if reportErr != nil || closeErr != nil {
				return RunRow{}, errors.Join(reportErr, closeErr)
			}
			lifecycle = report.Lifecycle
			break
		}
		if time.Now().After(deadline) {
			return RunRow{}, fmt.Errorf("Recall study SessionStart evidence is unavailable")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if start.TotalUTF8Bytes != injection.expectedUTF8Bytes {
		return RunRow{}, &invalidExecutionError{reasonCode: "session_start_treatment_mismatch"}
	}

	cfg := store.FallbackConfig(dataDir)
	localStore, err := store.New(cfg)
	if err != nil {
		return RunRow{}, err
	}
	snapshot, snapshotErr := localStore.RecallFeedbackReportSnapshot()
	closeErr := localStore.Close()
	if snapshotErr != nil || closeErr != nil {
		return RunRow{}, errors.Join(snapshotErr, closeErr)
	}
	recall, err := deriveStudyRecallEvidence(snapshot)
	if err != nil {
		return RunRow{}, err
	}

	row := RunRow{
		RunID: planned.RunID, SamplingUnitID: planned.SamplingUnitID, TaskClass: planned.TaskClass,
		Treatment: planned.Treatment, Outcome: "completed", TaskOutcome: "failed",
		FalseEmptyReview: "not_applicable", CheckpointSucceeded: lifecycle.Checkpoints.Completed > 0,
		StopConflictOrLoop:         lifecycle.Stop.Continuations > 0 || lifecycle.Stop.RecoveryExhausted > 0 || lifecycle.Stop.IntegrationFailures > 0,
		AutomaticInjectedUTF8Bytes: start.TotalUTF8Bytes, StartupCompactLatencyMillis: start.P95LatencyMillis,
	}
	if taskPassed {
		row.TaskOutcome = "succeeded"
		row.TimeToUsefulMillis = max(float64(elapsed)/float64(time.Millisecond), 0.001)
	}
	switch planned.Treatment {
	case "broad-chronological":
		if recall.searchObserved {
			return RunRow{}, &invalidExecutionError{reasonCode: "broad_targeted_recall_observed"}
		}
		row.RecallResultCount = 1
		row.Assessments = []Assessment{{ResultKey: injection.broadResultKey, Utility: "duplicate", Quality: "current", Source: "evaluator"}}
	case "targeted-recall":
		if !recall.searchObserved || recall.latencyMillis <= 0 {
			return RunRow{}, &invalidExecutionError{reasonCode: "targeted_recall_not_observed"}
		}
		row.RecallResultCount = len(recall.resultKeys)
		row.RecallLatencyMillis = recall.latencyMillis
		row.Assessments = recall.assessments
		row.FalseEmptyReview = recall.falseEmptyReview
	case "no-recall":
		if recall.searchObserved {
			return RunRow{}, &invalidExecutionError{reasonCode: "no_recall_treatment_contaminated"}
		}
	default:
		return RunRow{}, fmt.Errorf("Recall study treatment is unknown")
	}
	return row, validateRunRow(row)
}

func deriveStudyRecallEvidence(snapshot *store.RecallFeedbackReportSnapshot) (studyRecallEvidence, error) {
	evidence := studyRecallEvidence{falseEmptyReview: "not_applicable"}
	var searchOperations []store.RecallFeedbackOperationalMetric
	for _, operation := range snapshot.Operations {
		if operation.Operation == "search" {
			evidence.searchObserved = true
			searchOperations = append(searchOperations, operation)
		}
	}
	if !evidence.searchObserved {
		return evidence, nil
	}
	if len(snapshot.Runs) == 0 || len(snapshot.Runs) != len(searchOperations) {
		return studyRecallEvidence{}, &invalidExecutionError{reasonCode: "targeted_recall_attribution_unavailable"}
	}
	runKeys := make(map[string]bool, len(snapshot.Runs))
	exposuresByRun := make(map[string]int, len(snapshot.Runs))
	wantExposures := 0
	for index, run := range snapshot.Runs {
		operation := searchOperations[index]
		if run.RunKey == "" || run.ResultCount < 0 || run.ElapsedMonotonicMS == nil || *run.ElapsedMonotonicMS <= 0 ||
			runKeys[run.RunKey] || operation.ExposedResults != run.ResultCount || operation.ElapsedMonotonicMS == nil ||
			*operation.ElapsedMonotonicMS != *run.ElapsedMonotonicMS {
			return studyRecallEvidence{}, fmt.Errorf("Recall study search evidence is invalid")
		}
		runKeys[run.RunKey] = true
		wantExposures += run.ResultCount
		evidence.latencyMillis += float64(*run.ElapsedMonotonicMS)
	}
	exposureKeys := make(map[string]bool)
	observedExposures := 0
	for _, exposure := range snapshot.Exposures {
		if !runKeys[exposure.RunKey] || exposure.MemoryKey == "" {
			continue
		}
		observedExposures++
		exposuresByRun[exposure.RunKey]++
		exposureKeys[exposure.MemoryKey] = true
	}
	if observedExposures != wantExposures {
		return studyRecallEvidence{}, fmt.Errorf("Recall study exposure evidence is incomplete")
	}
	for _, run := range snapshot.Runs {
		if exposuresByRun[run.RunKey] != run.ResultCount {
			return studyRecallEvidence{}, fmt.Errorf("Recall study exposure attribution is inconsistent")
		}
	}
	for key := range exposureKeys {
		evidence.resultKeys = append(evidence.resultKeys, key)
	}
	sort.Strings(evidence.resultKeys)
	for _, label := range snapshot.Labels {
		if !runKeys[label.RunKey] || !exposureKeys[label.MemoryKey] {
			continue
		}
		utility, quality := label.Utility, label.Quality
		if utility == "" {
			utility = "unknown"
		}
		if quality == "" {
			quality = "unknown"
		}
		evidence.assessments = append(evidence.assessments, Assessment{
			ResultKey: label.MemoryKey, Utility: utility, Quality: quality, Source: label.Source,
		})
	}
	sort.Slice(evidence.assessments, func(i, j int) bool {
		left, right := evidence.assessments[i], evidence.assessments[j]
		return left.ResultKey+"\x00"+left.Source+"\x00"+left.Utility+"\x00"+left.Quality <
			right.ResultKey+"\x00"+right.Source+"\x00"+right.Utility+"\x00"+right.Quality
	})
	if len(evidence.resultKeys) == 0 {
		evidence.falseEmptyReview = "unknown"
		confirmed, rejected := false, false
		for _, review := range snapshot.EmptyReviews {
			if !runKeys[review.RunKey] {
				continue
			}
			if review.Value {
				confirmed = true
			} else {
				rejected = true
			}
		}
		switch {
		case confirmed && !rejected:
			evidence.falseEmptyReview = "confirmed"
		case rejected && !confirmed:
			evidence.falseEmptyReview = "rejected"
		}
	}
	return evidence, nil
}

func operationalFailureRow(planned PlannedRun, code string) RunRow {
	return RunRow{
		RunID: planned.RunID, SamplingUnitID: planned.SamplingUnitID, TaskClass: planned.TaskClass,
		Treatment: planned.Treatment, Outcome: "operational_failure", TaskOutcome: "not_applicable",
		OmissionCode: code, FalseEmptyReview: "not_applicable",
	}
}

func verifyStudyTaskResult(workspace string, input TaskInput, finalMessage, eventStream []byte) (bool, error) {
	status := exec.Command("git", "-C", workspace, "status", "--porcelain=v1", "--untracked-files=all")
	output, err := status.Output()
	if err != nil {
		return false, fmt.Errorf("inspect Recall study workspace: %w", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if line == "" {
			continue
		}
		if len(line) < 4 || filepath.ToSlash(strings.TrimSpace(line[3:])) != input.FixturePath {
			return false, nil
		}
	}
	fixture, err := os.ReadFile(filepath.Join(workspace, filepath.FromSlash(input.FixturePath)))
	if err != nil {
		return false, nil
	}
	switch input.TaskClass {
	case "implementation", "routine-non-durable":
		return equalStudyJSON(fixture, []byte(input.ExpectedResultUTF8)), nil
	case "repository-question", "diagnosis", "verification":
		if string(fixture) != input.FixtureUTF8 {
			return false, nil
		}
		return studyTaskOutputMatches(finalMessage, eventStream, []byte(input.ExpectedResultUTF8)), nil
	default:
		return false, fmt.Errorf("Recall study task class is unknown")
	}
}

func studyTaskOutputMatches(finalMessage, eventStream, expected []byte) bool {
	var expectedValue any
	if decodeStrictJSON(expected, &expectedValue) != nil {
		return false
	}
	var observed []any
	for _, line := range bytes.Split(eventStream, []byte{'\n'}) {
		var event struct {
			Type string `json:"type"`
			Item struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"item"`
		}
		if json.Unmarshal(line, &event) != nil || event.Type != "item.completed" || event.Item.Type != "agent_message" {
			continue
		}
		var value any
		if decodeStrictJSON([]byte(event.Item.Text), &value) == nil {
			observed = append(observed, value)
		}
	}
	var finalValue any
	if decodeStrictJSON(finalMessage, &finalValue) == nil && (len(observed) == 0 || !reflect.DeepEqual(finalValue, observed[len(observed)-1])) {
		observed = append(observed, finalValue)
	}
	return len(observed) == 1 && reflect.DeepEqual(observed[0], expectedValue)
}

func equalStudyJSON(actual, expected []byte) bool {
	var actualValue, expectedValue any
	if err := decodeStrictJSON(actual, &actualValue); err != nil {
		return false
	}
	if err := decodeStrictJSON(expected, &expectedValue); err != nil {
		return false
	}
	return reflect.DeepEqual(actualValue, expectedValue)
}

func extractStudyRevision(ctx context.Context, repo, revision, destination string) error {
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "git", "-C", repo, "archive", "--format=tar", revision)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	stderr := &studyBoundedBuffer{maximum: maxStudyProcessBytes}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		return err
	}
	extractErr := extractStudyTar(stdout, destination)
	waitErr := command.Wait()
	if extractErr != nil || waitErr != nil {
		return errors.Join(extractErr, fmt.Errorf("archive Recall study source: %w: %s", waitErr, strings.TrimSpace(string(stderr.Bytes()))))
	}
	return nil
}

func extractStudyTar(reader io.Reader, destination string) error {
	archive := tar.NewReader(reader)
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		clean := filepath.Clean(filepath.FromSlash(header.Name))
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("Recall study source archive contains an unsafe path")
		}
		target := filepath.Join(destination, clean)
		if relative, err := filepath.Rel(destination, target); err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("Recall study source archive escapes its destination")
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, fs.FileMode(header.Mode).Perm()); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(header.Mode).Perm())
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(file, archive)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				return errors.Join(copyErr, closeErr)
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
		}
	}
}

func verifyStudyPlugin(snapshot, wantVersion string) error {
	raw, err := os.ReadFile(filepath.Join(snapshot, "plugin", "codex", ".codex-plugin", "plugin.json"))
	if err != nil {
		return err
	}
	var manifest struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Version != wantVersion {
		return fmt.Errorf("Recall study Codex plugin version does not match the frozen tuple")
	}
	return nil
}

func buildStudyBinary(ctx context.Context, source, output, version, revision string) error {
	if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
		return err
	}
	flags := "-X main.version=" + version + " -X main.commit=" + revision
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-ldflags", flags, "-o", output, "./cmd/engram")
	command.Dir = source
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local")
	if combined, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build frozen Recall study Engram: %w: %s", err, strings.TrimSpace(string(combined)))
	}
	return nil
}

func initializeStudyWorkspace(ctx context.Context, workspace, remote string) error {
	commands := [][]string{
		{"init", "-q"}, {"config", "user.email", "recall-study@example.invalid"},
		{"config", "user.name", "Recall Study"}, {"remote", "add", "origin", remote},
		{"add", "-A"}, {"commit", "-qm", "frozen source snapshot"},
	}
	for _, arguments := range commands {
		command := exec.CommandContext(ctx, "git", append([]string{"-C", workspace}, arguments...)...)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("initialize Recall study workspace: %w: %s", err, strings.TrimSpace(string(output)))
		}
	}
	return nil
}

func writeStudyEngramWrapper(path, binary, dataDir, treatment string) error {
	canary := ""
	restrictRecall := treatment == "broad-chronological" || treatment == "no-recall"
	if treatment == "targeted-recall" || treatment == "no-recall" {
		canary = "targeted-recall"
	}
	script := "#!/bin/sh\n" +
		"export ENGRAM_DATA_DIR=" + shellStudyQuote(dataDir) + "\n" +
		"export ENGRAM_PROJECT=engram\n" +
		"export ENGRAM_RECALL_BASELINE=1\n"
	if canary != "" {
		script += "export ENGRAM_CODEX_RECALL_CANARY=" + shellStudyQuote(canary) + "\n"
	}
	if restrictRecall {
		script += "case \"$1\" in\n" +
			"  search|get|context) printf '%s\\n' '{\"code\":\"recall_disabled_by_study_treatment\",\"message\":\"Recall is disabled for this frozen treatment\"}' >&2; exit 2 ;;\n" +
			"  mcp) shift; exec " + shellStudyQuote(binary) + " mcp --tools=mem_current_project,mem_checkpoint,mem_checkpoint_status ;;\n" +
			"esac\n"
	}
	script += "exec " + shellStudyQuote(binary) + " \"$@\"\n"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(script), 0o700)
}

func studyProcessEnvironment(home, toolsDir, state string) []string {
	paths := []string{}
	if toolsDir != "" {
		paths = append(paths, toolsDir)
	}
	paths = append(paths, filepath.SplitList(studySystemPath())...)
	return []string{
		"HOME=" + home, "PATH=" + strings.Join(paths, string(os.PathListSeparator)),
		"TMPDIR=" + filepath.Join(state, "tmp"),
		"XDG_CONFIG_HOME=" + filepath.Join(state, "xdg-config"),
		"XDG_CACHE_HOME=" + filepath.Join(state, "xdg-cache"),
		"XDG_DATA_HOME=" + filepath.Join(state, "xdg-data"),
	}
}

func studySystemPath() string {
	return strings.Join([]string{"/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}, string(os.PathListSeparator))
}

func copyStudyTree(source, destination string) error {
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
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			return os.Symlink(link, target)
		}
		return copyStudyFile(path, target, info.Mode().Perm())
	})
}

func copyStudyFile(source, destination string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
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

func shellStudyQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func studyFileDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

type studyBoundedBuffer struct {
	buffer   bytes.Buffer
	maximum  int
	exceeded bool
}

func (buffer *studyBoundedBuffer) Write(value []byte) (int, error) {
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

func (buffer *studyBoundedBuffer) Bytes() []byte { return buffer.buffer.Bytes() }
