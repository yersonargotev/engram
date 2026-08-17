package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	projectpkg "github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
	"github.com/yersonargotev/engram/internal/taskbriefing"
)

func TestEncodeContextBriefingEnforcesPublicByteBudget(t *testing.T) {
	memories := make([]contextBriefingMemory, 0, 3)
	for index := 0; index < 3; index++ {
		memories = append(memories, contextBriefingMemory{
			Memory: store.Observation{
				ID: int64(index + 1), Type: "decision", Title: "Bounded task briefing",
				Content: strings.Repeat("complete durable memory content ", 55), Scope: "project",
			},
			Evidence: []taskbriefing.SelectionEvidence{{
				Signal: taskbriefing.SignalTaskIntent, MatchedTerms: []string{"bounded", "briefing"}, MatchedFields: []string{"title", "content"},
			}},
		})
	}
	base := contextBriefingOutput{Mode: "brief", Project: "engram", Memories: memories, Diagnostics: []taskbriefing.Diagnostic{}}
	retainedByFormat := make(map[string]int)

	for _, jsonMode := range []bool{false, true} {
		name := "human"
		if jsonMode {
			name = "json"
		}
		t.Run(name, func(t *testing.T) {
			encoded, err := encodeContextBriefing(base, jsonMode, true, taskbriefing.CalibratedDefaults.TotalOutputBudget)
			if err != nil {
				t.Fatalf("encodeContextBriefing: %v", err)
			}
			if len(encoded) > taskbriefing.CalibratedDefaults.TotalOutputBudget {
				t.Fatalf("encoded bytes = %d, budget = %d", len(encoded), taskbriefing.CalibratedDefaults.TotalOutputBudget)
			}
			text := string(encoded)
			if !strings.Contains(text, "output_budget_exhausted") {
				t.Fatalf("output lacks budget diagnostic: %q", text)
			}
			if jsonMode {
				var output contextBriefingOutput
				if err := json.Unmarshal(encoded, &output); err != nil {
					t.Fatalf("decode bounded JSON: %v", err)
				}
				if output.BudgetOmissions == 0 || len(output.Memories) == 0 {
					t.Fatalf("bounded output = %#v, want whole retained and omitted memories", output)
				}
				retainedByFormat[name] = len(output.Memories)
				for _, memory := range output.Memories {
					if memory.Memory.Content != memories[0].Memory.Content {
						t.Fatal("retained memory was truncated")
					}
				}
			} else {
				retainedByFormat[name] = strings.Count(text, "### #")
				if !strings.Contains(text, "Omitted:") || !strings.Contains(text, memories[0].Memory.Content) {
					t.Fatalf("human output does not preserve whole-memory omission contract: %q", text)
				}
			}
		})
	}
	if retainedByFormat["human"] <= retainedByFormat["json"] {
		t.Fatalf("selected format did not use its own byte budget: %#v", retainedByFormat)
	}
}

func TestEncodeContextBriefingRemovesConflictDiagnosticAfterBudgetOmission(t *testing.T) {
	memories := []contextBriefingMemory{
		{Memory: store.Observation{SyncID: "memory-a", Title: "Conflict A", Content: strings.Repeat("complete memory A ", 40)}},
		{Memory: store.Observation{SyncID: "memory-b", Title: "Conflict B", Content: strings.Repeat("complete memory B ", 40)}},
	}
	output := contextBriefingOutput{
		Mode:        "brief",
		Project:     "engram",
		Memories:    memories,
		Diagnostics: []taskbriefing.Diagnostic{{Code: taskbriefing.DiagnosticSelectedMemoryConflict}},
		conflictPairs: []taskbriefing.ConflictPair{{
			SourceID: "memory-a",
			TargetID: "memory-b",
		}},
	}
	oneMemory := output
	oneMemory.Memories = memories[:1]
	oneMemory.Diagnostics = []taskbriefing.Diagnostic{{Code: taskbriefing.DiagnosticOutputBudgetExhausted}}
	oneMemory.BudgetOmissions = 1
	encodedOne, err := json.Marshal(oneMemory)
	if err != nil {
		t.Fatalf("marshal one-memory output: %v", err)
	}

	encoded, err := encodeContextBriefing(output, true, true, len(encodedOne)+1)
	if err != nil {
		t.Fatalf("encodeContextBriefing: %v", err)
	}
	var bounded contextBriefingOutput
	if err := json.Unmarshal(encoded, &bounded); err != nil {
		t.Fatalf("decode bounded output: %v", err)
	}
	if len(bounded.Memories) != 1 || bounded.BudgetOmissions != 1 {
		t.Fatalf("bounded output = %#v, want one retained and one omitted memory", bounded)
	}
	if hasContextBriefingDiagnostic(bounded.Diagnostics, taskbriefing.DiagnosticSelectedMemoryConflict) {
		t.Fatalf("stale conflict diagnostic remained after one side was omitted: %#v", bounded.Diagnostics)
	}
	if !hasContextBriefingDiagnostic(bounded.Diagnostics, taskbriefing.DiagnosticOutputBudgetExhausted) {
		t.Fatalf("missing output budget diagnostic: %#v", bounded.Diagnostics)
	}
}

func TestFormatContextBriefingDiagnosticCoversKnownCodes(t *testing.T) {
	diagnostics := []taskbriefing.Diagnostic{
		{Code: taskbriefing.DiagnosticNoUsableSignals},
		{Code: taskbriefing.DiagnosticRepositoryProjectUnresolved},
		{Code: taskbriefing.DiagnosticRepositoryProjectMismatch},
		{Code: taskbriefing.DiagnosticBranchBaseUnresolved},
		{Code: taskbriefing.DiagnosticGitOperationFailed, Sources: []taskbriefing.SignalType{taskbriefing.SignalStagedDiff}},
		{Code: taskbriefing.DiagnosticTaskInputTruncated, Truncations: []taskbriefing.InputTruncation{{Signal: taskbriefing.SignalTaskIntent, TotalTerms: 13, AnalyzedTerms: 12, OmittedTerms: 1, CountComplete: true}}},
		{Code: taskbriefing.DiagnosticRepositoryInputTruncated, Truncations: []taskbriefing.InputTruncation{{Signal: taskbriefing.SignalBranchDiff, TotalTerms: 17, AnalyzedTerms: 16, OmittedTerms: 1, CountComplete: false}}},
		{Code: taskbriefing.DiagnosticResultLimitReached},
		{Code: taskbriefing.DiagnosticOutputBudgetExhausted},
		{Code: taskbriefing.DiagnosticSelectedMemoryConflict},
	}
	for _, diagnostic := range diagnostics {
		message := formatContextBriefingDiagnostic(diagnostic)
		if message == "" || message == "Task briefing completed with a typed degradation." {
			t.Errorf("diagnostic %s lacks owned formatting: %q", diagnostic.Code, message)
		}
	}
	if message := formatContextBriefingDiagnostic(diagnostics[4]); !strings.Contains(message, "staged_diff") {
		t.Fatalf("Git diagnostic lacks sources: %q", message)
	}
	if message := formatContextBriefingDiagnostic(diagnostics[6]); !strings.Contains(message, "acquisition cutoff reached") {
		t.Fatalf("incomplete truncation lacks prefix warning: %q", message)
	}
}

func TestCmdContextBriefUsesCleanBranchEvidenceWithoutTask(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "brief-clean-branch", "engram", "decision", "Clean branch evidence", "Use committed branch evidence from repository paths.", "project")
	repo := newBriefingCLIRepository(t, "engram")
	t.Chdir(repo)

	withArgs(t, "engram", "context", "engram", "--brief", "--base", "main", "--json")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	if strings.Contains(stdout, "Clean branch evidence guides selection") {
		t.Fatalf("structured output emitted raw diff content: %q", stdout)
	}
	result := decodeCLIJSON(t, stdout)
	base := result["base_resolution"].(map[string]any)
	if base["ref"] != "main" || base["source"] != "explicit" {
		t.Fatalf("base resolution = %#v", base)
	}
	memories := result["memories"].([]any)
	if len(memories) != 1 {
		t.Fatalf("memories = %#v, want clean-branch selection", memories)
	}
	evidence := memories[0].(map[string]any)["evidence"].([]any)
	if len(evidence) != 4 {
		t.Fatalf("evidence = %#v, want branch, diff, path, and commit sources", evidence)
	}

	withArgs(t, "engram", "context", "engram", "--brief", "--base", "main")
	stdout, stderr = captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" || !strings.Contains(stdout, "Base: main (explicit)") {
		t.Fatalf("human base resolution missing: stdout = %q, stderr = %q", stdout, stderr)
	}
}

func TestCmdContextBriefRepositoryMismatchReturnsSuccessfulEmptyBriefing(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "brief-other-repository", "engram", "decision", "Clean branch evidence", "Use committed branch evidence from repository paths.", "project")
	repo := newBriefingCLIRepository(t, "other-project")
	t.Chdir(repo)

	withArgs(t, "engram", "context", "engram", "--brief", "--base", "main", "--json")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	result := decodeCLIJSON(t, stdout)
	if len(result["memories"].([]any)) != 0 {
		t.Fatalf("memories = %#v, want empty mismatch result", result["memories"])
	}
	if _, exists := result["base_resolution"]; exists {
		t.Fatalf("mismatched repository leaked base resolution: %#v", result)
	}
	diagnostics := result["diagnostics"].([]any)
	if len(diagnostics) != 2 || diagnostics[0].(map[string]any)["code"] != "repository_project_mismatch" || diagnostics[1].(map[string]any)["code"] != "no_usable_signals" {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
}

func TestCmdContextBriefResolvesAutomaticBranchBases(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(t *testing.T, repo string)
		wantRef    string
		wantSource string
	}{
		{
			name: "configured upstream",
			configure: func(t *testing.T, repo string) {
				runBriefingGit(t, repo, "update-ref", "refs/remotes/origin/integration", "main")
				runBriefingGit(t, repo, "branch", "--set-upstream-to=origin/integration")
			},
			wantRef:    "origin/integration",
			wantSource: "upstream",
		},
		{
			name: "remote default after same-branch upstream",
			configure: func(t *testing.T, repo string) {
				runBriefingGit(t, repo, "update-ref", "refs/remotes/origin/main", "main")
				runBriefingGit(t, repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
				runBriefingGit(t, repo, "update-ref", "refs/remotes/origin/feat/clean-branch-evidence", "HEAD")
				runBriefingGit(t, repo, "branch", "--set-upstream-to=origin/feat/clean-branch-evidence")
			},
			wantRef:    "origin/main",
			wantSource: "remote_default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			mustSeedObservation(t, cfg, "brief-auto-base", "engram", "decision", "Clean branch evidence", "Use committed branch evidence from repository paths.", "project")
			repo := newBriefingCLIRepository(t, "engram")
			tt.configure(t, repo)
			t.Chdir(repo)

			withArgs(t, "engram", "context", "engram", "--brief", "--json")
			stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
			if stderr != "" {
				t.Fatalf("stderr = %q", stderr)
			}
			result := decodeCLIJSON(t, stdout)
			base := result["base_resolution"].(map[string]any)
			if base["ref"] != tt.wantRef || base["source"] != tt.wantSource {
				t.Fatalf("base resolution = %#v, want %s from %s", base, tt.wantRef, tt.wantSource)
			}
			if len(result["memories"].([]any)) != 1 {
				t.Fatalf("memories = %#v, want clean-branch selection", result["memories"])
			}
		})
	}
}

func TestCmdContextBriefUnresolvedBaseContinuesWithTaskOnly(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "brief-unresolved-base", "engram", "decision", "Deterministic briefing", "Implement deterministic briefing selection.", "project")
	repo := newBriefingCLIRepository(t, "engram")
	t.Chdir(repo)

	withArgs(t, "engram", "context", "engram", "--brief", "--task", "implement deterministic briefing selection", "--json")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	result := decodeCLIJSON(t, stdout)
	if _, exists := result["base_resolution"]; exists {
		t.Fatalf("unexpected base resolution: %#v", result)
	}
	if !jsonDiagnosticsContain(result["diagnostics"].([]any), "branch_base_unresolved") {
		t.Fatalf("diagnostics = %#v, want unresolved base", result["diagnostics"])
	}
	evidence := result["memories"].([]any)[0].(map[string]any)["evidence"].([]any)
	if len(evidence) != 1 || evidence[0].(map[string]any)["signal"] != "task_intent" {
		t.Fatalf("evidence = %#v, want task-only continuation", evidence)
	}
}

func TestCmdContextBriefFusesTaskAndCleanBranchEvidence(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "brief-fusion", "engram", "decision", "Clean branch evidence", "Use database migration strategy with committed repository paths.", "project")
	repo := newBriefingCLIRepository(t, "engram")
	t.Chdir(repo)

	withArgs(t, "engram", "context", "engram", "--brief", "--task", "database migration strategy", "--base", "main", "--json")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	result := decodeCLIJSON(t, stdout)
	evidence := result["memories"].([]any)[0].(map[string]any)["evidence"].([]any)
	if !jsonEvidenceContains(evidence, "task_intent") || !jsonEvidenceContains(evidence, "affected_path") {
		t.Fatalf("evidence = %#v, want authoritative task plus repository source", evidence)
	}
}

func TestCmdContextBriefExposesDirtyWorktreeSourceTypesWithoutRawContent(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "brief-dirty-worktree", "engram", "decision", "Dirty worktree evidence", "Staged worktree evidence, unstaged worktree evidence, and untracked worktree evidence select durable memories.", "project")
	repo := newBriefingCLIRepository(t, "engram")
	writeBriefingCLIFile(t, filepath.Join(repo, "staged_worktree_evidence.go"), "package evidence\n\n// staged worktree evidence\n")
	runBriefingGit(t, repo, "add", "staged_worktree_evidence.go")
	writeBriefingCLIFile(t, filepath.Join(repo, "README.md"), "base\n// unstaged worktree evidence\n")
	writeBriefingCLIFile(t, filepath.Join(repo, "untracked_worktree_evidence.txt"), "untracked file content must remain private\n")
	t.Chdir(repo)

	withArgs(t, "engram", "context", "engram", "--brief", "--base", "main", "--json")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	if strings.Contains(stdout, "// staged worktree evidence") || strings.Contains(stdout, "// unstaged worktree evidence") || strings.Contains(stdout, "untracked file content must remain private") {
		t.Fatalf("structured output emitted raw worktree content: %q", stdout)
	}
	memories := decodeCLIJSON(t, stdout)["memories"].([]any)
	if len(memories) != 1 {
		t.Fatalf("memories = %#v, want dirty-worktree selection", memories)
	}
	evidence := memories[0].(map[string]any)["evidence"].([]any)
	for _, source := range []string{"staged_diff", "unstaged_diff", "untracked_path"} {
		if !jsonEvidenceContains(evidence, source) {
			t.Fatalf("evidence = %#v, want %s source", evidence, source)
		}
	}
}

func TestCmdContextBriefHumanOutputExplainsPartialGitFailuresAndTruncation(t *testing.T) {
	cfg := testConfig(t)
	original := generateTaskBriefing
	generateTaskBriefing = func(*store.Store, taskbriefing.Input) (taskbriefing.Result, error) {
		return taskbriefing.Result{Diagnostics: []taskbriefing.Diagnostic{
			{Code: taskbriefing.DiagnosticGitOperationFailed, Sources: []taskbriefing.SignalType{taskbriefing.SignalStagedDiff, taskbriefing.SignalUntrackedPath}},
			{Code: taskbriefing.DiagnosticRepositoryInputTruncated, Truncations: []taskbriefing.InputTruncation{{Signal: taskbriefing.SignalUnstagedDiff, TotalTerms: 18, AnalyzedTerms: 16, OmittedTerms: 2, CountComplete: true}}},
		}}, nil
	}
	t.Cleanup(func() { generateTaskBriefing = original })
	mustSeedObservation(t, cfg, "brief-diagnostic-output", "engram", "decision", "Diagnostic output", "Diagnostic output memory.", "project")

	withArgs(t, "engram", "context", "engram", "--brief")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	for _, expected := range []string{"staged_diff", "untracked_path", "unstaged_diff: 18 total, 16 analyzed, 2 omitted"} {
		if !strings.Contains(stdout, expected) {
			t.Fatalf("human output missing %q: %q", expected, stdout)
		}
	}
}

func TestCmdContextBriefReturnsStructuredTaskSelection(t *testing.T) {
	cfg := testConfig(t)
	disableBriefingRepositoryInspection(t)
	stubExitWithPanic(t)
	mustSeedObservation(t, cfg, "brief-relevant", "engram", "decision", "Deterministic task briefing", "Keep the complete durable memory content in task briefing selection.", "project")
	mustSeedObservation(t, cfg, "brief-unrelated", "engram", "decision", "Cloud enrollment", "Cloud enrollment remains an explicit operation.", "project")

	const task = "implement deterministic task briefing selection"
	withArgs(t, "engram", "context", "engram", "--brief", "--task", task, "--scope", "project", "--limit", "3", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdContext(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("exit = %v, stderr = %q", recovered, stderr)
	}
	if strings.Contains(stdout, task) {
		t.Fatalf("structured output persisted or emitted raw task intent: %q", stdout)
	}

	result := decodeCLIJSON(t, stdout)
	if result["mode"] != "brief" || result["project"] != "engram" {
		t.Fatalf("result metadata = %#v", result)
	}
	memories := result["memories"].([]any)
	if len(memories) != 1 {
		t.Fatalf("memories = %#v, want one precise selection", memories)
	}
	selected := memories[0].(map[string]any)
	memory := selected["memory"].(map[string]any)
	if memory["content"] != "Keep the complete durable memory content in task briefing selection." {
		t.Fatalf("selected memory was not emitted whole: %#v", memory)
	}
	evidence := selected["evidence"].([]any)
	if len(evidence) != 1 || evidence[0].(map[string]any)["signal"] != "task_intent" {
		t.Fatalf("selection evidence = %#v", evidence)
	}
}

func TestCmdContextBriefHumanOutputIsCompleteAndExplainable(t *testing.T) {
	cfg := testConfig(t)
	const content = "Keep every selected durable memory whole, including this final sentence."
	mustSeedObservation(t, cfg, "brief-human", "engram", "decision", "Whole task briefing memory", content, "personal")

	withArgs(t, "engram", "context", "engram", "--brief", "--task", "whole task briefing memory", "--scope", "personal")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	for _, expected := range []string{"## Task Briefing", "Project: engram", content, "Selection evidence:", "task_intent"} {
		if !strings.Contains(stdout, expected) {
			t.Fatalf("human output missing %q: %q", expected, stdout)
		}
	}
}

func TestCmdContextBriefWithoutTaskReturnsSuccessfulEmptyGuidance(t *testing.T) {
	cfg := testConfig(t)
	disableBriefingRepositoryInspection(t)
	mustSeedObservation(t, cfg, "brief-empty", "engram", "decision", "Cloud enrollment", "Cloud enrollment remains explicit.", "project")
	withArgs(t, "engram", "context", "engram", "--brief", "--json")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	result := decodeCLIJSON(t, stdout)
	if len(result["memories"].([]any)) != 0 {
		t.Fatalf("memories = %#v, want explicit empty selection", result["memories"])
	}
	diagnostics := result["diagnostics"].([]any)
	if len(diagnostics) != 1 || diagnostics[0].(map[string]any)["code"] != "no_usable_signals" {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}

	withArgs(t, "engram", "context", "engram", "--brief")
	stdout, stderr = captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" || !strings.Contains(stdout, "Provide --task \"<intent>\"") {
		t.Fatalf("empty human output = %q, stderr = %q", stdout, stderr)
	}
}

func TestCmdContextBriefRequiresExactlyOneResolvedProject(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)
	original := detectProjectFull
	detectProjectFull = func(string) projectpkg.DetectionResult {
		return projectpkg.DetectionResult{
			Source:            projectpkg.SourceAmbiguous,
			Error:             projectpkg.ErrAmbiguousProject,
			AvailableProjects: []string{"alpha", "beta"},
		}
	}
	t.Cleanup(func() { detectProjectFull = original })

	withArgs(t, "engram", "context", "--brief", "--task", "deterministic briefing", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdContext(cfg) })
	if stdout != "" || recovered == nil {
		t.Fatalf("stdout = %q, exit = %v", stdout, recovered)
	}
	failure := decodeCLIJSON(t, stderr)
	if failure["code"] != "ambiguous_project" {
		t.Fatalf("failure = %#v", failure)
	}
	details := failure["details"].(map[string]any)
	if len(details["available_projects"].([]any)) != 2 {
		t.Fatalf("details = %#v", details)
	}
}

func TestCmdContextBriefRejectsUnknownExplicitProject(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)
	mustSeedObservation(t, cfg, "known-project", "known", "decision", "Known project", "Known project memory.", "project")

	withArgs(t, "engram", "context", "missing", "--brief", "--task", "deterministic briefing", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdContext(cfg) })
	if stdout != "" || recovered == nil {
		t.Fatalf("stdout = %q, exit = %v", stdout, recovered)
	}
	failure := decodeCLIJSON(t, stderr)
	if failure["code"] != "unknown_project" {
		t.Fatalf("failure = %#v", failure)
	}
	details := failure["details"].(map[string]any)
	available := details["available_projects"].([]any)
	if len(available) != 1 || available[0] != "known" {
		t.Fatalf("details = %#v", details)
	}
}

func TestCmdContextBriefRejectsAbsentDetectedProject(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)
	original := detectProjectFull
	detectProjectFull = func(string) projectpkg.DetectionResult { return projectpkg.DetectionResult{} }
	t.Cleanup(func() { detectProjectFull = original })

	withArgs(t, "engram", "context", "--brief", "--task", "deterministic briefing", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdContext(cfg) })
	if stdout != "" || recovered == nil {
		t.Fatalf("stdout = %q, exit = %v", stdout, recovered)
	}
	if failure := decodeCLIJSON(t, stderr); failure["code"] != "project_not_resolved" {
		t.Fatalf("failure = %#v", failure)
	}
}

func TestCmdContextBriefStoreFailureIsStructured(t *testing.T) {
	stubExitWithPanic(t)
	original := storeNew
	storeNew = func(store.Config) (*store.Store, error) { return nil, errors.New("store unavailable") }
	t.Cleanup(func() { storeNew = original })

	withArgs(t, "engram", "context", "engram", "--brief", "--task", "deterministic briefing", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdContext(store.Config{}) })
	if stdout != "" || recovered == nil {
		t.Fatalf("stdout = %q, exit = %v", stdout, recovered)
	}
	failure := decodeCLIJSON(t, stderr)
	if failure["code"] != "store_error" || !strings.Contains(failure["message"].(string), "store unavailable") {
		t.Fatalf("failure = %#v", failure)
	}
}

func TestCmdContextBriefPostOpenFailuresAreStructured(t *testing.T) {
	cfg := testConfig(t)
	tests := []struct {
		name     string
		wantCode string
		stub     func(t *testing.T)
	}{
		{
			name:     "project existence",
			wantCode: "project_resolution_failed",
			stub: func(t *testing.T) {
				original := storeProjectExists
				storeProjectExists = func(*store.Store, string) (bool, error) { return false, errors.New("project lookup failed") }
				t.Cleanup(func() { storeProjectExists = original })
			},
		},
		{
			name:     "available projects",
			wantCode: "project_resolution_failed",
			stub: func(t *testing.T) {
				originalExists := storeProjectExists
				originalList := storeListProjects
				storeProjectExists = func(*store.Store, string) (bool, error) { return false, nil }
				storeListProjects = func(*store.Store) ([]string, error) { return nil, errors.New("project list failed") }
				t.Cleanup(func() {
					storeProjectExists = originalExists
					storeListProjects = originalList
				})
			},
		},
		{
			name:     "briefing retrieval",
			wantCode: "memory_store_failure",
			stub: func(t *testing.T) {
				originalExists := storeProjectExists
				originalGenerate := generateTaskBriefing
				storeProjectExists = func(*store.Store, string) (bool, error) { return true, nil }
				closedStore, err := store.New(cfg)
				if err != nil {
					t.Fatalf("store.New: %v", err)
				}
				if err := closedStore.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				generateTaskBriefing = func(*store.Store, taskbriefing.Input) (taskbriefing.Result, error) {
					return taskbriefing.New(closedStore).Generate(taskbriefing.Input{Project: "engram", TaskIntent: "deterministic briefing"})
				}
				t.Cleanup(func() {
					storeProjectExists = originalExists
					generateTaskBriefing = originalGenerate
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubExitWithPanic(t)
			tt.stub(t)
			withArgs(t, "engram", "context", "engram", "--brief", "--task", "deterministic briefing", "--json")
			stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdContext(cfg) })
			if stdout != "" || recovered == nil {
				t.Fatalf("stdout = %q, exit = %v", stdout, recovered)
			}
			if failure := decodeCLIJSON(t, stderr); failure["code"] != tt.wantCode {
				t.Fatalf("failure = %#v, want code %q", failure, tt.wantCode)
			}
		})
	}
}

func TestCmdContextBriefRejectsInvalidFlags(t *testing.T) {
	cfg := testConfig(t)
	tests := [][]string{
		{"engram", "context", "--task", "briefing", "--json"},
		{"engram", "context", "--base", "main", "--json"},
		{"engram", "context", "--brief", "--base", "--json"},
		{"engram", "context", "--brief", "--task", "--json"},
		{"engram", "context", "--brief", "--limit", "0", "--json"},
		{"engram", "context", "--brief", "--limit", "6", "--json"},
		{"engram", "context", "--brief", "--limit", "many", "--json"},
		{"engram", "context", "--brief", "--scope", "--json"},
		{"engram", "context", "--brief", "--scope", "global", "--json"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args[2:], "_"), func(t *testing.T) {
			stubExitWithPanic(t)
			withArgs(t, args...)
			stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdContext(cfg) })
			if stdout != "" || recovered == nil {
				t.Fatalf("stdout = %q, exit = %v", stdout, recovered)
			}
			if decodeCLIJSON(t, stderr)["code"] != "invalid_arguments" {
				t.Fatalf("stderr = %q", stderr)
			}
		})
	}
}

func newBriefingCLIRepository(t *testing.T, remoteProject string) string {
	t.Helper()
	repo := t.TempDir()
	runBriefingGit(t, repo, "init", "-b", "main")
	runBriefingGit(t, repo, "config", "user.name", "Engram Test")
	runBriefingGit(t, repo, "config", "user.email", "engram@example.test")
	writeBriefingCLIFile(t, filepath.Join(repo, "README.md"), "base\n")
	runBriefingGit(t, repo, "add", "README.md")
	runBriefingGit(t, repo, "commit", "-m", "Initial repository")
	runBriefingGit(t, repo, "remote", "add", "origin", "https://example.test/acme/"+remoteProject+".git")
	runBriefingGit(t, repo, "switch", "-c", "feat/clean-branch-evidence")
	writeBriefingCLIFile(t, filepath.Join(repo, "internal", "repository", "branch_evidence.go"), "package repository\n\n// Clean branch evidence guides selection.\n")
	runBriefingGit(t, repo, "add", "internal/repository/branch_evidence.go")
	runBriefingGit(t, repo, "commit", "-m", "Add clean branch evidence")
	return repo
}

func writeBriefingCLIFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func runBriefingGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}

func disableBriefingRepositoryInspection(t *testing.T) {
	t.Helper()
	original := taskBriefingWorkingDirectory
	taskBriefingWorkingDirectory = func() string { return "" }
	t.Cleanup(func() { taskBriefingWorkingDirectory = original })
}

func jsonDiagnosticsContain(diagnostics []any, code string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.(map[string]any)["code"] == code {
			return true
		}
	}
	return false
}

func jsonEvidenceContains(evidence []any, signal string) bool {
	for _, item := range evidence {
		if item.(map[string]any)["signal"] == signal {
			return true
		}
	}
	return false
}

func TestCmdContextBriefWarnsAboutSelectedConflict(t *testing.T) {
	cfg := testConfig(t)
	leftID := mustSeedObservation(t, cfg, "brief-conflict-left", "engram", "decision", "Task briefing cache", "Use a task briefing cache for deterministic selection.", "project")
	rightID := mustSeedObservation(t, cfg, "brief-conflict-right", "engram", "decision", "Task briefing without cache", "Avoid a task briefing cache during deterministic selection.", "project")
	memoryStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	left, err := memoryStore.GetObservation(leftID)
	if err != nil {
		t.Fatalf("GetObservation(left): %v", err)
	}
	right, err := memoryStore.GetObservation(rightID)
	if err != nil {
		t.Fatalf("GetObservation(right): %v", err)
	}
	if _, err := memoryStore.SaveRelation(store.SaveRelationParams{SyncID: "brief-conflict", SourceID: left.SyncID, TargetID: right.SyncID}); err != nil {
		t.Fatalf("SaveRelation: %v", err)
	}
	if _, err := memoryStore.JudgeRelation(store.JudgeRelationParams{
		JudgmentID: "brief-conflict", Relation: store.RelationConflictsWith,
		MarkedByActor: "test", MarkedByKind: "system",
	}); err != nil {
		t.Fatalf("JudgeRelation: %v", err)
	}
	if err := memoryStore.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	withArgs(t, "engram", "context", "engram", "--brief", "--task", "task briefing cache deterministic selection")
	stdout, stderr := captureOutput(t, func() { cmdContext(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	if !strings.Contains(stdout, "Selected memories have a judged conflict") {
		t.Fatalf("human output lacks actionable conflict warning: %q", stdout)
	}
}
