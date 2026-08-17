package main

import (
	"errors"
	"strings"
	"testing"

	projectpkg "github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
	"github.com/yersonargotev/engram/internal/taskbriefing"
)

func TestCmdContextBriefReturnsStructuredTaskSelection(t *testing.T) {
	cfg := testConfig(t)
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
