package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func decodeCLIJSON(t *testing.T, output string) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("invalid CLI JSON: %v\n%s", err, output)
	}
	return result
}

func TestCmdCurrentProjectExposesWeakWriteAuthority(t *testing.T) {
	withCwd(t, t.TempDir())
	withArgs(t, "engram", "current-project", "--json")

	stdout, stderr := captureOutput(t, cmdCurrentProject)
	if stderr != "" {
		t.Fatalf("current-project stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	if payload["project_source"] != "dir_basename" || payload["project_strength"] != "weak" || payload["implicit_write_allowed"] != false || payload["safe_next_action"] != "provide an explicit project name and retry the write" {
		t.Fatalf("current-project identity policy = %v", payload)
	}
}

func TestCmdSearchStoreOpenFailureFailsOpen(t *testing.T) {
	originalStoreNew := storeNew
	storeNew = func(store.Config) (*store.Store, error) {
		return nil, errors.New("forced open failure")
	}
	t.Cleanup(func() { storeNew = originalStoreNew })

	withArgs(t, "engram", "search", "prior release", "--project", "engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSearch(store.Config{}) })
	if stderr != "" {
		t.Fatalf("search stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	warning, _ := payload["warning"].(map[string]any)
	diagnostics, _ := payload["diagnostics"].([]any)
	if warning["code"] != "recall_unavailable" || payload["result_count"] != float64(0) || len(diagnostics) != 1 {
		t.Fatalf("expected visible fail-open Recall, got %v", payload)
	}
}

func TestCLIMemoryJSONWorkflow(t *testing.T) {
	cfg := testConfig(t)

	withArgs(t, "engram", "save", "CLI contract", "durable searchable content", "--project", "CLI Project", "--type", "decision", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSave(cfg) })
	if stderr != "" {
		t.Fatalf("save stderr=%q", stderr)
	}
	saved := decodeCLIJSON(t, stdout)
	obs := saved["observation"].(map[string]any)
	id := int64(obs["id"].(float64))
	if saved["project"] != "cli project" {
		t.Fatalf("project=%v", saved["project"])
	}

	withArgs(t, "engram", "get", fmt.Sprint(id), "--json")
	stdout, stderr = captureOutput(t, func() { cmdGet(cfg) })
	legacy := decodeCLIJSON(t, stdout)
	if stderr != "" || legacy["observation"].(map[string]any)["id"] != float64(id) {
		t.Fatalf("legacy get stdout=%q stderr=%q", stdout, stderr)
	}
	if _, ok := legacy["relations"]; !ok {
		t.Fatalf("legacy get lost relations metadata: %v", legacy)
	}

	withArgs(t, "engram", "search", "durable searchable", "--project", "cli project", "--match-mode", "all", "--json")
	stdout, stderr = captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" {
		t.Fatalf("search stderr=%q", stderr)
	}
	search := decodeCLIJSON(t, stdout)
	if len(search["results"].([]any)) != 1 {
		t.Fatalf("results=%v", search["results"])
	}
	recallID := search["recall_id"].(string)
	resultID := search["opaque_result_ids"].([]any)[0].(string)

	withArgs(t, "engram", "get", "--recall-id", recallID, "--result-id", resultID, "--project", "cli project", "--json")
	stdout, stderr = captureOutput(t, func() { cmdGet(cfg) })
	if stderr != "" || !strings.Contains(stdout, "durable searchable content") {
		t.Fatalf("get stdout=%q stderr=%q", stdout, stderr)
	}

	withArgs(t, "engram", "update", fmt.Sprint(id), "--title", "Updated CLI contract", "--topic-key", "decision/cli-contract", "--json")
	stdout, stderr = captureOutput(t, func() { cmdUpdate(cfg) })
	if stderr != "" || decodeCLIJSON(t, stdout)["observation"].(map[string]any)["title"] != "Updated CLI contract" {
		t.Fatalf("update stdout=%q stderr=%q", stdout, stderr)
	}

	withArgs(t, "engram", "pin", fmt.Sprint(id), "--json")
	stdout, stderr = captureOutput(t, func() { cmdPin(cfg, true) })
	if stderr != "" || decodeCLIJSON(t, stdout)["pinned"] != true {
		t.Fatalf("pin stdout=%q stderr=%q", stdout, stderr)
	}

	withArgs(t, "engram", "review", "mark", fmt.Sprint(id), "--json")
	stdout, stderr = captureOutput(t, func() { cmdReview(cfg) })
	if stderr != "" || decodeCLIJSON(t, stdout)["local_only"] != true {
		t.Fatalf("review stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestCmdSearchReturnsBoundedRecallEnvelope(t *testing.T) {
	cfg := testConfig(t)
	oldVersion, oldCommit := version, commit
	version, commit = "9.9.9-test", "revision-test"
	t.Cleanup(func() { version, commit = oldVersion, oldCommit })
	mustSeedObservation(t, cfg, "recall-cli", "engram", "decision", "Bounded Recall CLI", "candidate summary content", "project")

	withArgs(t, "engram", "search", "Bounded Recall", "--project", "engram", "--json")
	stdout, stderr := captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" {
		t.Fatalf("search stderr=%q", stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	if recallID, _ := payload["recall_id"].(string); !strings.HasPrefix(recallID, "recall-") {
		t.Fatalf("recall_id=%v", payload["recall_id"])
	}
	if payload["result_count"] != float64(1) || len(payload["result_ids"].([]any)) != 1 || len(payload["opaque_result_ids"].([]any)) != 1 || len(payload["results"].([]any)) != 1 {
		t.Fatalf("result metadata=%v", payload)
	}
	if delivered, _ := payload["delivered_utf8_bytes"].(float64); delivered <= 0 || delivered > 4096 {
		t.Fatalf("delivered_utf8_bytes=%v", payload["delivered_utf8_bytes"])
	}
	if _, ok := payload["elapsed_monotonic_ms"].(float64); !ok {
		t.Fatalf("elapsed_monotonic_ms=%v", payload["elapsed_monotonic_ms"])
	}
	provenance := payload["provenance"].(map[string]any)
	if provenance["protocol_version"] != float64(1) || provenance["binary_version"] != "9.9.9-test" || provenance["binary_revision"] != "revision-test" {
		t.Fatalf("provenance=%v", provenance)
	}
	candidate := payload["results"].([]any)[0].(map[string]any)
	if candidate["summary"] != "candidate summary content" {
		t.Fatalf("candidate=%v", candidate)
	}
	if _, leaksFullObservation := candidate["observation"]; leaksFullObservation {
		t.Fatalf("candidate leaked full observation: %v", candidate)
	}
}

func TestCLISaveAcceptsNamedInputsJSON(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	withArgs(t,
		"engram", "save",
		"--title", "Named CLI contract",
		"--content", "named durable content",
		"--project", "Named Project",
		"--type", "decision",
		"--scope", "personal",
		"--topic-key", "decision/named-save",
		"--json",
	)

	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdSave(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("save panic=%v stderr=%q", recovered, stderr)
	}
	payload := decodeCLIJSON(t, stdout)
	observation := payload["observation"].(map[string]any)
	if observation["title"] != "Named CLI contract" || observation["content"] != "named durable content" {
		t.Fatalf("observation=%v", observation)
	}
	if observation["type"] != "decision" || observation["scope"] != "personal" || observation["topic_key"] != "decision/named-save" {
		t.Fatalf("observation metadata=%v", observation)
	}
	if payload["project"] != "named project" {
		t.Fatalf("project=%v", payload["project"])
	}
}

func TestCLIReadCommandsJSON(t *testing.T) {
	cfg := testConfig(t)
	id := mustSeedObservation(t, cfg, "json-session", "json-project", "decision", "JSON read", "full content", "project")

	tests := []struct {
		name string
		args []string
		run  func()
	}{
		{"timeline", []string{"engram", "timeline", fmt.Sprint(id), "--json"}, func() { cmdTimeline(cfg) }},
		{"context", []string{"engram", "context", "json-project", "--json"}, func() { cmdContext(cfg) }},
		{"stats", []string{"engram", "stats", "--json"}, func() { cmdStats(cfg) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withArgs(t, tt.args...)
			stdout, stderr := captureOutput(t, tt.run)
			if stderr != "" {
				t.Fatalf("stderr=%q", stderr)
			}
			decodeCLIJSON(t, stdout)
		})
	}
}

func TestCLIProjectsMergeDryRunAndApplyJSON(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "merge-a", "old-project", "manual", "old", "content", "project")
	s, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	proposalIdentity := store.CheckpointIdentity{Host: "codex", SessionID: "merge-proposal-session", RootTurnID: "merge-proposal-turn"}
	if _, _, err := s.RecordNeedsReviewCheckpoint(store.RecordNeedsReviewCheckpointParams{
		Identity: proposalIdentity,
		Project:  "old-project",
		Proposal: &store.MemoryProposalInput{Title: "merge proposal", Content: "merge proposal content"},
	}); err != nil {
		t.Fatalf("seed Memory proposal checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}
	withArgs(t, "engram", "projects", "merge", "--from", "old-project", "--to", "canonical", "--dry-run", "--json")
	stdout, stderr := captureOutput(t, func() { cmdProjects(cfg) })
	if stderr != "" {
		t.Fatalf("dry stderr=%q", stderr)
	}
	dry := decodeCLIJSON(t, stdout)
	if dry["dry_run"] != true || dry["memory_proposals_updated"] != float64(1) {
		t.Fatalf("dry=%v", dry)
	}
	if _, exists := dry["admission_shadow_runs_updated"]; exists {
		t.Fatalf("dry result retained Admission Shadow accounting: %v", dry)
	}
	withArgs(t, "engram", "projects", "merge", "--from", "old-project", "--to", "canonical", "--yes", "--json")
	stdout, stderr = captureOutput(t, func() { cmdProjects(cfg) })
	if stderr != "" {
		t.Fatalf("apply stderr=%q", stderr)
	}
	applied := decodeCLIJSON(t, stdout)
	if applied["dry_run"] != false || applied["memory_proposals_updated"] != float64(1) {
		t.Fatalf("applied=%v", applied)
	}
	if _, exists := applied["admission_shadow_runs_updated"]; exists {
		t.Fatalf("applied result retained Admission Shadow accounting: %v", applied)
	}
	s, err = store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	results, err := s.Search("old", store.SearchOptions{Project: "canonical", Limit: 10})
	if err != nil || len(results) != 1 {
		t.Fatalf("merge results=%v err=%v", results, err)
	}
	movedCheckpoint, err := s.GetMemoryCheckpoint(proposalIdentity)
	if err != nil || movedCheckpoint.Proposal == nil || movedCheckpoint.Proposal.Project != "canonical" {
		t.Fatalf("canonical Memory proposal checkpoint=%#v err=%v", movedCheckpoint, err)
	}
}

func TestCLIProjectsMergePreviewMatchesNormalizedAlias(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "alias-session", "old-project", "manual", "alias", "content", "project")
	withArgs(t, "engram", "projects", "merge", "--from", "old project", "--to", "canonical", "--dry-run", "--json")
	stdout, stderr := captureOutput(t, func() { cmdProjects(cfg) })
	if stderr != "" {
		t.Fatalf("stderr=%q", stderr)
	}
	preview := decodeCLIJSON(t, stdout)
	if preview["observations_updated"].(float64) != 1 || preview["sessions_updated"].(float64) != 1 {
		t.Fatalf("preview=%v", preview)
	}
}

func TestCLIStrictParsingReturnsJSONBeforeMutation(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)
	cfg := testConfig(t)
	id := mustSeedObservation(t, cfg, "strict-session", "strict-project", "manual", "strict", "content", "project")
	tests := []struct {
		name string
		args []string
		run  func()
	}{
		{"get unknown", []string{"engram", "get", fmt.Sprint(id), "--typo", "--json"}, func() { cmdGet(cfg) }},
		{"delete unknown", []string{"engram", "delete", fmt.Sprint(id), "--typo", "--json"}, func() { cmdDelete(cfg) }},
		{"timeline invalid", []string{"engram", "timeline", "not-an-id", "--json"}, func() { cmdTimeline(cfg) }},
		{"search flag value", []string{"engram", "search", "strict", "--project", "--json"}, func() { cmdSearch(cfg) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withArgs(t, tt.args...)
			stdout, stderr, recovered := captureOutputAndRecover(t, tt.run)
			if stdout != "" {
				t.Fatalf("stdout=%q", stdout)
			}
			if _, ok := recovered.(exitCode); !ok {
				t.Fatalf("exit=%v", recovered)
			}
			decodeCLIJSON(t, stderr)
		})
	}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.GetObservation(id); err != nil {
		t.Fatalf("invalid delete mutated observation: %v", err)
	}
}

func TestCLISaveRejectsWhitespaceContentJSON(t *testing.T) {
	stubRuntimeHooks(t)
	stubExitWithPanic(t)
	cfg := testConfig(t)
	withArgs(t, "engram", "save", "empty", "   ", "--project", "p", "--json")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdSave(cfg) })
	if stdout != "" {
		t.Fatalf("stdout=%q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("exit=%v", recovered)
	}
	decodeCLIJSON(t, stderr)
}
