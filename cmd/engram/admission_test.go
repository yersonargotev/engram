package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCmdAdmissionPreviewJSONUsesRealStoreWithoutPersisting(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "admission-cli-existing", "engram", "decision", "SQLite authority", "Local SQLite remains the source of truth.", "project")
	inputPath := writeAdmissionInput(t, memoryops.EvidenceBundle{
		Version: memoryops.EvidenceBundleVersion,
		Items: []memoryops.EvidenceItem{
			{Reference: "prompt-1", Source: memoryops.EvidenceSourceUserPrompt, Content: "Remember this: Explicit saves remain authoritative."},
			{Reference: "summary-1", Source: memoryops.EvidenceSourceSessionSummary, Content: "## Key Learnings\n- Local SQLite remains the source of truth."},
		},
	})
	before := admissionStoreStats(t, cfg)

	withArgs(t, "engram", "admission", "preview", "--project", "ENGRAM", "--input", inputPath, "--json")
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	result := decodeCLIJSON(t, stdout)
	if _, exists := result["acquisition"]; exists {
		t.Fatalf("input-mode result unexpectedly contains acquisition: %#v", result)
	}
	if result["mode"] != "shadow_preview" || result["project"] != "engram" {
		t.Fatalf("result = %#v", result)
	}
	proposals := result["proposals"].([]any)
	if len(proposals) != 2 {
		t.Fatalf("proposals = %#v", proposals)
	}
	firstAssessment := proposals[0].(map[string]any)["assessment"].(map[string]any)
	secondAssessment := proposals[1].(map[string]any)["assessment"].(map[string]any)
	if firstAssessment["recommendation"] != "admit" || secondAssessment["recommendation"] != "reject" {
		t.Fatalf("assessments = %#v %#v", firstAssessment, secondAssessment)
	}

	after := admissionStoreStats(t, cfg)
	if before.TotalObservations != after.TotalObservations || before.TotalSessions != after.TotalSessions || before.TotalPrompts != after.TotalPrompts {
		t.Fatalf("preview mutated memory state: before=%#v after=%#v", before, after)
	}
}

func TestCmdAdmissionPreviewSessionJSONUsesPersistedEvidenceWithoutPersisting(t *testing.T) {
	cfg := testConfig(t)
	mustSeedAdmissionSession(t, cfg, "session-cli-v2", "engram", []string{
		"Remember this: CLI session previews remain read-only.",
	}, "## Decisions\n- Session acquisition belongs to memoryops.")
	before := admissionStoreStats(t, cfg)

	withArgs(t, "engram", "admission", "preview", "--project", "engram", "--session", "session-cli-v2", "--json")
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	result := decodeCLIJSON(t, stdout)
	acquisition, ok := result["acquisition"].(map[string]any)
	if !ok || acquisition["mode"] != "session" || acquisition["session_id"] != "session-cli-v2" {
		t.Fatalf("acquisition = %#v", result["acquisition"])
	}
	if len(result["proposals"].([]any)) != 2 {
		t.Fatalf("proposals = %#v", result["proposals"])
	}
	after := admissionStoreStats(t, cfg)
	if before.TotalObservations != after.TotalObservations || before.TotalSessions != after.TotalSessions || before.TotalPrompts != after.TotalPrompts {
		t.Fatalf("preview mutated memory state: before=%#v after=%#v", before, after)
	}
}

func TestCmdAdmissionPreviewSessionHumanReportsCoverage(t *testing.T) {
	cfg := testConfig(t)
	mustSeedAdmissionSession(t, cfg, "session-cli-human", "engram", []string{
		"Remember this: Human output reports session coverage.",
	}, "")

	withArgs(t, "engram", "admission", "preview", "--project", "engram", "--session", "session-cli-human")
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	for _, wanted := range []string{"Evidence: session session-cli-human (v1)", "Coverage:", "user_prompt: 1/1 included", "session_summary: 0/0 included", "session_summary_unavailable"} {
		if !strings.Contains(stdout, wanted) {
			t.Fatalf("stdout missing %q: %q", wanted, stdout)
		}
	}
}

func TestCmdAdmissionPreviewSourceFlagsAreExclusive(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	input := writeAdmissionInput(t, memoryops.EvidenceBundle{Version: memoryops.EvidenceBundleVersion})
	tests := [][]string{
		{"engram", "admission", "preview", "--project", "engram", "--json"},
		{"engram", "admission", "preview", "--project", "engram", "--input", input, "--session", "session-1", "--json"},
		{"engram", "admission", "preview", "--project", "engram", "--input", input, "--input", input, "--json"},
		{"engram", "admission", "preview", "--project", "engram", "--session", "session-1", "--session", "session-2", "--json"},
	}
	for _, args := range tests {
		withArgs(t, args...)
		stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdAdmission(cfg) })
		if stdout != "" {
			t.Fatalf("stdout = %q", stdout)
		}
		if _, ok := recovered.(exitCode); !ok {
			t.Fatalf("exit = %#v", recovered)
		}
		if envelope := decodeCLIJSON(t, stderr); envelope["code"] != "invalid_arguments" {
			t.Fatalf("error = %#v", envelope)
		}
	}
}

func TestCmdAdmissionPreviewSessionResolutionErrorsAreJSON(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "seed-engram", "engram", "decision", "Seed", "Seed.", "project")
	mustSeedAdmissionSession(t, cfg, "session-other", "other", nil, "")
	tests := []struct {
		name      string
		sessionID string
		code      string
	}{
		{name: "unknown", sessionID: "missing", code: "unknown_session"},
		{name: "project mismatch", sessionID: "session-other", code: "session_project_mismatch"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withArgs(t, "engram", "admission", "preview", "--project", "engram", "--session", tc.sessionID, "--json")
			stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdAdmission(cfg) })
			if stdout != "" {
				t.Fatalf("stdout = %q", stdout)
			}
			if _, ok := recovered.(exitCode); !ok {
				t.Fatalf("exit = %#v", recovered)
			}
			envelope := decodeCLIJSON(t, stderr)
			if envelope["code"] != tc.code {
				t.Fatalf("error = %#v, want code %q", envelope, tc.code)
			}
		})
	}
}

func TestCmdAdmissionPreviewHumanOutput(t *testing.T) {
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "admission-cli-human", "engram", "decision", "Seed", "Seed content", "project")
	inputPath := writeAdmissionInput(t, memoryops.EvidenceBundle{
		Version: memoryops.EvidenceBundleVersion,
		Items:   []memoryops.EvidenceItem{{Reference: "prompt-1", Source: memoryops.EvidenceSourceUserPrompt, Content: "Recuerda esto: Los guardados explícitos siguen siendo autoritativos."}},
	})

	withArgs(t, "engram", "admission", "preview", "--project", "engram", "--input", inputPath)
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	for _, wanted := range []string{"Admission preview", "no memories were written", "[admit]", "Los guardados explícitos"} {
		if !strings.Contains(stdout, wanted) {
			t.Fatalf("stdout missing %q: %q", wanted, stdout)
		}
	}
}

func TestCmdAdmissionPreviewStrictErrorsAreJSON(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	validInput := writeAdmissionInput(t, memoryops.EvidenceBundle{Version: memoryops.EvidenceBundleVersion})
	unknownField := filepath.Join(t.TempDir(), "unknown.json")
	if err := os.WriteFile(unknownField, []byte(`{"version":"v1","items":[],"transcript":"forbidden"}`), 0o600); err != nil {
		t.Fatalf("write unknown-field input: %v", err)
	}

	tests := []struct {
		name string
		args []string
		code string
	}{
		{name: "missing subcommand", args: []string{"engram", "admission", "--json"}, code: "invalid_arguments"},
		{name: "unknown subcommand", args: []string{"engram", "admission", "classify", "--json"}, code: "invalid_arguments"},
		{name: "missing project", args: []string{"engram", "admission", "preview", "--input", validInput, "--json"}, code: "invalid_arguments"},
		{name: "missing input", args: []string{"engram", "admission", "preview", "--project", "engram", "--json"}, code: "invalid_arguments"},
		{name: "missing flag value", args: []string{"engram", "admission", "preview", "--project", "--json"}, code: "invalid_arguments"},
		{name: "unknown flag", args: []string{"engram", "admission", "preview", "--project", "engram", "--input", validInput, "--typo", "--json"}, code: "unknown_flag"},
		{name: "unknown JSON field", args: []string{"engram", "admission", "preview", "--project", "engram", "--input", unknownField, "--json"}, code: "invalid_evidence_bundle"},
		{name: "unknown project", args: []string{"engram", "admission", "preview", "--project", "missing", "--input", validInput, "--json"}, code: "unknown_project"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withArgs(t, tc.args...)
			stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdAdmission(cfg) })
			if stdout != "" {
				t.Fatalf("stdout = %q", stdout)
			}
			if _, ok := recovered.(exitCode); !ok {
				t.Fatalf("exit = %#v", recovered)
			}
			envelope := decodeCLIJSON(t, stderr)
			if envelope["code"] != tc.code {
				t.Fatalf("error = %#v, want code %q", envelope, tc.code)
			}
		})
	}
}

func writeAdmissionInput(t *testing.T, bundle memoryops.EvidenceBundle) string {
	t.Helper()
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal admission input: %v", err)
	}
	path := filepath.Join(t.TempDir(), "evidence.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write admission input: %v", err)
	}
	return path
}

func admissionStoreStats(t *testing.T, cfg store.Config) *store.Stats {
	t.Helper()
	memoryStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer memoryStore.Close()
	stats, err := memoryStore.Stats()
	if err != nil {
		t.Fatalf("store stats: %v", err)
	}
	return stats
}

func mustSeedAdmissionSession(t *testing.T, cfg store.Config, sessionID, project string, prompts []string, summary string) {
	t.Helper()
	memoryStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer memoryStore.Close()
	if err := memoryStore.CreateSession(sessionID, project, "/work/"+project); err != nil {
		t.Fatalf("create session: %v", err)
	}
	for _, content := range prompts {
		if _, err := memoryStore.AddPrompt(store.AddPromptParams{SessionID: sessionID, Project: project, Content: content}); err != nil {
			t.Fatalf("add prompt: %v", err)
		}
	}
	if summary != "" {
		if err := memoryStore.EndSession(sessionID, summary); err != nil {
			t.Fatalf("end session: %v", err)
		}
	}
}
