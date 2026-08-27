package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCmdAdmissionStudyFreezeRunReviewMetricsAndCleanupJSON(t *testing.T) {
	cfg := testConfig(t)
	contract := cliAdmissionStudyContract()
	contractPath := writeAdmissionStudyContract(t, contract)
	for _, cohort := range []string{"calibration", "held-out"} {
		mustSeedAdmissionSession(t, cfg, "study-cli-"+cohort, "engram", []string{
			"Remember this: CLI study runs remain attributable.",
		}, "## Decisions\n- Aggregate reports never expose row material.")
	}

	withArgs(t, "engram", "admission", "study", "freeze", "--input", contractPath, "--json")
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("freeze stderr = %q", stderr)
	}
	frozen := decodeCLIJSON(t, stdout)
	if frozen["already_frozen"] != false || frozen["study"].(map[string]any)["contract_hash"] == "" {
		t.Fatalf("frozen study = %#v", frozen)
	}

	var runIDs []string
	var proposalIDs []string
	for _, cohort := range []string{"calibration", "held-out"} {
		args := admissionStudyShadowArgs("study-cli-"+cohort, cohort)
		withArgs(t, args...)
		stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
		if stderr != "" {
			t.Fatalf("%s shadow stderr = %q", cohort, stderr)
		}
		shadow := decodeCLIJSON(t, stdout)
		run := shadow["run"].(map[string]any)
		expectedAdapter := "codex"
		if cohort == "held-out" {
			expectedAdapter = "claude-code"
		}
		if run["study_id"] != contract.StudyID || run["cohort"] != cohort || run["adapter"] != expectedAdapter ||
			run["consent_attestation"] != "consent-v1" || shadow["already_recorded"] != false {
			t.Fatalf("%s attributed shadow = %#v", cohort, shadow)
		}
		runIDs = append(runIDs, run["id"].(string))
		for _, proposal := range shadow["proposals"].([]any) {
			proposalIDs = append(proposalIDs, proposal.(map[string]any)["id"].(string))
		}
		withArgs(t, args...)
		stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
		if stderr != "" || decodeCLIJSON(t, stdout)["already_recorded"] != true {
			t.Fatalf("%s idempotent shadow stdout=%q stderr=%q", cohort, stdout, stderr)
		}
	}

	withArgs(t, "engram", "admission", "review", "list", "--study", contract.StudyID,
		"--study-version", contract.StudyVersion, "--reviewer", "reviewer-a", "--json")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("study review list stderr = %q", stderr)
	}
	queue := decodeCLIJSON(t, stdout)
	if len(queue["proposals"].([]any)) != 4 || queue["reviewer_id"] != "reviewer-a" {
		t.Fatalf("study review queue = %#v", queue)
	}

	for _, proposalID := range proposalIDs {
		for _, reviewerID := range []string{"reviewer-a", "reviewer-b"} {
			withArgs(t, "engram", "admission", "review", "mark", proposalID,
				"--reviewer", reviewerID, "--verdict", "admit", "--json")
			stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
			if stderr != "" || decodeCLIJSON(t, stdout)["review"].(map[string]any)["reviewer_id"] != reviewerID {
				t.Fatalf("review %s/%s stdout=%q stderr=%q", proposalID, reviewerID, stdout, stderr)
			}
		}
	}
	for _, runID := range runIDs {
		withArgs(t, "engram", "admission", "omission", "record", runID,
			"--reviewer", "reviewer-a", "--category", "decision",
			"--reason-code", "not_proposed", "--annotation", "Missed durable decision.", "--json")
		stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
		if stderr != "" || decodeCLIJSON(t, stdout)["already_recorded"] != false {
			t.Fatalf("omission stdout=%q stderr=%q", stdout, stderr)
		}
	}

	withArgs(t, "engram", "admission", "metrics", "--study", contract.StudyID,
		"--study-version", contract.StudyVersion, "--json")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("study metrics stderr = %q", stderr)
	}
	metrics := decodeCLIJSON(t, stdout)
	counts := metrics["counts"].(map[string]any)
	if counts["run_count"] != float64(2) || counts["proposal_count"] != float64(4) ||
		counts["review_event_count"] != float64(8) || counts["omission_count"] != float64(2) ||
		metrics["automatic_admission_enabled"] != false || metrics["gates"].(map[string]any)["go"] != true {
		t.Fatalf("study metrics = %#v", metrics)
	}
	for _, forbidden := range []string{"reviewer-a", "Missed durable decision.", "proposal_id", "evidence_refs", "run_id"} {
		if strings.Contains(stdout, forbidden) {
			t.Fatalf("aggregate stdout leaked %q: %s", forbidden, stdout)
		}
	}

	withArgs(t, "engram", "admission", "study", "cleanup", "--study", contract.StudyID,
		"--study-version", contract.StudyVersion, "--yes", "--json")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("cleanup stderr = %q", stderr)
	}
	cleanup := decodeCLIJSON(t, stdout)
	if cleanup["run_count"] != float64(2) || cleanup["proposal_count"] != float64(4) || cleanup["omission_count"] != float64(2) {
		t.Fatalf("cleanup = %#v", cleanup)
	}
}

func TestCmdAdmissionStudyStrictErrorsAreJSON(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	tests := []struct {
		name string
		args []string
		code string
	}{
		{name: "freeze missing input", args: []string{"engram", "admission", "study", "freeze", "--json"}, code: "invalid_arguments"},
		{name: "shadow partial study metadata", args: []string{"engram", "admission", "shadow", "--project", "engram", "--session", "s", "--study", "x", "--json"}, code: "invalid_arguments"},
		{name: "study review missing reviewer", args: []string{"engram", "admission", "review", "list", "--study", "x", "--study-version", "v1", "--json"}, code: "invalid_arguments"},
		{name: "omission missing labels", args: []string{"engram", "admission", "omission", "record", "run", "--reviewer", "r", "--json"}, code: "invalid_arguments"},
		{name: "metrics conflicting selectors", args: []string{"engram", "admission", "metrics", "--project", "engram", "--study", "x", "--study-version", "v1", "--json"}, code: "invalid_arguments"},
		{name: "cleanup requires confirmation", args: []string{"engram", "admission", "study", "cleanup", "--study", "x", "--study-version", "v1", "--json"}, code: "confirmation_required"},
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
			if envelope := decodeCLIJSON(t, stderr); envelope["code"] != tc.code {
				t.Fatalf("error = %#v, want %q", envelope, tc.code)
			}
		})
	}
}

func admissionStudyShadowArgs(sessionID, cohort string) []string {
	adapter, projectType, sessionShape := "codex", "cli", "feature"
	if cohort == "held-out" {
		adapter, projectType, sessionShape = "claude-code", "library", "bugfix"
	}
	return []string{
		"engram", "admission", "shadow", "--project", "engram", "--session", sessionID,
		"--study", "real-session-v1", "--study-version", "v1", "--cohort", cohort,
		"--adapter", adapter, "--project-type", projectType, "--session-shape", sessionShape,
		"--consent-attestation", "consent-v1", "--independent-review-required", "--json",
	}
}

func writeAdmissionStudyContract(t *testing.T, contract store.AdmissionStudyContract) string {
	t.Helper()
	encoded, err := json.Marshal(contract)
	if err != nil {
		t.Fatalf("marshal admission study contract: %v", err)
	}
	path := filepath.Join(t.TempDir(), "study.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write admission study contract: %v", err)
	}
	return path
}

func cliAdmissionStudyContract() store.AdmissionStudyContract {
	return store.AdmissionStudyContract{
		ContractVersion: store.AdmissionStudyContractVersion,
		StudyID:         "real-session-v1", StudyVersion: "v1", MetricsVersion: "admission-study-metrics-v1",
		Cohorts: []store.AdmissionStudyCohort{
			{ID: "calibration", Kind: "calibration", SessionIDs: []string{"study-cli-calibration"}, MinimumRuns: 1, MinimumProposals: 2, MinimumIndependentReviewedProposals: 1},
			{ID: "held-out", Kind: "held_out", SessionIDs: []string{"study-cli-held-out"}, MinimumRuns: 1, MinimumProposals: 2, MinimumIndependentReviewedProposals: 1},
		},
		Adapters: []string{"codex", "claude-code"}, ProjectTypes: []string{"cli", "library"}, SessionShapes: []string{"feature", "bugfix"},
		LabelSchema: store.AdmissionStudyLabelSchema{
			Version: "admission-study-labels-v1", Verdicts: []string{"admit", "review", "reject"},
			OmissionCategories: []string{"decision", "root_cause", "invariant", "constraint", "preference"},
			ReasonCodes:        []string{"not_proposed", "wrong_category", "insufficient_evidence"},
		},
		Thresholds: store.AdmissionStudyThresholds{
			MinimumPromotionPrecision: 0.9, MaximumReviewRate: 0.5, MinimumReviewCoverage: 1, MinimumInterReviewerAgreement: 0.8,
			MaximumProtectedFalseRejects: 0, MaximumUnsupportedProposals: 0, MaximumPrivacyLeaks: 0,
		},
		Consent:                 store.AdmissionStudyConsent{Required: true, Attestation: "consent-v1"},
		Retention:               store.AdmissionStudyRetention{Days: 30, Cleanup: store.AdmissionStudyCleanupExplicit},
		AllowedAggregateOutputs: []string{"counts", "distributions", "quality", "uncertainty", "sufficiency", "gates"},
	}
}
