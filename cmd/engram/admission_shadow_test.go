package main

import (
	"strings"
	"testing"
)

func TestCmdAdmissionShadowReviewAndMetricsJSON(t *testing.T) {
	cfg := testConfig(t)
	mustSeedAdmissionSession(t, cfg, "shadow-cli", "engram", []string{
		"Remember this: CLI shadow runs remain explicit.",
	}, "## Decisions\n- Human corrections remain append-only.")
	before := admissionStoreStats(t, cfg)

	withArgs(t, "engram", "admission", "shadow", "--project", "ENGRAM", "--session", "shadow-cli", "--json")
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("shadow stderr = %q", stderr)
	}
	shadow := decodeCLIJSON(t, stdout)
	run := shadow["run"].(map[string]any)
	if run["project"] != "engram" || run["session_id"] != "shadow-cli" {
		t.Fatalf("shadow run = %#v", run)
	}
	proposals := shadow["proposals"].([]any)
	if len(proposals) != 2 {
		t.Fatalf("shadow proposals = %#v", proposals)
	}
	after := admissionStoreStats(t, cfg)
	if before.TotalObservations != after.TotalObservations || before.TotalSessions != after.TotalSessions || before.TotalPrompts != after.TotalPrompts {
		t.Fatalf("shadow changed Memory state: before=%#v after=%#v", before, after)
	}

	withArgs(t, "engram", "admission", "review", "list", "--project", "engram", "--json")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("list stderr = %q", stderr)
	}
	listed := decodeCLIJSON(t, stdout)["proposals"].([]any)
	if len(listed) != 2 {
		t.Fatalf("review list = %#v", listed)
	}
	listedResult := decodeCLIJSON(t, stdout)
	runs := listedResult["runs"].([]any)
	if len(runs) != 1 || runs[0].(map[string]any)["session_id"] != "shadow-cli" {
		t.Fatalf("review run metadata = %#v", runs)
	}
	firstID := listed[0].(map[string]any)["id"].(string)
	secondID := listed[1].(map[string]any)["id"].(string)

	markArgs := []string{"engram", "admission", "review", "mark", firstID, "--verdict", "admit", "--note", "Confirmed.", "--json"}
	withArgs(t, markArgs...)
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" || decodeCLIJSON(t, stdout)["already_recorded"] != false {
		t.Fatalf("first mark stdout=%q stderr=%q", stdout, stderr)
	}
	withArgs(t, markArgs...)
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" || decodeCLIJSON(t, stdout)["already_recorded"] != true {
		t.Fatalf("repeated mark stdout=%q stderr=%q", stdout, stderr)
	}

	withArgs(t, "engram", "admission", "review", "mark", secondID, "--verdict", "reject", "--unsupported", "--privacy-leak", "--json")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("second mark stderr = %q", stderr)
	}
	secondReview := decodeCLIJSON(t, stdout)["review"].(map[string]any)
	if secondReview["unsupported"] != true || secondReview["privacy_leak"] != true {
		t.Fatalf("second review = %#v", secondReview)
	}

	withArgs(t, "engram", "admission", "metrics", "--project", "engram", "--json")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("metrics stderr = %q", stderr)
	}
	metrics := decodeCLIJSON(t, stdout)
	if metrics["run_count"] != float64(1) || metrics["proposal_count"] != float64(2) || metrics["reviewed_proposal_count"] != float64(2) {
		t.Fatalf("metrics = %#v", metrics)
	}
	if metrics["automatic_promotion_gate_blocked"] != true || metrics["automatic_reject_gate_blocked"] != false {
		t.Fatalf("gates = %#v", metrics)
	}
}

func TestCmdAdmissionShadowHumanOutput(t *testing.T) {
	cfg := testConfig(t)
	mustSeedAdmissionSession(t, cfg, "shadow-human", "engram", []string{
		"Remember this: Human shadow output is inspectable.",
	}, "")

	withArgs(t, "engram", "admission", "shadow", "--project", "engram", "--session", "shadow-human")
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	for _, wanted := range []string{"Admission shadow run", "retained locally", "no Memories were written", "[admit]", "shadow-human"} {
		if !strings.Contains(stdout, wanted) {
			t.Fatalf("stdout missing %q: %q", wanted, stdout)
		}
	}
}

func TestCmdAdmissionReviewAndMetricsHumanOutputIsComplete(t *testing.T) {
	cfg := testConfig(t)
	mustSeedAdmissionSession(t, cfg, "shadow-human-review", "engram", []string{
		"Remember this: Human review output remains complete.",
	}, "## Decisions\n- Human metrics expose distributions.")

	withArgs(t, "engram", "admission", "shadow", "--project", "engram", "--session", "shadow-human-review", "--json")
	stdout, stderr := captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("shadow stderr = %q", stderr)
	}
	shadow := decodeCLIJSON(t, stdout)
	proposals := shadow["proposals"].([]any)

	withArgs(t, "engram", "admission", "review", "list", "--project", "engram")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("review list stderr = %q", stderr)
	}
	for _, want := range []string{
		"Human review output remains complete.", "Category: explicit_request", "protected: true",
		"Assessment reasons: explicit_user_request", "Evidence: prompt:", "Policy: v1",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("review list missing %q: %q", want, stdout)
		}
	}

	for _, proposal := range proposals {
		id := proposal.(map[string]any)["id"].(string)
		withArgs(t, "engram", "admission", "review", "mark", id, "--verdict", "admit")
		_, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
		if stderr != "" {
			t.Fatalf("mark stderr = %q", stderr)
		}
	}

	withArgs(t, "engram", "admission", "metrics", "--project", "engram")
	stdout, stderr = captureOutput(t, func() { cmdAdmission(cfg) })
	if stderr != "" {
		t.Fatalf("metrics stderr = %q", stderr)
	}
	for _, want := range []string{
		"review events: 2", "Protected false rejects: 0", "Unsupported: 0; privacy leaks: 0",
		"Policy versions:", "Recommendations:", "Categories:", "Human verdicts:", "Reason codes:",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("metrics missing %q: %q", want, stdout)
		}
	}
}

func TestCmdAdmissionShadowStrictErrorsAreJSON(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	mustSeedObservation(t, cfg, "shadow-errors", "engram", "decision", "Seed", "Seed.", "project")
	tests := []struct {
		name string
		args []string
		code string
	}{
		{name: "shadow missing session", args: []string{"engram", "admission", "shadow", "--project", "engram", "--json"}, code: "invalid_arguments"},
		{name: "shadow unknown session", args: []string{"engram", "admission", "shadow", "--project", "engram", "--session", "missing", "--json"}, code: "unknown_session"},
		{name: "review missing command", args: []string{"engram", "admission", "review", "--json"}, code: "invalid_arguments"},
		{name: "review list missing project", args: []string{"engram", "admission", "review", "list", "--json"}, code: "invalid_arguments"},
		{name: "review mark invalid verdict", args: []string{"engram", "admission", "review", "mark", "proposal", "--verdict", "maybe", "--json"}, code: "invalid_arguments"},
		{name: "review mark unknown proposal", args: []string{"engram", "admission", "review", "mark", "missing", "--verdict", "admit", "--json"}, code: "unknown_admission_proposal"},
		{name: "metrics missing project", args: []string{"engram", "admission", "metrics", "--json"}, code: "invalid_arguments"},
		{name: "shadow unknown flag", args: []string{"engram", "admission", "shadow", "--project", "engram", "--session", "missing", "--typo", "--json"}, code: "unknown_flag"},
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
