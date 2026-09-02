package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/recallstudy"
)

func TestRecallStudyHelpIsConfigFreeAndSkipsUpdateChecks(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "must-not-exist")
	t.Setenv("ENGRAM_DATA_DIR", dataDir)
	withArgs(t, "engram", "recall-study", "--help")

	stdout, stderr := captureOutput(t, func() {
		if !handleConfigFreeCommand(os.Args[1:]) {
			t.Fatal("recall-study was not handled before configuration")
		}
	})
	if stderr != "" || !strings.Contains(stdout, "run-calibration|run-held-out|publish") {
		t.Fatalf("help stdout=%q stderr=%q", stdout, stderr)
	}
	if shouldCheckForUpdates(os.Args[1:]) {
		t.Fatal("recall-study should never perform update checks")
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("config-free help created product state: %v", err)
	}
}

func TestRecallStudyExecutionCommandsRequireRuntimeAndPrivateInputs(t *testing.T) {
	stubExitWithPanic(t)
	common := []string{
		"--contract", "contract.json", "--contract-hash", "contract.sha256",
		"--calibration-manifest", "calibration.json", "--calibration-hash", "calibration.sha256",
		"--held-out-manifest", "held-out.json", "--held-out-hash", "held-out.sha256",
		"--environment", "environment.json", "--consent", "consent.json",
	}
	tests := []struct {
		command string
		extra   []string
		want    string
	}{
		{command: "run-calibration", extra: []string{"--codex-binary", "codex", "--auth-file", "auth.json", "--output", "rows.json"}, want: "--source-repo"},
		{command: "run-held-out", extra: []string{"--source-repo", "repo", "--codex-binary", "codex", "--auth-file", "auth.json", "--output", "rows.json"}, want: "--calibration-rows"},
		{command: "publish", extra: []string{"--held-out-rows", "held-out-rows.json", "--output", "publication.json"}, want: "--calibration-rows"},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			args := append([]string{"engram", "recall-study", test.command}, common...)
			args = append(args, test.extra...)
			args = append(args, "--json")
			withArgs(t, args...)
			_, stderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
			if recovered == nil || !strings.Contains(stderr, test.want) {
				t.Fatalf("%s recovered=%v stderr=%q", test.command, recovered, stderr)
			}
		})
	}
}

func TestRecallStudyRestrictedCommandsRejectHeldOutInput(t *testing.T) {
	stubExitWithPanic(t)
	for _, command := range []string{"verify", "dry-run", "plan-calibration", "report"} {
		t.Run(command, func(t *testing.T) {
			withArgs(t, "engram", "recall-study", command, "--held-out-input", "/private/held-out.json", "--json")
			_, stderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
			if recovered == nil || !strings.Contains(stderr, "unknown recall-study flag --held-out-input") {
				t.Fatalf("%s recovered=%v stderr=%q", command, recovered, stderr)
			}
		})
	}
}

func TestRecallStudyCLIVerifiesFrozenDistributionWithoutMutation(t *testing.T) {
	stubExitWithPanic(t)
	dataDir := filepath.Join(t.TempDir(), "must-not-exist")
	t.Setenv("ENGRAM_DATA_DIR", dataDir)
	root := filepath.Join("..", "..")
	studyRoot := filepath.Join(root, "evals", "recall-study", "v1")
	withArgs(t, "engram", "recall-study", "verify-distribution",
		"--contract", filepath.Join(studyRoot, "contract.json"),
		"--contract-hash", filepath.Join(studyRoot, "contract.sha256"),
		"--publication", filepath.Join(studyRoot, "publication.json"),
		"--distribution", filepath.Join(studyRoot, "distribution.json"),
		"--distribution-hash", filepath.Join(studyRoot, "distribution.sha256"),
		"--source-repo", root,
		"--json",
	)

	stdout, stderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
	if recovered != nil || stderr != "" || strings.Contains(stdout, `"ready":`) ||
		!strings.Contains(stdout, `"source_revision_verified": true`) ||
		!strings.Contains(stdout, `"source_artifacts_verified": true`) ||
		!strings.Contains(stdout, `"post_install_readiness": "not_verified"`) ||
		!strings.Contains(stdout, `"post_install_verification_command": "engram setup status codex --json"`) ||
		!strings.Contains(stdout, `"disposition": "continue_canary"`) ||
		!strings.Contains(stdout, `"action": "preserve_verified_tuple"`) ||
		!strings.Contains(stdout, `"legacy_contraction_allowed": false`) ||
		!strings.Contains(stdout, `"release_required": false`) {
		t.Fatalf("verify-distribution recovered=%v stdout=%q stderr=%q", recovered, stdout, stderr)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("verify-distribution created product state: %v", err)
	}
}

func TestRecallStudyDistributionCLIRejectsInvalidFlagsAndInputs(t *testing.T) {
	stubExitWithPanic(t)
	root := filepath.Join("..", "..")
	studyRoot := filepath.Join(root, "evals", "recall-study", "v1")
	valid := []string{
		"--contract", filepath.Join(studyRoot, "contract.json"),
		"--contract-hash", filepath.Join(studyRoot, "contract.sha256"),
		"--publication", filepath.Join(studyRoot, "publication.json"),
		"--distribution", filepath.Join(studyRoot, "distribution.json"),
		"--distribution-hash", filepath.Join(studyRoot, "distribution.sha256"),
		"--source-repo", root, "--json",
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "unknown flag", args: []string{"--unknown", "value", "--json"}, want: "unknown recall-study flag --unknown"},
		{name: "positional", args: append(append([]string(nil), valid...), "unexpected"), want: "unexpected recall-study argument"},
		{name: "required", args: []string{"--json"}, want: "requires --contract"},
		{name: "contract", args: replaceFlagValue(valid, "--contract", filepath.Join(t.TempDir(), "missing.json")), want: "invalid_recall_study_contract"},
		{name: "publication", args: replaceFlagValue(valid, "--publication", filepath.Join(t.TempDir(), "missing.json")), want: "invalid_recall_study_publication"},
		{name: "distribution", args: replaceFlagValue(valid, "--distribution", filepath.Join(t.TempDir(), "missing.json")), want: "invalid_recall_distribution"},
		{name: "verification", args: replaceFlagValue(valid, "--source-repo", t.TempDir()), want: "recall_distribution_verification_failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			withArgs(t, append([]string{"engram", "recall-study", "verify-distribution"}, test.args...)...)
			_, stderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
			if recovered == nil || !strings.Contains(stderr, test.want) {
				t.Fatalf("recovered=%v stderr=%q, want %q", recovered, stderr, test.want)
			}
		})
	}
}

func replaceFlagValue(args []string, name, value string) []string {
	changed := append([]string(nil), args...)
	for index := range changed {
		if changed[index] == name && index+1 < len(changed) {
			changed[index+1] = value
			return changed
		}
	}
	return changed
}

func TestRecallStudyCLIValidatesAndPlansCommittedStudyWithoutHeldOutAccess(t *testing.T) {
	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	study, err := recallstudy.Load(filepath.Join(root, "contract.json"), filepath.Join(root, "contract.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	calibration, err := recallstudy.LoadManifest(filepath.Join(root, "calibration", "manifest.json"), filepath.Join(root, "calibration", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	heldOut, err := recallstudy.LoadManifest(filepath.Join(root, "held-out", "manifest.json"), filepath.Join(root, "held-out", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	environmentPath := filepath.Join(dir, "environment.json")
	consentPath := filepath.Join(dir, "consent.json")
	writeRecallStudyTestJSON(t, environmentPath, recallStudyCompatibilityEvidence(study))
	writeRecallStudyTestJSON(t, consentPath, recallstudy.ConsentEvidence{
		StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: study.ConsentCommitment(&calibration.Manifest, &heldOut.Manifest),
	})
	common := []string{
		"--contract", filepath.Join(root, "contract.json"), "--contract-hash", filepath.Join(root, "contract.sha256"),
		"--calibration-manifest", filepath.Join(root, "calibration", "manifest.json"), "--calibration-hash", filepath.Join(root, "calibration", "manifest.sha256"),
		"--held-out-manifest", filepath.Join(root, "held-out", "manifest.json"), "--held-out-hash", filepath.Join(root, "held-out", "manifest.sha256"),
		"--environment", environmentPath, "--consent", consentPath, "--json",
	}
	run := func(command string, extra ...string) string {
		t.Helper()
		args := append([]string{"engram", "recall-study", command}, common...)
		args = append(args, extra...)
		withArgs(t, args...)
		stdout, stderr := captureOutput(t, cmdRecallStudy)
		if stderr != "" {
			t.Fatalf("%s stderr = %q", command, stderr)
		}
		return stdout
	}
	if output := run("verify"); !strings.Contains(output, `"ready": true`) || !strings.Contains(output, `"held_out_inputs_accessed": false`) {
		t.Fatalf("verify output = %s", output)
	}
	if output := run("dry-run"); !strings.Contains(output, `"planned_runs": 1551`) || !strings.Contains(output, `"held_out_inputs_accessed": false`) {
		t.Fatalf("dry-run output = %s", output)
	}
	calibrationPlan := filepath.Join(dir, "calibration-plan.json")
	if output := run("plan-calibration", "--output", calibrationPlan); !strings.Contains(output, `"planned_runs": 180`) {
		t.Fatalf("plan-calibration output = %s", output)
	}
	info, err := os.Stat(calibrationPlan)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("calibration plan mode = %v err=%v", info, err)
	}
}

func TestRecallStudyCLIReportDerivesAndWritesAggregateEvidence(t *testing.T) {
	stubExitWithPanic(t)
	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	study, err := recallstudy.Load(filepath.Join(root, "contract.json"), filepath.Join(root, "contract.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	calibration, err := recallstudy.LoadManifest(filepath.Join(root, "calibration", "manifest.json"), filepath.Join(root, "calibration", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	heldOut, err := recallstudy.LoadManifest(filepath.Join(root, "held-out", "manifest.json"), filepath.Join(root, "held-out", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := study.Plan(&calibration.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	rows := recallstudy.RowSet{SchemaVersion: recallstudy.RowSetSchemaVersion, StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash, CohortID: calibration.Manifest.CohortID}
	for _, run := range plan {
		row := recallstudy.RunRow{RunID: run.RunID, SamplingUnitID: run.SamplingUnitID, TaskClass: run.TaskClass,
			Treatment: run.Treatment, Outcome: "completed", TaskOutcome: "succeeded", FalseEmptyReview: "not_applicable",
			CheckpointSucceeded: true, AutomaticInjectedUTF8Bytes: 1000, StartupCompactLatencyMillis: 100, TimeToUsefulMillis: 200}
		if run.Treatment == "targeted-recall" {
			row.AutomaticInjectedUTF8Bytes = 500
			row.StartupCompactLatencyMillis = 60
			row.RecallLatencyMillis = 100
			row.TimeToUsefulMillis = 120
		}
		if run.Treatment != "no-recall" {
			row.RecallResultCount = 1
			row.Assessments = []recallstudy.Assessment{{ResultKey: "salted-" + run.RunID, Utility: "orienting", Quality: "current", Source: "evaluator"}}
		}
		rows.Rows = append(rows.Rows, row)
	}
	dir := t.TempDir()
	environmentPath := filepath.Join(dir, "environment.json")
	consentPath := filepath.Join(dir, "consent.json")
	rowsPath := filepath.Join(dir, "rows.json")
	reportPath := filepath.Join(dir, "shared", "report.json")
	writeRecallStudyTestJSON(t, environmentPath, recallStudyCompatibilityEvidence(study))
	writeRecallStudyTestJSON(t, consentPath, recallstudy.ConsentEvidence{StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: study.ConsentCommitment(&calibration.Manifest, &heldOut.Manifest)})
	writeRecallStudyTestJSON(t, rowsPath, rows)
	common := []string{
		"--contract", filepath.Join(root, "contract.json"), "--contract-hash", filepath.Join(root, "contract.sha256"),
		"--calibration-manifest", filepath.Join(root, "calibration", "manifest.json"), "--calibration-hash", filepath.Join(root, "calibration", "manifest.sha256"),
		"--held-out-manifest", filepath.Join(root, "held-out", "manifest.json"), "--held-out-hash", filepath.Join(root, "held-out", "manifest.sha256"),
		"--environment", environmentPath, "--consent", consentPath, "--json",
	}
	args := append([]string{"engram", "recall-study", "report"}, common...)
	args = append(args, "--rows", rowsPath, "--output", reportPath)
	withArgs(t, args...)
	stdout, stderr := captureOutput(t, cmdRecallStudy)
	if stderr != "" || !strings.Contains(stdout, `"automatic_injected_bytes_reduction_percent"`) {
		t.Fatalf("report stdout=%q stderr=%q", stdout, stderr)
	}
	info, err := os.Stat(reportPath)
	if err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("shared report mode=%v err=%v", info, err)
	}
	reportRaw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(reportRaw), plan[0].RunID) || strings.Contains(string(reportRaw), "salted-") {
		t.Fatalf("shared report leaked private identifiers: %s", reportRaw)
	}
	heldOutRowsPath := filepath.Join(dir, "held-out-rows.json")
	heldOutRows := rows
	heldOutRows.CohortID = heldOut.Manifest.CohortID
	writeRecallStudyTestJSON(t, heldOutRowsPath, heldOutRows)
	args = append([]string{"engram", "recall-study", "report"}, common...)
	args = append(args, "--rows", heldOutRowsPath)
	withArgs(t, args...)
	_, heldOutStderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
	if recovered == nil || !strings.Contains(heldOutStderr, "issue #110") {
		t.Fatalf("held-out report recovered=%v stderr=%q", recovered, heldOutStderr)
	}

	rawRows, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	invalidRowsPath := filepath.Join(dir, "invalid-rows.json")
	invalidRows := strings.TrimSuffix(string(rawRows), "}") + `,"prompt":"PRIVATE"}`
	if err := os.WriteFile(invalidRowsPath, []byte(invalidRows), 0o600); err != nil {
		t.Fatal(err)
	}
	args = append([]string{"engram", "recall-study", "report"}, common...)
	args = append(args, "--rows", invalidRowsPath)
	withArgs(t, args...)
	_, invalidStderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
	if recovered == nil || !strings.Contains(invalidStderr, "unknown field") {
		t.Fatalf("invalid report recovered=%v stderr=%q", recovered, invalidStderr)
	}

	blockedParent := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blockedParent, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	args = append([]string{"engram", "recall-study", "report"}, common...)
	args = append(args, "--rows", rowsPath, "--output", filepath.Join(blockedParent, "report.json"))
	withArgs(t, args...)
	_, outputStderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
	if recovered == nil || !strings.Contains(outputStderr, `"code":"output_error"`) {
		t.Fatalf("output failure recovered=%v stderr=%q", recovered, outputStderr)
	}
}

func TestRecallStudyCLIPublishesOneAggregateOnlyDisposition(t *testing.T) {
	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	study, err := recallstudy.Load(filepath.Join(root, "contract.json"), filepath.Join(root, "contract.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	calibration, err := recallstudy.LoadManifest(filepath.Join(root, "calibration", "manifest.json"), filepath.Join(root, "calibration", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	heldOut, err := recallstudy.LoadManifest(filepath.Join(root, "held-out", "manifest.json"), filepath.Join(root, "held-out", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	environmentPath := filepath.Join(dir, "environment.json")
	consentPath := filepath.Join(dir, "consent.json")
	calibrationRowsPath := filepath.Join(dir, "calibration-rows.json")
	heldOutRowsPath := filepath.Join(dir, "held-out-rows.json")
	publicationPath := filepath.Join(dir, "shared", "publication.json")
	writeRecallStudyTestJSON(t, environmentPath, recallStudyCompatibilityEvidence(study))
	writeRecallStudyTestJSON(t, consentPath, recallstudy.ConsentEvidence{
		StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: study.ConsentCommitment(&calibration.Manifest, &heldOut.Manifest),
	})
	calibrationRows := recallStudyCompleteRows(t, study, &calibration.Manifest)
	heldOutRows := recallStudyCompleteRows(t, study, &heldOut.Manifest)
	writeRecallStudyTestJSON(t, calibrationRowsPath, calibrationRows)
	writeRecallStudyTestJSON(t, heldOutRowsPath, heldOutRows)

	withArgs(t, "engram", "recall-study", "publish",
		"--contract", filepath.Join(root, "contract.json"), "--contract-hash", filepath.Join(root, "contract.sha256"),
		"--calibration-manifest", filepath.Join(root, "calibration", "manifest.json"), "--calibration-hash", filepath.Join(root, "calibration", "manifest.sha256"),
		"--held-out-manifest", filepath.Join(root, "held-out", "manifest.json"), "--held-out-hash", filepath.Join(root, "held-out", "manifest.sha256"),
		"--environment", environmentPath, "--consent", consentPath,
		"--calibration-rows", calibrationRowsPath, "--held-out-rows", heldOutRowsPath,
		"--output", publicationPath, "--json")
	stdout, stderr := captureOutput(t, cmdRecallStudy)
	if stderr != "" || !strings.Contains(stdout, `"disposition": "continue_canary"`) || !strings.Contains(stdout, `"rollout_enabled": false`) {
		t.Fatalf("publish stdout=%q stderr=%q", stdout, stderr)
	}
	info, err := os.Stat(publicationPath)
	if err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("publication mode=%v err=%v", info, err)
	}
	raw, err := os.ReadFile(publicationPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), calibrationRows.Rows[0].RunID) || strings.Contains(string(raw), "salted-") || !strings.Contains(string(raw), `"shared_output": "aggregate-only"`) {
		t.Fatalf("publication is not aggregate-only: %s", raw)
	}
}

func writeRecallStudyTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func recallStudyCompatibilityEvidence(study *recallstudy.Study) recallstudy.CompatibilityEvidence {
	rangeV1 := &protocolcontract.VersionRange{Minimum: 1, Maximum: 1}
	provenance := "repository:https://github.com/yersonargotev/engram.git#revision:" + study.Contract.SourceRevision
	return recallstudy.CompatibilityEvidence{
		Revisions: study.Contract.Revisions,
		Compatibility: protocolcontract.Evaluate(
			protocolcontract.Declaration{Version: study.Contract.Revisions.ManagedPack.Version, Provenance: provenance, Supported: rangeV1},
			protocolcontract.Declaration{Version: study.Contract.Revisions.EngramBinary.Version, Provenance: provenance, Supported: rangeV1},
			protocolcontract.Declaration{Version: study.Contract.Revisions.CodexPlugin.Version, Provenance: provenance, Supported: rangeV1},
		),
	}
}

func recallStudyCompleteRows(t *testing.T, study *recallstudy.Study, manifest *recallstudy.Manifest) recallstudy.RowSet {
	t.Helper()
	plan, err := study.Plan(manifest)
	if err != nil {
		t.Fatal(err)
	}
	rows := recallstudy.RowSet{
		SchemaVersion: recallstudy.RowSetSchemaVersion, StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash, CohortID: manifest.CohortID,
	}
	for _, run := range plan {
		row := recallstudy.RunRow{
			RunID: run.RunID, SamplingUnitID: run.SamplingUnitID, TaskClass: run.TaskClass, Treatment: run.Treatment,
			Outcome: "completed", TaskOutcome: "succeeded", FalseEmptyReview: "not_applicable", CheckpointSucceeded: true,
			AutomaticInjectedUTF8Bytes: 1000, StartupCompactLatencyMillis: 100, TimeToUsefulMillis: 200,
		}
		if run.Treatment == "targeted-recall" {
			row.AutomaticInjectedUTF8Bytes = 500
			row.StartupCompactLatencyMillis = 60
			row.RecallLatencyMillis = 100
			row.TimeToUsefulMillis = 120
		}
		if run.Treatment != "no-recall" {
			row.RecallResultCount = 1
			row.Assessments = []recallstudy.Assessment{{ResultKey: "salted-" + run.RunID, Utility: "orienting", Quality: "current", Source: "evaluator"}}
		}
		rows.Rows = append(rows.Rows, row)
	}
	return rows
}
