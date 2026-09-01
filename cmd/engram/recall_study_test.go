package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	if stderr != "" || !strings.Contains(stdout, "recall-study verify|dry-run|calibrate|run-held-out|report") {
		t.Fatalf("help stdout=%q stderr=%q", stdout, stderr)
	}
	if shouldCheckForUpdates(os.Args[1:]) {
		t.Fatal("recall-study should never perform update checks")
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("config-free help created product state: %v", err)
	}
}

func TestRecallStudyRestrictedCommandsRejectHeldOutInput(t *testing.T) {
	stubExitWithPanic(t)
	for _, command := range []string{"verify", "dry-run", "calibrate", "report"} {
		t.Run(command, func(t *testing.T) {
			withArgs(t, "engram", "recall-study", command, "--held-out-input", "/private/held-out.json", "--json")
			_, stderr, recovered := captureOutputAndRecover(t, cmdRecallStudy)
			if recovered == nil || !strings.Contains(stderr, "unknown recall-study flag --held-out-input") {
				t.Fatalf("%s recovered=%v stderr=%q", command, recovered, stderr)
			}
		})
	}
}

func TestRecallStudyCLIValidatesAndPlansCommittedStudyWithoutHeldOutAccess(t *testing.T) {
	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	study, err := recallstudy.Load(filepath.Join(root, "contract.json"), filepath.Join(root, "contract.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	environmentPath := filepath.Join(dir, "environment.json")
	consentPath := filepath.Join(dir, "consent.json")
	writeRecallStudyTestJSON(t, environmentPath, recallstudy.CompatibilityEvidence{Revisions: study.Contract.Revisions, Ready: true})
	writeRecallStudyTestJSON(t, consentPath, recallstudy.ConsentEvidence{
		StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		CalibrationGranted: true, HeldOutGranted: true, ProofSHA256: strings.Repeat("c", 64),
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
	if output := run("calibrate", "--output", calibrationPlan); !strings.Contains(output, `"planned_runs": 180`) {
		t.Fatalf("calibrate output = %s", output)
	}
	info, err := os.Stat(calibrationPlan)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("calibration plan mode = %v err=%v", info, err)
	}
	heldOutPlan := filepath.Join(dir, "held-out-plan.json")
	if output := run("run-held-out", "--output", heldOutPlan); !strings.Contains(output, `"planned_runs": 1371`) || !strings.Contains(output, `"held_out_run_authorized": true`) {
		t.Fatalf("run-held-out output = %s", output)
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
