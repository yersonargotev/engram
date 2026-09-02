package recallstudy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeCohortRunner struct {
	study      *Study
	manifest   *Manifest
	calls      int
	cleanups   int
	cleanupErr error
}

type invalidCohortRunner struct{}

func (*invalidCohortRunner) Close() error { return nil }

func (*invalidCohortRunner) Run(context.Context, PlannedRun, TaskInput) (cohortRun, error) {
	return cohortRun{Cleanup: func() error { return nil }}, &invalidExecutionError{reasonCode: "targeted_recall_not_observed"}
}

func (runner *fakeCohortRunner) Close() error { return nil }

func (runner *fakeCohortRunner) Run(_ context.Context, planned PlannedRun, input TaskInput) (cohortRun, error) {
	runner.calls++
	if err := runner.study.VerifyTaskInput(runner.manifest, input); err != nil {
		return cohortRun{}, err
	}
	row := RunRow{
		RunID: planned.RunID, SamplingUnitID: planned.SamplingUnitID, TaskClass: planned.TaskClass,
		Treatment: planned.Treatment, Outcome: "completed", TaskOutcome: "succeeded",
		FalseEmptyReview: "not_applicable", CheckpointSucceeded: true,
		AutomaticInjectedUTF8Bytes: 100, StartupCompactLatencyMillis: 2, TimeToUsefulMillis: 3,
	}
	if planned.Treatment != "no-recall" {
		row.RecallResultCount = 1
		row.Assessments = []Assessment{{ResultKey: "result-" + planned.RunID, Utility: "orienting", Quality: "current", Source: "evaluator"}}
	}
	if planned.Treatment == "targeted-recall" {
		row.RecallLatencyMillis = 1
	}
	return cohortRun{Row: row, Cleanup: func() error {
		runner.cleanups++
		return runner.cleanupErr
	}}, nil
}

func TestExecuteRejectsHeldOutBeforeSuccessfulCalibrationWithoutMaterializingEvidence(t *testing.T) {
	study, calibration, heldOut := verifiedStudy(t)
	output := filepath.Join(t.TempDir(), "private", "held-out", "rows.json")

	_, err := study.Execute(context.Background(), ExecutionRequest{
		Verification: VerificationInput{
			Calibration:   calibration,
			HeldOut:       heldOut,
			Compatibility: compatibleEvidence(study),
			Consent:       consentEvidence(study, calibration, heldOut),
		},
		Cohort:     heldOut,
		OutputPath: output,
	})
	if err == nil || !strings.Contains(err.Error(), "successful calibration evidence") {
		t.Fatalf("Execute error = %v, want missing calibration evidence", err)
	}
	if _, statErr := os.Stat(filepath.Dir(output)); !os.IsNotExist(statErr) {
		t.Fatalf("held-out execution materialized private state before calibration: %v", statErr)
	}
}

func TestExecuteRejectsHeldOutWhenCalibrationHasOperationalFailures(t *testing.T) {
	study, calibration, heldOut := verifiedStudy(t)
	plan := mustPlan(t, study, calibration)
	rows := completeRows(study, calibration, plan)
	rows.Rows[0] = RunRow{
		RunID: rows.Rows[0].RunID, SamplingUnitID: rows.Rows[0].SamplingUnitID,
		TaskClass: rows.Rows[0].TaskClass, Treatment: rows.Rows[0].Treatment,
		Outcome: "operational_failure", TaskOutcome: "not_applicable",
		OmissionCode: "runner_process_failed", FalseEmptyReview: "not_applicable",
	}
	output := filepath.Join(t.TempDir(), "private", "held-out", "rows.json")

	_, err := study.Execute(context.Background(), ExecutionRequest{
		Verification: VerificationInput{
			Calibration:   calibration,
			HeldOut:       heldOut,
			Compatibility: compatibleEvidence(study),
			Consent:       consentEvidence(study, calibration, heldOut),
		},
		Cohort: heldOut, CalibrationRows: &rows, OutputPath: output,
	})
	if err == nil || !strings.Contains(err.Error(), "successful calibration evidence") {
		t.Fatalf("Execute error = %v, want unsuccessful calibration evidence", err)
	}
	if _, statErr := os.Stat(filepath.Dir(output)); !os.IsNotExist(statErr) {
		t.Fatalf("held-out execution materialized private state after failed calibration: %v", statErr)
	}
}

func TestExecutePersistsVerifiedCalibrationRowsAndResumes(t *testing.T) {
	study, calibration, heldOut := verifiedStudy(t)
	output := filepath.Join(t.TempDir(), "private", "calibration", "rows.json")
	verification := VerificationInput{
		Calibration: calibration, HeldOut: heldOut,
		Compatibility: compatibleEvidence(study), Consent: consentEvidence(study, calibration, heldOut),
	}
	runner := &fakeCohortRunner{study: study, manifest: calibration}

	result, err := study.Execute(context.Background(), ExecutionRequest{
		Verification: verification, Cohort: calibration, OutputPath: output, runner: runner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.PlannedRuns != 180 || result.ObservedRuns != 180 || !result.Complete || !result.NextStageReady || runner.calls != 180 || runner.cleanups != 180 {
		t.Fatalf("first execution result=%#v calls=%d cleanups=%d", result, runner.calls, runner.cleanups)
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("private rows mode=%v err=%v", info, err)
	}
	rows, err := ReadRowSet(output)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := study.Report(rows); err != nil {
		t.Fatalf("persisted rows do not satisfy the frozen report seam: %v", err)
	}

	resumed := &fakeCohortRunner{study: study, manifest: calibration}
	result, err = study.Execute(context.Background(), ExecutionRequest{
		Verification: verification, Cohort: calibration, OutputPath: output, runner: resumed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ObservedRuns != 180 || resumed.calls != 0 {
		t.Fatalf("resumed execution result=%#v calls=%d", result, resumed.calls)
	}
}

func TestExecuteCleansCellOnlyAfterPrivateProgressIsDurable(t *testing.T) {
	study, calibration, heldOut := verifiedStudy(t)
	verification := VerificationInput{
		Calibration: calibration, HeldOut: heldOut,
		Compatibility: compatibleEvidence(study), Consent: consentEvidence(study, calibration, heldOut),
	}
	t.Run("persistence failure retains cell", func(t *testing.T) {
		runner := &fakeCohortRunner{study: study, manifest: calibration}
		_, err := study.Execute(context.Background(), ExecutionRequest{
			Verification: verification, Cohort: calibration, OutputPath: filepath.Join(t.TempDir(), "rows.json"), runner: runner,
			writeRows: func(string, any) error { return errors.New("persistence failed") },
		})
		if err == nil || !strings.Contains(err.Error(), "persist Recall study progress") {
			t.Fatalf("Execute error = %v", err)
		}
		if runner.calls != 1 || runner.cleanups != 0 {
			t.Fatalf("calls=%d cleanups=%d, want cell retained", runner.calls, runner.cleanups)
		}
	})
	t.Run("cleanup failure follows persistence", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "private", "rows.json")
		runner := &fakeCohortRunner{study: study, manifest: calibration, cleanupErr: errors.New("cleanup failed")}
		_, err := study.Execute(context.Background(), ExecutionRequest{
			Verification: verification, Cohort: calibration, OutputPath: output, runner: runner,
		})
		if err == nil || !strings.Contains(err.Error(), "clean persisted Recall study cell") {
			t.Fatalf("Execute error = %v", err)
		}
		rows, readErr := ReadRowSet(output)
		if readErr != nil || len(rows.Rows) != 1 || runner.cleanups != 1 {
			t.Fatalf("durable rows=%d readErr=%v cleanups=%d", len(rows.Rows), readErr, runner.cleanups)
		}
	})
}

func TestExecutePublishesContinueCanaryWithoutFabricatingRowsWhenTreatmentEvidenceIsInvalid(t *testing.T) {
	study, calibration, heldOut := verifiedStudy(t)
	output := filepath.Join(t.TempDir(), "private", "calibration", "rows.json")
	result, err := study.Execute(context.Background(), ExecutionRequest{
		Verification: VerificationInput{
			Calibration: calibration, HeldOut: heldOut,
			Compatibility: compatibleEvidence(study), Consent: consentEvidence(study, calibration, heldOut),
		},
		Cohort: calibration, OutputPath: output, runner: &invalidCohortRunner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Complete || result.NextStageReady || result.Disposition != DispositionContinueCanary || result.ReasonCode != "targeted_recall_not_observed" || result.ObservedRuns != 0 {
		t.Fatalf("invalid calibration result = %#v", result)
	}
	if _, statErr := os.Stat(output); !os.IsNotExist(statErr) {
		t.Fatalf("invalid treatment evidence fabricated a row set: %v", statErr)
	}
	publication, err := study.PublishCalibrationStatus(result)
	if err != nil {
		t.Fatal(err)
	}
	if publication.Valid || publication.Disposition != DispositionContinueCanary || publication.Report != nil || publication.RolloutEnabled || len(publication.EvidenceGaps) != len(study.Contract.Gates) {
		t.Fatalf("invalid calibration publication = %#v", publication)
	}
}

func TestExecuteRejectsRuntimeDriftBeforeWritingProgress(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("scripted Codex version probe is Unix-specific")
	}
	study, calibration, heldOut := verifiedStudy(t)
	dir := t.TempDir()
	codex := filepath.Join(dir, "codex")
	if err := os.WriteFile(codex, []byte("#!/bin/sh\nprintf '%s\\n' 'codex-cli 0.151.0'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	auth := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(auth, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "private", "rows.json")

	_, err = study.Execute(context.Background(), ExecutionRequest{
		Verification: VerificationInput{
			Calibration: calibration, HeldOut: heldOut,
			Compatibility: compatibleEvidence(study), Consent: consentEvidence(study, calibration, heldOut),
		},
		Cohort: calibration, OutputPath: output,
		Runtime: ExecutionRuntime{SourceRepo: repo, CodexBinary: codex, AuthFile: auth, TempRoot: dir},
	})
	if err == nil || !strings.Contains(err.Error(), "Codex version mismatch") {
		t.Fatalf("Execute error=%v, want Codex version mismatch", err)
	}
	if _, statErr := os.Stat(output); !os.IsNotExist(statErr) {
		t.Fatalf("runtime drift wrote study progress: %v", statErr)
	}
}
