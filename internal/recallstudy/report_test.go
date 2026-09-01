package recallstudy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReportPreservesLabelsUnknownsDisagreementsAndOnlySharesAggregates(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	plan := mustPlan(t, study, calibration)
	rows := RowSet{
		SchemaVersion: RowSetSchemaVersion, StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		ContractSHA256: study.Hash, CohortID: calibration.CohortID,
	}
	disagreementAdded := false
	for index, run := range plan {
		row := RunRow{
			RunID: run.RunID, SamplingUnitID: run.SamplingUnitID, TaskClass: run.TaskClass, Treatment: run.Treatment,
			Outcome: "completed", RecallResultCount: 0, FalseEmptyReview: "not_applicable",
			CheckpointSucceeded: true, AutomaticInjectedUTF8Bytes: 1000, StartupCompactLatencyMillis: 100,
		}
		if run.Treatment == "targeted-recall" {
			row.RecallLatencyMillis = 100
		}
		if run.Treatment != "no-recall" {
			row.RecallResultCount = 1
			row.FalseEmptyReview = "not_applicable"
			if index%4 != 0 {
				row.Assessments = []Assessment{{ResultKey: "salted-result-" + run.RunID, Utility: "orienting", Quality: "current", Source: "evaluator"}}
			}
		}
		if !disagreementAdded && len(row.Assessments) > 0 {
			row.Assessments = append(row.Assessments,
				Assessment{ResultKey: row.Assessments[0].ResultKey, Utility: "unused", Quality: "stale", Source: "user_explicit"})
			disagreementAdded = true
		}
		rows.Rows = append(rows.Rows, row)
	}

	report, err := study.Report(rows)
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if report.Labels.RunsObserved != len(plan) || report.Labels.UnknownAssessments == 0 || report.Labels.Disagreements != 1 {
		t.Fatalf("label aggregates = %+v", report.Labels)
	}
	if len(report.Gates.Evidence) != 12 || report.RolloutEnabled {
		t.Fatalf("gate report = %+v, rollout_enabled=%v", report.Gates, report.RolloutEnabled)
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	shared := string(encoded)
	for _, forbidden := range []string{plan[0].RunID, plan[0].SamplingUnitID, "salted-result-", "prompt", "assistant_text", "transcript_path"} {
		if strings.Contains(shared, forbidden) {
			t.Fatalf("shared report leaked %q: %s", forbidden, shared)
		}
	}
}

func TestReportRejectsMissingRowsUnknownLabelsAndRawFields(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	plan := mustPlan(t, study, calibration)
	rows := completeRows(study, calibration, plan)

	missing := rows
	missing.Rows = missing.Rows[:len(missing.Rows)-1]
	if _, err := study.Report(missing); err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("missing-row Report() error = %v", err)
	}

	unknown := rows
	unknown.Rows = append([]RunRow(nil), rows.Rows...)
	unknown.Rows[0].Assessments = []Assessment{{ResultKey: "salted", Utility: "helpful", Quality: "current", Source: "evaluator"}}
	unknown.Rows[0].RecallResultCount = 1
	unknown.Rows[0].FalseEmptyReview = "not_applicable"
	if _, err := study.Report(unknown); err == nil || !strings.Contains(err.Error(), "utility") {
		t.Fatalf("unknown-label Report() error = %v", err)
	}

	raw := `{"schema_version":"recall-study-rows-v1","study_id":"x","study_version":"v1","contract_sha256":"` + strings.Repeat("a", 64) + `","cohort_id":"calibration-v1","rows":[],"prompt":"PRIVATE"}`
	path := filepath.Join(t.TempDir(), "rows.json")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRowSet(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("ReadRowSet() error = %v, want unknown field", err)
	}
}

func TestReportDerivesFrozenMetricsAndIntervalsFromRows(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	rows := completeRows(study, calibration, mustPlan(t, study, calibration))
	report, err := study.Report(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Gates.Gates) != 10 || report.Gates.AllPassed {
		t.Fatalf("derived gates = %+v", report.Gates)
	}
	bytes := metricByID(t, report.Gates, "automatic_injected_bytes_reduction_percent")
	if bytes.Point != 50 || bytes.CILower != 50 || bytes.CIUpper != 50 || bytes.Numerator != 30000 || bytes.Denominator != 60000 {
		t.Fatalf("derived bytes metric = %+v", bytes)
	}
	repeated, err := study.Report(rows)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, _ := json.Marshal(report.Gates.Evidence)
	secondJSON, _ := json.Marshal(repeated.Gates.Evidence)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("frozen bootstrap changed: first=%s second=%s", firstJSON, secondJSON)
	}

	mutated := rows
	mutated.Rows = append([]RunRow(nil), rows.Rows...)
	for index := range mutated.Rows {
		if mutated.Rows[index].Treatment == "targeted-recall" {
			mutated.Rows[index].AutomaticInjectedUTF8Bytes = 900
		}
	}
	changed, err := study.Report(mutated)
	if err != nil {
		t.Fatal(err)
	}
	changedBytes := metricByID(t, changed.Gates, "automatic_injected_bytes_reduction_percent").Point
	if changedBytes < 9.999 || changedBytes > 10.001 {
		t.Fatalf("metric was not bound to rows: %+v", changed.Gates.Evidence)
	}
}

func TestReportRejectsNoRecallEvidenceAndHeldOutRows(t *testing.T) {
	t.Parallel()

	study, calibration, heldOut := verifiedStudy(t)
	rows := completeRows(study, calibration, mustPlan(t, study, calibration))
	for index := range rows.Rows {
		if rows.Rows[index].Treatment == "no-recall" {
			rows.Rows[index].RecallResultCount = 1
			rows.Rows[index].FalseEmptyReview = "not_applicable"
			if _, err := study.Report(rows); err == nil || !strings.Contains(err.Error(), "no-Recall") {
				t.Fatalf("no-Recall contradiction error = %v", err)
			}
			break
		}
	}
	heldOutRows := completeRows(study, heldOut, mustPlan(t, study, heldOut))
	if _, err := study.Report(heldOutRows); err == nil || !strings.Contains(err.Error(), "calibration rows only") {
		t.Fatalf("held-out Report() error = %v", err)
	}
}

func TestPrivateAndSharedArtifactsUseDifferentPermissions(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private", "rows.json")
	sharedPath := filepath.Join(dir, "shared", "report.json")
	if err := WritePrivateJSON(privatePath, map[string]bool{"private": true}); err != nil {
		t.Fatal(err)
	}
	if err := WriteSharedJSON(sharedPath, map[string]bool{"aggregate": true}); err != nil {
		t.Fatal(err)
	}
	privateInfo, _ := os.Stat(privatePath)
	sharedInfo, _ := os.Stat(sharedPath)
	if privateInfo.Mode().Perm() != 0o600 || sharedInfo.Mode().Perm() != 0o644 {
		t.Fatalf("artifact modes = private %o shared %o", privateInfo.Mode().Perm(), sharedInfo.Mode().Perm())
	}
}

func completeRows(study *Study, manifest *Manifest, plan []PlannedRun) RowSet {
	rows := RowSet{SchemaVersion: RowSetSchemaVersion, StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash, CohortID: manifest.CohortID}
	for _, run := range plan {
		row := RunRow{RunID: run.RunID, SamplingUnitID: run.SamplingUnitID, TaskClass: run.TaskClass, Treatment: run.Treatment,
			Outcome: "completed", FalseEmptyReview: "not_applicable", CheckpointSucceeded: true,
			AutomaticInjectedUTF8Bytes: 1000, StartupCompactLatencyMillis: 100}
		if run.Treatment == "targeted-recall" {
			row.AutomaticInjectedUTF8Bytes = 500
			row.StartupCompactLatencyMillis = 60
			row.RecallLatencyMillis = 100
		}
		if run.Treatment != "no-recall" {
			row.RecallResultCount = 1
			row.Assessments = []Assessment{{ResultKey: "salted-" + run.RunID, Utility: "orienting", Quality: "current", Source: "evaluator"}}
		}
		rows.Rows = append(rows.Rows, row)
	}
	return rows
}

func gateByID(t *testing.T, report GateReport, id string) GateResult {
	t.Helper()
	for _, gate := range report.Gates {
		if gate.ID == id {
			return gate
		}
	}
	t.Fatalf("gate %q not found", id)
	return GateResult{}
}

func metricByID(t *testing.T, report GateReport, id string) MetricEvidence {
	t.Helper()
	for _, metric := range report.Evidence {
		if metric.Metric == id {
			return metric
		}
	}
	t.Fatalf("metric %q not found", id)
	return MetricEvidence{}
}
