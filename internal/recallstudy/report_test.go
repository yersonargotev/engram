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
			Outcome: "completed", RecallResultCount: 0, FalseEmptyReview: "unknown",
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

	evidence := passingEvidence()
	report, err := study.Report(rows, evidence)
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	if report.Labels.RunsObserved != len(plan) || report.Labels.UnknownAssessments == 0 || report.Labels.Disagreements != 1 {
		t.Fatalf("label aggregates = %+v", report.Labels)
	}
	if !report.Gates.AllPassed || report.RolloutEnabled {
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
	if _, err := study.Report(missing, passingEvidence()); err == nil || !strings.Contains(err.Error(), "complete") {
		t.Fatalf("missing-row Report() error = %v", err)
	}

	unknown := rows
	unknown.Rows = append([]RunRow(nil), rows.Rows...)
	unknown.Rows[0].Assessments = []Assessment{{ResultKey: "salted", Utility: "helpful", Quality: "current", Source: "evaluator"}}
	unknown.Rows[0].RecallResultCount = 1
	unknown.Rows[0].FalseEmptyReview = "not_applicable"
	if _, err := study.Report(unknown, passingEvidence()); err == nil || !strings.Contains(err.Error(), "utility") {
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

func TestGateEvaluatorUsesFrozenPointAndConfidenceIntervalClauses(t *testing.T) {
	t.Parallel()

	study, _, _ := verifiedStudy(t)
	passing, err := study.EvaluateGates(passingEvidence())
	if err != nil {
		t.Fatal(err)
	}
	if !passing.AllPassed || len(passing.Gates) != 10 {
		t.Fatalf("passing gates = %+v", passing)
	}

	failingEvidence := passingEvidence()
	for index := range failingEvidence {
		if failingEvidence[index].Metric == "harm_difference_pp" {
			failingEvidence[index].CIUpper = .1
		}
	}
	failing, err := study.EvaluateGates(failingEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if failing.AllPassed || gateByID(t, failing, "harm").Passed {
		t.Fatalf("failing gates = %+v", failing)
	}

	incomplete := passingEvidence()[:len(passingEvidence())-1]
	if _, err := study.EvaluateGates(incomplete); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("incomplete EvaluateGates() error = %v", err)
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
		rows.Rows = append(rows.Rows, RunRow{RunID: run.RunID, SamplingUnitID: run.SamplingUnitID, TaskClass: run.TaskClass, Treatment: run.Treatment, Outcome: "completed", FalseEmptyReview: "unknown"})
	}
	return rows
}

func passingEvidence() []MetricEvidence {
	return []MetricEvidence{
		{Metric: "checkpoint_rate_delta_pp", Point: 0, CILower: -1, CIUpper: 1, Numerator: 510, Denominator: 517},
		{Metric: "stop_growth_pp", Point: 0, CILower: -.5, CIUpper: .5, Numerator: 0, Denominator: 517},
		{Metric: "automatic_injected_bytes_reduction_percent", Point: 40, CILower: 35, CIUpper: 45, Numerator: 400, Denominator: 1000},
		{Metric: "startup_compact_p95_reduction_percent", Point: 30, CILower: 26, CIUpper: 34, Numerator: 30, Denominator: 100},
		{Metric: "recall_p95_ms", Point: 200, CILower: 190, CIUpper: 210, Numerator: 200, Denominator: 1},
		{Metric: "utility_relative_improvement_percent", Point: 12, CILower: 1, CIUpper: 23, Numerator: 300, Denominator: 517},
		{Metric: "noise_rate_percent", Point: 15, CILower: 12, CIUpper: 18, Numerator: 75, Denominator: 500},
		{Metric: "noise_improvement_pp", Point: 5, CILower: 1, CIUpper: 9, Numerator: 25, Denominator: 500},
		{Metric: "harm_rate_percent", Point: 1, CILower: .2, CIUpper: 1.8, Numerator: 5, Denominator: 500},
		{Metric: "harm_difference_pp", Point: -.2, CILower: -.5, CIUpper: 0, Numerator: 1, Denominator: 500},
		{Metric: "false_empty_rate_percent", Point: 2, CILower: 1, CIUpper: 4, Numerator: 10, Denominator: 500},
		{Metric: "explicit_label_coverage_percent", Point: 85, CILower: 81, CIUpper: 89, Numerator: 425, Denominator: 500},
	}
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
