package recallstudy

import (
	"encoding/json"
	"math"
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
			Outcome: "completed", TaskOutcome: "succeeded", RecallResultCount: 0, FalseEmptyReview: "not_applicable",
			CheckpointSucceeded: true, AutomaticInjectedUTF8Bytes: 1000, StartupCompactLatencyMillis: 100, TimeToUsefulMillis: 200,
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

func TestDerivedMetricsMatchIndependentReferenceVector(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	rows := completeRows(study, calibration, mustPlan(t, study, calibration))
	broadIndex, targetedIndex := 0, 0
	for index := range rows.Rows {
		row := &rows.Rows[index]
		switch row.Treatment {
		case "broad-chronological":
			row.CheckpointSucceeded = true
			row.StopConflictOrLoop = false
			utility := "orienting"
			if broadIndex >= 30 {
				utility = "unused"
				if broadIndex < 45 {
					utility = "duplicate"
				}
			}
			quality := "current"
			if broadIndex < 6 {
				quality = "stale"
			}
			row.Assessments = []Assessment{{ResultKey: "broad-result-" + row.RunID, Utility: utility, Quality: quality, Source: "evaluator"}}
			broadIndex++
		case "targeted-recall":
			row.CheckpointSucceeded = targetedIndex >= 6
			row.StopConflictOrLoop = targetedIndex < 3
			row.RecallLatencyMillis = float64(100 + targetedIndex)
			if targetedIndex < 20 {
				row.RecallResultCount = 0
				row.Assessments = nil
				row.FalseEmptyReview = "unknown"
				if targetedIndex == 0 {
					row.FalseEmptyReview = "confirmed"
				} else if targetedIndex < 15 {
					row.FalseEmptyReview = "rejected"
				}
			} else {
				utility := "orienting"
				if targetedIndex >= 50 {
					utility = "unused"
					if targetedIndex < 55 {
						utility = "duplicate"
					}
				}
				quality := "current"
				if targetedIndex < 22 {
					quality = "stale"
				}
				row.Assessments = []Assessment{{ResultKey: "target-result-" + row.RunID, Utility: utility, Quality: quality, Source: "evaluator"}}
			}
			targetedIndex++
		}
	}
	report, err := study.Report(rows)
	if err != nil {
		t.Fatal(err)
	}
	assertMetric := func(id string, point float64, numerator, denominator, unknown int) MetricEvidence {
		t.Helper()
		metric := metricByID(t, report.Gates, id)
		if math.Abs(metric.Point-point) > 0.000001 || metric.Numerator != numerator || metric.Denominator != denominator || metric.Unknown != unknown {
			t.Fatalf("metric %s = %+v, want point=%v numerator=%d denominator=%d unknown=%d", id, metric, point, numerator, denominator, unknown)
		}
		return metric
	}
	checkpointVector := assertMetric("checkpoint_rate_delta_pp", -10, 54, 60, 0)
	assertClose(t, checkpointVector.CILower, -18.333333333333332)
	assertClose(t, checkpointVector.CIUpper, -3.3333333333333335)
	stopVector := assertMetric("stop_growth_pp", 5, 3, 60, 0)
	assertClose(t, stopVector.CILower, 0)
	assertClose(t, stopVector.CIUpper, 11.666666666666666)
	assertMetric("automatic_injected_bytes_reduction_percent", 50, 30000, 60000, 0)
	startupVector := assertMetric("startup_compact_p95_reduction_percent", 40, 3600, 6000, 0)
	assertClose(t, startupVector.CILower, 40)
	assertClose(t, startupVector.CIUpper, 40)
	assertMetric("recall_p95_ms", 156, 156, 60, 0)
	utilityVector := assertMetric("utility_relative_improvement_percent", 50, 30, 40, 0)
	assertClose(t, utilityVector.CILower, 15.220483641536276)
	assertClose(t, utilityVector.CIUpper, 101.2987012987013)
	noise := assertMetric("noise_rate_percent", 25, 10, 40, 0)
	assertClose(t, noise.CILower, 14.187118639096303)
	assertClose(t, noise.CIUpper, 40.19396142076803)
	noiseImprovementVector := assertMetric("noise_improvement_pp", 25, 10, 40, 0)
	assertClose(t, noiseImprovementVector.CILower, 8.636363636363637)
	assertClose(t, noiseImprovementVector.CIUpper, 40.55555555555556)
	harm := assertMetric("harm_rate_percent", 5, 2, 40, 0)
	assertClose(t, harm.CILower, 1.3820667386148344)
	assertClose(t, harm.CIUpper, 16.503877369140962)
	harmDifferenceVector := assertMetric("harm_difference_pp", -5, 2, 40, 0)
	assertClose(t, harmDifferenceVector.CILower, -15)
	assertClose(t, harmDifferenceVector.CIUpper, 5.526315789473683)
	falseEmpty := assertMetric("false_empty_rate_percent", 100.0/15.0, 1, 15, 5)
	assertClose(t, falseEmpty.CILower, 1.1866895493268554)
	assertClose(t, falseEmpty.CIUpper, 29.816529873780027)
	coverage := assertMetric("explicit_label_coverage_percent", 100, 100, 100, 0)
	assertClose(t, coverage.CILower, 96.30065017930143)

	duplicate := treatmentMetricByID(t, report, "targeted-recall", "duplicate_rate_percent")
	if duplicate.Numerator != 5 || duplicate.Denominator != 40 || math.Abs(duplicate.Point-12.5) > 0.000001 {
		t.Fatalf("targeted duplicate metric = %+v", duplicate)
	}
	timeToUseful := treatmentMetricByID(t, report, "targeted-recall", "time_to_useful_p95_ms")
	if !timeToUseful.Available || timeToUseful.Point != 120 || timeToUseful.Denominator != 60 {
		t.Fatalf("targeted time-to-useful metric = %+v", timeToUseful)
	}

	failed := rows
	failed.Rows = append([]RunRow(nil), rows.Rows...)
	failedIndex := -1
	for index := range failed.Rows {
		if failed.Rows[index].Treatment == "targeted-recall" {
			failedIndex = index
			failed.Rows[index].Outcome = "operational_failure"
			failed.Rows[index].TaskOutcome = "not_applicable"
			failed.Rows[index].OmissionCode = "runner_process_failed"
			break
		}
	}
	if _, err := study.Report(failed); err == nil || !strings.Contains(err.Error(), "contains metric or quality evidence") {
		t.Fatalf("operational failure retained quality evidence: %v", err)
	}
	failed.Rows[failedIndex].RecallResultCount = 0
	failed.Rows[failedIndex].Assessments = nil
	failed.Rows[failedIndex].FalseEmptyReview = "not_applicable"
	failed.Rows[failedIndex].CheckpointSucceeded = false
	failed.Rows[failedIndex].StopConflictOrLoop = false
	failed.Rows[failedIndex].AutomaticInjectedUTF8Bytes = 0
	failed.Rows[failedIndex].StartupCompactLatencyMillis = 0
	failed.Rows[failedIndex].RecallLatencyMillis = 0
	failed.Rows[failedIndex].TimeToUsefulMillis = 0
	failedReport, err := study.Report(failed)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := metricByID(t, failedReport.Gates, "checkpoint_rate_delta_pp")
	if checkpoint.Denominator != 59 || checkpoint.Unknown != 1 {
		t.Fatalf("operational failure was not excluded and reported: %+v", checkpoint)
	}

	taskFailed := rows
	taskFailed.Rows = append([]RunRow(nil), rows.Rows...)
	taskFailed.Rows[failedIndex].TaskOutcome = "failed"
	taskFailed.Rows[failedIndex].TimeToUsefulMillis = 0
	taskFailedReport, err := study.Report(taskFailed)
	if err != nil {
		t.Fatal(err)
	}
	taskSuccess := treatmentMetricByID(t, taskFailedReport, "targeted-recall", "task_success_rate_percent")
	if taskSuccess.Denominator != 60 || taskSuccess.Numerator != 59 || taskSuccess.Unknown != 0 || taskFailedReport.Labels.TaskFailed != 1 {
		t.Fatalf("attempted task failure disappeared from quality evidence: metric=%+v labels=%+v", taskSuccess, taskFailedReport.Labels)
	}
}

func TestReportRejectsTreatmentContradictionsAndSupportsFrozenHeldOutShape(t *testing.T) {
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
	rows = completeRows(study, calibration, mustPlan(t, study, calibration))
	for index := range rows.Rows {
		if rows.Rows[index].Treatment == "broad-chronological" {
			rows.Rows[index].RecallLatencyMillis = 1
			if _, err := study.Report(rows); err == nil || !strings.Contains(err.Error(), "non-targeted") {
				t.Fatalf("broad Recall contradiction error = %v", err)
			}
			break
		}
	}
	heldOutRows := completeRows(study, heldOut, mustPlan(t, study, heldOut))
	fastStudy := *study
	fastStudy.Contract.Intervals.BootstrapResamples = 100
	if report, err := fastStudy.Report(heldOutRows); err != nil || report.CohortID != heldOut.CohortID {
		t.Fatalf("held-out Report() report=%+v error=%v", report, err)
	}
	calibrationRows := completeRows(study, calibration, mustPlan(t, study, calibration))
	combined := RowSet{SchemaVersion: RowSetSchemaVersion, StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		ContractSHA256: study.Hash, CohortID: CombinedCohortID, Rows: append(calibrationRows.Rows, heldOutRows.Rows...)}
	if report, err := fastStudy.Report(combined); err != nil || report.CohortID != CombinedCohortID || report.Labels.RunsObserved != 1551 || len(report.Treatments) != 34 {
		t.Fatalf("combined Report() report=%+v error=%v", report, err)
	}
}

func TestReportRetainsAnArmWithZeroSuccessfulTasks(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	rows := completeRows(study, calibration, mustPlan(t, study, calibration))
	failed := 0
	for index := range rows.Rows {
		if rows.Rows[index].Treatment == "no-recall" {
			rows.Rows[index].TaskOutcome = "failed"
			rows.Rows[index].TimeToUsefulMillis = 0
			failed++
		}
	}
	report, err := study.Report(rows)
	if err != nil {
		t.Fatalf("Report() rejected a complete zero-success arm: %v", err)
	}
	taskSuccess := treatmentMetricByID(t, report, "no-recall", "task_success_rate_percent")
	if taskSuccess.Point != 0 || taskSuccess.Numerator != 0 || taskSuccess.Denominator != failed || taskSuccess.Unknown != 0 {
		t.Fatalf("zero-success metric = %+v", taskSuccess)
	}
	timeToUseful := treatmentMetricByID(t, report, "no-recall", "time_to_useful_p95_ms")
	if timeToUseful.Available || timeToUseful.Point != 0 || timeToUseful.CILower != 0 || timeToUseful.CIUpper != 0 ||
		timeToUseful.Numerator != 0 || timeToUseful.Denominator != 0 || timeToUseful.Unknown != failed {
		t.Fatalf("all-unknown time-to-useful metric = %+v", timeToUseful)
	}
}

func TestReportRejectsUnfrozenFailureCodesAndResidualOperationalMetrics(t *testing.T) {
	t.Parallel()

	study, calibration, _ := verifiedStudy(t)
	base := completeRows(study, calibration, mustPlan(t, study, calibration))
	operationalIndex := -1
	for index := range base.Rows {
		if base.Rows[index].Treatment == "targeted-recall" {
			operationalIndex = index
			break
		}
	}
	operationalRows := func(outcome, code string) RowSet {
		rows := base
		rows.Rows = append([]RunRow(nil), base.Rows...)
		row := &rows.Rows[operationalIndex]
		row.Outcome = outcome
		row.TaskOutcome = "not_applicable"
		row.OmissionCode = code
		row.RecallResultCount = 0
		row.Assessments = nil
		row.FalseEmptyReview = "not_applicable"
		row.CheckpointSucceeded = false
		row.StopConflictOrLoop = false
		row.AutomaticInjectedUTF8Bytes = 0
		row.StartupCompactLatencyMillis = 0
		row.RecallLatencyMillis = 0
		row.TimeToUsefulMillis = 0
		return rows
	}
	tests := []struct {
		name   string
		rows   RowSet
		mutate func(*RunRow)
		want   string
	}{
		{name: "unknown operational code", rows: operationalRows("operational_failure", "arbitrary"), want: "outcome mapping"},
		{name: "unknown omission code", rows: operationalRows("omitted", "arbitrary"), want: "outcome mapping"},
		{name: "checkpoint evidence", rows: operationalRows("operational_failure", "runner_timeout"), mutate: func(row *RunRow) { row.CheckpointSucceeded = true }, want: "metric or quality evidence"},
		{name: "Stop evidence", rows: operationalRows("operational_failure", "runner_timeout"), mutate: func(row *RunRow) { row.StopConflictOrLoop = true }, want: "metric or quality evidence"},
		{name: "injected-byte evidence", rows: operationalRows("operational_failure", "runner_timeout"), mutate: func(row *RunRow) { row.AutomaticInjectedUTF8Bytes = 1 }, want: "metric or quality evidence"},
		{name: "startup evidence", rows: operationalRows("operational_failure", "runner_timeout"), mutate: func(row *RunRow) { row.StartupCompactLatencyMillis = 1 }, want: "metric or quality evidence"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.mutate != nil {
				test.mutate(&test.rows.Rows[operationalIndex])
			}
			if _, err := study.Report(test.rows); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Report() error = %v, want %q", err, test.want)
			}
		})
	}
	for _, valid := range []struct{ outcome, code string }{
		{"operational_failure", "runner_timeout"},
		{"operational_failure", "runner_process_failed"},
		{"operational_failure", "fixture_integrity_mismatch"},
		{"omitted", "task_not_attempted"},
	} {
		if _, err := study.Report(operationalRows(valid.outcome, valid.code)); err != nil {
			t.Fatalf("Report() rejected frozen mapping %s/%s: %v", valid.outcome, valid.code, err)
		}
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
			Outcome: "completed", TaskOutcome: "succeeded", FalseEmptyReview: "not_applicable", CheckpointSucceeded: true,
			AutomaticInjectedUTF8Bytes: 1000, StartupCompactLatencyMillis: 100, TimeToUsefulMillis: 200}
		if run.Treatment == "targeted-recall" {
			row.AutomaticInjectedUTF8Bytes = 500
			row.StartupCompactLatencyMillis = 60
			row.RecallLatencyMillis = 100
			row.TimeToUsefulMillis = 120
		}
		if run.Treatment != "no-recall" {
			row.RecallResultCount = 1
			row.Assessments = []Assessment{{ResultKey: "salted-" + run.RunID, Utility: "orienting", Quality: "current", Source: "evaluator"}}
		}
		rows.Rows = append(rows.Rows, row)
	}
	return rows
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

func treatmentMetricByID(t *testing.T, report Report, treatment, id string) TreatmentMetric {
	t.Helper()
	for _, metric := range report.Treatments {
		if metric.Treatment == treatment && metric.Metric == id {
			return metric
		}
	}
	t.Fatalf("treatment metric %s/%s not found", treatment, id)
	return TreatmentMetric{}
}

func assertClose(t *testing.T, actual, want float64) {
	t.Helper()
	if math.Abs(actual-want) > 0.000000001 {
		t.Fatalf("value = %.12f, want %.12f", actual, want)
	}
}
