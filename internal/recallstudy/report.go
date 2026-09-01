package recallstudy

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	RowSetSchemaVersion     = "recall-study-rows-v1"
	ReportSchemaVersion     = "recall-study-report-v1"
	GateReportSchemaVersion = "recall-study-gates-v1"
	CombinedCohortID        = "combined-v1"
	maxRowSetBytes          = 64 << 20
)

type RowSet struct {
	SchemaVersion  string   `json:"schema_version"`
	StudyID        string   `json:"study_id"`
	StudyVersion   string   `json:"study_version"`
	ContractSHA256 string   `json:"contract_sha256"`
	CohortID       string   `json:"cohort_id"`
	Rows           []RunRow `json:"rows"`
}

type RunRow struct {
	RunID                       string       `json:"run_id"`
	SamplingUnitID              string       `json:"sampling_unit_id"`
	TaskClass                   string       `json:"task_class"`
	Treatment                   string       `json:"treatment"`
	Outcome                     string       `json:"outcome"`
	TaskOutcome                 string       `json:"task_outcome"`
	OmissionCode                string       `json:"omission_code,omitempty"`
	RecallResultCount           int          `json:"recall_result_count"`
	FalseEmptyReview            string       `json:"false_empty_review"`
	Assessments                 []Assessment `json:"assessments,omitempty"`
	CheckpointSucceeded         bool         `json:"checkpoint_succeeded"`
	StopConflictOrLoop          bool         `json:"stop_conflict_or_loop"`
	AutomaticInjectedUTF8Bytes  int64        `json:"automatic_injected_utf8_bytes"`
	StartupCompactLatencyMillis float64      `json:"startup_compact_latency_ms"`
	RecallLatencyMillis         float64      `json:"recall_latency_ms"`
	TimeToUsefulMillis          float64      `json:"time_to_useful_ms"`
}

type Assessment struct {
	ResultKey string `json:"result_key"`
	Utility   string `json:"utility"`
	Quality   string `json:"quality"`
	Source    string `json:"source"`
}

type MetricEvidence struct {
	Metric      string  `json:"metric"`
	Point       float64 `json:"point"`
	CILower     float64 `json:"ci_lower"`
	CIUpper     float64 `json:"ci_upper"`
	Numerator   int     `json:"numerator"`
	Denominator int     `json:"denominator"`
	Unknown     int     `json:"unknown"`
}

type TreatmentMetric struct {
	Treatment   string  `json:"treatment"`
	Metric      string  `json:"metric"`
	Point       float64 `json:"point"`
	CILower     float64 `json:"ci_lower"`
	CIUpper     float64 `json:"ci_upper"`
	Numerator   int     `json:"numerator"`
	Denominator int     `json:"denominator"`
	Unknown     int     `json:"unknown"`
}

type ClauseResult struct {
	Metric     string  `json:"metric"`
	Statistic  string  `json:"statistic"`
	Observed   float64 `json:"observed"`
	Comparator string  `json:"comparator"`
	Threshold  float64 `json:"threshold"`
	Passed     bool    `json:"passed"`
}

type GateResult struct {
	ID      string         `json:"id"`
	Passed  bool           `json:"passed"`
	Clauses []ClauseResult `json:"clauses"`
}

type GateReport struct {
	SchemaVersion string           `json:"schema_version"`
	AllPassed     bool             `json:"all_passed"`
	Evidence      []MetricEvidence `json:"evidence"`
	Gates         []GateResult     `json:"gates"`
}

type TreatmentAggregate struct {
	Treatment           string         `json:"treatment"`
	Runs                int            `json:"runs"`
	OperationalFailures int            `json:"operational_failures"`
	Omissions           int            `json:"omissions"`
	TaskSucceeded       int            `json:"task_succeeded"`
	TaskFailed          int            `json:"task_failed"`
	ExposedMemories     int            `json:"exposed_memories"`
	UnknownAssessments  int            `json:"unknown_assessments"`
	Utility             map[string]int `json:"utility"`
	Quality             map[string]int `json:"quality"`
	LabelSources        map[string]int `json:"label_sources"`
}

type LabelAggregate struct {
	RunsObserved        int                  `json:"runs_observed"`
	OperationalFailures int                  `json:"operational_failures"`
	Omissions           int                  `json:"omissions"`
	TaskSucceeded       int                  `json:"task_succeeded"`
	TaskFailed          int                  `json:"task_failed"`
	ExposedMemories     int                  `json:"exposed_memories"`
	ExplicitAssessments int                  `json:"explicit_assessments"`
	UnknownAssessments  int                  `json:"unknown_assessments"`
	Disagreements       int                  `json:"disagreements"`
	FalseEmptyConfirmed int                  `json:"false_empty_confirmed"`
	FalseEmptyRejected  int                  `json:"false_empty_rejected"`
	FalseEmptyUnknown   int                  `json:"false_empty_unknown"`
	Treatments          []TreatmentAggregate `json:"treatments"`
}

type Report struct {
	SchemaVersion  string            `json:"schema_version"`
	StudyID        string            `json:"study_id"`
	StudyVersion   string            `json:"study_version"`
	ContractSHA256 string            `json:"contract_sha256"`
	CohortID       string            `json:"cohort_id"`
	Labels         LabelAggregate    `json:"labels"`
	Treatments     []TreatmentMetric `json:"treatment_metrics"`
	Gates          GateReport        `json:"gates"`
	SharedOutput   string            `json:"shared_output"`
	RolloutEnabled bool              `json:"rollout_enabled"`
}

func ReadRowSet(path string) (RowSet, error) {
	var rows RowSet
	if err := readStrictJSON(path, maxRowSetBytes, &rows); err != nil {
		return RowSet{}, fmt.Errorf("read Recall study rows: %w", err)
	}
	return rows, nil
}

func (study *Study) Report(rows RowSet) (Report, error) {
	labels, err := study.aggregateRows(rows)
	if err != nil {
		return Report{}, err
	}
	evidence, err := study.deriveMetrics(rows.Rows)
	if err != nil {
		return Report{}, err
	}
	gates, err := study.evaluateGates(evidence)
	if err != nil {
		return Report{}, err
	}
	treatments, err := study.deriveTreatmentMetrics(rows.Rows)
	if err != nil {
		return Report{}, err
	}
	return Report{
		SchemaVersion: ReportSchemaVersion, StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		ContractSHA256: study.Hash, CohortID: rows.CohortID, Labels: labels, Treatments: treatments, Gates: gates,
		SharedOutput: "aggregate-only", RolloutEnabled: false,
	}, nil
}

func (study *Study) aggregateRows(rows RowSet) (LabelAggregate, error) {
	if study == nil || rows.SchemaVersion != RowSetSchemaVersion || rows.StudyID != study.Contract.StudyID ||
		rows.StudyVersion != study.Contract.StudyVersion || rows.ContractSHA256 != study.Hash {
		return LabelAggregate{}, fmt.Errorf("Recall study row-set identity does not match the frozen contract")
	}
	plan, err := study.planForRows(rows.CohortID)
	if err != nil {
		return LabelAggregate{}, err
	}
	if len(rows.Rows) != len(plan) {
		return LabelAggregate{}, fmt.Errorf("Recall study row set must be complete: got %d rows, want %d", len(rows.Rows), len(plan))
	}
	want := make(map[string]PlannedRun, len(plan))
	for _, run := range plan {
		want[run.RunID] = run
	}
	seen := make(map[string]bool, len(rows.Rows))
	byTreatment := make(map[string]*TreatmentAggregate, len(study.Contract.Treatments))
	for _, treatment := range study.Contract.Treatments {
		byTreatment[treatment.ID] = &TreatmentAggregate{Treatment: treatment.ID, Utility: make(map[string]int), Quality: make(map[string]int), LabelSources: make(map[string]int)}
	}
	aggregate := LabelAggregate{RunsObserved: len(rows.Rows)}
	for _, row := range rows.Rows {
		planned, ok := want[row.RunID]
		if !ok || seen[row.RunID] || row.SamplingUnitID != planned.SamplingUnitID || row.TaskClass != planned.TaskClass || row.Treatment != planned.Treatment {
			return LabelAggregate{}, fmt.Errorf("Recall study row is duplicate or does not match the frozen plan")
		}
		seen[row.RunID] = true
		if err := validateRunRow(row); err != nil {
			return LabelAggregate{}, err
		}
		treatment := byTreatment[row.Treatment]
		treatment.Runs++
		switch row.Outcome {
		case "operational_failure":
			aggregate.OperationalFailures++
			treatment.OperationalFailures++
			continue
		case "omitted":
			aggregate.Omissions++
			treatment.Omissions++
			continue
		}
		switch row.TaskOutcome {
		case "succeeded":
			aggregate.TaskSucceeded++
			treatment.TaskSucceeded++
		case "failed":
			aggregate.TaskFailed++
			treatment.TaskFailed++
		}
		aggregate.ExposedMemories += row.RecallResultCount
		treatment.ExposedMemories += row.RecallResultCount
		switch row.FalseEmptyReview {
		case "confirmed":
			aggregate.FalseEmptyConfirmed++
		case "rejected":
			aggregate.FalseEmptyRejected++
		case "unknown":
			aggregate.FalseEmptyUnknown++
		}
		assessed := make(map[string]bool)
		resultLabels := make(map[string]Assessment)
		for _, assessment := range row.Assessments {
			treatment.Utility[assessment.Utility]++
			treatment.Quality[assessment.Quality]++
			treatment.LabelSources[assessment.Source]++
			assessed[assessment.ResultKey] = true
			if previous, ok := resultLabels[assessment.ResultKey]; ok && (previous.Utility != assessment.Utility || previous.Quality != assessment.Quality) {
				aggregate.Disagreements++
			}
			resultLabels[assessment.ResultKey] = assessment
		}
		aggregate.ExplicitAssessments += len(assessed)
		unknown := row.RecallResultCount - len(assessed)
		aggregate.UnknownAssessments += unknown
		treatment.UnknownAssessments += unknown
	}
	for _, treatment := range study.Contract.Treatments {
		aggregate.Treatments = append(aggregate.Treatments, *byTreatment[treatment.ID])
	}
	return aggregate, nil
}

func validateRunRow(row RunRow) error {
	if row.Outcome != "completed" && row.Outcome != "operational_failure" && row.Outcome != "omitted" {
		return fmt.Errorf("Recall study row outcome is invalid")
	}
	if row.Outcome == "completed" && row.OmissionCode != "" || row.Outcome != "completed" && strings.TrimSpace(row.OmissionCode) == "" {
		return fmt.Errorf("Recall study row omission metadata is invalid")
	}
	if row.Outcome == "completed" && row.TaskOutcome != "succeeded" && row.TaskOutcome != "failed" ||
		row.Outcome != "completed" && row.TaskOutcome != "not_applicable" {
		return fmt.Errorf("Recall study row task outcome is invalid")
	}
	if row.RecallResultCount < 0 {
		return fmt.Errorf("Recall study row result count is invalid")
	}
	if row.AutomaticInjectedUTF8Bytes < 0 || !finite(row.StartupCompactLatencyMillis) || row.StartupCompactLatencyMillis < 0 ||
		!finite(row.RecallLatencyMillis) || row.RecallLatencyMillis < 0 || !finite(row.TimeToUsefulMillis) || row.TimeToUsefulMillis < 0 {
		return fmt.Errorf("Recall study row measurements are invalid")
	}
	if row.Outcome == "completed" && row.TaskOutcome == "succeeded" && row.TimeToUsefulMillis <= 0 {
		return fmt.Errorf("Recall study completed row is missing time-to-useful evidence")
	}
	if row.Outcome == "completed" && row.TaskOutcome == "failed" && row.TimeToUsefulMillis != 0 {
		return fmt.Errorf("Recall study failed task cannot claim time-to-useful evidence")
	}
	if row.Outcome != "completed" && (row.RecallResultCount != 0 || len(row.Assessments) != 0 || row.FalseEmptyReview != "not_applicable" ||
		row.RecallLatencyMillis != 0 || row.TimeToUsefulMillis != 0) {
		return fmt.Errorf("Recall study non-completed row contains quality evidence")
	}
	if row.Outcome != "completed" {
		return nil
	}
	if row.Treatment == "targeted-recall" && row.Outcome == "completed" && row.RecallLatencyMillis <= 0 {
		return fmt.Errorf("Recall study targeted-Recall row is missing Recall latency")
	}
	if row.Treatment != "targeted-recall" && row.RecallLatencyMillis != 0 {
		return fmt.Errorf("Recall study non-targeted row contradicts the frozen Recall policy")
	}
	if row.Treatment == "no-recall" {
		if row.RecallResultCount != 0 || len(row.Assessments) != 0 || row.FalseEmptyReview != "not_applicable" || row.RecallLatencyMillis != 0 {
			return fmt.Errorf("Recall study no-Recall row contradicts its treatment")
		}
	} else if row.RecallResultCount == 0 && row.Treatment == "targeted-recall" {
		if row.FalseEmptyReview != "confirmed" && row.FalseEmptyReview != "rejected" && row.FalseEmptyReview != "unknown" {
			return fmt.Errorf("Recall study false-empty review is invalid")
		}
	} else if row.FalseEmptyReview != "not_applicable" {
		return fmt.Errorf("Recall study false-empty review is invalid")
	}
	utility := valuesSet([]string{"decisive", "orienting", "duplicate", "unused", "unknown"})
	quality := valuesSet([]string{"current", "stale", "contradictory", "unknown"})
	sources := valuesSet([]string{"agent_explicit", "user_explicit", "evaluator", "unknown"})
	seen := make(map[string]bool)
	results := make(map[string]bool)
	for _, assessment := range row.Assessments {
		if strings.TrimSpace(assessment.ResultKey) == "" || !utility[assessment.Utility] {
			return fmt.Errorf("Recall study assessment utility is invalid")
		}
		if !quality[assessment.Quality] {
			return fmt.Errorf("Recall study assessment quality is invalid")
		}
		if !sources[assessment.Source] {
			return fmt.Errorf("Recall study assessment label source is invalid")
		}
		key := assessment.ResultKey + "\x00" + assessment.Source
		if seen[key] {
			return fmt.Errorf("Recall study assessment attribution is duplicated")
		}
		seen[key] = true
		results[assessment.ResultKey] = true
	}
	if len(results) > row.RecallResultCount {
		return fmt.Errorf("Recall study assessments exceed exposed Memories")
	}
	return nil
}

func (study *Study) planForRows(cohortID string) ([]PlannedRun, error) {
	switch cohortID {
	case study.Contract.Cohorts.Calibration.ID:
		return plannedFromContract(study, study.Contract.Cohorts.Calibration), nil
	case study.Contract.Cohorts.HeldOut.ID:
		return plannedFromContract(study, study.Contract.Cohorts.HeldOut), nil
	case CombinedCohortID:
		calibration := plannedFromContract(study, study.Contract.Cohorts.Calibration)
		heldOut := plannedFromContract(study, study.Contract.Cohorts.HeldOut)
		return append(calibration, heldOut...), nil
	default:
		return nil, fmt.Errorf("Recall study row set names an unknown cohort")
	}
}

func plannedFromContract(study *Study, cohort CohortContract) []PlannedRun {
	classes := make([]string, 0, len(study.Contract.TaskClasses))
	for _, class := range study.Contract.TaskClasses {
		classes = append(classes, class.ID)
	}
	manifest := Manifest{
		StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion, CohortID: cohort.ID,
		Namespace: cohort.Namespace, FirstSamplingUnit: cohort.FirstSamplingUnit, SamplingUnits: cohort.SamplingUnits,
		TaskClassCycle: classes, SelectionSeed: cohort.SelectionSeed, Hash: cohort.ManifestSHA256,
	}
	plan, _ := study.Plan(&manifest)
	return plan
}

func (study *Study) evaluateGates(evidence []MetricEvidence) (GateReport, error) {
	if study == nil {
		return GateReport{}, fmt.Errorf("Recall study gate evaluation requires a contract")
	}
	want := make(map[string]bool)
	for _, gate := range study.Contract.Gates {
		for _, clause := range gate.Clauses {
			want[clause.Metric] = true
		}
	}
	byMetric := make(map[string]MetricEvidence, len(evidence))
	for _, metric := range evidence {
		if !want[metric.Metric] || byMetric[metric.Metric].Metric != "" {
			return GateReport{}, fmt.Errorf("Recall study gate evidence contains an unknown or duplicate metric %q", metric.Metric)
		}
		if !finiteMetric(metric) || metric.Denominator < 0 || metric.Numerator < 0 || metric.Unknown < 0 || metric.CILower > metric.CIUpper {
			return GateReport{}, fmt.Errorf("Recall study gate evidence for %q is invalid", metric.Metric)
		}
		byMetric[metric.Metric] = metric
	}
	for metric := range want {
		if byMetric[metric].Metric == "" {
			return GateReport{}, fmt.Errorf("Recall study gate evidence is missing %q", metric)
		}
	}
	orderedEvidence := append([]MetricEvidence(nil), evidence...)
	sort.Slice(orderedEvidence, func(i, j int) bool { return orderedEvidence[i].Metric < orderedEvidence[j].Metric })
	report := GateReport{SchemaVersion: GateReportSchemaVersion, AllPassed: true, Evidence: orderedEvidence}
	for _, gate := range study.Contract.Gates {
		result := GateResult{ID: gate.ID, Passed: true}
		for _, clause := range gate.Clauses {
			metric := byMetric[clause.Metric]
			observed := metric.Point
			switch clause.Statistic {
			case "ci_lower":
				observed = metric.CILower
			case "ci_upper":
				observed = metric.CIUpper
			case "point":
			default:
				return GateReport{}, fmt.Errorf("Recall study gate uses unsupported statistic %q", clause.Statistic)
			}
			passed := compare(observed, clause.Comparator, clause.Threshold)
			result.Clauses = append(result.Clauses, ClauseResult{Metric: clause.Metric, Statistic: clause.Statistic, Observed: observed, Comparator: clause.Comparator, Threshold: clause.Threshold, Passed: passed})
			result.Passed = result.Passed && passed
		}
		report.Gates = append(report.Gates, result)
		report.AllPassed = report.AllPassed && result.Passed
	}
	return report, nil
}

func compare(observed float64, comparator string, threshold float64) bool {
	switch comparator {
	case "gt":
		return observed > threshold
	case "gte":
		return observed >= threshold
	case "lt":
		return observed < threshold
	case "lte":
		return observed <= threshold
	default:
		return false
	}
}

func finiteMetric(metric MetricEvidence) bool {
	return !math.IsNaN(metric.Point) && !math.IsInf(metric.Point, 0) && !math.IsNaN(metric.CILower) && !math.IsInf(metric.CILower, 0) && !math.IsNaN(metric.CIUpper) && !math.IsInf(metric.CIUpper, 0)
}

func valuesSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}
