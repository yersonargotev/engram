package memoryops

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

const (
	RecallUtilityDecisive  = store.RecallUtilityDecisive
	RecallUtilityOrienting = store.RecallUtilityOrienting
	RecallUtilityDuplicate = store.RecallUtilityDuplicate
	RecallUtilityUnused    = store.RecallUtilityUnused

	RecallQualityCurrent       = store.RecallQualityCurrent
	RecallQualityStale         = store.RecallQualityStale
	RecallQualityContradictory = store.RecallQualityContradictory
	RecallQualityUnknown       = store.RecallQualityUnknown

	RecallFeedbackSourceAgentExplicit = store.RecallFeedbackSourceAgentExplicit
	RecallFeedbackSourceUserExplicit  = store.RecallFeedbackSourceUserExplicit
	RecallFeedbackSourceEvaluator     = store.RecallFeedbackSourceEvaluator

	RecallFeedbackStatusRecorded        = "recorded"
	RecallFeedbackStatusAlreadyRecorded = "already_recorded"
	RecallFeedbackStatusFailed          = "failed"

	RecallFeedbackErrorCodeInvalid          = "invalid_recall_feedback"
	RecallFeedbackErrorCodeResultNotExposed = "recall_feedback_result_not_exposed"
	RecallFeedbackErrorCodeTurnMismatch     = "recall_feedback_turn_mismatch"
	RecallFeedbackErrorCodeConflict         = "recall_feedback_conflict"
	RecallFeedbackErrorCodeFailed           = "recall_feedback_failed"

	RecallFeedbackReportSchemaVersion = "recall-feedback-report-v1"
	RecallFeedbackOperationSearch     = "search"
	RecallFeedbackOperationGet        = "get"
)

type RecallFeedbackItemInput struct {
	ResultID string `json:"result_id"`
	Utility  string `json:"utility,omitempty"`
	Quality  string `json:"quality,omitempty"`
	Source   string `json:"source"`
}

func (input *RecallFeedbackItemInput) UnmarshalJSON(data []byte) error {
	type plain RecallFeedbackItemInput
	var decoded plain
	if err := decodeStrictRecallFeedbackJSON(data, &decoded); err != nil {
		return err
	}
	*input = RecallFeedbackItemInput(decoded)
	return nil
}

type RecallFeedbackInput struct {
	RecallID   string                    `json:"recall_id"`
	Results    []RecallFeedbackItemInput `json:"results,omitempty"`
	FalseEmpty *RecallFalseEmptyInput    `json:"false_empty,omitempty"`
}

func (input *RecallFeedbackInput) UnmarshalJSON(data []byte) error {
	type plain RecallFeedbackInput
	var decoded plain
	if err := decodeStrictRecallFeedbackJSON(data, &decoded); err != nil {
		return err
	}
	*input = RecallFeedbackInput(decoded)
	return nil
}

type RecallFalseEmptyInput struct {
	Value  bool   `json:"value"`
	Source string `json:"source"`
}

func (input *RecallFalseEmptyInput) UnmarshalJSON(data []byte) error {
	type plain RecallFalseEmptyInput
	var decoded plain
	if err := decodeStrictRecallFeedbackJSON(data, &decoded); err != nil {
		return err
	}
	*input = RecallFalseEmptyInput(decoded)
	return nil
}

type RecallFeedbackError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type RecallFeedbackResult struct {
	Status                      string               `json:"status"`
	LabelsRecorded              int                  `json:"labels_recorded,omitempty"`
	LabelsAlreadyRecorded       int                  `json:"labels_already_recorded,omitempty"`
	EmptyReviewsRecorded        int                  `json:"empty_reviews_recorded,omitempty"`
	EmptyReviewsAlreadyRecorded int                  `json:"empty_reviews_already_recorded,omitempty"`
	Error                       *RecallFeedbackError `json:"error,omitempty"`
}

type RecallFeedbackConfidenceInterval struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

type RecallFeedbackRate struct {
	Numerator    int                              `json:"numerator"`
	Denominator  int                              `json:"denominator"`
	Unknown      int                              `json:"unknown"`
	Rate         float64                          `json:"rate"`
	Confidence95 RecallFeedbackConfidenceInterval `json:"confidence_95"`
}

type RecallFeedbackDurationReport struct {
	Denominator     int   `json:"denominator"`
	Samples         int   `json:"samples"`
	Unknown         int   `json:"unknown"`
	P50Milliseconds int64 `json:"p50_ms"`
	P95Milliseconds int64 `json:"p95_ms"`
}

type RecallFeedbackSourceReport struct {
	Source       string                       `json:"source"`
	Utility      RecallFeedbackRate           `json:"utility"`
	Noise        RecallFeedbackRate           `json:"noise"`
	Harm         RecallFeedbackRate           `json:"harm"`
	Duplicate    RecallFeedbackRate           `json:"duplicate"`
	FalseEmpty   RecallFeedbackRate           `json:"false_empty"`
	TimeToUseful RecallFeedbackDurationReport `json:"time_to_useful"`
}

type RecallFeedbackOperationReport struct {
	Operation              string `json:"operation"`
	Events                 int    `json:"events"`
	LatencySamples         int    `json:"latency_samples"`
	UnknownLatency         int    `json:"unknown_latency"`
	P50LatencyMilliseconds int64  `json:"p50_latency_ms"`
	P95LatencyMilliseconds int64  `json:"p95_latency_ms"`
	VolumeSamples          int    `json:"volume_samples"`
	UnknownVolume          int    `json:"unknown_volume"`
	TotalExposedResults    int    `json:"total_exposed_results"`
	TotalUTF8Bytes         int64  `json:"total_utf8_bytes"`
}

type RecallFeedbackReport struct {
	SchemaVersion  string                          `json:"schema_version"`
	GeneratedAt    string                          `json:"generated_at"`
	ExposedResults int                             `json:"exposed_results"`
	EmptyRuns      int                             `json:"empty_runs"`
	LabelCoverage  RecallFeedbackRate              `json:"label_coverage"`
	Sources        []RecallFeedbackSourceReport    `json:"sources"`
	Operations     []RecallFeedbackOperationReport `json:"operations"`
}

func (s *Service) recordOptionalRecallFeedback(identity store.CheckpointIdentity, input *RecallFeedbackInput) *RecallFeedbackResult {
	if input == nil {
		return nil
	}
	labels := make([]store.RecallFeedbackLabelInput, 0, len(input.Results))
	for _, result := range input.Results {
		labels = append(labels, store.RecallFeedbackLabelInput{
			ResultID: result.ResultID, Utility: result.Utility, Quality: result.Quality, Source: result.Source,
		})
	}
	recorded, err := s.store.RecordRecallFeedback(store.RecordRecallFeedbackParams{
		Identity: identity, RecallID: input.RecallID, Results: labels,
		FalseEmpty: recallFalseEmptyValue(input.FalseEmpty), FalseEmptySource: recallFalseEmptySource(input.FalseEmpty),
	})
	if err != nil {
		return &RecallFeedbackResult{
			Status: RecallFeedbackStatusFailed,
			Error:  &RecallFeedbackError{Code: RecallFeedbackErrorCode(err), Message: err.Error()},
		}
	}
	status := RecallFeedbackStatusRecorded
	if recorded.LabelsRecorded == 0 && recorded.EmptyReviewsRecorded == 0 {
		status = RecallFeedbackStatusAlreadyRecorded
	}
	return &RecallFeedbackResult{
		Status: status, LabelsRecorded: recorded.LabelsRecorded,
		LabelsAlreadyRecorded:       recorded.LabelsAlreadyRecorded,
		EmptyReviewsRecorded:        recorded.EmptyReviewsRecorded,
		EmptyReviewsAlreadyRecorded: recorded.EmptyReviewsAlreadyRecorded,
	}
}

// RecordRecallFeedback records the optional checkpoint sidecar without
// changing or revalidating the immutable terminal checkpoint result.
func (s *Service) RecordRecallFeedback(identity store.CheckpointIdentity, input *RecallFeedbackInput) *RecallFeedbackResult {
	return s.recordOptionalRecallFeedback(identity, input)
}

func InvalidRecallFeedbackResult(err error) *RecallFeedbackResult {
	return &RecallFeedbackResult{
		Status: RecallFeedbackStatusFailed,
		Error:  &RecallFeedbackError{Code: RecallFeedbackErrorCodeInvalid, Message: err.Error()},
	}
}

func decodeStrictRecallFeedbackJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple Recall feedback values")
		}
		return err
	}
	return nil
}

func recallFalseEmptyValue(input *RecallFalseEmptyInput) *bool {
	if input == nil {
		return nil
	}
	value := input.Value
	return &value
}

func recallFalseEmptySource(input *RecallFalseEmptyInput) string {
	if input == nil {
		return ""
	}
	return input.Source
}

func (s *Service) RecallFeedbackReport() (*RecallFeedbackReport, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	snapshot, err := s.store.RecallFeedbackReportSnapshot()
	if err != nil {
		return nil, err
	}
	report := &RecallFeedbackReport{
		SchemaVersion: RecallFeedbackReportSchemaVersion,
		GeneratedAt:   s.now().UTC().Format(time.RFC3339Nano),
	}
	exposedMemories := make(map[string]struct{})
	for _, exposure := range snapshot.Exposures {
		exposedMemories[exposure.TurnKey+":"+exposure.MemoryKey] = struct{}{}
	}
	report.ExposedResults = len(exposedMemories)
	for _, run := range snapshot.Runs {
		if run.ResultCount == 0 {
			report.EmptyRuns++
		}
	}
	labelledExposures := make(map[string]struct{})
	for _, label := range snapshot.Labels {
		labelledExposures[label.TurnKey+":"+label.MemoryKey] = struct{}{}
	}
	report.LabelCoverage = recallFeedbackRate(len(labelledExposures), report.ExposedResults, report.ExposedResults-len(labelledExposures))
	for _, source := range []string{
		RecallFeedbackSourceAgentExplicit, RecallFeedbackSourceUserExplicit, RecallFeedbackSourceEvaluator,
	} {
		report.Sources = append(report.Sources, recallFeedbackSourceAggregate(snapshot, source, report.ExposedResults, report.EmptyRuns))
	}
	report.Operations = recallFeedbackOperationAggregates(snapshot.Operations)
	return report, nil
}

func recallFeedbackSourceAggregate(snapshot *store.RecallFeedbackReportSnapshot, source string, exposedResults, emptyRuns int) RecallFeedbackSourceReport {
	report := RecallFeedbackSourceReport{Source: source}
	utilityDenominator, useful, noise, duplicates := 0, 0, 0, 0
	qualityDenominator, harmful := 0, 0
	utilityLabelled := make(map[string]struct{})
	qualityLabelled := make(map[string]struct{})
	usefulMemories := make(map[string]struct{})
	for _, label := range snapshot.Labels {
		if label.Source != source {
			continue
		}
		key := label.TurnKey + ":" + label.MemoryKey
		if label.Utility != "" {
			utilityDenominator++
			utilityLabelled[key] = struct{}{}
			switch label.Utility {
			case RecallUtilityDecisive, RecallUtilityOrienting:
				useful++
				usefulMemories[label.TurnKey+":"+label.MemoryKey] = struct{}{}
			case RecallUtilityDuplicate, RecallUtilityUnused:
				noise++
			}
			if label.Utility == RecallUtilityDuplicate {
				duplicates++
			}
		}
		if label.Quality != "" {
			qualityDenominator++
			qualityLabelled[key] = struct{}{}
			if label.Quality == RecallQualityStale || label.Quality == RecallQualityContradictory {
				harmful++
			}
		}
	}
	report.Utility = recallFeedbackRate(useful, utilityDenominator, exposedResults-len(utilityLabelled))
	report.Noise = recallFeedbackRate(noise, utilityDenominator, exposedResults-len(utilityLabelled))
	report.Duplicate = recallFeedbackRate(duplicates, utilityDenominator, exposedResults-len(utilityLabelled))
	report.Harm = recallFeedbackRate(harmful, qualityDenominator, exposedResults-len(qualityLabelled))

	falseEmptyDenominator, falseEmpty := 0, 0
	for _, review := range snapshot.EmptyReviews {
		if review.Source != source {
			continue
		}
		falseEmptyDenominator++
		if review.Value {
			falseEmpty++
		}
	}
	report.FalseEmpty = recallFeedbackRate(falseEmpty, falseEmptyDenominator, emptyRuns-falseEmptyDenominator)

	turns := make(map[string]struct{})
	firstRecallStarted := make(map[string]int64)
	timelineComplete := make(map[string]bool)
	runsByKey := make(map[string]store.RecallFeedbackRunMetric)
	for _, run := range snapshot.Runs {
		turns[run.TurnKey] = struct{}{}
		runsByKey[run.RunKey] = run
		if _, initialized := timelineComplete[run.TurnKey]; !initialized {
			timelineComplete[run.TurnKey] = true
		}
		if run.StartedAtUnixNano == nil || run.CompletedAtUnixNano == nil {
			timelineComplete[run.TurnKey] = false
			continue
		}
		if current, exists := firstRecallStarted[run.TurnKey]; !exists || *run.StartedAtUnixNano < current {
			firstRecallStarted[run.TurnKey] = *run.StartedAtUnixNano
		}
	}
	firstUsefulCompleted := make(map[string]int64)
	for _, exposure := range snapshot.Exposures {
		if _, useful := usefulMemories[exposure.TurnKey+":"+exposure.MemoryKey]; !useful {
			continue
		}
		run, exists := runsByKey[exposure.RunKey]
		if !exists || run.CompletedAtUnixNano == nil {
			timelineComplete[exposure.TurnKey] = false
			continue
		}
		if current, exists := firstUsefulCompleted[exposure.TurnKey]; !exists || *run.CompletedAtUnixNano < current {
			firstUsefulCompleted[exposure.TurnKey] = *run.CompletedAtUnixNano
		}
	}
	usefulByTurn := make(map[string]int64)
	for turnKey := range turns {
		started, hasStart := firstRecallStarted[turnKey]
		completed, hasUseful := firstUsefulCompleted[turnKey]
		if !timelineComplete[turnKey] || !hasStart || !hasUseful || completed < started {
			continue
		}
		usefulByTurn[turnKey] = (completed - started) / int64(time.Millisecond)
	}
	usefulDurations := make([]int64, 0, len(usefulByTurn))
	for _, duration := range usefulByTurn {
		usefulDurations = append(usefulDurations, duration)
	}
	report.TimeToUseful = RecallFeedbackDurationReport{
		Denominator: len(turns), Samples: len(usefulDurations), Unknown: len(turns) - len(usefulDurations),
		P50Milliseconds: percentileInt64(usefulDurations, 0.50), P95Milliseconds: percentileInt64(usefulDurations, 0.95),
	}
	return report
}

func recallFeedbackOperationAggregates(metrics []store.RecallFeedbackOperationalMetric) []RecallFeedbackOperationReport {
	grouped := make(map[string][]store.RecallFeedbackOperationalMetric)
	for _, metric := range metrics {
		grouped[metric.Operation] = append(grouped[metric.Operation], metric)
	}
	operations := make([]string, 0, len(grouped))
	for operation := range grouped {
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	result := make([]RecallFeedbackOperationReport, 0, len(operations))
	for _, operation := range operations {
		report := RecallFeedbackOperationReport{Operation: operation, Events: len(grouped[operation])}
		var latencies []int64
		for _, metric := range grouped[operation] {
			report.TotalExposedResults += metric.ExposedResults
			if metric.ElapsedMonotonicMS == nil {
				report.UnknownLatency++
			} else {
				latencies = append(latencies, *metric.ElapsedMonotonicMS)
			}
			if metric.DeliveredUTF8Bytes == nil {
				report.UnknownVolume++
			} else {
				report.VolumeSamples++
				report.TotalUTF8Bytes += *metric.DeliveredUTF8Bytes
			}
		}
		report.LatencySamples = len(latencies)
		report.P50LatencyMilliseconds = percentileInt64(latencies, 0.50)
		report.P95LatencyMilliseconds = percentileInt64(latencies, 0.95)
		result = append(result, report)
	}
	return result
}

func recallFeedbackRate(numerator, denominator, unknown int) RecallFeedbackRate {
	metric := RecallFeedbackRate{Numerator: numerator, Denominator: denominator, Unknown: max(unknown, 0)}
	if denominator == 0 {
		return metric
	}
	metric.Rate = float64(numerator) / float64(denominator)
	metric.Confidence95 = wilsonRecallFeedback95(numerator, denominator)
	return metric
}

func wilsonRecallFeedback95(numerator, denominator int) RecallFeedbackConfidenceInterval {
	if denominator == 0 {
		return RecallFeedbackConfidenceInterval{}
	}
	const z = 1.959963984540054
	n := float64(denominator)
	p := float64(numerator) / n
	denom := 1 + z*z/n
	center := (p + z*z/(2*n)) / denom
	margin := z * math.Sqrt((p*(1-p)+z*z/(4*n))/n) / denom
	return RecallFeedbackConfidenceInterval{Lower: max(0, center-margin), Upper: min(1, center+margin)}
}

func percentileInt64(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func RecallFeedbackErrorCode(err error) string {
	switch {
	case errors.Is(err, store.ErrRecallFeedbackInvalid):
		return RecallFeedbackErrorCodeInvalid
	case errors.Is(err, store.ErrRecallFeedbackResultNotExposed):
		return RecallFeedbackErrorCodeResultNotExposed
	case errors.Is(err, store.ErrRecallFeedbackTurnMismatch):
		return RecallFeedbackErrorCodeTurnMismatch
	case errors.Is(err, store.ErrRecallFeedbackConflict):
		return RecallFeedbackErrorCodeConflict
	default:
		return RecallFeedbackErrorCodeFailed
	}
}
