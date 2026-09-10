package recallquery

import (
	"fmt"
	"math"
	"strings"

	"github.com/yersonargotev/engram/internal/recallstudy"
)

type Report struct {
	StudyID                   string  `json:"study_id"`
	Version                   string  `json:"version"`
	ContractSHA256            string  `json:"contract_sha256"`
	CoreSourceRevision        string  `json:"core_source_revision"`
	Protocol                  int     `json:"protocol"`
	CalibrationPassed         bool    `json:"calibration_passed"`
	HeldOutOpened             bool    `json:"held_out_opened"`
	Disposition               string  `json:"disposition"`
	Groups                    []Group `json:"groups"`
	LabelOmission             string  `json:"label_omission"`
	ContentSelections         int     `json:"content_selections"`
	TaskTimeToUsefulAvailable bool    `json:"task_time_to_useful_available"`
}

type Group struct {
	Cohort                  string         `json:"cohort"`
	Variant                 string         `json:"variant"`
	QueryClass              string         `json:"query_class"`
	Attempted               int            `json:"attempted"`
	OperationalFailures     int            `json:"operational_failures"`
	FailureCodes            map[string]int `json:"failure_codes"`
	SuccessfulCallExposures int            `json:"successful_call_exposures"`
	First                   Summary        `json:"first_search"`
	Cumulative              Summary        `json:"post_reformulation"`
	Labels                  Labels         `json:"labels"`
}

type Labels struct {
	ExplicitAssessments int `json:"explicit_assessments"`
	UnknownUtility      int `json:"unknown_utility"`
	UnknownQuality      int `json:"unknown_quality"`
	OmittedAssessments  int `json:"omitted_assessments"`
	Disagreements       int `json:"disagreements"`
}

type Summary struct {
	EvidenceAvailable           bool                       `json:"evidence_available"`
	SearchCalls                 int                        `json:"search_calls"`
	Completed                   int                        `json:"completed_needs"`
	Empty                       int                        `json:"empty"`
	NonemptyMissingUseful       int                        `json:"nonempty_missing_useful"`
	Acceptable                  int                        `json:"acceptable_sets"`
	RelevantCurrent             int                        `json:"relevant_current_retrieved"`
	RelevantCurrentDenominator  int                        `json:"relevant_current_denominator"`
	Coverage                    float64                    `json:"mean_relevant_current_coverage"`
	ReciprocalRank              float64                    `json:"mean_reciprocal_rank"`
	Exposures                   int                        `json:"exposures"`
	NoiseExposures              int                        `json:"noise_exposures"`
	StaleExposures              int                        `json:"stale_exposures"`
	DuplicateExposures          int                        `json:"duplicate_exposures"`
	CandidateBytes              int                        `json:"candidate_bytes"`
	RecallLatencyMillis         float64                    `json:"total_recall_latency_ms"`
	DiscoveryLatencyMillis      *float64                   `json:"mean_candidate_discovery_latency_ms"`
	DiscoveryLatencyDenominator int                        `json:"candidate_discovery_latency_denominator"`
	AcceptableEvidence          recallstudy.MetricEvidence `json:"acceptable_set_evidence"`
}

type outcome struct {
	keys      []string
	bytes     int
	latency   float64
	discovery *float64
	calls     int
}

func assess(keys []string, bytes int, latency float64, discovery *float64) outcome {
	return outcome{keys: append([]string(nil), keys...), bytes: bytes, latency: latency, discovery: discovery}
}

func relevant(key string) bool { return key == "invariant" || key == "resolution" }

func (s *Summary) add(o outcome) {
	s.Completed++
	s.SearchCalls += o.calls
	s.RelevantCurrentDenominator += 2
	s.Exposures += len(o.keys)
	s.CandidateBytes += o.bytes
	s.RecallLatencyMillis += o.latency
	seen := map[string]bool{}
	groups := map[string]bool{}
	firstRelevant := 0
	uniqueRank := 0
	hits := 0
	for _, key := range o.keys {
		group := key
		if strings.HasPrefix(key, "history") {
			group = "history"
		}
		if groups[group] {
			s.DuplicateExposures++
		}
		groups[group] = true
		if !relevant(key) {
			s.NoiseExposures++
		}
		if key == "diagnosis" {
			s.StaleExposures++
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		uniqueRank++
		if relevant(key) {
			hits++
			if firstRelevant == 0 {
				firstRelevant = uniqueRank
			}
		}
	}
	s.RelevantCurrent += hits
	if seen["resolution"] {
		s.Acceptable++
	}
	if len(o.keys) == 0 {
		s.Empty++
	} else if hits == 0 {
		s.NonemptyMissingUseful++
	}
	if firstRelevant > 0 {
		s.ReciprocalRank += 1 / float64(firstRelevant)
	}
	if o.discovery != nil {
		if s.DiscoveryLatencyMillis == nil {
			s.DiscoveryLatencyMillis = new(float64)
		}
		*s.DiscoveryLatencyMillis += *o.discovery
		s.DiscoveryLatencyDenominator++
	}
}

func (s *Summary) finish() {
	s.EvidenceAvailable = s.Completed > 0
	if s.Completed > 0 {
		s.Coverage = float64(s.RelevantCurrent) / float64(s.RelevantCurrentDenominator)
		s.ReciprocalRank /= float64(s.Completed)
	}
	if s.DiscoveryLatencyDenominator > 0 {
		*s.DiscoveryLatencyMillis /= float64(s.DiscoveryLatencyDenominator)
	}
	low, high := wilson(s.Acceptable, s.Completed)
	point := 0.0
	if s.Completed > 0 {
		point = 100 * float64(s.Acceptable) / float64(s.Completed)
	}
	s.AcceptableEvidence = recallstudy.MetricEvidence{Metric: "acceptable_set_percent", Point: point, CILower: low, CIUpper: high, Numerator: s.Acceptable, Denominator: s.Completed}
}

func wilson(successes, total int) (float64, float64) {
	if total == 0 {
		return 0, 0
	}
	n := float64(total)
	p := float64(successes) / n
	z := 1.959963984540054
	center := (p + z*z/(2*n)) / (1 + z*z/n)
	radius := z * math.Sqrt(p*(1-p)/n+z*z/(4*n*n)) / (1 + z*z/n)
	return math.Max(0, 100*(center-radius)), math.Min(100, 100*(center+radius))
}

// Assess chooses the preregistered disposition from completed held-out groups.
func (r Report) Assess() string {
	groups := r.Groups
	if !groupsPassed(groups) {
		return "evaluation_incomplete"
	}
	byKey := map[string]Group{}
	for _, g := range groups {
		byKey[g.Variant+"/"+g.QueryClass] = g
	}
	correction := false
	gap := false
	for _, class := range []string{"identifier", "concept", "task"} {
		base := byKey["baseline/"+class]
		for _, variant := range variants[1:] {
			other := byKey[variant+"/"+class]
			for _, pair := range [][2]Summary{{base.First, other.First}, {base.Cumulative, other.Cumulative}} {
				if pair[1].Acceptable > pair[0].Acceptable || pair[1].Coverage > pair[0].Coverage || pair[1].StaleExposures < pair[0].StaleExposures {
					correction = true
				}
			}
		}
		if class != "identifier" {
			id := byKey["baseline/identifier"]
			if base.First.Acceptable < id.First.Acceptable || base.First.Coverage < id.First.Coverage || base.Cumulative.Acceptable < id.Cumulative.Acceptable || base.Cumulative.Coverage < id.Cumulative.Coverage {
				gap = true
			}
		}
	}
	if correction {
		return "editorial_lifecycle_correction_indicated"
	}
	if gap {
		return "ranking_investigation_requires_separate_approval"
	}
	return "no_ranking_change_indicated"
}

// SearchEvidence is synthetic retrieval evidence, never an explicit utility label.
type SearchEvidence struct {
	Keys           []string
	CandidateBytes int
	LatencyMillis  float64
}

// AnalyzeSearches reports one initial search and exactly one reformulation.
func analyzeSearches(calls []SearchEvidence) (outcome, outcome, error) {
	var first, cumulative outcome
	if len(calls) != 2 {
		return first, cumulative, fmt.Errorf("exactly one initial search and one reformulation required")
	}
	keys := []string{}
	bytes := 0
	latency := 0.0
	var discovery *float64
	for i, call := range calls {
		if len(call.Keys) > 5 || call.CandidateBytes < 2 || call.CandidateBytes > 4096 || call.LatencyMillis < 0 || math.IsNaN(call.LatencyMillis) || math.IsInf(call.LatencyMillis, 0) {
			return first, cumulative, fmt.Errorf("invalid bounded search evidence")
		}
		keys = append(keys, call.Keys...)
		bytes += call.CandidateBytes
		latency += call.LatencyMillis
		for _, key := range call.Keys {
			if key != "invariant" && key != "resolution" && key != "diagnosis" && key != "history1" && key != "history2" && key != "history3" && key != "distractor1" && key != "distractor2" {
				return first, cumulative, fmt.Errorf("unknown or ineligible fixture key")
			}
			if discovery == nil && relevant(key) {
				value := latency
				discovery = &value
			}
		}
		o := assess(keys, bytes, latency, discovery)
		o.calls = i + 1
		if i == 0 {
			first = o
		} else {
			cumulative = o
		}
	}
	return first, cumulative, nil
}

func AnalyzeSearches(calls []SearchEvidence) (Summary, Summary, error) {
	var first, cumulative Summary
	a, b, err := analyzeSearches(calls)
	if err != nil {
		return first, cumulative, err
	}
	first.add(a)
	first.finish()
	cumulative.add(b)
	cumulative.finish()
	return first, cumulative, nil
}
