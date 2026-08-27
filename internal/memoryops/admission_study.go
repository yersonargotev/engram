package memoryops

import (
	"fmt"
	"math"

	"github.com/yersonargotev/engram/internal/store"
)

type AdmissionStudyFreezeInput struct {
	Contract store.AdmissionStudyContract
}

type AdmissionStudyFreezeResult struct {
	Study         store.AdmissionStudy `json:"study"`
	AlreadyFrozen bool                 `json:"already_frozen"`
}

type AdmissionStudyOmissionInput struct {
	RunID      string
	ReviewerID string
	Category   string
	ReasonCode string
	Annotation string
}

type AdmissionStudyOmissionResult struct {
	Omission        store.AdmissionStudyOmission `json:"omission"`
	AlreadyRecorded bool                         `json:"already_recorded"`
}

type AdmissionStudyMetricsInput struct {
	StudyID      string
	StudyVersion string
}

type AdmissionStudyReviewListInput struct {
	StudyID      string
	StudyVersion string
	ReviewerID   string
}

type AdmissionStudyReviewListResult struct {
	StudyID      string                          `json:"study_id"`
	StudyVersion string                          `json:"study_version"`
	ReviewerID   string                          `json:"reviewer_id"`
	Runs         []store.AdmissionShadowRun      `json:"runs"`
	Proposals    []store.AdmissionShadowProposal `json:"proposals"`
}

type AdmissionStudyCounts struct {
	RunCount                         int `json:"run_count"`
	ProposalCount                    int `json:"proposal_count"`
	ReviewEventCount                 int `json:"review_event_count"`
	ReviewedProposalCount            int `json:"reviewed_proposal_count"`
	IndependentReviewedProposalCount int `json:"independent_reviewed_proposal_count"`
	OmissionCount                    int `json:"omission_count"`
}

type AdmissionStudyDistributions struct {
	ByCohort              map[string]int `json:"by_cohort"`
	ByPolicyVersion       map[string]int `json:"by_policy_version"`
	ByAdapter             map[string]int `json:"by_adapter"`
	ByProjectType         map[string]int `json:"by_project_type"`
	BySessionShape        map[string]int `json:"by_session_shape"`
	ByRecommendation      map[string]int `json:"by_recommendation"`
	ByHumanVerdict        map[string]int `json:"by_human_verdict"`
	ByCategory            map[string]int `json:"by_category"`
	ByReasonCode          map[string]int `json:"by_reason_code"`
	OmissionsByCategory   map[string]int `json:"omissions_by_category"`
	OmissionsByReasonCode map[string]int `json:"omissions_by_reason_code"`
}

type AdmissionStudyProportion struct {
	Numerator   int     `json:"numerator"`
	Denominator int     `json:"denominator"`
	Value       float64 `json:"value"`
}

type AdmissionStudyQuality struct {
	PromotionPrecision        AdmissionStudyProportion `json:"promotion_precision"`
	ReviewRate                AdmissionStudyProportion `json:"review_rate"`
	ReviewCoverage            AdmissionStudyProportion `json:"review_coverage"`
	InterReviewerAgreement    AdmissionStudyProportion `json:"inter_reviewer_agreement"`
	InterReviewerUnanimity    AdmissionStudyProportion `json:"inter_reviewer_unanimity"`
	PolicyAgreementCount      int                      `json:"policy_agreement_count"`
	PolicyDisagreementCount   int                      `json:"policy_disagreement_count"`
	ProtectedFalseRejectCount int                      `json:"protected_false_reject_count"`
	UnsupportedProposalCount  int                      `json:"unsupported_proposal_count"`
	PrivacyLeakCount          int                      `json:"privacy_leak_count"`
}

type AdmissionStudyConfidenceInterval struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

type AdmissionStudyUncertainty struct {
	ConfidenceLevel float64                                     `json:"confidence_level"`
	Method          string                                      `json:"method"`
	Proportions     map[string]AdmissionStudyConfidenceInterval `json:"proportions"`
}

type AdmissionStudyCohortSufficiency struct {
	RunCount                            int  `json:"run_count"`
	MinimumRuns                         int  `json:"minimum_runs"`
	ProposalCount                       int  `json:"proposal_count"`
	MinimumProposals                    int  `json:"minimum_proposals"`
	IndependentReviewedProposalCount    int  `json:"independent_reviewed_proposal_count"`
	MinimumIndependentReviewedProposals int  `json:"minimum_independent_reviewed_proposals"`
	Sufficient                          bool `json:"sufficient"`
}

type AdmissionStudySufficiency struct {
	Sufficient     bool                                          `json:"sufficient"`
	ByCohort       map[string]AdmissionStudyCohortSufficiency    `json:"by_cohort"`
	ByAdapter      map[string]AdmissionStudyDimensionSufficiency `json:"by_adapter"`
	ByProjectType  map[string]AdmissionStudyDimensionSufficiency `json:"by_project_type"`
	BySessionShape map[string]AdmissionStudyDimensionSufficiency `json:"by_session_shape"`
}

type AdmissionStudyDimensionSufficiency struct {
	RunCount    int  `json:"run_count"`
	MinimumRuns int  `json:"minimum_runs"`
	Sufficient  bool `json:"sufficient"`
}

type AdmissionStudyGates struct {
	SampleSufficient           bool `json:"sample_sufficient"`
	PromotionPrecisionPass     bool `json:"promotion_precision_pass"`
	ReviewRatePass             bool `json:"review_rate_pass"`
	ReviewCoveragePass         bool `json:"review_coverage_pass"`
	InterReviewerAgreementPass bool `json:"inter_reviewer_agreement_pass"`
	ProtectedFalseRejectsPass  bool `json:"protected_false_rejects_pass"`
	UnsupportedProposalsPass   bool `json:"unsupported_proposals_pass"`
	PrivacyLeaksPass           bool `json:"privacy_leaks_pass"`
	Go                         bool `json:"go"`
}

type AdmissionStudyMetricsResult struct {
	StudyID                   string                      `json:"study_id"`
	StudyVersion              string                      `json:"study_version"`
	ContractVersion           string                      `json:"contract_version"`
	ContractHash              string                      `json:"contract_hash"`
	MetricsVersion            string                      `json:"metrics_version"`
	AutomaticAdmissionEnabled bool                        `json:"automatic_admission_enabled"`
	Counts                    AdmissionStudyCounts        `json:"counts"`
	Distributions             AdmissionStudyDistributions `json:"distributions"`
	Quality                   AdmissionStudyQuality       `json:"quality"`
	Uncertainty               AdmissionStudyUncertainty   `json:"uncertainty"`
	Sufficiency               AdmissionStudySufficiency   `json:"sufficiency"`
	Gates                     AdmissionStudyGates         `json:"gates"`
}

type AdmissionStudyCleanupInput struct {
	StudyID      string
	StudyVersion string
}

func (s *Service) FreezeAdmissionStudy(input AdmissionStudyFreezeInput) (*AdmissionStudyFreezeResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	study, alreadyFrozen, err := s.store.FreezeAdmissionStudy(input.Contract)
	if err != nil {
		return nil, fmt.Errorf("freeze admission study: %w", err)
	}
	return &AdmissionStudyFreezeResult{Study: *study, AlreadyFrozen: alreadyFrozen}, nil
}

func (s *Service) RecordAdmissionStudyOmission(input AdmissionStudyOmissionInput) (*AdmissionStudyOmissionResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	omission, alreadyRecorded, err := s.store.AddAdmissionStudyOmission(store.AddAdmissionStudyOmissionParams{
		RunID: input.RunID, ReviewerID: input.ReviewerID, Category: input.Category,
		ReasonCode: input.ReasonCode, Annotation: input.Annotation,
	})
	if err != nil {
		return nil, fmt.Errorf("record admission study omission: %w", err)
	}
	return &AdmissionStudyOmissionResult{Omission: *omission, AlreadyRecorded: alreadyRecorded}, nil
}

func (s *Service) ListAdmissionStudyReviews(input AdmissionStudyReviewListInput) (*AdmissionStudyReviewListResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	reviewerID, err := store.NormalizeAdmissionStudyReviewerID(input.ReviewerID)
	if err != nil {
		return nil, err
	}
	study, err := s.store.GetAdmissionStudy(input.StudyID, input.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("get admission study: %w", err)
	}
	runs, err := s.store.ListAdmissionStudyRuns(study.Contract.StudyID, study.Contract.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("list admission study runs: %w", err)
	}
	proposals, err := s.store.ListAdmissionStudyProposals(study.Contract.StudyID, study.Contract.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("list admission study proposals: %w", err)
	}
	pending := make([]store.AdmissionShadowProposal, 0, len(proposals))
	pendingRunIDs := make(map[string]struct{})
	for _, proposal := range proposals {
		alreadyLabeled := false
		for _, review := range proposal.Reviews {
			if review.ReviewerID == reviewerID {
				alreadyLabeled = true
				break
			}
		}
		if alreadyLabeled {
			continue
		}
		proposal.Reviews = []store.AdmissionShadowReview{}
		pending = append(pending, proposal)
		pendingRunIDs[proposal.RunID] = struct{}{}
	}
	pendingRuns := make([]store.AdmissionShadowRun, 0, len(pendingRunIDs))
	for _, run := range runs {
		if _, pending := pendingRunIDs[run.ID]; pending {
			pendingRuns = append(pendingRuns, run)
		}
	}
	return &AdmissionStudyReviewListResult{
		StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		ReviewerID: reviewerID, Runs: pendingRuns, Proposals: pending,
	}, nil
}

func (s *Service) CleanupAdmissionStudy(input AdmissionStudyCleanupInput) (*store.AdmissionStudyCleanupResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	result, err := s.store.DeleteAdmissionStudy(input.StudyID, input.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("cleanup admission study: %w", err)
	}
	return result, nil
}

// AdmissionStudyMetrics derives only aggregate material. The result type has
// no row-level proposal, reviewer, Evidence reference, annotation, or session
// fields, so callers cannot accidentally serialize local review material.
func (s *Service) AdmissionStudyMetrics(input AdmissionStudyMetricsInput) (*AdmissionStudyMetricsResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	study, err := s.store.GetAdmissionStudy(input.StudyID, input.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("get admission study: %w", err)
	}
	runs, err := s.store.ListAdmissionStudyRuns(study.Contract.StudyID, study.Contract.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("list admission study runs: %w", err)
	}
	proposals, err := s.store.ListAdmissionStudyProposals(study.Contract.StudyID, study.Contract.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("list admission study proposals: %w", err)
	}
	omissions, err := s.store.ListAdmissionStudyOmissions(study.Contract.StudyID, study.Contract.StudyVersion)
	if err != nil {
		return nil, fmt.Errorf("list admission study omissions: %w", err)
	}

	result := &AdmissionStudyMetricsResult{
		StudyID: study.Contract.StudyID, StudyVersion: study.Contract.StudyVersion,
		ContractVersion: study.Contract.ContractVersion, ContractHash: study.ContractHash,
		MetricsVersion: study.Contract.MetricsVersion, AutomaticAdmissionEnabled: false,
		Counts:        AdmissionStudyCounts{RunCount: len(runs), ProposalCount: len(proposals), OmissionCount: len(omissions)},
		Distributions: newAdmissionStudyDistributions(),
		Sufficiency: AdmissionStudySufficiency{
			Sufficient: true, ByCohort: map[string]AdmissionStudyCohortSufficiency{},
			ByAdapter: map[string]AdmissionStudyDimensionSufficiency{}, ByProjectType: map[string]AdmissionStudyDimensionSufficiency{},
			BySessionShape: map[string]AdmissionStudyDimensionSufficiency{},
		},
	}
	runsByID := make(map[string]store.AdmissionShadowRun, len(runs))
	for _, run := range runs {
		if run.StudyContractHash != study.ContractHash {
			return nil, fmt.Errorf("%w: run %s is bound to a different contract hash", store.ErrAdmissionStudyContractChanged, run.ID)
		}
		runsByID[run.ID] = run
		cohort := result.Sufficiency.ByCohort[run.Cohort]
		cohort.RunCount++
		result.Sufficiency.ByCohort[run.Cohort] = cohort
		incrementAdmissionStudyDimension(result.Sufficiency.ByAdapter, run.Adapter)
		incrementAdmissionStudyDimension(result.Sufficiency.ByProjectType, run.ProjectType)
		incrementAdmissionStudyDimension(result.Sufficiency.BySessionShape, run.SessionShape)
	}

	for _, proposal := range proposals {
		run, ok := runsByID[proposal.RunID]
		if !ok {
			return nil, fmt.Errorf("admission study proposal references unknown run")
		}
		result.Distributions.ByCohort[run.Cohort]++
		result.Distributions.ByPolicyVersion[run.PolicyVersion]++
		result.Distributions.ByAdapter[run.Adapter]++
		result.Distributions.ByProjectType[run.ProjectType]++
		result.Distributions.BySessionShape[run.SessionShape]++
		result.Distributions.ByRecommendation[proposal.Recommendation]++
		result.Distributions.ByCategory[proposal.Category]++
		for _, reasonCode := range proposal.AssessmentReasonCodes {
			result.Distributions.ByReasonCode[reasonCode]++
		}
		cohort := result.Sufficiency.ByCohort[run.Cohort]
		cohort.ProposalCount++

		result.Counts.ReviewEventCount += len(proposal.Reviews)
		labels := latestAdmissionStudyReviewerLabels(proposal.Reviews)
		if len(labels) == 0 {
			result.Sufficiency.ByCohort[run.Cohort] = cohort
			continue
		}
		result.Counts.ReviewedProposalCount++
		primary := labels[0]
		result.Distributions.ByHumanVerdict[primary.Verdict]++
		if primary.Verdict == proposal.Recommendation {
			result.Quality.PolicyAgreementCount++
		} else {
			result.Quality.PolicyDisagreementCount++
		}
		if proposal.Recommendation == string(AdmissionAdmit) {
			result.Quality.PromotionPrecision.Denominator++
			if primary.Verdict == string(AdmissionAdmit) {
				result.Quality.PromotionPrecision.Numerator++
			}
		}
		if proposal.Protected && proposal.Recommendation == string(AdmissionReject) && primary.Verdict != string(AdmissionReject) {
			result.Quality.ProtectedFalseRejectCount++
		}
		unsupported, privacyLeak := false, false
		for _, label := range labels {
			unsupported = unsupported || label.Unsupported
			privacyLeak = privacyLeak || label.PrivacyLeak
		}
		if unsupported {
			result.Quality.UnsupportedProposalCount++
		}
		if privacyLeak {
			result.Quality.PrivacyLeakCount++
		}
		if run.IndependentReviewRequired && len(labels) >= 2 {
			result.Counts.IndependentReviewedProposalCount++
			cohort.IndependentReviewedProposalCount++
			result.Quality.InterReviewerUnanimity.Denominator++
			unanimous := true
			for first := 0; first < len(labels)-1; first++ {
				for second := first + 1; second < len(labels); second++ {
					result.Quality.InterReviewerAgreement.Denominator++
					if labels[first].Verdict == labels[second].Verdict {
						result.Quality.InterReviewerAgreement.Numerator++
					} else {
						unanimous = false
					}
				}
			}
			if unanimous {
				result.Quality.InterReviewerUnanimity.Numerator++
			}
		}
		result.Sufficiency.ByCohort[run.Cohort] = cohort
	}

	result.Quality.ReviewRate = AdmissionStudyProportion{Numerator: result.Distributions.ByRecommendation[string(AdmissionReview)], Denominator: len(proposals)}
	result.Quality.ReviewCoverage = AdmissionStudyProportion{Numerator: result.Counts.ReviewedProposalCount, Denominator: len(proposals)}
	setAdmissionStudyProportionValue(&result.Quality.PromotionPrecision)
	setAdmissionStudyProportionValue(&result.Quality.ReviewRate)
	setAdmissionStudyProportionValue(&result.Quality.ReviewCoverage)
	setAdmissionStudyProportionValue(&result.Quality.InterReviewerAgreement)
	setAdmissionStudyProportionValue(&result.Quality.InterReviewerUnanimity)

	for _, omission := range omissions {
		result.Distributions.OmissionsByCategory[omission.Category]++
		result.Distributions.OmissionsByReasonCode[omission.ReasonCode]++
	}
	for _, declared := range study.Contract.Cohorts {
		cohort := result.Sufficiency.ByCohort[declared.ID]
		cohort.MinimumRuns = declared.MinimumRuns
		cohort.MinimumProposals = declared.MinimumProposals
		cohort.MinimumIndependentReviewedProposals = declared.MinimumIndependentReviewedProposals
		cohort.Sufficient = cohort.RunCount >= cohort.MinimumRuns &&
			cohort.ProposalCount >= cohort.MinimumProposals &&
			cohort.IndependentReviewedProposalCount >= cohort.MinimumIndependentReviewedProposals
		if !cohort.Sufficient {
			result.Sufficiency.Sufficient = false
		}
		result.Sufficiency.ByCohort[declared.ID] = cohort
	}
	finalizeAdmissionStudyDimensions(&result.Sufficiency, study.Contract)
	result.Uncertainty = AdmissionStudyUncertainty{
		ConfidenceLevel: 0.95, Method: "wilson_score",
		Proportions: map[string]AdmissionStudyConfidenceInterval{
			"promotion_precision":      admissionStudyWilson95(result.Quality.PromotionPrecision),
			"review_rate":              admissionStudyWilson95(result.Quality.ReviewRate),
			"review_coverage":          admissionStudyWilson95(result.Quality.ReviewCoverage),
			"inter_reviewer_unanimity": admissionStudyWilson95(result.Quality.InterReviewerUnanimity),
		},
	}
	thresholds := study.Contract.Thresholds
	result.Gates = AdmissionStudyGates{
		SampleSufficient: result.Sufficiency.Sufficient,
		PromotionPrecisionPass: result.Quality.PromotionPrecision.Denominator > 0 &&
			result.Quality.PromotionPrecision.Value >= thresholds.MinimumPromotionPrecision,
		ReviewRatePass: result.Quality.ReviewRate.Denominator > 0 &&
			result.Quality.ReviewRate.Value <= thresholds.MaximumReviewRate,
		ReviewCoveragePass: result.Quality.ReviewCoverage.Denominator > 0 &&
			result.Quality.ReviewCoverage.Value >= thresholds.MinimumReviewCoverage,
		InterReviewerAgreementPass: result.Quality.InterReviewerAgreement.Denominator > 0 &&
			result.Quality.InterReviewerAgreement.Value >= thresholds.MinimumInterReviewerAgreement,
		ProtectedFalseRejectsPass: result.Quality.ProtectedFalseRejectCount <= thresholds.MaximumProtectedFalseRejects,
		UnsupportedProposalsPass:  result.Quality.UnsupportedProposalCount <= thresholds.MaximumUnsupportedProposals,
		PrivacyLeaksPass:          result.Quality.PrivacyLeakCount <= thresholds.MaximumPrivacyLeaks,
	}
	result.Gates.Go = result.Gates.SampleSufficient && result.Gates.PromotionPrecisionPass &&
		result.Gates.ReviewRatePass && result.Gates.ReviewCoveragePass && result.Gates.InterReviewerAgreementPass &&
		result.Gates.ProtectedFalseRejectsPass && result.Gates.UnsupportedProposalsPass && result.Gates.PrivacyLeaksPass
	return result, nil
}

func newAdmissionStudyDistributions() AdmissionStudyDistributions {
	return AdmissionStudyDistributions{
		ByCohort: map[string]int{}, ByPolicyVersion: map[string]int{}, ByAdapter: map[string]int{},
		ByProjectType: map[string]int{}, BySessionShape: map[string]int{}, ByRecommendation: map[string]int{},
		ByHumanVerdict: map[string]int{}, ByCategory: map[string]int{}, ByReasonCode: map[string]int{},
		OmissionsByCategory: map[string]int{}, OmissionsByReasonCode: map[string]int{},
	}
}

func incrementAdmissionStudyDimension(dimensions map[string]AdmissionStudyDimensionSufficiency, value string) {
	dimension := dimensions[value]
	dimension.RunCount++
	dimensions[value] = dimension
}

func finalizeAdmissionStudyDimensions(sufficiency *AdmissionStudySufficiency, contract store.AdmissionStudyContract) {
	for _, declared := range []struct {
		values []string
		result map[string]AdmissionStudyDimensionSufficiency
	}{
		{contract.Adapters, sufficiency.ByAdapter},
		{contract.ProjectTypes, sufficiency.ByProjectType},
		{contract.SessionShapes, sufficiency.BySessionShape},
	} {
		for _, value := range declared.values {
			dimension := declared.result[value]
			dimension.MinimumRuns = 1
			dimension.Sufficient = dimension.RunCount >= dimension.MinimumRuns
			if !dimension.Sufficient {
				sufficiency.Sufficient = false
			}
			declared.result[value] = dimension
		}
	}
}

func latestAdmissionStudyReviewerLabels(reviews []store.AdmissionShadowReview) []store.AdmissionShadowReview {
	order := make([]string, 0)
	latest := make(map[string]store.AdmissionShadowReview)
	for _, review := range reviews {
		if _, seen := latest[review.ReviewerID]; !seen {
			order = append(order, review.ReviewerID)
		}
		latest[review.ReviewerID] = review
	}
	result := make([]store.AdmissionShadowReview, 0, len(order))
	for _, reviewerID := range order {
		result = append(result, latest[reviewerID])
	}
	return result
}

func setAdmissionStudyProportionValue(proportion *AdmissionStudyProportion) {
	if proportion.Denominator == 0 {
		proportion.Value = 0
		return
	}
	proportion.Value = float64(proportion.Numerator) / float64(proportion.Denominator)
}

func admissionStudyWilson95(proportion AdmissionStudyProportion) AdmissionStudyConfidenceInterval {
	if proportion.Denominator == 0 {
		return AdmissionStudyConfidenceInterval{Lower: 0, Upper: 1}
	}
	n := float64(proportion.Denominator)
	p := float64(proportion.Numerator) / n
	z := 1.959963984540054
	zSquared := z * z
	denominator := 1 + zSquared/n
	center := (p + zSquared/(2*n)) / denominator
	margin := z * math.Sqrt((p*(1-p)+zSquared/(4*n))/n) / denominator
	return AdmissionStudyConfidenceInterval{Lower: math.Max(0, center-margin), Upper: math.Min(1, center+margin)}
}
