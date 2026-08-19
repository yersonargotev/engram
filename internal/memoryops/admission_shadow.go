package memoryops

import (
	"fmt"
	"strings"

	"github.com/yersonargotev/engram/internal/store"
)

type AdmissionShadowInput struct {
	Project   string
	SessionID string
}

type AdmissionShadowResult struct {
	Run         store.AdmissionShadowRun        `json:"run"`
	Proposals   []store.AdmissionShadowProposal `json:"proposals"`
	Acquisition *AdmissionAcquisition           `json:"acquisition"`
	Diagnostics []AdmissionDiagnostic           `json:"diagnostics"`
}

type AdmissionReviewListInput struct {
	Project string
}

type AdmissionReviewListResult struct {
	Project   string                          `json:"project"`
	Runs      []store.AdmissionShadowRun      `json:"runs"`
	Proposals []store.AdmissionShadowProposal `json:"proposals"`
}

type AdmissionReviewMarkInput struct {
	ProposalID  string
	Verdict     AdmissionRecommendation
	Note        string
	Unsupported bool
	PrivacyLeak bool
}

type AdmissionReviewMarkResult struct {
	Review          store.AdmissionShadowReview `json:"review"`
	AlreadyRecorded bool                        `json:"already_recorded"`
}

type AdmissionMetricsInput struct {
	Project string
}

type AdmissionMetricsResult struct {
	Project                       string         `json:"project"`
	RunCount                      int            `json:"run_count"`
	ProposalCount                 int            `json:"proposal_count"`
	ReviewCount                   int            `json:"review_count"`
	ReviewedProposalCount         int            `json:"reviewed_proposal_count"`
	PendingProposalCount          int            `json:"pending_proposal_count"`
	AgreementCount                int            `json:"agreement_count"`
	DisagreementCount             int            `json:"disagreement_count"`
	ProtectedFalseRejectCount     int            `json:"protected_false_reject_count"`
	UnsupportedCount              int            `json:"unsupported_count"`
	PrivacyLeakCount              int            `json:"privacy_leak_count"`
	ReasonCodedProposalCount      int            `json:"reason_coded_proposal_count"`
	AutomaticRejectGateBlocked    bool           `json:"automatic_reject_gate_blocked"`
	AutomaticPromotionGateBlocked bool           `json:"automatic_promotion_gate_blocked"`
	ByAdmissionVersion            map[string]int `json:"by_admission_version"`
	ByRecommendation              map[string]int `json:"by_recommendation"`
	ByCategory                    map[string]int `json:"by_category"`
	ByHumanVerdict                map[string]int `json:"by_human_verdict"`
	ByReasonCode                  map[string]int `json:"by_reason_code"`
}

// RunAdmissionShadow explicitly acquires one session's bounded evidence,
// evaluates it through the preview policy, and retains only derived snapshots.
func (s *Service) RunAdmissionShadow(input AdmissionShadowInput) (*AdmissionShadowResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	project, _ := store.NormalizeProject(input.Project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, ErrProjectRequired
	}
	sessionID := strings.TrimSpace(input.SessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("session id is required")
	}

	preview, err := s.PreviewAdmission(AdmissionPreviewInput{Project: project, SessionID: sessionID})
	if err != nil {
		return nil, err
	}
	if preview.Acquisition == nil {
		return nil, fmt.Errorf("shadow admission requires session acquisition metadata")
	}
	proposalInputs := make([]store.AdmissionShadowProposalInput, 0, len(preview.Proposals))
	for _, assessed := range preview.Proposals {
		proposalInputs = append(proposalInputs, store.AdmissionShadowProposalInput{
			Type:                  assessed.Proposal.Type,
			Title:                 assessed.Proposal.Title,
			Content:               assessed.Proposal.Content,
			Scope:                 assessed.Proposal.Scope,
			Category:              string(assessed.Proposal.Category),
			Protected:             assessed.Proposal.Protected,
			Recommendation:        string(assessed.Assessment.Recommendation),
			ProposalReasonCodes:   append([]string{}, assessed.Proposal.ReasonCodes...),
			AssessmentReasonCodes: append([]string{}, assessed.Assessment.ReasonCodes...),
			EvidenceRefs:          append([]string{}, assessed.Assessment.EvidenceRefs...),
		})
	}
	run, err := s.store.CreateAdmissionShadowRun(store.CreateAdmissionShadowRunParams{
		Project:              project,
		SessionID:            sessionID,
		Mode:                 preview.Acquisition.Mode,
		AdmissionVersion:     preview.Acquisition.EvidenceVersion,
		IncludedItems:        preview.Acquisition.IncludedItems,
		IncludedContentBytes: preview.Acquisition.IncludedContentBytes,
		Proposals:            proposalInputs,
	})
	if err != nil {
		return nil, fmt.Errorf("persist admission shadow run: %w", err)
	}
	proposals := run.Proposals
	run.Proposals = nil
	return &AdmissionShadowResult{
		Run:         *run,
		Proposals:   proposals,
		Acquisition: preview.Acquisition,
		Diagnostics: preview.Diagnostics,
	}, nil
}

// ListAdmissionReviews returns only retained proposals with no human correction.
func (s *Service) ListAdmissionReviews(input AdmissionReviewListInput) (*AdmissionReviewListResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	project, _ := store.NormalizeProject(input.Project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, ErrProjectRequired
	}
	proposals, err := s.store.ListAdmissionShadowProposals(project, true)
	if err != nil {
		return nil, fmt.Errorf("list admission review proposals: %w", err)
	}
	runs, err := s.store.ListAdmissionShadowRuns(project)
	if err != nil {
		return nil, fmt.Errorf("list admission review runs: %w", err)
	}
	pendingRunIDs := make(map[string]struct{}, len(proposals))
	for _, proposal := range proposals {
		pendingRunIDs[proposal.RunID] = struct{}{}
	}
	pendingRuns := make([]store.AdmissionShadowRun, 0, len(pendingRunIDs))
	for _, run := range runs {
		if _, pending := pendingRunIDs[run.ID]; pending {
			pendingRuns = append(pendingRuns, run)
		}
	}
	return &AdmissionReviewListResult{Project: project, Runs: pendingRuns, Proposals: proposals}, nil
}

// MarkAdmissionReview appends one explicit human correction. It never promotes
// or mutates the original proposal or assessment snapshot.
func (s *Service) MarkAdmissionReview(input AdmissionReviewMarkInput) (*AdmissionReviewMarkResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	proposalID := strings.TrimSpace(input.ProposalID)
	if proposalID == "" {
		return nil, fmt.Errorf("proposal id is required")
	}
	if !validAdmissionRecommendation(input.Verdict) {
		return nil, fmt.Errorf("invalid admission review verdict %q", input.Verdict)
	}
	review, alreadyRecorded, err := s.store.AddAdmissionShadowReview(store.AddAdmissionShadowReviewParams{
		ProposalID:  proposalID,
		Verdict:     string(input.Verdict),
		Note:        input.Note,
		Unsupported: input.Unsupported,
		PrivacyLeak: input.PrivacyLeak,
	})
	if err != nil {
		return nil, fmt.Errorf("record admission review: %w", err)
	}
	return &AdmissionReviewMarkResult{Review: *review, AlreadyRecorded: alreadyRecorded}, nil
}

// AdmissionMetrics derives project-local evaluation aggregates from immutable
// assessments and the latest append-only correction for each proposal.
func (s *Service) AdmissionMetrics(input AdmissionMetricsInput) (*AdmissionMetricsResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	project, _ := store.NormalizeProject(input.Project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, ErrProjectRequired
	}
	runs, err := s.store.ListAdmissionShadowRuns(project)
	if err != nil {
		return nil, fmt.Errorf("list admission shadow runs: %w", err)
	}
	proposals, err := s.store.ListAdmissionShadowProposals(project, false)
	if err != nil {
		return nil, fmt.Errorf("list admission shadow proposals: %w", err)
	}
	metrics := &AdmissionMetricsResult{
		Project:            project,
		RunCount:           len(runs),
		ProposalCount:      len(proposals),
		ByAdmissionVersion: map[string]int{},
		ByRecommendation:   map[string]int{},
		ByCategory:         map[string]int{},
		ByHumanVerdict:     map[string]int{},
		ByReasonCode:       map[string]int{},
	}
	versionsByRun := make(map[string]string, len(runs))
	for _, run := range runs {
		versionsByRun[run.ID] = run.AdmissionVersion
	}
	for _, proposal := range proposals {
		metrics.ByAdmissionVersion[versionsByRun[proposal.RunID]]++
		metrics.ByRecommendation[proposal.Recommendation]++
		metrics.ByCategory[proposal.Category]++
		if len(proposal.AssessmentReasonCodes) > 0 {
			metrics.ReasonCodedProposalCount++
		}
		for _, reasonCode := range proposal.AssessmentReasonCodes {
			metrics.ByReasonCode[reasonCode]++
		}
		metrics.ReviewCount += len(proposal.Reviews)
		if len(proposal.Reviews) == 0 {
			metrics.PendingProposalCount++
			continue
		}
		metrics.ReviewedProposalCount++
		latest := proposal.Reviews[len(proposal.Reviews)-1]
		metrics.ByHumanVerdict[latest.Verdict]++
		if latest.Verdict == proposal.Recommendation {
			metrics.AgreementCount++
		} else {
			metrics.DisagreementCount++
		}
		if proposal.Protected && proposal.Recommendation == string(AdmissionReject) && latest.Verdict != string(AdmissionReject) {
			metrics.ProtectedFalseRejectCount++
		}
		if latest.Unsupported {
			metrics.UnsupportedCount++
		}
		if latest.PrivacyLeak {
			metrics.PrivacyLeakCount++
		}
	}
	metrics.AutomaticRejectGateBlocked = metrics.ProtectedFalseRejectCount > 0
	metrics.AutomaticPromotionGateBlocked = metrics.UnsupportedCount > 0 || metrics.PrivacyLeakCount > 0
	return metrics, nil
}

func validAdmissionRecommendation(recommendation AdmissionRecommendation) bool {
	switch recommendation {
	case AdmissionAdmit, AdmissionReview, AdmissionReject:
		return true
	default:
		return false
	}
}
