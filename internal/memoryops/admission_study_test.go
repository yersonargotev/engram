package memoryops

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestAdmissionStudyFlowFreezesAttributesReviewsAndOmits(t *testing.T) {
	service := newTestService(t)
	contract := testAdmissionStudyContract()
	frozen, err := service.FreezeAdmissionStudy(AdmissionStudyFreezeInput{Contract: contract})
	if err != nil || frozen.AlreadyFrozen || frozen.Study.ContractHash == "" {
		t.Fatalf("freeze admission study = %#v err=%v", frozen, err)
	}
	seedShadowSession(t, service.store, "study-session", "engram")

	metadata := testAdmissionStudyRunMetadata("calibration")
	metadata.Adapter = "unknown"
	if _, err := service.RunAdmissionShadow(AdmissionShadowInput{
		Project: "engram", SessionID: "study-session", Study: &metadata,
	}); err == nil {
		t.Fatal("unknown frozen study metadata unexpectedly succeeded")
	}
	if runs, err := service.store.ListAdmissionShadowRuns("engram"); err != nil || len(runs) != 0 {
		t.Fatalf("invalid study run persisted = %#v err=%v", runs, err)
	}

	metadata = testAdmissionStudyRunMetadata("calibration")
	shadow, err := service.RunAdmissionShadow(AdmissionShadowInput{
		Project: "engram", SessionID: "study-session", Study: &metadata,
	})
	if err != nil {
		t.Fatalf("run attributed admission shadow: %v", err)
	}
	if shadow.Run.StudyID != contract.StudyID || shadow.Run.Cohort != "calibration" || shadow.Run.Adapter != "codex" {
		t.Fatalf("attributed run = %#v", shadow.Run)
	}
	repeated, err := service.RunAdmissionShadow(AdmissionShadowInput{
		Project: "engram", SessionID: "study-session", Study: &metadata,
	})
	if err != nil || repeated.AlreadyRecorded == nil || !*repeated.AlreadyRecorded || repeated.Run.ID != shadow.Run.ID {
		t.Fatalf("idempotent attributed run = %#v err=%v", repeated, err)
	}
	changedCohort := testAdmissionStudyRunMetadata("held-out")
	if _, err := service.RunAdmissionShadow(AdmissionShadowInput{
		Project: "engram", SessionID: "study-session", Study: &changedCohort,
	}); err == nil {
		t.Fatal("same session reattributed to another cohort unexpectedly succeeded")
	}
	proposalID := shadow.Proposals[0].ID
	queueA, err := service.ListAdmissionStudyReviews(AdmissionStudyReviewListInput{
		StudyID: contract.StudyID, StudyVersion: contract.StudyVersion, ReviewerID: "reviewer-a",
	})
	if err != nil || len(queueA.Proposals) != 2 || len(queueA.Runs) != 1 {
		t.Fatalf("reviewer A queue = %#v err=%v", queueA, err)
	}
	if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
		ProposalID: proposalID, Verdict: AdmissionAdmit,
	}); err == nil {
		t.Fatal("attributed review without reviewer unexpectedly succeeded")
	}
	first, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
		ProposalID: proposalID, ReviewerID: "reviewer-a", Verdict: AdmissionAdmit,
	})
	if err != nil || first.Review.ReviewerID != "reviewer-a" {
		t.Fatalf("first attributed review = %#v err=%v", first, err)
	}
	queueA, err = service.ListAdmissionStudyReviews(AdmissionStudyReviewListInput{
		StudyID: contract.StudyID, StudyVersion: contract.StudyVersion, ReviewerID: "reviewer-a",
	})
	if err != nil || len(queueA.Proposals) != 1 {
		t.Fatalf("reviewer A queue after label = %#v err=%v", queueA, err)
	}
	queueB, err := service.ListAdmissionStudyReviews(AdmissionStudyReviewListInput{
		StudyID: contract.StudyID, StudyVersion: contract.StudyVersion, ReviewerID: "reviewer-b",
	})
	if err != nil || len(queueB.Proposals) != 2 {
		t.Fatalf("reviewer B independent queue = %#v err=%v", queueB, err)
	}
	second, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
		ProposalID: proposalID, ReviewerID: "reviewer-b", Verdict: AdmissionReject,
	})
	if err != nil || second.Review.ReviewerID != "reviewer-b" || second.Review.ID == first.Review.ID {
		t.Fatalf("independent attributed review = %#v err=%v", second, err)
	}
	omission, err := service.RecordAdmissionStudyOmission(AdmissionStudyOmissionInput{
		RunID: shadow.Run.ID, ReviewerID: "reviewer-a", Category: "invariant",
		ReasonCode: "not_proposed", Annotation: "Missed invariant.",
	})
	if err != nil || omission.Omission.Category != "invariant" {
		t.Fatalf("record omission = %#v err=%v", omission, err)
	}
}

func TestAdmissionStudyMetricsAreAggregateSufficientAndUncertain(t *testing.T) {
	service := newTestService(t)
	contract := testAdmissionStudyContract()
	if _, err := service.FreezeAdmissionStudy(AdmissionStudyFreezeInput{Contract: contract}); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}

	for _, cohort := range []string{"calibration", "held-out"} {
		sessionID := "study-" + cohort
		seedShadowSession(t, service.store, sessionID, "engram")
		metadata := testAdmissionStudyRunMetadata(cohort)
		shadow, err := service.RunAdmissionShadow(AdmissionShadowInput{
			Project: "engram", SessionID: sessionID, Study: &metadata,
		})
		if err != nil {
			t.Fatalf("run %s cohort: %v", cohort, err)
		}
		for index, proposal := range shadow.Proposals {
			verdict := AdmissionAdmit
			if index == 1 {
				verdict = AdmissionReject
			}
			for _, reviewerID := range []string{"reviewer-a", "reviewer-b"} {
				if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
					ProposalID: proposal.ID, ReviewerID: reviewerID, Verdict: verdict,
				}); err != nil {
					t.Fatalf("review %s proposal %d by %s: %v", cohort, index, reviewerID, err)
				}
			}
		}
		if _, err := service.RecordAdmissionStudyOmission(AdmissionStudyOmissionInput{
			RunID: shadow.Run.ID, ReviewerID: "reviewer-a", Category: "decision",
			ReasonCode: "not_proposed", Annotation: "Missed decision.",
		}); err != nil {
			t.Fatalf("record %s omission: %v", cohort, err)
		}
	}

	metrics, err := service.AdmissionStudyMetrics(AdmissionStudyMetricsInput{
		StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
	})
	if err != nil {
		t.Fatalf("admission study metrics: %v", err)
	}
	if metrics.StudyID != contract.StudyID || metrics.MetricsVersion != contract.MetricsVersion || metrics.ContractHash == "" {
		t.Fatalf("study identity = %#v", metrics)
	}
	if metrics.Counts.RunCount != 2 || metrics.Counts.ProposalCount != 4 ||
		metrics.Counts.ReviewEventCount != 8 || metrics.Counts.ReviewedProposalCount != 4 ||
		metrics.Counts.IndependentReviewedProposalCount != 4 || metrics.Counts.OmissionCount != 2 {
		t.Fatalf("study counts = %#v", metrics.Counts)
	}
	if metrics.Distributions.ByCohort["calibration"] != 2 || metrics.Distributions.ByCohort["held-out"] != 2 ||
		metrics.Distributions.ByAdapter["codex"] != 2 || metrics.Distributions.ByAdapter["claude-code"] != 2 ||
		metrics.Distributions.ByHumanVerdict["admit"] != 2 ||
		metrics.Distributions.ByHumanVerdict["reject"] != 2 || metrics.Distributions.OmissionsByCategory["decision"] != 2 {
		t.Fatalf("study distributions = %#v", metrics.Distributions)
	}
	if metrics.Quality.PromotionPrecision.Numerator != 2 || metrics.Quality.PromotionPrecision.Denominator != 2 ||
		metrics.Quality.PromotionPrecision.Value != 1 || metrics.Quality.ReviewRate.Numerator != 2 ||
		metrics.Quality.ReviewRate.Denominator != 4 || metrics.Quality.InterReviewerAgreement.Value != 1 {
		t.Fatalf("study quality = %#v", metrics.Quality)
	}
	if !metrics.Sufficiency.Sufficient || !metrics.Sufficiency.ByCohort["calibration"].Sufficient ||
		!metrics.Sufficiency.ByCohort["held-out"].Sufficient || !metrics.Sufficiency.ByAdapter["codex"].Sufficient ||
		!metrics.Sufficiency.ByAdapter["claude-code"].Sufficient || !metrics.Sufficiency.ByProjectType["cli"].Sufficient ||
		!metrics.Sufficiency.ByProjectType["library"].Sufficient || !metrics.Sufficiency.BySessionShape["feature"].Sufficient ||
		!metrics.Sufficiency.BySessionShape["bugfix"].Sufficient || !metrics.Gates.ReviewCoveragePass || !metrics.Gates.Go {
		t.Fatalf("study sufficiency/gates = %#v %#v", metrics.Sufficiency, metrics.Gates)
	}
	for name, interval := range metrics.Uncertainty.Proportions {
		if interval.Lower < 0 || interval.Upper > 1 || interval.Lower > interval.Upper {
			t.Fatalf("invalid %s uncertainty = %#v", name, interval)
		}
	}
	encoded, err := json.Marshal(metrics)
	if err != nil {
		t.Fatalf("marshal aggregate metrics: %v", err)
	}
	for _, forbidden := range []string{"reviewer-a", "reviewer-b", "Missed decision.", "Explicit shadow requests remain authoritative.", "evidence_refs", "proposal_id", "run_id"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("aggregate metrics leaked row material %q: %s", forbidden, encoded)
		}
	}
}

func TestAdmissionStudyAgreementIncludesEveryReviewerPair(t *testing.T) {
	service := newTestService(t)
	contract := testAdmissionStudyContract()
	if _, err := service.FreezeAdmissionStudy(AdmissionStudyFreezeInput{Contract: contract}); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}
	seedShadowSession(t, service.store, "study-calibration", "engram")
	metadata := testAdmissionStudyRunMetadata("calibration")
	shadow, err := service.RunAdmissionShadow(AdmissionShadowInput{
		Project: "engram", SessionID: "study-calibration", Study: &metadata,
	})
	if err != nil {
		t.Fatalf("run admission study shadow: %v", err)
	}
	for reviewerID, verdict := range map[string]AdmissionRecommendation{
		"reviewer-a": AdmissionAdmit, "reviewer-b": AdmissionAdmit, "reviewer-c": AdmissionReject,
	} {
		if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
			ProposalID: shadow.Proposals[0].ID, ReviewerID: reviewerID, Verdict: verdict,
		}); err != nil {
			t.Fatalf("review by %s: %v", reviewerID, err)
		}
	}
	metrics, err := service.AdmissionStudyMetrics(AdmissionStudyMetricsInput{
		StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
	})
	if err != nil {
		t.Fatalf("admission study metrics: %v", err)
	}
	if metrics.Quality.InterReviewerAgreement.Numerator != 1 || metrics.Quality.InterReviewerAgreement.Denominator != 3 {
		t.Fatalf("inter-reviewer agreement = %#v, want 1/3", metrics.Quality.InterReviewerAgreement)
	}
	if metrics.Quality.InterReviewerUnanimity.Numerator != 0 || metrics.Quality.InterReviewerUnanimity.Denominator != 1 {
		t.Fatalf("inter-reviewer unanimity = %#v, want 0/1", metrics.Quality.InterReviewerUnanimity)
	}
	if _, exists := metrics.Uncertainty.Proportions["inter_reviewer_agreement"]; exists {
		t.Fatal("correlated pairwise agreement unexpectedly has a Wilson interval")
	}
	if interval, exists := metrics.Uncertainty.Proportions["inter_reviewer_unanimity"]; !exists || interval.Lower < 0 || interval.Upper > 1 {
		t.Fatalf("proposal-level unanimity interval = %#v exists=%t", interval, exists)
	}
}

func TestAdmissionStudyMetricsRequireCompleteReviewCoverage(t *testing.T) {
	service := newTestService(t)
	contract := testAdmissionStudyContract()
	if _, err := service.FreezeAdmissionStudy(AdmissionStudyFreezeInput{Contract: contract}); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}

	for _, cohort := range []string{"calibration", "held-out"} {
		sessionID := "incomplete-" + cohort
		seedShadowSession(t, service.store, sessionID, "engram")
		metadata := testAdmissionStudyRunMetadata(cohort)
		shadow, err := service.RunAdmissionShadow(AdmissionShadowInput{
			Project: "engram", SessionID: sessionID, Study: &metadata,
		})
		if err != nil {
			t.Fatalf("run %s cohort: %v", cohort, err)
		}
		for _, reviewerID := range []string{"reviewer-a", "reviewer-b"} {
			if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
				ProposalID: shadow.Proposals[0].ID, ReviewerID: reviewerID, Verdict: AdmissionAdmit,
			}); err != nil {
				t.Fatalf("review %s proposal by %s: %v", cohort, reviewerID, err)
			}
		}
	}

	metrics, err := service.AdmissionStudyMetrics(AdmissionStudyMetricsInput{
		StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
	})
	if err != nil {
		t.Fatalf("admission study metrics: %v", err)
	}
	if !metrics.Sufficiency.Sufficient {
		t.Fatalf("sampling should be sufficient: %#v", metrics.Sufficiency)
	}
	if metrics.Quality.ReviewCoverage.Value != 0.5 || metrics.Gates.ReviewCoveragePass || metrics.Gates.Go {
		t.Fatalf("incomplete review coverage gates = quality %#v gates %#v", metrics.Quality.ReviewCoverage, metrics.Gates)
	}
}

func TestAdmissionStudyMetricsRequireEveryDeclaredDimension(t *testing.T) {
	service := newTestService(t)
	contract := testAdmissionStudyContract()
	if _, err := service.FreezeAdmissionStudy(AdmissionStudyFreezeInput{Contract: contract}); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}

	for _, cohort := range []string{"calibration", "held-out"} {
		sessionID := "study-" + cohort
		seedShadowSession(t, service.store, sessionID, "engram")
		metadata := testAdmissionStudyRunMetadata(cohort)
		metadata.Adapter = "codex"
		metadata.ProjectType = "cli"
		metadata.SessionShape = "feature"
		shadow, err := service.RunAdmissionShadow(AdmissionShadowInput{
			Project: "engram", SessionID: sessionID, Study: &metadata,
		})
		if err != nil {
			t.Fatalf("run %s cohort: %v", cohort, err)
		}
		for _, proposal := range shadow.Proposals {
			for _, reviewerID := range []string{"reviewer-a", "reviewer-b"} {
				if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
					ProposalID: proposal.ID, ReviewerID: reviewerID, Verdict: AdmissionAdmit,
				}); err != nil {
					t.Fatalf("review %s proposal by %s: %v", cohort, reviewerID, err)
				}
			}
		}
	}

	metrics, err := service.AdmissionStudyMetrics(AdmissionStudyMetricsInput{
		StudyID: contract.StudyID, StudyVersion: contract.StudyVersion,
	})
	if err != nil {
		t.Fatalf("admission study metrics: %v", err)
	}
	if !metrics.Sufficiency.ByCohort["calibration"].Sufficient || !metrics.Sufficiency.ByCohort["held-out"].Sufficient {
		t.Fatalf("cohort sampling should be sufficient: %#v", metrics.Sufficiency.ByCohort)
	}
	if metrics.Sufficiency.Sufficient || metrics.Sufficiency.ByAdapter["claude-code"].Sufficient ||
		metrics.Sufficiency.ByProjectType["library"].Sufficient || metrics.Sufficiency.BySessionShape["bugfix"].Sufficient ||
		metrics.Gates.SampleSufficient || metrics.Gates.Go {
		t.Fatalf("missing declared dimensions did not block study: %#v %#v", metrics.Sufficiency, metrics.Gates)
	}
}

func TestAdmissionStudyDoesNotChangeLegacyProjectQueueOrMetrics(t *testing.T) {
	service := newTestService(t)
	contract := testAdmissionStudyContract()
	if _, err := service.FreezeAdmissionStudy(AdmissionStudyFreezeInput{Contract: contract}); err != nil {
		t.Fatalf("freeze admission study: %v", err)
	}
	seedShadowSession(t, service.store, "legacy-session", "engram")
	legacy, err := service.RunAdmissionShadow(AdmissionShadowInput{Project: "engram", SessionID: "legacy-session"})
	if err != nil {
		t.Fatalf("run legacy shadow: %v", err)
	}
	seedShadowSession(t, service.store, "attributed-session", "engram")
	metadata := testAdmissionStudyRunMetadata("calibration")
	studyRun, err := service.RunAdmissionShadow(AdmissionShadowInput{
		Project: "engram", SessionID: "attributed-session", Study: &metadata,
	})
	if err != nil {
		t.Fatalf("run attributed shadow: %v", err)
	}

	queue, err := service.ListAdmissionReviews(AdmissionReviewListInput{Project: "engram"})
	if err != nil {
		t.Fatalf("list legacy queue: %v", err)
	}
	if len(queue.Runs) != 1 || queue.Runs[0].ID != legacy.Run.ID || len(queue.Proposals) != len(legacy.Proposals) {
		t.Fatalf("legacy queue includes study rows: %#v", queue)
	}
	for _, proposal := range queue.Proposals {
		if proposal.RunID == studyRun.Run.ID {
			t.Fatalf("legacy queue exposed attributed proposal: %#v", proposal)
		}
	}
	metrics, err := service.AdmissionMetrics(AdmissionMetricsInput{Project: "engram"})
	if err != nil {
		t.Fatalf("legacy metrics: %v", err)
	}
	if metrics.RunCount != 1 || metrics.ProposalCount != len(legacy.Proposals) {
		t.Fatalf("legacy metrics include study rows: %#v", metrics)
	}
}

func testAdmissionStudyContract() store.AdmissionStudyContract {
	return store.AdmissionStudyContract{
		ContractVersion: store.AdmissionStudyContractVersion,
		StudyID:         "real-session-v1",
		StudyVersion:    "v1",
		MetricsVersion:  "admission-study-metrics-v1",
		Cohorts: []store.AdmissionStudyCohort{
			{ID: "calibration", Kind: "calibration", SessionIDs: []string{"study-session", "study-calibration", "incomplete-calibration", "attributed-session"}, MinimumRuns: 1, MinimumProposals: 2, MinimumIndependentReviewedProposals: 1},
			{ID: "held-out", Kind: "held_out", SessionIDs: []string{"study-held-out", "incomplete-held-out"}, MinimumRuns: 1, MinimumProposals: 2, MinimumIndependentReviewedProposals: 1},
		},
		Adapters:      []string{"codex", "claude-code"},
		ProjectTypes:  []string{"cli", "library"},
		SessionShapes: []string{"feature", "bugfix"},
		LabelSchema: store.AdmissionStudyLabelSchema{
			Version:            "admission-study-labels-v1",
			Verdicts:           []string{"admit", "review", "reject"},
			OmissionCategories: []string{"decision", "root_cause", "invariant", "constraint", "preference"},
			ReasonCodes:        []string{"not_proposed", "wrong_category", "insufficient_evidence"},
		},
		Thresholds: store.AdmissionStudyThresholds{
			MinimumPromotionPrecision: 0.9, MaximumReviewRate: 0.5,
			MinimumReviewCoverage:         1,
			MinimumInterReviewerAgreement: 0.8,
			MaximumProtectedFalseRejects:  0, MaximumUnsupportedProposals: 0, MaximumPrivacyLeaks: 0,
		},
		Consent:                 store.AdmissionStudyConsent{Required: true, Attestation: "consent-v1"},
		Retention:               store.AdmissionStudyRetention{Days: 30, Cleanup: store.AdmissionStudyCleanupExplicit},
		AllowedAggregateOutputs: []string{"counts", "distributions", "quality", "uncertainty", "sufficiency", "gates"},
	}
}

func testAdmissionStudyRunMetadata(cohort string) store.AdmissionStudyRunMetadata {
	metadata := store.AdmissionStudyRunMetadata{
		StudyID: "real-session-v1", StudyVersion: "v1", Cohort: cohort,
		Adapter: "codex", ProjectType: "cli", SessionShape: "feature",
		ConsentAttestation: "consent-v1", IndependentReviewRequired: true,
	}
	if cohort == "held-out" {
		metadata.Adapter = "claude-code"
		metadata.ProjectType = "library"
		metadata.SessionShape = "bugfix"
	}
	return metadata
}
