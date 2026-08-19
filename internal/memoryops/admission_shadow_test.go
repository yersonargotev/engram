package memoryops

import (
	"reflect"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestRunAdmissionShadowRetainsAssessmentsWithoutCreatingMemories(t *testing.T) {
	service := newTestService(t)
	seedShadowSession(t, service.store, "shadow-session", "engram")
	before := shadowMemoryStats(t, service.store)

	result, err := service.RunAdmissionShadow(AdmissionShadowInput{
		Project:   " ENGRAM ",
		SessionID: "shadow-session",
	})
	if err != nil {
		t.Fatalf("run admission shadow: %v", err)
	}
	if result.Run.Project != "engram" || result.Run.SessionID != "shadow-session" ||
		result.Run.EvidenceVersion != EvidenceBundleVersion || result.Run.GeneratorVersion != AdmissionGeneratorVersion ||
		result.Run.PolicyVersion != AdmissionPolicyVersion {
		t.Fatalf("run = %#v", result.Run)
	}
	if result.Run.IncludedItems != 2 || len(result.Proposals) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if result.Run.Proposals != nil {
		t.Fatalf("shadow result duplicates proposals inside run metadata: %#v", result.Run.Proposals)
	}
	if result.Proposals[0].Recommendation != string(AdmissionAdmit) || result.Proposals[1].Recommendation != string(AdmissionReview) {
		t.Fatalf("proposals = %#v", result.Proposals)
	}

	after := shadowMemoryStats(t, service.store)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("shadow changed Memory state: before=%#v after=%#v", before, after)
	}
	runs, err := service.store.ListAdmissionShadowRuns("engram")
	if err != nil || len(runs) != 1 {
		t.Fatalf("stored runs = %#v, err=%v", runs, err)
	}
}

func TestAdmissionReviewListMarkAndMetrics(t *testing.T) {
	service := newTestService(t)
	seedShadowSession(t, service.store, "shadow-review", "engram")
	shadow, err := service.RunAdmissionShadow(AdmissionShadowInput{Project: "engram", SessionID: "shadow-review"})
	if err != nil {
		t.Fatalf("run shadow: %v", err)
	}

	pending, err := service.ListAdmissionReviews(AdmissionReviewListInput{Project: "engram"})
	if err != nil {
		t.Fatalf("list admission reviews: %v", err)
	}
	if len(pending.Proposals) != 2 {
		t.Fatalf("pending proposals = %#v", pending.Proposals)
	}
	if len(pending.Runs) != 1 || pending.Runs[0].SessionID != "shadow-review" || pending.Runs[0].PolicyVersion != AdmissionPolicyVersion {
		t.Fatalf("pending run metadata = %#v", pending.Runs)
	}

	first, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
		ProposalID: shadow.Proposals[0].ID,
		Verdict:    AdmissionAdmit,
		Note:       "Matches the explicit request.",
	})
	if err != nil {
		t.Fatalf("mark first review: %v", err)
	}
	repeated, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
		ProposalID: shadow.Proposals[0].ID,
		Verdict:    AdmissionAdmit,
		Note:       "Matches the explicit request.",
	})
	if err != nil {
		t.Fatalf("repeat review: %v", err)
	}
	if first.Review.ID != repeated.Review.ID || !repeated.AlreadyRecorded {
		t.Fatalf("idempotent review = first %#v repeated %#v", first, repeated)
	}

	if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
		ProposalID:  shadow.Proposals[1].ID,
		Verdict:     AdmissionReject,
		Note:        "Unsupported in the source.",
		Unsupported: true,
	}); err != nil {
		t.Fatalf("mark unsupported review: %v", err)
	}

	pending, err = service.ListAdmissionReviews(AdmissionReviewListInput{Project: "engram"})
	if err != nil || len(pending.Proposals) != 0 || len(pending.Runs) != 0 {
		t.Fatalf("pending after review = %#v, err=%v", pending, err)
	}

	metrics, err := service.AdmissionMetrics(AdmissionMetricsInput{Project: "engram"})
	if err != nil {
		t.Fatalf("admission metrics: %v", err)
	}
	if metrics.RunCount != 1 || metrics.ProposalCount != 2 || metrics.ReviewedProposalCount != 2 || metrics.PendingProposalCount != 0 || metrics.ReviewCount != 2 {
		t.Fatalf("metrics counts = %#v", metrics)
	}
	if metrics.AgreementCount != 1 || metrics.DisagreementCount != 1 {
		t.Fatalf("agreement metrics = %#v", metrics)
	}
	if !metrics.AutomaticPromotionGateBlocked || metrics.UnsupportedCount != 1 || metrics.PrivacyLeakCount != 0 {
		t.Fatalf("promotion gate = %#v", metrics)
	}
	if metrics.AutomaticRejectGateBlocked || metrics.ProtectedFalseRejectCount != 0 {
		t.Fatalf("reject gate = %#v", metrics)
	}
	if metrics.ByRecommendation[string(AdmissionAdmit)] != 1 || metrics.ByRecommendation[string(AdmissionReview)] != 1 {
		t.Fatalf("recommendations = %#v", metrics.ByRecommendation)
	}
	if metrics.ByPolicyVersion[AdmissionPolicyVersion] != 2 {
		t.Fatalf("proposals by policy version = %#v", metrics.ByPolicyVersion)
	}
}

func TestAdmissionShadowOperationsValidateInputs(t *testing.T) {
	service := newTestService(t)
	seedShadowSession(t, service.store, "shadow-validation", "engram")

	if _, err := service.RunAdmissionShadow(AdmissionShadowInput{Project: "engram"}); err == nil {
		t.Fatal("missing session unexpectedly succeeded")
	}
	if _, err := service.ListAdmissionReviews(AdmissionReviewListInput{}); err == nil {
		t.Fatal("missing review project unexpectedly succeeded")
	}
	if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{ProposalID: "missing", Verdict: "maybe"}); err == nil {
		t.Fatal("invalid verdict unexpectedly succeeded")
	}
	if _, err := service.AdmissionMetrics(AdmissionMetricsInput{}); err == nil {
		t.Fatal("missing metrics project unexpectedly succeeded")
	}
}

func TestAdmissionMetricsBlocksAutomaticRejectAfterProtectedFalseReject(t *testing.T) {
	service := newTestService(t)
	run, err := service.store.CreateAdmissionShadowRun(store.CreateAdmissionShadowRunParams{
		Project:          "engram",
		Mode:             "session",
		EvidenceVersion:  EvidenceBundleVersion,
		GeneratorVersion: AdmissionGeneratorVersion,
		PolicyVersion:    AdmissionPolicyVersion,
		Proposals: []store.AdmissionShadowProposalInput{{
			Type:                  "decision",
			Title:                 "Protected proposal",
			Content:               "This must not be lost.",
			Scope:                 "project",
			Category:              string(ProposalDecision),
			Protected:             true,
			Recommendation:        string(AdmissionReject),
			AssessmentReasonCodes: []string{ReasonNormalizedExactDuplicate},
		}},
	})
	if err != nil {
		t.Fatalf("create protected rejected proposal: %v", err)
	}
	if _, err := service.MarkAdmissionReview(AdmissionReviewMarkInput{
		ProposalID: run.Proposals[0].ID,
		Verdict:    AdmissionAdmit,
	}); err != nil {
		t.Fatalf("correct protected proposal: %v", err)
	}

	metrics, err := service.AdmissionMetrics(AdmissionMetricsInput{Project: "engram"})
	if err != nil {
		t.Fatalf("admission metrics: %v", err)
	}
	if !metrics.AutomaticRejectGateBlocked || metrics.ProtectedFalseRejectCount != 1 {
		t.Fatalf("automatic reject gate = %#v", metrics)
	}
}

func seedShadowSession(t *testing.T, memoryStore *store.Store, sessionID, project string) {
	t.Helper()
	if err := memoryStore.CreateSession(sessionID, project, "/work/"+project); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := memoryStore.AddPrompt(store.AddPromptParams{
		SessionID: sessionID,
		Project:   project,
		Content:   "Remember this: Explicit shadow requests remain authoritative.",
	}); err != nil {
		t.Fatalf("add prompt: %v", err)
	}
	if err := memoryStore.EndSession(sessionID, "## Decisions\n- Shadow assessments remain local-only."); err != nil {
		t.Fatalf("end session: %v", err)
	}
}

func shadowMemoryStats(t *testing.T, memoryStore *store.Store) store.Stats {
	t.Helper()
	stats, err := memoryStore.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	return *stats
}
