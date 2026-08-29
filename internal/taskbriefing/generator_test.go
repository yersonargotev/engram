package taskbriefing

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestGenerateReportsEmptyStorePipeline(t *testing.T) {
	memoryStore := newTestStore(t)

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: "diagnose cache migration",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Pipeline.EligibleInventory != 0 || result.Pipeline.RetrievedCandidates != 0 {
		t.Fatalf("pipeline = %#v, want empty inventory and retrieval", result.Pipeline)
	}
	if !result.Pipeline.RetrievalCountComplete {
		t.Fatalf("pipeline = %#v, want complete retrieval count", result.Pipeline)
	}
	if result.EmptyResultReason != EmptyResultProjectHasNoMemories {
		t.Fatalf("empty result reason = %q, want %q", result.EmptyResultReason, EmptyResultProjectHasNoMemories)
	}
	if result.Fallback != nil {
		t.Fatalf("fallback = %#v, want none when the project has no memories", result.Fallback)
	}
}

func TestGenerateReportsNoCandidateMatchAndFallback(t *testing.T) {
	memoryStore := newTestStore(t)
	addTaskBriefingTestMemory(t, memoryStore, "unrelated-session", "Typography spacing", "Dashboard typography spacing guidance.")

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: "repair cache migration",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Pipeline.EligibleInventory != 1 || result.Pipeline.RetrievedCandidates != 0 {
		t.Fatalf("pipeline = %#v, want one eligible memory and no retrieved candidates", result.Pipeline)
	}
	if result.EmptyResultReason != EmptyResultNoCandidatesMatched {
		t.Fatalf("empty result reason = %q, want %q", result.EmptyResultReason, EmptyResultNoCandidatesMatched)
	}
	if result.Fallback == nil {
		t.Fatal("fallback is nil, want a targeted-search recommendation")
	}
	if result.Fallback.ReasonCode != FallbackNoCandidatesMatched {
		t.Fatalf("fallback reason = %q, want %q", result.Fallback.ReasonCode, FallbackNoCandidatesMatched)
	}
	wantAnchors := []string{"repair", "cache", "migration"}
	if !reflect.DeepEqual(result.Fallback.Anchors, wantAnchors) {
		t.Fatalf("fallback anchors = %v, want %v", result.Fallback.Anchors, wantAnchors)
	}
	wantArgs := []string{"search", "repair cache migration", "--project", "engram", "--match-mode", "all", "--limit", "5", "--json"}
	if !reflect.DeepEqual(result.Fallback.Invocation.Args, wantArgs) {
		t.Fatalf("fallback args = %v, want %v", result.Fallback.Invocation.Args, wantArgs)
	}
}

func TestGenerateReportsIncompleteCandidateRetrieval(t *testing.T) {
	memoryStore := newTestStore(t)
	for index := 0; index < candidateRetrievalLimit+1; index++ {
		addTaskBriefingTestMemory(
			t,
			memoryStore,
			"retrieval-session-"+strconv.Itoa(index),
			"Retrieval ceiling "+strconv.Itoa(index),
			"Retrieval ceiling diagnostics.",
		)
	}

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: "retrieval ceiling",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Pipeline.EligibleInventory != candidateRetrievalLimit+1 {
		t.Fatalf("eligible inventory = %d, want %d", result.Pipeline.EligibleInventory, candidateRetrievalLimit+1)
	}
	if result.Pipeline.RetrievedCandidates != candidateRetrievalLimit || result.Pipeline.RetrievalCountComplete {
		t.Fatalf("pipeline = %#v, want %d retrieved with an incomplete count", result.Pipeline, candidateRetrievalLimit)
	}
	if len(result.Pipeline.Retrievals) != 1 {
		t.Fatalf("retrieval reports = %#v, want one", result.Pipeline.Retrievals)
	}
	retrieval := result.Pipeline.Retrievals[0]
	if retrieval.Limit != candidateRetrievalLimit || retrieval.Retrieved != candidateRetrievalLimit || retrieval.CountComplete {
		t.Fatalf("retrieval report = %#v", retrieval)
	}
	if result.Fallback == nil || result.Fallback.ReasonCode != FallbackCandidateRetrievalTruncated {
		t.Fatalf("fallback = %#v, want retrieval-truncated recommendation", result.Fallback)
	}
}

func TestGenerateReportsKnownFalseOmissionAtTaskGate(t *testing.T) {
	memoryStore := newTestStore(t)
	memoryID := addTaskBriefingTestMemory(
		t,
		memoryStore,
		"known-omission-session",
		"Codex setup reproducibility contract",
		"Clean only its branch after the protected workflow.",
	)

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: "merge PR 56 for issue 43 and clean only its branch after protected validation",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Pipeline.TaskGateRejections != 1 || result.Pipeline.QualifiedCandidates != 0 {
		t.Fatalf("pipeline = %#v, want one Task-gate rejection and no qualified candidates", result.Pipeline)
	}
	if result.EmptyResultReason != EmptyResultCandidatesFiltered {
		t.Fatalf("empty result reason = %q, want %q", result.EmptyResultReason, EmptyResultCandidatesFiltered)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("rejections = %#v, want one", result.Rejections)
	}
	rejection := result.Rejections[0]
	if rejection.MemoryID != memoryID || rejection.Stage != RejectionStageTaskGate || rejection.ReasonCode != RejectionTaskEvidenceBelowGate {
		t.Fatalf("rejection = %#v", rejection)
	}
	if rejection.TaskEvidence == nil || rejection.TaskEvidence.Matched != 4 || rejection.TaskEvidence.Required != 6 {
		t.Fatalf("task evidence = %#v, want 4 matched of 6 required", rejection.TaskEvidence)
	}
	if result.Fallback == nil || result.Fallback.ReasonCode != FallbackCandidatesFiltered {
		t.Fatalf("fallback = %#v, want candidates-filtered recommendation", result.Fallback)
	}
	wantAnchors := []string{"pr 56", "issue 43"}
	if !reflect.DeepEqual(result.Fallback.Anchors, wantAnchors) {
		t.Fatalf("fallback anchors = %v, want %v", result.Fallback.Anchors, wantAnchors)
	}
}

func TestGenerateReportsRepositoryGateRejection(t *testing.T) {
	memoryStore := newTestStore(t)
	memoryID := addTaskBriefingTestMemory(t, memoryStore, "repository-gate-session", "Cache reference", "Cache reference guidance.")

	result, err := New(memoryStore).Generate(Input{
		Project:           "engram",
		RepositoryProject: "engram",
		Repository:        RepositorySignals{Branch: "feat/cache"},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Pipeline.RepositoryRejections != 1 || result.Pipeline.QualifiedCandidates != 0 {
		t.Fatalf("pipeline = %#v", result.Pipeline)
	}
	if len(result.Rejections) != 1 || result.Rejections[0].MemoryID != memoryID || result.Rejections[0].Stage != RejectionStageRepositoryGate {
		t.Fatalf("rejections = %#v", result.Rejections)
	}
}

func TestGenerateReportsThresholdRejection(t *testing.T) {
	memoryStore := newTestStore(t)
	memoryID := addTaskBriefingTestMemory(
		t,
		memoryStore,
		"threshold-session",
		"Owned cache branch",
		"Branch feat/cache-migration records the cache migration.",
	)

	result, err := New(memoryStore).Generate(Input{
		Project:           "engram",
		RepositoryProject: "engram",
		Repository:        RepositorySignals{Branch: "feat/cache-migration"},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Pipeline.ThresholdRejections != 1 || result.Pipeline.QualifiedCandidates != 0 {
		t.Fatalf("pipeline = %#v", result.Pipeline)
	}
	if result.EmptyResultReason != EmptyResultCandidatesBelowThreshold {
		t.Fatalf("empty result reason = %q, want %q", result.EmptyResultReason, EmptyResultCandidatesBelowThreshold)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("rejections = %#v, want one", result.Rejections)
	}
	rejection := result.Rejections[0]
	if rejection.MemoryID != memoryID || rejection.Stage != RejectionStageThreshold || rejection.ReasonCode != RejectionScoreBelowThreshold {
		t.Fatalf("rejection = %#v", rejection)
	}
	wantScore := CalibratedDefaults.BranchWeight + CalibratedDefaults.TitleOrTopicBonus
	if rejection.Score == nil || *rejection.Score != wantScore || rejection.Threshold == nil || *rejection.Threshold != CalibratedDefaults.InclusionThreshold {
		t.Fatalf("rejection = %#v, want score %d and threshold %d", rejection, wantScore, CalibratedDefaults.InclusionThreshold)
	}
	if result.Fallback == nil || result.Fallback.ReasonCode != FallbackCandidatesBelowThreshold {
		t.Fatalf("fallback = %#v, want below-threshold recommendation", result.Fallback)
	}
}

func TestGenerateOmitsFallbackForCompleteSuccessfulBriefing(t *testing.T) {
	memoryStore := newTestStore(t)
	addTaskBriefingTestMemory(t, memoryStore, "complete-session", "Cache migration decision", "Cache migration decision.")

	result, err := New(memoryStore).Generate(Input{Project: "engram", TaskIntent: "cache migration"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 || result.Fallback != nil {
		t.Fatalf("result = %#v, want one selected memory and no fallback", result)
	}
}

func TestGenerateDoesNotUseGenericFallbackAnchors(t *testing.T) {
	memoryStore := newTestStore(t)
	addTaskBriefingTestMemory(t, memoryStore, "generic-session", "Merge issue branch", "Merge issue branch workflow.")

	result, err := New(memoryStore).Generate(Input{Project: "engram", TaskIntent: "merge issue branch"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Fallback != nil {
		t.Fatalf("fallback = %#v, want none for generic-only anchors", result.Fallback)
	}
}

func TestGenerateBoundsRejectionDetailsDeterministically(t *testing.T) {
	memoryStore := newTestStore(t)
	wantIDs := make([]int64, 0, maximumRejectionDetails)
	for index := 0; index < maximumRejectionDetails+2; index++ {
		id := addTaskBriefingTestMemory(
			t,
			memoryStore,
			"bounded-rejection-session-"+strconv.Itoa(index),
			"Merge branch workflow "+strconv.Itoa(index),
			"Merge only its branch after validation.",
		)
		if index < maximumRejectionDetails {
			wantIDs = append(wantIDs, id)
		}
	}

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: "merge PR 56 for issue 43 and clean only its branch after protected validation",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Rejections) != maximumRejectionDetails || result.RejectionDetailsOmitted != 2 {
		t.Fatalf("rejection summary = %d details, %d omitted", len(result.Rejections), result.RejectionDetailsOmitted)
	}
	gotIDs := make([]int64, len(result.Rejections))
	for index, rejection := range result.Rejections {
		gotIDs[index] = rejection.MemoryID
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("rejection IDs = %v, want deterministic prefix %v", gotIDs, wantIDs)
	}
}

func TestGenerateReportsSupersededCandidateFiltering(t *testing.T) {
	memoryStore := newTestStore(t)
	oldID := addTaskBriefingTestMemory(t, memoryStore, "superseded-old-session", "Cache migration old", "Cache migration guidance.")
	newID := addTaskBriefingTestMemory(t, memoryStore, "superseded-new-session", "Cache migration new", "Cache migration replacement.")
	oldMemory, err := memoryStore.GetObservation(oldID)
	if err != nil {
		t.Fatalf("GetObservation(old): %v", err)
	}
	newMemory, err := memoryStore.GetObservation(newID)
	if err != nil {
		t.Fatalf("GetObservation(new): %v", err)
	}
	const relationID = "rel-task-briefing-supersedes"
	if _, err := memoryStore.SaveRelation(store.SaveRelationParams{SyncID: relationID, SourceID: newMemory.SyncID, TargetID: oldMemory.SyncID}); err != nil {
		t.Fatalf("SaveRelation: %v", err)
	}
	if _, err := memoryStore.JudgeRelation(store.JudgeRelationParams{
		JudgmentID: relationID, Relation: store.RelationSupersedes, MarkedByActor: "test", MarkedByKind: "system",
	}); err != nil {
		t.Fatalf("JudgeRelation: %v", err)
	}

	result, err := New(memoryStore).Generate(Input{Project: "engram", TaskIntent: "cache migration"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Pipeline.LifecycleRejections != 1 || result.Pipeline.QualifiedCandidates != 1 {
		t.Fatalf("pipeline = %#v", result.Pipeline)
	}
	if len(result.Rejections) != 1 || result.Rejections[0].MemoryID != oldID || result.Rejections[0].ReasonCode != RejectionSuperseded {
		t.Fatalf("rejections = %#v", result.Rejections)
	}
}

func TestGenerateDoesNotEmitOversizedFallbackAnchor(t *testing.T) {
	memoryStore := newTestStore(t)
	addTaskBriefingTestMemory(t, memoryStore, "oversized-fallback-session", "Unrelated memory", "Typography guidance.")

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: strings.Repeat("a", maximumFallbackAnchorBytes+1),
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.EmptyResultReason != EmptyResultNoCandidatesMatched {
		t.Fatalf("empty result reason = %q, want %q", result.EmptyResultReason, EmptyResultNoCandidatesMatched)
	}
	if result.Fallback != nil || result.FallbackCandidate != nil {
		t.Fatalf("fallback = %#v / candidate %#v, want none for an oversized anchor", result.Fallback, result.FallbackCandidate)
	}
}

func addTaskBriefingTestMemory(t *testing.T, memoryStore *store.Store, sessionID, title, content string) int64 {
	t.Helper()
	if err := memoryStore.CreateSession(sessionID, "engram", "/tmp/engram"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	id, err := memoryStore.AddObservation(store.AddObservationParams{
		SessionID: sessionID,
		Type:      "decision",
		Title:     title,
		Content:   content,
		Project:   "engram",
		Scope:     "project",
	})
	if err != nil {
		t.Fatalf("AddObservation: %v", err)
	}
	return id
}

func TestGenerateRetainsExactIdentifierAfterOversizedToken(t *testing.T) {
	memoryStore := newTestStore(t)
	if err := memoryStore.CreateSession("oversized-token-session", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := memoryStore.AddObservation(store.AddObservationParams{
		SessionID: "oversized-token-session",
		Type:      "decision",
		Title:     "Exact delivery identity",
		Content:   "alpha PR 56",
		Project:   "engram",
		Scope:     "project",
	}); err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	oversizedPR := "PR" + strings.Repeat("9", maximumGitTermBytes)
	task := "alpha bravo charlie delta echo foxtrot " + oversizedPR + ",PR 56"
	result, err := New(memoryStore).Generate(Input{Project: "engram", TaskIntent: task})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 {
		t.Fatalf("memories = %d, want exact identifier match after oversized token", len(result.Memories))
	}
	want := []string{"pr:#56"}
	if got := result.Memories[0].Evidence[0].MatchedIdentifiers; !reflect.DeepEqual(got, want) {
		t.Fatalf("matched identifiers = %v, want %v", got, want)
	}
	truncations := diagnosticTruncations(result.Diagnostics, DiagnosticTaskInputTruncated)
	if len(truncations) != 1 || truncations[0].AnalyzedTerms != 8 || truncations[0].OmittedTerms != 1 {
		t.Fatalf("truncations = %#v, want eight analyzed terms and one omitted oversized token", truncations)
	}
}

func TestGenerateHonorsSmallerResultLimit(t *testing.T) {
	memoryStore := newTestStore(t)
	for index, title := range []string{"Alpha task briefing", "Beta task briefing"} {
		sessionID := "limit-session-" + string(rune('a'+index))
		if err := memoryStore.CreateSession(sessionID, "engram", "/tmp/engram"); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if _, err := memoryStore.AddObservation(store.AddObservationParams{
			SessionID: sessionID,
			Type:      "decision",
			Title:     title,
			Content:   "Use deterministic task briefing selection for durable memories.",
			Project:   "engram",
			Scope:     "project",
		}); err != nil {
			t.Fatalf("AddObservation: %v", err)
		}
	}

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: "implement deterministic task briefing selection",
		Limit:      1,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 {
		t.Fatalf("memories = %d, want 1", len(result.Memories))
	}
	if result.ResultLimitOmissions != 1 {
		t.Fatalf("result limit omissions = %d, want 1", result.ResultLimitOmissions)
	}
	if !hasDiagnostic(result.Diagnostics, DiagnosticResultLimitReached) {
		t.Fatalf("diagnostics = %#v, want %s", result.Diagnostics, DiagnosticResultLimitReached)
	}
	if result.Pipeline.QualifiedCandidates != 2 || result.Fallback == nil || result.Fallback.ReasonCode != FallbackResultLimitReached {
		t.Fatalf("result = %#v, want two qualified candidates and a result-limit fallback", result)
	}
}

func TestGenerateReportsEveryMatchedMemoryField(t *testing.T) {
	memoryStore := newTestStore(t)
	if err := memoryStore.CreateSession("evidence-session", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := memoryStore.AddObservation(store.AddObservationParams{
		SessionID: "evidence-session",
		Type:      "decision",
		Title:     "Task briefing",
		Content:   "A task briefing selects durable memories.",
		Project:   "engram",
		Scope:     "project",
	}); err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	result, err := New(memoryStore).Generate(Input{Project: "engram", TaskIntent: "briefing"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 || len(result.Memories[0].Evidence) != 1 {
		t.Fatalf("result = %#v", result)
	}
	want := []string{"content", "title"}
	if got := result.Memories[0].Evidence[0].MatchedFields; !reflect.DeepEqual(got, want) {
		t.Fatalf("matched fields = %v, want %v", got, want)
	}
}
