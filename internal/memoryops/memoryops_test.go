package memoryops

import (
	"testing"
	"time"

	"github.com/yersonargotev/engram/internal/store"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	cfg.DataDir = t.TempDir()
	cfg.DedupeWindow = time.Hour
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return New(s)
}

func saveObservation(t *testing.T, service *Service, project, title, content string) *store.Observation {
	t.Helper()
	result, err := service.Save(SaveInput{
		SessionID: "session-" + project,
		CWD:       "/work/" + project,
		Project:   project,
		Type:      "decision",
		Title:     title,
		Content:   content,
	})
	if err != nil {
		t.Fatalf("save observation: %v", err)
	}
	return result.Observation
}

func TestSaveCreatesImplicitSessionNormalizesProjectAndReturnsSuggestion(t *testing.T) {
	service := newTestService(t)

	result, err := service.Save(SaveInput{
		SessionID: "session-1",
		CWD:       "/work/engram",
		Project:   "  Engram CLI  ",
		Type:      "architecture",
		Title:     "Auth model",
		Content:   "Keep authentication ownership explicit.",
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if result.Observation == nil {
		t.Fatal("save did not return the observation")
	}
	if got := *result.Observation.Project; got != "engram cli" {
		t.Fatalf("project = %q, want normalized project", got)
	}
	if result.SuggestedTopicKey != "architecture/auth-model" {
		t.Fatalf("suggested topic key = %q", result.SuggestedTopicKey)
	}
	session, err := service.store.GetSession("session-1")
	if err != nil {
		t.Fatalf("implicit session was not created: %v", err)
	}
	if session.Project != "engram cli" || session.Directory != "/work/engram" {
		t.Fatalf("session = %#v", session)
	}
}

func TestSaveReturnsConflictCandidates(t *testing.T) {
	service := newTestService(t)
	saveObservation(t, service, "engram", "Authentication model", "Authentication model uses explicit tokens.")

	result, err := service.Save(SaveInput{
		SessionID: "session-engram-2",
		CWD:       "/work/engram",
		Project:   "engram",
		Type:      "decision",
		Title:     "Authentication model update",
		Content:   "Authentication model uses explicit tokens and session claims.",
		CandidateOptions: store.CandidateOptions{
			Limit: 3,
		},
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if len(result.Candidates) == 0 {
		t.Fatal("expected similar observation candidate")
	}
	if result.Candidates[0].JudgmentID == "" {
		t.Fatal("candidate does not include a pending judgment id")
	}
}

func TestSearchAndGetReturnFullObservationsWithRelations(t *testing.T) {
	service := newTestService(t)
	first := saveObservation(t, service, "engram", "Auth token rotation", "Rotate API tokens whenever a credential is leaked.")
	second := saveObservation(t, service, "engram", "Auth token compromise", "A leaked API token requires immediate credential rotation.")

	compare, err := service.Compare(CompareInput{
		MemoryIDA:  first.ID,
		MemoryIDB:  second.ID,
		Relation:   store.RelationRelated,
		Confidence: 0.9,
		Reasoning:  "Both document token rotation.",
		Model:      "test-model",
	})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if compare.SyncID == "" {
		t.Fatal("compare did not persist a relation")
	}

	search, err := service.Search(SearchInput{Query: "token rotation", Project: "ENGRAM", Limit: 10})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(search.Observations) == 0 {
		t.Fatal("search returned no observations")
	}
	if search.Observations[0].Observation.Content == "" {
		t.Fatal("search returned truncated or absent content")
	}

	get, err := service.Get(first.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(get.Relations.AsSource)+len(get.Relations.AsTarget) == 0 {
		t.Fatal("get did not return relations")
	}
}

func TestJudgeReviewAndPinnedOperationsReturnReloadedObservation(t *testing.T) {
	service := newTestService(t)
	first := saveObservation(t, service, "engram", "Auth policy", "Use explicit authentication policy.")
	second := saveObservation(t, service, "engram", "Auth policy update", "Use explicit authentication policy with audit logging.")
	result, err := service.Save(SaveInput{
		SessionID: "session-engram-3",
		CWD:       "/work/engram",
		Project:   "engram",
		Type:      "decision",
		Title:     "Auth policy revision",
		Content:   "Use explicit authentication policy with audit logging and rotation.",
	})
	if err != nil {
		t.Fatalf("save candidate: %v", err)
	}
	if len(result.Candidates) == 0 {
		t.Fatal("expected pending candidate")
	}

	judged, err := service.Judge(JudgeInput{
		JudgmentID: result.Candidates[0].JudgmentID,
		Relation:   store.RelationRelated,
		Provenance: Provenance{Actor: "user", Kind: "human", SessionID: "session-engram-3"},
	})
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	if judged.JudgmentStatus != store.JudgmentStatusJudged || judged.MarkedByKind == nil || *judged.MarkedByKind != "human" {
		t.Fatalf("unexpected judged relation: %#v", judged)
	}

	pinned, err := service.SetPinned(first.ID, true)
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	if !pinned.Pinned {
		t.Fatal("observation was not pinned")
	}

	if err := service.store.MarkReviewed(second.ID); err != nil {
		t.Fatalf("seed review state: %v", err)
	}
	marked, err := service.ReviewMark(second.ID)
	if err != nil {
		t.Fatalf("mark reviewed: %v", err)
	}
	if marked.ID != second.ID {
		t.Fatalf("marked id = %d, want %d", marked.ID, second.ID)
	}
}

func TestMergeDryRunDoesNotMutateAndApplyReturnsCounts(t *testing.T) {
	service := newTestService(t)
	saveObservation(t, service, "legacy", "Legacy decision", "Legacy content")
	saveObservation(t, service, "canonical", "Canonical decision", "Canonical content")

	dryRun, err := service.Merge(MergeInput{Sources: []string{"legacy"}, Canonical: "canonical", DryRun: true})
	if err != nil {
		t.Fatalf("merge dry run: %v", err)
	}
	if !dryRun.DryRun || dryRun.ObservationsUpdated != 1 {
		t.Fatalf("unexpected dry run: %#v", dryRun)
	}
	search, err := service.Search(SearchInput{Query: "Legacy", Project: "legacy"})
	if err != nil || len(search.Observations) != 1 {
		t.Fatalf("dry run mutated project: results=%#v err=%v", search, err)
	}

	merged, err := service.Merge(MergeInput{Sources: []string{"legacy"}, Canonical: "canonical"})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if merged.DryRun || merged.ObservationsUpdated != 1 {
		t.Fatalf("unexpected merge: %#v", merged)
	}
	legacy, err := service.Search(SearchInput{Query: "Legacy", Project: "legacy"})
	if err != nil {
		t.Fatalf("search legacy: %v", err)
	}
	if len(legacy.Observations) != 0 {
		t.Fatalf("legacy project still has observations: %#v", legacy.Observations)
	}
}

func TestProjectScopedReadsRequireResolvedProjectAndRejectAmbiguousScope(t *testing.T) {
	service := newTestService(t)

	if _, err := service.Search(SearchInput{Query: "auth"}); err != ErrProjectRequired {
		t.Fatalf("search without project error = %v, want ErrProjectRequired", err)
	}
	if _, err := service.Search(SearchInput{Query: "auth", Project: "engram", AllProjects: true}); err == nil {
		t.Fatal("search accepted project and all-projects together")
	}
	if _, err := service.ReviewList(ReviewListInput{}); err != ErrProjectRequired {
		t.Fatalf("review list without project error = %v, want ErrProjectRequired", err)
	}
	if _, err := service.ReviewList(ReviewListInput{Project: "engram", AllProjects: true}); err == nil {
		t.Fatal("review list accepted project and all-projects together")
	}
}
