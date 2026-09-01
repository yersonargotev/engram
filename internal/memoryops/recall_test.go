package memoryops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
)

func TestRecallCandidatesDefaultsToFiveProjectResultsWithinFourKiB(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-default", nil }
	service.recallStartedAt = func() time.Time { return time.Unix(10, 0) }
	service.recallElapsed = func(started time.Time) time.Duration {
		if !started.Equal(time.Unix(10, 0)) {
			t.Fatalf("elapsed start = %v", started)
		}
		return 37 * time.Millisecond
	}
	if err := service.store.CreateSession("recall-budget", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}

	for i := 0; i < 6; i++ {
		if _, err := service.store.AddObservation(store.AddObservationParams{
			SessionID: "recall-budget",
			Project:   "engram",
			Scope:     "project",
			Type:      "decision",
			Title:     fmt.Sprintf("Recall budget candidate %d", i),
			Content:   strings.Repeat("🧠", 900),
		}); err != nil {
			t.Fatalf("seed candidate: %v", err)
		}
	}

	result, err := service.Recall(RecallInput{
		Query:           "Recall budget candidate",
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
		BinaryVersion:   "test-version",
		BinaryRevision:  "test-revision",
	})
	if err != nil {
		t.Fatalf("Recall() error = %v", err)
	}
	if result.RecallID != "recall-test-default" {
		t.Fatalf("recall_id = %q", result.RecallID)
	}
	if result.ResultCount != 5 || len(result.Candidates) != 5 || len(result.ResultIDs) != 5 || len(result.OpaqueResultIDs) != 5 {
		t.Fatalf("result count metadata = %d/%d/%d/%d bytes=%d candidates=%#v, want five candidates", result.ResultCount, len(result.Candidates), len(result.ResultIDs), len(result.OpaqueResultIDs), result.DeliveredUTF8Bytes, result.Candidates)
	}
	if result.DeliveredUTF8Bytes > RecallCandidateBudgetBytes {
		t.Fatalf("delivered_utf8_bytes = %d, budget = %d", result.DeliveredUTF8Bytes, RecallCandidateBudgetBytes)
	}
	if result.Provenance.ProtocolVersion != 1 || result.Provenance.BinaryVersion != "test-version" || result.Provenance.BinaryRevision != "test-revision" {
		t.Fatalf("provenance = %#v", result.Provenance)
	}
	if result.ElapsedMonotonicMS != 37 {
		t.Fatalf("elapsed_monotonic_ms = %d, want 37", result.ElapsedMonotonicMS)
	}
	for index, candidate := range result.Candidates {
		if result.ResultIDs[index] != candidate.ID || result.OpaqueResultIDs[index] != candidate.ResultID {
			t.Fatalf("additive result identity mapping at %d = %d/%q, candidate=%#v", index, result.ResultIDs[index], result.OpaqueResultIDs[index], candidate)
		}
		if !strings.HasSuffix(candidate.Summary, "…") {
			t.Fatalf("candidate summary was not byte-bounded: %q", candidate.Summary)
		}
		if !utf8.ValidString(candidate.Summary) {
			t.Fatalf("candidate summary is invalid UTF-8: %q", candidate.Summary)
		}
	}
}

func TestRecallPersistsLatencyOnlyAfterPrimaryRunPersistence(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-persisted-latency", nil }
	started := time.Unix(100, 0)
	service.recallStartedAt = func() time.Time { return started }
	saveObservation(t, service, "engram", "Persisted Recall latency", "primary Recall persistence precedes completion measurement")
	service.recallElapsed = func(time.Time) time.Duration {
		var persisted int
		if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM recall_runs WHERE recall_id = ?`, "recall-persisted-latency").Scan(&persisted); err != nil {
			t.Fatalf("inspect persisted Recall run: %v", err)
		}
		if persisted == 0 {
			return 5 * time.Millisecond
		}
		return 41 * time.Millisecond
	}
	turnIdentity := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-persisted-latency", RootTurnID: "turn-persisted-latency",
	}
	result, err := service.Recall(RecallInput{
		Query: "primary Recall persistence precedes completion measurement", Project: "engram",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || result.Warning != nil || result.ElapsedMonotonicMS != 41 {
		t.Fatalf("Recall result = %#v, err=%v", result, err)
	}
	var elapsed, completed int64
	if err := service.store.DB().QueryRow(`
		SELECT elapsed_monotonic_ms, completed_at_unix_nano
		FROM recall_runs WHERE recall_id = ?`, result.RecallID).Scan(&elapsed, &completed); err != nil {
		t.Fatalf("load completed Recall metrics: %v", err)
	}
	if elapsed != result.ElapsedMonotonicMS || completed != started.Add(41*time.Millisecond).UnixNano() {
		t.Fatalf("persisted Recall metrics = %d/%d, result=%d", elapsed, completed, result.ElapsedMonotonicMS)
	}
	var attributedElapsed int64
	if err := service.store.DB().QueryRow(`SELECT elapsed_monotonic_ms FROM recall_feedback_runs`).Scan(&attributedElapsed); err != nil {
		t.Fatalf("load attributed Recall latency: %v", err)
	}
	if attributedElapsed != result.ElapsedMonotonicMS {
		t.Fatalf("attributed Recall latency = %d, want %d", attributedElapsed, result.ElapsedMonotonicMS)
	}
}

func TestRecallPreservesDeliveredCandidatesWhenMetricCompletionFails(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-metric-completion-failure", nil }
	saveObservation(t, service, "engram", "Delivered despite metric failure", "a committed Recall result remains delivered")
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_recall_metric_completion
		BEFORE UPDATE OF elapsed_monotonic_ms ON recall_runs
		BEGIN
			SELECT RAISE(ABORT, 'forced Recall metric completion failure');
		END`); err != nil {
		t.Fatalf("create Recall metric failure trigger: %v", err)
	}
	turnIdentity := &store.CheckpointIdentity{
		Host: "codex", SessionID: "session-metric-completion-failure", RootTurnID: "turn-metric-completion-failure",
	}
	result, err := service.Recall(RecallInput{
		Query: "a committed Recall result remains delivered", Project: "engram",
		ProjectStrength: project.IdentityStrengthExplicit, TurnIdentity: turnIdentity,
	})
	if err != nil || result.Warning != nil || result.ResultCount != 1 || len(result.Candidates) != 1 {
		t.Fatalf("Recall delivery = %#v, err=%v", result, err)
	}
	if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "recall_metrics_unavailable" ||
		result.Diagnostics[0].Operation != "recall_metrics" {
		t.Fatalf("Recall metric diagnostic = %#v", result.Diagnostics)
	}
	var runElapsed, feedbackElapsed any
	if err := service.store.DB().QueryRow(`SELECT elapsed_monotonic_ms FROM recall_runs WHERE recall_id = ?`, result.RecallID).Scan(&runElapsed); err != nil {
		t.Fatalf("load pending Recall latency: %v", err)
	}
	if err := service.store.DB().QueryRow(`SELECT elapsed_monotonic_ms FROM recall_feedback_runs`).Scan(&feedbackElapsed); err != nil {
		t.Fatalf("load pending attributed Recall latency: %v", err)
	}
	if runElapsed != nil || feedbackElapsed != nil {
		t.Fatalf("failed completion persisted latency: run=%v feedback=%v", runElapsed, feedbackElapsed)
	}
	report, err := service.RecallFeedbackReport()
	if err != nil {
		t.Fatalf("report pending Recall metrics: %v", err)
	}
	search := findRecallOperationReport(t, report.Operations, RecallFeedbackOperationSearch)
	if report.ExposedResults != 1 || search.Events != 1 || search.TotalExposedResults != 1 ||
		search.LatencySamples != 0 || search.UnknownLatency != 1 {
		t.Fatalf("pending Recall metric report = %#v; report=%#v", search, report)
	}
}

func TestRecallCandidatesRejectsInvalidAuthorityDimensionsBeforeStoreAccess(t *testing.T) {
	service := newTestService(t)
	if err := service.store.Close(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name  string
		input RecallInput
		want  string
	}{
		{name: "unknown scope", input: RecallInput{Query: "secret", Scope: "typo", AllProjects: true}, want: "invalid recall scope"},
		{name: "invalid match mode", input: RecallInput{Query: "secret", Project: "engram", MatchMode: "or"}, want: "invalid match_mode"},
		{name: "conflicting project selectors", input: RecallInput{Query: "secret", Project: "engram", AllProjects: true}, want: "project and all projects cannot be used together"},
		{name: "implicit broad selector", input: RecallInput{Query: "secret", Scope: "personal", DeliberateScope: true}, want: "all projects is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := service.Recall(test.input)
			if err == nil || !strings.Contains(err.Error(), test.want) || result != nil {
				t.Fatalf("Recall() result=%#v error=%v, want %q", result, err, test.want)
			}
		})
	}
}

func TestNormalizeRecallScopeCanonicalizesKnownValuesWithoutWidening(t *testing.T) {
	for _, test := range []struct{ input, want string }{
		{input: "", want: "project"},
		{input: " Project ", want: "project"},
		{input: "PERSONAL", want: "personal"},
		{input: "global", want: "global"},
	} {
		got, err := NormalizeRecallScope(test.input)
		if err != nil || got != test.want {
			t.Fatalf("NormalizeRecallScope(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
}

func TestRecallCandidatesFollowUpAllowsTenWithoutWideningScopeOrByteBudget(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-follow-up", nil }
	if err := service.store.CreateSession("recall-follow-up", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}

	for i := 0; i < 11; i++ {
		if _, err := service.store.AddObservation(store.AddObservationParams{
			SessionID: "recall-follow-up",
			Project:   "engram",
			Scope:     "project",
			Type:      "decision",
			Title:     fmt.Sprintf("Follow up candidate %02d", i),
			Content:   "bounded project result",
		}); err != nil {
			t.Fatalf("seed candidate: %v", err)
		}
	}
	saveObservation(t, service, "other", "Follow up candidate foreign", "must stay outside the project")

	result, err := service.Recall(RecallInput{
		Query:           "Follow up candidate",
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
		Limit:           MaximumRecallCandidateLimit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v", err)
	}
	if result.ResultCount != MaximumRecallCandidateLimit {
		t.Fatalf("result_count = %d bytes=%d candidates=%#v, want %d", result.ResultCount, result.DeliveredUTF8Bytes, result.Candidates, MaximumRecallCandidateLimit)
	}
	if result.DeliveredUTF8Bytes > RecallCandidateBudgetBytes {
		t.Fatalf("delivered_utf8_bytes = %d, budget = %d", result.DeliveredUTF8Bytes, RecallCandidateBudgetBytes)
	}
	for _, candidate := range result.Candidates {
		if candidate.Project != "engram" || strings.Contains(candidate.Title, "foreign") {
			t.Fatalf("candidate widened scope: %#v", candidate)
		}
	}
}

func TestRecallCandidatesRequiresStrongOrExplicitAutomaticProjectAuthority(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-authority", nil }
	saveObservation(t, service, "engram", "Authority-aware recall", "weak automatic identity must not expose Memory")

	result, err := service.Recall(RecallInput{
		Query:           "Authority aware recall",
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthWeak,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v, want fail-open result", err)
	}
	if result.ResultCount != 0 || len(result.Candidates) != 0 {
		t.Fatalf("weak authority exposed candidates: %#v", result.Candidates)
	}
	if result.Warning == nil || result.Warning.Code != "recall_project_authority_required" {
		t.Fatalf("warning = %#v", result.Warning)
	}
	if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "weak_project_identity" {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
}

func TestRecallCandidatesExcludesInactiveDeletedAndSupersededMemories(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-eligibility", nil }

	active := saveObservation(t, service, "engram", "Eligibility recall active", "current active result")
	stale := saveObservation(t, service, "engram", "Eligibility recall stale", "review is overdue")
	deleted := saveObservation(t, service, "engram", "Eligibility recall deleted", "soft deleted result")
	old := saveObservation(t, service, "engram", "Eligibility recall old", "superseded result")
	current := saveObservation(t, service, "engram", "Eligibility recall current", "replacement result")

	if _, err := service.store.DB().Exec(`UPDATE observations SET review_after = datetime('now', '-1 day') WHERE id = ?`, stale.ID); err != nil {
		t.Fatalf("backdate review_after: %v", err)
	}
	if err := service.store.DeleteObservation(deleted.ID, false); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if _, err := service.Compare(CompareInput{
		MemoryIDA:  current.ID,
		MemoryIDB:  old.ID,
		Relation:   "supersedes",
		Confidence: 1,
		Reasoning:  "the current Memory replaces the old one",
		Model:      "test",
	}); err != nil {
		t.Fatalf("record supersedes relation: %v", err)
	}

	result, err := service.Recall(RecallInput{
		Query:           "Eligibility recall",
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
		Limit:           MaximumRecallCandidateLimit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v", err)
	}
	want := map[int64]bool{active.ID: true, current.ID: true}
	if result.ResultCount != len(want) {
		t.Fatalf("candidates = %#v, want only active and current", result.Candidates)
	}
	for _, candidate := range result.Candidates {
		if !want[candidate.ID] {
			t.Fatalf("ineligible candidate returned: %#v", candidate)
		}
	}
}

func TestRecallCandidatesFiltersEligibilityBeforeApplyingLimit(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-pre-limit", nil }
	stale := saveObservation(t, service, "engram", "prelimit prelimit prelimit", "prelimit")
	active := saveObservation(t, service, "engram", "eligible result", "prelimit")
	if _, err := service.store.DB().Exec(`UPDATE observations SET review_after = datetime('now', '-1 day') WHERE id = ?`, stale.ID); err != nil {
		t.Fatal(err)
	}

	result, err := service.Recall(RecallInput{
		Query: "prelimit", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCount != 1 || result.Candidates[0].ID != active.ID {
		t.Fatalf("candidates=%#v, want eligible id=%d", result.Candidates, active.ID)
	}
}

func TestRecallCandidatesReturnsSymmetricUnresolvedConflictWarnings(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-conflict", nil }
	first := saveObservation(t, service, "engram", "Conflict recall first", "keep SQLite")
	second := saveObservation(t, service, "engram", "Conflict recall second", "replace SQLite")

	if _, err := service.Compare(CompareInput{
		MemoryIDA:  first.ID,
		MemoryIDB:  second.ID,
		Relation:   "conflicts_with",
		Confidence: 1,
		Reasoning:  "the active Memories disagree",
		Model:      "test",
	}); err != nil {
		t.Fatalf("record conflict: %v", err)
	}

	result, err := service.Recall(RecallInput{
		Query:           "Conflict recall",
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v", err)
	}
	if result.ResultCount != 2 {
		t.Fatalf("candidates = %#v", result.Candidates)
	}
	for _, candidate := range result.Candidates {
		if len(candidate.Conflicts) != 1 || candidate.Conflicts[0].Status != "judged" || candidate.Conflicts[0].RelationID == "" {
			t.Fatalf("candidate %d conflicts = %#v", candidate.ID, candidate.Conflicts)
		}
		otherID := first.ID
		if candidate.ID == first.ID {
			otherID = second.ID
		}
		if candidate.Conflicts[0].MemoryID != otherID {
			t.Fatalf("candidate %d conflict target = %#v, want %d", candidate.ID, candidate.Conflicts[0], otherID)
		}
	}
}

func TestRecallCandidatesOnlyExposeEligibleInScopeConflictCounterparts(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-conflict-authority", nil }
	primary := saveObservation(t, service, "engram", "Authority boundary primary", "selected candidate")
	active := saveObservation(t, service, "engram", "safe active counterpart", "does not match the query")
	personal := saveObservation(t, service, "engram", "private personal counterpart", "does not match the query")
	stale := saveObservation(t, service, "engram", "stale counterpart", "does not match the query")
	deleted := saveObservation(t, service, "engram", "deleted counterpart", "does not match the query")
	superseded := saveObservation(t, service, "engram", "superseded counterpart", "does not match the query")
	replacement := saveObservation(t, service, "engram", "replacement counterpart", "does not match the query")
	if _, err := service.store.DB().Exec(`UPDATE observations SET scope = 'personal' WHERE id = ?`, personal.ID); err != nil {
		t.Fatal(err)
	}
	for _, counterpart := range []*store.Observation{active, personal, stale, deleted, superseded} {
		if _, err := service.Compare(CompareInput{
			MemoryIDA: primary.ID, MemoryIDB: counterpart.ID, Relation: "conflicts_with",
			Confidence: 1, Reasoning: "authority boundary fixture", Model: "test",
		}); err != nil {
			t.Fatalf("record conflict with %d: %v", counterpart.ID, err)
		}
	}
	if _, err := service.store.DB().Exec(`UPDATE observations SET review_after = datetime('now', '-1 day') WHERE id = ?`, stale.ID); err != nil {
		t.Fatal(err)
	}
	if err := service.store.DeleteObservation(deleted.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Compare(CompareInput{
		MemoryIDA: replacement.ID, MemoryIDB: superseded.ID, Relation: "supersedes",
		Confidence: 1, Reasoning: "replacement is current", Model: "test",
	}); err != nil {
		t.Fatal(err)
	}

	result, err := service.Recall(RecallInput{
		Query: "Authority boundary primary", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultCount != 1 || len(result.Candidates[0].Conflicts) != 1 {
		t.Fatalf("candidates=%#v, want exactly one safe conflict", result.Candidates)
	}
	conflict := result.Candidates[0].Conflicts[0]
	if conflict.MemoryID != active.ID || conflict.Title != active.Title || conflict.SyncID != active.SyncID {
		t.Fatalf("conflict=%#v, want active counterpart %#v", conflict, active)
	}
}

func TestRecallCandidatesRanksRelevanceBeforePinsAndRecency(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-ranking", nil }
	if err := service.store.CreateSession("recall-ranking", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	seed := func(title, content, updatedAt string, pinned bool) int64 {
		t.Helper()
		id, err := service.store.AddObservation(store.AddObservationParams{
			SessionID: "recall-ranking",
			Project:   "engram",
			Scope:     "project",
			Type:      "manual",
			Title:     title,
			Content:   content,
		})
		if err != nil {
			t.Fatalf("seed %q: %v", title, err)
		}
		if _, err := service.store.DB().Exec(`UPDATE observations SET updated_at = ? WHERE id = ?`, updatedAt, id); err != nil {
			t.Fatalf("set updated_at: %v", err)
		}
		if pinned {
			if err := service.store.PinObservation(id); err != nil {
				t.Fatalf("pin observation: %v", err)
			}
		}
		return id
	}

	mostRelevant := seed("rankingtoken rankingtoken", "aa", "2026-01-01 00:00:00", false)
	lowRelevantPinned := seed("content only pinned", "rankingtoken", "2026-12-01 00:00:00", true)
	pinnedTie := seed("rankingtoken alpha", "aa", "2026-01-02 00:00:00", true)
	unpinnedTie := seed("rankingtoken bravo", "bb", "2026-12-02 00:00:00", false)
	olderTie := seed("rankingtoken charl", "cc", "2026-01-03 00:00:00", false)
	newerTie := seed("rankingtoken delta", "dd", "2026-12-03 00:00:00", false)

	result, err := service.Recall(RecallInput{
		Query:           "rankingtoken",
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
		Limit:           MaximumRecallCandidateLimit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v", err)
	}
	position := map[int64]int{}
	for index, candidate := range result.Candidates {
		position[candidate.ID] = index
	}
	if position[mostRelevant] >= position[lowRelevantPinned] {
		t.Fatalf("pin overrode semantic relevance: %#v", result.ResultIDs)
	}
	if position[pinnedTie] >= position[unpinnedTie] {
		t.Fatalf("pin did not order the same relevance/currentness tier: %#v", result.ResultIDs)
	}
	if position[newerTie] >= position[olderTie] {
		t.Fatalf("recency was not the final tie-breaker: %#v", result.ResultIDs)
	}
}

func TestRecallCandidatesDefaultsToProjectScopeAndRequiresDeliberateBroadScope(t *testing.T) {
	service := newTestService(t)
	recallSequence := 0
	service.newRecallID = func() (string, error) {
		recallSequence++
		return fmt.Sprintf("recall-test-scope-%d", recallSequence), nil
	}
	if err := service.store.CreateSession("recall-scope", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	projectID, err := service.store.AddObservation(store.AddObservationParams{
		SessionID: "recall-scope", Project: "engram", Scope: "project", Type: "manual",
		Title: "Scope recall project", Content: "project Memory",
	})
	if err != nil {
		t.Fatalf("seed project Memory: %v", err)
	}
	personalID, err := service.store.AddObservation(store.AddObservationParams{
		SessionID: "recall-scope", Project: "engram", Scope: "personal", Type: "manual",
		Title: "Scope recall personal", Content: "personal Memory",
	})
	if err != nil {
		t.Fatalf("seed personal Memory: %v", err)
	}

	projectResult, err := service.Recall(RecallInput{
		Query: "Scope recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatalf("project Recall() error = %v", err)
	}
	if projectResult.ResultCount != 1 || projectResult.Candidates[0].ID != projectID {
		t.Fatalf("default scope candidates = %#v", projectResult.Candidates)
	}

	denied, err := service.Recall(RecallInput{
		Query: "Scope recall", Scope: "personal", AllProjects: true, ProjectStrength: project.IdentityStrengthAggregate,
	})
	if err != nil {
		t.Fatalf("broad Recall() error = %v", err)
	}
	if denied.Warning == nil || denied.ResultCount != 0 {
		t.Fatalf("implicit broad Recall was not denied: %#v", denied)
	}

	personalResult, err := service.Recall(RecallInput{
		Query: "Scope recall", Scope: "personal", AllProjects: true, ProjectStrength: project.IdentityStrengthAggregate, DeliberateScope: true,
	})
	if err != nil {
		t.Fatalf("deliberate personal Recall() error = %v", err)
	}
	if personalResult.ResultCount != 1 || personalResult.Candidates[0].ID != personalID {
		t.Fatalf("personal scope result = %#v", personalResult)
	}
}

func TestRecallCandidatesPropagatesCancellation(t *testing.T) {
	service := newTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := service.RecallContext(ctx, RecallInput{
		Query: "canceled recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("RecallContext() result=%#v error=%v, want context.Canceled", result, err)
	}
}

func TestRecallCandidatesOperationalFailureReturnsOneQuietWarningAndDiagnostics(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-test-failure", nil }
	if err := service.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	result, err := service.Recall(RecallInput{
		Query: "unavailable recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v, want fail-open result", err)
	}
	if result.ResultCount != 0 || len(result.Candidates) != 0 {
		t.Fatalf("failed Recall exposed candidates: %#v", result.Candidates)
	}
	if result.Warning == nil || result.Warning.Code != "recall_unavailable" {
		t.Fatalf("warning = %#v", result.Warning)
	}
	if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "recall_store_failure" || result.Diagnostics[0].Operation != "recall_candidates" {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
}

func TestRecallCandidatesIdentifierFailureFailsOpenWithOpaqueFallback(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "", errors.New("entropy unavailable") }
	service.newRecallFallbackID = func() string { return "recall-fallback-test" }

	result, err := service.Recall(RecallInput{
		Query: "identifier failure", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v, want fail-open result", err)
	}
	if result.RecallID != "recall-fallback-test" || result.ResultCount != 0 {
		t.Fatalf("result=%#v", result)
	}
	if result.Warning == nil || result.Warning.Code != "recall_unavailable" || len(result.Diagnostics) != 1 {
		t.Fatalf("warning=%#v diagnostics=%#v", result.Warning, result.Diagnostics)
	}
	if result.Diagnostics[0].Code != "recall_identifier_failure" || result.Diagnostics[0].Operation != "recall_identifier" {
		t.Fatalf("diagnostics=%#v", result.Diagnostics)
	}
}

func TestRecallCandidatesMissingStoreReturnsOneQuietWarningAndDiagnostics(t *testing.T) {
	result, err := New(nil).Recall(RecallInput{
		Query: "unavailable recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v, want fail-open result", err)
	}
	if result.ResultCount != 0 || result.Warning == nil || result.Warning.Code != "recall_unavailable" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "recall_store_failure" {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
}
