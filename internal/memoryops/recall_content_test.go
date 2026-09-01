package memoryops

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
)

func TestRecallContentReturnsSelectedMemoryWithinUTF8Budget(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-budget", nil }
	content := strings.Repeat("a", RecallContentBudgetBytes-1) + "🧠" + "tail"
	saveObservation(t, service, "engram", "Bounded complete Recall", content)

	recall, err := service.Recall(RecallInput{
		Query:           "Bounded complete Recall",
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatalf("Recall() error = %v", err)
	}
	if recall.ResultCount != 1 || recall.Candidates[0].ResultID == "" {
		t.Fatalf("Recall() candidates = %#v", recall.Candidates)
	}

	result, err := service.RecallContent(RecallContentInput{
		RecallID:        recall.RecallID,
		ResultID:        recall.Candidates[0].ResultID,
		Project:         "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatalf("RecallContent() error = %v", err)
	}
	if result.Warning != nil {
		t.Fatalf("RecallContent() warning = %#v diagnostics = %#v", result.Warning, result.Diagnostics)
	}
	if result.RecallID != recall.RecallID || result.ResultID != recall.Candidates[0].ResultID {
		t.Fatalf("Recall identity = %#v", result)
	}
	if result.Memory.Content != strings.Repeat("a", RecallContentBudgetBytes-1) {
		t.Fatalf("content bytes = %d, want %d", len(result.Memory.Content), RecallContentBudgetBytes-1)
	}
	if !utf8.ValidString(result.Memory.Content) {
		t.Fatal("content is not valid UTF-8")
	}
	if result.OriginalBytes != len(content) || result.DeliveredUTF8Bytes != RecallContentBudgetBytes-1 || result.LimitBytes != RecallContentBudgetBytes {
		t.Fatalf("byte metadata = original:%d delivered:%d limit:%d", result.OriginalBytes, result.DeliveredUTF8Bytes, result.LimitBytes)
	}
	if !result.Truncated || result.ContinuationPosition == nil || *result.ContinuationPosition != RecallContentBudgetBytes-1 {
		t.Fatalf("continuation metadata = truncated:%t position:%v", result.Truncated, result.ContinuationPosition)
	}
}

func TestRecallContentRequiresPositionedContinuationAndCompletesAtExactLimit(t *testing.T) {
	t.Run("positioned continuation", func(t *testing.T) {
		service := newTestService(t)
		service.newRecallID = func() (string, error) { return "recall-content-continuation", nil }
		content := strings.Repeat("a", RecallContentBudgetBytes) + "🧠continued"
		saveObservation(t, service, "engram", "Positioned complete Recall", content)
		recall, err := service.Recall(RecallInput{
			Query: "Positioned complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil {
			t.Fatal(err)
		}

		first, err := service.RecallContent(RecallContentInput{
			RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
			Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil || first.Warning != nil || first.ContinuationPosition == nil {
			t.Fatalf("first segment = %#v, error = %v", first, err)
		}
		second, err := service.RecallContent(RecallContentInput{
			RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
			Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
			Position: *first.ContinuationPosition,
		})
		if err != nil || second.Warning != nil {
			t.Fatalf("second segment = %#v, error = %v", second, err)
		}
		if second.Position != RecallContentBudgetBytes || second.Memory.Content != "🧠continued" || second.Truncated || second.ContinuationPosition != nil {
			t.Fatalf("second segment = %#v", second)
		}
	})

	t.Run("exact limit", func(t *testing.T) {
		service := newTestService(t)
		service.newRecallID = func() (string, error) { return "recall-content-exact", nil }
		content := strings.Repeat("x", RecallContentBudgetBytes)
		saveObservation(t, service, "engram", "Exact complete Recall", content)
		recall, err := service.Recall(RecallInput{
			Query: "Exact complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := service.RecallContent(RecallContentInput{
			RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
			Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil || result.Warning != nil {
			t.Fatalf("exact-limit result = %#v, error = %v", result, err)
		}
		if result.DeliveredUTF8Bytes != RecallContentBudgetBytes || result.Truncated || result.ContinuationPosition != nil {
			t.Fatalf("exact-limit metadata = %#v", result)
		}
	})
}

func TestRecallContentRejectsInvalidUTF8Positions(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-position", nil }
	content := "a🧠z"
	saveObservation(t, service, "engram", "Invalid Recall position", content)
	recall, err := service.Recall(RecallInput{
		Query: "Invalid Recall position", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}

	positions := []struct {
		name     string
		position int
	}{
		{name: "negative", position: -1},
		{name: "inside multibyte rune", position: 2},
		{name: "at end", position: len(content)},
		{name: "past end", position: len(content) + 1},
	}
	for _, test := range positions {
		t.Run(test.name, func(t *testing.T) {
			result, err := service.RecallContent(RecallContentInput{
				RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
				Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
				Position: test.position,
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Warning == nil || result.Warning.Code != "recall_position_invalid" || result.Memory.Content != "" || len(result.Diagnostics) != 1 {
				t.Fatalf("position %d result = %#v", test.position, result)
			}
		})
	}
}

func TestRecallContentRejectsPositionNotReturnedByPreviousSegment(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-unissued-position", nil }
	content := strings.Repeat("a", RecallContentBudgetBytes+20)
	saveObservation(t, service, "engram", "Unissued Recall position", content)
	recall, err := service.Recall(RecallInput{
		Query: "Unissued Recall position", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := service.RecallContent(RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		Position: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Warning == nil || result.Warning.Code != "recall_position_invalid" || result.Memory.Content != "" || result.Diagnostics[0].Code != "recall_position_invalid" {
		t.Fatalf("unissued position result = %#v", result)
	}
}

func TestRecallContentValidatesSelectionScopeAndCurrentProjectAuthority(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-authority", nil }
	saveObservation(t, service, "engram", "Authority-bound complete Recall", "private selected content")
	recall, err := service.Recall(RecallInput{
		Query: "Authority-bound complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	resultID := recall.Candidates[0].ResultID

	tests := []struct {
		name        string
		input       RecallContentInput
		warningCode string
	}{
		{
			name:        "weak current project authority",
			input:       RecallContentInput{RecallID: recall.RecallID, ResultID: resultID, Project: "engram", ProjectStrength: project.IdentityStrengthWeak},
			warningCode: "recall_project_authority_required",
		},
		{
			name:        "different project",
			input:       RecallContentInput{RecallID: recall.RecallID, ResultID: resultID, Project: "other", ProjectStrength: project.IdentityStrengthExplicit},
			warningCode: "recall_selection_invalid",
		},
		{
			name:        "widened scope",
			input:       RecallContentInput{RecallID: recall.RecallID, ResultID: resultID, Scope: "global", AllProjects: true, DeliberateScope: true, ProjectStrength: project.IdentityStrengthAggregate},
			warningCode: "recall_selection_invalid",
		},
		{
			name:        "unrelated result",
			input:       RecallContentInput{RecallID: recall.RecallID, ResultID: "result-unrelated", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit},
			warningCode: "recall_selection_invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := service.RecallContent(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if result.Warning == nil || result.Warning.Code != test.warningCode || result.Memory.Content != "" || len(result.Diagnostics) != 1 {
				t.Fatalf("RecallContent() = %#v", result)
			}
		})
	}
}

func TestRecallContentRejectsDeletedOrChangedSelectedMemory(t *testing.T) {
	t.Run("deleted", func(t *testing.T) {
		service := newTestService(t)
		service.newRecallID = func() (string, error) { return "recall-content-deleted", nil }
		memory := saveObservation(t, service, "engram", "Deleted complete Recall", "selected content")
		recall, err := service.Recall(RecallInput{
			Query: "Deleted complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := service.store.DeleteObservation(memory.ID, false); err != nil {
			t.Fatal(err)
		}
		result, err := service.RecallContent(RecallContentInput{
			RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
			Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil || result.Warning == nil || result.Diagnostics[0].Code != "recall_selection_unavailable" || result.Memory.Content != "" {
			t.Fatalf("deleted selection result = %#v, error = %v", result, err)
		}
	})

	t.Run("changed", func(t *testing.T) {
		service := newTestService(t)
		service.newRecallID = func() (string, error) { return "recall-content-changed", nil }
		memory := saveObservation(t, service, "engram", "Changed complete Recall", "selected content")
		recall, err := service.Recall(RecallInput{
			Query: "Changed complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil {
			t.Fatal(err)
		}
		changed := "new authoritative content"
		if _, err := service.store.UpdateObservation(memory.ID, store.UpdateObservationParams{Content: &changed}); err != nil {
			t.Fatal(err)
		}
		result, err := service.RecallContent(RecallContentInput{
			RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
			Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
		})
		if err != nil || result.Warning == nil || result.Diagnostics[0].Code != "recall_memory_changed" || result.Memory.Content != "" {
			t.Fatalf("changed selection result = %#v, error = %v", result, err)
		}
	})
}

func TestRecordRecallSegmentRevalidatesScopeAtCommit(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-scope-commit", nil }
	memory := saveObservation(t, service, "engram", "Scope commit boundary", "selected content")
	recall, err := service.Recall(RecallInput{
		Query: "Scope commit boundary", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := service.store.RecallSelectionContext(context.Background(), recall.RecallID, recall.Candidates[0].ResultID, "engram", "project", false)
	if err != nil {
		t.Fatal(err)
	}
	personal := "personal"
	if _, err := service.store.UpdateObservation(memory.ID, store.UpdateObservationParams{Scope: &personal}); err != nil {
		t.Fatal(err)
	}
	// Hold the selected revision constant so this test isolates the authority
	// predicate from the separate revision predicate.
	if _, err := service.store.DB().Exec(`UPDATE observations SET revision_count = ? WHERE id = ?`, selection.RevisionCount, memory.ID); err != nil {
		t.Fatal(err)
	}

	replayed, err := service.store.RecordRecallSegment(store.RecallSegmentRecord{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		ObservationID: memory.ID, RevisionCount: selection.RevisionCount,
		LocalRevisionCount: selection.LocalRevisionCount,
		Position:           0, OriginalBytes: len(memory.Content), DeliveredBytes: len(memory.Content),
		LimitBytes: RecallContentBudgetBytes,
	})
	if replayed || !errors.Is(err, store.ErrRecallSelectionUnavailable) {
		t.Fatalf("scope-changed segment replayed=%t error=%v", replayed, err)
	}
	var segments int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM recall_segments WHERE recall_id = ?`, recall.RecallID).Scan(&segments); err != nil {
		t.Fatal(err)
	}
	if segments != 0 {
		t.Fatalf("scope-changed selection recorded %d segments", segments)
	}
}

func TestRecallContentExactSegmentReplayIsIdempotent(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-replay", nil }
	saveObservation(t, service, "engram", "Replayed complete Recall", strings.Repeat("r", RecallContentBudgetBytes+20))
	recall, err := service.Recall(RecallInput{
		Query: "Replayed complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	}
	first, err := service.RecallContent(input)
	if err != nil || first.Warning != nil || first.Replayed {
		t.Fatalf("first segment = %#v, error = %v", first, err)
	}
	second, err := service.RecallContent(input)
	if err != nil || second.Warning != nil || !second.Replayed {
		t.Fatalf("replayed segment = %#v, error = %v", second, err)
	}
	if first.Memory.Content != second.Memory.Content || first.DeliveredUTF8Bytes != second.DeliveredUTF8Bytes || *first.ContinuationPosition != *second.ContinuationPosition {
		t.Fatalf("replay changed segment: first=%#v second=%#v", first, second)
	}
	var segmentCount int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM recall_segments WHERE recall_id = ? AND result_id = ?`, input.RecallID, input.ResultID).Scan(&segmentCount); err != nil {
		t.Fatal(err)
	}
	if segmentCount != 1 {
		t.Fatalf("persisted segment count = %d, want 1", segmentCount)
	}
}

func TestRecallContentPersistsLatencyOnlyAfterPrimarySegmentPersistence(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-persisted-latency", nil }
	saveObservation(t, service, "engram", "Persisted segment latency", "primary segment persistence precedes completion measurement")
	recall, err := service.Recall(RecallInput{
		Query: "primary segment persistence precedes completion measurement", Project: "engram",
		ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil || recall.ResultCount != 1 {
		t.Fatalf("Recall result = %#v, err=%v", recall, err)
	}
	service.recallElapsed = func(time.Time) time.Duration {
		var persisted int
		if err := service.store.DB().QueryRow(`
			SELECT COUNT(*) FROM recall_segments
			WHERE recall_id = ? AND result_id = ? AND position = 0`,
			recall.RecallID, recall.Candidates[0].ResultID).Scan(&persisted); err != nil {
			t.Fatalf("inspect persisted Recall segment: %v", err)
		}
		if persisted == 0 {
			return 7 * time.Millisecond
		}
		return 43 * time.Millisecond
	}
	result, err := service.RecallContent(RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil || result.Warning != nil || result.ElapsedMonotonicMS != 43 {
		t.Fatalf("Recall content result = %#v, err=%v", result, err)
	}
	var persistedElapsed int64
	if err := service.store.DB().QueryRow(`
		SELECT elapsed_monotonic_ms FROM recall_segments
		WHERE recall_id = ? AND result_id = ? AND position = 0`,
		result.RecallID, result.ResultID).Scan(&persistedElapsed); err != nil {
		t.Fatalf("load completed Recall segment latency: %v", err)
	}
	if persistedElapsed != result.ElapsedMonotonicMS {
		t.Fatalf("persisted segment latency = %d, result=%d", persistedElapsed, result.ElapsedMonotonicMS)
	}
}

func TestRecallContentPreservesDeliveredSegmentWhenMetricCompletionFails(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-metric-completion-failure", nil }
	content := "a committed complete Memory segment remains delivered"
	saveObservation(t, service, "engram", "Delivered content despite metric failure", content)
	recall, err := service.Recall(RecallInput{
		Query: content, Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil || recall.ResultCount != 1 {
		t.Fatalf("Recall result = %#v, err=%v", recall, err)
	}
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_recall_content_metric_completion
		BEFORE UPDATE OF elapsed_monotonic_ms ON recall_segments
		BEGIN
			SELECT RAISE(ABORT, 'forced Recall content metric completion failure');
		END`); err != nil {
		t.Fatalf("create Recall content metric failure trigger: %v", err)
	}
	result, err := service.RecallContent(RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil || result.Warning != nil || result.Memory.Content != content || result.DeliveredUTF8Bytes != len(content) {
		t.Fatalf("Recall content delivery = %#v, err=%v", result, err)
	}
	if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "recall_metrics_unavailable" ||
		result.Diagnostics[0].Operation != "recall_content_metrics" {
		t.Fatalf("Recall content metric diagnostic = %#v", result.Diagnostics)
	}
	var elapsed any
	if err := service.store.DB().QueryRow(`
		SELECT elapsed_monotonic_ms FROM recall_segments
		WHERE recall_id = ? AND result_id = ? AND position = 0`, result.RecallID, result.ResultID).Scan(&elapsed); err != nil {
		t.Fatalf("load pending Recall content latency: %v", err)
	}
	if elapsed != nil {
		t.Fatalf("failed Recall content completion persisted latency: %v", elapsed)
	}
	report, err := service.RecallFeedbackReport()
	if err != nil {
		t.Fatalf("report pending Recall content metrics: %v", err)
	}
	get := findRecallOperationReport(t, report.Operations, RecallFeedbackOperationGet)
	if get.Events != 1 || get.TotalExposedResults != 1 || get.TotalUTF8Bytes != int64(len(content)) ||
		get.LatencySamples != 0 || get.UnknownLatency != 1 {
		t.Fatalf("pending Recall content metric report = %#v", get)
	}
}

func TestRecallSegmentReplayRevalidatesCurrentMemoryAtomically(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-replay-race", nil }
	content := strings.Repeat("r", RecallContentBudgetBytes+20)
	memory := saveObservation(t, service, "engram", "Replay race complete Recall", content)
	recall, err := service.Recall(RecallInput{
		Query: "Replay race complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	}
	selection, err := service.store.RecallSelectionContext(context.Background(), recall.RecallID, recall.Candidates[0].ResultID, "engram", "project", false)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.RecallContent(input)
	if err != nil || first.Warning != nil {
		t.Fatalf("first segment = %#v, error = %v", first, err)
	}
	changed := "authoritative replacement"
	if _, err := service.store.UpdateObservation(memory.ID, store.UpdateObservationParams{Content: &changed}); err != nil {
		t.Fatal(err)
	}

	_, err = service.store.RecordRecallSegment(store.RecallSegmentRecord{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		ObservationID: memory.ID, RevisionCount: memory.RevisionCount,
		LocalRevisionCount: selection.LocalRevisionCount, Position: 0,
		OriginalBytes: len(content), DeliveredBytes: first.DeliveredUTF8Bytes,
		LimitBytes: first.LimitBytes, Truncated: first.Truncated,
		ContinuationPosition: first.ContinuationPosition,
	})
	if !errors.Is(err, store.ErrRecallSelectionUnavailable) {
		t.Fatalf("replayed stale segment error = %v", err)
	}
}

func TestRecallContentRejectsDivergentSyncApplyWithSameRevisionCount(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-divergent-sync", nil }
	memory := saveObservation(t, service, "engram", "Selected replica revision", "replica A content")
	recall, err := service.Recall(RecallInput{
		Query: "Selected replica revision", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"sync_id": memory.SyncID, "session_id": memory.SessionID,
		"type": memory.Type, "title": memory.Title, "content": "replica B divergent content",
		"project": "engram", "scope": memory.Scope,
		"revision_count": memory.RevisionCount, "duplicate_count": memory.DuplicateCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.store.ApplyPulledMutation(store.DefaultSyncTargetKey, store.SyncMutation{
		Seq: 1, TargetKey: store.DefaultSyncTargetKey, Entity: store.SyncEntityObservation,
		EntityKey: memory.SyncID, Op: store.SyncOpUpsert, Payload: string(payload),
	}); err != nil {
		t.Fatal(err)
	}

	result, err := service.RecallContent(RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Warning == nil || result.Diagnostics[0].Code != "recall_memory_changed" || result.Memory.Content != "" {
		t.Fatalf("same-revision divergent sync result = %#v", result)
	}
	var segments int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM recall_segments WHERE recall_id = ?`, recall.RecallID).Scan(&segments); err != nil {
		t.Fatal(err)
	}
	if segments != 0 {
		t.Fatalf("same-revision divergent sync recorded %d segments", segments)
	}
}

func TestRecallContentCancellationFailsOpenWithoutPartialSegment(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-canceled", nil }
	saveObservation(t, service, "engram", "Canceled complete Recall", "selected content")
	recall, err := service.Recall(RecallInput{
		Query: "Canceled complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := service.RecallContentContext(ctx, RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Warning == nil || result.Warning.Code != "recall_canceled" || result.Diagnostics[0].Code != "recall_canceled" || result.Memory.Content != "" {
		t.Fatalf("canceled result = %#v", result)
	}
	var segmentCount int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM recall_segments WHERE recall_id = ?`, recall.RecallID).Scan(&segmentCount); err != nil {
		t.Fatal(err)
	}
	if segmentCount != 0 {
		t.Fatalf("persisted segment count = %d, want 0", segmentCount)
	}
}

func TestRecallContentStoreFailureFailsOpenWithoutPartialSegment(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-store-failure", nil }
	saveObservation(t, service, "engram", "Failed complete Recall", "selected content")
	recall, err := service.Recall(RecallInput{
		Query: "Failed complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_recall_segment
		BEFORE INSERT ON recall_segments
		BEGIN
			SELECT RAISE(ABORT, 'forced recall segment failure');
		END;
	`); err != nil {
		t.Fatal(err)
	}
	result, err := service.RecallContent(RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Warning == nil || result.Warning.Code != "recall_unavailable" || result.Diagnostics[0].Code != "recall_store_failure" || result.Memory.Content != "" {
		t.Fatalf("Store failure result = %#v", result)
	}
	var segmentCount int
	if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM recall_segments WHERE recall_id = ?`, recall.RecallID).Scan(&segmentCount); err != nil {
		t.Fatal(err)
	}
	if segmentCount != 0 {
		t.Fatalf("persisted segment count = %d, want 0", segmentCount)
	}
}

func TestRecallRunPersistenceFailureRollsBackRunAndResults(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-run-rollback", nil }
	saveObservation(t, service, "engram", "Atomic Recall first", "selected first")
	saveObservation(t, service, "engram", "Atomic Recall second", "selected second")
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_second_recall_result
		BEFORE INSERT ON recall_results
		WHEN NEW.result_rank = 1
		BEGIN
			SELECT RAISE(ABORT, 'forced second recall result failure');
		END;
	`); err != nil {
		t.Fatal(err)
	}

	result, err := service.Recall(RecallInput{
		Query: "Atomic Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Warning == nil || result.Warning.Code != "recall_unavailable" || result.ResultCount != 0 || len(result.Candidates) != 0 {
		t.Fatalf("Recall() = %#v", result)
	}
	for _, table := range []string{"recall_runs", "recall_results"} {
		var count int
		if err := service.store.DB().QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE recall_id = ?`, result.RecallID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d, want 0", table, count)
		}
	}
}

func TestRecallContentContinuationAndReplaySurviveStoreReopen(t *testing.T) {
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	cfg.DataDir = t.TempDir()
	firstStore, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	firstService := New(firstStore)
	firstService.newRecallID = func() (string, error) { return "recall-content-reopen", nil }
	saveObservation(t, firstService, "engram", "Reopened complete Recall", strings.Repeat("p", RecallContentBudgetBytes)+"persisted continuation")
	recall, err := firstService.Recall(RecallInput{
		Query: "Reopened complete Recall", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	}
	first, err := firstService.RecallContent(input)
	if err != nil || first.Warning != nil || first.ContinuationPosition == nil {
		t.Fatalf("first segment = %#v, error = %v", first, err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopenedService := New(reopenedStore)
	replayed, err := reopenedService.RecallContent(input)
	if err != nil || replayed.Warning != nil || !replayed.Replayed || replayed.Memory.Content != first.Memory.Content {
		t.Fatalf("reopened replay = %#v, error = %v", replayed, err)
	}
	input.Position = *first.ContinuationPosition
	continuation, err := reopenedService.RecallContent(input)
	if err != nil || continuation.Warning != nil || continuation.Memory.Content != "persisted continuation" || continuation.Truncated {
		t.Fatalf("reopened continuation = %#v, error = %v", continuation, err)
	}
}

func TestRecallOperationalTablesPersistNoQueryOrMemoryContent(t *testing.T) {
	service := newTestService(t)
	service.newRecallID = func() (string, error) { return "recall-content-free", nil }
	secretContent := "selected content must remain only in Memory"
	saveObservation(t, service, "engram", "Content-free Recall operation", secretContent)
	recall, err := service.Recall(RecallInput{
		Query: "Content-free Recall operation", Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RecallContent(RecallContentInput{
		RecallID: recall.RecallID, ResultID: recall.Candidates[0].ResultID,
		Project: "engram", ProjectStrength: project.IdentityStrengthExplicit,
	}); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"recall_runs", "recall_results", "recall_segments"} {
		rows, err := service.store.DB().Query(`PRAGMA table_info(` + table + `)`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var cid, notNull, primaryKey int
			var name, columnType string
			var defaultValue any
			if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if name == "query" || name == "content" || name == "content_hash" {
				rows.Close()
				t.Fatalf("%s persists forbidden %s column", table, name)
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var captured int
	if err := service.store.DB().QueryRow(`
		SELECT COUNT(*) FROM recall_runs run
		JOIN recall_results result ON result.recall_id = run.recall_id
		JOIN recall_segments segment ON segment.recall_id = result.recall_id AND segment.result_id = result.result_id
		WHERE run.recall_id = ? AND run.project = ?`, recall.RecallID, secretContent).Scan(&captured); err != nil {
		t.Fatal(err)
	}
	if captured != 0 {
		t.Fatal("Recall operational state persisted Memory content")
	}
}
