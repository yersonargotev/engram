package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestRecoverDeferred_AppliesDeadRelationLocally(t *testing.T) {
	s, sourceID, targetID := setupSyncApplyStore(t)
	syncID := newSyncID("rel-recover")
	payload, err := json.Marshal(syncRelationPayload{
		SyncID:         syncID,
		SourceID:       sourceID,
		TargetID:       targetID,
		Relation:       RelationRelated,
		JudgmentStatus: JudgmentStatusJudged,
		Project:        "proj-apply",
		CreatedAt:      "2026-08-16T10:00:00Z",
		UpdatedAt:      "2026-08-16T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal relation payload: %v", err)
	}

	insertDeferredRow(t, s, syncID, SyncEntityRelation, string(payload), 7, "dead")
	if _, err := s.db.Exec(`
		UPDATE sync_apply_deferred
		SET last_error = 'historical failure', last_attempted_at = '2026-08-15 10:00:00'
		WHERE sync_id = ?
	`, syncID); err != nil {
		t.Fatalf("seed deferred history: %v", err)
	}

	var outboundBefore int
	if err := s.db.QueryRow(`SELECT count(*) FROM sync_mutations`).Scan(&outboundBefore); err != nil {
		t.Fatalf("count outbound before recovery: %v", err)
	}

	result, err := s.RecoverDeferred(syncID)
	if err != nil {
		t.Fatalf("RecoverDeferred: %v", err)
	}
	if result.SyncID != syncID || result.Status != "applied" || result.Result != "recovered" {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
	if got := countRelationRows(t, s, syncID); got != 1 {
		t.Fatalf("relation rows = %d, want 1", got)
	}

	row, err := s.GetDeferred(syncID)
	if err != nil {
		t.Fatalf("GetDeferred: %v", err)
	}
	if row.ApplyStatus != "applied" {
		t.Errorf("apply status = %q, want applied", row.ApplyStatus)
	}
	if row.RetryCount != 7 {
		t.Errorf("retry count = %d, want 7", row.RetryCount)
	}
	if row.LastError == nil || *row.LastError != "historical failure" {
		t.Errorf("last error = %v, want historical failure", row.LastError)
	}
	if row.LastAttemptedAt == nil || *row.LastAttemptedAt == "2026-08-15 10:00:00" {
		t.Errorf("last attempted at was not updated: %v", row.LastAttemptedAt)
	}

	var outboundAfter int
	if err := s.db.QueryRow(`SELECT count(*) FROM sync_mutations`).Scan(&outboundAfter); err != nil {
		t.Fatalf("count outbound after recovery: %v", err)
	}
	if outboundAfter != outboundBefore {
		t.Fatalf("outbound mutations changed: before=%d after=%d", outboundBefore, outboundAfter)
	}
}

type deferredRowSnapshot struct {
	Entity          string
	Payload         string
	ApplyStatus     string
	RetryCount      int
	LastError       *string
	LastAttemptedAt *string
	FirstSeenAt     string
}

func snapshotDeferredRow(t *testing.T, s *Store, syncID string) deferredRowSnapshot {
	t.Helper()
	row, err := s.GetDeferred(syncID)
	if err != nil {
		t.Fatalf("GetDeferred(%q): %v", syncID, err)
	}
	return deferredRowSnapshot{
		Entity:          row.Entity,
		Payload:         row.PayloadRaw,
		ApplyStatus:     row.ApplyStatus,
		RetryCount:      row.RetryCount,
		LastError:       row.LastError,
		LastAttemptedAt: row.LastAttemptedAt,
		FirstSeenAt:     row.FirstSeenAt,
	}
}

func TestRecoverDeferred_FailuresLeaveQueueRowUnchanged(t *testing.T) {
	t.Run("invalid payload", func(t *testing.T) {
		s := newTestStore(t)
		syncID := newSyncID("rel-invalid-recovery")
		insertDeferredRow(t, s, syncID, SyncEntityRelation, "not-json", 11, "dead")
		before := snapshotDeferredRow(t, s, syncID)

		_, err := s.RecoverDeferred(syncID)
		assertDeferredRecoveryFailure(t, err, "invalid_payload")
		if after := snapshotDeferredRow(t, s, syncID); !reflect.DeepEqual(after, before) {
			t.Fatalf("queue row changed:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	t.Run("payload identity mismatch", func(t *testing.T) {
		s, sourceID, targetID := setupSyncApplyStore(t)
		syncID := newSyncID("rel-identity-recovery")
		payload, err := json.Marshal(syncRelationPayload{
			SyncID:         newSyncID("rel-other"),
			SourceID:       sourceID,
			TargetID:       targetID,
			Relation:       RelationRelated,
			JudgmentStatus: JudgmentStatusJudged,
			Project:        "proj-apply",
		})
		if err != nil {
			t.Fatalf("marshal relation payload: %v", err)
		}
		insertDeferredRow(t, s, syncID, SyncEntityRelation, string(payload), 1, "dead")
		before := snapshotDeferredRow(t, s, syncID)

		_, err = s.RecoverDeferred(syncID)
		assertDeferredRecoveryFailure(t, err, "invalid_payload")
		if after := snapshotDeferredRow(t, s, syncID); !reflect.DeepEqual(after, before) {
			t.Fatalf("queue row changed:\nbefore=%+v\nafter=%+v", before, after)
		}
		if got := countRelationRows(t, s, syncID); got != 0 {
			t.Fatalf("relation rows = %d, want 0", got)
		}
	})

	t.Run("dependency missing", func(t *testing.T) {
		s, sourceID, _ := setupSyncApplyStore(t)
		syncID := newSyncID("rel-missing-recovery")
		payload, err := json.Marshal(syncRelationPayload{
			SyncID:         syncID,
			SourceID:       sourceID,
			TargetID:       "obs-missing-recovery",
			Relation:       RelationRelated,
			JudgmentStatus: JudgmentStatusJudged,
			Project:        "proj-apply",
		})
		if err != nil {
			t.Fatalf("marshal relation payload: %v", err)
		}
		insertDeferredRow(t, s, syncID, SyncEntityRelation, string(payload), 0, "dead")
		before := snapshotDeferredRow(t, s, syncID)

		_, err = s.RecoverDeferred(syncID)
		assertDeferredRecoveryFailure(t, err, "dependency_missing")
		if after := snapshotDeferredRow(t, s, syncID); !reflect.DeepEqual(after, before) {
			t.Fatalf("queue row changed:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	t.Run("transaction commit", func(t *testing.T) {
		s, sourceID, targetID := setupSyncApplyStore(t)
		syncID := newSyncID("rel-commit-recovery")
		payload, err := json.Marshal(syncRelationPayload{
			SyncID:         syncID,
			SourceID:       sourceID,
			TargetID:       targetID,
			Relation:       RelationRelated,
			JudgmentStatus: JudgmentStatusJudged,
			Project:        "proj-apply",
		})
		if err != nil {
			t.Fatalf("marshal relation payload: %v", err)
		}
		insertDeferredRow(t, s, syncID, SyncEntityRelation, string(payload), 3, "dead")
		before := snapshotDeferredRow(t, s, syncID)
		originalCommit := s.hooks.commit
		s.hooks.commit = func(*sql.Tx) error { return errors.New("injected commit failure") }

		_, err = s.RecoverDeferred(syncID)
		s.hooks.commit = originalCommit
		assertDeferredRecoveryFailure(t, err, "apply_failed")
		if after := snapshotDeferredRow(t, s, syncID); !reflect.DeepEqual(after, before) {
			t.Fatalf("queue row changed:\nbefore=%+v\nafter=%+v", before, after)
		}
		if got := countRelationRows(t, s, syncID); got != 0 {
			t.Fatalf("relation rows = %d after rolled-back recovery, want 0", got)
		}
	})
}

func assertDeferredRecoveryFailure(t *testing.T, err error, reason string) {
	t.Helper()
	if !errors.Is(err, ErrDeferredRecoveryFailed) {
		t.Fatalf("error = %v, want ErrDeferredRecoveryFailed", err)
	}
	var recoveryErr *DeferredRecoveryError
	if !errors.As(err, &recoveryErr) {
		t.Fatalf("error type = %T, want *DeferredRecoveryError", err)
	}
	if recoveryErr.Reason != reason {
		t.Fatalf("reason = %q, want %q (%v)", recoveryErr.Reason, reason, err)
	}
	if recoveryErr.Error() == fmt.Sprintf("%v", ErrDeferredRecoveryFailed) {
		t.Fatal("recovery error should retain diagnostic context")
	}
}

func TestRecoverDeferred_StateContract(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		s := newTestStore(t)
		_, err := s.RecoverDeferred("rel-unknown")
		if !errors.Is(err, ErrDeferredNotFound) {
			t.Fatalf("error = %v, want ErrDeferredNotFound", err)
		}
	})

	t.Run("deferred is ineligible regardless of retry count", func(t *testing.T) {
		s := newTestStore(t)
		syncID := newSyncID("rel-still-deferred")
		insertDeferredRow(t, s, syncID, SyncEntityRelation, "{}", 99, "deferred")
		before := snapshotDeferredRow(t, s, syncID)

		_, err := s.RecoverDeferred(syncID)
		if !errors.Is(err, ErrInvalidRecoveryState) {
			t.Fatalf("error = %v, want ErrInvalidRecoveryState", err)
		}
		var recoveryErr *DeferredRecoveryError
		if !errors.As(err, &recoveryErr) || recoveryErr.Status != "deferred" {
			t.Fatalf("error = %#v, want status deferred", err)
		}
		if after := snapshotDeferredRow(t, s, syncID); !reflect.DeepEqual(after, before) {
			t.Fatalf("queue row changed:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	t.Run("unsupported entity", func(t *testing.T) {
		s := newTestStore(t)
		syncID := newSyncID("obs-dead")
		insertDeferredRow(t, s, syncID, SyncEntityObservation, "{}", 0, "dead")
		before := snapshotDeferredRow(t, s, syncID)

		_, err := s.RecoverDeferred(syncID)
		if !errors.Is(err, ErrUnsupportedDeferredEntity) {
			t.Fatalf("error = %v, want ErrUnsupportedDeferredEntity", err)
		}
		if after := snapshotDeferredRow(t, s, syncID); !reflect.DeepEqual(after, before) {
			t.Fatalf("queue row changed:\nbefore=%+v\nafter=%+v", before, after)
		}
	})

	t.Run("applied is an idempotent repeat", func(t *testing.T) {
		s := newTestStore(t)
		syncID := newSyncID("rel-applied")
		insertDeferredRow(t, s, syncID, SyncEntityRelation, "{}", 4, "applied")
		before := snapshotDeferredRow(t, s, syncID)

		result, err := s.RecoverDeferred(syncID)
		if err != nil {
			t.Fatalf("RecoverDeferred: %v", err)
		}
		if result.SyncID != syncID || result.Status != "applied" || result.Result != "already_recovered" {
			t.Fatalf("unexpected recovery result: %+v", result)
		}
		if after := snapshotDeferredRow(t, s, syncID); !reflect.DeepEqual(after, before) {
			t.Fatalf("queue row changed:\nbefore=%+v\nafter=%+v", before, after)
		}
	})
}

func TestRecoverDeferred_ConcurrentAttemptsApplyExactlyOnce(t *testing.T) {
	s, sourceID, targetID := setupSyncApplyStore(t)
	second, err := New(s.cfg)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	syncID := newSyncID("rel-concurrent-recovery")
	payload, err := json.Marshal(syncRelationPayload{
		SyncID:         syncID,
		SourceID:       sourceID,
		TargetID:       targetID,
		Relation:       RelationRelated,
		JudgmentStatus: JudgmentStatusJudged,
		Project:        "proj-apply",
	})
	if err != nil {
		t.Fatalf("marshal relation payload: %v", err)
	}
	insertDeferredRow(t, s, syncID, SyncEntityRelation, string(payload), 5, "dead")
	if _, err := s.db.Exec(`
		CREATE TABLE recovery_apply_audit (sync_id TEXT NOT NULL);
		CREATE TRIGGER audit_recovery_relation_insert
		AFTER INSERT ON memory_relations
		WHEN new.sync_id = '` + syncID + `'
		BEGIN
			INSERT INTO recovery_apply_audit(sync_id) VALUES (new.sync_id);
		END;
	`); err != nil {
		t.Fatalf("create recovery audit trigger: %v", err)
	}

	stores := []*Store{s, second}
	results := make(chan DeferredRecoveryResult, len(stores))
	errs := make(chan error, len(stores))
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(len(stores))
	for _, candidate := range stores {
		go func(candidate *Store) {
			ready.Done()
			<-start
			result, err := candidate.RecoverDeferred(syncID)
			results <- result
			errs <- err
		}(candidate)
	}
	ready.Wait()
	close(start)

	resultCounts := map[string]int{}
	for range stores {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent recovery: %v", err)
		}
		resultCounts[(<-results).Result]++
	}
	if resultCounts["recovered"] != 1 || resultCounts["already_recovered"] != 1 {
		t.Fatalf("result counts = %v, want one recovered and one already_recovered", resultCounts)
	}

	var applyCount int
	if err := s.db.QueryRow(`SELECT count(*) FROM recovery_apply_audit WHERE sync_id = ?`, syncID).Scan(&applyCount); err != nil {
		t.Fatalf("count recovery applies: %v", err)
	}
	if applyCount != 1 {
		t.Fatalf("semantic apply count = %d, want 1", applyCount)
	}
}
