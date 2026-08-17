package store

import (
	"encoding/json"
	"testing"
)

func TestApplyPulledMutation_ReplacesTerminalDeferredEpisode(t *testing.T) {
	terminalStatuses := []string{"dead", "applied"}
	failures := []struct {
		name       string
		payload    func(t *testing.T, syncID, sourceID string) string
		wantStatus string
		wantError  bool
	}{
		{
			name: "missing dependency starts deferred episode",
			payload: func(t *testing.T, syncID, sourceID string) string {
				t.Helper()
				payload, err := json.Marshal(syncRelationPayload{
					SyncID:         syncID,
					SourceID:       sourceID,
					TargetID:       "obs-new-missing-dependency",
					Relation:       RelationRelated,
					JudgmentStatus: JudgmentStatusJudged,
					Project:        "proj-apply",
				})
				if err != nil {
					t.Fatalf("marshal relation payload: %v", err)
				}
				return string(payload)
			},
			wantStatus: "deferred",
		},
		{
			name: "invalid payload starts dead episode",
			payload: func(t *testing.T, _, _ string) string {
				t.Helper()
				return "new invalid payload"
			},
			wantStatus: "dead",
			wantError:  true,
		},
	}

	for _, terminalStatus := range terminalStatuses {
		for _, failure := range failures {
			t.Run(terminalStatus+"/"+failure.name, func(t *testing.T) {
				s, sourceID, _ := setupSyncApplyStore(t)
				if err := s.ensureSyncState(DefaultSyncTargetKey); err != nil {
					t.Fatalf("ensureSyncState: %v", err)
				}
				syncID := newSyncID("rel-new-episode")
				insertDeferredRow(t, s, syncID, SyncEntityRelation, "old payload", 12, terminalStatus)
				if _, err := s.db.Exec(`
					UPDATE sync_apply_deferred
					SET last_error = 'old error',
					    last_attempted_at = '2026-08-01 01:02:03',
					    first_seen_at = '2026-08-01 01:01:01'
					WHERE sync_id = ?
				`, syncID); err != nil {
					t.Fatalf("seed terminal episode: %v", err)
				}

				newPayload := failure.payload(t, syncID, sourceID)
				mutation := SyncMutation{
					Seq:       1,
					TargetKey: DefaultSyncTargetKey,
					Entity:    SyncEntityRelation,
					EntityKey: syncID,
					Op:        SyncOpUpsert,
					Payload:   newPayload,
					Source:    SyncSourceRemote,
				}
				if err := s.ApplyPulledMutation(DefaultSyncTargetKey, mutation); err != nil {
					t.Fatalf("ApplyPulledMutation: %v", err)
				}

				row, err := s.GetDeferred(syncID)
				if err != nil {
					t.Fatalf("GetDeferred: %v", err)
				}
				if row.PayloadRaw != newPayload {
					t.Errorf("payload = %q, want %q", row.PayloadRaw, newPayload)
				}
				if row.ApplyStatus != failure.wantStatus {
					t.Errorf("status = %q, want %q", row.ApplyStatus, failure.wantStatus)
				}
				if row.RetryCount != 0 {
					t.Errorf("retry count = %d, want 0", row.RetryCount)
				}
				if failure.wantError && (row.LastError == nil || *row.LastError == "old error") {
					t.Errorf("last error = %v, want new payload error", row.LastError)
				}
				if !failure.wantError && row.LastError != nil {
					t.Errorf("last error = %v, want nil", row.LastError)
				}
				if row.FirstSeenAt == "2026-08-01 01:01:01" {
					t.Errorf("first seen timestamp was not reset")
				}
				if !failure.wantError && row.LastAttemptedAt != nil {
					t.Errorf("last attempted at = %v, want nil for new deferred episode", row.LastAttemptedAt)
				}
			})
		}
	}
}
