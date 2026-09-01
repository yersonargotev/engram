package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadOrCreateRecallFeedbackSaltRetriesConcurrentPartialKey(t *testing.T) {
	dataDir := t.TempDir()
	path := filepath.Join(dataDir, recallFeedbackKeyFilename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create partial feedback key: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close partial feedback key: %v", err)
	}
	want := bytes.Repeat([]byte{7}, sha256.Size)
	written := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		written <- os.WriteFile(path, want, 0o600)
	}()

	got, err := loadOrCreateRecallFeedbackSalt(dataDir)
	if err != nil {
		t.Fatalf("load feedback salt: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("complete concurrent feedback key: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("feedback salt = %x, want %x", got, want)
	}
}

func TestRecallFeedbackStaysLocalAndExcludedFromMemorySurfacesAfterReopen(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	const recallID = "recall-feedback-exclusion-canary"
	identity := CheckpointIdentity{
		Host:       "feedback-host-canary",
		SessionID:  "feedback-session-canary",
		RootTurnID: "feedback-turn-canary",
	}
	if err := s.RecordRecallRunContext(context.Background(), RecallRunRecord{
		RecallID:     recallID,
		Project:      "engram",
		Scope:        "project",
		TurnIdentity: &identity,
	}); err != nil {
		t.Fatalf("record empty Recall run: %v", err)
	}
	falseEmpty := false
	if _, err := s.RecordRecallFeedback(RecordRecallFeedbackParams{
		Identity:         identity,
		RecallID:         recallID,
		FalseEmpty:       &falseEmpty,
		FalseEmptySource: RecallFeedbackSourceEvaluator,
	}); err != nil {
		t.Fatalf("record Recall feedback: %v", err)
	}
	keyInfo, err := os.Stat(filepath.Join(cfg.DataDir, recallFeedbackKeyFilename))
	if err != nil {
		t.Fatalf("stat Recall feedback key: %v", err)
	}
	if keyInfo.Mode().Perm() != 0o600 || keyInfo.Size() != sha256.Size {
		t.Fatalf("Recall feedback key mode/size = %v/%d, want 0600/%d", keyInfo.Mode().Perm(), keyInfo.Size(), sha256.Size)
	}
	assertRecallFeedbackExcluded(t, s)
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertRecallFeedbackExcluded(t, reopened)
	snapshot, err := reopened.RecallFeedbackReportSnapshot()
	if err != nil {
		t.Fatalf("load feedback report after reopen: %v", err)
	}
	if len(snapshot.Runs) != 1 || len(snapshot.EmptyReviews) != 1 {
		t.Fatalf("feedback report after reopen = %#v", snapshot)
	}
}

func TestRecallFeedbackTablesHaveNoSyncTriggers(t *testing.T) {
	s := newTestStore(t)
	for _, table := range []string{
		"recall_feedback_runs",
		"recall_feedback_exposures",
		"recall_feedback_labels",
		"recall_false_empty_reviews",
	} {
		var syncTriggerCount int
		if err := s.db.QueryRow(`
			SELECT COUNT(*) FROM sqlite_master
			WHERE type = 'trigger' AND tbl_name = ?
			  AND lower(sql) LIKE '%sync_mutations%'`, table).Scan(&syncTriggerCount); err != nil {
			t.Fatalf("query %s sync triggers: %v", table, err)
		}
		if syncTriggerCount != 0 {
			t.Fatalf("%s has %d sync triggers, want 0", table, syncTriggerCount)
		}
	}
}

func assertRecallFeedbackExcluded(t *testing.T, s *Store) {
	t.Helper()
	assertCheckpointExcludedFromMemorySurfaces(t, s, "feedback canary")

	exported, err := s.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	encoded, err := json.Marshal(exported)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}
	for _, forbidden := range []string{
		"recall-feedback-exclusion-canary",
		"feedback-host-canary",
		"feedback-session-canary",
		"feedback-turn-canary",
		"recall_feedback",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("export leaked Recall feedback token %q: %s", forbidden, encoded)
		}
	}

	imported := newTestStore(t)
	if _, err := imported.Import(exported); err != nil {
		t.Fatalf("import ordinary export: %v", err)
	}
	for _, table := range []string{
		"recall_feedback_runs",
		"recall_feedback_exposures",
		"recall_feedback_labels",
		"recall_false_empty_reviews",
	} {
		var rows int
		if err := imported.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&rows); err != nil {
			t.Fatalf("count imported %s: %v", table, err)
		}
		if rows != 0 {
			t.Fatalf("ordinary import restored %d %s rows, want 0", rows, table)
		}
	}
}
