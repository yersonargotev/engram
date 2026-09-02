package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestNewMigratesLegacyRecallResultsSchema(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()

	initial, err := New(cfg)
	if err != nil {
		t.Fatalf("create initial store: %v", err)
	}
	if err := initial.CreateSession("legacy-recall-session", "engram", "/tmp"); err != nil {
		t.Fatalf("create legacy Recall session: %v", err)
	}
	validID, err := initial.AddObservation(AddObservationParams{
		SessionID: "legacy-recall-session", Type: "decision", Title: "Valid legacy Recall",
		Content: "content that still matches", Project: "engram", Scope: "project",
	})
	if err != nil {
		t.Fatalf("add valid legacy observation: %v", err)
	}
	staleID, err := initial.AddObservation(AddObservationParams{
		SessionID: "legacy-recall-session", Type: "decision", Title: "Stale legacy Recall",
		Content: "current content", Project: "engram", Scope: "project",
	})
	if err != nil {
		t.Fatalf("add stale legacy observation: %v", err)
	}
	freshID, err := initial.AddObservation(AddObservationParams{
		SessionID: "legacy-recall-session", Type: "decision", Title: "Fresh Recall",
		Content: "fresh content", Project: "engram", Scope: "project",
	})
	if err != nil {
		t.Fatalf("add fresh observation: %v", err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("close initial store: %v", err)
	}

	raw, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "engram.db"))
	if err != nil {
		t.Fatalf("open legacy Recall database: %v", err)
	}
	if _, err := raw.Exec(`
		PRAGMA foreign_keys = ON;
		DROP TABLE recall_segments;
		DROP TABLE recall_results;
		CREATE TABLE recall_results (
			recall_id      TEXT    NOT NULL,
			result_id      TEXT    NOT NULL,
			observation_id INTEGER NOT NULL,
			content_hash   TEXT    NOT NULL,
			result_rank    INTEGER NOT NULL,
			created_at     TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (recall_id, result_id),
			UNIQUE (recall_id, observation_id),
			FOREIGN KEY (recall_id) REFERENCES recall_runs(recall_id) ON DELETE CASCADE,
			FOREIGN KEY (observation_id) REFERENCES observations(id) ON DELETE CASCADE
		);
		CREATE TABLE recall_segments (
			recall_id             TEXT    NOT NULL,
			result_id             TEXT    NOT NULL,
			position              INTEGER NOT NULL,
			original_bytes        INTEGER NOT NULL,
			delivered_bytes       INTEGER NOT NULL,
			limit_bytes           INTEGER NOT NULL,
			truncated             BOOLEAN NOT NULL,
			continuation_position INTEGER,
			created_at             TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (recall_id, result_id, position),
			FOREIGN KEY (recall_id, result_id) REFERENCES recall_results(recall_id, result_id) ON DELETE CASCADE
		);
		INSERT INTO recall_runs (recall_id, project, scope, all_projects)
		VALUES ('legacy-valid', 'engram', 'project', 0), ('legacy-stale', 'engram', 'project', 0);
	`); err != nil {
		_ = raw.Close()
		t.Fatalf("create legacy Recall schema: %v", err)
	}
	validHash := sha256.Sum256([]byte("content that still matches"))
	staleHash := sha256.Sum256([]byte("content before it changed"))
	if _, err := raw.Exec(`
		INSERT INTO recall_results (recall_id, result_id, observation_id, content_hash, result_rank)
		VALUES (?, 'result-valid', ?, ?, 0), (?, 'result-stale', ?, ?, 0)
	`, "legacy-valid", validID, hex.EncodeToString(validHash[:]), "legacy-stale", staleID, hex.EncodeToString(staleHash[:])); err != nil {
		_ = raw.Close()
		t.Fatalf("seed legacy Recall results: %v", err)
	}
	if _, err := raw.Exec(`
		INSERT INTO recall_segments (
			recall_id, result_id, position, original_bytes, delivered_bytes,
			limit_bytes, truncated, continuation_position
		) VALUES
			('legacy-valid', 'result-valid', 0, 26, 26, 16384, 0, NULL),
			('legacy-stale', 'result-stale', 0, 25, 25, 16384, 0, NULL)
	`); err != nil {
		_ = raw.Close()
		t.Fatalf("seed legacy Recall segments: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close legacy Recall database: %v", err)
	}

	migrated, err := New(cfg)
	if err != nil {
		t.Fatalf("open migrated Recall database: %v", err)
	}
	t.Cleanup(func() { _ = migrated.Close() })

	columns := recallResultColumns(t, migrated.DB())
	for _, name := range []string{"revision_count", "local_revision_count"} {
		if !columns[name] {
			t.Errorf("migrated recall_results is missing %s", name)
		}
	}
	if columns["content_hash"] {
		t.Error("migrated recall_results still contains content_hash")
	}

	selection, err := migrated.RecallSelectionContext(context.Background(), "legacy-valid", "result-valid", "engram", "project", false)
	if err != nil {
		t.Fatalf("load preserved legacy Recall selection: %v", err)
	}
	if selection.Observation.ID != validID || selection.RevisionCount != selection.Observation.RevisionCount ||
		selection.LocalRevisionCount != selection.CurrentLocalRevisionCount {
		t.Fatalf("preserved legacy Recall selection = %#v", selection)
	}
	if _, err := migrated.RecallSelectionContext(context.Background(), "legacy-stale", "result-stale", "engram", "project", false); !errors.Is(err, ErrRecallSelectionUnavailable) {
		t.Fatalf("stale legacy Recall selection error = %v, want ErrRecallSelectionUnavailable", err)
	}

	for recallID, want := range map[string]int{"legacy-valid": 1, "legacy-stale": 0} {
		var got int
		if err := migrated.DB().QueryRow(`SELECT COUNT(*) FROM recall_segments WHERE recall_id = ?`, recallID).Scan(&got); err != nil {
			t.Fatalf("count %s segments: %v", recallID, err)
		}
		if got != want {
			t.Errorf("%s segment count = %d, want %d", recallID, got, want)
		}
	}

	fresh, err := migrated.GetObservation(freshID)
	if err != nil {
		t.Fatalf("load fresh observation: %v", err)
	}
	if err := migrated.RecordRecallRunContext(context.Background(), RecallRunRecord{
		RecallID: "fresh-recall", Project: "engram", Scope: "project",
		Results: []RecallResultRecord{{
			ResultID: "fresh-result", Rank: 0,
			Snapshot: RecallObservationSnapshot{
				ID: fresh.ID, SyncID: fresh.SyncID, Title: fresh.Title, Type: fresh.Type,
				Content: fresh.Content, Project: "engram", Scope: fresh.Scope,
				RevisionCount: fresh.RevisionCount,
			},
		}},
	}); err != nil {
		t.Fatalf("record Recall after migration: %v", err)
	}
	if err := migrated.migrateRecallOperations(); err != nil {
		t.Fatalf("repeat Recall migration: %v", err)
	}
}

func recallResultColumns(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(recall_results)`)
	if err != nil {
		t.Fatalf("inspect recall_results columns: %v", err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan recall_results column: %v", err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("inspect recall_results columns: %v", err)
	}
	return columns
}
