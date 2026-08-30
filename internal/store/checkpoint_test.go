package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRecordSkippedCheckpointPersistsAcrossReplayAndReopen(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	identity := CheckpointIdentity{
		Host:       "codex",
		SessionID:  "session-opaque-123",
		RootTurnID: "turn-opaque-456",
	}
	first, alreadyRecorded, err := s.RecordSkippedCheckpoint(RecordSkippedCheckpointParams{
		Identity:   identity,
		ReasonCode: CheckpointSkipReasonNoDurableKnowledge,
	})
	if err != nil {
		t.Fatalf("record checkpoint: %v", err)
	}
	if alreadyRecorded {
		t.Fatal("first record reported replay")
	}
	if first.Identity != identity || first.Disposition != CheckpointDispositionSkipped ||
		first.ReasonCode != CheckpointSkipReasonNoDurableKnowledge ||
		first.ReasonVersion != CheckpointReasonVocabularyVersion ||
		first.CreatedAt == "" || first.UpdatedAt == "" {
		t.Fatalf("recorded checkpoint = %#v", first)
	}

	replayed, alreadyRecorded, err := s.RecordSkippedCheckpoint(RecordSkippedCheckpointParams{
		Identity:   identity,
		ReasonCode: CheckpointSkipReasonNoDurableKnowledge,
	})
	if err != nil {
		t.Fatalf("replay checkpoint: %v", err)
	}
	if !alreadyRecorded {
		t.Fatal("exact replay was not reported")
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed checkpoint = %#v, want %#v", replayed, first)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	got, err := reopened.GetMemoryCheckpoint(identity)
	if err != nil {
		t.Fatalf("get checkpoint after reopen: %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("reopened checkpoint = %#v, want %#v", got, first)
	}
}

func TestSkippedCheckpointRejectsInvalidSemanticStates(t *testing.T) {
	s := newTestStore(t)
	validIdentity := CheckpointIdentity{Host: "codex", SessionID: "session-valid", RootTurnID: "turn-valid"}
	tests := []struct {
		name     string
		identity CheckpointIdentity
		reason   string
		wantErr  error
	}{
		{name: "missing host", identity: CheckpointIdentity{SessionID: "session", RootTurnID: "turn"}, reason: CheckpointSkipReasonNoDurableKnowledge, wantErr: ErrCheckpointInvalidIdentity},
		{name: "missing session", identity: CheckpointIdentity{Host: "codex", RootTurnID: "turn"}, reason: CheckpointSkipReasonNoDurableKnowledge, wantErr: ErrCheckpointInvalidIdentity},
		{name: "missing root turn", identity: CheckpointIdentity{Host: "codex", SessionID: "session"}, reason: CheckpointSkipReasonNoDurableKnowledge, wantErr: ErrCheckpointInvalidIdentity},
		{name: "oversized root turn", identity: CheckpointIdentity{Host: "codex", SessionID: "session", RootTurnID: strings.Repeat("x", maxCheckpointOpaqueIDBytes+1)}, reason: CheckpointSkipReasonNoDurableKnowledge, wantErr: ErrCheckpointInvalidIdentity},
		{name: "missing reason", identity: validIdentity, wantErr: ErrCheckpointInvalidReason},
		{name: "integration failure", identity: validIdentity, reason: "integration_missing", wantErr: ErrCheckpointInvalidReason},
		{name: "processing failure", identity: validIdentity, reason: "processing_failed", wantErr: ErrCheckpointInvalidReason},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := s.RecordSkippedCheckpoint(RecordSkippedCheckpointParams{Identity: tt.identity, ReasonCode: tt.reason})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}

	_, err := s.GetMemoryCheckpoint(validIdentity)
	if !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("missing checkpoint error = %v, want ErrCheckpointNotFound", err)
	}
}

func TestSkippedCheckpointRejectsAConflictingTerminalResult(t *testing.T) {
	s := newTestStore(t)
	identity := CheckpointIdentity{Host: "codex", SessionID: "session-conflict", RootTurnID: "turn-conflict"}
	if _, err := s.db.Exec(`
		INSERT INTO memory_checkpoints (host, session_id, root_turn_id, disposition)
		VALUES (?, ?, ?, ?)`, identity.Host, identity.SessionID, identity.RootTurnID, CheckpointDispositionSaved); err != nil {
		t.Fatalf("seed saved checkpoint: %v", err)
	}

	_, _, err := s.RecordSkippedCheckpoint(RecordSkippedCheckpointParams{
		Identity: identity, ReasonCode: CheckpointSkipReasonNoDurableKnowledge,
	})
	if !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("error = %v, want ErrCheckpointConflict", err)
	}
}

func TestSkippedCheckpointIsExcludedFromMemorySurfacesAfterReopen(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	identity := CheckpointIdentity{
		Host:       "codex-checkpoint-canary",
		SessionID:  "session-checkpoint-canary",
		RootTurnID: "turn-checkpoint-canary",
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, _, err := s.RecordSkippedCheckpoint(RecordSkippedCheckpointParams{
		Identity: identity, ReasonCode: CheckpointSkipReasonNoDurableKnowledge,
	}); err != nil {
		t.Fatalf("record checkpoint: %v", err)
	}
	assertCheckpointExcludedFromMemorySurfaces(t, s, "checkpoint canary")
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertCheckpointExcludedFromMemorySurfaces(t, reopened, "checkpoint canary")
}

func TestCheckpointLedgerContainsOnlyBoundedOperationalFields(t *testing.T) {
	s := newTestStore(t)
	wantColumns := []string{
		"id", "host", "session_id", "root_turn_id", "disposition",
		"reason_code", "reason_version", "created_at", "updated_at",
	}
	rows, err := s.db.Query(`PRAGMA table_info(memory_checkpoints)`)
	if err != nil {
		t.Fatalf("checkpoint table info: %v", err)
	}
	defer rows.Close()
	var gotColumns []string
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan checkpoint column: %v", err)
		}
		gotColumns = append(gotColumns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("checkpoint columns: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close checkpoint columns: %v", err)
	}
	if !reflect.DeepEqual(gotColumns, wantColumns) {
		t.Fatalf("checkpoint columns = %v, want %v", gotColumns, wantColumns)
	}

	var syncTriggerCount int
	if err := s.db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'trigger' AND tbl_name = 'memory_checkpoints'
		  AND lower(sql) LIKE '%sync_mutations%'`).Scan(&syncTriggerCount); err != nil {
		t.Fatalf("query checkpoint sync triggers: %v", err)
	}
	if syncTriggerCount != 0 {
		t.Fatalf("checkpoint table has %d sync triggers, want 0", syncTriggerCount)
	}
}

func TestNeedsReviewProposalIsRedactedAndExcludedFromMemorySurfacesAfterReopen(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	identity := CheckpointIdentity{
		Host: "codex-proposal-canary", SessionID: "session-proposal-canary", RootTurnID: "turn-proposal-canary",
	}
	input := MemoryProposalInput{
		Title:   "Proposal privacy canary",
		Content: "Keep this reviewable. <private>proposal-secret-canary</private>",
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	checkpoint, _, err := s.RecordNeedsReviewCheckpoint(RecordNeedsReviewCheckpointParams{
		Identity: identity, Project: "engram", Proposal: &input,
	})
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}
	proposal := checkpoint.Proposal
	if strings.Contains(proposal.Content, "proposal-secret-canary") || !strings.Contains(proposal.Content, "[REDACTED]") {
		t.Fatalf("proposal content was not redacted: %q", proposal.Content)
	}
	assertCheckpointExcludedFromMemorySurfaces(t, s, "Proposal privacy canary")
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedCheckpoint, err := reopened.GetMemoryCheckpoint(identity)
	if err != nil {
		t.Fatalf("get checkpoint after reopen: %v", err)
	}
	if !reflect.DeepEqual(reopenedCheckpoint, checkpoint) {
		t.Fatalf("reopened checkpoint = %#v, want %#v", reopenedCheckpoint, checkpoint)
	}
	assertCheckpointExcludedFromMemorySurfaces(t, reopened, "Proposal privacy canary")
}

func TestMemoryProposalTablesHaveNoSyncTriggers(t *testing.T) {
	s := newTestStore(t)
	for _, table := range []string{"memory_proposals", "memory_checkpoint_proposal_references"} {
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

func TestNeedsReviewMigrationExtendsTheSavedCheckpointSchema(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "engram.db"))
	if err != nil {
		t.Fatalf("open saved-checkpoint schema: %v", err)
	}
	_, err = raw.Exec(`
		CREATE TABLE memory_checkpoints (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			host TEXT NOT NULL,
			session_id TEXT NOT NULL,
			root_turn_id TEXT NOT NULL,
			disposition TEXT NOT NULL,
			reason_code TEXT,
			reason_version INTEGER,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now')),
			UNIQUE (host, session_id, root_turn_id)
		);
		CREATE TABLE memory_checkpoint_references (
			checkpoint_id INTEGER NOT NULL REFERENCES memory_checkpoints(id) ON DELETE CASCADE,
			reference_order INTEGER NOT NULL,
			reference_kind TEXT NOT NULL CHECK (reference_kind = 'memory'),
			memory_id INTEGER NOT NULL,
			memory_sync_id TEXT NOT NULL,
			project TEXT NOT NULL,
			PRIMARY KEY (checkpoint_id, reference_order),
			UNIQUE (checkpoint_id, reference_kind, memory_sync_id)
		);
		INSERT INTO memory_checkpoints (
			host, session_id, root_turn_id, disposition, reason_code, reason_version
		) VALUES ('codex', 'existing-session', 'existing-turn', 'skipped', 'no_durable_knowledge', 1);
	`)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("create saved-checkpoint schema: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close saved-checkpoint schema: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("migrate saved-checkpoint schema: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	existing, err := s.GetMemoryCheckpoint(CheckpointIdentity{Host: "codex", SessionID: "existing-session", RootTurnID: "existing-turn"})
	if err != nil || existing.Disposition != CheckpointDispositionSkipped {
		t.Fatalf("existing checkpoint after migration = %#v, err = %v", existing, err)
	}
	created, _, err := s.RecordNeedsReviewCheckpoint(RecordNeedsReviewCheckpointParams{
		Identity: CheckpointIdentity{Host: "codex", SessionID: "new-session", RootTurnID: "new-turn"},
		Project:  "engram",
		Proposal: &MemoryProposalInput{
			Title: "Migrated proposal", Content: "Review after migration.",
		},
	})
	if err != nil || created.Proposal == nil || created.Proposal.ID == "" {
		t.Fatalf("needs-review checkpoint after migration = %#v, err = %v", created, err)
	}
}

func TestMemoryProposalMigrationRebuildsMinimalSchemaAndPreservesCheckpointSnapshot(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := createLegacyMemoryProposalFixture(t, cfg)
	if err := raw.Close(); err != nil {
		t.Fatalf("close v2 proposal fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open v3 store: %v", err)
	}
	identity := CheckpointIdentity{Host: "codex", SessionID: "session-v2-proposal", RootTurnID: "turn-v2-proposal"}
	checkpoint, err := s.GetMemoryCheckpoint(identity)
	if err != nil {
		_ = s.Close()
		t.Fatalf("get migrated checkpoint: %v", err)
	}
	wantProposal := &MemoryProposal{
		ID: "proposal-v2-preserved", Project: "engram", Title: "Preserved title",
		Content: "Preserved content", CreatedAt: "2026-08-29 12:34:56",
	}
	if !reflect.DeepEqual(checkpoint.Proposal, wantProposal) || len(checkpoint.References) != 0 {
		_ = s.Close()
		t.Fatalf("migrated checkpoint = %#v, want proposal %#v", checkpoint, wantProposal)
	}

	rows, err := s.DB().Query(`PRAGMA table_info(memory_proposals)`)
	if err != nil {
		_ = s.Close()
		t.Fatalf("proposal table info: %v", err)
	}
	var columns []string
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			_ = s.Close()
			t.Fatalf("scan proposal column: %v", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Close(); err != nil {
		_ = s.Close()
		t.Fatalf("close proposal columns: %v", err)
	}
	if want := []string{"id", "project", "title", "content", "created_at"}; !reflect.DeepEqual(columns, want) {
		_ = s.Close()
		t.Fatalf("proposal columns = %v, want %v", columns, want)
	}

	violations, err := s.DB().Query(`PRAGMA foreign_key_check`)
	if err != nil {
		_ = s.Close()
		t.Fatalf("foreign key check: %v", err)
	}
	if violations.Next() {
		_ = violations.Close()
		_ = s.Close()
		t.Fatal("foreign key check reported a violation")
	}
	if err := violations.Close(); err != nil {
		_ = s.Close()
		t.Fatalf("close foreign key check: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close migrated store: %v", err)
	}

	reopened, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen migrated store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	replayed, err := reopened.GetMemoryCheckpoint(identity)
	if err != nil || !reflect.DeepEqual(replayed, checkpoint) {
		t.Fatalf("reopened checkpoint = %#v, err = %v, want %#v", replayed, err, checkpoint)
	}
}

func TestMemoryProposalMigrationRollsBackAfterPartialRebuildFailure(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := createLegacyMemoryProposalFixture(t, cfg)
	t.Cleanup(func() { _ = raw.Close() })

	s := &Store{db: raw, cfg: cfg, hooks: defaultStoreHooks()}
	exec := s.hooks.exec
	s.hooks.exec = func(db execer, query string, args ...any) (sql.Result, error) {
		if strings.Contains(query, "CREATE TABLE memory_proposals_v3") {
			query = strings.Replace(query, "DROP TABLE memory_proposals;", "SELECT * FROM injected_missing_table;\nDROP TABLE memory_proposals;", 1)
		}
		return exec(db, query, args...)
	}

	if err := s.rebuildLegacyMemoryProposals(); err == nil {
		t.Fatal("proposal migration succeeded after injected rebuild failure")
	}
	var proposals, references int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM memory_proposals WHERE id = 'proposal-v2-preserved'`).Scan(&proposals); err != nil || proposals != 1 {
		t.Fatalf("legacy proposals after rollback = %d, err = %v", proposals, err)
	}
	if err := raw.QueryRow(`SELECT COUNT(*) FROM memory_checkpoint_proposal_references WHERE proposal_id = 'proposal-v2-preserved'`).Scan(&references); err != nil || references != 1 {
		t.Fatalf("legacy references after rollback = %d, err = %v", references, err)
	}
	for _, table := range []string{"memory_proposals_v3", "memory_checkpoint_proposal_references_v3"} {
		var count int
		if err := raw.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("temporary table %s after rollback = %d, err = %v", table, count, err)
		}
	}
	var legacyType string
	if err := raw.QueryRow(`SELECT type FROM memory_proposals WHERE id = 'proposal-v2-preserved'`).Scan(&legacyType); err != nil || legacyType != "decision" {
		t.Fatalf("legacy metadata after rollback = %q, err = %v", legacyType, err)
	}
}

func createLegacyMemoryProposalFixture(t *testing.T, cfg Config) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "engram.db"))
	if err != nil {
		t.Fatalf("open v2 proposal schema: %v", err)
	}
	raw.SetMaxOpenConns(1)
	_, err = raw.Exec(`
		PRAGMA foreign_keys = ON;
		CREATE TABLE memory_proposals (
			id TEXT PRIMARY KEY,
			project TEXT NOT NULL,
			type TEXT NOT NULL,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			scope TEXT NOT NULL,
			category TEXT NOT NULL,
			protected BOOLEAN NOT NULL DEFAULT 0,
			evidence_refs TEXT NOT NULL DEFAULT '[]',
			reason_codes TEXT NOT NULL DEFAULT '[]',
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		);
		CREATE TABLE memory_checkpoints (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			host TEXT NOT NULL,
			session_id TEXT NOT NULL,
			root_turn_id TEXT NOT NULL,
			disposition TEXT NOT NULL,
			reason_code TEXT,
			reason_version INTEGER,
			created_at TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT NOT NULL DEFAULT (datetime('now')),
			UNIQUE (host, session_id, root_turn_id)
		);
		CREATE TABLE memory_checkpoint_proposal_references (
			checkpoint_id INTEGER PRIMARY KEY REFERENCES memory_checkpoints(id) ON DELETE CASCADE,
			proposal_id TEXT NOT NULL REFERENCES memory_proposals(id) ON DELETE RESTRICT,
			project TEXT NOT NULL
		);
		INSERT INTO memory_proposals (
			id, project, type, title, content, scope, category, protected,
			evidence_refs, reason_codes, created_at
		) VALUES (
			'proposal-v2-preserved', 'engram', 'decision', 'Preserved title',
			'Preserved content', 'project', 'decision', 1,
			'["session-summary"]', '["requires_review"]', '2026-08-29 12:34:56'
		);
		INSERT INTO memory_checkpoints (
			id, host, session_id, root_turn_id, disposition, created_at, updated_at
		) VALUES (
			41, 'codex', 'session-v2-proposal', 'turn-v2-proposal', 'needs_review',
			'2026-08-29 12:35:00', '2026-08-29 12:35:00'
		);
		INSERT INTO memory_checkpoint_proposal_references (checkpoint_id, proposal_id, project)
		VALUES (41, 'proposal-v2-preserved', 'engram');
	`)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("create v2 proposal fixture: %v", err)
	}
	return raw
}

func assertCheckpointExcludedFromMemorySurfaces(t *testing.T, s *Store, canaryQuery string) {
	t.Helper()
	results, err := s.Search(canaryQuery, SearchOptions{Limit: 10, MatchMode: "any"})
	if err != nil {
		t.Fatalf("search checkpoint canary: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("search returned checkpoint data: %#v", results)
	}

	context, err := s.FormatContext("engram", "project")
	if err != nil {
		t.Fatalf("format context: %v", err)
	}
	if context != "" {
		t.Fatalf("context included checkpoint data: %q", context)
	}

	stats, err := s.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.TotalSessions != 0 || stats.TotalObservations != 0 || stats.TotalPrompts != 0 || len(stats.Projects) != 0 {
		t.Fatalf("memory stats included checkpoint data: %#v", stats)
	}

	for name, export := range map[string]func() (*ExportData, error){
		"all":     s.Export,
		"project": func() (*ExportData, error) { return s.ExportProject("engram") },
	} {
		data, err := export()
		if err != nil {
			t.Fatalf("%s export: %v", name, err)
		}
		encoded, err := json.Marshal(data)
		if err != nil {
			t.Fatalf("marshal %s export: %v", name, err)
		}
		if strings.Contains(string(encoded), "checkpoint-canary") ||
			len(data.Sessions) != 0 || len(data.Observations) != 0 || len(data.Prompts) != 0 {
			t.Fatalf("%s export included checkpoint data: %s", name, encoded)
		}
	}

	mutations, err := s.ListPendingSyncMutations(DefaultSyncTargetKey, 100)
	if err != nil {
		t.Fatalf("pending sync mutations: %v", err)
	}
	if len(mutations) != 0 {
		t.Fatalf("checkpoint created cloud mutations: %#v", mutations)
	}
}

func TestMemoryProposalFollowsProjectLifecycleWithoutEnteringSync(t *testing.T) {
	s := newTestStore(t)
	identity := CheckpointIdentity{Host: "codex", SessionID: "proposal-lifecycle-session", RootTurnID: "proposal-lifecycle-turn"}
	_, _, err := s.RecordNeedsReviewCheckpoint(RecordNeedsReviewCheckpointParams{
		Identity: identity,
		Project:  "proposal-old",
		Proposal: &MemoryProposalInput{
			Title: "Proposal lifecycle", Content: "Keep project ownership coherent.",
		},
	})
	if err != nil {
		t.Fatalf("create proposal checkpoint: %v", err)
	}
	exists, err := s.ProjectExists("proposal-old")
	if err != nil || !exists {
		t.Fatalf("proposal-only project exists = %v, err = %v", exists, err)
	}

	migrated, err := s.MigrateProject("proposal-old", "proposal-intermediate")
	if err != nil {
		t.Fatalf("migrate proposal-only project: %v", err)
	}
	if !migrated.Migrated || migrated.MemoryProposalsUpdated != 1 {
		t.Fatalf("migration result = %#v, want one proposal moved", migrated)
	}
	checkpoint, err := s.GetMemoryCheckpoint(identity)
	if err != nil || checkpoint.Proposal.Project != "proposal-intermediate" {
		t.Fatalf("checkpoint after migration = %#v, err = %v", checkpoint, err)
	}

	preview, err := s.PreviewMergeProjects([]string{"proposal-intermediate"}, "proposal-canonical")
	if err != nil {
		t.Fatalf("preview proposal-only merge: %v", err)
	}
	if len(preview.SourcesMerged) != 1 || preview.MemoryProposalsUpdated != 1 {
		t.Fatalf("merge preview = %#v, want one proposal moved", preview)
	}
	merged, err := s.MergeProjects([]string{"proposal-intermediate"}, "proposal-canonical")
	if err != nil {
		t.Fatalf("merge proposal-only project: %v", err)
	}
	if len(merged.SourcesMerged) != 1 || merged.MemoryProposalsUpdated != preview.MemoryProposalsUpdated {
		t.Fatalf("merge result = %#v, preview = %#v", merged, preview)
	}
	checkpoint, err = s.GetMemoryCheckpoint(identity)
	if err != nil || checkpoint.Proposal.Project != "proposal-canonical" {
		t.Fatalf("checkpoint after merge = %#v, err = %v", checkpoint, err)
	}

	deleted, err := s.DeleteProject("proposal-canonical", false)
	if err != nil {
		t.Fatalf("delete proposal-only project: %v", err)
	}
	if deleted.MemoryProposalsDeleted != 1 || deleted.MemoryCheckpointsDeleted != 1 {
		t.Fatalf("delete result = %#v, want proposal and checkpoint deleted", deleted)
	}
	if _, err := s.GetMemoryCheckpoint(identity); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("checkpoint after delete error = %v, want not found", err)
	}
	var mutations int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sync_mutations`).Scan(&mutations); err != nil {
		t.Fatalf("count sync mutations: %v", err)
	}
	if mutations != 0 {
		t.Fatalf("proposal lifecycle created %d sync mutations, want 0", mutations)
	}
}
