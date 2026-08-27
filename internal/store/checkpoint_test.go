package store

import (
	"encoding/json"
	"errors"
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
	exists, err := s.ProjectExists("engram")
	if err != nil {
		t.Fatalf("project exists: %v", err)
	}
	if exists {
		t.Fatal("checkpoint created a Memory project surface")
	}
}
