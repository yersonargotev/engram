package store

import (
	"strings"
	"testing"
)

const legacyPromptCanary = "LEGACY-PROMPT-CANARY-102"

func seedLegacyPrompt(t *testing.T, s *Store, sessionID, project, syncID string) {
	t.Helper()
	if _, err := s.DB().Exec(
		`INSERT INTO user_prompts (sync_id, session_id, content, project, created_at)
		 VALUES (?, ?, ?, ?, '2025-01-02 03:04:05')`,
		syncID, sessionID, legacyPromptCanary, project,
	); err != nil {
		t.Fatalf("seed Legacy prompt: %v", err)
	}
}

func TestLegacyPromptsStayOutsideOrdinaryReadSurfaces(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateSession("legacy-session", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	seedLegacyPrompt(t, s, "legacy-session", "engram", "legacy-prompt-sync-id")

	context, err := s.FormatContext("engram", "project")
	if err != nil {
		t.Fatalf("format context: %v", err)
	}
	if strings.Contains(context, legacyPromptCanary) || strings.Contains(context, "Recent User Prompts") {
		t.Fatalf("ordinary context exposed Legacy prompt: %q", context)
	}

	compaction, err := s.FormatCompactionContext("legacy-session")
	if err != nil {
		t.Fatalf("format compaction context: %v", err)
	}
	if strings.Contains(compaction, legacyPromptCanary) || strings.Contains(compaction, "Recent User Prompts") {
		t.Fatalf("compaction context exposed Legacy prompt: %q", compaction)
	}

	stats, err := s.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.TotalPrompts != 0 {
		t.Fatalf("ordinary stats counted %d Legacy prompts", stats.TotalPrompts)
	}

	exported, err := s.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(exported.Prompts) != 0 {
		t.Fatalf("ordinary export exposed Legacy prompts: %+v", exported.Prompts)
	}
}

func TestLegacyPromptsDoNotCreateOrdinaryProjectOrExportClosure(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.DB().Exec(
		`INSERT INTO sessions (id, project, directory) VALUES ('legacy-only-session', 'other-project', '/tmp/legacy')`,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	seedLegacyPrompt(t, s, "legacy-only-session", "legacy-only", "legacy-only-sync-id")

	exists, err := s.ProjectExists("legacy-only")
	if err != nil {
		t.Fatalf("project exists: %v", err)
	}
	if exists {
		t.Fatal("Legacy prompt made its archive project visible as an ordinary project")
	}

	exported, err := s.ExportProject("legacy-only")
	if err != nil {
		t.Fatalf("export project: %v", err)
	}
	if len(exported.Prompts) != 0 || len(exported.Sessions) != 0 {
		t.Fatalf("Legacy prompt created ordinary export closure: %+v", exported)
	}
}

func TestOrdinaryImportIgnoresLegacyPrompts(t *testing.T) {
	s := newTestStore(t)
	result, err := s.Import(&ExportData{Prompts: []Prompt{{
		SyncID:    "incoming-legacy-prompt",
		SessionID: "missing-session",
		Content:   legacyPromptCanary,
		Project:   "engram",
		CreatedAt: "2025-01-02 03:04:05",
	}}})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if result.PromptsImported != 0 {
		t.Fatalf("ordinary import reported %d Legacy prompts", result.PromptsImported)
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM user_prompts WHERE content = ?`, legacyPromptCanary).Scan(&count); err != nil {
		t.Fatalf("count imported prompts: %v", err)
	}
	if count != 0 {
		t.Fatalf("ordinary import persisted %d Legacy prompts", count)
	}
}

func TestPromptSyncMutationsAreNeitherListedNorApplied(t *testing.T) {
	s := newTestStore(t)
	if err := s.EnrollProject("engram"); err != nil {
		t.Fatalf("enroll project: %v", err)
	}
	if _, err := s.DB().Exec(
		`INSERT INTO sync_mutations
		 (target_key, entity, entity_key, op, payload, source, project)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		DefaultSyncTargetKey, SyncEntityPrompt, "legacy-pending", SyncOpUpsert,
		`{"sync_id":"legacy-pending","session_id":"legacy-session","content":"LEGACY-PROMPT-CANARY-102","project":"engram"}`,
		SyncSourceLocal, "engram",
	); err != nil {
		t.Fatalf("seed prompt mutation: %v", err)
	}

	pending, err := s.ListPendingSyncMutations(DefaultSyncTargetKey, 100)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	for _, mutation := range pending {
		if mutation.Entity == SyncEntityPrompt {
			t.Fatalf("pending Legacy prompt mutation escaped: %+v", mutation)
		}
	}

	if err := s.ApplyPulledMutation(DefaultSyncTargetKey, SyncMutation{
		Seq:       42,
		Entity:    SyncEntityPrompt,
		EntityKey: "incoming-prompt",
		Op:        SyncOpUpsert,
		Payload:   `{"sync_id":"incoming-prompt","session_id":"remote-session","content":"LEGACY-PROMPT-CANARY-102","project":"engram"}`,
		Project:   "engram",
	}); err != nil {
		t.Fatalf("apply pulled prompt: %v", err)
	}

	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM user_prompts`).Scan(&count); err != nil {
		t.Fatalf("count prompts: %v", err)
	}
	if count != 0 {
		t.Fatalf("pulled prompt materialized %d Legacy rows", count)
	}
	state, err := s.GetSyncState(DefaultSyncTargetKey)
	if err != nil {
		t.Fatalf("get sync state: %v", err)
	}
	if state.LastPulledSeq != 42 {
		t.Fatalf("ignored prompt did not advance pull cursor: %d", state.LastPulledSeq)
	}
}
