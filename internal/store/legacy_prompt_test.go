package store

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestLegacyPromptInventoryIsMetadataOnlyAndScopeBounded(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)

	got, err := s.InventoryLegacyPrompts(LegacyPromptScope{Project: " Alpha "})
	if err != nil {
		t.Fatalf("inventory project: %v", err)
	}
	want := &LegacyPromptInventory{
		Scope:    LegacyPromptScope{Project: "alpha"},
		Count:    2,
		Sessions: 2,
		OldestAt: "2026-01-01 00:00:00",
		NewestAt: "2026-01-03 00:00:00",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory = %#v, want %#v", got, want)
	}

	unowned, err := s.InventoryLegacyPrompts(LegacyPromptScope{Unowned: true})
	if err != nil {
		t.Fatalf("inventory unowned: %v", err)
	}
	if unowned.Count != 2 || unowned.Sessions != 2 || unowned.Scope != (LegacyPromptScope{Unowned: true}) {
		t.Fatalf("unowned inventory = %#v", unowned)
	}

	all, err := s.InventoryLegacyPrompts(LegacyPromptScope{All: true})
	if err != nil {
		t.Fatalf("inventory all: %v", err)
	}
	if all.Count != 4 || all.Sessions != 4 || all.Scope != (LegacyPromptScope{All: true}) {
		t.Fatalf("all inventory = %#v", all)
	}
}

func TestLegacyPromptAccessUsesStableCursorWithinExactScope(t *testing.T) {
	s := newTestStore(t)
	ids := seedLegacyPromptRows(t, s)

	first, err := s.AccessLegacyPrompts(LegacyPromptScope{Project: "alpha"}, 0, 1)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first.Prompts) != 1 || first.Prompts[0].ID != ids[0] || first.Prompts[0].Content != "alpha first" {
		t.Fatalf("first page = %#v", first)
	}
	if first.NextCursor != ids[0] {
		t.Fatalf("next cursor = %d, want %d", first.NextCursor, ids[0])
	}

	second, err := s.AccessLegacyPrompts(LegacyPromptScope{Project: "alpha"}, first.NextCursor, 1)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second.Prompts) != 1 || second.Prompts[0].ID != ids[2] || second.Prompts[0].Content != "alpha second" {
		t.Fatalf("second page = %#v", second)
	}
	if second.NextCursor != 0 {
		t.Fatalf("terminal next cursor = %d, want 0", second.NextCursor)
	}

	unowned, err := s.AccessLegacyPrompts(LegacyPromptScope{Unowned: true}, 0, 10)
	if err != nil {
		t.Fatalf("unowned page: %v", err)
	}
	if len(unowned.Prompts) != 2 || unowned.Prompts[0].ID != ids[1] || unowned.Prompts[1].ID != ids[3] {
		t.Fatalf("unowned page = %#v", unowned)
	}
}

func TestExportLegacyPromptsSupportsExplicitAllScope(t *testing.T) {
	s := newTestStore(t)
	ids := seedLegacyPromptRows(t, s)

	rows, err := s.ExportLegacyPrompts(LegacyPromptScope{All: true})
	if err != nil {
		t.Fatalf("export all: %v", err)
	}
	if len(rows) != len(ids) {
		t.Fatalf("exported %d rows, want %d", len(rows), len(ids))
	}
	for i, row := range rows {
		if row.ID != ids[i] {
			t.Fatalf("row %d id = %d, want %d", i, row.ID, ids[i])
		}
		if row.SessionID == "" || row.Content == "" || row.CreatedAt == "" {
			t.Fatalf("row %d did not preserve the Legacy record: %#v", i, row)
		}
	}
}

func TestPurgeLegacyPromptsDeletesOnlyArchiveRowsWithoutSyncEffects(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	if _, err := s.DB().Exec(`
		INSERT INTO prompt_tombstones (sync_id, session_id, project, deleted_at)
		VALUES ('existing-tombstone', 'old-session', 'alpha', '2025-12-31 00:00:00')`); err != nil {
		t.Fatalf("seed tombstone: %v", err)
	}
	seedLegacyPromptMutation(t, s, "legacy-1", "alpha", `{"sync_id":"legacy-1","project":"alpha","content":"alpha first","custom":"preserved"}`, "abandoned")
	seedLegacyPromptMutation(t, s, "orphan-alpha", "alpha", `{"sync_id":"orphan-alpha","project":"alpha","content":"orphan alpha copy"}`, "abandoned")
	seedLegacyPromptMutation(t, s, "unowned-copy", "", `{"sync_id":"unowned-copy","content":"unowned null"}`, "abandoned")
	seedLegacyPromptMutation(t, s, "beta-copy", "beta", `{"sync_id":"beta-copy","project":"beta","content":"beta copy"}`, "abandoned")
	seedLegacyPromptMutation(t, s, "ambiguous-custom", "beta", `{"sync_id":"ambiguous-custom","project":"alpha","content":"custom conflict"}`, "abandoned")
	seedLegacyPromptMutation(t, s, "pending-alpha", "alpha", `{"sync_id":"pending-alpha","project":"alpha","content":"pending copy"}`, "pending")
	seedLegacyPromptMutation(t, s, "legacy-3", "alpha", `not-json alpha second copy`, "abandoned")
	seedLegacyPromptMutation(t, s, "malformed-alpha", "alpha", `not-json orphan alpha copy`, "abandoned")
	seedLegacyPromptMutation(t, s, "case-content", "alpha", `{"sync_id":"case-content","project":"alpha","Content":"case secret"}`, "abandoned")
	seedLegacyPromptMutation(t, s, "malformed-unowned", "", `not-json unowned copy`, "abandoned")

	unchangedPayloads := map[string]string{}
	for _, key := range []string{"unowned-copy", "beta-copy", "ambiguous-custom", "malformed-unowned"} {
		var payload string
		if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = ?`, key).Scan(&payload); err != nil {
			t.Fatalf("snapshot mutation %s: %v", key, err)
		}
		unchangedPayloads[key] = payload
	}

	deleted, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"})
	if err != nil {
		t.Fatalf("purge project: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	for table, want := range map[string]int{
		"user_prompts":      2,
		"prompt_tombstones": 1,
		"sync_mutations":    10,
	} {
		var got int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if got != want {
			t.Fatalf("%s count = %d, want %d", table, got, want)
		}
	}
	for _, key := range []string{"legacy-1", "orphan-alpha", "pending-alpha", "case-content"} {
		var payload string
		if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = ?`, key).Scan(&payload); err != nil {
			t.Fatalf("load purged mutation %s: %v", key, err)
		}
		if strings.Contains(strings.ToLower(payload), `"content"`) || strings.Contains(payload, "orphan alpha copy") || strings.Contains(payload, "case secret") {
			t.Fatalf("purged mutation %s retained full content: %s", key, payload)
		}
		if key == "legacy-1" && !strings.Contains(payload, `"custom":"preserved"`) {
			t.Fatalf("purged mutation lost non-content metadata: %s", payload)
		}
	}
	var invalidPayload string
	if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = 'legacy-3'`).Scan(&invalidPayload); err != nil {
		t.Fatalf("load purged invalid mutation: %v", err)
	}
	if invalidPayload != `{"purged":true}` {
		t.Fatalf("purged invalid mutation payload = %s", invalidPayload)
	}
	var malformedOrphan string
	if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = 'malformed-alpha'`).Scan(&malformedOrphan); err != nil {
		t.Fatalf("load purged malformed orphan mutation: %v", err)
	}
	if malformedOrphan != `{"purged":true}` {
		t.Fatalf("purged malformed orphan payload = %s", malformedOrphan)
	}
	for key, want := range unchangedPayloads {
		var got string
		if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = ?`, key).Scan(&got); err != nil {
			t.Fatalf("load unrelated mutation %s: %v", key, err)
		}
		if got != want {
			t.Fatalf("unrelated mutation %s changed:\ngot  %s\nwant %s", key, got, want)
		}
	}
	remaining, err := s.ExportLegacyPrompts(LegacyPromptScope{All: true})
	if err != nil {
		t.Fatalf("export remaining: %v", err)
	}
	for _, prompt := range remaining {
		if prompt.Project == "alpha" {
			t.Fatalf("purge left project row: %#v", prompt)
		}
	}
}

func TestPurgeLegacyPromptsScrubsUnownedMutationCopiesOnly(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	seedLegacyPromptMutation(t, s, "unowned-valid", "", `{"sync_id":"unowned-valid","content":"unowned copy"}`, "abandoned")
	seedLegacyPromptMutation(t, s, "unowned-malformed", "", `not-json unowned copy`, "pending")
	seedLegacyPromptMutation(t, s, "alpha-valid", "alpha", `{"sync_id":"alpha-valid","project":"alpha","content":"alpha copy"}`, "abandoned")
	var alphaBefore string
	if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = 'alpha-valid'`).Scan(&alphaBefore); err != nil {
		t.Fatalf("snapshot alpha mutation: %v", err)
	}

	deleted, err := s.PurgeLegacyPrompts(LegacyPromptScope{Unowned: true})
	if err != nil {
		t.Fatalf("purge unowned: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	for _, key := range []string{"unowned-valid", "unowned-malformed"} {
		var payload string
		if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = ?`, key).Scan(&payload); err != nil {
			t.Fatalf("load mutation %s: %v", key, err)
		}
		if strings.Contains(strings.ToLower(payload), "content") || strings.Contains(payload, "unowned copy") {
			t.Fatalf("unowned mutation %s retained content: %s", key, payload)
		}
	}
	var alphaAfter string
	if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = 'alpha-valid'`).Scan(&alphaAfter); err != nil {
		t.Fatalf("load alpha mutation: %v", err)
	}
	if alphaAfter != alphaBefore {
		t.Fatalf("project mutation changed during unowned purge:\ngot  %s\nwant %s", alphaAfter, alphaBefore)
	}
}

func TestPurgeLegacyPromptsRollsBackPrimaryAndCopiesWhenRedactionFails(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	wantPayload := `{"sync_id":"legacy-1","project":"alpha","content":"alpha first"}`
	seedLegacyPromptMutation(t, s, "legacy-1", "alpha", wantPayload, "abandoned")
	originalExec := s.hooks.exec
	s.hooks.exec = func(db execer, query string, args ...any) (sql.Result, error) {
		if strings.Contains(query, "UPDATE sync_mutations SET payload") {
			return nil, errors.New("injected Legacy mutation redaction failure")
		}
		return originalExec(db, query, args...)
	}

	if _, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"}); err == nil || !strings.Contains(err.Error(), "injected Legacy mutation redaction failure") {
		t.Fatalf("purge error = %v, want injected failure", err)
	}
	s.hooks.exec = originalExec
	inventory, err := s.InventoryLegacyPrompts(LegacyPromptScope{Project: "alpha"})
	if err != nil {
		t.Fatalf("inventory after rollback: %v", err)
	}
	if inventory.Count != 2 {
		t.Fatalf("alpha prompts after rollback = %d, want 2", inventory.Count)
	}
	var gotPayload string
	if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = 'legacy-1'`).Scan(&gotPayload); err != nil {
		t.Fatalf("load mutation after rollback: %v", err)
	}
	if gotPayload != wantPayload {
		t.Fatalf("mutation after rollback = %s, want %s", gotPayload, wantPayload)
	}
}

func TestPurgeLegacyPromptsPurgesExactCanonicalFTSCopiesWithoutDroppingIndex(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	if _, err := s.DB().Exec(`
		CREATE VIRTUAL TABLE prompts_fts USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		);
		INSERT INTO prompts_fts(prompts_fts) VALUES ('rebuild');
	`); err != nil {
		t.Fatalf("seed canonical Legacy FTS: %v", err)
	}

	if _, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"}); err != nil {
		t.Fatalf("purge project: %v", err)
	}
	var matches int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompts_fts WHERE prompts_fts MATCH 'alpha'`).Scan(&matches); err != nil {
		t.Fatalf("search canonical Legacy FTS: %v", err)
	}
	if matches != 0 {
		t.Fatalf("canonical Legacy FTS retained %d matching copies", matches)
	}
	var exists bool
	if err := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'prompts_fts')`).Scan(&exists); err != nil {
		t.Fatalf("inspect canonical Legacy FTS: %v", err)
	}
	if !exists {
		t.Fatal("purge dropped the canonical Legacy FTS object instead of removing selected copies")
	}
}

func TestPurgeLegacyPromptsUsesExactCanonicalFTSDeleteTrigger(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	if _, err := s.DB().Exec(`
		CREATE VIRTUAL TABLE prompts_fts USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		);
		CREATE TRIGGER prompt_fts_delete AFTER DELETE ON user_prompts BEGIN
			INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
			VALUES ('delete', old.id, old.content, old.project);
		END;
		INSERT INTO prompts_fts(prompts_fts) VALUES ('rebuild');
	`); err != nil {
		t.Fatalf("seed canonical Legacy FTS trigger: %v", err)
	}

	if _, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"}); err != nil {
		t.Fatalf("purge project: %v", err)
	}
	var matches int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompts_fts WHERE prompts_fts MATCH 'alpha'`).Scan(&matches); err != nil {
		t.Fatalf("search canonical Legacy FTS: %v", err)
	}
	if matches != 0 {
		t.Fatalf("canonical delete trigger retained %d matching copies", matches)
	}
	var triggerDDL string
	if err := s.DB().QueryRow(`SELECT sql FROM sqlite_schema WHERE name = 'prompt_fts_delete'`).Scan(&triggerDDL); err != nil {
		t.Fatalf("canonical delete trigger was dropped: %v", err)
	}
}

func TestPurgeLegacyPromptsPreservesCustomizedFTSDeleteTrigger(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	wantPayload := `{"sync_id":"legacy-1","project":"alpha","content":"alpha first"}`
	seedLegacyPromptMutation(t, s, "legacy-1", "alpha", wantPayload, "abandoned")
	if _, err := s.DB().Exec(`
		CREATE VIRTUAL TABLE prompts_fts USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		);
		CREATE TRIGGER prompt_fts_delete AFTER DELETE ON user_prompts BEGIN
			SELECT count(*) FROM prompts_fts;
		END;
		INSERT INTO prompts_fts(prompts_fts) VALUES ('rebuild');
	`); err != nil {
		t.Fatalf("seed customized Legacy FTS trigger: %v", err)
	}
	var beforeDDL string
	if err := s.DB().QueryRow(`SELECT sql FROM sqlite_schema WHERE name = 'prompt_fts_delete'`).Scan(&beforeDDL); err != nil {
		t.Fatalf("snapshot customized Legacy FTS trigger: %v", err)
	}

	if _, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"}); !errors.Is(err, ErrLegacyPromptPurgeCustomizedFTS) {
		t.Fatalf("purge error = %v, want ErrLegacyPromptPurgeCustomizedFTS", err)
	}
	var afterDDL string
	if err := s.DB().QueryRow(`SELECT sql FROM sqlite_schema WHERE name = 'prompt_fts_delete'`).Scan(&afterDDL); err != nil {
		t.Fatalf("customized Legacy FTS trigger was dropped: %v", err)
	}
	if afterDDL != beforeDDL {
		t.Fatal("customized Legacy FTS trigger DDL changed")
	}
	var matches int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompts_fts WHERE prompts_fts MATCH 'alpha'`).Scan(&matches); err != nil {
		t.Fatalf("search custom-owned Legacy FTS: %v", err)
	}
	if matches == 0 {
		t.Fatal("purge modified FTS content controlled by a customized delete trigger")
	}
	inventory, err := s.InventoryLegacyPrompts(LegacyPromptScope{Project: "alpha"})
	if err != nil {
		t.Fatalf("inventory after blocked purge: %v", err)
	}
	if inventory.Count != 2 {
		t.Fatalf("blocked purge deleted archive rows: count=%d", inventory.Count)
	}
	var gotPayload string
	if err := s.DB().QueryRow(`SELECT payload FROM sync_mutations WHERE entity_key = 'legacy-1'`).Scan(&gotPayload); err != nil {
		t.Fatalf("load mutation after blocked purge: %v", err)
	}
	if gotPayload != wantPayload {
		t.Fatalf("blocked purge changed journal payload: %s", gotPayload)
	}
}

func TestPurgeLegacyPromptsPreservesCustomizedFTSObject(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	if _, err := s.DB().Exec(`
		CREATE VIRTUAL TABLE prompts_fts USING fts5(content, project);
		INSERT INTO prompts_fts(rowid, content, project) VALUES (9001, 'custom alpha copy', 'alpha');
	`); err != nil {
		t.Fatalf("seed customized Legacy FTS: %v", err)
	}
	var beforeDDL string
	if err := s.DB().QueryRow(`SELECT sql FROM sqlite_schema WHERE name = 'prompts_fts'`).Scan(&beforeDDL); err != nil {
		t.Fatalf("snapshot customized Legacy FTS: %v", err)
	}

	if _, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"}); !errors.Is(err, ErrLegacyPromptPurgeCustomizedFTS) {
		t.Fatalf("purge error = %v, want ErrLegacyPromptPurgeCustomizedFTS", err)
	}
	var content, afterDDL string
	if err := s.DB().QueryRow(`SELECT content FROM prompts_fts WHERE rowid = 9001`).Scan(&content); err != nil {
		t.Fatalf("customized Legacy FTS row was changed: %v", err)
	}
	if err := s.DB().QueryRow(`SELECT sql FROM sqlite_schema WHERE name = 'prompts_fts'`).Scan(&afterDDL); err != nil {
		t.Fatalf("inspect customized Legacy FTS: %v", err)
	}
	if content != "custom alpha copy" || afterDDL != beforeDDL {
		t.Fatalf("customized Legacy FTS changed: content=%q ddl_equal=%v", content, afterDDL == beforeDDL)
	}
	inventory, err := s.InventoryLegacyPrompts(LegacyPromptScope{Project: "alpha"})
	if err != nil {
		t.Fatalf("inventory after blocked purge: %v", err)
	}
	if inventory.Count != 2 {
		t.Fatalf("blocked purge deleted archive rows: count=%d", inventory.Count)
	}
}

func TestPurgeLegacyPromptsBlocksCaseVariedCanonicalFTSTableName(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	if _, err := s.DB().Exec(`
		CREATE VIRTUAL TABLE PROMPTS_FTS USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		);
		INSERT INTO PROMPTS_FTS(PROMPTS_FTS) VALUES ('rebuild');
	`); err != nil {
		t.Fatalf("seed case-varied Legacy FTS: %v", err)
	}

	if _, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"}); !errors.Is(err, ErrLegacyPromptPurgeCustomizedFTS) {
		t.Fatalf("purge error = %v, want ErrLegacyPromptPurgeCustomizedFTS", err)
	}
	inventory, err := s.InventoryLegacyPrompts(LegacyPromptScope{Project: "alpha"})
	if err != nil {
		t.Fatalf("inventory after blocked purge: %v", err)
	}
	if inventory.Count != 2 {
		t.Fatalf("blocked purge deleted archive rows: count=%d", inventory.Count)
	}
	var matches int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM PROMPTS_FTS WHERE PROMPTS_FTS MATCH 'alpha'`).Scan(&matches); err != nil {
		t.Fatalf("search case-varied Legacy FTS: %v", err)
	}
	if matches != 2 {
		t.Fatalf("blocked purge changed case-varied FTS copies: matches=%d", matches)
	}
}

func TestPurgeLegacyPromptsBlocksChangedQuotedDeleteLiteral(t *testing.T) {
	s := newTestStore(t)
	seedLegacyPromptRows(t, s)
	if _, err := s.DB().Exec(`
		CREATE VIRTUAL TABLE prompts_fts USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		);
		CREATE TRIGGER prompt_fts_delete AFTER DELETE ON user_prompts BEGIN
			INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
			VALUES ('DELETE', old.id, old.content, old.project);
		END;
		INSERT INTO prompts_fts(prompts_fts) VALUES ('rebuild');
	`); err != nil {
		t.Fatalf("seed changed-literal Legacy FTS trigger: %v", err)
	}

	if _, err := s.PurgeLegacyPrompts(LegacyPromptScope{Project: "alpha"}); !errors.Is(err, ErrLegacyPromptPurgeCustomizedFTS) {
		t.Fatalf("purge error = %v, want ErrLegacyPromptPurgeCustomizedFTS", err)
	}
	inventory, err := s.InventoryLegacyPrompts(LegacyPromptScope{Project: "alpha"})
	if err != nil {
		t.Fatalf("inventory after blocked purge: %v", err)
	}
	if inventory.Count != 2 {
		t.Fatalf("blocked purge deleted archive rows: count=%d", inventory.Count)
	}
	var matches int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompts_fts WHERE prompts_fts MATCH 'alpha'`).Scan(&matches); err != nil {
		t.Fatalf("search changed-literal Legacy FTS: %v", err)
	}
	if matches != 2 {
		t.Fatalf("blocked purge changed FTS copies: matches=%d", matches)
	}
}

func seedLegacyPromptRows(t *testing.T, s *Store) []int64 {
	t.Helper()
	fixtures := []struct {
		syncID, sessionID, content string
		project                    any
		createdAt                  string
	}{
		{"legacy-1", "session-a", "alpha first", "alpha", "2026-01-01 00:00:00"},
		{"legacy-2", "session-u", "unowned null", nil, "2026-01-02 00:00:00"},
		{"legacy-3", "session-b", "alpha second", "alpha", "2026-01-03 00:00:00"},
		{"legacy-4", "session-w", "unowned whitespace", "  ", "2026-01-04 00:00:00"},
	}
	ids := make([]int64, 0, len(fixtures))
	for _, fixture := range fixtures {
		sessionProject := ""
		if project, ok := fixture.project.(string); ok {
			sessionProject = strings.TrimSpace(project)
		}
		if _, err := s.DB().Exec(`
			INSERT OR IGNORE INTO sessions (id, project, directory)
			VALUES (?, ?, ?)`, fixture.sessionID, sessionProject, "/tmp/"+fixture.sessionID); err != nil {
			t.Fatalf("seed session: %v", err)
		}
		res, err := s.DB().Exec(`
			INSERT INTO user_prompts (sync_id, session_id, content, project, created_at)
			VALUES (?, ?, ?, ?, ?)`,
			fixture.syncID, fixture.sessionID, fixture.content, fixture.project, fixture.createdAt,
		)
		if err != nil {
			t.Fatalf("seed prompt: %v", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatalf("seed prompt id: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

func seedLegacyPromptMutation(t *testing.T, s *Store, entityKey, project, payload, disposition string) {
	t.Helper()
	if _, err := s.DB().Exec(`
		INSERT INTO sync_mutations (
			target_key, entity, entity_key, op, payload, project, disposition,
			disposition_reason
		) VALUES ('cloud', 'prompt', ?, 'upsert', ?, ?, ?, 'legacy_prompt_frozen')`,
		entityKey, payload, project, disposition,
	); err != nil {
		t.Fatalf("seed mutation %s: %v", entityKey, err)
	}
}
