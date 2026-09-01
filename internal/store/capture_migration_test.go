package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestFreshStoreStartsCaptureDisabledWithoutLegacyPromptFTS(t *testing.T) {
	s := newTestStore(t)

	for _, table := range []string{"capture_consents", "diagnostic_captures", "content_capture_migration_state"} {
		var exists bool
		if err := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?)`, table).Scan(&exists); err != nil {
			t.Fatalf("inspect table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("capture table %s is missing", table)
		}
	}
	assertNoCanonicalPromptFTS(t, s.DB())

	var grants int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM capture_consents`).Scan(&grants); err != nil {
		t.Fatalf("count grants: %v", err)
	}
	if grants != 0 {
		t.Fatalf("fresh Store grants = %d, want capture disabled", grants)
	}
}

func TestCaptureMigrationPreservesLegacyRowsAndFreezesPendingPromptMutations(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	if _, err := raw.Exec(`
		INSERT INTO sessions (id, project, directory) VALUES ('legacy-session', 'engram', '/work/engram');
		INSERT INTO user_prompts (sync_id, session_id, content, project, created_at)
		VALUES (NULL, 'legacy-session', 'LEGACY-PROMPT-CANARY-102', NULL, '2026-08-30 12:34:56');
		INSERT INTO sync_mutations (target_key, entity, entity_key, op, payload, source, project)
		VALUES ('cloud', 'prompt', 'legacy-prompt-key', 'upsert', '{"content":"LEGACY-PROMPT-CANARY-102"}', 'local', 'engram');
	`); err != nil {
		t.Fatalf("seed Legacy prompt: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close pre-capture fixture: %v", err)
	}

	for _, phase := range []string{"upgrade", "rerun"} {
		t.Run(phase, func(t *testing.T) {
			s, err := New(cfg)
			if err != nil {
				t.Fatalf("open migrated Store: %v", err)
			}
			defer s.Close()

			var syncID, project sql.NullString
			var sessionID, content, createdAt string
			if err := s.DB().QueryRow(`
				SELECT sync_id, session_id, content, project, created_at
				FROM user_prompts WHERE content = 'LEGACY-PROMPT-CANARY-102'`).
				Scan(&syncID, &sessionID, &content, &project, &createdAt); err != nil {
				t.Fatalf("read preserved Legacy prompt: %v", err)
			}
			if syncID.Valid || project.Valid || sessionID != "legacy-session" || content != "LEGACY-PROMPT-CANARY-102" || createdAt != "2026-08-30 12:34:56" {
				t.Fatalf("Legacy prompt was rewritten: sync=%+v session=%q content=%q project=%+v created=%q", syncID, sessionID, content, project, createdAt)
			}

			assertNoCanonicalPromptFTS(t, s.DB())
			var disposition, reason string
			if err := s.DB().QueryRow(`SELECT disposition, disposition_reason FROM sync_mutations WHERE entity = 'prompt'`).Scan(&disposition, &reason); err != nil {
				t.Fatalf("read frozen mutation: %v", err)
			}
			if disposition != "abandoned" || reason != "legacy_prompt_frozen" {
				t.Fatalf("prompt mutation disposition=(%q,%q), want abandoned legacy_prompt_frozen", disposition, reason)
			}
			var tombstones int
			if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompt_tombstones`).Scan(&tombstones); err != nil {
				t.Fatalf("count tombstones: %v", err)
			}
			if tombstones != 0 {
				t.Fatalf("migration created %d remote tombstones", tombstones)
			}
		})
	}
}

func TestCaptureMigrationPreservesCustomizedLegacyPromptFTS(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	if _, err := raw.Exec(`
		DROP TRIGGER prompt_fts_insert;
		CREATE TRIGGER prompt_fts_insert AFTER INSERT ON user_prompts BEGIN
			SELECT 1;
		END;
	`); err != nil {
		t.Fatalf("customize Legacy FTS trigger: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close customized fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open customized fixture: %v", err)
	}
	defer s.Close()
	for _, name := range []string{"prompts_fts", "prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update"} {
		var exists bool
		if err := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?)`, name).Scan(&exists); err != nil {
			t.Fatalf("inspect customized object %s: %v", name, err)
		}
		if !exists {
			t.Fatalf("customized Legacy object %s was removed", name)
		}
	}
	var status string
	if err := s.DB().QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
		t.Fatalf("read migration status: %v", err)
	}
	if status != LegacyPromptFTSCustomizedPreserved {
		t.Fatalf("legacy FTS status = %q", status)
	}
}

func TestCaptureMigrationPreservesAmbiguousLegacyPromptFTSOwnership(t *testing.T) {
	tests := []struct {
		name        string
		customize   string
		extraObject string
	}{
		{
			name: "canonical markers plus tokenizer option",
			customize: `
				DROP TABLE prompts_fts;
				CREATE VIRTUAL TABLE prompts_fts USING fts5(
					content,
					project,
					content='user_prompts',
					content_rowid='id',
					tokenize='porter'
				);`,
		},
		{
			name: "canonical trigger plus audit statement",
			customize: `
				CREATE TABLE prompt_insert_audit (prompt_id INTEGER NOT NULL);
				DROP TRIGGER prompt_fts_insert;
				CREATE TRIGGER prompt_fts_insert AFTER INSERT ON user_prompts BEGIN
					INSERT INTO prompts_fts(rowid, content, project)
					VALUES (new.id, new.content, new.project);
					INSERT INTO prompt_insert_audit(prompt_id) VALUES (new.id);
				END;`,
		},
		{
			name: "unexpected relevant trigger name",
			customize: `
				CREATE TRIGGER custom_prompt_fts_audit AFTER INSERT ON user_prompts BEGIN
					SELECT count(*) FROM prompts_fts;
				END;`,
			extraObject: "custom_prompt_fts_audit",
		},
		{
			name: "changed quoted delete literal",
			customize: `
				DROP TRIGGER prompt_fts_delete;
				CREATE TRIGGER prompt_fts_delete AFTER DELETE ON user_prompts BEGIN
					INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
					VALUES ('DELETE', old.id, old.content, old.project);
				END;`,
		},
		{
			name: "unexpected view dependency",
			customize: `
				CREATE VIEW prompt_fts_archive_view AS
				SELECT rowid, content, project FROM "PROMPTS_FTS";`,
			extraObject: "prompt_fts_archive_view",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mustDefaultConfig(t)
			cfg.DataDir = t.TempDir()
			raw := seedPreCaptureStore(t, cfg)
			if _, err := raw.Exec(tt.customize); err != nil {
				_ = raw.Close()
				t.Fatalf("customize Legacy FTS: %v", err)
			}
			if err := raw.Close(); err != nil {
				t.Fatalf("close customized fixture: %v", err)
			}

			s, err := New(cfg)
			if err != nil {
				t.Fatalf("open customized fixture: %v", err)
			}
			defer s.Close()
			objects := []string{"prompts_fts", "prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update"}
			if tt.extraObject != "" {
				objects = append(objects, tt.extraObject)
			}
			for _, name := range objects {
				var exists bool
				if err := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?)`, name).Scan(&exists); err != nil {
					t.Fatalf("inspect customized object %s: %v", name, err)
				}
				if !exists {
					t.Fatalf("ambiguous Legacy object %s was removed", name)
				}
			}
			var status string
			if err := s.DB().QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
				t.Fatalf("read migration status: %v", err)
			}
			if status != LegacyPromptFTSCustomizedPreserved {
				t.Fatalf("legacy FTS status = %q, want %q", status, LegacyPromptFTSCustomizedPreserved)
			}
		})
	}
}

func TestNormalizeSQLiteDDLPreservesQuotedAndEscapedBytes(t *testing.T) {
	ddl := "  CREATE TRIGGER \"Prompt\"\"Audit\" AFTER DELETE ON [User_Prompts] BEGIN\n" +
		"SELECT 'It''s DELETE', `Mixed``Identifier`; END  "
	want := "create trigger \"Prompt\"\"Audit\" after delete on [User_Prompts] begin " +
		"select 'It''s DELETE', `Mixed``Identifier`; end"
	if got := normalizeSQLiteDDL(ddl); got != want {
		t.Fatalf("normalized DDL = %q, want %q", got, want)
	}
}

func TestSQLiteDDLLexerHandlesCommentsWithoutChangingQuotedContent(t *testing.T) {
	ddl := "  SELECT  '-- Not a comment', \"/*Quoted--Identifier*/\"  -- Keep ' CASE\n" +
		" /* Block \" -- CASE */ FROM  prompts_fts  "
	want := "select '-- Not a comment', \"/*Quoted--Identifier*/\" -- Keep ' CASE\n" +
		" /* Block \" -- CASE */ from prompts_fts"
	if got := normalizeSQLiteDDL(ddl); got != want {
		t.Fatalf("normalized commented DDL = %q, want %q", got, want)
	}

	for _, ddl := range []string{
		"-- prompts_fts is documentation only\nSELECT 1",
		"/* prompts_fts is documentation only */ SELECT 1",
	} {
		if sqliteDDLReferencesIdentifier(ddl, "prompts_fts") {
			t.Fatalf("comment text was treated as an identifier reference: %q", ddl)
		}
	}
	if !sqliteDDLReferencesIdentifier("-- unmatched quote '\nSELECT * FROM prompts_fts", "prompts_fts") {
		t.Fatal("line comment hid the following identifier reference")
	}
	if !sqliteDDLReferencesIdentifier("/* unmatched quote ' */ SELECT * FROM `PROMPTS_FTS`", "prompts_fts") {
		t.Fatal("block comment or quoted identifier hid the following identifier reference")
	}
}

func TestCaptureMigrationPreservesCaseVariedLegacyPromptFTSObjectName(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	if _, err := raw.Exec(`
		DROP TABLE prompts_fts;
		CREATE VIRTUAL TABLE PROMPTS_FTS USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		);`); err != nil {
		_ = raw.Close()
		t.Fatalf("case-vary Legacy FTS table: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close case-varied fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open case-varied fixture: %v", err)
	}
	defer s.Close()
	for _, name := range []string{"PROMPTS_FTS", "prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update"} {
		var exists bool
		if err := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?)`, name).Scan(&exists); err != nil {
			t.Fatalf("inspect case-varied object %s: %v", name, err)
		}
		if !exists {
			t.Fatalf("case-varied Legacy object %s was removed", name)
		}
	}
	var status string
	if err := s.DB().QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
		t.Fatalf("read migration status: %v", err)
	}
	if status != LegacyPromptFTSCustomizedPreserved {
		t.Fatalf("legacy FTS status = %q, want %q", status, LegacyPromptFTSCustomizedPreserved)
	}
}

func TestCaptureMigrationPreservesExternalDependencyOnFTSShadowObject(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	if _, err := raw.Exec(`
		CREATE VIEW prompt_fts_shadow_archive AS
		SELECT id, block FROM prompts_fts_data;`); err != nil {
		_ = raw.Close()
		t.Fatalf("create Legacy FTS shadow dependency: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close shadow dependency fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open shadow dependency fixture: %v", err)
	}
	defer s.Close()
	for _, name := range []string{"prompts_fts", "prompts_fts_data", "prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update", "prompt_fts_shadow_archive"} {
		var exists bool
		if err := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?)`, name).Scan(&exists); err != nil {
			t.Fatalf("inspect preserved object %s: %v", name, err)
		}
		if !exists {
			t.Fatalf("shadow dependency object %s was removed", name)
		}
	}
	var rows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompt_fts_shadow_archive`).Scan(&rows); err != nil {
		t.Fatalf("preserved shadow dependency is broken: %v", err)
	}
	var status string
	if err := s.DB().QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
		t.Fatalf("read migration status: %v", err)
	}
	if status != LegacyPromptFTSCustomizedPreserved {
		t.Fatalf("legacy FTS status = %q, want %q", status, LegacyPromptFTSCustomizedPreserved)
	}
}

func TestCaptureMigrationPreservesShadowDependencyAfterUnmatchedQuoteInLineComment(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	if _, err := raw.Exec(`
		CREATE VIEW prompt_fts_commented_shadow_archive AS
		-- user note with unmatched quote ' before the dependency
		SELECT id, block FROM prompts_fts_data;`); err != nil {
		_ = raw.Close()
		t.Fatalf("create commented Legacy FTS shadow dependency: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close commented shadow dependency fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open commented shadow dependency fixture: %v", err)
	}
	defer s.Close()
	for _, name := range []string{"prompts_fts", "prompts_fts_data", "prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update", "prompt_fts_commented_shadow_archive"} {
		var exists bool
		if err := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?)`, name).Scan(&exists); err != nil {
			t.Fatalf("inspect preserved object %s: %v", name, err)
		}
		if !exists {
			t.Fatalf("commented shadow dependency object %s was removed", name)
		}
	}
	var rows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM prompt_fts_commented_shadow_archive`).Scan(&rows); err != nil {
		t.Fatalf("preserved commented shadow dependency is broken: %v", err)
	}
	var status string
	if err := s.DB().QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
		t.Fatalf("read migration status: %v", err)
	}
	if status != LegacyPromptFTSCustomizedPreserved {
		t.Fatalf("legacy FTS status = %q, want %q", status, LegacyPromptFTSCustomizedPreserved)
	}
}

func TestCaptureMigrationRetiresExactCanonicalLegacyPromptFTS(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	if err := raw.Close(); err != nil {
		t.Fatalf("close canonical fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open canonical fixture: %v", err)
	}
	defer s.Close()
	assertNoCanonicalPromptFTS(t, s.DB())
	var status string
	if err := s.DB().QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
		t.Fatalf("read migration status: %v", err)
	}
	if status != LegacyPromptFTSRetired {
		t.Fatalf("legacy FTS status = %q, want %q", status, LegacyPromptFTSRetired)
	}
}

func TestCaptureMigrationRetiresSafePartialExactCanonicalLegacyPromptFTS(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	if _, err := raw.Exec(`
		DROP TRIGGER prompt_fts_insert;
		DROP TRIGGER prompt_fts_update;`); err != nil {
		_ = raw.Close()
		t.Fatalf("make partial canonical fixture: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close partial canonical fixture: %v", err)
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("open partial canonical fixture: %v", err)
	}
	defer s.Close()
	assertNoCanonicalPromptFTS(t, s.DB())
	var status string
	if err := s.DB().QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
		t.Fatalf("read migration status: %v", err)
	}
	if status != LegacyPromptFTSRetired {
		t.Fatalf("legacy FTS status = %q, want %q", status, LegacyPromptFTSRetired)
	}
}

func TestCaptureMigrationFailureRollsBackAndRetrySucceeds(t *testing.T) {
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	raw := seedPreCaptureStore(t, cfg)
	s := &Store{db: raw, cfg: cfg, hooks: defaultStoreHooks()}
	originalExec := s.hooks.exec
	s.hooks.exec = func(db execer, query string, args ...any) (sql.Result, error) {
		if strings.TrimSpace(query) == "DROP TABLE IF EXISTS prompts_fts" {
			return nil, errors.New("injected prompt FTS retirement failure")
		}
		return originalExec(db, query, args...)
	}

	if err := s.migrate(); err == nil || !strings.Contains(err.Error(), "injected prompt FTS retirement failure") {
		t.Fatalf("migration error = %v, want injected failure", err)
	}
	for _, name := range []string{"prompts_fts", "prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update"} {
		var exists bool
		if err := raw.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?)`, name).Scan(&exists); err != nil {
			t.Fatalf("inspect rollback object %s: %v", name, err)
		}
		if !exists {
			t.Fatalf("migration rollback did not restore %s", name)
		}
	}
	var captureTables int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name IN ('capture_consents','diagnostic_captures','content_capture_migration_state')`).Scan(&captureTables); err != nil {
		t.Fatalf("inspect capture schema after rollback: %v", err)
	}
	if captureTables != 0 {
		t.Fatalf("capture tables after rollback = %d, want 0", captureTables)
	}

	s.hooks.exec = originalExec
	if err := s.migrate(); err != nil {
		t.Fatalf("retry migration: %v", err)
	}
	assertNoCanonicalPromptFTS(t, raw)
	if err := raw.Close(); err != nil {
		t.Fatalf("close retried Store: %v", err)
	}
}

func seedPreCaptureStore(t *testing.T, cfg Config) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "engram.db"))
	if err != nil {
		t.Fatalf("open pre-capture fixture: %v", err)
	}
	if _, err := db.Exec(legacyDDLPostMemoryConflictAudit); err != nil {
		_ = db.Close()
		t.Fatalf("seed pre-capture schema: %v", err)
	}
	return db
}

func assertNoCanonicalPromptFTS(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`
		SELECT name FROM sqlite_schema
		WHERE name IN ('prompts_fts','prompt_fts_insert','prompt_fts_delete','prompt_fts_update')
		ORDER BY name`)
	if err != nil {
		t.Fatalf("inspect Legacy prompt FTS: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan Legacy prompt FTS: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Legacy prompt FTS: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("Legacy prompt FTS objects remain: %v", names)
	}
}
