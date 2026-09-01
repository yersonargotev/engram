package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"
)

const (
	CaptureContentTypePrompt         = "prompt"
	CaptureContentTypeSubagentOutput = "subagent_output"

	DefaultDiagnosticRetentionDays = 7
	MaxDiagnosticRetentionDays     = 30
	diagnosticCaptureJanitorPeriod = time.Hour

	LegacyPromptFTSRetired             = "retired"
	LegacyPromptFTSAbsent              = "absent"
	LegacyPromptFTSCustomizedPreserved = "customized_preserved"
)

type CaptureConsent struct {
	Project       string     `json:"project"`
	ContentType   string     `json:"content_type"`
	SessionID     string     `json:"session_id,omitempty"`
	RetentionDays int        `json:"retention_days"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type CaptureConsentStatus struct {
	Consent         *CaptureConsent `json:"consent,omitempty"`
	Expired         bool            `json:"expired"`
	StoredCount     int64           `json:"stored_count"`
	LegacyFTSStatus string          `json:"legacy_prompt_fts_status"`
}

type CaptureDiagnosticParams struct {
	Project     string
	ContentType string
	SessionID   string
	Content     string
	Now         time.Time
}

type CaptureDiagnosticResult struct {
	Captured  bool
	ID        int64
	ExpiresAt *time.Time
}

type rowQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func (s *Store) UpsertCaptureConsent(consent CaptureConsent) error {
	project, _ := NormalizeProject(consent.Project)
	var expiresAt any
	if consent.ExpiresAt != nil {
		expiresAt = consent.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	updatedAt := consent.UpdatedAt.UTC()
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	_, err := s.execHook(s.db, `
		INSERT INTO capture_consents (
			project, content_type, session_id, retention_days, expires_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, content_type, session_id) DO UPDATE SET
			retention_days = excluded.retention_days,
			expires_at = excluded.expires_at,
			updated_at = excluded.updated_at`,
		project, consent.ContentType, consent.SessionID, consent.RetentionDays, expiresAt,
		updatedAt.Format(time.RFC3339Nano),
	)
	return err
}

func (s *Store) DeleteCaptureConsent(project, contentType, sessionID string) (bool, error) {
	project, _ = NormalizeProject(project)
	res, err := s.execHook(s.db, `
		DELETE FROM capture_consents
		WHERE project = ? AND content_type = ? AND session_id = ?`,
		project, contentType, sessionID,
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (s *Store) CaptureConsentStatus(project, contentType, sessionID string, now time.Time) (*CaptureConsentStatus, error) {
	project, _ = NormalizeProject(project)
	consent, err := s.effectiveCaptureConsent(s.db, project, contentType, sessionID, now.UTC())
	if err != nil {
		return nil, err
	}
	expired := false
	if consent == nil && sessionID != "" {
		if err := s.db.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM capture_consents
				WHERE project = ? AND content_type = ? AND session_id = ?
				  AND expires_at IS NOT NULL AND expires_at <= ?
			)`, project, contentType, sessionID, now.UTC().Format(time.RFC3339Nano)).Scan(&expired); err != nil {
			return nil, err
		}
	}
	var count int64
	if err := s.db.QueryRow(`
		SELECT COUNT(*)
		FROM diagnostic_captures
		WHERE project = ? AND content_type = ? AND expires_at > ?`,
		project, contentType, now.UTC().Format(time.RFC3339Nano),
	).Scan(&count); err != nil {
		return nil, err
	}
	legacyStatus, err := s.legacyPromptFTSStatus()
	if err != nil {
		return nil, err
	}
	return &CaptureConsentStatus{Consent: consent, Expired: expired, StoredCount: count, LegacyFTSStatus: legacyStatus}, nil
}

func (s *Store) CaptureDiagnostic(params CaptureDiagnosticParams) (*CaptureDiagnosticResult, error) {
	params.Project, _ = NormalizeProject(params.Project)
	content, _ := s.prepareStoredContent(params.Content)
	if content == "" {
		return nil, ErrPromptContentRequired
	}
	now := params.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result := &CaptureDiagnosticResult{}
	err := s.withTx(func(tx *sql.Tx) error {
		if _, err := s.execHook(tx, `DELETE FROM diagnostic_captures WHERE expires_at <= ?`, now.Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("purge expired Diagnostic capture: %w", err)
		}
		consent, err := s.effectiveCaptureConsent(tx, params.Project, params.ContentType, params.SessionID, now)
		if err != nil {
			return err
		}
		if consent == nil {
			return nil
		}
		expiresAt := now.Add(time.Duration(consent.RetentionDays) * 24 * time.Hour)
		res, err := s.execHook(tx, `
			INSERT INTO diagnostic_captures (
				project, content_type, session_id, content, created_at, expires_at
			) VALUES (?, ?, ?, ?, ?, ?)`,
			params.Project, params.ContentType, params.SessionID, content,
			now.Format(time.RFC3339Nano), expiresAt.Format(time.RFC3339Nano),
		)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		result.Captured = true
		result.ID = id
		result.ExpiresAt = &expiresAt
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) PurgeDiagnosticCapture(project, contentType string) (int64, error) {
	project, _ = NormalizeProject(project)
	res, err := s.execHook(s.db, `DELETE FROM diagnostic_captures WHERE project = ? AND content_type = ?`, project, contentType)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) purgeExpiredDiagnosticCaptures(now time.Time) error {
	if _, err := s.execHook(s.db, `DELETE FROM diagnostic_captures WHERE expires_at <= ?`, now.UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("purge expired Diagnostic capture: %w", err)
	}
	return nil
}

func (s *Store) startDiagnosticCaptureRetention() error {
	now := time.Now().UTC()
	if s.cfg.diagnosticCaptureNow != nil {
		now = s.cfg.diagnosticCaptureNow().UTC()
	}
	if err := s.purgeExpiredDiagnosticCaptures(now); err != nil {
		return err
	}

	ticks := s.cfg.diagnosticCaptureJanitorTicks
	stopTicker := func() {}
	if ticks == nil {
		ticker := time.NewTicker(diagnosticCaptureJanitorPeriod)
		ticks = ticker.C
		stopTicker = ticker.Stop
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.diagnosticCaptureJanitorCancel = cancel
	s.diagnosticCaptureJanitorDone = make(chan struct{})
	go func() {
		defer close(s.diagnosticCaptureJanitorDone)
		defer stopTicker()
		for {
			select {
			case <-ctx.Done():
				return
			case sweepAt, ok := <-ticks:
				if !ok {
					return
				}
				if err := s.purgeExpiredDiagnosticCaptures(sweepAt); err != nil {
					log.Printf("engram: Diagnostic capture retention sweep failed: %v", err)
				}
			}
		}
	}()
	return nil
}

func (s *Store) stopDiagnosticCaptureRetention() {
	if s.diagnosticCaptureJanitorCancel == nil {
		return
	}
	s.diagnosticCaptureJanitorCancel()
	<-s.diagnosticCaptureJanitorDone
}

func (s *Store) effectiveCaptureConsent(db rowQueryer, project, contentType, sessionID string, now time.Time) (*CaptureConsent, error) {
	row := db.QueryRow(`
		SELECT project, content_type, session_id, retention_days, expires_at, updated_at
		FROM capture_consents
		WHERE project = ? AND content_type = ?
		  AND updated_at <= ?
		  AND (
			(session_id = ? AND session_id <> '' AND expires_at > ?)
			OR session_id = ''
		  )
		ORDER BY CASE WHEN session_id = ? AND session_id <> '' THEN 0 ELSE 1 END
		LIMIT 1`,
		project, contentType, now.Format(time.RFC3339Nano), sessionID, now.Format(time.RFC3339Nano), sessionID,
	)
	var consent CaptureConsent
	var expiresAt sql.NullString
	var updatedAt string
	if err := row.Scan(&consent.Project, &consent.ContentType, &consent.SessionID, &consent.RetentionDays, &expiresAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if expiresAt.Valid {
		parsed, err := time.Parse(time.RFC3339Nano, expiresAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse capture consent expiry: %w", err)
		}
		consent.ExpiresAt = &parsed
	}
	parsedUpdatedAt, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("parse capture consent update time: %w", err)
	}
	consent.UpdatedAt = parsedUpdatedAt
	return &consent, nil
}

func (s *Store) migrateContentCapture() error {
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := s.execHook(tx, `
			CREATE TABLE IF NOT EXISTS capture_consents (
				project        TEXT NOT NULL,
				content_type   TEXT NOT NULL,
				session_id     TEXT NOT NULL DEFAULT '',
				retention_days INTEGER NOT NULL,
				expires_at     TEXT,
				updated_at     TEXT NOT NULL,
				PRIMARY KEY (project, content_type, session_id)
			);
			CREATE TABLE IF NOT EXISTS diagnostic_captures (
				id           INTEGER PRIMARY KEY AUTOINCREMENT,
				project      TEXT NOT NULL,
				content_type TEXT NOT NULL,
				session_id   TEXT NOT NULL DEFAULT '',
				content      TEXT NOT NULL,
				created_at   TEXT NOT NULL,
				expires_at   TEXT NOT NULL
			);
			CREATE INDEX IF NOT EXISTS idx_diagnostic_capture_expiry
				ON diagnostic_captures(expires_at);
			CREATE INDEX IF NOT EXISTS idx_diagnostic_capture_scope
				ON diagnostic_captures(project, content_type, session_id, created_at DESC);
			CREATE TABLE IF NOT EXISTS content_capture_migration_state (
				singleton         INTEGER PRIMARY KEY CHECK (singleton = 1),
				legacy_fts_status TEXT NOT NULL,
				updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
			);`); err != nil {
			return fmt.Errorf("create Content capture schema: %w", err)
		}

		ftsStatus, owned, err := inspectLegacyPromptFTS(tx)
		if err != nil {
			return err
		}
		if owned {
			for _, trigger := range []string{"prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update"} {
				if _, err := s.execHook(tx, "DROP TRIGGER IF EXISTS "+trigger); err != nil {
					return fmt.Errorf("retire Legacy prompt FTS trigger %s: %w", trigger, err)
				}
			}
			if _, err := s.execHook(tx, "DROP TABLE IF EXISTS prompts_fts"); err != nil {
				return fmt.Errorf("retire Legacy prompt FTS table: %w", err)
			}
			if ftsStatus != LegacyPromptFTSAbsent {
				ftsStatus = LegacyPromptFTSRetired
			}
		}

		if _, err := s.execHook(tx, `
			UPDATE sync_mutations
			SET disposition = 'abandoned',
			    disposition_reason = 'legacy_prompt_frozen',
			    disposition_evidence = NULL,
			    disposition_at = COALESCE(disposition_at, datetime('now'))
			WHERE entity = 'prompt' AND disposition = 'pending'`); err != nil {
			return fmt.Errorf("freeze pending Legacy prompt mutations: %w", err)
		}
		if _, err := s.execHook(tx, `
			INSERT INTO content_capture_migration_state (singleton, legacy_fts_status, updated_at)
			VALUES (1, ?, datetime('now'))
			ON CONFLICT(singleton) DO UPDATE SET
				legacy_fts_status = excluded.legacy_fts_status,
				updated_at = excluded.updated_at`, ftsStatus); err != nil {
			return fmt.Errorf("record Content capture migration state: %w", err)
		}
		return nil
	})
}

func inspectLegacyPromptFTS(tx *sql.Tx) (status string, owned bool, err error) {
	rows, err := tx.Query(`
		SELECT type, name, ifnull(sql, '')
		FROM sqlite_schema
		WHERE lower(name) IN ('prompts_fts', 'prompt_fts_insert', 'prompt_fts_delete', 'prompt_fts_update')
		   OR instr(lower(ifnull(sql, '')), 'prompts_fts') > 0
		ORDER BY type, name`)
	if err != nil {
		return "", false, fmt.Errorf("inspect Legacy prompt FTS objects: %w", err)
	}
	defer rows.Close()
	found := false
	canonicalObjects := make(map[string]struct{}, 4)
	for rows.Next() {
		var objectType, name, ddl string
		if err := rows.Scan(&objectType, &name, &ddl); err != nil {
			return "", false, fmt.Errorf("scan Legacy prompt FTS object: %w", err)
		}
		expectedType, expectedDDL, canonical := canonicalLegacyPromptFTSObject(name)
		if !canonical {
			if sqliteDDLReferencesIdentifier(ddl, "prompts_fts") {
				return LegacyPromptFTSCustomizedPreserved, false, nil
			}
			continue
		}
		found = true
		canonicalObjects[name] = struct{}{}
		if objectType != expectedType || canonicalSQLiteDDL(ddl) != canonicalSQLiteDDL(expectedDDL) {
			return LegacyPromptFTSCustomizedPreserved, false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("inspect Legacy prompt FTS objects: %w", err)
	}
	if err := rows.Close(); err != nil {
		return "", false, fmt.Errorf("close Legacy prompt FTS inspection: %w", err)
	}
	if !found {
		return LegacyPromptFTSAbsent, true, nil
	}
	ambiguous, err := legacyPromptFTSRemovalHasExternalDependencies(tx, canonicalObjects)
	if err != nil {
		return "", false, err
	}
	if ambiguous {
		return LegacyPromptFTSCustomizedPreserved, false, nil
	}
	return LegacyPromptFTSRetired, true, nil
}

type sqliteSchemaObject struct {
	objectType string
	name       string
	tableName  string
	ddl        string
}

func legacyPromptFTSRemovalHasExternalDependencies(tx *sql.Tx, canonicalObjects map[string]struct{}) (bool, error) {
	before, err := loadSQLiteSchemaObjects(tx)
	if err != nil {
		return false, err
	}
	tableKinds, err := loadSQLiteTableKinds(tx)
	if err != nil {
		return false, err
	}

	scheduledIdentifiers := make(map[string]struct{})
	ownedObjects := make(map[string]struct{})
	for name := range canonicalObjects {
		scheduledIdentifiers[strings.ToLower(name)] = struct{}{}
		for key, object := range before {
			if object.name == name {
				ownedObjects[key] = struct{}{}
			}
		}
	}

	if _, hasCanonicalTable := canonicalObjects["prompts_fts"]; hasCanonicalTable {
		after, err := probeLegacyPromptFTSRemoval(tx, canonicalObjects)
		if err != nil {
			return false, err
		}
		removed := make(map[string]sqliteSchemaObject)
		for key, object := range before {
			if _, remains := after[key]; !remains {
				removed[key] = object
				scheduledIdentifiers[strings.ToLower(object.name)] = struct{}{}
			}
		}

		ownedShadowTables := make(map[string]struct{})
		for key, object := range removed {
			if object.objectType == "table" && tableKinds[strings.ToLower(object.name)] == "shadow" {
				ownedObjects[key] = struct{}{}
				ownedShadowTables[strings.ToLower(object.name)] = struct{}{}
			}
		}
		for key, object := range removed {
			if object.objectType == "index" && object.ddl == "" {
				if _, engineOwned := ownedShadowTables[strings.ToLower(object.tableName)]; engineOwned {
					ownedObjects[key] = struct{}{}
				}
			}
		}
		for key := range removed {
			if _, engineOwned := ownedObjects[key]; !engineOwned {
				return true, nil
			}
		}
	}

	for key, object := range before {
		if _, engineOwned := ownedObjects[key]; engineOwned || object.ddl == "" {
			continue
		}
		for identifier := range scheduledIdentifiers {
			if sqliteDDLReferencesIdentifier(object.ddl, identifier) {
				return true, nil
			}
		}
	}
	return false, nil
}

func probeLegacyPromptFTSRemoval(tx *sql.Tx, canonicalObjects map[string]struct{}) (map[string]sqliteSchemaObject, error) {
	const savepoint = "engram_prompt_fts_retirement_probe"
	if _, err := tx.Exec("SAVEPOINT " + savepoint); err != nil {
		return nil, fmt.Errorf("start Legacy prompt FTS retirement probe: %w", err)
	}
	rollback := func(cause error) (map[string]sqliteSchemaObject, error) {
		_, rollbackErr := tx.Exec("ROLLBACK TO " + savepoint)
		_, releaseErr := tx.Exec("RELEASE " + savepoint)
		if rollbackErr != nil || releaseErr != nil {
			return nil, errors.Join(cause, rollbackErr, releaseErr)
		}
		return nil, cause
	}
	for _, trigger := range []string{"prompt_fts_insert", "prompt_fts_delete", "prompt_fts_update"} {
		if _, present := canonicalObjects[trigger]; !present {
			continue
		}
		if _, err := tx.Exec("DROP TRIGGER " + trigger); err != nil {
			return rollback(fmt.Errorf("probe Legacy prompt FTS trigger removal %s: %w", trigger, err))
		}
	}
	if _, err := tx.Exec("DROP TABLE prompts_fts"); err != nil {
		return rollback(fmt.Errorf("probe Legacy prompt FTS table removal: %w", err))
	}
	after, err := loadSQLiteSchemaObjects(tx)
	if err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec("ROLLBACK TO " + savepoint); err != nil {
		return nil, fmt.Errorf("rollback Legacy prompt FTS retirement probe: %w", err)
	}
	if _, err := tx.Exec("RELEASE " + savepoint); err != nil {
		return nil, fmt.Errorf("release Legacy prompt FTS retirement probe: %w", err)
	}
	return after, nil
}

func loadSQLiteSchemaObjects(tx *sql.Tx) (map[string]sqliteSchemaObject, error) {
	rows, err := tx.Query(`SELECT type, name, tbl_name, ifnull(sql, '') FROM sqlite_schema`)
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite schema ownership: %w", err)
	}
	defer rows.Close()
	objects := make(map[string]sqliteSchemaObject)
	for rows.Next() {
		var object sqliteSchemaObject
		if err := rows.Scan(&object.objectType, &object.name, &object.tableName, &object.ddl); err != nil {
			return nil, fmt.Errorf("scan SQLite schema ownership: %w", err)
		}
		objects[object.objectType+"\x00"+object.name] = object
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inspect SQLite schema ownership: %w", err)
	}
	return objects, nil
}

func loadSQLiteTableKinds(tx *sql.Tx) (map[string]string, error) {
	rows, err := tx.Query(`PRAGMA main.table_list`)
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite table ownership: %w", err)
	}
	defer rows.Close()
	kinds := make(map[string]string)
	for rows.Next() {
		var schema, name, kind string
		var columns, withoutRowID, strict int
		if err := rows.Scan(&schema, &name, &kind, &columns, &withoutRowID, &strict); err != nil {
			return nil, fmt.Errorf("scan SQLite table ownership: %w", err)
		}
		if schema == "main" {
			kinds[strings.ToLower(name)] = kind
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inspect SQLite table ownership: %w", err)
	}
	return kinds, nil
}

func canonicalLegacyPromptFTSObject(name string) (objectType, ddl string, ok bool) {
	switch name {
	case "prompts_fts":
		return "table", `CREATE VIRTUAL TABLE prompts_fts USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		)`, true
	case "prompt_fts_insert":
		return "trigger", `CREATE TRIGGER prompt_fts_insert AFTER INSERT ON user_prompts BEGIN
			INSERT INTO prompts_fts(rowid, content, project)
			VALUES (new.id, new.content, new.project);
		END`, true
	case "prompt_fts_delete":
		return "trigger", `CREATE TRIGGER prompt_fts_delete AFTER DELETE ON user_prompts BEGIN
			INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
			VALUES ('delete', old.id, old.content, old.project);
		END`, true
	case "prompt_fts_update":
		return "trigger", `CREATE TRIGGER prompt_fts_update AFTER UPDATE ON user_prompts BEGIN
			INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
			VALUES ('delete', old.id, old.content, old.project);
			INSERT INTO prompts_fts(rowid, content, project)
			VALUES (new.id, new.content, new.project);
		END`, true
	default:
		return "", "", false
	}
}

func normalizeSQLiteDDL(value string) string {
	var normalized strings.Builder
	pendingSpace := false
	for _, part := range splitSQLiteDDL(value) {
		if part.kind != sqliteDDLCode {
			if pendingSpace && normalized.Len() > 0 {
				normalized.WriteByte(' ')
			}
			pendingSpace = false
			normalized.WriteString(part.text)
			continue
		}
		for _, r := range part.text {
			if unicode.IsSpace(r) {
				pendingSpace = true
				continue
			}
			if pendingSpace && normalized.Len() > 0 {
				normalized.WriteByte(' ')
			}
			pendingSpace = false
			normalized.WriteRune(unicode.ToLower(r))
		}
	}
	return normalized.String()
}

func canonicalSQLiteDDL(value string) string {
	normalized := normalizeSQLiteDDL(value)
	for _, prefix := range []struct{ from, to string }{
		{"create virtual table if not exists ", "create virtual table "},
		{"create trigger if not exists ", "create trigger "},
	} {
		if strings.HasPrefix(normalized, prefix.from) {
			return prefix.to + strings.TrimPrefix(normalized, prefix.from)
		}
	}
	return normalized
}

type sqliteDDLPartKind uint8

const (
	sqliteDDLCode sqliteDDLPartKind = iota
	sqliteDDLSingleQuoted
	sqliteDDLDoubleQuoted
	sqliteDDLBacktickQuoted
	sqliteDDLBracketQuoted
	sqliteDDLLineComment
	sqliteDDLBlockComment
)

type sqliteDDLPart struct {
	text string
	kind sqliteDDLPartKind
}

func splitSQLiteDDL(value string) []sqliteDDLPart {
	runes := []rune(value)
	parts := make([]sqliteDDLPart, 0, 8)
	start := 0
	for i := 0; i < len(runes); {
		if i+1 < len(runes) && runes[i] == '-' && runes[i+1] == '-' {
			if start < i {
				parts = append(parts, sqliteDDLPart{text: string(runes[start:i]), kind: sqliteDDLCode})
			}
			commentStart := i
			i += 2
			for i < len(runes) && runes[i] != '\n' {
				i++
			}
			if i < len(runes) {
				i++
			}
			parts = append(parts, sqliteDDLPart{text: string(runes[commentStart:i]), kind: sqliteDDLLineComment})
			start = i
			continue
		}
		if i+1 < len(runes) && runes[i] == '/' && runes[i+1] == '*' {
			if start < i {
				parts = append(parts, sqliteDDLPart{text: string(runes[start:i]), kind: sqliteDDLCode})
			}
			commentStart := i
			i += 2
			for i+1 < len(runes) && (runes[i] != '*' || runes[i+1] != '/') {
				i++
			}
			if i+1 < len(runes) {
				i += 2
			} else {
				i = len(runes)
			}
			parts = append(parts, sqliteDDLPart{text: string(runes[commentStart:i]), kind: sqliteDDLBlockComment})
			start = i
			continue
		}

		opener, kind := runes[i], sqliteDDLCode
		closer := opener
		switch opener {
		case '\'':
			kind = sqliteDDLSingleQuoted
		case '"':
			kind = sqliteDDLDoubleQuoted
		case '`':
			kind = sqliteDDLBacktickQuoted
		case '[':
			kind = sqliteDDLBracketQuoted
			closer = ']'
		default:
			i++
			continue
		}
		if start < i {
			parts = append(parts, sqliteDDLPart{text: string(runes[start:i]), kind: sqliteDDLCode})
		}
		quotedStart := i
		i++
		for i < len(runes) {
			if runes[i] != closer {
				i++
				continue
			}
			if i+1 < len(runes) && runes[i+1] == closer {
				i += 2
				continue
			}
			i++
			break
		}
		parts = append(parts, sqliteDDLPart{text: string(runes[quotedStart:i]), kind: kind})
		start = i
	}
	if start < len(runes) {
		parts = append(parts, sqliteDDLPart{text: string(runes[start:]), kind: sqliteDDLCode})
	}
	return parts
}

func sqliteDDLReferencesIdentifier(ddl, identifier string) bool {
	want := strings.ToLower(identifier)
	for _, token := range sqliteDDLTokens(ddl) {
		if token == want {
			return true
		}
	}
	return false
}

func sqliteDDLHasTokenSequence(ddl string, sequence ...string) bool {
	tokens := sqliteDDLTokens(ddl)
	if len(sequence) == 0 || len(tokens) < len(sequence) {
		return false
	}
	for start := 0; start <= len(tokens)-len(sequence); start++ {
		matched := true
		for i, want := range sequence {
			if tokens[start+i] != strings.ToLower(want) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func sqliteDDLTokens(value string) []string {
	var tokens []string
	for _, part := range splitSQLiteDDL(value) {
		if part.kind == sqliteDDLLineComment || part.kind == sqliteDDLBlockComment {
			continue
		}
		if part.kind != sqliteDDLCode {
			content := unquoteSQLiteDDLPart(part)
			if content != "" {
				tokens = append(tokens, strings.ToLower(content))
			}
			continue
		}
		runes := []rune(part.text)
		for i := 0; i < len(runes); {
			for i < len(runes) && !isSQLiteIdentifierRune(runes[i]) {
				i++
			}
			start := i
			for i < len(runes) && isSQLiteIdentifierRune(runes[i]) {
				i++
			}
			if start < i {
				tokens = append(tokens, strings.ToLower(string(runes[start:i])))
			}
		}
	}
	return tokens
}

func unquoteSQLiteDDLPart(part sqliteDDLPart) string {
	runes := []rune(part.text)
	if len(runes) < 2 {
		return ""
	}
	content := string(runes[1 : len(runes)-1])
	var closer rune
	switch part.kind {
	case sqliteDDLSingleQuoted:
		closer = '\''
	case sqliteDDLDoubleQuoted:
		closer = '"'
	case sqliteDDLBacktickQuoted:
		closer = '`'
	case sqliteDDLBracketQuoted:
		closer = ']'
	default:
		return ""
	}
	return strings.ReplaceAll(content, string([]rune{closer, closer}), string(closer))
}

func isSQLiteIdentifierRune(r rune) bool {
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func (s *Store) legacyPromptFTSStatus() (string, error) {
	var status string
	if err := s.db.QueryRow(`SELECT legacy_fts_status FROM content_capture_migration_state WHERE singleton = 1`).Scan(&status); err != nil {
		return "", err
	}
	return status, nil
}
