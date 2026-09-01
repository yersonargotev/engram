package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	DefaultLegacyPromptPageSize = 20
	MaxLegacyPromptPageSize     = 100
)

var (
	ErrLegacyPromptInvalidScope       = errors.New("invalid Legacy prompt scope")
	ErrLegacyPromptPurgeCustomizedFTS = errors.New("Legacy prompt purge blocked by customized FTS ownership")
)

// LegacyPromptScope selects exactly one explicit archive boundary. All is
// accepted only by export; ordinary inspection and purge stay project- or
// unowned-scoped.
type LegacyPromptScope struct {
	Project string `json:"project,omitempty"`
	Unowned bool   `json:"unowned,omitempty"`
	All     bool   `json:"all,omitempty"`
}

// LegacyPromptInventory deliberately contains metadata only. Prompt content is
// available solely through the explicit access and export operations.
type LegacyPromptInventory struct {
	Scope    LegacyPromptScope `json:"scope"`
	Count    int64             `json:"count"`
	Sessions int64             `json:"sessions"`
	OldestAt string            `json:"oldest_at,omitempty"`
	NewestAt string            `json:"newest_at,omitempty"`
}

type LegacyPromptPage struct {
	Scope      LegacyPromptScope `json:"scope"`
	Prompts    []Prompt          `json:"prompts"`
	NextCursor int64             `json:"next_cursor,omitempty"`
}

type legacyPromptPurgeRow struct {
	ID      int64
	SyncID  string
	Content string
	Project sql.NullString
}

// InventoryLegacyPrompts reports only archive metadata for a single project or
// for rows whose project ownership was never established.
func (s *Store) InventoryLegacyPrompts(scope LegacyPromptScope) (*LegacyPromptInventory, error) {
	scope, err := normalizeLegacyPromptScope(scope, true)
	if err != nil {
		return nil, err
	}
	where, args := legacyPromptScopePredicate(scope)
	result := &LegacyPromptInventory{Scope: scope}
	query := `
		SELECT COUNT(*), COUNT(DISTINCT session_id),
		       COALESCE(MIN(created_at), ''), COALESCE(MAX(created_at), '')
		FROM user_prompts`
	if where != "" {
		query += " WHERE " + where
	}
	if err := s.db.QueryRow(query, args...).Scan(
		&result.Count, &result.Sessions, &result.OldestAt, &result.NewestAt,
	); err != nil {
		return nil, fmt.Errorf("inventory Legacy prompts: %w", err)
	}
	return result, nil
}

// AccessLegacyPrompts returns a stable, bounded page in ascending archive ID
// order. A zero NextCursor means the selected scope has no further page.
func (s *Store) AccessLegacyPrompts(scope LegacyPromptScope, cursor int64, limit int) (*LegacyPromptPage, error) {
	scope, err := normalizeLegacyPromptScope(scope, false)
	if err != nil {
		return nil, err
	}
	if cursor < 0 {
		return nil, errors.New("Legacy prompt cursor cannot be negative")
	}
	if limit <= 0 {
		limit = DefaultLegacyPromptPageSize
	}
	if limit > MaxLegacyPromptPageSize {
		return nil, fmt.Errorf("Legacy prompt page limit exceeds %d", MaxLegacyPromptPageSize)
	}

	where, args := legacyPromptScopePredicate(scope)
	args = append(args, cursor, limit+1)
	rows, err := s.queryItHook(s.db, `
		SELECT id, ifnull(sync_id, ''), session_id, content, ifnull(project, ''), created_at
		FROM user_prompts
		WHERE `+where+` AND id > ?
		ORDER BY id ASC
		LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("access Legacy prompts: %w", err)
	}
	defer rows.Close()

	result := &LegacyPromptPage{Scope: scope, Prompts: make([]Prompt, 0, limit)}
	for rows.Next() {
		var prompt Prompt
		if err := rows.Scan(&prompt.ID, &prompt.SyncID, &prompt.SessionID, &prompt.Content, &prompt.Project, &prompt.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan Legacy prompt: %w", err)
		}
		result.Prompts = append(result.Prompts, prompt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("access Legacy prompts: %w", err)
	}
	if len(result.Prompts) > limit {
		result.Prompts = result.Prompts[:limit]
		result.NextCursor = result.Prompts[len(result.Prompts)-1].ID
	}
	return result, nil
}

// ExportLegacyPrompts supplies intact archive records to an explicit exporter.
// It performs no serialization or filesystem writes and accepts an explicit All
// scope in addition to project and unowned scopes.
func (s *Store) ExportLegacyPrompts(scope LegacyPromptScope) ([]Prompt, error) {
	scope, err := normalizeLegacyPromptScope(scope, true)
	if err != nil {
		return nil, err
	}
	where, args := legacyPromptScopePredicate(scope)
	query := `
		SELECT id, ifnull(sync_id, ''), session_id, content, ifnull(project, ''), created_at
		FROM user_prompts`
	if where != "" {
		query += " WHERE " + where
	}
	query += " ORDER BY id ASC"

	rows, err := s.queryItHook(s.db, query, args...)
	if err != nil {
		return nil, fmt.Errorf("export Legacy prompts: %w", err)
	}
	defer rows.Close()

	results := make([]Prompt, 0)
	for rows.Next() {
		var prompt Prompt
		if err := rows.Scan(&prompt.ID, &prompt.SyncID, &prompt.SessionID, &prompt.Content, &prompt.Project, &prompt.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan Legacy prompt export: %w", err)
		}
		results = append(results, prompt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export Legacy prompts: %w", err)
	}
	return results, nil
}

// PurgeLegacyPrompts directly deletes archive rows inside one explicit scope.
// It intentionally creates neither prompt tombstones nor sync mutations.
func (s *Store) PurgeLegacyPrompts(scope LegacyPromptScope) (int64, error) {
	scope, err := normalizeLegacyPromptScope(scope, false)
	if err != nil {
		return 0, err
	}
	where, args := legacyPromptScopePredicate(scope)
	var deleted int64
	if err := s.withTx(func(tx *sql.Tx) error {
		prompts, err := s.loadLegacyPromptPurgeRows(tx, where, args)
		if err != nil {
			return err
		}
		if err := s.purgeOwnedLegacyPromptFTSCopies(tx, prompts); err != nil {
			return err
		}
		res, err := s.execHook(tx, "DELETE FROM user_prompts WHERE "+where, args...)
		if err != nil {
			return fmt.Errorf("purge Legacy prompts: %w", err)
		}
		deleted, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("count purged Legacy prompts: %w", err)
		}
		if err := s.redactLegacyPromptMutationCopies(tx, scope, prompts); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (s *Store) loadLegacyPromptPurgeRows(tx *sql.Tx, where string, args []any) ([]legacyPromptPurgeRow, error) {
	rows, err := s.queryItHook(tx, `
		SELECT id, ifnull(sync_id, ''), content, project
		FROM user_prompts
		WHERE `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("load Legacy prompt purge scope: %w", err)
	}
	defer rows.Close()

	results := make([]legacyPromptPurgeRow, 0)
	for rows.Next() {
		var prompt legacyPromptPurgeRow
		if err := rows.Scan(&prompt.ID, &prompt.SyncID, &prompt.Content, &prompt.Project); err != nil {
			return nil, fmt.Errorf("scan Legacy prompt purge scope: %w", err)
		}
		results = append(results, prompt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load Legacy prompt purge scope: %w", err)
	}
	return results, nil
}

// purgeOwnedLegacyPromptFTSCopies removes matching rows only when prompts_fts
// is the exact historical external-content table and no customized DELETE
// trigger can conflict with the canonical FTS5 delete operation. Customized or
// unknown FTS ownership blocks the entire transaction so purge cannot report a
// partial success while a user-owned full-content copy remains.
func (s *Store) purgeOwnedLegacyPromptFTSCopies(tx *sql.Tx, prompts []legacyPromptPurgeRow) error {
	owned, exists, err := exactCanonicalLegacyPromptFTSTable(tx)
	if err != nil || !exists {
		return err
	}
	if !owned {
		return fmt.Errorf("%w: prompts_fts table is not the canonical Engram object", ErrLegacyPromptPurgeCustomizedFTS)
	}
	deleteMode, err := canonicalLegacyPromptFTSDeleteMode(tx)
	if err != nil {
		return err
	}
	switch deleteMode {
	case legacyPromptFTSDeleteByCanonicalTrigger:
		// The following user_prompts DELETE invokes the exact historical trigger.
		return nil
	case legacyPromptFTSDeleteDirect:
		for _, prompt := range prompts {
			var project any
			if prompt.Project.Valid {
				project = prompt.Project.String
			}
			if _, err := s.execHook(tx, `
				INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
				VALUES ('delete', ?, ?, ?)`, prompt.ID, prompt.Content, project); err != nil {
				return fmt.Errorf("purge canonical Legacy prompt FTS copy #%d: %w", prompt.ID, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: prompt FTS delete trigger is customized", ErrLegacyPromptPurgeCustomizedFTS)
	}
}

const (
	legacyPromptFTSDeleteUnsafe = iota
	legacyPromptFTSDeleteDirect
	legacyPromptFTSDeleteByCanonicalTrigger
)

func exactCanonicalLegacyPromptFTSTable(tx *sql.Tx) (owned, exists bool, err error) {
	var name, ddl string
	err = tx.QueryRow(`SELECT name, ifnull(sql, '') FROM sqlite_schema WHERE type = 'table' AND lower(name) = 'prompts_fts'`).Scan(&name, &ddl)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("inspect Legacy prompt FTS table ownership: %w", err)
	}
	if name != "prompts_fts" {
		return false, true, nil
	}
	_, expected, _ := canonicalLegacyPromptFTSObject("prompts_fts")
	return canonicalSQLiteDDL(ddl) == canonicalSQLiteDDL(expected), true, nil
}

func canonicalLegacyPromptFTSDeleteMode(tx *sql.Tx) (int, error) {
	rows, err := tx.Query(`
		SELECT name, ifnull(sql, '')
		FROM sqlite_schema
		WHERE type = 'trigger'
		  AND lower(tbl_name) = 'user_prompts'`)
	if err != nil {
		return legacyPromptFTSDeleteUnsafe, fmt.Errorf("inspect Legacy prompt FTS delete triggers: %w", err)
	}
	defer rows.Close()

	_, expected, _ := canonicalLegacyPromptFTSObject("prompt_fts_delete")
	found := false
	for rows.Next() {
		var name, ddl string
		if err := rows.Scan(&name, &ddl); err != nil {
			return legacyPromptFTSDeleteUnsafe, fmt.Errorf("scan Legacy prompt FTS delete trigger: %w", err)
		}
		if !sqliteDDLHasTokenSequence(ddl, "delete", "on", "user_prompts") || !sqliteDDLReferencesIdentifier(ddl, "prompts_fts") {
			continue
		}
		if name != "prompt_fts_delete" || canonicalSQLiteDDL(ddl) != canonicalSQLiteDDL(expected) {
			return legacyPromptFTSDeleteUnsafe, nil
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return legacyPromptFTSDeleteUnsafe, fmt.Errorf("inspect Legacy prompt FTS delete triggers: %w", err)
	}
	if found {
		return legacyPromptFTSDeleteByCanonicalTrigger, nil
	}
	return legacyPromptFTSDeleteDirect, nil
}

func (s *Store) redactLegacyPromptMutationCopies(tx *sql.Tx, scope LegacyPromptScope, prompts []legacyPromptPurgeRow) error {
	purgedSyncIDs := make(map[string]struct{}, len(prompts))
	for _, prompt := range prompts {
		if prompt.SyncID != "" {
			purgedSyncIDs[prompt.SyncID] = struct{}{}
		}
	}

	rows, err := s.queryItHook(tx, `
		SELECT seq, entity_key, payload, ifnull(project, '')
		FROM sync_mutations
		WHERE entity = 'prompt'`)
	if err != nil {
		return fmt.Errorf("load Legacy prompt mutation copies: %w", err)
	}
	type mutationUpdate struct {
		seq     int64
		payload string
	}
	updates := make([]mutationUpdate, 0)
	for rows.Next() {
		var seq int64
		var entityKey, payload, journalProject string
		if err := rows.Scan(&seq, &entityKey, &payload, &journalProject); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan Legacy prompt mutation copy: %w", err)
		}
		_, exactPrompt := purgedSyncIDs[entityKey]
		fields := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(payload), &fields); err != nil {
			if exactPrompt || legacyPromptJournalMatchesScope(journalProject, scope) {
				updates = append(updates, mutationUpdate{seq: seq, payload: `{"purged":true}`})
			}
			continue
		}
		if !exactPrompt && !legacyPromptMutationMatchesScope(fields, journalProject, scope) {
			continue
		}
		hasContent := false
		for key := range fields {
			if strings.EqualFold(key, "content") {
				delete(fields, key)
				hasContent = true
			}
		}
		if !hasContent {
			continue
		}
		redacted, err := json.Marshal(fields)
		if err != nil {
			_ = rows.Close()
			return fmt.Errorf("redact Legacy prompt mutation #%d: %w", seq, err)
		}
		updates = append(updates, mutationUpdate{seq: seq, payload: string(redacted)})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("load Legacy prompt mutation copies: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close Legacy prompt mutation copies: %w", err)
	}
	for _, update := range updates {
		if _, err := s.execHook(tx, `UPDATE sync_mutations SET payload = ? WHERE seq = ?`, update.payload, update.seq); err != nil {
			return fmt.Errorf("persist Legacy prompt mutation redaction #%d: %w", update.seq, err)
		}
	}
	return nil
}

func legacyPromptJournalMatchesScope(journalProject string, scope LegacyPromptScope) bool {
	journalProject, _ = NormalizeProject(journalProject)
	if scope.Unowned {
		return journalProject == ""
	}
	return journalProject == scope.Project
}

func legacyPromptMutationMatchesScope(fields map[string]json.RawMessage, journalProject string, scope LegacyPromptScope) bool {
	journalProject, _ = NormalizeProject(journalProject)
	payloadProject := ""
	if raw, ok := fields["project"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &payloadProject); err != nil {
			return false
		}
		payloadProject, _ = NormalizeProject(payloadProject)
	}
	if payloadProject != "" && journalProject != "" && payloadProject != journalProject {
		return false
	}
	effectiveProject := payloadProject
	if effectiveProject == "" {
		effectiveProject = journalProject
	}
	if scope.Unowned {
		return effectiveProject == ""
	}
	return effectiveProject == scope.Project
}

func normalizeLegacyPromptScope(scope LegacyPromptScope, allowAll bool) (LegacyPromptScope, error) {
	project, _ := NormalizeProject(scope.Project)
	scope.Project = project
	selected := 0
	if project != "" {
		selected++
	}
	if scope.Unowned {
		selected++
	}
	if scope.All {
		selected++
	}
	if selected != 1 || (scope.All && !allowAll) {
		return LegacyPromptScope{}, ErrLegacyPromptInvalidScope
	}
	return scope, nil
}

func legacyPromptScopePredicate(scope LegacyPromptScope) (string, []any) {
	switch {
	case scope.All:
		return "", nil
	case scope.Unowned:
		return "ifnull(trim(project), '') = ''", nil
	default:
		return "ifnull(project, '') = ?", []any{strings.TrimSpace(scope.Project)}
	}
}
