package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

// OpenSessionIdentityRecovery opens existing local state without migrations,
// enrollment repair, retention cleanup, or creation of missing source data.
func OpenSessionIdentityRecovery(cfg Config) (*Store, error) {
	if !filepath.IsAbs(cfg.DataDir) {
		return nil, identityRecoveryError("invalid_data_directory", "recovery requires an absolute data directory")
	}
	path := filepath.Join(cfg.DataDir, "engram.db")
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("open existing recovery database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, identityRecoveryError("invalid_database", "recovery requires an existing regular SQLite database")
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String() + "?mode=rw"
	db, err := openDB("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{"PRAGMA busy_timeout=5000", "PRAGMA foreign_keys=ON"} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return &Store{db: db, cfg: cfg, hooks: defaultStoreHooks()}, nil
}

// SessionIdentityRecoveryRequest selects one exact, project-owned blank identity.
// IDs are opaque: only project names are normalized.
type SessionIdentityRecoveryRequest struct {
	Project     string `json:"project"`
	SourceID    string `json:"source_id"`
	CanonicalID string `json:"canonical_id"`
}

type SessionIdentityRecoveryMutation struct {
	Seq       int64  `json:"seq"`
	Entity    string `json:"entity"`
	EntityKey string `json:"entity_key"`
	Op        string `json:"op"`
}

type SessionIdentityRecoveryPlan struct {
	SessionIdentityRecoveryRequest
	Status             string                            `json:"status"`
	PreviewToken       string                            `json:"preview_token"`
	ObservationIDs     []int64                           `json:"observation_ids"`
	ArchivedPromptIDs  []int64                           `json:"archived_prompt_ids"`
	PendingMutations   []SessionIdentityRecoveryMutation `json:"pending_mutations"`
	PreservedMutations []SessionIdentityRecoveryMutation `json:"preserved_mutations"`
	BackupPath         string                            `json:"backup_path,omitempty"`
}

type SessionIdentityRecoveryError struct{ Code, Message string }

func (e *SessionIdentityRecoveryError) Error() string { return e.Message }
func identityRecoveryError(code, message string) error {
	return &SessionIdentityRecoveryError{Code: code, Message: message}
}

type identityRecoveryRows struct {
	Columns []string
	Rows    [][]any
}

// Include full target state in the digest, but expose only reference metadata.
func readIdentityRecoveryRows(tx *sql.Tx, query string, args ...any) (identityRecoveryRows, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return identityRecoveryRows{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return identityRecoveryRows{}, err
	}
	result := identityRecoveryRows{Columns: columns, Rows: [][]any{}}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return result, err
		}
		result.Rows = append(result.Rows, values)
	}
	return result, rows.Err()
}

func (r identityRecoveryRows) value(row []any, column string) any {
	for i, name := range r.Columns {
		if name == column {
			return row[i]
		}
	}
	return nil
}
func (r identityRecoveryRows) text(row []any, column string) string {
	v := r.value(row, column)
	if v == nil {
		return ""
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(v)
}

func decodeIdentityRecoveryPayload(raw string) (map[string]json.RawMessage, error) {
	if !utf8.ValidString(raw) {
		return nil, fmt.Errorf("payload is not valid UTF-8")
	}
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, `"`) {
		var inner string
		if err := json.Unmarshal([]byte(raw), &inner); err != nil {
			return nil, err
		}
		raw = inner
	}
	if !json.Valid([]byte(raw)) {
		return nil, fmt.Errorf("payload is not valid JSON")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, fmt.Errorf("payload must be an object")
	}
	body := map[string]json.RawMessage{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key := keyToken.(string)
		if _, exists := body[key]; exists {
			return nil, fmt.Errorf("payload fields must be unique")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		body[key] = value
	}
	return body, nil
}

func decodeIdentityRecoveryString(raw json.RawMessage) (string, error) {
	var value string
	if len(raw) == 0 || raw[0] != '"' {
		return value, fmt.Errorf("identity must be a JSON string")
	}
	err := json.Unmarshal(raw, &value)
	return value, err
}

type identityRecoveryMutationChange struct {
	Seq                int64
	EntityKey, Payload string
}
type identityRecoveryState struct {
	Plan                     SessionIdentityRecoveryPlan
	Changes                  []identityRecoveryMutationChange
	ObservationUpdateTrigger string
}

const identityRecoveryObservationUpdateTrigger = `CREATE TRIGGER obs_fts_update AFTER UPDATE ON observations BEGIN
	INSERT INTO observations_fts(observations_fts, rowid, title, content, tool_name, type, project, topic_key)
	VALUES ('delete', old.id, old.title, old.content, old.tool_name, old.type, old.project, old.topic_key);
	INSERT INTO observations_fts(rowid, title, content, tool_name, type, project, topic_key)
	VALUES (new.id, new.title, new.content, new.tool_name, new.type, new.project, new.topic_key);
END`

func inspectIdentityRecoverySchema(tx *sql.Tx) (identityRecoveryRows, string, error) {
	objects, err := readIdentityRecoveryRows(tx, `SELECT type,name,tbl_name,ifnull(sql,'') AS ddl FROM sqlite_schema WHERE tbl_name IN ('sessions','observations','user_prompts','sync_mutations','session_identity_recoveries') ORDER BY type,name`)
	if err != nil {
		return objects, "", err
	}
	updateTrigger := ""
	for _, row := range objects.Rows {
		if objects.text(row, "type") != "trigger" {
			continue
		}
		name, ddl := objects.text(row, "name"), objects.text(row, "ddl")
		if name == "obs_fts_update" && canonicalSQLiteDDL(ddl) == canonicalSQLiteDDL(identityRecoveryObservationUpdateTrigger) {
			updateTrigger = ddl
			continue
		}
		// Only the two canonical observation insert/delete triggers are harmless:
		// this operation updates observations rather than inserting or deleting.
		if objects.text(row, "tbl_name") == "observations" && (name == "obs_fts_insert" || name == "obs_fts_delete") {
			if (name == "obs_fts_insert" && sqliteDDLHasTokenSequence(ddl, "after", "insert", "on", "observations")) || (name == "obs_fts_delete" && sqliteDDLHasTokenSequence(ddl, "after", "delete", "on", "observations")) {
				continue
			}
		}
		return objects, "", identityRecoveryError("unsafe_schema_object", fmt.Sprintf("cannot guarantee identity-only recovery with trigger %q", name))
	}
	tables, err := readIdentityRecoveryRows(tx, `SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name`)
	if err != nil {
		return objects, "", err
	}
	for _, row := range tables.Rows {
		name := tables.text(row, "name")
		if name == "observations" || name == "user_prompts" || name == "session_identity_recoveries" {
			continue
		}
		var dependent bool
		if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_list(?) WHERE lower("table")='sessions')`, name).Scan(&dependent); err != nil {
			return objects, "", err
		}
		if dependent {
			return objects, "", identityRecoveryError("unsafe_schema_object", fmt.Sprintf("unsupported session dependency in table %q", name))
		}
	}
	return objects, updateTrigger, nil
}

func validateIdentityRecoveryRequest(request SessionIdentityRecoveryRequest) (SessionIdentityRecoveryRequest, error) {
	request.Project, _ = NormalizeProject(request.Project)
	if request.Project == "" {
		return request, identityRecoveryError("project_required", "an explicit owning project is required")
	}
	if !isBlankSessionID(request.SourceID) {
		return request, identityRecoveryError("source_not_blank", "only a blank source session identity can be recovered")
	}
	if err := validateSessionID(request.CanonicalID); err != nil {
		return request, identityRecoveryError("canonical_id_required", "a nonblank operator-supplied canonical session ID is required")
	}
	return request, nil
}

func (s *Store) PreviewSessionIdentityRecovery(request SessionIdentityRecoveryRequest) (SessionIdentityRecoveryPlan, error) {
	request, err := validateIdentityRecoveryRequest(request)
	if err != nil {
		return SessionIdentityRecoveryPlan{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return SessionIdentityRecoveryPlan{}, err
	}
	defer tx.Rollback()
	state, err := s.identityRecoveryStateTx(tx, request)
	return state.Plan, err
}

func (s *Store) identityRecoveryStateTx(tx *sql.Tx, request SessionIdentityRecoveryRequest) (identityRecoveryState, error) {
	state := identityRecoveryState{Plan: SessionIdentityRecoveryPlan{SessionIdentityRecoveryRequest: request, Status: "planned", ObservationIDs: []int64{}, ArchivedPromptIDs: []int64{}, PendingMutations: []SessionIdentityRecoveryMutation{}, PreservedMutations: []SessionIdentityRecoveryMutation{}}}
	source, err := readIdentityRecoveryRows(tx, `SELECT rowid AS source_rowid, * FROM sessions WHERE id = ?`, request.SourceID)
	if err != nil {
		return state, err
	}
	var receiptTable bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='session_identity_recoveries')`).Scan(&receiptTable); err != nil {
		return state, err
	}
	if receiptTable {
		var destination, token, project string
		var rowid int64
		err := tx.QueryRow(`SELECT canonical_id, preview_token, destination_rowid FROM session_identity_recoveries WHERE project=? AND source_id=?`, request.Project, request.SourceID).Scan(&destination, &token, &rowid)
		if err == nil {
			if len(source.Rows) != 0 || destination != request.CanonicalID {
				return state, identityRecoveryError("recovery_conflict", "a recovery receipt conflicts with the selected source or destination")
			}
			var currentRowid int64
			if err := tx.QueryRow(`SELECT rowid, project FROM sessions WHERE id=?`, destination).Scan(&currentRowid, &project); err != nil || currentRowid != rowid || project != request.Project {
				return state, identityRecoveryError("recovery_conflict", "the receipt destination identity or ownership changed")
			}
			state.Plan.Status, state.Plan.PreviewToken = "already_recovered", token
			return state, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return state, err
		}
	}
	if len(source.Rows) != 1 {
		return state, identityRecoveryError("source_not_found", "the exact blank source session does not exist; destination existence alone is not proof of recovery")
	}
	if source.text(source.Rows[0], "project") != request.Project {
		return state, identityRecoveryError("ownership_conflict", "the source session does not belong to the explicit project")
	}
	var collision bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?)`, request.CanonicalID).Scan(&collision); err != nil {
		return state, err
	}
	if collision {
		return state, identityRecoveryError("destination_collision", "the canonical session ID already exists")
	}
	syncState, err := readIdentityRecoveryRows(tx, `SELECT * FROM sync_state WHERE target_key=?`, DefaultSyncTargetKey)
	if err != nil {
		return state, err
	}
	for _, row := range syncState.Rows {
		if syncState.text(row, "lease_owner") == "" {
			continue
		}
		until, err := time.Parse(time.RFC3339, syncState.text(row, "lease_until"))
		if err != nil || until.After(time.Now().UTC()) {
			return state, identityRecoveryError("sync_in_progress", "stop active synchronization and release its lease before recovering a session identity")
		}
	}
	schema, trigger, err := inspectIdentityRecoverySchema(tx)
	if err != nil {
		return state, err
	}
	state.ObservationUpdateTrigger = trigger
	observations, err := readIdentityRecoveryRows(tx, `SELECT * FROM observations WHERE session_id=? ORDER BY id`, request.SourceID)
	if err != nil {
		return state, err
	}
	prompts, err := readIdentityRecoveryRows(tx, `SELECT * FROM user_prompts WHERE session_id=? ORDER BY id`, request.SourceID)
	if err != nil {
		return state, err
	}
	ownedSyncIDs := map[string]bool{}
	for group, refs := range []identityRecoveryRows{observations, prompts} {
		for _, row := range refs.Rows {
			owner := refs.text(row, "project")
			if owner != "" && owner != request.Project {
				return state, identityRecoveryError("ownership_conflict", "a dependent row has conflicting project ownership")
			}
			id, ok := refs.value(row, "id").(int64)
			if !ok {
				return state, identityRecoveryError("unsafe_schema_object", "dependent rows must have canonical integer identities")
			}
			if group == 0 {
				state.Plan.ObservationIDs = append(state.Plan.ObservationIDs, id)
				ownedSyncIDs[refs.text(row, "sync_id")] = true
			} else {
				state.Plan.ArchivedPromptIDs = append(state.Plan.ArchivedPromptIDs, id)
			}
		}
	}
	journal, err := readIdentityRecoveryRows(tx, `SELECT * FROM sync_mutations WHERE entity IN ('session','observation','prompt') ORDER BY seq`)
	if err != nil {
		return state, err
	}
	affected := identityRecoveryRows{Columns: journal.Columns, Rows: [][]any{}}
	for _, row := range journal.Rows {
		entity, key := journal.text(row, "entity"), journal.text(row, "entity_key")
		body, decodeErr := decodeIdentityRecoveryPayload(journal.text(row, "payload"))
		stringField := func(field string) string { var v string; _ = json.Unmarshal(body[field], &v); return v }
		identityField := "session_id"
		if entity == SyncEntitySession {
			identityField = "id"
		}
		identity, identityErr := decodeIdentityRecoveryString(body[identityField])
		matchesBody := identityErr == nil && identity == request.SourceID
		matchesKey := entity == SyncEntitySession && key == request.SourceID
		matchesObservation := entity == SyncEntityObservation && key != "" && ownedSyncIDs[key]
		if !matchesBody && !matchesKey && !matchesObservation {
			continue
		}
		affected.Rows = append(affected.Rows, row)
		ref := SessionIdentityRecoveryMutation{Seq: journal.value(row, "seq").(int64), Entity: entity, EntityKey: key, Op: journal.text(row, "op")}
		pending := journal.value(row, "acked_at") == nil && journal.text(row, "disposition") == SyncMutationDispositionPending && entity != SyncEntityPrompt
		if !pending {
			state.Plan.PreservedMutations = append(state.Plan.PreservedMutations, ref)
			continue
		}
		if journal.text(row, "project") != request.Project || journal.text(row, "target_key") != DefaultSyncTargetKey || journal.text(row, "source") != SyncSourceLocal {
			return state, identityRecoveryError("ownership_conflict", "an affected pending mutation has conflicting project, target, or provenance")
		}
		if decodeErr != nil || body == nil || identityErr != nil {
			return state, identityRecoveryError("invalid_pending_payload", fmt.Sprintf("affected pending mutation %d has an invalid identity payload", ref.Seq))
		}
		if rawProject, exists := body["project"]; exists && string(rawProject) != "null" {
			if _, err := decodeIdentityRecoveryString(rawProject); err != nil {
				return state, identityRecoveryError("invalid_pending_payload", fmt.Sprintf("pending mutation %d has invalid project ownership metadata", ref.Seq))
			}
		}
		if !matchesBody || (entity == SyncEntitySession && key != request.SourceID) || (entity == SyncEntityObservation && (strings.TrimSpace(stringField("sync_id")) == "" || !ownedSyncIDs[stringField("sync_id")] || key != stringField("sync_id"))) || (stringField("project") != "" && stringField("project") != request.Project) || (ref.Op != SyncOpUpsert && ref.Op != SyncOpDelete) {
			return state, identityRecoveryError("ambiguous_pending_reference", fmt.Sprintf("pending mutation %d cannot be attributed unambiguously", ref.Seq))
		}
		body[identityField], _ = json.Marshal(request.CanonicalID)
		payload, err := json.Marshal(body)
		if err != nil {
			return state, err
		}
		if entity == SyncEntitySession {
			key = request.CanonicalID
		}
		state.Changes = append(state.Changes, identityRecoveryMutationChange{Seq: ref.Seq, EntityKey: key, Payload: string(payload)})
		state.Plan.PendingMutations = append(state.Plan.PendingMutations, ref)
	}
	encoded, err := json.Marshal([]any{request, source, observations, prompts, affected, schema, syncState})
	if err != nil {
		return state, err
	}
	digest := sha256.Sum256(encoded)
	state.Plan.PreviewToken = hex.EncodeToString(digest[:])
	return state, nil
}

func (s *Store) ApplySessionIdentityRecovery(request SessionIdentityRecoveryRequest, previewToken string) (SessionIdentityRecoveryPlan, error) {
	request, err := validateIdentityRecoveryRequest(request)
	if err != nil {
		return SessionIdentityRecoveryPlan{}, err
	}
	if strings.TrimSpace(previewToken) == "" {
		return SessionIdentityRecoveryPlan{}, identityRecoveryError("preview_required", "apply requires the exact preview token")
	}
	preview, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		return preview, err
	}
	if preview.PreviewToken != previewToken {
		return preview, identityRecoveryError("stale_preview", "the selected recovery targets changed; obtain a new preview")
	}
	if preview.Status == "already_recovered" {
		return preview, nil
	}
	backup, err := s.BackupSQLite()
	if err != nil {
		return preview, err
	}
	var result SessionIdentityRecoveryPlan
	err = s.withTx(func(tx *sql.Tx) error {
		state, err := s.identityRecoveryStateTx(tx, request)
		if err != nil {
			return err
		}
		if state.Plan.PreviewToken != previewToken {
			return identityRecoveryError("stale_preview", "the selected recovery targets changed after backup; obtain a new preview")
		}
		result = state.Plan
		if result.Status == "already_recovered" {
			return nil
		}
		if _, err := s.execHook(tx, `PRAGMA defer_foreign_keys=ON`); err != nil {
			return err
		}
		if _, err := s.execHook(tx, `UPDATE sessions SET id=? WHERE id=? AND project=?`, request.CanonicalID, request.SourceID, request.Project); err != nil {
			return err
		}
		// Session linkage does not change indexed content. Temporarily suspend the
		// verified canonical update trigger inside this transaction so soft-deleted
		// Memories remain absent from FTS and unchanged index entries stay intact.
		if state.ObservationUpdateTrigger != "" {
			if _, err := s.execHook(tx, `DROP TRIGGER obs_fts_update`); err != nil {
				return err
			}
		}
		if _, err := s.execHook(tx, `UPDATE observations SET session_id=? WHERE session_id=?`, request.CanonicalID, request.SourceID); err != nil {
			return err
		}
		if state.ObservationUpdateTrigger != "" {
			if _, err := s.execHook(tx, state.ObservationUpdateTrigger); err != nil {
				return err
			}
		}
		if _, err := s.execHook(tx, `UPDATE user_prompts SET session_id=? WHERE session_id=?`, request.CanonicalID, request.SourceID); err != nil {
			return err
		}
		for _, change := range state.Changes {
			if _, err := s.execHook(tx, `UPDATE sync_mutations SET entity_key=?,payload=? WHERE seq=?`, change.EntityKey, change.Payload, change.Seq); err != nil {
				return err
			}
		}
		if _, err := s.execHook(tx, `CREATE TABLE IF NOT EXISTS session_identity_recoveries (
			project TEXT NOT NULL, source_id TEXT NOT NULL, canonical_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
			destination_rowid INTEGER NOT NULL, preview_token TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY(project,source_id))`); err != nil {
			return err
		}
		if _, err := s.execHook(tx, `INSERT INTO session_identity_recoveries(project,source_id,canonical_id,destination_rowid,preview_token) SELECT ?,?,?,rowid,? FROM sessions WHERE id=?`, request.Project, request.SourceID, request.CanonicalID, previewToken, request.CanonicalID); err != nil {
			return err
		}
		result.Status, result.BackupPath = "applied", backup
		return nil
	})
	if err != nil {
		return SessionIdentityRecoveryPlan{}, err
	}
	return result, nil
}
