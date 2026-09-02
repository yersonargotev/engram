package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

var ErrRecallSelectionUnavailable = errors.New("recall selection is unavailable")

const recallObservationSelectColumns = `o.id, ifnull(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
	       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.review_after, o.pinned, o.created_at, o.updated_at, o.deleted_at`

// RecallRunRecord is the content-free local boundary for one candidate Recall.
type RecallRunRecord struct {
	RecallID            string
	Project             string
	Scope               string
	AllProjects         bool
	DeliveredUTF8Bytes  int
	ElapsedMonotonicMS  int64
	ProtocolVersion     int
	BinaryVersion       string
	BinaryRevision      string
	TurnIdentity        *CheckpointIdentity
	StartedAtUnixNano   int64
	CompletedAtUnixNano int64
	MetricsPending      bool
	Results             []RecallResultRecord
}

// RecallObservationSnapshot is the transient selected Memory representation
// revalidated while a candidate Recall is committed. It is never persisted as
// operational content.
type RecallObservationSnapshot struct {
	ID            int64
	SyncID        string
	Title         string
	Type          string
	Content       string
	Project       string
	Scope         string
	RevisionCount int
}

// RecallResultRecord binds one opaque result identity to the exact selected
// Memory snapshot exposed by a Recall run.
type RecallResultRecord struct {
	ResultID string
	Snapshot RecallObservationSnapshot
	Rank     int
}

// RecallSelection is the exact current Memory selected from one Recall run.
type RecallSelection struct {
	Observation               *Observation
	RevisionCount             int
	LocalRevisionCount        int
	CurrentLocalRevisionCount int
}

// RecallSegmentRecord is content-free operational evidence for one explicit
// complete-content segment. The selected Memory revision and authority boundary
// are revalidated in the transaction without persisting Memory content.
type RecallSegmentRecord struct {
	RecallID             string
	ResultID             string
	ObservationID        int64
	RevisionCount        int
	LocalRevisionCount   int
	Position             int
	OriginalBytes        int
	DeliveredBytes       int
	LimitBytes           int
	Truncated            bool
	ContinuationPosition *int
	ElapsedMonotonicMS   int64
	ProtocolVersion      int
	BinaryVersion        string
	BinaryRevision       string
	MetricsPending       bool
}

func (s *Store) migrateRecallOperations() error {
	_, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS recall_runs (
			recall_id    TEXT PRIMARY KEY,
			project      TEXT NOT NULL DEFAULT '',
			scope        TEXT NOT NULL,
			all_projects BOOLEAN NOT NULL DEFAULT 0,
			result_count INTEGER,
			delivered_utf8_bytes INTEGER,
			elapsed_monotonic_ms INTEGER,
			protocol_version INTEGER,
			binary_version TEXT,
			binary_revision TEXT,
			turn_key     TEXT CHECK (turn_key IS NULL OR length(turn_key) = 64),
			started_at_unix_nano INTEGER,
			completed_at_unix_nano INTEGER,
			created_at   TEXT NOT NULL DEFAULT (datetime('now'))
		);
		CREATE TABLE IF NOT EXISTS recall_results (
			recall_id      TEXT    NOT NULL,
			result_id      TEXT    NOT NULL,
			observation_id INTEGER NOT NULL,
			revision_count INTEGER NOT NULL,
			local_revision_count INTEGER NOT NULL,
			result_rank    INTEGER NOT NULL,
			created_at     TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (recall_id, result_id),
			UNIQUE (recall_id, observation_id),
			FOREIGN KEY (recall_id) REFERENCES recall_runs(recall_id) ON DELETE CASCADE,
			FOREIGN KEY (observation_id) REFERENCES observations(id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS recall_segments (
			recall_id             TEXT    NOT NULL,
			result_id             TEXT    NOT NULL,
			position              INTEGER NOT NULL,
			original_bytes        INTEGER NOT NULL,
			delivered_bytes       INTEGER NOT NULL,
			limit_bytes           INTEGER NOT NULL,
			truncated             BOOLEAN NOT NULL,
			continuation_position INTEGER,
			elapsed_monotonic_ms   INTEGER,
			protocol_version      INTEGER,
			binary_version        TEXT,
			binary_revision       TEXT,
			created_at             TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (recall_id, result_id, position),
			FOREIGN KEY (recall_id, result_id) REFERENCES recall_results(recall_id, result_id) ON DELETE CASCADE
		);
		CREATE INDEX IF NOT EXISTS idx_recall_results_observation ON recall_results(observation_id);
	`)
	if err != nil {
		return err
	}
	for _, column := range []struct {
		table, name, definition string
	}{
		{table: "recall_runs", name: "delivered_utf8_bytes", definition: "INTEGER"},
		{table: "recall_runs", name: "result_count", definition: "INTEGER"},
		{table: "recall_runs", name: "elapsed_monotonic_ms", definition: "INTEGER"},
		{table: "recall_runs", name: "protocol_version", definition: "INTEGER"},
		{table: "recall_runs", name: "binary_version", definition: "TEXT"},
		{table: "recall_runs", name: "binary_revision", definition: "TEXT"},
		{table: "recall_runs", name: "turn_key", definition: "TEXT CHECK (turn_key IS NULL OR length(turn_key) = 64)"},
		{table: "recall_runs", name: "started_at_unix_nano", definition: "INTEGER"},
		{table: "recall_runs", name: "completed_at_unix_nano", definition: "INTEGER"},
		{table: "recall_segments", name: "elapsed_monotonic_ms", definition: "INTEGER"},
		{table: "recall_segments", name: "protocol_version", definition: "INTEGER"},
		{table: "recall_segments", name: "binary_version", definition: "TEXT"},
		{table: "recall_segments", name: "binary_revision", definition: "TEXT"},
	} {
		if err := s.addColumnIfNotExists(column.table, column.name, column.definition); err != nil {
			return err
		}
	}
	return s.migrateLegacyRecallResults()
}

// migrateLegacyRecallResults upgrades the short-lived pre-release schema that
// identified selected content by SHA-256. Only selections whose content still
// matches that hash can be mapped safely to the current revision snapshot.
func (s *Store) migrateLegacyRecallResults() error {
	rows, err := s.queryItHook(s.db, `PRAGMA table_info(recall_results)`)
	if err != nil {
		return fmt.Errorf("inspect legacy recall_results schema: %w", err)
	}
	hasContentHash := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return closeRowsWithError(rows, fmt.Errorf("inspect legacy recall_results schema: %w", err))
		}
		if name == "content_hash" {
			hasContentHash = true
		}
	}
	if err := rows.Err(); err != nil {
		return closeRowsWithError(rows, fmt.Errorf("inspect legacy recall_results schema: %w", err))
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("inspect legacy recall_results schema: %w", err)
	}
	if !hasContentHash {
		return nil
	}

	tx, err := s.beginTxHook()
	if err != nil {
		return fmt.Errorf("migrate legacy recall_results: begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := s.execHook(tx, `
		CREATE TABLE recall_results_migrated (
			recall_id      TEXT    NOT NULL,
			result_id      TEXT    NOT NULL,
			observation_id INTEGER NOT NULL,
			revision_count INTEGER NOT NULL,
			local_revision_count INTEGER NOT NULL,
			result_rank    INTEGER NOT NULL,
			created_at     TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (recall_id, result_id),
			UNIQUE (recall_id, observation_id),
			FOREIGN KEY (recall_id) REFERENCES recall_runs(recall_id) ON DELETE CASCADE,
			FOREIGN KEY (observation_id) REFERENCES observations(id) ON DELETE CASCADE
		);
		CREATE TABLE recall_segments_migrated (
			recall_id             TEXT    NOT NULL,
			result_id             TEXT    NOT NULL,
			position              INTEGER NOT NULL,
			original_bytes        INTEGER NOT NULL,
			delivered_bytes       INTEGER NOT NULL,
			limit_bytes           INTEGER NOT NULL,
			truncated             BOOLEAN NOT NULL,
			continuation_position INTEGER,
			elapsed_monotonic_ms   INTEGER,
			protocol_version       INTEGER,
			binary_version         TEXT,
			binary_revision        TEXT,
			created_at             TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (recall_id, result_id, position),
			FOREIGN KEY (recall_id, result_id) REFERENCES recall_results_migrated(recall_id, result_id) ON DELETE CASCADE
		);
	`); err != nil {
		return fmt.Errorf("migrate legacy recall_results: create replacement tables: %w", err)
	}

	legacyRows, err := s.queryItHook(tx, `
		SELECT result.recall_id, result.result_id, result.observation_id,
		       result.content_hash, result.result_rank, result.created_at,
		       observation.content, observation.revision_count, observation.local_revision_count
		FROM recall_results result
		JOIN recall_runs run ON run.recall_id = result.recall_id
		JOIN observations observation ON observation.id = result.observation_id
		ORDER BY result.recall_id, result.result_rank
	`)
	if err != nil {
		return fmt.Errorf("migrate legacy recall_results: load selections: %w", err)
	}
	type validLegacyResult struct {
		recallID, resultID, createdAt string
		observationID                 int64
		resultRank                    int
		revisionCount                 int
		localRevisionCount            int
	}
	validResults := make([]validLegacyResult, 0)
	for legacyRows.Next() {
		var result validLegacyResult
		var contentHash, content string
		if err := legacyRows.Scan(
			&result.recallID, &result.resultID, &result.observationID,
			&contentHash, &result.resultRank, &result.createdAt,
			&content, &result.revisionCount, &result.localRevisionCount,
		); err != nil {
			return closeRowsWithError(legacyRows, fmt.Errorf("migrate legacy recall_results: scan selection: %w", err))
		}
		digest := sha256.Sum256([]byte(content))
		if contentHash == hex.EncodeToString(digest[:]) {
			validResults = append(validResults, result)
		}
	}
	if err := legacyRows.Err(); err != nil {
		return closeRowsWithError(legacyRows, fmt.Errorf("migrate legacy recall_results: load selections: %w", err))
	}
	if err := legacyRows.Close(); err != nil {
		return fmt.Errorf("migrate legacy recall_results: load selections: %w", err)
	}

	for _, result := range validResults {
		if _, err := s.execHook(tx, `
			INSERT INTO recall_results_migrated (
				recall_id, result_id, observation_id, revision_count,
				local_revision_count, result_rank, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		`, result.recallID, result.resultID, result.observationID, result.revisionCount,
			result.localRevisionCount, result.resultRank, result.createdAt); err != nil {
			return fmt.Errorf("migrate legacy recall_results: preserve selection: %w", err)
		}
	}
	if _, err := s.execHook(tx, `
		INSERT INTO recall_segments_migrated (
			recall_id, result_id, position, original_bytes, delivered_bytes,
			limit_bytes, truncated, continuation_position, elapsed_monotonic_ms,
			protocol_version, binary_version, binary_revision, created_at
		)
		SELECT segment.recall_id, segment.result_id, segment.position,
		       segment.original_bytes, segment.delivered_bytes, segment.limit_bytes,
		       segment.truncated, segment.continuation_position, segment.elapsed_monotonic_ms,
		       segment.protocol_version, segment.binary_version, segment.binary_revision,
		       segment.created_at
		FROM recall_segments segment
		JOIN recall_results_migrated result
		  ON result.recall_id = segment.recall_id AND result.result_id = segment.result_id;
		DROP TABLE recall_segments;
		DROP TABLE recall_results;
		ALTER TABLE recall_results_migrated RENAME TO recall_results;
		ALTER TABLE recall_segments_migrated RENAME TO recall_segments;
		CREATE INDEX idx_recall_results_observation ON recall_results(observation_id);
	`); err != nil {
		return fmt.Errorf("migrate legacy recall_results: replace tables: %w", err)
	}
	if err := s.commitHook(tx); err != nil {
		return fmt.Errorf("migrate legacy recall_results: commit: %w", err)
	}
	return nil
}

// RecordRecallRunContext atomically records one candidate response and every
// opaque result identity it actually exposed. Root-bound runs also snapshot
// only salted attribution keys so unknown exposure cohorts survive Memory
// lifecycle changes.
func (s *Store) RecordRecallRunContext(ctx context.Context, record RecallRunRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record.Project, _ = NormalizeProject(record.Project)
	record.Scope = NormalizeObservationScope(record.Scope)
	elapsedMonotonicMS := any(record.ElapsedMonotonicMS)
	completedAtUnixNano := nullableNonZeroInt64(record.CompletedAtUnixNano)
	if record.MetricsPending {
		elapsedMonotonicMS = nil
		completedAtUnixNano = nil
	}
	var turnKey any
	var feedbackSalt []byte
	var feedbackRunKey, feedbackTurnKey string
	if record.TurnIdentity != nil {
		if err := validateCheckpointIdentity(*record.TurnIdentity); err != nil {
			return err
		}
		salt, err := loadOrCreateRecallFeedbackSalt(s.cfg.DataDir)
		if err != nil {
			return fmt.Errorf("load Recall attribution salt: %w", err)
		}
		feedbackSalt = salt
		feedbackRunKey = recallFeedbackDigest(salt, "run", record.RecallID)
		feedbackTurnKey = recallFeedbackDigest(salt, "turn", record.TurnIdentity.Host, record.TurnIdentity.SessionID, record.TurnIdentity.RootTurnID)
		turnKey = feedbackTurnKey
	}
	return s.withTx(func(tx *sql.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := s.execHook(tx, `
			INSERT INTO recall_runs (
				recall_id, project, scope, all_projects, result_count, delivered_utf8_bytes,
				elapsed_monotonic_ms, protocol_version, binary_version, binary_revision,
				turn_key, started_at_unix_nano, completed_at_unix_nano
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			record.RecallID, record.Project, record.Scope, record.AllProjects,
			len(record.Results), record.DeliveredUTF8Bytes, elapsedMonotonicMS, record.ProtocolVersion,
			record.BinaryVersion, record.BinaryRevision, turnKey,
			nullableNonZeroInt64(record.StartedAtUnixNano), completedAtUnixNano); err != nil {
			return fmt.Errorf("record recall run: %w", err)
		}
		if feedbackRunKey != "" {
			if _, err := s.execHook(tx, `
				INSERT INTO recall_feedback_runs (
					run_key, turn_key, result_count, delivered_utf8_bytes,
					elapsed_monotonic_ms, protocol_version, binary_version, binary_revision
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				feedbackRunKey, feedbackTurnKey, len(record.Results), record.DeliveredUTF8Bytes,
				elapsedMonotonicMS, record.ProtocolVersion, record.BinaryVersion, record.BinaryRevision); err != nil {
				return fmt.Errorf("snapshot root-turn Recall run: %w", err)
			}
		}
		for _, result := range record.Results {
			if err := ctx.Err(); err != nil {
				return err
			}
			snapshot := result.Snapshot
			snapshot.Project, _ = NormalizeProject(snapshot.Project)
			snapshot.Scope = NormalizeObservationScope(snapshot.Scope)
			res, err := s.execHook(tx, `
				INSERT INTO recall_results (
					recall_id, result_id, observation_id, revision_count,
					local_revision_count, result_rank
				)
				SELECT ?, ?, o.id, o.revision_count, o.local_revision_count, ?
				FROM observations o
				JOIN recall_runs run ON run.recall_id = ?
				WHERE o.id = ? AND ifnull(o.sync_id, '') = ?
				  AND o.title = ? AND o.type = ? AND o.content = ?
				  AND LOWER(ifnull(o.project, '')) = ? AND o.scope = ? AND o.revision_count = ?
				  AND o.deleted_at IS NULL AND o.scope = run.scope
				  AND (run.all_projects = 1 OR LOWER(o.project) = run.project)`+searchEligibilitySQL("o", searchPolicy{activeOnly: true, excludeSuperseded: true}),
				record.RecallID, result.ResultID, result.Rank, record.RecallID,
				snapshot.ID, snapshot.SyncID, snapshot.Title, snapshot.Type, snapshot.Content,
				snapshot.Project, snapshot.Scope, snapshot.RevisionCount)
			if err != nil {
				return fmt.Errorf("record recall result: %w", err)
			}
			if affected, _ := res.RowsAffected(); affected != 1 {
				return fmt.Errorf("record recall result: %w", ErrRecallSelectionUnavailable)
			}
			if feedbackRunKey != "" {
				memoryKey := recallFeedbackDigest(feedbackSalt, "memory", snapshot.SyncID)
				if _, err := s.execHook(tx, `
					INSERT INTO recall_feedback_exposures (run_key, memory_key, result_rank)
					VALUES (?, ?, ?)`, feedbackRunKey, memoryKey, result.Rank); err != nil {
					return fmt.Errorf("snapshot root-turn Recall exposure: %w", err)
				}
			}
		}
		return ctx.Err()
	})
}

// CompleteRecallRunContext persists measurements captured only after the
// primary Recall response and exposure snapshot have committed.
func (s *Store) CompleteRecallRunContext(ctx context.Context, recallID string, elapsedMonotonicMS, completedAtUnixNano int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var turnKey sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT turn_key FROM recall_runs WHERE recall_id = ?`, recallID).Scan(&turnKey); errors.Is(err, sql.ErrNoRows) {
		return ErrRecallSelectionUnavailable
	} else if err != nil {
		return fmt.Errorf("load Recall run completion target: %w", err)
	}
	var feedbackRunKey string
	if turnKey.Valid {
		salt, err := readRecallFeedbackSalt(s.cfg.DataDir)
		if err != nil {
			return fmt.Errorf("load Recall attribution salt: %w", err)
		}
		feedbackRunKey = recallFeedbackDigest(salt, "run", recallID)
	}
	return s.withTx(func(tx *sql.Tx) error {
		res, err := s.execHook(tx, `
			UPDATE recall_runs
			SET elapsed_monotonic_ms = ?, completed_at_unix_nano = ?
			WHERE recall_id = ?`, elapsedMonotonicMS, completedAtUnixNano, recallID)
		if err != nil {
			return fmt.Errorf("complete Recall run metrics: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected != 1 {
			return ErrRecallSelectionUnavailable
		}
		if feedbackRunKey == "" {
			return ctx.Err()
		}
		res, err = s.execHook(tx, `
			UPDATE recall_feedback_runs SET elapsed_monotonic_ms = ? WHERE run_key = ?`,
			elapsedMonotonicMS, feedbackRunKey)
		if err != nil {
			return fmt.Errorf("complete attributed Recall run metrics: %w", err)
		}
		if affected, _ := res.RowsAffected(); affected != 1 {
			return ErrRecallSelectionUnavailable
		}
		return ctx.Err()
	})
}

func nullableNonZeroInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

// RecallSelectionContext resolves only the result selected from the exact
// stored scope. Deleted, stale, superseded, or boundary-mismatched Memories are
// unavailable and never expose content.
func (s *Store) RecallSelectionContext(ctx context.Context, recallID, resultID, project, scope string, allProjects bool) (*RecallSelection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	project, _ = NormalizeProject(project)
	scope = NormalizeObservationScope(scope)
	query := `SELECT rr.revision_count, rr.local_revision_count, o.local_revision_count, ` + recallObservationSelectColumns + `
		FROM recall_results rr
		JOIN recall_runs run ON run.recall_id = rr.recall_id
		JOIN observations o ON o.id = rr.observation_id
		WHERE rr.recall_id = ? AND rr.result_id = ?
		  AND run.project = ? AND run.scope = ? AND run.all_projects = ?
		  AND o.deleted_at IS NULL AND o.scope = run.scope`
	query += searchEligibilitySQL("o", searchPolicy{activeOnly: true, excludeSuperseded: true})
	if !allProjects {
		query += " AND LOWER(o.project) = run.project"
	}
	row := s.db.QueryRowContext(ctx, query, recallID, resultID, project, scope, allProjects)
	selection := &RecallSelection{Observation: &Observation{}}
	if err := row.Scan(
		&selection.RevisionCount,
		&selection.LocalRevisionCount,
		&selection.CurrentLocalRevisionCount,
		&selection.Observation.ID, &selection.Observation.SyncID, &selection.Observation.SessionID,
		&selection.Observation.Type, &selection.Observation.Title, &selection.Observation.Content,
		&selection.Observation.ToolName, &selection.Observation.Project, &selection.Observation.Scope,
		&selection.Observation.TopicKey, &selection.Observation.RevisionCount, &selection.Observation.DuplicateCount,
		&selection.Observation.LastSeenAt, &selection.Observation.ReviewAfter, &selection.Observation.Pinned,
		&selection.Observation.CreatedAt, &selection.Observation.UpdatedAt, &selection.Observation.DeletedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecallSelectionUnavailable
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("load recall selection: %w", err)
	}
	return selection, nil
}

// RecallContinuationAuthorizedContext reports whether position was exposed as
// the continuation of a previously delivered segment for this exact selection.
func (s *Store) RecallContinuationAuthorizedContext(ctx context.Context, recallID, resultID string, position int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var exists int
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM recall_segments
			WHERE recall_id = ? AND result_id = ? AND continuation_position = ?
		)`, recallID, resultID, position).Scan(&exists)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, fmt.Errorf("validate recall continuation: %w", err)
	}
	return exists == 1, nil
}

// RecordRecallSegment atomically revalidates the selected content and records
// one content-free segment. Exact replay is idempotent.
func (s *Store) RecordRecallSegment(record RecallSegmentRecord) (bool, error) {
	return s.RecordRecallSegmentContext(context.Background(), record)
}

func (s *Store) RecordRecallSegmentContext(ctx context.Context, record RecallSegmentRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	continuation := any(nil)
	if record.ContinuationPosition != nil {
		continuation = *record.ContinuationPosition
	}
	elapsedMonotonicMS := any(record.ElapsedMonotonicMS)
	if record.MetricsPending {
		elapsedMonotonicMS = nil
	}
	replayed := false
	err := s.withTx(func(tx *sql.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := s.execHook(tx, `
			INSERT OR IGNORE INTO recall_segments (
				recall_id, result_id, position, original_bytes, delivered_bytes,
				limit_bytes, truncated, continuation_position, elapsed_monotonic_ms,
				protocol_version, binary_version, binary_revision
			)
			SELECT rr.recall_id, rr.result_id, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
			FROM recall_results rr
			JOIN recall_runs run ON run.recall_id = rr.recall_id
			JOIN observations o ON o.id = rr.observation_id
			WHERE rr.recall_id = ? AND rr.result_id = ? AND rr.observation_id = ?
			  AND rr.revision_count = ? AND o.revision_count = rr.revision_count
			  AND rr.local_revision_count = ? AND o.local_revision_count = rr.local_revision_count
			  AND o.deleted_at IS NULL AND o.scope = run.scope
			  AND (run.all_projects = 1 OR LOWER(o.project) = run.project)`+searchEligibilitySQL("o", searchPolicy{activeOnly: true, excludeSuperseded: true}),
			record.Position, record.OriginalBytes, record.DeliveredBytes, record.LimitBytes,
			record.Truncated, continuation, elapsedMonotonicMS, record.ProtocolVersion,
			record.BinaryVersion, record.BinaryRevision,
			record.RecallID, record.ResultID, record.ObservationID,
			record.RevisionCount, record.LocalRevisionCount)
		if err != nil {
			return fmt.Errorf("record recall segment: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if affected, _ := res.RowsAffected(); affected == 1 {
			return nil
		}
		var originalBytes, deliveredBytes, limitBytes int
		var truncated bool
		var storedContinuation sql.NullInt64
		err = tx.QueryRow(`
			SELECT segment.original_bytes, segment.delivered_bytes, segment.limit_bytes,
			       segment.truncated, segment.continuation_position
			FROM recall_segments segment
			JOIN recall_results rr
			  ON rr.recall_id = segment.recall_id AND rr.result_id = segment.result_id
			JOIN recall_runs run ON run.recall_id = rr.recall_id
			JOIN observations o ON o.id = rr.observation_id
			WHERE segment.recall_id = ? AND segment.result_id = ? AND segment.position = ?
			  AND rr.observation_id = ? AND rr.revision_count = ? AND o.revision_count = rr.revision_count
			  AND rr.local_revision_count = ? AND o.local_revision_count = rr.local_revision_count
			  AND o.deleted_at IS NULL AND o.scope = run.scope
			  AND (run.all_projects = 1 OR LOWER(o.project) = run.project)`+
			searchEligibilitySQL("o", searchPolicy{activeOnly: true, excludeSuperseded: true}),
			record.RecallID, record.ResultID, record.Position, record.ObservationID,
			record.RevisionCount, record.LocalRevisionCount).Scan(
			&originalBytes, &deliveredBytes, &limitBytes, &truncated, &storedContinuation)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrRecallSelectionUnavailable
		}
		if err != nil {
			return fmt.Errorf("load replayed recall segment: %w", err)
		}
		wantContinuation := record.ContinuationPosition != nil
		if originalBytes != record.OriginalBytes || deliveredBytes != record.DeliveredBytes || limitBytes != record.LimitBytes ||
			truncated != record.Truncated || storedContinuation.Valid != wantContinuation ||
			(storedContinuation.Valid && int(storedContinuation.Int64) != *record.ContinuationPosition) {
			return ErrRecallSelectionUnavailable
		}
		replayed = true
		return ctx.Err()
	})
	return replayed, err
}

// CompleteRecallSegmentContext persists first-delivery latency only after the
// segment and its continuation authority have committed.
func (s *Store) CompleteRecallSegmentContext(ctx context.Context, recallID, resultID string, position int, elapsedMonotonicMS int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	res, err := s.execHook(s.db, `
		UPDATE recall_segments SET elapsed_monotonic_ms = ?
		WHERE recall_id = ? AND result_id = ? AND position = ?`,
		elapsedMonotonicMS, recallID, resultID, position)
	if err != nil {
		return fmt.Errorf("complete Recall segment metrics: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		return ErrRecallSelectionUnavailable
	}
	return ctx.Err()
}
