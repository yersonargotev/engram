package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrRecallSelectionUnavailable = errors.New("recall selection is unavailable")

const recallObservationSelectColumns = `o.id, ifnull(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
	       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.review_after, o.pinned, o.created_at, o.updated_at, o.deleted_at`

// RecallRunRecord is the content-free local boundary for one candidate Recall.
type RecallRunRecord struct {
	RecallID    string
	Project     string
	Scope       string
	AllProjects bool
	Results     []RecallResultRecord
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
}

func (s *Store) migrateRecallOperations() error {
	_, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS recall_runs (
			recall_id    TEXT PRIMARY KEY,
			project      TEXT NOT NULL DEFAULT '',
			scope        TEXT NOT NULL,
			all_projects BOOLEAN NOT NULL DEFAULT 0,
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
			created_at             TEXT    NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (recall_id, result_id, position),
			FOREIGN KEY (recall_id, result_id) REFERENCES recall_results(recall_id, result_id) ON DELETE CASCADE
		);
		CREATE INDEX IF NOT EXISTS idx_recall_results_observation ON recall_results(observation_id);
	`)
	return err
}

// RecordRecallRunContext atomically records one candidate response and every
// opaque result identity it actually exposed.
func (s *Store) RecordRecallRunContext(ctx context.Context, record RecallRunRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record.Project, _ = NormalizeProject(record.Project)
	record.Scope = NormalizeObservationScope(record.Scope)
	return s.withTx(func(tx *sql.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := s.execHook(tx, `INSERT INTO recall_runs (recall_id, project, scope, all_projects) VALUES (?, ?, ?, ?)`,
			record.RecallID, record.Project, record.Scope, record.AllProjects); err != nil {
			return fmt.Errorf("record recall run: %w", err)
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
		}
		return ctx.Err()
	})
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
	replayed := false
	err := s.withTx(func(tx *sql.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := s.execHook(tx, `
			INSERT OR IGNORE INTO recall_segments (
				recall_id, result_id, position, original_bytes, delivered_bytes,
				limit_bytes, truncated, continuation_position
			)
			SELECT rr.recall_id, rr.result_id, ?, ?, ?, ?, ?, ?
			FROM recall_results rr
			JOIN recall_runs run ON run.recall_id = rr.recall_id
			JOIN observations o ON o.id = rr.observation_id
			WHERE rr.recall_id = ? AND rr.result_id = ? AND rr.observation_id = ?
			  AND rr.revision_count = ? AND o.revision_count = rr.revision_count
			  AND rr.local_revision_count = ? AND o.local_revision_count = rr.local_revision_count
			  AND o.deleted_at IS NULL AND o.scope = run.scope
			  AND (run.all_projects = 1 OR LOWER(o.project) = run.project)`+searchEligibilitySQL("o", searchPolicy{activeOnly: true, excludeSuperseded: true}),
			record.Position, record.OriginalBytes, record.DeliveredBytes, record.LimitBytes,
			record.Truncated, continuation, record.RecallID, record.ResultID, record.ObservationID,
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
