package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	CheckpointDispositionSaved       = "saved"
	CheckpointDispositionSkipped     = "skipped"
	CheckpointDispositionNeedsReview = "needs_review"

	CheckpointReasonVocabularyVersion      = 1
	CheckpointSkipReasonNoDurableKnowledge = "no_durable_knowledge"

	maxCheckpointHostBytes     = 64
	maxCheckpointOpaqueIDBytes = 255
)

var (
	ErrCheckpointInvalidIdentity   = errors.New("invalid checkpoint identity")
	ErrCheckpointInvalidReason     = errors.New("invalid checkpoint reason")
	ErrCheckpointInvalidReferences = errors.New("invalid checkpoint references")
	ErrCheckpointMemoryNotFound    = errors.New("checkpoint Memory not found")
	ErrCheckpointProjectMismatch   = errors.New("checkpoint Memory belongs to a different project")
	ErrCheckpointConflict          = errors.New("checkpoint already recorded with a different terminal result")
	ErrCheckpointNotFound          = errors.New("checkpoint not found")
)

// CheckpointIdentity is the opaque, host-provided idempotency key for one
// settled root user turn. It is intentionally independent from Engram sessions.
type CheckpointIdentity struct {
	Host       string `json:"host"`
	SessionID  string `json:"session_id"`
	RootTurnID string `json:"root_turn_id"`
}

// MemoryCheckpoint is local operational state proving that a root user turn
// reached a terminal Memory disposition. It is not a Memory and never enters
// Memory search, context, export, or sync surfaces.
type MemoryCheckpoint struct {
	Identity      CheckpointIdentity    `json:"identity"`
	Disposition   string                `json:"disposition"`
	ReasonCode    string                `json:"reason_code,omitempty"`
	ReasonVersion int                   `json:"reason_version,omitempty"`
	References    []CheckpointReference `json:"references,omitempty"`
	CreatedAt     string                `json:"created_at"`
	UpdatedAt     string                `json:"updated_at"`
}

const CheckpointReferenceKindMemory = "memory"

// CheckpointReference is an immutable, local-only pointer from a terminal
// checkpoint to the durable object that justified its disposition.
type CheckpointReference struct {
	Kind         string `json:"kind"`
	MemoryID     int64  `json:"memory_id"`
	MemorySyncID string `json:"memory_sync_id"`
	Project      string `json:"project"`
}

type RecordSkippedCheckpointParams struct {
	Identity   CheckpointIdentity
	ReasonCode string
}

type RecordSavedCheckpointParams struct {
	Identity  CheckpointIdentity
	Project   string
	Directory string
	MemoryIDs []int64
	Memories  []AddObservationParams
}

// migrateMemoryCheckpoints creates the local-only checkpoint ledger. The table
// deliberately has no sync triggers and is not part of ExportData.
func (s *Store) migrateMemoryCheckpoints() error {
	_, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS memory_checkpoints (
			id             INTEGER PRIMARY KEY AUTOINCREMENT,
			host           TEXT    NOT NULL CHECK (length(host) BETWEEN 1 AND 64),
			session_id     TEXT    NOT NULL CHECK (length(session_id) BETWEEN 1 AND 255),
			root_turn_id   TEXT    NOT NULL CHECK (length(root_turn_id) BETWEEN 1 AND 255),
			disposition    TEXT    NOT NULL CHECK (disposition IN ('saved', 'skipped', 'needs_review')),
			reason_code    TEXT,
			reason_version INTEGER,
			created_at     TEXT    NOT NULL DEFAULT (datetime('now')),
			updated_at     TEXT    NOT NULL DEFAULT (datetime('now')),
			UNIQUE (host, session_id, root_turn_id),
			CHECK (
				(disposition = 'skipped' AND reason_code IS NOT NULL AND reason_version IS NOT NULL)
				OR
				(disposition IN ('saved', 'needs_review') AND reason_code IS NULL AND reason_version IS NULL)
			)
			);

			CREATE TABLE IF NOT EXISTS memory_checkpoint_references (
				checkpoint_id  INTEGER NOT NULL REFERENCES memory_checkpoints(id) ON DELETE CASCADE,
				reference_order INTEGER NOT NULL,
				reference_kind TEXT    NOT NULL CHECK (reference_kind = 'memory'),
				memory_id      INTEGER NOT NULL,
				memory_sync_id TEXT    NOT NULL,
				project        TEXT    NOT NULL,
				PRIMARY KEY (checkpoint_id, reference_order),
				UNIQUE (checkpoint_id, reference_kind, memory_sync_id)
			);
		`)
	return err
}

// RecordSkippedCheckpoint records the terminal skipped disposition for one
// root user turn. An exact replay returns the original row and alreadyRecorded
// true; a different terminal result for the same identity is rejected.
func (s *Store) RecordSkippedCheckpoint(p RecordSkippedCheckpointParams) (*MemoryCheckpoint, bool, error) {
	if err := validateCheckpointIdentity(p.Identity); err != nil {
		return nil, false, err
	}
	if p.ReasonCode != CheckpointSkipReasonNoDurableKnowledge {
		return nil, false, ErrCheckpointInvalidReason
	}

	var checkpoint MemoryCheckpoint
	alreadyRecorded := false
	err := s.withTx(func(tx *sql.Tx) error {
		result, err := s.execHook(tx, `
			INSERT INTO memory_checkpoints (
				host, session_id, root_turn_id, disposition, reason_code, reason_version
			) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(host, session_id, root_turn_id) DO NOTHING`,
			p.Identity.Host,
			p.Identity.SessionID,
			p.Identity.RootTurnID,
			CheckpointDispositionSkipped,
			p.ReasonCode,
			CheckpointReasonVocabularyVersion,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		alreadyRecorded = rowsAffected == 0
		stored, err := loadMemoryCheckpoint(tx, p.Identity)
		if err != nil {
			return err
		}
		checkpoint = *stored
		if alreadyRecorded && (checkpoint.Disposition != CheckpointDispositionSkipped ||
			checkpoint.ReasonCode != p.ReasonCode ||
			checkpoint.ReasonVersion != CheckpointReasonVocabularyVersion) {
			return ErrCheckpointConflict
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return &checkpoint, alreadyRecorded, nil
}

// RecordSavedCheckpoint attaches existing or newly created Memories to one
// terminal saved disposition. Memories, sync mutations, references, and the
// checkpoint commit in one transaction.
func (s *Store) RecordSavedCheckpoint(p RecordSavedCheckpointParams) (*MemoryCheckpoint, bool, error) {
	if err := validateCheckpointIdentity(p.Identity); err != nil {
		return nil, false, err
	}

	var checkpoint *MemoryCheckpoint
	alreadyRecorded := false
	err := s.withTx(func(tx *sql.Tx) error {
		stored, err := loadMemoryCheckpoint(tx, p.Identity)
		if err == nil {
			if stored.Disposition != CheckpointDispositionSaved {
				return ErrCheckpointConflict
			}
			checkpoint = stored
			alreadyRecorded = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		project, _ := NormalizeProject(p.Project)
		if project == "" || len(p.MemoryIDs)+len(p.Memories) == 0 {
			return ErrCheckpointInvalidReferences
		}
		seenMemoryIDs := make(map[int64]struct{}, len(p.MemoryIDs)+len(p.Memories))
		for _, memoryID := range p.MemoryIDs {
			if memoryID <= 0 {
				return ErrCheckpointInvalidReferences
			}
			if _, exists := seenMemoryIDs[memoryID]; exists {
				return ErrCheckpointInvalidReferences
			}
			seenMemoryIDs[memoryID] = struct{}{}
		}
		for _, memory := range p.Memories {
			if strings.TrimSpace(memory.Title) == "" || strings.TrimSpace(memory.Content) == "" {
				return ErrCheckpointInvalidReferences
			}
		}

		result, err := s.execHook(tx, `
			INSERT INTO memory_checkpoints (host, session_id, root_turn_id, disposition)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(host, session_id, root_turn_id) DO NOTHING`,
			p.Identity.Host, p.Identity.SessionID, p.Identity.RootTurnID, CheckpointDispositionSaved,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			stored, err := loadMemoryCheckpoint(tx, p.Identity)
			if err != nil {
				return err
			}
			if stored.Disposition != CheckpointDispositionSaved {
				return ErrCheckpointConflict
			}
			checkpoint = stored
			alreadyRecorded = true
			return nil
		}

		references := make([]CheckpointReference, 0, len(p.MemoryIDs)+len(p.Memories))
		for _, memoryID := range p.MemoryIDs {
			memory, err := s.getObservationTx(tx, memoryID)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrCheckpointMemoryNotFound
			}
			if err != nil {
				return err
			}
			memoryProject := ""
			if memory.Project != nil {
				memoryProject, _ = NormalizeProject(*memory.Project)
			}
			if memoryProject != project {
				return ErrCheckpointProjectMismatch
			}
			references = append(references, CheckpointReference{
				Kind: CheckpointReferenceKindMemory, MemoryID: memory.ID,
				MemorySyncID: memory.SyncID, Project: memoryProject,
			})
		}

		if len(p.Memories) > 0 {
			if err := s.createSessionTx(tx, p.Identity.SessionID, project, p.Directory); err != nil {
				return err
			}
			var storedProject, storedDirectory, startedAt string
			if err := tx.QueryRow(`SELECT project, directory, started_at FROM sessions WHERE id = ?`, p.Identity.SessionID).
				Scan(&storedProject, &storedDirectory, &startedAt); err != nil {
				return err
			}
			if storedProject != project {
				return ErrCheckpointProjectMismatch
			}
			if err := s.enqueueSyncMutationTx(tx, SyncEntitySession, p.Identity.SessionID, SyncOpUpsert, syncSessionPayload{
				ID: p.Identity.SessionID, Project: storedProject, Directory: storedDirectory, StartedAt: startedAt,
			}); err != nil {
				return err
			}
		}

		for _, memoryInput := range p.Memories {
			memoryInput.SessionID = p.Identity.SessionID
			memoryInput.Project = project
			if strings.TrimSpace(memoryInput.Type) == "" {
				memoryInput.Type = "manual"
			}
			memory, err := s.addObservationTx(tx, memoryInput)
			if err != nil {
				return err
			}
			if _, exists := seenMemoryIDs[memory.ID]; exists {
				return ErrCheckpointInvalidReferences
			}
			seenMemoryIDs[memory.ID] = struct{}{}
			references = append(references, CheckpointReference{
				Kind: CheckpointReferenceKindMemory, MemoryID: memory.ID,
				MemorySyncID: memory.SyncID, Project: project,
			})
		}

		var checkpointID int64
		if err := tx.QueryRow(`
			SELECT id FROM memory_checkpoints
			WHERE host = ? AND session_id = ? AND root_turn_id = ?`,
			p.Identity.Host, p.Identity.SessionID, p.Identity.RootTurnID,
		).Scan(&checkpointID); err != nil {
			return err
		}
		for index, reference := range references {
			if _, err := s.execHook(tx, `
				INSERT INTO memory_checkpoint_references (
					checkpoint_id, reference_order, reference_kind, memory_id, memory_sync_id, project
				) VALUES (?, ?, ?, ?, ?, ?)`,
				checkpointID, index, reference.Kind, reference.MemoryID, reference.MemorySyncID, reference.Project,
			); err != nil {
				return err
			}
		}
		checkpoint, err = loadMemoryCheckpoint(tx, p.Identity)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return checkpoint, alreadyRecorded, nil
}

// GetMemoryCheckpoint returns the terminal checkpoint for an exact root-turn
// identity without exposing it through normal Memory read surfaces.
func (s *Store) GetMemoryCheckpoint(identity CheckpointIdentity) (*MemoryCheckpoint, error) {
	if err := validateCheckpointIdentity(identity); err != nil {
		return nil, err
	}
	checkpoint, err := loadMemoryCheckpoint(s.db, identity)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCheckpointNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get memory checkpoint: %w", err)
	}
	return checkpoint, nil
}

const checkpointByIdentityQuery = `
	SELECT host, session_id, root_turn_id, disposition,
	       reason_code, reason_version, created_at, updated_at
	FROM memory_checkpoints
	WHERE host = ? AND session_id = ? AND root_turn_id = ?`

type checkpointScanner interface {
	Scan(dest ...any) error
}

type checkpointQuerier interface {
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

func loadMemoryCheckpoint(q checkpointQuerier, identity CheckpointIdentity) (*MemoryCheckpoint, error) {
	var checkpoint MemoryCheckpoint
	if err := scanMemoryCheckpoint(q.QueryRow(checkpointByIdentityQuery,
		identity.Host, identity.SessionID, identity.RootTurnID), &checkpoint); err != nil {
		return nil, err
	}
	rows, err := q.Query(`
		SELECT r.reference_kind, r.memory_id, r.memory_sync_id, r.project
		FROM memory_checkpoint_references r
		JOIN memory_checkpoints c ON c.id = r.checkpoint_id
		WHERE c.host = ? AND c.session_id = ? AND c.root_turn_id = ?
		ORDER BY r.reference_order`, identity.Host, identity.SessionID, identity.RootTurnID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var reference CheckpointReference
		if err := rows.Scan(&reference.Kind, &reference.MemoryID, &reference.MemorySyncID, &reference.Project); err != nil {
			return nil, err
		}
		checkpoint.References = append(checkpoint.References, reference)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func scanMemoryCheckpoint(scanner checkpointScanner, checkpoint *MemoryCheckpoint) error {
	var reasonCode sql.NullString
	var reasonVersion sql.NullInt64
	err := scanner.Scan(
		&checkpoint.Identity.Host,
		&checkpoint.Identity.SessionID,
		&checkpoint.Identity.RootTurnID,
		&checkpoint.Disposition,
		&reasonCode,
		&reasonVersion,
		&checkpoint.CreatedAt,
		&checkpoint.UpdatedAt,
	)
	if err != nil {
		return err
	}
	if reasonCode.Valid {
		checkpoint.ReasonCode = reasonCode.String
	}
	if reasonVersion.Valid {
		checkpoint.ReasonVersion = int(reasonVersion.Int64)
	}
	return nil
}

func validateCheckpointIdentity(identity CheckpointIdentity) error {
	if err := validateCheckpointIdentityPart("host", identity.Host, maxCheckpointHostBytes); err != nil {
		return err
	}
	if err := validateCheckpointIdentityPart("session_id", identity.SessionID, maxCheckpointOpaqueIDBytes); err != nil {
		return err
	}
	return validateCheckpointIdentityPart("root_turn_id", identity.RootTurnID, maxCheckpointOpaqueIDBytes)
}

func validateCheckpointIdentityPart(field, value string, maxBytes int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s is required", ErrCheckpointInvalidIdentity, field)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: %s must not have surrounding whitespace", ErrCheckpointInvalidIdentity, field)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrCheckpointInvalidIdentity, field, maxBytes)
	}
	return nil
}
