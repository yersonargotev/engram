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
	ErrCheckpointInvalidIdentity = errors.New("invalid checkpoint identity")
	ErrCheckpointInvalidReason   = errors.New("invalid checkpoint reason")
	ErrCheckpointConflict        = errors.New("checkpoint already recorded with a different terminal result")
	ErrCheckpointNotFound        = errors.New("checkpoint not found")
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
	Identity      CheckpointIdentity `json:"identity"`
	Disposition   string             `json:"disposition"`
	ReasonCode    string             `json:"reason_code,omitempty"`
	ReasonVersion int                `json:"reason_version,omitempty"`
	CreatedAt     string             `json:"created_at"`
	UpdatedAt     string             `json:"updated_at"`
}

type RecordSkippedCheckpointParams struct {
	Identity   CheckpointIdentity
	ReasonCode string
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
		if err := scanMemoryCheckpoint(tx.QueryRow(checkpointByIdentityQuery,
			p.Identity.Host, p.Identity.SessionID, p.Identity.RootTurnID), &checkpoint); err != nil {
			return err
		}
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

// GetMemoryCheckpoint returns the terminal checkpoint for an exact root-turn
// identity without exposing it through normal Memory read surfaces.
func (s *Store) GetMemoryCheckpoint(identity CheckpointIdentity) (*MemoryCheckpoint, error) {
	if err := validateCheckpointIdentity(identity); err != nil {
		return nil, err
	}
	var checkpoint MemoryCheckpoint
	err := scanMemoryCheckpoint(s.db.QueryRow(checkpointByIdentityQuery,
		identity.Host, identity.SessionID, identity.RootTurnID), &checkpoint)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCheckpointNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get memory checkpoint: %w", err)
	}
	return &checkpoint, nil
}

const checkpointByIdentityQuery = `
	SELECT host, session_id, root_turn_id, disposition,
	       reason_code, reason_version, created_at, updated_at
	FROM memory_checkpoints
	WHERE host = ? AND session_id = ? AND root_turn_id = ?`

type checkpointScanner interface {
	Scan(dest ...any) error
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
