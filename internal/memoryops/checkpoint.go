package memoryops

import (
	"errors"
	"fmt"

	"github.com/yersonargotev/engram/internal/store"
)

const (
	CheckpointIdempotencyCreated         = "created"
	CheckpointIdempotencyAlreadyRecorded = "already_recorded"

	CheckpointErrorCodeInvalidDisposition = "invalid_checkpoint_disposition"
	CheckpointErrorCodeInvalidIdentity    = "invalid_checkpoint_identity"
	CheckpointErrorCodeInvalidReason      = "invalid_checkpoint_reason"
	CheckpointErrorCodeConflict           = "checkpoint_conflict"
	CheckpointErrorCodeNotFound           = "checkpoint_not_found"
	CheckpointErrorCodeFailed             = "checkpoint_failed"
)

var ErrCheckpointInvalidDisposition = errors.New("invalid checkpoint disposition")

// CheckpointRecordInput is transport-neutral input for finalizing one root
// user turn. The current surface exposes only skipped; the disposition field keeps the
// shared interface compatible with the other terminal states defined by the
// accepted checkpoint contract.
type CheckpointRecordInput struct {
	Host        string `json:"host"`
	SessionID   string `json:"session_id"`
	RootTurnID  string `json:"root_turn_id"`
	Disposition string `json:"disposition"`
	ReasonCode  string `json:"reason_code"`
}

type CheckpointRecordResult struct {
	Checkpoint  *store.MemoryCheckpoint `json:"checkpoint"`
	Idempotency string                  `json:"idempotency"`
}

type CheckpointStatusInput struct {
	Host       string `json:"host"`
	SessionID  string `json:"session_id"`
	RootTurnID string `json:"root_turn_id"`
}

type CheckpointStatusResult struct {
	Checkpoint *store.MemoryCheckpoint `json:"checkpoint"`
}

// RecordCheckpoint finalizes one root user turn through the Store-owned state
// machine and returns a stable idempotency result for every adapter.
func (s *Service) RecordCheckpoint(input CheckpointRecordInput) (*CheckpointRecordResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if input.Disposition != store.CheckpointDispositionSkipped {
		return nil, ErrCheckpointInvalidDisposition
	}
	checkpoint, alreadyRecorded, err := s.store.RecordSkippedCheckpoint(store.RecordSkippedCheckpointParams{
		Identity: store.CheckpointIdentity{
			Host:       input.Host,
			SessionID:  input.SessionID,
			RootTurnID: input.RootTurnID,
		},
		ReasonCode: input.ReasonCode,
	})
	if err != nil {
		return nil, fmt.Errorf("record checkpoint: %w", err)
	}
	idempotency := CheckpointIdempotencyCreated
	if alreadyRecorded {
		idempotency = CheckpointIdempotencyAlreadyRecorded
	}
	return &CheckpointRecordResult{Checkpoint: checkpoint, Idempotency: idempotency}, nil
}

// CheckpointStatus inspects one exact root-turn checkpoint without consulting
// normal Memory read surfaces.
func (s *Service) CheckpointStatus(input CheckpointStatusInput) (*CheckpointStatusResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	checkpoint, err := s.store.GetMemoryCheckpoint(store.CheckpointIdentity{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	})
	if err != nil {
		return nil, fmt.Errorf("inspect checkpoint: %w", err)
	}
	return &CheckpointStatusResult{Checkpoint: checkpoint}, nil
}

// CheckpointErrorCode maps domain and persistence errors to the stable code
// shared by CLI and MCP adapters.
func CheckpointErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrCheckpointInvalidDisposition):
		return CheckpointErrorCodeInvalidDisposition
	case errors.Is(err, store.ErrCheckpointInvalidIdentity):
		return CheckpointErrorCodeInvalidIdentity
	case errors.Is(err, store.ErrCheckpointInvalidReason):
		return CheckpointErrorCodeInvalidReason
	case errors.Is(err, store.ErrCheckpointConflict):
		return CheckpointErrorCodeConflict
	case errors.Is(err, store.ErrCheckpointNotFound):
		return CheckpointErrorCodeNotFound
	default:
		return CheckpointErrorCodeFailed
	}
}
