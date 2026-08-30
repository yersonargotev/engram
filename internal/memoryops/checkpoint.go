package memoryops

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/yersonargotev/engram/internal/store"
)

const (
	CheckpointIdempotencyCreated         = "created"
	CheckpointIdempotencyAlreadyRecorded = "already_recorded"

	CheckpointErrorCodeInvalidDisposition = "invalid_checkpoint_disposition"
	CheckpointErrorCodeInvalidIdentity    = "invalid_checkpoint_identity"
	CheckpointErrorCodeInvalidReason      = "invalid_checkpoint_reason"
	CheckpointErrorCodeInvalidReferences  = "invalid_checkpoint_references"
	CheckpointErrorCodeMemoryNotFound     = "checkpoint_memory_not_found"
	CheckpointErrorCodeProjectMismatch    = "checkpoint_project_mismatch"
	CheckpointErrorCodeConflict           = "checkpoint_conflict"
	CheckpointErrorCodeNotFound           = "checkpoint_not_found"
	CheckpointErrorCodeFailed             = "checkpoint_failed"
)

var ErrCheckpointInvalidDisposition = errors.New("invalid checkpoint disposition")

// CheckpointRecordInput is transport-neutral input for finalizing one root
// user turn as saved, skipped, or needs_review.
type CheckpointRecordInput struct {
	Host        string                   `json:"host"`
	SessionID   string                   `json:"session_id"`
	RootTurnID  string                   `json:"root_turn_id"`
	Disposition string                   `json:"disposition"`
	ReasonCode  string                   `json:"reason_code"`
	Project     string                   `json:"project,omitempty"`
	MemoryIDs   []int64                  `json:"memory_ids,omitempty"`
	Memories    []CheckpointMemoryInput  `json:"memories,omitempty"`
	Proposal    *CheckpointProposalInput `json:"proposal,omitempty"`
	CWD         string                   `json:"-"`
}

// CheckpointProposalInput is one local Memory proposal to retain atomically
// for explicit review without creating or assessing a Memory.
type CheckpointProposalInput struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

// UnmarshalJSON rejects stale proposal fields instead of silently discarding
// them at an adapter boundary.
func (p *CheckpointProposalInput) UnmarshalJSON(data []byte) error {
	type proposalInput CheckpointProposalInput
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded proposalInput
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple proposal values")
		}
		return err
	}
	*p = CheckpointProposalInput(decoded)
	return nil
}

// CheckpointMemoryInput is one Memory to create as part of an atomic saved
// checkpoint. Project and session provenance come from the enclosing operation.
type CheckpointMemoryInput struct {
	Type     string `json:"type,omitempty"`
	Title    string `json:"title"`
	Content  string `json:"content"`
	ToolName string `json:"tool_name,omitempty"`
	Scope    string `json:"scope,omitempty"`
	TopicKey string `json:"topic_key,omitempty"`
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

type CheckpointVerificationOutcome string

const (
	CheckpointVerificationComplete             CheckpointVerificationOutcome = "complete"
	CheckpointVerificationContinuationRequired CheckpointVerificationOutcome = "continuation_required"
	CheckpointVerificationRecoveryExhausted    CheckpointVerificationOutcome = "recovery_exhausted"
)

// CheckpointVerificationInput identifies one root turn and whether its single
// recovery continuation is already active.
type CheckpointVerificationInput struct {
	Host           string
	SessionID      string
	RootTurnID     string
	RecoveryActive bool
}

// RecordCheckpoint finalizes one root user turn through the Store-owned state
// machine and returns a stable idempotency result for every adapter.
func (s *Service) RecordCheckpoint(input CheckpointRecordInput) (*CheckpointRecordResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	identity := store.CheckpointIdentity{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	}
	var checkpoint *store.MemoryCheckpoint
	var alreadyRecorded bool
	var err error
	switch input.Disposition {
	case store.CheckpointDispositionSkipped:
		if input.Project != "" || len(input.MemoryIDs) > 0 || len(input.Memories) > 0 ||
			input.Proposal != nil {
			return nil, store.ErrCheckpointInvalidReferences
		}
		checkpoint, alreadyRecorded, err = s.store.RecordSkippedCheckpoint(store.RecordSkippedCheckpointParams{
			Identity: identity, ReasonCode: input.ReasonCode,
		})
	case store.CheckpointDispositionSaved:
		if input.ReasonCode != "" {
			return nil, store.ErrCheckpointInvalidReason
		}
		if input.Proposal != nil {
			return nil, store.ErrCheckpointInvalidReferences
		}
		memories := make([]store.AddObservationParams, 0, len(input.Memories))
		for _, memory := range input.Memories {
			typ := memory.Type
			if strings.TrimSpace(typ) == "" {
				typ = "manual"
			}
			memories = append(memories, store.AddObservationParams{
				Type:     typ,
				Title:    memory.Title,
				Content:  memory.Content,
				ToolName: memory.ToolName,
				Scope:    memory.Scope,
				TopicKey: memory.TopicKey,
			})
		}
		checkpoint, alreadyRecorded, err = s.store.RecordSavedCheckpoint(store.RecordSavedCheckpointParams{
			Identity: identity, Project: input.Project, Directory: input.CWD,
			MemoryIDs: input.MemoryIDs, Memories: memories,
		})
	case store.CheckpointDispositionNeedsReview:
		if input.ReasonCode != "" || len(input.MemoryIDs) > 0 || len(input.Memories) > 0 {
			return nil, store.ErrCheckpointInvalidReferences
		}
		var proposal *store.MemoryProposalInput
		if input.Proposal != nil {
			proposal = &store.MemoryProposalInput{
				Title: input.Proposal.Title, Content: input.Proposal.Content,
			}
		}
		checkpoint, alreadyRecorded, err = s.store.RecordNeedsReviewCheckpoint(store.RecordNeedsReviewCheckpointParams{
			Identity: identity, Project: input.Project, Proposal: proposal,
		})
	default:
		return nil, ErrCheckpointInvalidDisposition
	}
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

// VerifyCheckpoint enforces the one-recovery rule against the Store-owned
// terminal checkpoint ledger. Adapters only translate the returned outcome to
// their host protocol.
func (s *Service) VerifyCheckpoint(input CheckpointVerificationInput) (CheckpointVerificationOutcome, error) {
	_, err := s.CheckpointStatus(CheckpointStatusInput{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	})
	if err == nil {
		return CheckpointVerificationComplete, nil
	}
	if !errors.Is(err, store.ErrCheckpointNotFound) {
		return "", err
	}
	if input.RecoveryActive {
		return CheckpointVerificationRecoveryExhausted, nil
	}
	return CheckpointVerificationContinuationRequired, nil
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
	case errors.Is(err, store.ErrCheckpointInvalidReferences):
		return CheckpointErrorCodeInvalidReferences
	case errors.Is(err, store.ErrCheckpointMemoryNotFound):
		return CheckpointErrorCodeMemoryNotFound
	case errors.Is(err, store.ErrCheckpointProjectMismatch):
		return CheckpointErrorCodeProjectMismatch
	case errors.Is(err, store.ErrCheckpointConflict):
		return CheckpointErrorCodeConflict
	case errors.Is(err, store.ErrCheckpointNotFound):
		return CheckpointErrorCodeNotFound
	default:
		return CheckpointErrorCodeFailed
	}
}
