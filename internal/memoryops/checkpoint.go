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
	CheckpointPreflightCandidateLimit    = 3

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

// CheckpointMemoryInput is one prospective or settled Memory used by preflight
// or an atomic saved/needs-review checkpoint. Project and session provenance
// come from the enclosing operation.
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

// CheckpointReplayInput is the minimum payload needed to resolve an immutable
// terminal result before an adapter validates retry-only fields.
type CheckpointReplayInput struct {
	Host        string `json:"host"`
	SessionID   string `json:"session_id"`
	RootTurnID  string `json:"root_turn_id"`
	Disposition string `json:"disposition"`
}

// CheckpointPreflightInput describes prospective settled Memories without a
// root-turn identity because preflight is read-only and cannot create a
// checkpoint. Project is explicit so candidates never cross Memory ownership
// boundaries.
type CheckpointPreflightInput struct {
	Project  string                  `json:"project"`
	Memories []CheckpointMemoryInput `json:"memories"`
}

type CheckpointPreflightDuplicate struct {
	InputIndex int                       `json:"input_index"`
	Reference  store.CheckpointReference `json:"reference"`
}

type CheckpointPreflightCandidate struct {
	InputIndex int                       `json:"input_index"`
	Reference  store.CheckpointReference `json:"reference"`
	Type       string                    `json:"type"`
	Title      string                    `json:"title"`
	Content    string                    `json:"content"`
	Scope      string                    `json:"scope"`
	TopicKey   *string                   `json:"topic_key,omitempty"`
	Score      float64                   `json:"score"`
}

type CheckpointPreflightResult struct {
	Project         string                         `json:"project"`
	CandidateLimit  int                            `json:"candidate_limit"`
	ExactDuplicates []CheckpointPreflightDuplicate `json:"exact_duplicates,omitempty"`
	Candidates      []CheckpointPreflightCandidate `json:"candidates,omitempty"`
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

// ReplayCheckpoint returns an existing immutable checkpoint when identity and
// disposition match. It deliberately inspects no retry payload fields.
func (s *Service) ReplayCheckpoint(input CheckpointReplayInput) (*CheckpointRecordResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	identity := store.CheckpointIdentity{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	}
	stored, err := s.store.GetMemoryCheckpoint(identity)
	if err != nil {
		return nil, err
	}
	if stored.Disposition != input.Disposition {
		return nil, store.ErrCheckpointConflict
	}
	return &CheckpointRecordResult{
		Checkpoint: stored, Idempotency: CheckpointIdempotencyAlreadyRecorded,
	}, nil
}

// RecordCheckpoint finalizes one root user turn through the Store-owned state
// machine and returns a stable idempotency result for every adapter.
func (s *Service) RecordCheckpoint(input CheckpointRecordInput) (*CheckpointRecordResult, error) {
	replayed, replayErr := s.ReplayCheckpoint(CheckpointReplayInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID, Disposition: input.Disposition,
	})
	if replayErr == nil {
		return replayed, nil
	}
	if !errors.Is(replayErr, store.ErrCheckpointNotFound) {
		return nil, replayErr
	}
	identity := store.CheckpointIdentity{Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID}
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
		checkpoint, alreadyRecorded, err = s.store.RecordSavedCheckpoint(store.RecordSavedCheckpointParams{
			Identity: identity, Project: input.Project, Directory: input.CWD,
			MemoryIDs: input.MemoryIDs, Memories: checkpointStoreMemories(input.Memories),
		})
	case store.CheckpointDispositionNeedsReview:
		if input.ReasonCode != "" {
			return nil, store.ErrCheckpointInvalidReferences
		}
		var proposal *store.MemoryProposalInput
		if input.Proposal != nil {
			proposal = &store.MemoryProposalInput{
				Title: input.Proposal.Title, Content: input.Proposal.Content,
			}
		}
		checkpoint, alreadyRecorded, err = s.store.RecordNeedsReviewCheckpoint(store.RecordNeedsReviewCheckpointParams{
			Identity: identity, Project: input.Project, Directory: input.CWD,
			MemoryIDs: input.MemoryIDs, Memories: checkpointStoreMemories(input.Memories), Proposal: proposal,
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

// PreflightCheckpoint assesses proposed settled Memories without persisting
// provisional Memory, relation, proposal, checkpoint, review, or sync state.
// Exact duplicates are returned as reusable references. Semantic candidates
// are full same-project Memory snapshots, de-duplicated and globally bounded.
func (s *Service) PreflightCheckpoint(input CheckpointPreflightInput) (*CheckpointPreflightResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	project, _ := store.NormalizeProject(input.Project)
	if project == "" || len(input.Memories) == 0 {
		return nil, store.ErrCheckpointInvalidReferences
	}

	result := &CheckpointPreflightResult{Project: project, CandidateLimit: CheckpointPreflightCandidateLimit}
	memories := checkpointStoreMemories(input.Memories)
	exactMemoryIDs := make(map[int64]struct{}, len(memories))
	nonExactInputs := make([]int, 0, len(memories))
	for inputIndex, memory := range memories {
		memory.Project = project
		exact, err := s.store.FindExactCheckpointMemory(memory)
		if err != nil {
			return nil, err
		}
		if exact != nil {
			exactMemoryIDs[exact.ID] = struct{}{}
			result.ExactDuplicates = append(result.ExactDuplicates, CheckpointPreflightDuplicate{
				InputIndex: inputIndex,
				Reference:  checkpointReference(exact, project),
			})
			continue
		}
		nonExactInputs = append(nonExactInputs, inputIndex)
	}

	type candidateGroup struct {
		inputIndex int
		candidates []store.SearchResult
		cursor     int
	}
	groups := make([]candidateGroup, 0, len(nonExactInputs))
	for _, inputIndex := range nonExactInputs {
		memory := memories[inputIndex]
		candidateQuery := strings.TrimSpace(memory.Title + " " + memory.Content)
		candidates, err := s.store.Search(candidateQuery, store.SearchOptions{
			Project: project, Scope: memory.Scope,
			Limit: CheckpointPreflightCandidateLimit + len(exactMemoryIDs), MatchMode: "any",
		})
		if err != nil {
			return nil, fmt.Errorf("preflight checkpoint candidates: %w", err)
		}
		groups = append(groups, candidateGroup{inputIndex: inputIndex, candidates: candidates})
	}

	seenCandidates := exactMemoryIDs
	for len(result.Candidates) < CheckpointPreflightCandidateLimit {
		progressed := false
		for groupIndex := range groups {
			group := &groups[groupIndex]
			for group.cursor < len(group.candidates) {
				candidate := group.candidates[group.cursor]
				group.cursor++
				if _, duplicate := seenCandidates[candidate.ID]; duplicate {
					continue
				}
				seenCandidates[candidate.ID] = struct{}{}
				result.Candidates = append(result.Candidates, CheckpointPreflightCandidate{
					InputIndex: group.inputIndex,
					Reference:  checkpointReference(&candidate.Observation, project),
					Type:       candidate.Type,
					Title:      candidate.Title,
					Content:    candidate.Content,
					Scope:      candidate.Scope,
					TopicKey:   candidate.TopicKey,
					Score:      candidate.Rank,
				})
				progressed = true
				break
			}
			if len(result.Candidates) == CheckpointPreflightCandidateLimit {
				break
			}
		}
		if !progressed {
			break
		}
	}
	return result, nil
}

func checkpointStoreMemories(memories []CheckpointMemoryInput) []store.AddObservationParams {
	result := make([]store.AddObservationParams, 0, len(memories))
	for _, memory := range memories {
		typ := memory.Type
		if strings.TrimSpace(typ) == "" {
			typ = "manual"
		}
		result = append(result, store.AddObservationParams{
			Type: typ, Title: memory.Title, Content: memory.Content, ToolName: memory.ToolName,
			Scope: store.NormalizeObservationScope(memory.Scope), TopicKey: memory.TopicKey,
		})
	}
	return result
}

func checkpointReference(memory *store.Observation, project string) store.CheckpointReference {
	return store.CheckpointReference{
		Kind: store.CheckpointReferenceKindMemory, MemoryID: memory.ID,
		MemorySyncID: memory.SyncID, Project: project,
	}
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
