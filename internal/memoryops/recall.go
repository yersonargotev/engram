package memoryops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/store"
)

const (
	DefaultRecallCandidateLimit = 5
	MaximumRecallCandidateLimit = 10
	RecallCandidateBudgetBytes  = 4 * 1024
	recallCandidateSummaryBytes = 512
)

// RecallInput is the complete transport-neutral request for candidate Recall.
// An explicit limit above five represents the deliberate follow-up allowed by
// the Protocol; callers must not use it for the initial request.
type RecallInput struct {
	Query           string
	Type            string
	Project         string
	Scope           string
	Limit           int
	MatchMode       string
	AllProjects     bool
	ProjectStrength project.IdentityStrength
	DeliberateScope bool
	BinaryVersion   string
	BinaryRevision  string
}

// RecallProvenance identifies the machine semantics and executable that
// produced a Recall response. Pack and host-plugin provenance are deliberately
// absent because generic Core calls cannot establish them authoritatively.
type RecallProvenance struct {
	ProtocolVersion int    `json:"protocol_version"`
	BinaryVersion   string `json:"binary_version"`
	BinaryRevision  string `json:"binary_revision,omitempty"`
}

// RecallConflict is an explicit unresolved conflict involving one candidate.
type RecallConflict struct {
	RelationID string `json:"relation_id"`
	MemoryID   int64  `json:"memory_id,omitempty"`
	SyncID     string `json:"sync_id,omitempty"`
	Title      string `json:"title,omitempty"`
	Status     string `json:"status"`
}

// RecallCandidate is the bounded discovery representation returned before a
// deliberate complete-Memory follow-up.
type RecallCandidate struct {
	ID        int64            `json:"id"`
	SyncID    string           `json:"sync_id"`
	Title     string           `json:"title"`
	Type      string           `json:"type"`
	Project   string           `json:"project,omitempty"`
	Scope     string           `json:"scope"`
	Pinned    bool             `json:"pinned"`
	Summary   string           `json:"summary"`
	Conflicts []RecallConflict `json:"conflicts,omitempty"`
}

// RecallWarning is the single quiet actionable warning emitted when Recall
// cannot safely return candidates without blocking the caller's task.
type RecallWarning struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	NextAction string `json:"next_action"`
}

// RecallDiagnostic carries stable structured failure metadata outside prose.
type RecallDiagnostic struct {
	Code      string `json:"code"`
	Operation string `json:"operation"`
	Detail    string `json:"detail,omitempty"`
}

// RecallResult is one bounded candidate Recall response. DeliveredUTF8Bytes
// counts the JSON encoding of Candidates only; response metadata is outside the
// 4 KiB candidate budget by contract.
type RecallResult struct {
	RecallID           string             `json:"recall_id"`
	Candidates         []RecallCandidate  `json:"results"`
	ResultIDs          []int64            `json:"result_ids"`
	ResultCount        int                `json:"result_count"`
	DeliveredUTF8Bytes int                `json:"delivered_utf8_bytes"`
	ElapsedMonotonicMS int64              `json:"elapsed_monotonic_ms"`
	Provenance         RecallProvenance   `json:"provenance"`
	Warning            *RecallWarning     `json:"warning,omitempty"`
	Diagnostics        []RecallDiagnostic `json:"diagnostics,omitempty"`
}

// Recall returns the smallest useful candidate set for one authorized scope.
func (s *Service) Recall(input RecallInput) (*RecallResult, error) {
	return s.RecallContext(context.Background(), input)
}

// RecallContext is Recall with caller cancellation propagated through Store
// search and relation loading.
func (s *Service) RecallContext(ctx context.Context, input RecallInput) (*RecallResult, error) {
	started := time.Now()
	query := strings.TrimSpace(input.Query)
	if query == "" {
		return nil, errors.New("recall query is required")
	}
	input.Scope = strings.TrimSpace(input.Scope)
	if input.Scope == "" {
		input.Scope = "project"
	}
	limit := input.Limit
	if limit == 0 {
		limit = DefaultRecallCandidateLimit
	}
	if limit < 1 || limit > MaximumRecallCandidateLimit {
		return nil, fmt.Errorf("recall limit must be between 1 and %d", MaximumRecallCandidateLimit)
	}

	recallID, err := s.newRecallID()
	if err != nil {
		return nil, fmt.Errorf("create recall identifier: %w", err)
	}
	result := &RecallResult{
		RecallID:           recallID,
		Candidates:         []RecallCandidate{},
		ResultIDs:          []int64{},
		DeliveredUTF8Bytes: len("[]"),
		Provenance: RecallProvenance{
			ProtocolVersion: protocolcontract.Version,
			BinaryVersion:   fallback(input.BinaryVersion, "unknown"),
			BinaryRevision:  strings.TrimSpace(input.BinaryRevision),
		},
	}
	defer func() { result.ElapsedMonotonicMS = time.Since(started).Milliseconds() }()
	if !recallAuthorityAllows(input) {
		result.Warning = &RecallWarning{
			Code:       "recall_project_authority_required",
			Message:    "Recall skipped because automatic project identity is not strong or explicit.",
			NextAction: "Provide an explicit project when prior Memory can materially change the task.",
		}
		result.Diagnostics = []RecallDiagnostic{{
			Code:      project.WriteAuthorityErrorCode,
			Operation: "recall_candidates",
			Detail:    fmt.Sprintf("project identity strength is %s", input.ProjectStrength),
		}}
		return result, nil
	}
	if err := s.requireStore(); err != nil {
		setRecallUnavailable(result, err)
		return result, nil
	}

	search, err := s.recallCandidatesContext(ctx, SearchInput{
		Query:       query,
		Type:        input.Type,
		Project:     input.Project,
		Scope:       input.Scope,
		Limit:       limit,
		MatchMode:   input.MatchMode,
		AllProjects: input.AllProjects,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		setRecallUnavailable(result, err)
		return result, nil
	}

	for _, item := range search.Observations {
		observation := item.Observation
		candidate := RecallCandidate{
			ID:        observation.ID,
			SyncID:    observation.SyncID,
			Title:     observation.Title,
			Type:      observation.Type,
			Scope:     observation.Scope,
			Pinned:    observation.Pinned,
			Summary:   truncateRecallUTF8(observation.Content, recallCandidateSummaryBytes),
			Conflicts: recallConflicts(item.Relations),
		}
		if observation.Project != nil {
			candidate.Project = *observation.Project
		}
		result.Candidates = append(result.Candidates, candidate)
	}
	result.Candidates = fitRecallCandidates(result.Candidates, RecallCandidateBudgetBytes)
	for _, candidate := range result.Candidates {
		result.ResultIDs = append(result.ResultIDs, candidate.ID)
	}
	result.ResultCount = len(result.Candidates)
	encoded, _ := json.Marshal(result.Candidates)
	result.DeliveredUTF8Bytes = len(encoded)
	return result, nil
}

func setRecallUnavailable(result *RecallResult, err error) {
	result.Warning = &RecallWarning{
		Code:       "recall_unavailable",
		Message:    "Recall is unavailable; continuing without Memory.",
		NextAction: "Retry once with narrow anchors only if prior Memory remains material to the task.",
	}
	result.Diagnostics = []RecallDiagnostic{{
		Code:      "recall_store_failure",
		Operation: "recall_candidates",
		Detail:    err.Error(),
	}}
}

func recallConflicts(relations store.ObservationRelations) []RecallConflict {
	conflicts := make([]RecallConflict, 0)
	for _, relation := range relations.AsSource {
		if !isUnresolvedRecallConflict(relation) {
			continue
		}
		conflicts = append(conflicts, RecallConflict{
			RelationID: relation.SyncID,
			MemoryID:   relation.TargetIntID,
			SyncID:     relation.TargetID,
			Title:      relation.TargetTitle,
			Status:     relation.JudgmentStatus,
		})
	}
	for _, relation := range relations.AsTarget {
		if !isUnresolvedRecallConflict(relation) {
			continue
		}
		conflicts = append(conflicts, RecallConflict{
			RelationID: relation.SyncID,
			MemoryID:   relation.SourceIntID,
			SyncID:     relation.SourceID,
			Title:      relation.SourceTitle,
			Status:     relation.JudgmentStatus,
		})
	}
	return conflicts
}

func isUnresolvedRecallConflict(relation store.Relation) bool {
	return relation.JudgmentStatus == store.JudgmentStatusPending ||
		(relation.JudgmentStatus == store.JudgmentStatusJudged && relation.Relation == store.RelationConflictsWith)
}

func recallAuthorityAllows(input RecallInput) bool {
	if input.AllProjects || input.Scope != "project" {
		return input.DeliberateScope
	}
	return input.ProjectStrength == project.IdentityStrengthStrong || input.ProjectStrength == project.IdentityStrengthExplicit
}

func fitRecallCandidates(candidates []RecallCandidate, budget int) []RecallCandidate {
	result := make([]RecallCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		trial := append(result, candidate)
		encoded, err := json.Marshal(trial)
		if err == nil && len(encoded) <= budget {
			result = trial
			continue
		}
		candidate.Summary = ""
		trial = append(result, candidate)
		encoded, err = json.Marshal(trial)
		if err != nil || len(encoded) > budget {
			break
		}
		result = trial
	}
	return result
}

func truncateRecallUTF8(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	const suffix = "…"
	limit -= len(suffix)
	if limit <= 0 {
		return suffix
	}
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + suffix
}

func newRecallID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "recall-" + hex.EncodeToString(raw), nil
}

func fallback(value, fallbackValue string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallbackValue
}
