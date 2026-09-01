package memoryops

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/protocolcontract"
	"github.com/yersonargotev/engram/internal/store"
)

const (
	DefaultRecallCandidateLimit = protocolcontract.RecallInitialCandidateLimit
	MaximumRecallCandidateLimit = protocolcontract.RecallFollowupCandidateLimit
	RecallCandidateBudgetBytes  = protocolcontract.RecallCandidateUTF8Budget
	RecallContentBudgetBytes    = protocolcontract.RecallContentUTF8Limit
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
	TurnIdentity    *store.CheckpointIdentity
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
	ResultID  string           `json:"result_id"`
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
	OpaqueResultIDs    []string           `json:"opaque_result_ids"`
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
	query := strings.TrimSpace(input.Query)
	if query == "" {
		return nil, errors.New("recall query is required")
	}
	var err error
	input.Scope, err = NormalizeRecallScope(input.Scope)
	if err != nil {
		return nil, err
	}
	input.MatchMode, err = NormalizeRecallMatchMode(input.MatchMode)
	if err != nil {
		return nil, err
	}
	if input.AllProjects && strings.TrimSpace(input.Project) != "" {
		return nil, errors.New("project and all projects cannot be used together")
	}
	if input.Scope != "project" && strings.TrimSpace(input.Project) == "" && !input.AllProjects {
		return nil, errors.New("all projects is required for broad recall without an explicit project")
	}
	limit := input.Limit
	if limit == 0 {
		limit = DefaultRecallCandidateLimit
	}
	if limit < 1 || limit > MaximumRecallCandidateLimit {
		return nil, fmt.Errorf("recall limit must be between 1 and %d", MaximumRecallCandidateLimit)
	}

	started := s.recallStartedAt()
	if input.TurnIdentity != nil {
		if err := store.ValidateCheckpointIdentity(*input.TurnIdentity); err != nil {
			return nil, err
		}
	}
	recallID, identifierErr := s.newRecallID()
	if identifierErr != nil {
		recallID = s.newRecallFallbackID()
	}
	result := &RecallResult{
		RecallID:           recallID,
		Candidates:         []RecallCandidate{},
		ResultIDs:          []int64{},
		OpaqueResultIDs:    []string{},
		DeliveredUTF8Bytes: len("[]"),
		Provenance: RecallProvenance{
			ProtocolVersion: protocolcontract.Version,
			BinaryVersion:   fallback(input.BinaryVersion, "unknown"),
			BinaryRevision:  strings.TrimSpace(input.BinaryRevision),
		},
	}
	elapsedFinalized := false
	defer func() {
		if !elapsedFinalized {
			result.ElapsedMonotonicMS = s.recallElapsed(started).Milliseconds()
		}
	}()
	if identifierErr != nil {
		setRecallUnavailable(result, "recall_identifier_failure", "recall_identifier", identifierErr)
		return result, nil
	}
	if !recallAuthorityAllows(input) {
		setRecallAuthorityRequired(result, input)
		return result, nil
	}
	if err := s.requireStore(); err != nil {
		setRecallUnavailable(result, "recall_store_failure", "recall_candidates", err)
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
		setRecallUnavailable(result, "recall_store_failure", "recall_candidates", err)
		return result, nil
	}
	eligibleConflictTargets, err := s.loadEligibleRecallConflictTargets(ctx, search, input)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		setRecallUnavailable(result, "recall_store_failure", "recall_conflict_eligibility", err)
		return result, nil
	}

	for index, item := range search.Observations {
		observation := item.Observation
		candidate := RecallCandidate{
			ResultID:  recallResultID(recallID, observation.SyncID, index),
			ID:        observation.ID,
			SyncID:    observation.SyncID,
			Title:     observation.Title,
			Type:      observation.Type,
			Scope:     observation.Scope,
			Pinned:    observation.Pinned,
			Summary:   truncateRecallUTF8(observation.Content, recallCandidateSummaryBytes),
			Conflicts: recallConflicts(item.Relations, eligibleConflictTargets),
		}
		if observation.Project != nil {
			candidate.Project = *observation.Project
		}
		result.Candidates = append(result.Candidates, candidate)
	}
	result.Candidates = fitRecallCandidates(result.Candidates, RecallCandidateBudgetBytes)
	record := store.RecallRunRecord{
		RecallID: recallID, Project: input.Project, Scope: input.Scope, AllProjects: input.AllProjects,
		TurnIdentity: input.TurnIdentity, StartedAtUnixNano: started.UnixNano(), MetricsPending: true,
		Results: make([]store.RecallResultRecord, 0, len(result.Candidates)),
	}
	for index, candidate := range result.Candidates {
		result.ResultIDs = append(result.ResultIDs, candidate.ID)
		result.OpaqueResultIDs = append(result.OpaqueResultIDs, candidate.ResultID)
		record.Results = append(record.Results, store.RecallResultRecord{
			ResultID: candidate.ResultID,
			Snapshot: store.RecallObservationSnapshot{
				ID: candidate.ID, SyncID: candidate.SyncID, Title: candidate.Title,
				Type: candidate.Type, Content: search.Observations[index].Observation.Content,
				Project: candidate.Project, Scope: candidate.Scope,
				RevisionCount: search.Observations[index].Observation.RevisionCount,
			},
			Rank: index,
		})
	}
	result.ResultCount = len(result.Candidates)
	encoded, _ := json.Marshal(result.Candidates)
	result.DeliveredUTF8Bytes = len(encoded)
	record.DeliveredUTF8Bytes = result.DeliveredUTF8Bytes
	record.ProtocolVersion = result.Provenance.ProtocolVersion
	record.BinaryVersion = result.Provenance.BinaryVersion
	record.BinaryRevision = result.Provenance.BinaryRevision
	if err := s.store.RecordRecallRunContext(ctx, record); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		result.Candidates = []RecallCandidate{}
		result.ResultIDs = []int64{}
		result.OpaqueResultIDs = []string{}
		result.ResultCount = 0
		result.DeliveredUTF8Bytes = len("[]")
		setRecallUnavailable(result, "recall_store_failure", "recall_run", err)
		return result, nil
	}
	elapsed := s.recallElapsed(started)
	result.ElapsedMonotonicMS = elapsed.Milliseconds()
	if err := s.store.CompleteRecallRunContext(ctx, recallID, result.ElapsedMonotonicMS, started.Add(elapsed).UnixNano()); err != nil {
		result.Diagnostics = append(result.Diagnostics, RecallDiagnostic{
			Code: "recall_metrics_unavailable", Operation: "recall_metrics", Detail: err.Error(),
		})
	}
	elapsedFinalized = true
	return result, nil
}

// NormalizeRecallScope owns the accepted transport-neutral Recall scope values.
// It must run before any adapter derives cross-project authority from scope.
func NormalizeRecallScope(value string) (string, error) {
	scope := strings.ToLower(strings.TrimSpace(value))
	if scope == "" {
		return "project", nil
	}
	switch scope {
	case "project", "personal", "global":
		return scope, nil
	default:
		return "", fmt.Errorf("invalid recall scope %q: must be project, personal, or global", value)
	}
}

// NormalizeRecallMatchMode owns the accepted transport-neutral token matching modes.
func NormalizeRecallMatchMode(value string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(value))
	switch mode {
	case "", "all", "any":
		return mode, nil
	default:
		return "", fmt.Errorf("invalid match_mode %q: must be all or any", value)
	}
}

func setRecallUnavailable(result *RecallResult, code, operation string, err error) {
	result.Warning = &RecallWarning{
		Code:       "recall_unavailable",
		Message:    "Recall is unavailable; continuing without Memory.",
		NextAction: "Retry once with narrow anchors only if prior Memory remains material to the task.",
	}
	result.Diagnostics = []RecallDiagnostic{{
		Code:      code,
		Operation: operation,
		Detail:    err.Error(),
	}}
}

func setRecallAuthorityRequired(result *RecallResult, input RecallInput) {
	if input.AllProjects || input.Scope != "project" {
		result.Warning = &RecallWarning{
			Code:       "recall_scope_relevance_required",
			Message:    "Recall skipped because broad scope was not explicitly relevant to this task.",
			NextAction: "Request personal or cross-project Recall only when that broader scope is material.",
		}
		result.Diagnostics = []RecallDiagnostic{{
			Code:      "recall_scope_relevance_required",
			Operation: "recall_candidates",
			Detail:    fmt.Sprintf("scope=%s all_projects=%t", input.Scope, input.AllProjects),
		}}
		return
	}
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
}

func (s *Service) loadEligibleRecallConflictTargets(ctx context.Context, search *SearchResult, input RecallInput) (map[string]store.RecallConflictTarget, error) {
	syncIDs := make([]string, 0)
	for _, item := range search.Observations {
		for _, relation := range item.Relations.AsSource {
			if isUnresolvedRecallConflict(relation) && relation.TargetID != "" {
				syncIDs = append(syncIDs, relation.TargetID)
			}
		}
		for _, relation := range item.Relations.AsTarget {
			if isUnresolvedRecallConflict(relation) && relation.SourceID != "" {
				syncIDs = append(syncIDs, relation.SourceID)
			}
		}
	}
	return s.store.RecallEligibleConflictTargetsContext(ctx, syncIDs, store.SearchOptions{
		Project: input.Project,
		Scope:   input.Scope,
	})
}

func recallConflicts(relations store.ObservationRelations, eligible map[string]store.RecallConflictTarget) []RecallConflict {
	conflicts := make([]RecallConflict, 0)
	for _, relation := range relations.AsSource {
		if !isUnresolvedRecallConflict(relation) {
			continue
		}
		target, ok := eligible[relation.TargetID]
		if !ok {
			continue
		}
		conflicts = append(conflicts, RecallConflict{
			RelationID: relation.SyncID,
			MemoryID:   target.ID,
			SyncID:     target.SyncID,
			Title:      target.Title,
			Status:     relation.JudgmentStatus,
		})
	}
	for _, relation := range relations.AsTarget {
		if !isUnresolvedRecallConflict(relation) {
			continue
		}
		target, ok := eligible[relation.SourceID]
		if !ok {
			continue
		}
		conflicts = append(conflicts, RecallConflict{
			RelationID: relation.SyncID,
			MemoryID:   target.ID,
			SyncID:     target.SyncID,
			Title:      target.Title,
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

func recallResultID(recallID, syncID string, rank int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", recallID, syncID, rank)))
	return "result-" + hex.EncodeToString(digest[:16])
}

var recallFallbackSequence atomic.Uint64

func newRecallFallbackID() string {
	seed := fmt.Sprintf("%d:%d", time.Now().UnixNano(), recallFallbackSequence.Add(1))
	digest := sha256.Sum256([]byte(seed))
	return "recall-" + hex.EncodeToString(digest[:16])
}

func fallback(value, fallbackValue string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallbackValue
}
