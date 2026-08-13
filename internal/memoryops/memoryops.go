// Package memoryops contains transport-neutral memory operations shared by
// Engram front ends. Persistence remains owned by internal/store.
package memoryops

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Gentleman-Programming/engram/internal/store"
)

var (
	// ErrStoreRequired is returned when an operation is attempted without a store.
	ErrStoreRequired = errors.New("memory operations require a store")
	// ErrSessionRequired is returned because saving an observation must retain session provenance.
	ErrSessionRequired = errors.New("session id is required")
	// ErrProjectRequired is returned when a project-scoped operation lacks a resolved project.
	ErrProjectRequired = errors.New("project is required unless all projects is requested")
)

// Service coordinates domain operations without knowing about CLI, MCP, or rendering.
type Service struct {
	store *store.Store
}

// New creates a memory operation service backed by the local source-of-truth store.
func New(s *store.Store) *Service {
	return &Service{store: s}
}

// SaveInput is the caller-resolved input for saving an observation. Project is
// trusted input from the caller and is normalized before persistence; this
// package intentionally performs no cwd or project detection.
type SaveInput struct {
	SessionID string
	CWD       string
	Project   string
	Type      string
	Title     string
	Content   string
	ToolName  string
	Scope     string
	TopicKey  string

	// CandidateOptions customizes post-save candidate detection. Project and
	// Scope are always derived from this input so candidates stay in the same
	// memory boundary as the saved observation.
	CandidateOptions store.CandidateOptions
}

// SaveResult contains the persisted observation, a non-persisting topic key
// suggestion, and any pending relation candidates discovered after the save.
// CandidateDetectionError is non-nil only when the observation was saved but
// candidate detection failed; callers can report it without treating the save
// itself as failed.
type SaveResult struct {
	Observation             *store.Observation
	SuggestedTopicKey       string
	Candidates              []store.Candidate
	CandidateDetectionError error
}

// Save creates the implicit session when needed, persists an observation, then
// finds conflict candidates. A candidate lookup failure never rolls back a
// successful save because it runs after the observation transaction.
func (s *Service) Save(input SaveInput) (*SaveResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.SessionID) == "" {
		return nil, ErrSessionRequired
	}
	if strings.TrimSpace(input.Content) == "" {
		return nil, errors.New("content is required")
	}

	project, _ := store.NormalizeProject(input.Project)
	typ := input.Type
	if strings.TrimSpace(typ) == "" {
		typ = "manual"
	}
	if err := s.store.CreateSession(input.SessionID, project, input.CWD); err != nil {
		return nil, fmt.Errorf("create implicit session: %w", err)
	}

	id, err := s.store.AddObservation(store.AddObservationParams{
		SessionID: input.SessionID,
		Type:      typ,
		Title:     input.Title,
		Content:   input.Content,
		ToolName:  input.ToolName,
		Project:   project,
		Scope:     input.Scope,
		TopicKey:  input.TopicKey,
	})
	if err != nil {
		return nil, fmt.Errorf("save observation: %w", err)
	}
	observation, err := s.store.GetObservation(id)
	if err != nil {
		return nil, fmt.Errorf("reload saved observation: %w", err)
	}

	result := &SaveResult{Observation: observation}
	if strings.TrimSpace(input.TopicKey) == "" {
		result.SuggestedTopicKey = store.SuggestTopicKey(typ, input.Title, input.Content)
	}

	candidateOptions := input.CandidateOptions
	candidateOptions.Project = project
	candidateOptions.Scope = input.Scope
	candidates, candidateErr := s.store.FindCandidates(id, candidateOptions)
	if candidateErr != nil {
		result.CandidateDetectionError = candidateErr
		return result, nil
	}
	result.Candidates = candidates
	return result, nil
}

// SearchInput controls a full-content memory search. Project must already be
// resolved by the caller unless AllProjects is explicitly true.
type SearchInput struct {
	Query       string
	Type        string
	Project     string
	Scope       string
	Limit       int
	MatchMode   string
	AllProjects bool
}

// ObservationResult pairs a search result with all known relations involving it.
type ObservationResult struct {
	Observation store.SearchResult
	Relations   store.ObservationRelations
}

// SearchResult contains full observations and relation data loaded in one batch.
type SearchResult struct {
	Observations []ObservationResult
}

// Search runs the store search and enriches every result with relations without
// N+1 database queries.
func (s *Service) Search(input SearchInput) (*SearchResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if input.AllProjects && strings.TrimSpace(input.Project) != "" {
		return nil, errors.New("project and all projects cannot be used together")
	}
	project := ""
	if !input.AllProjects {
		if strings.TrimSpace(input.Project) == "" {
			return nil, ErrProjectRequired
		}
		project, _ = store.NormalizeProject(input.Project)
	}

	observations, err := s.store.Search(input.Query, store.SearchOptions{
		Type:      input.Type,
		Project:   project,
		Scope:     input.Scope,
		Limit:     input.Limit,
		MatchMode: input.MatchMode,
	})
	if err != nil {
		return nil, fmt.Errorf("search observations: %w", err)
	}

	syncIDs := make([]string, 0, len(observations))
	for _, observation := range observations {
		if observation.SyncID != "" {
			syncIDs = append(syncIDs, observation.SyncID)
		}
	}
	relations, err := s.store.GetRelationsForObservations(syncIDs)
	if err != nil {
		return nil, fmt.Errorf("load search relations: %w", err)
	}

	result := &SearchResult{Observations: make([]ObservationResult, 0, len(observations))}
	for _, observation := range observations {
		rels := relations[observation.SyncID]
		if rels.AsSource == nil {
			rels.AsSource = []store.Relation{}
		}
		if rels.AsTarget == nil {
			rels.AsTarget = []store.Relation{}
		}
		result.Observations = append(result.Observations, ObservationResult{
			Observation: observation,
			Relations:   rels,
		})
	}
	return result, nil
}

// GetResult is one full observation plus every non-orphaned relation involving it.
type GetResult struct {
	Observation *store.Observation
	Relations   store.ObservationRelations
}

// Get retrieves one observation and its relations.
func (s *Service) Get(id int64) (*GetResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if id <= 0 {
		return nil, errors.New("observation id must be positive")
	}
	observation, err := s.store.GetObservation(id)
	if err != nil {
		return nil, fmt.Errorf("get observation: %w", err)
	}
	relations, err := s.store.GetRelationsForObservations([]string{observation.SyncID})
	if err != nil {
		return nil, fmt.Errorf("load observation relations: %w", err)
	}
	rels := relations[observation.SyncID]
	if rels.AsSource == nil {
		rels.AsSource = []store.Relation{}
	}
	if rels.AsTarget == nil {
		rels.AsTarget = []store.Relation{}
	}
	return &GetResult{Observation: observation, Relations: rels}, nil
}

// CompareInput is a verdict already made by a caller. Compare never invokes an
// LLM; it resolves local IDs and persists that verdict through the store.
type CompareInput struct {
	MemoryIDA  int64
	MemoryIDB  int64
	Relation   string
	Confidence float64
	Reasoning  string
	Model      string
}

// CompareResult contains the relation sync ID. It is empty for not_conflict,
// matching store.JudgeBySemantic's no-op contract.
type CompareResult struct {
	SyncID string `json:"sync_id"`
}

// Compare resolves integer observation IDs then persists the supplied semantic verdict.
func (s *Service) Compare(input CompareInput) (*CompareResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if input.MemoryIDA <= 0 || input.MemoryIDB <= 0 {
		return nil, errors.New("memory ids must be positive")
	}
	first, err := s.store.GetObservation(input.MemoryIDA)
	if err != nil {
		return nil, fmt.Errorf("observation id=%d not found: %w", input.MemoryIDA, err)
	}
	second, err := s.store.GetObservation(input.MemoryIDB)
	if err != nil {
		return nil, fmt.Errorf("observation id=%d not found: %w", input.MemoryIDB, err)
	}
	syncID, err := s.store.JudgeBySemantic(store.JudgeBySemanticParams{
		SourceID:   first.SyncID,
		TargetID:   second.SyncID,
		Relation:   input.Relation,
		Confidence: input.Confidence,
		Reasoning:  input.Reasoning,
		Model:      input.Model,
	})
	if err != nil {
		return nil, fmt.Errorf("compare observations: %w", err)
	}
	return &CompareResult{SyncID: syncID}, nil
}

// Provenance identifies who made a relation judgment.
type Provenance struct {
	Actor     string
	Kind      string
	Model     string
	SessionID string
}

// JudgeInput is a typed pending-relation verdict with optional supporting data.
type JudgeInput struct {
	JudgmentID string
	Relation   string
	Reason     *string
	Evidence   *string
	Confidence *float64
	Provenance Provenance
}

// Judge records a verdict on a previously-created pending relation.
func (s *Service) Judge(input JudgeInput) (*store.Relation, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.JudgmentID) == "" {
		return nil, errors.New("judgment id is required")
	}
	relation, err := s.store.JudgeRelation(store.JudgeRelationParams{
		JudgmentID:    input.JudgmentID,
		Relation:      input.Relation,
		Reason:        input.Reason,
		Evidence:      input.Evidence,
		Confidence:    input.Confidence,
		MarkedByActor: input.Provenance.Actor,
		MarkedByKind:  input.Provenance.Kind,
		MarkedByModel: input.Provenance.Model,
		SessionID:     input.Provenance.SessionID,
	})
	if err != nil {
		return nil, fmt.Errorf("judge relation: %w", err)
	}
	return relation, nil
}

// ReviewListInput controls review queries. Project must be resolved by the
// caller unless AllProjects is explicitly requested.
type ReviewListInput struct {
	Project     string
	Limit       int
	AllProjects bool
}

// ReviewList returns observations whose review lifecycle is due.
func (s *Service) ReviewList(input ReviewListInput) ([]store.Observation, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if input.AllProjects && strings.TrimSpace(input.Project) != "" {
		return nil, errors.New("project and all projects cannot be used together")
	}
	project := ""
	if !input.AllProjects {
		if strings.TrimSpace(input.Project) == "" {
			return nil, ErrProjectRequired
		}
		project, _ = store.NormalizeProject(input.Project)
	}
	observations, err := s.store.ObservationsNeedingReview(project, input.Limit)
	if err != nil {
		return nil, fmt.Errorf("list observations needing review: %w", err)
	}
	return observations, nil
}

// ReviewMark resets an observation's local review cycle and returns its new state.
func (s *Service) ReviewMark(id int64) (*store.Observation, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if id <= 0 {
		return nil, errors.New("observation id must be positive")
	}
	if err := s.store.MarkReviewed(id); err != nil {
		return nil, fmt.Errorf("mark observation reviewed: %w", err)
	}
	observation, err := s.store.GetObservation(id)
	if err != nil {
		return nil, fmt.Errorf("reload reviewed observation: %w", err)
	}
	return observation, nil
}

// SetPinned changes the local-only pin state and returns the reloaded observation.
func (s *Service) SetPinned(id int64, pinned bool) (*store.Observation, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	if id <= 0 {
		return nil, errors.New("observation id must be positive")
	}
	var err error
	if pinned {
		err = s.store.PinObservation(id)
	} else {
		err = s.store.UnpinObservation(id)
	}
	if err != nil {
		return nil, fmt.Errorf("set observation pinned: %w", err)
	}
	observation, err := s.store.GetObservation(id)
	if err != nil {
		return nil, fmt.Errorf("reload pinned observation: %w", err)
	}
	return observation, nil
}

// MergeInput controls an explicit project migration. DryRun returns projected
// record counts and does not modify the store.
type MergeInput struct {
	Sources   []string
	Canonical string
	DryRun    bool
}

// MergeResult is the record-count summary for an explicit project merge.
type MergeResult struct {
	store.MergeResult
	DryRun bool `json:"dry_run"`
}

// Merge either previews or applies a deterministic project merge. The preview
// is based on the store's project aggregate counts and is exact for normalized
// project rows, which is the format produced by current store writes.
func (s *Service) Merge(input MergeInput) (*MergeResult, error) {
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	canonical, _ := store.NormalizeProject(input.Canonical)
	if canonical == "" {
		return nil, errors.New("canonical project name is required")
	}
	sources := normalizedSources(input.Sources, canonical)
	if len(sources) == 0 {
		return nil, errors.New("at least one source project is required")
	}
	if !input.DryRun {
		merged, err := s.store.MergeProjects(sources, canonical)
		if err != nil {
			return nil, fmt.Errorf("merge projects: %w", err)
		}
		return &MergeResult{MergeResult: *merged}, nil
	}

	preview, err := s.store.PreviewMergeProjects(input.Sources, canonical)
	if err != nil {
		return nil, fmt.Errorf("preview project merge: %w", err)
	}
	return &MergeResult{MergeResult: *preview, DryRun: true}, nil
}

func (s *Service) requireStore() error {
	if s == nil || s.store == nil {
		return ErrStoreRequired
	}
	return nil
}

func normalizedSources(sources []string, canonical string) []string {
	seen := make(map[string]struct{}, len(sources))
	normalized := make([]string, 0, len(sources))
	for _, source := range sources {
		name, _ := store.NormalizeProject(source)
		if name == "" || name == canonical {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}
