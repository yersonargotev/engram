// Package taskbriefing selects durable memories for a transient task.
package taskbriefing

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/yersonargotev/engram/internal/store"
)

type SignalType string

const (
	SignalTaskIntent    SignalType = "task_intent"
	SignalBranch        SignalType = "branch"
	SignalBranchDiff    SignalType = "branch_diff"
	SignalStagedDiff    SignalType = "staged_diff"
	SignalUnstagedDiff  SignalType = "unstaged_diff"
	SignalAffectedPath  SignalType = "affected_path"
	SignalCommitSubject SignalType = "commit_subject"
	SignalUntrackedPath SignalType = "untracked_path"
)

type RepositorySignals struct {
	Branch                 string            `json:"branch"`
	BranchDiff             string            `json:"branch_diff"`
	StagedDiff             string            `json:"staged_diff"`
	UnstagedDiff           string            `json:"unstaged_diff"`
	AffectedPaths          []string          `json:"affected_paths"`
	CommitSubjects         []string          `json:"commit_subjects"`
	UntrackedPaths         []string          `json:"untracked_paths"`
	BaseUnresolved         bool              `json:"base_unresolved"`
	GitFailures            []SignalType      `json:"git_failures"`
	BaseResolution         *BaseResolution   `json:"base_resolution,omitempty"`
	AcquisitionTruncations []InputTruncation `json:"-"`
}

type Input struct {
	Project           string            `json:"project"`
	Scope             string            `json:"scope"`
	TaskIntent        string            `json:"task_intent"`
	Limit             int               `json:"limit,omitempty"`
	RepositoryProject string            `json:"repository_project"`
	Repository        RepositorySignals `json:"repository"`
	WorkingDirectory  string            `json:"working_directory,omitempty"`
	ExplicitBase      string            `json:"explicit_base,omitempty"`
}

type Defaults struct {
	TaskWeight          int
	BranchWeight        int
	BranchDiffWeight    int
	StagedDiffWeight    int
	UnstagedDiffWeight  int
	AffectedPathWeight  int
	CommitSubjectWeight int
	UntrackedPathWeight int
	TitleOrTopicBonus   int
	PinBoost            int
	InclusionThreshold  int
	MaximumResultCount  int
	TotalOutputBudget   int
	TaskTermLimit       int
	BranchTermLimit     int
	DiffTermLimit       int
	PathTermLimit       int
	CommitTermLimit     int
	UntrackedTermLimit  int
	GitInputByteLimit   int64
}

var CalibratedDefaults = Defaults{
	TaskWeight:          12,
	BranchWeight:        3,
	BranchDiffWeight:    5,
	StagedDiffWeight:    6,
	UnstagedDiffWeight:  6,
	AffectedPathWeight:  7,
	CommitSubjectWeight: 6,
	UntrackedPathWeight: 7,
	TitleOrTopicBonus:   2,
	PinBoost:            2,
	InclusionThreshold:  10,
	MaximumResultCount:  5,
	TotalOutputBudget:   4096,
	TaskTermLimit:       12,
	BranchTermLimit:     6,
	DiffTermLimit:       16,
	PathTermLimit:       12,
	CommitTermLimit:     12,
	UntrackedTermLimit:  12,
	GitInputByteLimit:   1024 * 1024,
}

const (
	candidateRetrievalLimit         = 20
	candidateIdentifierLimit        = 8
	candidateFieldContributionLimit = 6
	candidateFieldTermLimit         = 64
	exactIdentifierStrength         = 4
	maximumRejectionDetails         = 8
	maximumFallbackAnchorBytes      = 96
)

var (
	issueIdentifierPattern  = regexp.MustCompile(`(?i)(?:#|\bissue[\s:#-]*)([0-9]+)\b`)
	prIdentifierPattern     = regexp.MustCompile(`(?i)\b(?:pr|pull[\s-]*request)[\s:#-]*([0-9]+)\b`)
	branchIdentifierPattern = regexp.MustCompile(`(?i)\bbranch(?:[\s_-]+name)?[\s:=]+([a-z0-9](?:[a-z0-9._/-]*[a-z0-9._-])?)(?:$|[^a-z0-9._/-])`)
	commitIdentifierPattern = regexp.MustCompile(`(?i)\bcommit[\s:#-]*([0-9a-f]{7,40})\b`)
	pathIdentifierPattern   = regexp.MustCompile(`(?i)\bpath[\s:=]+([a-z0-9._-]+(?:/[a-z0-9._-]+)+)(?:$|[^a-z0-9._/-])`)
	topicIdentifierPattern  = regexp.MustCompile(`(?i)\btopic(?:[\s_-]+key)?[\s:=]+([a-z0-9._-]+(?:/[a-z0-9._-]+)*)(?:$|[^a-z0-9._/-])`)
	compoundTokenPattern    = regexp.MustCompile(`(?i)([a-z0-9._-]+(?:/[a-z0-9._-]+)+)(?:$|[^a-z0-9._/-])`)
	genericWorkflowTerms    = map[string]struct{}{
		"after": {}, "an": {}, "and": {}, "as": {}, "at": {}, "be": {}, "before": {}, "branch": {},
		"by": {}, "change": {}, "changes": {}, "clean": {}, "code": {}, "commit": {}, "do": {}, "does": {},
		"done": {}, "feat": {}, "feature": {}, "fix": {}, "for": {}, "from": {}, "implement": {},
		"implementation": {}, "in": {}, "into": {}, "is": {}, "issue": {}, "it": {}, "its": {},
		"merge": {}, "merged": {}, "no": {}, "not": {}, "of": {}, "on": {}, "only": {}, "or": {},
		"out": {}, "path": {}, "pr": {}, "pull": {}, "request": {}, "test": {}, "tests": {}, "that": {},
		"the": {}, "then": {}, "these": {}, "this": {}, "those": {}, "through": {}, "to": {}, "topic": {},
		"under": {}, "update": {}, "updated": {}, "use": {}, "uses": {}, "using": {}, "via": {}, "was": {},
		"were": {}, "will": {}, "with": {}, "without": {}, "work": {},
	}
)

type SelectionEvidence struct {
	Signal                  SignalType `json:"signal"`
	MatchedTerms            []string   `json:"matched_terms"`
	MatchedFields           []string   `json:"matched_fields"`
	MatchedIdentifiers      []string   `json:"matched_identifiers,omitempty"`
	MatchedDistinctiveTerms []string   `json:"matched_distinctive_terms,omitempty"`
}

type SelectedMemory struct {
	Memory   store.Observation   `json:"memory"`
	Score    int                 `json:"score"`
	PinBoost int                 `json:"pin_boost"`
	Evidence []SelectionEvidence `json:"evidence"`
}

type Result struct {
	Memories                []SelectedMemory     `json:"memories"`
	Diagnostics             []Diagnostic         `json:"diagnostics"`
	BaseResolution          *BaseResolution      `json:"base_resolution,omitempty"`
	Pipeline                PipelineAccounting   `json:"pipeline"`
	EmptyResultReason       EmptyResultReason    `json:"empty_result_reason,omitempty"`
	Fallback                *SearchFallback      `json:"fallback,omitempty"`
	FallbackCandidate       *SearchFallback      `json:"-"`
	Rejections              []CandidateRejection `json:"rejections"`
	RejectionDetailsOmitted int                  `json:"rejection_details_omitted"`
	ResultLimitOmissions    int                  `json:"result_limit_omissions,omitempty"`
	ConflictPairs           []ConflictPair       `json:"-"`
}

type RejectionStage string

const (
	RejectionStageLifecycle      RejectionStage = "lifecycle"
	RejectionStageTaskGate       RejectionStage = "task_gate"
	RejectionStageRepositoryGate RejectionStage = "repository_gate"
	RejectionStageThreshold      RejectionStage = "threshold"
)

type RejectionReasonCode string

const (
	RejectionSuperseded             RejectionReasonCode = "superseded"
	RejectionTaskEvidenceBelowGate  RejectionReasonCode = "task_evidence_below_gate"
	RejectionRepositoryEvidenceWeak RejectionReasonCode = "repository_evidence_below_gate"
	RejectionScoreBelowThreshold    RejectionReasonCode = "score_below_threshold"
)

type TaskEvidenceProgress struct {
	Matched  int `json:"matched"`
	Required int `json:"required"`
}

type CandidateRejection struct {
	MemoryID     int64                 `json:"memory_id"`
	Stage        RejectionStage        `json:"stage"`
	ReasonCode   RejectionReasonCode   `json:"reason_code"`
	TaskEvidence *TaskEvidenceProgress `json:"task_evidence,omitempty"`
	Score        *int                  `json:"score,omitempty"`
	Threshold    *int                  `json:"threshold,omitempty"`
}

type PipelineAccounting struct {
	EligibleInventory      int                   `json:"eligible_inventory"`
	RetrievedCandidates    int                   `json:"retrieved_candidates"`
	RetrievalCountComplete bool                  `json:"retrieval_count_complete"`
	Retrievals             []RetrievalAccounting `json:"retrievals"`
	TaskGateRejections     int                   `json:"task_gate_rejections"`
	RepositoryRejections   int                   `json:"repository_gate_rejections"`
	LifecycleRejections    int                   `json:"lifecycle_rejections"`
	ThresholdRejections    int                   `json:"threshold_rejections"`
	QualifiedCandidates    int                   `json:"qualified_candidates"`
}

type RetrievalAccounting struct {
	Signals       []SignalType `json:"signals"`
	Limit         int          `json:"limit"`
	Retrieved     int          `json:"retrieved"`
	CountComplete bool         `json:"count_complete"`
}

type EmptyResultReason string

const (
	EmptyResultProjectHasNoMemories     EmptyResultReason = "project_has_no_memories"
	EmptyResultNoCandidatesMatched      EmptyResultReason = "no_candidates_matched"
	EmptyResultCandidatesFiltered       EmptyResultReason = "candidates_filtered"
	EmptyResultCandidatesBelowThreshold EmptyResultReason = "candidates_below_threshold"
	EmptyResultNoUsableSignals          EmptyResultReason = "no_usable_signals"
)

type FallbackReasonCode string

const (
	FallbackNoCandidatesMatched         FallbackReasonCode = "no_candidates_matched"
	FallbackCandidateRetrievalTruncated FallbackReasonCode = "candidate_retrieval_truncated"
	FallbackCandidatesFiltered          FallbackReasonCode = "candidates_filtered"
	FallbackCandidatesBelowThreshold    FallbackReasonCode = "candidates_below_threshold"
	FallbackResultLimitReached          FallbackReasonCode = "result_limit_reached"
	FallbackOutputBudgetExhausted       FallbackReasonCode = "output_budget_exhausted"
)

type SearchFallback struct {
	ReasonCode FallbackReasonCode `json:"reason_code"`
	Anchors    []string           `json:"anchors"`
	Project    string             `json:"project"`
	Scope      string             `json:"scope"`
	Invocation SearchInvocation   `json:"invocation"`
}

type SearchInvocation struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

type ConflictPair struct {
	SourceID string
	TargetID string
}

type Diagnostic struct {
	Code        DiagnosticCode    `json:"code"`
	Sources     []SignalType      `json:"sources,omitempty"`
	Truncations []InputTruncation `json:"truncations,omitempty"`
}

type DiagnosticCode string

const (
	DiagnosticNoUsableSignals             DiagnosticCode = "no_usable_signals"
	DiagnosticRepositoryProjectUnresolved DiagnosticCode = "repository_project_unresolved"
	DiagnosticRepositoryProjectMismatch   DiagnosticCode = "repository_project_mismatch"
	DiagnosticBranchBaseUnresolved        DiagnosticCode = "branch_base_unresolved"
	DiagnosticGitOperationFailed          DiagnosticCode = "git_operation_failed"
	DiagnosticTaskInputTruncated          DiagnosticCode = "task_input_truncated"
	DiagnosticRepositoryInputTruncated    DiagnosticCode = "repository_input_truncated"
	DiagnosticResultLimitReached          DiagnosticCode = "result_limit_reached"
	DiagnosticOutputBudgetExhausted       DiagnosticCode = "output_budget_exhausted"
	DiagnosticSelectedMemoryConflict      DiagnosticCode = "selected_memory_conflict"
)

type InputTruncation struct {
	Signal        SignalType `json:"signal"`
	OmittedTerms  int        `json:"omitted_terms"`
	TotalTerms    int        `json:"total_terms,omitempty"`
	AnalyzedTerms int        `json:"analyzed_terms,omitempty"`
	CountComplete bool       `json:"count_complete"`
}

var ErrMemoryStore = errors.New("task briefing: memory store failure")

type generateError struct {
	code GenerateErrorCode
	err  error
}

func (e *generateError) Error() string { return string(e.code) + ": " + e.err.Error() }
func (e *generateError) Unwrap() error { return e.err }

type GenerateErrorCode string

const (
	ErrorMemoryStoreFailure GenerateErrorCode = "memory_store_failure"
)

func ErrorCode(err error) GenerateErrorCode {
	var coded *generateError
	if errors.As(err, &coded) {
		return coded.code
	}
	return ""
}

type Generator struct {
	store *store.Store
}

func New(memoryStore *store.Store) *Generator {
	return &Generator{store: memoryStore}
}

func (g *Generator) Generate(input Input) (Result, error) {
	if input.WorkingDirectory != "" {
		input.RepositoryProject, input.Repository = inspectRepository(input.WorkingDirectory, input.ExplicitBase, input.Project)
	}
	eligibleInventory, err := g.store.CountSearchableObservations(input.Project, input.Scope)
	if err != nil {
		return Result{}, &generateError{code: ErrorMemoryStoreFailure, err: fmt.Errorf("%w: %v", ErrMemoryStore, err)}
	}
	pipeline := PipelineAccounting{
		EligibleInventory: eligibleInventory, RetrievalCountComplete: true, Retrievals: []RetrievalAccounting{},
	}
	signals, diagnostics, baseResolution := buildSignals(input)
	if len(signals) == 0 {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticNoUsableSignals})
		return Result{
			Diagnostics: diagnostics, BaseResolution: baseResolution, Pipeline: pipeline,
			EmptyResultReason: EmptyResultNoUsableSignals,
		}, nil
	}
	groups := groupSignals(signals)
	retrieval, err := g.retrieveCandidates(input, groups)
	if err != nil {
		return Result{}, err
	}
	pipeline.RetrievedCandidates = len(retrieval.candidates)
	pipeline.RetrievalCountComplete = retrieval.countComplete
	pipeline.Retrievals = retrieval.reports
	relations, err := g.loadCandidateRelations(retrieval.candidates)
	if err != nil {
		return Result{}, err
	}
	ranking := rankCandidates(input, groups, retrieval.candidates, relations)
	pipeline.TaskGateRejections = ranking.taskGateRejections
	pipeline.RepositoryRejections = ranking.repositoryRejections
	pipeline.LifecycleRejections = ranking.lifecycleRejections
	pipeline.ThresholdRejections = ranking.thresholdRejections
	pipeline.QualifiedCandidates = len(ranking.selected)
	result := g.boundSelection(input.Limit, ranking.selected, diagnostics, baseResolution, relations)
	result.Pipeline = pipeline
	result.Rejections = ranking.rejections
	result.RejectionDetailsOmitted = ranking.rejectionDetailsOmitted
	result.EmptyResultReason = emptyResultReason(pipeline, len(ranking.selected))
	fallbackCandidate := buildSearchFallback(input, signals)
	result.FallbackCandidate = fallbackCandidate
	switch {
	case !pipeline.RetrievalCountComplete:
		result.Fallback = fallbackWithReason(fallbackCandidate, FallbackCandidateRetrievalTruncated)
	case pipeline.RetrievedCandidates == 0 && pipeline.EligibleInventory > 0:
		result.Fallback = fallbackWithReason(fallbackCandidate, FallbackNoCandidatesMatched)
	case pipeline.TaskGateRejections > 0 || pipeline.RepositoryRejections > 0 || pipeline.LifecycleRejections > 0:
		result.Fallback = fallbackWithReason(fallbackCandidate, FallbackCandidatesFiltered)
	case pipeline.ThresholdRejections > 0:
		result.Fallback = fallbackWithReason(fallbackCandidate, FallbackCandidatesBelowThreshold)
	case result.ResultLimitOmissions > 0:
		result.Fallback = fallbackWithReason(fallbackCandidate, FallbackResultLimitReached)
	}
	return result, nil
}

type candidateRetrieval struct {
	candidates    map[int64]store.Observation
	reports       []RetrievalAccounting
	countComplete bool
}

func (g *Generator) retrieveCandidates(input Input, groups []signalGroup) (candidateRetrieval, error) {
	candidates := make(map[int64]store.Observation)
	reports := make([]RetrievalAccounting, 0, len(groups))
	countComplete := true
	for _, group := range groups {
		results, err := g.store.Search(strings.Join(group.terms, " "), store.SearchOptions{
			Project: input.Project, Scope: input.Scope, Limit: candidateRetrievalLimit, MatchMode: "any",
		})
		if err != nil {
			return candidateRetrieval{}, &generateError{code: ErrorMemoryStoreFailure, err: fmt.Errorf("%w: %v", ErrMemoryStore, err)}
		}
		complete := len(results) < candidateRetrievalLimit
		if !complete {
			countComplete = false
		}
		for _, result := range results {
			candidates[result.ID] = result.Observation
		}
		reports = append(reports, RetrievalAccounting{
			Signals: append([]SignalType(nil), group.sources...), Limit: candidateRetrievalLimit,
			Retrieved: len(results), CountComplete: complete,
		})
	}
	return candidateRetrieval{candidates: candidates, reports: reports, countComplete: countComplete}, nil
}

func emptyResultReason(pipeline PipelineAccounting, qualified int) EmptyResultReason {
	if qualified > 0 {
		return ""
	}
	if pipeline.EligibleInventory == 0 {
		return EmptyResultProjectHasNoMemories
	}
	if pipeline.RetrievedCandidates == 0 {
		return EmptyResultNoCandidatesMatched
	}
	if pipeline.ThresholdRejections > 0 && pipeline.TaskGateRejections == 0 && pipeline.RepositoryRejections == 0 && pipeline.LifecycleRejections == 0 {
		return EmptyResultCandidatesBelowThreshold
	}
	return EmptyResultCandidatesFiltered
}

func buildSearchFallback(input Input, signals []weightedSignal) *SearchFallback {
	anchors := make([]string, 0, 3)
	seen := make(map[string]struct{})
	appendAnchor := func(anchor string) {
		if len(anchors) >= 3 || anchor == "" || len(anchor) > maximumFallbackAnchorBytes {
			return
		}
		if _, found := seen[anchor]; found {
			return
		}
		seen[anchor] = struct{}{}
		anchors = append(anchors, anchor)
	}
	for _, signal := range signals {
		for _, identifier := range signal.identifiers {
			appendAnchor(searchAnchorForIdentifier(identifier))
		}
	}
	for _, signal := range signals {
		for _, term := range signal.distinctiveTerms {
			appendAnchor(term)
		}
	}
	if len(anchors) == 0 {
		return nil
	}
	query := strings.Join(anchors, " ")
	args := []string{"search", query, "--project", input.Project, "--match-mode", "all", "--limit", "5", "--json"}
	scope := input.Scope
	if scope == "" {
		scope = "project_and_personal"
	} else {
		args = append(args[:4], append([]string{"--scope", input.Scope}, args[4:]...)...)
	}
	return &SearchFallback{
		Anchors: anchors, Project: input.Project, Scope: scope,
		Invocation: SearchInvocation{Command: "engram", Args: args},
	}
}

func searchAnchorForIdentifier(identifier string) string {
	for _, prefix := range []string{"pr:#", "issue:#"} {
		if value, found := strings.CutPrefix(identifier, prefix); found {
			return strings.TrimSuffix(prefix, ":#") + " " + value
		}
	}
	for _, prefix := range []string{"branch:", "commit:", "path:", "topic:"} {
		if value, found := strings.CutPrefix(identifier, prefix); found {
			if _, generic := genericWorkflowTerms[value]; generic {
				return ""
			}
			return value
		}
	}
	return ""
}

func fallbackWithReason(fallback *SearchFallback, reason FallbackReasonCode) *SearchFallback {
	if fallback == nil {
		return nil
	}
	copy := *fallback
	copy.ReasonCode = reason
	return &copy
}

func (g *Generator) loadCandidateRelations(candidates map[int64]store.Observation) (map[string]store.ObservationRelations, error) {
	syncIDs := make([]string, 0, len(candidates))
	for _, memory := range candidates {
		syncIDs = append(syncIDs, memory.SyncID)
	}
	sort.Strings(syncIDs)
	relations, err := g.store.GetRelationsForObservations(syncIDs)
	if err != nil {
		return nil, &generateError{code: ErrorMemoryStoreFailure, err: fmt.Errorf("%w: %v", ErrMemoryStore, err)}
	}
	return relations, nil
}

type rankingResult struct {
	selected                []SelectedMemory
	rejections              []CandidateRejection
	rejectionDetailsOmitted int
	taskGateRejections      int
	repositoryRejections    int
	lifecycleRejections     int
	thresholdRejections     int
}

func rankCandidates(input Input, groups []signalGroup, candidates map[int64]store.Observation, relations map[string]store.ObservationRelations) rankingResult {
	ranked := rankingResult{
		selected:   make([]SelectedMemory, 0, len(candidates)),
		rejections: make([]CandidateRejection, 0, min(len(candidates), maximumRejectionDetails)),
	}
	recordRejection := func(rejection CandidateRejection) {
		if len(ranked.rejections) < maximumRejectionDetails {
			ranked.rejections = append(ranked.rejections, rejection)
			return
		}
		ranked.rejectionDetailsOmitted++
	}

	candidateIDs := make([]int64, 0, len(candidates))
	for id := range candidates {
		candidateIDs = append(candidateIDs, id)
	}
	sort.Slice(candidateIDs, func(i, j int) bool { return candidateIDs[i] < candidateIDs[j] })

	for _, candidateID := range candidateIDs {
		memory := candidates[candidateID]
		if isSuperseded(relations[memory.SyncID]) {
			ranked.lifecycleRejections++
			recordRejection(CandidateRejection{MemoryID: memory.ID, Stage: RejectionStageLifecycle, ReasonCode: RejectionSuperseded})
			continue
		}
		matches := make([]matchedSignalGroup, 0, len(groups))
		hasTaskEvidence := false
		hasStrongRepositoryEvidence := false
		taskProgress := TaskEvidenceProgress{}
		for _, group := range groups {
			evidence := matchEvidence(memory, group)
			isTaskGroup := containsSignalType(group.sources, SignalTaskIntent)
			if isTaskGroup {
				matched, required := group.taskEvidenceProgress(evidence)
				taskProgress = TaskEvidenceProgress{Matched: matched, Required: required}
			}
			if group.qualifies(evidence) {
				matches = append(matches, matchedSignalGroup{group: group, evidence: evidence})
				if isTaskGroup {
					hasTaskEvidence = true
				} else {
					hasStrongRepositoryEvidence = true
				}
				continue
			}
			if input.TaskIntent == "" && !isTaskGroup && len(evidence.MatchedTerms) >= 2 {
				matches = append(matches, matchedSignalGroup{group: group, evidence: evidence})
			}
		}
		if input.TaskIntent != "" && !hasTaskEvidence {
			ranked.taskGateRejections++
			progress := taskProgress
			recordRejection(CandidateRejection{
				MemoryID: memory.ID, Stage: RejectionStageTaskGate, ReasonCode: RejectionTaskEvidenceBelowGate,
				TaskEvidence: &progress,
			})
			continue
		}
		if input.TaskIntent == "" && !hasStrongRepositoryEvidence {
			ranked.repositoryRejections++
			recordRejection(CandidateRejection{MemoryID: memory.ID, Stage: RejectionStageRepositoryGate, ReasonCode: RejectionRepositoryEvidenceWeak})
			continue
		}
		evidenceItems := expandSelectionEvidence(matches)
		baseScore := scoreMatchedGroups(matches, false)
		if baseScore < CalibratedDefaults.InclusionThreshold {
			ranked.thresholdRejections++
			score := baseScore
			threshold := CalibratedDefaults.InclusionThreshold
			recordRejection(CandidateRejection{
				MemoryID: memory.ID, Stage: RejectionStageThreshold, ReasonCode: RejectionScoreBelowThreshold,
				Score: &score, Threshold: &threshold,
			})
			continue
		}
		score := scoreMatchedGroups(matches, true)
		pinBoost := 0
		if memory.Pinned {
			pinBoost = CalibratedDefaults.PinBoost
			score += pinBoost
		}
		ranked.selected = append(ranked.selected, SelectedMemory{
			Memory:   memory,
			Score:    score,
			PinBoost: pinBoost,
			Evidence: evidenceItems,
		})
	}
	sort.Slice(ranked.selected, func(i, j int) bool {
		if ranked.selected[i].Score != ranked.selected[j].Score {
			return ranked.selected[i].Score > ranked.selected[j].Score
		}
		if ranked.selected[i].Memory.Title != ranked.selected[j].Memory.Title {
			return ranked.selected[i].Memory.Title < ranked.selected[j].Memory.Title
		}
		return ranked.selected[i].Memory.ID < ranked.selected[j].Memory.ID
	})
	return ranked
}

type matchedSignalGroup struct {
	group    signalGroup
	evidence SelectionEvidence
}

func (g signalGroup) qualifies(evidence SelectionEvidence) bool {
	identifierStrength := len(g.identifiers) * exactIdentifierStrength
	matchedStrength := len(evidence.MatchedIdentifiers)*exactIdentifierStrength + len(evidence.MatchedDistinctiveTerms)
	if containsSignalType(g.sources, SignalTaskIntent) {
		totalStrength := identifierStrength + len(g.distinctiveTerms)
		return totalStrength > 0 && matchedStrength >= (totalStrength+1)/2
	}
	return len(evidence.MatchedIdentifiers) > 0 ||
		(len(evidence.MatchedDistinctiveTerms) > 0 && len(evidence.MatchedTerms) >= 2)
}

func (g signalGroup) taskEvidenceProgress(evidence SelectionEvidence) (int, int) {
	matched := len(evidence.MatchedIdentifiers)*exactIdentifierStrength + len(evidence.MatchedDistinctiveTerms)
	total := len(g.identifiers)*exactIdentifierStrength + len(g.distinctiveTerms)
	required := (total + 1) / 2
	if required == 0 {
		required = 1
	}
	return matched, required
}

func expandSelectionEvidence(matches []matchedSignalGroup) []SelectionEvidence {
	var evidenceItems []SelectionEvidence
	for _, match := range matches {
		for _, source := range match.group.sources {
			evidence := match.evidence
			evidence.Signal = source
			evidenceItems = append(evidenceItems, evidence)
		}
	}
	return evidenceItems
}

func scoreMatchedGroups(matches []matchedSignalGroup, includeMatchStrength bool) int {
	score := 0
	for _, match := range matches {
		score += match.group.weight
		if includeMatchStrength {
			score += len(match.evidence.MatchedIdentifiers)*exactIdentifierStrength + len(match.evidence.MatchedDistinctiveTerms)
		}
		if containsString(match.evidence.MatchedFields, "title") || containsString(match.evidence.MatchedFields, "topic_key") {
			score += CalibratedDefaults.TitleOrTopicBonus
		}
	}
	return score
}

func (g *Generator) boundSelection(inputLimit int, selected []SelectedMemory, diagnostics []Diagnostic, baseResolution *BaseResolution, relations map[string]store.ObservationRelations) Result {
	resultLimit := CalibratedDefaults.MaximumResultCount
	if inputLimit > 0 && inputLimit < resultLimit {
		resultLimit = inputLimit
	}
	resultLimitOmissions := 0
	if len(selected) > resultLimit {
		resultLimitOmissions = len(selected) - resultLimit
		selected = selected[:resultLimit]
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticResultLimitReached})
	}

	if hasSelectedConflict(selected, relations) {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticSelectedMemoryConflict})
	}

	return Result{
		Memories: selected, Diagnostics: diagnostics, BaseResolution: baseResolution,
		ResultLimitOmissions: resultLimitOmissions,
		ConflictPairs:        selectedConflictPairs(selected, relations),
	}
}

type weightedSignal struct {
	kind             SignalType
	terms            []string
	identifiers      []string
	distinctiveTerms []string
	weight           int
}

type signalGroup struct {
	terms            []string
	identifiers      []string
	distinctiveTerms []string
	sources          []SignalType
	weight           int
}

func groupSignals(signals []weightedSignal) []signalGroup {
	groups := make([]signalGroup, 0, len(signals))
	byQuery := make(map[string]int, len(signals))
	for _, signal := range signals {
		groupKey := strings.Join(signal.terms, " ") + "\x00" + strings.Join(signal.identifiers, " ")
		if index, found := byQuery[groupKey]; found {
			groups[index].sources = append(groups[index].sources, signal.kind)
			if signal.weight > groups[index].weight {
				groups[index].weight = signal.weight
			}
			continue
		}
		byQuery[groupKey] = len(groups)
		groups = append(groups, signalGroup{
			terms: append([]string(nil), signal.terms...), identifiers: append([]string(nil), signal.identifiers...),
			distinctiveTerms: append([]string(nil), signal.distinctiveTerms...), sources: []SignalType{signal.kind}, weight: signal.weight,
		})
	}
	return groups
}

func buildSignals(input Input) ([]weightedSignal, []Diagnostic, *BaseResolution) {
	var diagnostics []Diagnostic
	repository := input.Repository
	baseResolution := repository.BaseResolution
	hasRepositoryInput := repositoryHasInput(repository)
	if hasRepositoryInput && input.RepositoryProject == "" {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticRepositoryProjectUnresolved})
		repository = RepositorySignals{}
		baseResolution = nil
	} else if input.RepositoryProject != "" && !strings.EqualFold(input.RepositoryProject, input.Project) {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticRepositoryProjectMismatch})
		repository = RepositorySignals{}
		baseResolution = nil
	} else {
		if repository.BaseUnresolved {
			diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticBranchBaseUnresolved})
		}
		if len(repository.GitFailures) > 0 {
			sources := normalizeGitFailureSources(repository.GitFailures)
			if len(sources) > 0 {
				diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticGitOperationFailed, Sources: sources})
			}
		}
	}
	rawSignals := []struct {
		kind   SignalType
		raw    string
		weight int
		limit  int
	}{
		{kind: SignalTaskIntent, raw: input.TaskIntent, weight: CalibratedDefaults.TaskWeight, limit: CalibratedDefaults.TaskTermLimit},
		{kind: SignalBranch, raw: repository.Branch, weight: CalibratedDefaults.BranchWeight, limit: CalibratedDefaults.BranchTermLimit},
		{kind: SignalBranchDiff, raw: repository.BranchDiff, weight: CalibratedDefaults.BranchDiffWeight, limit: CalibratedDefaults.DiffTermLimit},
		{kind: SignalStagedDiff, raw: repository.StagedDiff, weight: CalibratedDefaults.StagedDiffWeight, limit: CalibratedDefaults.DiffTermLimit},
		{kind: SignalUnstagedDiff, raw: repository.UnstagedDiff, weight: CalibratedDefaults.UnstagedDiffWeight, limit: CalibratedDefaults.DiffTermLimit},
		{kind: SignalAffectedPath, raw: strings.Join(repository.AffectedPaths, " "), weight: CalibratedDefaults.AffectedPathWeight, limit: CalibratedDefaults.PathTermLimit},
		{kind: SignalCommitSubject, raw: strings.Join(repository.CommitSubjects, " "), weight: CalibratedDefaults.CommitSubjectWeight, limit: CalibratedDefaults.CommitTermLimit},
		{kind: SignalUntrackedPath, raw: strings.Join(repository.UntrackedPaths, " "), weight: CalibratedDefaults.UntrackedPathWeight, limit: CalibratedDefaults.UntrackedTermLimit},
	}
	signals := make([]weightedSignal, 0, len(rawSignals))
	repositoryTruncations := append([]InputTruncation(nil), repository.AcquisitionTruncations...)
	for _, raw := range rawSignals {
		terms, omitted := normalizeTermsWithCount(raw.raw, raw.limit)
		if omitted > 0 {
			truncation := InputTruncation{Signal: raw.kind, OmittedTerms: omitted, TotalTerms: len(terms) + omitted, AnalyzedTerms: len(terms), CountComplete: true}
			if raw.kind == SignalTaskIntent {
				diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticTaskInputTruncated, Truncations: []InputTruncation{truncation}})
			} else {
				repositoryTruncations = append(repositoryTruncations, truncation)
			}
		}
		if len(terms) == 0 {
			continue
		}
		identifierSignal := raw.kind
		if raw.kind == SignalBranch && omitted > 0 {
			identifierSignal = ""
		}
		identifiers := extractExactIdentifiers(boundedIdentifierInput(raw.raw, raw.limit), identifierSignal, raw.limit)
		signals = append(signals, weightedSignal{
			kind: raw.kind, terms: terms, identifiers: identifiers,
			distinctiveTerms: distinctiveTerms(terms), weight: raw.weight,
		})
	}
	if len(repositoryTruncations) > 0 {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticRepositoryInputTruncated, Truncations: repositoryTruncations})
	}
	return signals, diagnostics, baseResolution
}

func normalizeTerms(raw string, limit int) []string {
	terms, _ := normalizeTermsWithCount(raw, limit)
	return terms
}

func normalizeTermsWithCount(raw string, limit int) ([]string, int) {
	terms, omitted, _, _ := collectTerms(strings.NewReader(raw), limit, 0)
	return terms, omitted
}

func matchEvidence(memory store.Observation, group signalGroup) SelectionEvidence {
	fields := []struct {
		name  string
		value string
	}{
		{name: "title", value: memory.Title},
		{name: "content", value: memory.Content},
		{name: "type", value: memory.Type},
	}
	if memory.TopicKey != nil {
		fields = append(fields, struct {
			name  string
			value string
		}{name: "topic_key", value: *memory.TopicKey})
	}

	matchedTerms := make([]string, 0, len(group.terms))
	matchedIdentifiers := make([]string, 0, len(group.identifiers))
	matchedFields := make(map[string]struct{})
	identifierMatches := make(map[string]struct{}, len(group.identifiers))
	termMatches := make(map[string]struct{}, len(group.terms))
	distinctiveSet := stringSet(group.distinctiveTerms)
	for _, field := range fields {
		boundedField := boundedIdentifierInput(field.value, candidateFieldTermLimit)
		fieldIdentifiers := stringSet(extractMemoryIdentifiers(boundedField, field.name, candidateIdentifierLimit))
		fieldTerms := stringSet(normalizeTerms(field.value, candidateFieldTermLimit))
		remaining := candidateFieldContributionLimit
		for _, identifier := range group.identifiers {
			if _, found := fieldIdentifiers[identifier]; found {
				matchedFields[field.name] = struct{}{}
				if _, alreadyMatched := identifierMatches[identifier]; !alreadyMatched && remaining > 0 {
					identifierMatches[identifier] = struct{}{}
					remaining--
				}
			}
		}
		for _, term := range group.distinctiveTerms {
			if _, found := fieldTerms[term]; found {
				matchedFields[field.name] = struct{}{}
				if _, alreadyMatched := termMatches[term]; !alreadyMatched && remaining > 0 {
					termMatches[term] = struct{}{}
					remaining--
				}
			}
		}
		for _, term := range group.terms {
			if _, distinctive := distinctiveSet[term]; distinctive {
				continue
			}
			if _, found := fieldTerms[term]; found {
				matchedFields[field.name] = struct{}{}
				if _, alreadyMatched := termMatches[term]; !alreadyMatched && remaining > 0 {
					termMatches[term] = struct{}{}
					remaining--
				}
			}
		}
	}
	for _, identifier := range group.identifiers {
		if _, found := identifierMatches[identifier]; found {
			matchedIdentifiers = append(matchedIdentifiers, identifier)
		}
	}
	for _, term := range group.terms {
		if _, found := termMatches[term]; found {
			matchedTerms = append(matchedTerms, term)
		}
	}
	fieldNames := make([]string, 0, len(matchedFields))
	for field := range matchedFields {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)
	matchedDistinctiveTerms := intersectOrdered(group.distinctiveTerms, stringSet(matchedTerms))
	return SelectionEvidence{
		Signal: group.sources[0], MatchedTerms: matchedTerms, MatchedFields: fieldNames,
		MatchedIdentifiers: matchedIdentifiers, MatchedDistinctiveTerms: matchedDistinctiveTerms,
	}
}

func extractExactIdentifiers(raw string, signal SignalType, limit int) []string {
	identifiers := make([]string, 0, min(limit, candidateIdentifierLimit))
	seen := make(map[string]struct{})
	appendIdentifier := func(identifier string) {
		if len(identifiers) >= limit || identifier == "" {
			return
		}
		if _, found := seen[identifier]; found {
			return
		}
		seen[identifier] = struct{}{}
		identifiers = append(identifiers, identifier)
	}
	appendMatches := func(source, prefix string, pattern *regexp.Regexp) {
		for _, match := range pattern.FindAllStringSubmatch(source, -1) {
			if len(identifiers) >= limit || len(match) < 2 {
				return
			}
			appendIdentifier(prefix + normalizeIdentifierValue(match[1]))
		}
	}
	appendMatches(raw, "pr:#", prIdentifierPattern)
	withoutPullRequests := prIdentifierPattern.ReplaceAllString(raw, " ")
	appendMatches(withoutPullRequests, "issue:#", issueIdentifierPattern)
	appendMatches(raw, "branch:", branchIdentifierPattern)
	appendMatches(raw, "commit:", commitIdentifierPattern)
	appendMatches(raw, "path:", pathIdentifierPattern)
	appendMatches(raw, "topic:", topicIdentifierPattern)
	if signal == SignalBranch {
		appendIdentifier("branch:" + normalizeIdentifierValue(raw))
	}
	if signal == SignalAffectedPath || signal == SignalUntrackedPath || signal == SignalBranchDiff || signal == SignalStagedDiff || signal == SignalUnstagedDiff {
		for _, match := range compoundTokenPattern.FindAllStringSubmatch(raw, -1) {
			if len(match) >= 2 {
				appendIdentifier("path:" + normalizeIdentifierValue(match[1]))
			}
		}
	}
	return identifiers
}

func extractMemoryIdentifiers(raw, field string, limit int) []string {
	identifiers := extractExactIdentifiers(raw, "", limit)
	if field == "topic_key" && len(identifiers) < limit {
		topicIdentifier := "topic:" + normalizeIdentifierValue(raw)
		if topicIdentifier != "topic:" && !containsString(identifiers, topicIdentifier) {
			identifiers = append(identifiers, topicIdentifier)
		}
	}
	return identifiers
}

func normalizeIdentifierValue(value string) string {
	normalized := strings.ToLower(strings.Trim(value, " \t\r\n.,;:()[]{}<>\"'`"))
	normalized = strings.TrimPrefix(normalized, "./")
	return strings.TrimSuffix(normalized, "/")
}

func distinctiveTerms(terms []string) []string {
	distinctive := make([]string, 0, len(terms))
	for _, term := range terms {
		if _, generic := genericWorkflowTerms[term]; generic || onlyDigits(term) {
			continue
		}
		distinctive = append(distinctive, term)
	}
	return distinctive
}

func onlyDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func intersectOrdered(values []string, matches map[string]struct{}) []string {
	intersection := make([]string, 0, len(values))
	for _, value := range values {
		if _, found := matches[value]; found {
			intersection = append(intersection, value)
		}
	}
	return intersection
}

func repositoryHasInput(repository RepositorySignals) bool {
	return repository.Branch != "" || repository.BranchDiff != "" || repository.StagedDiff != "" ||
		repository.UnstagedDiff != "" || len(repository.AffectedPaths) > 0 ||
		len(repository.CommitSubjects) > 0 || len(repository.UntrackedPaths) > 0 ||
		repository.BaseUnresolved || len(repository.GitFailures) > 0 || repository.BaseResolution != nil
}

func normalizeGitFailureSources(sources []SignalType) []SignalType {
	allowed := map[SignalType]bool{
		SignalBranch: true, SignalBranchDiff: true, SignalStagedDiff: true,
		SignalUnstagedDiff: true, SignalAffectedPath: true,
		SignalCommitSubject: true, SignalUntrackedPath: true,
	}
	seen := make(map[SignalType]struct{}, len(sources))
	result := make([]SignalType, 0, len(allowed))
	for _, source := range sources {
		if !allowed[source] {
			continue
		}
		if _, exists := seen[source]; exists {
			continue
		}
		seen[source] = struct{}{}
		result = append(result, source)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsSignalType(values []SignalType, target SignalType) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func hasDiagnostic(diagnostics []Diagnostic, code DiagnosticCode) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func isSuperseded(relations store.ObservationRelations) bool {
	for _, relation := range relations.AsTarget {
		if relation.JudgmentStatus == store.JudgmentStatusJudged && relation.Relation == store.RelationSupersedes {
			return true
		}
	}
	return false
}

func hasSelectedConflict(selected []SelectedMemory, relations map[string]store.ObservationRelations) bool {
	return len(selectedConflictPairs(selected, relations)) > 0
}

func selectedConflictPairs(selected []SelectedMemory, relations map[string]store.ObservationRelations) []ConflictPair {
	selectedIDs := make(map[string]struct{}, len(selected))
	for _, memory := range selected {
		selectedIDs[memory.Memory.SyncID] = struct{}{}
	}
	var pairs []ConflictPair
	for _, memory := range selected {
		for _, relation := range relations[memory.Memory.SyncID].AsSource {
			if relation.JudgmentStatus != store.JudgmentStatusJudged || relation.Relation != store.RelationConflictsWith {
				continue
			}
			if _, found := selectedIDs[relation.TargetID]; found {
				pairs = append(pairs, ConflictPair{SourceID: memory.Memory.SyncID, TargetID: relation.TargetID})
			}
		}
	}
	return pairs
}
