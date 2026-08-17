// Package taskbriefing selects durable memories for a transient task.
package taskbriefing

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

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
	Branch         string       `json:"branch"`
	BranchDiff     string       `json:"branch_diff"`
	StagedDiff     string       `json:"staged_diff"`
	UnstagedDiff   string       `json:"unstaged_diff"`
	AffectedPaths  []string     `json:"affected_paths"`
	CommitSubjects []string     `json:"commit_subjects"`
	UntrackedPaths []string     `json:"untracked_paths"`
	BaseUnresolved bool         `json:"base_unresolved"`
	GitFailures    []SignalType `json:"git_failures"`
}

type Input struct {
	Project           string            `json:"project"`
	Scope             string            `json:"scope"`
	TaskIntent        string            `json:"task_intent"`
	Limit             int               `json:"limit,omitempty"`
	RepositoryProject string            `json:"repository_project"`
	Repository        RepositorySignals `json:"repository"`
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
}

const candidateRetrievalLimit = 20

type SelectionEvidence struct {
	Signal        SignalType `json:"signal"`
	MatchedTerms  []string   `json:"matched_terms"`
	MatchedFields []string   `json:"matched_fields"`
}

type SelectedMemory struct {
	Memory   store.Observation   `json:"memory"`
	Score    int                 `json:"score"`
	PinBoost int                 `json:"pin_boost"`
	Evidence []SelectionEvidence `json:"evidence"`
}

type Result struct {
	Memories             []SelectedMemory `json:"memories"`
	Diagnostics          []Diagnostic     `json:"diagnostics"`
	ResultLimitOmissions int              `json:"result_limit_omissions,omitempty"`
	BudgetOmissions      int              `json:"budget_omissions"`
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
	Signal       SignalType `json:"signal"`
	OmittedTerms int        `json:"omitted_terms"`
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
	ErrorMemoryStoreFailure  GenerateErrorCode = "memory_store_failure"
	ErrorOutputBudgetFailure GenerateErrorCode = "output_budget_failure"
)

func ErrorCode(err error) GenerateErrorCode {
	var coded *generateError
	if errors.As(err, &coded) {
		return coded.code
	}
	return ""
}

type Generator struct {
	store        *store.Store
	outputBudget int
}

func New(memoryStore *store.Store) *Generator {
	return &Generator{store: memoryStore, outputBudget: CalibratedDefaults.TotalOutputBudget}
}

func (g *Generator) Generate(input Input) (Result, error) {
	signals, diagnostics := buildSignals(input)
	if len(signals) == 0 {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticNoUsableSignals})
		result := Result{Diagnostics: diagnostics}
		if !fitsOutputBudget(result, g.outputBudget) {
			return Result{}, &generateError{code: ErrorOutputBudgetFailure, err: errors.New("diagnostics exceed total output budget")}
		}
		return result, nil
	}

	candidates := make(map[int64]store.Observation)
	for _, signal := range signals {
		results, err := g.store.Search(strings.Join(signal.terms, " "), store.SearchOptions{
			Project:   input.Project,
			Scope:     input.Scope,
			Limit:     candidateRetrievalLimit,
			MatchMode: "any",
		})
		if err != nil {
			return Result{}, &generateError{code: ErrorMemoryStoreFailure, err: fmt.Errorf("%w: %v", ErrMemoryStore, err)}
		}
		for _, result := range results {
			candidates[result.ID] = result.Observation
		}
	}

	syncIDs := make([]string, 0, len(candidates))
	for _, memory := range candidates {
		syncIDs = append(syncIDs, memory.SyncID)
	}
	sort.Strings(syncIDs)
	relations, err := g.store.GetRelationsForObservations(syncIDs)
	if err != nil {
		return Result{}, &generateError{code: ErrorMemoryStoreFailure, err: fmt.Errorf("%w: %v", ErrMemoryStore, err)}
	}

	selected := make([]SelectedMemory, 0, len(candidates))
	for _, memory := range candidates {
		if isSuperseded(relations[memory.SyncID]) {
			continue
		}
		var evidenceItems []SelectionEvidence
		score := 0
		hasTaskEvidence := false
		for _, signal := range signals {
			evidence := matchEvidence(memory, signal.kind, signal.terms)
			minimumTerms := 2
			if signal.kind == SignalTaskIntent {
				minimumTerms = (len(signal.terms) + 1) / 2
			}
			if len(evidence.MatchedTerms) < minimumTerms {
				continue
			}
			evidenceItems = append(evidenceItems, evidence)
			score += signal.weight
			if containsString(evidence.MatchedFields, "title") || containsString(evidence.MatchedFields, "topic_key") {
				score += CalibratedDefaults.TitleOrTopicBonus
			}
			if signal.kind == SignalTaskIntent {
				hasTaskEvidence = true
			}
		}
		if input.TaskIntent != "" && !hasTaskEvidence {
			continue
		}
		if score < CalibratedDefaults.InclusionThreshold {
			continue
		}
		pinBoost := 0
		if memory.Pinned {
			pinBoost = CalibratedDefaults.PinBoost
			score += pinBoost
		}
		selected = append(selected, SelectedMemory{
			Memory:   memory,
			Score:    score,
			PinBoost: pinBoost,
			Evidence: evidenceItems,
		})
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].Score != selected[j].Score {
			return selected[i].Score > selected[j].Score
		}
		if selected[i].Memory.Title != selected[j].Memory.Title {
			return selected[i].Memory.Title < selected[j].Memory.Title
		}
		return selected[i].Memory.ID < selected[j].Memory.ID
	})

	resultLimit := CalibratedDefaults.MaximumResultCount
	if input.Limit > 0 && input.Limit < resultLimit {
		resultLimit = input.Limit
	}
	resultLimitOmissions := 0
	if len(selected) > resultLimit {
		resultLimitOmissions = len(selected) - resultLimit
		selected = selected[:resultLimit]
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticResultLimitReached})
	}

	bounded := make([]SelectedMemory, 0, len(selected))
	budgetOmissions := 0
	for _, memory := range selected {
		candidateMemories := appendCopy(bounded, memory)
		if !fitsOutputBudget(Result{Memories: candidateMemories, Diagnostics: diagnostics, ResultLimitOmissions: resultLimitOmissions}, g.outputBudget) {
			budgetOmissions++
			continue
		}
		bounded = append(bounded, memory)
	}
	if budgetOmissions > 0 {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticOutputBudgetExhausted})
	}
	if hasSelectedConflict(bounded, relations) {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticSelectedMemoryConflict})
	}
	for !fitsOutputBudget(Result{Memories: bounded, Diagnostics: diagnostics, ResultLimitOmissions: resultLimitOmissions, BudgetOmissions: budgetOmissions}, g.outputBudget) && len(bounded) > 0 {
		bounded = bounded[:len(bounded)-1]
		budgetOmissions++
		if !hasDiagnostic(diagnostics, DiagnosticOutputBudgetExhausted) {
			diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticOutputBudgetExhausted})
		}
		diagnostics = withoutDiagnostic(diagnostics, DiagnosticSelectedMemoryConflict)
		if hasSelectedConflict(bounded, relations) {
			diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticSelectedMemoryConflict})
		}
	}

	return Result{Memories: bounded, Diagnostics: diagnostics, ResultLimitOmissions: resultLimitOmissions, BudgetOmissions: budgetOmissions}, nil
}

type weightedSignal struct {
	kind   SignalType
	terms  []string
	weight int
}

func buildSignals(input Input) ([]weightedSignal, []Diagnostic) {
	var diagnostics []Diagnostic
	repository := input.Repository
	hasRepositoryInput := repositoryHasInput(repository)
	if hasRepositoryInput && input.RepositoryProject == "" {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticRepositoryProjectUnresolved})
		repository = RepositorySignals{}
	} else if input.RepositoryProject != "" && !strings.EqualFold(input.RepositoryProject, input.Project) {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticRepositoryProjectMismatch})
		repository = RepositorySignals{}
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
	var repositoryTruncations []InputTruncation
	for _, raw := range rawSignals {
		terms, omitted := normalizeTermsWithCount(raw.raw, raw.limit)
		if omitted > 0 {
			truncation := InputTruncation{Signal: raw.kind, OmittedTerms: omitted}
			if raw.kind == SignalTaskIntent {
				diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticTaskInputTruncated, Truncations: []InputTruncation{truncation}})
			} else {
				repositoryTruncations = append(repositoryTruncations, truncation)
			}
		}
		if len(terms) == 0 {
			continue
		}
		signals = append(signals, weightedSignal{kind: raw.kind, terms: terms, weight: raw.weight})
	}
	if len(repositoryTruncations) > 0 {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticRepositoryInputTruncated, Truncations: repositoryTruncations})
	}
	return signals, diagnostics
}

var tokenSeparator = regexp.MustCompile(`[^\pL\pN]+`)

func normalizeTerms(raw string, limit int) []string {
	terms, _ := normalizeTermsWithCount(raw, limit)
	return terms
}

func normalizeTermsWithCount(raw string, limit int) ([]string, int) {
	normalized := tokenSeparator.ReplaceAllString(strings.ToLower(raw), " ")
	seen := make(map[string]struct{})
	capacity := limit
	if capacity > len(normalized) {
		capacity = len(normalized)
	}
	terms := make([]string, 0, capacity)
	omitted := 0
	for _, term := range strings.Fields(normalized) {
		if shouldIgnoreTerm(term) {
			continue
		}
		if _, exists := seen[term]; exists {
			continue
		}
		seen[term] = struct{}{}
		if len(terms) < limit {
			terms = append(terms, term)
		} else {
			omitted++
		}
	}
	return terms, omitted
}

func matchEvidence(memory store.Observation, signal SignalType, terms []string) SelectionEvidence {
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

	matchedTerms := make([]string, 0, len(terms))
	matchedFields := make(map[string]struct{})
	for _, term := range terms {
		matched := false
		for _, field := range fields {
			if containsTerm(field.value, term) {
				matchedFields[field.name] = struct{}{}
				matched = true
			}
		}
		if matched {
			matchedTerms = append(matchedTerms, term)
		}
	}
	fieldNames := make([]string, 0, len(matchedFields))
	for field := range matchedFields {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)
	return SelectionEvidence{Signal: signal, MatchedTerms: matchedTerms, MatchedFields: fieldNames}
}

func containsTerm(value, term string) bool {
	for _, candidate := range normalizeTerms(value, int(^uint(0)>>1)) {
		if candidate == term {
			return true
		}
	}
	return false
}

func shouldIgnoreTerm(term string) bool {
	if len([]rune(term)) < 2 {
		return true
	}
	for _, r := range term {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func repositoryHasInput(repository RepositorySignals) bool {
	return repository.Branch != "" || repository.BranchDiff != "" || repository.StagedDiff != "" ||
		repository.UnstagedDiff != "" || len(repository.AffectedPaths) > 0 ||
		len(repository.CommitSubjects) > 0 || len(repository.UntrackedPaths) > 0 ||
		repository.BaseUnresolved || len(repository.GitFailures) > 0
}

func appendCopy(memories []SelectedMemory, memory SelectedMemory) []SelectedMemory {
	result := make([]SelectedMemory, len(memories), len(memories)+1)
	copy(result, memories)
	return append(result, memory)
}

func fitsOutputBudget(result Result, budget int) bool {
	encoded, err := json.Marshal(result)
	return err == nil && len(encoded) <= budget
}

func hasDiagnostic(diagnostics []Diagnostic, code DiagnosticCode) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func withoutDiagnostic(diagnostics []Diagnostic, code DiagnosticCode) []Diagnostic {
	filtered := diagnostics[:0]
	for _, diagnostic := range diagnostics {
		if diagnostic.Code != code {
			filtered = append(filtered, diagnostic)
		}
	}
	return filtered
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

func isSuperseded(relations store.ObservationRelations) bool {
	for _, relation := range relations.AsTarget {
		if relation.JudgmentStatus == store.JudgmentStatusJudged && relation.Relation == store.RelationSupersedes {
			return true
		}
	}
	return false
}

func hasSelectedConflict(selected []SelectedMemory, relations map[string]store.ObservationRelations) bool {
	selectedIDs := make(map[string]struct{}, len(selected))
	for _, memory := range selected {
		selectedIDs[memory.Memory.SyncID] = struct{}{}
	}
	for _, memory := range selected {
		for _, relation := range relations[memory.Memory.SyncID].AsSource {
			if relation.JudgmentStatus != store.JudgmentStatusJudged || relation.Relation != store.RelationConflictsWith {
				continue
			}
			if _, found := selectedIDs[relation.TargetID]; found {
				return true
			}
		}
	}
	return false
}
