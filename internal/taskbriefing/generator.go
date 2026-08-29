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
)

var (
	issueIdentifierPattern  = regexp.MustCompile(`(?i)(?:#|\bissue[\s:#-]*)([0-9]+)`)
	prIdentifierPattern     = regexp.MustCompile(`(?i)\b(?:pr|pull[\s-]*request)[\s:#-]*([0-9]+)`)
	branchIdentifierPattern = regexp.MustCompile(`(?i)\bbranch(?:[\s_-]+name)?[\s:=]+([a-z0-9][a-z0-9._/-]*)`)
	commitIdentifierPattern = regexp.MustCompile(`(?i)\bcommit[\s:#-]*([0-9a-f]{7,40})\b`)
	pathIdentifierPattern   = regexp.MustCompile(`(?i)\bpath[\s:=]+([a-z0-9._-]+(?:/[a-z0-9._-]+)+)`)
	topicIdentifierPattern  = regexp.MustCompile(`(?i)\btopic(?:[\s_-]+key)?[\s:=]+([a-z0-9._-]+(?:/[a-z0-9._-]+)*)`)
	compoundTokenPattern    = regexp.MustCompile(`(?i)[a-z0-9._-]+(?:/[a-z0-9._-]+)+`)
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
	Memories             []SelectedMemory `json:"memories"`
	Diagnostics          []Diagnostic     `json:"diagnostics"`
	BaseResolution       *BaseResolution  `json:"base_resolution,omitempty"`
	ResultLimitOmissions int              `json:"result_limit_omissions,omitempty"`
	ConflictPairs        []ConflictPair   `json:"-"`
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
	signals, diagnostics, baseResolution := buildSignals(input)
	if len(signals) == 0 {
		diagnostics = append(diagnostics, Diagnostic{Code: DiagnosticNoUsableSignals})
		return Result{Diagnostics: diagnostics, BaseResolution: baseResolution}, nil
	}
	groups := groupSignals(signals)
	candidates, err := g.retrieveCandidates(input, groups)
	if err != nil {
		return Result{}, err
	}
	relations, err := g.loadCandidateRelations(candidates)
	if err != nil {
		return Result{}, err
	}
	selected := rankCandidates(input, groups, candidates, relations)
	return g.boundSelection(input.Limit, selected, diagnostics, baseResolution, relations), nil
}

func (g *Generator) retrieveCandidates(input Input, groups []signalGroup) (map[int64]store.Observation, error) {
	candidates := make(map[int64]store.Observation)
	for _, group := range groups {
		results, err := g.store.Search(strings.Join(group.terms, " "), store.SearchOptions{
			Project: input.Project, Scope: input.Scope, Limit: candidateRetrievalLimit, MatchMode: "any",
		})
		if err != nil {
			return nil, &generateError{code: ErrorMemoryStoreFailure, err: fmt.Errorf("%w: %v", ErrMemoryStore, err)}
		}
		for _, result := range results {
			candidates[result.ID] = result.Observation
		}
	}
	return candidates, nil
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

func rankCandidates(input Input, groups []signalGroup, candidates map[int64]store.Observation, relations map[string]store.ObservationRelations) []SelectedMemory {
	selected := make([]SelectedMemory, 0, len(candidates))
	for _, memory := range candidates {
		if isSuperseded(relations[memory.SyncID]) {
			continue
		}
		matches := make([]matchedSignalGroup, 0, len(groups))
		hasTaskEvidence := false
		hasStrongRepositoryEvidence := false
		for _, group := range groups {
			evidence := matchEvidence(memory, group)
			isTaskGroup := containsSignalType(group.sources, SignalTaskIntent)
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
			continue
		}
		if input.TaskIntent == "" && !hasStrongRepositoryEvidence {
			continue
		}
		evidenceItems := expandSelectionEvidence(matches)
		baseScore := scoreMatchedGroups(matches, false)
		if baseScore < CalibratedDefaults.InclusionThreshold {
			continue
		}
		score := scoreMatchedGroups(matches, true)
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
	return selected
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
		identifiers := extractExactIdentifiers(boundedTermPrefix(raw.raw, raw.limit), raw.kind, raw.limit)
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
		boundedField := boundedTermPrefix(field.value, candidateFieldTermLimit)
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
		for _, token := range compoundTokenPattern.FindAllString(raw, -1) {
			appendIdentifier("path:" + normalizeIdentifierValue(token))
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
