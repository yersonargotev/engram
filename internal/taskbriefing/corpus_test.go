package taskbriefing

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

type corpus struct {
	Version   int              `json:"version"`
	Defaults  Defaults         `json:"defaults"`
	Scenarios []corpusScenario `json:"scenarios"`
}

type corpusScenario struct {
	Name             string            `json:"name"`
	Purpose          string            `json:"purpose"`
	Input            Input             `json:"input"`
	Memories         []memoryFixture   `json:"memories"`
	Relations        []relationFixture `json:"relations"`
	OversizedSources []oversizedSource `json:"oversized_sources"`
	RepeatGitFailure int               `json:"repeat_git_failure"`
	OutputBudget     int               `json:"output_budget"`
	Repeat           int               `json:"repeat"`
	CloseStore       bool              `json:"close_store"`
	Expected         corpusExpected    `json:"expected"`
}

type oversizedSource struct {
	Signal         SignalType `json:"signal"`
	RetainedMemory string     `json:"retained_memory"`
	TailMemory     string     `json:"tail_memory"`
}

type memoryFixture struct {
	Key           string `json:"key"`
	Title         string `json:"title"`
	Content       string `json:"content"`
	ContentRepeat int    `json:"content_repeat"`
	Project       string `json:"project"`
	Scope         string `json:"scope"`
	TopicKey      string `json:"topic_key"`
	Pinned        bool   `json:"pinned"`
	Deleted       bool   `json:"deleted"`
}

type relationFixture struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	Relation string `json:"relation"`
}

type memoryExpectation struct {
	Key     string       `json:"key"`
	Because string       `json:"because"`
	Signals []SignalType `json:"signals"`
}

type corpusExpected struct {
	MustAppear      []memoryExpectation `json:"must_appear"`
	MustNotAppear   []memoryExpectation `json:"must_not_appear"`
	Order           []string            `json:"order"`
	Diagnostics     []DiagnosticCode    `json:"diagnostics"`
	ErrorCode       GenerateErrorCode   `json:"error_code"`
	BudgetOmissions int                 `json:"budget_omissions"`
	Truncations     map[SignalType]int  `json:"truncations"`
}

func TestScenarioCorpus(t *testing.T) {
	data, err := os.ReadFile("prototype/testdata/v1/scenarios.json")
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	var suite corpus
	if err := json.Unmarshal(data, &suite); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	if suite.Version != 1 {
		t.Fatalf("corpus version = %d, want 1", suite.Version)
	}
	if !reflect.DeepEqual(suite.Defaults, CalibratedDefaults) {
		t.Fatalf("corpus defaults = %#v, implementation defaults = %#v", suite.Defaults, CalibratedDefaults)
	}

	for _, scenario := range suite.Scenarios {
		scenario := scenario
		t.Run(scenario.Name, func(t *testing.T) {
			runCorpusScenario(t, scenario)
		})
	}
}

func runCorpusScenario(t *testing.T, scenario corpusScenario) {
	t.Helper()
	if scenario.Expected.MustAppear == nil || scenario.Expected.MustNotAppear == nil {
		t.Fatal("every scenario must declare must_appear and must_not_appear")
	}
	input := scenario.Input
	for index := 0; index < scenario.RepeatGitFailure; index++ {
		input.Repository.GitFailures = append(input.Repository.GitFailures, SignalUnstagedDiff)
	}
	memories := append([]memoryFixture(nil), scenario.Memories...)
	applyOversizedSources(t, &input, memories, scenario.OversizedSources)
	memoryStore := newTestStore(t)
	fixtures := seedCorpusMemories(t, memoryStore, memories)
	seedCorpusRelations(t, memoryStore, fixtures, scenario.Relations)
	for _, fixture := range memories {
		if fixture.Deleted {
			if err := memoryStore.DeleteObservation(fixtures[fixture.Key].ID, false); err != nil {
				t.Fatalf("delete %s: %v", fixture.Key, err)
			}
		}
	}
	before, err := memoryStore.Stats()
	if err != nil {
		t.Fatalf("stats before: %v", err)
	}
	if scenario.CloseStore {
		if err := memoryStore.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}

	repeat := scenario.Repeat
	if repeat < 2 {
		repeat = 2
	}
	var baseline Result
	for run := 0; run < repeat; run++ {
		generator := New(memoryStore)
		if scenario.OutputBudget > 0 {
			generator.outputBudget = scenario.OutputBudget
		}
		result, generateErr := generator.Generate(input)
		if scenario.Expected.ErrorCode != "" {
			if generateErr == nil || ErrorCode(generateErr) != scenario.Expected.ErrorCode {
				t.Fatalf("Generate error = %v, want %s", generateErr, scenario.Expected.ErrorCode)
			}
			if scenario.Expected.ErrorCode == ErrorMemoryStoreFailure && !errors.Is(generateErr, ErrMemoryStore) {
				t.Fatalf("Generate error = %v, want ErrMemoryStore", generateErr)
			}
			continue
		}
		if generateErr != nil {
			t.Fatalf("Generate: %v", generateErr)
		}
		if run > 0 && !reflect.DeepEqual(baseline, result) {
			t.Fatalf("run %d differs\nfirst: %#v\nagain: %#v", run+1, baseline, result)
		}
		baseline = result
	}
	if scenario.Expected.ErrorCode != "" {
		return
	}

	assertCorpusExpectations(t, scenario, baseline, fixtures)
	after, err := memoryStore.Stats()
	if err != nil {
		t.Fatalf("stats after: %v", err)
	}
	if before.TotalObservations != after.TotalObservations {
		t.Fatalf("Generate persisted transient input: observations before=%d after=%d", before.TotalObservations, after.TotalObservations)
	}
}

func seedCorpusMemories(t *testing.T, memoryStore *store.Store, memories []memoryFixture) map[string]store.Observation {
	t.Helper()
	fixtures := make(map[string]store.Observation, len(memories))
	for index, fixture := range memories {
		sessionID := fmt.Sprintf("corpus-%02d-%s", index, fixture.Key)
		if err := memoryStore.CreateSession(sessionID, fixture.Project, "/tmp/"+fixture.Project); err != nil {
			t.Fatalf("CreateSession(%s): %v", fixture.Key, err)
		}
		content := fixture.Content
		if fixture.ContentRepeat > 0 {
			content = strings.Repeat(content, fixture.ContentRepeat)
		}
		id, err := memoryStore.AddObservation(store.AddObservationParams{
			SessionID: sessionID,
			Type:      "decision",
			Title:     fixture.Title,
			Content:   content,
			Project:   fixture.Project,
			Scope:     fixture.Scope,
			TopicKey:  fixture.TopicKey,
		})
		if err != nil {
			t.Fatalf("AddObservation(%s): %v", fixture.Key, err)
		}
		if fixture.Pinned {
			if err := memoryStore.PinObservation(id); err != nil {
				t.Fatalf("PinObservation(%s): %v", fixture.Key, err)
			}
		}
		observation, err := memoryStore.GetObservation(id)
		if err != nil {
			t.Fatalf("GetObservation(%s): %v", fixture.Key, err)
		}
		fixtures[fixture.Key] = *observation
	}
	return fixtures
}

func seedCorpusRelations(t *testing.T, memoryStore *store.Store, memories map[string]store.Observation, relations []relationFixture) {
	t.Helper()
	for index, fixture := range relations {
		judgmentID := fmt.Sprintf("rel-corpus-%04d", index)
		if _, err := memoryStore.SaveRelation(store.SaveRelationParams{
			SyncID:   judgmentID,
			SourceID: memories[fixture.Source].SyncID,
			TargetID: memories[fixture.Target].SyncID,
		}); err != nil {
			t.Fatalf("SaveRelation(%s): %v", judgmentID, err)
		}
		if _, err := memoryStore.JudgeRelation(store.JudgeRelationParams{
			JudgmentID:    judgmentID,
			Relation:      fixture.Relation,
			MarkedByActor: "corpus",
			MarkedByKind:  "system",
		}); err != nil {
			t.Fatalf("JudgeRelation(%s): %v", judgmentID, err)
		}
	}
}

func assertCorpusExpectations(t *testing.T, scenario corpusScenario, result Result, fixtures map[string]store.Observation) {
	t.Helper()
	selected := make(map[int64]SelectedMemory, len(result.Memories))
	for _, memory := range result.Memories {
		selected[memory.Memory.ID] = memory
	}
	for _, expectation := range scenario.Expected.MustAppear {
		memory, found := selected[fixtures[expectation.Key].ID]
		if !found {
			t.Errorf("%s must appear: %s", expectation.Key, expectation.Because)
			continue
		}
		actualSignals := make([]SignalType, 0, len(memory.Evidence))
		for _, evidence := range memory.Evidence {
			actualSignals = append(actualSignals, evidence.Signal)
		}
		for _, signal := range expectation.Signals {
			if !containsSignal(actualSignals, signal) {
				t.Errorf("%s evidence signals = %v, want %s: %s", expectation.Key, actualSignals, signal, expectation.Because)
			}
		}
		t.Logf("included %s: %s", expectation.Key, expectation.Because)
	}
	for _, expectation := range scenario.Expected.MustNotAppear {
		if _, found := selected[fixtures[expectation.Key].ID]; found {
			t.Errorf("%s must not appear: %s", expectation.Key, expectation.Because)
		}
		t.Logf("excluded %s: %s", expectation.Key, expectation.Because)
	}

	if len(scenario.Expected.Order) > 0 {
		actual := make([]string, 0, len(result.Memories))
		byID := make(map[int64]string, len(fixtures))
		for key, fixture := range fixtures {
			byID[fixture.ID] = key
		}
		for _, memory := range result.Memories {
			actual = append(actual, byID[memory.Memory.ID])
		}
		if !reflect.DeepEqual(actual, scenario.Expected.Order) {
			t.Errorf("selection order = %v, want %v", actual, scenario.Expected.Order)
		}
	}
	actualDiagnostics := make([]DiagnosticCode, 0, len(result.Diagnostics))
	for _, diagnostic := range result.Diagnostics {
		actualDiagnostics = append(actualDiagnostics, diagnostic.Code)
	}
	sort.Slice(actualDiagnostics, func(i, j int) bool { return actualDiagnostics[i] < actualDiagnostics[j] })
	wantDiagnostics := append([]DiagnosticCode(nil), scenario.Expected.Diagnostics...)
	sort.Slice(wantDiagnostics, func(i, j int) bool { return wantDiagnostics[i] < wantDiagnostics[j] })
	if !equalDiagnosticCodes(actualDiagnostics, wantDiagnostics) {
		t.Errorf("diagnostics = %v, want %v", actualDiagnostics, wantDiagnostics)
	}
	if result.BudgetOmissions != scenario.Expected.BudgetOmissions {
		t.Errorf("budget omissions = %d, want %d", result.BudgetOmissions, scenario.Expected.BudgetOmissions)
	}
	actualTruncations := make(map[SignalType]int)
	for _, diagnostic := range result.Diagnostics {
		for _, truncation := range diagnostic.Truncations {
			actualTruncations[truncation.Signal] = truncation.OmittedTerms
		}
	}
	if !equalTruncations(actualTruncations, scenario.Expected.Truncations) {
		t.Errorf("truncations = %v, want %v", actualTruncations, scenario.Expected.Truncations)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if len(encoded) > CalibratedDefaults.TotalOutputBudget {
		t.Errorf("result uses %d bytes, exceeds total output budget %d", len(encoded), CalibratedDefaults.TotalOutputBudget)
	}
}

func containsSignal(signals []SignalType, target SignalType) bool {
	for _, signal := range signals {
		if signal == target {
			return true
		}
	}
	return false
}

func applyOversizedSources(t *testing.T, input *Input, memories []memoryFixture, sources []oversizedSource) {
	t.Helper()
	for _, source := range sources {
		limit := sourceTermLimit(t, source.Signal)
		slug := strings.ReplaceAll(string(source.Signal), "_", "")
		prefix := "x" + slug + "a x" + slug + "b"
		retainedTerms := normalizeTerms(prefix, limit)
		for index := 0; len(retainedTerms) < limit; index++ {
			retainedTerms = append(retainedTerms, fmt.Sprintf("x%sp%02d", slug, index))
		}
		tail := "xtail" + slug
		raw := strings.Join(append(append([]string(nil), retainedTerms...), tail), " ")
		setSignalInput(t, input, source.Signal, raw)
		matchedTermCount := 2
		if source.Signal == SignalTaskIntent {
			matchedTermCount = (limit + 1) / 2
		}
		appendFixtureContent(t, memories, source.RetainedMemory, strings.Join(retainedTerms[:matchedTermCount], " "))
		appendFixtureContent(t, memories, source.TailMemory, tail)
	}
}

type oversizedSignalSpec struct {
	limit func() int
	set   func(*Input, string)
}

var oversizedSignalSpecs = map[SignalType]oversizedSignalSpec{
	SignalTaskIntent:    {limit: func() int { return CalibratedDefaults.TaskTermLimit }, set: func(input *Input, raw string) { input.TaskIntent = raw }},
	SignalBranch:        {limit: func() int { return CalibratedDefaults.BranchTermLimit }, set: func(input *Input, raw string) { input.Repository.Branch = raw }},
	SignalBranchDiff:    {limit: func() int { return CalibratedDefaults.DiffTermLimit }, set: func(input *Input, raw string) { input.Repository.BranchDiff = raw }},
	SignalStagedDiff:    {limit: func() int { return CalibratedDefaults.DiffTermLimit }, set: func(input *Input, raw string) { input.Repository.StagedDiff = raw }},
	SignalUnstagedDiff:  {limit: func() int { return CalibratedDefaults.DiffTermLimit }, set: func(input *Input, raw string) { input.Repository.UnstagedDiff = raw }},
	SignalAffectedPath:  {limit: func() int { return CalibratedDefaults.PathTermLimit }, set: func(input *Input, raw string) { input.Repository.AffectedPaths = []string{raw} }},
	SignalCommitSubject: {limit: func() int { return CalibratedDefaults.CommitTermLimit }, set: func(input *Input, raw string) { input.Repository.CommitSubjects = []string{raw} }},
	SignalUntrackedPath: {limit: func() int { return CalibratedDefaults.UntrackedTermLimit }, set: func(input *Input, raw string) { input.Repository.UntrackedPaths = []string{raw} }},
}

func sourceTermLimit(t *testing.T, signal SignalType) int {
	t.Helper()
	spec, found := oversizedSignalSpecs[signal]
	if !found {
		t.Fatalf("unsupported oversized source %q", signal)
	}
	return spec.limit()
}

func setSignalInput(t *testing.T, input *Input, signal SignalType, raw string) {
	t.Helper()
	spec, found := oversizedSignalSpecs[signal]
	if !found {
		t.Fatalf("unsupported signal input %q", signal)
	}
	spec.set(input, raw)
}

func appendFixtureContent(t *testing.T, memories []memoryFixture, key, content string) {
	t.Helper()
	for index := range memories {
		if memories[index].Key == key {
			memories[index].Content += " " + content
			return
		}
	}
	t.Fatalf("memory fixture %q not found", key)
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.DataDir = t.TempDir()
	memoryStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = memoryStore.Close() })
	return memoryStore
}

func equalDiagnosticCodes(left, right []DiagnosticCode) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalTruncations(left, right map[SignalType]int) bool {
	if len(left) != len(right) {
		return false
	}
	for signal, omitted := range left {
		if right[signal] != omitted {
			return false
		}
	}
	return true
}
