package activationstudy

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
)

type EventSet struct {
	SchemaVersion  string             `json:"schema_version"`
	StudyID        string             `json:"study_id"`
	StudyVersion   string             `json:"study_version"`
	ContractSHA256 string             `json:"contract_sha256"`
	Verification   VerificationReport `json:"verification"`
	Records        []RunRecord        `json:"records"`
}

type Report struct {
	SchemaVersion      string             `json:"schema_version"`
	StudyID            string             `json:"study_id"`
	StudyVersion       string             `json:"study_version"`
	ContractSHA256     string             `json:"contract_sha256"`
	SampleSize         SampleSize         `json:"sample_size"`
	Rates              []RateMetric       `json:"rates"`
	PairedDifferences  []PairedDifference `json:"paired_differences"`
	Omissions          []CodeCount        `json:"omissions"`
	ProtocolDeviations []CodeCount        `json:"protocol_deviations"`
	Questions          StudyQuestions     `json:"study_questions"`
}

type SampleSize struct {
	Planned             int `json:"planned"`
	Retained            int `json:"retained"`
	IntegrationFailures int `json:"integration_failures"`
}

type RateMetric struct {
	Scope     string  `json:"scope"`
	Value     string  `json:"value"`
	Treatment string  `json:"treatment"`
	Event     string  `json:"event"`
	N         int     `json:"n"`
	Count     int     `json:"count"`
	Omitted   int     `json:"omitted"`
	Rate      float64 `json:"rate"`
	Lower     float64 `json:"lower_95"`
	Upper     float64 `json:"upper_95"`
}

type PairedDifference struct {
	Scope           string  `json:"scope"`
	Value           string  `json:"value"`
	FirstTreatment  string  `json:"first_treatment"`
	SecondTreatment string  `json:"second_treatment"`
	Event           string  `json:"event"`
	N               int     `json:"n"`
	Omitted         int     `json:"omitted"`
	Difference      float64 `json:"difference"`
	Lower           float64 `json:"lower_95"`
	Upper           float64 `json:"upper_95"`
}

type CodeCount struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

type StudyQuestions struct {
	RepositoryGuidance  string `json:"repository_guidance"`
	OverlappingSkills   string `json:"overlapping_skills"`
	CLIFollowsSelection string `json:"cli_follows_selection"`
	UsefulOutcomes      string `json:"useful_outcomes"`
}

type analysisScope struct {
	name  string
	value string
	match func(RunRecord) bool
}

// Analyze validates one complete bounded event set and produces deterministic aggregates.
func (study *Study) Analyze(events EventSet) (Report, error) {
	if events.SchemaVersion != "codex-activation-event-set-v1" || events.StudyID != study.Contract.StudyID ||
		events.StudyVersion != study.Contract.StudyVersion || events.ContractSHA256 != study.Hash {
		return Report{}, fmt.Errorf("activation event set does not match the frozen contract")
	}
	if err := study.validateVerification(events.Verification); err != nil {
		return Report{}, err
	}
	plan := study.Plan()
	records, err := validateEventRecords(plan, events.Records, study.Contract.Codex.AvailableSkills)
	if err != nil {
		return Report{}, err
	}
	report := Report{
		SchemaVersion: "codex-activation-report-v1", StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash,
		SampleSize: SampleSize{Planned: len(plan), Retained: len(records)},
	}
	for _, record := range records {
		if record.Events["integration_failure"] {
			report.SampleSize.IntegrationFailures++
		}
	}

	scopes := []analysisScope{{name: "overall", value: "all", match: func(RunRecord) bool { return true }}}
	for _, prompt := range study.Contract.Prompts {
		class := prompt.Class
		scopes = append(scopes, analysisScope{name: "prompt_class", value: class, match: func(record RunRecord) bool { return record.PromptClass == class }})
	}
	for _, shape := range study.Contract.SessionShapes {
		shape := shape
		scopes = append(scopes, analysisScope{name: "session_shape", value: shape, match: func(record RunRecord) bool { return record.SessionShape == shape }})
	}

	for _, scope := range scopes {
		for _, treatment := range study.Contract.Treatments {
			for _, event := range study.Contract.Events {
				report.Rates = append(report.Rates, calculateRate(records, scope, treatment.ID, event))
			}
		}
		for _, comparison := range [][2]string{{"engram-normal", "engram-ablated"}, {"engram-normal", "neutral"}, {"engram-ablated", "neutral"}} {
			for _, event := range study.Contract.Events {
				report.PairedDifferences = append(report.PairedDifferences,
					study.calculatePairedDifference(records, scope, comparison[0], comparison[1], event))
			}
		}
	}
	sort.Slice(report.Rates, func(i, j int) bool { return rateKey(report.Rates[i]) < rateKey(report.Rates[j]) })
	sort.Slice(report.PairedDifferences, func(i, j int) bool {
		return differenceKey(report.PairedDifferences[i]) < differenceKey(report.PairedDifferences[j])
	})
	report.Omissions = countCodes(records, func(record RunRecord) []string { return record.Omissions })
	report.ProtocolDeviations = countCodes(records, func(record RunRecord) []string { return record.Deviations })
	report.Questions = answerStudyQuestions(records, report)
	return report, nil
}

func (study *Study) validateVerification(verification VerificationReport) error {
	contractSkill := SkillIdentity{
		Name: study.Contract.UserSkill.Name, Revision: study.Contract.UserSkill.Revision,
		TreeSHA256: study.Contract.UserSkill.TreeSHA256,
	}
	if verification.ContractSHA256 != study.Hash || verification.SourceRevision != study.Contract.SourceRevision || verification.UserSkill != contractSkill {
		return fmt.Errorf("activation event set verification identity does not match the frozen contract")
	}
	if !verification.CodexPromptInputVerified || !verification.CleanupVerified ||
		strings.Join(verification.CodexSkillInventory, "\x00") != strings.Join(study.Contract.Codex.AvailableSkills, "\x00") {
		return fmt.Errorf("activation event set runtime verification is incomplete")
	}
	wantFixtures := map[string]bool{"engram-normal": true, "engram-ablated": true, "neutral": true}
	seenFixtures := make(map[string]bool, len(verification.Fixtures))
	for _, fixture := range verification.Fixtures {
		if !wantFixtures[fixture.ID] || seenFixtures[fixture.ID] || !validHexDigest(fixture.ManifestSHA256, sha256.Size) ||
			fixture.PluginEnabled || fixture.MCPEnabled || fixture.PromptHooksEnabled || fixture.StopVerifierEnabled {
			return fmt.Errorf("activation event set contains an invalid fixture verification")
		}
		seenFixtures[fixture.ID] = true
	}
	if len(seenFixtures) != len(wantFixtures) || verification.Ablation.RemovedGuidanceRows != 2 ||
		len(verification.Ablation.ChangedFiles) != 1 || verification.Ablation.ChangedFiles[0] != "AGENTS.md" {
		return fmt.Errorf("activation event set treatment verification is incomplete")
	}
	return nil
}

func validateEventRecords(plan []PlannedRun, input []RunRecord, availableSkills []string) ([]RunRecord, error) {
	if len(input) != len(plan) {
		return nil, fmt.Errorf("activation event set has %d cells, want %d", len(input), len(plan))
	}
	return validatePartialEventRecords(plan, input, availableSkills)
}

func validatePartialEventRecords(plan []PlannedRun, input []RunRecord, availableSkills []string) ([]RunRecord, error) {
	expected := make(map[string]PlannedRun, len(plan))
	for _, run := range plan {
		expected[run.CellID] = run
	}
	seen := make(map[string]bool, len(input))
	records := make([]RunRecord, len(input))
	copy(records, input)
	for _, record := range records {
		run, ok := expected[record.CellID]
		if !ok || seen[record.CellID] {
			return nil, fmt.Errorf("activation event set contains unknown or duplicate cell %q", record.CellID)
		}
		seen[record.CellID] = true
		if record.SchemaVersion != "codex-activation-events-v1" || record.PromptID != run.PromptID ||
			record.PromptClass != run.PromptClass || record.Treatment != run.Treatment ||
			record.Repetition != run.Repetition || record.SessionShape != run.SessionShape || record.Sequence != run.Sequence {
			return nil, fmt.Errorf("activation event cell %q does not match its plan", record.CellID)
		}
		if len(record.Events) != len(requiredActivationEvents()) {
			return nil, fmt.Errorf("activation event cell %q has an incomplete event inventory", record.CellID)
		}
		for _, event := range requiredActivationEvents() {
			if _, ok := record.Events[event]; !ok {
				return nil, fmt.Errorf("activation event cell %q is missing %s", record.CellID, event)
			}
		}
		if strings.Join(record.AvailableSkills, "\x00") != strings.Join(availableSkills, "\x00") {
			return nil, fmt.Errorf("activation event cell %q has an unexpected skill inventory", record.CellID)
		}
		if err := validateBoundedRecord(record); err != nil {
			return nil, fmt.Errorf("activation event cell %q: %w", record.CellID, err)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].CellID < records[j].CellID })
	return records, nil
}

func validateBoundedRecord(record RunRecord) error {
	reads := sortedUnique(record.SkillReads)
	if strings.Join(reads, "\x00") != strings.Join(record.SkillReads, "\x00") {
		return fmt.Errorf("skill reads are not canonical")
	}
	allowedReads := map[string]bool{
		"user:engram-memory-cli": true, "project:engram-memory-protocol": true, "project:engram-memory-cli": true,
	}
	for _, read := range reads {
		if !allowedReads[read] {
			return fmt.Errorf("skill reads contain an unbounded value")
		}
	}
	for _, operation := range record.Operations {
		if operation.Operation != "other" && canonicalOperation(operation.Operation) == "" {
			return fmt.Errorf("operations contain an unbounded value")
		}
	}
	allowedOmissions := map[string]bool{
		"codex_process_failed": true, "codex_event_limit_exceeded": true,
		"final_message_unavailable": true, "shim_event_parse_failed": true,
		"codex_event_parse_failed": true, "preservation_verification_failed": true,
	}
	for _, omission := range record.Omissions {
		if !allowedOmissions[omission] {
			return fmt.Errorf("omissions contain an unbounded value")
		}
	}
	for _, deviation := range record.Deviations {
		if deviation != "fixture_mutated" {
			return fmt.Errorf("protocol deviations contain an unbounded value")
		}
	}
	if record.Events["engram_not_invoked"] && record.Events["integration_failure"] {
		return fmt.Errorf("non-invocation and integration failure are conflated")
	}
	if record.Events["memory_write_succeeded"] && !record.Events["memory_write_attempted"] ||
		record.Events["checkpoint_succeeded"] && !record.Events["checkpoint_attempted"] ||
		record.Events["useful_preservation"] && !record.Events["memory_write_succeeded"] ||
		record.Events["useful_recall"] && !record.Events["task_brief_invoked"] && !record.Events["targeted_search_invoked"] {
		return fmt.Errorf("event implications are inconsistent")
	}
	return nil
}

func calculateRate(records []RunRecord, scope analysisScope, treatment, event string) RateMetric {
	metric := RateMetric{Scope: scope.name, Value: scope.value, Treatment: treatment, Event: event}
	for _, record := range records {
		if record.Treatment != treatment || !scope.match(record) {
			continue
		}
		if recordOmittedForEvent(record, event) {
			metric.Omitted++
			continue
		}
		metric.N++
		if record.Events[event] {
			metric.Count++
		}
	}
	metric.Rate, metric.Lower, metric.Upper = wilson95(metric.Count, metric.N)
	return metric
}

func wilson95(successes, total int) (float64, float64, float64) {
	if total == 0 {
		return 0, 0, 0
	}
	const z = 1.959963984540054
	n := float64(total)
	p := float64(successes) / n
	z2 := z * z
	denominator := 1 + z2/n
	center := (p + z2/(2*n)) / denominator
	margin := z * math.Sqrt((p*(1-p)+z2/(4*n))/n) / denominator
	return p, math.Max(0, center-margin), math.Min(1, center+margin)
}

func (study *Study) calculatePairedDifference(records []RunRecord, scope analysisScope, first, second, event string) PairedDifference {
	metric := PairedDifference{
		Scope: scope.name, Value: scope.value, FirstTreatment: first, SecondTreatment: second, Event: event,
	}
	byPair := make(map[string]map[string]RunRecord)
	for _, record := range records {
		if !scope.match(record) || (record.Treatment != first && record.Treatment != second) {
			continue
		}
		pair := fmt.Sprintf("%s:%d", record.PromptID, record.Repetition)
		if byPair[pair] == nil {
			byPair[pair] = make(map[string]RunRecord)
		}
		byPair[pair][record.Treatment] = record
	}
	pairIDs := make([]string, 0, len(byPair))
	for pair := range byPair {
		pairIDs = append(pairIDs, pair)
	}
	sort.Strings(pairIDs)
	differences := make([]float64, 0, len(pairIDs))
	for _, pair := range pairIDs {
		firstRecord, firstOK := byPair[pair][first]
		secondRecord, secondOK := byPair[pair][second]
		if !firstOK || !secondOK || recordOmittedForEvent(firstRecord, event) || recordOmittedForEvent(secondRecord, event) {
			metric.Omitted++
			continue
		}
		difference := boolFloat(firstRecord.Events[event]) - boolFloat(secondRecord.Events[event])
		differences = append(differences, difference)
	}
	metric.N = len(differences)
	if metric.N == 0 {
		return metric
	}
	metric.Difference = mean(differences)
	seed := study.Contract.Metrics.BootstrapSeed + "\x00" + differenceKey(metric)
	metric.Lower, metric.Upper = bootstrap95(differences, study.Contract.Metrics.BootstrapResamples, seed)
	metric.Lower = math.Min(metric.Lower, metric.Difference)
	metric.Upper = math.Max(metric.Upper, metric.Difference)
	return metric
}

func recordOmittedForEvent(record RunRecord, event string) bool {
	if event == "integration_failure" {
		return false
	}
	if record.Events["integration_failure"] {
		return true
	}
	if event == "useful_recall" && containsString(record.Omissions, "final_message_unavailable") {
		return true
	}
	return event == "useful_preservation" && containsString(record.Omissions, "preservation_verification_failed")
}

func bootstrap95(values []float64, resamples int, seed string) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	digest := sha256.Sum256([]byte(seed))
	random := xorshift64{state: binary.LittleEndian.Uint64(digest[:8])}
	if random.state == 0 {
		random.state = 0x9e3779b97f4a7c15
	}
	means := make([]float64, resamples)
	for sample := 0; sample < resamples; sample++ {
		total := 0.0
		for range values {
			total += values[random.next()%uint64(len(values))]
		}
		means[sample] = total / float64(len(values))
	}
	sort.Float64s(means)
	lowerIndex := int(math.Floor(0.025 * float64(resamples-1)))
	upperIndex := int(math.Ceil(0.975 * float64(resamples-1)))
	return means[lowerIndex], means[upperIndex]
}

type xorshift64 struct{ state uint64 }

func (random *xorshift64) next() uint64 {
	value := random.state
	value ^= value << 13
	value ^= value >> 7
	value ^= value << 17
	random.state = value
	return value
}

func answerStudyQuestions(records []RunRecord, report Report) StudyQuestions {
	normal := lookupRate(report.Rates, "overall", "all", "engram-normal", "engram_not_invoked")
	ablated := lookupRate(report.Rates, "overall", "all", "engram-ablated", "engram_not_invoked")
	guidanceDifference := (1 - normal.Rate) - (1 - ablated.Rate)
	overlap := lookupRate(report.Rates, "overall", "all", "engram-normal", "overlapping_memory_skills_read")

	selected, followed := 0, 0
	for _, record := range records {
		if record.Events["integration_failure"] || len(record.SkillReads) == 0 {
			continue
		}
		selected++
		if len(record.Operations) > 0 {
			followed++
		}
	}
	recall := eventCount(records, "explicit_recall", "useful_recall")
	preserve := eventCount(records, "explicit_preservation", "useful_preservation")
	return StudyQuestions{
		RepositoryGuidance:  fmt.Sprintf("Normal Engram guidance changed observed activation by %.1f percentage points versus the matched ablation; uncertainty is reported in paired_differences.", guidanceDifference*100),
		OverlappingSkills:   fmt.Sprintf("Overlapping Memory skill reads occurred in %d of %d eligible normal-Engram runs (%.1f%%).", overlap.Count, overlap.N, overlap.Rate*100),
		CLIFollowsSelection: fmt.Sprintf("Engram CLI invocation followed a Memory skill read in %d of %d eligible runs.", followed, selected),
		UsefulOutcomes:      fmt.Sprintf("Useful recall was observed in %d explicit-recall runs and verified preservation in %d explicit-preservation runs.", recall, preserve),
	}
}

func lookupRate(metrics []RateMetric, scope, value, treatment, event string) RateMetric {
	for _, metric := range metrics {
		if metric.Scope == scope && metric.Value == value && metric.Treatment == treatment && metric.Event == event {
			return metric
		}
	}
	return RateMetric{}
}

func eventCount(records []RunRecord, class, event string) int {
	count := 0
	for _, record := range records {
		if record.PromptClass == class && !record.Events["integration_failure"] && record.Events[event] {
			count++
		}
	}
	return count
}

func countCodes(records []RunRecord, selectCodes func(RunRecord) []string) []CodeCount {
	counts := make(map[string]int)
	for _, record := range records {
		for _, code := range selectCodes(record) {
			counts[code]++
		}
	}
	result := make([]CodeCount, 0, len(counts))
	for code, count := range counts {
		result = append(result, CodeCount{Code: code, Count: count})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Code < result[j].Code })
	return result
}

func mean(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func rateKey(metric RateMetric) string {
	return strings.Join([]string{metric.Scope, metric.Value, metric.Treatment, metric.Event}, "\x00")
}

func differenceKey(metric PairedDifference) string {
	return strings.Join([]string{metric.Scope, metric.Value, metric.FirstTreatment, metric.SecondTreatment, metric.Event}, "\x00")
}
