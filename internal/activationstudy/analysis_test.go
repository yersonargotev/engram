package activationstudy

import (
	"encoding/json"
	"math"
	"slices"
	"strings"
	"testing"
)

func TestAnalyzeProducesDeterministicPairedActivationReport(t *testing.T) {
	t.Parallel()

	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	records := recordsForPlan(study.Plan())
	for index := range records {
		record := &records[index]
		switch record.Treatment {
		case "engram-normal":
			record.Events["user_skill_read"] = true
			record.Events["current_project_invoked"] = true
			record.SkillReads = []string{"user:engram-memory-cli"}
			if record.Repetition == 1 {
				record.Events["project_memory_protocol_read"] = true
				record.Events["overlapping_memory_skills_read"] = true
				record.SkillReads = append(record.SkillReads, "project:engram-memory-protocol")
			}
		case "engram-ablated":
			if record.Repetition == 1 {
				record.Events["user_skill_read"] = true
				record.SkillReads = []string{"user:engram-memory-cli"}
			}
		case "neutral":
			record.Events["engram_not_invoked"] = true
		}
		if record.PromptClass == "explicit_recall" && record.Treatment == "engram-normal" {
			record.Events["task_brief_invoked"] = true
			record.Events["useful_recall"] = true
		}
		slices.Sort(record.SkillReads)
	}
	for index := range records {
		if records[index].CellID == "routine-r02-neutral" {
			records[index].Events["integration_failure"] = true
			records[index].Events["engram_not_invoked"] = false
			records[index].Omissions = []string{"codex_process_failed"}
		}
	}
	for index := range records {
		if records[index].Treatment == "engram-normal" && records[index].PromptClass == "implementation" {
			records[index].Omissions = append(records[index].Omissions, "final_message_unavailable")
			break
		}
	}

	events := EventSet{
		SchemaVersion: "codex-activation-event-set-v1", StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash,
		Verification: validVerification(study), Records: records,
	}
	report, err := study.Analyze(events)
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if report.SampleSize.Planned != 36 || report.SampleSize.Retained != 36 || report.SampleSize.IntegrationFailures != 1 {
		t.Fatalf("sample size = %#v", report.SampleSize)
	}

	normal := findRate(t, report, "overall", "all", "engram-normal", "user_skill_read")
	if normal.N != 12 || normal.Count != 12 || normal.Rate != 1 || normal.Lower < 0 || normal.Upper > 1 {
		t.Fatalf("normal user-skill rate = %#v", normal)
	}
	ablated := findRate(t, report, "overall", "all", "engram-ablated", "user_skill_read")
	if ablated.N != 12 || ablated.Count != 6 || math.Abs(ablated.Rate-0.5) > 1e-12 {
		t.Fatalf("ablated user-skill rate = %#v", ablated)
	}
	difference := findDifference(t, report, "overall", "all", "engram-normal", "engram-ablated", "user_skill_read")
	if difference.N != 12 || math.Abs(difference.Difference-0.5) > 1e-12 || difference.Lower > difference.Difference || difference.Upper < difference.Difference {
		t.Fatalf("paired difference = %#v", difference)
	}
	failedCellRate := findRate(t, report, "overall", "all", "neutral", "user_skill_read")
	if failedCellRate.N != 11 || failedCellRate.Omitted != 1 {
		t.Fatalf("neutral semantic denominator = %#v", failedCellRate)
	}
	omittedRecall := findRate(t, report, "overall", "all", "engram-normal", "useful_recall")
	if omittedRecall.N != 11 || omittedRecall.Omitted != 1 {
		t.Fatalf("normal useful-recall denominator = %#v", omittedRecall)
	}
	if report.Questions.RepositoryGuidance == "" || report.Questions.OverlappingSkills == "" || report.Questions.CLIFollowsSelection == "" || report.Questions.UsefulOutcomes == "" {
		t.Fatalf("missing final study answers: %#v", report.Questions)
	}

	firstJSON, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	reversed := slices.Clone(records)
	slices.Reverse(reversed)
	events.Records = reversed
	secondReport, err := study.Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(secondReport)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("Analyze() output changed with event input order")
	}
}

func TestAnalyzeRejectsMissingOrDuplicateCells(t *testing.T) {
	t.Parallel()

	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	records := recordsForPlan(study.Plan())
	events := EventSet{
		SchemaVersion: "codex-activation-event-set-v1", StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash,
		Verification: validVerification(study), Records: records[:len(records)-1],
	}
	if _, err := study.Analyze(events); err == nil {
		t.Fatal("Analyze() accepted a missing cell")
	}
	events.Records = append(records, records[0])
	if _, err := study.Analyze(events); err == nil {
		t.Fatal("Analyze() accepted a duplicate cell")
	}
}

func TestAnalyzeRejectsUnverifiedOrUnboundedEvidence(t *testing.T) {
	t.Parallel()

	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	events := EventSet{
		SchemaVersion: "codex-activation-event-set-v1", StudyID: study.Contract.StudyID,
		StudyVersion: study.Contract.StudyVersion, ContractSHA256: study.Hash,
		Verification: validVerification(study), Records: recordsForPlan(study.Plan()),
	}
	events.Verification.CleanupVerified = false
	if _, err := study.Analyze(events); err == nil {
		t.Fatal("Analyze() accepted evidence without verified cleanup")
	}
	events.Verification = validVerification(study)
	events.Records[0].Omissions = []string{"/private/raw/session-id"}
	if _, err := study.Analyze(events); err == nil {
		t.Fatal("Analyze() accepted an unbounded omission code")
	}
}

func validVerification(study *Study) VerificationReport {
	digest := strings.Repeat("a", 64)
	return VerificationReport{
		ContractSHA256: study.Hash,
		SourceRevision: study.Contract.SourceRevision,
		UserSkill: SkillIdentity{
			Name: study.Contract.UserSkill.Name, Revision: study.Contract.UserSkill.Revision,
			TreeSHA256: study.Contract.UserSkill.TreeSHA256,
		},
		Fixtures: []FixtureIdentity{
			{ID: "engram-normal", ManifestSHA256: digest},
			{ID: "engram-ablated", ManifestSHA256: digest},
			{ID: "neutral", ManifestSHA256: digest},
		},
		Ablation:                 AblationIdentity{ChangedFiles: []string{"AGENTS.md"}, RemovedGuidanceRows: 2},
		CodexSkillInventory:      study.Contract.Codex.AvailableSkills,
		CodexPromptInputVerified: true,
		CleanupVerified:          true,
	}
}

func recordsForPlan(plan []PlannedRun) []RunRecord {
	records := make([]RunRecord, len(plan))
	for index, run := range plan {
		records[index] = RunRecord{
			SchemaVersion: "codex-activation-events-v1", Sequence: run.Sequence, CellID: run.CellID,
			PromptID: run.PromptID, PromptClass: run.PromptClass, Treatment: run.Treatment,
			Repetition: run.Repetition, SessionShape: run.SessionShape, AvailableSkills: []string{"engram-memory-cli"},
			Events: make(map[string]bool),
		}
		for _, event := range requiredActivationEvents() {
			records[index].Events[event] = false
		}
		records[index].Events["skill_description_available"] = true
	}
	return records
}

func findRate(t *testing.T, report Report, scope, value, treatment, event string) RateMetric {
	t.Helper()
	for _, metric := range report.Rates {
		if metric.Scope == scope && metric.Value == value && metric.Treatment == treatment && metric.Event == event {
			return metric
		}
	}
	t.Fatalf("rate not found: %s/%s/%s/%s", scope, value, treatment, event)
	return RateMetric{}
}

func findDifference(t *testing.T, report Report, scope, value, first, second, event string) PairedDifference {
	t.Helper()
	for _, metric := range report.PairedDifferences {
		if metric.Scope == scope && metric.Value == value && metric.FirstTreatment == first && metric.SecondTreatment == second && metric.Event == event {
			return metric
		}
	}
	t.Fatalf("difference not found: %s/%s/%s/%s/%s", scope, value, first, second, event)
	return PairedDifference{}
}
