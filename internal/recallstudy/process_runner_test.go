package recallstudy

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/codexlifecycle"
	"github.com/yersonargotev/engram/internal/store"
)

func TestStudyTaskOutputFindsAnswerBeforeLifecycleContinuation(t *testing.T) {
	stream := []byte("" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"{\"answer\":\"frozen\"}"}}` + "\n" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"Checkpoint verified as already finalized."}}` + "\n")
	if !studyTaskOutputMatches([]byte("Checkpoint verified as already finalized."), stream, []byte(`{"answer":"frozen"}`)) {
		t.Fatal("task answer before lifecycle continuation was not verified")
	}
	if studyTaskOutputMatches([]byte(`{"answer":"wrong"}`), stream, []byte(`{"answer":"different"}`)) {
		t.Fatal("unobserved task answer passed verification")
	}
	conflicting := append(append([]byte(nil), stream...), []byte(`{"type":"item.completed","item":{"type":"agent_message","text":"{\"answer\":\"superseded\"}"}}`+"\n")...)
	if studyTaskOutputMatches([]byte(`{"answer":"superseded"}`), conflicting, []byte(`{"answer":"frozen"}`)) {
		t.Fatal("superseded historical answer passed verification")
	}
}

func TestStudyRecallEvidencePreservesOpaqueExposuresLabelsAndDisagreements(t *testing.T) {
	firstLatency, secondLatency := int64(11), int64(13)
	snapshot := &store.RecallFeedbackReportSnapshot{
		Operations: []store.RecallFeedbackOperationalMetric{
			{Operation: "search", ExposedResults: 1, ElapsedMonotonicMS: &firstLatency},
			{Operation: "search", ExposedResults: 1, ElapsedMonotonicMS: &secondLatency},
		},
		Runs: []store.RecallFeedbackRunMetric{
			{RunKey: "run-a", ResultCount: 1, ElapsedMonotonicMS: &firstLatency},
			{RunKey: "run-b", ResultCount: 1, ElapsedMonotonicMS: &secondLatency},
		},
		Exposures: []store.RecallFeedbackExposureMetric{
			{RunKey: "run-a", MemoryKey: "memory-a"},
			{RunKey: "run-b", MemoryKey: "memory-a"},
		},
		Labels: []store.RecallFeedbackLabelMetric{
			{RunKey: "run-a", MemoryKey: "memory-a", Utility: "orienting", Quality: "current", Source: "agent_explicit"},
			{RunKey: "run-b", MemoryKey: "memory-a", Quality: "current", Source: "evaluator"},
		},
	}
	evidence, err := deriveStudyRecallEvidence(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.searchObserved || evidence.latencyMillis != 24 || len(evidence.resultKeys) != 1 || evidence.resultKeys[0] != "memory-a" ||
		len(evidence.assessments) != 2 || evidence.assessments[1].Utility != "unknown" || evidence.falseEmptyReview != "not_applicable" {
		t.Fatalf("Recall evidence = %#v", evidence)
	}
}

func TestStudyRecallEvidencePreservesFalseEmptyAndRejectsUnattributedSearch(t *testing.T) {
	latency := int64(7)
	reviewed, err := deriveStudyRecallEvidence(&store.RecallFeedbackReportSnapshot{
		Operations: []store.RecallFeedbackOperationalMetric{{Operation: "search", ElapsedMonotonicMS: &latency}},
		Runs:       []store.RecallFeedbackRunMetric{{RunKey: "run-a", ElapsedMonotonicMS: &latency}},
		EmptyReviews: []store.RecallFalseEmptyMetric{
			{RunKey: "run-a", Source: "agent_explicit", Value: true},
			{RunKey: "run-a", Source: "evaluator", Value: false},
		},
	})
	if err != nil || reviewed.falseEmptyReview != "unknown" || len(reviewed.resultKeys) != 0 {
		t.Fatalf("false-empty evidence=%#v err=%v", reviewed, err)
	}
	_, err = deriveStudyRecallEvidence(&store.RecallFeedbackReportSnapshot{
		Operations: []store.RecallFeedbackOperationalMetric{{Operation: "search"}},
	})
	var invalid *invalidExecutionError
	if !errors.As(err, &invalid) || invalid.reasonCode != "targeted_recall_attribution_unavailable" {
		t.Fatalf("unattributed search error = %v", err)
	}
}

func TestStudyCodexLaunchDoesNotGrantModelWriteAccessToHarnessState(t *testing.T) {
	study, _, _ := verifiedStudy(t)
	runner := &processCohortRunner{study: study}
	arguments := runner.codexArguments("/cell/workspace", "/cell/harness/final-message.txt")
	if strings.Contains(strings.Join(arguments, "\x00"), "--add-dir") {
		t.Fatalf("Codex arguments grant an extra writable directory: %v", arguments)
	}
	environment := studyProcessEnvironment("/cell/harness/home", "/cell/harness/tools", "/cell/model")
	if len(environment) != 6 || strings.Contains(strings.Join(environment, "\x00"), "ENGRAM_DATA_DIR") {
		t.Fatalf("Codex environment escapes the frozen allowlist: %v", environment)
	}
}

func TestStudyInjectionEvidenceBindsExactTreatmentContext(t *testing.T) {
	root := t.TempDir()
	skill := filepath.Join(root, "plugin", "codex", "skills", "memory", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(skill), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(skill, []byte("<!-- engram:checkpoint-cue:start -->\nrecall cue\n<!-- engram:checkpoint-cue:end -->\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &processCohortRunner{snapshot: root}
	broadContext := "synthetic broad context"
	broad, err := runner.injectionEvidence(PlannedRun{Treatment: "broad-chronological"}, broadContext)
	if err != nil {
		t.Fatal(err)
	}
	targeted, err := runner.injectionEvidence(PlannedRun{Treatment: "targeted-recall"}, broadContext)
	if err != nil {
		t.Fatal(err)
	}
	wantBroad, _ := codexlifecycle.BuildModelContext("recall cue", broadContext, codexlifecycle.MaxInjectedUTF8Bytes)
	wantTargeted, _ := codexlifecycle.BuildModelContext("recall cue", "", codexlifecycle.MaxInjectedUTF8Bytes)
	if broad.expectedUTF8Bytes != int64(len(wantBroad)) || targeted.expectedUTF8Bytes != int64(len(wantTargeted)) ||
		broad.broadResultKey != studyFileDigest("recall-study-broad-exposure-v1\x00"+broadContext) {
		t.Fatalf("broad=%#v targeted=%#v", broad, targeted)
	}
}

func TestProcessRunnerRetainsActiveCellUntilPersistenceCleanup(t *testing.T) {
	root := t.TempDir()
	cell := filepath.Join(root, "cell-active")
	if err := os.Mkdir(cell, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := &processCohortRunner{root: root, activeCell: cell}
	if err := runner.Close(); err == nil || !strings.Contains(err.Error(), "retained unpersisted cell evidence") {
		t.Fatalf("Close error = %v", err)
	}
	if _, err := os.Stat(cell); err != nil {
		t.Fatalf("active cell was removed before persistence: %v", err)
	}
	runner.activeCell = ""
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("closed runner root still exists: %v", err)
	}
}
