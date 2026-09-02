package recallstudy

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
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
	joinedArguments := strings.Join(arguments, "\x00")
	for _, setting := range []string{
		"sandbox_workspace_write.exclude_tmpdir_env_var=true",
		"sandbox_workspace_write.exclude_slash_tmp=true",
	} {
		if !strings.Contains(joinedArguments, setting) {
			t.Fatalf("Codex arguments do not disable global temp root %q: %v", setting, arguments)
		}
	}
	environment := studyProcessEnvironment("/cell/harness/home", "/cell/harness/tools", "/cell/model")
	if len(environment) != 6 || strings.Contains(strings.Join(environment, "\x00"), "ENGRAM_DATA_DIR") {
		t.Fatalf("Codex environment escapes the frozen allowlist: %v", environment)
	}
}

func TestStudyLifecycleWrapperKeepsRepositoryVisibleAndRedirectsEveryTreatment(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	lifecycleCWD := filepath.Join(root, "lifecycle-cwd")
	dataDir := filepath.Join(root, "data")
	manifest := filepath.Join(workspace, ".engram", "manifest.json")
	for _, directory := range []string{filepath.Dir(manifest), lifecycleCWD, dataDir} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	want := []byte("frozen manifest\n")
	if err := os.WriteFile(manifest, want, 0o640); err != nil {
		t.Fatal(err)
	}
	fake := filepath.Join(root, "fake-engram")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nprintf 'canary=%s\\n' \"${ENGRAM_CODEX_RECALL_CANARY-}\"\n/bin/cat\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, treatment := range []string{"broad-chronological", "targeted-recall", "no-recall"} {
		wrapper := filepath.Join(root, treatment, "engram")
		if err := writeStudyEngramWrapper(wrapper, fake, dataDir, treatment, workspace, lifecycleCWD); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(wrapper, "lifecycle", "session-start")
		command.Env = []string{"PATH=/usr/bin:/bin"}
		command.Stdin = strings.NewReader(`{"session_id":"cell","cwd":"` + workspace + `","source":"startup"}`)
		output, err := command.Output()
		if err != nil {
			t.Fatalf("%s lifecycle wrapper: %v", treatment, err)
		}
		text := string(output)
		if strings.Contains(text, workspace) || !strings.Contains(text, lifecycleCWD) {
			t.Fatalf("%s lifecycle cwd was not redirected: %s", treatment, text)
		}
		wantCanary := "canary=\n"
		if treatment != "broad-chronological" {
			wantCanary = "canary=targeted-recall\n"
		}
		if !strings.HasPrefix(text, wantCanary) {
			t.Fatalf("%s lifecycle canary = %q, want prefix %q", treatment, text, wantCanary)
		}
	}

	got, err := os.ReadFile(manifest)
	if err != nil || string(got) != string(want) {
		t.Fatalf("model-visible manifest=%q err=%v", got, err)
	}
	info, err := os.Stat(manifest)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("model-visible manifest mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestStudyAuthGuardRemovesOnlyTheDisposableCopy(t *testing.T) {
	codexHome := t.TempDir()
	if err := writeStudyAuthGuard(codexHome); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(codexHome, "hooks.json")
	raw, err := os.ReadFile(path)
	if err != nil || !json.Valid(raw) {
		t.Fatalf("auth guard is invalid: %q err=%v", raw, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("auth guard mode=%v err=%v", info.Mode().Perm(), err)
	}
	text := string(raw)
	if !strings.Contains(text, "PreToolUse") || !strings.Contains(text, `${HOME}/.codex/auth.json`) || strings.Contains(text, "CODEX_HOME") {
		t.Fatalf("auth guard does not target the isolated HOME copy: %s", text)
	}
}

func TestSyntheticStudyStoreRejectsImportedObservations(t *testing.T) {
	dataDir := t.TempDir()
	localStore, err := store.New(store.FallbackConfig(dataDir))
	if err != nil {
		t.Fatal(err)
	}
	if err := localStore.CreateSession("study-session", "engram", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"one", "two", "three"} {
		if _, err := localStore.AddObservation(store.AddObservationParams{
			SessionID: "study-session", Type: "manual", Title: value, Content: value, Project: "engram", Scope: "project",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := localStore.Close(); err != nil {
		t.Fatal(err)
	}
	err = verifySyntheticStudyStore(dataDir, 1, 2)
	var invalid *invalidExecutionError
	if !errors.As(err, &invalid) || invalid.reasonCode != "study_store_contaminated" {
		t.Fatalf("contaminated store error = %v", err)
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
	broadContext := "## Memory from Previous Sessions\n\n### Recent Sessions\n- **engram** (2026-09-01 20:24:36) [1 observations]\n\n### Recent Observations\n- synthetic\n"
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
	wantBroadBytes := int64(len(wantBroad) + len("- **engram** (0000-00-00 00:00:00) [0 observations]\n"))
	if broad.expectedUTF8Bytes != wantBroadBytes || targeted.expectedUTF8Bytes != int64(len(wantTargeted)) ||
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
