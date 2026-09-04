package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCmdCheckpointRecordReplayAndStatusJSON(t *testing.T) {
	cfg := testConfig(t)
	identityArgs := []string{
		"--host", "codex",
		"--session-id", "session-cli-123",
		"--root-turn-id", "turn-cli-456",
	}

	recordArgs := append([]string{"engram", "checkpoint", "record"}, identityArgs...)
	recordArgs = append(recordArgs,
		"--disposition", store.CheckpointDispositionSkipped,
		"--reason", store.CheckpointSkipReasonNoDurableKnowledge,
		"--json",
	)
	withArgs(t, recordArgs...)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("record stderr = %q", stderr)
	}
	var created memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(stdout), &created); err != nil {
		t.Fatalf("decode record JSON: %v\n%s", err, stdout)
	}
	if created.Idempotency != memoryops.CheckpointIdempotencyCreated || created.Checkpoint == nil {
		t.Fatalf("created result = %#v", created)
	}

	withArgs(t, recordArgs...)
	stdout, stderr = captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("replay stderr = %q", stderr)
	}
	var replayed memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(stdout), &replayed); err != nil {
		t.Fatalf("decode replay JSON: %v\n%s", err, stdout)
	}
	if replayed.Idempotency != memoryops.CheckpointIdempotencyAlreadyRecorded ||
		!reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("replayed result = %#v, want checkpoint %#v", replayed, created.Checkpoint)
	}

	statusArgs := append([]string{"engram", "checkpoint", "status"}, identityArgs...)
	statusArgs = append(statusArgs, "--json")
	withArgs(t, statusArgs...)
	stdout, stderr = captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("status stderr = %q", stderr)
	}
	var status memoryops.CheckpointStatusResult
	if err := json.Unmarshal([]byte(stdout), &status); err != nil {
		t.Fatalf("decode status JSON: %v\n%s", err, stdout)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("status checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
}

func TestCmdCheckpointRecordsOptionalRecallFeedbackJSON(t *testing.T) {
	cfg := testConfig(t)
	identity := store.CheckpointIdentity{
		Host: "codex", SessionID: "session-cli-feedback", RootTurnID: "turn-cli-feedback",
	}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.RecordRecallRunContext(context.Background(), store.RecallRunRecord{
		RecallID: "recall-cli-feedback", Project: "engram", Scope: "project",
		DeliveredUTF8Bytes: 2, ElapsedMonotonicMS: 9, ProtocolVersion: 1, BinaryVersion: "test",
		TurnIdentity: &identity,
	}); err != nil {
		_ = s.Close()
		t.Fatalf("seed empty Recall run: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}
	feedback, err := json.Marshal(memoryops.RecallFeedbackInput{
		RecallID:   "recall-cli-feedback",
		FalseEmpty: &memoryops.RecallFalseEmptyInput{Value: true, Source: memoryops.RecallFeedbackSourceEvaluator},
	})
	if err != nil {
		t.Fatalf("encode Recall feedback: %v", err)
	}
	withArgs(t,
		"engram", "checkpoint", "record",
		"--host", identity.Host, "--session-id", identity.SessionID, "--root-turn-id", identity.RootTurnID,
		"--disposition", store.CheckpointDispositionSkipped,
		"--reason", store.CheckpointSkipReasonNoDurableKnowledge,
		"--recall-feedback-json", string(feedback), "--json",
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("checkpoint feedback stderr = %q", stderr)
	}
	var result memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode checkpoint feedback result: %v\n%s", err, stdout)
	}
	if result.Idempotency != memoryops.CheckpointIdempotencyCreated || result.RecallFeedback == nil ||
		result.RecallFeedback.Status != memoryops.RecallFeedbackStatusRecorded ||
		result.RecallFeedback.EmptyReviewsRecorded != 1 {
		t.Fatalf("checkpoint feedback result = %#v", result)
	}
}

func TestCmdSearchBindsRecallFeedbackToExactCheckpointTurn(t *testing.T) {
	cfg := testConfig(t)
	identity := store.CheckpointIdentity{
		Host: "--codex", SessionID: "--session-cli-search-feedback", RootTurnID: "--turn-cli-search-feedback",
	}
	withArgs(t,
		"engram", "search", "no matching feedback memory", "--project", "engram",
		"--host="+identity.Host, "--session-id="+identity.SessionID, "--root-turn-id="+identity.RootTurnID,
		"--json",
	)
	stdout, stderr := captureOutput(t, func() { cmdSearch(cfg) })
	if stderr != "" {
		t.Fatalf("bound search stderr = %q", stderr)
	}
	var search struct {
		RecallID    string `json:"recall_id"`
		ResultCount int    `json:"result_count"`
	}
	if err := json.Unmarshal([]byte(stdout), &search); err != nil {
		t.Fatalf("decode bound search: %v\n%s", err, stdout)
	}
	if search.RecallID == "" || search.ResultCount != 0 {
		t.Fatalf("bound search = %#v", search)
	}
	feedback, err := json.Marshal(memoryops.RecallFeedbackInput{
		RecallID: search.RecallID,
		FalseEmpty: &memoryops.RecallFalseEmptyInput{
			Value: true, Source: memoryops.RecallFeedbackSourceAgentExplicit,
		},
	})
	if err != nil {
		t.Fatalf("encode bound Recall feedback: %v", err)
	}
	withArgs(t,
		"engram", "checkpoint", "record",
		"--host="+identity.Host, "--session-id="+identity.SessionID, "--root-turn-id="+identity.RootTurnID,
		"--disposition", store.CheckpointDispositionSkipped,
		"--reason", store.CheckpointSkipReasonNoDurableKnowledge,
		"--recall-feedback-json", string(feedback), "--json",
	)
	stdout, stderr = captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("bound checkpoint stderr = %q", stderr)
	}
	var checkpoint memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(stdout), &checkpoint); err != nil {
		t.Fatalf("decode bound checkpoint: %v\n%s", err, stdout)
	}
	if checkpoint.RecallFeedback == nil || checkpoint.RecallFeedback.Status != memoryops.RecallFeedbackStatusRecorded {
		t.Fatalf("bound checkpoint = %#v", checkpoint)
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open bound Recall store: %v", err)
	}
	defer s.Close()
	var turnKey string
	if err := s.DB().QueryRow(`SELECT turn_key FROM recall_runs WHERE recall_id = ?`, search.RecallID).Scan(&turnKey); err != nil {
		t.Fatalf("load bound Recall turn key: %v", err)
	}
	if len(turnKey) != 64 || strings.Contains(turnKey, identity.Host) ||
		strings.Contains(turnKey, identity.SessionID) || strings.Contains(turnKey, identity.RootTurnID) {
		t.Fatalf("bound Recall turn key leaked raw identity: %q", turnKey)
	}
}

func TestCmdSearchRequiresCompleteCheckpointIdentity(t *testing.T) {
	stubExitWithPanic(t)
	withArgs(t,
		"engram", "search", "partial identity", "--project", "engram",
		"--host", "codex", "--json",
	)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdSearch(testConfig(t)) })
	if stdout != "" {
		t.Fatalf("partial identity stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok || !strings.Contains(stderr, "invalid_checkpoint_identity") ||
		!strings.Contains(stderr, "must be provided together") {
		t.Fatalf("partial identity panic=%v stderr=%q", recovered, stderr)
	}
}

func TestCmdCheckpointKeepsHumanFeedbackFailureVisibleAfterCompletion(t *testing.T) {
	cfg := testConfig(t)
	withArgs(t,
		"engram", "checkpoint", "record",
		"--host", "codex", "--session-id", "session-human-feedback", "--root-turn-id", "turn-human-feedback",
		"--disposition", store.CheckpointDispositionSkipped,
		"--reason", store.CheckpointSkipReasonNoDurableKnowledge,
		"--recall-feedback-json", `{"recall_id":"missing", "unexpected":true}`,
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("checkpoint feedback stderr = %q", stderr)
	}
	for _, want := range []string{"Memory checkpoint created: skipped", "Recall feedback failed", "invalid_recall_feedback"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("checkpoint feedback output %q does not contain %q", stdout, want)
		}
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open completed checkpoint store: %v", err)
	}
	defer s.Close()
	if _, err := s.GetMemoryCheckpoint(store.CheckpointIdentity{
		Host: "codex", SessionID: "session-human-feedback", RootTurnID: "turn-human-feedback",
	}); err != nil {
		t.Fatalf("checkpoint did not remain complete after feedback failure: %v", err)
	}
}

func TestCmdCheckpointStatusHumanOutputExposesSavedReferences(t *testing.T) {
	cfg := testConfig(t)
	memoryIDs := seedCheckpointParityMemories(t, cfg, "engram", 1)
	identity := checkpointParityIdentity{"codex", "session-human-saved", "turn-human-saved"}
	runCheckpointCLIRecordSaved(t, cfg, identity, "engram", memoryIDs)

	withArgs(t,
		"engram", "checkpoint", "status",
		"--host", identity.host,
		"--session-id", identity.sessionID,
		"--root-turn-id", identity.rootTurnID,
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("status stderr = %q", stderr)
	}
	for _, want := range []string{
		"Memory checkpoint: saved (1 Memories)",
		fmt.Sprintf("Memory #%d", memoryIDs[0]),
		"project engram",
		"obs-",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("status output %q does not contain %q", stdout, want)
		}
	}
}

func TestCmdCheckpointStatusHumanOutputExposesProposalSnapshot(t *testing.T) {
	cfg := testConfig(t)
	identity := checkpointParityIdentity{"codex", "session-human-needs-review", "turn-human-needs-review"}
	created := runCheckpointCLIRecordProposal(t, cfg, identity, "engram", "", map[string]any{
		"title": "Human review proposal", "content": "Review this proposal.",
	})
	checkpoint := created["checkpoint"].(map[string]any)
	proposalID := checkpoint["proposal"].(map[string]any)["id"].(string)

	withArgs(t,
		"engram", "checkpoint", "status",
		"--host", identity.host,
		"--session-id", identity.sessionID,
		"--root-turn-id", identity.rootTurnID,
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("status stderr = %q", stderr)
	}
	for _, want := range []string{"Memory checkpoint: needs_review", proposalID, "project engram"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("status output %q does not contain %q", stdout, want)
		}
	}
}

func TestCmdCheckpointPreflightIsReadOnlyAndMixedStatusExposesBothOutcomes(t *testing.T) {
	cfg := testConfig(t)
	memoryIDs := seedCheckpointParityMemories(t, cfg, "engram", 1)

	withArgs(t,
		"engram", "checkpoint", "preflight",
		"--project=engram",
		`--memory-json={"type":"decision","title":"Parity Memory 1","content":"Durable parity content 1"}`,
		"--json",
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("preflight stderr = %q", stderr)
	}
	var preflight memoryops.CheckpointPreflightResult
	if err := json.Unmarshal([]byte(stdout), &preflight); err != nil {
		t.Fatalf("decode preflight JSON: %v\n%s", err, stdout)
	}
	if preflight.Project != "engram" || preflight.CandidateLimit != 3 || len(preflight.ExactDuplicates) != 1 ||
		preflight.ExactDuplicates[0].Reference.MemoryID != memoryIDs[0] {
		t.Fatalf("preflight result = %#v", preflight)
	}

	identity := checkpointParityIdentity{"codex", "session-human-mixed", "turn-human-mixed"}
	withArgs(t,
		"engram", "checkpoint", "record",
		"--host="+identity.host,
		"--session-id="+identity.sessionID,
		"--root-turn-id="+identity.rootTurnID,
		"--disposition=needs_review",
		"--project=engram",
		fmt.Sprintf("--memory-id=%d", memoryIDs[0]),
		`--memory-json={"type":"discovery","title":"Mixed settled discovery","content":"This result is settled."}`,
		`--proposal-json={"title":"Mixed unresolved proposal","content":"This conflict needs review."}`,
	)
	stdout, stderr = captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("Mixed record stderr = %q", stderr)
	}
	for _, want := range []string{"needs_review", "2 Memories", "proposal", "Memory #"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("Mixed record output %q does not contain %q", stdout, want)
		}
	}

	withArgs(t,
		"engram", "checkpoint", "status",
		"--host="+identity.host,
		"--session-id="+identity.sessionID,
		"--root-turn-id="+identity.rootTurnID,
	)
	stdout, stderr = captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("Mixed status stderr = %q", stderr)
	}
	for _, want := range []string{"needs_review", "2 Memories", "proposal", "Memory #", "project engram"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("Mixed status output %q does not contain %q", stdout, want)
		}
	}
}

func TestCheckpointCLIProcessJSONContract(t *testing.T) {
	if testing.CoverMode() != "" {
		t.Skip("expected non-zero helper subprocess exits corrupt Go coverage output")
	}
	dataDir := t.TempDir()
	run := func(t *testing.T, helperCase string, wantExit int) (string, string) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointProcessHelper$")
		cmd.Env = append(os.Environ(),
			"GO_WANT_CHECKPOINT_PROCESS=1",
			"CHECKPOINT_HELPER_CASE="+helperCase,
			"ENGRAM_DATA_DIR="+dataDir,
		)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		if wantExit == 0 {
			if err != nil {
				t.Fatalf("helper %s: %v, stderr=%q", helperCase, err, stderr.String())
			}
		} else {
			exitErr, ok := err.(*exec.ExitError)
			if !ok || exitErr.ExitCode() != wantExit {
				t.Fatalf("helper %s exit = %v, want %d", helperCase, err, wantExit)
			}
		}
		return stdout.String(), stderr.String()
	}

	stdout, stderr := run(t, "record", 0)
	if stderr != "" || decodeCLIJSON(t, stdout)["idempotency"] != memoryops.CheckpointIdempotencyCreated {
		t.Fatalf("record stdout=%q stderr=%q", stdout, stderr)
	}
	stdout, stderr = run(t, "record", 0)
	if stderr != "" || decodeCLIJSON(t, stdout)["idempotency"] != memoryops.CheckpointIdempotencyAlreadyRecorded {
		t.Fatalf("replay stdout=%q stderr=%q", stdout, stderr)
	}
	stdout, stderr = run(t, "malformed-replay", 0)
	if stderr != "" || decodeCLIJSON(t, stdout)["idempotency"] != memoryops.CheckpointIdempotencyAlreadyRecorded {
		t.Fatalf("identity-first malformed replay stdout=%q stderr=%q", stdout, stderr)
	}
	stdout, stderr = run(t, "status", 0)
	if stderr != "" || decodeCLIJSON(t, stdout)["checkpoint"] == nil {
		t.Fatalf("status stdout=%q stderr=%q", stdout, stderr)
	}
	stdout, stderr = run(t, "processing-failed", 1)
	if stdout != "" || decodeCLIJSON(t, stderr)["code"] != memoryops.CheckpointErrorCodeInvalidReason {
		t.Fatalf("invalid stdout=%q stderr=%q", stdout, stderr)
	}
	stdout, stderr = run(t, "leading-dash-identity", 0)
	if stderr != "" || decodeCLIJSON(t, stdout)["idempotency"] != memoryops.CheckpointIdempotencyCreated {
		t.Fatalf("leading-dash stdout=%q stderr=%q", stdout, stderr)
	}
	stdout, stderr = run(t, "needs-review", 0)
	if stderr != "" {
		t.Fatalf("needs-review stdout=%q stderr=%q", stdout, stderr)
	}
	checkpoint, _ := decodeCLIJSON(t, stdout)["checkpoint"].(map[string]any)
	proposal, _ := checkpoint["proposal"].(map[string]any)
	if checkpoint["disposition"] != store.CheckpointDispositionNeedsReview || len(proposal) != 5 ||
		proposal["id"] == "" || proposal["project"] != "engram" ||
		proposal["title"] != "Review process proposal" || proposal["content"] != "Keep this local until review." ||
		proposal["created_at"] == "" {
		t.Fatalf("needs-review checkpoint=%#v", checkpoint)
	}
}

func TestCheckpointProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_CHECKPOINT_PROCESS") != "1" {
		return
	}
	os.Args = []string{
		"engram", "checkpoint", "record",
		"--host", "codex",
		"--session-id", "session-process-123",
		"--root-turn-id", "turn-process-456",
		"--disposition", store.CheckpointDispositionSkipped,
		"--reason", store.CheckpointSkipReasonNoDurableKnowledge,
		"--json",
	}
	switch os.Getenv("CHECKPOINT_HELPER_CASE") {
	case "record":
	case "status":
		os.Args = []string{
			"engram", "checkpoint", "status",
			"--host", "codex",
			"--session-id", "session-process-123",
			"--root-turn-id", "turn-process-456",
			"--json",
		}
	case "processing-failed":
		os.Args[8] = "turn-process-invalid-reason"
		os.Args[len(os.Args)-2] = "processing_failed"
	case "malformed-replay":
		os.Args = append(os.Args, "--memory-id=not-an-integer")
	case "leading-dash-identity":
		os.Args = []string{
			"engram", "checkpoint", "record",
			"--host=--codex",
			"--session-id=--help",
			"--root-turn-id=-h",
			"--disposition=skipped",
			"--reason=no_durable_knowledge",
			"--json",
		}
	case "needs-review":
		os.Args = []string{
			"engram", "checkpoint", "record",
			"--host=codex",
			"--session-id=session-process-needs-review",
			"--root-turn-id=turn-process-needs-review",
			"--disposition=needs_review",
			"--project=engram",
			`--proposal-json={"title":"Review process proposal","content":"Keep this local until review."}`,
			"--json",
		}
	default:
		t.Fatalf("unknown checkpoint helper case %q", os.Getenv("CHECKPOINT_HELPER_CASE"))
	}
	main()
	os.Exit(0)
}

func TestCheckpointMissingValuePreservesJSONErrorContract(t *testing.T) {
	stubExitWithPanic(t)
	withArgs(t,
		"engram", "checkpoint", "record",
		"--host", "codex",
		"--session-id", "session-missing-reason",
		"--root-turn-id", "turn-missing-reason",
		"--disposition", "skipped",
		"--reason", "--json",
	)

	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(testConfig(t)) })
	if stdout != "" {
		t.Fatalf("missing-value stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("missing-value exit = %v", recovered)
	}
	if got := decodeCLIJSON(t, stderr)["code"]; got != "invalid_arguments" {
		t.Fatalf("missing-value code = %#v, stderr=%q", got, stderr)
	}
}

func TestParseCheckpointArgsAcceptsInlineLeadingDashIdentity(t *testing.T) {
	opts, err := parseCheckpointArgs([]string{
		"record",
		"--host=--codex",
		"--session-id=--help",
		"--root-turn-id=-h",
		"--disposition=skipped",
		"--reason=no_durable_knowledge",
		"--json",
	})
	if err != nil {
		t.Fatalf("parse checkpoint args: %v", err)
	}
	if opts.Help || !opts.JSONMode || opts.Host != "--codex" || opts.SessionID != "--help" || opts.RootTurnID != "-h" {
		t.Fatalf("parsed options = %#v", opts)
	}
}

func TestMainCheckpointHelpDoesNotCreateLocalDatabase(t *testing.T) {
	for _, args := range [][]string{
		{"engram", "checkpoint", "--help"},
		{"engram", "checkpoint", "record", "--help"},
		{"engram", "checkpoint", "record", "--host", "codex", "--help"},
		{"engram", "checkpoint", "record", "--reason", "--help"},
		{"engram", "checkpoint", "status", "-h"},
	} {
		t.Run(args[len(args)-1], func(t *testing.T) {
			stubRuntimeHooks(t)
			dataDir := t.TempDir()
			t.Setenv("ENGRAM_DATA_DIR", dataDir)
			withArgs(t, args...)
			stdout, stderr, recovered := captureOutputAndRecover(t, func() { main() })
			if recovered != nil || stderr != "" {
				t.Fatalf("help panic=%v stderr=%q", recovered, stderr)
			}
			if stdout == "" {
				t.Fatal("checkpoint help was empty")
			}
			if _, err := os.Stat(filepath.Join(dataDir, "engram.db")); !os.IsNotExist(err) {
				t.Fatalf("checkpoint help created store: %v", err)
			}
		})
	}
}

func TestCheckpointHelpDocumentsAllTerminalDispositions(t *testing.T) {
	stdout, stderr := captureOutput(t, printCheckpointUsage)
	if stderr != "" {
		t.Fatalf("checkpoint help stderr = %q", stderr)
	}
	for _, want := range []string{
		store.CheckpointDispositionSaved,
		store.CheckpointDispositionSkipped,
		store.CheckpointDispositionNeedsReview,
		"--proposal-json",
		`"title"`,
		`"content"`,
		"checkpoint verify-stop --host HOST",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("checkpoint help %q does not contain %q", stdout, want)
		}
	}
	if strings.Contains(stdout, "--proposal-id") {
		t.Fatalf("checkpoint help still exposes proposal_id: %q", stdout)
	}
}

func TestCheckpointVerifyStopCLIProcessContract(t *testing.T) {
	if testing.CoverMode() != "" {
		t.Skip("expected subprocess exits corrupt Go coverage output")
	}
	dataDir := t.TempDir()
	cfg := store.Config{DataDir: dataDir}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open checkpoint store: %v", err)
	}
	service := memoryops.New(s)
	if _, err := service.RecordCheckpoint(memoryops.CheckpointRecordInput{
		Host: "codex", SessionID: "session-terminal", RootTurnID: "turn-terminal",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	}); err != nil {
		t.Fatalf("record terminal checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close checkpoint store: %v", err)
	}

	run := func(t *testing.T, input string) map[string]any {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointVerifyStopProcessHelper$")
		cmd.Env = append(os.Environ(),
			"GO_WANT_CHECKPOINT_STOP_PROCESS=1",
			"ENGRAM_DATA_DIR="+dataDir,
		)
		cmd.Stdin = strings.NewReader(input)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("verify-stop process: %v, stdout=%q stderr=%q", err, stdout.String(), stderr.String())
		}
		if stderr.Len() != 0 {
			t.Fatalf("verify-stop stderr = %q", stderr.String())
		}
		return decodeCLIJSON(t, stdout.String())
	}

	terminal := run(t, `{"session_id":"session-terminal","turn_id":"turn-terminal","stop_hook_active":false}`)
	if len(terminal) != 0 {
		t.Fatalf("terminal response = %#v, want empty object", terminal)
	}

	missing := run(t, `{"session_id":"session-missing","turn_id":"turn-missing","stop_hook_active":false}`)
	if missing["decision"] != "block" || !strings.Contains(fmt.Sprint(missing["reason"]), `{"host":"codex","session_id":"session-missing","root_turn_id":"turn-missing"}`) {
		t.Fatalf("missing response = %#v", missing)
	}

	replayed := run(t, `{"session_id":"session-missing","turn_id":"turn-missing","stop_hook_active":true}`)
	if replayed["decision"] != nil || !strings.Contains(fmt.Sprint(replayed["systemMessage"]), "single recovery continuation") {
		t.Fatalf("replayed missing response = %#v", replayed)
	}

	for _, input := range []string{
		`{`,
		`{"session_id":"session","turn_id":"turn"}`,
		`{"session_id":47,"turn_id":"turn","stop_hook_active":false}`,
	} {
		invalid := run(t, input)
		if invalid["decision"] != nil || !strings.Contains(fmt.Sprint(invalid["systemMessage"]), "Stop input") {
			t.Fatalf("invalid input %q response = %#v", input, invalid)
		}
	}
}

func TestCheckpointVerifyStopProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_CHECKPOINT_STOP_PROCESS") != "1" {
		return
	}
	os.Args = []string{"engram", "checkpoint", "verify-stop", "--host=codex"}
	main()
	os.Exit(0)
}

func TestCheckpointVerifyStopCursorIncompleteInputDoesNotFollowUp(t *testing.T) {
	cfg := testConfig(t)
	for _, input := range []string{
		`{`,
		`{"conversation_id":"conv-incomplete"}`,
		`{"generation_id":"gen-incomplete"}`,
	} {
		stdout, stderr := captureOutput(t, func() {
			cmdCheckpointVerifyStop(cfg, "cursor", strings.NewReader(input))
		})
		if stderr != "" {
			t.Fatalf("stderr = %q for %s", stderr, input)
		}
		response := decodeCLIJSON(t, stdout)
		if response["followup_message"] != nil || strings.Contains(stdout, store.CheckpointDispositionSkipped) {
			t.Fatalf("incomplete Cursor stop %q invented work: %#v", input, response)
		}
	}
}

func TestCheckpointVerifyStopCursorStoreFailureDoesNotInventDisposition(t *testing.T) {
	originalStoreNew := storeNew
	storeNew = func(store.Config) (*store.Store, error) {
		return nil, errors.New("injected store failure")
	}
	t.Cleanup(func() { storeNew = originalStoreNew })

	stdout, stderr := captureOutput(t, func() {
		cmdCheckpointVerifyStop(testConfig(t), "cursor", strings.NewReader(`{
  "conversation_id": "conv-cursor-store-failure",
  "generation_id": "gen-cursor-store-failure",
  "status": "completed",
  "loop_count": 0
}`))
	})
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	response := decodeCLIJSON(t, stdout)
	if response["followup_message"] != nil {
		t.Fatalf("store failure requested a follow-up: %#v", response)
	}
	if strings.Contains(stdout, store.CheckpointDispositionSkipped) || strings.Contains(stdout, store.CheckpointSkipReasonNoDurableKnowledge) {
		t.Fatalf("store failure invented a disposition: %s", stdout)
	}
}

func TestCheckpointVerifyStopCursorAbortedDoesNotFollowUp(t *testing.T) {
	cfg := testConfig(t)
	stdout, stderr := captureOutput(t, func() {
		cmdCheckpointVerifyStop(cfg, "cursor", strings.NewReader(`{
  "conversation_id": "conv-cursor-aborted",
  "generation_id": "gen-cursor-aborted",
  "status": "aborted",
  "loop_count": 0
}`))
	})
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	response := decodeCLIJSON(t, stdout)
	if response["followup_message"] != nil {
		t.Fatalf("aborted Cursor stop requested a follow-up: %#v", response)
	}
}

func TestCheckpointVerifyStopCursorRequestsFollowUpWhenCheckpointMissing(t *testing.T) {
	cfg := testConfig(t)
	stdout, stderr := captureOutput(t, func() {
		cmdCheckpointVerifyStop(cfg, "cursor", strings.NewReader(`{
  "conversation_id": "conv-cursor-missing",
  "generation_id": "gen-cursor-missing",
  "status": "completed",
  "loop_count": 0
}`))
	})
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	response := decodeCLIJSON(t, stdout)
	followup, _ := response["followup_message"].(string)
	if !strings.Contains(followup, `{"host":"cursor","session_id":"conv-cursor-missing","root_turn_id":"gen-cursor-missing"}`) {
		t.Fatalf("missing Cursor follow-up = %#v", response)
	}
	if response["decision"] != nil {
		t.Fatalf("Cursor stop used Codex decision field: %#v", response)
	}
	if strings.Contains(stdout, store.CheckpointDispositionSkipped) || strings.Contains(stdout, store.CheckpointSkipReasonNoDurableKnowledge) {
		t.Fatalf("Cursor stop invented a disposition: %s", stdout)
	}
}

func TestCheckpointVerifyStopCursorSilentWhenCheckpointExists(t *testing.T) {
	cfg := testConfig(t)
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open checkpoint store: %v", err)
	}
	if _, err := memoryops.New(s).RecordCheckpoint(memoryops.CheckpointRecordInput{
		Host: "cursor", SessionID: "conv-cursor-done", RootTurnID: "gen-cursor-done",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	}); err != nil {
		t.Fatalf("record Cursor checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close checkpoint store: %v", err)
	}

	stdout, stderr := captureOutput(t, func() {
		cmdCheckpointVerifyStop(cfg, "cursor", strings.NewReader(`{
  "conversation_id": "conv-cursor-done",
  "generation_id": "gen-cursor-done",
  "status": "completed",
  "loop_count": 0
}`))
	})
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	response := decodeCLIJSON(t, stdout)
	if len(response) != 0 {
		t.Fatalf("complete Cursor stop = %#v, want empty object", response)
	}
}

func TestCheckpointVerifyStopCursorDoesNotFollowUpAfterOneRecovery(t *testing.T) {
	cfg := testConfig(t)
	stdout, stderr := captureOutput(t, func() {
		cmdCheckpointVerifyStop(cfg, "cursor", strings.NewReader(`{
  "conversation_id": "conv-cursor-replay",
  "generation_id": "gen-cursor-replay",
  "status": "completed",
  "loop_count": 1
}`))
	})
	if stderr != "" {
		t.Fatalf("stderr = %q", stderr)
	}
	response := decodeCLIJSON(t, stdout)
	if response["followup_message"] != nil {
		t.Fatalf("replayed Cursor stop requested another follow-up: %#v", response)
	}
}

func TestCheckpointVerifyStopReportsStoreFailureWithoutInventingDisposition(t *testing.T) {
	originalStoreNew := storeNew
	storeNew = func(store.Config) (*store.Store, error) {
		return nil, errors.New("injected store failure")
	}
	t.Cleanup(func() { storeNew = originalStoreNew })

	stdout, stderr := captureOutput(t, func() {
		cmdCheckpointVerifyStop(testConfig(t), "codex", strings.NewReader(
			`{"session_id":"session-store-failure","turn_id":"turn-store-failure","stop_hook_active":false}`,
		))
	})
	if stderr != "" {
		t.Fatalf("store failure stderr = %q", stderr)
	}
	response := decodeCLIJSON(t, stdout)
	if response["decision"] != nil || !strings.Contains(fmt.Sprint(response["systemMessage"]), "integration failure") {
		t.Fatalf("store failure response = %#v", response)
	}
	if strings.Contains(stdout, store.CheckpointDispositionSkipped) || strings.Contains(stdout, store.CheckpointSkipReasonNoDurableKnowledge) {
		t.Fatalf("store failure invented a disposition: %s", stdout)
	}
}
