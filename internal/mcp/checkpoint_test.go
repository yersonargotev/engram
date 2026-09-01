package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCheckpointToolsRecordReplayStatusAndRejectFailureReasons(t *testing.T) {
	s := newMCPTestStore(t)
	record := CheckpointToolHandler(s)
	status := CheckpointStatusToolHandler(s)
	arguments := map[string]any{
		"host":         "codex",
		"session_id":   "session-mcp-123",
		"root_turn_id": "turn-mcp-456",
		"disposition":  store.CheckpointDispositionSkipped,
		"reason":       store.CheckpointSkipReasonNoDurableKnowledge,
	}

	createdResponse, err := record(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: arguments}})
	if err != nil || createdResponse.IsError {
		t.Fatalf("record response = %#v, err = %v", createdResponse, err)
	}
	var created memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(callResultText(t, createdResponse)), &created); err != nil {
		t.Fatalf("decode record response: %v", err)
	}
	if created.Idempotency != memoryops.CheckpointIdempotencyCreated || created.Checkpoint == nil {
		t.Fatalf("created result = %#v", created)
	}

	replayedResponse, err := record(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: arguments}})
	if err != nil || replayedResponse.IsError {
		t.Fatalf("replay response = %#v, err = %v", replayedResponse, err)
	}
	var replayed memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(callResultText(t, replayedResponse)), &replayed); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayed.Idempotency != memoryops.CheckpointIdempotencyAlreadyRecorded ||
		!reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("replayed result = %#v, want checkpoint %#v", replayed, created.Checkpoint)
	}

	statusResponse, err := status(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"host":         arguments["host"],
		"session_id":   arguments["session_id"],
		"root_turn_id": arguments["root_turn_id"],
	}}})
	if err != nil || statusResponse.IsError {
		t.Fatalf("status response = %#v, err = %v", statusResponse, err)
	}
	var statusResult memoryops.CheckpointStatusResult
	if err := json.Unmarshal([]byte(callResultText(t, statusResponse)), &statusResult); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if !reflect.DeepEqual(statusResult.Checkpoint, created.Checkpoint) {
		t.Fatalf("status checkpoint = %#v, want %#v", statusResult.Checkpoint, created.Checkpoint)
	}

	invalidArguments := make(map[string]any, len(arguments))
	for key, value := range arguments {
		invalidArguments[key] = value
	}
	invalidArguments["root_turn_id"] = "turn-mcp-invalid"
	invalidArguments["reason"] = "processing_failed"
	invalidResponse, err := record(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: invalidArguments}})
	if err != nil || !invalidResponse.IsError {
		t.Fatalf("invalid response = %#v, err = %v", invalidResponse, err)
	}
	var invalidEnvelope map[string]any
	if err := json.Unmarshal([]byte(callResultText(t, invalidResponse)), &invalidEnvelope); err != nil {
		t.Fatalf("decode invalid response: %v", err)
	}
	if invalidEnvelope["code"] != memoryops.CheckpointErrorCodeInvalidReason {
		t.Fatalf("invalid envelope = %#v", invalidEnvelope)
	}
}

func TestCheckpointObserverReportsConflictWithoutMemoryOrProposalPayload(t *testing.T) {
	s := newMCPTestStore(t)
	identity := map[string]any{
		"host": "codex", "session_id": "session-observed", "root_turn_id": "turn-observed",
	}
	initial := map[string]any{
		"host": identity["host"], "session_id": identity["session_id"], "root_turn_id": identity["root_turn_id"],
		"disposition": store.CheckpointDispositionSkipped, "reason": store.CheckpointSkipReasonNoDurableKnowledge,
	}
	created, err := CheckpointToolHandler(s)(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: initial}})
	if err != nil || created.IsError {
		t.Fatalf("initial checkpoint response = %#v, err = %v", created, err)
	}

	var observed []CheckpointObservation
	conflicting := map[string]any{
		"host": identity["host"], "session_id": identity["session_id"], "root_turn_id": identity["root_turn_id"],
		"disposition": store.CheckpointDispositionNeedsReview,
		"project":     "engram",
		"proposal": map[string]any{
			"title": "PROPOSAL-TITLE-MUST-NOT-REACH-OBSERVER", "content": "PROPOSAL-CONTENT-MUST-NOT-REACH-OBSERVER",
		},
	}
	response, err := CheckpointToolHandlerWithObserver(s, func(value CheckpointObservation) {
		observed = append(observed, value)
	})(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: conflicting}})
	if err != nil || !response.IsError {
		t.Fatalf("conflict response = %#v, err = %v", response, err)
	}
	if len(observed) != 1 || observed[0].Host != "codex" || observed[0].SessionID != "session-observed" || observed[0].RootTurnID != "turn-observed" || observed[0].Outcome != CheckpointConflict {
		t.Fatalf("checkpoint observations = %+v", observed)
	}
	encoded, err := json.Marshal(observed)
	if err != nil {
		t.Fatalf("json.Marshal(observed): %v", err)
	}
	if strings.Contains(string(encoded), "PROPOSAL-") {
		t.Fatalf("checkpoint observer leaked proposal payload: %s", encoded)
	}
}

func TestRegisteredCheckpointWriteReturnsJSONForQueueCancellation(t *testing.T) {
	s := newMCPTestStore(t)
	srv := NewServerWithTools(s, map[string]bool{"mem_checkpoint": true})
	tool := srv.ListTools()["mem_checkpoint"]
	if tool == nil {
		t.Fatal("registered mem_checkpoint tool not found")
	}

	identity := store.CheckpointIdentity{
		Host: "codex", SessionID: "session-mcp-canceled", RootTurnID: "turn-mcp-canceled",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response, err := tool.Handler(ctx, mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"host":         identity.Host,
		"session_id":   identity.SessionID,
		"root_turn_id": identity.RootTurnID,
		"disposition":  store.CheckpointDispositionNeedsReview,
		"project":      "engram",
		"proposal": map[string]any{
			"type": "decision", "title": "Canceled proposal", "content": "This must not persist.",
			"scope": "project", "category": "decision",
		},
	}}})
	if err != nil || !response.IsError {
		t.Fatalf("canceled response = %#v, err = %v", response, err)
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(callResultText(t, response)), &envelope); err != nil {
		t.Fatalf("decode canceled response: %v", err)
	}
	if envelope["code"] != memoryops.CheckpointErrorCodeFailed {
		t.Fatalf("canceled envelope = %#v", envelope)
	}
	if _, err := s.GetMemoryCheckpoint(identity); !errors.Is(err, store.ErrCheckpointNotFound) {
		t.Fatalf("canceled write persisted checkpoint: %v", err)
	}
	var proposals int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM memory_proposals`).Scan(&proposals); err != nil {
		t.Fatalf("count proposals after canceled write: %v", err)
	}
	if proposals != 0 {
		t.Fatalf("canceled write persisted %d proposals", proposals)
	}
}

func TestCheckpointToolCreatesNeedsReviewProposalAndExposesReference(t *testing.T) {
	s := newMCPTestStore(t)
	record := CheckpointToolHandler(s)
	arguments := map[string]any{
		"host":         "codex",
		"session_id":   "session-mcp-needs-review",
		"root_turn_id": "turn-mcp-needs-review",
		"disposition":  store.CheckpointDispositionNeedsReview,
		"project":      "engram",
		"proposal": map[string]any{
			"title":   "Review MCP proposal",
			"content": "This proposal must remain local until review.",
		},
	}

	response, err := record(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: arguments}})
	if err != nil || response.IsError {
		t.Fatalf("record response = %#v, err = %v", response, err)
	}
	var created memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(callResultText(t, response)), &created); err != nil {
		t.Fatalf("decode record response: %v", err)
	}
	if created.Checkpoint == nil || created.Checkpoint.Proposal == nil ||
		created.Checkpoint.Proposal.ID == "" || created.Checkpoint.Proposal.Project != "engram" ||
		created.Checkpoint.Proposal.Title != "Review MCP proposal" ||
		created.Checkpoint.Proposal.Content != "This proposal must remain local until review." ||
		created.Checkpoint.Proposal.CreatedAt == "" {
		t.Fatalf("created result = %#v", created)
	}
}

func TestCheckpointToolPreflightIsReadOnlyAndDoesNotReportTerminalCoverage(t *testing.T) {
	s := newMCPTestStore(t)
	service := memoryops.New(s)
	seed, err := service.Save(memoryops.SaveInput{
		SessionID: "session-mcp-preflight-seed", CWD: "/work/engram", Project: "engram",
		Type: "decision", Title: "MCP preflight duplicate", Content: "Reuse this exact Memory.",
		CandidateOptions: store.CandidateOptions{SkipInsert: true},
	})
	if err != nil || seed.Observation == nil {
		t.Fatalf("seed preflight Memory: result=%#v err=%v", seed, err)
	}
	var observationsBefore, checkpointsBefore, proposalsBefore, mutationsBefore int
	for table, target := range map[string]*int{
		"observations": &observationsBefore, "memory_checkpoints": &checkpointsBefore,
		"memory_proposals": &proposalsBefore, "sync_mutations": &mutationsBefore,
	} {
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(target); err != nil {
			t.Fatalf("count %s before preflight: %v", table, err)
		}
	}

	var observations []CheckpointObservation
	response, err := CheckpointToolHandlerWithObserver(s, func(value CheckpointObservation) {
		observations = append(observations, value)
	})(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"operation": "preflight",
		"project":   "engram",
		"memories": []any{map[string]any{
			"type": "decision", "title": "MCP preflight duplicate", "content": "Reuse this exact Memory.",
		}},
	}}})
	if err != nil || response.IsError {
		t.Fatalf("preflight response = %#v, err = %v", response, err)
	}
	var result memoryops.CheckpointPreflightResult
	if err := json.Unmarshal([]byte(callResultText(t, response)), &result); err != nil {
		t.Fatalf("decode preflight response: %v", err)
	}
	if len(result.ExactDuplicates) != 1 || result.ExactDuplicates[0].Reference.MemoryID != seed.Observation.ID ||
		len(result.Candidates) != 0 || result.CandidateLimit != 3 {
		t.Fatalf("preflight result = %#v", result)
	}
	if len(observations) != 0 {
		t.Fatalf("preflight reported terminal checkpoint coverage: %+v", observations)
	}
	invalidResponse, err := CheckpointToolHandlerWithObserver(s, func(value CheckpointObservation) {
		observations = append(observations, value)
	})(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"operation": "preflight", "project": "engram", "memories": "not-an-array",
	}}})
	if err != nil || !invalidResponse.IsError || len(observations) != 0 {
		t.Fatalf("invalid preflight response=%#v err=%v terminal observations=%+v", invalidResponse, err, observations)
	}

	for table, want := range map[string]int{
		"observations": observationsBefore, "memory_checkpoints": checkpointsBefore,
		"memory_proposals": proposalsBefore, "sync_mutations": mutationsBefore,
	} {
		var got int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatalf("count %s after preflight: %v", table, err)
		}
		if got != want {
			t.Fatalf("preflight changed %s: %d -> %d", table, want, got)
		}
	}
}

func TestCheckpointToolCreatesMixedMemoryCheckpoint(t *testing.T) {
	s := newMCPTestStore(t)
	service := memoryops.New(s)
	seed, err := service.Save(memoryops.SaveInput{
		SessionID: "session-mcp-mixed-seed", CWD: "/work/engram", Project: "engram",
		Type: "decision", Title: "MCP settled Memory", Content: "This result is settled.",
		CandidateOptions: store.CandidateOptions{SkipInsert: true},
	})
	if err != nil || seed.Observation == nil {
		t.Fatalf("seed Mixed Memory: result=%#v err=%v", seed, err)
	}

	response, err := CheckpointToolHandler(s)(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"host": "codex", "session_id": "session-mcp-mixed", "root_turn_id": "turn-mcp-mixed",
		"disposition": store.CheckpointDispositionNeedsReview, "project": "engram",
		"memory_ids": []any{float64(seed.Observation.ID)},
		"memories": []any{map[string]any{
			"type": "discovery", "title": "MCP settled discovery", "content": "This result is also settled.",
		}},
		"proposal": map[string]any{
			"title": "MCP unresolved conflict", "content": "This result still needs review.",
		},
	}}})
	if err != nil || response.IsError {
		t.Fatalf("Mixed Memory response = %#v, err = %v", response, err)
	}
	var result memoryops.CheckpointRecordResult
	if err := json.Unmarshal([]byte(callResultText(t, response)), &result); err != nil {
		t.Fatalf("decode Mixed Memory response: %v", err)
	}
	if result.Checkpoint == nil || result.Checkpoint.Proposal == nil || len(result.Checkpoint.References) != 2 ||
		result.Checkpoint.References[0].MemoryID != seed.Observation.ID {
		t.Fatalf("Mixed Memory result = %#v", result)
	}
}

func TestCheckpointToolSchemaExposesOnlyMinimalInlineNeedsReviewProposal(t *testing.T) {
	s := newMCPTestStore(t)
	tool := NewServerWithTools(s, map[string]bool{"mem_checkpoint": true}).GetTool("mem_checkpoint")
	if tool == nil {
		t.Fatal("mem_checkpoint not registered")
	}
	if _, ok := tool.Tool.InputSchema.Properties["proposal_id"]; ok {
		t.Fatal("mem_checkpoint schema still exposes proposal_id")
	}
	proposal, ok := tool.Tool.InputSchema.Properties["proposal"].(map[string]any)
	if !ok {
		t.Fatalf("mem_checkpoint proposal schema = %#v", tool.Tool.InputSchema.Properties["proposal"])
	}
	properties, ok := proposal["properties"].(map[string]any)
	if !ok || len(properties) != 2 || properties["title"] == nil || properties["content"] == nil {
		t.Fatalf("mem_checkpoint proposal properties = %#v", proposal["properties"])
	}
	if proposal["additionalProperties"] != false || proposal["minProperties"] != 2 || proposal["maxProperties"] != 2 {
		t.Fatalf("mem_checkpoint proposal bounds = %#v", proposal)
	}
	if !strings.Contains(tool.Tool.Description, store.CheckpointDispositionNeedsReview) {
		t.Fatalf("mem_checkpoint description = %q", tool.Tool.Description)
	}
	if _, ok := tool.Tool.InputSchema.Properties["operation"]; !ok {
		t.Fatalf("mem_checkpoint schema does not expose read-only preflight operation")
	}
}
