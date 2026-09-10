package mcp

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
)

func TestCheckpointRejectsMalformedSupersession(t *testing.T) {
	s := newMCPTestStore(t)
	result, err := CheckpointToolHandler(s)(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{
		"host": "codex", "session_id": "supersession", "root_turn_id": "malformed", "disposition": "skipped", "reason": "no_durable_knowledge",
		"supersessions": []any{map[string]any{"unexpected": true}},
	}}})
	if err != nil || !result.IsError || !strings.Contains(callResultText(t, result), "invalid_checkpoint_supersession") {
		t.Fatalf("result=%#v err=%v text=%s", result, err, callResultText(t, result))
	}
}

func TestCheckpointMCPSupersessionReplayAndRecordOnlyFields(t *testing.T) {
	s := newMCPTestStore(t)
	handler := CheckpointToolHandler(s)
	args := map[string]any{"host": "codex", "session_id": "supersession", "root_turn_id": "old", "disposition": "saved", "project": "test",
		"memories": []any{map[string]any{"title": "Synthetic cache diagnosis", "content": "Synthetic cache failure reproduces at commit abc."}}}
	call := func() map[string]any {
		t.Helper()
		result, err := handler(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: args}})
		if err != nil || result.IsError {
			t.Fatalf("call: %v %s", err, callResultText(t, result))
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(callResultText(t, result)), &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	old := call()
	oldID := old["checkpoint"].(map[string]any)["references"].([]any)[0].(map[string]any)["memory_id"]
	memory := []any{map[string]any{"title": "Synthetic cache diagnosis resolution", "content": "Synthetic cache failure fixed at commit def; replaces diagnosis for current guidance."}}
	args = map[string]any{"operation": "preflight", "project": "test", "memories": memory}
	preflight := call()
	candidates := preflight["candidates"].([]any)
	if len(candidates) == 0 {
		t.Fatal("expected target candidate")
	}
	version := candidates[0].(map[string]any)["target_version"]
	if version == nil || version == "" {
		t.Fatalf("missing target version: %#v", preflight)
	}
	declaration := map[string]any{"replacement_input_index": 0, "target_memory_id": oldID, "target_version": version, "reason": "Verified guidance at def"}
	args = map[string]any{"host": "codex", "session_id": "supersession", "root_turn_id": "new", "disposition": "saved", "project": "test", "memories": memory, "supersessions": []any{declaration}}
	declaration["target_version"] = "stale"
	staleResult, staleErr := handler(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: args}})
	if staleErr != nil || !staleResult.IsError || !strings.Contains(callResultText(t, staleResult), "checkpoint_supersession_stale") {
		t.Fatalf("stale: %#v %v", staleResult, staleErr)
	}
	declaration["target_version"] = version
	created := call()
	for _, payload := range []any{"malformed", []any{map[string]any{"replacement_input_index": 0, "target_memory_id": oldID, "target_version": "stale", "reason": "retry"}}} {
		args["supersessions"] = payload
		replay := call()
		if replay["idempotency"] != "already_recorded" || !reflect.DeepEqual(replay["checkpoint"], created["checkpoint"]) {
			t.Fatalf("replay=%#v", replay)
		}
	}
	args["disposition"] = "skipped"
	result, err := handler(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: args}})
	if err != nil || !result.IsError || !strings.Contains(callResultText(t, result), "checkpoint_conflict") {
		t.Fatalf("changed disposition=%#v %v", result, err)
	}
	for _, operation := range []string{"preflight", "status"} {
		req := mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: map[string]any{"operation": operation, "supersessions": []any{}}}}
		selected := handler
		if operation == "status" {
			selected = CheckpointStatusToolHandler(s)
		}
		result, err := selected(context.Background(), req)
		if err != nil || !result.IsError || !strings.Contains(callResultText(t, result), "invalid_checkpoint_supersession") {
			t.Fatalf("%s result=%#v %v", operation, result, err)
		}
	}
}
