package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	engrammcp "github.com/yersonargotev/engram/internal/mcp"
)

func TestCheckpointCLIRejectsMalformedSupersession(t *testing.T) {
	stubExitWithPanic(t)
	withArgs(t, "engram", "checkpoint", "record", "--host", "codex", "--session-id", "supersession", "--root-turn-id", "malformed", "--disposition", "skipped", "--reason", "no_durable_knowledge", "--supersession-json", `{"unexpected":true}`, "--json")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(testConfig(t)) })
	if recovered == nil || !strings.Contains(stderr, "invalid_checkpoint_supersession") {
		t.Fatalf("stderr=%s recovered=%v", stderr, recovered)
	}
}

func TestCheckpointCLIExplicitSupersessionAndIdentityFirstReplay(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	run := func(args ...string) map[string]any {
		t.Helper()
		withArgs(t, append([]string{"engram", "checkpoint"}, args...)...)
		stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
		if recovered != nil || stderr != "" {
			t.Fatalf("command failed: %s %v", stderr, recovered)
		}
		var result map[string]any
		if err := json.Unmarshal([]byte(stdout), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	identity := []string{"record", "--host", "codex", "--session-id", "supersession", "--root-turn-id", "old", "--disposition", "saved", "--project", "engram", "--json"}
	old := run(append(identity, "--memory-json", `{"title":"Synthetic cache diagnosis","content":"Synthetic cache failure reproduces at commit abc."}`)...)
	oldID := old["checkpoint"].(map[string]any)["references"].([]any)[0].(map[string]any)["memory_id"]
	memory := `{"title":"Synthetic cache diagnosis resolution","content":"Synthetic cache failure fixed at commit def; replaces diagnosis for current guidance."}`
	preflight := run("preflight", "--project", "engram", "--memory-json", memory, "--json")
	candidates := preflight["candidates"].([]any)
	if len(candidates) == 0 {
		t.Fatal("expected reviewed target candidate")
	}
	version := candidates[0].(map[string]any)["target_version"]
	if version == nil || version == "" {
		t.Fatalf("missing target version: %#v", preflight)
	}
	identity[6] = "new"
	declaration := fmt.Sprintf(`{"replacement_input_index":0,"target_memory_id":%.0f,"target_version":%q,"reason":"Verified current guidance at def"}`, oldID, version)
	withArgs(t, append([]string{"engram", "checkpoint"}, append(identity, "--memory-json", memory, "--supersession-json", strings.Replace(declaration, fmt.Sprint(version), "stale", 1))...)...)
	_, staleError, staleExit := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if staleExit == nil || !strings.Contains(staleError, "checkpoint_supersession_stale") {
		t.Fatalf("stale target: %s %v", staleError, staleExit)
	}
	created := run(append(identity, "--memory-json", memory, "--supersession-json", declaration)...)
	for _, payload := range []string{"{", strings.Replace(declaration, fmt.Sprint(version), "stale", 1)} {
		replay := run(append(identity, "--supersession-json", payload)...)
		if replay["idempotency"] != "already_recorded" || !reflect.DeepEqual(replay["checkpoint"], created["checkpoint"]) {
			t.Fatalf("replay=%#v", replay)
		}
	}
	identity[8] = "skipped"
	withArgs(t, append([]string{"engram", "checkpoint"}, append(identity, "--supersession-json", "{")...)...)
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if recovered == nil || !strings.Contains(stderr, "checkpoint_conflict") {
		t.Fatalf("changed disposition: %s %v", stderr, recovered)
	}
}

func TestCheckpointSupersessionCLIAndMCPRejectMalformedDeclarations(t *testing.T) {
	for _, payload := range []string{
		`{"unknown":true}`, `null`, `[]`, `{"replacement_input_index":0.5}`,
		`{"replacement_input_index":0,"replacement_memory_id":2,"target_memory_id":1,"target_version":"v","reason":"reviewed"}`,
		`{"replacement_input_index":0,"target_memory_id":1,"target_version":"v","reason":""}`,
	} {
		t.Run(payload, func(t *testing.T) {
			cfg := testConfig(t)
			var declaration any
			if err := json.Unmarshal([]byte(payload), &declaration); err != nil {
				t.Fatal(err)
			}
			identity := checkpointParityIdentity{"codex", "malformed", "declaration"}
			stubExitWithPanic(t)
			withArgs(t, "engram", "checkpoint", "record", "--host=codex", "--session-id=malformed", "--root-turn-id=declaration", "--disposition=saved", "--project=engram", "--memory-json="+`{"title":"Replacement","content":"Reviewed replacement"}`, "--supersession-json="+payload, "--json")
			_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
			if recovered == nil {
				t.Fatal("expected CLI rejection")
			}
			cli := decodeCLIJSON(t, stderr)
			s := openCheckpointParityStore(t, cfg)
			args := checkpointParityIdentityArguments(identity)
			args["disposition"] = "saved"
			args["project"] = "engram"
			args["supersessions"] = []any{declaration}
			args["memories"] = []any{map[string]any{"title": "Replacement", "content": "Reviewed replacement"}}
			mcp := callCheckpointMCP(t, engrammcp.CheckpointToolHandler(s), args, true)
			if cli["code"] != "invalid_checkpoint_supersession" || cli["code"] != mcp["code"] {
				t.Fatalf("CLI=%#v MCP=%#v", cli, mcp)
			}
		})
	}
}

func TestCheckpointCLISupersessionIsRecordOnly(t *testing.T) {
	for _, action := range []string{"preflight", "status", "verify-stop"} {
		t.Run(action, func(t *testing.T) {
			stubExitWithPanic(t)
			withArgs(t, "engram", "checkpoint", action, "--supersession-json", `{}`, "--json")
			_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(testConfig(t)) })
			if recovered == nil || !strings.Contains(stderr, "invalid_checkpoint_supersession") {
				t.Fatalf("error=%s %v", stderr, recovered)
			}
		})
	}
}
