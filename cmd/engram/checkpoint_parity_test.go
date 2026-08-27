package main

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	engrammcp "github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCheckpointCLIAndMCPParity(t *testing.T) {
	cfg := testConfig(t)
	cliIdentity := checkpointParityIdentity{"codex", "session-cli-parity", "turn-cli-parity"}
	mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-parity", "turn-mcp-parity"}

	cliCreated := runCheckpointCLIRecord(t, cfg, cliIdentity, store.CheckpointSkipReasonNoDurableKnowledge)
	s := openCheckpointParityStore(t, cfg)
	mcpRecord := engrammcp.CheckpointToolHandler(s)
	mcpStatus := engrammcp.CheckpointStatusToolHandler(s)
	mcpCreated := callCheckpointMCP(t, mcpRecord, checkpointParityRecordArguments(mcpIdentity, store.CheckpointSkipReasonNoDurableKnowledge), false)
	assertCheckpointParity(t, "created", cliCreated, mcpCreated)

	cliReplayed := runCheckpointCLIRecord(t, cfg, cliIdentity, store.CheckpointSkipReasonNoDurableKnowledge)
	mcpReplayed := callCheckpointMCP(t, mcpRecord, checkpointParityRecordArguments(mcpIdentity, store.CheckpointSkipReasonNoDurableKnowledge), false)
	assertCheckpointParity(t, "replayed", cliReplayed, mcpReplayed)

	cliStatus := runCheckpointCLIStatus(t, cfg, cliIdentity)
	mcpStatusResult := callCheckpointMCP(t, mcpStatus, checkpointParityIdentityArguments(mcpIdentity), false)
	assertCheckpointParity(t, "status", cliStatus, mcpStatusResult)

	invalidCLIIdentity := checkpointParityIdentity{"codex", "session-cli-invalid", "turn-cli-invalid"}
	invalidMCPIdentity := checkpointParityIdentity{"codex", "session-mcp-invalid", "turn-mcp-invalid"}
	cliInvalid := runCheckpointCLIRecordError(t, cfg, invalidCLIIdentity, "processing_failed")
	mcpInvalid := callCheckpointMCP(t, mcpRecord, checkpointParityRecordArguments(invalidMCPIdentity, "processing_failed"), true)
	if !reflect.DeepEqual(cliInvalid, mcpInvalid) {
		t.Fatalf("validation envelopes differ\nCLI=%#v\nMCP=%#v", cliInvalid, mcpInvalid)
	}

	missingCLIIdentity := checkpointParityIdentity{"", "session-cli-missing", "turn-cli-missing"}
	missingMCPIdentity := checkpointParityIdentity{"", "session-mcp-missing", "turn-mcp-missing"}
	cliMissing := runCheckpointCLIRecordError(t, cfg, missingCLIIdentity, store.CheckpointSkipReasonNoDurableKnowledge)
	mcpMissing := callCheckpointMCP(t, mcpRecord, checkpointParityRecordArguments(missingMCPIdentity, store.CheckpointSkipReasonNoDurableKnowledge), true)
	if !reflect.DeepEqual(cliMissing, mcpMissing) {
		t.Fatalf("identity validation envelopes differ\nCLI=%#v\nMCP=%#v", cliMissing, mcpMissing)
	}

	notFoundCLIIdentity := checkpointParityIdentity{"codex", "session-cli-not-found", "turn-cli-not-found"}
	notFoundMCPIdentity := checkpointParityIdentity{"codex", "session-mcp-not-found", "turn-mcp-not-found"}
	cliNotFound := runCheckpointCLIStatusError(t, cfg, notFoundCLIIdentity)
	mcpNotFound := callCheckpointMCP(t, mcpStatus, checkpointParityIdentityArguments(notFoundMCPIdentity), true)
	if !reflect.DeepEqual(cliNotFound, mcpNotFound) {
		t.Fatalf("not-found envelopes differ\nCLI=%#v\nMCP=%#v", cliNotFound, mcpNotFound)
	}
}

func TestCheckpointCLIAndMCPParityForLeadingDashIdentities(t *testing.T) {
	cfg := testConfig(t)
	cliIdentity := checkpointParityIdentity{"--codex-cli", "--help", "-h-cli"}
	mcpIdentity := checkpointParityIdentity{"--codex-mcp", "--help", "-h-mcp"}

	cliCreated := runCheckpointCLIRecord(t, cfg, cliIdentity, store.CheckpointSkipReasonNoDurableKnowledge)
	s := openCheckpointParityStore(t, cfg)
	mcpRecord := engrammcp.CheckpointToolHandler(s)
	mcpCreated := callCheckpointMCP(t, mcpRecord, checkpointParityRecordArguments(mcpIdentity, store.CheckpointSkipReasonNoDurableKnowledge), false)
	assertCheckpointParity(t, "leading-dash created", cliCreated, mcpCreated)

	cliStatus := runCheckpointCLIStatus(t, cfg, cliIdentity)
	mcpStatus := callCheckpointMCP(t, engrammcp.CheckpointStatusToolHandler(s), checkpointParityIdentityArguments(mcpIdentity), false)
	assertCheckpointParity(t, "leading-dash status", cliStatus, mcpStatus)
}

type checkpointParityIdentity struct {
	host       string
	sessionID  string
	rootTurnID string
}

func runCheckpointCLIRecord(t *testing.T, cfg store.Config, identity checkpointParityIdentity, reason string) map[string]any {
	t.Helper()
	withArgs(t, "engram", "checkpoint", "record",
		"--host="+identity.host,
		"--session-id="+identity.sessionID,
		"--root-turn-id="+identity.rootTurnID,
		"--disposition="+store.CheckpointDispositionSkipped,
		"--reason="+reason,
		"--json",
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI record stderr = %q", stderr)
	}
	return decodeCLIJSON(t, stdout)
}

func runCheckpointCLIStatus(t *testing.T, cfg store.Config, identity checkpointParityIdentity) map[string]any {
	t.Helper()
	withArgs(t, "engram", "checkpoint", "status",
		"--host="+identity.host,
		"--session-id="+identity.sessionID,
		"--root-turn-id="+identity.rootTurnID,
		"--json",
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI status stderr = %q", stderr)
	}
	return decodeCLIJSON(t, stdout)
}

func runCheckpointCLIRecordError(t *testing.T, cfg store.Config, identity checkpointParityIdentity, reason string) map[string]any {
	t.Helper()
	stubExitWithPanic(t)
	withArgs(t, "engram", "checkpoint", "record",
		"--host", identity.host,
		"--session-id", identity.sessionID,
		"--root-turn-id", identity.rootTurnID,
		"--disposition", store.CheckpointDispositionSkipped,
		"--reason", reason,
		"--json",
	)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if stdout != "" {
		t.Fatalf("CLI error stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("CLI error exit = %v", recovered)
	}
	return decodeCLIJSON(t, stderr)
}

func runCheckpointCLIStatusError(t *testing.T, cfg store.Config, identity checkpointParityIdentity) map[string]any {
	t.Helper()
	stubExitWithPanic(t)
	withArgs(t, "engram", "checkpoint", "status",
		"--host", identity.host,
		"--session-id", identity.sessionID,
		"--root-turn-id", identity.rootTurnID,
		"--json",
	)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if stdout != "" {
		t.Fatalf("CLI status error stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("CLI status error exit = %v", recovered)
	}
	return decodeCLIJSON(t, stderr)
}

func openCheckpointParityStore(t *testing.T, cfg store.Config) *store.Store {
	t.Helper()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open parity store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func checkpointParityRecordArguments(identity checkpointParityIdentity, reason string) map[string]any {
	arguments := checkpointParityIdentityArguments(identity)
	arguments["disposition"] = store.CheckpointDispositionSkipped
	arguments["reason"] = reason
	return arguments
}

func checkpointParityIdentityArguments(identity checkpointParityIdentity) map[string]any {
	return map[string]any{
		"host":         identity.host,
		"session_id":   identity.sessionID,
		"root_turn_id": identity.rootTurnID,
	}
}

func callCheckpointMCP(t *testing.T, handler func(context.Context, mcppkg.CallToolRequest) (*mcppkg.CallToolResult, error), arguments map[string]any, wantError bool) map[string]any {
	t.Helper()
	result, err := handler(context.Background(), mcppkg.CallToolRequest{Params: mcppkg.CallToolParams{Arguments: arguments}})
	if err != nil {
		t.Fatalf("MCP handler: %v", err)
	}
	if result.IsError != wantError {
		t.Fatalf("MCP IsError = %t, want %t", result.IsError, wantError)
	}
	text, ok := mcppkg.AsTextContent(result.Content[0])
	if !ok {
		t.Fatal("MCP result did not contain text")
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(text.Text), &envelope); err != nil {
		t.Fatalf("decode MCP JSON: %v\n%s", err, text.Text)
	}
	return envelope
}

func assertCheckpointParity(t *testing.T, label string, cli, mcp map[string]any) {
	t.Helper()
	if got, want := normalizedCheckpointEnvelope(cli), normalizedCheckpointEnvelope(mcp); !reflect.DeepEqual(got, want) {
		t.Fatalf("%s envelopes differ\nCLI=%#v\nMCP=%#v", label, got, want)
	}
}

func normalizedCheckpointEnvelope(envelope map[string]any) map[string]any {
	normalized := map[string]any{}
	if idempotency, ok := envelope["idempotency"]; ok {
		normalized["idempotency"] = idempotency
	}
	checkpoint, _ := envelope["checkpoint"].(map[string]any)
	normalized["checkpoint"] = map[string]any{
		"disposition":    checkpoint["disposition"],
		"reason_code":    checkpoint["reason_code"],
		"reason_version": checkpoint["reason_version"],
	}
	return normalized
}
