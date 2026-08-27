package main

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestCheckpointCLIAndMCPParityForSavedExistingMemories(t *testing.T) {
	cfg := testConfig(t)
	memoryIDs := seedCheckpointParityMemories(t, cfg, "engram", 2)
	cliIdentity := checkpointParityIdentity{"codex", "session-cli-saved", "turn-cli-saved"}
	mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-saved", "turn-mcp-saved"}

	cliCreated := runCheckpointCLIRecordSaved(t, cfg, cliIdentity, "engram", memoryIDs)
	s := openCheckpointParityStore(t, cfg)
	mcpRecord := engrammcp.CheckpointToolHandler(s)
	mcpCreated := callCheckpointMCP(t, mcpRecord, checkpointParitySavedArguments(mcpIdentity, "engram", memoryIDs), false)
	assertCheckpointParity(t, "saved created", cliCreated, mcpCreated)

	cliReplayed := runCheckpointCLIRecordSaved(t, cfg, cliIdentity, "engram", memoryIDs)
	mcpReplayed := callCheckpointMCP(t, mcpRecord, checkpointParitySavedArguments(mcpIdentity, "engram", memoryIDs), false)
	assertCheckpointParity(t, "saved replayed", cliReplayed, mcpReplayed)

	cliStatus := runCheckpointCLIStatus(t, cfg, cliIdentity)
	mcpStatus := callCheckpointMCP(t, engrammcp.CheckpointStatusToolHandler(s), checkpointParityIdentityArguments(mcpIdentity), false)
	assertCheckpointParity(t, "saved status", cliStatus, mcpStatus)
}

func TestCheckpointCLIAndMCPParityForAtomicallyCreatedMemories(t *testing.T) {
	cfg := testConfig(t)
	memories := []map[string]any{
		{"type": "decision", "title": "Created parity decision", "content": "Create this decision atomically."},
		{"type": "discovery", "title": "Created parity discovery", "content": "Create this discovery atomically."},
	}
	cliIdentity := checkpointParityIdentity{"codex", "session-cli-created", "turn-cli-created"}
	mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-created", "turn-mcp-created"}

	cliCreated := runCheckpointCLIRecordCreated(t, cfg, cliIdentity, "engram", memories)
	s := openCheckpointParityStore(t, cfg)
	mcpRecord := engrammcp.CheckpointToolHandler(s)
	mcpArguments := checkpointParityIdentityArguments(mcpIdentity)
	mcpArguments["disposition"] = store.CheckpointDispositionSaved
	mcpArguments["project"] = "engram"
	mcpArguments["memories"] = memories
	mcpCreated := callCheckpointMCP(t, mcpRecord, mcpArguments, false)
	if got, want := normalizedCreatedCheckpointEnvelope(cliCreated), normalizedCreatedCheckpointEnvelope(mcpCreated); !reflect.DeepEqual(got, want) {
		t.Fatalf("created Memory envelopes differ\nCLI=%#v\nMCP=%#v", got, want)
	}

	var memoriesBeforeReplay int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&memoriesBeforeReplay); err != nil {
		t.Fatalf("count created Memories: %v", err)
	}
	cliReplayed := runCheckpointCLIRecordCreated(t, cfg, cliIdentity, "engram", memories)
	mcpReplayed := callCheckpointMCP(t, mcpRecord, mcpArguments, false)
	if got, want := normalizedCreatedCheckpointEnvelope(cliReplayed), normalizedCreatedCheckpointEnvelope(mcpReplayed); !reflect.DeepEqual(got, want) {
		t.Fatalf("created Memory replay envelopes differ\nCLI=%#v\nMCP=%#v", got, want)
	}
	var memoriesAfterReplay int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&memoriesAfterReplay); err != nil {
		t.Fatalf("count Memories after replay: %v", err)
	}
	if memoriesAfterReplay != memoriesBeforeReplay {
		t.Fatalf("replay created duplicate Memories: %d -> %d", memoriesBeforeReplay, memoriesAfterReplay)
	}
}

func TestCheckpointCLIAndMCPParityForInvalidSavedReferences(t *testing.T) {
	cfg := testConfig(t)
	cliIdentity := checkpointParityIdentity{"codex", "session-cli-invalid-saved", "turn-cli-invalid-saved"}
	mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-invalid-saved", "turn-mcp-invalid-saved"}

	cliError := runCheckpointCLIRecordSavedError(t, cfg, cliIdentity, "engram", []int64{9999})
	s := openCheckpointParityStore(t, cfg)
	mcpError := callCheckpointMCP(t, engrammcp.CheckpointToolHandler(s), checkpointParitySavedArguments(mcpIdentity, "engram", []int64{9999}), true)
	if !reflect.DeepEqual(cliError, mcpError) {
		t.Fatalf("saved reference errors differ\nCLI=%#v\nMCP=%#v", cliError, mcpError)
	}
}

func TestCheckpointCLIAndMCPParityForMalformedSavedReferences(t *testing.T) {
	cfg := testConfig(t)
	s := openCheckpointParityStore(t, cfg)
	mcpRecord := engrammcp.CheckpointToolHandler(s)
	tests := []struct {
		name         string
		cliFlag      string
		mcpField     string
		mcpValue     any
		identityBase string
	}{
		{name: "non-integer Memory ID", cliFlag: "--memory-id=abc", mcpField: "memory_ids", mcpValue: []any{"abc"}, identityBase: "bad-id"},
		{name: "malformed inline Memory", cliFlag: "--memory-json=not-json", mcpField: "memories", mcpValue: "not-an-array", identityBase: "bad-memory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cliIdentity := checkpointParityIdentity{"codex", "session-cli-" + tt.identityBase, "turn-cli-" + tt.identityBase}
			mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-" + tt.identityBase, "turn-mcp-" + tt.identityBase}
			cliError := runCheckpointCLIArgumentError(t, cfg, cliIdentity, tt.cliFlag)
			mcpArguments := checkpointParityIdentityArguments(mcpIdentity)
			mcpArguments["disposition"] = store.CheckpointDispositionSaved
			mcpArguments["project"] = "engram"
			mcpArguments[tt.mcpField] = tt.mcpValue
			mcpError := callCheckpointMCP(t, mcpRecord, mcpArguments, true)
			if !reflect.DeepEqual(cliError, mcpError) {
				t.Fatalf("malformed reference errors differ\nCLI=%#v\nMCP=%#v", cliError, mcpError)
			}
		})
	}
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

func runCheckpointCLIRecordSaved(t *testing.T, cfg store.Config, identity checkpointParityIdentity, project string, memoryIDs []int64) map[string]any {
	t.Helper()
	args := []string{
		"engram", "checkpoint", "record",
		"--host=" + identity.host,
		"--session-id=" + identity.sessionID,
		"--root-turn-id=" + identity.rootTurnID,
		"--disposition=" + store.CheckpointDispositionSaved,
		"--project=" + project,
	}
	for _, memoryID := range memoryIDs {
		args = append(args, fmt.Sprintf("--memory-id=%d", memoryID))
	}
	args = append(args, "--json")
	withArgs(t, args...)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI saved record stderr = %q", stderr)
	}
	return decodeCLIJSON(t, stdout)
}

func runCheckpointCLIRecordCreated(t *testing.T, cfg store.Config, identity checkpointParityIdentity, project string, memories []map[string]any) map[string]any {
	t.Helper()
	args := []string{
		"engram", "checkpoint", "record",
		"--host=" + identity.host,
		"--session-id=" + identity.sessionID,
		"--root-turn-id=" + identity.rootTurnID,
		"--disposition=" + store.CheckpointDispositionSaved,
		"--project=" + project,
	}
	for _, memory := range memories {
		encoded, err := json.Marshal(memory)
		if err != nil {
			t.Fatalf("encode inline Memory: %v", err)
		}
		args = append(args, "--memory-json="+string(encoded))
	}
	args = append(args, "--json")
	withArgs(t, args...)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI created record stderr = %q", stderr)
	}
	return decodeCLIJSON(t, stdout)
}

func runCheckpointCLIRecordSavedError(t *testing.T, cfg store.Config, identity checkpointParityIdentity, project string, memoryIDs []int64) map[string]any {
	t.Helper()
	stubExitWithPanic(t)
	args := []string{
		"engram", "checkpoint", "record",
		"--host=" + identity.host,
		"--session-id=" + identity.sessionID,
		"--root-turn-id=" + identity.rootTurnID,
		"--disposition=" + store.CheckpointDispositionSaved,
		"--project=" + project,
	}
	for _, memoryID := range memoryIDs {
		args = append(args, fmt.Sprintf("--memory-id=%d", memoryID))
	}
	args = append(args, "--json")
	withArgs(t, args...)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if stdout != "" {
		t.Fatalf("CLI saved error stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("CLI saved error exit = %v", recovered)
	}
	return decodeCLIJSON(t, stderr)
}

func runCheckpointCLIArgumentError(t *testing.T, cfg store.Config, identity checkpointParityIdentity, malformedFlag string) map[string]any {
	t.Helper()
	stubExitWithPanic(t)
	withArgs(t,
		"engram", "checkpoint", "record",
		"--host="+identity.host,
		"--session-id="+identity.sessionID,
		"--root-turn-id="+identity.rootTurnID,
		"--disposition="+store.CheckpointDispositionSaved,
		"--project=engram",
		malformedFlag,
		"--json",
	)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if stdout != "" {
		t.Fatalf("CLI malformed reference stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("CLI malformed reference exit = %v", recovered)
	}
	return decodeCLIJSON(t, stderr)
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

func checkpointParitySavedArguments(identity checkpointParityIdentity, project string, memoryIDs []int64) map[string]any {
	arguments := checkpointParityIdentityArguments(identity)
	arguments["disposition"] = store.CheckpointDispositionSaved
	arguments["project"] = project
	values := make([]any, 0, len(memoryIDs))
	for _, memoryID := range memoryIDs {
		values = append(values, float64(memoryID))
	}
	arguments["memory_ids"] = values
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
		"references":     checkpoint["references"],
	}
	return normalized
}

func normalizedCreatedCheckpointEnvelope(envelope map[string]any) map[string]any {
	normalized := normalizedCheckpointEnvelope(envelope)
	checkpoint := normalized["checkpoint"].(map[string]any)
	references, _ := checkpoint["references"].([]any)
	semantics := make([]map[string]any, 0, len(references))
	for _, rawReference := range references {
		reference, _ := rawReference.(map[string]any)
		semantics = append(semantics, map[string]any{
			"kind":    reference["kind"],
			"project": reference["project"],
		})
	}
	checkpoint["references"] = semantics
	return normalized
}

func seedCheckpointParityMemories(t *testing.T, cfg store.Config, project string, count int) []int64 {
	t.Helper()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open seed store: %v", err)
	}
	if err := s.CreateSession("session-parity-memories", project, "/work/"+project); err != nil {
		_ = s.Close()
		t.Fatalf("create seed session: %v", err)
	}
	ids := make([]int64, 0, count)
	for index := 0; index < count; index++ {
		id, err := s.AddObservation(store.AddObservationParams{
			SessionID: "session-parity-memories",
			Project:   project,
			Type:      "decision",
			Title:     fmt.Sprintf("Parity Memory %d", index+1),
			Content:   fmt.Sprintf("Durable parity content %d", index+1),
		})
		if err != nil {
			_ = s.Close()
			t.Fatalf("save seed Memory: %v", err)
		}
		ids = append(ids, id)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}
	return ids
}
