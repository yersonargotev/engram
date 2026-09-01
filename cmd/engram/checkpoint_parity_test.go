package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	mcppkg "github.com/mark3labs/mcp-go/mcp"
	engrammcp "github.com/yersonargotev/engram/internal/mcp"
	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCheckpointCLIAndMCPParityForOptionalRecallFeedback(t *testing.T) {
	cfg := testConfig(t)
	s := openCheckpointParityStore(t, cfg)
	cliIdentity := checkpointParityIdentity{"codex", "session-cli-feedback-parity", "turn-cli-feedback-parity"}
	mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-feedback-parity", "turn-mcp-feedback-parity"}
	boundRuns := map[string]checkpointParityIdentity{
		"recall-cli-feedback-parity": cliIdentity,
		"recall-mcp-feedback-parity": mcpIdentity,
	}
	for recallID, identity := range boundRuns {
		turnIdentity := store.CheckpointIdentity{
			Host: identity.host, SessionID: identity.sessionID, RootTurnID: identity.rootTurnID,
		}
		if err := s.RecordRecallRunContext(context.Background(), store.RecallRunRecord{
			RecallID: recallID, Project: "engram", Scope: "project",
			DeliveredUTF8Bytes: 2, ElapsedMonotonicMS: 6, ProtocolVersion: 1, BinaryVersion: "test",
			TurnIdentity: &turnIdentity,
		}); err != nil {
			t.Fatalf("seed %s: %v", recallID, err)
		}
	}
	cliFeedback := memoryops.RecallFeedbackInput{
		RecallID:   "recall-cli-feedback-parity",
		FalseEmpty: &memoryops.RecallFalseEmptyInput{Value: true, Source: memoryops.RecallFeedbackSourceEvaluator},
	}
	mcpFeedback := map[string]any{
		"recall_id":   "recall-mcp-feedback-parity",
		"false_empty": map[string]any{"value": true, "source": memoryops.RecallFeedbackSourceEvaluator},
	}
	cli := runCheckpointCLIRecordFeedback(t, cfg, cliIdentity, cliFeedback)
	arguments := checkpointParityRecordArguments(mcpIdentity, store.CheckpointSkipReasonNoDurableKnowledge)
	arguments["recall_feedback"] = mcpFeedback
	mcp := callCheckpointMCP(t, engrammcp.CheckpointToolHandler(s), arguments, false)
	if got, want := normalizedCheckpointEnvelope(cli), normalizedCheckpointEnvelope(mcp); !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoint envelopes differ\nCLI=%#v\nMCP=%#v", got, want)
	}
	if !reflect.DeepEqual(cli["recall_feedback"], mcp["recall_feedback"]) {
		t.Fatalf("Recall feedback envelopes differ\nCLI=%#v\nMCP=%#v", cli["recall_feedback"], mcp["recall_feedback"])
	}
}

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

func TestCheckpointCLIAndMCPParityForNeedsReviewProposals(t *testing.T) {
	cfg := testConfig(t)
	proposal := map[string]any{
		"title": "Created review proposal", "content": "Keep this local until review.",
	}
	cliIdentity := checkpointParityIdentity{"codex", "session-cli-needs-review", "turn-cli-needs-review"}
	mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-needs-review", "turn-mcp-needs-review"}

	cliCreated := runCheckpointCLIRecordProposal(t, cfg, cliIdentity, "engram", "", proposal)
	s := openCheckpointParityStore(t, cfg)
	mcpRecord := engrammcp.CheckpointToolHandler(s)
	mcpArguments := checkpointParityProposalArguments(mcpIdentity, "engram", "", proposal)
	mcpCreated := callCheckpointMCP(t, mcpRecord, mcpArguments, false)
	if got, want := normalizedCreatedCheckpointEnvelope(cliCreated), normalizedCreatedCheckpointEnvelope(mcpCreated); !reflect.DeepEqual(got, want) {
		t.Fatalf("created proposal envelopes differ\nCLI=%#v\nMCP=%#v", got, want)
	}
	cliProposal := checkpointProposalSnapshot(t, cliCreated)
	mcpProposal := checkpointProposalSnapshot(t, mcpCreated)
	assertCompleteCheckpointProposal(t, cliProposal, "engram", proposal)
	assertCompleteCheckpointProposal(t, mcpProposal, "engram", proposal)

	cliReplayed := runCheckpointCLIRecordProposal(t, cfg, cliIdentity, "", "", nil)
	mcpReplayed := callCheckpointMCP(t, mcpRecord, map[string]any{
		"host": mcpIdentity.host, "session_id": mcpIdentity.sessionID, "root_turn_id": mcpIdentity.rootTurnID,
		"disposition": store.CheckpointDispositionNeedsReview,
	}, false)
	if got, want := normalizedCreatedCheckpointEnvelope(cliReplayed), normalizedCreatedCheckpointEnvelope(mcpReplayed); !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed proposal envelopes differ\nCLI=%#v\nMCP=%#v", got, want)
	}
	if replayed := checkpointProposalSnapshot(t, cliReplayed); !reflect.DeepEqual(replayed, cliProposal) {
		t.Fatalf("CLI replay proposal = %#v, want %#v", replayed, cliProposal)
	}
	if replayed := checkpointProposalSnapshot(t, mcpReplayed); !reflect.DeepEqual(replayed, mcpProposal) {
		t.Fatalf("MCP replay proposal = %#v, want %#v", replayed, mcpProposal)
	}

	cliStatus := runCheckpointCLIStatus(t, cfg, cliIdentity)
	mcpStatus := callCheckpointMCP(t, engrammcp.CheckpointStatusToolHandler(s), checkpointParityIdentityArguments(mcpIdentity), false)
	if got, want := normalizedCreatedCheckpointEnvelope(cliStatus), normalizedCreatedCheckpointEnvelope(mcpStatus); !reflect.DeepEqual(got, want) {
		t.Fatalf("proposal status envelopes differ\nCLI=%#v\nMCP=%#v", got, want)
	}
	if statusProposal := checkpointProposalSnapshot(t, cliStatus); !reflect.DeepEqual(statusProposal, cliProposal) {
		t.Fatalf("CLI status proposal = %#v, want %#v", statusProposal, cliProposal)
	}
	if statusProposal := checkpointProposalSnapshot(t, mcpStatus); !reflect.DeepEqual(statusProposal, mcpProposal) {
		t.Fatalf("MCP status proposal = %#v, want %#v", statusProposal, mcpProposal)
	}
}

func TestCheckpointCLIAndMCPParityForPreflightAndMixedMemory(t *testing.T) {
	cfg := testConfig(t)
	memoryIDs := seedCheckpointParityMemories(t, cfg, "engram", 1)
	memory := map[string]any{
		"type": "decision", "title": "Parity Memory 1", "content": "Durable parity content 1",
	}
	encodedMemory, err := json.Marshal(memory)
	if err != nil {
		t.Fatalf("encode preflight Memory: %v", err)
	}
	withArgs(t, "engram", "checkpoint", "preflight", "--project=engram", "--memory-json="+string(encodedMemory), "--json")
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI preflight stderr = %q", stderr)
	}
	cliPreflight := decodeCLIJSON(t, stdout)

	s := openCheckpointParityStore(t, cfg)
	mcpPreflight := callCheckpointMCP(t, engrammcp.CheckpointToolHandler(s), map[string]any{
		"operation": "preflight", "project": "engram", "memories": []any{memory},
	}, false)
	if !reflect.DeepEqual(cliPreflight, mcpPreflight) {
		t.Fatalf("preflight envelopes differ\nCLI=%#v\nMCP=%#v", cliPreflight, mcpPreflight)
	}
	if duplicates, _ := cliPreflight["exact_duplicates"].([]any); len(duplicates) != 1 {
		t.Fatalf("preflight exact duplicates = %#v", cliPreflight)
	}

	proposal := map[string]any{"title": "Mixed parity proposal", "content": "Keep this unresolved result local."}
	cliIdentity := checkpointParityIdentity{"codex", "session-cli-mixed", "turn-cli-mixed"}
	cliArgs := checkpointCLIProposalArgs(t, cliIdentity, "engram", "", proposal)
	cliArgs = append(cliArgs, fmt.Sprintf("--memory-id=%d", memoryIDs[0]), "--json")
	withArgs(t, cliArgs...)
	stdout, stderr = captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI Mixed Memory stderr = %q", stderr)
	}
	cliMixed := decodeCLIJSON(t, stdout)

	mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-mixed", "turn-mcp-mixed"}
	mcpArgs := checkpointParityProposalArguments(mcpIdentity, "engram", "", proposal)
	mcpArgs["memory_ids"] = []any{float64(memoryIDs[0])}
	mcpMixed := callCheckpointMCP(t, engrammcp.CheckpointToolHandler(s), mcpArgs, false)
	if got, want := normalizedCreatedCheckpointEnvelope(cliMixed), normalizedCreatedCheckpointEnvelope(mcpMixed); !reflect.DeepEqual(got, want) {
		t.Fatalf("Mixed Memory envelopes differ\nCLI=%#v\nMCP=%#v", got, want)
	}
	for surface, envelope := range map[string]map[string]any{"CLI": cliMixed, "MCP": mcpMixed} {
		checkpoint := envelope["checkpoint"].(map[string]any)
		if references, _ := checkpoint["references"].([]any); len(references) != 1 || checkpoint["proposal"] == nil {
			t.Fatalf("%s Mixed Memory checkpoint = %#v", surface, checkpoint)
		}
	}
}

func TestCheckpointCLIAndMCPParityRejectsMissingAndStaleNeedsReviewInputs(t *testing.T) {
	cfg := testConfig(t)
	s := openCheckpointParityStore(t, cfg)
	tests := []struct {
		name          string
		cliProposalID string
		mcpProposalID string
		project       string
		proposal      any
	}{
		{name: "empty", project: "engram"},
		{name: "removed proposal id", project: "engram", cliProposalID: "proposal-cli-stale", mcpProposalID: "proposal-mcp-stale"},
		{name: "invalid-inline", project: "engram", proposal: map[string]any{
			"title": "Missing content",
		}},
		{name: "removed type", project: "engram", proposal: staleCheckpointProposal("type", "decision")},
		{name: "removed scope", project: "engram", proposal: staleCheckpointProposal("scope", "project")},
		{name: "removed category", project: "engram", proposal: staleCheckpointProposal("category", "decision")},
		{name: "removed protected", project: "engram", proposal: staleCheckpointProposal("protected", true)},
		{name: "removed evidence refs", project: "engram", proposal: staleCheckpointProposal("evidence_refs", []string{"session-summary"})},
		{name: "removed reason codes", project: "engram", proposal: staleCheckpointProposal("reason_codes", []string{"requires_review"})},
		{name: "malformed", project: "engram", proposal: "not-an-object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cliErrorIdentity := checkpointParityIdentity{"codex", "session-cli-proposal-" + tt.name, "turn-cli-proposal-" + tt.name}
			mcpErrorIdentity := checkpointParityIdentity{"codex", "session-mcp-proposal-" + tt.name, "turn-mcp-proposal-" + tt.name}
			cliError := runCheckpointCLIProposalError(t, cfg, cliErrorIdentity, tt.project, tt.cliProposalID, tt.proposal)
			mcpArguments := checkpointParityProposalArguments(mcpErrorIdentity, tt.project, tt.mcpProposalID, nil)
			if tt.proposal != nil {
				mcpArguments["proposal"] = tt.proposal
			}
			mcpError := callCheckpointMCP(t, engrammcp.CheckpointToolHandler(s), mcpArguments, true)
			if !reflect.DeepEqual(cliError, mcpError) {
				t.Fatalf("proposal errors differ\nCLI=%#v\nMCP=%#v", cliError, mcpError)
			}
		})
	}
}

func staleCheckpointProposal(field string, value any) map[string]any {
	proposal := map[string]any{
		"title": "Stale proposal", "content": "Reject removed Admission metadata.",
	}
	proposal[field] = value
	return proposal
}

func TestCheckpointCLIAndMCPParityRejectsProposalFieldsOutsideNeedsReview(t *testing.T) {
	cfg := testConfig(t)
	s := openCheckpointParityStore(t, cfg)
	mcpRecord := engrammcp.CheckpointToolHandler(s)
	for _, disposition := range []string{store.CheckpointDispositionSkipped, store.CheckpointDispositionSaved} {
		t.Run(disposition, func(t *testing.T) {
			cliIdentity := checkpointParityIdentity{"codex", "session-cli-unexpected-proposal-" + disposition, "turn-cli-unexpected-proposal-" + disposition}
			mcpIdentity := checkpointParityIdentity{"codex", "session-mcp-unexpected-proposal-" + disposition, "turn-mcp-unexpected-proposal-" + disposition}
			cliError := runCheckpointCLIUnexpectedProposalError(t, cfg, cliIdentity, disposition)
			mcpArguments := checkpointParityIdentityArguments(mcpIdentity)
			mcpArguments["disposition"] = disposition
			mcpArguments["proposal_id"] = "proposal-missing"
			if disposition == store.CheckpointDispositionSkipped {
				mcpArguments["reason"] = store.CheckpointSkipReasonNoDurableKnowledge
			} else {
				mcpArguments["project"] = "engram"
				mcpArguments["memory_ids"] = []any{9999}
			}
			mcpError := callCheckpointMCP(t, mcpRecord, mcpArguments, true)
			if !reflect.DeepEqual(cliError, mcpError) {
				t.Fatalf("unexpected proposal errors differ\nCLI=%#v\nMCP=%#v", cliError, mcpError)
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

func runCheckpointCLIRecordFeedback(t *testing.T, cfg store.Config, identity checkpointParityIdentity, feedback memoryops.RecallFeedbackInput) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(feedback)
	if err != nil {
		t.Fatalf("encode CLI Recall feedback: %v", err)
	}
	withArgs(t,
		"engram", "checkpoint", "record",
		"--host="+identity.host,
		"--session-id="+identity.sessionID,
		"--root-turn-id="+identity.rootTurnID,
		"--disposition="+store.CheckpointDispositionSkipped,
		"--reason="+store.CheckpointSkipReasonNoDurableKnowledge,
		"--recall-feedback-json="+string(encoded),
		"--json",
	)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI Recall feedback stderr = %q", stderr)
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

func runCheckpointCLIRecordProposal(t *testing.T, cfg store.Config, identity checkpointParityIdentity, project, proposalID string, proposal map[string]any) map[string]any {
	t.Helper()
	args := checkpointCLIProposalArgs(t, identity, project, proposalID, proposal)
	args = append(args, "--json")
	withArgs(t, args...)
	stdout, stderr := captureOutput(t, func() { cmdCheckpoint(cfg) })
	if stderr != "" {
		t.Fatalf("CLI needs-review record stderr = %q", stderr)
	}
	return decodeCLIJSON(t, stdout)
}

func runCheckpointCLIProposalError(t *testing.T, cfg store.Config, identity checkpointParityIdentity, project, proposalID string, proposal any) map[string]any {
	t.Helper()
	stubExitWithPanic(t)
	args := []string{
		"engram", "checkpoint", "record",
		"--host=" + identity.host,
		"--session-id=" + identity.sessionID,
		"--root-turn-id=" + identity.rootTurnID,
		"--disposition=" + store.CheckpointDispositionNeedsReview,
	}
	if project != "" {
		args = append(args, "--project="+project)
	}
	if proposalID != "" {
		args = append(args, "--proposal-id="+proposalID)
	}
	if proposal != nil {
		if value, ok := proposal.(string); ok {
			args = append(args, "--proposal-json="+value)
		} else {
			encoded, err := json.Marshal(proposal)
			if err != nil {
				t.Fatalf("encode invalid proposal: %v", err)
			}
			args = append(args, "--proposal-json="+string(encoded))
		}
	}
	args = append(args, "--json")
	withArgs(t, args...)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if stdout != "" {
		t.Fatalf("CLI proposal error stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("CLI proposal error exit = %v", recovered)
	}
	return decodeCLIJSON(t, stderr)
}

func runCheckpointCLIUnexpectedProposalError(t *testing.T, cfg store.Config, identity checkpointParityIdentity, disposition string) map[string]any {
	t.Helper()
	stubExitWithPanic(t)
	args := []string{
		"engram", "checkpoint", "record",
		"--host=" + identity.host,
		"--session-id=" + identity.sessionID,
		"--root-turn-id=" + identity.rootTurnID,
		"--disposition=" + disposition,
		"--proposal-id=proposal-missing",
	}
	if disposition == store.CheckpointDispositionSkipped {
		args = append(args, "--reason="+store.CheckpointSkipReasonNoDurableKnowledge)
	} else {
		args = append(args, "--project=engram", "--memory-id=9999")
	}
	args = append(args, "--json")
	withArgs(t, args...)
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCheckpoint(cfg) })
	if stdout != "" {
		t.Fatalf("CLI unexpected proposal error stdout = %q", stdout)
	}
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("CLI unexpected proposal error exit = %v", recovered)
	}
	return decodeCLIJSON(t, stderr)
}

func checkpointCLIProposalArgs(t *testing.T, identity checkpointParityIdentity, project, proposalID string, proposal map[string]any) []string {
	t.Helper()
	args := []string{
		"engram", "checkpoint", "record",
		"--host=" + identity.host,
		"--session-id=" + identity.sessionID,
		"--root-turn-id=" + identity.rootTurnID,
		"--disposition=" + store.CheckpointDispositionNeedsReview,
	}
	if project != "" {
		args = append(args, "--project="+project)
	}
	if proposalID != "" {
		args = append(args, "--proposal-id="+proposalID)
	}
	if proposal != nil {
		encoded, err := json.Marshal(proposal)
		if err != nil {
			t.Fatalf("encode proposal: %v", err)
		}
		args = append(args, "--proposal-json="+string(encoded))
	}
	return args
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

func checkpointParityProposalArguments(identity checkpointParityIdentity, project, proposalID string, proposal map[string]any) map[string]any {
	arguments := checkpointParityIdentityArguments(identity)
	arguments["disposition"] = store.CheckpointDispositionNeedsReview
	if project != "" {
		arguments["project"] = project
	}
	if proposalID != "" {
		arguments["proposal_id"] = proposalID
	}
	if proposal != nil {
		arguments["proposal"] = proposal
	}
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
		"proposal":       checkpoint["proposal"],
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
	if proposal, ok := checkpoint["proposal"].(map[string]any); ok {
		checkpoint["proposal"] = map[string]any{
			"project": proposal["project"],
			"title":   proposal["title"],
			"content": proposal["content"],
		}
	}
	return normalized
}

func checkpointProposalSnapshot(t *testing.T, envelope map[string]any) map[string]any {
	t.Helper()
	checkpoint, ok := envelope["checkpoint"].(map[string]any)
	if !ok {
		t.Fatalf("checkpoint envelope = %#v", envelope)
	}
	proposal, ok := checkpoint["proposal"].(map[string]any)
	if !ok {
		t.Fatalf("checkpoint proposal = %#v", checkpoint["proposal"])
	}
	return proposal
}

func assertCompleteCheckpointProposal(t *testing.T, got map[string]any, project string, input map[string]any) {
	t.Helper()
	for _, field := range []string{"id", "project", "title", "content", "created_at"} {
		if value, ok := got[field].(string); !ok || value == "" {
			t.Fatalf("proposal %s = %#v, want non-empty string in %#v", field, got[field], got)
		}
	}
	if got["project"] != project || got["title"] != input["title"] || got["content"] != input["content"] {
		t.Fatalf("proposal snapshot = %#v, want project %q and input %#v", got, project, input)
	}
	if len(got) != 5 {
		t.Fatalf("proposal snapshot fields = %#v, want only id, project, title, content, created_at", got)
	}
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
