package taskbriefing

import (
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestGenerateExcludesMemoryCheckpoints(t *testing.T) {
	s := newTestStore(t)
	_, _, err := s.RecordSkippedCheckpoint(store.RecordSkippedCheckpointParams{
		Identity: store.CheckpointIdentity{
			Host:       "codex-checkpoint-canary",
			SessionID:  "session-checkpoint-canary",
			RootTurnID: "turn-checkpoint-canary",
		},
		ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	})
	if err != nil {
		t.Fatalf("record checkpoint: %v", err)
	}
	_, _, err = s.RecordNeedsReviewCheckpoint(store.RecordNeedsReviewCheckpointParams{
		Identity: store.CheckpointIdentity{
			Host: "codex-proposal-canary", SessionID: "session-proposal-canary", RootTurnID: "turn-proposal-canary",
		},
		Project: "engram",
		Proposal: &store.MemoryProposalInput{
			Type: "decision", Title: "Proposal briefing canary", Content: "proposal briefing canary",
			Scope: "project", Category: "decision", ReasonCodes: []string{"requires_review"},
		},
	})
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}

	result, err := New(s).Generate(Input{
		Project:    "engram",
		Scope:      "project",
		TaskIntent: "checkpoint proposal briefing canary",
		Limit:      5,
	})
	if err != nil {
		t.Fatalf("generate task briefing: %v", err)
	}
	if len(result.Memories) != 0 {
		t.Fatalf("task briefing included checkpoint data: %#v", result.Memories)
	}
}
