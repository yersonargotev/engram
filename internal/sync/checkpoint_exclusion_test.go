package sync

import (
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestChunkExportExcludesMemoryCheckpoints(t *testing.T) {
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
			Title: "Proposal chunk canary", Content: "Proposal chunk canary content",
		},
	})
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}

	result, err := New(s, t.TempDir()).Export("checkpoint-test", "engram")
	if err != nil {
		t.Fatalf("export chunk: %v", err)
	}
	if !result.IsEmpty || result.ChunkID != "" ||
		result.SessionsExported != 0 || result.ObservationsExported != 0 ||
		result.PromptsExported != 0 || result.MutationsExported != 0 {
		t.Fatalf("chunk export included checkpoint data: %#v", result)
	}
}
