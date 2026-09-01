package obsidian

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

type checkpointStoreReader struct {
	store *store.Store
}

func (r checkpointStoreReader) Export() (*store.ExportData, error) {
	return r.store.Export()
}

func (r checkpointStoreReader) Stats() *store.Stats {
	stats, _ := r.store.Stats()
	return stats
}

func TestObsidianExportExcludesMemoryCheckpointsFromRealStore(t *testing.T) {
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	cfg.DataDir = t.TempDir()
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	_, _, err = s.RecordSkippedCheckpoint(store.RecordSkippedCheckpointParams{
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
		Project: "engram", Directory: "/work/engram",
		Memories: []store.AddObservationParams{{
			Type: "decision", Title: "Settled Obsidian Memory", Content: "settled-obsidian-memory-canary",
		}},
		Proposal: &store.MemoryProposalInput{
			Title: "Proposal Obsidian canary", Content: "proposal-obsidian-canary",
		},
	})
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
	}
	feedbackIdentity := store.CheckpointIdentity{
		Host: "feedback-obsidian-host", SessionID: "feedback-obsidian-session", RootTurnID: "feedback-obsidian-turn",
	}
	if err := s.RecordRecallRunContext(context.Background(), store.RecallRunRecord{
		RecallID:     "recall-feedback-obsidian-canary",
		Project:      "engram",
		Scope:        "project",
		TurnIdentity: &feedbackIdentity,
	}); err != nil {
		t.Fatalf("record empty Recall run: %v", err)
	}
	falseEmpty := false
	if _, err := s.RecordRecallFeedback(store.RecordRecallFeedbackParams{
		Identity:         feedbackIdentity,
		RecallID:         "recall-feedback-obsidian-canary",
		FalseEmpty:       &falseEmpty,
		FalseEmptySource: store.RecallFeedbackSourceEvaluator,
	}); err != nil {
		t.Fatalf("record Recall feedback: %v", err)
	}

	vault := t.TempDir()
	result, err := NewExporter(checkpointStoreReader{store: s}, ExportConfig{
		VaultPath:   vault,
		Force:       true,
		GraphConfig: GraphConfigSkip,
	}).Export()
	if err != nil {
		t.Fatalf("export to Obsidian: %v", err)
	}
	if result.Created != 1 || result.Updated != 0 {
		t.Fatalf("Obsidian Mixed export = %#v, want only the settled Memory", result)
	}

	settledMemoryFound := false
	err = filepath.WalkDir(vault, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(content)
		if strings.Contains(text, "settled-obsidian-memory-canary") {
			settledMemoryFound = true
		}
		if strings.Contains(text, "checkpoint-canary") ||
			strings.Contains(text, "proposal-obsidian-canary") ||
			strings.Contains(text, "feedback-obsidian") ||
			strings.Contains(text, "recall-feedback-obsidian-canary") ||
			strings.Contains(text, store.CheckpointSkipReasonNoDurableKnowledge) {
			t.Fatalf("Obsidian file %s included checkpoint data: %s", path, text)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inspect Obsidian vault: %v", err)
	}
	if !settledMemoryFound {
		t.Fatal("Obsidian Mixed export omitted the settled Memory")
	}
}
