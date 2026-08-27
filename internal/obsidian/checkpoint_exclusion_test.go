package obsidian

import (
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
		Project: "engram",
		Proposal: &store.MemoryProposalInput{
			Type: "decision", Title: "Proposal Obsidian canary", Content: "proposal-obsidian-canary",
			Scope: "project", Category: "decision", ReasonCodes: []string{"requires_review"},
		},
	})
	if err != nil {
		t.Fatalf("record needs-review checkpoint: %v", err)
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
	if result.Created != 0 || result.Updated != 0 {
		t.Fatalf("Obsidian export created checkpoint files: %#v", result)
	}

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
		if strings.Contains(text, "checkpoint-canary") ||
			strings.Contains(text, "proposal-obsidian-canary") ||
			strings.Contains(text, store.CheckpointSkipReasonNoDurableKnowledge) {
			t.Fatalf("Obsidian file %s included checkpoint data: %s", path, text)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inspect Obsidian vault: %v", err)
	}
}
