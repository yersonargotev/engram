package sync

import (
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestLegacyPromptsAreExcludedFromChunkConstruction(t *testing.T) {
	chunk := ChunkData{
		Sessions: []store.Session{{ID: "session-1", Project: "engram"}},
		Prompts: []store.Prompt{{
			SyncID: "legacy-prompt-1", SessionID: "session-1", Content: "LEGACY-PROMPT-CANARY-102", Project: "engram",
		}},
		Mutations: []store.SyncMutation{{
			Seq: 1, Entity: store.SyncEntityPrompt, EntityKey: "legacy-prompt-1", Op: store.SyncOpUpsert,
			Payload: `{"sync_id":"legacy-prompt-1","session_id":"session-1","content":"LEGACY-PROMPT-CANARY-102","project":"engram"}`,
		}},
	}

	if got := synthesizeMutationsFromChunk(chunk); len(got) != 1 || got[0].Entity != store.SyncEntitySession {
		t.Fatalf("synthesized Legacy prompt mutation: %+v", got)
	}
	if got := buildImportMutations(chunk); len(got) != 0 {
		t.Fatalf("retained explicit Legacy prompt mutation: %+v", got)
	}
	if got := estimateMutationImportResult(chunk); got.PromptsImported != 0 {
		t.Fatalf("estimated %d imported Legacy prompts", got.PromptsImported)
	}
}

func TestLegacyPromptsAreExcludedFromIncrementalAndProjectFilters(t *testing.T) {
	data := &store.ExportData{
		Version:  "0.1.0",
		Sessions: []store.Session{{ID: "session-1", Project: "engram", StartedAt: "2025-01-01 00:00:00"}},
		Prompts: []store.Prompt{{
			ID: 1, SyncID: "legacy-prompt-1", SessionID: "session-1", Content: "LEGACY-PROMPT-CANARY-102", Project: "engram", CreatedAt: "2025-01-02 00:00:00",
		}},
	}

	incremental := (&Syncer{}).filterNewData(data, "")
	if len(incremental.Prompts) != 0 {
		t.Fatalf("incremental filter exposed Legacy prompts: %+v", incremental.Prompts)
	}
	project := filterByProject(data, "engram")
	if len(project.Prompts) != 0 {
		t.Fatalf("project filter exposed Legacy prompts: %+v", project.Prompts)
	}
}
