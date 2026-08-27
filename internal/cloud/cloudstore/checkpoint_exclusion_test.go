package cloudstore

import (
	"encoding/json"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCloudMaterializationExcludesMemoryCheckpointEntities(t *testing.T) {
	payload, counts, err := materializedMutationBatchChunk([]MutationEntry{
		{
			Project:   "engram",
			Entity:    "memory_checkpoint",
			EntityKey: "turn-checkpoint-canary",
			Op:        store.SyncOpUpsert,
			Payload:   json.RawMessage(`{"reason_code":"no_durable_knowledge"}`),
		},
		{
			Project:   "engram",
			Entity:    "memory_proposal",
			EntityKey: "proposal-checkpoint-canary",
			Op:        store.SyncOpUpsert,
			Payload:   json.RawMessage(`{"content":"proposal-cloud-canary"}`),
		},
	})
	if err != nil {
		t.Fatalf("materialize checkpoint entity: %v", err)
	}
	if len(payload) != 0 || counts.sessions != 0 || counts.observations != 0 || counts.prompts != 0 {
		t.Fatalf("cloud materialization included checkpoint data: payload=%s counts=%#v", payload, counts)
	}
}
