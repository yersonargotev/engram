//go:build e2e

package server

import (
	"net/http"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestCheckpointAPIRoundTripAndConflictE2E(t *testing.T) {
	_, ts := newE2EServer(t)
	client := ts.Client()
	identity := map[string]any{
		"host": "pi", "session_id": "session-checkpoint-e2e", "root_turn_id": "turn-checkpoint-e2e",
	}
	record := map[string]any{
		"host": identity["host"], "session_id": identity["session_id"], "root_turn_id": identity["root_turn_id"],
		"disposition": "skipped", "reason_code": "no_durable_knowledge",
	}
	createdResp := postJSON(t, client, ts.URL+"/checkpoints", record)
	if createdResp.StatusCode != http.StatusCreated {
		t.Fatalf("record status = %d", createdResp.StatusCode)
	}
	created := decodeJSON[memoryops.CheckpointRecordResult](t, createdResp)
	if created.Idempotency != memoryops.CheckpointIdempotencyCreated || created.Checkpoint == nil {
		t.Fatalf("record response = %#v", created)
	}

	statusResp, err := client.Get(ts.URL + "/checkpoints/status?host=pi&session_id=session-checkpoint-e2e&root_turn_id=turn-checkpoint-e2e")
	if err != nil {
		t.Fatalf("status request: %v", err)
	}
	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", statusResp.StatusCode)
	}
	status := decodeJSON[memoryops.CheckpointStatusResult](t, statusResp)
	if status.Checkpoint == nil || status.Checkpoint.Disposition != "skipped" {
		t.Fatalf("status response = %#v", status)
	}

	conflict := map[string]any{
		"host": identity["host"], "session_id": identity["session_id"], "root_turn_id": identity["root_turn_id"],
		"disposition": "needs_review", "project": "engram",
		"proposal": map[string]any{"title": "Review", "content": "Conflicting disposition"},
	}
	conflictResp := postJSON(t, client, ts.URL+"/checkpoints", conflict)
	if conflictResp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d", conflictResp.StatusCode)
	}
	conflictBody := decodeJSON[map[string]any](t, conflictResp)
	if conflictBody["code"] != memoryops.CheckpointErrorCodeConflict || conflictBody["message"] == "" {
		t.Fatalf("conflict response = %#v", conflictBody)
	}
}

func TestCheckpointPreflightAndMixedMemoryE2E(t *testing.T) {
	s, ts := newE2EServer(t)
	client := ts.Client()
	if err := s.CreateSession("session-checkpoint-preflight-e2e", "engram", "/work/engram"); err != nil {
		t.Fatalf("create preflight session: %v", err)
	}
	memoryID, err := s.AddObservation(store.AddObservationParams{
		SessionID: "session-checkpoint-preflight-e2e", Project: "engram", Type: "decision",
		Title: "E2E preflight duplicate", Content: "Reuse this exact Memory.",
	})
	if err != nil {
		t.Fatalf("seed preflight Memory: %v", err)
	}

	preflightResp := postJSON(t, client, ts.URL+"/checkpoints/preflight", map[string]any{
		"project": "engram",
		"memories": []any{map[string]any{
			"type": "decision", "title": "E2E preflight duplicate", "content": "Reuse this exact Memory.",
		}},
	})
	if preflightResp.StatusCode != http.StatusOK {
		t.Fatalf("preflight status = %d", preflightResp.StatusCode)
	}
	preflight := decodeJSON[memoryops.CheckpointPreflightResult](t, preflightResp)
	if len(preflight.ExactDuplicates) != 1 || preflight.ExactDuplicates[0].Reference.MemoryID != memoryID {
		t.Fatalf("preflight response = %#v", preflight)
	}

	mixedResp := postJSON(t, client, ts.URL+"/checkpoints", map[string]any{
		"host": "pi", "session_id": "session-checkpoint-mixed-e2e", "root_turn_id": "turn-checkpoint-mixed-e2e",
		"disposition": "needs_review", "project": "engram", "memory_ids": []int64{memoryID},
		"memories": []any{map[string]any{
			"type": "discovery", "title": "E2E settled discovery", "content": "This result is settled.",
		}},
		"proposal": map[string]any{"title": "E2E unresolved conflict", "content": "This result needs review."},
	})
	if mixedResp.StatusCode != http.StatusCreated {
		t.Fatalf("Mixed Memory status = %d", mixedResp.StatusCode)
	}
	mixed := decodeJSON[memoryops.CheckpointRecordResult](t, mixedResp)
	if mixed.Checkpoint == nil || len(mixed.Checkpoint.References) != 2 || mixed.Checkpoint.Proposal == nil {
		t.Fatalf("Mixed Memory response = %#v", mixed)
	}
}
