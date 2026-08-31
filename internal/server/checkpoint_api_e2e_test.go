//go:build e2e

package server

import (
	"net/http"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
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
