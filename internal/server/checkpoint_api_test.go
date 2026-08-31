package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
)

func TestCheckpointAPIUsesServerOwnedStoreAndStructuredErrors(t *testing.T) {
	h := New(newServerTestStore(t), 0).Handler()
	identity := `{"host":"pi","session_id":"session-http","root_turn_id":"turn-http"}`

	record := httptest.NewRequest(http.MethodPost, "/checkpoints", strings.NewReader(
		`{"host":"pi","session_id":"session-http","root_turn_id":"turn-http","disposition":"skipped","reason_code":"no_durable_knowledge"}`,
	))
	record.Header.Set("Content-Type", "application/json")
	recorded := httptest.NewRecorder()
	h.ServeHTTP(recorded, record)
	if recorded.Code != http.StatusCreated {
		t.Fatalf("record status = %d body=%s", recorded.Code, recorded.Body.String())
	}
	var created memoryops.CheckpointRecordResult
	if err := json.Unmarshal(recorded.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode record response: %v", err)
	}
	if created.Idempotency != memoryops.CheckpointIdempotencyCreated || created.Checkpoint == nil {
		t.Fatalf("record response = %#v", created)
	}
	replay := httptest.NewRequest(http.MethodPost, "/checkpoints", strings.NewReader(
		`{"host":"pi","session_id":"session-http","root_turn_id":"turn-http","disposition":"skipped","reason_code":"no_durable_knowledge"}`,
	))
	replayed := httptest.NewRecorder()
	h.ServeHTTP(replayed, replay)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay status = %d body=%s", replayed.Code, replayed.Body.String())
	}
	var exactReplay memoryops.CheckpointRecordResult
	if err := json.Unmarshal(replayed.Body.Bytes(), &exactReplay); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if exactReplay.Idempotency != memoryops.CheckpointIdempotencyAlreadyRecorded {
		t.Fatalf("replay response = %#v", exactReplay)
	}

	status := httptest.NewRequest(http.MethodGet, "/checkpoints/status?host=pi&session_id=session-http&root_turn_id=turn-http", nil)
	inspected := httptest.NewRecorder()
	h.ServeHTTP(inspected, status)
	if inspected.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", inspected.Code, inspected.Body.String())
	}
	var found memoryops.CheckpointStatusResult
	if err := json.Unmarshal(inspected.Body.Bytes(), &found); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if found.Checkpoint == nil || found.Checkpoint.Identity.Host != "pi" {
		t.Fatalf("status response = %#v", found)
	}

	conflict := httptest.NewRequest(http.MethodPost, "/checkpoints", strings.NewReader(
		strings.TrimSuffix(identity, "}")+`,"disposition":"needs_review","project":"engram","proposal":{"title":"Review","content":"Different terminal result"}}`,
	))
	conflicted := httptest.NewRecorder()
	h.ServeHTTP(conflicted, conflict)
	assertCheckpointAPIError(t, conflicted, http.StatusConflict, memoryops.CheckpointErrorCodeConflict)

	invalid := httptest.NewRequest(http.MethodPost, "/checkpoints", strings.NewReader(`{"host":`))
	rejected := httptest.NewRecorder()
	h.ServeHTTP(rejected, invalid)
	assertCheckpointAPIError(t, rejected, http.StatusBadRequest, "invalid_checkpoint_request")
}

func TestCheckpointStatusAPIRejectsInvalidIdentity(t *testing.T) {
	h := New(newServerTestStore(t), 0).Handler()
	req := httptest.NewRequest(http.MethodGet, "/checkpoints/status?host=pi", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assertCheckpointAPIError(t, rec, http.StatusBadRequest, memoryops.CheckpointErrorCodeInvalidIdentity)
}

func assertCheckpointAPIError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, wantStatus, rec.Body.String())
	}
	var envelope struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if envelope.Code != wantCode || strings.TrimSpace(envelope.Message) == "" {
		t.Fatalf("error response = %#v, want code %q with message", envelope, wantCode)
	}
}
