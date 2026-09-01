package server

import (
	"encoding/json"
	"fmt"
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
	malformedReplay := httptest.NewRequest(http.MethodPost, "/checkpoints", strings.NewReader(
		`{"host":"pi","session_id":"session-http","root_turn_id":"turn-http","disposition":"skipped","memory_ids":["not-an-integer"]}`,
	))
	malformedReplayed := httptest.NewRecorder()
	h.ServeHTTP(malformedReplayed, malformedReplay)
	if malformedReplayed.Code != http.StatusOK {
		t.Fatalf("identity-first malformed replay status = %d body=%s", malformedReplayed.Code, malformedReplayed.Body.String())
	}
	var originalReplay memoryops.CheckpointRecordResult
	if err := json.Unmarshal(malformedReplayed.Body.Bytes(), &originalReplay); err != nil {
		t.Fatalf("decode malformed replay: %v", err)
	}
	if originalReplay.Idempotency != memoryops.CheckpointIdempotencyAlreadyRecorded || originalReplay.Checkpoint == nil ||
		originalReplay.Checkpoint.ReasonCode != "no_durable_knowledge" {
		t.Fatalf("malformed replay response = %#v", originalReplay)
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

func TestCheckpointPreflightAPIIsReadOnlyAndMixedRecordReturnsBothOutcomes(t *testing.T) {
	s := newServerTestStore(t)
	service := memoryops.New(s)
	seed, err := service.Save(memoryops.SaveInput{
		SessionID: "session-http-preflight-seed", CWD: "/work/engram", Project: "engram",
		Type: "decision", Title: "HTTP preflight duplicate", Content: "Reuse this exact Memory.",
	})
	if err != nil || seed.Observation == nil {
		t.Fatalf("seed HTTP preflight Memory: result=%#v err=%v", seed, err)
	}
	h := New(s, 0).Handler()

	var checkpointsBefore, proposalsBefore, mutationsBefore int
	for table, target := range map[string]*int{
		"memory_checkpoints": &checkpointsBefore, "memory_proposals": &proposalsBefore, "sync_mutations": &mutationsBefore,
	} {
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(target); err != nil {
			t.Fatalf("count %s before HTTP preflight: %v", table, err)
		}
	}
	preflight := httptest.NewRequest(http.MethodPost, "/checkpoints/preflight", strings.NewReader(
		`{"project":"engram","memories":[{"type":"decision","title":"HTTP preflight duplicate","content":"Reuse this exact Memory."}]}`,
	))
	preflighted := httptest.NewRecorder()
	h.ServeHTTP(preflighted, preflight)
	if preflighted.Code != http.StatusOK {
		t.Fatalf("preflight status = %d body=%s", preflighted.Code, preflighted.Body.String())
	}
	var preview memoryops.CheckpointPreflightResult
	if err := json.Unmarshal(preflighted.Body.Bytes(), &preview); err != nil {
		t.Fatalf("decode HTTP preflight: %v", err)
	}
	if len(preview.ExactDuplicates) != 1 || preview.ExactDuplicates[0].Reference.MemoryID != seed.Observation.ID {
		t.Fatalf("HTTP preflight = %#v", preview)
	}
	invalidPreflight := httptest.NewRequest(http.MethodPost, "/checkpoints/preflight", strings.NewReader(
		`{"project":"engram","memories":[],"unexpected":true}`,
	))
	invalidPreflighted := httptest.NewRecorder()
	h.ServeHTTP(invalidPreflighted, invalidPreflight)
	assertCheckpointAPIError(t, invalidPreflighted, http.StatusBadRequest, checkpointHTTPErrorCodeInvalidRequest)
	for table, want := range map[string]int{
		"memory_checkpoints": checkpointsBefore, "memory_proposals": proposalsBefore, "sync_mutations": mutationsBefore,
	} {
		var got int
		if err := s.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatalf("count %s after HTTP preflight: %v", table, err)
		}
		if got != want {
			t.Fatalf("HTTP preflight changed %s: %d -> %d", table, want, got)
		}
	}

	mixed := httptest.NewRequest(http.MethodPost, "/checkpoints", strings.NewReader(
		`{"host":"pi","session_id":"session-http-mixed","root_turn_id":"turn-http-mixed","disposition":"needs_review","project":"engram","memory_ids":[`+
			fmt.Sprint(seed.Observation.ID)+`],"proposal":{"title":"HTTP unresolved conflict","content":"This conflict needs review."}}`,
	))
	mixedRecorded := httptest.NewRecorder()
	h.ServeHTTP(mixedRecorded, mixed)
	if mixedRecorded.Code != http.StatusCreated {
		t.Fatalf("Mixed Memory status = %d body=%s", mixedRecorded.Code, mixedRecorded.Body.String())
	}
	var created memoryops.CheckpointRecordResult
	if err := json.Unmarshal(mixedRecorded.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode HTTP Mixed Memory: %v", err)
	}
	if created.Checkpoint == nil || len(created.Checkpoint.References) != 1 || created.Checkpoint.Proposal == nil {
		t.Fatalf("HTTP Mixed Memory = %#v", created)
	}
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
