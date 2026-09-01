package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestPromptCaptureAPIDefaultsOffWithoutLeakingContent(t *testing.T) {
	st := newServerTestStore(t)
	if err := st.CreateSession("session-capture-off", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}

	srv := New(st, 0)
	var writes atomic.Int32
	srv.SetOnWrite(func() { writes.Add(1) })
	const sentinel = "PROMPT-MUST-NOT-LEAK-102"
	req := httptest.NewRequest(http.MethodPost, "/prompts", strings.NewReader(`{"session_id":"session-capture-off","project":"engram","content":"`+sentinel+`"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["captured"] != false || body["reason_code"] != memoryops.CaptureReasonConsentDisabled {
		t.Fatalf("response = %#v, want disabled metadata", body)
	}
	if strings.Contains(rec.Body.String(), sentinel) {
		t.Fatalf("response leaked prompt content: %s", rec.Body.String())
	}
	if writes.Load() != 0 {
		t.Fatalf("disabled capture notified autosync %d times", writes.Load())
	}
	assertPromptCaptureBoundaryCounts(t, st, 0, 0)
}

func TestPromptCaptureAPIUsesExplicitConsentAndReturnsOnlyMetadata(t *testing.T) {
	st := newServerTestStore(t)
	if err := st.CreateSession("session-capture-on", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := memoryops.New(st).EnableCapture(memoryops.CaptureEnableInput{
		Project: "engram", ContentType: store.CaptureContentTypePrompt,
	}); err != nil {
		t.Fatalf("enable capture: %v", err)
	}

	srv := New(st, 0)
	var writes atomic.Int32
	srv.SetOnWrite(func() { writes.Add(1) })
	const sentinel = "CONSENTED-PROMPT-MUST-NOT-LEAK-102"
	req := httptest.NewRequest(http.MethodPost, "/prompts", strings.NewReader(`{"session_id":"session-capture-on","project":"engram","content":"`+sentinel+`"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["captured"] != true || body["reason_code"] != memoryops.CaptureReasonCaptured || body["expires_at"] == nil {
		t.Fatalf("response = %#v, want captured metadata", body)
	}
	if _, exposesLegacyID := body["id"]; exposesLegacyID {
		t.Fatalf("response exposed a Legacy prompt identity: %#v", body)
	}
	if strings.Contains(rec.Body.String(), sentinel) {
		t.Fatalf("response leaked prompt content: %s", rec.Body.String())
	}
	if writes.Load() != 0 {
		t.Fatalf("local-only Diagnostic capture notified autosync %d times", writes.Load())
	}
	assertPromptCaptureBoundaryCounts(t, st, 1, 0)
}

func TestLegacyPromptHTTPReadsAndDeleteAreGone(t *testing.T) {
	st := newServerTestStore(t)
	if err := st.CreateSession("legacy-session", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	legacyID, err := st.AddPrompt(store.AddPromptParams{SessionID: "legacy-session", Project: "engram", Content: "LEGACY-CONTENT-MUST-NOT-LEAK"})
	if err != nil {
		t.Fatalf("seed Legacy prompt: %v", err)
	}

	h := New(st, 0).Handler()
	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/prompts/recent?project=engram"},
		{http.MethodGet, "/prompts/search?q=LEGACY-CONTENT-MUST-NOT-LEAK&project=engram"},
		{http.MethodDelete, "/prompts/" + strconv.FormatInt(legacyID, 10)},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusGone {
				t.Fatalf("status = %d, want 410; body=%s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "LEGACY-CONTENT-MUST-NOT-LEAK") {
				t.Fatalf("retired endpoint leaked Legacy content: %s", rec.Body.String())
			}
		})
	}

	var remaining int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM user_prompts WHERE id = ?`, legacyID).Scan(&remaining); err != nil {
		t.Fatalf("count Legacy prompt: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("retired HTTP delete mutated Legacy prompt, remaining=%d", remaining)
	}
}

func assertPromptCaptureBoundaryCounts(t *testing.T, st *store.Store, wantDiagnostic, wantLegacy int) {
	t.Helper()
	var diagnostic, legacy int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM diagnostic_captures`).Scan(&diagnostic); err != nil {
		t.Fatalf("count Diagnostic captures: %v", err)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM user_prompts`).Scan(&legacy); err != nil {
		t.Fatalf("count Legacy prompts: %v", err)
	}
	if diagnostic != wantDiagnostic || legacy != wantLegacy {
		t.Fatalf("capture boundary counts Diagnostic=%d Legacy=%d, want %d/%d", diagnostic, legacy, wantDiagnostic, wantLegacy)
	}
}
