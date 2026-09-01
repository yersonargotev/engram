package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestHandleRecallReturnsBoundedCoreEnvelope(t *testing.T) {
	st := newServerTestStore(t)
	if err := st.CreateSession("recall-http", "engram", "/tmp/engram"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if _, err := st.AddObservation(store.AddObservationParams{
			SessionID: "recall-http",
			Type:      "decision",
			Title:     fmt.Sprintf("Bounded HTTP Recall %d", i),
			Content:   fmt.Sprintf("bounded recall transport candidate %d", i),
			Project:   "engram",
			Scope:     "project",
		}); err != nil {
			t.Fatal(err)
		}
	}
	srv := New(st, 0)
	srv.SetRecallProvenance("3.4.0-test", "abc123")
	req := httptest.NewRequest(http.MethodGet, "/recall?q=bounded+recall&project=engram", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var result memoryops.RecallResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.ResultCount != 5 || len(result.ResultIDs) != 5 || result.DeliveredUTF8Bytes > memoryops.RecallCandidateBudgetBytes {
		t.Fatalf("unbounded Recall response: %#v", result)
	}
	if result.Provenance.BinaryVersion != "3.4.0-test" || result.Provenance.BinaryRevision != "abc123" {
		t.Fatalf("provenance=%#v", result.Provenance)
	}
}

func TestHandleRecallValidatesRequestContract(t *testing.T) {
	srv := New(newServerTestStore(t), 0)
	for _, test := range []struct {
		path string
		want string
	}{
		{path: "/recall", want: "q parameter is required"},
		{path: "/recall?q=x&limit=11", want: "limit must be between 1 and 10"},
		{path: "/recall?q=x&match_mode=or", want: "invalid match_mode"},
		{path: "/recall?q=x&scope=typo", want: "invalid recall scope"},
		{path: "/recall?q=x&project=engram&all_projects=true", want: "all_projects cannot be combined with project"},
		{path: "/recall?q=x&project=engram&project_strength=imaginary", want: "invalid project_strength"},
	} {
		t.Run(test.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, test.path, nil))
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), test.want) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandleRecallStoreFailureFailsOpen(t *testing.T) {
	st := newServerTestStore(t)
	srv := New(st, 0)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/recall?q=release&project=engram", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var result memoryops.RecallResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Warning == nil || result.Warning.Code != "recall_unavailable" || len(result.Diagnostics) != 1 {
		t.Fatalf("fail-open result=%#v", result)
	}
}
