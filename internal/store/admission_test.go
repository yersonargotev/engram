package store

import (
	"reflect"
	"testing"
)

func TestObservationContentExistsUsesNormalizedProjectScopeAndContent(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateSession("admission-existing", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := s.AddObservation(AddObservationParams{
		SessionID: "admission-existing",
		Type:      "decision",
		Title:     "SQLite authority",
		Content:   "Local SQLite remains the source of truth.",
		Project:   "engram",
		Scope:     "project",
	}); err != nil {
		t.Fatalf("add observation: %v", err)
	}

	tests := []struct {
		name    string
		content string
		project string
		scope   string
		want    bool
	}{
		{name: "normalized duplicate", content: "  LOCAL  sqlite remains the source of truth. ", project: " ENGRAM ", scope: "project", want: true},
		{name: "different content", content: "Cloud is the source of truth.", project: "engram", scope: "project", want: false},
		{name: "different project", content: "Local SQLite remains the source of truth.", project: "other", scope: "project", want: false},
		{name: "different scope", content: "Local SQLite remains the source of truth.", project: "engram", scope: "personal", want: false},
		{name: "empty content", content: "  ", project: "engram", scope: "project", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.ObservationContentExists(tc.content, tc.project, tc.scope)
			if err != nil {
				t.Fatalf("observation content exists: %v", err)
			}
			if got != tc.want {
				t.Fatalf("exists = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSessionPromptsReturnsLatestWindowInChronologicalOrder(t *testing.T) {
	s := newTestStore(t)
	for _, sessionID := range []string{"session-a", "session-b"} {
		if err := s.CreateSession(sessionID, "engram", "/work/engram"); err != nil {
			t.Fatalf("create session %s: %v", sessionID, err)
		}
	}
	for _, content := range []string{"first", "second", "third"} {
		if _, err := s.AddPrompt(AddPromptParams{SessionID: "session-a", Project: "engram", Content: content}); err != nil {
			t.Fatalf("add prompt %q: %v", content, err)
		}
	}
	if _, err := s.AddPrompt(AddPromptParams{SessionID: "session-b", Project: "engram", Content: "other"}); err != nil {
		t.Fatalf("add other-session prompt: %v", err)
	}

	prompts, total, err := s.SessionPrompts("session-a", 2)
	if err != nil {
		t.Fatalf("session prompts: %v", err)
	}
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}
	got := make([]string, 0, len(prompts))
	for _, prompt := range prompts {
		got = append(got, prompt.Content)
	}
	if want := []string{"second", "third"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("contents = %#v, want %#v", got, want)
	}
}

func TestSessionPromptsReturnsEmptyResult(t *testing.T) {
	s := newTestStore(t)
	prompts, total, err := s.SessionPrompts("missing", 10)
	if err != nil {
		t.Fatalf("session prompts: %v", err)
	}
	if len(prompts) != 0 || total != 0 {
		t.Fatalf("prompts = %#v, total = %d", prompts, total)
	}
}

func TestLatestSessionObservationByTypeIgnoresDeletedAndOtherTypes(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateSession("session-summary", "engram", "/work/engram"); err != nil {
		t.Fatalf("create session: %v", err)
	}
	firstID, err := s.AddObservation(AddObservationParams{
		SessionID: "session-summary", Type: "session_summary", Title: "First", Content: "First summary.", Project: "engram", Scope: "project",
	})
	if err != nil {
		t.Fatalf("add first summary: %v", err)
	}
	if _, err := s.AddObservation(AddObservationParams{
		SessionID: "session-summary", Type: "decision", Title: "Other", Content: "Other content.", Project: "engram", Scope: "project",
	}); err != nil {
		t.Fatalf("add other type: %v", err)
	}
	deletedID, err := s.AddObservation(AddObservationParams{
		SessionID: "session-summary", Type: "session_summary", Title: "Deleted", Content: "Deleted summary.", Project: "engram", Scope: "project",
	})
	if err != nil {
		t.Fatalf("add deleted summary: %v", err)
	}
	if err := s.DeleteObservation(deletedID, false); err != nil {
		t.Fatalf("soft delete summary: %v", err)
	}

	got, err := s.LatestSessionObservationByType("session-summary", "session_summary")
	if err != nil {
		t.Fatalf("latest session observation: %v", err)
	}
	if got == nil || got.ID != firstID {
		t.Fatalf("observation = %#v, want id %d", got, firstID)
	}
	none, err := s.LatestSessionObservationByType("session-summary", "missing")
	if err != nil || none != nil {
		t.Fatalf("missing observation = %#v, err = %v", none, err)
	}
}
