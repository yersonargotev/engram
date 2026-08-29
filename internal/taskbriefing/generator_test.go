package taskbriefing

import (
	"reflect"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestGenerateRetainsExactIdentifierAfterOversizedToken(t *testing.T) {
	memoryStore := newTestStore(t)
	if err := memoryStore.CreateSession("oversized-token-session", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := memoryStore.AddObservation(store.AddObservationParams{
		SessionID: "oversized-token-session",
		Type:      "decision",
		Title:     "Exact delivery identity",
		Content:   "alpha PR 56",
		Project:   "engram",
		Scope:     "project",
	}); err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	task := "alpha bravo charlie delta echo foxtrot " + strings.Repeat("x", maximumGitTermBytes+1) + " PR 56"
	result, err := New(memoryStore).Generate(Input{Project: "engram", TaskIntent: task})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 {
		t.Fatalf("memories = %d, want exact identifier match after oversized token", len(result.Memories))
	}
	want := []string{"pr:#56"}
	if got := result.Memories[0].Evidence[0].MatchedIdentifiers; !reflect.DeepEqual(got, want) {
		t.Fatalf("matched identifiers = %v, want %v", got, want)
	}
	truncations := diagnosticTruncations(result.Diagnostics, DiagnosticTaskInputTruncated)
	if len(truncations) != 1 || truncations[0].AnalyzedTerms != 8 || truncations[0].OmittedTerms != 1 {
		t.Fatalf("truncations = %#v, want eight analyzed terms and one omitted oversized token", truncations)
	}
}

func TestGenerateHonorsSmallerResultLimit(t *testing.T) {
	memoryStore := newTestStore(t)
	for index, title := range []string{"Alpha task briefing", "Beta task briefing"} {
		sessionID := "limit-session-" + string(rune('a'+index))
		if err := memoryStore.CreateSession(sessionID, "engram", "/tmp/engram"); err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if _, err := memoryStore.AddObservation(store.AddObservationParams{
			SessionID: sessionID,
			Type:      "decision",
			Title:     title,
			Content:   "Use deterministic task briefing selection for durable memories.",
			Project:   "engram",
			Scope:     "project",
		}); err != nil {
			t.Fatalf("AddObservation: %v", err)
		}
	}

	result, err := New(memoryStore).Generate(Input{
		Project:    "engram",
		TaskIntent: "implement deterministic task briefing selection",
		Limit:      1,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 {
		t.Fatalf("memories = %d, want 1", len(result.Memories))
	}
	if result.ResultLimitOmissions != 1 {
		t.Fatalf("result limit omissions = %d, want 1", result.ResultLimitOmissions)
	}
	if !hasDiagnostic(result.Diagnostics, DiagnosticResultLimitReached) {
		t.Fatalf("diagnostics = %#v, want %s", result.Diagnostics, DiagnosticResultLimitReached)
	}
}

func TestGenerateReportsEveryMatchedMemoryField(t *testing.T) {
	memoryStore := newTestStore(t)
	if err := memoryStore.CreateSession("evidence-session", "engram", "/tmp/engram"); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := memoryStore.AddObservation(store.AddObservationParams{
		SessionID: "evidence-session",
		Type:      "decision",
		Title:     "Task briefing",
		Content:   "A task briefing selects durable memories.",
		Project:   "engram",
		Scope:     "project",
	}); err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	result, err := New(memoryStore).Generate(Input{Project: "engram", TaskIntent: "briefing"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 || len(result.Memories[0].Evidence) != 1 {
		t.Fatalf("result = %#v", result)
	}
	want := []string{"content", "title"}
	if got := result.Memories[0].Evidence[0].MatchedFields; !reflect.DeepEqual(got, want) {
		t.Fatalf("matched fields = %v, want %v", got, want)
	}
}
