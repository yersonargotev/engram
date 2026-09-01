package store

import (
	"context"
	"errors"
	"testing"
)

func TestRecallEligibleConflictTargetsContextEnforcesAuthorityAndLifecycle(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateSession("recall-conflict-targets", "engram", "/work/engram"); err != nil {
		t.Fatal(err)
	}
	seed := func(title, scope string) *Observation {
		t.Helper()
		id, err := s.AddObservation(AddObservationParams{
			SessionID: "recall-conflict-targets", Project: "engram", Scope: scope,
			Type: "decision", Title: title, Content: "conflict counterpart",
		})
		if err != nil {
			t.Fatal(err)
		}
		observation, err := s.GetObservation(id)
		if err != nil {
			t.Fatal(err)
		}
		return observation
	}
	active := seed("active target", "project")
	personal := seed("personal target", "personal")
	stale := seed("stale target", "project")
	if _, err := s.DB().Exec(`UPDATE observations SET review_after = datetime('now', '-1 day') WHERE id = ?`, stale.ID); err != nil {
		t.Fatal(err)
	}

	targets, err := s.RecallEligibleConflictTargetsContext(context.Background(), []string{
		active.SyncID, active.SyncID, personal.SyncID, stale.SyncID, "", "missing-sync-id",
	}, SearchOptions{Project: " ENGRAM ", Scope: "project"})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[active.SyncID].ID != active.ID || targets[active.SyncID].Title != active.Title {
		t.Fatalf("targets=%#v, want only active in-scope target", targets)
	}

	empty, err := s.RecallEligibleConflictTargetsContext(context.Background(), nil, SearchOptions{})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty targets=%#v error=%v", empty, err)
	}
}

func TestRecallEligibleConflictTargetsContextPropagatesCancellationAndQueryFailure(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := s.RecallEligibleConflictTargetsContext(ctx, []string{"sync-id"}, SearchOptions{}); !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("canceled result=%#v error=%v", result, err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if result, err := s.RecallEligibleConflictTargetsContext(context.Background(), []string{"sync-id"}, SearchOptions{}); err == nil || result != nil {
		t.Fatalf("closed-store result=%#v error=%v", result, err)
	}
}
