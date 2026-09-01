package store

import "testing"

func TestAdvancePulledCursorIsMonotonicForDefaultTarget(t *testing.T) {
	s := newTestStore(t)

	if err := s.AdvancePulledCursor("", 212); err != nil {
		t.Fatalf("AdvancePulledCursor: %v", err)
	}
	state, err := s.GetSyncState(DefaultSyncTargetKey)
	if err != nil {
		t.Fatalf("GetSyncState after advance: %v", err)
	}
	if state.LastPulledSeq != 212 {
		t.Fatalf("LastPulledSeq = %d, want 212", state.LastPulledSeq)
	}

	if err := s.AdvancePulledCursor(DefaultSyncTargetKey, 107); err != nil {
		t.Fatalf("AdvancePulledCursor with older seq: %v", err)
	}
	state, err = s.GetSyncState("")
	if err != nil {
		t.Fatalf("GetSyncState after older seq: %v", err)
	}
	if state.LastPulledSeq != 212 {
		t.Fatalf("LastPulledSeq regressed to %d, want 212", state.LastPulledSeq)
	}
}
