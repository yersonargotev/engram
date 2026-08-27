package memoryops

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestCheckpointRecordAndStatusShareIdempotentDomainResult(t *testing.T) {
	service := newTestService(t)
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-domain-123",
		RootTurnID:  "turn-domain-456",
		Disposition: store.CheckpointDispositionSkipped,
		ReasonCode:  store.CheckpointSkipReasonNoDurableKnowledge,
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record checkpoint: %v", err)
	}
	if created.Idempotency != CheckpointIdempotencyCreated || created.Checkpoint == nil {
		t.Fatalf("created result = %#v", created)
	}

	replayed, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("replay checkpoint: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded {
		t.Fatalf("replay idempotency = %q", replayed.Idempotency)
	}
	if !reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("replayed checkpoint = %#v, want %#v", replayed.Checkpoint, created.Checkpoint)
	}

	status, err := service.CheckpointStatus(CheckpointStatusInput{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	})
	if err != nil {
		t.Fatalf("checkpoint status: %v", err)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("status checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
}

func TestSavedCheckpointAttachesAnExistingMemory(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Saved checkpoint", "Attach this durable decision to the completed root turn.")
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-saved-existing",
		RootTurnID:  "turn-saved-existing",
		Disposition: store.CheckpointDispositionSaved,
		Project:     "engram",
		MemoryIDs:   []int64{memory.ID},
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record saved checkpoint: %v", err)
	}
	if created.Idempotency != CheckpointIdempotencyCreated {
		t.Fatalf("idempotency = %q, want %q", created.Idempotency, CheckpointIdempotencyCreated)
	}
	wantReferences := []store.CheckpointReference{{
		Kind:         store.CheckpointReferenceKindMemory,
		MemoryID:     memory.ID,
		MemorySyncID: memory.SyncID,
		Project:      "engram",
	}}
	if created.Checkpoint.Disposition != store.CheckpointDispositionSaved ||
		!reflect.DeepEqual(created.Checkpoint.References, wantReferences) {
		t.Fatalf("saved checkpoint = %#v, want references %#v", created.Checkpoint, wantReferences)
	}

	status, err := service.CheckpointStatus(CheckpointStatusInput{
		Host:       input.Host,
		SessionID:  input.SessionID,
		RootTurnID: input.RootTurnID,
	})
	if err != nil {
		t.Fatalf("saved checkpoint status: %v", err)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("status checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
}

func TestCheckpointErrorsHaveStableTransportCodes(t *testing.T) {
	service := newTestService(t)
	tests := []struct {
		name     string
		record   *CheckpointRecordInput
		status   *CheckpointStatusInput
		wantCode string
		wantErr  error
	}{
		{
			name: "unsupported disposition",
			record: &CheckpointRecordInput{
				Host: "codex", SessionID: "session", RootTurnID: "turn", Disposition: store.CheckpointDispositionNeedsReview,
			},
			wantCode: CheckpointErrorCodeInvalidDisposition,
			wantErr:  ErrCheckpointInvalidDisposition,
		},
		{
			name: "missing identity",
			record: &CheckpointRecordInput{
				SessionID: "session", RootTurnID: "turn", Disposition: store.CheckpointDispositionSkipped,
				ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
			},
			wantCode: CheckpointErrorCodeInvalidIdentity,
			wantErr:  store.ErrCheckpointInvalidIdentity,
		},
		{
			name: "processing failure is not a semantic skip",
			record: &CheckpointRecordInput{
				Host: "codex", SessionID: "session", RootTurnID: "turn", Disposition: store.CheckpointDispositionSkipped,
				ReasonCode: "processing_failed",
			},
			wantCode: CheckpointErrorCodeInvalidReason,
			wantErr:  store.ErrCheckpointInvalidReason,
		},
		{
			name:     "status not found",
			status:   &CheckpointStatusInput{Host: "codex", SessionID: "missing-session", RootTurnID: "missing-turn"},
			wantCode: CheckpointErrorCodeNotFound,
			wantErr:  store.ErrCheckpointNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.record != nil {
				_, err = service.RecordCheckpoint(*tt.record)
			} else {
				_, err = service.CheckpointStatus(*tt.status)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got := CheckpointErrorCode(err); got != tt.wantCode {
				t.Fatalf("error code = %q, want %q", got, tt.wantCode)
			}
		})
	}
}

func TestSavedCheckpointRejectsInvalidReferenceSetsWithoutRecording(t *testing.T) {
	service := newTestService(t)
	engramMemory := saveObservation(t, service, "engram", "Engram decision", "This Memory belongs to Engram.")
	otherMemory := saveObservation(t, service, "other", "Other decision", "This Memory belongs to another project.")
	tests := []struct {
		name      string
		memoryIDs []int64
		wantErr   error
		wantCode  string
	}{
		{name: "empty", wantErr: store.ErrCheckpointInvalidReferences, wantCode: CheckpointErrorCodeInvalidReferences},
		{name: "duplicate", memoryIDs: []int64{engramMemory.ID, engramMemory.ID}, wantErr: store.ErrCheckpointInvalidReferences, wantCode: CheckpointErrorCodeInvalidReferences},
		{name: "nonexistent", memoryIDs: []int64{engramMemory.ID + otherMemory.ID + 1000}, wantErr: store.ErrCheckpointMemoryNotFound, wantCode: CheckpointErrorCodeMemoryNotFound},
		{name: "cross project", memoryIDs: []int64{engramMemory.ID, otherMemory.ID}, wantErr: store.ErrCheckpointProjectMismatch, wantCode: CheckpointErrorCodeProjectMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := CheckpointStatusInput{
				Host:       "codex",
				SessionID:  "session-invalid-" + strings.ReplaceAll(tt.name, " ", "-"),
				RootTurnID: "turn-invalid-" + strings.ReplaceAll(tt.name, " ", "-"),
			}
			_, err := service.RecordCheckpoint(CheckpointRecordInput{
				Host:        identity.Host,
				SessionID:   identity.SessionID,
				RootTurnID:  identity.RootTurnID,
				Disposition: store.CheckpointDispositionSaved,
				Project:     "engram",
				MemoryIDs:   tt.memoryIDs,
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got := CheckpointErrorCode(err); got != tt.wantCode {
				t.Fatalf("error code = %q, want %q", got, tt.wantCode)
			}
			if _, statusErr := service.CheckpointStatus(identity); !errors.Is(statusErr, store.ErrCheckpointNotFound) {
				t.Fatalf("checkpoint changed after rejection: %v", statusErr)
			}
		})
	}
}

func TestSavedCheckpointRollsBackCreatedMemoriesAndCheckpointOnReferenceFailure(t *testing.T) {
	service := newTestService(t)
	if _, err := service.store.DB().Exec(`
		CREATE TRIGGER fail_checkpoint_reference
		BEFORE INSERT ON memory_checkpoint_references
		BEGIN
			SELECT RAISE(ABORT, 'injected checkpoint reference failure');
		END;`); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	_, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-atomic-rollback",
		RootTurnID:  "turn-atomic-rollback",
		Disposition: store.CheckpointDispositionSaved,
		Project:     "engram",
		CWD:         "/work/engram",
		Memories: []CheckpointMemoryInput{
			{Type: "decision", Title: "Atomic canary one", Content: "The first created Memory must roll back."},
			{Type: "discovery", Title: "Atomic canary two", Content: "The second created Memory must roll back."},
		},
	})
	if err == nil {
		t.Fatal("record saved checkpoint succeeded despite injected reference failure")
	}

	for table, want := range map[string]int{
		"sessions":                     0,
		"observations":                 0,
		"memory_checkpoints":           0,
		"memory_checkpoint_references": 0,
		"sync_mutations":               0,
	} {
		var got int
		if queryErr := service.store.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); queryErr != nil {
			t.Fatalf("count %s: %v", table, queryErr)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d after rollback", table, got, want)
		}
	}
}

func TestSavedCheckpointCreatesMultipleMemoriesReplaysAndReopens(t *testing.T) {
	cfg, err := store.DefaultConfig()
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	cfg.DataDir = t.TempDir()
	firstStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	service := New(firstStore)
	existing := saveObservation(t, service, "engram", "Existing Memory", "This Memory was saved earlier in the turn.")
	input := CheckpointRecordInput{
		Host:        "codex",
		SessionID:   "session-saved-create",
		RootTurnID:  "turn-saved-create",
		Disposition: store.CheckpointDispositionSaved,
		Project:     "ENGRAM",
		CWD:         "/work/engram",
		MemoryIDs:   []int64{existing.ID},
		Memories: []CheckpointMemoryInput{
			{Type: "decision", Title: "Created decision", Content: "Create this decision inside finalization."},
			{Type: "discovery", Title: "Created discovery", Content: "Create this discovery inside finalization."},
		},
	}

	created, err := service.RecordCheckpoint(input)
	if err != nil {
		t.Fatalf("record saved checkpoint: %v", err)
	}
	if len(created.Checkpoint.References) != 3 {
		t.Fatalf("references = %#v, want three", created.Checkpoint.References)
	}
	if created.Checkpoint.References[0].MemoryID != existing.ID {
		t.Fatalf("first reference = %#v, want existing Memory", created.Checkpoint.References[0])
	}
	for _, reference := range created.Checkpoint.References {
		if reference.Kind != store.CheckpointReferenceKindMemory || reference.Project != "engram" {
			t.Fatalf("reference = %#v", reference)
		}
		if _, getErr := service.Get(reference.MemoryID); getErr != nil {
			t.Fatalf("get referenced Memory %d: %v", reference.MemoryID, getErr)
		}
	}

	var memoriesBeforeReplay, mutationsBeforeReplay int
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&memoriesBeforeReplay); err != nil {
		t.Fatalf("count Memories: %v", err)
	}
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM sync_mutations`).Scan(&mutationsBeforeReplay); err != nil {
		t.Fatalf("count sync mutations: %v", err)
	}
	replayed, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
		Disposition: store.CheckpointDispositionSaved,
	})
	if err != nil {
		t.Fatalf("replay saved checkpoint: %v", err)
	}
	if replayed.Idempotency != CheckpointIdempotencyAlreadyRecorded || !reflect.DeepEqual(replayed.Checkpoint, created.Checkpoint) {
		t.Fatalf("replayed result = %#v, want original %#v", replayed, created)
	}
	var memoriesAfterReplay, mutationsAfterReplay int
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&memoriesAfterReplay); err != nil {
		t.Fatalf("count Memories after replay: %v", err)
	}
	if err := firstStore.DB().QueryRow(`SELECT COUNT(*) FROM sync_mutations`).Scan(&mutationsAfterReplay); err != nil {
		t.Fatalf("count sync mutations after replay: %v", err)
	}
	if memoriesAfterReplay != memoriesBeforeReplay || mutationsAfterReplay != mutationsBeforeReplay {
		t.Fatalf("replay changed state: Memories %d->%d, mutations %d->%d", memoriesBeforeReplay, memoriesAfterReplay, mutationsBeforeReplay, mutationsAfterReplay)
	}

	if err := firstStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	reopenedStore, err := store.New(cfg)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	status, err := New(reopenedStore).CheckpointStatus(CheckpointStatusInput{
		Host: input.Host, SessionID: input.SessionID, RootTurnID: input.RootTurnID,
	})
	if err != nil {
		t.Fatalf("status after reopen: %v", err)
	}
	if !reflect.DeepEqual(status.Checkpoint, created.Checkpoint) {
		t.Fatalf("reopened checkpoint = %#v, want %#v", status.Checkpoint, created.Checkpoint)
	}
}

func TestSavedCheckpointRejectsIllegalTerminalTransitions(t *testing.T) {
	service := newTestService(t)
	memory := saveObservation(t, service, "engram", "Transition Memory", "Use this Memory to exercise terminal transitions.")

	skippedIdentity := CheckpointRecordInput{
		Host: "codex", SessionID: "session-skipped-first", RootTurnID: "turn-skipped-first",
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	}
	if _, err := service.RecordCheckpoint(skippedIdentity); err != nil {
		t.Fatalf("record skipped checkpoint: %v", err)
	}
	_, err := service.RecordCheckpoint(CheckpointRecordInput{
		Host: skippedIdentity.Host, SessionID: skippedIdentity.SessionID, RootTurnID: skippedIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSaved, Project: "engram", MemoryIDs: []int64{memory.ID},
	})
	if !errors.Is(err, store.ErrCheckpointConflict) {
		t.Fatalf("saved after skipped error = %v, want conflict", err)
	}

	savedIdentity := CheckpointRecordInput{
		Host: "codex", SessionID: "session-saved-first", RootTurnID: "turn-saved-first",
		Disposition: store.CheckpointDispositionSaved, Project: "engram", MemoryIDs: []int64{memory.ID},
	}
	if _, err := service.RecordCheckpoint(savedIdentity); err != nil {
		t.Fatalf("record saved checkpoint: %v", err)
	}
	_, err = service.RecordCheckpoint(CheckpointRecordInput{
		Host: savedIdentity.Host, SessionID: savedIdentity.SessionID, RootTurnID: savedIdentity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
	})
	if !errors.Is(err, store.ErrCheckpointConflict) {
		t.Fatalf("skipped after saved error = %v, want conflict", err)
	}
}
