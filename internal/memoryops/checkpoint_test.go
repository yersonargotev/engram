package memoryops

import (
	"errors"
	"reflect"
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
				Host: "codex", SessionID: "session", RootTurnID: "turn", Disposition: store.CheckpointDispositionSaved,
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
