package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func TestRecallFeedbackReportIsSeparatelyInvokedAndAggregateOnly(t *testing.T) {
	cfg := testConfig(t)
	identity := store.CheckpointIdentity{
		Host: "codex-private", SessionID: "session-private", RootTurnID: "turn-private",
	}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.RecordRecallRunContext(context.Background(), store.RecallRunRecord{
		RecallID: "recall-report-private", Project: "engram", Scope: "project",
		DeliveredUTF8Bytes: 2, ElapsedMonotonicMS: 5, ProtocolVersion: 1, BinaryVersion: "test",
		TurnIdentity: &identity,
	}); err != nil {
		_ = s.Close()
		t.Fatalf("seed empty Recall run: %v", err)
	}
	falseEmpty := true
	result, err := memoryops.New(s).RecordCheckpoint(memoryops.CheckpointRecordInput{
		Host: identity.Host, SessionID: identity.SessionID, RootTurnID: identity.RootTurnID,
		Disposition: store.CheckpointDispositionSkipped, ReasonCode: store.CheckpointSkipReasonNoDurableKnowledge,
		RecallFeedback: &memoryops.RecallFeedbackInput{
			RecallID:   "recall-report-private",
			FalseEmpty: &memoryops.RecallFalseEmptyInput{Value: falseEmpty, Source: memoryops.RecallFeedbackSourceEvaluator},
		},
	})
	if err != nil || result.RecallFeedback == nil || result.RecallFeedback.Status != memoryops.RecallFeedbackStatusRecorded {
		_ = s.Close()
		t.Fatalf("seed Recall feedback: result=%#v err=%v", result, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	withArgs(t, "engram", "recall-feedback", "report", "--json")
	stdout, stderr := captureOutput(t, func() { cmdRecallFeedback(cfg) })
	if stderr != "" {
		t.Fatalf("Recall feedback report stderr = %q", stderr)
	}
	var report memoryops.RecallFeedbackReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("decode Recall feedback report: %v\n%s", err, stdout)
	}
	if report.SchemaVersion != memoryops.RecallFeedbackReportSchemaVersion || report.EmptyRuns != 1 ||
		len(report.Sources) != 3 || len(report.Operations) != 1 {
		t.Fatalf("Recall feedback report = %#v", report)
	}
	for _, forbidden := range []string{
		"codex-private", "session-private", "turn-private", "recall-report-private", "engram",
	} {
		if strings.Contains(stdout, forbidden) {
			t.Fatalf("aggregate report leaked %q: %s", forbidden, stdout)
		}
	}

	withArgs(t, "engram", "recall-feedback", "report")
	human, humanErr := captureOutput(t, func() { cmdRecallFeedback(cfg) })
	if humanErr != "" || !strings.Contains(human, "Recall feedback report") ||
		!strings.Contains(human, "false-empty") || strings.Contains(human, "recall-report-private") {
		t.Fatalf("human report stdout=%q stderr=%q", human, humanErr)
	}
}
