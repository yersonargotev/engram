package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

// These tests keep the CLI contracts aligned with mem_judge and mem_compare:
// CLI judging has human provenance, while compare persists an already-decided
// semantic verdict and preserves not_conflict as a successful no-op.

func TestCmdConflictsJudge_ParityHappyPath(t *testing.T) {
	cfg := testConfig(t)
	_, _, judgmentID := seedRelation(t, cfg, "judge-cli")

	withArgs(t, "engram", "conflicts", "judge", judgmentID,
		"--relation", "supersedes", "--reason", "new decision", "--evidence", "issue-42",
		"--session-id", "cli-session", "--json")
	stdout, stderr := captureOutput(t, func() { cmdConflicts(cfg) })
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}

	var envelope struct {
		Relation store.Relation `json:"relation"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("judge JSON: %v; output=%q", err, stdout)
	}
	if envelope.Relation.SyncID != judgmentID || envelope.Relation.Relation != "supersedes" || envelope.Relation.JudgmentStatus != "judged" {
		t.Fatalf("unexpected judged relation: %#v", envelope.Relation)
	}
	if envelope.Relation.MarkedByActor == nil || *envelope.Relation.MarkedByActor != "cli" {
		t.Fatalf("expected cli actor, got %#v", envelope.Relation.MarkedByActor)
	}
	if envelope.Relation.MarkedByKind == nil || *envelope.Relation.MarkedByKind != "human" {
		t.Fatalf("expected human kind, got %#v", envelope.Relation.MarkedByKind)
	}
	if envelope.Relation.Confidence == nil || *envelope.Relation.Confidence != 1 {
		t.Fatalf("expected default confidence 1, got %#v", envelope.Relation.Confidence)
	}
}

func TestCmdConflictsJudge_RequiresRelation(t *testing.T) {
	cfg := testConfig(t)
	_, _, judgmentID := seedRelation(t, cfg, "judge-required")
	withArgs(t, "engram", "conflicts", "judge", judgmentID, "--json")
	stubExitWithPanic(t)
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConflicts(cfg) })
	if recovered == nil {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr, "--relation is required") {
		t.Fatalf("expected missing relation error, got %q", stderr)
	}
}

func TestCmdConflictsCompare_ParityHappyPath(t *testing.T) {
	cfg := testConfig(t)
	srcSync, tgtSync, _ := seedRelation(t, cfg, "compare-cli")
	db := openTestDB(t, cfg)
	var idA, idB int64
	if err := db.QueryRow(`SELECT id FROM observations WHERE sync_id = ?`, srcSync).Scan(&idA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT id FROM observations WHERE sync_id = ?`, tgtSync).Scan(&idB); err != nil {
		t.Fatal(err)
	}

	// Use the real IDs rather than relying on fixture insertion order.
	withArgs(t, "engram", "conflicts", "compare", int64String(idA), int64String(idB),
		"--relation", "related", "--confidence", "0.75", "--reasoning", "Both record the same design context.", "--model", "reviewer-v1", "--json")
	stdout, stderr := captureOutput(t, func() { cmdConflicts(cfg) })
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	var envelope struct {
		SyncID string `json:"sync_id"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("compare JSON: %v; output=%q", err, stdout)
	}
	if envelope.SyncID == "" {
		t.Fatal("expected persisted relation sync_id")
	}
	s, err := store.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rel, err := s.GetRelation(envelope.SyncID)
	if err != nil {
		t.Fatal(err)
	}
	if rel.Relation != "related" || rel.MarkedByKind == nil || *rel.MarkedByKind != "system" {
		t.Fatalf("unexpected persisted relation: %#v", rel)
	}
}

func TestCmdConflictsCompare_NotConflictIsSuccessfulNoOp(t *testing.T) {
	cfg := testConfig(t)
	srcSync, tgtSync, _ := seedRelation(t, cfg, "compare-noop")
	db := openTestDB(t, cfg)
	var idA, idB int64
	if err := db.QueryRow(`SELECT id FROM observations WHERE sync_id = ?`, srcSync).Scan(&idA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT id FROM observations WHERE sync_id = ?`, tgtSync).Scan(&idB); err != nil {
		t.Fatal(err)
	}

	withArgs(t, "engram", "conflicts", "compare", int64String(idA), int64String(idB),
		"--relation", "not_conflict", "--confidence", "1", "--reasoning", "These memories concern different topics.", "--json")
	stdout, stderr := captureOutput(t, func() { cmdConflicts(cfg) })
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
	var envelope map[string]string
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope["sync_id"] != "" {
		t.Fatalf("not_conflict must return an empty sync_id, got %q", envelope["sync_id"])
	}
}

func TestCmdConflictsCompare_RejectsCrossProject(t *testing.T) {
	cfg := testConfig(t)
	srcA, _, _ := seedRelation(t, cfg, "compare-project-a")
	srcB, _, _ := seedRelation(t, cfg, "compare-project-b")
	db := openTestDB(t, cfg)
	var idA, idB int64
	if err := db.QueryRow(`SELECT id FROM observations WHERE sync_id = ?`, srcA).Scan(&idA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT id FROM observations WHERE sync_id = ?`, srcB).Scan(&idB); err != nil {
		t.Fatal(err)
	}

	withArgs(t, "engram", "conflicts", "compare", int64String(idA), int64String(idB),
		"--relation", "conflicts_with", "--confidence", "0.9", "--reasoning", "They conflict.")
	stubExitWithPanic(t)
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConflicts(cfg) })
	if recovered == nil {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(stderr, "different projects") {
		t.Fatalf("expected cross-project error, recovered=%v stderr=%q", recovered, stderr)
	}
}

func int64String(value int64) string {
	return fmt.Sprintf("%d", value)
}
