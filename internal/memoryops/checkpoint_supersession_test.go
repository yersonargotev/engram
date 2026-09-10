package memoryops

import (
	"github.com/yersonargotev/engram/internal/store"
	"testing"
)

func TestCheckpointPreflightSupersessionCommitsEvaluatedTarget(t *testing.T) {
	service := newTestService(t)
	old, err := service.RecordCheckpoint(CheckpointRecordInput{Host: "codex", SessionID: "supersession", RootTurnID: "old", Disposition: "saved", Project: "test", Memories: []CheckpointMemoryInput{{Title: "Synthetic cache failure", Content: "Cache failure reproduces at commit abc."}}})
	if err != nil {
		t.Fatal(err)
	}
	memory := CheckpointMemoryInput{Title: "Synthetic cache failure resolved", Content: "The cache failure is resolved by commit def."}
	preflight, err := service.PreflightCheckpoint(CheckpointPreflightInput{Project: "test", Memories: []CheckpointMemoryInput{memory}})
	if err != nil || len(preflight.Candidates) != 1 {
		t.Fatalf("preflight %#v %v", preflight, err)
	}
	target := preflight.Candidates[0]
	if target.TargetVersion == "" || target.Reference.MemoryID != old.Checkpoint.References[0].MemoryID {
		t.Fatalf("target %#v", target)
	}
	index := 0
	result, err := service.RecordCheckpoint(CheckpointRecordInput{Host: "codex", SessionID: "supersession", RootTurnID: "new", Disposition: "saved", Project: "test", Memories: []CheckpointMemoryInput{memory}, Supersessions: []CheckpointSupersessionInput{{ReplacementInputIndex: &index, TargetMemoryID: target.Reference.MemoryID, TargetVersion: target.TargetVersion, Reason: "Verified fix at def"}}})
	if err != nil || result.Checkpoint.Disposition != store.CheckpointDispositionSaved {
		t.Fatalf("record %#v %v", result, err)
	}
}
