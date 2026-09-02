package recallstudy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProcessCohortRunnerExecutesOneFrozenMatchedBlock(t *testing.T) {
	if os.Getenv("ENGRAM_RECALL_STUDY_INTEGRATION") != "1" {
		t.Skip("set ENGRAM_RECALL_STUDY_INTEGRATION=1 to run the real Codex adapter")
	}
	sourceRepo := os.Getenv("ENGRAM_RECALL_STUDY_SOURCE_REPO")
	codexBinary := os.Getenv("ENGRAM_RECALL_STUDY_CODEX_BINARY")
	authFile := os.Getenv("ENGRAM_RECALL_STUDY_AUTH_FILE")
	if sourceRepo == "" || codexBinary == "" || authFile == "" {
		t.Fatal("real Recall study integration requires source repo, Codex binary, and auth file")
	}

	root := filepath.Join("..", "..", "evals", "recall-study", "v1")
	study, err := Load(filepath.Join(root, "contract.json"), filepath.Join(root, "contract.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	calibration, err := LoadManifest(filepath.Join(root, "calibration", "manifest.json"), filepath.Join(root, "calibration", "manifest.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	compatibility, err := ReadCompatibilityEvidence(filepath.Join(root, "private", "environment.json"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := study.Plan(&calibration.Manifest)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	runner, err := newProcessCohortRunner(ctx, study, &calibration.Manifest, compatibility.Compatibility, ExecutionRuntime{
		SourceRepo: sourceRepo, CodexBinary: codexBinary, AuthFile: authFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runner.Close(); err != nil {
			t.Error(err)
		}
	})

	firstUnit := plan[0].SamplingUnitID
	treatmentFilter := os.Getenv("ENGRAM_RECALL_STUDY_TREATMENT")
	seen := make(map[string]bool)
	for _, planned := range plan {
		if planned.SamplingUnitID != firstUnit || treatmentFilter != "" && planned.Treatment != treatmentFilter {
			continue
		}
		input := frozenTaskInput(study.Contract, &calibration.Manifest, planned.SamplingUnitID, planned.TaskClass)
		run, err := runner.Run(ctx, planned, input)
		if err != nil {
			var invalid *invalidExecutionError
			if planned.Treatment == "targeted-recall" && errors.As(err, &invalid) && invalid.reasonCode == "targeted_recall_not_observed" {
				if cleanupErr := run.Cleanup(); cleanupErr != nil {
					t.Fatal(cleanupErr)
				}
				seen[planned.Treatment] = true
				t.Log("targeted-recall: agent did not initiate Recall; calibration must continue-canary without opening held-out")
				continue
			}
			t.Fatalf("%s treatment: %v", planned.Treatment, err)
		}
		row := run.Row
		if row.Outcome != "completed" {
			t.Fatalf("%s treatment outcome=%s omission=%s", planned.Treatment, row.Outcome, row.OmissionCode)
		}
		seen[planned.Treatment] = true
		t.Logf("%s: task=%s results=%d checkpoint=%t stop_conflict_or_loop=%t", row.Treatment, row.TaskOutcome, row.RecallResultCount, row.CheckpointSucceeded, row.StopConflictOrLoop)
		if err := run.Cleanup(); err != nil {
			t.Fatal(err)
		}
	}
	wantTreatments := len(study.Contract.Treatments)
	if treatmentFilter != "" {
		wantTreatments = 1
	}
	if len(seen) != wantTreatments {
		t.Fatalf("matched block covered %d treatments, want %d", len(seen), wantTreatments)
	}
}
