package recallquery_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/yersonargotev/engram/internal/recallquery"
)

func TestCalibrationRunsBoundedCoreRecallWithoutHeldOut(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"contract.json", "calibration.json"} {
		raw, err := os.ReadFile(filepath.Join("../../evals/recall-query/v1", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	report, err := recallquery.Execute(context.Background(), root, false)
	if err != nil {
		t.Fatal(err)
	}
	if !report.CalibrationPassed || report.HeldOutOpened || len(report.Groups) != 12 {
		t.Fatalf("calibration gate: %+v", report)
	}
	for _, g := range report.Groups {
		if g.Attempted != 3 || g.OperationalFailures != 0 || g.First.Completed != 3 || g.Cumulative.Completed != 3 {
			t.Fatalf("accounting: %+v", g)
		}
		if g.Cumulative.Exposures > 30 || g.Cumulative.CandidateBytes > 3*8192 {
			t.Fatalf("budget bypass: %+v", g)
		}
		if g.Labels.UnknownUtility != g.Cumulative.Exposures || g.Labels.ExplicitAssessments != 0 {
			t.Fatalf("fabricated utility: %+v", g.Labels)
		}
		if g.Variant == "editorial_supersession" && g.Cumulative.StaleExposures != 0 {
			t.Fatalf("supersession leaked: %+v", g)
		}
	}
}

func TestCancelledCalibrationKeepsHeldOutClosedAndQualityUnknown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := recallquery.Execute(ctx, "../../evals/recall-query/v1", true)
	if err != nil {
		t.Fatal(err)
	}
	if report.CalibrationPassed || report.HeldOutOpened || report.Disposition != "evaluation_incomplete" {
		t.Fatalf("failed prerequisite bypass: %+v", report)
	}
	for _, g := range report.Groups {
		if g.OperationalFailures != 3 || g.First.Completed != 0 || g.First.Empty != 0 || g.First.NonemptyMissingUseful != 0 {
			t.Fatalf("operational failure became quality result: %+v", g)
		}
	}
}
