package recallbaseline

import (
	"math"
	"strings"
	"testing"
)

func TestAnalyzePowerDerivesReproducibleControlledStudySize(t *testing.T) {
	t.Parallel()

	analysis, err := AnalyzePower(PowerAssumptions{
		BaselineRate:                0.50,
		MinimumDetectableDifference: 0.10,
		Alpha:                       0.05,
		Power:                       0.80,
		Comparisons:                 3,
		Treatments:                  3,
	})
	if err != nil {
		t.Fatalf("AnalyzePower() error = %v", err)
	}
	if analysis.SchemaVersion != PowerSchemaVersion || analysis.Method != "two-sided-two-proportion-normal-bonferroni-v1" {
		t.Fatalf("analysis identity = %+v", analysis)
	}
	if math.Abs(analysis.PerComparisonAlpha-(0.05/3)) > 1e-12 {
		t.Fatalf("per-comparison alpha = %.15f", analysis.PerComparisonAlpha)
	}
	if analysis.RequiredPerTreatment != 517 || analysis.RequiredTotal != 1551 {
		t.Fatalf("required sample = %d per treatment, %d total", analysis.RequiredPerTreatment, analysis.RequiredTotal)
	}
	if analysis.HeldOutAccessed {
		t.Fatal("power analysis must not access held-out sessions")
	}
}

func TestAnalyzePowerRejectsUndeclaredOrImpossibleAssumptions(t *testing.T) {
	t.Parallel()

	for _, assumptions := range []PowerAssumptions{
		{},
		{BaselineRate: .95, MinimumDetectableDifference: .10, Alpha: .05, Power: .80, Comparisons: 1, Treatments: 2},
		{BaselineRate: .50, MinimumDetectableDifference: .10, Alpha: 0, Power: .80, Comparisons: 1, Treatments: 2},
		{BaselineRate: .50, MinimumDetectableDifference: 1e-300, Alpha: .05, Power: .80, Comparisons: 1, Treatments: 2},
	} {
		if _, err := AnalyzePower(assumptions); err == nil {
			t.Fatalf("AnalyzePower(%+v) succeeded, want error", assumptions)
		}
	}
}

func TestAnalyzePowerRejectsNonFiniteAssumptions(t *testing.T) {
	t.Parallel()

	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := AnalyzePower(PowerAssumptions{
			BaselineRate: value, MinimumDetectableDifference: 0.1,
			Alpha: 0.05, Power: 0.8, Comparisons: 3, Treatments: 3,
		})
		if err == nil || !strings.Contains(err.Error(), "finite") {
			t.Fatalf("AnalyzePower(%v) error = %v", value, err)
		}
	}
}
