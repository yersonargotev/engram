package recallbaseline

import (
	"fmt"
	"math"
)

const PowerSchemaVersion = "recall-baseline-power-v1"

type PowerAssumptions struct {
	BaselineRate                float64 `json:"baseline_rate"`
	MinimumDetectableDifference float64 `json:"minimum_detectable_absolute_difference"`
	Alpha                       float64 `json:"familywise_alpha"`
	Power                       float64 `json:"power"`
	Comparisons                 int     `json:"comparisons"`
	Treatments                  int     `json:"treatments"`
}

type PowerAnalysis struct {
	SchemaVersion        string           `json:"schema_version"`
	Method               string           `json:"method"`
	Assumptions          PowerAssumptions `json:"assumptions"`
	AlternativeRate      float64          `json:"alternative_rate"`
	PerComparisonAlpha   float64          `json:"per_comparison_alpha"`
	RequiredPerTreatment int              `json:"required_per_treatment"`
	RequiredTotal        int              `json:"required_total"`
	HeldOutAccessed      bool             `json:"held_out_accessed"`
}

// AnalyzePower uses the standard normal approximation for two independent
// proportions. It is deliberately conservative for the later paired study and
// applies Bonferroni correction to the declared familywise alpha. The function
// accepts no dataset or path, so held-out sessions cannot affect the result.
func AnalyzePower(assumptions PowerAssumptions) (PowerAnalysis, error) {
	alternative := assumptions.BaselineRate + assumptions.MinimumDetectableDifference
	switch {
	case !finite(assumptions.BaselineRate) || !finite(assumptions.MinimumDetectableDifference) ||
		!finite(assumptions.Alpha) || !finite(assumptions.Power):
		return PowerAnalysis{}, fmt.Errorf("power assumptions must be finite")
	case assumptions.BaselineRate <= 0 || assumptions.BaselineRate >= 1:
		return PowerAnalysis{}, fmt.Errorf("baseline rate must be between zero and one")
	case assumptions.MinimumDetectableDifference <= 0 || alternative >= 1:
		return PowerAnalysis{}, fmt.Errorf("minimum detectable difference must produce an alternative rate between zero and one")
	case assumptions.Alpha <= 0 || assumptions.Alpha >= 1:
		return PowerAnalysis{}, fmt.Errorf("familywise alpha must be between zero and one")
	case assumptions.Power <= 0.5 || assumptions.Power >= 1:
		return PowerAnalysis{}, fmt.Errorf("power must be greater than one half and less than one")
	case assumptions.Comparisons < 1:
		return PowerAnalysis{}, fmt.Errorf("comparisons must be positive")
	case assumptions.Treatments < 2:
		return PowerAnalysis{}, fmt.Errorf("treatments must be at least two")
	}

	perComparisonAlpha := assumptions.Alpha / float64(assumptions.Comparisons)
	baseline := assumptions.BaselineRate
	pooled := (baseline + alternative) / 2
	zAlpha := normalQuantile(1 - perComparisonAlpha/2)
	zPower := normalQuantile(assumptions.Power)
	numerator := zAlpha*math.Sqrt(2*pooled*(1-pooled)) +
		zPower*math.Sqrt(baseline*(1-baseline)+alternative*(1-alternative))
	requiredFloat := math.Ceil((numerator * numerator) /
		(assumptions.MinimumDetectableDifference * assumptions.MinimumDetectableDifference))
	maxInt := int(^uint(0) >> 1)
	if !finite(requiredFloat) || requiredFloat > float64(maxInt/assumptions.Treatments) {
		return PowerAnalysis{}, fmt.Errorf("declared assumptions require an unsupported sample size")
	}
	required := int(requiredFloat)

	return PowerAnalysis{
		SchemaVersion:        PowerSchemaVersion,
		Method:               "two-sided-two-proportion-normal-bonferroni-v1",
		Assumptions:          assumptions,
		AlternativeRate:      alternative,
		PerComparisonAlpha:   perComparisonAlpha,
		RequiredPerTreatment: required,
		RequiredTotal:        required * assumptions.Treatments,
		HeldOutAccessed:      false,
	}, nil
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func normalQuantile(probability float64) float64 {
	return math.Sqrt2 * math.Erfinv(2*probability-1)
}
