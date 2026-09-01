package recallstudy

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

type treatmentPair struct {
	broad    RunRow
	targeted RunRow
}

type labelStats struct {
	exposed         int
	explicitUtility int
	useful          int
	noise           int
	explicitQuality int
	harm            int
	explicitLabels  int
}

func (study *Study) deriveMetrics(rows []RunRow) ([]MetricEvidence, error) {
	pairs, omitted, err := pairedRows(rows)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, fmt.Errorf("Recall study metrics require at least one completed broad/targeted pair")
	}
	resamples := study.Contract.Intervals.BootstrapResamples
	seed := study.Contract.Intervals.BootstrapSeed
	evidence := make([]MetricEvidence, 0, 12)

	evidence = append(evidence, pairedBooleanMetric(
		"checkpoint_rate_delta_pp", pairs, omitted, seed, resamples,
		func(row RunRow) bool { return row.CheckpointSucceeded },
	))
	evidence = append(evidence, pairedBooleanMetric(
		"stop_growth_pp", pairs, omitted, seed, resamples,
		func(row RunRow) bool { return row.StopConflictOrLoop },
	))

	bytesMetric, err := pairedReductionMetric(
		"automatic_injected_bytes_reduction_percent", pairs, omitted, seed, resamples,
		func(row RunRow) float64 { return float64(row.AutomaticInjectedUTF8Bytes) }, mean,
	)
	if err != nil {
		return nil, err
	}
	evidence = append(evidence, bytesMetric)

	startupMetric, err := pairedReductionMetric(
		"startup_compact_p95_reduction_percent", pairs, omitted, seed, resamples,
		func(row RunRow) float64 { return row.StartupCompactLatencyMillis },
		func(values []float64) float64 { return percentile(values, .95) },
	)
	if err != nil {
		return nil, err
	}
	evidence = append(evidence, startupMetric)

	recallLatencies := make([]float64, 0, len(pairs))
	for _, pair := range pairs {
		if pair.targeted.RecallLatencyMillis > 0 {
			recallLatencies = append(recallLatencies, pair.targeted.RecallLatencyMillis)
		}
	}
	if len(recallLatencies) == 0 {
		return nil, fmt.Errorf("Recall study metrics require targeted Recall latency observations")
	}
	recallPoint := percentile(recallLatencies, .95)
	recallLower, recallUpper := bootstrapValues(seed+"\x00recall_p95_ms", recallLatencies, resamples, func(sample []float64) float64 {
		return percentile(sample, .95)
	})
	evidence = append(evidence, MetricEvidence{Metric: "recall_p95_ms", Point: recallPoint, CILower: recallLower, CIUpper: recallUpper,
		Numerator: int(math.Round(recallPoint)), Denominator: len(recallLatencies), Unknown: len(rows)/3 - len(recallLatencies)})

	utilityMetric, err := pairedLabelRatioMetric("utility_relative_improvement_percent", pairs, omitted, seed, resamples,
		func(stats labelStats) (int, int) { return stats.useful, stats.explicitUtility }, true)
	if err != nil {
		return nil, err
	}
	evidence = append(evidence, utilityMetric)

	targetNoise, targetUtilityDenominator, targetUtilityUnknown := aggregateTargetLabelRate(pairs, func(stats labelStats) (int, int, int) {
		return stats.noise, stats.explicitUtility, stats.exposed - stats.explicitUtility
	})
	noiseLower, noiseUpper := wilsonPercent(targetNoise, targetUtilityDenominator)
	evidence = append(evidence, MetricEvidence{Metric: "noise_rate_percent", Point: percent(targetNoise, targetUtilityDenominator),
		CILower: noiseLower, CIUpper: noiseUpper, Numerator: targetNoise, Denominator: targetUtilityDenominator, Unknown: targetUtilityUnknown})
	noiseImprovement, err := pairedLabelRatioMetric("noise_improvement_pp", pairs, omitted, seed, resamples,
		func(stats labelStats) (int, int) { return stats.noise, stats.explicitUtility }, false)
	if err != nil {
		return nil, err
	}
	evidence = append(evidence, noiseImprovement)

	targetHarm, targetQualityDenominator, targetQualityUnknown := aggregateTargetLabelRate(pairs, func(stats labelStats) (int, int, int) {
		return stats.harm, stats.explicitQuality, stats.exposed - stats.explicitQuality
	})
	harmLower, harmUpper := wilsonPercent(targetHarm, targetQualityDenominator)
	evidence = append(evidence, MetricEvidence{Metric: "harm_rate_percent", Point: percent(targetHarm, targetQualityDenominator),
		CILower: harmLower, CIUpper: harmUpper, Numerator: targetHarm, Denominator: targetQualityDenominator, Unknown: targetQualityUnknown})
	harmDifference, err := pairedLabelRatioMetric("harm_difference_pp", pairs, omitted, seed, resamples,
		func(stats labelStats) (int, int) { return stats.harm, stats.explicitQuality }, false)
	if err != nil {
		return nil, err
	}
	harmDifference.Point = -harmDifference.Point
	harmDifference.CILower, harmDifference.CIUpper = -harmDifference.CIUpper, -harmDifference.CILower
	evidence = append(evidence, harmDifference)

	falseEmpty, reviewedEmpty, unknownEmpty := 0, 0, 0
	for _, pair := range pairs {
		if pair.targeted.RecallResultCount != 0 {
			continue
		}
		switch pair.targeted.FalseEmptyReview {
		case "confirmed":
			falseEmpty++
			reviewedEmpty++
		case "rejected":
			reviewedEmpty++
		case "unknown":
			unknownEmpty++
		}
	}
	falseLower, falseUpper := wilsonPercent(falseEmpty, reviewedEmpty)
	evidence = append(evidence, MetricEvidence{Metric: "false_empty_rate_percent", Point: percent(falseEmpty, reviewedEmpty),
		CILower: falseLower, CIUpper: falseUpper, Numerator: falseEmpty, Denominator: reviewedEmpty, Unknown: unknownEmpty})

	exposed, explicit := 0, 0
	for _, pair := range pairs {
		for _, row := range []RunRow{pair.broad, pair.targeted} {
			stats := rowLabelStats(row)
			exposed += stats.exposed
			explicit += stats.explicitLabels
		}
	}
	coverageLower, coverageUpper := wilsonPercent(explicit, exposed)
	evidence = append(evidence, MetricEvidence{Metric: "explicit_label_coverage_percent", Point: percent(explicit, exposed),
		CILower: coverageLower, CIUpper: coverageUpper, Numerator: explicit, Denominator: exposed, Unknown: exposed - explicit})
	return evidence, nil
}

func (study *Study) deriveTreatmentMetrics(rows []RunRow) ([]TreatmentMetric, error) {
	byTreatment := make(map[string][]RunRow, len(study.Contract.Treatments))
	for _, row := range rows {
		if row.Outcome == "completed" {
			byTreatment[row.Treatment] = append(byTreatment[row.Treatment], row)
		}
	}
	result := make([]TreatmentMetric, 0, len(study.Contract.Treatments)*10)
	for _, treatment := range study.Contract.Treatments {
		completed := byTreatment[treatment.ID]
		unknownRuns := countTreatmentRows(rows, treatment.ID) - len(completed)
		if len(completed) == 0 {
			return nil, fmt.Errorf("Recall study treatment %s has no completed rows", treatment.ID)
		}
		successful := make([]RunRow, 0, len(completed))
		for _, row := range completed {
			if row.TaskOutcome == "succeeded" {
				successful = append(successful, row)
			}
		}
		result = append(result,
			treatmentRateMetric(treatment.ID, "task_success_rate_percent", completed, unknownRuns, func(row RunRow) bool { return row.TaskOutcome == "succeeded" }),
			treatmentRateMetric(treatment.ID, "checkpoint_success_rate_percent", completed, unknownRuns, func(row RunRow) bool { return row.CheckpointSucceeded }),
			treatmentRateMetric(treatment.ID, "stop_conflict_or_loop_rate_percent", completed, unknownRuns, func(row RunRow) bool { return row.StopConflictOrLoop }),
			treatmentValueMetric(study, treatment.ID, "automatic_injected_bytes_mean", completed, unknownRuns, func(row RunRow) float64 { return float64(row.AutomaticInjectedUTF8Bytes) }, mean),
			treatmentValueMetric(study, treatment.ID, "startup_compact_p95_ms", completed, unknownRuns, func(row RunRow) float64 { return row.StartupCompactLatencyMillis }, func(values []float64) float64 { return percentile(values, .95) }),
		)
		if len(successful) == 0 {
			result = append(result, unavailableTreatmentValueMetric(treatment.ID, "time_to_useful_p95_ms", countTreatmentRows(rows, treatment.ID)))
		} else {
			result = append(result, treatmentValueMetric(study, treatment.ID, "time_to_useful_p95_ms", successful,
				unknownRuns+len(completed)-len(successful), func(row RunRow) float64 { return row.TimeToUsefulMillis }, func(values []float64) float64 { return percentile(values, .95) }))
		}
		if treatment.ID == "targeted-recall" {
			result = append(result, treatmentValueMetric(study, treatment.ID, "recall_p95_ms", completed, unknownRuns,
				func(row RunRow) float64 { return row.RecallLatencyMillis }, func(values []float64) float64 { return percentile(values, .95) }))
		}
		result = append(result, treatmentLabelMetrics(treatment.ID, completed, unknownRuns)...)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Treatment == result[j].Treatment {
			return result[i].Metric < result[j].Metric
		}
		return result[i].Treatment < result[j].Treatment
	})
	return result, nil
}

func treatmentRateMetric(treatment, metric string, rows []RunRow, unknown int, predicate func(RunRow) bool) TreatmentMetric {
	count := 0
	for _, row := range rows {
		count += boolInt(predicate(row))
	}
	lower, upper := wilsonPercent(count, len(rows))
	return TreatmentMetric{Treatment: treatment, Metric: metric, Available: true, Point: percent(count, len(rows)), CILower: lower, CIUpper: upper,
		Numerator: count, Denominator: len(rows), Unknown: unknown}
}

func treatmentValueMetric(study *Study, treatment, metric string, rows []RunRow, unknown int, value func(RunRow) float64, statistic func([]float64) float64) TreatmentMetric {
	values := make([]float64, len(rows))
	for index, row := range rows {
		values[index] = value(row)
	}
	point := statistic(values)
	lower, upper := bootstrapValues(study.Contract.Intervals.BootstrapSeed+"\x00"+treatment+"\x00"+metric, values,
		study.Contract.Intervals.BootstrapResamples, statistic)
	return TreatmentMetric{Treatment: treatment, Metric: metric, Available: true, Point: point, CILower: lower, CIUpper: upper,
		Numerator: int(math.Round(sum(values))), Denominator: len(values), Unknown: unknown}
}

func unavailableTreatmentValueMetric(treatment, metric string, unknown int) TreatmentMetric {
	return TreatmentMetric{Treatment: treatment, Metric: metric, Available: false, Unknown: unknown}
}

func treatmentLabelMetrics(treatment string, rows []RunRow, unknownRuns int) []TreatmentMetric {
	aggregate := labelStats{}
	duplicate := 0
	for _, row := range rows {
		stats := rowLabelStats(row)
		aggregate.exposed += stats.exposed
		aggregate.explicitUtility += stats.explicitUtility
		aggregate.useful += stats.useful
		aggregate.noise += stats.noise
		aggregate.explicitQuality += stats.explicitQuality
		aggregate.harm += stats.harm
		aggregate.explicitLabels += stats.explicitLabels
		duplicate += duplicateResults(row)
	}
	metric := func(id string, numerator, denominator, unknown int) TreatmentMetric {
		if denominator == 0 {
			return TreatmentMetric{Treatment: treatment, Metric: id, Available: false, Unknown: unknown + unknownRuns}
		}
		lower, upper := wilsonPercent(numerator, denominator)
		return TreatmentMetric{Treatment: treatment, Metric: id, Available: true, Point: percent(numerator, denominator), CILower: lower, CIUpper: upper,
			Numerator: numerator, Denominator: denominator, Unknown: unknown + unknownRuns}
	}
	return []TreatmentMetric{
		metric("utility_rate_percent", aggregate.useful, aggregate.explicitUtility, aggregate.exposed-aggregate.explicitUtility),
		metric("noise_rate_percent", aggregate.noise, aggregate.explicitUtility, aggregate.exposed-aggregate.explicitUtility),
		metric("duplicate_rate_percent", duplicate, aggregate.explicitUtility, aggregate.exposed-aggregate.explicitUtility),
		metric("harm_rate_percent", aggregate.harm, aggregate.explicitQuality, aggregate.exposed-aggregate.explicitQuality),
		metric("explicit_label_coverage_percent", aggregate.explicitLabels, aggregate.exposed, aggregate.exposed-aggregate.explicitLabels),
	}
}

func duplicateResults(row RunRow) int {
	seen := make(map[string]bool)
	for _, assessment := range row.Assessments {
		if assessment.Source != "unknown" && assessment.Utility == "duplicate" {
			seen[assessment.ResultKey] = true
		}
	}
	return len(seen)
}

func countTreatmentRows(rows []RunRow, treatment string) int {
	count := 0
	for _, row := range rows {
		if row.Treatment == treatment {
			count++
		}
	}
	return count
}

func pairedRows(rows []RunRow) ([]treatmentPair, int, error) {
	byUnit := make(map[string]map[string]RunRow)
	for _, row := range rows {
		if byUnit[row.SamplingUnitID] == nil {
			byUnit[row.SamplingUnitID] = make(map[string]RunRow)
		}
		byUnit[row.SamplingUnitID][row.Treatment] = row
	}
	ids := make([]string, 0, len(byUnit))
	for id := range byUnit {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	pairs := make([]treatmentPair, 0, len(ids))
	omitted := 0
	for _, id := range ids {
		broad, broadOK := byUnit[id]["broad-chronological"]
		targeted, targetedOK := byUnit[id]["targeted-recall"]
		if !broadOK || !targetedOK {
			return nil, 0, fmt.Errorf("Recall study metrics require paired broad and targeted rows")
		}
		if broad.Outcome != "completed" || targeted.Outcome != "completed" {
			omitted++
			continue
		}
		pairs = append(pairs, treatmentPair{broad: broad, targeted: targeted})
	}
	return pairs, omitted, nil
}

func pairedBooleanMetric(metric string, pairs []treatmentPair, omitted int, seed string, resamples int, value func(RunRow) bool) MetricEvidence {
	differences := make([]float64, len(pairs))
	targetCount := 0
	for index, pair := range pairs {
		broad, targeted := boolFloat(value(pair.broad)), boolFloat(value(pair.targeted))
		differences[index] = (targeted - broad) * 100
		targetCount += int(targeted)
	}
	point := mean(differences)
	lower, upper := bootstrapValues(seed+"\x00"+metric, differences, resamples, mean)
	return MetricEvidence{Metric: metric, Point: point, CILower: lower, CIUpper: upper, Numerator: targetCount, Denominator: len(pairs), Unknown: omitted}
}

func pairedReductionMetric(metric string, pairs []treatmentPair, omitted int, seed string, resamples int, value func(RunRow) float64, aggregate func([]float64) float64) (MetricEvidence, error) {
	statistic := func(indices []int) float64 {
		broad := make([]float64, len(indices))
		targeted := make([]float64, len(indices))
		for output, index := range indices {
			broad[output] = value(pairs[index].broad)
			targeted[output] = value(pairs[index].targeted)
		}
		baseline := aggregate(broad)
		if baseline <= 0 {
			return math.NaN()
		}
		return 100 * (1 - aggregate(targeted)/baseline)
	}
	point := statistic(identityIndices(len(pairs)))
	if !finite(point) {
		return MetricEvidence{}, fmt.Errorf("Recall study metric %s requires a positive broad-treatment baseline", metric)
	}
	lower, upper, ok := bootstrapIndices(seed+"\x00"+metric, len(pairs), resamples, statistic)
	if !ok {
		return MetricEvidence{}, fmt.Errorf("Recall study metric %s could not derive its frozen interval", metric)
	}
	targetTotal, broadTotal := 0.0, 0.0
	for _, pair := range pairs {
		targetTotal += value(pair.targeted)
		broadTotal += value(pair.broad)
	}
	return MetricEvidence{Metric: metric, Point: point, CILower: lower, CIUpper: upper,
		Numerator: int(math.Round(targetTotal)), Denominator: int(math.Round(broadTotal)), Unknown: omitted}, nil
}

func pairedLabelRatioMetric(metric string, pairs []treatmentPair, omitted int, seed string, resamples int, selectCounts func(labelStats) (int, int), relative bool) (MetricEvidence, error) {
	statistic := func(indices []int) float64 {
		broadNumerator, broadDenominator := 0, 0
		targetNumerator, targetDenominator := 0, 0
		for _, index := range indices {
			bn, bd := selectCounts(rowLabelStats(pairs[index].broad))
			tn, td := selectCounts(rowLabelStats(pairs[index].targeted))
			broadNumerator += bn
			broadDenominator += bd
			targetNumerator += tn
			targetDenominator += td
		}
		if broadDenominator == 0 || targetDenominator == 0 {
			return math.NaN()
		}
		broadRate := float64(broadNumerator) / float64(broadDenominator)
		targetRate := float64(targetNumerator) / float64(targetDenominator)
		if relative {
			if broadRate == 0 {
				return math.NaN()
			}
			return 100 * (targetRate/broadRate - 1)
		}
		return 100 * (broadRate - targetRate)
	}
	point := statistic(identityIndices(len(pairs)))
	if !finite(point) {
		return MetricEvidence{}, fmt.Errorf("Recall study metric %s requires explicit labels in both compared treatments", metric)
	}
	lower, upper, ok := bootstrapIndices(seed+"\x00"+metric, len(pairs), resamples, statistic)
	if !ok {
		return MetricEvidence{}, fmt.Errorf("Recall study metric %s could not derive its frozen interval", metric)
	}
	targetNumerator, targetDenominator, unknown := 0, 0, 0
	for _, pair := range pairs {
		stats := rowLabelStats(pair.targeted)
		numerator, denominator := selectCounts(stats)
		targetNumerator += numerator
		targetDenominator += denominator
		unknown += stats.exposed - denominator
	}
	return MetricEvidence{Metric: metric, Point: point, CILower: lower, CIUpper: upper,
		Numerator: targetNumerator, Denominator: targetDenominator, Unknown: unknown + omitted}, nil
}

func aggregateTargetLabelRate(pairs []treatmentPair, selectCounts func(labelStats) (int, int, int)) (int, int, int) {
	numerator, denominator, unknown := 0, 0, 0
	for _, pair := range pairs {
		n, d, u := selectCounts(rowLabelStats(pair.targeted))
		numerator += n
		denominator += d
		unknown += u
	}
	return numerator, denominator, unknown
}

func rowLabelStats(row RunRow) labelStats {
	stats := labelStats{exposed: row.RecallResultCount}
	type resultState struct{ utility, quality, useful, noise, harm bool }
	results := make(map[string]resultState)
	for _, assessment := range row.Assessments {
		state := results[assessment.ResultKey]
		if assessment.Source != "unknown" {
			if assessment.Utility != "unknown" {
				state.utility = true
				state.useful = state.useful || assessment.Utility == "decisive" || assessment.Utility == "orienting"
				state.noise = state.noise || assessment.Utility == "duplicate" || assessment.Utility == "unused"
			}
			if assessment.Quality != "unknown" {
				state.quality = true
				state.harm = state.harm || assessment.Quality == "stale" || assessment.Quality == "contradictory"
			}
		}
		results[assessment.ResultKey] = state
	}
	for _, state := range results {
		stats.explicitUtility += boolInt(state.utility)
		stats.useful += boolInt(state.useful)
		stats.noise += boolInt(state.noise)
		stats.explicitQuality += boolInt(state.quality)
		stats.harm += boolInt(state.harm)
		stats.explicitLabels += boolInt(state.utility && state.quality)
	}
	return stats
}

func wilsonPercent(numerator, denominator int) (float64, float64) {
	if denominator == 0 {
		return 0, 100
	}
	z := 1.959963984540054
	n := float64(denominator)
	p := float64(numerator) / n
	denominatorTerm := 1 + z*z/n
	center := (p + z*z/(2*n)) / denominatorTerm
	margin := z * math.Sqrt((p*(1-p)+z*z/(4*n))/n) / denominatorTerm
	return 100 * math.Max(0, center-margin), 100 * math.Min(1, center+margin)
}

func bootstrapValues(seed string, values []float64, resamples int, statistic func([]float64) float64) (float64, float64) {
	indicesStatistic := func(indices []int) float64 {
		sample := make([]float64, len(indices))
		for output, index := range indices {
			sample[output] = values[index]
		}
		return statistic(sample)
	}
	lower, upper, _ := bootstrapIndices(seed, len(values), resamples, indicesStatistic)
	return lower, upper
}

func bootstrapIndices(seed string, size, resamples int, statistic func([]int) float64) (float64, float64, bool) {
	if size == 0 || resamples < 1 {
		return 0, 0, false
	}
	digest := sha256.Sum256([]byte(seed))
	state := binary.BigEndian.Uint64(digest[:8])
	sample := make([]int, size)
	values := make([]float64, 0, resamples)
	for iteration := 0; iteration < resamples; iteration++ {
		for index := range sample {
			sample[index] = int(nextSplitMix64(&state) % uint64(size))
		}
		value := statistic(sample)
		if finite(value) {
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return 0, 0, false
	}
	sort.Float64s(values)
	return percentileSorted(values, .025), percentileSorted(values, .975), true
}

func nextSplitMix64(state *uint64) uint64 {
	*state += 0x9e3779b97f4a7c15
	value := *state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func identityIndices(size int) []int {
	indices := make([]int, size)
	for index := range indices {
		indices[index] = index
	}
	return indices
}

func percentile(values []float64, probability float64) float64 {
	copyOfValues := append([]float64(nil), values...)
	sort.Float64s(copyOfValues)
	return percentileSorted(copyOfValues, probability)
}

func percentileSorted(values []float64, probability float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	index := int(math.Ceil(probability*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func sum(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total
}

func percent(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return 100 * float64(numerator) / float64(denominator)
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
