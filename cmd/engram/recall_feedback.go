package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func cmdRecallFeedback(cfg store.Config) {
	args := os.Args[2:]
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printRecallFeedbackUsage()
		return
	}
	if strings.ToLower(strings.TrimSpace(args[0])) != "report" {
		failCLI(hasArg("--json"), "invalid_arguments", "recall-feedback action must be report", nil)
		return
	}
	jsonMode := false
	for _, arg := range args[1:] {
		if arg == "--json" {
			jsonMode = true
			continue
		}
		failCLI(jsonMode || hasArg("--json"), "invalid_arguments", "recall-feedback report accepts only --json", nil)
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	report, err := memoryops.New(s).RecallFeedbackReport()
	if err != nil {
		failCLI(jsonMode, "recall_feedback_report_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(report)
		return
	}
	printRecallFeedbackReport(report)
}

func printRecallFeedbackReport(report *memoryops.RecallFeedbackReport) {
	fmt.Printf("Recall feedback report (%s)\n", report.SchemaVersion)
	fmt.Printf("Exposed results: %d; empty runs: %d; label coverage: %d/%d (unknown %d, %.1f%%)\n",
		report.ExposedResults, report.EmptyRuns, report.LabelCoverage.Numerator,
		report.LabelCoverage.Denominator, report.LabelCoverage.Unknown, 100*report.LabelCoverage.Rate)
	for _, source := range report.Sources {
		fmt.Printf("%s: utility %.1f%% (%d/%d, unknown %d); noise %.1f%%; harm %.1f%%; duplicate %.1f%%; false-empty %.1f%% (%d/%d, unknown %d); time-to-useful p50/p95 %d/%d ms (%d/%d, unknown %d)\n",
			source.Source,
			100*source.Utility.Rate, source.Utility.Numerator, source.Utility.Denominator, source.Utility.Unknown,
			100*source.Noise.Rate, 100*source.Harm.Rate, 100*source.Duplicate.Rate,
			100*source.FalseEmpty.Rate, source.FalseEmpty.Numerator, source.FalseEmpty.Denominator, source.FalseEmpty.Unknown,
			source.TimeToUseful.P50Milliseconds, source.TimeToUseful.P95Milliseconds,
			source.TimeToUseful.Samples, source.TimeToUseful.Denominator, source.TimeToUseful.Unknown)
	}
	for _, operation := range report.Operations {
		fmt.Printf("%s: %d events; latency p50/p95 %d/%d ms (%d samples, %d unknown); volume %d UTF-8 bytes (%d samples, %d unknown); exposed %d\n",
			operation.Operation, operation.Events,
			operation.P50LatencyMilliseconds, operation.P95LatencyMilliseconds,
			operation.LatencySamples, operation.UnknownLatency,
			operation.TotalUTF8Bytes, operation.VolumeSamples, operation.UnknownVolume,
			operation.TotalExposedResults)
	}
}

func printRecallFeedbackUsage() {
	fmt.Println("Usage: engram recall-feedback report [--json]")
}
