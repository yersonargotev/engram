package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/recallstudy"
)

type recallStudyFlags struct {
	ContractPath, ContractHashPath               string
	CalibrationManifestPath, CalibrationHashPath string
	HeldOutManifestPath, HeldOutHashPath         string
	EnvironmentPath, ConsentPath, RowsPath       string
	CalibrationRowsPath, HeldOutRowsPath         string
	SourceRepo, CodexBinary, AuthFile            string
	OutputPath, PublicationOutputPath            string
	JSONMode                                     bool
}

type recallStudyDistributionFlags struct {
	ContractPath, ContractHashPath         string
	PublicationPath                        string
	DistributionPath, DistributionHashPath string
	SourceRepo                             string
	JSONMode                               bool
}

type recallStudyRequiredFlag struct {
	Name, Value string
}

type recallStudyInputs struct {
	study             *recallstudy.Study
	calibration       *recallstudy.Manifest
	heldOut           *recallstudy.Manifest
	verificationInput recallstudy.VerificationInput
	verification      recallstudy.VerificationReport
}

type recallStudyRunPlan struct {
	SchemaVersion         string                   `json:"schema_version"`
	StudyID               string                   `json:"study_id"`
	StudyVersion          string                   `json:"study_version"`
	ContractSHA256        string                   `json:"contract_sha256"`
	CohortID              string                   `json:"cohort_id"`
	HeldOutInputsAccessed bool                     `json:"held_out_inputs_accessed"`
	Runs                  []recallstudy.PlannedRun `json:"runs"`
}

func cmdRecallStudy() {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "recall-study requires verify, dry-run, plan-calibration, report, run-calibration, run-held-out, publish, or verify-distribution", nil)
		return
	}
	subcommand := strings.ToLower(strings.TrimSpace(os.Args[2]))
	switch subcommand {
	case "help", "--help", "-h":
		printRecallStudyUsage()
		return
	case "verify-distribution":
		cmdRecallStudyVerifyDistribution(os.Args[3:])
		return
	case "verify", "dry-run", "plan-calibration", "report", "run-calibration", "run-held-out", "publish":
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown recall-study command %q", os.Args[2]), nil)
		return
	}
	flags, ok := parseRecallStudyFlags(subcommand, os.Args[3:])
	if !ok {
		return
	}
	inputs, ok := loadRecallStudyInputs(flags)
	if !ok {
		return
	}
	switch subcommand {
	case "verify":
		writeRecallStudyResult(flags.JSONMode, inputs.verification)
	case "dry-run":
		calibration, err := inputs.study.Plan(inputs.calibration)
		if err != nil {
			failCLI(flags.JSONMode, "recall_study_plan_failed", err.Error(), nil)
			return
		}
		heldOut, err := inputs.study.Plan(inputs.heldOut)
		if err != nil {
			failCLI(flags.JSONMode, "recall_study_plan_failed", err.Error(), nil)
			return
		}
		writeRecallStudyResult(flags.JSONMode, map[string]any{
			"schema_version": "recall-study-dry-run-v1", "study_id": inputs.study.Contract.StudyID,
			"study_version": inputs.study.Contract.StudyVersion, "contract_sha256": inputs.study.Hash,
			"calibration_runs": len(calibration), "held_out_runs": len(heldOut), "planned_runs": len(calibration) + len(heldOut),
			"held_out_inputs_accessed": false,
		})
	case "plan-calibration":
		writeRecallStudyPlan(flags, inputs, inputs.calibration)
	case "report":
		rows, err := recallstudy.ReadRowSet(flags.RowsPath)
		if err != nil {
			failCLI(flags.JSONMode, "invalid_recall_study_rows", err.Error(), nil)
			return
		}
		if rows.CohortID != inputs.calibration.CohortID {
			failCLI(flags.JSONMode, "recall_study_report_failed", "recall-study report accepts disposable calibration rows only; held-out analysis belongs to issue #110", nil)
			return
		}
		report, err := inputs.study.Report(rows)
		if err != nil {
			failCLI(flags.JSONMode, "recall_study_report_failed", err.Error(), nil)
			return
		}
		if flags.OutputPath != "" {
			if err := recallstudy.WriteSharedJSON(flags.OutputPath, report); err != nil {
				failCLI(flags.JSONMode, "output_error", err.Error(), nil)
				return
			}
		}
		writeRecallStudyResult(flags.JSONMode, report)
	case "run-calibration":
		result, err := inputs.study.Execute(context.Background(), recallstudy.ExecutionRequest{
			Verification: inputs.verificationInput,
			Cohort:       inputs.calibration,
			OutputPath:   flags.OutputPath,
			Runtime: recallstudy.ExecutionRuntime{
				SourceRepo: flags.SourceRepo, CodexBinary: flags.CodexBinary, AuthFile: flags.AuthFile,
			},
		})
		if err != nil {
			failCLI(flags.JSONMode, "recall_study_execution_failed", err.Error(), nil)
			return
		}
		if result.Disposition != "" && flags.PublicationOutputPath != "" {
			publication, publishErr := inputs.study.PublishCalibrationStatus(result)
			if publishErr != nil {
				failCLI(flags.JSONMode, "recall_study_publication_failed", publishErr.Error(), nil)
				return
			}
			if writeErr := recallstudy.WriteSharedJSON(flags.PublicationOutputPath, publication); writeErr != nil {
				failCLI(flags.JSONMode, "output_error", writeErr.Error(), nil)
				return
			}
		}
		writeRecallStudyResult(flags.JSONMode, result)
	case "run-held-out":
		calibrationRows, err := recallstudy.ReadRowSet(flags.CalibrationRowsPath)
		if err != nil {
			failCLI(flags.JSONMode, "invalid_recall_study_rows", err.Error(), nil)
			return
		}
		result, err := inputs.study.Execute(context.Background(), recallstudy.ExecutionRequest{
			Verification:    inputs.verificationInput,
			Cohort:          inputs.heldOut,
			CalibrationRows: &calibrationRows,
			OutputPath:      flags.OutputPath,
			Runtime: recallstudy.ExecutionRuntime{
				SourceRepo: flags.SourceRepo, CodexBinary: flags.CodexBinary, AuthFile: flags.AuthFile,
			},
		})
		if err != nil {
			failCLI(flags.JSONMode, "recall_study_execution_failed", err.Error(), nil)
			return
		}
		writeRecallStudyResult(flags.JSONMode, result)
	case "publish":
		calibrationRows, err := recallstudy.ReadRowSet(flags.CalibrationRowsPath)
		if err != nil {
			failCLI(flags.JSONMode, "invalid_recall_study_rows", err.Error(), nil)
			return
		}
		heldOutRows, err := recallstudy.ReadRowSet(flags.HeldOutRowsPath)
		if err != nil {
			failCLI(flags.JSONMode, "invalid_recall_study_rows", err.Error(), nil)
			return
		}
		publication, err := inputs.study.Publish(calibrationRows, heldOutRows)
		if err != nil {
			failCLI(flags.JSONMode, "recall_study_publication_failed", err.Error(), nil)
			return
		}
		if err := recallstudy.WriteSharedJSON(flags.OutputPath, publication); err != nil {
			failCLI(flags.JSONMode, "output_error", err.Error(), nil)
			return
		}
		writeRecallStudyResult(flags.JSONMode, map[string]any{
			"schema_version":  publication.SchemaVersion,
			"scope":           publication.Scope,
			"disposition":     publication.Disposition,
			"evidence_gaps":   publication.EvidenceGaps,
			"rollout_enabled": false,
		})
	}
}

func cmdRecallStudyVerifyDistribution(args []string) {
	flags := recallStudyDistributionFlags{}
	set := flag.NewFlagSet("recall-study verify-distribution", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.StringVar(&flags.ContractPath, "contract", "", "frozen contract JSON")
	set.StringVar(&flags.ContractHashPath, "contract-hash", "", "frozen contract SHA-256 sidecar")
	set.StringVar(&flags.PublicationPath, "publication", "", "immutable study publication JSON")
	set.StringVar(&flags.DistributionPath, "distribution", "", "content-addressed distribution outcome JSON")
	set.StringVar(&flags.DistributionHashPath, "distribution-hash", "", "distribution outcome SHA-256 sidecar")
	set.StringVar(&flags.SourceRepo, "source-repo", "", "source repository containing the pinned artifacts")
	set.BoolVar(&flags.JSONMode, "json", false, "emit JSON")
	if !parseRecallStudyFlagSet(set, args, &flags.JSONMode) {
		return
	}
	required := []recallStudyRequiredFlag{
		{Name: "--contract", Value: flags.ContractPath}, {Name: "--contract-hash", Value: flags.ContractHashPath},
		{Name: "--publication", Value: flags.PublicationPath}, {Name: "--distribution", Value: flags.DistributionPath},
		{Name: "--distribution-hash", Value: flags.DistributionHashPath}, {Name: "--source-repo", Value: flags.SourceRepo},
	}
	if !requireRecallStudyFlags(flags.JSONMode, "verify-distribution", required) {
		return
	}

	study, err := recallstudy.Load(flags.ContractPath, flags.ContractHashPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_study_contract", err.Error(), nil)
		return
	}
	publication, err := recallstudy.LoadPublication(flags.PublicationPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_study_publication", err.Error(), nil)
		return
	}
	outcome, err := recallstudy.LoadDistributionOutcome(flags.DistributionPath, flags.DistributionHashPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_distribution", err.Error(), nil)
		return
	}
	verification, err := study.VerifyDistributionOutcome(publication, outcome, flags.SourceRepo)
	if err != nil {
		failCLI(flags.JSONMode, "recall_distribution_verification_failed", err.Error(), nil)
		return
	}
	writeRecallStudyResult(flags.JSONMode, verification)
}

func parseRecallStudyFlags(subcommand string, args []string) (recallStudyFlags, bool) {
	flags := recallStudyFlags{}
	set := flag.NewFlagSet("recall-study "+subcommand, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.StringVar(&flags.ContractPath, "contract", "", "frozen contract JSON")
	set.StringVar(&flags.ContractHashPath, "contract-hash", "", "contract SHA-256 sidecar")
	set.StringVar(&flags.CalibrationManifestPath, "calibration-manifest", "", "frozen calibration manifest")
	set.StringVar(&flags.CalibrationHashPath, "calibration-hash", "", "calibration manifest SHA-256 sidecar")
	set.StringVar(&flags.HeldOutManifestPath, "held-out-manifest", "", "frozen held-out manifest metadata")
	set.StringVar(&flags.HeldOutHashPath, "held-out-hash", "", "held-out manifest SHA-256 sidecar")
	set.StringVar(&flags.EnvironmentPath, "environment", "", "verified Compatibility tuple JSON")
	set.StringVar(&flags.ConsentPath, "consent", "", "explicit consent evidence JSON")
	set.BoolVar(&flags.JSONMode, "json", false, "emit JSON")
	if subcommand == "plan-calibration" || subcommand == "report" || subcommand == "run-calibration" || subcommand == "run-held-out" || subcommand == "publish" {
		set.StringVar(&flags.OutputPath, "output", "", "output JSON path")
	}
	if subcommand == "report" {
		set.StringVar(&flags.RowsPath, "rows", "", "private row-level results JSON")
	}
	if subcommand == "run-calibration" || subcommand == "run-held-out" {
		set.StringVar(&flags.SourceRepo, "source-repo", "", "source repository containing the frozen revision")
		set.StringVar(&flags.CodexBinary, "codex-binary", "", "frozen Codex executable")
		set.StringVar(&flags.AuthFile, "auth-file", "", "Codex authentication file")
	}
	if subcommand == "run-calibration" {
		set.StringVar(&flags.PublicationOutputPath, "publication-output", "", "aggregate-only output for an invalid calibration disposition")
	}
	if subcommand == "run-held-out" || subcommand == "publish" {
		set.StringVar(&flags.CalibrationRowsPath, "calibration-rows", "", "private calibration rows JSON")
	}
	if subcommand == "publish" {
		set.StringVar(&flags.HeldOutRowsPath, "held-out-rows", "", "private held-out rows JSON")
	}
	if !parseRecallStudyFlagSet(set, args, &flags.JSONMode) {
		return flags, false
	}
	required := []recallStudyRequiredFlag{
		{Name: "--contract", Value: flags.ContractPath}, {Name: "--contract-hash", Value: flags.ContractHashPath},
		{Name: "--calibration-manifest", Value: flags.CalibrationManifestPath}, {Name: "--calibration-hash", Value: flags.CalibrationHashPath},
		{Name: "--held-out-manifest", Value: flags.HeldOutManifestPath}, {Name: "--held-out-hash", Value: flags.HeldOutHashPath},
		{Name: "--environment", Value: flags.EnvironmentPath}, {Name: "--consent", Value: flags.ConsentPath},
	}
	if subcommand == "plan-calibration" {
		required = append(required, recallStudyRequiredFlag{Name: "--output", Value: flags.OutputPath})
	}
	if subcommand == "report" {
		required = append(required, recallStudyRequiredFlag{Name: "--rows", Value: flags.RowsPath})
	}
	if subcommand == "run-calibration" || subcommand == "run-held-out" {
		required = append(required,
			recallStudyRequiredFlag{Name: "--source-repo", Value: flags.SourceRepo},
			recallStudyRequiredFlag{Name: "--codex-binary", Value: flags.CodexBinary},
			recallStudyRequiredFlag{Name: "--auth-file", Value: flags.AuthFile},
			recallStudyRequiredFlag{Name: "--output", Value: flags.OutputPath},
		)
	}
	if subcommand == "run-held-out" || subcommand == "publish" {
		required = append(required, recallStudyRequiredFlag{Name: "--calibration-rows", Value: flags.CalibrationRowsPath})
	}
	if subcommand == "publish" {
		required = append(required,
			recallStudyRequiredFlag{Name: "--held-out-rows", Value: flags.HeldOutRowsPath},
			recallStudyRequiredFlag{Name: "--output", Value: flags.OutputPath},
		)
	}
	if !requireRecallStudyFlags(flags.JSONMode, subcommand, required) {
		return flags, false
	}
	return flags, true
}

func parseRecallStudyFlagSet(set *flag.FlagSet, args []string, jsonMode *bool) bool {
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printRecallStudyUsage()
			return false
		}
		code := "invalid_arguments"
		message := err.Error()
		if strings.Contains(message, "flag provided but not defined") {
			code = "unknown_flag"
			name := strings.TrimSpace(strings.TrimPrefix(message, "flag provided but not defined:"))
			message = "unknown recall-study flag -" + name
		}
		failCLI(*jsonMode || hasArg("--json"), code, message, nil)
		return false
	}
	if set.NArg() != 0 {
		failCLI(*jsonMode, "invalid_arguments", fmt.Sprintf("unexpected recall-study argument %q", set.Arg(0)), nil)
		return false
	}
	return true
}

func requireRecallStudyFlags(jsonMode bool, subcommand string, required []recallStudyRequiredFlag) bool {
	for _, requiredFlag := range required {
		if strings.TrimSpace(requiredFlag.Value) == "" {
			failCLI(jsonMode, "invalid_arguments", "recall-study "+subcommand+" requires "+requiredFlag.Name, nil)
			return false
		}
	}
	return true
}

func loadRecallStudyInputs(flags recallStudyFlags) (recallStudyInputs, bool) {
	study, err := recallstudy.Load(flags.ContractPath, flags.ContractHashPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_study_contract", err.Error(), nil)
		return recallStudyInputs{}, false
	}
	calibration, err := recallstudy.LoadManifest(flags.CalibrationManifestPath, flags.CalibrationHashPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_study_manifest", err.Error(), nil)
		return recallStudyInputs{}, false
	}
	heldOut, err := recallstudy.LoadManifest(flags.HeldOutManifestPath, flags.HeldOutHashPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_study_manifest", err.Error(), nil)
		return recallStudyInputs{}, false
	}
	compatibility, err := recallstudy.ReadCompatibilityEvidence(flags.EnvironmentPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_study_environment", err.Error(), nil)
		return recallStudyInputs{}, false
	}
	consent, err := recallstudy.ReadConsentEvidence(flags.ConsentPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_recall_study_consent", err.Error(), nil)
		return recallStudyInputs{}, false
	}
	verificationInput := recallstudy.VerificationInput{Calibration: &calibration.Manifest, HeldOut: &heldOut.Manifest, Compatibility: compatibility, Consent: consent}
	verification, err := study.Verify(verificationInput)
	if err != nil {
		failCLI(flags.JSONMode, "recall_study_verification_failed", err.Error(), nil)
		return recallStudyInputs{}, false
	}
	return recallStudyInputs{study: study, calibration: &calibration.Manifest, heldOut: &heldOut.Manifest, verificationInput: verificationInput, verification: verification}, true
}

func writeRecallStudyPlan(flags recallStudyFlags, inputs recallStudyInputs, manifest *recallstudy.Manifest) {
	plan, err := inputs.study.Plan(manifest)
	if err != nil {
		failCLI(flags.JSONMode, "recall_study_plan_failed", err.Error(), nil)
		return
	}
	result := recallStudyRunPlan{
		SchemaVersion: "recall-study-run-plan-v1", StudyID: inputs.study.Contract.StudyID, StudyVersion: inputs.study.Contract.StudyVersion,
		ContractSHA256: inputs.study.Hash, CohortID: manifest.CohortID, HeldOutInputsAccessed: false, Runs: plan,
	}
	if err := recallstudy.WritePrivateJSON(flags.OutputPath, result); err != nil {
		failCLI(flags.JSONMode, "output_error", err.Error(), nil)
		return
	}
	writeRecallStudyResult(flags.JSONMode, map[string]any{
		"schema_version": result.SchemaVersion, "study_id": result.StudyID, "study_version": result.StudyVersion,
		"contract_sha256": result.ContractSHA256, "cohort_id": result.CohortID, "planned_runs": len(result.Runs),
		"held_out_inputs_accessed": false,
	})
}

func writeRecallStudyResult(jsonMode bool, value any) {
	if jsonMode {
		if err := writeCLIJSON(value); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		failCLI(false, "output_error", err.Error(), nil)
		return
	}
	fmt.Println(string(encoded))
}

func printRecallStudyUsage() {
	fmt.Println(`usage: engram recall-study verify|dry-run|plan-calibration|report|run-calibration|run-held-out|publish|verify-distribution [options]

Execution, planning, report, and publication commands require the frozen
contract, both manifest metadata files and their SHA-256 sidecars, a verified
Compatibility tuple, and explicit consent evidence. verify-distribution uses
only the public contract, publication, applied outcome, and pinned source tree.

  verify         Validate frozen metadata without opening held-out inputs
  dry-run        Return aggregate plan counts without opening held-out inputs
  plan-calibration  Write a private calibration run plan with --output FILE
  report            Derive aggregate evidence from calibration --rows
  run-calibration   Execute or resume the frozen private calibration cohort
  run-held-out      Execute or resume held-out after successful --calibration-rows
  publish           Write one aggregate-only disposition from both private row sets
  verify-distribution  Verify the frozen applied disposition and pinned source artifacts without mutation`)
}
