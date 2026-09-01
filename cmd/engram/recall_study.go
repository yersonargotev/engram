package main

import (
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
	OutputPath                                   string
	JSONMode                                     bool
}

type recallStudyInputs struct {
	study        *recallstudy.Study
	calibration  *recallstudy.Manifest
	heldOut      *recallstudy.Manifest
	verification recallstudy.VerificationReport
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
		failCLI(jsonMode, "invalid_arguments", "recall-study requires verify, dry-run, plan-calibration, or report", nil)
		return
	}
	subcommand := strings.ToLower(strings.TrimSpace(os.Args[2]))
	switch subcommand {
	case "help", "--help", "-h":
		printRecallStudyUsage()
		return
	case "verify", "dry-run", "plan-calibration", "report":
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
	}
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
	if subcommand == "plan-calibration" || subcommand == "report" {
		set.StringVar(&flags.OutputPath, "output", "", "output JSON path")
	}
	if subcommand == "report" {
		set.StringVar(&flags.RowsPath, "rows", "", "private row-level results JSON")
	}
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printRecallStudyUsage()
			return flags, false
		}
		code := "invalid_arguments"
		message := err.Error()
		if strings.Contains(message, "flag provided but not defined") {
			code = "unknown_flag"
			name := strings.TrimSpace(strings.TrimPrefix(message, "flag provided but not defined:"))
			message = "unknown recall-study flag -" + name
		}
		failCLI(hasArg("--json"), code, message, nil)
		return flags, false
	}
	if set.NArg() != 0 {
		failCLI(flags.JSONMode, "invalid_arguments", fmt.Sprintf("unexpected recall-study argument %q", set.Arg(0)), nil)
		return flags, false
	}
	required := map[string]string{
		"--contract": flags.ContractPath, "--contract-hash": flags.ContractHashPath,
		"--calibration-manifest": flags.CalibrationManifestPath, "--calibration-hash": flags.CalibrationHashPath,
		"--held-out-manifest": flags.HeldOutManifestPath, "--held-out-hash": flags.HeldOutHashPath,
		"--environment": flags.EnvironmentPath, "--consent": flags.ConsentPath,
	}
	if subcommand == "plan-calibration" {
		required["--output"] = flags.OutputPath
	}
	if subcommand == "report" {
		required["--rows"] = flags.RowsPath
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			failCLI(flags.JSONMode, "invalid_arguments", "recall-study "+subcommand+" requires "+name, nil)
			return flags, false
		}
	}
	return flags, true
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
	verification, err := study.Verify(recallstudy.VerificationInput{Calibration: &calibration.Manifest, HeldOut: &heldOut.Manifest, Compatibility: compatibility, Consent: consent})
	if err != nil {
		failCLI(flags.JSONMode, "recall_study_verification_failed", err.Error(), nil)
		return recallStudyInputs{}, false
	}
	return recallStudyInputs{study: study, calibration: &calibration.Manifest, heldOut: &heldOut.Manifest, verification: verification}, true
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
	fmt.Println(`usage: engram recall-study verify|dry-run|plan-calibration|report [options]

All commands require the frozen contract, both manifest metadata files and their
SHA-256 sidecars, a verified Compatibility tuple, and explicit consent evidence.

  verify         Validate frozen metadata without opening held-out inputs
  dry-run        Return aggregate plan counts without opening held-out inputs
  plan-calibration  Write a private calibration run plan with --output FILE
  report            Derive aggregate evidence from --rows; optionally write --output FILE`)
}
