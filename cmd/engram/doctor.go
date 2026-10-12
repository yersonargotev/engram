package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/diagnostic"
	"github.com/yersonargotev/engram/internal/store"
)

func cmdDoctor(cfg store.Config) {
	if len(os.Args) > 2 && os.Args[2] == "repair" {
		cmdDoctorRepair(cfg)
		return
	}
	jsonOut := hasArg("--json")
	project := ""
	check := ""
	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--json":
			jsonOut = true
		case "--project":
			if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
				failCLI(jsonOut, "missing_flag_value", "--project requires a value", nil)
				return
			}
			project = os.Args[i+1]
			i++
		case "--check":
			if i+1 >= len(os.Args) || strings.HasPrefix(os.Args[i+1], "--") {
				failCLI(jsonOut, "missing_flag_value", "--check requires a value", nil)
				return
			}
			check = os.Args[i+1]
			i++
		case "--help", "-h", "help":
			printDoctorUsage()
			return
		default:
			failCLI(jsonOut, "unknown_flag", fmt.Sprintf("unknown doctor argument %q", os.Args[i]), nil)
			return
		}
	}

	project, _ = store.NormalizeProject(project)
	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonOut, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()

	report, err := runDiagnostics(context.Background(), s, strings.TrimSpace(project), strings.TrimSpace(check))
	if err != nil {
		report = diagnostic.ErrorReport(project, err)
		if jsonOut {
			failCLI(true, "diagnostic_failed", err.Error(), map[string]any{"report": report})
			return
		} else {
			fmt.Fprintf(os.Stderr, "engram doctor failed: %s\n", err)
		}
		if errors.Is(err, diagnostic.ErrInvalidCheck) {
			exitFunc(1)
		}
		return
	}

	if jsonOut {
		writeDoctorJSON(report)
		return
	}
	renderDoctorText(report)
}

func printDoctorUsage() {
	fmt.Fprintln(os.Stdout, "usage: engram doctor [--json] [--project PROJECT] [--check CODE]")
	fmt.Fprintln(os.Stdout, "       engram doctor repair --project PROJECT --check CODE (--plan|--dry-run|--apply)")
	fmt.Fprintln(os.Stdout, "       engram doctor repair [--project PROJECT] --check "+diagnostic.CheckSyncMutationRequiredFields+" (--plan|--dry-run|--apply)")
	fmt.Fprintln(os.Stdout, "note: --project is required for every repair check except "+diagnostic.CheckSyncMutationRequiredFields+", where it optionally scopes the quarantine to one project.")
	fmt.Fprintln(os.Stdout, "checks: "+strings.Join(diagnostic.RegisteredCodes(), ", "))
	fmt.Fprintln(os.Stdout, "identity recovery: --check invalid_session_identity --source-id ID --canonical-id ID (--plan|--dry-run|--apply) [--preview-token TOKEN] [--yes] [--json]")
}

func cmdDoctorRepair(cfg store.Config) {
	project := ""
	check := ""
	mode := diagnostic.RepairMode("")
	modeCount := 0
	sourceID, canonicalID, previewToken := "", "", ""
	sourceProvided, canonicalProvided, yes := false, false, false
	jsonMode := hasArg("--json")
	explicitRecovery := isSessionIdentityRecoveryCommand(os.Args[1:])
	failRepair := func(code, message string) {
		if explicitRecovery {
			failCLI(jsonMode, code, message, nil)
		} else {
			failDoctorRepair(message)
		}
	}
	for i := 3; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--source-id", "--canonical-id", "--preview-token":
			flag := os.Args[i]
			if i+1 >= len(os.Args) {
				failCLI(jsonMode, "missing_flag_value", flag+" requires a value", nil)
				return
			}
			value := os.Args[i+1]
			switch flag {
			case "--source-id":
				if sourceProvided {
					failRepair("invalid_recovery_arguments", "--source-id must be supplied exactly once")
					return
				}
				sourceID, sourceProvided = value, true
			case "--canonical-id":
				if canonicalProvided {
					failRepair("invalid_recovery_arguments", "--canonical-id must be supplied exactly once")
					return
				}
				canonicalID, canonicalProvided = value, true
			case "--preview-token":
				previewToken = value
			}
			i++
		case "--yes":
			yes = true
		case "--json":
		case "--project":
			if i+1 >= len(os.Args) {
				failRepair("missing_flag_value", "--project requires a value")
				return
			}
			project = os.Args[i+1]
			i++
		case "--check":
			if i+1 >= len(os.Args) {
				failRepair("missing_flag_value", "--check requires a value")
				return
			}
			check = os.Args[i+1]
			i++
		case "--plan":
			mode = diagnostic.RepairModePlan
			modeCount++
		case "--dry-run":
			mode = diagnostic.RepairModeDryRun
			modeCount++
		case "--apply":
			mode = diagnostic.RepairModeApply
			modeCount++
		case "--help", "-h", "help":
			printDoctorUsage()
			return
		default:
			failRepair("unknown_flag", fmt.Sprintf("unknown doctor repair argument %q", os.Args[i]))
			return
		}
	}

	project, _ = store.NormalizeProject(project)
	project = strings.TrimSpace(project)
	check = strings.TrimSpace(check)
	if project == "" && check != diagnostic.CheckSyncMutationRequiredFields {
		failRepair("project_required", "--project is required")
		return
	}
	if check == "" {
		failRepair("missing_flag_value", "--check is required")
		return
	}
	if modeCount != 1 {
		failRepair("invalid_recovery_arguments", "exactly one of --plan, --dry-run, or --apply is required")
		return
	}
	if !isSupportedDoctorRepairCheck(check) {
		failRepair("invalid_recovery_arguments", "unsupported repair check "+check)
		return
	}
	if sourceProvided || canonicalProvided || previewToken != "" || yes {
		if check != diagnostic.CheckInvalidSessionIdentity || !sourceProvided || !canonicalProvided {
			failCLI(jsonMode, "invalid_recovery_arguments", "identity recovery requires --check invalid_session_identity, explicit --source-id, and --canonical-id", nil)
			return
		}
		cmdSessionIdentityRecovery(cfg, store.SessionIdentityRecoveryRequest{Project: project, SourceID: sourceID, CanonicalID: canonicalID}, mode, previewToken, yes, jsonMode)
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()
	if check == diagnostic.CheckSyncMutationRequiredFields {
		report, err := s.QuarantineIrreparableSyncMutations(project, mode == diagnostic.RepairModeApply)
		if err != nil {
			failDoctorRepair(err.Error())
			return
		}
		writeDoctorRepairJSON(report)
		return
	}

	ctx := context.Background()
	report, err := runDiagnostics(ctx, s, project, check)
	if err != nil {
		failDoctorRepair(err.Error())
		return
	}
	plan, err := diagnostic.BuildRepairPlan(ctx, diagnostic.Scope{Store: s, Project: project}, report, check, mode)
	if err != nil {
		failDoctorRepair(err.Error())
		return
	}
	actions := make([]store.SessionProjectReclassification, 0, len(plan.Actions))
	for _, action := range plan.Actions {
		actions = append(actions, store.SessionProjectReclassification{SessionID: action.SessionID, FromProject: action.FromProject, ToProject: action.ToProject})
	}
	if mode == diagnostic.RepairModeApply && len(actions) > 0 {
		counts, err := s.EstimateSessionProjectReclassification(actions)
		if err != nil {
			failDoctorRepair(err.Error())
			return
		}
		plan.Counts.SessionsPlanned = counts.Sessions
		plan.Counts.ObservationsPlanned = counts.Observations
		plan.Counts.PromptsPlanned = counts.Prompts
		result, err := s.ApplySessionProjectReclassification(actions)
		if err != nil {
			failDoctorRepair(err.Error())
			return
		}
		plan.Status = "applied"
		plan.BackupPath = result.BackupPath
		plan.Counts.SessionsApplied = result.Counts.Sessions
		plan.Counts.ObservationsApplied = result.Counts.Observations
		plan.Counts.PromptsApplied = result.Counts.Prompts
	} else {
		counts, err := s.EstimateSessionProjectReclassification(actions)
		if err != nil {
			failDoctorRepair(err.Error())
			return
		}
		plan.Counts.SessionsPlanned = counts.Sessions
		plan.Counts.ObservationsPlanned = counts.Observations
		plan.Counts.PromptsPlanned = counts.Prompts
	}
	writeDoctorRepairJSON(plan)
}

func isSupportedDoctorRepairCheck(check string) bool {
	switch check {
	case diagnostic.CheckSessionProjectDirectoryMismatch,
		diagnostic.CheckManualSessionNameProjectMismatch,
		diagnostic.CheckInvalidSessionIdentity,
		diagnostic.CheckSyncMutationRequiredFields:
		return true
	default:
		return false
	}
}

func failDoctorRepair(message string) {
	fmt.Fprintln(os.Stderr, "engram doctor repair failed: "+message)
	printDoctorUsage()
	exitFunc(1)
}

func writeDoctorRepairJSON(value any) {
	out, err := jsonMarshalIndent(value, "", "  ")
	if err != nil {
		fatal(err)
		return
	}
	fmt.Println(string(out))
}

func writeDoctorJSON(report diagnostic.Report) {
	out, err := jsonMarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err)
		return
	}
	fmt.Println(string(out))
}

func renderDoctorText(report diagnostic.Report) {
	fmt.Printf("Engram Doctor: %s\n", report.Status)
	if report.Project != "" {
		fmt.Printf("Project: %s\n", report.Project)
	}
	fmt.Printf("Checks: %d ok=%d warnings=%d blocked=%d errors=%d\n\n", report.Summary.Total, report.Summary.OK, report.Summary.Warnings, report.Summary.Blocked, report.Summary.Errors)
	for _, check := range report.Checks {
		fmt.Printf("[%s] %s — %s\n", check.Result, check.CheckID, check.Message)
		if check.Why != "" {
			fmt.Printf("  why: %s\n", check.Why)
		}
		if check.SafeNextStep != "" {
			fmt.Printf("  next: %s\n", check.SafeNextStep)
		}
		for _, finding := range check.Findings {
			fmt.Printf("  - %s: %s\n", finding.ReasonCode, finding.Message)
			if len(finding.Evidence) > 0 {
				fmt.Printf("    evidence: %s\n", string(finding.Evidence))
			}
		}
	}
}
