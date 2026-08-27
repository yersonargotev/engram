package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

const maxAdmissionInputBytes = 1 << 20

type admissionFlagOptions struct {
	Project       bool
	Input         bool
	Session       bool
	StudySelector bool
	StudyRun      bool
	Reviewer      bool
	Yes           bool
}

type admissionFlags struct {
	Project                   string
	InputPath                 string
	SessionID                 string
	StudyID                   string
	StudyVersion              string
	Cohort                    string
	Adapter                   string
	ProjectType               string
	SessionShape              string
	ConsentAttestation        string
	ReviewerID                string
	IndependentReviewRequired bool
	Yes                       bool
	JSONMode                  bool
}

func cmdAdmission(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "admission requires preview, shadow, review, or metrics", nil)
		return
	}
	switch os.Args[2] {
	case "preview":
		cmdAdmissionPreview(cfg, jsonMode)
	case "shadow":
		cmdAdmissionShadow(cfg, jsonMode)
	case "review":
		cmdAdmissionReview(cfg, jsonMode)
	case "metrics":
		cmdAdmissionMetrics(cfg, jsonMode)
	case "study":
		cmdAdmissionStudy(cfg, jsonMode)
	case "omission":
		cmdAdmissionOmission(cfg, jsonMode)
	case "help", "--help", "-h":
		printAdmissionUsage()
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown admission subcommand %q", os.Args[2]), nil)
	}
}

func cmdAdmissionPreview(cfg store.Config, jsonMode bool) {
	flags, proceed := parseAdmissionFlags(3, jsonMode, admissionFlagOptions{Project: true, Input: true, Session: true})
	if !proceed {
		return
	}
	project := flags.Project
	inputPath := flags.InputPath
	sessionID := flags.SessionID
	jsonMode = flags.JSONMode
	if project == "" {
		failCLI(jsonMode, "invalid_arguments", "admission preview requires --project", nil)
		return
	}
	if (inputPath == "") == (sessionID == "") {
		failCLI(jsonMode, "invalid_arguments", "admission preview requires exactly one of --input or --session", nil)
		return
	}
	project, _ = store.NormalizeProject(project)

	var bundle memoryops.EvidenceBundle
	if inputPath != "" {
		var err error
		bundle, err = readAdmissionEvidenceBundle(inputPath)
		if err != nil {
			failCLI(jsonMode, "invalid_evidence_bundle", err.Error(), nil)
			return
		}
	}
	memoryStore, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer memoryStore.Close()

	exists, err := memoryStore.ProjectExists(project)
	if err != nil {
		failCLI(jsonMode, "project_resolution_failed", err.Error(), nil)
		return
	}
	if !exists {
		availableProjects, listErr := memoryStore.ListProjectNames()
		if listErr != nil {
			failCLI(jsonMode, "project_resolution_failed", listErr.Error(), nil)
			return
		}
		failCLI(jsonMode, "unknown_project", fmt.Sprintf("unknown project: %s", project), map[string]any{
			"available_projects": availableProjects,
		})
		return
	}

	result, err := memoryops.New(memoryStore).PreviewAdmission(memoryops.AdmissionPreviewInput{
		Project:   project,
		Evidence:  bundle,
		SessionID: sessionID,
	})
	if err != nil {
		if errors.Is(err, memoryops.ErrAdmissionSessionNotFound) {
			failCLI(jsonMode, "unknown_session", err.Error(), map[string]any{"session_id": sessionID})
			return
		}
		if errors.Is(err, memoryops.ErrAdmissionSessionProjectMismatch) {
			details := map[string]any{"session_id": sessionID, "requested_project": project}
			var mismatch *memoryops.AdmissionSessionProjectMismatchError
			if errors.As(err, &mismatch) {
				details["session_project"] = mismatch.SessionProject
			}
			failCLI(jsonMode, "session_project_mismatch", err.Error(), details)
			return
		}
		if sessionID != "" {
			failCLI(jsonMode, "session_evidence_failed", err.Error(), map[string]any{"session_id": sessionID})
			return
		}
		failCLI(jsonMode, "admission_preview_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	renderAdmissionPreview(result)
}

func parseAdmissionFlags(start int, jsonMode bool, options admissionFlagOptions) (admissionFlags, bool) {
	flags := admissionFlags{JSONMode: jsonMode}
	seen := map[string]bool{}
	for index := start; index < len(os.Args); index++ {
		argument := os.Args[index]
		switch argument {
		case "--json":
			flags.JSONMode = true
		case "--independent-review-required":
			if !options.StudyRun || seen[argument] {
				if seen[argument] {
					failCLI(flags.JSONMode, "invalid_arguments", argument+" may only be provided once", nil)
				} else {
					admissionUnknownArgument(flags.JSONMode, argument)
				}
				return admissionFlags{}, false
			}
			seen[argument] = true
			flags.IndependentReviewRequired = true
		case "--yes":
			if !options.Yes || seen[argument] {
				if seen[argument] {
					failCLI(flags.JSONMode, "invalid_arguments", argument+" may only be provided once", nil)
				} else {
					admissionUnknownArgument(flags.JSONMode, argument)
				}
				return admissionFlags{}, false
			}
			seen[argument] = true
			flags.Yes = true
		case "--project", "--input", "--session", "--study", "--study-version", "--cohort", "--adapter", "--project-type", "--session-shape", "--consent-attestation", "--reviewer":
			allowed := (argument == "--project" && options.Project) ||
				(argument == "--input" && options.Input) ||
				(argument == "--session" && options.Session) ||
				((argument == "--study" || argument == "--study-version") && (options.StudySelector || options.StudyRun)) ||
				((argument == "--cohort" || argument == "--adapter" || argument == "--project-type" || argument == "--session-shape" || argument == "--consent-attestation") && options.StudyRun) ||
				(argument == "--reviewer" && options.Reviewer)
			if !allowed {
				admissionUnknownArgument(flags.JSONMode, argument)
				return admissionFlags{}, false
			}
			if seen[argument] {
				failCLI(flags.JSONMode, "invalid_arguments", argument+" may only be provided once", nil)
				return admissionFlags{}, false
			}
			seen[argument] = true
			if index+1 >= len(os.Args) || strings.HasPrefix(os.Args[index+1], "--") || strings.TrimSpace(os.Args[index+1]) == "" {
				message := argument + " requires a non-empty value"
				if argument == "--input" {
					message = "--input requires a file path or - for stdin"
				}
				failCLI(flags.JSONMode, "invalid_arguments", message, nil)
				return admissionFlags{}, false
			}
			value := strings.TrimSpace(os.Args[index+1])
			index++
			switch argument {
			case "--project":
				flags.Project = value
			case "--input":
				flags.InputPath = value
			case "--session":
				flags.SessionID = value
			case "--study":
				flags.StudyID = value
			case "--study-version":
				flags.StudyVersion = value
			case "--cohort":
				flags.Cohort = value
			case "--adapter":
				flags.Adapter = value
			case "--project-type":
				flags.ProjectType = value
			case "--session-shape":
				flags.SessionShape = value
			case "--consent-attestation":
				flags.ConsentAttestation = value
			case "--reviewer":
				flags.ReviewerID = value
			}
		case "help", "--help", "-h":
			printAdmissionUsage()
			return admissionFlags{}, false
		default:
			admissionUnknownArgument(flags.JSONMode, argument)
			return admissionFlags{}, false
		}
	}
	return flags, true
}

func readAdmissionEvidenceBundle(path string) (memoryops.EvidenceBundle, error) {
	var reader io.Reader
	var file *os.File
	if path == "-" {
		reader = os.Stdin
	} else {
		opened, err := os.Open(path)
		if err != nil {
			return memoryops.EvidenceBundle{}, fmt.Errorf("open evidence bundle: %w", err)
		}
		file = opened
		defer file.Close()
		reader = file
	}
	encoded, err := io.ReadAll(io.LimitReader(reader, maxAdmissionInputBytes+1))
	if err != nil {
		return memoryops.EvidenceBundle{}, fmt.Errorf("read evidence bundle: %w", err)
	}
	if len(encoded) > maxAdmissionInputBytes {
		return memoryops.EvidenceBundle{}, fmt.Errorf("evidence bundle JSON exceeds %d bytes", maxAdmissionInputBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var bundle memoryops.EvidenceBundle
	if err := decoder.Decode(&bundle); err != nil {
		return memoryops.EvidenceBundle{}, fmt.Errorf("decode evidence bundle: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return memoryops.EvidenceBundle{}, fmt.Errorf("decode evidence bundle: multiple JSON values are not allowed")
		}
		return memoryops.EvidenceBundle{}, fmt.Errorf("decode evidence bundle trailing data: %w", err)
	}
	return bundle, nil
}

func renderAdmissionPreview(result *memoryops.AdmissionPreviewResult) {
	fmt.Println("Admission preview (shadow mode; no memories were written)")
	fmt.Printf("Project: %s\n", result.Project)
	if acquisition := result.Acquisition; acquisition != nil {
		fmt.Printf("Evidence: session %s (%s)\n", acquisition.SessionID, acquisition.EvidenceVersion)
		fmt.Println("Coverage:")
		for _, coverage := range acquisition.Sources {
			fmt.Printf("  %s: %d/%d included", coverage.Source, coverage.IncludedItems, coverage.AvailableItems)
			if coverage.OmittedItems > 0 {
				fmt.Printf("; %d omitted", coverage.OmittedItems)
			}
			if coverage.TruncatedItems > 0 {
				fmt.Printf("; %d truncated", coverage.TruncatedItems)
			}
			fmt.Println()
		}
	}
	if len(result.Proposals) == 0 {
		fmt.Println("No Memory proposals were generated.")
	} else {
		for index, assessed := range result.Proposals {
			fmt.Printf("\n%d. [%s] %s\n", index+1, assessed.Assessment.Recommendation, assessed.Proposal.Title)
			fmt.Printf("   Category: %s; protected: %t\n", assessed.Proposal.Category, assessed.Proposal.Protected)
			fmt.Printf("   Reasons: %s\n", strings.Join(assessed.Assessment.ReasonCodes, ", "))
			fmt.Printf("   Evidence: %s\n", strings.Join(assessed.Assessment.EvidenceRefs, ", "))
		}
	}
	if len(result.Diagnostics) > 0 {
		fmt.Println("Diagnostics:")
		for _, diagnostic := range result.Diagnostics {
			fmt.Printf("  %s: %s\n", diagnostic.Code, diagnostic.Message)
		}
	}
}

func printAdmissionUsage() {
	fmt.Fprintln(os.Stdout, "usage: engram admission preview --project PROJECT (--input FILE|- | --session SESSION_ID) [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission shadow --project PROJECT --session SESSION_ID [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission shadow --project PROJECT --session SESSION_ID --study ID --study-version VERSION --cohort COHORT --adapter ADAPTER --project-type TYPE --session-shape SHAPE --consent-attestation ID [--independent-review-required] [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission study freeze --input FILE|- [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission study cleanup --study ID --study-version VERSION --yes [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission review list (--project PROJECT | --study ID --study-version VERSION --reviewer REVIEWER) [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission review mark PROPOSAL_ID [--reviewer REVIEWER] --verdict admit|review|reject [--note TEXT] [--unsupported|--clear-unsupported] [--privacy-leak|--clear-privacy-leak] [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission omission record RUN_ID --reviewer REVIEWER --category CATEGORY --reason-code CODE --annotation TEXT [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission metrics (--project PROJECT | --study ID --study-version VERSION) [--json]")
	fmt.Fprintln(os.Stdout, "Preview never persists; shadow retains derived local snapshots for review and metrics.")
}
