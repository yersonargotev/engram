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
	case "help", "--help", "-h":
		printAdmissionUsage()
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown admission subcommand %q", os.Args[2]), nil)
	}
}

func cmdAdmissionPreview(cfg store.Config, jsonMode bool) {
	project := ""
	inputPath := ""
	sessionID := ""
	seenProject := false
	seenInput := false
	seenSession := false
	for index := 3; index < len(os.Args); index++ {
		switch os.Args[index] {
		case "--json":
			jsonMode = true
		case "--project":
			if seenProject {
				failCLI(jsonMode, "invalid_arguments", "--project may only be provided once", nil)
				return
			}
			seenProject = true
			if index+1 >= len(os.Args) || strings.HasPrefix(os.Args[index+1], "--") {
				failCLI(jsonMode, "invalid_arguments", "--project requires a non-empty value", nil)
				return
			}
			project = strings.TrimSpace(os.Args[index+1])
			index++
			if project == "" {
				failCLI(jsonMode, "invalid_arguments", "--project requires a non-empty value", nil)
				return
			}
		case "--input":
			if seenInput {
				failCLI(jsonMode, "invalid_arguments", "--input may only be provided once", nil)
				return
			}
			seenInput = true
			if index+1 >= len(os.Args) || strings.HasPrefix(os.Args[index+1], "--") {
				failCLI(jsonMode, "invalid_arguments", "--input requires a file path or - for stdin", nil)
				return
			}
			inputPath = strings.TrimSpace(os.Args[index+1])
			index++
			if inputPath == "" {
				failCLI(jsonMode, "invalid_arguments", "--input requires a file path or - for stdin", nil)
				return
			}
		case "--session":
			if seenSession {
				failCLI(jsonMode, "invalid_arguments", "--session may only be provided once", nil)
				return
			}
			seenSession = true
			if index+1 >= len(os.Args) || strings.HasPrefix(os.Args[index+1], "--") {
				failCLI(jsonMode, "invalid_arguments", "--session requires a non-empty session ID", nil)
				return
			}
			sessionID = strings.TrimSpace(os.Args[index+1])
			index++
			if sessionID == "" {
				failCLI(jsonMode, "invalid_arguments", "--session requires a non-empty session ID", nil)
				return
			}
		case "help", "--help", "-h":
			printAdmissionUsage()
			return
		default:
			if strings.HasPrefix(os.Args[index], "--") {
				failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", os.Args[index]), nil)
			} else {
				failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unexpected admission preview argument %q", os.Args[index]), nil)
			}
			return
		}
	}
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
	fmt.Fprintln(os.Stdout, "       engram admission review list --project PROJECT [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission review mark PROPOSAL_ID --verdict admit|review|reject [--note TEXT] [--unsupported] [--privacy-leak] [--json]")
	fmt.Fprintln(os.Stdout, "       engram admission metrics --project PROJECT [--json]")
	fmt.Fprintln(os.Stdout, "Preview never persists; shadow retains derived local snapshots for review and metrics.")
}
