package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

var captureInputInteractive = func() bool {
	info, err := os.Stdin.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func cmdCapture(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "capture requires status, enable, disable, or purge", nil)
		return
	}

	switch strings.ToLower(strings.TrimSpace(os.Args[2])) {
	case "status":
		cmdCaptureStatus(cfg, os.Args[3:])
	case "enable":
		cmdCaptureEnable(cfg, os.Args[3:])
	case "disable":
		cmdCaptureDisable(cfg, os.Args[3:])
	case "purge":
		cmdCapturePurge(cfg, os.Args[3:])
	case "help", "--help", "-h":
		printCaptureUsage()
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown capture command %q", os.Args[2]), nil)
	}
}

func cmdCapturePurge(cfg store.Config, args []string) {
	set := newCaptureFlagSet("purge")
	project, contentType := "", ""
	yes, jsonMode := false, false
	set.StringVar(&project, "project", "", "project name")
	set.StringVar(&contentType, "type", "", "capture content type")
	set.BoolVar(&yes, "yes", false, "confirm permanent purge")
	set.BoolVar(&jsonMode, "json", false, "emit JSON")
	if !parseCaptureFlags(set, args, hasArg("--json")) {
		return
	}
	if !validateExplicitCaptureScope(project, contentType, jsonMode) {
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	service := memoryops.New(s)
	status, err := service.CaptureStatus(memoryops.CaptureStatusInput{Project: project, ContentType: contentType})
	if err != nil {
		failCaptureOperation(jsonMode, "capture_status_failed", err)
		return
	}
	if !yes {
		preview := map[string]any{
			"project": status.Project, "content_type": status.ContentType, "stored_count": status.StoredCount,
		}
		if jsonMode || !captureInputInteractive() {
			message := "capture purge requires --yes in non-interactive mode"
			if jsonMode {
				message = "capture purge requires --yes in JSON mode"
			}
			failCLI(jsonMode, "confirmation_required", message, map[string]any{"preview": preview})
			return
		}
		fmt.Printf("Permanently purge %d %s Diagnostic captures for %q? [y/N] ", status.StoredCount, status.ContentType, status.Project)
		var answer string
		_, _ = scanInputLine(&answer)
		if normalized := strings.ToLower(strings.TrimSpace(answer)); normalized != "y" && normalized != "yes" {
			fmt.Println("Aborted.")
			return
		}
	}

	result, err := service.PurgeCapture(memoryops.CapturePurgeInput{Project: status.Project, ContentType: status.ContentType})
	if err != nil {
		failCLI(jsonMode, "capture_purge_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		_ = writeCLIJSON(result)
		return
	}
	fmt.Printf("Purged %d %s Diagnostic captures for %q. Capture consent was not changed.\n",
		result.Deleted, result.ContentType, result.Project)
}

func cmdCaptureDisable(cfg store.Config, args []string) {
	set := newCaptureFlagSet("disable")
	project, contentType, sessionID := "", "", ""
	jsonMode := false
	set.StringVar(&project, "project", "", "project name")
	set.StringVar(&contentType, "type", "", "capture content type")
	set.StringVar(&sessionID, "session-id", "", "opaque session identifier")
	set.BoolVar(&jsonMode, "json", false, "emit JSON")
	if !parseCaptureFlags(set, args, hasArg("--json")) {
		return
	}
	if !validateExplicitCaptureScope(project, contentType, jsonMode) {
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	status, err := memoryops.New(s).DisableCapture(memoryops.CaptureDisableInput{
		Project: project, ContentType: contentType, SessionID: sessionID,
	})
	if err != nil {
		failCaptureOperation(jsonMode, "capture_disable_failed", err)
		return
	}
	writeCaptureStatus(status, jsonMode)
}

func cmdCaptureEnable(cfg store.Config, args []string) {
	set := newCaptureFlagSet("enable")
	project, contentType, sessionID, expiresAtRaw := "", "", "", ""
	retentionDays := store.DefaultDiagnosticRetentionDays
	jsonMode := false
	set.StringVar(&project, "project", "", "project name")
	set.StringVar(&contentType, "type", "", "capture content type")
	set.StringVar(&sessionID, "session-id", "", "opaque session identifier")
	set.StringVar(&expiresAtRaw, "expires-at", "", "session grant expiry in RFC3339")
	set.IntVar(&retentionDays, "retention-days", store.DefaultDiagnosticRetentionDays, "retention in days")
	set.BoolVar(&jsonMode, "json", false, "emit JSON")
	if !parseCaptureFlags(set, args, hasArg("--json")) {
		return
	}
	if !validateExplicitCaptureScope(project, contentType, jsonMode) {
		return
	}
	if retentionDays < 1 || retentionDays > store.MaxDiagnosticRetentionDays {
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("--retention-days must be between 1 and %d", store.MaxDiagnosticRetentionDays), nil)
		return
	}
	sessionProvided := strings.TrimSpace(sessionID) != ""
	expiryProvided := strings.TrimSpace(expiresAtRaw) != ""
	if sessionProvided != expiryProvided {
		failCLI(jsonMode, "invalid_arguments", "--session-id and --expires-at must be provided together", nil)
		return
	}

	var expiresAt *time.Time
	if strings.TrimSpace(expiresAtRaw) != "" {
		parsed, err := time.Parse(time.RFC3339, expiresAtRaw)
		if err != nil {
			failCLI(jsonMode, "invalid_arguments", "--expires-at must be RFC3339", nil)
			return
		}
		expiresAt = &parsed
		if !parsed.After(time.Now()) {
			failCLI(jsonMode, "invalid_arguments", "--expires-at must be in the future", nil)
			return
		}
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	status, err := memoryops.New(s).EnableCapture(memoryops.CaptureEnableInput{
		Project: project, ContentType: contentType, SessionID: sessionID,
		ExpiresAt: expiresAt, RetentionDays: retentionDays,
	})
	if err != nil {
		failCaptureOperation(jsonMode, "capture_enable_failed", err)
		return
	}
	writeCaptureStatus(status, jsonMode)
}

func cmdCaptureStatus(cfg store.Config, args []string) {
	set := newCaptureFlagSet("status")
	project, contentType, sessionID := "", store.CaptureContentTypePrompt, ""
	jsonMode := false
	set.StringVar(&project, "project", "", "project name")
	set.StringVar(&contentType, "type", store.CaptureContentTypePrompt, "capture content type")
	set.StringVar(&sessionID, "session-id", "", "opaque session identifier")
	set.BoolVar(&jsonMode, "json", false, "emit JSON")
	if !parseCaptureFlags(set, args, hasArg("--json")) {
		return
	}
	if strings.TrimSpace(project) == "" {
		resolved := detectProjectFull(currentCWD())
		if resolved.Error != nil || strings.TrimSpace(resolved.Project) == "" {
			failCLI(jsonMode, "ambiguous_project", "could not resolve current project; provide --project", map[string]any{"available_projects": resolved.AvailableProjects})
			return
		}
		project = resolved.Project
	}

	s, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer s.Close()
	status, err := memoryops.New(s).CaptureStatus(memoryops.CaptureStatusInput{
		Project: project, ContentType: contentType, SessionID: sessionID,
	})
	if err != nil {
		failCaptureOperation(jsonMode, "capture_status_failed", err)
		return
	}
	writeCaptureStatus(status, jsonMode)
}

func writeCaptureStatus(status *memoryops.CaptureStatusResult, jsonMode bool) {
	if jsonMode {
		_ = writeCLIJSON(status)
		return
	}
	fmt.Printf("Diagnostic capture %s for %q (%s, %d-day retention, %d stored).\n",
		captureEnabledLabel(status.Enabled), status.Project, status.Scope, status.RetentionDays, status.StoredCount)
}

func newCaptureFlagSet(subcommand string) *flag.FlagSet {
	set := flag.NewFlagSet("capture "+subcommand, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	return set
}

func parseCaptureFlags(set *flag.FlagSet, args []string, jsonMode bool) bool {
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printCaptureUsage()
			return false
		}
		failCLI(jsonMode, "invalid_arguments", err.Error(), nil)
		return false
	}
	if set.NArg() > 0 {
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unexpected capture argument %q", set.Arg(0)), nil)
		return false
	}
	return true
}

func validateExplicitCaptureScope(project, contentType string, jsonMode bool) bool {
	if strings.TrimSpace(project) == "" {
		failCLI(jsonMode, "invalid_arguments", "--project is required", nil)
		return false
	}
	if strings.TrimSpace(contentType) == "" {
		failCLI(jsonMode, "invalid_arguments", "--type is required", nil)
		return false
	}
	return true
}

func failCaptureOperation(jsonMode bool, operationCode string, err error) {
	code := operationCode
	if errors.Is(err, memoryops.ErrProjectRequired) ||
		errors.Is(err, memoryops.ErrCaptureInvalidContentType) ||
		errors.Is(err, memoryops.ErrCaptureInvalidRetention) ||
		errors.Is(err, memoryops.ErrCaptureInvalidSessionGrant) {
		code = "invalid_arguments"
	}
	failCLI(jsonMode, code, err.Error(), nil)
}

func captureEnabledLabel(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
}

func printCaptureUsage() {
	fmt.Println(`usage: engram capture status|enable|disable|purge [options]

  status   [--project PROJECT] [--type TYPE] [--session-id ID] [--json]
  enable   --project PROJECT --type TYPE [--session-id ID --expires-at RFC3339]
           [--retention-days 1..30] [--json]
  disable  --project PROJECT --type TYPE [--session-id ID] [--json]
  purge    --project PROJECT --type TYPE [--yes] [--json]`)
}
