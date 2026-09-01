package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/memoryops"
	projectpkg "github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/recallbaseline"
	"github.com/yersonargotev/engram/internal/store"
)

const (
	maxSubagentHookInputBytes           = 32 * 1024
	maxPromptHookInputBytes             = 4 * 1024 * 1024
	maxPromptHookAdditionalContextBytes = 1024
)

type subagentHookEvent struct {
	SessionID            *string `json:"session_id"`
	TurnID               *string `json:"turn_id"`
	CWD                  *string `json:"cwd"`
	LastAssistantMessage *string `json:"last_assistant_message"`
}

type subagentHookResponse struct {
	SystemMessage string `json:"systemMessage,omitempty"`
}

type promptHookEvent struct {
	SessionID *string `json:"session_id"`
	TurnID    *string `json:"turn_id"`
	CWD       *string `json:"cwd"`
	Prompt    *string `json:"prompt"`
}

type promptHookIdentity struct {
	Host       string `json:"host"`
	SessionID  string `json:"session_id"`
	RootTurnID string `json:"root_turn_id"`
}

type promptPersistEvent struct {
	promptHookEvent
	ObservedAt time.Time `json:"_engram_observed_at"`
}

var captureInputInteractive = func() bool {
	info, err := os.Stdin.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

var startPromptCapturePersistence = startCodexPromptCapturePersistence

func cmdCapture(cfg store.Config) {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "capture requires status, enable, disable, purge, prompt-hook, or subagent-hook", nil)
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
	case "subagent-hook":
		cmdCaptureSubagentHook(cfg, os.Args[3:], os.Stdin)
	case "prompt-hook":
		cmdCapturePromptHook(cfg, os.Args[3:], os.Stdin)
	case "prompt-persist":
		cmdCapturePromptPersist(cfg, os.Args[3:], os.Stdin)
	case "help", "--help", "-h":
		printCaptureUsage()
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown capture command %q", os.Args[2]), nil)
	}
}

func cmdCapturePromptHook(cfg store.Config, args []string, input io.Reader) {
	observedAt := time.Now().UTC()
	set := newCaptureFlagSet("prompt-hook")
	host := ""
	set.StringVar(&host, "host", "", "hook host (codex)")
	if err := set.Parse(args); err != nil || set.NArg() != 0 || strings.ToLower(strings.TrimSpace(host)) != "codex" {
		_ = writeCLIJSON(map[string]any{})
		return
	}

	raw, err := io.ReadAll(io.LimitReader(input, maxPromptHookInputBytes+1))
	if err != nil || len(raw) > maxPromptHookInputBytes || !utf8.Valid(raw) {
		_ = writeCLIJSON(map[string]any{})
		return
	}
	var event promptHookEvent
	if err := json.Unmarshal(raw, &event); err != nil || event.SessionID == nil || event.TurnID == nil {
		_ = writeCLIJSON(map[string]any{})
		return
	}
	sessionID := *event.SessionID
	rootTurnID := *event.TurnID
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(rootTurnID) == "" {
		_ = writeCLIJSON(map[string]any{})
		return
	}

	identity, _ := json.Marshal(promptHookIdentity{Host: "codex", SessionID: sessionID, RootTurnID: rootTurnID})
	context := "Engram checkpoint identity for this root user turn: " + string(identity) + ". Pass these opaque values unchanged to mem_checkpoint or engram checkpoint record."
	if len(context) > maxPromptHookAdditionalContextBytes {
		_ = writeCLIJSON(map[string]any{})
		return
	}
	response := map[string]any{
		"hookSpecificOutput": map[string]any{
			"hookEventName":     "UserPromptSubmit",
			"additionalContext": context,
		},
	}
	_ = writeCLIJSON(response)
	if event.Prompt != nil && strings.TrimSpace(*event.Prompt) != "" {
		persistRaw, err := json.Marshal(promptPersistEvent{promptHookEvent: event, ObservedAt: observedAt})
		if err == nil {
			startPromptCapturePersistence(persistRaw)
		}
	}
}

func cmdCapturePromptPersist(cfg store.Config, args []string, input io.Reader) {
	set := newCaptureFlagSet("prompt-persist")
	host := ""
	set.StringVar(&host, "host", "", "hook host (codex)")
	if err := set.Parse(args); err != nil || set.NArg() != 0 || strings.ToLower(strings.TrimSpace(host)) != "codex" {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(input, maxPromptHookInputBytes+1))
	if err != nil || len(raw) > maxPromptHookInputBytes || !utf8.Valid(raw) {
		return
	}
	var persisted promptPersistEvent
	if err := json.Unmarshal(raw, &persisted); err != nil || persisted.SessionID == nil || persisted.Prompt == nil ||
		strings.TrimSpace(*persisted.SessionID) == "" || strings.TrimSpace(*persisted.Prompt) == "" || persisted.ObservedAt.IsZero() {
		return
	}
	now := time.Now().UTC()
	if persisted.ObservedAt.After(now.Add(5*time.Second)) || persisted.ObservedAt.Before(now.Add(-30*time.Second)) {
		return
	}
	captureCodexPromptDiagnostic(cfg, persisted.promptHookEvent, *persisted.SessionID, persisted.ObservedAt)
}

func startCodexPromptCapturePersistence(raw []byte) {
	executable, err := os.Executable()
	if err != nil {
		return
	}
	command := exec.Command(executable, "capture", "prompt-persist", "--host=codex")
	command.Stdout = nil
	command.Stderr = nil
	stdin, err := command.StdinPipe()
	if err != nil {
		return
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return
	}
	if _, err := stdin.Write(raw); err != nil {
		_ = stdin.Close()
		_ = command.Process.Kill()
		_ = command.Wait()
		return
	}
	_ = stdin.Close()
	_ = command.Process.Release()
}

func captureCodexPromptDiagnostic(cfg store.Config, event promptHookEvent, sessionID string, observedAt time.Time) {
	outcome := recallbaseline.OutcomeUnknown
	defer func() { recordSubagentCaptureBaselineOperation(cfg, "prompt", outcome) }()
	cwd := currentCWD()
	if event.CWD != nil && strings.TrimSpace(*event.CWD) != "" {
		cwd = *event.CWD
	}
	project, err := resolveCodexLifecycleProject(cwd)
	if err != nil {
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		return
	}
	defer s.Close()
	session, err := s.GetSession(sessionID)
	if err != nil {
		return
	}
	sessionProject, _ := store.NormalizeProject(session.Project)
	if sessionProject != project {
		return
	}
	result, err := memoryops.New(s).Capture(memoryops.CaptureInput{
		Project: project, ContentType: store.CaptureContentTypePrompt,
		SessionID: sessionID, Content: *event.Prompt, ObservedAt: observedAt,
	})
	if err != nil {
		return
	}
	outcome = recallbaseline.OutcomeDisabled
	if result.Captured {
		outcome = recallbaseline.OutcomeEnabled
	}
}

func recordSubagentCaptureBaselineOperation(cfg store.Config, operation string, outcome recallbaseline.Outcome) {
	recordRecallBaselineEvents(cfg, recallbaseline.Event{
		Kind: recallbaseline.EventCapture, Surface: recallbaseline.SurfaceLifecycle,
		Operation: operation, Outcome: outcome,
	})
}

func cmdCaptureSubagentHook(cfg store.Config, args []string, input io.Reader) {
	set := newCaptureFlagSet("subagent-hook")
	host := ""
	set.StringVar(&host, "host", "", "hook host (codex or claude-code)")
	if err := set.Parse(args); err != nil || set.NArg() != 0 {
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture unavailable: invalid hook command."})
		return
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if host != "codex" && host != "claude-code" {
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture unavailable: unsupported hook host."})
		return
	}

	raw, err := io.ReadAll(io.LimitReader(input, maxSubagentHookInputBytes+1))
	if err != nil || len(raw) > maxSubagentHookInputBytes || !utf8.Valid(raw) {
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture rejected: hook input is malformed or oversized."})
		return
	}
	var event subagentHookEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture rejected: hook input is malformed or oversized."})
		return
	}

	sessionID := subagentHookSessionID(host, event)
	recordRecallBaselineEvents(cfg, recallbaseline.Event{
		Kind: recallbaseline.EventSubagentStop, Surface: recallbaseline.SurfaceLifecycle,
		Operation: "subagent_stop", Outcome: recallbaseline.OutcomeObserved,
	})
	if event.LastAssistantMessage == nil || strings.TrimSpace(*event.LastAssistantMessage) == "" {
		writeSubagentHookResponse(subagentHookResponse{})
		return
	}

	project, err := resolveSubagentHookProject(event.CWD)
	if err != nil {
		recordSubagentCaptureBaseline(cfg, recallbaseline.OutcomeUnknown)
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture unavailable: project identity is not authoritative."})
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		recordSubagentCaptureBaseline(cfg, recallbaseline.OutcomeUnknown)
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture unavailable: local storage could not be opened."})
		return
	}
	defer s.Close()

	result, err := memoryops.New(s).CaptureSubagentDiagnostic(memoryops.SubagentDiagnosticInput{
		Project: project, SessionID: sessionID, Envelope: *event.LastAssistantMessage,
	})
	if errors.Is(err, memoryops.ErrSubagentDiagnosticEnvelope) {
		recordSubagentCaptureBaseline(cfg, recallbaseline.OutcomeEnabled)
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture rejected: the consented message is not a valid bounded engram_diagnostic envelope."})
		return
	}
	if err != nil {
		recordSubagentCaptureBaseline(cfg, recallbaseline.OutcomeUnknown)
		writeSubagentHookResponse(subagentHookResponse{SystemMessage: "Engram subagent Diagnostic capture unavailable: the local capture boundary failed."})
		return
	}
	if result.Captured {
		recordSubagentCaptureBaseline(cfg, recallbaseline.OutcomeEnabled)
	} else {
		recordSubagentCaptureBaseline(cfg, recallbaseline.OutcomeDisabled)
	}
	writeSubagentHookResponse(subagentHookResponse{})
}

func subagentHookSessionID(host string, event subagentHookEvent) string {
	if host == "codex" && event.TurnID != nil {
		return strings.TrimSpace(*event.TurnID)
	}
	if host == "claude-code" && event.SessionID != nil {
		return strings.TrimSpace(*event.SessionID)
	}
	return ""
}

func resolveSubagentHookProject(cwd *string) (string, error) {
	if override, ok := projectpkg.ProcessOverride(""); ok {
		project, _ := store.NormalizeProject(override)
		if project != "" {
			return project, nil
		}
	}
	directory := currentCWD()
	if cwd != nil && strings.TrimSpace(*cwd) != "" {
		directory = strings.TrimSpace(*cwd)
	}
	resolved := detectProjectFull(directory)
	if err := projectpkg.RequireImplicitWriteAuthority(resolved); err != nil {
		return "", err
	}
	project, _ := store.NormalizeProject(resolved.Project)
	if project == "" {
		return "", memoryops.ErrProjectRequired
	}
	return project, nil
}

func recordSubagentCaptureBaseline(cfg store.Config, outcome recallbaseline.Outcome) {
	recordSubagentCaptureBaselineOperation(cfg, "subagent", outcome)
}

func writeSubagentHookResponse(response subagentHookResponse) {
	_ = writeCLIJSON(response)
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
	fmt.Println(`usage: engram capture status|enable|disable|purge|prompt-hook|subagent-hook [options]

  status   [--project PROJECT] [--type TYPE] [--session-id ID] [--json]
  enable   --project PROJECT --type TYPE [--session-id ID --expires-at RFC3339]
           [--retention-days 1..30] [--json]
  disable  --project PROJECT --type TYPE [--session-id ID] [--json]
  purge    --project PROJECT --type TYPE [--yes] [--json]
  prompt-hook --host codex
  subagent-hook --host codex|claude-code`)
}
