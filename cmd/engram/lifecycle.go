package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yersonargotev/engram/internal/codexlifecycle"
	projectpkg "github.com/yersonargotev/engram/internal/project"
	"github.com/yersonargotev/engram/internal/store"
)

const maxCodexLifecycleInputBytes = 64 * 1024

type codexLifecycleEvent struct {
	SessionID *string `json:"session_id"`
	CWD       *string `json:"cwd"`
	Source    *string `json:"source"`
}

type codexLifecycleResponse struct {
	HookSpecificOutput struct {
		HookEventName     string `json:"hookEventName"`
		AdditionalContext string `json:"additionalContext"`
	} `json:"hookSpecificOutput"`
}

var startLifecycleImport = startCodexLifecycleImport

func cmdLifecycle(cfg store.Config) {
	if len(os.Args) < 3 {
		writeEmptyHookResponse()
		return
	}
	switch strings.ToLower(strings.TrimSpace(os.Args[2])) {
	case "session-start":
		cmdLifecycleSessionStart(cfg, os.Args[3:], os.Stdin)
	default:
		writeEmptyHookResponse()
	}
}

func cmdLifecycleSessionStart(cfg store.Config, args []string, input io.Reader) {
	started := time.Now()
	set := flag.NewFlagSet("lifecycle session-start", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	host, pluginRoot := "", ""
	set.StringVar(&host, "host", "", "hook host")
	set.StringVar(&pluginRoot, "plugin-root", "", "installed plugin root")
	if err := set.Parse(args); err != nil || set.NArg() != 0 || strings.ToLower(strings.TrimSpace(host)) != "codex" {
		writeEmptyHookResponse()
		return
	}

	raw, err := io.ReadAll(io.LimitReader(input, maxCodexLifecycleInputBytes+1))
	if err != nil || len(raw) > maxCodexLifecycleInputBytes || !utf8.Valid(raw) {
		writeEmptyHookResponse()
		return
	}
	var event codexLifecycleEvent
	if err := json.Unmarshal(raw, &event); err != nil || event.SessionID == nil || event.CWD == nil || event.Source == nil {
		writeEmptyHookResponse()
		return
	}
	sessionID := *event.SessionID
	cwd := *event.CWD
	source := strings.ToLower(strings.TrimSpace(*event.Source))
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(cwd) == "" || !codexLifecycleSource(source) {
		writeEmptyHookResponse()
		return
	}

	cue, err := codexlifecycle.ReadCanonicalCue(pluginRoot)
	if err != nil {
		writeEmptyHookResponse()
		return
	}
	selection := codexlifecycle.SelectTreatment(os.Getenv(codexlifecycle.EnvTreatment))
	extra := ""
	project, projectErr := resolveCodexLifecycleProject(cwd)
	if projectErr == nil {
		s, storeErr := storeNew(cfg)
		if storeErr == nil {
			if registerErr := s.CreateSession(sessionID, project, cwd); registerErr == nil {
				switch {
				case !selection.Enabled:
					extra, _ = s.FormatContext(project, "")
					if source != "compact" {
						startLifecycleImport(cwd, project)
					}
				case source == "compact" && selection.Treatment == codexlifecycle.TreatmentCueOnlyTargetedRecallExactSession:
					extra, _ = s.FormatCompactionContext(sessionID)
				}
			}
			_ = s.Close()
		}
	}

	modelContext, _ := codexlifecycle.BuildModelContext(cue, extra, codexlifecycle.MaxInjectedUTF8Bytes)
	response := codexLifecycleResponse{}
	response.HookSpecificOutput.HookEventName = "SessionStart"
	response.HookSpecificOutput.AdditionalContext = modelContext
	_ = writeCLIJSON(response)
	startCodexLifecycleBaseline(time.Since(started), len(modelContext))
}

func startCodexLifecycleBaseline(latency time.Duration, deliveredBytes int) {
	if !recallBaselineCollectionEnabled() {
		return
	}
	executable, err := os.Executable()
	if err != nil {
		return
	}
	latencyMillis := strconv.FormatFloat(float64(latency)/float64(time.Millisecond), 'f', 6, 64)
	command := exec.Command(executable,
		"recall-baseline", "record",
		"--kind", "operation", "--surface", "lifecycle", "--operation", "session_start", "--outcome", "success",
		"--latency-ms", latencyMillis, "--delivered-bytes", strconv.Itoa(deliveredBytes),
	)
	command.Stdin = nil
	command.Stdout = nil
	command.Stderr = nil
	if err := command.Start(); err == nil {
		_ = command.Process.Release()
	}
}

func codexLifecycleSource(source string) bool {
	switch source {
	case "startup", "resume", "clear", "compact":
		return true
	default:
		return false
	}
}

func resolveCodexLifecycleProject(cwd string) (string, error) {
	if override, ok := projectpkg.ProcessOverride(""); ok {
		project, _ := store.NormalizeProject(override)
		if project != "" {
			return project, nil
		}
	}
	resolved := detectProjectFull(cwd)
	if err := projectpkg.RequireImplicitWriteAuthority(resolved); err != nil {
		return "", err
	}
	project, _ := store.NormalizeProject(resolved.Project)
	if project == "" {
		return "", fmt.Errorf("project is required")
	}
	return project, nil
}

func startCodexLifecycleImport(cwd, project string) {
	if _, err := os.Stat(filepath.Join(cwd, ".engram", "manifest.json")); err != nil {
		return
	}
	executable, err := os.Executable()
	if err != nil {
		return
	}
	command := exec.Command(executable, "sync", "--import", "--project", project)
	command.Dir = cwd
	command.Stdin = nil
	command.Stdout = nil
	command.Stderr = nil
	if err := command.Start(); err == nil {
		_ = command.Process.Release()
	}
}

func writeEmptyHookResponse() {
	_ = writeCLIJSON(map[string]any{})
}
