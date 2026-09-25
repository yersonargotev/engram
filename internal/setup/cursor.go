package setup

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"strconv"

	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

const cursorAgentPluginEmbedRoot = "plugins/engram"

func cursorPluginDir() string {
	home, _ := userHome()
	return filepath.Join(home, ".cursor", "plugins", "local", "engram")
}

func installCursorWithOptions(options InstallOptions) (*Result, error) {
	version, commit, err := cursorReleaseIdentity(options)
	if err != nil {
		return nil, err
	}
	if _, err := userHome(); err != nil {
		return nil, err
	}

	dest := cursorPluginDir()
	files, err := materializeCursorAgentPlugin(dest)
	if err != nil {
		return nil, err
	}
	if err := installCursorPluginBinary(dest); err != nil {
		return nil, err
	}
	if err := rewriteCursorPluginMCPCommand(dest); err != nil {
		return nil, err
	}
	files++
	if err := stampCursorPluginIdentity(dest, version, commit); err != nil {
		return nil, err
	}
	files++
	hookPreserved, err := installCursorUserHooks(dest)
	if err != nil {
		return nil, err
	}
	if len(hookPreserved) == 0 {
		files++
	}
	preserved, err := installCursorNativeMCP(dest)
	if err != nil {
		return nil, err
	}
	if len(preserved) == 0 {
		if _, cliErr := lookPathFn("agent"); cliErr == nil {
			// The verified user-level registration serves both Cursor surfaces.
			// Remove the installed plugin MCP declaration to avoid duplicate IDs.
			if err := os.Remove(filepath.Join(dest, "mcp.json")); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("remove duplicate Cursor plugin MCP: %w", err)
			}
			files--
		}
	}

	result := &Result{
		Agent:       "cursor",
		Destination: dest,
		Files:       files,
		Preserved:   append(preserved, hookPreserved...),
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("resolve Cursor setup directory: %w", err)
	}
	status, err := InspectCursorStatus(version, commit, workingDirectory)
	if err != nil {
		return nil, fmt.Errorf("inspect installed Cursor integration: %w", err)
	}
	result.Checks = cursorSetupCapabilityChecks(status, result.Preserved)
	result.Complete = checksReady(result.Checks)
	return result, nil
}

func cursorSetupCapabilityChecks(status CursorIntegrationStatus, preserved []string) []CapabilityCheck {
	checks := make([]CapabilityCheck, 0, 5)
	for _, capability := range []string{"plugin", "skill", "mcp", "hooks", "cli_mcp"} {
		var matched *CursorIntegrationCheck
		for i := range status.Checks {
			if status.Checks[i].Capability == capability {
				matched = &status.Checks[i]
				break
			}
		}
		if matched == nil {
			checks = append(checks, CapabilityCheck{Capability: capability, Status: CheckMissing, Detail: "post-install inspection did not report this capability"})
			continue
		}
		if cursorCapabilityPreserved(capability, preserved) {
			checks = append(checks, CapabilityCheck{
				Capability: capability,
				Status:     CheckPreserved,
				Detail:     "custom activation state was preserved and may take precedence over the installed integration",
			})
			continue
		}
		setupStatus := CheckFailed
		switch matched.Status {
		case CursorCheckReady:
			setupStatus = CheckReady
		case CursorCheckMissing:
			setupStatus = CheckMissing
		case CursorCheckCustomized:
			setupStatus = CheckPreserved
		case CursorCheckPending:
			setupStatus = CheckPending
		}
		checks = append(checks, CapabilityCheck{Capability: capability, Status: setupStatus, Detail: matched.Reason})
	}
	return checks
}

func cursorCapabilityPreserved(capability string, preserved []string) bool {
	want := ""
	switch capability {
	case "mcp":
		want = "mcpServers.engram"
	case "hooks":
		want = "hooks"
	default:
		return false
	}
	for _, item := range preserved {
		if item == want {
			return true
		}
	}
	return false
}

func cursorReleaseIdentity(options InstallOptions) (string, string, error) {
	if options.Development {
		return strings.TrimPrefix(strings.TrimSpace(options.Version), "v"), strings.ToLower(strings.TrimSpace(options.Commit)), nil
	}

	version := strings.TrimPrefix(strings.TrimSpace(options.Version), "v")
	commit := strings.ToLower(strings.TrimSpace(options.Commit))
	decodedCommit, err := hex.DecodeString(commit)
	releaseVersion := "v" + version
	if !semver.IsValid(releaseVersion) || module.IsPseudoVersion(releaseVersion) || strings.Contains(version, "+dirty") || err != nil || len(decodedCommit) != 20 {
		return "", "", fmt.Errorf("stable Cursor setup requires a release identity with a semantic version and exact 40-character commit; rerun from a release build or use explicit development mode")
	}
	return version, commit, nil
}

func stampCursorPluginIdentity(dest, version, commit string) error {
	identity := map[string]string{
		"version": version,
		"commit":  commit,
	}
	raw, err := jsonMarshalIndentFn(identity, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal Cursor release identity: %w", err)
	}
	if err := writeFileFn(filepath.Join(dest, ".engram-release.json"), append(raw, '\n'), 0644); err != nil {
		return fmt.Errorf("write Cursor release identity: %w", err)
	}

	manifestPath := filepath.Join(dest, "plugin.json")
	manifestRaw, err := readFileFn(manifestPath)
	if err != nil {
		return fmt.Errorf("read installed Cursor plugin manifest: %w", err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return fmt.Errorf("parse installed Cursor plugin manifest: %w", err)
	}
	manifest["version"] = version
	updated, err := jsonMarshalIndentFn(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal installed Cursor plugin manifest: %w", err)
	}
	if err := writeFileFn(manifestPath, append(updated, '\n'), 0644); err != nil {
		return fmt.Errorf("write installed Cursor plugin manifest: %w", err)
	}
	return nil
}

func materializeCursorAgentPlugin(dest string) (int, error) {
	if err := os.MkdirAll(dest, 0755); err != nil {
		return 0, fmt.Errorf("create Cursor plugin directory: %w", err)
	}

	files := 0
	err := fs.WalkDir(cursorAgentPluginFS, cursorAgentPluginEmbedRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(cursorAgentPluginEmbedRoot, path)
		if err != nil {
			return err
		}
		data, err := cursorAgentPluginFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read embedded Agent Plugin %s: %w", rel, err)
		}
		target := filepath.Join(dest, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return fmt.Errorf("create Cursor plugin path for %s: %w", rel, err)
		}
		if err := writeFileFn(target, data, 0644); err != nil {
			return fmt.Errorf("write Cursor plugin %s: %w", rel, err)
		}
		files++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return files, nil
}

func installCursorPluginBinary(dest string) error {
	src, err := osExecutable()
	if err != nil {
		return fmt.Errorf("resolve Engram binary for Cursor plugin: %w", err)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read Engram binary for Cursor plugin: %w", err)
	}
	target := cursorHookBinary(dest)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return fmt.Errorf("create Cursor plugin bin directory: %w", err)
	}
	if err := writeFileFn(target, data, 0755); err != nil {
		return fmt.Errorf("write Cursor plugin binary: %w", err)
	}
	return nil
}

func rewriteCursorPluginMCPCommand(dest string) error {
	path := filepath.Join(dest, "mcp.json")
	raw, err := readFileFn(path)
	if err != nil {
		return fmt.Errorf("read Cursor plugin MCP: %w", err)
	}
	var config struct {
		Schema  string `json:"$schema"`
		Servers map[string]struct {
			Type    string   `json:"type"`
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return fmt.Errorf("parse Cursor plugin MCP: %w", err)
	}
	entry, ok := config.Servers["engram"]
	if !ok {
		return fmt.Errorf("Cursor plugin MCP is missing the engram server")
	}
	entry.Command = cursorHookBinary(dest)
	config.Servers["engram"] = entry
	updated, err := jsonMarshalIndentFn(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal Cursor plugin MCP: %w", err)
	}
	if err := writeFileFn(path, append(updated, '\n'), 0644); err != nil {
		return fmt.Errorf("write Cursor plugin MCP: %w", err)
	}
	return nil
}

func cursorMCPPath() string {
	home, _ := userHome()
	return filepath.Join(home, ".cursor", "mcp.json")
}

func cursorHooksPath() string {
	home, _ := userHome()
	return filepath.Join(home, ".cursor", "hooks.json")
}

type cursorUserHooksFile struct {
	Version int                          `json:"version"`
	Hooks   map[string][]json.RawMessage `json:"hooks"`
}

type cursorHookSpec struct {
	Command   string `json:"command"`
	LoopLimit *int   `json:"loop_limit,omitempty"`
}

func installCursorUserHooks(pluginRoot string) ([]string, error) {
	path := cursorHooksPath()
	config, preserved, err := readCursorUserHooks(path)
	if err != nil {
		return nil, err
	}
	if preserved != nil {
		return preserved, nil
	}

	loopLimit := 1
	owned := []struct {
		event string
		spec  cursorHookSpec
	}{
		{event: "sessionStart", spec: cursorHookSpec{Command: cursorSessionStartCommand(pluginRoot)}},
		{event: "beforeSubmitPrompt", spec: cursorHookSpec{Command: cursorPromptSubmitCommand(pluginRoot)}},
		{event: "stop", spec: cursorHookSpec{Command: cursorStopCommand(pluginRoot), LoopLimit: &loopLimit}},
	}
	if config.Hooks == nil {
		config.Hooks = map[string][]json.RawMessage{}
	}
	config.Version = 1
	for _, ownedHook := range owned {
		kept := make([]json.RawMessage, 0, len(config.Hooks[ownedHook.event]))
		for _, raw := range config.Hooks[ownedHook.event] {
			var existing cursorHookSpec
			if err := json.Unmarshal(raw, &existing); err != nil {
				kept = append(kept, raw)
				continue
			}
			if cursorOwnedHookCommand(existing.Command) {
				continue
			}
			kept = append(kept, raw)
		}
		entry, err := jsonMarshalFn(ownedHook.spec)
		if err != nil {
			return nil, fmt.Errorf("marshal Cursor %s hook: %w", ownedHook.event, err)
		}
		config.Hooks[ownedHook.event] = append(kept, json.RawMessage(entry))
	}
	if err := writeCursorUserHooks(path, config); err != nil {
		return nil, err
	}
	return nil, nil
}

func readCursorUserHooks(path string) (cursorUserHooksFile, []string, error) {
	raw, err := readFileFn(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cursorUserHooksFile{Version: 1, Hooks: map[string][]json.RawMessage{}}, nil, nil
		}
		return cursorUserHooksFile{}, nil, fmt.Errorf("read Cursor user hooks: %w", err)
	}
	var config cursorUserHooksFile
	if err := json.Unmarshal(raw, &config); err != nil {
		return cursorUserHooksFile{}, []string{"hooks"}, nil
	}
	if config.Hooks == nil {
		config.Hooks = map[string][]json.RawMessage{}
	}
	return config, nil, nil
}

func writeCursorUserHooks(path string, config cursorUserHooksFile) error {
	raw, err := jsonMarshalIndentFn(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal Cursor user hooks: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create Cursor hooks directory: %w", err)
	}
	if err := writeFileFn(path, append(raw, '\n'), 0644); err != nil {
		return fmt.Errorf("write Cursor user hooks: %w", err)
	}
	return nil
}

func cursorOwnedHookCommand(command string) bool {
	command = strings.TrimSpace(command)
	if !strings.Contains(command, "--host=cursor") && !strings.Contains(command, "--host cursor") {
		return false
	}
	return strings.Contains(command, "lifecycle session-start") ||
		strings.Contains(command, "lifecycle prompt-submit") ||
		strings.Contains(command, "checkpoint verify-stop")
}

func cursorSessionStartCommand(pluginRoot string) string {
	return quoteCursorHookArg(cursorHookBinary(pluginRoot)) +
		" lifecycle session-start --host=cursor --plugin-root=" + quoteCursorHookArg(pluginRoot)
}

func cursorPromptSubmitCommand(pluginRoot string) string {
	return quoteCursorHookArg(cursorHookBinary(pluginRoot)) + " lifecycle prompt-submit --host=cursor"
}

func cursorStopCommand(pluginRoot string) string {
	return quoteCursorHookArg(cursorHookBinary(pluginRoot)) + " checkpoint verify-stop --host=cursor"
}

func cursorHookBinary(pluginRoot string) string {
	return filepath.Join(pluginRoot, "bin", "engram")
}

func quoteCursorHookArg(value string) string {
	if strings.ContainsAny(value, " \t\"'") {
		return strconv.Quote(value)
	}
	return value
}

// installCursorNativeMCP gives Cursor CLI a user-level registration. Cursor CLI
// does not discover the local plugin's mcp.json. Preserve unknown entries and
// restore the previous file if the new registration is not discoverable.
func installCursorNativeMCP(pluginRoot string) ([]string, error) {
	path := cursorMCPPath()
	previous, readErr := readFileFn(path)
	if readErr != nil && !os.IsNotExist(readErr) {
		return []string{"mcpServers.engram"}, nil
	}
	config, err := readJSONConfig(path)
	if err != nil {
		return []string{"mcpServers.engram"}, nil
	}
	raw, ok := config["mcpServers"]
	servers := make(map[string]json.RawMessage)
	if ok && json.Unmarshal(raw, &servers) != nil {
		return []string{"mcpServers.engram"}, nil
	}
	if servers == nil {
		servers = make(map[string]json.RawMessage)
	}
	entry, exists := servers["engram"]
	if exists && !cursorNativeMCPOwned(entry) {
		return []string{"mcpServers.engram"}, nil
	}
	command := cursorHookBinary(pluginRoot)
	updatedEntry, err := jsonMarshalFn(map[string]any{
		"type": "stdio", "command": command, "args": []string{"mcp", "--tools=agent"},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal Cursor native MCP entry: %w", err)
	}
	servers["engram"] = updatedEntry
	block, err := jsonMarshalFn(servers)
	if err != nil {
		return nil, fmt.Errorf("marshal Cursor native MCP servers: %w", err)
	}
	config["mcpServers"] = json.RawMessage(block)
	if err := writeJSONConfig(path, config); err != nil {
		return nil, fmt.Errorf("write Cursor native MCP: %w", err)
	}
	if _, err := lookPathFn("agent"); err == nil {
		isolated, err := os.MkdirTemp("", "engram-cursor-cli-verify-")
		if err != nil {
			return nil, fmt.Errorf("create Cursor CLI verification directory: %w", err)
		}
		defer os.RemoveAll(isolated)
		check := inspectCursorCLIMCPStatus(isolated)
		if check.Status == CursorCheckMissing || check.Status == CursorCheckUnavailable || check.Status == CursorCheckCustomized {
			if readErr == nil {
				if restoreErr := writeFileFn(path, previous, 0644); restoreErr != nil {
					return nil, fmt.Errorf("Cursor CLI verification failed (%s); restore previous native MCP: %w", check.ReasonCode, restoreErr)
				}
			} else if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
				return nil, fmt.Errorf("Cursor CLI verification failed (%s); remove new native MCP: %w", check.ReasonCode, removeErr)
			}
			return nil, fmt.Errorf("Cursor CLI did not discover the new Engram registration: %s", check.Reason)
		}
	}
	return nil, nil
}

func cursorNativeMCPOwned(raw json.RawMessage) bool {
	var entry map[string]any
	if err := json.Unmarshal(raw, &entry); err != nil {
		return false
	}
	for key := range entry {
		if key != "command" && key != "args" && key != "type" {
			return false
		}
	}
	if kind, ok := entry["type"]; ok && kind != "stdio" {
		return false
	}
	command, _ := entry["command"].(string)
	if command != "engram" && command != "engram.exe" && command != cursorHookBinary(cursorPluginDir()) {
		return false
	}
	args, ok := entry["args"].([]any)
	if !ok || len(args) != 2 {
		return false
	}
	return args[0] == "mcp" && args[1] == "--tools=agent"
}
