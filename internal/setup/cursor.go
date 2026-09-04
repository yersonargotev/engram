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
	preserved, err := retireOwnedCursorNativeMCP()
	if err != nil {
		return nil, err
	}

	return &Result{
		Agent:       "cursor",
		Destination: dest,
		Files:       files,
		Preserved:   append(preserved, hookPreserved...),
	}, nil
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
	return strings.Contains(command, "lifecycle session-start") || strings.Contains(command, "checkpoint verify-stop")
}

func cursorSessionStartCommand(pluginRoot string) string {
	return quoteCursorHookArg(cursorHookBinary(pluginRoot)) +
		" lifecycle session-start --host=cursor --plugin-root=" + quoteCursorHookArg(pluginRoot)
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

func retireOwnedCursorNativeMCP() ([]string, error) {
	path := cursorMCPPath()
	config, err := readJSONConfig(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return []string{"mcpServers.engram"}, nil
	}
	raw, ok := config["mcpServers"]
	if !ok {
		return nil, nil
	}
	servers := make(map[string]json.RawMessage)
	if err := json.Unmarshal(raw, &servers); err != nil {
		return []string{"mcpServers.engram"}, nil
	}
	if servers == nil {
		return nil, nil
	}
	entry, ok := servers["engram"]
	if !ok {
		return nil, nil
	}
	if !cursorNativeMCPOwned(entry) {
		return []string{"mcpServers.engram"}, nil
	}
	delete(servers, "engram")
	if len(servers) == 0 && len(config) == 1 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove owned Cursor native MCP: %w", err)
		}
		return nil, nil
	}
	block, err := jsonMarshalFn(servers)
	if err != nil {
		return nil, fmt.Errorf("marshal Cursor native MCP servers: %w", err)
	}
	config["mcpServers"] = json.RawMessage(block)
	if err := writeJSONConfig(path, config); err != nil {
		return nil, fmt.Errorf("rewrite Cursor native MCP: %w", err)
	}
	return nil, nil
}

func cursorNativeMCPOwned(raw json.RawMessage) bool {
	var entry map[string]any
	if err := json.Unmarshal(raw, &entry); err != nil {
		return false
	}
	command, _ := entry["command"].(string)
	if !cursorNativeMCPCommandOwned(command) {
		return false
	}
	args, ok := entry["args"].([]any)
	if !ok || len(args) != 2 {
		return false
	}
	return args[0] == "mcp" && args[1] == "--tools=agent"
}

func cursorNativeMCPCommandOwned(command string) bool {
	base := strings.ToLower(filepath.Base(filepath.Clean(command)))
	return base == "engram" || base == "engram.exe"
}
