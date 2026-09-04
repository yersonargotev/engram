package setup

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

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
	files++
	if err := stampCursorPluginIdentity(dest, version, commit); err != nil {
		return nil, err
	}
	files++
	preserved, err := retireOwnedCursorNativeMCP()
	if err != nil {
		return nil, err
	}

	return &Result{
		Agent:       "cursor",
		Destination: dest,
		Files:       files,
		Preserved:   preserved,
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
	target := filepath.Join(dest, "bin", "engram")
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return fmt.Errorf("create Cursor plugin bin directory: %w", err)
	}
	if err := writeFileFn(target, data, 0755); err != nil {
		return fmt.Errorf("write Cursor plugin binary: %w", err)
	}
	return nil
}

func cursorMCPPath() string {
	home, _ := userHome()
	return filepath.Join(home, ".cursor", "mcp.json")
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
