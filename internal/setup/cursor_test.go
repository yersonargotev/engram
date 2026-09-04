package setup

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func stubCursorInstallEnv(t *testing.T) string {
	t.Helper()
	resetSetupSeams(t)
	home := useTestHome(t)
	runtimeGOOS = "linux"
	bin := filepath.Join(t.TempDir(), "engram")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write stub engram binary: %v", err)
	}
	osExecutable = func() (string, error) { return bin, nil }
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("APPDATA", "")
	return home
}

func TestInstallCursorWritesAgentPluginInLocalPluginDirectory(t *testing.T) {
	home := stubCursorInstallEnv(t)

	result, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	})
	if err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}
	if result.Agent != "cursor" {
		t.Fatalf("result.Agent = %q, want cursor", result.Agent)
	}

	pluginRoot := filepath.Join(home, ".cursor", "plugins", "local", "engram")
	for _, rel := range []string{
		"plugin.json",
		"mcp.json",
		filepath.Join("skills", "engram-memory", "SKILL.md"),
		filepath.Join("bin", "engram"),
	} {
		path := filepath.Join(pluginRoot, rel)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("installed Agent Plugin missing %s: %v", rel, err)
		}
	}
}

func TestInstallCursorUsesPluginMCPInsteadOfNativeActivationWrite(t *testing.T) {
	home := stubCursorInstallEnv(t)

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	if _, err := os.Stat(filepath.Join(home, ".cursor", "mcp.json")); !os.IsNotExist(err) {
		t.Fatalf("native ~/.cursor/mcp.json = %v, want absent so MCP comes from the plugin", err)
	}

	pluginRoot := filepath.Join(home, ".cursor", "plugins", "local", "engram")
	raw, err := os.ReadFile(filepath.Join(pluginRoot, "mcp.json"))
	if err != nil {
		t.Fatalf("read installed plugin MCP: %v", err)
	}
	wantCommand := cursorHookBinary(pluginRoot)
	if !strings.Contains(string(raw), strconv.Quote(wantCommand)) {
		t.Fatalf("plugin MCP command = %s, want absolute plugin binary %q so Cursor does not resolve ./bin/engram against the workspace cwd", raw, wantCommand)
	}
	if strings.Contains(string(raw), `"./bin/engram"`) {
		t.Fatalf("installed plugin MCP kept the portable relative command; Cursor spawn would ENOENT against the workspace")
	}
	if !strings.Contains(string(raw), `"--tools=agent"`) {
		t.Fatalf("plugin MCP args = %s, want --tools=agent", raw)
	}
	if _, err := os.Stat(filepath.Join(home, ".cursor", "rules", "engram.mdc")); !os.IsNotExist(err) {
		t.Fatalf("global Cursor rule file = %v, want absent", err)
	}
}

func TestInstallCursorDoesNotCopySkillIntoUserSkillTrees(t *testing.T) {
	home := stubCursorInstallEnv(t)

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	for _, rel := range []string{
		filepath.Join(".cursor", "skills", "engram-memory", "SKILL.md"),
		filepath.Join(".agents", "skills", "engram-memory", "SKILL.md"),
		filepath.Join(".agents", "skills", "engram-memory-cli", "SKILL.md"),
	} {
		if _, err := os.Stat(filepath.Join(home, rel)); !os.IsNotExist(err) {
			t.Errorf("user skill tree %s = %v, want absent", rel, err)
		}
	}
}

func TestInstallCursorDoesNotWriteProjectCursorFiles(t *testing.T) {
	home := stubCursorInstallEnv(t)
	project := t.TempDir()
	if err := os.MkdirAll(filepath.Join(project, ".cursor"), 0755); err != nil {
		t.Fatalf("create project .cursor: %v", err)
	}
	t.Chdir(project)

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(project, ".cursor"))
	if err != nil {
		t.Fatalf("read project .cursor: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("project .cursor entries = %v, want empty", names(entries))
	}
	if _, err := os.Stat(filepath.Join(home, ".cursor", "plugins", "local", "engram", "plugin.json")); err != nil {
		t.Fatalf("user-level plugin missing after project-cwd install: %v", err)
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = entry.Name()
	}
	return out
}

func TestInstallCursorRefreshReplacesOwnedPluginAndPreservesNeighbors(t *testing.T) {
	home := stubCursorInstallEnv(t)
	pluginRoot := filepath.Join(home, ".cursor", "plugins", "local", "engram")
	neighbor := filepath.Join(home, ".cursor", "plugins", "local", "other-plugin", "plugin.json")
	staleSkill := filepath.Join(pluginRoot, "skills", "engram-memory", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(neighbor), 0755); err != nil {
		t.Fatalf("create neighbor plugin: %v", err)
	}
	if err := os.WriteFile(neighbor, []byte(`{"name":"other-plugin"}`), 0644); err != nil {
		t.Fatalf("write neighbor plugin: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(staleSkill), 0755); err != nil {
		t.Fatalf("create stale skill dir: %v", err)
	}
	if err := os.WriteFile(staleSkill, []byte("stale owned skill\n"), 0644); err != nil {
		t.Fatalf("write stale skill: %v", err)
	}

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("first InstallWithOptions(cursor): %v", err)
	}
	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("second InstallWithOptions(cursor): %v", err)
	}

	gotNeighbor, err := os.ReadFile(neighbor)
	if err != nil {
		t.Fatalf("read neighbor plugin: %v", err)
	}
	if string(gotNeighbor) != `{"name":"other-plugin"}` {
		t.Fatalf("neighbor plugin = %s, want unchanged", gotNeighbor)
	}

	skill, err := os.ReadFile(staleSkill)
	if err != nil {
		t.Fatalf("read refreshed skill: %v", err)
	}
	if string(skill) == "stale owned skill\n" {
		t.Fatal("owned plugin skill was not refreshed")
	}
	if !strings.Contains(string(skill), "Terminal Memory commit") {
		t.Fatalf("refreshed skill missing editorial rubric: %s", skill)
	}
}

func TestInstallCursorDevelopmentAllowsUnpinnedIdentity(t *testing.T) {
	home := stubCursorInstallEnv(t)

	if _, err := InstallWithOptions("cursor", InstallOptions{Development: true}); err != nil {
		t.Fatalf("development Cursor setup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, ".cursor", "plugins", "local", "engram", "plugin.json")); err != nil {
		t.Fatalf("development install missing Agent Plugin: %v", err)
	}
}

func TestInstallCursorStableSetupRequiresPinnedReleaseIdentity(t *testing.T) {
	stubCursorInstallEnv(t)

	_, err := InstallWithOptions("cursor", InstallOptions{})
	if err == nil {
		t.Fatal("stable Cursor setup without version and commit succeeded")
	}
	if !strings.Contains(err.Error(), "release identity") {
		t.Fatalf("error = %q, want release identity requirement", err)
	}

	_, err = InstallWithOptions("cursor", InstallOptions{Version: "main", Commit: testReleaseCommit})
	if err == nil {
		t.Fatal("stable Cursor setup accepted moving branch main")
	}
}

func TestInstallCursorPinsReleaseIdentityOnInstalledPlugin(t *testing.T) {
	home := stubCursorInstallEnv(t)

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(home, ".cursor", "plugins", "local", "engram", ".engram-release.json"))
	if err != nil {
		t.Fatalf("read pinned release identity: %v", err)
	}
	if !strings.Contains(string(raw), `"version": "2.2.1"`) {
		t.Fatalf("release identity = %s, want version 2.2.1", raw)
	}
	if !strings.Contains(string(raw), `"commit": "`+testReleaseCommit+`"`) {
		t.Fatalf("release identity = %s, want commit %s", raw, testReleaseCommit)
	}
	if strings.Contains(string(raw), "main") {
		t.Fatalf("release identity tracked main: %s", raw)
	}

	manifest, err := os.ReadFile(filepath.Join(home, ".cursor", "plugins", "local", "engram", "plugin.json"))
	if err != nil {
		t.Fatalf("read installed manifest: %v", err)
	}
	if !strings.Contains(string(manifest), `"version": "2.2.1"`) {
		t.Fatalf("installed plugin.json version = %s, want 2.2.1", manifest)
	}
}

func TestInstallCursorRemovesOwnedNativeMCPAndPreservesOtherServers(t *testing.T) {
	home := stubCursorInstallEnv(t)
	native := filepath.Join(home, ".cursor", "mcp.json")
	if err := os.MkdirAll(filepath.Dir(native), 0755); err != nil {
		t.Fatalf("create cursor config dir: %v", err)
	}
	existing := `{
  "mcpServers": {
    "engram": {
      "command": "/usr/local/bin/engram",
      "args": ["mcp", "--tools=agent"]
    },
    "other": {
      "command": "other"
    }
  }
}`
	if err := os.WriteFile(native, []byte(existing), 0644); err != nil {
		t.Fatalf("write native MCP: %v", err)
	}

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	raw, err := os.ReadFile(native)
	if err != nil {
		t.Fatalf("read native MCP after install: %v", err)
	}
	if strings.Contains(string(raw), `"engram"`) {
		t.Fatalf("owned native engram MCP remained: %s", raw)
	}
	if !strings.Contains(string(raw), `"other"`) {
		t.Fatalf("neighbor MCP server was removed: %s", raw)
	}
}

func TestInstallCursorRemovesNativeMCPFileWhenOnlyOwnedEngramRemains(t *testing.T) {
	home := stubCursorInstallEnv(t)
	native := filepath.Join(home, ".cursor", "mcp.json")
	if err := os.MkdirAll(filepath.Dir(native), 0755); err != nil {
		t.Fatalf("create cursor config dir: %v", err)
	}
	onlyOwned := `{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--tools=agent"]
    }
  }
}`
	if err := os.WriteFile(native, []byte(onlyOwned), 0644); err != nil {
		t.Fatalf("write native MCP: %v", err)
	}

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}
	if _, err := os.Stat(native); !os.IsNotExist(err) {
		t.Fatalf("native MCP file = %v, want removed when only an owned engram entry remained", err)
	}
}

func TestInstallCursorPreservesMalformedNativeMCP(t *testing.T) {
	home := stubCursorInstallEnv(t)
	native := filepath.Join(home, ".cursor", "mcp.json")
	if err := os.MkdirAll(filepath.Dir(native), 0755); err != nil {
		t.Fatalf("create cursor config dir: %v", err)
	}
	if err := os.WriteFile(native, []byte("{not-json"), 0644); err != nil {
		t.Fatalf("write malformed native MCP: %v", err)
	}

	result, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	})
	if err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}
	if !slices.Contains(result.Preserved, "mcpServers.engram") {
		t.Fatalf("result.Preserved = %v, want mcpServers.engram for malformed native MCP", result.Preserved)
	}
	got, err := os.ReadFile(native)
	if err != nil {
		t.Fatalf("read malformed native MCP: %v", err)
	}
	if string(got) != "{not-json" {
		t.Fatalf("malformed native MCP was rewritten: %q", got)
	}
}

func TestInstallCursorPreservesCustomNativeMCP(t *testing.T) {
	home := stubCursorInstallEnv(t)
	native := filepath.Join(home, ".cursor", "mcp.json")
	if err := os.MkdirAll(filepath.Dir(native), 0755); err != nil {
		t.Fatalf("create cursor config dir: %v", err)
	}
	custom := `{
  "mcpServers": {
    "engram": {
      "command": "custom-engram",
      "args": ["mcp", "--tools=all"]
    }
  }
}`
	if err := os.WriteFile(native, []byte(custom), 0644); err != nil {
		t.Fatalf("write custom native MCP: %v", err)
	}

	result, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	})
	if err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}
	if !slices.Contains(result.Preserved, "mcpServers.engram") {
		t.Fatalf("result.Preserved = %v, want mcpServers.engram", result.Preserved)
	}

	raw, err := os.ReadFile(native)
	if err != nil {
		t.Fatalf("read native MCP after install: %v", err)
	}
	if !strings.Contains(string(raw), `"custom-engram"`) || !strings.Contains(string(raw), `"--tools=all"`) {
		t.Fatalf("custom native MCP was rewritten: %s", raw)
	}
}

func TestInstallCursorDoesNotWritePluginOrProjectHooks(t *testing.T) {
	home := stubCursorInstallEnv(t)
	project := t.TempDir()
	if err := os.MkdirAll(filepath.Join(project, ".cursor"), 0755); err != nil {
		t.Fatalf("create project .cursor: %v", err)
	}
	t.Chdir(project)

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	if _, err := os.Stat(filepath.Join(home, ".cursor", "plugins", "local", "engram", "hooks.json")); !os.IsNotExist(err) {
		t.Fatalf("portable plugin bundled hooks.json = %v, want absent", err)
	}
	if _, err := os.Stat(filepath.Join(project, ".cursor", "hooks.json")); !os.IsNotExist(err) {
		t.Fatalf("project hooks.json = %v, want absent", err)
	}
}

func TestInstallCursorWritesUserHooksForCueAndStop(t *testing.T) {
	home := stubCursorInstallEnv(t)

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(home, ".cursor", "hooks.json"))
	if err != nil {
		t.Fatalf("read user Cursor hooks: %v", err)
	}
	pluginBin := filepath.Join(home, ".cursor", "plugins", "local", "engram", "bin", "engram")
	pluginRoot := filepath.Join(home, ".cursor", "plugins", "local", "engram")
	if !strings.Contains(string(raw), `"version": 1`) {
		t.Fatalf("hooks.json = %s, want version 1", raw)
	}
	if !strings.Contains(string(raw), `"sessionStart"`) {
		t.Fatalf("hooks.json missing sessionStart: %s", raw)
	}
	if !strings.Contains(string(raw), `"stop"`) {
		t.Fatalf("hooks.json missing stop: %s", raw)
	}
	if !strings.Contains(string(raw), "lifecycle session-start --host=cursor") {
		t.Fatalf("sessionStart hook is not the Cursor lifecycle adapter: %s", raw)
	}
	if !strings.Contains(string(raw), "checkpoint verify-stop --host=cursor") {
		t.Fatalf("stop hook is not the Cursor checkpoint verifier: %s", raw)
	}
	if !strings.Contains(string(raw), pluginBin) {
		t.Fatalf("hooks.json = %s, want plugin binary %s", raw, pluginBin)
	}
	if !strings.Contains(string(raw), "--plugin-root="+pluginRoot) && !strings.Contains(string(raw), `--plugin-root="`+pluginRoot+`"`) {
		t.Fatalf("hooks.json = %s, want plugin-root %s", raw, pluginRoot)
	}
	if !strings.Contains(string(raw), `"loop_limit": 1`) {
		t.Fatalf("stop hook = %s, want one recovery follow-up", raw)
	}
}

func TestInstallCursorLeavesPromptAndSubagentCaptureOff(t *testing.T) {
	home := stubCursorInstallEnv(t)

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(home, ".cursor", "hooks.json"))
	if err != nil {
		t.Fatalf("read user Cursor hooks: %v", err)
	}
	for _, forbidden := range []string{
		"beforeSubmitPrompt",
		"subagentStart",
		"subagentStop",
		"capture prompt",
		"capture subagent",
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("hooks.json enabled capture surface %q: %s", forbidden, raw)
		}
	}
}

func TestInstallCursorRefreshUpdatesOwnedHooksAndPreservesUserHooks(t *testing.T) {
	home := stubCursorInstallEnv(t)
	hooksPath := filepath.Join(home, ".cursor", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(hooksPath), 0755); err != nil {
		t.Fatalf("create cursor config dir: %v", err)
	}
	existing := `{
  "version": 1,
  "hooks": {
    "sessionStart": [
      {
        "command": "echo user-owned-session-start"
      }
    ],
    "stop": [
      {
        "command": "/old/engram checkpoint verify-stop --host=cursor",
        "loop_limit": 9
      }
    ],
    "beforeShellExecution": [
      {
        "command": "echo user-owned-shell"
      }
    ]
  }
}`
	if err := os.WriteFile(hooksPath, []byte(existing), 0644); err != nil {
		t.Fatalf("write existing hooks: %v", err)
	}

	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}

	raw, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatalf("read refreshed hooks: %v", err)
	}
	if !strings.Contains(string(raw), "echo user-owned-session-start") {
		t.Fatalf("user sessionStart hook was removed: %s", raw)
	}
	if !strings.Contains(string(raw), "echo user-owned-shell") {
		t.Fatalf("user-owned hook event was removed: %s", raw)
	}
	if strings.Contains(string(raw), "/old/engram checkpoint verify-stop --host=cursor") {
		t.Fatalf("owned stop hook was not refreshed: %s", raw)
	}
	if !strings.Contains(string(raw), "lifecycle session-start --host=cursor") {
		t.Fatalf("owned sessionStart hook was not written: %s", raw)
	}
	if !strings.Contains(string(raw), "checkpoint verify-stop --host=cursor") {
		t.Fatalf("owned stop hook was not written: %s", raw)
	}
	if strings.Contains(string(raw), `"loop_limit": 9`) {
		t.Fatalf("stale owned loop_limit remained: %s", raw)
	}
}

func TestInstallCursorPreservesMalformedUserHooks(t *testing.T) {
	home := stubCursorInstallEnv(t)
	hooksPath := filepath.Join(home, ".cursor", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(hooksPath), 0755); err != nil {
		t.Fatalf("create cursor config dir: %v", err)
	}
	if err := os.WriteFile(hooksPath, []byte("{not-json"), 0644); err != nil {
		t.Fatalf("write malformed hooks: %v", err)
	}

	result, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	})
	if err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}
	if !slices.Contains(result.Preserved, "hooks") {
		t.Fatalf("result.Preserved = %v, want hooks for malformed user hooks", result.Preserved)
	}
	got, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatalf("read malformed hooks: %v", err)
	}
	if string(got) != "{not-json" {
		t.Fatalf("malformed user hooks were rewritten: %q", got)
	}
}

func TestRewriteCursorPluginMCPCommandRejectsMissingServer(t *testing.T) {
	resetSetupSeams(t)
	dest := t.TempDir()
	path := filepath.Join(dest, "mcp.json")
	if err := os.WriteFile(path, []byte(`{"$schema":"https://agent-plugins.org/schemas/1.0.0/mcp.schema.json","mcpServers":{}}`), 0644); err != nil {
		t.Fatalf("write incomplete plugin MCP: %v", err)
	}

	err := rewriteCursorPluginMCPCommand(dest)
	if err == nil || !strings.Contains(err.Error(), "missing the engram server") {
		t.Fatalf("rewriteCursorPluginMCPCommand = %v, want missing engram server", err)
	}
}

func TestEmbeddedCursorAgentPluginMatchesSource(t *testing.T) {
	for _, rel := range []string{
		"plugin.json",
		"mcp.json",
		filepath.Join("skills", "engram-memory", "SKILL.md"),
	} {
		source, err := os.ReadFile(filepath.Join("..", "..", "plugin", "engram", rel))
		if err != nil {
			t.Fatalf("read source Agent Plugin %s: %v", rel, err)
		}
		embedded, err := os.ReadFile(filepath.Join("plugins", "engram", rel))
		if err != nil {
			t.Fatalf("read embedded Agent Plugin %s: %v", rel, err)
		}
		if !bytes.Equal(source, embedded) {
			t.Fatalf("embedded Agent Plugin %s drifted from plugin/engram; regenerate the embedded copy", rel)
		}
	}
}
