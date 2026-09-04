package setup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestInspectCursorStatusEmptyProfileIsConservativeAndReadOnly(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	cwd := filepath.Join(home, "workspace")
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	before := snapshotStatusTestTree(t, home)

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, cwd)
	if err != nil {
		t.Fatalf("inspect empty Cursor profile: %v", err)
	}
	if status.SchemaVersion != CursorIntegrationStatusSchemaVersion || status.Agent != "cursor" || status.Mode != CursorModeUnknown {
		t.Fatalf("status header = %#v", status)
	}

	want := []struct {
		capability string
		status     CursorCheckStatus
		reason     string
	}{
		{"plugin", CursorCheckMissing, "plugin_missing"},
		{"skill", CursorCheckMissing, "skill_missing"},
		{"mcp", CursorCheckMissing, "mcp_missing"},
		{"hooks", CursorCheckMissing, "hooks_missing"},
		{"user_rules", CursorCheckUnknown, "user_rules_unknown"},
	}
	if len(status.Checks) != len(want) {
		t.Fatalf("checks = %#v, want %d", status.Checks, len(want))
	}
	for i, expected := range want {
		got := status.Checks[i]
		if got.Capability != expected.capability || got.Status != expected.status || got.ReasonCode != expected.reason {
			t.Fatalf("check[%d] = %#v, want capability=%q status=%q reason=%q", i, got, expected.capability, expected.status, expected.reason)
		}
		if got.Reason == "" || got.Evidence == nil {
			t.Fatalf("check[%d] missing reason or evidence: %#v", i, got)
		}
	}

	after := snapshotStatusTestTree(t, home)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("empty Cursor status mutated the home tree")
	}
}

func TestInspectCursorStatusCompleteInstallIsCheckpointReady(t *testing.T) {
	home := installPinnedCursor(t)
	cwd := filepath.Join(home, "workspace")
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, cwd)
	if err != nil {
		t.Fatalf("inspect installed Cursor profile: %v", err)
	}
	if status.Agent != "cursor" || status.Mode != CursorModeCheckpointReady {
		t.Fatalf("status header = %#v", status)
	}

	plugin := cursorCheck(t, status, "plugin")
	if plugin.Status != CursorCheckReady || plugin.ReasonCode != "plugin_ready" {
		t.Fatalf("plugin = %#v", plugin)
	}
	skill := cursorCheck(t, status, "skill")
	if skill.Status != CursorCheckReady || skill.ReasonCode != "skill_ready" {
		t.Fatalf("skill = %#v", skill)
	}
	mcp := cursorCheck(t, status, "mcp")
	if mcp.Status != CursorCheckReady || mcp.ReasonCode != "mcp_ready" || cursorEvidenceValue(mcp, "source") != "plugin" {
		t.Fatalf("mcp = %#v", mcp)
	}
	hooks := cursorCheck(t, status, "hooks")
	if hooks.Status != CursorCheckReady || hooks.ReasonCode != "hooks_ready" {
		t.Fatalf("hooks = %#v", hooks)
	}
	userRules := cursorCheck(t, status, "user_rules")
	if userRules.Status != CursorCheckUnknown || userRules.ReasonCode != "user_rules_unknown" {
		t.Fatalf("user_rules = %#v, want unknown", userRules)
	}
}

func TestInspectCursorStatusMCPOnlyIsNotCheckpointReady(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	writeCursorStatusFile(t, filepath.Join(home, ".cursor", "mcp.json"), `{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--tools=agent"]
    }
  }
}`)

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect MCP-only Cursor profile: %v", err)
	}
	if status.Mode == CursorModeCheckpointReady {
		t.Fatalf("MCP-only profile claimed checkpoint readiness: %#v", status)
	}
	if status.Mode != CursorModeMCPOnly {
		t.Fatalf("mode = %q, want mcp_only", status.Mode)
	}
	if got := cursorCheck(t, status, "plugin"); got.Status != CursorCheckMissing {
		t.Fatalf("plugin = %#v, want missing", got)
	}
	if got := cursorCheck(t, status, "skill"); got.Status != CursorCheckMissing {
		t.Fatalf("skill = %#v, want missing", got)
	}
	mcp := cursorCheck(t, status, "mcp")
	if mcp.Status != CursorCheckReady || cursorEvidenceValue(mcp, "source") != "native" {
		t.Fatalf("mcp = %#v, want ready native-only", mcp)
	}
	if got := cursorCheck(t, status, "hooks"); got.Status != CursorCheckMissing {
		t.Fatalf("hooks = %#v, want missing", got)
	}
	if got := cursorCheck(t, status, "user_rules"); got.Status != CursorCheckUnknown {
		t.Fatalf("user_rules = %#v, want unknown", got)
	}
}

func TestInspectCursorStatusDistinguishesStalePluginAndSkill(t *testing.T) {
	home := installPinnedCursor(t)
	pluginRoot := filepath.Join(home, ".cursor", "plugins", "local", "engram")
	writeCursorStatusFile(t, filepath.Join(pluginRoot, ".engram-release.json"), `{
  "version": "1.0.0",
  "commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}`)

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect stale plugin: %v", err)
	}
	plugin := cursorCheck(t, status, "plugin")
	if plugin.Status != CursorCheckStale || plugin.ReasonCode != "plugin_stale" {
		t.Fatalf("stale plugin = %#v", plugin)
	}
	if status.Mode == CursorModeCheckpointReady {
		t.Fatalf("stale plugin claimed checkpoint readiness: %#v", status)
	}

	home = installPinnedCursor(t)
	writeCursorStatusFile(t, filepath.Join(home, ".cursor", "plugins", "local", "engram", "skills", "engram-memory", "SKILL.md"), `---
name: engram-memory
version: "0.0.0"
---
stale rubric
`)
	status, err = InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect stale skill: %v", err)
	}
	skill := cursorCheck(t, status, "skill")
	if skill.Status != CursorCheckStale || skill.ReasonCode != "skill_stale" {
		t.Fatalf("stale skill = %#v", skill)
	}
}

func TestInspectCursorStatusDistinguishesCustomizedMCPAndHooks(t *testing.T) {
	home := installPinnedCursor(t)
	writeCursorStatusFile(t, filepath.Join(home, ".cursor", "plugins", "local", "engram", "mcp.json"), `{
  "mcpServers": {
    "engram": {
      "type": "stdio",
      "command": "custom-engram",
      "args": ["mcp", "--tools=all"]
    }
  }
}`)

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect customized MCP: %v", err)
	}
	mcp := cursorCheck(t, status, "mcp")
	if mcp.Status != CursorCheckCustomized || mcp.ReasonCode != "mcp_customized" {
		t.Fatalf("customized mcp = %#v", mcp)
	}

	home = installPinnedCursor(t)
	writeCursorStatusFile(t, filepath.Join(home, ".cursor", "hooks.json"), `{
  "version": 1,
  "hooks": {
    "sessionStart": [{"command": "/elsewhere/engram lifecycle session-start --host=cursor"}],
    "stop": [{"command": "/elsewhere/engram checkpoint verify-stop --host=cursor", "loop_limit": 1}]
  }
}`)
	status, err = InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect customized hooks: %v", err)
	}
	hooks := cursorCheck(t, status, "hooks")
	if hooks.Status != CursorCheckCustomized || hooks.ReasonCode != "hooks_customized" {
		t.Fatalf("customized hooks = %#v", hooks)
	}
	if status.Mode == CursorModeCheckpointReady {
		t.Fatalf("customized hooks claimed checkpoint readiness: %#v", status)
	}
}

func TestInspectCursorStatusReportsLeftoverUserSkillAsCustomized(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	writeCursorStatusFile(t, filepath.Join(home, ".agents", "skills", "engram-memory-cli", "SKILL.md"), `---
name: engram-memory-cli
---
retired user skill
`)

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect leftover user skill: %v", err)
	}
	pluginSkill := cursorCheckByReason(t, status, "skill", "skill_missing")
	if pluginSkill.Status != CursorCheckMissing {
		t.Fatalf("canonical skill = %#v, want missing", pluginSkill)
	}
	leftover := cursorCheckByReason(t, status, "skill", "skill_customized")
	if leftover.Status != CursorCheckCustomized || cursorEvidenceValue(leftover, "source") != "user" {
		t.Fatalf("leftover skill = %#v", leftover)
	}
	if !strings.Contains(leftover.Reason, "engram setup") {
		t.Fatalf("leftover skill reason = %q, want migration via engram setup", leftover.Reason)
	}
	if status.Mode == CursorModeCheckpointReady {
		t.Fatalf("leftover user skill claimed checkpoint readiness: %#v", status)
	}
}

func TestInspectCursorStatusReportsLeftoverUserSkillBesideInstalledPlugin(t *testing.T) {
	home := installPinnedCursor(t)
	writeCursorStatusFile(t, filepath.Join(home, ".agents", "skills", "engram-memory-cli", "SKILL.md"), `---
name: engram-memory-cli
---
retired user skill
`)

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect leftover skill beside plugin: %v", err)
	}
	if cursorCheckByReason(t, status, "skill", "skill_ready").Status != CursorCheckReady {
		t.Fatal("canonical plugin skill was replaced by leftover user skill")
	}
	leftover := cursorCheckByReason(t, status, "skill", "skill_customized")
	if leftover.Status != CursorCheckCustomized || cursorEvidenceValue(leftover, "source") != "user" {
		t.Fatalf("leftover skill = %#v", leftover)
	}
	if status.Mode != CursorModeCheckpointReady {
		t.Fatalf("mode = %q, want checkpoint_ready when leftover is extra evidence", status.Mode)
	}
}

func TestInspectCursorStatusDistinguishesCustomizedAndIncompletePlugin(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	pluginRoot := filepath.Join(home, ".cursor", "plugins", "local", "engram")

	writeCursorStatusFile(t, filepath.Join(pluginRoot, "plugin.json"), `{"name":"other-plugin"}`)
	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect non-engram plugin: %v", err)
	}
	plugin := cursorCheck(t, status, "plugin")
	if plugin.Status != CursorCheckCustomized || plugin.ReasonCode != "plugin_customized" {
		t.Fatalf("foreign plugin = %#v", plugin)
	}

	writeCursorStatusFile(t, filepath.Join(pluginRoot, "plugin.json"), `{"name":"engram"}`)
	status, err = InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect plugin without identity: %v", err)
	}
	plugin = cursorCheck(t, status, "plugin")
	if plugin.Status != CursorCheckCustomized || plugin.ReasonCode != "plugin_customized" {
		t.Fatalf("plugin without identity = %#v", plugin)
	}

	writeCursorStatusFile(t, filepath.Join(pluginRoot, ".engram-release.json"), `{not-json`)
	status, err = InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect invalid identity: %v", err)
	}
	plugin = cursorCheck(t, status, "plugin")
	if plugin.Status != CursorCheckCustomized || plugin.ReasonCode != "plugin_customized" {
		t.Fatalf("invalid identity = %#v", plugin)
	}

	writeCursorStatusFile(t, filepath.Join(pluginRoot, ".engram-release.json"), `{
  "version": "2.2.1",
  "commit": "`+testReleaseCommit+`"
}`)
	status, err = InspectCursorStatus("2.2.1", testReleaseCommit, home)
	if err != nil {
		t.Fatalf("inspect plugin without binary: %v", err)
	}
	plugin = cursorCheck(t, status, "plugin")
	if plugin.Status != CursorCheckCustomized || plugin.ReasonCode != "plugin_customized" {
		t.Fatalf("plugin without binary = %#v", plugin)
	}
}

func TestInspectCursorStatusDoesNotReadCapturedContent(t *testing.T) {
	home := installPinnedCursor(t)
	secret := "CAPTURED-PROMPT-SECRET-138"
	for _, rel := range []string{
		filepath.Join(".engram", "captures", "prompt.json"),
		filepath.Join(".cursor", "projects", "secret", "agent-transcripts", "prompt.md"),
		filepath.Join("workspace", ".engram", "captures", "subagent.json"),
	} {
		writeCursorStatusFile(t, filepath.Join(home, rel), secret)
	}
	dataDir := filepath.Join(home, ".engram")
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	readPaths := make([]string, 0, 8)
	originalRead := readFileFn
	readFileFn = func(path string) ([]byte, error) {
		readPaths = append(readPaths, path)
		if strings.Contains(path, "captures") || strings.Contains(path, "transcript") || strings.Contains(path, secret) {
			t.Fatalf("status read captured content path %s", path)
		}
		return originalRead(path)
	}

	status, err := InspectCursorStatus("2.2.1", testReleaseCommit, filepath.Join(home, "workspace"))
	if err != nil {
		t.Fatalf("inspect Cursor profile with planted captures: %v", err)
	}
	encoded := mustMarshalCursorStatus(t, status)
	if strings.Contains(encoded, secret) {
		t.Fatalf("status leaked captured content: %s", encoded)
	}
	for _, path := range readPaths {
		if strings.Contains(path, dataDir) && strings.Contains(path, "captures") {
			t.Fatalf("status opened capture store path %s", path)
		}
	}
}

func installPinnedCursor(t *testing.T) string {
	t.Helper()
	home := stubCursorInstallEnv(t)
	if _, err := InstallWithOptions("cursor", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	}); err != nil {
		t.Fatalf("InstallWithOptions(cursor): %v", err)
	}
	return home
}

func cursorCheck(t *testing.T, status CursorIntegrationStatus, capability string) CursorIntegrationCheck {
	t.Helper()
	var matches []CursorIntegrationCheck
	for _, check := range status.Checks {
		if check.Capability == capability {
			matches = append(matches, check)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("capability %q checks = %#v, want exactly one", capability, matches)
	}
	return matches[0]
}

func cursorCheckByReason(t *testing.T, status CursorIntegrationStatus, capability, reasonCode string) CursorIntegrationCheck {
	t.Helper()
	for _, check := range status.Checks {
		if check.Capability == capability && check.ReasonCode == reasonCode {
			return check
		}
	}
	t.Fatalf("missing capability %q reason %q in %#v", capability, reasonCode, status.Checks)
	return CursorIntegrationCheck{}
}

func cursorEvidenceValue(check CursorIntegrationCheck, name string) string {
	for _, evidence := range check.Evidence {
		if evidence.Name == name {
			return evidence.Value
		}
	}
	return ""
}

func writeCursorStatusFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create %s parent: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustMarshalCursorStatus(t *testing.T, status CursorIntegrationStatus) string {
	t.Helper()
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal Cursor status: %v", err)
	}
	return string(raw)
}
