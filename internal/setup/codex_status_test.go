package setup

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/protocolcontract"
)

func TestInspectCodexStatusEmptyProfileIsConservativeAndReadOnly(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	cwd := filepath.Join(home, "workspace")
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	before := snapshotStatusTestTree(t, home)

	osExecutable = func() (string, error) { return "/opt/engram/bin/engram", nil }
	lookPathFn = func(name string) (string, error) {
		if name != "codex" {
			t.Fatalf("unexpected executable lookup %q", name)
		}
		return "", errors.New("not found")
	}
	runCommand = func(string, ...string) ([]byte, error) {
		t.Fatal("empty profile must not execute Codex commands")
		return nil, nil
	}

	status, err := InspectCodexStatus("2.2.1", cwd)
	if err != nil {
		t.Fatalf("inspect empty Codex profile: %v", err)
	}
	if status.SchemaVersion != CodexIntegrationStatusSchemaVersion || status.Agent != "codex" || status.Mode != CodexModeUnknown {
		t.Fatalf("status header = %#v", status)
	}

	want := []struct {
		capability string
		status     CodexIntegrationCheckStatus
		reason     string
	}{
		{"engram_cli", CodexCheckReady, "engram_cli_available"},
		{"codex_cli", CodexCheckMissing, "codex_cli_missing"},
		{"skill", CodexCheckMissing, "engram_skill_missing"},
		{"marketplace", CodexCheckMissing, "marketplace_missing"},
		{"plugin", CodexCheckMissing, "plugin_missing"},
		{"mcp_configuration", CodexCheckMissing, "mcp_configuration_missing"},
		{"mcp_readiness", CodexCheckMissing, "mcp_not_configured"},
		{"prompt_hook", CodexCheckMissing, "plugin_missing"},
		{"session_hook", CodexCheckMissing, "plugin_missing"},
		{"activation_cue", CodexCheckMissing, "plugin_missing"},
		{"stop_verifier", CodexCheckMissing, "plugin_missing"},
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
			t.Fatalf("check[%d] lacks bounded reason/evidence: %#v", i, got)
		}
	}
	if got := status.Checks[0].Evidence; !reflect.DeepEqual(got, []CodexIntegrationEvidence{{Name: "path", Value: "/opt/engram/bin/engram"}, {Name: "version", Value: "2.2.1"}}) {
		t.Fatalf("Engram CLI evidence = %#v", got)
	}

	after := snapshotStatusTestTree(t, home)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("status inspection mutated isolated profile:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestInspectCodexStatusDiscoversRepositoryAndUserSkills(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	codexAdminSkillsDirFn = func() string { return filepath.Join(home, "admin-skills") }

	repo := filepath.Join(home, "workspace")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatalf("create repository marker: %v", err)
	}
	repoSkillPath := filepath.Join(repo, ".agents", "skills", "engram-project-memory", "SKILL.md")
	writeStatusTestFile(t, repoSkillPath, "---\nname: engram-memory-project\ndescription: Project memory.\n---\nRepo skill.\n")
	userSkillPath := filepath.Join(home, ".agents", "skills", "engram-memory-cli", "SKILL.md")
	writeStatusTestFile(t, userSkillPath, "---\nname: engram-memory-cli\ndescription: Recall Engram memory.\nmetadata:\n  version: \"1.2.3\"\n---\nUser skill.\n")

	osExecutable = func() (string, error) { return "/opt/engram/bin/engram", nil }
	lookPathFn = func(name string) (string, error) {
		if name == "codex" {
			return "/opt/codex/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(name string, args ...string) ([]byte, error) {
		if name != "/opt/codex/bin/codex" || !slices.Equal(args, []string{"--version"}) {
			t.Fatalf("unexpected status command: %s %v", name, args)
		}
		return []byte("codex-cli 0.150.1\n"), nil
	}

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect skill+CLI profile: %v", err)
	}
	if status.Mode != CodexModeManualSkillCLI {
		t.Fatalf("mode = %q, want %q", status.Mode, CodexModeManualSkillCLI)
	}

	codexCLI := statusChecksByCapability(status.Checks, "codex_cli")
	if len(codexCLI) != 1 || codexCLI[0].Status != CodexCheckReady || evidenceValue(codexCLI[0], "version") != "codex-cli 0.150.1" {
		t.Fatalf("Codex CLI check = %#v", codexCLI)
	}

	skills := statusChecksByCapability(status.Checks, "skill")
	if len(skills) != 2 {
		t.Fatalf("skill checks = %#v, want repo and user skills", skills)
	}
	resolvedRepoSkillPath, err := filepath.EvalSymlinks(repoSkillPath)
	if err != nil {
		t.Fatalf("resolve repository skill path: %v", err)
	}
	resolvedUserSkillPath, err := filepath.EvalSymlinks(userSkillPath)
	if err != nil {
		t.Fatalf("resolve user skill path: %v", err)
	}
	if evidenceValue(skills[0], "scope") != "repo" || evidenceValue(skills[0], "path") != resolvedRepoSkillPath {
		t.Fatalf("repository skill = %#v", skills[0])
	}
	if evidenceValue(skills[1], "scope") != "user" || evidenceValue(skills[1], "path") != resolvedUserSkillPath {
		t.Fatalf("user skill = %#v", skills[1])
	}
	if got := evidenceValue(skills[1], "sha256"); got != "016e3f450e5d6b4bf81c9d57385708f2650b34feb65e7afa16dc687585126d3b" {
		t.Fatalf("user skill SHA-256 = %q", got)
	}
	if got := evidenceValue(skills[1], "version"); got != "1.2.3" {
		t.Fatalf("user skill version = %q", got)
	}
}

func TestInspectCodexStatusCompleteSupportedPluginIsCheckpointReady(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	codexAdminSkillsDirFn = func() string { return filepath.Join(home, "admin-skills") }
	repo := filepath.Join(home, "workspace")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatalf("create repository marker: %v", err)
	}
	installCurrentManagedPackStatusFixture(t, home)

	const engramPath = "/opt/engram/bin/engram"
	osExecutable = func() (string, error) { return engramPath, nil }
	configPath := filepath.Join(home, ".codex", "config.toml")
	writeStatusTestFile(t, configPath, `[marketplaces.engram]
source_type = "git"
source = "https://github.com/yersonargotev/engram.git"
ref = "v2.2.1"

[plugins."engram@engram"]
enabled = true

[mcp_servers.engram]
command = "/opt/engram/bin/engram"
args = ["mcp", "--tools=agent"]
`)

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	writeCanonicalCodexActivationFixture(t, filepath.Join(marketplaceRoot, "plugin", "codex"))
	installedPath := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "0.1.7")
	writeCanonicalCodexActivationFixture(t, installedPath)

	lookPathFn = func(name string) (string, error) {
		switch name {
		case "codex":
			return "/opt/codex/bin/codex", nil
		case engramPath:
			return engramPath, nil
		default:
			return "", errors.New("not found")
		}
	}
	var commands [][]string
	runCommand = func(name string, args ...string) ([]byte, error) {
		if name != "/opt/codex/bin/codex" {
			t.Fatalf("unexpected command executable %q", name)
		}
		commands = append(commands, append([]string(nil), args...))
		switch {
		case slices.Equal(args, []string{"--version"}):
			return []byte("codex-cli 0.150.1\n"), nil
		case slices.Equal(args, []string{"plugin", "list", "--json"}):
			return []byte(fmt.Sprintf(`{"installed":[{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.7","installed":true,"enabled":true,"source":{"source":"local","path":%q},"marketplaceSource":{"sourceType":"git","source":"https://github.com/yersonargotev/engram.git"}}],"available":[]}`, filepath.Join(marketplaceRoot, "plugin", "codex"))), nil
		default:
			return nil, fmt.Errorf("unexpected Codex command: %v", args)
		}
	}
	probeCalls := 0
	runCodexCheckpointProbeFn = func(name string, args ...string) ([]byte, error) {
		probeCalls++
		if name != engramPath || !slices.Equal(args, []string{"checkpoint", "--help"}) {
			return nil, fmt.Errorf("unexpected checkpoint probe: %s %v", name, args)
		}
		return []byte("engram checkpoint record\nengram checkpoint status\nengram checkpoint verify-stop\n"), nil
	}

	before := snapshotStatusTestTree(t, home)
	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect complete Codex integration: %v", err)
	}
	if status.Mode != CodexModeCheckpointReady {
		t.Fatalf("mode = %q, checks=%#v", status.Mode, status.Checks)
	}
	if status.Compatibility.Status != protocolcontract.CompatibilityReady ||
		status.Compatibility.ReasonCode != protocolcontract.ReasonLegacyCompatible ||
		status.Compatibility.Intersection == nil || len(status.Compatibility.Axes) != 4 {
		t.Fatalf("Protocol compatibility = %#v", status.Compatibility)
	}
	for _, capability := range []string{"marketplace", "plugin", "mcp_configuration", "mcp_readiness", "prompt_hook", "session_hook", "activation_cue", "stop_verifier"} {
		matches := statusChecksByCapability(status.Checks, capability)
		if len(matches) != 1 || matches[0].Status != CodexCheckReady {
			t.Fatalf("%s check = %#v", capability, matches)
		}
	}
	plugin := statusChecksByCapability(status.Checks, "plugin")[0]
	if evidenceValue(plugin, "installed_version") != "0.1.7" || evidenceValue(plugin, "installed_revision") != testReleaseCommit || evidenceValue(plugin, "enabled") != "true" {
		t.Fatalf("plugin provenance = %#v", plugin.Evidence)
	}
	pluginSkills := statusChecksByCapability(status.Checks, "skill")
	if len(pluginSkills) != 2 || evidenceValue(pluginSkills[1], "source") != "plugin" || evidenceValue(pluginSkills[1], "name") != "engram-memory" {
		t.Fatalf("plugin skill checks = %#v", pluginSkills)
	}
	if probeCalls != 1 {
		t.Fatalf("checkpoint probe calls = %d, want 1", probeCalls)
	}
	if !reflect.DeepEqual(commands, [][]string{{"--version"}, {"plugin", "list", "--json"}}) {
		t.Fatalf("Codex commands = %#v", commands)
	}
	after := snapshotStatusTestTree(t, home)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("complete status inspection mutated profile:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestCodexActivationCueDoesNotDependOnSessionEndHook(t *testing.T) {
	resetSetupSeams(t)
	installedPath := t.TempDir()
	writeCanonicalCodexActivationFixture(t, installedPath)
	hooksPath := filepath.Join(installedPath, "hooks", "hooks.json")
	hooksRaw, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatalf("read canonical hooks: %v", err)
	}
	var manifest codexActivationHooksManifest
	if err := json.Unmarshal(hooksRaw, &manifest); err != nil {
		t.Fatalf("decode canonical hooks: %v", err)
	}
	delete(manifest.Hooks, "SessionEnd")
	hooksWithoutSessionEnd, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode hooks without SessionEnd: %v", err)
	}

	if !verifyInstalledCodexActivation(installedPath, hooksWithoutSessionEnd) {
		t.Fatal("model-visible activation cue should depend on the canonical skill and SessionStart projection, not SessionEnd")
	}
	if verifyInstalledCodexSessionHooks(installedPath, hooksWithoutSessionEnd) {
		t.Fatal("session hook readiness should require the independent SessionEnd contract")
	}
}

func TestInspectCodexStatusMCPOnlySeparatesConfigurationFromReadiness(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	codexAdminSkillsDirFn = func() string { return filepath.Join(home, "admin-skills") }
	repo := filepath.Join(home, "workspace")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatalf("create repository marker: %v", err)
	}
	const engramPath = "/opt/engram/bin/engram"
	osExecutable = func() (string, error) { return engramPath, nil }
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[mcp_servers.engram]
command = "/opt/engram/bin/engram"
args = ["mcp", "--tools=agent"]
`)
	lookPathFn = func(name string) (string, error) {
		switch name {
		case "codex":
			return "/opt/codex/bin/codex", nil
		case engramPath:
			return engramPath, nil
		default:
			return "", errors.New("not found")
		}
	}
	runCommand = func(name string, args ...string) ([]byte, error) {
		if name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"--version"}) {
			return []byte("codex-cli 0.150.1\n"), nil
		}
		t.Fatalf("MCP-only status executed unexpected command: %s %v", name, args)
		return nil, nil
	}

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect MCP-only profile: %v", err)
	}
	if status.Mode != CodexModeMCPOnly {
		t.Fatalf("mode = %q, checks=%#v", status.Mode, status.Checks)
	}
	configuration := statusChecksByCapability(status.Checks, "mcp_configuration")[0]
	readiness := statusChecksByCapability(status.Checks, "mcp_readiness")[0]
	if configuration.Status != CodexCheckReady || configuration.ReasonCode != "mcp_configuration_ready" {
		t.Fatalf("MCP configuration = %#v", configuration)
	}
	if readiness.Status != CodexCheckReady || readiness.ReasonCode != "mcp_adapter_ready" {
		t.Fatalf("MCP readiness = %#v", readiness)
	}
	if statusChecksByCapability(status.Checks, "plugin")[0].Status != CodexCheckMissing {
		t.Fatalf("MCP-only profile reported plugin ready: %#v", status.Checks)
	}
}

func TestInspectCodexStatusStaleMCPConfigurationIsNotReady(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	codexAdminSkillsDirFn = func() string { return filepath.Join(home, "admin-skills") }
	repo := filepath.Join(home, "workspace")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatalf("create repository marker: %v", err)
	}
	osExecutable = func() (string, error) { return "/opt/engram/bin/engram", nil }
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[mcp_servers.engram]
command = "/missing/engram"
args = ["mcp", "--tools=agent"]
`)
	lookPathFn = func(name string) (string, error) {
		if name == "codex" {
			return "/opt/codex/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(name string, args ...string) ([]byte, error) {
		if name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"--version"}) {
			return []byte("codex-cli 0.150.1\n"), nil
		}
		t.Fatalf("stale MCP status executed unexpected command: %s %v", name, args)
		return nil, nil
	}
	runCodexCheckpointProbeFn = func(string, ...string) ([]byte, error) {
		t.Fatal("missing MCP executable must not be probed")
		return nil, nil
	}

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect stale MCP profile: %v", err)
	}
	configuration := statusChecksByCapability(status.Checks, "mcp_configuration")[0]
	readiness := statusChecksByCapability(status.Checks, "mcp_readiness")[0]
	if configuration.Status != CodexCheckReady {
		t.Fatalf("stale MCP configuration should remain attributable: %#v", configuration)
	}
	if readiness.Status != CodexCheckUnavailable || readiness.ReasonCode != "mcp_executable_missing" {
		t.Fatalf("stale MCP readiness = %#v", readiness)
	}
	if status.Mode == CodexModeMCPOnly || status.Mode == CodexModeCheckpointReady {
		t.Fatalf("stale MCP configuration overstated mode %q", status.Mode)
	}
}

func TestInspectCodexStatusMarketplaceOnlyDoesNotClaimPluginInstallation(t *testing.T) {
	home, repo := prepareBasicCodexStatusProfile(t)
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[marketplaces.engram]
source_type = "git"
source = "https://github.com/yersonargotev/engram.git"
ref = "v2.2.1"
`)

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect marketplace-only profile: %v", err)
	}
	marketplace := statusChecksByCapability(status.Checks, "marketplace")[0]
	plugin := statusChecksByCapability(status.Checks, "plugin")[0]
	if marketplace.Status != CodexCheckReady || plugin.Status != CodexCheckMissing {
		t.Fatalf("marketplace/plugin checks = %#v / %#v", marketplace, plugin)
	}
	if status.Mode != CodexModeUnknown {
		t.Fatalf("marketplace-only mode = %q, want %q", status.Mode, CodexModeUnknown)
	}
}

func TestInspectCodexStatusPartialPluginRemainsPartial(t *testing.T) {
	home, repo := prepareBasicCodexStatusProfile(t)
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[marketplaces.engram]
source_type = "git"
source = "https://github.com/yersonargotev/engram.git"
ref = "v2.2.1"

[plugins."engram@engram"]
enabled = true
`)
	runCommand = func(name string, args ...string) ([]byte, error) {
		switch {
		case name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"--version"}):
			return []byte("codex-cli 0.150.1\n"), nil
		case name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"plugin", "list", "--json"}):
			return []byte(`{"installed":[]}`), nil
		default:
			t.Fatalf("partial profile executed unexpected command: %s %v", name, args)
			return nil, nil
		}
	}

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect partial plugin profile: %v", err)
	}
	plugin := statusChecksByCapability(status.Checks, "plugin")[0]
	if plugin.Status != CodexCheckPartial || plugin.ReasonCode != "plugin_not_listed" {
		t.Fatalf("partial plugin check = %#v", plugin)
	}
	if evidenceValue(plugin, "configured_enabled") != "true" {
		t.Fatalf("partial plugin enablement evidence = %#v", plugin.Evidence)
	}
	if status.Mode != CodexModePartialPlugin {
		t.Fatalf("partial plugin mode = %q, want %q", status.Mode, CodexModePartialPlugin)
	}
}

func TestInspectCodexStatusDisabledUnlistedPluginReportsConfiguredState(t *testing.T) {
	home, repo := prepareBasicCodexStatusProfile(t)
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[marketplaces.engram]
source_type = "git"
source = "https://github.com/yersonargotev/engram.git"
ref = "v2.2.1"

[plugins."engram@engram"]
enabled = false
`)
	runCommand = func(name string, args ...string) ([]byte, error) {
		switch {
		case name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"--version"}):
			return []byte("codex-cli 0.150.1\n"), nil
		case name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"plugin", "list", "--json"}):
			return []byte(`{"installed":[]}`), nil
		default:
			t.Fatalf("disabled unlisted profile executed unexpected command: %s %v", name, args)
			return nil, nil
		}
	}

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect disabled unlisted plugin: %v", err)
	}
	plugin := statusChecksByCapability(status.Checks, "plugin")[0]
	if plugin.Status != CodexCheckPartial || plugin.ReasonCode != "plugin_disabled_not_listed" {
		t.Fatalf("disabled unlisted plugin = %#v", plugin)
	}
	if evidenceValue(plugin, "configured_enabled") != "false" {
		t.Fatalf("disabled unlisted evidence = %#v", plugin.Evidence)
	}
	if status.Mode != CodexModePartialPlugin {
		t.Fatalf("disabled unlisted mode = %q, want %q", status.Mode, CodexModePartialPlugin)
	}
}

func TestInspectCodexStatusCustomizedStateRemainsUnknown(t *testing.T) {
	home, repo := prepareBasicCodexStatusProfile(t)
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[marketplaces.engram]
source_type = "path"
source = "/tmp/custom-engram"
ref = "worktree"

[mcp_servers.engram]
command = "/opt/custom/bin/engram-wrapper"
args = ["mcp", "--tools=custom"]
`)

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect customized profile: %v", err)
	}
	marketplace := statusChecksByCapability(status.Checks, "marketplace")[0]
	mcp := statusChecksByCapability(status.Checks, "mcp_configuration")[0]
	if marketplace.Status != CodexCheckCustomized || mcp.Status != CodexCheckCustomized {
		t.Fatalf("customized checks = %#v / %#v", marketplace, mcp)
	}
	if status.Mode != CodexModeUnknown {
		t.Fatalf("customized mode = %q, want %q", status.Mode, CodexModeUnknown)
	}
}

func TestInspectCodexStatusMissingCodexCLICannotVerifyConfiguredPlugin(t *testing.T) {
	home, repo := prepareBasicCodexStatusProfile(t)
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[marketplaces.engram]
source_type = "git"
source = "https://github.com/yersonargotev/engram.git"
ref = "v2.2.1"

[plugins."engram@engram"]
enabled = true
`)
	lookPathFn = func(string) (string, error) { return "", errors.New("not found") }
	runCommand = func(string, ...string) ([]byte, error) {
		t.Fatal("missing Codex CLI profile must not execute Codex")
		return nil, nil
	}

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect missing-Codex profile: %v", err)
	}
	plugin := statusChecksByCapability(status.Checks, "plugin")[0]
	if plugin.Status != CodexCheckUnavailable || plugin.ReasonCode != "codex_cli_missing" {
		t.Fatalf("plugin without Codex CLI = %#v", plugin)
	}
	if status.Mode != CodexModePartialPlugin {
		t.Fatalf("missing-Codex configured plugin mode = %q, want %q", status.Mode, CodexModePartialPlugin)
	}
}

func TestInspectCodexStatusReportsAttributableDisabledPlugin(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	codexAdminSkillsDirFn = func() string { return filepath.Join(home, "admin-skills") }
	repo := filepath.Join(home, "workspace")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatalf("create repository marker: %v", err)
	}
	osExecutable = func() (string, error) { return "/opt/engram/bin/engram", nil }
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), `[marketplaces.engram]
source_type = "git"
source = "https://github.com/yersonargotev/engram.git"
ref = "v2.2.1"

[plugins."engram@engram"]
enabled = false
`)

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	writeCanonicalCodexActivationFixture(t, filepath.Join(marketplaceRoot, "plugin", "codex"))
	installedPath := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "0.1.7")
	writeCanonicalCodexActivationFixture(t, installedPath)

	lookPathFn = func(name string) (string, error) {
		if name == "codex" {
			return "/opt/codex/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(name string, args ...string) ([]byte, error) {
		switch {
		case name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"--version"}):
			return []byte("codex-cli 0.150.1\n"), nil
		case name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"plugin", "list", "--json"}):
			return []byte(fmt.Sprintf(`{"installed":[{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.7","installed":true,"enabled":false,"source":{"source":"local","path":%q},"marketplaceSource":{"sourceType":"git","source":"https://github.com/yersonargotev/engram.git"}}]}`, filepath.Join(marketplaceRoot, "plugin", "codex"))), nil
		default:
			t.Fatalf("disabled plugin profile executed unexpected command: %s %v", name, args)
			return nil, nil
		}
	}

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect disabled plugin profile: %v", err)
	}
	marketplace := statusChecksByCapability(status.Checks, "marketplace")[0]
	plugin := statusChecksByCapability(status.Checks, "plugin")[0]
	if marketplace.Status != CodexCheckReady {
		t.Fatalf("disabled plugin obscured marketplace registration: %#v", marketplace)
	}
	if plugin.Status != CodexCheckPartial || plugin.ReasonCode != "plugin_disabled" {
		t.Fatalf("disabled plugin check = %#v", plugin)
	}
	if evidenceValue(plugin, "installed") != "true" || evidenceValue(plugin, "enabled") != "false" ||
		evidenceValue(plugin, "installed_version") != "0.1.7" || evidenceValue(plugin, "installed_revision") != testReleaseCommit {
		t.Fatalf("disabled plugin evidence = %#v", plugin.Evidence)
	}
	if status.Mode != CodexModePartialPlugin {
		t.Fatalf("disabled plugin mode = %q, want %q", status.Mode, CodexModePartialPlugin)
	}
}

func TestInspectCodexStatusDistinguishesInvalidAndCustomizedMCP(t *testing.T) {
	tests := []struct {
		name       string
		section    string
		wantStatus CodexIntegrationCheckStatus
		wantReason string
	}{
		{
			name: "invalid TOML value",
			section: `[mcp_servers.engram]
command = [
args = ["mcp", "--tools=agent"]
`,
			wantStatus: CodexCheckInvalid,
			wantReason: "mcp_configuration_invalid",
		},
		{
			name: "valid custom contract",
			section: `[mcp_servers.engram]
command = "/opt/custom/bin/engram-wrapper"
args = ["mcp", "--tools=custom"]
`,
			wantStatus: CodexCheckCustomized,
			wantReason: "mcp_configuration_customized",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home, repo := prepareBasicCodexStatusProfile(t)
			writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), tt.section)

			status, err := InspectCodexStatus("2.2.1", repo)
			if err != nil {
				t.Fatalf("inspect MCP profile: %v", err)
			}
			mcp := statusChecksByCapability(status.Checks, "mcp_configuration")[0]
			if mcp.Status != tt.wantStatus || mcp.ReasonCode != tt.wantReason {
				t.Fatalf("MCP configuration = %#v, want status=%q reason=%q", mcp, tt.wantStatus, tt.wantReason)
			}
		})
	}
}

func TestInspectCodexStatusDoesNotActivateDisabledStandaloneSkill(t *testing.T) {
	home, repo := prepareBasicCodexStatusProfile(t)
	skillPath := filepath.Join(home, ".agents", "skills", "engram-memory-cli", "SKILL.md")
	writeStatusTestFile(t, skillPath, "---\nname: engram-memory-cli\ndescription: Recall Engram memory.\n---\nUser skill.\n")
	writeStatusTestFile(t, filepath.Join(home, ".codex", "config.toml"), fmt.Sprintf(`[[skills.config]]
path = %q
enabled = false
`, skillPath))

	status, err := InspectCodexStatus("2.2.1", repo)
	if err != nil {
		t.Fatalf("inspect disabled skill profile: %v", err)
	}
	skills := statusChecksByCapability(status.Checks, "skill")
	if len(skills) != 1 || skills[0].Status != CodexCheckPartial || skills[0].ReasonCode != "engram_skill_disabled" {
		t.Fatalf("disabled skill checks = %#v", skills)
	}
	if evidenceValue(skills[0], "enabled") != "false" {
		t.Fatalf("disabled skill evidence = %#v", skills[0].Evidence)
	}
	if status.Mode != CodexModeUnknown {
		t.Fatalf("disabled skill mode = %q, want %q", status.Mode, CodexModeUnknown)
	}
}

func TestCodexStatusCheckBoundsReasonAndEvidence(t *testing.T) {
	check := codexStatusCheck(
		strings.Repeat("c", 80),
		CodexCheckUnavailable,
		strings.Repeat("r", 120),
		strings.Repeat("x", 600),
		codexEvidence(strings.Repeat("n", 80), strings.Repeat("v", 600)),
	)
	if len([]rune(check.Capability)) != 64 || len([]rune(check.ReasonCode)) != 96 || len([]rune(check.Reason)) != 512 {
		t.Fatalf("bounded check fields = capability:%d reason_code:%d reason:%d", len([]rune(check.Capability)), len([]rune(check.ReasonCode)), len([]rune(check.Reason)))
	}
	if len(check.Evidence) != 1 || len([]rune(check.Evidence[0].Name)) != 64 || len([]rune(check.Evidence[0].Value)) != 512 {
		t.Fatalf("bounded evidence = %#v", check.Evidence)
	}
}

func prepareBasicCodexStatusProfile(t *testing.T) (home, repo string) {
	t.Helper()
	resetSetupSeams(t)
	home = useTestHome(t)
	codexAdminSkillsDirFn = func() string { return filepath.Join(home, "admin-skills") }
	repo = filepath.Join(home, "workspace")
	if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatalf("create repository marker: %v", err)
	}
	osExecutable = func() (string, error) { return "/opt/engram/bin/engram", nil }
	lookPathFn = func(name string) (string, error) {
		if name == "codex" {
			return "/opt/codex/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(name string, args ...string) ([]byte, error) {
		if name == "/opt/codex/bin/codex" && slices.Equal(args, []string{"--version"}) {
			return []byte("codex-cli 0.150.1\n"), nil
		}
		t.Fatalf("basic status profile executed unexpected command: %s %v", name, args)
		return nil, nil
	}
	return home, repo
}

func installCurrentManagedPackStatusFixture(t *testing.T, home string) {
	t.Helper()
	bundleRoot := t.TempDir()
	files := map[string]string{
		filepath.Join("packs", "engram", "pack.json"):            filepath.Join("..", "..", "pack.json"),
		filepath.Join("assets", "protocol-contract-v1.json"):     filepath.Join("..", "..", "assets", "protocol-contract-v1.json"),
		filepath.Join("skills", "engram-memory-cli", "SKILL.md"): filepath.Join("..", "..", "skills", "engram-memory-cli", "SKILL.md"),
	}
	for destination, source := range files {
		raw, err := os.ReadFile(source)
		if err != nil {
			t.Fatalf("read Managed Pack fixture %s: %v", source, err)
		}
		writeStatusTestFile(t, filepath.Join(bundleRoot, destination), string(raw))
	}
	userSkillRoot := filepath.Join(home, ".agents", "skills")
	if err := os.MkdirAll(userSkillRoot, 0o755); err != nil {
		t.Fatalf("create user skill root: %v", err)
	}
	if err := os.Symlink(filepath.Join(bundleRoot, "skills", "engram-memory-cli"), filepath.Join(userSkillRoot, "engram-memory-cli")); err != nil {
		t.Fatalf("link Managed Pack skill projection: %v", err)
	}
}

func snapshotStatusTestTree(t *testing.T, root string) []string {
	t.Helper()
	var snapshot []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		item := relative + ":" + entry.Type().String()
		if entry.Type().IsRegular() {
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			digest := sha256.Sum256(content)
			item += fmt.Sprintf(":%x", digest)
		}
		snapshot = append(snapshot, item)
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot %s: %v", root, err)
	}
	return snapshot
}

func writeStatusTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create %s parent: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func statusChecksByCapability(checks []CodexIntegrationCheck, capability string) []CodexIntegrationCheck {
	var matches []CodexIntegrationCheck
	for _, check := range checks {
		if check.Capability == capability {
			matches = append(matches, check)
		}
	}
	return matches
}

func evidenceValue(check CodexIntegrationCheck, name string) string {
	for _, evidence := range check.Evidence {
		if evidence.Name == name {
			return evidence.Value
		}
	}
	return ""
}
