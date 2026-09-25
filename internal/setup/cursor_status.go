package setup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CursorIntegrationStatusSchemaVersion identifies the additive JSON contract
// emitted by `engram setup status cursor --json`.
const CursorIntegrationStatusSchemaVersion = "cursor-integration-status-v1"

// CursorOperatingMode is the conservative mode derived from independently
// inspected Cursor integration surfaces.
type CursorOperatingMode string

const (
	CursorModeUnknown         CursorOperatingMode = "unknown"
	CursorModeMCPOnly         CursorOperatingMode = "mcp_only"
	CursorModePartial         CursorOperatingMode = "partial"
	CursorModeCheckpointReady CursorOperatingMode = "checkpoint_ready"
)

// CursorCheckStatus reports the state of one independently observable
// Cursor integration surface.
type CursorCheckStatus string

const (
	CursorCheckReady       CursorCheckStatus = "ready"
	CursorCheckMissing     CursorCheckStatus = "missing"
	CursorCheckStale       CursorCheckStatus = "stale"
	CursorCheckCustomized  CursorCheckStatus = "customized"
	CursorCheckUnavailable CursorCheckStatus = "unavailable"
	CursorCheckUnknown     CursorCheckStatus = "unknown"
	CursorCheckPending     CursorCheckStatus = "pending"
)

var runCursorMCPProbeFn = probeCursorMCP
var runCursorCLICommandFn = runCursorCLICommand

var requiredCursorAgentTools = []string{
	"mem_current_project",
	"mem_search",
	"mem_get_observation",
	"mem_checkpoint",
	"mem_checkpoint_status",
}

// CursorIntegrationEvidence is one bounded, named fact supporting a check.
type CursorIntegrationEvidence struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// CursorIntegrationCheck describes one integration surface without collapsing
// partial or customized state into readiness.
type CursorIntegrationCheck struct {
	Capability string                      `json:"capability"`
	Status     CursorCheckStatus           `json:"status"`
	ReasonCode string                      `json:"reason_code"`
	Reason     string                      `json:"reason"`
	Evidence   []CursorIntegrationEvidence `json:"evidence"`
}

// CursorIntegrationStatus is a deterministic, read-only capability snapshot.
type CursorIntegrationStatus struct {
	SchemaVersion string                   `json:"schema_version"`
	Agent         string                   `json:"agent"`
	Mode          CursorOperatingMode      `json:"mode"`
	Checks        []CursorIntegrationCheck `json:"checks"`
}

// InspectCursorStatus inspects the active Cursor integration without installing
// or repairing it. An attributable MCP registration is actively probed in a
// short-lived subprocess so readiness means protocol availability, not file
// presence alone.
func InspectCursorStatus(runningVersion, runningRevision, workingDirectory string) (CursorIntegrationStatus, error) {
	plugin := inspectCursorPluginStatus(runningVersion, runningRevision)
	skills := inspectCursorSkillStatus(plugin)
	mcp := inspectCursorMCPStatus(plugin)
	hooks := inspectCursorHooksStatus(plugin)
	userRules := cursorStatusCheck(
		"user_rules", CursorCheckUnknown, "user_rules_unknown",
		"Cursor User Rules live in the Settings store, which setup cannot inspect.",
	)
	checks := []CursorIntegrationCheck{plugin.Check}
	checks = append(checks, skills...)
	checks = append(checks, mcp, hooks, userRules)
	checks = append(checks, inspectCursorCLIMCPStatus(workingDirectory), cursorStatusCheck(
		"cli_use", CursorCheckUnknown, "cli_use_not_observed",
		"A status snapshot cannot attribute a prior MCP call to a fresh Cursor CLI session; verify a real call with the content-free baseline.",
	))
	return CursorIntegrationStatus{
		SchemaVersion: CursorIntegrationStatusSchemaVersion,
		Agent:         "cursor",
		Mode:          deriveCursorOperatingMode(checks),
		Checks:        checks,
	}, nil
}

func runCursorCLICommand(directory string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	home, err := userHome()
	if err != nil {
		return "", err
	}
	isolatedHome, err := os.MkdirTemp("", "engram-cursor-status-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(isolatedHome)
	configDir := filepath.Join(isolatedHome, ".cursor")
	if err := os.Mkdir(configDir, 0700); err != nil {
		return "", err
	}
	for _, name := range []string{"mcp.json", "cli-config.json"} {
		data, err := os.ReadFile(filepath.Join(home, ".cursor", name))
		if err == nil {
			if err := os.WriteFile(filepath.Join(configDir, name), data, 0600); err != nil {
				return "", err
			}
		} else if !os.IsNotExist(err) {
			return "", err
		}
	}
	command := exec.CommandContext(ctx, "agent", append([]string{"mcp"}, args...)...)
	environment := os.Environ()
	for i := range environment {
		if strings.HasPrefix(environment[i], "HOME=") {
			environment = append(environment[:i], environment[i+1:]...)
			break
		}
	}
	command.Env = append(environment, "HOME="+isolatedHome)
	if directory != "" {
		command.Dir = directory
	}
	output, err := command.CombinedOutput()
	return string(output), err
}

// inspectCursorCLIMCPStatus uses the host's inventory. Approval is left to
// Cursor and is never inferred from an isolated Engram protocol probe.
func inspectCursorCLIMCPStatus(directory string) CursorIntegrationCheck {
	if _, err := lookPathFn("agent"); err != nil {
		return cursorStatusCheck("cli_mcp", CursorCheckUnavailable, "cli_unavailable", "Cursor CLI is not installed or is not on PATH.")
	}
	source := "user"
	path := cursorMCPPath()
	if directory != "" {
		projectPath := filepath.Join(directory, ".cursor", "mcp.json")
		if raw, err := readFileFn(projectPath); err == nil {
			if _, found, _ := cursorNativeMCPEntry(raw); found {
				source, path = "project", projectPath
			}
		}
	}
	evidence := []CursorIntegrationEvidence{cursorEvidence("source", source), cursorEvidence("path", path)}
	if source == "user" {
		pluginPath := filepath.Join(cursorPluginDir(), "mcp.json")
		if raw, err := readFileFn(pluginPath); err == nil && cursorPluginMCPOwned(cursorPluginDir(), raw) {
			evidence = append(evidence, cursorEvidence("editor_plugin_mcp", "also_registered"))
		}
	}
	output, err := runCursorCLICommandFn(directory, "list")
	if err != nil {
		return cursorStatusCheck("cli_mcp", CursorCheckUnavailable, "cli_inventory_unavailable", "Cursor CLI could not list MCP servers.", evidence...)
	}
	state := ""
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "engram: ") {
			state = strings.TrimSpace(strings.TrimPrefix(line, "engram: "))
			break
		}
	}
	if state == "" {
		return cursorStatusCheck("cli_mcp", CursorCheckMissing, "cli_not_discovered", "Cursor CLI did not discover an Engram MCP server.", evidence...)
	}
	evidence = append(evidence, cursorEvidence("host_state", state))
	if strings.Contains(strings.ToLower(state), "needs approval") {
		return cursorStatusCheck("cli_mcp", CursorCheckPending, "cli_approval_pending", "Cursor CLI discovered Engram; user approval is pending.", evidence...)
	}
	if state != "ready" {
		return cursorStatusCheck("cli_mcp", CursorCheckUnavailable, "cli_server_unavailable", "Cursor CLI discovered Engram but cannot use it.", evidence...)
	}
	toolOutput, err := runCursorCLICommandFn(directory, "list-tools", "engram")
	if err != nil {
		return cursorStatusCheck("cli_mcp", CursorCheckUnavailable, "cli_tools_unavailable", "Cursor CLI could not list Engram tools.", evidence...)
	}
	var names []string
	for _, line := range strings.Split(toolOutput, "\n") {
		if strings.HasPrefix(line, "- ") {
			name, _, _ := strings.Cut(strings.TrimPrefix(line, "- "), " ")
			names = append(names, name)
		}
	}
	missing, unexpected := cursorAgentToolCatalogDifference(names)
	if len(names) != len(requiredCursorAgentTools) || len(missing) != 0 || len(unexpected) != 0 {
		return cursorStatusCheck("cli_mcp", CursorCheckUnavailable, "cli_tools_incomplete", "Cursor CLI tool catalog does not match the five Engram agent tools.", evidence...)
	}
	return cursorStatusCheck("cli_mcp", CursorCheckReady, "cli_ready", "Cursor CLI discovered and can list the five Engram agent tools.", append(evidence, cursorEvidence("tool_count", "5"))...)
}

type cursorPluginInspection struct {
	Check CursorIntegrationCheck
	Root  string
}

func inspectCursorPluginStatus(runningVersion, runningRevision string) cursorPluginInspection {
	root := cursorPluginDir()
	manifestPath := filepath.Join(root, "plugin.json")
	raw, err := readFileFn(manifestPath)
	if os.IsNotExist(err) {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckMissing, "plugin_missing",
			"The Engram Agent Plugin is not installed in Cursor's local plugin directory.",
			cursorEvidence("path", manifestPath),
		)}
	}
	if err != nil {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckMissing, "plugin_missing",
			"The Engram Agent Plugin could not be inspected.",
			cursorEvidence("path", manifestPath),
		)}
	}

	var manifest map[string]any
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckCustomized, "plugin_customized",
			"An Engram-named plugin directory exists but its manifest is not attributable.",
			cursorEvidence("path", manifestPath),
		), Root: root}
	}
	name, _ := manifest["name"].(string)
	if name != "engram" {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckCustomized, "plugin_customized",
			"A plugin exists at the Engram path but does not match the supported Agent Plugin contract.",
			cursorEvidence("path", manifestPath),
		), Root: root}
	}

	identityPath := filepath.Join(root, ".engram-release.json")
	identityRaw, identityErr := readFileFn(identityPath)
	binaryPath := filepath.Join(root, "bin", "engram")
	_, binaryErr := statFn(binaryPath)
	evidence := []CursorIntegrationEvidence{
		cursorEvidence("path", root),
		cursorEvidence("manifest", manifestPath),
	}
	if binaryErr == nil {
		evidence = append(evidence, cursorEvidence("binary", binaryPath))
	}

	if identityErr != nil {
		if os.IsNotExist(identityErr) {
			return cursorPluginInspection{Check: cursorStatusCheck(
				"plugin", CursorCheckCustomized, "plugin_customized",
				"The Agent Plugin is present without a pinned release identity.",
				evidence...,
			), Root: root}
		}
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckCustomized, "plugin_customized",
			"The Agent Plugin release identity could not be read.",
			evidence...,
		), Root: root}
	}

	var identity map[string]string
	if err := json.Unmarshal(identityRaw, &identity); err != nil {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckCustomized, "plugin_customized",
			"The Agent Plugin release identity is not attributable.",
			evidence...,
		), Root: root}
	}
	version := strings.TrimSpace(identity["version"])
	commit := strings.ToLower(strings.TrimSpace(identity["commit"]))
	evidence = append(evidence,
		cursorEvidence("installed_version", version),
		cursorEvidence("installed_commit", commit),
	)
	runningVersion = strings.TrimPrefix(strings.TrimSpace(runningVersion), "v")
	runningRevision = strings.ToLower(strings.TrimSpace(runningRevision))
	evidence = append(evidence,
		cursorEvidence("running_version", runningVersion),
		cursorEvidence("running_commit", runningRevision),
	)
	if binaryErr != nil {
		if os.IsNotExist(binaryErr) {
			return cursorPluginInspection{Check: cursorStatusCheck(
				"plugin", CursorCheckMissing, "plugin_binary_missing",
				"The Agent Plugin is missing its pinned Engram binary.",
				append(evidence, cursorEvidence("binary_state", "missing"))...,
			), Root: root}
		}
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckUnavailable, "plugin_binary_unavailable",
			"The Agent Plugin binary could not be inspected.",
			evidence...,
		), Root: root}
	}
	currentBinary, err := osExecutable()
	if err != nil {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckUnavailable, "plugin_binary_unavailable",
			"The running Engram binary could not be resolved for comparison.", evidence...,
		), Root: root}
	}
	installedDigest, err := cursorBinaryDigest(binaryPath)
	if err != nil {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckUnavailable, "plugin_binary_unavailable",
			"The Agent Plugin binary could not be read for comparison.", evidence...,
		), Root: root}
	}
	runningDigest, err := cursorBinaryDigest(currentBinary)
	if err != nil {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckUnavailable, "plugin_binary_unavailable",
			"The running Engram binary could not be read for comparison.", evidence...,
		), Root: root}
	}
	if installedDigest != runningDigest {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckStale, "plugin_binary_stale",
			"The Agent Plugin MCP binary differs from the running Engram binary; rerun setup cursor to refresh it.",
			append(evidence, cursorEvidence("binary_state", "stale"))...,
		), Root: root}
	}
	evidence = append(evidence, cursorEvidence("binary_state", "current"))
	if runningVersion != "" && version != runningVersion || runningRevision != "" && commit != runningRevision {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckStale, "plugin_stale",
			"The installed Agent Plugin does not match the running Engram release identity.",
			evidence...,
		), Root: root}
	}
	return cursorPluginInspection{
		Check: cursorStatusCheck(
			"plugin", CursorCheckReady, "plugin_ready",
			"The local Cursor Agent Plugin matches the running Engram release identity.",
			evidence...,
		),
		Root: root,
	}
}

func cursorBinaryDigest(path string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	file, err := os.Open(path)
	if err != nil {
		return digest, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return digest, err
	}
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func inspectCursorSkillStatus(plugin cursorPluginInspection) []CursorIntegrationCheck {
	checks := []CursorIntegrationCheck{inspectCursorPluginSkill(plugin)}
	return append(checks, inspectCursorLeftoverUserSkills()...)
}

func inspectCursorPluginSkill(plugin cursorPluginInspection) CursorIntegrationCheck {
	if plugin.Root == "" {
		return cursorStatusCheck(
			"skill", CursorCheckMissing, "skill_missing",
			"The canonical engram-memory skill is not installed in the Cursor Agent Plugin.",
		)
	}
	path := filepath.Join(plugin.Root, "skills", "engram-memory", "SKILL.md")
	raw, err := readFileFn(path)
	if os.IsNotExist(err) {
		return cursorStatusCheck(
			"skill", CursorCheckMissing, "skill_missing",
			"The canonical engram-memory skill is not installed in the Cursor Agent Plugin.",
			cursorEvidence("path", path),
		)
	}
	if err != nil {
		return cursorStatusCheck(
			"skill", CursorCheckCustomized, "skill_customized",
			"The plugin skill could not be read.",
			cursorEvidence("path", path),
		)
	}
	name, _, ok := codexEngramSkillIdentity(string(raw))
	digest := sha256.Sum256(raw)
	evidence := []CursorIntegrationEvidence{
		cursorEvidence("path", path),
		cursorEvidence("sha256", hex.EncodeToString(digest[:])),
	}
	if name != "" {
		evidence = append(evidence, cursorEvidence("name", name))
	}
	if !ok || name != "engram-memory" {
		return cursorStatusCheck(
			"skill", CursorCheckCustomized, "skill_customized",
			"A skill exists in the plugin path but is not the canonical engram-memory rubric.",
			evidence...,
		)
	}
	expected, expectedErr := cursorAgentPluginFS.ReadFile(cursorAgentPluginEmbedRoot + "/skills/engram-memory/SKILL.md")
	if expectedErr == nil && !bytes.Equal(raw, expected) {
		return cursorStatusCheck(
			"skill", CursorCheckStale, "skill_stale",
			"The installed plugin skill does not match the running editorial engram-memory rubric.",
			evidence...,
		)
	}
	return cursorStatusCheck(
		"skill", CursorCheckReady, "skill_ready",
		"The installed Agent Plugin provides the canonical engram-memory skill.",
		evidence...,
	)
}

func inspectCursorLeftoverUserSkills() []CursorIntegrationCheck {
	home, err := userHome()
	if err != nil || strings.TrimSpace(home) == "" {
		return nil
	}
	var checks []CursorIntegrationCheck
	for _, rel := range []string{
		filepath.Join(".agents", "skills", "engram-memory", "SKILL.md"),
		filepath.Join(".agents", "skills", "engram-memory-cli", "SKILL.md"),
		filepath.Join(".cursor", "skills", "engram-memory", "SKILL.md"),
	} {
		path := filepath.Join(home, rel)
		raw, readErr := readFileFn(path)
		if readErr != nil {
			continue
		}
		name, _, ok := codexEngramSkillIdentity(string(raw))
		if !ok {
			continue
		}
		checks = append(checks, cursorStatusCheck(
			"skill", CursorCheckCustomized, "skill_customized",
			"A leftover user skill copy is present and is not the canonical plugin skill. Run engram setup to install the editorial rubric.",
			cursorEvidence("path", path),
			cursorEvidence("name", name),
			cursorEvidence("source", "user"),
		))
	}
	return checks
}

func inspectCursorMCPStatus(plugin cursorPluginInspection) CursorIntegrationCheck {
	if plugin.Root != "" {
		path := filepath.Join(plugin.Root, "mcp.json")
		raw, err := readFileFn(path)
		if err == nil {
			if cursorPluginMCPOwned(plugin.Root, raw) {
				if conflict := inspectCursorNativeMCPConflict(path); conflict != nil {
					return *conflict
				}
				return inspectCursorMCPRuntime(path, "plugin", cursorHookBinary(plugin.Root), []string{"mcp", "--tools=agent"})
			}
			if len(raw) > 0 {
				return cursorStatusCheck(
					"mcp", CursorCheckCustomized, "mcp_customized",
					"The plugin MCP registration exists but does not match the supported contract.",
					cursorEvidence("path", path),
					cursorEvidence("source", "plugin"),
				)
			}
		}
	}

	nativePath := cursorMCPPath()
	raw, err := readFileFn(nativePath)
	if os.IsNotExist(err) {
		return cursorStatusCheck(
			"mcp", CursorCheckMissing, "mcp_missing",
			"No Engram MCP registration was found in the Agent Plugin or native Cursor MCP file.",
			cursorEvidence("path", nativePath),
		)
	}
	if err != nil {
		return cursorStatusCheck(
			"mcp", CursorCheckCustomized, "mcp_customized",
			"The native Cursor MCP file could not be inspected.",
			cursorEvidence("path", nativePath),
		)
	}
	entry, found, owned := cursorNativeMCPEntry(raw)
	if !found {
		return cursorStatusCheck(
			"mcp", CursorCheckMissing, "mcp_missing",
			"No Engram MCP registration was found in the Agent Plugin or native Cursor MCP file.",
			cursorEvidence("path", nativePath),
		)
	}
	if !owned {
		return cursorStatusCheck(
			"mcp", CursorCheckCustomized, "mcp_customized",
			"A native Engram-named MCP registration exists but does not match the supported contract.",
			cursorEvidence("path", nativePath),
			cursorEvidence("source", "native"),
		)
	}
	command, _ := entry["command"].(string)
	args := cursorMCPEntryArgs(entry)
	return inspectCursorMCPRuntime(nativePath, "native", command, args)
}

func inspectCursorNativeMCPConflict(pluginPath string) *CursorIntegrationCheck {
	nativePath := cursorMCPPath()
	raw, err := readFileFn(nativePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		check := cursorStatusCheck(
			"mcp", CursorCheckCustomized, "mcp_native_conflict",
			"The native Cursor MCP file cannot be inspected and may take precedence over the plugin registration.",
			cursorEvidence("path", pluginPath), cursorEvidence("native_path", nativePath),
		)
		return &check
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		check := cursorStatusCheck(
			"mcp", CursorCheckCustomized, "mcp_native_conflict",
			"The native Cursor MCP file is not attributable JSON and may take precedence over the plugin registration.",
			cursorEvidence("path", pluginPath), cursorEvidence("native_path", nativePath),
		)
		return &check
	}
	_, found, owned := cursorNativeMCPEntry(raw)
	if !found {
		return nil
	}
	if owned {
		entry, _, _ := cursorNativeMCPEntry(raw)
		if entry["command"] == cursorHookBinary(filepath.Dir(pluginPath)) {
			return nil
		}
	}
	check := cursorStatusCheck(
		"mcp", CursorCheckCustomized, "mcp_native_conflict", "A custom native Engram MCP registration may take precedence over the plugin registration.",
		cursorEvidence("path", pluginPath), cursorEvidence("native_path", nativePath),
	)
	return &check
}

func inspectCursorMCPRuntime(configPath, source, command string, args []string) CursorIntegrationCheck {
	evidence := []CursorIntegrationEvidence{
		cursorEvidence("path", configPath),
		cursorEvidence("source", source),
		cursorEvidence("command", command),
	}
	resolved, err := lookPathFn(command)
	if err != nil || strings.TrimSpace(resolved) == "" {
		return cursorStatusCheck(
			"mcp", CursorCheckUnavailable, "mcp_executable_missing",
			"The configured Engram MCP executable cannot be resolved as an executable file.", evidence...,
		)
	}
	evidence = append(evidence, cursorEvidence("resolved_path", resolved), cursorEvidence("transport", "stdio"))
	tools, err := runCursorMCPProbeFn(resolved, args...)
	if err != nil {
		return cursorStatusCheck(
			"mcp", CursorCheckUnavailable, "mcp_protocol_unavailable",
			"The configured executable did not complete MCP initialize and tools/list.", evidence...,
		)
	}
	missing, unexpected := cursorAgentToolCatalogDifference(tools)
	evidence = append(evidence, cursorEvidence("tool_count", fmt.Sprintf("%d", len(tools))))
	if len(missing) > 0 || len(unexpected) > 0 || len(tools) != len(requiredCursorAgentTools) {
		evidence = append(evidence, cursorEvidence("missing_tools", strings.Join(missing, ",")))
		evidence = append(evidence, cursorEvidence("unexpected_tools", strings.Join(unexpected, ",")))
		return cursorStatusCheck(
			"mcp", CursorCheckUnavailable, "mcp_tools_incomplete",
			"MCP initialize and tools/list succeeded, but the tool catalog does not exactly match the five-tool agent profile.", evidence...,
		)
	}
	reasonCode := "mcp_ready"
	reason := "The configured executable completed MCP initialize and tools/list with all five agent tools."
	if source == "native" {
		reasonCode = "mcp_native_only"
		reason = "The native Cursor MCP entry completed initialize and tools/list with all five agent tools."
	}
	return cursorStatusCheck("mcp", CursorCheckReady, reasonCode, reason, evidence...)
}

func cursorMCPEntryArgs(entry map[string]any) []string {
	raw, _ := entry["args"].([]any)
	args := make([]string, 0, len(raw))
	for _, value := range raw {
		arg, ok := value.(string)
		if !ok {
			return nil
		}
		args = append(args, arg)
	}
	return args
}

func cursorAgentToolCatalogDifference(tools []string) ([]string, []string) {
	present := make(map[string]bool, len(tools))
	for _, tool := range tools {
		present[tool] = true
	}
	var missing []string
	for _, required := range requiredCursorAgentTools {
		if !present[required] {
			missing = append(missing, required)
		}
	}
	required := make(map[string]bool, len(requiredCursorAgentTools))
	for _, tool := range requiredCursorAgentTools {
		required[tool] = true
	}
	var unexpected []string
	for _, tool := range tools {
		if !required[tool] {
			unexpected = append(unexpected, tool)
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return missing, unexpected
}

func probeCursorMCP(command string, args ...string) ([]string, error) {
	result, err := runStdioMCPProbe(command, args, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return result.Tools, nil
}

func inspectCursorHooksStatus(plugin cursorPluginInspection) CursorIntegrationCheck {
	path := cursorHooksPath()
	raw, err := readFileFn(path)
	if os.IsNotExist(err) {
		return cursorStatusCheck(
			"hooks", CursorCheckMissing, "hooks_missing",
			"Cursor user hooks for the activation cue, root-turn identity, and stop follow-up are not installed.",
			cursorEvidence("path", path),
		)
	}
	if err != nil {
		return cursorStatusCheck(
			"hooks", CursorCheckCustomized, "hooks_customized",
			"The Cursor user hooks file could not be inspected.",
			cursorEvidence("path", path),
		)
	}
	var config cursorUserHooksFile
	if err := json.Unmarshal(raw, &config); err != nil {
		return cursorStatusCheck(
			"hooks", CursorCheckCustomized, "hooks_customized",
			"The Cursor user hooks file exists but is not attributable JSON.",
			cursorEvidence("path", path),
		)
	}

	sessionStart, sessionOwned := cursorOwnedHook(config.Hooks["sessionStart"], "lifecycle session-start")
	promptSubmit, promptOwned := cursorOwnedHook(config.Hooks["beforeSubmitPrompt"], "lifecycle prompt-submit")
	stop, stopOwned := cursorOwnedHook(config.Hooks["stop"], "checkpoint verify-stop")
	if !sessionOwned && !promptOwned && !stopOwned {
		return cursorStatusCheck(
			"hooks", CursorCheckMissing, "hooks_missing",
			"Cursor user hooks exist, but no Engram-owned cue, identity, or stop entries were found.",
			cursorEvidence("path", path),
		)
	}
	evidence := []CursorIntegrationEvidence{cursorEvidence("path", path)}
	if sessionStart != "" {
		evidence = append(evidence, cursorEvidence("session_start", sessionStart))
	}
	if promptSubmit != "" {
		evidence = append(evidence, cursorEvidence("prompt_submit", promptSubmit))
	}
	if stop != "" {
		evidence = append(evidence, cursorEvidence("stop", stop))
	}
	expectedStart := ""
	expectedPrompt := ""
	expectedStop := ""
	if plugin.Root != "" {
		expectedStart = cursorSessionStartCommand(plugin.Root)
		expectedPrompt = cursorPromptSubmitCommand(plugin.Root)
		expectedStop = cursorStopCommand(plugin.Root)
	}
	if plugin.Root == "" || sessionStart != expectedStart || promptSubmit != expectedPrompt || stop != expectedStop {
		return cursorStatusCheck(
			"hooks", CursorCheckCustomized, "hooks_customized",
			"Engram-named Cursor hooks exist but do not match the supported cue, identity, and stop contract.",
			evidence...,
		)
	}
	return cursorStatusCheck(
		"hooks", CursorCheckReady, "hooks_ready",
		"Cursor user hooks deliver the activation cue, root-turn identity, and stop follow-up from the installed plugin binary.",
		evidence...,
	)
}

func deriveCursorOperatingMode(checks []CursorIntegrationCheck) CursorOperatingMode {
	ready := func(capability string) bool {
		for _, check := range checks {
			if check.Capability == capability && check.Status == CursorCheckReady {
				return true
			}
		}
		return false
	}
	present := func(capability string) bool {
		for _, check := range checks {
			if check.Capability == capability && check.Status != CursorCheckMissing {
				return true
			}
		}
		return false
	}
	if ready("plugin") && ready("skill") && ready("mcp") && ready("hooks") && ready("cli_mcp") {
		return CursorModeCheckpointReady
	}
	if !present("plugin") && !present("skill") && !present("hooks") && ready("mcp") {
		return CursorModeMCPOnly
	}
	if present("plugin") || present("skill") || present("mcp") || present("hooks") {
		return CursorModePartial
	}
	return CursorModeUnknown
}

func cursorPluginMCPOwned(pluginRoot string, raw []byte) bool {
	var config struct {
		Servers map[string]struct {
			Type    string   `json:"type"`
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return false
	}
	entry, ok := config.Servers["engram"]
	if !ok {
		return false
	}
	return entry.Type == "stdio" && entry.Command == cursorHookBinary(pluginRoot) &&
		len(entry.Args) == 2 && entry.Args[0] == "mcp" && entry.Args[1] == "--tools=agent"
}

func cursorNativeMCPEntry(raw []byte) (map[string]any, bool, bool) {
	var config map[string]json.RawMessage
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, false, false
	}
	serversRaw, ok := config["mcpServers"]
	if !ok {
		return nil, false, false
	}
	var servers map[string]json.RawMessage
	if err := json.Unmarshal(serversRaw, &servers); err != nil {
		return nil, false, false
	}
	entryRaw, ok := servers["engram"]
	if !ok {
		return nil, false, false
	}
	var entry map[string]any
	if err := json.Unmarshal(entryRaw, &entry); err != nil {
		return nil, true, false
	}
	return entry, true, cursorNativeMCPOwned(entryRaw)
}

func cursorOwnedHook(entries []json.RawMessage, needle string) (string, bool) {
	for _, raw := range entries {
		var spec cursorHookSpec
		if err := json.Unmarshal(raw, &spec); err != nil {
			continue
		}
		if cursorOwnedHookCommand(spec.Command) && strings.Contains(spec.Command, needle) {
			return spec.Command, true
		}
	}
	return "", false
}

func cursorEvidence(name, value string) CursorIntegrationEvidence {
	return CursorIntegrationEvidence{Name: name, Value: value}
}

func cursorStatusCheck(capability string, status CursorCheckStatus, reasonCode, reason string, evidence ...CursorIntegrationEvidence) CursorIntegrationCheck {
	items := make([]CursorIntegrationEvidence, 0, len(evidence))
	for _, item := range evidence {
		items = append(items, CursorIntegrationEvidence{
			Name:  boundedCodexStatusText(item.Name, 64),
			Value: boundedCodexStatusText(item.Value, 512),
		})
	}
	return CursorIntegrationCheck{
		Capability: boundedCodexStatusText(capability, 64),
		Status:     status, ReasonCode: boundedCodexStatusText(reasonCode, 96),
		Reason:   boundedCodexStatusText(reason, 512),
		Evidence: items,
	}
}
