package setup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
	CursorCheckReady      CursorCheckStatus = "ready"
	CursorCheckMissing    CursorCheckStatus = "missing"
	CursorCheckStale      CursorCheckStatus = "stale"
	CursorCheckCustomized CursorCheckStatus = "customized"
	CursorCheckUnknown    CursorCheckStatus = "unknown"
)

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

// InspectCursorStatus inspects the active Cursor integration without
// installing, repairing, starting, or persisting anything.
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
	return CursorIntegrationStatus{
		SchemaVersion: CursorIntegrationStatusSchemaVersion,
		Agent:         "cursor",
		Mode:          deriveCursorOperatingMode(checks),
		Checks:        checks,
	}, nil
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
	if binaryErr != nil {
		return cursorPluginInspection{Check: cursorStatusCheck(
			"plugin", CursorCheckCustomized, "plugin_customized",
			"The Agent Plugin is missing its pinned Engram binary.",
			evidence...,
		), Root: root}
	}
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
			"A leftover user skill copy is present and is not the canonical plugin skill. Run engram setup to install the editorial rubric; Packy may remain for compatibility.",
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
			if cursorPluginMCPOwned(raw) {
				return cursorStatusCheck(
					"mcp", CursorCheckReady, "mcp_ready",
					"MCP registration comes from the installed Agent Plugin.",
					cursorEvidence("path", path),
					cursorEvidence("source", "plugin"),
					cursorEvidence("command", "./bin/engram"),
				)
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
	return cursorStatusCheck(
		"mcp", CursorCheckReady, "mcp_native_only",
		"An Engram MCP registration exists only as a native Cursor entry, not as plugin activation.",
		cursorEvidence("path", nativePath),
		cursorEvidence("source", "native"),
		cursorEvidence("command", command),
	)
}

func inspectCursorHooksStatus(plugin cursorPluginInspection) CursorIntegrationCheck {
	path := cursorHooksPath()
	raw, err := readFileFn(path)
	if os.IsNotExist(err) {
		return cursorStatusCheck(
			"hooks", CursorCheckMissing, "hooks_missing",
			"Cursor user hooks for the activation cue and stop follow-up are not installed.",
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
	stop, stopOwned := cursorOwnedHook(config.Hooks["stop"], "checkpoint verify-stop")
	if !sessionOwned && !stopOwned {
		return cursorStatusCheck(
			"hooks", CursorCheckMissing, "hooks_missing",
			"Cursor user hooks exist, but no Engram-owned cue or stop entries were found.",
			cursorEvidence("path", path),
		)
	}
	evidence := []CursorIntegrationEvidence{cursorEvidence("path", path)}
	if sessionStart != "" {
		evidence = append(evidence, cursorEvidence("session_start", sessionStart))
	}
	if stop != "" {
		evidence = append(evidence, cursorEvidence("stop", stop))
	}
	expectedStart := ""
	expectedStop := ""
	if plugin.Root != "" {
		expectedStart = cursorSessionStartCommand(plugin.Root)
		expectedStop = cursorStopCommand(plugin.Root)
	}
	if plugin.Root == "" || sessionStart != expectedStart || stop != expectedStop {
		return cursorStatusCheck(
			"hooks", CursorCheckCustomized, "hooks_customized",
			"Engram-named Cursor hooks exist but do not match the supported cue and stop contract.",
			evidence...,
		)
	}
	return cursorStatusCheck(
		"hooks", CursorCheckReady, "hooks_ready",
		"Cursor user hooks deliver the activation cue and stop follow-up from the installed plugin binary.",
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
	if ready("plugin") && ready("skill") && ready("mcp") && ready("hooks") {
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

func cursorPluginMCPOwned(raw []byte) bool {
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
	return entry.Type == "stdio" && entry.Command == "./bin/engram" &&
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
