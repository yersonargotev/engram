package setup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yersonargotev/engram/internal/protocolcontract"
	"golang.org/x/mod/semver"
)

// CodexIntegrationStatusSchemaVersion identifies the additive JSON contract
// emitted by `engram setup status codex --json`.
const CodexIntegrationStatusSchemaVersion = "codex-integration-status-v1"

// CodexOperatingMode is the conservative mode derived from independently
// inspected Codex integration surfaces.
type CodexOperatingMode string

const (
	CodexModeUnknown         CodexOperatingMode = "unknown"
	CodexModeManualSkillCLI  CodexOperatingMode = "manual_skill_cli"
	CodexModeMCPOnly         CodexOperatingMode = "mcp_only"
	CodexModePartialPlugin   CodexOperatingMode = "partial_plugin"
	CodexModeCheckpointReady CodexOperatingMode = "checkpoint_ready"
)

// CodexIntegrationCheckStatus reports the state of one independently
// observable Codex integration surface.
type CodexIntegrationCheckStatus string

const (
	CodexCheckReady       CodexIntegrationCheckStatus = "ready"
	CodexCheckMissing     CodexIntegrationCheckStatus = "missing"
	CodexCheckPartial     CodexIntegrationCheckStatus = "partial"
	CodexCheckInvalid     CodexIntegrationCheckStatus = "invalid"
	CodexCheckUnavailable CodexIntegrationCheckStatus = "unavailable"
	CodexCheckCustomized  CodexIntegrationCheckStatus = "customized"
)

// CodexIntegrationEvidence is one bounded, named fact supporting a check.
type CodexIntegrationEvidence struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// CodexIntegrationCheck describes one integration surface without collapsing
// partial or customized state into readiness.
type CodexIntegrationCheck struct {
	Capability string                      `json:"capability"`
	Status     CodexIntegrationCheckStatus `json:"status"`
	ReasonCode string                      `json:"reason_code"`
	Reason     string                      `json:"reason"`
	Evidence   []CodexIntegrationEvidence  `json:"evidence"`
}

// CodexIntegrationStatus is a deterministic, read-only capability snapshot.
type CodexIntegrationStatus struct {
	SchemaVersion string                               `json:"schema_version"`
	Agent         string                               `json:"agent"`
	Mode          CodexOperatingMode                   `json:"mode"`
	Compatibility protocolcontract.CompatibilityReport `json:"compatibility"`
	Checks        []CodexIntegrationCheck              `json:"checks"`
}

// InspectCodexStatus inspects the active Codex integration without installing,
// repairing, starting, or persisting anything.
func InspectCodexStatus(runningVersion, workingDirectory string) (CodexIntegrationStatus, error) {
	return inspectCodexStatus(runningVersion, "", workingDirectory)
}

// InspectCodexStatusWithRevision adds the running binary's build revision to
// the attributable, read-only compatibility evidence.
func InspectCodexStatusWithRevision(runningVersion, runningRevision, workingDirectory string) (CodexIntegrationStatus, error) {
	return inspectCodexStatus(runningVersion, runningRevision, workingDirectory)
}

func inspectCodexStatus(runningVersion, runningRevision, workingDirectory string) (CodexIntegrationStatus, error) {
	checks := make([]CodexIntegrationCheck, 0, 11)

	runningPath, runningErr := osExecutable()
	if runningErr != nil || strings.TrimSpace(runningPath) == "" {
		checks = append(checks, codexStatusCheck(
			"engram_cli", CodexCheckUnavailable, "engram_cli_path_unavailable",
			"The running Engram executable path could not be resolved.",
		))
	} else {
		evidence := []CodexIntegrationEvidence{
			codexEvidence("path", filepath.Clean(runningPath)),
			codexEvidence("version", strings.TrimSpace(runningVersion)),
		}
		if strings.TrimSpace(runningRevision) != "" {
			evidence = append(evidence, codexEvidence("revision", strings.TrimSpace(runningRevision)))
		}
		checks = append(checks, codexStatusCheck(
			"engram_cli", CodexCheckReady, "engram_cli_available",
			"The running Engram CLI is available.",
			evidence...,
		))
	}

	codexPath, codexErr := lookPathFn("codex")
	if codexErr != nil || strings.TrimSpace(codexPath) == "" {
		checks = append(checks, codexStatusCheck(
			"codex_cli", CodexCheckMissing, "codex_cli_missing",
			"The Codex CLI is not available on PATH.",
		))
		codexPath = ""
	} else {
		versionOutput, versionErr := runCommand(codexPath, "--version")
		if versionErr != nil {
			checks = append(checks, codexStatusCheck(
				"codex_cli", CodexCheckUnavailable, "codex_cli_version_failed",
				"The Codex CLI path resolved, but its version probe failed.",
				codexEvidence("path", filepath.Clean(codexPath)),
			))
		} else {
			checks = append(checks, codexStatusCheck(
				"codex_cli", CodexCheckReady, "codex_cli_available",
				"The Codex CLI is available.",
				codexEvidence("path", filepath.Clean(codexPath)),
				codexEvidence("version", string(versionOutput)),
			))
		}
	}

	configPath := codexConfigPath()
	skillChecks := inspectCodexStandaloneSkills(workingDirectory, configPath)
	marketplace := inspectCodexStatusMarketplace(configPath)
	marketplaceCheck := marketplace.Check

	plugin := inspectCodexPluginStatus(codexPath, configPath, marketplace)
	compatibility := inspectCodexProtocolCompatibility(runningVersion, runningRevision, runningPath, skillChecks, plugin)
	if plugin.Skill != nil {
		if len(skillChecks) == 1 && skillChecks[0].Status == CodexCheckMissing {
			skillChecks[0] = *plugin.Skill
		} else {
			skillChecks = append(skillChecks, *plugin.Skill)
		}
	}
	if plugin.Revision != "" {
		marketplaceCheck.Evidence = append(marketplaceCheck.Evidence, CodexIntegrationEvidence{Name: "resolved_revision", Value: plugin.Revision})
	}
	checks = append(checks, skillChecks...)
	checks = append(checks, marketplaceCheck, plugin.Check)

	mcpConfiguration := inspectCodexMCPConfiguration(configPath)
	checks = append(checks, mcpConfiguration)
	checks = append(checks, inspectCodexMCPReadiness(mcpConfiguration))
	checks = append(checks,
		codexPluginCapabilityCheck(plugin, "prompt_hook", plugin.Capabilities.PromptHookReady),
		codexPluginCapabilityCheck(plugin, "session_hook", plugin.Capabilities.SessionHookReady),
		codexPluginCapabilityCheck(plugin, "activation_cue", plugin.Capabilities.ActivationCueReady),
		codexPluginCapabilityCheck(plugin, "stop_verifier", plugin.Capabilities.VerifierReady),
	)

	mode := deriveCodexOperatingMode(checks)
	if mode == CodexModeCheckpointReady && compatibility.Status != protocolcontract.CompatibilityReady {
		mode = CodexModePartialPlugin
	}
	return CodexIntegrationStatus{
		SchemaVersion: CodexIntegrationStatusSchemaVersion,
		Agent:         "codex",
		Mode:          mode,
		Compatibility: compatibility,
		Checks:        checks,
	}, nil
}

type codexStatusMarketplaceInspection struct {
	Check                    CodexIntegrationCheck
	Present                  bool
	Attributable             bool
	Ref                      string
	PluginPresent            bool
	PluginConfigAttributable bool
	PluginEnabled            bool
	CachePresent             bool
}

func inspectCodexStatusMarketplace(configPath string) codexStatusMarketplaceInspection {
	inspection := codexStatusMarketplaceInspection{}
	cachePresent, cacheErr := codexPluginCachePresent(configPath)
	if cacheErr != nil {
		inspection.Check = codexStatusCheck(
			"marketplace", CodexCheckUnavailable, "marketplace_inspection_failed",
			"The Engram marketplace state could not be inspected.", codexEvidence("config_path", configPath),
		)
		return inspection
	}
	inspection.CachePresent = cachePresent

	data, err := readFileFn(configPath)
	if os.IsNotExist(err) {
		inspection.Check = codexStatusCheck(
			"marketplace", CodexCheckMissing, "marketplace_missing",
			"The Engram marketplace is not registered.", codexEvidence("config_path", configPath),
		)
		return inspection
	}
	if err != nil {
		inspection.Check = codexStatusCheck(
			"marketplace", CodexCheckUnavailable, "marketplace_inspection_failed",
			"The Codex configuration could not be read.", codexEvidence("config_path", configPath),
		)
		return inspection
	}

	content := string(data)
	marketplace, marketplacePresent, marketplaceValid := codexTOMLTable(content, "marketplaces.engram")
	plugin, pluginPresent, pluginValid := codexTOMLTable(content, `plugins."engram@engram"`)
	inspection.Present = marketplacePresent
	inspection.PluginPresent = pluginPresent
	if pluginPresent && pluginValid && len(plugin) == 1 {
		switch strings.TrimSpace(plugin["enabled"]) {
		case "true":
			inspection.PluginConfigAttributable = true
			inspection.PluginEnabled = true
		case "false":
			inspection.PluginConfigAttributable = true
		}
	}

	if !marketplacePresent {
		inspection.Check = codexStatusCheck(
			"marketplace", CodexCheckMissing, "marketplace_missing",
			"The Engram marketplace is not registered.", codexEvidence("config_path", configPath),
		)
		return inspection
	}

	sourceType, sourceTypeOK := decodeTOMLString(marketplace["source_type"])
	source, sourceOK := decodeTOMLString(marketplace["source"])
	ref, refOK := decodeTOMLString(marketplace["ref"])
	inspection.Ref = ref
	inspection.Attributable = marketplaceValid && len(marketplace) == 3 && sourceTypeOK && sourceOK && refOK &&
		(ref == "main" || semver.IsValid(ref)) && sourceType == "git" &&
		source == "https://github.com/yersonargotev/engram.git"
	if !inspection.Attributable {
		inspection.Check = codexStatusCheck(
			"marketplace", CodexCheckCustomized, "marketplace_customized",
			"An Engram-named marketplace registration exists but does not match the supported contract.",
			codexEvidence("config_path", configPath),
		)
		return inspection
	}

	inspection.Check = codexStatusCheck(
		"marketplace", CodexCheckReady, "marketplace_registered",
		"The Engram marketplace is registered with attributable provenance.",
		codexEvidence("config_path", configPath),
		codexEvidence("source", "https://github.com/yersonargotev/engram.git"),
		codexEvidence("requested_ref", ref),
	)
	return inspection
}

func inspectCodexStandaloneSkills(workingDirectory, configPath string) []CodexIntegrationCheck {
	type skillRoot struct {
		path  string
		scope string
	}
	roots := make([]skillRoot, 0, 5)
	for _, path := range codexRepositorySkillRoots(workingDirectory) {
		roots = append(roots, skillRoot{path: path, scope: "repo"})
	}
	if home, err := userHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		roots = append(roots, skillRoot{path: filepath.Join(home, ".agents", "skills"), scope: "user"})
	}
	if admin := strings.TrimSpace(codexAdminSkillsDirFn()); admin != "" {
		roots = append(roots, skillRoot{path: admin, scope: "admin"})
	}

	disabledPaths := codexDisabledSkillPaths(configPath)
	var checks []CodexIntegrationCheck
	for _, root := range roots {
		entries, err := os.ReadDir(root.path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			discoveredPath := filepath.Join(root.path, entry.Name(), "SKILL.md")
			resolvedPath, err := filepath.EvalSymlinks(discoveredPath)
			if err != nil {
				continue
			}
			content, err := readFileFn(resolvedPath)
			if err != nil {
				continue
			}
			name, version, ok := codexEngramSkillIdentity(string(content))
			if !ok {
				continue
			}
			digest := sha256.Sum256(content)
			evidence := []CodexIntegrationEvidence{
				codexEvidence("scope", root.scope),
				codexEvidence("source", "standalone"),
				codexEvidence("path", filepath.Clean(resolvedPath)),
				codexEvidence("name", name),
				codexEvidence("sha256", hex.EncodeToString(digest[:])),
			}
			if version != "" {
				evidence = append(evidence, codexEvidence("version", version))
			}
			status := CodexCheckReady
			reasonCode := "engram_skill_discovered"
			reason := "A standalone Engram memory skill is discoverable by Codex."
			if _, disabled := disabledPaths[filepath.Clean(resolvedPath)]; disabled {
				status = CodexCheckPartial
				reasonCode = "engram_skill_disabled"
				reason = "An Engram memory skill exists but is disabled in Codex configuration."
				evidence = append(evidence, codexEvidence("enabled", "false"))
			}
			checks = append(checks, codexStatusCheck(
				"skill", status, reasonCode, reason, evidence...,
			))
		}
	}
	if len(checks) == 0 {
		return []CodexIntegrationCheck{codexStatusCheck(
			"skill", CodexCheckMissing, "engram_skill_missing",
			"No standalone Engram memory skill was discovered for Codex.",
		)}
	}
	return checks
}

func codexDisabledSkillPaths(configPath string) map[string]struct{} {
	disabled := make(map[string]struct{})
	data, err := readFileFn(configPath)
	if err != nil {
		return disabled
	}

	inSkillConfig := false
	configuredPath := ""
	enabled := ""
	flush := func() {
		if !inSkillConfig || strings.TrimSpace(enabled) != "false" {
			configuredPath = ""
			enabled = ""
			return
		}
		path, ok := decodeTOMLString(configuredPath)
		if !ok || strings.TrimSpace(path) == "" {
			configuredPath = ""
			enabled = ""
			return
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(filepath.Dir(configPath), path)
		}
		if filepath.Base(path) != "SKILL.md" {
			path = filepath.Join(path, "SKILL.md")
		}
		path = filepath.Clean(path)
		if resolved, resolveErr := filepath.EvalSymlinks(path); resolveErr == nil {
			path = filepath.Clean(resolved)
		}
		disabled[path] = struct{}{}
		configuredPath = ""
		enabled = ""
	}

	for _, line := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") {
			flush()
			inSkillConfig = trimmed == "[[skills.config]]"
			continue
		}
		if !inSkillConfig || trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 {
			continue
		}
		switch strings.TrimSpace(parts[0]) {
		case "path":
			configuredPath = strings.TrimSpace(parts[1])
		case "enabled":
			enabled = strings.TrimSpace(parts[1])
		}
	}
	flush()
	return disabled
}

func codexRepositorySkillRoots(workingDirectory string) []string {
	workingDirectory = strings.TrimSpace(workingDirectory)
	if workingDirectory == "" {
		return nil
	}
	abs, err := filepath.Abs(workingDirectory)
	if err != nil {
		return nil
	}
	abs = filepath.Clean(abs)

	repoRoot := ""
	for current := abs; ; current = filepath.Dir(current) {
		if _, err := statFn(filepath.Join(current, ".git")); err == nil {
			repoRoot = current
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	if repoRoot == "" {
		return []string{filepath.Join(abs, ".agents", "skills")}
	}

	var roots []string
	for current := abs; ; current = filepath.Dir(current) {
		roots = append(roots, filepath.Join(current, ".agents", "skills"))
		if current == repoRoot {
			break
		}
	}
	return roots
}

func codexEngramSkillIdentity(content string) (name, version string, ok bool) {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return "", "", false
	}
	for _, line := range lines[1:] {
		trimmed := strings.TrimSpace(line)
		if trimmed == "---" {
			break
		}
		parts := strings.SplitN(trimmed, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if decoded, err := strconv.Unquote(value); err == nil {
			value = decoded
		}
		switch key {
		case "name":
			name = value
		case "version":
			version = value
		}
	}
	return name, version, name == "engram-memory" || strings.HasPrefix(name, "engram-memory-")
}

type codexPluginInspection struct {
	Check         CodexIntegrationCheck
	Capabilities  installedCodexPlugin
	InstalledPath string
	Revision      string
	Skill         *CodexIntegrationCheck
}

func inspectCodexPluginStatus(codexPath, configPath string, marketplace codexStatusMarketplaceInspection) codexPluginInspection {
	result := func(check CodexIntegrationCheck) codexPluginInspection {
		return codexPluginInspection{Check: check}
	}
	if marketplace.Check.Status == CodexCheckUnavailable {
		return result(codexStatusCheck(
			"plugin", CodexCheckUnavailable, "plugin_configuration_unreadable",
			"The plugin state cannot be inspected because Codex configuration is unavailable.",
		))
	}
	if !marketplace.Attributable {
		if !marketplace.Present && !marketplace.PluginPresent && !marketplace.CachePresent {
			return result(codexStatusCheck(
				"plugin", CodexCheckMissing, "plugin_missing",
				"The Engram Codex plugin is not installed and enabled.",
			))
		}
		return result(codexStatusCheck(
			"plugin", CodexCheckCustomized, "plugin_state_customized",
			"The Engram-named plugin state is customized or cannot be attributed safely.",
		))
	}
	if !marketplace.PluginPresent {
		if marketplace.CachePresent {
			return result(codexStatusCheck(
				"plugin", CodexCheckCustomized, "plugin_cache_unattributed",
				"An Engram plugin cache exists without attributable plugin configuration.",
			))
		}
		return result(codexStatusCheck(
			"plugin", CodexCheckMissing, "plugin_missing",
			"The Engram Codex plugin is not installed and enabled.",
		))
	}
	if !marketplace.PluginConfigAttributable {
		return result(codexStatusCheck(
			"plugin", CodexCheckCustomized, "plugin_configuration_customized",
			"The Engram plugin configuration does not match the supported enabled-state contract.",
		))
	}
	if codexPath == "" {
		return result(codexStatusCheck(
			"plugin", CodexCheckUnavailable, "codex_cli_missing",
			"The installed plugin state cannot be verified without the Codex CLI.",
			codexEvidence("configured_enabled", strconv.FormatBool(marketplace.PluginEnabled)),
		))
	}

	output, err := runCommand(codexPath, "plugin", "list", "--json")
	if err != nil {
		return result(codexStatusCheck(
			"plugin", CodexCheckUnavailable, "plugin_list_failed",
			"The Codex plugin inventory could not be read.",
		))
	}
	var listed codexPluginListResult
	if err := json.Unmarshal(output, &listed); err != nil {
		return result(codexStatusCheck(
			"plugin", CodexCheckInvalid, "plugin_list_invalid",
			"The Codex plugin inventory returned invalid JSON.",
		))
	}
	var match *codexListedPlugin
	for i := range listed.Installed {
		candidate := &listed.Installed[i]
		if candidate.PluginID != "engram@engram" {
			continue
		}
		if match != nil {
			return result(codexStatusCheck(
				"plugin", CodexCheckInvalid, "plugin_duplicate",
				"The Codex plugin inventory contains duplicate Engram installations.",
			))
		}
		match = candidate
	}
	if match == nil {
		reasonCode := "plugin_not_listed"
		reason := "Codex configuration enables the Engram plugin, but the runtime inventory does not list it."
		if !marketplace.PluginEnabled {
			reasonCode = "plugin_disabled_not_listed"
			reason = "Codex configuration disables the Engram plugin, and the runtime inventory does not list an installation."
		}
		return result(codexStatusCheck(
			"plugin", CodexCheckPartial, reasonCode, reason,
			codexEvidence("configured_enabled", strconv.FormatBool(marketplace.PluginEnabled)),
		))
	}
	if match.Name != "engram" || match.MarketplaceName != "engram" ||
		match.Source.Source != "local" || !semver.IsValid("v"+match.Version) ||
		match.MarketplaceSource.SourceType != "git" ||
		match.MarketplaceSource.Source != "https://github.com/yersonargotev/engram.git" {
		return result(codexStatusCheck(
			"plugin", CodexCheckCustomized, "plugin_provenance_unrecognized",
			"The installed Engram plugin does not have attributable supported provenance.",
			codexEvidence("plugin_id", match.PluginID),
		))
	}
	sourcePath := filepath.Clean(match.Source.Path)
	marketplaceRoot := filepath.Dir(filepath.Dir(sourcePath))
	if sourcePath != filepath.Join(marketplaceRoot, "plugin", "codex") {
		return result(codexStatusCheck(
			"plugin", CodexCheckCustomized, "plugin_source_layout_unrecognized",
			"The Engram plugin source is outside the supported marketplace layout.",
		))
	}
	identity, err := verifyCodexMarketplaceRoot(marketplaceRoot, "")
	if err != nil {
		return result(codexStatusCheck(
			"plugin", CodexCheckCustomized, "marketplace_revision_unverified",
			"The plugin marketplace revision could not be attributed safely.",
		))
	}
	resolvedRef, err := gitResolveRefFn(marketplaceRoot, marketplace.Ref)
	if err != nil {
		return result(codexStatusCheck(
			"plugin", CodexCheckCustomized, "marketplace_ref_unresolved",
			"The configured marketplace ref could not be resolved locally.",
		))
	}
	configuredRevision, err := normalizeGitCommit(strings.TrimSpace(string(resolvedRef)))
	if err != nil || configuredRevision != identity.Commit {
		return result(codexStatusCheck(
			"plugin", CodexCheckCustomized, "marketplace_ref_mismatch",
			"The configured marketplace ref does not match the checked-out revision.",
		))
	}
	verifiedAssets, err := snapshotVerifiedCodexMarketplacePlugin(marketplaceRoot, identity.Commit)
	if err != nil {
		return codexPluginInspection{
			Check: codexStatusCheck(
				"plugin", CodexCheckCustomized, "marketplace_assets_unverified",
				"The marketplace plugin assets could not be verified byte-for-byte.",
				codexEvidence("plugin_id", match.PluginID),
				codexEvidence("installed", strconv.FormatBool(match.Installed)),
				codexEvidence("enabled", strconv.FormatBool(match.Enabled)),
				codexEvidence("installed_version", match.Version),
				codexEvidence("installed_revision", identity.Commit),
			),
			Revision: identity.Commit,
		}
	}
	installedPath := filepath.Join(filepath.Dir(configPath), "plugins", "cache", "engram", "engram", match.Version)
	evidence := []CodexIntegrationEvidence{
		codexEvidence("plugin_id", match.PluginID),
		codexEvidence("configured_enabled", strconv.FormatBool(marketplace.PluginEnabled)),
		codexEvidence("installed", strconv.FormatBool(match.Installed)),
		codexEvidence("enabled", strconv.FormatBool(match.Enabled)),
		codexEvidence("source", "https://github.com/yersonargotev/engram.git"),
		codexEvidence("source_path", sourcePath),
		codexEvidence("installed_path", installedPath),
		codexEvidence("installed_version", match.Version),
		codexEvidence("installed_revision", identity.Commit),
		codexEvidence("requested_ref", marketplace.Ref),
	}
	if !match.Installed {
		return codexPluginInspection{
			Check: codexStatusCheck(
				"plugin", CodexCheckPartial, "plugin_not_installed",
				"Codex knows the attributable Engram plugin, but it is not installed.", evidence...,
			),
			Revision: identity.Commit,
		}
	}
	capabilities, err := verifyCodexPluginAtLocation(match.Version, installedPath, verifiedAssets)
	if err != nil {
		return codexPluginInspection{
			Check: codexStatusCheck(
				"plugin", CodexCheckCustomized, "plugin_assets_unverified",
				"The installed plugin does not match its attributable marketplace source.", evidence...,
			),
			InstalledPath: installedPath,
			Revision:      identity.Commit,
		}
	}

	inspection := codexPluginInspection{
		Capabilities:  capabilities,
		InstalledPath: installedPath,
		Revision:      identity.Commit,
	}
	switch {
	case match.Enabled != marketplace.PluginEnabled:
		inspection.Check = codexStatusCheck(
			"plugin", CodexCheckPartial, "plugin_enablement_mismatch",
			"Codex configuration and runtime inventory disagree about whether the plugin is enabled.", evidence...,
		)
	case !match.Enabled:
		inspection.Check = codexStatusCheck(
			"plugin", CodexCheckPartial, "plugin_disabled",
			"The attributable Engram plugin is installed but disabled.", evidence...,
		)
	default:
		inspection.Check = codexStatusCheck(
			"plugin", CodexCheckReady, "plugin_ready",
			"One enabled Engram plugin matches its attributable marketplace source.", evidence...,
		)
	}
	if skill := inspectCodexPluginSkill(installedPath, match.Enabled); skill != nil {
		inspection.Skill = skill
	}
	return inspection
}

func inspectCodexPluginSkill(installedPath string, enabled bool) *CodexIntegrationCheck {
	path := filepath.Join(installedPath, "skills", "memory", "SKILL.md")
	content, err := readFileFn(path)
	if err != nil {
		return nil
	}
	name, version, ok := codexEngramSkillIdentity(string(content))
	if !ok {
		return nil
	}
	digest := sha256.Sum256(content)
	evidence := []CodexIntegrationEvidence{
		codexEvidence("scope", "plugin"),
		codexEvidence("source", "plugin"),
		codexEvidence("path", filepath.Clean(path)),
		codexEvidence("name", name),
		codexEvidence("sha256", hex.EncodeToString(digest[:])),
	}
	if version != "" {
		evidence = append(evidence, codexEvidence("version", version))
	}
	status := CodexCheckReady
	reasonCode := "engram_plugin_skill_discovered"
	reason := "The installed Engram plugin provides its canonical memory skill."
	if !enabled {
		status = CodexCheckPartial
		reasonCode = "engram_plugin_skill_disabled"
		reason = "The installed Engram plugin provides its canonical memory skill, but the plugin is disabled."
		evidence = append(evidence, codexEvidence("enabled", "false"))
	}
	check := codexStatusCheck(
		"skill", status, reasonCode, reason, evidence...,
	)
	return &check
}

func inspectCodexMCPReadiness(configuration CodexIntegrationCheck) CodexIntegrationCheck {
	if configuration.Status != CodexCheckReady {
		status := CodexCheckMissing
		if configuration.Status != CodexCheckMissing {
			status = CodexCheckUnavailable
		}
		return codexStatusCheck(
			"mcp_readiness", status, "mcp_not_configured",
			"MCP readiness cannot be established without an attributable configuration.",
		)
	}
	command := codexStatusEvidenceValue(configuration, "command")
	resolved, err := lookPathFn(command)
	if err != nil || strings.TrimSpace(resolved) == "" {
		return codexStatusCheck(
			"mcp_readiness", CodexCheckUnavailable, "mcp_executable_missing",
			"The configured Engram MCP executable cannot be resolved.", codexEvidence("command", command),
		)
	}
	ready, detail := codexCheckpointAdaptersReadyFor(resolved)
	if !ready {
		return codexStatusCheck(
			"mcp_readiness", CodexCheckUnavailable, "mcp_adapter_unavailable",
			detail,
			codexEvidence("command", command),
			codexEvidence("resolved_path", resolved),
			codexEvidence("transport", "stdio"),
		)
	}
	return codexStatusCheck(
		"mcp_readiness", CodexCheckReady, "mcp_adapter_ready",
		"The configured MCP executable resolves and exposes the checkpoint CLI contract.",
		codexEvidence("command", command),
		codexEvidence("resolved_path", resolved),
		codexEvidence("transport", "stdio"),
	)
}

func codexPluginCapabilityCheck(plugin codexPluginInspection, capability string, ready bool) CodexIntegrationCheck {
	if plugin.Check.Status != CodexCheckReady {
		status := CodexCheckMissing
		reasonCode := "plugin_missing"
		reason := "The capability is unavailable because the Engram plugin is not installed and ready."
		if plugin.Check.Status != CodexCheckMissing {
			status = CodexCheckUnavailable
			reasonCode = "plugin_not_ready"
			reason = "The capability cannot be verified until the Engram plugin is attributable and ready."
		}
		return codexStatusCheck(capability, status, reasonCode, reason)
	}
	if !ready {
		return codexStatusCheck(
			capability, CodexCheckMissing, capability+"_not_ready",
			fmt.Sprintf("The installed plugin does not satisfy the canonical %s contract.", strings.ReplaceAll(capability, "_", " ")),
			codexEvidence("installed_path", plugin.InstalledPath),
		)
	}
	return codexStatusCheck(
		capability, CodexCheckReady, capability+"_ready",
		fmt.Sprintf("The installed plugin satisfies the canonical %s contract.", strings.ReplaceAll(capability, "_", " ")),
		codexEvidence("installed_path", plugin.InstalledPath),
	)
}

func codexStatusEvidenceValue(check CodexIntegrationCheck, name string) string {
	for _, evidence := range check.Evidence {
		if evidence.Name == name {
			return evidence.Value
		}
	}
	return ""
}

func inspectCodexMCPConfiguration(configPath string) CodexIntegrationCheck {
	data, err := readFileFn(configPath)
	if os.IsNotExist(err) {
		return codexStatusCheck(
			"mcp_configuration", CodexCheckMissing, "mcp_configuration_missing",
			"No Engram MCP registration was found.", codexEvidence("config_path", configPath),
		)
	}
	if err != nil {
		return codexStatusCheck(
			"mcp_configuration", CodexCheckUnavailable, "mcp_configuration_unreadable",
			"The Codex configuration could not be read.", codexEvidence("config_path", configPath),
		)
	}
	start, end, found := tomlSectionBounds(string(data), "mcp_servers.engram")
	if !found {
		return codexStatusCheck(
			"mcp_configuration", CodexCheckMissing, "mcp_configuration_missing",
			"No Engram MCP registration was found.", codexEvidence("config_path", configPath),
		)
	}
	section := string(data[start:end])
	values, present, valid := codexTOMLTable(section, "mcp_servers.engram")
	command, commandOK := decodeTOMLString(values["command"])
	var args []string
	argsOK := json.Unmarshal([]byte(values["args"]), &args) == nil
	if !present || !valid || !commandOK || !argsOK {
		return codexStatusCheck(
			"mcp_configuration", CodexCheckInvalid, "mcp_configuration_invalid",
			"The Engram MCP registration is present but syntactically invalid.",
			codexEvidence("config_path", configPath),
		)
	}
	if !codexMCPSectionOwned(section) {
		return codexStatusCheck(
			"mcp_configuration", CodexCheckCustomized, "mcp_configuration_customized",
			"An Engram-named MCP registration exists but does not match the supported contract.",
			codexEvidence("config_path", configPath),
		)
	}
	return codexStatusCheck(
		"mcp_configuration", CodexCheckReady, "mcp_configuration_ready",
		"The Engram MCP registration matches the supported contract.",
		codexEvidence("config_path", configPath),
		codexEvidence("command", command),
		codexEvidence("transport", "stdio"),
	)
}

func deriveCodexOperatingMode(checks []CodexIntegrationCheck) CodexOperatingMode {
	ready := func(capability string) bool {
		for _, check := range checks {
			if check.Capability == capability && check.Status == CodexCheckReady {
				return true
			}
		}
		return false
	}
	checkpointCapabilities := []string{"engram_cli", "codex_cli", "plugin", "mcp_configuration", "mcp_readiness", "prompt_hook", "session_hook", "activation_cue", "stop_verifier"}
	checkpointReady := true
	for _, capability := range checkpointCapabilities {
		checkpointReady = checkpointReady && ready(capability)
	}
	if checkpointReady {
		return CodexModeCheckpointReady
	}
	if ready("plugin") || (ready("marketplace") && codexCapabilityPresent(checks, "plugin")) {
		return CodexModePartialPlugin
	}
	if ready("mcp_configuration") && ready("mcp_readiness") {
		return CodexModeMCPOnly
	}
	if ready("engram_cli") && ready("codex_cli") && ready("skill") && !ready("plugin") && !ready("mcp_configuration") {
		return CodexModeManualSkillCLI
	}
	return CodexModeUnknown
}

func codexCapabilityPresent(checks []CodexIntegrationCheck, capability string) bool {
	for _, check := range checks {
		if check.Capability == capability && check.Status != CodexCheckMissing {
			return true
		}
	}
	return false
}

func codexEvidence(name, value string) CodexIntegrationEvidence {
	return CodexIntegrationEvidence{Name: name, Value: value}
}

func codexStatusCheck(capability string, status CodexIntegrationCheckStatus, reasonCode, reason string, evidence ...CodexIntegrationEvidence) CodexIntegrationCheck {
	items := make([]CodexIntegrationEvidence, 0, len(evidence))
	for _, item := range evidence {
		items = append(items, CodexIntegrationEvidence{
			Name:  boundedCodexStatusText(item.Name, 64),
			Value: boundedCodexStatusText(item.Value, 512),
		})
	}
	return CodexIntegrationCheck{
		Capability: boundedCodexStatusText(capability, 64),
		Status:     status,
		ReasonCode: boundedCodexStatusText(reasonCode, 96),
		Reason:     boundedCodexStatusText(reason, 512),
		Evidence:   items,
	}
}

func boundedCodexStatusText(value string, limit int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
