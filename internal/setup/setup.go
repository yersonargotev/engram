// Package setup handles agent plugin installation.
//
//   - OpenCode: copies embedded plugin file to ~/.config/opencode/plugins/
//     (patching ENGRAM_BIN to bake in the absolute binary path as a final
//     fallback) and injects MCP registration in opencode.json using the
//     resolved absolute binary path so child processes never require PATH
//     resolution in headless/systemd environments.
//   - Claude Code: runs `claude plugin marketplace add` + `claude plugin install`,
//     then writes a durable MCP config to ~/.claude/mcp/engram.json using the
//     absolute binary path so the subprocess never needs PATH resolution.
//   - Gemini CLI: injects MCP registration in ~/.gemini/settings.json
//   - Codex: injects MCP registration in ~/.codex/config.toml
//   - Pi: installs gentle-engram/pi-mcp-adapter packages and writes Pi MCP config
package setup

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	atomicfile "github.com/natefinch/atomic"
	"github.com/yersonargotev/engram/internal/mcp"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

var (
	runtimeGOOS  = runtime.GOOS
	userHomeDir  = os.UserHomeDir
	lookPathFn   = exec.LookPath
	osExecutable = os.Executable
	runCommand   = func(name string, args ...string) ([]byte, error) {
		return exec.Command(name, args...).CombinedOutput()
	}
	gitStatusFn = func(root string) ([]byte, error) {
		return exec.Command("git", "-C", root, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching", "--", ".agents/plugins/marketplace.json", "plugin/codex").CombinedOutput()
	}
	gitResolveRefFn = func(root, ref string) ([]byte, error) {
		return exec.Command("git", "-C", root, "rev-parse", "--verify", ref+"^{commit}").CombinedOutput()
	}
	openCodeReadFile = func(path string) ([]byte, error) {
		return openCodeFS.ReadFile(path)
	}
	statFn                    = os.Stat
	openCodeWriteFileFn       = os.WriteFile
	readFileFn                = os.ReadFile
	writeFileFn               = os.WriteFile
	atomicWriteFileFn         = writeFileAtomic
	jsonMarshalFn             = json.Marshal
	jsonMarshalIndentFn       = json.MarshalIndent
	injectOpenCodeMCPFn       = injectOpenCodeMCP
	injectOpenCodeTUIPluginFn = injectOpenCodeTUIPlugin
	injectGeminiMCPFn         = injectGeminiMCP
	writeGeminiSystemPromptFn = writeGeminiSystemPrompt
	injectCodexMCPFn          = injectCodexMCP
	addClaudeCodeAllowlistFn  = AddClaudeCodeAllowlist
	writeClaudeCodeUserMCPFn  = writeClaudeCodeUserMCP

	// resolveMiseNodeVersionFn resolves the active Node version managed by mise.
	// It runs "mise current node" and returns the result as a "node@X.Y.Z" specifier.
	// Returns an empty string when the version cannot be determined.
	resolveMiseNodeVersionFn = resolveMiseNodeVersion
)

//go:embed plugins/opencode/*
var openCodeFS embed.FS

// Agent represents a supported AI coding agent.
type Agent struct {
	Name        string
	Description string
	InstallDir  string // resolved at runtime (display only for claude-code)
}

// Result holds the outcome of an installation.
type Result struct {
	Agent            string
	Destination      string
	Files            int
	TUIPluginEnabled bool
	Complete         bool
	Checks           []CapabilityCheck
	Preserved        []string
}

// CheckStatus describes one independently observable setup capability.
type CheckStatus string

const (
	CheckReady     CheckStatus = "ready"
	CheckMissing   CheckStatus = "missing"
	CheckPreserved CheckStatus = "preserved"
	CheckFailed    CheckStatus = "failed"
)

// CapabilityCheck reports whether one setup capability is ready without
// collapsing partial installation into a misleading success state.
type CapabilityCheck struct {
	Capability string
	Status     CheckStatus
	Detail     string
}

// Check returns the named capability check when the installer reported it.
func (r *Result) Check(capability string) (CapabilityCheck, bool) {
	for _, check := range r.Checks {
		if check.Capability == capability {
			return check, true
		}
	}
	return CapabilityCheck{}, false
}

// InstallOptions describes the release identity used by setup adapters that
// install Git-backed assets. Stable setup requires both Version and Commit;
// Development explicitly opts into the moving main branch.
type InstallOptions struct {
	Version     string
	Commit      string
	Development bool
}

const claudeCodeMarketplace = "yersonargotev/engram"
const codexMarketplace = "yersonargotev/engram"

const openCodeSubagentStatuslinePlugin = "opencode-subagent-statusline"

const piGentleEngramPackage = "npm:gentle-engram@0.1.8"
const piMCPAdapterPackage = "npm:pi-mcp-adapter"

// claudeCodeMCPTools are the MCP tool permission names for the agent profile
// registered by the engram Claude Code plugin and durable user-level MCP config.
// Adding these to ~/.claude/settings.json permissions.allow prevents Claude Code
// from prompting for confirmation on every tool call.
var claudeCodeMCPTools = claudeCodePermissionTools(mcp.ResolveTools("agent"))

func claudeCodePermissionTools(agentTools map[string]bool) []string {
	toolNames := make([]string, 0, len(agentTools))
	for toolName, enabled := range agentTools {
		if enabled {
			toolNames = append(toolNames, toolName)
		}
	}
	sort.Strings(toolNames)

	// Claude Code's bare/user-level MCP config uses the server id "engram".
	// Older plugin installs have been observed with a plugin-scoped server id;
	// allowlisting both forms is harmless and keeps re-running setup idempotent.
	prefixes := []string{"mcp__engram__", "mcp__plugin_engram_engram__"}
	permissions := make([]string, 0, len(toolNames)*len(prefixes))
	for _, prefix := range prefixes {
		for _, toolName := range toolNames {
			permissions = append(permissions, prefix+toolName)
		}
	}
	return permissions
}

// codexEngramBlock is the canonical Codex TOML MCP block.
// Command is always the bare "engram" name in this constant because
// upsertCodexEngramBlock generates the actual content via codexEngramBlockStr()
// which uses resolveEngramCommand() at runtime. This constant is kept for tests
// that verify idempotency against the already-written string when os.Executable
// returns "engram" (fallback path).
const codexEngramBlock = "[mcp_servers.engram]\ncommand = \"engram\"\nargs = [\"mcp\", \"--tools=agent\"]"

// codexEngramBlockStr returns the Codex TOML block for the engram MCP server,
// using the resolved absolute binary path from os.Executable().
func codexEngramBlockStr() string {
	cmd := resolveEngramCommand()
	return "[mcp_servers.engram]\ncommand = " + fmt.Sprintf("%q", cmd) + "\nargs = [\"mcp\", \"--tools=agent\"]"
}

const memoryProtocolMarkdown = `## Engram Persistent Memory — Protocol

You have access to Engram, a persistent memory system that survives across sessions and compactions.

### WHEN TO SAVE (mandatory — not optional)

Call mem_save IMMEDIATELY after any of these:
- Bug fix completed
- Architecture or design decision made
- Non-obvious discovery about the codebase
- Configuration change or environment setup
- Pattern established (naming, structure, convention)
- User preference or constraint learned

Format for mem_save:
- **title**: Verb + what — short, searchable (e.g. "Fixed N+1 query in UserList", "Chose Zustand over Redux")
- **type**: bugfix | decision | architecture | discovery | pattern | config | preference
- **scope**: project (default) | personal
- **topic_key** (optional, recommended for evolving decisions): stable key like architecture/auth-model
- **content**:
  **What**: One sentence — what was done
  **Why**: What motivated it (user request, bug, performance, etc.)
  **Where**: Files or paths affected
  **Learned**: Gotchas, edge cases, things that surprised you (omit if none)

### Topic update rules (mandatory)

- Different topics must not overwrite each other (e.g. architecture vs bugfix)
- Reuse the same topic_key to update an evolving topic instead of creating new observations
- If unsure about the key, call mem_suggest_topic_key first and then reuse it
- Use mem_update when you have an exact observation ID to correct

### WHEN TO SEARCH MEMORY

When the user asks to recall something — any variation of "remember", "recall", "what did we do",
"how did we solve", "recordar", "acordate", "qué hicimos", or references to past work:
1. First call mem_context — checks recent session history (fast, cheap)
2. If not found, call mem_search with relevant keywords (FTS5 full-text search)
3. If you find a match, use mem_get_observation for full untruncated content

Also search memory PROACTIVELY when:
- Starting work on something that might have been done before
- The user mentions a topic you have no context on — check if past sessions covered it

### SESSION CLOSE PROTOCOL (mandatory)

Before ending a session or saying "done" / "listo" / "that's it", you MUST:
1. Call mem_session_summary with this structure:

## Goal
[What we were working on this session]

## Instructions
[User preferences or constraints discovered — skip if none]

## Discoveries
- [Technical findings, gotchas, non-obvious learnings]

## Accomplished
- [Completed items with key details]

## Next Steps
- [What remains to be done — for the next session]

## Relevant Files
- path/to/file — [what it does or what changed]

This is NOT optional. If you skip this, the next session starts blind.

### PASSIVE CAPTURE — automatic learning extraction

When completing a task or subtask, include a "## Key Learnings:" section at the end of your response
with numbered items. Engram will automatically extract and save these as observations.

Example:
## Key Learnings:

1. bcrypt cost=12 is the right balance for our server performance
2. JWT refresh tokens need atomic rotation to prevent race conditions

You can also call mem_capture_passive(content) directly with any text that contains a learning section.
This is a safety net — it captures knowledge even if you forget to call mem_save explicitly.

### AFTER COMPACTION

If you see a message about compaction or context reset, or if you see "FIRST ACTION REQUIRED" in your context:
1. IMMEDIATELY call mem_session_summary with the compacted summary content — this persists what was done before compaction
2. Then call mem_context to recover any additional context from previous sessions
3. Only THEN continue working

Do not skip step 1. Without it, everything done before compaction is lost from memory.
`

const codexCompactPromptMarkdown = `You are compacting a coding session that uses Engram persistent memory.

You MUST prepend this exact sentence at the top of the compacted summary:

FIRST ACTION REQUIRED: Call mem_session_summary with the content of this compacted summary before doing anything else, then call mem_context.

After that sentence, summarize:
- Goal
- Key technical discoveries and decisions
- Completed work
- Remaining next steps
- Relevant files changed

Keep it concise and high-signal.`

// SupportedAgents returns the list of agents that have plugins available.
// The list is derived from the registry (agentAdapters) so adding an agent there
// surfaces it here and in `engram setup --help` automatically.
func SupportedAgents() []Agent {
	adapters := agentAdapters()
	agents := make([]Agent, 0, len(adapters))
	for _, a := range adapters {
		agents = append(agents, Agent{
			Name:        a.slug,
			Description: a.description,
			InstallDir:  a.displayDir(),
		})
	}
	return agents
}

// Install installs the plugin for the given agent by looking it up in the
// registry and running its adapter (a bespoke installer or the generic driver).
func Install(agentName string) (*Result, error) {
	return InstallWithOptions(agentName, InstallOptions{})
}

// InstallWithOptions installs an agent integration using the supplied build
// identity. Non-Git-backed adapters ignore the release-specific options.
func InstallWithOptions(agentName string, options InstallOptions) (*Result, error) {
	for _, a := range agentAdapters() {
		if a.slug == agentName {
			return installFromAdapter(a, options)
		}
	}
	return nil, fmt.Errorf("unknown agent: %q (supported: %s)", agentName, strings.Join(supportedSlugs(), ", "))
}

// ─── Pi ──────────────────────────────────────────────────────────────────────

func piAgentDir() string {
	if dir := strings.TrimSpace(os.Getenv("PI_CODING_AGENT_DIR")); dir != "" {
		return dir
	}
	home, err := userHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return filepath.Join(".pi", "agent")
	}
	return filepath.Join(home, ".pi", "agent")
}

func installPi() (*Result, error) {
	if _, err := runCommand("pi", "install", piGentleEngramPackage); err != nil {
		return nil, fmt.Errorf("install %s: %w", piGentleEngramPackage, err)
	}
	if _, err := runCommand("pi", "install", piMCPAdapterPackage); err != nil {
		return nil, fmt.Errorf("install %s: %w", piMCPAdapterPackage, err)
	}

	agentDir := piAgentDir()
	settingsPath := filepath.Join(agentDir, "settings.json")
	files := 0

	// ensurePiNpmCommand must run before ensurePiPackageSettings so that a single
	// write covers both npm command pinning and package list updates when both are
	// needed on a fresh install. If npmCommand was already set we still proceed and
	// let ensurePiPackageSettings handle the packages field independently.
	npmChanged, err := ensurePiNpmCommand(settingsPath)
	if err != nil {
		return nil, err
	}

	settingsChanged, err := ensurePiPackageSettings(settingsPath)
	if err != nil {
		return nil, err
	}
	if npmChanged || settingsChanged {
		files++
	}

	mcpChanged, err := ensurePiMCPConfig(filepath.Join(agentDir, "mcp.json"))
	if err != nil {
		return nil, err
	}
	if mcpChanged {
		files++
	}

	return &Result{Agent: "pi", Destination: agentDir, Files: files}, nil
}

func ensurePiPackageSettings(settingsPath string) (bool, error) {
	config, err := readJSONConfig(settingsPath)
	if err != nil {
		return false, fmt.Errorf("read Pi settings: %w", err)
	}
	packages, err := readRawArrayField(config, "packages", settingsPath)
	if err != nil {
		return false, err
	}
	changed := false
	for _, pkg := range []string{piGentleEngramPackage, piMCPAdapterPackage} {
		if !rawArrayContainsString(packages, pkg) {
			raw, err := jsonMarshalFn(pkg)
			if err != nil {
				return false, fmt.Errorf("marshal Pi package %q: %w", pkg, err)
			}
			packages = append(packages, raw)
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	config["packages"], err = jsonMarshalFn(packages)
	if err != nil {
		return false, fmt.Errorf("marshal Pi packages: %w", err)
	}
	return true, writeJSONConfig(settingsPath, config)
}

// ensurePiNpmCommand pins the npm command in Pi's settings.json when mise is
// detected. This prevents Node version drift from silently changing which npm
// root Pi uses for package lookups and installs.
//
// Behavior:
//   - If mise is not found in PATH: no-op (returns false, nil).
//   - If npmCommand already exists in settings.json: no-op (returns false, nil).
//   - Otherwise: writes npmCommand as ["mise", "exec", "<node-spec>", "--", "npm"].
//
// The node spec is resolved via "mise current node". If resolution fails,
// the bare "node" tool name is used so mise still picks the active version.
func ensurePiNpmCommand(settingsPath string) (bool, error) {
	if _, err := lookPathFn("mise"); err != nil {
		return false, nil // mise not present — nothing to pin
	}

	config, err := readJSONConfig(settingsPath)
	if err != nil {
		return false, fmt.Errorf("read Pi settings for npmCommand: %w", err)
	}

	if _, exists := config["npmCommand"]; exists {
		return false, nil // user already configured npmCommand — preserve it
	}

	nodeSpec := resolveMiseNodeVersionFn()
	if nodeSpec == "" {
		nodeSpec = "node" // fallback: let mise pick the active version at runtime
	}

	npmCmd := []string{"mise", "exec", nodeSpec, "--", "npm"}
	raw, err := jsonMarshalFn(npmCmd)
	if err != nil {
		return false, fmt.Errorf("marshal Pi npmCommand: %w", err)
	}
	config["npmCommand"] = raw
	return true, writeJSONConfig(settingsPath, config)
}

// resolveMiseNodeVersion returns the active Node version managed by mise as a
// versioned spec string (e.g. "node@22.12.0"). Returns an empty string when
// the version cannot be determined.
func resolveMiseNodeVersion() string {
	out, err := runCommand("mise", "current", "node")
	if err != nil {
		return ""
	}
	version := strings.TrimSpace(string(out))
	if version == "" {
		return ""
	}
	return "node@" + version
}

func ensurePiMCPConfig(mcpPath string) (bool, error) {
	config, err := readJSONConfig(mcpPath)
	if err != nil {
		return false, fmt.Errorf("read Pi MCP config: %w", err)
	}
	servers := make(map[string]json.RawMessage)
	if raw, ok := config["mcpServers"]; ok {
		if err := json.Unmarshal(raw, &servers); err != nil {
			return false, fmt.Errorf("parse Pi mcpServers: %w", err)
		}
	}
	if _, exists := servers["engram"]; exists {
		return false, nil
	}
	server := map[string]any{
		"command":     resolveEngramCommand(),
		"args":        []string{"mcp", "--tools=agent"},
		"lifecycle":   "lazy",
		"directTools": false,
	}
	raw, err := jsonMarshalFn(server)
	if err != nil {
		return false, fmt.Errorf("marshal Pi Engram MCP server: %w", err)
	}
	servers["engram"] = raw
	config["mcpServers"], err = jsonMarshalFn(servers)
	if err != nil {
		return false, fmt.Errorf("marshal Pi mcpServers: %w", err)
	}
	return true, writeJSONConfig(mcpPath, config)
}

func readJSONConfig(path string) (map[string]json.RawMessage, error) {
	data, err := readFileFn(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]json.RawMessage), nil
		}
		return nil, err
	}
	var config map[string]json.RawMessage
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	if config == nil {
		return nil, fmt.Errorf("%s must contain a JSON object", path)
	}
	return config, nil
}

func writeJSONConfig(path string, config map[string]json.RawMessage) error {
	output, err := jsonMarshalIndentFn(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	return writeFileFn(path, append(output, '\n'), 0644)
}

func readRawArrayField(config map[string]json.RawMessage, key, path string) ([]json.RawMessage, error) {
	raw, ok := config[key]
	if !ok {
		return nil, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("parse %s %q: %w", path, key, err)
	}
	return values, nil
}

func rawArrayContainsString(values []json.RawMessage, target string) bool {
	for _, value := range values {
		var decoded string
		if err := json.Unmarshal(value, &decoded); err == nil && decoded == target {
			return true
		}
	}
	return false
}

// ─── OpenCode ────────────────────────────────────────────────────────────────

// patchEngramBINLine rewrites the ENGRAM_BIN constant declaration in the
// plugin source so the installed copy contains an absolute fallback path.
//
// Original line in source:
//
//	const ENGRAM_BIN = process.env.ENGRAM_BIN ?? "engram"
//
// Patched line in installed copy:
//
//	const ENGRAM_BIN = process.env.ENGRAM_BIN ?? Bun.which("engram") ?? "/abs/path/engram"
//
// Priority (left to right, first truthy wins):
//  1. ENGRAM_BIN env var — explicit user override, always respected.
//  2. Bun.which("engram") — runtime PATH lookup; works in interactive shells.
//  3. Absolute baked-in path — works in headless/systemd where PATH is stripped.
//
// If absBin is already bare "engram" (os.Executable fallback) we don't add it
// as the third fallback because it would be redundant with Bun.which("engram").
func patchEngramBINLine(src []byte, absBin string) []byte {
	const marker = `const ENGRAM_BIN = process.env.ENGRAM_BIN ?? "engram"`

	var replacement string
	if absBin == "engram" {
		// os.Executable failed — add Bun.which but no baked-in absolute path
		replacement = `const ENGRAM_BIN = process.env.ENGRAM_BIN ?? Bun.which("engram") ?? "engram"`
	} else {
		// Normal case: bake in the absolute path as final fallback
		replacement = fmt.Sprintf(
			`const ENGRAM_BIN = process.env.ENGRAM_BIN ?? Bun.which("engram") ?? %q`,
			absBin,
		)
	}

	return []byte(strings.Replace(string(src), marker, replacement, 1))
}

func installOpenCode() (*Result, error) {
	dir := openCodePluginDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create plugin dir %s: %w", dir, err)
	}

	data, err := openCodeReadFile("plugins/opencode/engram.ts")
	if err != nil {
		return nil, fmt.Errorf("read embedded engram.ts: %w", err)
	}

	// Patch ENGRAM_BIN in the installed copy so the plugin can find the binary
	// in headless/systemd environments where PATH may not include user tool dirs.
	// The installed file gets a baked-in absolute path while still honoring
	// process.env.ENGRAM_BIN (explicit user override) and Bun.which("engram")
	// (runtime PATH lookup when PATH is available). The source plugin file is
	// not modified — it keeps the simple env-var form for development flexibility.
	data = patchEngramBINLine(data, resolveEngramCommand())

	dest := filepath.Join(dir, "engram.ts")
	if err := openCodeWriteFileFn(dest, data, 0644); err != nil {
		return nil, fmt.Errorf("write %s: %w", dest, err)
	}

	// Register engram MCP server in opencode.json and the subagent monitor in tui.json.
	files := 1
	if err := injectOpenCodeMCPFn(); err != nil {
		// Non-fatal: plugin works, MCP just needs manual config
		cmd := resolveEngramCommand()
		fmt.Fprintf(os.Stderr, "warning: could not auto-register MCP server in opencode.json: %v\n", err)
		fmt.Fprintf(os.Stderr, "  Add manually to your opencode.json under \"mcp\":\n")
		fmt.Fprintf(os.Stderr, "  \"engram\": { \"type\": \"local\", \"command\": [%q, \"mcp\", \"--tools=agent\"], \"enabled\": true }\n", cmd)
	} else {
		files++
	}

	tuiEnabled := false
	if err := injectOpenCodeTUIPluginFn(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not enable subagent monitor in tui.json: %v\n", err)
		fmt.Fprintf(os.Stderr, "  Add manually to your tui.json under \"plugin\": [%q]\n", openCodeSubagentStatuslinePlugin)
	} else {
		files++
		tuiEnabled = true
	}

	return &Result{
		Agent:            "opencode",
		Destination:      dir,
		Files:            files,
		TUIPluginEnabled: tuiEnabled,
	}, nil
}

// injectOpenCodeTUIPlugin adds the subagent monitor package to tui.json.
// It preserves the existing config and only appends the package when missing.
func injectOpenCodeTUIPlugin() error {
	configPath := openCodeTUIConfigPath()

	var config map[string]json.RawMessage
	data, err := readFileFn(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			config = make(map[string]json.RawMessage)
		} else {
			return fmt.Errorf("read config: %w", err)
		}
	} else {
		cleaned := stripJSONC(data)
		if err := json.Unmarshal(cleaned, &config); err != nil {
			return fmt.Errorf("parse config: %w", err)
		}
	}

	var plugins []string
	if raw, exists := config["plugin"]; exists {
		if err := json.Unmarshal(raw, &plugins); err != nil {
			return fmt.Errorf("parse plugin block: %w", err)
		}
	}

	for _, plugin := range plugins {
		if plugin == openCodeSubagentStatuslinePlugin {
			return nil
		}
	}

	plugins = append(plugins, openCodeSubagentStatuslinePlugin)
	pluginsJSON, err := jsonMarshalFn(plugins)
	if err != nil {
		return fmt.Errorf("marshal plugin block: %w", err)
	}
	config["plugin"] = json.RawMessage(pluginsJSON)

	output, err := jsonMarshalIndentFn(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := writeFileFn(configPath, output, 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

// injectOpenCodeMCP adds the engram MCP server entry to opencode.json.
// It reads the existing config, adds/updates the engram entry under "mcp",
// and writes it back preserving all other settings.
func injectOpenCodeMCP() error {
	configPath := openCodeConfigPath()

	// Read existing config (or start with empty object)
	var config map[string]json.RawMessage
	data, err := readFileFn(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			config = make(map[string]json.RawMessage)
		} else {
			return fmt.Errorf("read config: %w", err)
		}
	} else {
		cleaned := stripJSONC(data)
		if err := json.Unmarshal(cleaned, &config); err != nil {
			return fmt.Errorf("parse config: %w", err)
		}
	}

	// Parse or create the "mcp" block
	var mcpBlock map[string]json.RawMessage
	if raw, exists := config["mcp"]; exists {
		if err := json.Unmarshal(raw, &mcpBlock); err != nil {
			return fmt.Errorf("parse mcp block: %w", err)
		}
	} else {
		mcpBlock = make(map[string]json.RawMessage)
	}

	// Check if engram is already registered
	if _, exists := mcpBlock["engram"]; exists {
		return nil // already registered, nothing to do
	}

	// Add engram MCP entry (agent profile — only tools agents need).
	// Use resolveEngramCommand() so Windows users (and headless Linux setups
	// where PATH is not inherited) get the absolute binary path.
	engramEntry := map[string]interface{}{
		"type":    "local",
		"command": []string{resolveEngramCommand(), "mcp", "--tools=agent"},
		"enabled": true,
	}
	entryJSON, err := jsonMarshalFn(engramEntry)
	if err != nil {
		return fmt.Errorf("marshal engram entry: %w", err)
	}
	mcpBlock["engram"] = json.RawMessage(entryJSON)

	// Write mcp block back to config
	mcpJSON, err := jsonMarshalFn(mcpBlock)
	if err != nil {
		return fmt.Errorf("marshal mcp block: %w", err)
	}
	config["mcp"] = json.RawMessage(mcpJSON)

	// Write config back with indentation
	output, err := jsonMarshalIndentFn(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := writeFileFn(configPath, output, 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

// openCodeConfigPath returns the path to the OpenCode config file.
// It checks for opencode.jsonc first (preferred), then falls back to opencode.json.
func openCodeConfigPath() string {
	dir := openCodeConfigDir()
	jsonc := filepath.Join(dir, "opencode.jsonc")
	if _, err := statFn(jsonc); err == nil {
		return jsonc
	}
	return filepath.Join(dir, "opencode.json")
}

// openCodeTUIConfigPath returns the path to the OpenCode TUI config file.
// It checks for tui.jsonc first, then falls back to tui.json.
func openCodeTUIConfigPath() string {
	dir := openCodeConfigDir()
	jsonc := filepath.Join(dir, "tui.jsonc")
	if _, err := statFn(jsonc); err == nil {
		return jsonc
	}
	return filepath.Join(dir, "tui.json")
}

// openCodeConfigDir returns the directory containing the OpenCode config.
func openCodeConfigDir() string {
	home, _ := userHomeDir()

	// OpenCode reads from ~/.config/opencode/ on ALL platforms (including Windows),
	// ignoring the Windows %APPDATA% convention. Match that behavior.
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "opencode")
	}
	return filepath.Join(home, ".config", "opencode")
}

// stripJSONC removes single-line (//) and multi-line (/* */) comments
// from JSONC content, returning valid JSON. Comments inside quoted strings
// are preserved.
func stripJSONC(data []byte) []byte {
	var out []byte
	i := 0
	for i < len(data) {
		// Handle strings — pass through verbatim
		if data[i] == '"' {
			out = append(out, data[i])
			i++
			for i < len(data) && data[i] != '"' {
				if data[i] == '\\' && i+1 < len(data) {
					out = append(out, data[i], data[i+1])
					i += 2
					continue
				}
				out = append(out, data[i])
				i++
			}
			if i < len(data) {
				out = append(out, data[i])
				i++
			}
			continue
		}
		// Single-line comment
		if i+1 < len(data) && data[i] == '/' && data[i+1] == '/' {
			for i < len(data) && data[i] != '\n' {
				i++
			}
			continue
		}
		// Multi-line comment
		if i+1 < len(data) && data[i] == '/' && data[i+1] == '*' {
			i += 2
			for i+1 < len(data) && !(data[i] == '*' && data[i+1] == '/') {
				i++
			}
			if i+1 < len(data) {
				i += 2 // skip past */
			} else {
				i = len(data) // unterminated: consume everything
			}
			continue
		}
		out = append(out, data[i])
		i++
	}
	return out
}

// ─── Claude Code ─────────────────────────────────────────────────────────────

func installClaudeCode() (*Result, error) {
	// Check that claude CLI is available
	claudeBin, err := lookPathFn("claude")
	if err != nil {
		return nil, fmt.Errorf("claude CLI not found in PATH — install Claude Code first: https://docs.anthropic.com/en/docs/claude-code")
	}

	// Step 1: Add marketplace (idempotent — if already added, claude will say so)
	addOut, err := runCommand(claudeBin, "plugin", "marketplace", "add", claudeCodeMarketplace)
	addOutputStr := strings.TrimSpace(string(addOut))
	if err != nil {
		// If marketplace is already added, that's fine
		if !strings.Contains(addOutputStr, "already") {
			return nil, fmt.Errorf("marketplace add failed: %s", addOutputStr)
		}
	}

	// Step 2: Install the plugin
	installOut, err := runCommand(claudeBin, "plugin", "install", "engram")
	installOutputStr := strings.TrimSpace(string(installOut))
	if err != nil {
		// If plugin is already installed, that's fine
		if !strings.Contains(installOutputStr, "already") {
			return nil, fmt.Errorf("plugin install failed: %s", installOutputStr)
		}
	}

	// Step 3: Write a durable user-level MCP config at ~/.claude/mcp/engram.json
	// with the absolute binary path. This survives plugin cache auto-updates and
	// works on Windows where MCP subprocesses may not inherit PATH.
	files := 0
	if err := writeClaudeCodeUserMCPFn(); err != nil {
		// Non-fatal: the plugin still works via the plugin cache .mcp.json.
		// Warn so Windows users know to check their PATH if tools don't appear.
		fmt.Fprintf(os.Stderr, "warning: could not write user MCP config (~/.claude/mcp/engram.json): %v\n", err)
		fmt.Fprintf(os.Stderr, "  The plugin is installed but MCP may not start on Windows if engram is not in PATH.\n")
	} else {
		files = 1
	}

	return &Result{
		Agent:       "claude-code",
		Destination: claudeCodeMCPDir(),
		Files:       files,
	}, nil
}

// claudeCodeMCPDir returns the directory for user-level Claude Code MCP configs.
// Files placed here are NOT managed by the plugin system and survive plugin updates.
func claudeCodeMCPDir() string {
	home, _ := userHomeDir()
	return filepath.Join(home, ".claude", "mcp")
}

// claudeCodeUserMCPPath returns the path for the engram MCP config in the
// user-level MCP directory.
func claudeCodeUserMCPPath() string {
	return filepath.Join(claudeCodeMCPDir(), "engram.json")
}

// writeClaudeCodeUserMCP writes ~/.claude/mcp/engram.json with the absolute
// path to the engram binary. This is idempotent — it always writes (overwrites)
// so that if the binary moves (e.g. brew upgrade), running setup again fixes it.
// Using os.Executable() instead of PATH lookup ensures the correct binary is
// referenced even when PATH is not propagated to MCP subprocesses (Windows).
func writeClaudeCodeUserMCP() error {
	exe, err := osExecutable()
	if err != nil {
		return fmt.Errorf("resolve binary path: %w", err)
	}
	// Resolve any symlinks so the path is stable across package manager updates.
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}

	entry := map[string]any{
		"command": exe,
		"args":    []string{"mcp", "--tools=agent"},
	}
	data, err := jsonMarshalIndentFn(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal mcp config: %w", err)
	}

	dir := claudeCodeMCPDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create mcp dir: %w", err)
	}

	if err := writeFileFn(claudeCodeUserMCPPath(), data, 0644); err != nil {
		return fmt.Errorf("write mcp config: %w", err)
	}

	return nil
}

func claudeCodeSettingsPath() string {
	home, _ := userHomeDir()
	return filepath.Join(home, ".claude", "settings.json")
}

// AddClaudeCodeAllowlist adds engram MCP tool names to ~/.claude/settings.json
// permissions.allow so Claude Code doesn't prompt for confirmation on each call.
// Idempotent: skips tools already present in the list.
func AddClaudeCodeAllowlist() error {
	settingsPath := claudeCodeSettingsPath()

	// Read existing settings (or start fresh)
	var config map[string]json.RawMessage
	data, err := readFileFn(settingsPath)
	if err != nil {
		if os.IsNotExist(err) {
			config = make(map[string]json.RawMessage)
		} else {
			return fmt.Errorf("read settings: %w", err)
		}
	} else {
		if err := json.Unmarshal(data, &config); err != nil {
			return fmt.Errorf("parse settings: %w", err)
		}
	}

	// Parse or create permissions block
	var permissions map[string]json.RawMessage
	if raw, exists := config["permissions"]; exists {
		if err := json.Unmarshal(raw, &permissions); err != nil {
			return fmt.Errorf("parse permissions: %w", err)
		}
	} else {
		permissions = make(map[string]json.RawMessage)
	}

	// Parse or create allow list
	var allowList []string
	if raw, exists := permissions["allow"]; exists {
		if err := json.Unmarshal(raw, &allowList); err != nil {
			return fmt.Errorf("parse allow list: %w", err)
		}
	}

	// Build set of existing entries for O(1) lookup
	existing := make(map[string]bool, len(allowList))
	for _, entry := range allowList {
		existing[entry] = true
	}

	// Add only missing tools
	added := 0
	for _, tool := range claudeCodeMCPTools {
		if !existing[tool] {
			allowList = append(allowList, tool)
			added++
		}
	}

	if added == 0 {
		return nil // all tools already present
	}

	// Write back
	allowJSON, err := jsonMarshalFn(allowList)
	if err != nil {
		return fmt.Errorf("marshal allow list: %w", err)
	}
	permissions["allow"] = json.RawMessage(allowJSON)

	permJSON, err := jsonMarshalFn(permissions)
	if err != nil {
		return fmt.Errorf("marshal permissions: %w", err)
	}
	config["permissions"] = json.RawMessage(permJSON)

	output, err := jsonMarshalIndentFn(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}

	// Ensure ~/.claude/ directory exists
	if err := os.MkdirAll(filepath.Dir(settingsPath), 0755); err != nil {
		return fmt.Errorf("create settings dir: %w", err)
	}

	if err := writeFileFn(settingsPath, output, 0644); err != nil {
		return fmt.Errorf("write settings: %w", err)
	}

	return nil
}

// ─── Gemini CLI ──────────────────────────────────────────────────────────────

func installGeminiCLI() (*Result, error) {
	path := geminiConfigPath()
	if err := injectGeminiMCPFn(path); err != nil {
		return nil, err
	}

	if err := writeGeminiSystemPromptFn(); err != nil {
		return nil, err
	}

	// Clean up GEMINI_SYSTEM_MD if previously set — it causes Gemini to look
	// for system.md relative to CWD instead of ~/.gemini/, breaking any
	// directory that isn't $HOME. Gemini CLI already reads ~/.gemini/system.md
	// by default without this env var.
	removeGeminiEnvOverride()

	return &Result{
		Agent:       "gemini-cli",
		Destination: filepath.Dir(path),
		Files:       2,
	}, nil
}

func injectGeminiMCP(configPath string) error {
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	var config map[string]json.RawMessage
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			config = make(map[string]json.RawMessage)
		} else {
			return fmt.Errorf("read config: %w", err)
		}
	} else {
		if err := json.Unmarshal(data, &config); err != nil {
			return fmt.Errorf("parse config: %w", err)
		}
	}

	var mcpServers map[string]json.RawMessage
	if raw, exists := config["mcpServers"]; exists {
		if err := json.Unmarshal(raw, &mcpServers); err != nil {
			return fmt.Errorf("parse mcpServers block: %w", err)
		}
	} else {
		mcpServers = make(map[string]json.RawMessage)
	}

	engramEntry := map[string]any{
		"command": resolveEngramCommand(),
		"args":    []string{"mcp", "--tools=agent"},
	}
	entryJSON, err := jsonMarshalFn(engramEntry)
	if err != nil {
		return fmt.Errorf("marshal engram entry: %w", err)
	}
	mcpServers["engram"] = json.RawMessage(entryJSON)

	mcpJSON, err := jsonMarshalFn(mcpServers)
	if err != nil {
		return fmt.Errorf("marshal mcpServers block: %w", err)
	}
	config["mcpServers"] = json.RawMessage(mcpJSON)

	output, err := jsonMarshalIndentFn(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := writeFileFn(configPath, output, 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

// resolveEngramCommand returns the most stable command to spawn the engram
// binary. It uses os.Executable() so that headless/systemd environments (where
// PATH is not reliably inherited by child processes) still find the binary.
//
// Homebrew (and Linuxbrew) resolve the `engram` symlink to a versioned Cellar
// path such as /opt/homebrew/Cellar/engram/1.16.1/bin/engram. That path is
// removed on the next `brew upgrade`, so baking it into MCP client configs
// leaves a stale command that fails to spawn (ENOENT). When the resolved
// executable points into a versioned Cellar directory we prefer the stable
// <brew-prefix>/bin/engram symlink, which brew repoints at the current version,
// so registrations survive upgrades. Falls back to bare "engram" only when
// os.Executable() fails or the stable symlink is missing.
func resolveEngramCommand() string {
	exe, err := osExecutable()
	if err != nil {
		return "engram" // fallback to PATH-based name
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	if stable, ok := stableHomebrewEngramCommand(exe); ok {
		return stable
	}
	return exe
}

// stableHomebrewEngramCommand maps a versioned Homebrew Cellar path to the
// stable "<brew-prefix>/bin/engram" symlink that brew keeps pointing at the
// current version. It returns ("", false) when exe is not a versioned Cellar
// path, so non-Homebrew installs keep their resolved absolute path. When the
// derived stable symlink does not exist on disk it falls back to the bare
// "engram" name so the command still resolves via PATH.
func stableHomebrewEngramCommand(exe string) (string, bool) {
	clean := filepath.ToSlash(filepath.Clean(exe))
	base := strings.ToLower(filepath.Base(clean))
	if base != "engram" && base != "engram.exe" {
		return "", false
	}

	for _, marker := range []string{"/Cellar/engram/", "/Caskroom/engram/"} {
		idx := strings.Index(clean, marker)
		if idx < 0 {
			continue
		}
		// Everything before the Cellar/Caskroom marker is the brew prefix,
		// whose bin symlink survives package upgrades.
		stable := clean[:idx] + "/bin/engram"
		if _, err := statFn(stable); err == nil {
			return filepath.FromSlash(stable), true
		}
		return "engram", true
	}
	return "", false
}

func writeGeminiSystemPrompt() error {
	systemPath := geminiSystemPromptPath()
	if err := os.MkdirAll(filepath.Dir(systemPath), 0755); err != nil {
		return fmt.Errorf("create gemini system prompt dir: %w", err)
	}

	if err := os.WriteFile(systemPath, []byte(memoryProtocolMarkdown), 0644); err != nil {
		return fmt.Errorf("write gemini system prompt: %w", err)
	}

	return nil
}

// removeGeminiEnvOverride removes any GEMINI_SYSTEM_MD line from ~/.gemini/.env.
// Previous versions of engram added this line, but it causes Gemini CLI to look
// for system.md relative to CWD instead of ~/.gemini/. Best-effort cleanup.
func removeGeminiEnvOverride() {
	envPath := geminiEnvPath()
	content, err := readFileFn(envPath)
	if err != nil {
		return // file doesn't exist or unreadable — nothing to clean
	}

	text := strings.ReplaceAll(string(content), "\r\n", "\n")
	var lines []string
	changed := false
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "GEMINI_SYSTEM_MD=") {
			changed = true
			continue
		}
		lines = append(lines, line)
	}

	if changed {
		result := strings.TrimSpace(strings.Join(lines, "\n"))
		if result == "" {
			os.Remove(envPath) // delete empty env file
		} else {
			_ = writeFileFn(envPath, []byte(result+"\n"), 0644)
		}
	}
}

// ─── Codex ───────────────────────────────────────────────────────────────────

func installCodexWithOptions(options InstallOptions) (*Result, error) {
	ref, err := codexInstallRef(options)
	if err != nil {
		return nil, err
	}
	path := codexConfigPath()
	beforeFiles := captureCodexSetupFiles()
	result := &Result{
		Agent:       "codex",
		Destination: filepath.Dir(path),
		Preserved:   codexPreservedLegacySettings(path),
	}
	transaction, err := loadCodexSetupTransaction(path, ref, options)
	if err != nil {
		result.Preserved = appendUnique(result.Preserved, filepath.Base(codexSetupTransactionPath(path)))
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckPreserved,
			Detail:     "custom or unrecognized interrupted setup state was preserved byte-for-byte: " + err.Error(),
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}
	marketplaceState, err := inspectCodexMarketplaceState(path)
	if err != nil {
		return nil, err
	}
	if marketplaceState.Preserved != "" {
		interruptedInstallCache := transaction != nil && transaction.Kind == "install" &&
			marketplaceState.Present && !marketplaceState.PluginPresent && marketplaceState.Ref == ref &&
			marketplaceState.Preserved == `plugins."engram@engram"`
		if interruptedInstallCache {
			marketplaceState.Preserved = ""
			marketplaceState.Detail = ""
		} else {
			result.Preserved = appendUnique(result.Preserved, marketplaceState.Preserved)
			result.Checks = append(result.Checks, CapabilityCheck{
				Capability: "plugin",
				Status:     CheckPreserved,
				Detail:     marketplaceState.Detail,
			})
			return finishIncompleteCodexSetup(result, beforeFiles), nil
		}
	}

	// Install and verify the plugin before publishing local MCP or activation
	// configuration. This keeps failed or interrupted source verification from
	// leaving a misleading local setup behind.
	codexBin, err := lookPathFn("codex")
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: codex CLI not found in PATH — no Codex setup files were changed.\n")
		fmt.Fprintf(os.Stderr, "  To install manually, run:\n")
		fmt.Fprintf(os.Stderr, "    codex plugin marketplace add %s --ref %s --json\n", codexMarketplace, ref)
		fmt.Fprintf(os.Stderr, "    codex plugin add engram@engram --json\n")
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckMissing,
			Detail:     "codex CLI not found in PATH",
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}
	existingMarketplaceRoot := ""
	if transaction != nil && transaction.Kind == "install" {
		existingMarketplaceRoot, err = verifyInterruptedCodexPluginInstall(marketplaceState, transaction)
	} else if marketplaceState.PluginPresent {
		if transaction != nil {
			existingMarketplaceRoot, err = verifyInterruptedCodexPluginState(codexBin, path, marketplaceState.Ref, transaction)
		} else {
			existingMarketplaceRoot, err = verifyExistingCodexPluginState(codexBin, path, marketplaceState.Ref)
		}
	}
	if err != nil {
		result.Preserved = appendUnique(result.Preserved, `plugins."engram@engram"`)
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckPreserved,
			Detail:     "existing plugin state could not be attributed byte-for-byte and was preserved: " + err.Error(),
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}
	if marketplaceState.Present && marketplaceState.PluginPresent && marketplaceState.Ref != ref && transaction == nil {
		transaction, err = beginCodexSetupTransaction(path, marketplaceState.Ref, ref, options, existingMarketplaceRoot)
		if err != nil {
			return nil, err
		}
	}
	rollbackTransition := func() string {
		if transaction == nil || transaction.Kind != "upgrade" {
			return ""
		}
		if err := updateCodexMarketplaceRef(path, transaction.FromRef); err != nil {
			return "; additionally failed to restore the previous marketplace ref: " + err.Error()
		}
		return ""
	}

	marketplaceRoot := ""
	if marketplaceState.Present {
		if marketplaceState.Ref != ref {
			if err := updateCodexMarketplaceRef(path, ref); err != nil {
				return nil, err
			}
		}
		upgradeOut, upgradeErr := runCommand(codexBin, "plugin", "marketplace", "upgrade", "engram", "--json")
		if upgradeErr != nil {
			result.Checks = append(result.Checks, CapabilityCheck{
				Capability: "plugin",
				Status:     CheckFailed,
				Detail:     "marketplace upgrade failed: " + strings.TrimSpace(string(upgradeOut)) + rollbackTransition(),
			})
			return finishIncompleteCodexSetup(result, beforeFiles), nil
		}
		marketplaceRoot, err = codexMarketplaceRootFromUpgrade(upgradeOut)
	} else {
		addOut, addErr := runCommand(codexBin, "plugin", "marketplace", "add", codexMarketplace, "--ref", ref, "--json")
		if addErr != nil {
			result.Checks = append(result.Checks, CapabilityCheck{
				Capability: "plugin",
				Status:     CheckFailed,
				Detail:     "marketplace add failed: " + strings.TrimSpace(string(addOut)),
			})
			return finishIncompleteCodexSetup(result, beforeFiles), nil
		}
		marketplaceRoot, err = codexMarketplaceRootFromAdd(addOut)
	}
	if err != nil {
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckFailed,
			Detail:     err.Error() + rollbackTransition(),
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}

	expectedCommit := options.Commit
	if options.Development {
		expectedCommit = ""
	}
	marketplaceIdentity, err := verifyCodexMarketplaceRoot(marketplaceRoot, expectedCommit)
	if err != nil {
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckFailed,
			Detail:     err.Error() + rollbackTransition(),
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}
	verifiedPluginAssets, err := snapshotVerifiedCodexMarketplacePlugin(marketplaceRoot, marketplaceIdentity.Commit)
	if err != nil {
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckFailed,
			Detail:     err.Error() + rollbackTransition(),
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}
	if transaction == nil && !marketplaceState.PluginPresent {
		transaction, err = beginCodexPluginInstallTransaction(path, ref, options, marketplaceRoot, verifiedPluginAssets)
		if err != nil {
			return nil, err
		}
	}

	// Step 2: install the plugin (idempotent — tolerate "already" in output).
	pluginOut, err := runCommand(codexBin, "plugin", "add", "engram@engram", "--json")
	pluginOutputStr := strings.TrimSpace(string(pluginOut))
	if err != nil && !strings.Contains(strings.ToLower(pluginOutputStr), "already") {
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckFailed,
			Detail:     "plugin add failed: " + pluginOutputStr + rollbackTransition(),
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}

	pluginCapabilities, err := verifyInstalledCodexPlugin(pluginOut, verifiedPluginAssets)
	if err != nil {
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "plugin",
			Status:     CheckFailed,
			Detail:     err.Error() + rollbackTransition(),
		})
		return finishIncompleteCodexSetup(result, beforeFiles), nil
	}
	if transaction != nil {
		if err := os.Remove(codexSetupTransactionPath(path)); err != nil && !os.IsNotExist(err) {
			result.Checks = append(result.Checks, CapabilityCheck{
				Capability: "plugin",
				Status:     CheckFailed,
				Detail:     "remove completed setup transaction: " + err.Error(),
			})
			return finishIncompleteCodexSetup(result, beforeFiles), nil
		}
	}
	result.Checks = append(result.Checks, CapabilityCheck{
		Capability: "plugin",
		Status:     CheckReady,
		Detail:     fmt.Sprintf("verified %s at %s (%s)", marketplaceIdentity.Source, marketplaceIdentity.Commit, pluginCapabilities.Version),
	})

	if err := injectCodexMCPFn(path); err != nil {
		return nil, err
	}
	_, preserved, err := ensureCodexLegacyActivation(path)
	if err != nil {
		return nil, err
	}
	result.Preserved = appendUnique(result.Preserved, preserved...)
	result.Files = countChangedCodexSetupFiles(beforeFiles)

	if pluginCapabilities.MCPReady && codexMCPReady(path) {
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "mcp",
			Status:     CheckReady,
			Detail:     "plugin MCP manifest and stable executable registration verified",
		})
	} else {
		result.Checks = append(result.Checks, CapabilityCheck{
			Capability: "mcp",
			Status:     CheckFailed,
			Detail:     "plugin MCP manifest or stable executable registration is invalid",
		})
	}

	activationStatus := CheckMissing
	activationDetail := "installed plugin does not provide a verifiable canonical activation cue"
	if pluginCapabilities.ActivationCueReady {
		activationStatus = CheckReady
		activationDetail = "installed plugin canonical activation cue verified"
	}
	result.Checks = append(result.Checks, CapabilityCheck{
		Capability: "activation-cue",
		Status:     activationStatus,
		Detail:     activationDetail,
	})

	verifierStatus := CheckMissing
	verifierDetail := "installed plugin does not provide a Stop verifier"
	if pluginCapabilities.VerifierReady {
		verifierStatus = CheckReady
		verifierDetail = "installed plugin Stop verifier verified"
	}
	result.Checks = append(result.Checks, CapabilityCheck{
		Capability: "verifier",
		Status:     verifierStatus,
		Detail:     verifierDetail,
	})
	result.Complete = checksReady(result.Checks)
	return result, nil
}

type setupFileSnapshot struct {
	data   []byte
	exists bool
}

func captureCodexSetupFiles() map[string]setupFileSnapshot {
	paths := []string{codexConfigPath(), codexInstructionsPath(), codexCompactPromptPath()}
	snapshots := make(map[string]setupFileSnapshot, len(paths))
	for _, path := range paths {
		data, err := readFileFn(path)
		snapshots[path] = setupFileSnapshot{data: data, exists: err == nil}
	}
	return snapshots
}

func countChangedCodexSetupFiles(before map[string]setupFileSnapshot) int {
	changed := 0
	for path, previous := range before {
		current, err := readFileFn(path)
		exists := err == nil
		if previous.exists != exists || (exists && !bytes.Equal(previous.data, current)) {
			changed++
		}
	}
	return changed
}

func codexInstallRef(options InstallOptions) (string, error) {
	if options.Development {
		return "main", nil
	}

	version := strings.TrimPrefix(strings.TrimSpace(options.Version), "v")
	commit := strings.ToLower(strings.TrimSpace(options.Commit))
	decodedCommit, err := hex.DecodeString(commit)
	releaseVersion := "v" + version
	if !semver.IsValid(releaseVersion) || module.IsPseudoVersion(releaseVersion) || strings.Contains(version, "+dirty") || err != nil || len(decodedCommit) != 20 {
		return "", fmt.Errorf("stable Codex setup requires a release identity with a semantic version and exact 40-character commit; rerun from a release build or use explicit development mode")
	}
	return releaseVersion, nil
}

const codexSetupTransactionSchema = 1

type codexSetupTransaction struct {
	Schema              int    `json:"schema"`
	Kind                string `json:"kind"`
	Source              string `json:"source"`
	FromRef             string `json:"from_ref"`
	ToRef               string `json:"to_ref"`
	FromCommit          string `json:"from_commit"`
	ToCommit            string `json:"to_commit,omitempty"`
	MarketplaceRoot     string `json:"marketplace_root"`
	InstalledPath       string `json:"installed_path,omitempty"`
	OldPluginTreeSHA256 string `json:"old_plugin_tree_sha256"`
}

func codexSetupTransactionPath(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), ".engram-setup-transaction.json")
}

func encodeCodexSetupTransaction(transaction codexSetupTransaction) ([]byte, error) {
	data, err := json.MarshalIndent(transaction, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func loadCodexSetupTransaction(configPath, targetRef string, options InstallOptions) (*codexSetupTransaction, error) {
	path := codexSetupTransactionPath(configPath)
	data, err := readFileFn(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read setup transaction: %w", err)
	}
	var transaction codexSetupTransaction
	if err := json.Unmarshal(data, &transaction); err != nil {
		return nil, fmt.Errorf("parse setup transaction: %w", err)
	}
	canonical, err := encodeCodexSetupTransaction(transaction)
	if err != nil || !bytes.Equal(data, canonical) {
		return nil, fmt.Errorf("setup transaction does not match Engram's generated format")
	}
	targetCommit := strings.ToLower(strings.TrimSpace(options.Commit))
	if options.Development {
		targetCommit = ""
	}
	if transaction.Schema != codexSetupTransactionSchema ||
		transaction.Source != "https://github.com/yersonargotev/engram.git" ||
		transaction.ToRef != targetRef || transaction.ToCommit != targetCommit ||
		!filepath.IsAbs(transaction.MarketplaceRoot) {
		return nil, fmt.Errorf("setup transaction identity does not match this requested transition")
	}
	switch transaction.Kind {
	case "upgrade":
		_, fromCommitErr := normalizeGitCommit(transaction.FromCommit)
		oldDigest, digestErr := hex.DecodeString(transaction.OldPluginTreeSHA256)
		if transaction.FromRef == "" || transaction.FromRef == transaction.ToRef ||
			fromCommitErr != nil || digestErr != nil || len(oldDigest) != sha256.Size || transaction.InstalledPath != "" {
			return nil, fmt.Errorf("upgrade transaction identity is incomplete")
		}
	case "install":
		cacheRoot := filepath.Join(filepath.Dir(configPath), "plugins", "cache", "engram", "engram")
		relative, relErr := filepath.Rel(cacheRoot, transaction.InstalledPath)
		if transaction.FromRef != "" || transaction.FromCommit != "" || transaction.OldPluginTreeSHA256 != "" ||
			!filepath.IsAbs(transaction.InstalledPath) || relErr != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("install transaction identity is incomplete")
		}
	default:
		return nil, fmt.Errorf("setup transaction kind is not recognized")
	}
	return &transaction, nil
}

func beginCodexSetupTransaction(configPath, fromRef, toRef string, options InstallOptions, marketplaceRoot string) (*codexSetupTransaction, error) {
	identity, err := verifyCodexMarketplaceRoot(marketplaceRoot, "")
	if err != nil {
		return nil, err
	}
	assets, err := snapshotVerifiedCodexMarketplacePlugin(marketplaceRoot, identity.Commit)
	if err != nil {
		return nil, err
	}
	toCommit := strings.ToLower(strings.TrimSpace(options.Commit))
	if options.Development {
		toCommit = ""
	}
	transaction := &codexSetupTransaction{
		Schema:              codexSetupTransactionSchema,
		Kind:                "upgrade",
		Source:              identity.Source,
		FromRef:             fromRef,
		ToRef:               toRef,
		FromCommit:          identity.Commit,
		ToCommit:            toCommit,
		MarketplaceRoot:     filepath.Clean(marketplaceRoot),
		OldPluginTreeSHA256: codexPluginTreeSHA256(assets),
	}
	data, err := encodeCodexSetupTransaction(*transaction)
	if err != nil {
		return nil, fmt.Errorf("encode setup transaction: %w", err)
	}
	if err := atomicWriteFileFn(codexSetupTransactionPath(configPath), data, 0600); err != nil {
		return nil, fmt.Errorf("write setup transaction: %w", err)
	}
	return transaction, nil
}

func beginCodexPluginInstallTransaction(configPath, targetRef string, options InstallOptions, marketplaceRoot string, assets map[string]codexPluginTreeEntry) (*codexSetupTransaction, error) {
	identity, err := verifyCodexMarketplaceRoot(marketplaceRoot, "")
	if err != nil {
		return nil, err
	}
	version, err := codexPluginVersionFromSnapshot(assets)
	if err != nil {
		return nil, err
	}
	targetCommit := strings.ToLower(strings.TrimSpace(options.Commit))
	if options.Development {
		targetCommit = ""
	}
	transaction := &codexSetupTransaction{
		Schema:          codexSetupTransactionSchema,
		Kind:            "install",
		Source:          identity.Source,
		ToRef:           targetRef,
		ToCommit:        targetCommit,
		MarketplaceRoot: filepath.Clean(marketplaceRoot),
		InstalledPath:   filepath.Join(filepath.Dir(configPath), "plugins", "cache", "engram", "engram", version),
	}
	data, err := encodeCodexSetupTransaction(*transaction)
	if err != nil {
		return nil, fmt.Errorf("encode setup transaction: %w", err)
	}
	if err := atomicWriteFileFn(codexSetupTransactionPath(configPath), data, 0600); err != nil {
		return nil, fmt.Errorf("write setup transaction: %w", err)
	}
	return transaction, nil
}

type codexMarketplaceState struct {
	Present       bool
	PluginPresent bool
	Ref           string
	Preserved     string
	Detail        string
}

func inspectCodexMarketplaceState(configPath string) (codexMarketplaceState, error) {
	cachePresent, err := codexPluginCachePresent(configPath)
	if err != nil {
		return codexMarketplaceState{}, err
	}
	data, err := readFileFn(configPath)
	if os.IsNotExist(err) {
		if cachePresent {
			return codexMarketplaceState{Preserved: `plugins."engram@engram"`, Detail: "plugin cache exists without attributable Codex plugin state and was preserved"}, nil
		}
		return codexMarketplaceState{}, nil
	}
	if err != nil {
		return codexMarketplaceState{}, fmt.Errorf("read Codex marketplace config: %w", err)
	}

	marketplace, present, valid := codexTOMLTable(string(data), "marketplaces.engram")
	plugin, pluginPresent, pluginValid := codexTOMLTable(string(data), `plugins."engram@engram"`)
	if !present {
		if pluginPresent {
			return codexMarketplaceState{Preserved: `plugins."engram@engram"`, Detail: "plugin state exists without an attributable Engram marketplace and was preserved"}, nil
		}
		if cachePresent {
			return codexMarketplaceState{Preserved: `plugins."engram@engram"`, Detail: "plugin cache exists without an attributable Engram marketplace and was preserved"}, nil
		}
		return codexMarketplaceState{}, nil
	}

	sourceType, sourceTypeOK := decodeTOMLString(marketplace["source_type"])
	source, sourceOK := decodeTOMLString(marketplace["source"])
	ref, refOK := decodeTOMLString(marketplace["ref"])
	refKnown := ref == "main" || semver.IsValid(ref)
	marketplaceOwned := valid && len(marketplace) == 3 && sourceTypeOK && sourceOK && refOK && refKnown &&
		sourceType == "git" && source == "https://github.com/yersonargotev/engram.git"
	pluginOwned := !pluginPresent || (pluginValid && len(plugin) == 1 && strings.TrimSpace(plugin["enabled"]) == "true")
	if !marketplaceOwned || !pluginOwned {
		return codexMarketplaceState{
			Present:   true,
			Ref:       ref,
			Preserved: "marketplaces.engram",
			Detail:    "custom or unrecognized marketplace/plugin state was preserved byte-for-byte",
		}, nil
	}
	if !pluginPresent && cachePresent {
		return codexMarketplaceState{
			Present:   true,
			Ref:       ref,
			Preserved: `plugins."engram@engram"`,
			Detail:    "plugin cache exists without attributable enabled plugin state and was preserved",
		}, nil
	}
	return codexMarketplaceState{Present: true, PluginPresent: pluginPresent, Ref: ref}, nil
}

func codexPluginCachePresent(configPath string) (bool, error) {
	cacheRoot := filepath.Join(filepath.Dir(configPath), "plugins", "cache", "engram", "engram")
	entries, err := os.ReadDir(cacheRoot)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect Codex plugin cache: %w", err)
	}
	return len(entries) > 0, nil
}

func codexTOMLTable(content, table string) (map[string]string, bool, bool) {
	values := make(map[string]string)
	inTable := false
	present := false
	valid := true
	for _, line := range strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if header, ok := tomlTableHeader(trimmed); ok {
			if inTable {
				break
			}
			inTable = header == table
			present = present || inTable
			continue
		}
		if !inTable || trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			valid = false
			continue
		}
		values[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return values, present, valid
}

func decodeTOMLString(raw string) (string, bool) {
	value, err := strconv.Unquote(strings.TrimSpace(raw))
	return value, err == nil
}

func updateCodexMarketplaceRef(configPath, ref string) error {
	data, err := readFileFn(configPath)
	if err != nil {
		return fmt.Errorf("read owned Codex marketplace config: %w", err)
	}
	updated, ok := replaceTOMLTableString(string(data), "marketplaces.engram", "ref", ref)
	if !ok {
		return fmt.Errorf("owned Codex marketplace is missing its ref field")
	}
	if updated == string(data) {
		return nil
	}
	if err := atomicWriteFileFn(configPath, []byte(updated), 0644); err != nil {
		return fmt.Errorf("update owned Codex marketplace ref: %w", err)
	}
	return nil
}

func replaceTOMLTableString(content, table, key, value string) (string, bool) {
	newline := "\n"
	if strings.Contains(content, "\r\n") {
		newline = "\r\n"
	}
	lines := strings.Split(content, newline)
	inTable := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if header, ok := tomlTableHeader(trimmed); ok {
			if inTable {
				break
			}
			inTable = header == table
			continue
		}
		if !inTable {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == key {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			lines[i] = indent + key + " = " + strconv.Quote(value)
			return strings.Join(lines, newline), true
		}
	}
	return content, false
}

func completeCodexChecks(result *Result) *Result {
	has := make(map[string]bool, len(result.Checks))
	for _, check := range result.Checks {
		has[check.Capability] = true
	}
	for _, capability := range []string{"plugin", "mcp", "activation-cue", "verifier"} {
		if has[capability] {
			continue
		}
		status := CheckMissing
		detail := "not verified because a prerequisite capability is unavailable"
		result.Checks = append(result.Checks, CapabilityCheck{Capability: capability, Status: status, Detail: detail})
	}
	result.Complete = checksReady(result.Checks)
	return result
}

func finishIncompleteCodexSetup(result *Result, beforeFiles map[string]setupFileSnapshot) *Result {
	result.Files = countChangedCodexSetupFiles(beforeFiles)
	return completeCodexChecks(result)
}

type codexLegacySetting struct {
	key     string
	path    string
	content string
}

func codexLegacySettings() []codexLegacySetting {
	return []codexLegacySetting{
		{key: "model_instructions_file", path: codexInstructionsPath(), content: memoryProtocolMarkdown},
		{key: "experimental_compact_prompt_file", path: codexCompactPromptPath(), content: codexCompactPromptMarkdown},
	}
}

func ensureCodexLegacyActivation(configPath string) (int, []string, error) {
	data, err := readFileFn(configPath)
	if err != nil && !os.IsNotExist(err) {
		return 0, nil, fmt.Errorf("read Codex config: %w", err)
	}
	content := string(data)
	configChanged := false
	filesChanged := 0
	var preserved []string

	var createdSettings []codexLegacySetting
	rollbackCreated := func() {
		for _, setting := range createdSettings {
			data, err := os.ReadFile(setting.path)
			if err == nil && string(data) == setting.content {
				_ = os.Remove(setting.path)
			}
		}
	}

	for _, setting := range codexLegacySettings() {
		value, found, valid := topLevelTOMLString(content, setting.key)
		generated, readErr := readFileFn(setting.path)
		generatedKnown := readErr == nil && string(generated) == setting.content
		generatedMissing := os.IsNotExist(readErr)

		if found {
			if !valid || value != setting.path || !generatedKnown {
				preserved = append(preserved, setting.key)
			}
			continue
		}
		if !generatedMissing {
			preserved = append(preserved, setting.key)
			continue
		}
		if generatedMissing {
			if err := atomicWriteFileFn(setting.path, []byte(setting.content), 0644); err != nil {
				rollbackCreated()
				return filesChanged, preserved, fmt.Errorf("write Codex legacy activation %s: %w", setting.key, err)
			}
			createdSettings = append(createdSettings, setting)
			filesChanged++
		}
		content = upsertTopLevelTOMLString(content, setting.key, setting.path)
		configChanged = true
	}

	if configChanged {
		content = upsertCodexEngramBlock(content)
		if err := atomicWriteFileFn(configPath, []byte(content), 0644); err != nil {
			rollbackCreated()
			return filesChanged, preserved, fmt.Errorf("write Codex legacy activation config: %w", err)
		}
	}
	return filesChanged, preserved, nil
}

func appendUnique(values []string, additions ...string) []string {
	for _, addition := range additions {
		found := false
		for _, value := range values {
			if value == addition {
				found = true
				break
			}
		}
		if !found {
			values = append(values, addition)
		}
	}
	return values
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return atomicfile.ReplaceFile(tmpPath, path)
}

func checksReady(checks []CapabilityCheck) bool {
	if len(checks) == 0 {
		return false
	}
	for _, check := range checks {
		if check.Status != CheckReady {
			return false
		}
	}
	return true
}

type codexPluginAddResult struct {
	Name            string `json:"name"`
	MarketplaceName string `json:"marketplaceName"`
	Version         string `json:"version"`
	InstalledPath   string `json:"installedPath"`
}

type codexPluginListResult struct {
	Installed []codexListedPlugin `json:"installed"`
}

type codexListedPlugin struct {
	PluginID        string `json:"pluginId"`
	Name            string `json:"name"`
	MarketplaceName string `json:"marketplaceName"`
	Version         string `json:"version"`
	Installed       bool   `json:"installed"`
	Enabled         bool   `json:"enabled"`
	Source          struct {
		Source string `json:"source"`
		Path   string `json:"path"`
	} `json:"source"`
	MarketplaceSource struct {
		SourceType string `json:"sourceType"`
		Source     string `json:"source"`
	} `json:"marketplaceSource"`
}

type installedCodexPlugin struct {
	Version            string
	MCPReady           bool
	ActivationCueReady bool
	VerifierReady      bool
}

type codexInstalledPluginLocation struct {
	MarketplaceRoot string
	InstalledPath   string
}

func verifyInstalledCodexPlugin(output []byte, verifiedPluginAssets map[string]codexPluginTreeEntry) (installedCodexPlugin, error) {
	var installed codexPluginAddResult
	if err := json.Unmarshal(output, &installed); err != nil || strings.TrimSpace(installed.InstalledPath) == "" {
		return installedCodexPlugin{}, fmt.Errorf("plugin add did not return an installed path")
	}
	if installed.Name != "engram" || installed.MarketplaceName != "engram" {
		return installedCodexPlugin{}, fmt.Errorf("installed plugin identity is %s@%s, want engram@engram", installed.Name, installed.MarketplaceName)
	}

	manifestRaw, err := readFileFn(filepath.Join(installed.InstalledPath, ".codex-plugin", "plugin.json"))
	if err != nil {
		return installedCodexPlugin{}, fmt.Errorf("verify installed plugin manifest: %w", err)
	}
	var manifest struct {
		Name       string `json:"name"`
		Version    string `json:"version"`
		Repository string `json:"repository"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return installedCodexPlugin{}, fmt.Errorf("verify installed plugin manifest: %w", err)
	}
	if manifest.Name != "engram" || manifest.Repository != "https://github.com/yersonargotev/engram" || manifest.Version != installed.Version {
		return installedCodexPlugin{}, fmt.Errorf("installed plugin manifest does not match Engram authority and version")
	}
	if err := compareCodexPluginTreeSnapshot(verifiedPluginAssets, installed.InstalledPath); err != nil {
		return installedCodexPlugin{}, fmt.Errorf("installed plugin does not match the verified marketplace checkout: %w", err)
	}

	capabilities := installedCodexPlugin{Version: installed.Version}
	mcpRaw, err := readFileFn(filepath.Join(installed.InstalledPath, ".mcp.json"))
	if err == nil {
		var mcpManifest struct {
			MCPServers map[string]struct {
				Command string   `json:"command"`
				Args    []string `json:"args"`
			} `json:"mcpServers"`
		}
		if json.Unmarshal(mcpRaw, &mcpManifest) == nil {
			engram, ok := mcpManifest.MCPServers["engram"]
			capabilities.MCPReady = ok && engram.Command == "engram" && slicesEqual(engram.Args, []string{"mcp", "--tools=agent"})
		}
	}

	hooksRaw, err := readFileFn(filepath.Join(installed.InstalledPath, "hooks", "hooks.json"))
	if err == nil {
		var hooksManifest struct {
			Hooks map[string]json.RawMessage `json:"hooks"`
		}
		if json.Unmarshal(hooksRaw, &hooksManifest) == nil {
			var stopHooks []json.RawMessage
			stopRaw, ok := hooksManifest.Hooks["Stop"]
			capabilities.VerifierReady = ok && json.Unmarshal(stopRaw, &stopHooks) == nil && len(stopHooks) > 0
		}
	}
	return capabilities, nil
}

func inspectInstalledCodexPluginLocation(codexBin, configPath string) (codexInstalledPluginLocation, error) {
	output, err := runCommand(codexBin, "plugin", "list", "--json")
	if err != nil {
		return codexInstalledPluginLocation{}, fmt.Errorf("plugin list failed: %s", strings.TrimSpace(string(output)))
	}
	var listed codexPluginListResult
	if err := json.Unmarshal(output, &listed); err != nil {
		return codexInstalledPluginLocation{}, fmt.Errorf("parse plugin list: %w", err)
	}
	var match *codexListedPlugin
	for i := range listed.Installed {
		candidate := &listed.Installed[i]
		if candidate.PluginID == "engram@engram" {
			if match != nil {
				return codexInstalledPluginLocation{}, fmt.Errorf("plugin list returned duplicate Engram installations")
			}
			match = candidate
		}
	}
	if match == nil || match.Name != "engram" || match.MarketplaceName != "engram" || !match.Installed || !match.Enabled {
		return codexInstalledPluginLocation{}, fmt.Errorf("plugin list does not contain one enabled engram@engram installation")
	}
	if match.Source.Source != "local" || !semver.IsValid("v"+match.Version) || match.MarketplaceSource.SourceType != "git" || match.MarketplaceSource.Source != "https://github.com/yersonargotev/engram.git" {
		return codexInstalledPluginLocation{}, fmt.Errorf("plugin list source or version is not attributable to Engram")
	}

	sourcePath := filepath.Clean(match.Source.Path)
	marketplaceRoot := filepath.Dir(filepath.Dir(sourcePath))
	if sourcePath != filepath.Join(marketplaceRoot, "plugin", "codex") {
		return codexInstalledPluginLocation{}, fmt.Errorf("plugin source path is outside the Engram marketplace layout")
	}
	return codexInstalledPluginLocation{
		MarketplaceRoot: marketplaceRoot,
		InstalledPath:   filepath.Join(filepath.Dir(configPath), "plugins", "cache", "engram", "engram", match.Version),
	}, nil
}

func verifyExistingCodexPluginState(codexBin, configPath, configuredRef string) (string, error) {
	location, err := inspectInstalledCodexPluginLocation(codexBin, configPath)
	if err != nil {
		return "", err
	}
	marketplaceRoot := location.MarketplaceRoot
	refCommitRaw, err := gitResolveRefFn(marketplaceRoot, configuredRef)
	if err != nil {
		return "", fmt.Errorf("resolve configured marketplace ref %s: %s", configuredRef, strings.TrimSpace(string(refCommitRaw)))
	}
	refCommit, err := normalizeGitCommit(strings.TrimSpace(string(refCommitRaw)))
	if err != nil {
		return "", fmt.Errorf("resolve configured marketplace ref %s: %w", configuredRef, err)
	}
	if _, err := verifyCodexMarketplaceRoot(marketplaceRoot, refCommit); err != nil {
		return "", err
	}
	verifiedPluginAssets, err := snapshotVerifiedCodexMarketplacePlugin(marketplaceRoot, refCommit)
	if err != nil {
		return "", err
	}
	if err := compareCodexPluginTreeSnapshot(verifiedPluginAssets, location.InstalledPath); err != nil {
		return "", fmt.Errorf("existing installed plugin differs from its marketplace source: %w", err)
	}
	return marketplaceRoot, nil
}

func verifyInterruptedCodexPluginState(codexBin, configPath, configuredRef string, transaction *codexSetupTransaction) (string, error) {
	if configuredRef != transaction.FromRef && configuredRef != transaction.ToRef {
		return "", fmt.Errorf("marketplace ref %q is outside the interrupted transition", configuredRef)
	}
	location, err := inspectInstalledCodexPluginLocation(codexBin, configPath)
	if err != nil {
		return "", err
	}
	if filepath.Clean(location.MarketplaceRoot) != filepath.Clean(transaction.MarketplaceRoot) {
		return "", fmt.Errorf("marketplace path changed during the interrupted transition")
	}
	identity, err := verifyCodexMarketplaceRoot(location.MarketplaceRoot, "")
	if err != nil {
		return "", err
	}
	targetCommit := transaction.ToCommit
	if targetCommit == "" {
		resolved, err := gitResolveRefFn(location.MarketplaceRoot, transaction.ToRef)
		if err != nil {
			return "", fmt.Errorf("resolve interrupted target ref %s: %s", transaction.ToRef, strings.TrimSpace(string(resolved)))
		}
		targetCommit, err = normalizeGitCommit(strings.TrimSpace(string(resolved)))
		if err != nil {
			return "", fmt.Errorf("resolve interrupted target ref %s: %w", transaction.ToRef, err)
		}
	}
	if identity.Commit != transaction.FromCommit && identity.Commit != targetCommit {
		return "", fmt.Errorf("marketplace checkout is neither the previous nor requested commit")
	}
	currentAssets, err := snapshotVerifiedCodexMarketplacePlugin(location.MarketplaceRoot, identity.Commit)
	if err != nil {
		return "", err
	}
	installedAssets, err := snapshotCodexPluginTree(location.InstalledPath)
	if err != nil {
		return "", fmt.Errorf("read interrupted installed plugin assets: %w", err)
	}
	if codexPluginTreeSHA256(installedAssets) != transaction.OldPluginTreeSHA256 {
		if err := compareCodexPluginTreeSnapshot(currentAssets, location.InstalledPath); err != nil {
			return "", fmt.Errorf("installed plugin is neither the previous nor requested generated tree: %w", err)
		}
	}
	return location.MarketplaceRoot, nil
}

func verifyInterruptedCodexPluginInstall(marketplaceState codexMarketplaceState, transaction *codexSetupTransaction) (string, error) {
	if !marketplaceState.Present || marketplaceState.Ref != transaction.ToRef {
		return "", fmt.Errorf("marketplace config no longer matches the interrupted plugin installation")
	}
	targetCommit := transaction.ToCommit
	if targetCommit == "" {
		resolved, err := gitResolveRefFn(transaction.MarketplaceRoot, transaction.ToRef)
		if err != nil {
			return "", fmt.Errorf("resolve interrupted target ref %s: %s", transaction.ToRef, strings.TrimSpace(string(resolved)))
		}
		targetCommit, err = normalizeGitCommit(strings.TrimSpace(string(resolved)))
		if err != nil {
			return "", fmt.Errorf("resolve interrupted target ref %s: %w", transaction.ToRef, err)
		}
	}
	assets, err := snapshotVerifiedCodexMarketplacePlugin(transaction.MarketplaceRoot, targetCommit)
	if err != nil {
		return "", err
	}
	installed, err := snapshotCodexPluginTree(transaction.InstalledPath)
	if os.IsNotExist(err) {
		return transaction.MarketplaceRoot, nil
	}
	if err != nil {
		return "", fmt.Errorf("read interrupted plugin cache: %w", err)
	}
	if err := codexPluginTreeSubset(assets, installed); err != nil {
		return "", fmt.Errorf("interrupted plugin cache contains unattributable bytes: %w", err)
	}
	return transaction.MarketplaceRoot, nil
}

func verifyCodexMarketplaceAssets(root string) error {
	status, err := gitStatusFn(root)
	if err != nil {
		return fmt.Errorf("verify marketplace asset tree: %s", strings.TrimSpace(string(status)))
	}
	if dirty := strings.TrimSpace(string(status)); dirty != "" {
		return fmt.Errorf("marketplace asset tree differs from verified commit: %s", dirty)
	}
	return nil
}

func snapshotVerifiedCodexMarketplacePlugin(root, expectedCommit string) (map[string]codexPluginTreeEntry, error) {
	if _, err := verifyCodexMarketplaceRoot(root, expectedCommit); err != nil {
		return nil, err
	}
	if err := verifyCodexMarketplaceAssets(root); err != nil {
		return nil, err
	}
	snapshot, err := snapshotCodexPluginTree(filepath.Join(root, "plugin", "codex"))
	if err != nil {
		return nil, fmt.Errorf("snapshot verified marketplace plugin assets: %w", err)
	}
	if _, err := verifyCodexMarketplaceRoot(root, expectedCommit); err != nil {
		return nil, err
	}
	if err := verifyCodexMarketplaceAssets(root); err != nil {
		return nil, err
	}
	return snapshot, nil
}

type codexPluginTreeEntry struct {
	mode os.FileMode
	data []byte
}

func codexPluginTreeSHA256(entries map[string]codexPluginTreeEntry) string {
	paths := make([]string, 0, len(entries))
	for path := range entries {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, path := range paths {
		entry := entries[path]
		fmt.Fprintf(hash, "%d:%s:%d:%d:", len(path), path, uint32(entry.mode), len(entry.data))
		_, _ = hash.Write(entry.data)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func codexPluginVersionFromSnapshot(entries map[string]codexPluginTreeEntry) (string, error) {
	manifest, ok := entries[".codex-plugin/plugin.json"]
	if !ok || !manifest.mode.IsRegular() {
		return "", fmt.Errorf("verified plugin snapshot is missing its manifest")
	}
	var identity struct {
		Name       string `json:"name"`
		Version    string `json:"version"`
		Repository string `json:"repository"`
	}
	if err := json.Unmarshal(manifest.data, &identity); err != nil {
		return "", fmt.Errorf("parse verified plugin manifest: %w", err)
	}
	if identity.Name != "engram" || identity.Repository != "https://github.com/yersonargotev/engram" || !semver.IsValid("v"+identity.Version) {
		return "", fmt.Errorf("verified plugin manifest identity is invalid")
	}
	return identity.Version, nil
}

func codexPluginTreeSubset(expected, actual map[string]codexPluginTreeEntry) error {
	for path, entry := range actual {
		want, ok := expected[path]
		if !ok || entry.mode != want.mode || !bytes.Equal(entry.data, want.data) {
			return fmt.Errorf("asset %s differs", path)
		}
	}
	return nil
}

func compareCodexPluginTreeSnapshot(source map[string]codexPluginTreeEntry, installedRoot string) error {
	installed, err := snapshotCodexPluginTree(installedRoot)
	if err != nil {
		return fmt.Errorf("read installed plugin assets: %w", err)
	}
	if len(source) != len(installed) {
		return fmt.Errorf("asset count is %d, want %d", len(installed), len(source))
	}
	for path, expected := range source {
		actual, ok := installed[path]
		if !ok || actual.mode != expected.mode || !bytes.Equal(actual.data, expected.data) {
			return fmt.Errorf("asset %s differs", path)
		}
	}
	return nil
}

func snapshotCodexPluginTree(root string) (map[string]codexPluginTreeEntry, error) {
	entries := make(map[string]codexPluginTreeEntry)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		mode := info.Mode().Type()
		if entry.IsDir() {
			entries[filepath.ToSlash(relative)] = codexPluginTreeEntry{mode: mode}
			return nil
		}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			entries[filepath.ToSlash(relative)] = codexPluginTreeEntry{mode: mode | info.Mode().Perm(), data: data}
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			entries[filepath.ToSlash(relative)] = codexPluginTreeEntry{mode: os.ModeSymlink, data: []byte(target)}
			return nil
		}
		return fmt.Errorf("unsupported plugin asset type at %s", relative)
	})
	return entries, err
}

func slicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func codexMCPReady(configPath string) bool {
	data, err := readFileFn(configPath)
	if err != nil {
		return false
	}
	start, end, found := tomlSectionBounds(string(data), "mcp_servers.engram")
	if !found {
		return false
	}
	values, present, valid := codexTOMLTable(string(data[start:end]), "mcp_servers.engram")
	if !present || !valid || len(values) != 2 {
		return false
	}
	command, commandOK := decodeTOMLString(values["command"])
	var args []string
	argsOK := json.Unmarshal([]byte(values["args"]), &args) == nil
	return commandOK && argsOK && command == resolveEngramCommand() && slicesEqual(args, []string{"mcp", "--tools=agent"})
}

func codexPreservedLegacySettings(configPath string) []string {
	data, err := readFileFn(configPath)
	if err != nil {
		return nil
	}

	var preserved []string
	for _, setting := range codexLegacySettings() {
		value, found, valid := topLevelTOMLString(string(data), setting.key)
		if !found {
			continue
		}
		generated, readErr := readFileFn(setting.path)
		if !valid || value != setting.path || readErr != nil || string(generated) != setting.content {
			preserved = append(preserved, setting.key)
		}
	}
	return preserved
}

func topLevelTOMLString(content, key string) (value string, found, valid bool) {
	for _, line := range strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if _, ok := tomlTableHeader(trimmed); ok {
			break
		}
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) != key {
			continue
		}
		decoded, err := strconv.Unquote(strings.TrimSpace(parts[1]))
		if err != nil {
			return "", true, false
		}
		return decoded, true, true
	}
	return "", false, false
}

type codexMarketplaceAddResult struct {
	InstalledRoot string `json:"installedRoot"`
}

type codexMarketplaceUpgradeResult struct {
	UpgradedRoots []string `json:"upgradedRoots"`
	Errors        []string `json:"errors"`
}

type codexMarketplaceIdentity struct {
	Source string
	Commit string
}

func codexMarketplaceRootFromAdd(output []byte) (string, error) {
	var added codexMarketplaceAddResult
	if err := json.Unmarshal(output, &added); err != nil || strings.TrimSpace(added.InstalledRoot) == "" {
		return "", fmt.Errorf("marketplace add did not return an installed root")
	}
	return added.InstalledRoot, nil
}

func codexMarketplaceRootFromUpgrade(output []byte) (string, error) {
	var upgraded codexMarketplaceUpgradeResult
	if err := json.Unmarshal(output, &upgraded); err != nil || len(upgraded.UpgradedRoots) != 1 || len(upgraded.Errors) > 0 || strings.TrimSpace(upgraded.UpgradedRoots[0]) == "" {
		return "", fmt.Errorf("marketplace upgrade did not return one verified root")
	}
	return upgraded.UpgradedRoots[0], nil
}

func verifyCodexMarketplaceRoot(root, expectedCommit string) (codexMarketplaceIdentity, error) {
	if strings.TrimSpace(root) == "" {
		return codexMarketplaceIdentity{}, fmt.Errorf("marketplace verification requires an installed root")
	}

	actualCommit, err := gitHeadCommit(filepath.Join(root, ".git"))
	if err != nil {
		return codexMarketplaceIdentity{}, fmt.Errorf("verify marketplace commit: %w", err)
	}

	config, err := readFileFn(filepath.Join(root, ".git", "config"))
	if err != nil {
		return codexMarketplaceIdentity{}, fmt.Errorf("verify marketplace source: %w", err)
	}
	actualSource := gitRemoteOrigin(string(config))
	const expectedSource = "https://github.com/yersonargotev/engram.git"
	if actualSource != expectedSource {
		return codexMarketplaceIdentity{}, fmt.Errorf("marketplace source %q does not match authority %q", actualSource, expectedSource)
	}
	if expected := strings.ToLower(strings.TrimSpace(expectedCommit)); expected != "" && actualCommit != expected {
		return codexMarketplaceIdentity{}, fmt.Errorf("marketplace commit %s does not match expected commit %s", actualCommit, expected)
	}

	return codexMarketplaceIdentity{Source: actualSource, Commit: actualCommit}, nil
}

func gitHeadCommit(gitDir string) (string, error) {
	headRaw, err := readFileFn(filepath.Join(gitDir, "HEAD"))
	if err != nil {
		return "", err
	}
	head := strings.TrimSpace(string(headRaw))
	if !strings.HasPrefix(head, "ref:") {
		return normalizeGitCommit(head)
	}

	ref := strings.TrimSpace(strings.TrimPrefix(head, "ref:"))
	cleanRef := filepath.ToSlash(filepath.Clean(ref))
	if !strings.HasPrefix(cleanRef, "refs/") || strings.Contains(cleanRef, "../") {
		return "", fmt.Errorf("unexpected symbolic HEAD %q", head)
	}
	if refRaw, readErr := readFileFn(filepath.Join(gitDir, filepath.FromSlash(cleanRef))); readErr == nil {
		return normalizeGitCommit(strings.TrimSpace(string(refRaw)))
	}

	packedRefs, packedErr := readFileFn(filepath.Join(gitDir, "packed-refs"))
	if packedErr != nil {
		return "", fmt.Errorf("resolve symbolic HEAD %q: %w", cleanRef, packedErr)
	}
	for _, line := range strings.Split(strings.ReplaceAll(string(packedRefs), "\r\n", "\n"), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == cleanRef {
			return normalizeGitCommit(fields[0])
		}
	}
	return "", fmt.Errorf("resolve symbolic HEAD %q: ref not found", cleanRef)
}

func normalizeGitCommit(commit string) (string, error) {
	commit = strings.TrimSpace(commit)
	decodedCommit, err := hex.DecodeString(commit)
	if err != nil || len(decodedCommit) != 20 {
		return "", fmt.Errorf("unexpected HEAD %q", commit)
	}
	return strings.ToLower(commit), nil
}

func gitRemoteOrigin(config string) string {
	inOrigin := false
	for _, line := range strings.Split(strings.ReplaceAll(config, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			inOrigin = trimmed == `[remote "origin"]`
			continue
		}
		if inOrigin && strings.HasPrefix(trimmed, "url") {
			parts := strings.SplitN(trimmed, "=", 2)
			if len(parts) == 2 {
				return strings.TrimSpace(parts[1])
			}
		}
	}
	return ""
}

func injectCodexMCP(configPath string) error {
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	data, err := readFileFn(configPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read config: %w", err)
	}

	updated := upsertCodexEngramBlock(string(data))
	if string(data) == updated {
		return nil
	}
	if err := atomicWriteFileFn(configPath, []byte(updated), 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	return nil
}

func upsertCodexEngramBlock(content string) string {
	newline := "\n"
	if strings.Contains(content, "\r\n") {
		newline = "\r\n"
	}
	block := codexEngramBlockStr()
	if newline != "\n" {
		block = strings.ReplaceAll(block, "\n", newline)
	}

	start, end, found := tomlSectionBounds(content, "mcp_servers.engram")
	if found {
		section := content[start:end]
		if !codexMCPSectionOwned(section) {
			return content
		}
		replacement := block + newline
		if end < len(content) {
			replacement += newline
		}
		if section == replacement {
			return content
		}
		return content[:start] + replacement + content[end:]
	}

	if strings.TrimSpace(content) == "" {
		return block + newline
	}
	separator := newline + newline
	if strings.HasSuffix(content, newline+newline) {
		separator = ""
	} else if strings.HasSuffix(content, newline) {
		separator = newline
	}
	return content + separator + block + newline
}

func upsertTopLevelTOMLString(content, key, value string) string {
	newline := "\n"
	if strings.Contains(content, "\r\n") {
		newline = "\r\n"
	}
	lines := strings.Split(content, newline)
	lineValue := fmt.Sprintf("%s = %q", key, value)

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if _, ok := tomlTableHeader(trimmed); ok {
			break
		}
		if strings.HasPrefix(trimmed, key+" ") || strings.HasPrefix(trimmed, key+"=") {
			lines[i] = lineValue
			return strings.Join(lines, newline)
		}
	}

	if strings.TrimSpace(content) == "" {
		return lineValue + newline
	}

	headerAt := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if _, ok := tomlTableHeader(trimmed); ok {
			headerAt = i
			break
		}
	}
	if headerAt == -1 {
		if strings.HasSuffix(content, newline) {
			return content + lineValue + newline
		}
		return content + newline + lineValue + newline
	}

	insertAt := headerAt
	for insertAt > 0 && strings.TrimSpace(lines[insertAt-1]) == "" {
		insertAt--
	}
	out := append([]string(nil), lines[:insertAt]...)
	out = append(out, lineValue)
	if insertAt == headerAt {
		out = append(out, "")
	}
	out = append(out, lines[insertAt:]...)
	return strings.Join(out, newline)
}

func tomlSectionBounds(content, table string) (start, end int, found bool) {
	for offset := 0; offset < len(content); {
		lineStart := offset
		newlineAt := strings.IndexByte(content[offset:], '\n')
		if newlineAt == -1 {
			offset = len(content)
		} else {
			offset += newlineAt + 1
		}
		line := strings.TrimSpace(content[lineStart:offset])
		if current, ok := tomlTableHeader(line); ok {
			if found {
				return start, lineStart, true
			}
			if current == table {
				start = lineStart
				found = true
			}
		}
	}
	if found {
		return start, len(content), true
	}
	return 0, 0, false
}

func codexMCPSectionOwned(section string) bool {
	normalized := strings.TrimSpace(strings.ReplaceAll(section, "\r\n", "\n"))
	lines := strings.Split(normalized, "\n")
	if len(lines) != 3 || strings.TrimSpace(lines[0]) != "[mcp_servers.engram]" ||
		!strings.HasPrefix(lines[1], "command = ") || lines[2] != `args = ["mcp", "--tools=agent"]` {
		return false
	}
	values, present, valid := codexTOMLTable(section, "mcp_servers.engram")
	if !present || !valid || len(values) != 2 {
		return false
	}
	command, commandOK := decodeTOMLString(values["command"])
	var args []string
	argsOK := json.Unmarshal([]byte(values["args"]), &args) == nil
	normalizedCommand := strings.ReplaceAll(command, "\\", "/")
	base := strings.ToLower(filepath.Base(normalizedCommand))
	return commandOK && argsOK && (base == "engram" || base == "engram.exe") && slicesEqual(args, []string{"mcp", "--tools=agent"})
}

func tomlTableHeader(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "[[") {
		return "", false
	}
	closeAt := strings.IndexByte(trimmed, ']')
	if closeAt <= 1 {
		return "", false
	}
	tail := strings.TrimSpace(trimmed[closeAt+1:])
	if tail != "" && !strings.HasPrefix(tail, "#") {
		return "", false
	}
	return trimmed[1:closeAt], true
}

// ─── Platform paths ──────────────────────────────────────────────────────────

func openCodePluginDir() string {
	return filepath.Join(openCodeConfigDir(), "plugins")
}

func geminiConfigPath() string {
	home, _ := userHomeDir()

	switch runtimeGOOS {
	case "windows":
		if appData := os.Getenv("APPDATA"); appData != "" {
			return filepath.Join(appData, "gemini", "settings.json")
		}
		return filepath.Join(home, "AppData", "Roaming", "gemini", "settings.json")
	default:
		return filepath.Join(home, ".gemini", "settings.json")
	}
}

func geminiSystemPromptPath() string {
	return filepath.Join(filepath.Dir(geminiConfigPath()), "system.md")
}

func geminiEnvPath() string {
	return filepath.Join(filepath.Dir(geminiConfigPath()), ".env")
}

func codexConfigPath() string {
	home, _ := userHomeDir()

	switch runtimeGOOS {
	case "windows":
		if appData := os.Getenv("APPDATA"); appData != "" {
			return filepath.Join(appData, "codex", "config.toml")
		}
		return filepath.Join(home, "AppData", "Roaming", "codex", "config.toml")
	default:
		return filepath.Join(home, ".codex", "config.toml")
	}
}

func codexInstructionsPath() string {
	return filepath.Join(filepath.Dir(codexConfigPath()), "engram-instructions.md")
}

func codexCompactPromptPath() string {
	return filepath.Join(filepath.Dir(codexConfigPath()), "engram-compact-prompt.md")
}
