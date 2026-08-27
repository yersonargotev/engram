# Research: `engram setup` independence

**Date:** 2026-08-26

**Repository snapshot:** `091a7f90cbbbd6adecd5e4ca64ac892f152aac95`

**Scope:** Read-only analysis of setup behavior and distribution boundaries. No package, issue, or implementation changes were made.

## Executive conclusion

`engram setup` does **not** execute, import, or require the `gentle-ai` binary or Go module. The current Go module has no `gentle-ai` dependency, and the only executable commands issued by setup are host commands (`pi`, `claude`, `codex`, and optionally `mise`) plus package installs selected for individual hosts. The apparent Gentle AI dependency comes mainly from Pi's Engram-owned package name, `gentle-engram`, and from a `--protocol` compatibility contract originally requested by the downstream Gentle AI installer.

That said, setup is not semantically independent today. It silently installs two third-party helpers (`pi-mcp-adapter` and `opencode-subagent-statusline`), installs an Engram-owned Pi package under Gentle branding, uses external host marketplaces, and carries a protocol flag whose only active reader is the Claude Code hook. Pi also has a concrete version drift: setup and `pi-engram init` pin `gentle-engram@0.1.8`, while the first-party manifest and npm registry publish `0.1.10`.

**Recommendation (inference from the evidence):** adopt **semantic ownership** as the setup contract. `engram setup <host>` should install only host prerequisites and Engram-owned artifacts needed for Engram. Optional third-party enhancements must be opt-in and separately reported. The core binary should own the canonical checkpoint protocol; host packages, MCP entries, rules, and hooks should be thin adapters. This is feasible without making Engram a fully self-contained agent runtime.

**Maintainer status:** this recommendation is not a separate Gentle AI independence initiative. Q10-Q15 were omitted; relevant cleanup may be incorporated only where the approved checkpoint/setup redesign requires it. Canonical distribution authority is `yersonargotev/engram` (Q16).

## 1. Does setup depend on Gentle AI?

### Documented facts

- The module is `github.com/yersonargotev/engram`; `go.mod` contains no Gentle AI module. A source search finds no production Go/TypeScript/JavaScript/shell import or invocation of `gentle-ai`. The remaining exact `gentle-ai` strings in Go are test fixtures representing a project name. Sources: [`go.mod`](../../go.mod), [`internal/setup/setup.go`](../../internal/setup/setup.go), repository search at the pinned snapshot.
- `engram setup` dispatches entirely through Engram's own registry and installers. Sources: [`internal/setup/registry.go`](../../internal/setup/registry.go), [`internal/setup/agents.go`](../../internal/setup/agents.go), [`internal/setup/setup.go`](../../internal/setup/setup.go), [`cmd/engram/main.go`](../../cmd/engram/main.go).
- The Pi installer runs `pi install npm:gentle-engram@0.1.8` and `pi install npm:pi-mcp-adapter`; it does not run `gentle-ai`. Source: [`internal/setup/setup.go`](../../internal/setup/setup.go).
- `gentle-engram` is first-party Engram code in `plugin/pi`, published from this repository. Its manifest points back to the Engram repository and the current npm registry metadata reports version `0.1.10`. Sources: [`plugin/pi/package.json`](../../plugin/pi/package.json), [npm registry document](https://registry.npmjs.org/gentle-engram/latest).
- Gentle AI is an upstream/downstream ecosystem installer: its own documentation says it wires Engram into agents while Engram owns the memory store, and its setup component probes and invokes `engram setup`. That is Gentle AI depending on Engram's setup contract, not Engram depending on the Gentle AI executable. Sources: [Gentle AI memory-core boundary](https://github.com/Gentleman-Programming/gentle-ai/blob/main/docs/codebase/memory-core.md), [Gentle AI Engram setup component](https://github.com/Gentleman-Programming/gentle-ai/blob/main/internal/components/engram/setup.go).
- The local checkout's `origin` is `yersonargotev/engram` and `upstream` is `Gentleman-Programming/engram`. Organization or marketplace ownership is a source/distribution relationship, not by itself a runtime dependency.

### Terminology

| Name | What it is | Relationship to `engram setup` |
|---|---|---|
| `gentle-ai` | Separate ecosystem configurator/installer | Downstream consumer; not executed or imported by Engram setup |
| `gentle-engram` | Engram's first-party Pi npm package | Directly installed by `engram setup pi`; Gentle-branded but Engram-owned |
| Gentleman Programming | GitHub organization/ecosystem and current upstream | Source/distribution governance relationship, not an executable requirement |
| `pi-mcp-adapter` | Third-party Pi MCP gateway | Directly installed today by `engram setup pi` |

**Answer:** narrowly, yes, Engram is already independent of the `gentle-ai` program. Semantically and in branding/distribution, setup still carries Gentle ecosystem coupling and unrelated third-party choices.

## 2. Current setup inventory and ownership

All setup slugs register the Engram binary as `engram mcp --tools=agent` (usually by absolute resolved path) unless the host-specific integration uses Engram's HTTP API as well.

| Slug | Written/installed artifacts and commands | Ownership classification |
|---|---|---|
| `opencode` | Copies embedded `engram.ts`; modifies `opencode.json(c)` for MCP; modifies `tui.json(c)` to add `opencode-subagent-statusline` | Plugin and MCP entry are Engram-owned; OpenCode config is host-owned; statusline is third-party and not required for memory |
| `pi` | Runs `pi install npm:gentle-engram@0.1.8`; runs `pi install npm:pi-mcp-adapter`; updates `settings.json`; writes `mcp.json`; optionally runs `mise current node` and writes `npmCommand` | `pi` and config schema are host-owned; `gentle-engram` is Engram-owned; `pi-mcp-adapter` is third-party; `mise` is optional third-party environment tooling |
| `claude-code` | Runs `claude plugin marketplace add yersonargotev/engram`; runs `claude plugin install engram`; writes `~/.claude/mcp/engram.json`; optional allowlist update is a separate Engram setup action | Claude CLI/plugin system is host-owned; marketplace contents and MCP declaration are Engram-owned |
| `gemini-cli` | Writes `~/.gemini/settings.json` MCP entry; overwrites Engram-owned `~/.gemini/system.md`; removes obsolete `GEMINI_SYSTEM_MD` override from `.env` | Host config surface plus Engram-owned protocol content; no helper package |
| `codex` | Writes MCP TOML, `engram-instructions.md`, and `engram-compact-prompt.md`; best-effort runs `codex plugin marketplace add yersonargotev/engram --ref main` and `codex plugin add engram@engram` | Codex CLI/plugin system is host-owned; files/plugin are Engram-owned; tracking `main` is an unpinned distribution risk |
| `antigravity-cli` | Writes shared Gemini MCP config and marker block in `~/.gemini/GEMINI.md` | Host config plus Engram-owned content; no helper package |
| `windsurf` | Writes MCP config and marker block in `global_rules.md` | Host config plus Engram-owned content; no helper package |
| `qwen` | Writes MCP entry and marker block in `QWEN.md` | Host config plus Engram-owned content; no helper package |
| `kiro` | Writes MCP entry and Engram steering file | Host config plus Engram-owned content; no helper package |
| `cursor` | Writes MCP entry and an informational `engram-memory-protocol.md` that the user must paste into User Rules | Host config plus Engram-owned content; setup cannot finish the host UI step |
| `vscode-copilot` | Writes user `mcp.json` entry and `prompts/engram.instructions.md` | Host config plus Engram-owned content; no helper package |
| `kilocode` | Writes OpenCode-shaped MCP entry and marker block in global `AGENTS.md` | Host config plus Engram-owned content; no helper package |

Primary repository sources: [`internal/setup/agents.go`](../../internal/setup/agents.go), [`internal/setup/registry.go`](../../internal/setup/registry.go), [`internal/setup/setup.go`](../../internal/setup/setup.go), [`plugin/`](../../plugin), and [`docs/AGENT-SETUP.md`](../AGENT-SETUP.md).

### External package findings

- Pi itself supports npm packages and native extensions that call `pi.registerTool`; therefore installing a Pi package is a host facility, not a Gentle AI facility. [Official Pi extension examples](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/examples/extensions/README.md#writing-extensions).
- `gentle-engram` currently declares first-party runtime dependencies on `@earendil-works/pi-tui` and `typebox`, the Pi host as a peer, and `pi-mcp-adapter` as an **optional** peer. The package source already registers Pi-native tools, so the adapter is not required for its primary tool path. Sources: [`plugin/pi/package.json`](../../plugin/pi/package.json), [`plugin/pi/index.ts`](../../plugin/pi/index.ts), [npm registry document](https://registry.npmjs.org/gentle-engram/latest).
- The setup code nonetheless installs the optional adapter unconditionally and writes an MCP gateway entry with `directTools: false`. Source: [`internal/setup/setup.go`](../../internal/setup/setup.go).
- Current npm registry metadata attributes `pi-mcp-adapter@2.29.0` to a separate repository/maintainer. It brings its own MCP client, validation, keyring, and related dependency tree. [Official npm registry document](https://registry.npmjs.org/pi-mcp-adapter/latest).
- Current npm registry metadata attributes `opencode-subagent-statusline@1.3.0` to a separate repository/maintainer. OpenCode installs configured npm plugins through its own package mechanism. Sources: [npm registry document](https://registry.npmjs.org/opencode-subagent-statusline/latest), [official OpenCode plugin docs](https://dev.opencode.ai/docs/plugins/).
- `mise` is only consulted if already on `PATH`; setup preserves an existing `npmCommand`. It is a compatibility helper for selecting npm under the active Node toolchain, not an Engram requirement. Source: [`internal/setup/setup.go`](../../internal/setup/setup.go).

### Confirmed version drift

Three first-party locations pin `gentle-engram@0.1.8`: the Go setup constant, `pi-engram`'s self-install/config constant, and documentation. The package manifest and npm `latest` are `0.1.10`. Sources: [`internal/setup/setup.go`](../../internal/setup/setup.go), [`plugin/pi/cli.js`](../../plugin/pi/cli.js), [`plugin/pi/package.json`](../../plugin/pi/package.json), [npm registry](https://registry.npmjs.org/gentle-engram/latest). This is a concrete consequence of duplicating package identity/version outside the manifest.

## 3. Possible independence contracts

| Contract | Promise | Feasibility | Trade-off |
|---|---|---|---|
| **Narrow: no Gentle AI executable** | Setup never invokes/imports/requires `gentle-ai` | Already true | Does not address third-party packages, branding, duplicate policy, or unpinned artifacts |
| **Semantic ownership** | Setup installs only host prerequisites and Engram-owned artifacts necessary for Engram; unrelated helpers are explicit opt-ins; Engram core owns memory/checkpoint semantics | Feasible for every current slug | Still relies on each host's supported config/package/plugin mechanism and on normal runtime libraries inside Engram-owned packages |
| **Strict self-contained distribution** | The single Go binary contains every adapter and needs no external package, marketplace, Node runtime, or host CLI | Not fully feasible while using host-native plugin/extension APIs; the host itself and its runtime are unavoidable | Could embed/copy more source, but loses native update/signing flows and duplicates host package management; increases maintenance and supply-chain responsibility |

**Recommendation:** use semantic ownership as the product contract. Define “independent” around authority and necessity, not around eliminating the host's own extension mechanism. A strict single-file promise would conflict with Pi/OpenCode/Claude/Codex native integration models and is not needed to remove Gentle AI coupling.

## 4. Pi without `pi-mcp-adapter`

### Documented facts

- Pi extensions can register native LLM tools directly with `pi.registerTool`. [Official Pi docs/example](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/examples/extensions/README.md#writing-extensions).
- `gentle-engram` already does this: its extension registers compact native `mem_*` tools and calls Engram's HTTP API, while also capturing lifecycle events. Sources: [`plugin/pi/index.ts`](../../plugin/pi/index.ts), [`plugin/pi/README.md`](../../plugin/pi/README.md).
- Its own manifest marks `pi-mcp-adapter` optional, and its README states the adapter is only needed for the optional MCP gateway/compatibility path. Sources: [`plugin/pi/package.json`](../../plugin/pi/package.json), [`plugin/pi/README.md`](../../plugin/pi/README.md).

**Conclusion:** removing `pi-mcp-adapter` from the default Engram setup is technically viable now. The Pi-native path depends on `engram serve`/HTTP instead of stdio MCP, so parity must be verified against the canonical MCP/CLI operation set; missing operations should be implemented in the Engram-owned adapter or a shared generated contract, not recovered by silently reinstalling the general-purpose gateway. Users who need other MCP servers can opt into `pi-mcp-adapter` independently.

### Renaming `gentle-engram`

An npm rename is a new package identity, not an in-place rename. Existing Pi `settings.json` entries would continue loading `gentle-engram`; adding a new name without removing the old one can load both extensions and duplicate tools/events. The package also contains hard-coded references to its old package spec. Sources: [`internal/setup/setup.go`](../../internal/setup/setup.go), [`plugin/pi/cli.js`](../../plugin/pi/cli.js), [`plugin/pi/package.json`](../../plugin/pi/package.json).

**Recommendation (inference):** if independent branding is part of the product requirement, publish a clearly Engram-owned new package name, teach setup to detect/migrate the old package atomically, prevent simultaneous loading, and deprecate the old npm package with migration instructions. Do not couple that migration to removal of `pi-mcp-adapter`; they are separate changes. If branding is not a requirement, keeping the existing package name while fixing ownership and setup semantics is lower risk.

## 5. Does `--protocol` still have standalone value?

The archived first-party proposal is explicit: Gentle AI requested a side-effect-free way to opt Claude Code into slim hook output because MCP `serverInstructions` already duplicated the full protocol. The feature persisted modes for all slugs, but only Claude Code's `session-start.sh` and `post-compaction.sh` consume the value; other adapters remain full-only. Sources: [`openspec/changes/archive/2026-07-08-setup-protocol-flag/proposal.md`](../../openspec/changes/archive/2026-07-08-setup-protocol-flag/proposal.md), [`internal/setup/protocol.go`](../../internal/setup/protocol.go), [`plugin/claude-code/scripts/session-start.sh`](../../plugin/claude-code/scripts/session-start.sh), [`plugin/claude-code/scripts/post-compaction.sh`](../../plugin/claude-code/scripts/post-compaction.sh), [`cmd/engram/main.go`](../../cmd/engram/main.go).

It therefore has a real but narrow standalone function today: deduplicating Claude's static protocol while preserving compatibility with older/full configurations. It is not a general protocol architecture, and its per-slug persistence overstates its actual consumers.

**Recommendation (inference):** keep it only as a compatibility input during migration. The canonical checkpoint protocol should determine which minimal rule/skill/hook content each setup target receives. Once released adapters can prove a single authoritative instruction path, map legacy `slim/full` to the new configuration, warn on explicit use, then remove `protocol-mode.json` and the hidden `protocol-mode` subcommand in a documented compatibility release. Immediate deletion would reintroduce duplicate Claude prompt content for existing users.

## 6. Target architecture and migration order

The following is a recommendation, not current documented behavior:

1. **Declare the contract:** `engram setup` owns only Engram integration; host prerequisites are checked, Engram-owned artifacts are installed, and third-party enhancements require explicit opt-in.
2. **Create one setup plan/receipt:** before mutation, report host-owned files, Engram-owned artifacts, external commands/packages, versions/channels, and removals/migrations. This makes independence testable.
3. **Centralize artifact identity:** derive Pi package version from a release manifest rather than constants in Go, JS, and docs. Never track a marketplace's `main` branch in the stable setup path.
4. **Pi first:** make the Engram-owned native extension the default; stop installing/configuring `pi-mcp-adapter`; add a separate compatibility option. Verify tool and checkpoint parity through the HTTP/native path. Handle any package rename as an atomic migration with duplicate prevention.
5. **OpenCode:** stop adding `opencode-subagent-statusline` during Engram setup. Keep it as a separately named optional enhancement. The embedded Engram plugin and MCP entry remain sufficient for Engram ownership.
6. **Claude/Codex:** retain native marketplaces only as host distribution mechanisms, but publish from the canonical Engram source with release-pinned metadata. Remove duplicate MCP/rule/hook paths as the checkpoint architecture decides which one is authoritative.
7. **Declarative adapters:** generate MCP registration and the minimal activation cue from one Engram-owned setup model. Avoid copying the full protocol into shared `AGENTS.md`/rules surfaces.
8. **Protocol migration:** absorb the useful `slim` behavior into canonical checkpoint adapter configuration, preserve a compatibility read, then deprecate the Gentle-AI-origin flag.
9. **Verification:** isolated HOME/XDG tests per slug must assert both positive artifacts and negative independence guarantees: no `gentle-ai` invocation, no undeclared third-party install, no moving channel, no duplicate active protocol, and idempotent upgrade/removal.

## 7. Persistent activation cue: Codex and Claude Code

### Codex

#### Documented facts

- The universal plugin model packages skills, an MCP server, or both; the public overview does not define a plugin resource that is an always-loaded behavioral instruction/rules file. A skill is progressive-disclosure content: Codex initially receives its name and description and loads the full `SKILL.md` only when the skill is selected. Sources: [official Codex plugin guide](https://developers.openai.com/codex/plugins), [official Codex skill guide](https://developers.openai.com/codex/skills).
- Codex CLI plugins can bundle lifecycle hooks. Codex discovers `hooks/hooks.json` by default or a `hooks` path/inline object from `.codex-plugin/plugin.json`. Plugin hooks require explicit trust and run alongside other hook sources. [Official Codex hooks reference](https://developers.openai.com/codex/hooks#plugin-bundled-hooks).
- A `SessionStart` hook's plain stdout or `hookSpecificOutput.additionalContext` becomes extra developer context. It also runs after compaction when matching `source: "compact"`, before the next model request. `UserPromptSubmit` can likewise add developer context. [Official Codex hooks reference](https://developers.openai.com/codex/hooks#sessionstart).
- Codex's behavioral project-instruction surface is `AGENTS.md`: it reads one global file from `$CODEX_HOME` and then one instruction file per directory from project root to the working directory. `project_doc_fallback_filenames` only adds alternative names during that project walk; it does not turn a plugin file into project guidance. [Official AGENTS.md guide](https://developers.openai.com/codex/guides/agents-md).
- Codex `.rules` files are **execution policy**, controlling which commands may run outside the sandbox. They are not model behavioral instructions and must not be used for Engram's memory activation cue. [Official Codex rules reference](https://developers.openai.com/codex/rules).
- `model_instructions_file` is a dedicated host config key, but it is a **replacement** for built-in instructions rather than an additive plugin instruction resource. Engram currently writes this global key and a dedicated file in `installCodex`. Sources: [official Codex config reference](https://developers.openai.com/codex/config-reference), [`internal/setup/setup.go`](../../internal/setup/setup.go).

**Conclusion:** Codex has no documented always-loaded static instruction file owned natively by the universal plugin manifest. It does have a plugin-native `SessionStart` context-injection mechanism. That hook can deliver a short activation cue at startup/resume/clear/compact without taking ownership of the user's global `model_instructions_file` or editing shared `AGENTS.md`.

### Claude Code

#### Documented facts

- Claude Code plugins can package skills, agents, hooks, MCP/LSP servers, output styles, themes, and monitors. The plugin reference explicitly says a root `CLAUDE.md` is **not** loaded as project context; plugins contribute context through skills, agents, and hooks. It does not list `rules/` as a plugin resource. Plugin `settings.json` currently supports only `agent` and `subagentStatusLine`, so it cannot carry behavioral instructions. [Official Claude Code plugin reference](https://code.claude.com/docs/en/plugins-reference).
- Plugin skills are model-invoked/task-selected capabilities, not guaranteed always-on context. For static context that never changes, Claude's hook documentation prefers `CLAUDE.md`, but that recommendation concerns host/user/project configuration, not a plugin-root `CLAUDE.md`. Sources: [official plugin guide](https://code.claude.com/docs/en/plugins), [official hooks reference](https://code.claude.com/docs/en/hooks#add-context-for-claude).
- A plugin can bundle `hooks/hooks.json`. `SessionStart` can return `hookSpecificOutput.additionalContext` or plain stdout, which Claude inserts before the first prompt; `UserPromptSubmit` can inject alongside each prompt. [Official Claude Code hooks reference](https://code.claude.com/docs/en/hooks#sessionstart).
- Host-native persistent instruction files are user/project `CLAUDE.md` and `.claude/rules/*.md`. Unscoped rules load every session; path-scoped rules load when matching files are opened. These are not plugin-contained resources. [Official Claude Code memory/rules guide](https://code.claude.com/docs/en/memory#organize-rules-with-clauderules).

**Conclusion:** Claude Code also lacks a plugin-native always-loaded static rules/`CLAUDE.md` resource. Its plugin-native `SessionStart` hook is the documented additive path for a short activation cue; the plugin skill can hold the full protocol for on-demand use.

### Evidence-based delivery precedence

For Engram's **short activation cue**, use this precedence:

1. **Plugin-native lifecycle context**, when available: one `SessionStart` hook that emits a short, stable `additionalContext` cue and re-emits it after compaction. This is additive and versioned with the plugin in both Codex and Claude Code.
2. **Dedicated additive host instruction surface**, if a host has one and no reliable plugin hook. It must be Engram-owned and must not replace another tool's or the user's instruction file.
3. **Shared global `AGENTS.md` / `CLAUDE.md` or host rules**, only when neither of the above can provide persistent loading. Write a minimal marker-delimited cue, never the full protocol, and preserve user content.
4. **Skill for detail, not activation:** keep the rubric/workflow in one Engram skill that the short cue names or that task matching can load.

For Codex specifically, do **not** use `.rules`; do **not** keep setting `model_instructions_file` merely to activate Engram because it is replacement semantics. For Claude Code, do **not** place `CLAUDE.md` or `rules/` inside the plugin expecting them to load. The existing Engram plugin hooks are the correct packaging seam, but their output must use documented model-visible `additionalContext` rather than UI-only fields. Sources: [`plugin/codex/hooks/hooks.json`](../../plugin/codex/hooks/hooks.json), [`plugin/claude-code/hooks/hooks.json`](../../plugin/claude-code/hooks/hooks.json), [Codex hooks](https://developers.openai.com/codex/hooks), [Claude Code hooks](https://code.claude.com/docs/en/hooks).

This precedence concerns the short cue only. The checkpoint core remains the semantic authority; hook delivery does not decide whether a memory is saved.

## 8. Maintainer decisions

### Recorded decisions

- **Q10-Q15 are omitted.** The maintainer is not opening a separate Gentle AI independence initiative. The setup findings remain valid inputs to the checkpoint/setup redesign, but they do not create an independent product program.
- **Q16 is decided:** all source, package, marketplace, release, and setup distribution authority belongs to [`yersonargotev/engram`](https://github.com/yersonargotev/engram). `Gentleman-Programming/engram` may remain an upstream relationship, but it is not canonical distribution authority for Engram.
- **Q17 is decided:** prefer the plugin `SessionStart` cue; do not overwrite
  `model_instructions_file`, global `AGENTS.md`, or global `CLAUDE.md` for Codex
  or Claude Code. Only if a host lacks plugin-native and dedicated persistent
  delivery may setup add a minimal marker block to a shared instruction surface.
- **Q18 is decided:** stable setup must install release/tag-identified artifacts
  from `yersonargotev/engram` and pin Git-backed inputs to the exact release
  commit. Tracking `main` is restricted to an explicit development mode.
- **Q19 is decided:** setup may retire legacy instruction configuration only
  when the current value, path, and content prove exact Engram ownership. It
  preserves and reports unknown or customized state, and verifies the
  replacement capability before removing an owned legacy path.

## Uncertainties

- Availability and governance of a preferred replacement npm package name/scope were not tested because no publication or reservation was authorized.
- This research proves Pi native-tool feasibility from the current source contract, but full operation-by-operation parity with the evolving MCP agent profile needs an executable contract test before removing the adapter from a release.
- Marketplace commands and schemas can change with host versions. The target should be verified against the supported host-version matrix at implementation time.
