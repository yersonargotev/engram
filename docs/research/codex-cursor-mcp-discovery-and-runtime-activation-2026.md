# Codex and Cursor MCP discovery and runtime activation

**Date:** 2026-09-22
**Scope:** How Codex and Cursor discover and load MCP servers, instructions, skills, and plugins; why an installed/configured MCP can still be absent from a session; and which outcomes Engram can verify without reading conversation transcripts.

## Executive conclusion

Before the remediation described below, Engram treated **valid files and executable presence** as MCP readiness. Neither `engram setup status codex` nor `engram setup status cursor` proved that the host discovered the server, completed the MCP initialization handshake, admitted its tools to the current session catalog, or made those tools salient to the model. This was the principal semantic gap behind a report of “ready” while `mem_*` appeared absent or was not selected.

Installation, discovery, activation, and execution are separate states:

1. **Installed:** plugin/config files and an Engram executable exist.
2. **Discovered:** the host has selected those files for the current user, workspace, trust policy, and client.
3. **Activated:** the server is enabled, starts and initializes before the host closes its initial tool catalog, and its tools survive allow/deny and approval policy.
4. **Executed:** the model selects a tool, the host authorizes the call, and the server returns a result.

The original setup status established mostly (1), partially inferred (2), and did not establish (3) or (4). The fix direction was not merely “write the config again”; Engram needed a host-observed activation signal and a local event ledger for initialization, tool discovery, invocation, and outcome.

## Implemented remediation

The implementation now separates evidence instead of collapsing it into one
“ready” claim:

- Codex status reports owned configuration, the effective `codex mcp list
  --json` registration, and a bounded isolated stdio `initialize` plus
  `tools/list` probe as distinct checks. The managed registration is enabled,
  required, has a 10-second startup timeout, and allowlists exactly the five
  agent-profile tools. `CODEX_HOME` is honored consistently.
- Cursor setup no longer prints success without post-install checks. Cursor
  status runs the same protocol-level probe against a temporary Engram store
  and rejects missing, duplicate, or extra tools. It does not claim that an
  already-running editor has reloaded the plugin.
- The canonical activation cue tells the agent to resolve deferred `mem_*`
  tools from the host catalog before falling back to CLI, then use
  `mem_current_project` and one narrow `mem_search` for history-dependent work.
- The content-free local baseline is default-on with explicit opt-out. Its v2
  report distinguishes MCP process start, initialization, tool listing, and
  actual tool calls by an allowlisted host hint. Setup probes disable collection
  and use temporary stores so they do not masquerade as real activation.

This closes the static-readiness and silent-CLI-fallback defects. It does not
make a protocol probe equivalent to a host session: only runtime ledger evidence
can show that Codex or Cursor actually initialized, listed, and called Engram.
Nor can content-free telemetry determine whether a missed Recall would have
been semantically useful; explicit Recall feedback remains the utility signal.

## Documented host behavior

### Codex

- Codex reads MCP configuration from user `~/.codex/config.toml` and project `.codex/config.toml`; project configuration is loaded only for trusted projects. The desktop app, CLI, and IDE extension share configuration for the same Codex host ([OpenAI MCP documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)). Sharing persistent configuration is not a statement that an already-running session refreshed its tool catalog.
- Codex builds an **initial tool catalog**. Optional MCP servers share a default startup grace of only 1,000 ms; setting `mcp_optional_startup_grace_ms = 0` makes Codex wait for each server's `startup_timeout_sec`. An optional server may therefore be valid and executable but miss the initial catalog if initialization is slow ([OpenAI MCP documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli), [configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)).
- A server can be configured but disabled with `enabled = false`. `enabled_tools` and `disabled_tools` filter its catalog, with the deny list applied after the allow list. Per-server and per-tool approval policy also affects use ([OpenAI MCP documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)).
- Codex reads the MCP `instructions` field during initialization and uses it as server-wide guidance. OpenAI recommends making the first 512 characters self-contained because that guidance participates in deciding whether to use a server ([OpenAI MCP documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)). A server with weak or absent instructions may be callable but selected less often.
- Skills use progressive disclosure: initially the model sees only each skill's name, description, and path; the full `SKILL.md` is loaded only after selection. The initial skill inventory has a context budget and may omit skills when many are installed. Codex scans `.agents/skills` from the working directory to the repository root, plus user/admin/system locations ([OpenAI skill documentation](https://learn.chatgpt.com/docs/build-skills)). Thus “skill file exists” does not prove the model saw or selected it.
- Plugin-bundled hooks are unmanaged and are skipped until the user trusts their exact current definition. Installing or enabling the plugin does not grant hook trust ([OpenAI plugin packaging documentation](https://developers.openai.com/plugins/build/plugins), [OpenAI hooks documentation](https://learn.chatgpt.com/docs/hooks)). This can disable Engram's recall cue and checkpoint enforcement independently from MCP configuration.
- Plugin MCP servers can be enabled/disabled and filtered under `plugins.<plugin>.mcp_servers.<server>`. A standalone `[mcp_servers.engram]` registration and a plugin-bundled registration are distinct configuration sources and should be observed separately ([OpenAI MCP documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)).

### Cursor

- Cursor's native file locations are project `.cursor/mcp.json` and global `~/.cursor/mcp.json`. Its documented interpolation variables include `${workspaceFolder}`, `${userHome}`, and `${env:NAME}` ([Cursor MCP documentation](https://cursor.com/docs/mcp)).
- Cursor plugins have a separate discovery path. Local plugins are copied under `~/.cursor/plugins/local`, require a Cursor restart or **Developer: Reload Window**, and load only if local plugin imports are allowed. On Enterprise, local imports are off by default; a marketplace plugin of the same name takes precedence; and symlinks pointing outside the local plugin directory are skipped ([Cursor plugin documentation](https://cursor.com/docs/plugins)).
- Plugin component discovery finds root `mcp.json` by convention unless the manifest's `mcpServers` field overrides that discovery. Cursor expands `${CURSOR_PLUGIN_ROOT}` but not the Agent Plugins standard `${PLUGIN_ROOT}` or `${PLUGIN_DATA}` variables ([Cursor plugin reference](https://cursor.com/docs/reference/plugins)).
- Installed/distributed is not equivalent to enabled: disabled MCP servers do not load or appear in chat. Team marketplace publication also does not install or enable a plugin for every developer, and authentication may still be required ([Cursor plugin documentation](https://cursor.com/docs/plugins)).
- Only tools listed under **Available Tools** are candidates for automatic use. Cursor asks for approval by default, and Run Mode can route calls through additional classification ([Cursor MCP documentation](https://cursor.com/docs/mcp)).
- Cursor exposes host-side evidence that Engram should use: **MCP Logs** include initialization, tool calls, and errors; Cursor CLI exposes `agent mcp list` and `agent mcp list-tools <identifier>` with connection status, source, transport, and tool schemas ([Cursor MCP documentation](https://cursor.com/docs/mcp), [Cursor CLI MCP documentation](https://cursor.com/docs/cli/mcp)).

## Pre-remediation repository evidence and concrete gaps

The following subsections describe the implementation inspected before this
change. They are retained as root-cause evidence, not as current behavior.

### Codex setup verified shape, not host activation

`installCodexWithOptions` installs and verifies the plugin, then writes a standalone MCP registration. Its success condition checks the plugin manifest, expected `mem_checkpoint` profile entries, the CLI checkpoint help text, and the exact TOML block ([setup.go](../../internal/setup/setup.go#L1182), [setup.go](../../internal/setup/setup.go#L1394), [setup.go](../../internal/setup/setup.go#L2662)).

The plugin verifier parses `.mcp.json` and accepts it when the command is `engram` and arguments are `mcp --tools=agent`; it does not launch an MCP client, perform `initialize`, request `tools/list`, or query Codex's effective tool catalog ([setup.go](../../internal/setup/setup.go#L2238)). Likewise, status marks the configuration ready from the TOML syntax and ownership signature alone ([codex_status.go](../../internal/setup/codex_status.go#L1198)).

This explains how status can report both `mcp_configuration_ready` and `mcp_adapter_ready` without proving where `mem_*` appears in the current model session. The names overstate what was tested; they mean “supported registration and executable contract,” not runtime readiness.

The Codex plugin declares MCP in its compatibility manifest and also setup writes standalone MCP config ([plugin manifest](../../plugin/codex/.codex-plugin/plugin.json), [plugin MCP manifest](../../plugin/codex/.mcp.json)). Dual registration increases ambiguity: a status probe can validate one source while a session resolves, filters, or fails another. The product should identify the **effective source** reported by Codex and reject duplicate active identities unless explicitly supported.

### Cursor setup verified the plugin tree before Cursor reload/discovery

Cursor setup materializes an Agent Plugin directly into `~/.cursor/plugins/local/engram`, copies the running binary, rewrites the MCP command to its absolute installed path, installs user hooks, and removes an owned native `~/.cursor/mcp.json` entry ([cursor.go](../../internal/setup/cursor.go#L20), [cursor.go](../../internal/setup/cursor.go#L174), [cursor.go](../../internal/setup/cursor.go#L227), [cursor.go](../../internal/setup/cursor.go#L334)). This relies entirely on Cursor's local-plugin discovery and a later reload.

Cursor status calls MCP ready when the plugin's `mcp.json` has the supported static shape ([cursor_status.go](../../internal/setup/cursor_status.go#L284)). It does not establish that local imports are permitted, Cursor was reloaded, the local plugin won name precedence, the server connected, or tools entered Available Tools.

On the inspected machine, `engram setup status cursor --json` reported plugin MCP `ready`, while `agent mcp list` returned `No MCP servers configured (expected in .cursor/mcp.json or ~/.cursor/mcp.json)`. This is direct evidence that Engram's current static plugin-file check is not equivalent to Cursor CLI discovery. The documentation does not establish whether Cursor CLI must discover manually copied local plugins, so the supported conclusion is narrower: **Engram must probe each promised surface independently instead of inferring CLI readiness from editor plugin files.**

### Current-machine snapshot

Read-only commands run on 2026-09-22 showed:

- `codex mcp list --json`: Engram was configured and enabled with `/opt/homebrew/bin/engram mcp --tools=agent`.
- `codex plugin list --json`: `engram@engram` was installed and enabled.
- `engram setup status codex --json`: static MCP checks were ready, but overall mode was `partial_plugin` because the Managed Pack declaration was missing; the installed plugin was older than the running binary/protocol.
- The active API session omitted `mem_*` from its top-level tool declaration, but all five tools were present in the unified `exec` gateway's deferred `ALL_TOOLS` inventory; a direct deferred call to `mem_current_project` succeeded. The original CLI fallback therefore resulted from agent-side deferred-tool discovery, not from MCP absence. This still demonstrates that command-level configuration inventory is insufficient evidence of model-visible salience or selection.
- `engram setup status cursor --json`: MCP and hooks were reported ready, but the plugin and skill were stale.
- `agent mcp list`: no Cursor CLI MCP servers were discovered.

These observations are environment-specific, not universal host behavior, but they reproduce the semantic gap the user reported.

## Failure taxonomy

| Stage | Failure that leaves files looking correct | Host-observable evidence Engram should collect |
|---|---|---|
| Install | wrong/stale binary; stale plugin; duplicate source; partial transaction | file identity, version, digest, source and exact command |
| Discover | wrong config scope; untrusted project; local plugins disabled; reload required; marketplace precedence; unsupported surface | host inventory with effective source, enabled state, reload generation and policy denial |
| Initialize | spawn failure; missing environment; bad cwd; protocol error; authentication failure; crash; optional startup grace exceeded | start timestamp, PID/transport, initialize completion, duration, exit/error class |
| Catalog | tool filtered by allow/deny policy; server missed initial catalog; schema rejected; host tool budget/omission | `tools/list` names plus host-effective Available Tools for the session |
| Select | weak skill description; skill omitted; skill not selected; no/weak MCP instructions; recall cue hook untrusted or absent | cue delivery, skill inventory/selection, server-instruction digest, recall opportunity counter |
| Execute | approval denied; Run Mode/classifier blocks; timeout; tool/server error | invocation, approval disposition, latency, terminal status, bounded error code |
| Use | result ignored or not attributable to later action | explicit local recall-feedback event, not transcript inference |

## Release-gate success criteria

The implementation in this change delivers the setup/protocol and content-free
runtime-observation foundation. Criteria that require a genuinely fresh Codex
or Cursor model session, a matched task cohort, or a semantic
Recall-opportunity denominator remain release-gate follow-up; the isolated
protocol probes must not be presented as satisfying them.

### Setup correctness

1. **Static installed:** exact plugin/config identity remains useful but is labeled `installed`, not `ready`.
2. **Host discovered:** after setup, a fresh host process reports Engram from the intended source and enabled. Codex verification must consume `codex mcp list --json` and plugin inventory. Cursor editor and Cursor CLI must be verified independently; no claim may cross surfaces without host evidence.
3. **Protocol initialized:** a controlled fresh session observes one successful MCP initialization within the relevant startup budget.
4. **Catalog exposed:** the controlled server's tool list contains exactly `mem_current_project`, `mem_search`, `mem_get_observation`, `mem_checkpoint`, and `mem_checkpoint_status` for the agent profile. Host-session admission remains a separate runtime fact.
5. **Activation delivered:** the fresh session records the recall cue as delivered. For Codex, hook trust must be explicitly checked; for Cursor, reload generation and effective hook registration must be checked.
6. **Round trip:** a non-destructive canary tool call succeeds against an isolated project/store and is attributed to that session.
7. **No false ready:** any missing host evidence produces `unknown`/`not_observed`, never `ready`.

### Local observability contract

Engram should own a content-free append-only local ledger keyed by host, surface, session ID, project ID, plugin/binary version, and monotonic timestamp. Record bounded events:

- `setup_written`
- `host_discovered`
- `server_start_attempted`
- `server_initialized` / `server_init_failed`
- `tools_listed`
- `tool_catalog_observed` (when the host exposes this)
- `activation_cue_delivered`
- `recall_opportunity`
- `recall_invoked`
- `recall_succeeded` / `recall_failed`
- `checkpoint_invoked` / `checkpoint_succeeded` / `checkpoint_failed`

Do not store prompts, memory contents, search queries, or tool results by default. Store tool names, counts, durations, stable reason codes, version/source identity, and hashes where correlation is needed. This supports diagnosing Recall without conversation review while preserving local-first privacy.

### Controlled verification

For each supported surface and release:

1. Create an isolated home, project, and Engram store.
2. Install with `engram setup <surface>`.
3. Launch a genuinely fresh host process/session; do not reuse the installer process.
4. Assert host discovery and exact source.
5. Assert MCP initialization and exact tool catalog.
6. Seed one uniquely identifiable harmless Memory.
7. Run a preregistered task whose completion requires that Memory but does not name Engram or the Memory.
8. Assert one targeted Recall invocation, successful observation fetch, correct use, and exactly one terminal checkpoint.
9. Run a matched task with no relevant Memory and assert bounded/no noisy Recall.
10. Compare treatment and control on activation rate, successful Recall rate, useful-result rate, latency, and false-positive Recall rate.

Minimum release gate: 100% install/discovery/initialization/catalog success across repeated clean starts for each claimed surface; zero false `ready` states; and a statistically predeclared improvement in useful Recall over the current treatment without materially increasing irrelevant Recall or startup latency.

## Supported conclusions and remaining uncertainty

**Supported:** Engram's former readiness checks were static and could produce false confidence about session availability. The implemented probes now prove configuration, Codex inventory, and server protocol capability separately, while runtime observations remain the authority for a real host session. Codex startup grace, trust, enable/filter policy, and skill progressive disclosure can independently suppress activation; Cursor local-plugin policy/reload/precedence and server enablement can do the same.

**Inference:** the missing `mem_*` tools in the active Codex API session may be caused by catalog construction timing or a host/session tool-injection boundary. The available public docs do not expose enough session-internal diagnostics to choose one cause from configuration inventory alone.

**Unsupported today:** whether Cursor CLI is contractually required to discover plugins manually placed in `~/.cursor/plugins/local`. Its documented `agent mcp list` did not discover Engram locally, so Engram should either add a native user/project MCP registration for the CLI or narrow the product claim until Cursor documents plugin-to-CLI discovery.

## Implementation decisions still required

1. Choose a single authoritative MCP registration per Codex session (plugin-provided or standalone) and treat the other as migration-only.
2. Decide whether `engram setup cursor` promises the editor only or editor plus CLI. If it promises both, install and verify each source explicitly.
3. Decide whether to add a dedicated host-session acceptance command beyond the isolated setup/status protocol probe.
4. Extend the content-free funnel with an explicitly defined Recall-opportunity denominator before claiming a missed-Recall rate; absence of a search alone is not evidence that a search was warranted.
