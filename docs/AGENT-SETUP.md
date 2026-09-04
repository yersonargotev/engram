[← Back to README](../README.md)

# Agent Setup

Engram works with **any MCP-compatible agent**. Pick your agent below.

> Cloud bootstrap automation in agent scripts/plugins is intentionally deferred in this rollout. Use `engram cloud ...` manually for now.
>
> Deferred validation scope for this rollout:
>
> - Setup/plugin scripts are **not** yet validated as cloud enrollment/login orchestrators.
> - `engram setup ...` installs MCP/plugin integrations only; it does **not** auto-run `engram cloud config/enroll/upgrade`.
> - Cloud onboarding contract remains CLI-first until script-level cloud flows are explicitly implemented.

## Quick Reference

| Agent         | One-liner                                                                                    | Manual Config                                      |
| ------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| Claude Code   | `claude plugin marketplace add yersonargotev/engram && claude plugin install engram` | [Details](#claude-code)                            |
| Pi            | `engram setup pi`                                                                            | [Details](#pi)                                     |
| OpenCode      | `engram setup opencode`                                                                      | [Details](#opencode)                               |
| Gemini CLI    | `engram setup gemini-cli`                                                                    | [Details](#gemini-cli)                             |
| Codex           | `engram setup codex`                                                                         | [Details](#codex)                                  |
| Antigravity CLI | `engram setup antigravity-cli`                                                               | [Details](#antigravity)                            |
| Windsurf        | `engram setup windsurf`                                                                      | [Details](#windsurf)                               |
| Qwen Code       | `engram setup qwen`                                                                          | [Details](#qwen-code)                              |
| Kiro            | `engram setup kiro`                                                                          | [Details](#kiro)                                   |
| Cursor          | `engram setup cursor`                                                                        | [Details](#cursor)                                 |
| VS Code Copilot | `engram setup vscode-copilot`                                                                | [Details](#vs-code-copilot--claude-code-extension) |
| Kilo Code       | `engram setup kilocode`                                                                      | [Details](#kilo-code)                              |
| Any MCP agent   | `engram mcp --tools=agent` (stdio)                                                           | [Details](#any-other-mcp-agent)                    |

> **Native setup for all agents above.** `engram setup <agent>` installs that
> host's Engram-owned activation surface — a plugin, MCP registration, and/or
> instruction file, depending on the agent. The per-agent sections below describe
> the exact files each command touches and the manual equivalent.

`engram setup` is the Memory install control plane. New installs do not need
Packy to project a user-level `engram-memory-cli` skill. Hosts that install an
Engram plugin get the canonical `engram-memory` checkpoint rubric from setup;
other hosts get setup-owned MCP and protocol files. Existing Packy or
`~/.agents/skills/engram-memory-cli` copies are leftover user skills: status
reports them and they are not the canonical rubric. Run `engram setup <agent>`
for the host you use, then keep or remove the leftover copy. The Managed Pack
and Packy may remain for compatibility; they are not the happy-path install.
A disabled standalone `engram-memory-cli` skill is not Codex activation.

The `agent` profile exposes exactly `mem_current_project`, `mem_search`,
`mem_get_observation`, `mem_checkpoint`, and `mem_checkpoint_status`. Add
`curation` for independent authoring, optional Session summary, context, review,
relations, diagnosis, and pins; `lifecycle` for host session and Content-capture
operations; or `admin` for destructive and operational maintenance. `all`
retains all 24 tools for deliberate broad integrations.

A Memory operation reads or changes durable Memory or checkpoint state. An
agent lifecycle operation reports host activity or captures Content and never
selects the root turn's disposition.

## Diagnostic capture consent

Prompt and subagent-output capture are independently disabled by default on
fresh setup, upgrade, and setup reruns. Setup and read-only status report
capability and current consent without enabling capture or reading captured
content. Manage each local consent and content lifecycle explicitly:

```bash
engram capture status --project <name> --type prompt
engram capture enable --project <name> --type prompt          # 7 days by default
engram capture enable --project <name> --type prompt --retention-days 30
engram capture disable --project <name> --type prompt         # does not purge
engram capture purge --project <name> --type prompt           # separate confirmation
engram capture status --project <name> --type subagent_output
engram capture enable --project <name> --type subagent_output # independent grant
engram capture purge --project <name> --type subagent_output  # separate confirmation
```

Consent is scoped to project and content type. A narrower session grant is
optional and must expire. Retention cannot exceed 30 days. Diagnostic Content
is local-only and never appears in Memory/FTS/Recall/context, sync/cloud,
ordinary export/import, Obsidian, or retired candidate/promotion flows.

Subagent capture never inherits prompt consent. When explicitly enabled, the
host lifecycle accepts only a bounded `engram_diagnostic` JSON envelope with
`kind`, `title`, `learning`, and optional `evidence_ref`. It rejects raw
transcripts, ordinary last-message output, `stdout` fallback, and extra or
oversized fields. A subagent event never creates Memory, a proposal, a
checkpoint, a Session summary, or a retired evaluation/promotion record. The
root agent alone decides whether settled knowledge or an unresolved proposal
belongs in the terminal Memory checkpoint.

Prompts stored before this boundary remain a frozen Legacy archive. They are
not rewritten, reclassified, indexed, synced, or automatically deleted during
setup or migration. Use `engram legacy-prompts inventory`, `access`, `export`,
or the separately confirmed `purge` deliberately. `inventory --all` reports a
content-free total without requiring prior knowledge of archived project names.
Purge removes exact Engram-owned journal and FTS copies transactionally. It
fails closed without deleting archive rows when customized FTS ownership could
leave a content copy behind.

## Pi

Install Engram's Pi package, the MCP adapter, and Pi MCP config:

```bash
engram setup pi
```

`engram setup pi` runs `pi install npm:gentle-engram@0.1.8` and `pi install npm:pi-mcp-adapter`, then ensures Pi settings contain both packages and writes `mcpServers.engram` in the Pi agent MCP config when no Engram server is already configured. Existing `mcpServers.engram` entries are preserved.

When [mise](https://mise.jdx.dev/) is detected in `PATH`, `engram setup pi` also auto-pins `npmCommand` in Pi's `settings.json` to `["mise", "exec", "node@<version>", "--", "npm"]`, preventing Node version drift from silently changing which npm root Pi uses. If `npmCommand` already exists in `settings.json`, the existing value is preserved. This step is a no-op when mise is not installed.

Manual equivalent:

```bash
pi install npm:gentle-engram@0.1.8
pi install npm:pi-mcp-adapter
pi-engram init
```

Restart Pi after installation.

The package has two paths:

- **HTTP lifecycle events**: the Pi extension reports exact runtime identity, summaries, passive task learnings, and compact Pi-native `mem_*` calls to `engram serve`. Prompt content is persisted only through Core's explicit local Diagnostic consent gate.
- **MCP gateway**: `pi-mcp-adapter` exposes Engram's MCP surface by launching `engram mcp --tools=agent` and is also used by other Pi MCP integrations such as Notion.

Use an existing Engram HTTP server:

```bash
# Set ENGRAM_URL before launching the Pi agent CLI ("pi" is the command, not part of the URL)
ENGRAM_URL=http://127.0.0.1:7437 pi
```

`ENGRAM_URL` tells the `gentle-engram` Pi extension to use an already-running `engram serve` instance instead of auto-starting one. This is standard shell syntax: `KEY=value command`. The URL is the HTTP REST API base; it is not an MCP endpoint.

Use a custom Engram binary for MCP tools and local auto-start:

```bash
ENGRAM_BIN=/path/to/engram pi
```

If the binary is missing, the MCP launcher exits cleanly instead of crashing Pi with `spawn engram ENOENT`.

### Project auto-detection (important)

`mem_save` resolves its write project in this order: validated explicit `project`, existing `session_id` association, repo `.engram/config.json`/cwd detection, then directory-basename fallback. Use an explicit `project` when you intentionally want to target a known project; invalid or unbacked names fail loudly instead of silently falling back.

Other write tools still primarily use cwd/repo detection unless their schema says otherwise. Start the MCP server from the repo or add `.engram/config.json` when you want deterministic default writes.

OpenCode binds `mem_save`, `mem_save_prompt`, `mem_session_summary`, and `mem_capture_passive` to its confirmed top-level runtime session and maps subagents to their authoritative parent.

Pi binds `mem_save`, `mem_save_prompt`, `mem_session_summary`, and `mem_capture_passive` to the exact `ctx.sessionManager.getSessionId()` runtime session. Those four wrappers ignore model-supplied session IDs, and a missing or unacknowledged runtime session fails safely instead of writing under a synthesized ID.

To lock write tools to the canonical project for a repo, add `.engram/config.json` at the repo root:

```json
{
  "project_name": "sias-app"
}
```

When present, `project_name` is the default auto-detected target for writes from the repo and its subdirectories and overrides lower-confidence cwd/git detection. It is NOT an unbreakable lock against an explicit `mem_save(project=...)`, but explicit project writes are still validated against known context before they are accepted. Read tools can still use an explicit `project` filter when you need to query another existing project. Empty or invalid `project_name` values fail writes loudly instead of falling back silently.

For monorepos, prefer subproject configs such as `backend/.engram/config.json` and `frontend/.engram/config.json`. Engram uses the **nearest** config under the enclosing git root, so backend/frontend can resolve as separate projects while still blocking `$HOME/.engram/config.json` ancestor leakage.

**Recommended first call:** `mem_current_project` — confirms which project Engram detected before you start writing. It returns `project_source`, `project_strength`, and `implicit_write_allowed`, plus `available_projects` when cwd is ambiguous. Weak `git_root`, `git_child`, and `dir_basename` identities are useful for reads but do not authorize writes.

If a write returns `weak_project_identity`, do not copy the detected name back as
if it were user confirmation. Ask for the exact project and retry through an
explicit project field, or add a deliberate `.engram/config.json`. The stable
safe action is `provide an explicit project name and retry the write`.

If a write tool returns `ambiguous_project`, the agent must not guess. This happens when the MCP server is started from a parent directory that contains multiple repositories, for example:

```text
/Users/you/work
├── alan-thegentleman/
├── angular-18-jest-playwright/
└── engram/
```

The first write fails with an error like:

```json
{
  "error_code": "ambiguous_project",
  "available_projects": [
    "alan-thegentleman",
    "angular-18-jest-playwright",
    "engram"
  ]
}
```

Ask the user to choose exactly one value from `available_projects`. For ambiguous-project recovery, retry `mem_save` with BOTH fields:

```json
{
  "project": "chosen-project-from-available-projects",
  "project_choice_reason": "user_selected_after_ambiguous_project"
}
```

On success, `mem_save` writes to the selected project and reports the recovery source:

```json
{
  "project": "engram",
  "project_source": "user_selected_after_ambiguous_project",
  "project_path": "/Users/you/work/engram"
}
```

If the exact choices normalize to the same stored project bucket, Engram returns `project_name_collision` instead of writing. Ask the user to rename or disambiguate the colliding projects before retrying.

### Ambiguous-project recovery rules

Normal `mem_save` precedence:

- explicit `project`
- existing `session_id` project
- repo `.engram/config.json` / cwd detection
- directory-basename fallback

Additional rules:

- `project`, after trimming surrounding whitespace, must be a name, not a path.
- Empty, whitespace-only, path-like, or control-character names are rejected.
- Names are normalized the same way the store normalizes projects.
- Invalid explicit `project` names fail loudly.
- Valid-looking explicit `project` names are accepted only when backed by known context: an existing local project in the store, a matching existing session project, the nearest resolvable repo/subproject `.engram/config.json`, or exact ambiguous-project recovery.
- Unbacked explicit `project` values are rejected; `mem_save(project=...)` is a validated selection, not an arbitrary project-creation path.
- If `session_id` is provided and no session exists, `mem_save` fails loudly instead of falling back to cwd/config detection.
- If both explicit `project` and `session_id` are supplied, they must match after normalization or the write is rejected.
- `project_choice_reason=user_selected_after_ambiguous_project` is only valid when cwd detection is actually ambiguous; stale flags on a non-ambiguous cwd do not override explicit `project` precedence or session mismatch checks.
- When ambiguous-project recovery is active, `project` must exactly match one of `available_projects`; invented or normalized guesses are rejected.
- Exact choices may still fail with `project_name_collision` when two available names collapse to the same normalized storage bucket, such as `foo--bar` and `foo-bar`.
- Ordinary explicit `mem_save(project=...)` calls may also fail with `project_name_collision` when the raw explicit name collapses into an existing config-backed, session-backed, or store-backed project bucket, such as `foo--bar` versus `foo-bar`.

`mem_save_prompt` keeps the older cwd/default project-resolution behavior, but
it no longer implies persistence. Its content is written only when explicit
local Diagnostic consent exists for that project and content type. Its
`project` field is only for ambiguous-project recovery together with
`project_choice_reason=user_selected_after_ambiguous_project`.

Mental model:

```text
normal mem_save call
        ↓
explicit project wins when valid
        ↓
otherwise existing session project wins
        ↓
otherwise repo/cwd detection picks the default target
```

Ambiguous recovery:

```text
write fails with ambiguous_project
        ↓
user chooses one exact value from available_projects
        ↓
agent retries with project + project_choice_reason
        ↓
Engram validates the exact choice and writes to that repo
```

If validation returns `project_name_collision`, do not guess. Ask the user to disambiguate the project names first.

Alternatives: `cd` into the target repo before starting the MCP server, or add repo `.engram/config.json`.

**Read tools** (`mem_search`, `mem_context`, `mem_stats`, `mem_timeline`, `mem_doctor`) accept an optional `project` override validated against the store. Omit it to auto-detect. Default `mem_get_observation` uses the selected candidate's opaque `recall_id` and `result_id`; optional project/scope inputs must match that Recall run, and explicit continuation uses only the returned UTF-8 byte position. Explicit curation may still pass a numeric `id` for the legacy complete view, but cannot mix it with an opaque selection.

---

## OpenCode

> **Prerequisite**: Install the `engram` binary first (via [Homebrew](INSTALLATION.md#homebrew-macos--linux), [Windows binary](INSTALLATION.md#windows), [binary download](INSTALLATION.md#download-binary-all-platforms), or [source](INSTALLATION.md#install-from-source-macos--linux)). The plugin needs it for the MCP server and session tracking.

**Recommended: Full setup with one command** — installs the plugin AND registers the MCP server in `opencode.json` automatically:

```bash
engram setup opencode
```

This does three things:

1. Copies the plugin to `~/.config/opencode/plugins/engram.ts` (session tracking, Memory Protocol, compaction recovery)
2. Adds the `engram` MCP server entry to your `opencode.json` with `--tools=agent` (five agent-facing tools)
3. Adds `opencode-subagent-statusline` to your `tui.json` or `tui.jsonc` so OpenCode shows sub-agent activity in the sidebar/home footer

The plugin auto-starts the HTTP server if needed for session tracking. If your environment blocks background processes, run it manually:

```bash
engram serve &
```

The default `agent` profile contains exactly `mem_current_project`,
`mem_search`, `mem_get_observation`, `mem_checkpoint`, and
`mem_checkpoint_status`. Local-only `mem_pin` and `mem_unpin` are available
through the `curation` profile.

> **Windows**: OpenCode uses `~/.config/opencode/` on Windows too (it does not read `%APPDATA%\opencode\`). `engram setup opencode` writes to `~/.config/opencode/plugins/` and `~/.config/opencode/opencode.json`. To run the server in the background: `Start-Process engram -ArgumentList "serve" -WindowStyle Hidden` (PowerShell) or just run `engram serve` in a separate terminal.

**Alternative: Manual MCP-only setup** (no plugin, all 24 tools by default):

Add to your `opencode.json` (global: `~/.config/opencode/opencode.json` on all platforms, or project-level):

```json
{
  "mcp": {
    "engram": {
      "type": "local",
      "command": ["engram", "mcp"],
      "enabled": true
    }
  }
}
```

See [Plugins → OpenCode Plugin](PLUGINS.md#opencode-plugin) for details on what the plugin provides beyond bare MCP.

---

## Claude Code

> **Prerequisite**: Install the `engram` binary first (via [Homebrew](INSTALLATION.md#homebrew-macos--linux), [Windows binary](INSTALLATION.md#windows), [binary download](INSTALLATION.md#download-binary-all-platforms), or [source](INSTALLATION.md#install-from-source-macos--linux)). The plugin needs it for the MCP server and session tracking scripts.

**Option A: Plugin via marketplace (recommended)** — full session management, auto-import, compaction recovery, and Memory Protocol skill:

```bash
claude plugin marketplace add yersonargotev/engram
claude plugin install engram
```

That's it. The plugin registers the MCP server, hooks, and Memory Protocol skill automatically.

> **If the marketplace command fails with a schema error**
>
> Older Claude Code CLI versions cannot parse some plugin manifest fields and will reject `claude plugin marketplace add` with messages like `Invalid schema: plugins.0.source: Invalid input`. The fix is to update the CLI:
>
> ```bash
> claude --version  # check what you have
> claude update     # upgrade to the latest
> ```
>
> Then re-run the marketplace command. If you cannot update for some reason, **Option C (Bare MCP)** below works on any Claude Code version because it does not go through the marketplace.

**Option B: Plugin via `engram setup`** — same plugin, installed from the embedded binary:

```bash
engram setup claude-code
```

During setup, Engram also attempts to write durable user-level MCP config to `~/.claude/mcp/engram.json` using the absolute `engram` binary path; if that write is not possible, setup warns and continues. You'll be asked whether to add engram's agent-profile MCP tools to `~/.claude/settings.json` `permissions.allow`. The setup writes entries for both the durable user-level MCP server id (`mcp__engram__...`) and the plugin-scoped server id used by older Claude Code plugin installs, so re-running setup repairs stale or incomplete allowlists without adding startup delay.

**Option C: Bare MCP** — all 24 tools by default, no session management:

Add to your `.claude/settings.json` (project) or `~/.claude/settings.json` (global):

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp"]
    }
  }
}
```

With bare MCP, add a [Surviving Compaction](#surviving-compaction-recommended) prompt to your `CLAUDE.md` so the agent remembers to use Engram after context resets.

> **Windows note:** The Claude Code plugin hooks use bash scripts. On Windows, Claude Code runs hooks through Git Bash (bundled with [Git for Windows](https://gitforwindows.org/)) or WSL. The `UserPromptSubmit` hook automatically switches to a fork-light safe path under Git Bash/MSYS2: the first-prompt ToolSearch still runs, while later save-reminder checks are skipped so prompt submission does not block. If Git Bash itself is blocked by Defender/EDR, the plugin also ships `scripts/user-prompt-submit.ps1` as a native PowerShell fallback for local override/testing. **Option C (Bare MCP)** remains the no-hook fallback and works natively on Windows without any shell dependency. Windows usernames containing spaces (e.g. `C:\Users\John Doe\...`) are supported — all hook commands quote `${CLAUDE_PLUGIN_ROOT}` so the path is passed as a single argument even when it contains spaces.

PowerShell fallback test and local override example:

```powershell
'{"session_id":"edr/test:1"}' | pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\path\to\engram\plugin\claude-code\scripts\user-prompt-submit.ps1"
```

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "pwsh -NoProfile -ExecutionPolicy Bypass -File \"C:\\path\\to\\engram\\plugin\\claude-code\\scripts\\user-prompt-submit.ps1\"",
            "timeout": 2
          }
        ]
      }
    ]
  }
}
```

See [Plugins → Claude Code Plugin](PLUGINS.md#claude-code-plugin) for details on what the plugin provides.

### Troubleshooting: Claude Code plugin install on Linux

If `claude plugin install engram` fails on Linux with an error like:

```
EXDEV: cross-device link not permitted
```

this is a Node.js `fs.rename` limitation, not an Engram bug. Node uses `fs.rename` to move the downloaded plugin archive from the system temp directory (`/tmp`) to the plugin destination under your home directory. On many Linux systems `/tmp` and `/home` live on separate filesystems (common with `tmpfs` on `/tmp`), and the kernel rejects cross-device renames.

**One-shot workaround** — set `TMPDIR` to a location on the same filesystem as your home directory before running the install:

```bash
mkdir -p ~/.cache/claude-tmp
TMPDIR=~/.cache/claude-tmp claude plugin install engram
```

**Permanent fix** — add the export to your shell rc file so all future `claude plugin install` commands work without the prefix:

```bash
# ~/.bashrc or ~/.zshrc
export TMPDIR="$HOME/.cache/claude-tmp"
mkdir -p "$TMPDIR"
```

Then reload your shell (`source ~/.bashrc`) and re-run the install.

> This is an upstream Claude Code CLI limitation that affects any plugin installed via `claude plugin install`, not just Engram. Docker-based environments are typically not affected because the container's `/tmp` and `/home` usually share the same overlay filesystem.

---

## Gemini CLI

Recommended: one command to set up Gemini MCP and compaction recovery:

```bash
engram setup gemini-cli
```

`engram setup gemini-cli` now does three things:

- Registers `mcpServers.engram` in `~/.gemini/settings.json` (Windows: `%APPDATA%\gemini\settings.json`)
- Writes `~/.gemini/system.md` with the Engram Memory Protocol (includes post-compaction recovery)
- Ensures `~/.gemini/.env` contains `GEMINI_SYSTEM_MD=1` so Gemini actually loads that system prompt

> `engram setup gemini-cli` automatically writes the full Memory Protocol to `~/.gemini/system.md`, so the agent knows when selective Recall can change the work and how to make one Terminal Memory commit after the root turn settles. No additional configuration needed.

Manual alternative: add to your `~/.gemini/settings.json` (global) or `.gemini/settings.json` (project); on Windows: `%APPDATA%\gemini\settings.json`:

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--tools=agent"]
    }
  }
}
```

Or via the CLI:

```bash
gemini mcp add engram engram mcp --tools=agent
```

---

## Codex

Recommended: use a released Engram binary to install and verify the matching Codex integration:

```bash
engram setup codex
```

Stable setup is tied to the Engram binary that runs it. The command derives a release tag from the binary version, installs the marketplace from `https://github.com/yersonargotev/engram.git`, and verifies that the checked-out marketplace `HEAD` is the exact commit embedded in that binary. It never follows a moving branch in stable mode.

The command reports four independent capabilities:

- `plugin`: the marketplace authority, release commit, and installed plugin identity are verified.
- `mcp`: both the plugin MCP manifest and `[mcp_servers.engram]` are valid, the configured executable advertises `checkpoint record`, `checkpoint status`, and `checkpoint verify-stop`, and the MCP agent profile exposes `mem_checkpoint` plus `mem_checkpoint_status`. Homebrew installs use the stable `bin/engram` symlink instead of a versioned Cellar or Caskroom path.
- `activation-cue`: the installed plugin contains the complete canonical checkpoint skill, projects its single short cue through the same direct Core command on Unix and Windows, limits complete model-visible `SessionStart.additionalContext` to 4 KiB, and covers `startup`, `resume`, `clear`, and `compact` exactly once.
- `verifier`: the installed plugin provides the exact synchronous Engram `Stop` commands for Unix and Windows, a three-second timeout, and the canonical `scripts/stop.sh` Unix launcher from the verified plugin tree. Windows delegates directly to the Engram CLI without an intermediate script.

Setup is complete only when all four checks are `ready`. If the Codex CLI, plugin, MCP manifest, activation cue, or verifier is absent, the command reports an incomplete result instead of claiming success.

### Read-only integration status

Inspect the current integration without rerunning setup:

```bash
engram setup status codex
engram setup status codex --json
```

The command does not install, upgrade, repair, start an MCP server, rewrite configuration, or save Memory. It uses bounded read-only probes (`--version`, plugin inventory, filesystem/configuration inspection, and `checkpoint --help`) and reports these surfaces independently:

- `compatibility`: the Managed Pack version, Engram binary version, Codex plugin version, and monotonic Protocol contract version are separate axes. Each distributable reports attributable provenance and an inclusive `supported_protocol` range. Readiness uses the intersection of those ranges, never equality between the three distributable versions.

- `engram_cli` and `codex_cli`: availability, resolved executable path, and version. The Engram binary also reports its embedded source revision when available; the real CLI path fails compatibility provenance closed when that revision is malformed.
- `skill`: every relevant repository, user, administrator, or plugin-provided Engram memory skill, including scope, resolved path, SHA-256 identity, optional version, and disabled state when configured. A standalone `engram-memory-cli` copy is leftover compatibility evidence, not the canonical skill and not Codex activation.
- `marketplace` and `plugin`: registration is kept separate from installed/enabled plugin state; attributable source, requested ref, installed version, and resolved revision are included when known.
- `mcp_configuration` and `mcp_readiness`: a present registration is kept separate from an executable that passes the non-starting checkpoint CLI preflight. Missing, invalid, customized, and unavailable states remain distinct; status does not claim that a live stdio transport was contacted.
- `prompt_hook`, `session_hook`, `subagent_hook`, `activation_cue`, and `stop_verifier`: each canonical plugin contract is verified separately. The separate content-free `subagent_capture` object reports `default_disabled`, `consented`, `expired`, or `unavailable` without reading captured content or exposing session identifiers.
- `lifecycle_canary`: the selected treatment, default/environment source, canonical cue readiness, injection limit, and content-free aggregate SessionStart latency/injected bytes. Missing observations remain `not_observed`; status never enables collection, Capture, or the canary.

The stable `mode` field is conservative:

| Mode | Meaning |
| --- | --- |
| `manual_skill_cli` | Engram and Codex CLIs plus at least one enabled standalone skill other than leftover `engram-memory-cli` are available, without an attributable plugin or MCP registration. |
| `mcp_only` | The supported MCP registration and non-starting executable preflight are ready, but the complete plugin contract is not. |
| `partial_plugin` | Attributable plugin state exists, but one or more required capabilities are missing, unavailable, or unverified. |
| `checkpoint_ready` | The attributable Managed Pack, binary, plugin, and Protocol ranges intersect, and the plugin, MCP configuration/readiness, prompt/session/subagent hooks, activation cue, and Stop verifier are all ready. |
| `unknown` | The observed combination does not safely match another mode, including marketplace-only and customized states. |

JSON output uses the additive schema `codex-integration-status-v1`. Its `compatibility` object uses `protocol-compatibility-v1`, contains all four axes and their provenance, and returns either `protocol_compatible`, `legacy_compatible`, or a stable incompatible reason such as `managed_pack_missing`, `managed_pack_unprovenanced`, `managed_pack_protocol_range_malformed`, or `no_protocol_intersection`. Every capability check contains `capability`, `status`, `reason_code`, a bounded human reason, and bounded named evidence. Output is deterministic for unchanged local state.

The expand path recognizes the exact legacy Managed Pack `3.1.2` and Codex plugin `0.1.5` fingerprints as Protocol v1 declarations. It also preserves the verified previous Packs `3.3.0` and `3.2.0` plus plugin `0.1.6` coordinates. The current Managed Pack `3.3.1`, binary contract, and Codex plugin `0.1.7` project the terminal Memory policy and five-tool agent profile while still declaring `legacy_compatible`; later Capture and lifecycle slices remain independently staged. A mixed upgrade remains ready while every attributable range still intersects. Status does not rewrite that installation, and an unknown artifact with the same version is not admitted by version alone. Remove the legacy flag only after every remaining projection satisfies the target contract.

The frozen v1 evaluation selected `continue_canary`, so the applied distribution
outcome pins that current legacy-compatible tuple as a unit and authorizes no
release, rollout, downgrade, or legacy-path contraction. Use the read-only
`engram recall-study verify-distribution` command described in
[`RECALL-STUDY.md`](RECALL-STUDY.md) to verify its exact Git revision and source
hashes. Its embedded Git object membership proof requires neither full local
history nor network access. That source-outcome check does not claim anything
about the current installation: verify post-install readiness independently with
`engram setup status codex --json`.

Status describes installed capability only. It is not evidence that Codex loaded or invoked a skill, that hooks ran in the current session, or that the model created a Memory. Use session and checkpoint evidence for those claims.

### Checkpoint activation contract

The detailed `saved`, `skipped(no_durable_knowledge)`, and `needs_review`
rubric lives only in the installed `engram-memory` skill. The cue is a marked
short paragraph inside that same file; `SessionStart` hooks extract it instead
of carrying another protocol copy. Hook output uses
`hookSpecificOutput.additionalContext`, so the cue reaches the model rather than
appearing only as a UI warning.

The canary is disabled by default, preserving the current bounded broad-context
treatment. To select the cue-only treatment for a Codex process, set:

```bash
export ENGRAM_CODEX_RECALL_CANARY=targeted-recall
```

Use `targeted-recall-exact-session` only for the declared compact variant: its
`startup`, `resume`, and `clear` events remain cue-only, while `compact` may add
bounded context from the exact persisted session. Neither value enables
Capture. An unknown value is not treated as a canary and is surfaced as
`lifecycle_canary_treatment_invalid` by read-only status.

On every actual user prompt, `UserPromptSubmit` forwards Codex's opaque
`session_id` and `turn_id` as the checkpoint identity
`(host=codex, session_id, root_turn_id)`. This identity path always runs for a
valid event and never depends on prompt persistence. The prompt hook may offer
content to Core, but Diagnostic capture is disabled by default and requires
explicit local project/content-type consent. Prompt persistence receives the
event only through a detached Core command's `stdin`, and eligibility is bound
to the event's observed time so later consent cannot capture earlier content.
The `SubagentStop` hook delegates
directly to Core and defaults to no persistence; even with independent
`subagent_output` consent it accepts only the bounded Diagnostic envelope
described above. Neither hook selects a disposition or creates a checkpoint.
Tool calls, subagents, compaction events, and continuations remain within the
original root user turn; the root agent applies the canonical skill and
finalizes once.

The Unix and Windows manifests use the same direct Core commands for
`SessionStart`, `UserPromptSubmit`, and `SessionEnd`; no Git Bash lifecycle
wrapper is needed. `SessionEnd` closes only the exact existing session and
never creates Memory, a proposal, checkpoint, summary, feedback, or model
context.

`Stop` delegates the complete event to
`engram checkpoint verify-stop --host=codex`. The Go core queries the exact identity in the local checkpoint
ledger. A terminal `saved`, `skipped`, or `needs_review` result completes
normally. Only an absent checkpoint requests one recovery continuation,
carrying the original identity and instructing the agent not to checkpoint the
continuation itself. If `stop_hook_active` is already true, Engram never
requests a second continuation. Invalid input and store failures become visible
integration messages. Executable failures, malformed command output, and the
verified three-second hook timeout remain visible to Codex and are never
converted into `skipped`.

### Ownership and reruns

Engram changes Codex state only when it can attribute that state to its known generated value, expected path, and generated content. After all four replacement capabilities are `ready`, setup retires the former top-level `model_instructions_file` and `experimental_compact_prompt_file` activation only when each setting points to Engram's expected path and the corresponding regular file still has Engram's exact generated content. Modified files, custom paths, duplicate or malformed settings, orphaned files, and unrecognized marketplace or plugin state are preserved byte-for-byte and named in the result. A failed replacement check leaves the prior working activation path in place with a diagnostic. Engram never edits shared `AGENTS.md` or `CLAUDE.md` files.

Writes use atomic replacement. Exact-owned legacy files move through Engram-owned recovery paths before their settings are published; a rerun restores the prior activation or completes cleanup after an interruption. Fresh setup and supported legacy upgrades converge on one canonical detailed skill, one short cue, one synchronous Stop verifier, and thin CLI/MCP adapters without legacy protocol copies. A successful rerun is byte-stable and does not create duplicate MCP blocks.
An owned transaction marker makes marketplace upgrades restartable across interruptions between the ref change, marketplace refresh, and plugin installation; Engram removes it only after the requested plugin tree is verified.

For local plugin development only, explicitly opt into the moving `main` branch:

```bash
engram setup codex --development
```

Stable builds without a valid release version and exact commit refuse to mutate Codex state. If the Codex CLI is unavailable, no local Codex setup files are changed; install Codex and rerun the same stable command.

Manual MCP-only alternative: add the following registration to `~/.codex/config.toml` (Windows: `%APPDATA%\codex\config.toml`). This does not install the canonical checkpoint skill, cue, or Stop verifier, so use `engram setup codex` for the complete integration:

```toml
[mcp_servers.engram]
command = "engram"
args = ["mcp", "--tools=agent"]
```

### Troubleshooting: "MCP Transport closed"

Codex communicates with Engram over a stdio MCP session that is started fresh each time Codex launches. If that session becomes stale — for example after replacing the `engram` binary, editing `config.toml` or the instruction files, or force-stopping an `engram` process — subsequent tool calls fail with:

```
Transport closed
```

**Recovery sequence**

1. Close the current Codex chat or window entirely.
2. If any `engram` processes are still running, stop them:
   - macOS/Linux: `pkill -x engram`
   - Windows: `taskkill /IM engram.exe /F`
3. Open a new Codex chat. Codex starts a fresh `engram mcp` stdio process on launch, which clears the stale session.

**Prevention**

- After replacing `engram.exe` / the `engram` binary, always start a new Codex chat before using memory tools.
- After editing `~/.codex/config.toml`, `engram-instructions.md`, or `engram-compact-prompt.md`, restart Codex to pick up the new config.
- Avoid force-killing `engram` while a Codex session is active; prefer closing the chat first so Codex can shut down the MCP process cleanly.

> **Windows note:** On Windows the stale process is most commonly left behind after an in-place binary replacement. The `taskkill` command above reliably clears it. If Codex shows the error immediately on a fresh chat, confirm that the new `engram.exe` is in `PATH` and that no older copy is shadowing it.

---

## VS Code (Copilot / Claude Code Extension)

VS Code supports MCP servers natively in its chat panel (Copilot agent mode). This works with **any** AI agent running inside VS Code — Copilot, Claude Code extension, or any other MCP-compatible chat provider.

**Automated (user profile):**

```bash
engram setup vscode-copilot
```

This registers the engram server under the `servers` object (with `type: stdio`) in your VS Code User `mcp.json` and writes a Copilot instructions file at `<User>/prompts/engram.instructions.md` (frontmatter `applyTo: "**"`). User dir per platform: macOS `~/Library/Application Support/Code/User/`, Linux `~/.config/Code/User/`, Windows `%APPDATA%\Code\User\`.

**Option A: Workspace config** (recommended for teams — commit to source control):

Add to `.vscode/mcp.json` in your project:

```json
{
  "servers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--tools=agent"]
    }
  }
}
```

**Option B: User profile** (global, available across all workspaces):

1. Open Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`)
2. Run **MCP: Open User Configuration**
3. Add the same `engram` server entry above to VS Code User `mcp.json`:
   - macOS: `~/Library/Application Support/Code/User/mcp.json`
   - Linux: `~/.config/Code/User/mcp.json`
   - Windows: `%APPDATA%\Code\User\mcp.json`

**Option C: CLI one-liner:**

```bash
code --add-mcp "{\"name\":\"engram\",\"command\":\"engram\",\"args\":[\"mcp\",\"--tools=agent\"]}"
```

> **Using Claude Code extension in VS Code?** The Claude Code extension runs inside VS Code but uses its own MCP config. Follow the [Claude Code](#claude-code) instructions above — the `.claude/settings.json` config works whether you use Claude Code as a CLI or as a VS Code extension.

> **Windows**: Make sure `engram.exe` is in your `PATH`. VS Code resolves MCP commands from the system PATH.

**Adding the Memory Protocol** (recommended — teaches selective Recall and one terminal commit):

Without the Memory Protocol, the agent has the tools but not the canonical durability and Recall policy. Add these instructions to your agent's prompt:

**For Copilot:** Create a `.instructions.md` file in the VS Code User `prompts/`
folder and add the canonical pointer described in
[DOCS.md](../DOCS.md#memory-protocol).

Recommended file path:

- macOS: `~/Library/Application Support/Code/User/prompts/engram-memory.instructions.md`
- Linux: `~/.config/Code/User/prompts/engram-memory.instructions.md`
- Windows: `%APPDATA%\Code\User\prompts\engram-memory.instructions.md`

**For any VS Code chat extension:** Add the Memory Protocol text to your extension's custom instructions or system prompt configuration.

The Memory Protocol tells the agent to use selective Recall and make one
terminal `saved`, `needs_review`, or `skipped(no_durable_knowledge)` checkpoint
after each settled root user turn. The canonical skill owns the detailed rubric.
Independent save and optional Session summary remain explicit curation
workflows.

Selective Recall is authority-aware: automatic use requires strong or explicit
project identity and starts with one narrow request capped at five candidates
and 4 KiB, with at most one reformulation. Limits 6-10 and personal or
cross-project scope are deliberate. Empty Recall is a warning-free success;
unavailable Recall is non-blocking and leaves one warning plus diagnostics.

See [Surviving Compaction](#surviving-compaction-recommended) for the minimal
pointer and [DOCS.md](../DOCS.md#memory-protocol) for the canonical policy.

### Project detection in VS Code, WSL, and CI

VS Code, WSL, and most CI runners start the MCP server process without inheriting the shell's working directory, so cwd-based project detection may resolve to the wrong project or fall back to a weak directory basename. Weak detection remains available for generic discovery, but candidate Recall returns empty with one warning and writes fail closed with `weak_project_identity`.

The reliable fix is to pin the project explicitly at startup time. Both forms below work:

**Flag form** (recommended — visible in config):

```json
{
  "servers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--project=my-project", "--tools=agent"]
    }
  }
}
```

**Environment variable form** (useful when the config format does not support extra args, or when you want to override without editing the config file):

```json
{
  "servers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--tools=agent"],
      "env": {
        "ENGRAM_PROJECT": "my-project"
      }
    }
  }
}
```

Both `--project=my-project` and `ENGRAM_PROJECT=my-project` set `MCPConfig.DefaultProject`, which takes precedence over cwd detection for every read and write tool for the lifetime of that MCP process.

> The `--project` flag and `ENGRAM_PROJECT` env var are the same mechanism. If both are supplied, the flag wins. The value must match an existing project name in your Engram store; unknown names are rejected so typos fail loudly instead of silently creating a new project bucket.

Same pattern applies to:
- WSL terminals where VS Code opens a remote window (`\\wsl$\...` paths) — the MCP server process runs inside WSL but VS Code does not forward the workspace directory as cwd.
- CI pipelines (GitHub Actions, GitLab CI, etc.) where the agent runs in a container and the checkout path differs from the project name you use locally.
- Any Docker-based agent host where the container cwd does not match your Engram project name.

---

## Antigravity

[Antigravity](https://antigravity.google) is Google's AI-first IDE/CLI with native MCP and skill support.

**Automated:**

```bash
engram setup antigravity-cli
```

This registers `mcpServers.engram` in the shared `~/.gemini/config/mcp_config.json` (read by Antigravity CLI, IDE, and SDK) and writes the Memory Protocol as a marker-delimited block in `~/.gemini/GEMINI.md`, preserving any existing content.

**Manual** — open the MCP Store (`...` dropdown in the agent panel) → **Manage MCP Servers** → **View raw config**, and add to `~/.gemini/config/mcp_config.json`:

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--tools=agent"]
    }
  }
}
```

Then add the canonical Memory pointer as a global rule in
`~/.gemini/GEMINI.md`. See [DOCS.md](../DOCS.md#memory-protocol).

> **Note:** Antigravity has its own skill, rule, and MCP systems separate from VS Code. Do not use `.vscode/mcp.json`. This is distinct from `engram setup gemini-cli`, which writes the Gemini CLI's own `settings.json` / `system.md`.

---

## Cursor

**Automated:**

```bash
engram setup cursor
```

This installs the portable Agent Plugin into Cursor's local plugin directory at `~/.cursor/plugins/local/engram`: `plugin.json`, editorial `skills/engram-memory`, plugin `mcp.json`, and a copy of the running binary. The portable package keeps a plugin-relative `./bin/engram mcp --tools=agent` command; setup rewrites the installed `mcp.json` command to that copied binary's absolute path because Cursor resolves relative MCP commands against the workspace cwd, not the plugin root. Stable setup pins that install to the binary's release version and commit. A second run refreshes only the Engram-owned plugin and leaves other local plugins alone.

Setup also writes user-level Cursor hooks in `~/.cursor/hooks.json`. `sessionStart` injects the short activation cue from the installed skill. `stop` may emit one follow-up when that root turn has no Memory checkpoint. The hook commands parse Cursor JSON, call Engram, and return; they do not decide durability. Prompt and subagent capture stay off. A second run refreshes Engram-owned hook entries and leaves user-owned hooks in place.

Setup does not write a second native `~/.cursor/mcp.json` activation entry, does not copy the skill into `~/.cursor/skills` or `~/.agents/skills`, and does not write project-level `.cursor` files in the current working tree. An existing Engram-owned `mcpServers.engram` entry that still matches the old native shape is removed after the plugin is in place; a customized entry is preserved.

### Read-only integration status

Inspect the current Cursor integration without rerunning setup:

```bash
engram setup status cursor
engram setup status cursor --json
```

The command does not install, repair, start an MCP server, or read captured content. It reports `plugin`, `skill`, `MCP`, and `hooks` as independent file-checkable capabilities. `user_rules` stays `unknown` because setup cannot inspect the Settings store. Missing, stale, customized, and ready states stay distinct. An empty or MCP-only profile is not `checkpoint_ready`. Leftover user skill copies, including Packy `engram-memory-cli`, are reported and are not treated as the canonical plugin skill. Run `engram setup cursor` to install the editorial rubric; Packy may remain for compatibility.

**Manual** — copy `plugin/engram/` to `~/.cursor/plugins/local/engram`, place the Engram binary at `bin/engram`, rewrite the installed `mcp.json` command from `./bin/engram` to that binary's absolute path, and add user-level `sessionStart` / `stop` hook entries in `~/.cursor/hooks.json` that call that binary (`lifecycle session-start --host=cursor` and `checkpoint verify-stop --host=cursor`). Then restart Cursor or run Developer: Reload Window.

> Reload Cursor after setup so the local Agent Plugin is discovered. On Teams and Enterprise, local plugin imports may be gated by an admin setting.

User-level hooks (`~/.cursor/hooks.json`) and the local Agent Plugin (`~/.cursor/plugins/local/engram`) are not Cloud Agent coverage. Cursor Cloud Agents do not load those user-scoped files.

---

## Windsurf

**Automated:**

```bash
engram setup windsurf
```

This registers `mcpServers.engram` in `~/.codeium/windsurf/mcp_config.json` (Cascade's MCP config) and writes the Memory Protocol as a marker block in `~/.codeium/windsurf/memories/global_rules.md`.

**Manual** — add to `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp", "--tools=agent"]
    }
  }
}
```

> **Memory Protocol:** Add the canonical Memory pointer to `~/.codeium/windsurf/memories/global_rules.md`. See [DOCS.md](../DOCS.md#memory-protocol).

---

## Qwen Code

**Automated:**

```bash
engram setup qwen
```

Registers `mcpServers.engram` in `~/.qwen/settings.json` and writes the Memory Protocol as a marker block in `~/.qwen/QWEN.md`.

---

## Kiro

**Automated:**

```bash
engram setup kiro
```

Registers `mcpServers.engram` in `~/.kiro/settings/mcp.json` and writes the Memory Protocol as a marker block in `~/.kiro/steering/engram.md`. (Kiro uses a split layout: MCP and steering live under `~/.kiro/` regardless of where the IDE keeps app settings.)

---

## Kilo Code

**Automated:**

```bash
engram setup kilocode
```

Registers the engram server under the OpenCode-style `mcp` object in `~/.config/kilo/opencode.json` and writes the Memory Protocol as a marker block in `~/.config/kilo/AGENTS.md`.

---

## Any other MCP agent

The pattern is always the same — point your agent's MCP config to
`engram mcp --tools=agent` via stdio transport. Select a specialized profile
only when that integration owns the corresponding workflow.

---

## Surviving Compaction (Recommended)

> **Is this step required?** No — `engram setup` handles the host-specific wiring. These snippets are an optional resilience layer. Add them if your agent forgets about Engram after long sessions or context resets. They are especially useful for agents that do not have a full plugin (VS Code, Windsurf, Antigravity) and have no automated session tracking.

When an agent does not support the complete Engram plugin, add this compact
pointer to its persistent instruction surface:

**For Claude Code** (`CLAUDE.md`):

```markdown
## Memory

Use the canonical Engram Memory skill. After each settled root user turn, commit
one terminal Memory checkpoint. Recall prior Memory only when it can change the work.
```

**For OpenCode** (agent prompt in `opencode.json`):

```
Use the canonical Engram Memory skill. After each settled root user turn, commit
one terminal Memory checkpoint. Recall prior Memory only when it can change the work.
```

**For Gemini CLI** (`GEMINI.md`):

```markdown
## Memory

Use the canonical Engram Memory skill. After each settled root user turn, commit
one terminal Memory checkpoint. Recall prior Memory only when it can change the work.
```

**For VS Code** (`Code/User/prompts/*.instructions.md` or custom instructions):

```markdown
## Memory

Use the canonical Engram Memory skill. After each settled root user turn, commit
one terminal Memory checkpoint. Recall prior Memory only when it can change the work.
```

**For Antigravity** (`~/.gemini/GEMINI.md` or `.agent/rules/`):

```markdown
## Memory

Use the canonical Engram Memory skill. After each settled root user turn, commit
one terminal Memory checkpoint. Recall prior Memory only when it can change the work.
```

**For Cursor** (`engram setup cursor` already installs the short activation cue):

`sessionStart` re-delivers this short activation cue from the installed `engram-memory` skill after compaction. Do not paste the full Memory rubric into User Rules or a global `.mdc` file.

```markdown
For every root user turn, use the engram-memory skill to make exactly one Terminal Memory commit after all causal work settles: `saved`, `skipped(no_durable_knowledge)`, or `needs_review`. Current user intent, maintained source, and runtime evidence override Memory. Reuse the supplied host checkpoint identity across continuations; subagents do not create checkpoints.
```

**For Windsurf** (`.windsurfrules`):

```
Use the canonical Engram Memory skill. Commit one terminal Memory checkpoint for
each settled root user turn and use selective Recall when it can change the work.
```

This is the **nuclear option** — system prompts survive everything, including compaction. Use it when you want guaranteed agent behavior without relying on plugin hooks. It is optional for agents that have a full plugin (Claude Code, OpenCode, Gemini CLI, Codex, Cursor) and required for agents that do not (VS Code, Windsurf, Antigravity).

---

## Conflict Surfacing (automatic)

When you save a memory with `mem_save`, Engram automatically scans for similar existing observations using FTS5 full-text search. If any candidates are found above a relevance threshold, the response includes a `candidates[]` array and `judgment_required: true`. Nothing to configure — this runs on every save.

### What the agent sees

`mem_save` returns an enriched envelope when candidates exist:

```json
{
  "result": "Memory saved: \"...\"\nCONFLICT REVIEW PENDING — 2 candidate(s); use mem_judge to record verdicts.",
  "id": 42,
  "sync_id": "obs_abc123",
  "judgment_required": true,
  "judgment_status": "pending",
  "judgment_id": "rel-<hex>",
  "candidates": [
    {
      "id": 18,
      "sync_id": "obs_xyz789",
      "title": "We use sessions for auth",
      "type": "decision",
      "score": -3.14,
      "judgment_id": "rel-<hex-for-this-pair>"
    }
  ]
}
```

When no candidates are found, `judgment_required` is `false` and no `candidates` field is present. The `result` string is unchanged.

### How the agent resolves conflicts

The agent iterates `candidates[]` and calls `mem_judge` once per entry, using that entry's own `judgment_id`. The agent does NOT use the top-level `judgment_id` for multiple candidates — each candidate has its own.

The agent's built-in heuristic (from `serverInstructions`) decides when to ask the user versus resolve autonomously:

- **Ask the user** when confidence is below 0.7, OR when the chosen relation is `supersedes` or `conflicts_with` AND the observation type is `architecture`, `policy`, or `decision`.
- **Resolve silently** when confidence >= 0.7 AND the relation is `related`, `compatible`, `scoped`, or `not_conflict`.

When asking, the agent raises it naturally in the conversation — not as a blocking CLI prompt or dashboard action.

### How the user sees this

The user sees it in the normal conversation flow. Example:

> "I noticed memory #18 ('We use sessions for auth') might conflict with what we just saved. Want me to mark the new one as superseding it, or are they about different scopes? I can also mark them as compatible if both still apply."

There is no separate dashboard or conflict list in Phase 1.

### What happens after judgment

Once the agent calls `mem_judge` with a verdict:

- The relation row is persisted with `judgment_status: "judged"` and the chosen `relation`.
- If the relation is `supersedes`, future candidate Recall excludes the judged target as obsolete.
- If the relation is `conflicts_with`, future eligible candidates on both sides contain a structured `conflicts[]` warning with the relation and related Memory identity. Counterpart metadata is omitted unless that related Memory independently passes the same active, current, project, and scope boundary.
- If the relation is `compatible`, `related`, `scoped`, or `not_conflict`, the judgment is stored in `memory_relations` but no Recall conflict appears.

**Cloud sync**: when the project is enrolled in Engram Cloud and autosync is enabled, `mem_judge` verdicts propagate to other machines via the standard mutation push/pull cycle. Candidate eligibility and structured conflicts reflect the judgment on any machine that has pulled the relevant mutations. Relations that reference an observation not yet present locally are deferred and retried automatically on subsequent pull cycles — the verdict is never lost.

Nothing breaks if `mem_judge` is never called — pending relations accumulate unjudged but do not affect other operations.

### Proactive semantic comparison (mem_compare)

Agents can also judge the relationship between any two memories using
`mem_compare` from the `curation` profile. Unlike `mem_judge`, which resolves a
candidate surfaced by `mem_save`, `mem_compare` lets the agent compare any two
observation IDs it has already read and persist a verdict directly.

See [Plugins → mem_compare reference](PLUGINS.md#mcp-tool-reference--mem_compare) for parameters and behavior.

---

## Cloud Autosync toggle

`engram serve` and `engram mcp` support continuous background replication to an Engram Cloud server. This is **opt-in** and never fatal on missing config.

### Prerequisites

1. A running Engram Cloud server (see `docker-compose.cloud.yml` or `engram cloud serve`). The server must be a build that includes the mutation endpoints (`POST /sync/mutations/push`, `GET /sync/mutations/pull`). If the server is older, autosync enters `PhaseBackoff` with `reason_code: transport_failed` and logs `server_unsupported` to stderr.

2. A valid bearer token configured on the server.

### Enable autosync

```sh
export ENGRAM_CLOUD_AUTOSYNC=1          # exact "1" only
export ENGRAM_CLOUD_TOKEN=your-token    # bearer token
export ENGRAM_CLOUD_SERVER=https://cloud.engram.example.com

engram serve
# or
engram mcp
```

The process logs `[autosync] started (server=...)` on success. Missing token or server URL logs `[autosync] ERROR: ...` and the process starts normally without autosync.
For `engram mcp`, autosync runs for the lifetime of the stdio MCP process and is stopped when that process exits.

---

## Cloud dashboard (templ contributors)

If you are contributing to the cloud dashboard (`internal/cloud/dashboard/`), the HTML components are rendered via [templ](https://templ.guide/). Before committing changes to any `.templ` file, regenerate the Go output:

```sh
# Download pinned version (first time only)
go mod download

# Regenerate
make templ
# or directly:
go tool templ generate ./internal/cloud/dashboard/...
```

Commit the regenerated `components_templ.go`, `layout_templ.go`, and `login_templ.go` alongside your `.templ` source changes. CI will fail if they are missing or outdated (`TestTemplGeneratedFilesAreCheckedIn`).
