[← Back to README](../README.md)

# Plugins

> Deferred scope note: plugin-level automatic cloud enrollment/login/upgrade orchestration is not part of this rollout yet. Current cloud flows are CLI-driven (`engram cloud ...`).
>
> Validation boundary (current): plugin scripts are validated for memory/session workflows, not as cloud bootstrap orchestrators. Use CLI for cloud config/auth/enrollment/upgrade.

- [Current plugin coverage](#current-plugin-coverage)
- [OpenCode Plugin](#opencode-plugin)
- [Claude Code Plugin](#claude-code-plugin)
- [Codex Plugin](#codex-plugin)
- [Privacy](#privacy)

---

## Current plugin coverage

| Integration | Coverage |
|---|---|
| OpenCode | TypeScript plugin plus MCP registration via `engram setup opencode`. |
| Claude Code | Marketplace/bundled plugin plus best-effort durable user MCP config via `engram setup claude-code`. |
| Codex | Codex plugin assets under `plugin/codex/`; `engram setup codex` pins the binary's release tag and commit, preserves unrecognized state, and reports plugin, MCP, activation-cue, and verifier readiness separately. |
| Pi | Pi package under `plugin/pi/` exposes Pi-native HTTP memory tools and configures MCP through `pi-mcp-adapter`. |

---

## OpenCode Plugin

For [OpenCode](https://opencode.ai) users, a thin TypeScript plugin adds enhanced session management on top of the MCP tools:

```bash
# Install via engram (recommended — works from Homebrew or binary install)
engram setup opencode

# Or manually: cp plugin/opencode/engram.ts ~/.config/opencode/plugins/
```

The plugin auto-starts the HTTP server if it's not already running — no manual `engram serve` needed.

> **Local model compatibility:** The plugin works with all models, including local ones served via llama.cpp, Ollama, or similar. The Memory Protocol is concatenated into the existing system prompt (not added as a separate system message), so models with strict Jinja templates (Qwen, Mistral/Ministral) work correctly.

### What the Plugin Does

The plugin:
- **Auto-starts** the engram server if not running
- **Consumes core project identity** from `/project/current`; weak identities
  allow generic discovery but candidate Recall returns empty with one warning
  and cannot trigger session or Memory writes
- **Auto-imports** git-synced memories from `.engram/manifest.json` only when the current project has strong write authority
- **Creates sessions** on-demand via `ensureSession()` (resilient to restarts/reconnects)
- **Injects the Memory Protocol** into the agent's system prompt via `chat.system.transform` — selective Recall plus one terminal Memory checkpoint per settled root user turn. The protocol is concatenated into the existing system message (not pushed as a separate one), ensuring compatibility with models that only accept a single system block (Qwen, Mistral/Ministral via llama.cpp, etc.)
- **Injects session-only runtime context** into the compaction prompt; manual `mem_context` and `GET /context` remain project/scope-scoped
- **Carries compaction context** without turning compaction into a new root user turn or disposition
- **Strips `<private>` tags** before sending data
- **Enables** `opencode-subagent-statusline` in `tui.json` or `tui.jsonc` during `engram setup opencode`, adding a live sub-agent monitor to OpenCode's sidebar/home footer. To disable it later, remove `"opencode-subagent-statusline"` from the `"plugin"` array in your TUI config and restart OpenCode.

**No raw tool call recording** — the agent commits durable Memory through the
canonical terminal checkpoint. Independent saves and optional Session summaries
remain explicit curation operations.

### Memory Protocol (injected via system prompt)

The plugin injects the canonical terminal policy: selective Recall when prior
Memory can change the work, followed by exactly one `saved`, `needs_review`, or
`skipped(no_durable_knowledge)` checkpoint after the root turn settles.

### Three Layers of Memory Resilience

The OpenCode plugin uses a defense-in-depth strategy to ensure memories survive compaction:

| Layer | Mechanism | Survives Compaction? |
|-------|-----------|---------------------|
| **System Prompt** | `MEMORY_INSTRUCTIONS` concatenated into existing system prompt via `chat.system.transform` | Always present |
| **Compaction Hook** | Injects session-bound context and preserves the same root-turn checkpoint cue | Fires during compaction |
| **Canonical skill** | Keeps selective Recall optional and commits once only after the root turn settles | Always present |

---

## Claude Code Plugin

For [Claude Code](https://docs.anthropic.com/en/docs/claude-code) users, a plugin adds enhanced session management using Claude's native hook and skill system:

```bash
# Install via Claude Code marketplace (recommended)
claude plugin marketplace add yersonargotev/engram
claude plugin install engram

# Or via engram binary (works from Homebrew or binary install)
engram setup claude-code

# Or for local development/testing from the repo
claude --plugin-dir ./plugin/claude-code
```

### What the Plugin Provides (vs bare MCP)

| Feature | Bare MCP | Plugin |
|---------|----------|--------|
| MCP tools available | 24 with `engram mcp --tools=all` | Five agent-profile tools (`engram mcp --tools=agent`) |
| Session tracking (auto-start) | ✗ | ✓ |
| Auto-import git-synced memories | ✗ | ✓ |
| Compaction recovery | ✗ | ✓ |
| Memory Protocol skill | ✗ | ✓ |
| Previous session context injection | ✗ | ✓ |

The agent profile contains exactly `mem_current_project`, `mem_search`,
`mem_get_observation`, `mem_checkpoint`, and `mem_checkpoint_status`.
Local-only pins are available through the `curation` profile.

### Plugin Structure

```
plugin/claude-code/
├── .claude-plugin/plugin.json     # Plugin manifest
├── .mcp.json                      # Registers engram MCP server
├── hooks/hooks.json               # SessionStart + SubagentStop + Stop lifecycle hooks
├── scripts/
│   ├── session-start.sh           # Ensures server, creates session, imports chunks, injects context
│   ├── post-compaction.sh         # Injects previous context + recovery instructions
│   ├── user-prompt-submit.sh      # Loads MCP tools on first prompt; Windows Git Bash safe mode
│   ├── user-prompt-submit.ps1     # Optional Windows-native fallback for locked-down endpoints
│   └── session-stop.sh            # Logs end-of-session event
└── skills/memory/SKILL.md         # Canonical terminal Memory policy
```

### How It Works

**On session start** (`startup`):
1. Ensures the engram HTTP server is running
2. Resolves the project through `/project/current` and creates a session only for strong or explicit authority
3. Auto-imports git-synced chunks from `.engram/manifest.json` (if present)
4. Injects previous session context into Claude's initial context

**On compaction** (`compact`):
1. Injects the previous session context and compacted summary.
2. Preserves the original root-turn identity across the continuation.
3. Leaves disposition selection to the canonical skill after causal work settles.

**On subagent completion** (`SubagentStop`): the manifest delegates directly to
`engram capture subagent-hook --host=claude-code`. The default is no
persistence. Independent `subagent_output` consent permits only a bounded
`engram_diagnostic` JSON envelope; raw output and `stdout` fallback are
rejected, and no subagent event creates Memory or checkpoint state.

**On user prompt submit**:
1. The hook reports the opaque host/session/root-turn identity independently of prompt persistence. It offers prompt content only to Core's local consent gate after strong or explicit project resolution; weak identities and disabled consent fail closed without blocking the user prompt.
2. The first prompt injects a ToolSearch instruction so Claude Code loads Engram MCP tools before responding.
3. Later prompts may inject a save reminder if the local Engram API is fast and available.
4. On Windows Git Bash/MSYS2, the hook uses a bash-builtin-only safe path to avoid fork-heavy helpers (`jq`, `git`, `curl`, `date`). In that mode first-prompt ToolSearch still works, but later save reminders degrade to `{}` so prompt submission stays fast.

If Git Bash itself is blocked by enterprise security tooling, `scripts/user-prompt-submit.ps1` is provided as a native PowerShell fallback for manual hook testing or local override.

PowerShell local override/testing example for locked-down Windows endpoints:

```powershell
# Test the native fallback directly. First run emits ToolSearch; second run emits {}.
'{"session_id":"edr/test:1"}' | pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\path\to\engram\plugin\claude-code\scripts\user-prompt-submit.ps1"

# Local Claude Code override in .claude/settings.json or user settings:
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

**Memory Protocol skill** (always available):

- Owns the single `saved`, `needs_review`, or `skipped(no_durable_knowledge)` rubric.
- Uses selective Recall only when prior history can change the work: automatic
  project Recall requires strong/explicit identity, begins at five candidates
  and 4 KiB, and fails open visibly without blocking the task.
- Commits one terminal checkpoint per settled root user turn, including across compaction.
- Reserves independent save and optional Session summary for explicit curation.

---

## Codex Plugin

The Codex plugin is a thin activation and identity adapter around the Go
checkpoint core:

```text
plugin/codex/
├── .codex-plugin/plugin.json
├── .mcp.json
├── hooks/hooks.json
├── scripts/
│   ├── _checkpoint.sh             # Extracts and renders the canonical cue
│   ├── session-start.sh           # startup, resume, and clear
│   ├── post-compaction.sh         # compact
│   ├── user-prompt-submit.sh      # Opaque root identity + consent-gated capture offer
│   └── stop.sh                    # Unix exact-checkpoint verifier
└── skills/memory/SKILL.md         # Canonical cue and complete rubric
```

The skill is the single source for both the minimal cue and the detailed
`saved`, `skipped(no_durable_knowledge)`, and `needs_review` rubric. The two
`SessionStart` scripts extract that cue and return it through
`hookSpecificOutput.additionalContext`; they contain no independent Memory
policy. Codex runs them exactly once for each supported source: `startup`,
`resume`, `clear`, and `compact`.

`UserPromptSubmit` forwards Codex's `turn_id` as Engram's `root_turn_id` beside
the session ID. It does not finalize a checkpoint. This preserves one root-turn
identity for the root agent while tools and subagents remain internal activity.
Identity reporting does not require prompt persistence: Diagnostic capture is
off by default and Core evaluates any content offer against explicit local
project/content-type consent.

`SubagentStop` delegates directly to
`engram capture subagent-hook --host=codex` on Unix and Windows. It does not
use a passive-extraction script. Without independent `subagent_output` consent
it persists nothing; with consent it accepts only the bounded Diagnostic
envelope and remains outside Memory, proposals, checkpoints, summaries, and
retired evaluation/promotion state. The root agent alone owns terminal
preservation.

`Stop` delegates the complete event to
`engram checkpoint verify-stop --host=codex`; Windows invokes that command
directly, while Unix uses the thin `stop.sh` launcher. The Go core queries that exact identity in the local checkpoint
ledger. A terminal `saved`, `skipped`, or `needs_review` checkpoint completes
with no continuation. Only an absent checkpoint can request one recovery
continuation, which carries the original identity and tells the root agent not
to checkpoint the continuation itself. Codex's `stop_hook_active` prevents a
second continuation. Invalid input and store failures are surfaced as
integration messages. Executable failures, malformed command output, and
Codex's three-second hook timeout remain visible and never become `skipped`.

`engram setup codex` verifies the immutable plugin tree, MCP manifest, canonical
skill, cue, lifecycle coverage, exact synchronous `Stop` command, timeout, and
the Unix launcher used by the verifier. It preserves user-owned settings and never
adds protocol prose to shared `AGENTS.md` or `CLAUDE.md` files. Setup reports
complete only when both activation and checkpoint verification are ready.

`engram setup status codex [--json]` is the non-mutating diagnostic counterpart.
It reports standalone and plugin-provided skills, marketplace registration,
installed/enabled plugin provenance, MCP configuration and executable preflight,
prompt/session/subagent hooks, the canonical activation cue, and the Stop
verifier as separate checks. Its content-free `subagent_capture` object reports
`default_disabled`, `consented`, `expired`, or `unavailable` without reading
captured content or exposing session identifiers. It also reports Managed Pack,
binary, plugin, and Protocol contract versions separately, with attributable
range declarations and their computed intersection. Its `manual_skill_cli`, `mcp_only`, `partial_plugin`,
`checkpoint_ready`, and `unknown` modes never promote marketplace registration
or customized, missing, malformed, ambiguous, or non-overlapping Protocol state
to full readiness. The result is an installed-capability
snapshot; it does not prove that Codex invoked the skill or created a Memory in
a particular session.

Protocol v1 is Core-owned in `internal/protocolcontract`. The single parity
fixture at `assets/protocol-contract-v1.json` is distributed by the Managed Pack
and validated against the five minimum MCP tools, MCP initialization guidance,
checkpoint tool descriptions, canonical cue markers, plugin metadata, and setup
compatibility. Editorial skill prose remains hand-authored; the fixture detects
contract drift rather than generating that prose.

---

## MCP Tool Reference — mem_judge

`mem_judge` is available in the `curation` profile
(`engram mcp --tools=curation`). It is not exposed in the default `agent` or
`admin` profiles.

### Purpose

Records a verdict on a pending memory conflict surfaced by `mem_save`. When `mem_save` returns `judgment_required: true`, the agent iterates `candidates[]` and calls `mem_judge` once per entry.

### Parameters

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `judgment_id` | yes | string | From `candidates[].judgment_id` in the `mem_save` response. Format: `rel-<hex>`. |
| `relation` | yes | string | One of: `related`, `compatible`, `scoped`, `conflicts_with`, `supersedes`, `not_conflict` |
| `reason` | no | string | Free-text explanation of the verdict |
| `evidence` | no | string | Supporting evidence (JSON or free text; e.g., user's exact words) |
| `confidence` | no | float | 0.0..1.0 — default 1.0; clamped to range |
| `session_id` | no | string | Session ID for provenance (auto-detected if omitted) |

### Behavior

On success, `mem_judge`:
- Flips `judgment_status` from `pending` to `judged` on the matching `memory_relations` row
- Persists `relation`, `reason`, `evidence`, `confidence`, actor provenance (`actor="agent"`, `marked_by_kind="agent"`), and `session_id`
- Returns the updated relation row as JSON

On error (unknown `judgment_id` or invalid `relation`), returns `IsError: true`. The relation row is NOT mutated on error.

Re-judging an already-judged `judgment_id` overwrites the verdict (deliberate revision is allowed).

### Candidate Recall relation behavior

After a verdict is recorded, bounded `mem_search` candidates behave as follows:

| Relation verdict | Candidate Recall behavior |
|-----------------|---------------------------|
| `supersedes` | The judged target is excluded as obsolete; the current source may still qualify. |
| `conflicts_with` (judged) | Both eligible sides carry a structured `conflicts[]` entry with `status: "judged"`. |
| `pending` (not yet judged) | Both eligible sides carry a structured `conflicts[]` entry with `status: "pending"`. |
| `compatible`, `related`, `scoped`, `not_conflict` | The judgment is stored but not surfaced as a Recall conflict. |

Deleted, inactive, and superseded Memories are excluded before the limit.
Conflict entries contain `relation_id`, the related Memory ID/sync ID/title,
and status. They are warnings, not an instruction to silently choose one side.

### Multi-actor sync_id namespace

Multiple agents can independently analyze the same pair of observations and each produce a distinct `memory_relations` row — even if they refer to the same `(source_id, target_id)` pair. Each row receives its own unique `sync_id`, so there is **no uniqueness constraint** on `(source_id, target_id)`.

Each row remains a distinct conflict entry, keyed by `relation_id`; consumers
must not deduplicate multiple actors merely because they reference the same
Memory pair. Actor/model provenance remains on the underlying relation row.

### Cloud sync for judgments

When a project is enrolled in Engram Cloud and autosync is enabled, `mem_judge` verdicts sync across machines. The `memory_relations` table propagates via the standard mutation push/pull cycle — the same pipeline used for observations and sessions. Judgments affect candidate eligibility and structured conflicts on any machine that has pulled the relevant mutations.

Relations where the referenced observation does not yet exist locally are deferred (see `sync_apply_deferred`) and retried automatically on subsequent pull cycles.

### mem_save envelope fields (conflict surfacing)

When `mem_save` detects candidates, the JSON response includes:

| Field | Type | Description |
|-------|------|-------------|
| `judgment_required` | bool | `true` when candidates were found; `false` otherwise |
| `judgment_status` | string | `"pending"` (only present when `judgment_required: true`) |
| `judgment_id` | string | Convenience: the first candidate's `judgment_id` (use `candidates[].judgment_id` for multi-candidate loops) |
| `candidates` | array | Each entry has `id`, `sync_id`, `title`, `type`, `score`, `judgment_id`, and optionally `topic_key` |
| `id` | int | Internal ID of the just-saved observation |
| `sync_id` | string | Stable sync ID of the just-saved observation |

Old clients that read only the `result` string continue to work — these fields are additive.

### mem_save prompt capture

`mem_save` accepts `capture_prompt` as an optional boolean and defaults it to
`false`. Passing `capture_prompt=true` only offers same-process prompt context
to Core's Diagnostic capture gate. A write requires explicit local consent for
the project and `prompt` content type; an optional session grant must have an
expiry. Retention defaults to 7 days and cannot exceed 30 days.

If no current prompt is available, consent is disabled, or capture fails,
`mem_save` still succeeds and no prompt is invented from observation content.
`mem_save_prompt` is subject to the same gate and never affects opaque root-turn
identity. Diagnostic Content is local-only and excluded from Memory, FTS,
Recall, context, sync/cloud, ordinary export/import, Obsidian, and retired candidate and
Promotion. Existing `user_prompts` rows form a frozen Legacy archive accessible
only through explicit inventory, access, export, and separately confirmed purge.

---

## Admin Observability (conflict layer)

Phase 3 adds an admin-facing observability layer over the conflict/relation system. This is NOT for end users — end users continue to interact with conflicts via the normal agent conversation flow (Phase 1). The tools below are for operators and maintainers who need to inspect or audit the `memory_relations` and `sync_apply_deferred` tables directly.

### engram conflicts CLI

The `engram conflicts <sub-command>` command provides read and scan access to the conflict layer from the terminal. It is intended for maintainers, not for agents or end users.

| Sub-command | What it does |
|-------------|-------------|
| `engram conflicts list` | List `memory_relations` rows with optional `--project`, `--status`, `--since`, `--limit` filters |
| `engram conflicts show <id>` | Show full detail for one relation row (source/target observation snippets) |
| `engram conflicts stats` | Aggregate counts grouped by relation type and judgment status; includes deferred and dead queue sizes |
| `engram conflicts scan` | Walk observations for a project, find conflict candidates, and (with `--apply`) insert new pending relation rows up to a `--max-insert` cap |
| `engram conflicts deferred` | Inspect and replay rows in `sync_apply_deferred`, or recover exactly one `dead` relation locally with `--recover <sync_id>`; targeted recovery is idempotent and never republishes an outbound mutation |

When `--project` is omitted, the command falls back to the cwd-detected project (same resolution as all other `engram` commands).

`engram conflicts scan` also supports `--semantic` for LLM-judge semantic detection beyond FTS5 lexical candidates. This catches vocabulary-different concepts that share no keywords (e.g., "Hexagonal Architecture" vs "Ports and Adapters"). Set `ENGRAM_AGENT_CLI=claude` or `ENGRAM_AGENT_CLI=opencode` before running. Additional flags: `--concurrency N` (default 5), `--timeout-per-call N` seconds (default 60), `--max-semantic N` (default 100), `--yes` (skip confirmation).

> **Subscription note**: `--semantic` uses your existing agent CLI quota (Claude Pro/Max, OpenCode subscription). Engram itself adds no extra cost — you pay only what your LLM provider charges for the prompts.

For the full HTTP API reference and CLI flag details, see [DOCS.md](../DOCS.md).

### HTTP endpoints

All six `/conflicts/*` endpoints are served by `engram serve` on the local runtime (`127.0.0.1:7437`). They are not exposed on the cloud runtime. Full request/response documentation is in [DOCS.md](../DOCS.md).

| Route | Purpose |
|-------|---------|
| `GET /conflicts` | Paginated list of relation rows |
| `GET /conflicts/{relation_id}` | Single relation detail |
| `GET /conflicts/stats` | Aggregate counts |
| `POST /conflicts/scan` | Run scan (dry-run or apply) |
| `GET /conflicts/deferred` | List deferred queue |
| `POST /conflicts/deferred/replay` | Trigger ReplayDeferred cycle |

---

## MCP Tool Reference — mem_compare

`mem_compare` is available in the `curation` profile
(`engram mcp --tools=curation`). It is not exposed in the default `agent` or
`admin` profiles.

### Purpose

Records a verdict on a semantic comparison between two memories. The agent reads both memories, judges their relationship using its LLM reasoning, and calls `mem_compare` to persist the verdict. Unlike `mem_judge` (which resolves a pre-existing `pending` candidate surfaced by `mem_save`), `mem_compare` creates a new relation row directly — useful for proactive analysis that goes beyond FTS5 lexical matching.

### Parameters

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `memory_id_a` | yes | int | Observation ID of the first memory |
| `memory_id_b` | yes | int | Observation ID of the second memory |
| `relation` | yes | string | One of: `conflicts_with` | `supersedes` | `scoped` | `related` | `compatible` | `not_conflict` |
| `confidence` | yes | float | 0.0..1.0 |
| `reasoning` | yes | string | Explanation of the verdict (max 200 chars) |
| `model` | no | string | Model name for provenance (e.g. `"claude-haiku-4-5"`) |

### Behavior

On success, `mem_compare`:
- Persists a relation row with system provenance (`marked_by_kind="system"`, `marked_by_actor="engram"`)
- Is idempotent: the same `(source_id, target_id)` pair updates the existing row rather than inserting a duplicate
- Returns `{"sync_id": "<rel-hex>"}` on a persisted verdict

`not_conflict` verdicts are no-ops — the call succeeds and returns `{"sync_id": ""}` but no row is written, matching the scan flow contract.

Cross-project relations (where `memory_id_a` and `memory_id_b` belong to different projects) are rejected with an error.

### When to call mem_compare

`mem_compare` is intended for agent-initiated semantic audit workflows, not for routine memory saves. Typical usage:

```
# Agent reads two memories, judges their relation, calls mem_compare
mem_compare(memory_id_a=18, memory_id_b=42, relation="supersedes",
            confidence=0.85, reasoning="New arch decision replaces the older one",
            model="claude-haiku-4-5")
```

For the conflict surfacing flow triggered by `mem_save` (where candidates are surfaced automatically), use `mem_judge` instead.

---

## Privacy

Wrap sensitive content in `<private>` tags — it gets stripped at TWO levels:

```
Set up API with <private>sk-abc123</private> key
→ Set up API with [REDACTED] key
```

1. **Plugin layer** — stripped before data leaves the process
2. **Store layer** — `stripPrivateTags()` in Go before any DB write
