# Memory checkpoint integration surfaces

**Date:** 2026-08-26

**Status:** Research complete; recommendation accepted in ADR-0006

**Question:** Which combination of MCP, CLI, skills, persistent rules, hooks, and plugins can reliably produce one auditable memory disposition without repeating the full protocol across every surface?

## Executive conclusion

Do **not** choose `skill + CLI + one Stop hook` as a universal architecture. That was a useful hypothesis, but the host contracts do not support it uniformly:

- skills are progressively disclosed or activated on demand in all five hosts studied, so a skill alone is not a reliable always-on activation mechanism;
- persistent instruction files are always-on context in the hosts that support them, but they are behavioral guidance, not deterministic enforcement;
- the relevant turn boundary has a different name and different control semantics per host (`Stop`, `AfterAgent`, `agent_settled`, or an OpenCode event), and OpenCode does not currently expose a documented equivalent with the same continuation guarantee;
- MCP is the best agent-native transport for structured reads and writes, but tool availability and server instructions cannot guarantee that the model calls a checkpoint tool;
- a plugin is useful packaging and host adaptation, but must not become a second owner of memory semantics.

The recommended architecture is therefore **one core checkpoint contract with capability-based host adapters**:

1. Engram's Go core owns a single idempotent checkpoint operation and local audit record.
2. Expose that same operation through both an MCP tool (preferred model-facing path) and a CLI command (hook and fallback path). They are two adapters, not two protocols.
3. Install a **short activation cue** through the least invasive reliably
   model-visible host surface. For Codex and Claude Code, use plugin-bundled
   `SessionStart` context; then prefer a dedicated additive instruction surface;
   edit shared instruction files only as a last resort. The cue says only that
   every root user turn must end in `saved`, `skipped(reason)`, or
   `needs_review`; the detailed rubric stays in one skill.
4. Where a host has a reliable post-turn continuation event, install one thin verifier for that boundary: Codex `Stop`, Claude Code `Stop`, Gemini CLI `AfterAgent`, and Pi `agent_settled`. The verifier checks for a checkpoint keyed to the completed root turn; it does not decide memory quality or duplicate the protocol.
5. Treat OpenCode separately. Its plugin can observe runtime/session events and inject system context, but current public documentation does not establish a blocking, retry-capable final-response hook equivalent to the other hosts. Use best-effort verification there until the host exposes a stable completion gate; do not claim a cross-host guarantee.
6. Treat plugins as delivery bundles for the adapters a host needs. The core service and schema remain the semantic authority.

This design intentionally retains MCP, host instruction surfaces, skills, CLI,
and selected hooks, but gives each exactly one job and generates the repeated
short text from one canonical source. The problem is not the number of installed
artifacts by itself; it is duplicated semantic ownership and repeated prose.

## Facts from the current Engram repository

The following are documented or directly observable in the repository at commit [`091a7f9`](https://github.com/yersonargotev/engram/tree/091a7f90cbbbd6adecd5e4ca64ac892f152aac95):

- The architecture already states that plugins are thin adapters and core memory semantics belong in Go ([integration guide](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/docs/codebase/integrations.md)).
- MCP is an interface over the store. Its server instructions already repeat proactive-save and session-summary policy, while individual tool descriptions repeat portions again ([`internal/mcp/mcp.go`](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/internal/mcp/mcp.go)). Improving those descriptions can improve discovery and tool use, but there is no call-completion enforcement in the MCP server.
- OpenCode injects a full `MEMORY_INSTRUCTIONS` block into every model request and adds periodic reminders, while also exposing event listeners and passive capture ([`plugin/opencode/engram.ts`](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/plugin/opencode/engram.ts)).
- Pi likewise embeds the full protocol and injects it in `before_agent_start`; it also registers native tools and lifecycle handling ([`plugin/pi/index.ts`](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/plugin/pi/index.ts)).
- Codex and Claude Code packages contain several lifecycle hooks, scripts, MCP configuration, and a skill. The current Codex hook manifest has `SessionStart`, `UserPromptSubmit`, `SubagentStop`, and `SessionEnd`, but no `Stop` verifier ([Codex hooks](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/plugin/codex/hooks/hooks.json)); Claude Code does have a `Stop` hook, but its current script performs asynchronous passive/session capture rather than a checkpoint gate ([Claude hooks](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/plugin/claude-code/hooks/hooks.json)).
- `engram setup` already writes both MCP configuration and instruction surfaces for several declarative hosts, and uses bespoke installers where the host requires it ([`internal/setup/agents.go`](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/internal/setup/agents.go), [`registry.go`](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/internal/setup/registry.go)).
- `sessions` has one mutable `summary` field, while observations are searchable through FTS and participate in memory behavior ([store schema](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/internal/store/store.go#L700-L775)).
- Admission shadow already demonstrates an appropriate local-only boundary: dedicated run/proposal/review tables, no sync triggers, no raw evidence, and exclusion from export ([`internal/store/admission_shadow.go`](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/internal/store/admission_shadow.go#L101-L170)).

## Host capability evidence

### Codex

- Codex builds an `AGENTS.md` instruction chain once per run, with scoped precedence ([official AGENTS.md guide](https://developers.openai.com/codex/guides/agents-md)). This makes concise rules a stable session-level cue, but changes are not automatically a per-turn event.
- Codex plugins can bundle hooks, and `SessionStart` stdout or
  `hookSpecificOutput.additionalContext` becomes model-visible developer context,
  including after compaction. Plugin manifests do not define a static behavioral
  `rules/` component; Codex `.rules` governs command execution policy instead
  ([official plugin packaging](https://developers.openai.com/plugins/build/plugins),
  [official hooks reference](https://learn.chatgpt.com/docs/hooks),
  [official rules reference](https://learn.chatgpt.com/docs/agent-configuration/rules)).
- Codex skills use progressive disclosure: initially only name and description are present; the full skill is loaded after explicit or implicit activation ([official skills guide](https://developers.openai.com/codex/skills)). A skill whose body says “always active” is still not itself always loaded.
- `UserPromptSubmit` can add model-visible developer context through `hookSpecificOutput.additionalContext`; `systemMessage` is surfaced as a UI/event warning, not model context ([official hooks reference](https://developers.openai.com/codex/hooks)).
- `Stop` can return `decision: "block"` with a reason; Codex continues by creating a continuation prompt, and `stop_hook_active` exists to prevent loops ([official hooks reference, Stop](https://developers.openai.com/codex/hooks#stop)).

**Inference:** a Codex plugin should deliver the short cue through
`SessionStart` and use `Stop` only as a verifier. The verifier can close an
omitted checkpoint only if it is synchronous, uses the documented continuation
response, carries the original root-turn identity across that continuation, and
exits once the disposition exists. Neither hook should embed the admission
rubric, and setup should not edit shared `AGENTS.md` when the plugin path works.

### Claude Code

- `CLAUDE.md` is loaded in every session and is explicitly described as context, not enforced configuration. Anthropic recommends moving multi-step or narrowly relevant procedures into a skill and keeping always-on instructions concise ([official memory guide](https://code.claude.com/docs/en/memory)). Claude Code reads `CLAUDE.md`, not `AGENTS.md`, unless `CLAUDE.md` imports it.
- A plugin-root `CLAUDE.md` is not loaded, and `rules/` is not a plugin
  component. Plugins can instead bundle a `SessionStart` hook whose stdout or
  `additionalContext` is inserted before the first prompt
  ([official plugin reference](https://code.claude.com/docs/en/plugins-reference),
  [official hooks reference](https://code.claude.com/docs/en/hooks#sessionstart)).
- Skills can load automatically from matching requests or be invoked explicitly ([official skills guide](https://code.claude.com/docs/en/skills)). This is useful for the detailed rubric, not sufficient as the only checkpoint trigger.
- `Stop` runs after the main agent finishes responding, can continue it with feedback, exposes `stop_hook_active`, and has an eight-consecutive-continuation cap. `SessionEnd` cannot block and its JSON output is discarded ([official hooks reference](https://code.claude.com/docs/en/hooks#stop), [SessionEnd](https://code.claude.com/docs/en/hooks#sessionend)).
- Claude Code now defers MCP tools through tool search by default on supported models. Server instructions tell Claude when to discover the tools and are limited to 2 KB ([official MCP guide](https://code.claude.com/docs/en/mcp#tool-search)).

**Inference:** improve Engram's MCP server instructions, but use a short
plugin-bundled `SessionStart` cue plus a `Stop` verifier for the guarantee.
Shared `CLAUDE.md` is a last-resort fallback, not part of the normal plugin
install. `SessionEnd` is suitable for cleanup only.

### Gemini CLI

- `GEMINI.md` provides hierarchical instructional context and can import a canonical protocol file. The configured context filename can also include `AGENTS.md` ([official GEMINI.md guide](https://geminicli.com/docs/cli/gemini-md/)).
- Agent Skills are activated on demand and require activation consent; they cannot be assumed to run at every turn ([official skills management](https://geminicli.com/docs/cli/using-agent-skills/)).
- `AfterAgent` fires once per turn after the final response and can reject it to cause a retry. `SessionEnd` is for cleanup/persistence at shutdown; `BeforeAgent` is the per-prompt context injection point ([official hooks reference](https://geminicli.com/docs/hooks/reference/)). Google explicitly recommends `AfterAgent`, rather than per-model-output hooks, when only final completion needs checking ([hook best practices](https://geminicli.com/docs/hooks/best-practices/)).

**Inference:** Gemini's equivalent is not named `Stop`; one `AfterAgent` verifier is the correct boundary. A universal hook manifest would be a false abstraction.

### Pi

- Pi skills are progressively disclosed: only names/descriptions begin in the system prompt and the model loads a matching `SKILL.md` on demand ([official repository skills documentation](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/skills.md)).
- Pi extensions distinguish `agent_end` from `agent_settled`: after `agent_end`, automatic retry, compaction, or queued follow-up may still occur; `agent_settled` fires when no automatic continuation remains. `session_shutdown` is teardown/cleanup, not a per-turn completion gate ([official extension documentation](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/extensions.md)).
- Pi deliberately does not provide built-in MCP; Engram currently uses a Pi-native provider plus an MCP adapter package, so the transport shape is already host-specific ([Engram Pi adapter](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/plugin/pi/index.ts)).

**Inference:** verify at `agent_settled`, not `agent_end` or `session_shutdown`. The verifier should call the same core checkpoint operation through the native/CLI adapter available to Pi.

### OpenCode

- OpenCode skills are loaded on demand; only descriptions are advertised until the skill tool loads the body ([official skills guide](https://opencode.ai/docs/skills)).
- OpenCode accepts explicit always-on instruction files through configuration, and current V2 instruction discovery uses `AGENTS.md` plus dynamic skill/reference/MCP/session sources ([official configuration](https://dev.opencode.ai/docs/config), [V2 instructions](https://opencode.ai/v2/docs/instructions)).
- The V2 plugin API is explicitly beta. It can mutate system/messages/tools immediately before dispatch and subscribe to the server's public event stream, but the published runtime-hook table does not document a blocking final-response verifier equivalent to Claude/Codex `Stop` or Gemini `AfterAgent` ([official V2 plugin API](https://opencode.ai/v2/docs/build/plugins/)).
- The Engram integration currently uses the older plugin API and injects the complete protocol on every model request ([Engram OpenCode adapter](https://github.com/yersonargotev/engram/blob/091a7f90cbbbd6adecd5e4ca64ac892f152aac95/plugin/opencode/engram.ts)).

**Inference and uncertainty:** OpenCode can observe idle/completion-like events in source releases, but the current public plugin contract does not document a stable, blocking continuation gate. A reliable cross-mode guarantee, especially in non-interactive `run`, is unsupported by the sources reviewed. Engram should use a best-effort local audit/reminder on OpenCode and label that capability honestly until a stable gate exists.

## Comparison of integration choices

Scores are architectural judgments derived from the documented facts above: `high`, `medium`, or `low`.

| Choice | Activates reliably | Structured write | Can verify omission | Cross-host portability | Duplication/drift | Recommendation |
|---|---:|---:|---:|---:|---:|---|
| Improve MCP tools only | Low-medium | High | Low | High where MCP exists | Low | Keep and improve, but insufficient alone |
| Reduce/improve hooks only | Medium-high on supported hosts | Medium via CLI/API | High where a continuation hook exists | Low | Medium | Use thin, capability-specific verifiers only |
| Skill + CLI + injected cue | Medium | High | Low | Medium-high | Low if generated | Good baseline/fallback, not a guarantee |
| Skill + CLI + one universal Stop hook | High only on some hosts | High | High only on some hosts | Low | Medium | Reject as universal design |
| Plugin owns full behavior | Medium-high | High | Host-dependent | Low | High | Reject semantic ownership; keep packaging only |
| Skill + CLI, no hooks/cue | Low | High | None | High | Low | Opt-in/manual mode only |
| Core checkpoint + MCP/CLI adapters + short cue + per-host verifier | High where host supports a gate; best-effort otherwise | High | High where supported | Medium-high | Lowest semantic duplication | **Recommended** |

### 1. Conserve and improve MCP

MCP should remain because it is the structured, discoverable interface the model can call without constructing shell commands. Improvements should include:

- add one `mem_checkpoint` tool whose response contract is exactly the same as the CLI command;
- put the short activation cue and the three outcomes in MCP server instructions, ahead of secondary tool descriptions;
- reduce repeated prose in `mem_save`, `mem_session_summary`, skills, and plugins by generating descriptions from one canonical contract;
- return stable machine-readable reason codes and an idempotency result (`created`, `already_recorded`, or `updated`);
- expose a read-only `mem_checkpoint_status` only if status cannot be returned by the write tool or queried by the hook through CLI.

MCP cannot be the enforcement layer: the server is passive until the model invokes a tool, and some hosts defer MCP tools.

### 2. Reduce and improve hooks

Hooks should be reduced by responsibility, not necessarily by raw count. Session start/context recovery, compaction recovery, prompt capture, subagent capture, and checkpoint verification are different lifecycle responsibilities. Combining them into one script would reduce file count while increasing coupling.

For the checkpoint feature specifically, install at most **one verifier per host**, at the most settled root-turn event the host exposes. Its algorithm is deterministic:

1. derive `(host, session_id, root_turn_id)` from hook input or adapter-owned causal state;
2. query the core for that key;
3. exit successfully if a terminal disposition exists;
4. if missing and the host supports continuation, request exactly one continuation instructing the agent to record `saved`, `skipped(reason)`, or `needs_review`;
5. recognize `stop_hook_active`/retry state or the existing checkpoint to prevent loops;
6. if the host cannot continue reliably, record/report `missing` as telemetry without pretending it was assessed.

The hook must not infer what deserves memory from transcripts. That would create a second admission engine in shell or TypeScript.

### 3. Skill + CLI + activation cue

This is better than skill + CLI alone. The always-on cue improves discovery;
the skill carries the full rubric; the CLI gives deterministic storage. It is
also a strong fallback on hosts without MCP.

However, a cue is still model guidance whether delivered by a hook or an
instruction file, and skill activation remains model-mediated. Without a
verifier, omissions remain possible. Therefore this combination is the
**portable baseline**, not the strongest guarantee.

The injected block should be short, for example:

> At the end of each root user turn, record exactly one Engram memory checkpoint: `saved`, `skipped(reason)`, or `needs_review`. Use the Engram memory skill for the rubric and `mem_checkpoint` or `engram checkpoint record` to persist it. Subagents do not create independent checkpoints.

Do not inject the entire save/search/session/compaction protocol into
`AGENTS.md`, `CLAUDE.md`, and `GEMINI.md`. Generate one canonical cue and keep
host-specific text limited to the adapter mechanics. For Codex and Claude Code,
the normal plugin install carries that cue in `SessionStart` context rather than
editing the shared files.

### 4. Skill + CLI + one Stop hook

This remains a sound **Codex- or Claude-specific deployment profile**, not the system architecture. It maps to Gemini as `AfterAgent`, Pi as `agent_settled`, and has no currently documented OpenCode equivalent with the same guarantee. Naming the architecture after one host event hides the actual capability requirement: “post-root-turn event that can request one continuation.”

### 5. Plugin as packaging versus authority

A plugin may package:

- the skill;
- MCP registration or a native tool adapter;
- the short host-native activation cue;
- the host's thin lifecycle verifier;
- setup/uninstall metadata.

It must not own:

- admissibility or skip-reason policy;
- checkpoint state transitions;
- idempotency;
- privacy/redaction rules;
- sync/export eligibility.

Those belong to the Go core. This matches Engram's existing thin-plugin guardrail and makes “plugin” a distribution answer rather than another semantic layer.

### 6. Skill + CLI without hooks

Support this as an explicit low-intrusion/manual profile. It is useful for hosts without stable hooks or for users who reject lifecycle interception. Document its guarantee as “agent-guided, auditable when called,” not “every turn assessed.” Adding the short persistent cue materially improves activation without adding a plugin where the host can read the cue and execute the CLI directly.

## Q5: checkpoint boundary

### Recommendation

Use **one checkpoint per root user turn, finalized when that root agent run becomes settled**.

“Root user turn” means the causal unit beginning with an actual user message and ending after all model/tool/subagent/automatic-continuation work caused by that message settles. It does **not** mean every low-level LLM response, tool call, subagent completion, hook continuation, or compaction retry.

Why:

- **Task completion is not a portable event.** A single user turn can contain several tasks, and agents/hosts do not share a formal task object.
- **Session end is too late and unreliable as the primary boundary.** It cannot block in Claude Code, is cleanup-oriented in Gemini and Pi, and may never run after a crash or abandoned terminal.
- **Every internal turn is too noisy.** Tool loops, subagents, retries, and compaction can multiply records without adding user-level meaning.
- **The root turn is observable and testable.** Codex/Claude expose turn-final hooks, Gemini says `AfterAgent` fires once per turn, and Pi exposes `agent_settled` after automatic continuations finish.
- **A terminal `skipped(reason)` record separates “assessed and not durable” from “Engram failed to activate.”** This directly addresses the current diagnosis gap without polluting Memories.

### Idempotency requirements

- Primary identity: `(host, session_id, root_turn_id)`; if a host lacks a stable root turn ID, the adapter must create one when the user prompt arrives and carry it through continuations.
- Exactly one current disposition per key, written with an upsert or compare-and-set transition.
- A hook-generated continuation retains the same root-turn key.
- Subagent IDs are evidence/provenance only and never checkpoint keys.
- `saved` may reference multiple Memory IDs if one root turn produced multiple durable facts.
- Replays/resumes return `already_recorded`; they do not create another row.
- A state may move from `needs_review` to `saved` or `skipped` through an explicit review action, preserving history or an audit timestamp. It must not silently oscillate.

### Expected noise

This creates one small local audit row per user turn, including conversational turns with nothing durable. That is intentional operational telemetry, but it needs retention controls. It does **not** create one searchable Memory per turn.

## Q6: persistence model

### Recommendation

Create a dedicated, local-only checkpoint boundary, preferably:

- `memory_checkpoints`: identity, project, host, opaque session/root-turn identifiers, disposition, reason code, timestamps, protocol version, and optional assessment actor/version;
- `memory_checkpoint_refs`: checkpoint ID plus typed references such as `observation`, `proposal`, or later `review`.

No raw user prompt, transcript, generated rationale, Memory content, or repository file content should be copied into the checkpoint tables. A `skipped` row stores an enumerated reason code, not free-form content. Free-form diagnostic detail, if ever necessary, should be opt-in, bounded, redacted, and separately retained.

### Why not the alternatives

| Storage choice | Problem |
|---|---|
| Memory/observation | Makes `skipped` operational telemetry searchable as knowledge, affects FTS/context, risks sync/export, and confuses “assessment happened” with “durable knowledge exists” |
| Session metadata/summary | One mutable field cannot represent multiple root turns or references, loses per-turn auditability, and overloads session lifecycle state |
| Admission proposal | Correct only for `needs_review`; it cannot naturally represent successful saves or deliberate skips, and the current shadow proposal system is an experiment rather than a general checkpoint ledger |
| Dedicated checkpoint record | Represents the invariant directly, can remain out of FTS/sync/export, supports idempotency and retention, and can reference Memories/proposals without copying them |

### Privacy, sync, and search constraints

- Follow the existing admission-shadow pattern: no sync triggers and no inclusion in export or cloud payloads by default.
- Exclude checkpoint rows and reason text from FTS, `mem_search`, task briefing, context injection, and Memory counts.
- Keep identifiers opaque; do not store the prompt as an idempotency key. If derivation is necessary, use an adapter-provided stable ID or a one-way local hash with a versioned scheme.
- Apply normal private-tag redaction before any optional diagnostic string reaches storage.
- Add an explicit retention policy because one row per root turn grows independently of durable Memories. A reasonable initial policy is bounded local retention by age or maximum rows, but the exact default needs product evidence rather than being fixed by this research.
- Make audit/read surfaces opt-in or administrative; the normal Memory UI should not show skipped checkpoints as knowledge.

### Atomicity

Where possible, write the checkpoint and its target in one core transaction:

- `saved`: save/upsert the observation(s), then create checkpoint references before commit;
- `needs_review`: create the proposal and checkpoint reference before commit;
- `skipped`: write only the checkpoint row.

If an existing `mem_save` happens earlier in the turn, `mem_checkpoint(saved, refs=[...])` may attach to it later. The core must validate that referenced objects exist and belong to the same project/scope allowed by policy.

## Recommended responsibility map

| Concern | Owner |
|---|---|
| Disposition state machine and reason-code validation | Go core/store service |
| Idempotency and atomic references | Go core/store service |
| MCP `mem_checkpoint` | Thin MCP adapter |
| `engram checkpoint record/status` | Thin CLI adapter |
| Detailed save/skip/review rubric | One canonical skill/source |
| Always-on activation sentence | Generated host-native cue; plugin lifecycle context where supported |
| Turn identity and lifecycle signal | Thin host adapter |
| Missing-checkpoint continuation | One verifier on supported hosts |
| Plugin/extension | Packaging of the above host assets |
| Search, sync, export exclusion | Store/sync core policy |

## Proposed delivery slices

This is a recommendation, not authorization to implement:

1. **Core contract and local ledger:** define reason codes, state transitions, schema, retention boundary, atomicity, CLI, and tests.
2. **MCP parity:** expose the same service as `mem_checkpoint`; improve server instructions; remove duplicated policy prose from individual adapters.
3. **Canonical protocol generation:** produce the short cue and detailed skill from one source; test installed files for semantic equality and idempotent setup.
4. **Codex proof:** implement and evaluate the verifier against Codex's documented continuation event using real-session telemetry.
5. **Capability adapters:** Gemini `AfterAgent`, Pi `agent_settled`, then an explicitly best-effort OpenCode profile.
6. **Consolidation:** remove obsolete reminders/protocol copies only after the checkpoint path proves equal or better activation.

Issue #34 should remain a real-session admission-policy evaluation. The activation/checkpoint contract and integration consolidation are separate work because they change product guarantees and host adapters rather than merely validating shadow-policy output.

## Deferred evidence topics

Q10-Q15 were omitted by the maintainer rather than frozen as implementation
decisions. Skip-reason expansion, retention defaults, OpenCode reclassification,
failure UX, reference cardinality refinements, and later host rollout details are
outside the current decision tree. A delivery issue may reopen one only when its
slice reaches that boundary and brings current primary-source or real-session
evidence; none blocks the accepted Codex-first architecture.

## Source quality and limitations

- Host behavior claims use current official documentation or official upstream repositories accessed on 2026-08-26.
- Repository claims are pinned to Engram commit `091a7f90cbbbd6adecd5e4ca64ac892f152aac95`.
- OpenCode's V2 plugin API is documented as beta; its capability conclusion is deliberately conservative.
- Pi's upstream repository has changed organization/package naming over time. The current sources are under `earendil-works/pi`; implementation should still pin and test the exact Pi package version Engram installs.
- No source reviewed establishes that prompts alone can guarantee model compliance. Recommendations that combine guidance with verification are architectural inferences, explicitly labeled above.
