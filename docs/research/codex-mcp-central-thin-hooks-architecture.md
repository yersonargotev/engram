# Codex architecture proposal: useful recall through MCP and thin hooks

**Date:** 2026-08-30

**Status:** architecture, specification, and ticket graph approved; documentation baseline [#112](https://github.com/yersonargotev/engram/issues/112) precedes the [delivery tickets](#recommended-delivery-flow); no runtime implementation has been authorized or delivered

**Repository snapshot:** `pack-v3.1.2` / `79a90fb`

## Executive summary

The proposal is **directionally sound but not yet true of the current source**. The recommended target combines a versioned, Core-owned contract for machine-verifiable checkpoint and recall invariants with one canonical editorial skill; parity tests keep MCP initialization instructions, tool descriptions, and activation cues aligned, while hooks remain limited to host lifecycle transport. Codex's documented surfaces support that split: MCP server `instructions` provide server-wide cross-tool guidance; skills carry progressively disclosed workflows; hooks can inject developer context or synchronously continue a turn, but hooks are not a complete enforcement boundary ([MCP](https://developers.openai.com/codex/mcp/), [skills](https://developers.openai.com/codex/skills/), [hooks](https://developers.openai.com/codex/hooks/)).

The product objective is not to preserve the current architecture. It is to maximize **useful recall per unit of context and latency** while retaining one reliable checkpoint per eligible root turn. Earlier accepted decisions are evidence and reversible baselines, not constraints on this proposal. The initial target therefore injects no historical Memory or prompt content automatically on `startup`, `resume`, or `clear`; it teaches the model to run bounded, task-directed MCP recall only when prior project knowledge can change the work. Exact-session recovery after compaction remains a canary, not a presumed requirement.

The current repository already establishes several strong foundations:

- the plugin skill is explicitly canonical, and its cue is extracted rather than duplicated ([skill](../../plugin/codex/skills/memory/SKILL.md#L8-L13), [snapshot helper](https://github.com/yersonargotev/engram/blob/79a90fbc3a68b053f8260e1d16d84e15606fc5ef/plugin/codex/scripts/_checkpoint.sh#L1-L31));
- `(host, session_id, root_turn_id)` is an opaque, unique, local checkpoint key ([store](../../internal/store/checkpoint.go#L32-L42), [schema](../../internal/store/checkpoint.go#L96-L112));
- `Stop` is already a four-line adapter into the core verifier ([stop.sh](../../plugin/codex/scripts/stop.sh#L1-L4)); and
- `SessionEnd` only closes the session today; it does not create a Memory or summary ([session-end.sh](../../plugin/codex/scripts/session-end.sh#L1-L40)).

However, four exact policy conflicts should be fixed before calling the architecture unified:

1. MCP initialization still says `mem_session_summary` is mandatory before saying “done” and says to save after **any** decision, fix, discovery, or convention ([mcp.go](../../internal/mcp/mcp.go#L186-L215)). The canonical skill instead requires one terminal root-turn checkpoint, selective recall, and a value/safety rubric ([skill](../../plugin/codex/skills/memory/SKILL.md#L15-L90)). These are competing completion and persistence policies.
2. The repository-scoped `engram-memory-protocol` skill independently orders immediate `mem_save` calls and a mandatory `mem_session_summary` at session close ([project skill](../../skills/memory-protocol/SKILL.md)). Because `AGENTS.md` activates that skill for decisions and session closure, this is an active agent-policy conflict, not stale documentation.
3. `UserPromptSubmit` currently persists every non-empty full prompt best-effort by default ([snapshot user-prompt-submit.sh](https://github.com/yersonargotev/engram/blob/79a90fbc3a68b053f8260e1d16d84e15606fc5ef/plugin/codex/scripts/user-prompt-submit.sh#L23-L40)). Prompt content is searchable/sync-capable product data, not local-only checkpoint metadata ([schema](../../internal/store/store.go#L833-L859), [FTS](../../internal/store/store.go#L1151-L1179), [sync mutation](../../internal/store/store.go#L2857-L2907)). Full-prompt capture therefore must become explicit opt-in, independently of opaque identity delivery.
4. `SubagentStop` currently posts the complete last subagent message to passive extraction by default ([snapshot subagent-stop.sh](https://github.com/yersonargotev/engram/blob/79a90fbc3a68b053f8260e1d16d84e15606fc5ef/plugin/codex/scripts/subagent-stop.sh#L18-L39)). That contradicts the canonical rule that only the root agent finalizes and increases duplication, privacy, and latency risk. Disable it by default; if retained, accept only an explicit bounded learning envelope, never infer from an entire subagent answer.

The highest-priority implementation sequence is: (P0) align every agent-facing policy with the checkpoint skill; (P0) make full-prompt and subagent-output capture opt-in; (P1) make startup/resume/clear cue-only and evaluate exact-session compaction recovery separately; (P1) add local-only usefulness and noise telemetry; (P2) simplify the 20-tool agent profile and version reporting. Preserve `Stop` as the sole synchronous “exactly one checkpoint” verifier unless measured evidence shows a better Codex boundary.

## Scope and method

This review evaluates each element of the proposed Codex architecture against:

1. current official OpenAI documentation, opened on 2026-08-30, restricted to `developers.openai.com` redirects into `learn.chatgpt.com`;
2. the current repository source, tests, docs, and focused git history;
3. installed/cache metadata only as point-in-time operational evidence, never as source authority.

No held-out session, rollout transcript, prompt body, Memory body, or other user content was inspected. Runtime checks were limited to version/status/configuration metadata (`engram --version`, `engram setup status codex --json`, `codex plugin list`, and `codex mcp list`). Earlier aggregate research is used only for its already-published, content-free conclusion that lifecycle coverage improved while MCP causality remained unisolated ([activation study](codex-engram-activation-mcp-vs-cli.md#conclusion)).

### Optimization objective and decision posture

Rank decisions in this order:

1. **Useful context:** recover durable knowledge that changes a decision, prevents an error, or materially reduces discovery work.
2. **Low noise:** minimize duplicate, unused, stale, and raw chronological content delivered to the model.
3. **Reliable completion:** preserve exactly one terminal checkpoint for every eligible settled root turn.
4. **Privacy and locality:** collect no raw prompt, subagent output, transcript, or feedback content without explicit consent.
5. **Operational cost:** keep model-visible bytes and p50/p95 lifecycle latency bounded.

### Approved root decisions

The first `grill-with-docs` round was accepted on 2026-08-30:

1. Optimize Recall utility within explicit noise, latency, volume, privacy, and
   checkpoint constraints, with a bias toward precision when recall coverage and
   noise conflict.
2. Keep full-prompt and subagent-result Content capture behind explicit opt-in;
   default persistence is limited to selected Memories, checkpoint state, and
   content-free operational evidence.
3. Validate the change in Codex first while keeping Core contracts host-neutral.
4. Require a prospective controlled comparison before changing recall defaults;
   observational cohorts may establish a baseline but not causal attribution.
5. Fail open and visibly for recall failures. For a missing checkpoint, request at
   most one identity-preserving continuation, then surface an incomplete result
   rather than blocking or looping indefinitely.

These decisions are recorded in
[ADR-0010](../adr/0010-optimize-recall-for-bounded-utility.md). They approve a
target and evaluation posture, not implementation.

The second `grill-with-docs` round was accepted on 2026-08-30:

6. Make recall agent-initiated for history-dependent work; self-contained turns do
   not recall merely to satisfy protocol.
7. Return a bounded candidate set first and retrieve complete Memory content only
   through deliberate follow-up.
8. Treat Memories as advisory. Current user intent and maintained or runtime
   evidence prevail, and contradictions must be surfaced.
9. Give Core ownership of versioned machine-verifiable invariants, keep the
   canonical skill editorial, and enforce parity across MCP and hook projections.
10. Scope Content capture consent separately by project and content type, with an
    optional expiring session grant.
11. Prevent `SubagentStop` from producing durable knowledge automatically. Under
    consent it may retain diagnostic content; the root agent explicitly proposes
    any durable learning.
12. Keep `mem_session_summary` as an explicit optional workflow outside the
    default MCP profile.
13. Record Recall feedback only as optional, local, content-free attribution;
    absent feedback remains unknown and never blocks the checkpoint.
14. Keep telemetry local by default and allow only a separate, explicit,
    aggregate-only research export with no content or raw identifiers.

The third `grill-with-docs` round was accepted on 2026-08-30:

15. Create or reference settled Memories atomically at the terminal checkpoint by
    default. Reserve independent `mem_save` for explicit curation or material
    loss-risk handoffs, not automatic immediate saving.
16. Keep only `mem_current_project`, `mem_search`, `mem_get_observation`,
    `mem_checkpoint`, and `mem_checkpoint_status` always visible; defer or
    specialize every other MCP operation.
17. Start with five candidates and 4 KiB, allow ten candidates only after explicit
    follow-up, limit one full Memory response to 16 KiB, and make truncation and
    continuation explicit.
18. Re-deliver only the canonical cue after compaction, preserve the opaque identity
    supplied by `UserPromptSubmit` without deriving a replacement, evaluate bounded
    exact-session recovery as a canary, and never default to project history.
19. Put consented prompt and subagent diagnostics in isolated local-only storage,
    excluded from Memory, FTS, recall, context, sync, cloud, and ordinary export,
    with seven-day default retention configurable up to thirty days. Preserve
    existing data during migration.
20. Compare the current broad injection, cue-only targeted recall, and cue-only no
    recall through prospective paired randomized task-class trials. Determine the
    sample size from baseline power analysis.
21. Show one concise warning only for actionable recall failure or incomplete
    checkpoint, keep structured detail in status and diagnostics, and treat empty
    recall as a normal result.
22. Release through expand-contract Compatibility tuples: backward-compatible
    binary first, then compatible plugin and Pack projections, verified retirement,
    and tuple-pinned rollback.

The fourth `grill-with-docs` round was accepted on 2026-08-30:

23. Recall only for explicit historical signals, allow at most one reformulated
    search when relevant knowledge is expected, and do not recall for self-contained
    work.
24. Require strong or explicit project identity for automatic recall and writes;
    make personal and cross-project recall explicitly relevant or user-directed.
25. Filter by active state and scope, rank semantic relevance before pin and
    recency, exclude superseded Memories, and keep relevant conflicts visible.
26. Run a bounded non-persisting Terminal Memory preflight, reuse exact duplicates,
    expose at most three semantic candidates, and route material ambiguity to
    `needs_review` before atomic finalization.
27. Allow a `needs_review` checkpoint to attach zero or more settled Memories and
    exactly one proposal, preserving mixed outcomes without proposal Promotion.
28. Expose the same five semantic MCP tools across hosts; vary only lifecycle
    transport and Checkpoint guarantee strength by host capability.
29. Gate general availability on the preregistered checkpoint, Stop, byte, latency,
    utility, noise, harm, false-empty, and label-coverage thresholds recorded in
    ADR-0010.
30. Own Capture consent through local status, enable, disable, and separately
    confirmed purge operations; setup reports but never enables, and the canary has
    no dashboard workflow.
31. Freeze existing `user_prompts` as an explicitly accessible Legacy prompt
    archive with no new writes, FTS, recall, or sync mutations by default and no
    automatic migration, deletion, or remote tombstones.
32. Use a monotonic Protocol contract integer with independent telemetry/capture
    schema versions and manifest-declared compatibility ranges.

[ADR-0006](../adr/0006-own-memory-checkpoints-in-core.md) remains strong evidence for core ownership, opaque identity, local-only checkpoints, and a capability-specific verifier. This proposal explicitly reopens its delivery assumptions about how much `SessionStart` context is useful. [ADR-0009](../adr/0009-retire-task-briefing.md) remains evidence against rebuilding a separate automatic Task Brief policy: deliberate `search` plus chronological `context` was simpler and no worse on the available evaluation. Its decision had medium confidence and names the evidence needed to revisit it, so the staged recall comparison below may also refute that baseline.

Classification means:

- **Confirmed:** supported by the documented Codex contract and already consistent with current Engram source, or clearly suitable as a target.
- **Partially confirmed:** supported in principle, but current source or host limitations require qualification.
- **Refuted:** contradicted by current source or by an official host contract.

## Current-state evidence

### Official Codex/OpenAI contracts

- Plugins may bundle skills, MCP servers, and hooks; hooks require explicit trust review ([plugins](https://developers.openai.com/codex/plugins/)).
- Codex initially sees skill name/description, then loads full `SKILL.md` only after selection; implicit selection depends on the description ([skills](https://developers.openai.com/codex/skills/)). Therefore a skill alone is not a deterministic always-on checkpoint trigger.
- Codex reads MCP `instructions` at initialization and uses them as server-wide guidance alongside tools. OpenAI recommends cross-tool workflows/constraints there and asks that the first 512 characters be self-contained ([MCP](https://developers.openai.com/codex/mcp/)). This makes MCP the correct always-visible guidance seam, not the sole semantic owner.
- `SessionStart` supports `startup`, `resume`, `clear`, and `compact`; its `additionalContext` becomes developer context, including immediately after mid-turn compaction ([hooks](https://developers.openai.com/codex/hooks/#sessionstart)).
- `PostCompact` is a distinct event and ignores plain stdout; the existing plugin instead uses `SessionStart(source=compact)`, which is documented and sufficient. Adding a second PostCompact hook would duplicate recovery ([hooks](https://developers.openai.com/codex/hooks/#postcompact)).
- `UserPromptSubmit` receives full `prompt` and turn-scoped `turn_id`, can add developer context, and can block submission ([hooks](https://developers.openai.com/codex/hooks/#userpromptsubmit)). Engram deliberately does not block there.
- `SubagentStop` receives `last_assistant_message` and can continue a subagent. This is a lifecycle capability, not a recommendation to persist the message ([hooks](https://developers.openai.com/codex/hooks/#subagentstop)).
- `Stop` can continue the turn with a generated continuation prompt; `stop_hook_active` exists to prevent loops ([hooks](https://developers.openai.com/codex/hooks/#stop)).
- `SessionEnd` is main-thread-only, synchronous, advisory, and may happen after 30 minutes of inactivity. Its output cannot steer Codex or keep the thread open ([hooks](https://developers.openai.com/codex/hooks/#sessionend)). It is unsuitable for creating a model-reviewed Memory or guaranteeing a summary.
- Hooks are useful guardrails, not a complete enforcement boundary; specialized tool paths may opt out ([hooks](https://developers.openai.com/codex/hooks/)).

### Repository contracts and implementation

The plugin currently declares six effective lifecycle paths: `SessionStart` for normal starts, `SessionStart(source=compact)` routed to a separate script, `UserPromptSubmit`, `SubagentStop`, `Stop`, and `SessionEnd` ([hooks.json](../../plugin/codex/hooks/hooks.json#L1-L76)).

The canonical plugin skill defines one checkpoint per settled root user turn, opaque identity reuse, root-only finalization, selective recall, three dispositions, and idempotent replay ([SKILL.md](../../plugin/codex/skills/memory/SKILL.md#L15-L45), [SKILL.md](../../plugin/codex/skills/memory/SKILL.md#L92-L137)). `internal/setup` verifies that this skill contains the cue and full rubric and verifies that every start source has exactly one matching adapter ([codex_activation.go](../../internal/setup/codex_activation.go#L28-L90), [codex_activation.go](../../internal/setup/codex_activation.go#L115-L177)). This establishes intended authority, not merely comments.

Current lifecycle behavior is heavier than the target:

- `SessionStart` starts an HTTP server, creates a session, may import a repository manifest in the background, fetches full project context, and emits cue plus context ([snapshot session-start.sh](https://github.com/yersonargotev/engram/blob/79a90fbc3a68b053f8260e1d16d84e15606fc5ef/plugin/codex/scripts/session-start.sh#L20-L136)).
- compact recovery re-creates the session and fetches context again ([snapshot post-compaction.sh](https://github.com/yersonargotev/engram/blob/79a90fbc3a68b053f8260e1d16d84e15606fc5ef/plugin/codex/scripts/post-compaction.sh#L14-L39)).
- prompt submission saves the full prompt and separately emits opaque identity ([snapshot user-prompt-submit.sh](https://github.com/yersonargotev/engram/blob/79a90fbc3a68b053f8260e1d16d84e15606fc5ef/plugin/codex/scripts/user-prompt-submit.sh#L23-L55)).
- subagent stop sends the whole last assistant message to passive capture ([snapshot subagent-stop.sh](https://github.com/yersonargotev/engram/blob/79a90fbc3a68b053f8260e1d16d84e15606fc5ef/plugin/codex/scripts/subagent-stop.sh#L18-L39)).
- Stop delegates to the binary verifier ([stop.sh](../../plugin/codex/scripts/stop.sh#L1-L4)).
- SessionEnd only marks the existing session ended ([session-end.sh](../../plugin/codex/scripts/session-end.sh#L26-L40)).

The MCP `agent` profile exposes 20 tools, including session summary, prompt save, passive capture, checkpoint, and curation operations ([mcp.go](../../internal/mcp/mcp.go#L91-L145)). Server guidance independently mandates a session summary and proactive saves ([mcp.go](../../internal/mcp/mcp.go#L186-L215)). The repository `engram-memory-protocol` skill repeats the older immediate-save/session-summary policy. Together these are the main semantic drift.

The core checkpoint ledger is correctly local operational state, excluded from Memory search/context/export/sync, and uses a uniqueness constraint over the opaque identity ([checkpoint.go](../../internal/store/checkpoint.go#L32-L63), [checkpoint.go](../../internal/store/checkpoint.go#L84-L134)). That layer should remain authoritative for idempotency and disposition validation.

Focused history supports the intended evolution: `3b8fb20` introduced the canonical skill/cue; `5fd2f17` added Stop enforcement; `d65480c` published the activation analysis; `4321b3f` narrowed `needs_review`; and `3421d4b` most recently changed SessionStart daemon behavior. These commits show incremental convergence, but they do not resolve MCP/prompt/subagent policy drift.

### Installed/cache snapshot (non-authoritative)

At observation time:

- source/Managed Pack: `pack.json` is `3.1.2` ([pack.json](../../pack.json#L1-L16));
- installed binary: `engram 3.0.0` at `/opt/homebrew/bin/engram`;
- installed Codex plugin: `engram@engram` version `0.1.5`, cached at `~/.codex/plugins/cache/engram/engram/0.1.5`, attributable to requested ref `v3.0.0` and revision `f28d9bf…`;
- the source plugin manifest is also `0.1.5` ([plugin.json](../../plugin/codex/.codex-plugin/plugin.json#L1-L16));
- `codex mcp list` reports the Engram stdio MCP enabled with `mcp --tools=agent`.

This proves the three version axes can diverge. It does **not** prove the installed cache represents current source at `79a90fb`, and it must not override repository evidence.

## Claim-by-claim assessment

| Proposal claim | Assessment | Evidence and qualification |
| --- | --- | --- |
| MCP central + minimal hooks | **Partially confirmed** | MCP is the documented server-wide guidance/tool seam, while hooks are lifecycle adapters. But semantic authority should remain in shared core/contract, not MCP prose alone; current MCP prose conflicts with the skill. |
| Unify canonical skill, MCP instructions, and tool descriptions | **Confirmed as a target; refuted as current state** | Skill authority is already asserted and tested, but MCP still mandates summaries/proactive saves. Tool/profile comments also derive from older cross-host counts rather than one contract ([mcp.go](../../internal/mcp/mcp.go#L106-L129)). |
| Thin SessionStart/PostCompact to cue, identity, and strictly necessary recovery | **Partially confirmed** | Thin context injection is documented. Current Codex plugin has no PostCompact hook; it correctly uses `SessionStart(source=compact)`. Both scripts currently do session/network/context work. Identity originates at `UserPromptSubmit`, not SessionStart, because only turn-scoped events have `turn_id`. |
| Preserve opaque identity in UserPromptSubmit | **Confirmed** | Official event supplies `turn_id`; source forwards it unchanged with session and host. Core documents/checks it as opaque. |
| Make full prompt capture opt-in | **Confirmed as target; refuted as current state** | Current source captures every non-empty prompt. Since prompts enter FTS and sync-capable storage, default capture exceeds the minimum checkpoint contract. |
| Keep Stop as verifier of exactly one checkpoint per root turn | **Confirmed with caveat** | Current adapter and unique DB key implement this. Codex can continue a turn, but hooks are not an absolute host-wide security boundary. Acceptance must test continuation loops and fail-visible behavior. |
| Disable SubagentStop by default or accept only explicit learnings | **Confirmed as target; refuted as current state** | Current hook captures the whole output. Canonical skill says subagents do not checkpoint. Explicit structured learning is safer than passive extraction if a subagent path remains. |
| SessionEnd only closes; never creates Memories/summaries | **Confirmed and already implemented** | Current script performs only `/sessions/{id}/end`. Official semantics are advisory and potentially delayed, reinforcing this design. |
| Add local usefulness measurement | **Confirmed as a product/research target; unsupported by current schema** | Existing checkpoint and prompt/session tables measure lifecycle completion, not whether retrieved Memories affected work. New local-only events are required. |
| Clarify Managed Pack/skill, binary, and plugin versions | **Confirmed** | Current snapshot already shows `3.1.2`, `3.0.0`, and `0.1.5`. Treating “Engram version” as one number is demonstrably ambiguous. |

## Recommended target architecture

```text
Protocol contract (core-owned, versioned)
  ├─ defines machine-verifiable checkpoint and recall invariants
  ├─ defines compatibility and telemetry vocabularies
  └─ supplies parity fixtures for every model-facing projection

Canonical skill (plugin-owned editorial rubric)
  ├─ owns the human-readable workflow and checkpoint cue
  └─ remains parity-tested against the Protocol contract

MCP guidance and tool descriptions
  └─ project the same vocabulary without owning independent policy

Codex lifecycle adapters (plugin-owned)
  SessionStart(startup/resume/clear) -> register if necessary + cue; no history
  SessionStart(compact)              -> cue; preserve prior identity; exact-session recovery only if proven useful
  UserPromptSubmit                   -> opaque identity; diagnostic prompt capture only if opted in
  Stop                               -> core verify-stop; continue only if absent
  SubagentStop                       -> disabled by default; opted-in diagnostic capture only
  SessionEnd                         -> close existing session only

Core/domain (binary-owned)
  project resolution, checkpoint validation/idempotency, persistence,
  privacy gates, local telemetry, retention/export/sync exclusion

Adapters
  MCP = preferred model-facing read/write transport
  CLI = hook transport, human diagnostics, and fallback
```

### Ownership rules

1. **Core/domain owns semantics.** Dispositions, identity validation, replay, conflict behavior, local-only exclusions, and telemetry vocabularies belong in Go domain/store code.
2. **The skill owns human-readable workflow depth, not independent rules.** Golden parity tests constrain its cue/rubric against the core protocol version; machine schemas do not generate the prose.
3. **MCP owns model-facing discovery.** Initialization instructions should contain only cross-tool policy; each tool description should state when to use the tool and defer disposition validity to core validation.
4. **Hooks own host event translation only.** They may parse documented stable fields, call one bounded core operation, and emit documented JSON. They must not infer durable knowledge from arbitrary transcript/message content.
5. **Setup/status owns provenance and compatibility reporting.** It should report the three versions independently plus contract compatibility, not collapse readiness into version equality.

### Default recall policy

Use the smallest context path that can answer the current task:

1. `SessionStart(startup|resume|clear)` delivers the canonical cue and performs only lifecycle registration required by the checkpoint contract. Completion means zero historical Memory, prompt, or session-summary content was injected.
2. When the task is history-dependent, run one project-scoped `mem_search` with one to three distinctive anchors and a default limit of five. Completion means every returned item was accounted for before acting.
3. Request chronological `mem_context` separately only when recent session continuity can change the work. Model-facing chronological context excludes raw prompts by default and has explicit item and byte budgets.
4. Fetch full content with `mem_get_observation` only for selected results whose truncated form is insufficient.
5. `SessionStart(source=compact)` always re-delivers the cue. It adds bounded exact-session recovery only in the canary variant; broad project `/context` output is never the compaction default.

Every recall response should return an opaque local `recall_id`, result IDs, elapsed time, and delivered byte count outside the prose payload. This makes usefulness measurable without treating retrieval as proof of use.

### Concrete prioritized improvements

**P0 — remove conflicting persistence policy**

- Replace MCP's “mandatory session summary” with “one terminal checkpoint for each settled root user turn; save only when the canonical rubric says durable.”
- Remove `mem_session_summary` from eager/core guidance; keep it deferred only if a separately documented user workflow still needs it.
- Replace the immediate-save/session-summary procedure in `skills/memory-protocol/SKILL.md` with a sharp pointer to the canonical checkpoint skill. Keep `skills/engram-memory-cli/SKILL.md` focused on selective project recall/curation and explicitly route cue-bearing root turns to the checkpoint skill.
- Add golden parity fixtures for MCP instructions, checkpoint tool descriptions, the canonical skill, and its extracted cue against one protocol vocabulary/version.
- Update contributor and user docs in the same change so no maintained page preserves the retired mandatory-summary or broad-startup-recall policy.

**P0 — close privacy-default gaps**

- Split `UserPromptSubmit` into always-on identity delivery and an explicit `capture_full_prompts` option defaulting to `false`.
- Remove `SubagentStop` from the default manifest. If enabled, require a small JSON envelope such as `{kind, title, learning, evidence_ref?}` emitted deliberately by the subagent; reject raw transcript/message fallback.
- Keep all usefulness instrumentation content-free and local-only by construction.

**P1 — thin lifecycle work**

- Keep one documented `SessionStart(source=compact)` path; do not add a duplicative PostCompact hook.
- Move daemon availability/import concerns out of model-context hooks where possible. If session registration is required for prompt/session telemetry, make it one bounded binary command or MCP-tool hook with clear failure telemetry.
- Make `startup`, `resume`, and `clear` cue-only by default. Return bounded task-directed recall through MCP, with byte/item limits and elapsed time. Do not inject full `/context` output.
- For compact canaries, use the existing session-scoped `/context/compaction` boundary rather than project-wide `/context`; compare it against cue-only recovery before retaining it.
- Keep `Stop` synchronous, short, idempotent, and fail-visible. Test `stop_hook_active` handling explicitly.

**P2 — simplify surface and reporting**

- Re-evaluate the 20-tool agent profile using current canonical workflows; defer curation/admin/session-summary/passive-capture tools unless required.
- Extend `engram setup status codex --json` with `pack_version`, `binary_version`, `plugin_version`, `protocol_contract_version`, and compatibility results.

## Proposed instrumentation schema and metric definitions

Use a new local-only table family with no FTS, sync mutation, cloud, export/import, Obsidian, context, or Memory lifecycle exposure. Do not attach raw prompts, queries, Memory content, assistant text, transcript paths, or opaque host identifiers directly. Hash a per-install salted turn key if linkage is needed.

### Event schema

```json
{
  "schema_version": 1,
  "event_id": "local random id",
  "occurred_at": "UTC timestamp",
  "project_key": "local normalized project id or salted hash",
  "turn_key": "per-install salted hash(host, session_id, root_turn_id)",
  "event": "recall_completed | memory_exposed | memory_use_labeled | checkpoint_completed",
  "surface": "mcp | hook | cli",
  "operation": "context | search | get",
  "memory_key": "per-install salted hash(memory sync id)",
  "rank": 1,
  "result_count": 3,
  "returned_bytes": 2048,
  "latency_ms": 18,
  "use": "decisive | orienting | duplicate | unused",
  "quality": "current | stale | contradictory | unknown",
  "label_source": "agent_explicit | user_explicit | evaluator",
  "protocol_version": 1,
  "binary_version": "3.0.0",
  "plugin_version": "0.1.5",
  "pack_version": "3.1.2"
}
```

Prefer normalized tables internally (`recall_runs`, `recall_items`, `memory_use_labels`) with uniqueness on `(turn_key, memory_key, label_source)` and append-only label history. The JSON above is the diagnostic/export shape, not permission to include this local data in ordinary export/sync.

### Definitions

- **Recovered/exposed Memories:** distinct `memory_key` values returned to model context during a turn. A database search hit not delivered to the model does not count.
- **Used Memories:** exposed Memories with an explicit `decisive` or `orienting` label. Never infer use merely because a tool returned the item.
- **Decisive:** changed a material choice, prevented an incorrect action, supplied required authority, or determined the final result.
- **Orienting:** reduced discovery effort or established relevant context but did not determine the result.
- **Duplicate:** added no new value because the same fact was already present in current maintained source/context or another exposed Memory.
- **Unused:** exposed but not relied on. Absence of a label is `unknown`, not automatically unused.
- **Stale:** the Memory's claim was superseded or no longer matched current authoritative evidence.
- **Contradictory:** the Memory materially conflicted with another active Memory or current authority and required resolution. Do not equate every stale item with contradiction.
- **Recall latency:** monotonic elapsed milliseconds from operation start through the bounded response being ready; report p50/p95 by operation/surface.
- **Recall volume:** result count and UTF-8 bytes delivered to model context, plus optional estimated tokens if measured consistently.
- **Utility rate:** `(decisive + orienting) / explicitly_labeled_exposed`; never use all exposed items as denominator when labels are missing.
- **Noise rate:** `(duplicate + unused) / explicitly_use_labeled_exposed`.
- **Harm rate:** `(stale + contradictory) / explicitly_quality_labeled_exposed`.
- **Duplicate rate:** `duplicate / explicitly_use_labeled_exposed`.
- **False-empty rate:** recall runs returning zero items where an evaluator or user later identifies an already-existing relevant Memory; report only on an explicitly reviewed cohort.
- **Time to useful Memory:** elapsed time from the first recall request until the first `decisive` or `orienting` item is delivered; unknown when no explicit label exists.
- **Checkpoint coverage:** closed eligible root turns with exactly one terminal checkpoint / closed eligible root turns. Report missing and conflict counts separately.

Percentages such as “Engram changed decisions in 15%” are self-reported or evaluator-attributed impact, not causal proof. Reports must name the label source, denominator, unknown count, and confidence interval. A causal claim requires a prospective paired or randomized recall strategy comparison.

Labels should be explicit and cheap: add an optional `used_memory_ids`/`memory_feedback` field to the terminal checkpoint operation, or a separate idempotent local feedback tool. Do not make feedback mandatory for checkpoint completion in the first rollout; otherwise instrumentation will degrade the primary invariant.

## Staged rollout, acceptance, and rollback

### Stage 0 — freeze contract and baseline

Create a versioned protocol fixture and collect 1–2 weeks of aggregate, content-free baseline data under current behavior. Do not use held-out sessions. Record checkpoint coverage, Stop continuations, prompt-capture enablement, SubagentStop calls, MCP/CLI operation counts, p50/p95 latency, and bytes delivered.

**Accept:** baseline query is reproducible, emits no content/identifiers, and separates three version axes.

**Rollback:** disable only new telemetry if schema/privacy review fails; no checkpoint behavior changes yet.

### Stage 1 — policy parity and privacy defaults

Align skill/MCP/tool descriptions, default full-prompt capture off, and default SubagentStop off. Add source/golden tests and setup-status diagnostics.

**Accept:** zero contradictory mandatory-summary/proactive-save strings; identity still reaches every eligible root turn; checkpoint integration tests remain green; no full prompt or subagent message is persisted under defaults.

**Rollback:** restore prior surface bundle as one pinned plugin release, but keep explicit warning that it captures prompts/subagent output. Do not partially mix new skill with old MCP instructions.

### Stage 2 — thin startup/compaction canary

Canary cue-only startup and reduced hook work for opt-in local users. Compare broad current startup context, cue-only startup with model-directed recall, and cue plus bounded exact-session compact recovery while holding binary/model/task protocol stable.

**Accept:** checkpoint coverage is non-inferior within 2 percentage points with no new conflicts; p95 SessionStart and compact-recovery latency improves by at least 25%; injected bytes fall by at least 30%; no increase greater than 1 percentage point in Stop continuation loops/failures.

**Rollback:** select the prior plugin release; schema remains backward-compatible and unused telemetry can remain local until normal retention cleanup.

### Stage 3 — usefulness evaluation

Collect explicit labels on representative non-held-out workflows. Compare broad chronological injection, cue-only plus targeted search, and cue-only with no recall on genuinely self-contained tasks. Pre-register task classes so a low-noise strategy cannot win merely by receiving easier work.

**Accept:** at least 80% label completion in the evaluation cohort; `decisive + orienting` rate improves over baseline, stale/contradictory rate does not worsen, and p95 recall stays within an agreed local budget (initial proposal: 100 ms for context, 250 ms for search). Report confidence intervals and raw denominators.

**Rollback:** disable automatic recall or label prompting independently; retain manual MCP recall and checkpoint enforcement.

### Stage 4 — general availability and cleanup

Remove old duplicate instruction paths only after installed-state migration proves exact ownership and compatibility. Publish a compatibility matrix for pack, binary, plugin, and protocol versions.

**Accept:** upgrade/downgrade tests, Windows host-wrapper tests, setup status, fresh install, resume, mid-turn compaction, Stop replay, and uninstall preserve user/custom state.

**Rollback:** pin the prior compatible plugin/pack coordinate; never overwrite customized configuration or user-owned files.

## Versioning terminology

Use four explicit terms, three distributable versions plus one contract version:

| Term | Owner/source | Meaning | Current source / observed installed |
| --- | --- | --- | --- |
| **Managed Pack version** | `pack.json` / Packy distribution | Version of the `engram-memory-cli` skill projection and its Pack metadata | source and installed projection `3.1.2` at observation time |
| **Engram binary version** | GoReleaser/linker and `engram --version` | CLI/MCP/core/store implementation | installed `3.0.0`; repository HEAD is not itself a released binary version |
| **Codex plugin version** | `plugin/codex/.codex-plugin/plugin.json` | Bundle of hooks, plugin skill, and MCP registration | source and installed `0.1.5` at observation time |
| **Protocol contract version** | new core-owned constant/schema | Compatibility of checkpoint rubric, hook identity envelope, and telemetry vocabulary | not currently explicit; introduce `1` |

Never say only “Engram 3.1.2 is installed” when referring to the Pack. Recommended status wording is: “Managed Pack 3.1.2; binary 3.0.0; Codex plugin 0.1.5; checkpoint protocol v1; compatibility ready/not ready.” The plugin marketplace requested ref/revision is provenance, not a fourth product version.

## Risks and privacy controls

- **Prompt sensitivity:** full prompts can contain secrets, personal data, or proprietary content. Default off; show the exact retention/sync effect before opt-in.
- **Subagent overcollection:** last messages may reproduce source, credentials, or unrelated context. Default off and prohibit raw-message inference.
- **Context injection leakage:** automatic project context increases exposure to the model and token volume. Bound items/bytes, respect project authority, and expose counts in diagnostics.
- **Telemetry re-identification:** stable raw session/turn/Memory IDs can reconstruct behavior. Use per-install salted keys, short retention, aggregate reporting, and no external sync.
- **False utility attribution:** tool return is not use. Require explicit labels and preserve `unknown`.
- **Enforcement overclaim:** Stop improves protocol compliance but Codex documents hooks as guardrails, not a complete boundary. Report missing hooks/checkpoints honestly.
- **Version skew:** plugin, pack skill, and binary can disagree on schema or vocabulary. Gate readiness on a compatibility matrix and test downgrade.
- **Hook latency/failure:** synchronous hooks block the user-visible lifecycle. Keep only Stop and SessionEnd synchronous requirements; bound all network/process work and emit diagnostic counters.

## Design-tree status

The product and architecture frontier is empty after four `grill-with-docs`
rounds and 32 accepted decisions. No product choice remains silently assumed.
The user confirmed shared understanding on 2026-08-30. Runtime implementation
has not been authorized or delivered; the approved target is now maintained in
[specification #98](https://github.com/yersonargotev/engram/issues/98) and its
delivery tickets.

The following are evidence conditions, not open product decisions:

1. Measure the current baseline and calculate the prospective study sample size.
2. Refresh official host contracts and the installed Compatibility tuple at spec
   and release qualification time.
3. Run the consented canary and require every preregistered gate before general
   availability.
4. Inventory exact legacy prompt state and ownership before any retirement, export,
   or purge operation; preserve unknown or customized state.

## Recommended delivery flow

`ask-matt` classifies research as a standalone input to the main idea-to-ship flow. This proposal is a multi-session change, so continue in this order:

1. Shared understanding is confirmed and `/grill-with-docs` is closed.
2. `/to-spec` is complete in [GitHub issue #98](https://github.com/yersonargotev/engram/issues/98). It defines Recall defaults, privacy defaults, Protocol compatibility, telemetry denominators, migrations, and rollback behavior without depending on this research note for hidden requirements.
3. `/to-tickets` is complete. The graph uses end-to-end slices with native blocking edges across Protocol compatibility, baseline measurement, policy parity, privacy defaults, checkpoint semantics, bounded Recall, lifecycle thinning, feedback, evaluation, and final distribution contract. Documentation baseline [#112](https://github.com/yersonargotev/engram/issues/112) blocks [#99](https://github.com/yersonargotev/engram/issues/99) until the approved domain and architecture evidence lands; closing #112 makes #99 the only unblocked frontier again. #98 remains the unmodified parent specification.

Delivery tickets: [#99](https://github.com/yersonargotev/engram/issues/99), [#100](https://github.com/yersonargotev/engram/issues/100), [#101](https://github.com/yersonargotev/engram/issues/101), [#102](https://github.com/yersonargotev/engram/issues/102), [#103](https://github.com/yersonargotev/engram/issues/103), [#104](https://github.com/yersonargotev/engram/issues/104), [#105](https://github.com/yersonargotev/engram/issues/105), [#106](https://github.com/yersonargotev/engram/issues/106), [#107](https://github.com/yersonargotev/engram/issues/107), [#108](https://github.com/yersonargotev/engram/issues/108), [#109](https://github.com/yersonargotev/engram/issues/109), [#110](https://github.com/yersonargotev/engram/issues/110), and [#111](https://github.com/yersonargotev/engram/issues/111).

Do not implement directly from this research note; use it as evidence and preserve the staged evaluation gates in the approved spec.

## Decision

Adopt the architecture in [ADR-0010](../adr/0010-optimize-recall-for-bounded-utility.md): optimize for **useful Recall density**, enforce **core-owned protocol parity**, default raw capture off, keep five semantic MCP tools visible, use Terminal Memory commits, and release only verified Compatibility tuples. Keep hooks transport-only, startup and compaction cue-only by default, Stop as the sole synchronous verifier, and Recall feedback local and explicitly attributed.

[ADR-0011](../adr/0011-preserve-mixed-terminal-memory-outcomes.md) deliberately extends [ADR-0006](../adr/0006-own-memory-checkpoints-in-core.md) so one `needs_review` checkpoint can preserve settled Memories plus exactly one proposal without Promotion. ADR-0006 otherwise remains authoritative for identity, replay, local-only state, core ownership, and thin adapters. [ADR-0009](../adr/0009-retire-task-briefing.md) remains the current Task Brief decision unless the preregistered Recall evaluation contradicts its relevant evidence.
