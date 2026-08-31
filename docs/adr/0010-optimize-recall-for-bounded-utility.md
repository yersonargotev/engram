---
status: accepted
---

# Optimize Recall for bounded utility

Engram will optimize useful Recall subject to explicit noise, latency, volume,
privacy, and checkpoint constraints, with a bias toward precision when coverage
and noise conflict. Raw prompts and subagent results remain available only through
explicit opt-in Content capture; they are not default Memory inputs. Codex is the
first evaluation host, while Core contracts remain host-neutral.

Default Recall behavior may change only after a prospective controlled comparison,
not from observational counts alone. Recall failures fail open with a visible
diagnostic. A missing checkpoint may cause one identity-preserving continuation;
if persistence still fails, the host surfaces the incomplete result and stops
retrying rather than blocking or looping indefinitely.

This revises [ADR-0006](0006-own-memory-checkpoints-in-core.md) only where its
Checkpoint guarantee implied an absolute completion barrier. Core ownership,
opaque checkpoint identity, local-only checkpoint state, canonical semantics, and
thin adapters remain accepted.

The default Recall strategy is agent-initiated: no historical content is injected
merely because a session or turn starts. When earlier project knowledge can change
the work, Recall first returns a bounded candidate set and retrieves complete
content only by deliberate follow-up. Recall authority keeps Memories advisory;
current user intent and maintained or runtime evidence prevail, and contradictions
are surfaced rather than silently resolved.

Core owns a versioned Protocol contract of machine-verifiable invariants, while
the canonical skill owns the editorial rubric. Parity tests keep MCP instructions,
tool descriptions, and activation cues aligned without generating prose from a
machine schema.

Capture consent is separate by project and content type, with an optional expiring
session grant. `SubagentStop` never creates durable knowledge automatically; under
consent it may retain diagnostic content, while the root agent must explicitly
propose any durable learning. Session summaries remain explicit optional workflows
outside the default MCP profile. Recall feedback is optional, local, content-free,
and unknown when absent; research export is a separate, explicit, aggregate-only
operation with no content or raw identifiers.

Settled durable knowledge normally enters Engram through one Terminal Memory
commit. Independent `mem_save` remains available only for explicit curation or a
long-running handoff with material loss risk; agent policy no longer requires a
save after every decision. The default MCP agent profile always exposes only
`mem_current_project`, `mem_search`, `mem_get_observation`, `mem_checkpoint`, and
`mem_checkpoint_status`; other Memory operations use deferred or specialized
profiles. Duplicate/conflict preflight and mixed terminal outcomes are specified
separately in
[ADR-0011](0011-preserve-mixed-terminal-memory-outcomes.md).

The initial Recall budget is five candidates and 4 KiB for the first response,
ten candidates only after an explicit follow-up, and 16 KiB for one complete
Memory retrieval. Truncation is visible and further content requires an explicit
positioned request. Startup and compaction inject only the canonical cue and
no historical content by default. `UserPromptSubmit` remains the sole source of
opaque root-turn identity; compaction preserves that identity and never derives a
replacement. Bounded exact-session compact recovery remains a canary, while broad
project history is not a candidate default.

Diagnostic capture uses isolated local-only state excluded from Memory, FTS,
Recall, context, sync, cloud, and ordinary export. Its default retention is seven
days and may be configured up to thirty days, with immediate deletion by project
and content type. Existing captured data is preserved during migration rather
than automatically deleted or reclassified.

Recall strategies are compared prospectively through paired randomized task-class
trials using the same model, repository, and task protocol. One concise warning is
shown only for actionable recall failure or an incomplete checkpoint; structured
details live in status and diagnostics, and empty recall is not an error. Releases
follow expand-contract ordering: a backward-compatible binary precedes compatible
plugin and Pack projections, installed replacement is verified before retirement,
and rollback pins one verified Compatibility tuple.

Recall is history-dependent only when prior decisions, tracked work, releases,
configuration, preferences, or known failures can change the current result. The
agent performs one narrow search and may reformulate it once when relevant Memory
is reasonably expected. Automatic Recall requires strong or explicit project
identity; personal and cross-project scope require explicit task relevance or user
direction.

Eligible candidates are active and in scope. Semantic relevance ranks first, pins
may raise a candidate only within the same relevance and currentness tier, and
recency breaks remaining ties. Deleted and superseded Memories are excluded by
default; relevant unresolved conflicts remain visible as warnings. Every supported
host exposes the same five semantic tools, while lifecycle transport and guarantee
strength remain capability-specific.

General availability requires checkpoint non-inferiority within two percentage
points, Stop conflict or loop growth below one point, at least 30% fewer injected
bytes, at least 25% lower startup/compaction p95 latency, Recall p95 below 250 ms,
at least 10% relative Recall utility improvement, noise below 20%, harm no higher
than 2%, false-empty no higher than 5%, and at least 80% explicit label coverage.
The utility improvement's confidence interval may not have a negative lower bound;
noise must improve over baseline, and harm may not worsen.

Capture consent is controlled locally through status, enable, disable, and
separately confirmed purge operations; setup reports but never enables it, and no
dashboard surface enters the canary. Existing `user_prompts` becomes a Legacy
prompt archive: new writes, FTS, Recall, and sync mutations stop by default, while
explicit legacy access, export, and purge preserve user control without automatic
migration, deletion, or remote tombstones.

The Protocol contract uses a monotonic integer version. Identity, disposition,
persistence, minimum tool surface, Recall or Capture defaults, and compatibility
requirements increment it; editorial-only changes do not. Telemetry and Diagnostic
capture schemas version independently. Pack, plugin, and binary manifests declare
supported protocol ranges, and readiness requires verified provenance plus a
non-empty range intersection.

The detailed behavioral contract and delivery ownership remain in
[specification #98](https://github.com/yersonargotev/engram/issues/98) and its
[delivery graph](../research/codex-mcp-central-thin-hooks-architecture.md#recommended-delivery-flow).
This ADR does not authorize runtime implementation by itself.
