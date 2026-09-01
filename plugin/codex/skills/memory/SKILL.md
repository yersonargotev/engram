---
name: engram-memory
description: "Checkpoint each settled root user turn as saved, skipped(no_durable_knowledge), or needs_review. Use when the Engram checkpoint cue or checkpoint identity appears, or when finalizing durable knowledge."
---

# Engram Memory checkpoint protocol

<!-- engram:checkpoint-cue:start -->
For every root user turn, use the engram-memory skill to make exactly one Terminal Memory commit after all causal work settles: `saved`, `skipped(no_durable_knowledge)`, or `needs_review`. Current user intent, maintained source, and runtime evidence override Memory. Reuse the supplied Codex checkpoint identity across continuations; subagents do not create checkpoints.
<!-- engram:checkpoint-cue:end -->

The cue above is the canonical model-visible activation text. Host adapters may
extract and deliver it, but must not maintain their own Memory rubric.

## Terminal Memory commit

The default write path for normal agent work is one terminal Memory commit:
choose the root turn's disposition after its causal work settles, then commit
the checkpoint and any new durable Memories atomically. Do not create a
separate Memory first when the terminal commit can carry the same result.

Independent save is reserved for explicit curation or a long-running,
material loss-risk handoff that must preserve knowledge before the root turn
can settle. It does not replace the later terminal checkpoint for that turn.
An optional Session summary is also a curation operation, not a root-turn
completion requirement.

## Workflow

1. Keep the supplied `(host, session_id, root_turn_id)` as the identity of the
   original root user turn.
2. Recall prior Memory only when it can change the work.
3. Draft any prospective Memories, then run one read-only Terminal Memory
   preflight for them before choosing the disposition.
4. Account for every exact duplicate and every returned semantic candidate.
5. After all causal work settles, apply the disposition rubric once.
6. Finalize through `mem_checkpoint` or the equivalent CLI command.

The protocol is complete when the exact identity returns `created` or
`already_recorded` for one terminal disposition. An adapter or persistence
failure leaves the checkpoint incomplete and must remain visible.

## Root user turn boundary

A root user turn starts with one actual user message and ends only after all
agent, tool, subagent, compaction, and automatic-continuation work caused by
that message has settled.

- Finalize one checkpoint for the root turn, not one per response or task.
- Tool calls, subagents, compaction events, and verifier continuations never
  create independent checkpoints.
- Only the root agent finalizes the checkpoint.
- Treat `host`, `session_id`, and `root_turn_id` from developer context as
  opaque values. Never derive replacements from prompt text, process IDs, tool
  IDs, or subagent IDs.

## Recall when it can change the work

Current user intent, maintained source, and runtime evidence override Memory.
Memory is advisory; surface unresolved conflicts, and treat empty Recall as a
successful result rather than inventing context.

Recall only when prior decisions, tracked work, release state, configuration,
preferences, or known failures can materially change the task, or when the user
explicitly asks to remember prior work. A routine self-contained turn needs no
search. Automatic Recall requires strong or explicit project identity from
`mem_current_project`; a weak identity returns no candidates and one actionable
warning.

Start with one project-scoped `mem_search` using one to three narrow anchors.
The initial request returns at most five candidate summaries and 4 KiB. If
relevant Memory is reasonably expected, reformulate that same lookup intent at
most once. A deliberate follow-up may set `limit` from 6 through 10, but the
4 KiB budget and original scope still apply. Use `mem_get_observation` only for
a selected candidate whose complete content matters. Pass the `recall_id` and
that candidate's opaque `result_id`. One response contains at most 16 KiB of
valid UTF-8 content and reports original bytes, delivered bytes, the limit, and
truncation. When truncated, issue a new request with exactly the returned
`continuation_position`; never infer a position, page silently, or widen the
original project and Recall scope.

Personal or cross-project Recall requires explicit task relevance or user
direction. Account for each candidate before acting. Core excludes deleted,
inactive, and superseded Memories, orders relevance/currentness before pins and
recency, and returns unresolved conflicts explicitly. If Recall is unavailable,
continue the task after its single warning and structured diagnostics.
`mem_context` remains optional curation for explicit chronological review and is
not part of the default five-tool path.

## Preflight prospective Memories

Before finalizing a turn that may save durable knowledge, call `mem_checkpoint`
with `operation: "preflight"`, the exact project, and the prospective inline
`memories`. Preflight is bounded and non-persisting: it creates no Memory,
proposal, checkpoint, relation, sync mutation, review state, or retired
candidate-evaluation state. It returns exact duplicate references and at most three full,
same-project semantic candidates across the prospective set.

Reuse every exact duplicate instead of creating it again. Compare every
semantic candidate with the prospective Memory. A clear, low-risk duplicate,
relation, or distinct outcome may settle directly. Choose `needs_review` when
the relationship remains ambiguous or a material architecture, policy, or
decision conflict requires human judgment. Preflight is evidence for the
rubric; it does not create the terminal checkpoint.

```json
{
  "operation": "preflight",
  "project": "<exact project>",
  "memories": [{"title": "<title>", "content": "<durable result>"}]
}
```

## Choose a disposition

Assess the completed root turn once, after the causal work has settled.

### `saved`

Choose `saved` when the turn produced durable knowledge worth recalling in a
future session, such as:

- an architecture or implementation decision and its reason;
- a bug's non-obvious root cause and verified fix;
- a reusable codebase invariant, convention, workflow, or gotcha;
- a durable configuration constraint or user preference;
- a significant external artifact whose identity or result matters later.

Keep each Memory concise, safe to persist, and independently useful. Use an
existing Memory reference when it already contains the durable result. When
creating new Memories for the checkpoint, use the inline `memories` input so
the core creates the Memories and checkpoint atomically. Do not save secrets,
raw transcripts, or routine activity logs.

### `skipped(no_durable_knowledge)`

Choose `skipped` only after applying the saved and review rubrics and finding no
durable knowledge. Examples include a simple explanation, status read, trivial
formatting, or routine implementation whose result is already fully represented
by maintained source and documentation.

The only supported skip reason is `no_durable_knowledge`. A missing tool,
invalid identity, timeout, persistence failure, or other integration problem is
not a skip disposition and must not be disguised as one.

### `needs_review`

Choose `needs_review` when the turn surfaced potentially durable knowledge but
it is too ambiguous, incomplete, conflicting, or sensitive to admit directly as
a Memory. Retain one bounded, redacted Memory proposal that states the candidate
knowledge and why review is needed. Preserve every independently settled
same-project Memory in the same checkpoint through `memory_ids` or inline
`memories`. A `needs_review` result with at least one settled Memory is a Mixed
Memory checkpoint.

Provide exactly one inline `proposal` so the core creates the settled Memories,
proposal, ordered references, and checkpoint atomically. `needs_review` is not
a fallback for infrastructure failure and does not mean "decide later" when the
saved or skipped rubric already gives a clear answer. A proposal remains local
audit evidence and is never automatically promoted to Memory.

## Finalize idempotently

Use `mem_checkpoint` with the exact identity supplied for the original root
user turn:

```json
{
  "host": "codex",
  "session_id": "<opaque session id>",
  "root_turn_id": "<opaque original turn id>",
  "disposition": "skipped",
  "reason": "no_durable_knowledge"
}
```

For `saved`, pass one or more `memory_ids` or inline `memories` and the exact
project. For `needs_review`, pass zero or more settled `memory_ids` or inline
`memories`, exactly one inline `proposal` containing `title` and `content`, and
the exact project. Do not attach Memory or proposal references to a skipped
checkpoint.

If MCP is unavailable, use the equivalent CLI adapter:

```bash
engram checkpoint record \
  --host codex \
  --session-id '<opaque session id>' \
  --root-turn-id '<opaque original turn id>' \
  --disposition skipped \
  --reason no_durable_knowledge \
  --json
```

An `already_recorded` result is success: the original root turn was already
finalized with the same terminal disposition. Never create a replacement
identity on replay. A conflict means a different terminal result already exists;
surface it instead of overwriting or recording a second checkpoint.

When a verifier continuation asks for a missing checkpoint, use the original
identity carried in that continuation, finalize once, and then finish the same
root user turn. Do not treat the continuation prompt as a new user turn.

## After compaction

The activation cue is delivered again after compaction. Recover only the context
needed to continue, keep the original checkpoint identity supplied for the root
turn, and finalize that same turn after its remaining work settles.
