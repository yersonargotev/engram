---
name: engram-memory
description: "Checkpoint each settled root user turn as saved, skipped(no_durable_knowledge), or needs_review. Use when the Engram checkpoint cue or checkpoint identity appears, or when finalizing durable knowledge."
---

# Engram Memory checkpoint protocol

<!-- engram:checkpoint-cue:start -->
For every root user turn, use the engram-memory skill to finalize exactly one Engram checkpoint: `saved`, `skipped(no_durable_knowledge)`, or `needs_review`. Reuse the supplied Codex checkpoint identity across continuations; subagents do not create checkpoints.
<!-- engram:checkpoint-cue:end -->

The cue above is the canonical model-visible activation text. Host adapters may
extract and deliver it, but must not maintain their own Memory rubric.

## Workflow

1. Keep the supplied `(host, session_id, root_turn_id)` as the identity of the
   original root user turn.
2. Recall prior Memory only when it can change the work.
3. After all causal work settles, apply the disposition rubric once.
4. Finalize through `mem_checkpoint` or the equivalent CLI command.

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

Before similar or history-dependent work, use `mem_context` and then a targeted
`mem_search` if needed. Recall is selective: a routine self-contained turn does
not require a search merely to satisfy the checkpoint protocol.

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
creating new Memories for the checkpoint, prefer the inline `memories` input so
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
knowledge and why review is needed.

Use an existing proposal reference when appropriate. Otherwise prefer the
inline `proposal` input so the core creates the proposal and checkpoint
atomically. `needs_review` is not a fallback for infrastructure failure and does
not mean "decide later" when the saved or skipped rubric already gives a clear
answer.

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
project. For `needs_review`, pass exactly one inline `proposal` containing
`title` and `content`, plus the exact project. Do not attach Memory or proposal
references to a skipped checkpoint.

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
