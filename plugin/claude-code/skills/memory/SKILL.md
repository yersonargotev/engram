---
name: engram-memory
description: "Commit one terminal Memory disposition for each settled root user turn: saved, needs_review, or skipped(no_durable_knowledge)."
---

# Engram Terminal Memory protocol

For every settled root user turn, make exactly one Terminal Memory commit:
`saved`, `needs_review`, or `skipped(no_durable_knowledge)`. A root turn ends
only after all agent, tool, subagent, compaction, and continuation work caused
by the user's message has settled. Subagents and lifecycle events do not create
independent commits.

## Default tools

The default agent profile contains exactly these tools:

- `mem_current_project` establishes project scope and write authority.
- `mem_search` recalls prior Memory when it can change the current work.
- `mem_get_observation` retrieves complete content for a selected result.
- `mem_checkpoint` commits the terminal disposition and durable result.
- `mem_checkpoint_status` inspects the exact root-turn checkpoint.

Use deferred curation, lifecycle, or admin profiles only for an explicit
specialized workflow. `mem_save` is an independent curation operation, not the
default commit. `mem_session_summary` is optional curation for an explicit
handoff with material loss risk; it is not an agent lifecycle requirement.

## Authority and recall

Current user intent, maintained source, and runtime evidence override Memory.
Recall only when prior decisions, tracked work, release/configuration,
preferences, or known failures can materially change the task; self-contained
work needs no search. Automatic Recall requires strong or explicit project
identity. Start with one narrow project search of at most five candidates and
4 KiB, then reformulate the same intent at most once. Limits 6 through 10 are a
deliberate follow-up; personal or cross-project scope requires explicit task
relevance or user direction. Empty Recall is successful, conflicts stay
explicit, and unavailable Recall fails open with one warning plus diagnostics
without blocking work.
For a checkpoint-capable root turn, pass its exact `host`, `session_id`, and
`root_turn_id` together on every search so later explicit feedback can prove
same-turn exposure. Omit all three if exact root identity is unavailable.
Retrieve complete content only for a selected candidate by passing the
`recall_id` and its opaque `result_id` to `mem_get_observation`. Each response
contains at most 16 KiB of valid UTF-8 content and reports its byte limit and
truncation. A truncated segment requires a new request with exactly the returned
`continuation_position`; do not page silently or widen the original Recall scope.

Treat the supplied `host`, `session_id`, and `root_turn_id` as opaque; reuse
them unchanged across compaction or verifier continuations and never invent
replacements.

Attach optional `recall_feedback` at the checkpoint only for an actually known
assessment of one exact Recall run bound to this exact root turn and its
exposed opaque results. A different or unbound turn is ineligible. Utility is
`decisive`, `orienting`, `duplicate`, or `unused`; quality is `current`,
`stale`, `contradictory`, or `unknown`. Sources are `agent_explicit` for the
agent's stated assessment, `user_explicit` for a direct user assessment, and
`evaluator` for a separately invoked evaluator. An explicitly reviewed empty
run may carry `false_empty`. Omitted feedback remains unknown; Recall delivery,
citation, ordering, and checkpoint outcome imply no label. Feedback failure is
reported separately and never changes terminal checkpoint completion.

Before finalizing prospective Memories, call `mem_checkpoint` with
`operation: "preflight"`, the exact project, and those inline `memories`. This
bounded read creates nothing and returns exact duplicates plus at most three
full same-project semantic candidates. Reuse exact duplicates and account for
every candidate. Clear low-risk duplicate, relation, or distinct outcomes may
settle; ambiguity or a material architecture, policy, or decision conflict
selects `needs_review`.

## Choose a disposition

- Choose `saved` for durable knowledge worth recalling later, such as a
  reasoned decision, non-obvious root cause and verified fix, reusable
  invariant, configuration constraint, preference, or significant artifact.
  Prefer inline `memories` so creation and the checkpoint are atomic.
- Choose `needs_review` for potentially durable knowledge that is ambiguous,
  incomplete, conflicting, or sensitive. Preserve any independently settled
  Memories and attach exactly one bounded, redacted inline `proposal`. A result
  with at least one Memory is Mixed; this disposition is not a fallback for
  tool failure and never automatically promotes its proposal.
- Choose `skipped` only when there is no durable knowledge. Its only reason is
  `no_durable_knowledge`. Missing tools, invalid identity, timeouts, and
  persistence failures are not skip dispositions.

Do not save secrets, raw transcripts, routine activity logs, or capture output
merely because a lifecycle event occurred.

## Finalize idempotently

After all causal work settles, call `mem_checkpoint` once with the exact root
identity. A `created` or matching `already_recorded` result completes the
protocol. Surface a conflict or integration failure; never overwrite it or
record a second identity.

```json
{
  "host": "claude-code",
  "session_id": "<opaque session id>",
  "root_turn_id": "<opaque original turn id>",
  "disposition": "skipped",
  "reason": "no_durable_knowledge"
}
```

For `saved`, include the exact project plus one or more `memory_ids` or inline
`memories`. For `needs_review`, include the exact project, zero or more settled
`memory_ids` or inline `memories`, and one inline `proposal` with `title` and
`content`.

After compaction, recover only what is needed to continue and finalize the same
root turn after its remaining work settles. Compaction does not require a
Session summary or create a new checkpoint identity.
