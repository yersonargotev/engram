---
name: engram-memory
description: "Checkpoint each settled root user turn as saved, skipped(no_durable_knowledge), or needs_review. Use when the Engram checkpoint cue or checkpoint identity appears, or when finalizing durable knowledge."
---

# Engram Memory checkpoint protocol

<!-- engram:checkpoint-cue:start -->
For every root user turn, use the engram-memory skill to make exactly one Terminal Memory commit after all causal work settles: `saved`, `skipped(no_durable_knowledge)`, or `needs_review`. Current user intent, maintained source, and runtime evidence override Memory. Reuse the supplied host checkpoint identity across continuations; subagents do not create checkpoints.
<!-- engram:checkpoint-cue:end -->

The cue above is the canonical model-visible activation text. Host adapters may
extract and deliver it, but must not maintain their own Memory rubric.

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
3. Draft prospective Memories using the authoring guidance below, then run one
   read-only Terminal Memory preflight before choosing the disposition.
4. Account for every exact duplicate and every returned semantic candidate.
5. After all causal work settles, apply the disposition rubric once.
6. Finalize through `mem_checkpoint`. If MCP is unavailable, use the equivalent
   CLI command `engram checkpoint record`.

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

## Project binding and cross-repository work

Before committing work for another repository in an ongoing conversation,
distinguish these boundaries:

| Fact | Meaning for a Terminal Memory commit |
| --- | --- |
| Detected project | Current working-directory context; strong or explicit detection establishes automatic project scope. |
| Explicit target authority | User intent can authorize work in a target project; it does not reassign an internal session. |
| Memory ownership | Every referenced or inline Memory in one checkpoint must belong to its selected project. |
| Internal session binding | Inline Memories reuse the host `session_id` as an Engram session. An existing nonempty session project must match the selected project. |
| Opaque checkpoint identity | `(host, session_id, root_turn_id)` identifies one terminal result. Changing cwd or `project` does not change that identity. |

This is the current CLI/MCP Core contract. A conversation that already created
inline Memories for project A cannot create inline Memories for B in a later
turn using that same session. Explicit B authority and `--project B` do not
remove this restriction. A references-only checkpoint can attach existing B
Memories without the inline session check, but all references must still belong
to B. This does not authorize creating a separate Memory merely to bypass the
normal Terminal Memory commit.

Preflight assesses prospective Memories within a project; it receives no
host/session/root-turn identity. Success does not establish that the session
can record them. `checkpoint_project_mismatch` can mean either a referenced
Memory belongs to another project or an inline session is bound elsewhere.

For cross-repository work, keep the supplied identity and assess the actual
knowledge and its intended owner. If the normal contract cannot store new
inline knowledge in the destination project, report that limitation. An
original-project handoff is valid only when its content is independently useful
there and write authority exists there; state explicitly that it is stored in A,
not B, and does not fulfill a destination-write request. Independent curation
remains reserved for explicit curation or a material loss-risk handoff under its
existing rules. Multi-project Core design is separate future work.

After a structured record rejection, correct only a legitimate input and retry
with the same identity and disposition. If revised content could be saved, run
preflight for that content and project first. A failed transaction leaves no
terminal checkpoint; if no legitimate correction exists, leave the incomplete
result visible. An integration failure is neither `no_durable_knowledge` nor a
reason to invent an identity or reassign a session. For an ambiguous transport
outcome, inspect the exact checkpoint before retrying; routine successful or
structured-error responses need no status polling. Same-disposition replay
returns the original terminal result, not an appended or moved Memory.

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
For a checkpoint-capable root turn, pass its exact `host`, `session_id`, and
`root_turn_id` together on every search so later explicit Recall feedback can
prove same-turn exposure. Omit all three if exact root identity is unavailable.
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
direction. Account for each candidate before acting. By default, Core excludes deleted,
inactive, and superseded Memories, orders relevance/currentness before pins and
recency, and returns unresolved conflicts explicitly. If Recall is unavailable,
continue the task after its single warning and structured diagnostics.
`mem_context` remains optional curation for explicit chronological review and is
not part of the default five-tool path.

## Inspect history deliberately

For a historical question, set `include_history: true` on `mem_search` (CLI:
`engram search ... --include-history --json`). This admits superseded Memories;
active-review eligibility, deleted exclusion, project/scope authority, ranking,
and the five/ten-candidate, 4 KiB and positioned 16 KiB content bounds still
apply. Core retains `include_history` on the local Recall run and returns it
on search/get; retrieve through that run's opaque selection without adding a
history flag to get. Default searches continue to exclude superseded targets.

Candidates and retrieved content expose `created_at`, `updated_at`,
`review_state`, and `review_after`. Dates, review state, and Memory type do not
prove currentness or applicability. Read the authored evidence and qualifications
and compare them with maintained source and runtime evidence.

Historical results expose up to three `supersessions` with direction
`superseded_by` or `supersedes`, plus `supersessions_omitted` for the remainder.
Only an eligible endpoint inside the authorized scope exposes `memory_id`,
`sync_id`, and a UTF-8-safe title of at most 256 bytes; otherwise
`endpoint_available: false` exposes no endpoint identity or title. To inspect
an available replacement, explicitly search its title or distinctive concept
anchors within the same scope, verify the candidate's `id` matches the hint's `memory_id`, then get
that candidate through its own `recall_id` and opaque `result_id`. An endpoint
hint is not an opaque content selection or permission to widen scope.

Use explicit curation through existing update operations to replace generic
legacy titles with evidence-grounded discovery anchors. Preserve ownership and
applicability unless independently established; a title or date cannot imply a
rename, scope migration, or newly applicable guidance. Optional explicit Session
summaries remain available for their existing curation purpose.

Historical Recall is additive within Protocol v2; older binaries may reject the
new flag. Check actual support rather than inferring a released compatible tuple
from the Protocol number alone. The added run boolean is local operational
state; it does not reinterpret the Memory lifecycle or Memory schema.

## Record only explicit Recall feedback

At the terminal checkpoint, attach optional `recall_feedback` only when an
assessment is actually known for one exact Recall run and only the opaque
results that run exposed after being bound to this exact root turn at search
time. A different or unbound turn is ineligible. Utility is `decisive`,
`orienting`, `duplicate`, or
`unused`; quality is `current`, `stale`, `contradictory`, or `unknown`. Use
`agent_explicit` for your stated assessment, `user_explicit` for a direct user
assessment, and `evaluator` only for a separately invoked evaluator. Explicitly
reviewed zero-result runs may carry `false_empty`.

Omit unknown assessments. Absence means unknown, never unused, current,
false-empty, or failure; retrieval, citation, ordering, and checkpoint outcome
do not imply a label. Feedback is a content-free local sidecar. Its failure
must remain visible but never changes or rolls back the terminal checkpoint;
retry it through the exact checkpoint identity.

## Preflight prospective Memories

Before finalizing a turn that may save durable knowledge, call `mem_checkpoint`
with `operation: "preflight"`, the exact project, and the prospective inline
`memories`. Preflight is bounded and non-persisting: it creates no Memory,
proposal, checkpoint, relation, sync mutation, review state, or retired
candidate-evaluation state. It returns exact duplicate references and at most three full,
same-project semantic candidates across the prospective set.

Successful results include `assessment`: `exact_duplicates` and
`semantic_candidates` are `assessed` within the existing project and candidate
bounds; `session_project_compatibility` and `final_record_eligibility` are
`not_assessed`. This coverage declaration also applies to empty and exact-only
results. Treat `not_assessed` as unknown, never compatible, incompatible, or
authorized. Preflight reserves no state and does not guarantee a later commit;
final transaction validation and exact replay remain authoritative.

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

## Commit an explicitly evaluated replacement

When a preflight candidate's current guidance is explicitly replaced, preserve
its `reference.memory_id` and opaque `target_version` alongside the evaluated
content. Similarity, recency, and a sentence saying “supersedes” are evidence for
judgment only. Different versions or scopes may both remain useful: leave
independent historical truths eligible. Reuse exact duplicates; keep material
ambiguity under `needs_review`.

For a settled replacement, include `supersessions` in the same terminal
`mem_checkpoint` record as its replacement Memory. Each declaration contains
`target_memory_id`, the returned `target_version`, a concise `reason`, and
exactly one replacement selector: zero-based `replacement_input_index` into
`memories`, or `replacement_memory_id` also present in `memory_ids`. This binds
the relation to a settled Memory, including duplicate/reference reuse; a proposal
cannot replace a Memory. A Mixed Memory checkpoint may carry settled
supersessions alongside its isolated proposal.

```json
"supersessions": [{
  "replacement_input_index": 0,
  "target_memory_id": 42,
  "target_version": "<opaque version returned by preflight>",
  "reason": "The verified fix replaces the prior diagnosis as current guidance."
}]
```

This optional operation requires Protocol v2 and a preflight response exposing
`target_version`. If that evidence is absent, report unavailable atomic support;
do not claim that a prose-only save or a second post-checkpoint curation write
completed the replacement. Unrelated turns need no extra call.

Core validates project/write authority, endpoints, and evaluated target freshness
inside the new transaction. A stale target rejects the whole commit visibly:
reinspect and reevaluate it before retrying with the original root identity.
`created` commits the settled Memories, directed relations, checkpoint, optional
proposal, and enrollment-authorized local sync mutations together. This guarantees
local atomicity, not simultaneous cloud delivery. Default Recall excludes the
target; deliberate historical inspection retains its content and relationship.
Avoid a topic-key overwrite of the target, which would lose the evaluated history.
A previously stored reverse-direction relation is rejected rather than silently
reversing independent curation; inspect and resolve it through that separate
maintenance workflow before reevaluating the replacement.

After an ambiguous response loss, inspect the original checkpoint identity and
retry it when absent. `already_recorded` returns the original terminal result
without validating the changed target again, adding a relation, or accepting new
payload. A disposition change remains a conflict. Independent curation remains
available for separately authorized maintenance; it is not the completion path
for a replacement created by this terminal commit.

## Author for future Recall

Ask: **Will this prevent a specific repeated investigation or mistake?** Lead
with the reusable decision, invariant, non-obvious cause, or durable constraint
and the rationale needed to apply it. Keep applicability, uncertainty, and
necessary version boundaries beside the claim; put compact evidence references
after the knowledge. A title helps discovery, but the content must stand alone.

Ordinary Recall currently exposes a UTF-8-safe content prefix, capped at 512
bytes per candidate, rather than a semantic summary. The aggregate response
budget can further omit a summary. Put the actionable lesson and its essential
qualification early enough to be useful in a returned prefix; inspect ordinary
Recall with representative examples when changing this guidance. Full retrieval
can supply detail, but should not be needed merely to discover the lesson.
Preserve material qualifications even when they cannot fit: prefix placement
improves an opportunity for useful Recall, not guaranteed visibility. Judge
usefulness by the decision a future agent can make, not exact prose or a
universal word quota.

Keep one finding cohesive; split only independently reusable outcomes. Choose
a natural structure for the finding. CLI example labels such as What/Why/Where/
Learned are placeholders, not a required schema: put the reusable lesson and
its reason in the opening content rather than reserving them for a final label.

Delivery metadata earns space when it changes applicability or prevents a
specific repeated investigation: a first fixed version, an exact compatibility
boundary, or the identity of an external artifact that must be found again.
Place that boundary beside the claim and link supporting evidence afterward.
A PR number, full commit hashes, tests, CI, and cleanup do not by themselves
justify a companion delivery Memory. Apply the disposition rubric below to the
knowledge the turn produced.

### Contrasting examples

These are synthetic editorial examples, not claims about a real project.
Quoted contents illustrate choices rather than a mandatory format.

**Bug fix.** Delivery-first: “Merged PR 42; tests and CI passed; deleted the
branch. The retry bug is fixed.” A reusable saved finding instead leads with
cause, correction, and scope:

> In client v2.4, reuse the idempotency key when retrying a timed-out payment
> request: the server may have committed before the response was lost. Creating
> a new key can charge twice. This applies only to endpoints supporting
> idempotency keys; other endpoints still require reconciliation. Evidence:
> payment-client PR 42, timeout replay test.

The version and endpoint qualification determine where the correction is safe;
the PR locates evidence. CI and branch cleanup add no reusable lesson here.

**Architectural decision.** Activity-first: “Discussed cache options and selected
process-local caching.” A useful saved decision preserves the tradeoff:

> For the single-worker importer, keep the schema cache process-local to avoid
> cross-worker invalidation state. This assumes one worker owns the whole import;
> parallel workers would require revisiting ownership before sharing the cache.
> The decision reduces coordination, not database reads across independent runs.
> Evidence: importer ADR 7.

The assumption belongs beside the choice; removing it would turn a conditional
decision into unsafe general advice.

**Routine delivery.** “Renamed the documented command; PR 51 merged, checks
passed, branch removed” normally yields `skipped(no_durable_knowledge)` when
maintained source and docs already contain the complete result. An external
artifact can itself warrant `saved` when its identity prevents rediscovery:

> Use vendor support case CASE-82 for the unresolved importer timeout; it holds
> the vendor-requested trace and is the channel for follow-up. The vendor has
> not confirmed a cause or fix. Evidence: the team's vendor case link.

Here the case identity and unresolved state are the useful knowledge. Preserve
only a safe, actual reference in real work; inventing a cause would weaken it.

**No durable knowledge.** Answering “which command lists sessions?” from current
CLI help normally yields `skipped(no_durable_knowledge)`. Saving “answered the
question” would only duplicate activity. If the investigation instead uncovers
a verified, non-obvious constraint, assess that finding under `saved`; if it
remains materially uncertain, apply `needs_review`, preserving any independently
settled findings in the same Mixed Memory checkpoint.

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
  "host": "<host>",
  "session_id": "<opaque session id>",
  "root_turn_id": "<opaque original turn id>",
  "disposition": "skipped",
  "reason": "no_durable_knowledge",
  "recall_feedback": {
    "recall_id": "<exact Recall run>",
    "results": [{
      "result_id": "<opaque exposed result>",
      "utility": "orienting",
      "source": "agent_explicit"
    }]
  }
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
  --host '<host>' \
  --session-id '<opaque session id>' \
  --root-turn-id '<opaque original turn id>' \
  --disposition skipped \
  --reason no_durable_knowledge \
  --recall-feedback-json '{"recall_id":"<exact Recall run>","results":[{"result_id":"<opaque exposed result>","utility":"orienting","source":"agent_explicit"}]}' \
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
