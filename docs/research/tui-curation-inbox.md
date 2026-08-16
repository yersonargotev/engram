# TUI curation inbox: viability and product value

Date: 2026-08-16

## Decision

**Go, with a narrower product model.** A TUI curation workflow is technically
viable and completes an already-approved Engram capability, but it should not
ship as one flat inbox containing “expired memories, conflicts, pin/unpin and
review.” Those concepts do not have the same semantics.

The coherent model is a **Review queue** with two eventual work-item types:

1. a memory whose review date is due; and
2. a relation awaiting a human judgment.

Pin/unpin is an action available while inspecting a memory, not an inbox item.
Deletion is not needed for the first release. Engram does not currently have
expired memories: `expires_at` exists in the schema but is deliberately always
NULL, while the implemented lifecycle exposes only `active` and
`needs_review` ([store model](../../internal/store/store.go#L88-L127),
[original design](../../openspec/changes/archive/2026-04-24-memory-conflict-surfacing/design.md#L406-L414)).

## Why this adds value

The proposal closes a real last-mile gap rather than inventing a new subsystem:

- Engram already calculates review dates for decisions, policies, and
  preferences, queries due memories, and resets their cycles
  ([decay policy](../../internal/store/store.go#L241-L255),
  [review query and mutation](../../internal/store/store.go#L2498-L2547)).
- The shared memory service already exposes review list/mark and pin/unpin
  operations, so the TUI can call domain behavior rather than duplicate rules
  ([memory operations](../../internal/memoryops/memoryops.go#L323-L390)).
- The lifecycle issue explicitly left a dedicated TUI “Review Queue” for a
  follow-up. Its maintainer guidance says `needs_review` memories must be
  verified before use and must not be marked reviewed automatically without
  explicit confirmation or a dedicated maintenance command
  ([issue #437](https://github.com/Gentleman-Programming/engram/issues/437),
  [maintainer decision](https://github.com/Gentleman-Programming/engram/issues/437#issuecomment-4691304002)).
- Today the TUI displays state, review date, and pin state, but its observation
  detail supports only scrolling, copying, timeline, and back navigation. It
  therefore reveals maintenance signals without allowing users to act on them
  ([detail rendering](../../internal/tui/view.go#L475-L538),
  [detail keys](../../internal/tui/update.go#L503-L527)).
- Conflict substrate also exists: pending relations can be listed with both
  titles and judged through the store contract
  ([relation list shape](../../internal/store/relations.go#L89-L124),
  [judgment semantics](../../internal/store/relations.go#L556-L690),
  [relation listing](../../internal/store/relations.go#L1066-L1097)).

The user value is **memory trust**: a human gets a bounded place to verify old
guidance and resolve ambiguous relationships before agents keep relying on it.
This directly supports Engram's product promise of durable project knowledge,
because durability without maintenance eventually turns old guidance into
misleading guidance. The original lifecycle issue documents that exact failure
mode and the lack of an audit surface ([issue #437](https://github.com/Gentleman-Programming/engram/issues/437)).

## Correct domain model

| Proposed concept | Actual Engram meaning | Inbox behavior |
| --- | --- | --- |
| “Expired memory” | Not implemented; `expires_at` is NULL. A due `review_after` means “verify,” not “invalid.” | Rename to **Needs review**. Never hide or delete automatically. |
| Review | Confirm the memory is still accurate, then reset its local review cycle. | `Mark reviewed` completes the item, behind an explicit action. |
| Conflict | A pending candidate relation needs a verdict such as `related`, `compatible`, `scoped`, `supersedes`, `conflicts_with`, or `not_conflict`. | Separate queue type with a side-by-side comparison and decision-specific copy. |
| Pin/unpin | Local context priority; pinned memories appear before recent context. It does not express freshness or correctness. | Toggle from memory detail; it does not complete a review item. |
| Delete | Soft/hard removal with different audit consequences. | Exclude from the MVP; add only with a separately designed confirmation flow. |

Pinning was designed specifically to preserve important context and was kept
separate from lifecycle freshness. The approved issue also excluded bulk
pin/unpin and a dedicated pinned screen
([issue #483](https://github.com/Gentleman-Programming/engram/issues/483),
[PR #484](https://github.com/Gentleman-Programming/engram/pull/484)). Treating
pins as inbox work would collapse “important” and “needs attention” into one
misleading state.

## Recommended delivery

### Increment 1: review queue only

Add one dashboard entry, **Review memories**, scoped to the current project.
The list is oldest-due first and shows type, title, how long review is overdue,
and pin state. Selecting an item opens the existing full memory detail with
three relevant outcomes:

- **Mark reviewed**: explicit confirmation that the content remains accurate;
- **Pin/unpin**: change context priority without removing the item; and
- **Back/defer**: leave it due without changing data.

Do not mark an item reviewed merely because it was opened. Do not add bulk
actions. Do not add delete. An empty state should explain that no memories are
due; loading and error states must be explicit, matching the TUI quality rules.

This increment is small but end-to-end: it turns an existing signal into a
safe maintenance workflow, uses real store semantics, and contains no
irreversible action. It also validates whether users actually accumulate and
clear due work before Engram invests in the more complex conflict experience.

### Increment 2: pending judgments

Only after the review queue proves useful, add a second filter/tab for pending
relations. This cannot reuse the single-memory detail unchanged. It needs:

- both complete memories visible together;
- the candidate reason/evidence and provenance when present;
- plain-language explanations of each verdict;
- a visible distinction between `supersedes` and `conflicts_with`; and
- confirmation for high-impact decisions or policies.

Judging a relation overwrites the pending row and may enqueue a cloud sync
mutation for enrolled projects, so it is semantically heavier than a local
review reset ([JudgeRelation](../../internal/store/relations.go#L556-L690)).
The original conflict proposal likewise requires conversation before recording
a destructive/high-impact verdict
([proposal](../../openspec/changes/archive/2026-04-24-memory-conflict-surfacing/proposal.md#L232-L245)).

### Later, only with separate evidence

- Editing from the TUI: useful for correcting an inaccurate due memory, but it
  requires a real editor/form interaction and should not be hidden inside
  `Mark reviewed`.
- Soft deletion/archive: design recovery, relation visibility, and confirmation
  first.
- Actual expiration: requires defining what `expires_at` means, how it affects
  recall/context, sync behavior, and recovery. It is not a UI-only addition.
- Bulk triage: consider only after observing repetitive, low-risk decisions.

## Architecture and safety constraints

The TUI should remain an adapter over shared domain operations. Local SQLite is
the source of truth, and UI controls must correspond to enforced behavior
([architecture guardrail](../../skills/architecture-guardrails/SKILL.md#L20-L33),
[business rule](../../skills/business-rules/SKILL.md#L20-L28)). Review dates and
pins are intentionally local-only, while judged relations can sync; the ADR
records these contract boundaries
([ADR-0001](../adr/0001-cli-as-memory-domain-contract.md#L24-L32)). The UI must
say “local to this device” where that distinction affects expectations.

Implementation should use asynchronous Bubble Tea commands and explicit
success/error messages, preserve the originating queue/filter when returning
from detail, clamp scrolling after refresh/resize, and add deterministic model,
update, and view tests. These are existing project requirements, not optional
polish ([TUI rules](../../skills/tui-quality/SKILL.md#L17-L36)).

## Main risks

| Risk | Consequence | Mitigation |
| --- | --- | --- |
| Calling due memories “expired” | Users may assume they are invalid or safe to delete. | Use `Needs review`; explain that the date is a verification reminder. |
| Flat inbox of heterogeneous controls | Superficial UI with unclear completion semantics. | Two work-item types; pins are metadata/actions; delete is separate. |
| Accidental review completion | Stale knowledge gets certified without inspection. | Explicit action, never on open; show the next review date before confirming. |
| Conflict verdict without enough context | Incorrect `supersedes`/`conflicts_with` relation may propagate. | Side-by-side full content, provenance, explanations, confirmation. |
| Empty queues for newer/small projects | Feature appears valuable in theory but is rarely used. | Ship review-only first and measure real queue formation/clearance. |
| Local/cloud expectation mismatch | Review or pin changes appear “lost” on another device. | Label local-only actions and keep relation sync behavior distinct. |

## Success criteria

The feature is worth extending beyond Increment 1 if dogfooding or opt-in
feedback shows that:

1. long-lived projects regularly produce due review items;
2. users can understand why an item is present without reading documentation;
3. users complete review intentionally, with no accidental marks;
4. median time and keystrokes to inspect and resolve an item are lower than the
   equivalent CLI workflow;
5. users correct, supersede, or deliberately defer some items rather than
   mechanically clearing every item; and
6. no support reports confuse `needs_review` with deletion/expiration or local
   review/pin state with synced state.

Useful local counters would be queue size, outcome (`reviewed`, `deferred`,
`edited`, later `judged`), and time-to-resolution. They should remain local or
be collected only through an explicit telemetry policy.

## Final assessment

- **Viable:** yes; the store/service contracts and TUI navigation patterns are
  already present.
- **Useful:** yes for long-lived projects, because it converts visible but
  unactionable lifecycle metadata into deliberate maintenance.
- **Valuable to Engram:** yes; it improves the reliability of agent recall and
  completes a follow-up explicitly anticipated by the lifecycle design.
- **Recommended scope:** ship a project-scoped review queue first. Add conflict
  judgment only as a distinct comparison workflow. Keep pin/unpin contextual
  and exclude deletion and “expiration” from the initial feature.
