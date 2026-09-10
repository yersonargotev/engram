---
status: accepted
---

# Group Terminal Memory by project beneath immutable host identity

Accepted design for [issue #159](https://github.com/yersonargotev/engram/issues/159),
following its [authoritative Agent Brief](https://github.com/yersonargotev/engram/issues/159#issuecomment-5613339506).
This decision is normative for future implementation, not a claim of shipped
behavior. Runtime remains single-project. Runtime implementation, migration,
lifecycle enablement, and release require separate scopes and approval; the
[implementation and migration plan](../multi-project-memory-plan.md) defines those gates.

## Product decision

A root user turn can produce durable knowledge for several projects. Each Memory
and Memory proposal has exactly one owner. A checkpoint records the terminal
outcome of the whole turn, rather than owning all knowledge under one project.
Recall remains project-scoped by default.

Keep `(host, session_id, root_turn_id)` unchanged and unique. Core associates each
`(host, session_id, project)` with an internal project-owned persistence session.
Neither the agent nor a host adapter generates replacement identity values.

Reject deliberately single-project sessions as the selected product behavior:
they cannot satisfy same-turn multi-project results without storing knowledge in
the wrong destination or performing separate nonterminal writes. Reject removing
ownership checks: references, proposals, and supersessions still need ownership.
Reject project in checkpoint identity: it permits several terminal results for one turn.

## Agent operation is a required product contract

Agents are primary operators. Every Memory, whether created by a checkpoint or
independent authorized curation, remains accessible through supported machine-readable
operations for creation, scoped search, complete retrieval, update, review, relations,
supersession, and deletion. New internal sessions must not create a special class
of Memory that only a human, UI, raw SQL, or session-ID workaround can manage.
Use the existing CLI and MCP curation/admin surfaces for their supported operations;
keep equivalent operations consistent through Core and document profile discovery.
The minimal default tool profile need not expose every management tool at once.
The design delivery must identify and correct contradictory guidance that describes
specialized management as categorically unavailable to agents. Profile separation
controls discovery and exposure, not whether an authorized agent may be an operator.

Delegated task authority is sufficient within its scope. Do not require a human
confirmation for every Memory, project group, or checkpoint. Existing noninteractive
confirmation controls for explicitly authorized destructive management remain usable
by agents; this does not grant blanket deletion authority. A host/agent asks the user
only when scope or a consequential decision cannot be resolved within its delegation.

`needs_review` records unresolved knowledge; it does not require a human operator
as a universal runtime prerequisite. An authorized agent can inspect the original
checkpoint, investigate further, and commit newly settled knowledge in a later root
turn. The original proposal stays immutable and is not converted into a Memory or
appended to the finalized checkpoint. Human judgment is needed only when the actual
uncertainty or authority boundary requires it. Product-design and migration approvals
in the delivery process are distinct from normal agent Memory operations.

## Selected future input

Retain the existing envelope and add `projects`, an ordered list of nonempty groups:

```json
{
  "host": "codex",
  "session_id": "opaque-host-session",
  "root_turn_id": "opaque-root-turn",
  "disposition": "needs_review",
  "projects": [
    {
      "project": "engram",
      "memories": [{"title": "Settled finding", "content": "Reusable knowledge."}]
    },
    {
      "project": "homebrew-tap",
      "proposal": {"title": "Unresolved finding", "content": "Candidate knowledge and review reason."}
    }
  ]
}
```

This is a future contract example, not an executable command today.

Each group accepts `project`, `memory_ids`, `memories`, `supersessions`, and at most
one `proposal`. Existing inner shapes remain unchanged. Normalize project names
using Core's existing rules; reject duplicate normalized projects, empty groups,
unknown group fields, and missing/empty project names. Do not merge groups silently.
Memories cannot override their group's owner. Supersession input indices are local
to that group's `memories`; both endpoints must belong to that project.

The legacy top-level `project` and content fields remain supported as one group.
Reject combining them with `projects` on a new record. CLI adds repeatable
`--project-json` carrying one group; MCP accepts the same groups through `projects`.
Adapters parse and delegate; Core owns normalization, semantics, and validation.
No additional mandatory tool or write operation is introduced.

## Disposition and proposals

| Disposition | Required content |
| --- | --- |
| `saved` | At least one settled Memory/reference overall; every group has settled content; no proposals. |
| `needs_review` | At least one proposal overall; each group has settled content, one proposal, or both. |
| `skipped` | No project groups or content; existing `no_durable_knowledge` reason. |

There is one disposition for the entire turn. The caller submits it and Core
validates the content against it; Core must not silently change the disposition.
A Mixed Memory checkpoint contains at least one settled Memory and at least one
proposal across all groups, even if those belong to different projects.

Permit at most one proposal per project per checkpoint, rather than one proposal
for the whole turn. Several unresolved findings for one owner can share its bounded
proposal. Do not force a proposal for settled-only projects. Proposal-only groups
are valid and do not require creating a persistence session. Proposals remain
immutable local audit evidence, excluded from Recall, sync, export, and promotion.

Ownership uncertainty is not a new shared or ownerless project: the agent must
resolve the destination before committing affected content. It cannot mark such
content skipped merely because ownership is unresolved or assign it to cwd by default.
This extends [ADR-0011](0011-preserve-mixed-terminal-memory-outcomes.md):
the grouped contract admits one proposal per project, while the shipped legacy
contract still admits exactly one proposal for the whole checkpoint.

## Authority and detection

Keep the existing trusted-local-caller boundary. An explicit project on each
group is the caller's assertion of destination authority. Core validates each
selection and actual ownership independently under the same local policy used
for single-project writes. Do not add caller-controlled `authorized: true` fields
or pretend that Core can infer user consent from a project string.

The host/agent must have task authority for every selected destination. A cwd
change, an existing Memory reference, a prior session binding, or successful
preflight does not grant that authority. Weak/missing detection cannot be upgraded
by copying its guessed project into an explicit group. An explicitly authorized
target can be supplied without strong cwd detection and need not already contain
Memories. Changing cwd alone must not automatically emit a destination group.

No new ACL registry or conversation parser is proposed. If a future untrusted
transport supplies an enforced allowlist, Core must apply it to every destination
before writing; the present CLI/MCP are not such a security boundary. Acceptance
tests must distinguish missing explicit selection and ownership rejection from
human-authorization claims that the current runtime cannot independently verify.
This interprets #159's independent write-authority requirement as per-destination
validation at the trusted local boundary plus the host/agent's existing delegated
task authority. This is the selected interpretation for the formal design; it does
not introduce per-write human approval or claim runtime proof of human consent.

## Internal session provenance

Use a local association from `(host, host_session_id, canonical project)` to a
Core-generated opaque internal session ID. Generate a fresh ID when no association
exists and enforce uniqueness transactionally; never derive it by unescaped string
concatenation. The association is reused across root turns and after reopen.
Different hosts with identical session strings receive distinct associations.

Apply this to new inline commits through both grouped and legacy inputs. Leave
every historical session, Memory, checkpoint identity, and reference untouched.
Do not opportunistically claim an old host-named session: its host provenance may
be ambiguous. New sessions may therefore coexist with old sessions for the same
conversation. Independent lifecycle operations and summaries keep their existing
meaning; a checkpoint does not fabricate session start/end activity or summaries.

Create associations and sessions only for groups with inline Memories. A
references-only group validates Memory ownership but does not need a session;
the existing asymmetry becomes an explicit distinction between creating and
referencing knowledge, rather than a workaround for cross-project creation.
The selected project must not inherit another project's cwd as directory provenance.
Use Core-verified matching directory context when available; otherwise store an
empty directory. Project identity does not require a filesystem path.

## Atomicity, replay, and results

Commit the ledger, ordered references, proposals, new associations/sessions,
Memories, supersession relations, and locally enqueued sync mutations in one
SQLite transaction. Any invalid destination, stale supersession target, or write
failure rolls back all new domain writes, even if an earlier group was valid.
Store initialization migrations are separate from this transaction.

Keep replay before new-payload validation: the same identity and disposition
returns the original result, even if new groups are malformed, reordered, or
different. A different disposition conflicts. Replay cannot append projects.
Resolve concurrent duplicate calls inside the transaction as well: exactly one
wins, and others replay its result or conflict. Different turns racing to establish
the same association must converge on one session without orphan writes.

Keep existing flat ordered Memory references, which already carry project.
Add ordered plural `proposals` to results, each retaining its own project. Preserve
the legacy singular `proposal` when there is exactly one, and omit it when there
are several; never select an arbitrary proposal for an older reader. Persist order
so status and replay return the same references and proposals after reopen.
Flatten Memories by group order, preserving existing within-group ordering rules.
Errors retain existing codes where applicable and add the failing group index;
include project/Memory detail only under the existing authorized disclosure policy.

Recall feedback remains the existing optional post-commit local sidecar, not part
of the atomic guarantee. Keep its current single-Recall-run shape; multi-project
feedback aggregation is separate scope and is not required to record the checkpoint.

## Preflight, sync, and lifecycle

Keep preflight project-scoped and read-only. Run it for each project with prospective
settled Memories and assess all returned candidates before the one terminal commit.
Existing per-call bounds remain; do not introduce automatic global Recall. Preflight
still does not reserve state or guarantee final eligibility. Proposal-only groups
do not require an empty preflight call.

Sync project-owned persistence sessions and settled Memories using existing entity
contracts and enrollment rules. Keep checkpoint identities, association mappings,
and proposals local-only. Enqueue allowed mutations atomically locally; remote
delivery is independently ordered/retried per existing sync semantics, not a
distributed multi-project transaction. A local-only destination can coexist with
an enrolled one. Preserve existing policy rejection behavior without silently
dropping a required write. Do not replicate raw host identity through the association.
Logical export/import retains sessions and their Memory foreign keys but does not
restore local association or checkpoint continuity; a later commit can create a
new internal session. A complete SQLite backup preserves those local records.

Project lifecycle needs explicit coverage before enabling this model. Today deleting
a project's proposal can delete its whole checkpoint. That would erase another
project's terminal evidence. For new multi-project checkpoints, retain the identity
and disposition as a content-free terminal tombstone when deleting owned content;
remove only that project's content/references and retain other projects' evidence.
For partial deletion, status/replay retain surviving content and add
`content_removed: true` plus `removed_group_indices`, containing the original
zero-based group positions without deleted project names or content. For total
deletion, the terminal result contains identity, disposition, timestamps, and that
removal metadata, with no Memory/proposal content. The original disposition remains
even when the last proposal is removed. This is an explicit authorized-deletion
exception to immutable result content, never an exception to terminal identity:
later replay returns the retained snapshot and cannot recreate deleted content or
make the root turn writable again. These new shapes apply to grouped Protocol 3
records and require Protocol 3 renderers, including a safe proposal-free
`needs_review` view. Existing legacy deletion behavior is not silently changed.
This lifecycle extension requires explicit approval in the implementation scope.

Rename updates association ownership under existing project operations without
changing internal IDs. Merge can bring several historical sessions and proposals
under one project: preserve them as historical evidence, choose a stable association
for future writes, and apply the one-group/one-proposal rule only at new admission.
Do not rewrite historical proposals to force the new admission cardinality.
Association rows must not introduce a new project-existence or deletion leak.

## Compatibility and delivery gates

Target the next Protocol version (3 relative to inspected Protocol 2), because
plural proposals and session provenance change observable semantics. Preserve
legacy single-project inputs, old persisted evidence, and old checkpoint reads.
Older clients must not be advertised as capable of reading plural-proposal or
content-removed grouped results;
update compatibility fixtures and declarations for the actual verified range.
Do not let an old binary silently ignore grouped input and finalize an incomplete turn.
Capability verification precedes sending the new shape to an installed binary.

The required schema work is additive association storage plus plural proposal
references and lifecycle/tombstone support. A separately approved migration must
preserve old proposal IDs, content, reference order, and ledger identities; no
automatic reassociation, reporter-session repair, destructive downgrade, or release
is authorized by this proposal. Publish current-behavior docs only with working code.

Suggested implementation sequence, behind the new capability until complete:

1. Approve the contract and its ADR-0011/glossary changes; approve a migration and
   lifecycle brief with explicit preservation/downgrade boundaries.
2. Implement Core associations, grouped transaction semantics, plural results,
   migration, and lifecycle protection together with disposable Store tests.
3. Add equivalent CLI/MCP input and rendering, Protocol fixtures, verifier/status
   compatibility, and user guidance. Validate the compatibility tuple before release.

## Acceptance scenarios

- An agent completes grouped creation, scoped Recall/retrieval, update, review,
  relation/supersession, and authorized deletion of the resulting Memories through
  supported noninteractive interfaces, without a UI, SQL, fabricated session ID, or
  obligatory human approval. Exercise equivalent CLI/MCP operations with their
  documented profiles and preserve existing management semantics.
- An agent investigates an unresolved proposal and saves newly settled knowledge
  in a later turn; the original checkpoint/proposal remains immutable and replay
  does not append or promote anything.
- A then B in separate root turns succeeds inline under one unchanged host session;
  original A evidence stays A. Repeating A reuses its new internal association.
- A and B inline in one turn yields one ledger row and distinct owned sessions;
  mixed inline/reference and reference-only combinations obey the same owner checks.
- Saved A plus proposal B is Mixed; proposal A plus proposal B has two local proposals;
  a single-project legacy Mixed call keeps its singular result projection.
- Missing project, duplicate normalized project, empty group, invalid disposition,
  mixed legacy/grouped input, or mismatched reference rejects the entire new commit.
- Host guidance does not select a new destination from cwd alone or weak detection;
  explicit authorized destination works independently of cwd. No test claims to
  prove user consent from a caller-controlled field.
- Inject a failure in the final group and a stale supersession: no new session,
  association, Memory, relation, proposal, sync mutation, or checkpoint survives.
- Race identical and conflicting calls, then reopen; one terminal outcome survives.
  Replay changed/malformed input cannot append a destination or alter the outcome.
- Equal session strings from two different hosts do not share new internal sessions.
- Recall for A cannot return B through grouped writes; proposal/checkpoint exclusion
  holds for Recall, export, and sync. Verify enrolled and local-only combinations.
- Load a pre-change database, migrate, replay historical checkpoints, import/export
  and push/pull existing sessions/Memories without reassociation or lost evidence.
- Rename/merge/delete A leaves B evidence and terminal uniqueness intact, including
  plural proposals and association collisions after project merge.
- Run the same behavior matrix through Core, CLI, and MCP; older supported payloads
  remain accepted, and unsupported grouped-capability combinations fail visibly.

## Evidence and delivery boundary

The [retained investigation](../research/multi-project-terminal-memory-contract.md)
contains the source revision, disposable baseline experiment, rejected options,
and original proposal. [Earlier cross-project research](../research/host-conversation-multiple-projects.md)
records the inline/reference asymmetry. These baseline checks establish the
current restriction; they are not evidence that grouped behavior works.

The [implementation and migration plan](../multi-project-memory-plan.md) maps
this contract to owned work, agent management interfaces, and acceptance proof.
Approval of this ADR does not change the installed Protocol range or authorize
repairing existing sessions.
