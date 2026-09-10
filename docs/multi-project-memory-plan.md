# Multi-project Terminal Memory implementation and migration plan

Status: design-delivery plan for [#159](https://github.com/yersonargotev/engram/issues/159).
[ADR-0012](adr/0012-group-terminal-memory-by-project.md) owns the accepted future
contract. This plan prepares two separately approvable scopes; neither runtime
implementation nor migration execution is authorized by merging the design.
Current CLI/MCP checkpoints still accept one project. The
[retained research](research/multi-project-terminal-memory-contract.md) documents
that baseline and the design rationale.

## Implementation brief: grouped Core and adapter behavior

Objective: let one authorized root turn commit project A and B atomically under
its original identity, and let subsequent turns reuse project-owned provenance.
Do not expand the default five-tool profile, add ACLs, parse conversations for
consent, promote proposals, repair historical sessions, or release a new binary.

| Increment | Owner and observable completion |
| --- | --- |
| Persistence prerequisite | `internal/store`: the separately approved migration/lifecycle scope below preserves old evidence and supports grouped snapshots before grouped admission is enabled. |
| One project, new provenance | `internal/store` and `internal/memoryops`: Core resolves `(host, session_id, project)` to a fresh internal session for new inline records, including legacy input. A then B then A succeeds without changing old sessions. Reference/proposal-only groups allocate no session. Matching directory provenance is verified or empty. |
| Grouped transaction | `internal/memoryops` validates explicit destinations and normalizes groups; `internal/store` atomically persists associations, Memories, proposals, references, supersessions, ledger, and permitted sync mutations. Group-local indices, ordering, exact replay, collision handling, and rollback follow ADR-0012. |
| Protocol and adapters | `cmd/engram` parses repeatable future `--project-json`; `internal/mcp` parses `projects`; both delegate to Core. Implement plural/singular projections, group-index errors, removed-content status/replay, and proposal-free `needs_review` rendering. Target Protocol 3 relative to the current Protocol 2 baseline. |
| Host and distribution compatibility | `internal/protocolcontract`, setup/verifier and host assets: prove supported tuples and reject unsupported grouped capabilities before sending payloads. Preserve legacy requests/reads. Publish runtime guidance and regenerate canonical skill projections only with working code and verified compatibility. Release remains separately authorized. |

Work in behavior-first vertical slices at public Store/service and CLI/MCP
interfaces identified by the issue. Use disposable databases and synthetic host
identities; one failing behavior test precedes its minimal implementation.
Keep the grouped capability unavailable until persistence, lifecycle, both
adapters, and compatibility proofs are complete. Do not publish a partial
implementation that can admit grouped content before protecting its deletion.

## Agent management matrix

These are existing operation names inspected at `8189fca0be53d84ce6c5b36706065416b21be08b`,
not executable grouped examples. CLI Memory operations use `--json` for structured
success/error output. MCP profiles can be combined through `engram mcp --tools=...`;
`all` exposes all operations, while normal host setup explicitly selects `agent`.
Profile availability is not permission to act outside delegated task scope.

| Intent | CLI | MCP / profile | Semantics and grouped acceptance |
| --- | --- | --- | --- |
| Terminal creation | `checkpoint preflight`, `checkpoint record` | `mem_checkpoint` / `agent` | Preflight each prospective project's settled content; one terminal commit. Grouped inputs are not implemented. |
| Independent creation | `save` | `mem_save` / `curation` | Explicit curation or material loss-risk handoff only; not a terminal workaround. Resulting Memories must remain manageable regardless of session provenance. |
| Scoped search | `search` | `mem_search` / `agent` | A cannot retrieve B merely because both were committed together. Preserve bounded Recall and explicit scope. |
| Complete retrieval | `get <id>` for deliberate curation; `get --recall-id --result-id` and returned `--position` for bounded Recall | `mem_get_observation` / `agent` | Retrieve by supported identity; follow returned continuation positions without widening scope. |
| Update | `update <id>` | `mem_update` / `curation` | Partial update cannot move ownership. Preserve supported topic-key clearing controls. |
| Review Memory | `review list`, `review mark <id>` | `mem_review` (`list`, `mark_reviewed`) / `curation` | Local review scheduling is not proof of truth or proposal promotion. |
| Relate | `conflicts compare`, `conflicts judge` | `mem_compare`, `mem_judge` / `curation` | Persist an explicit verdict; preserve same-project validation and existing judgment-specific uncertainty rules. |
| Supersede | `checkpoint record --supersession-json`; independent `conflicts compare ... --relation supersedes` | `mem_checkpoint` / `agent`; `mem_compare` / `curation` | Protocol 2 already supports atomic terminal replacement with evaluated `target_version`; future indices are group-local. Independent curation stays distinct. |
| Delete Memory | `delete <id> [--hard] --json` | `mem_delete` (`hard_delete` when requested) / `admin` | Supported noninteractive management requires delegated deletion scope. Verify soft and hard deletion leave other projects intact. |
| Inspect unresolved proposal | `checkpoint status` | `mem_checkpoint_status` / `agent` | Investigate and save newly settled knowledge in a later turn; never append or promote the original proposal. |
| Project maintenance | `projects merge`; `delete project` | `mem_merge_projects` / `admin` for merge | CLI covers broader project lifecycle than MCP; rename is a Store lifecycle operation (also exposed by interactive consolidation), not a standalone `projects rename` command. No rename/delete-project MCP tool is implied. Preserve existing noninteractive confirmation controls. Grouped lifecycle protection remains implementation work. |

Sources: [CLI memory operations](../cmd/engram/cli_memory.go),
[CLI deletion and profile selection](../cmd/engram/main.go),
[relation commands](../cmd/engram/conflicts.go),
[MCP registration and profiles](../internal/mcp/mcp.go), and
[atomic supersession](../internal/memoryops/checkpoint.go).

The supported individual-Memory management surfaces already exist. Missing work
is grouped creation/results, Core provenance, plural proposal persistence,
protected grouped deletion, and verified compatibility across those interfaces.
No new Memory class may require SQL, a UI, or a fabricated session ID to manage.
No categorical human-only restriction applies to specialized profiles. Existing
judgment-specific escalation and destructive confirmation controls remain;
`needs_review` itself does not impose a mandatory human-operated queue.

## Separately approvable migration and lifecycle scope

Objective: add local association storage, plural proposal references and grouped
result/tombstone metadata without rewriting old evidence. Approval must cover
this entire preservation boundary before grouped admission can be enabled.

- Preserve historical checkpoint identities, dispositions, timestamps, proposal
  IDs/content, ordered references, sessions, and Memory foreign keys. Distinguish
  legacy and grouped records explicitly so legacy deletion semantics do not
  silently change. Do not claim old host-named sessions or infer their host.
- Enforce association uniqueness transactionally. Two hosts using identical
  session strings remain distinct; concurrent turns for one host/project converge
  without orphan sessions. Use Core-generated opaque IDs, not concatenation.
- Keep associations and checkpoint/proposal evidence local-only. Existing session
  and Memory sync follows enrollment. A local-only group may coexist with an
  enrolled group; actual policy rejection fails the whole local write. Remote
  delivery is not an atomic multi-project transaction.
- Rename must update association ownership without changing internal IDs. Merge
  preserves all historical sessions/proposals and chooses a stable association
  for future writes when keys collide. Do not force historical merged proposals
  into the new one-proposal-per-group admission rule.
- For grouped records, deletion removes only authorized owned content/references;
  preserve other projects and terminal uniqueness. Persist `content_removed`
  and original `removed_group_indices` without removed project names/content.
  Total deletion retains a content-free identity/disposition/timestamp tombstone.
  A surviving `needs_review` may have no proposal; status/replay render it safely
  and can neither recreate deleted content nor reopen that root turn.
- Cover Memory soft/hard deletion, project deletion, and any session-deletion
  cascade that reaches grouped content. Keep removal metadata accurate under
  repeated operations. Association rows must not leak deleted project existence.
- Logical export/import preserves sessions and Memory foreign keys but not local
  association/checkpoint continuity; later writes may create fresh sessions.
  Complete SQLite backups preserve local continuity. Verify both behaviors.

Migration implementation must use disposable pre-change fixtures with singular
and Mixed checkpoints, proposal-only projects, references, supersessions,
enrolled/local-only destinations and legacy host-session collisions. Inspect
schema constraints and cascade paths before selecting additive SQL. Migration
failure must roll back its own work; Store-open initialization is separate from
checkpoint atomicity. Compare domain rows, not only migration timestamps.

Before execution, provide a versioned backup/restore rehearsal, expected schema
and evidence inventory, failure injection results, and a downgrade decision.
Do not assume old binaries safely read or mutate grouped state: block unsupported
combinations through verified compatibility. No destructive down-migration or
post-upgrade data loss is authorized. If rollback would require restoring a
pre-upgrade backup and losing later writes, stop and obtain a separate recovery
scope; do not silently restore it. Real database migration, reporter repair,
data reassociation and release remain outside this design delivery.

## Acceptance proof for the implementation

Use the full [ADR acceptance scenarios](adr/0012-group-terminal-memory-by-project.md#acceptance-scenarios)
as the contract. This table assigns proof to its observable boundaries; it does
not claim that future scenarios pass today.

| Scenario family | Required proof |
| --- | --- |
| Sequential and same-turn destinations | Disposable Core tests: A/B/A inline across turns; A+B inline, mixed references and reference-only combinations in one turn; unchanged opaque identity and exactly one terminal result. |
| Dispositions and proposals | Core plus CLI/MCP: saved, proposal-only A+B, saved A/proposal B Mixed, legacy singular projection, no sessions for reference/proposal-only groups, local-only proposals. |
| Authority and invalid inputs | Core/adapter rejection of missing selection, duplicate normalized names, empty/unknown fields, mismatched owners, mixed input shapes, invalid disposition and stale targets. Host scenarios separately cover cwd-only/weak detection and explicit delegated targets; tests cannot infer user consent from strings. |
| Atomicity and concurrency | Inject final-group and sync-enqueue failures; inspect absence of all new domain writes. Race same/different dispositions and association establishment, reopen and replay changed/malformed payloads; no append or orphan writes. |
| Provenance and isolation | Equal session strings across hosts, matching/empty directory, A-only Recall excludes B, enrolled/local-only sync combinations, logical export/import and full backup/reopen. |
| Complete agent management | Real isolated CLI and MCP: create grouped A+B, search/retrieve each within scope, update and mark review, compare/supersede within A, then authorized soft/hard delete in A. B's content, ownership and terminal evidence survive. No SQL or UI in the operator path. |
| Unresolved knowledge | Inspect original proposal, investigate, commit settled knowledge in a new turn, then replay the original: unchanged proposal, no promotion/append. |
| Lifecycle and old evidence | Migrate old fixtures, replay old checkpoints, rename/merge collision cases, partial/total grouped deletion and proposal-free `needs_review`, stable removed indices, repeated deletion and inability to finalize the old turn anew. |
| Compatibility | Same matrix through CLI/MCP and Core; old supported inputs accepted; unsupported grouped tuples rejected before record; status/verifier render plural and removed results. |

Design-delivery verification reads the rendered Markdown structure, resolves local
links, checks the JSON example, compares every formal-brief constraint, and checks
current operation names against source/help. TDD adds no runtime tests in this
PR because only documents and explanatory comments change. Required existing
unit/E2E suites and independent Standards/Spec reviews still apply; their results
belong to the PR's exact candidate evidence, not claims of grouped runtime support.
