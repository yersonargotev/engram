[← Codebase Guide](../CODEBASE-GUIDE.md) | [← Previous: Repository Map](repository-map.md) | [Next: Interfaces →](interfaces.md)

# Memory Core

**The memory core is `internal/store`: local SQLite + FTS5 is Engram's source of truth.** Interfaces should translate user/agent intent into store operations instead of reimplementing persistence rules.

## Commit and retrieve flow

The memory flow does not start in the database. It starts with the agent deciding something is worth remembering.

```text
1. The root user turn and all causal work settle

2. The agent preflights prospective Memories without persistence
   exact duplicates reused / at most three same-project candidates reviewed

3. The agent chooses one terminal disposition
   saved / needs_review / skipped(no_durable_knowledge)

4. mem_checkpoint commits the disposition, settled Memories, and proposal atomically

5. internal/project classifies identity; internal/mcp validates the contract
   strong config/remote or explicit/session authority → write
   weak git root/child/basename → generic discovery only; candidate Recall warns and returns empty

6. internal/store persists
   sessions / observations / memory_relations / sync_mutations
   FTS5 indexes for Memory search

7. Later work
   memoryops.Recall → at most five active/current candidate summaries and 4 KiB
   → mem_get_observation only when selected full detail can change the task

8. The terminal checkpoint may attach explicit Recall feedback as a local
   sidecar; checkpoint completion remains independent
```

## Store mental entities

| Entity | Purpose | Relevant files |
|---|---|---|
| `sessions` | Groups work from one agent session. | `internal/store/store.go`, `internal/mcp/activity.go` |
| `observations` | Curated memories: decisions, bugs, patterns, discoveries, summaries. | `internal/store/store.go`, `internal/store/store_test.go` |
| `observations_fts` | FTS5 search index. | `internal/store/store.go`, `DOCS.md#database-schema` |
| `diagnostic_captures` / `capture_consents` | Local-only Diagnostic Content and explicit project/content-type consent. Capture is disabled by default; retention is 7 days by default and at most 30 days. | `internal/store/capture.go`, `internal/memoryops/capture.go` |
| `user_prompts` | Frozen Legacy prompt archive. It is available only through explicit inventory, access, export, and separately confirmed purge operations; canonical journal/FTS copies are purged transactionally, while customized FTS ownership blocks the purge before mutation. | `internal/store/store.go`, `internal/memoryops/legacy_prompt.go` |
| `memory_relations` | Relationships/judgments between memories for semantic conflict surfacing. | `internal/store/relations.go`, `internal/mcp/mcp_judge_test.go` |
| `sync_mutations` | Queue of changes for sync/autosync. | `internal/store/store.go`, `internal/sync/sync.go`, `internal/cloud/autosync/manager.go` |
| `sync_apply_deferred` | Pull mutations deferred because dependencies are missing. | `internal/store/sync_apply_test.go`, `internal/server/server.go` |
| `memory_checkpoints` / checkpoint reference tables | Local-only root-turn dispositions and typed Memory or Memory-proposal references, excluded from every durable-Memory and replication surface. | `internal/store/checkpoint.go`, `internal/memoryops/checkpoint.go` |
| `memory_proposals` | Immutable local `needs_review` checkpoint audit snapshots that remain outside Memory retrieval and every replication surface; no workflow converts them into Memories. | `internal/store/memory_proposal.go` |
| `recall_runs` / `recall_results` / `recall_segments` | Content-free local Recall identity, selected semantic revision/local apply generation, and explicit segment positions. These tables retain no query or Memory content and never replicate. | `internal/store/recall.go`, `internal/memoryops/recall_content.go` |
| `recall_feedback_runs` / exposure, label, and false-empty tables | Per-install salted root-bound exposure snapshots and explicit Recall assessments. Unknown cohorts survive Memory deletion; the tables remain outside every Memory, export, and replication surface, and only aggregate reports leave Store. | `internal/store/recall_feedback.go`, `internal/memoryops/recall_feedback.go` |

For schema details, use [DOCS.md — Database Schema](../../DOCS.md#database-schema).

## Memory invariants

- Canonical agent guidance owns the durable-knowledge rubric. Independent
  `mem_save` remains available for explicit curation or a material loss-risk handoff.
- `topic_key` is for evolving topics; distinct decisions are not mixed under the same key.
- `scope=project` is the default; `scope=personal` exists for non-shared memory.
- Soft delete (`deleted_at`) hides data without physically deleting it unless explicit hard delete is used.
- Project detection is not Recall/write authority. Strong `config`/`git_remote`
  and validated explicit/session sources may run automatic candidate Recall or
  write; weak `git_root`/`git_child`/`dir_basename` sources remain generic
  read-only discovery until the caller supplies explicit authority.
- Candidate Recall belongs to `internal/memoryops`; Store supplies deterministic
  active/non-superseded candidates before `LIMIT`, while CLI, MCP, and HTTP only
  resolve authority and render the envelope. The default is five candidates and
  4 KiB, with up to ten only for deliberate follow-up.
- Search is progressive: compact candidate summaries first, then at most 16 KiB
  for one opaque selected result. `mem_get_observation` continues only through a
  new request at the returned UTF-8 byte position with unchanged authority.
- Recall feedback is an optional checkpoint sidecar. A Recall run is eligible
  only when search bound it to the same exact root turn; missing labels remain
  unknown, raw identities are used only for transient validation, and feedback
  failure never changes terminal checkpoint completion.
- Diagnostic Content is not Memory. It is excluded from Memory/FTS/Recall/context, ordinary export/import, sync/cloud, Obsidian, and retired candidate/promotion flows.
- Legacy prompts are preserved byte-for-byte during migration and remain outside those same ordinary surfaces. Migration neither reclassifies nor uploads them and creates no deletion tombstone.

## Local store change checklist

- [ ] The rule really belongs in `internal/store`.
- [ ] Migration/schema is covered by existing or new tests.
- [ ] FTS/dedupe/topic/scope/soft delete remain coherent.
- [ ] If it touches sync, mutations are queued or applied correctly.
- [ ] `internal/store/*_test.go` covers the expected flow and edge cases.
- [ ] `DOCS.md#database-schema` is updated if schema or public semantics change.

---

[← Previous: Repository Map](repository-map.md) | [Next: Interfaces →](interfaces.md)
