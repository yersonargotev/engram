[← Back to README](README.md)

# Engram — Technical Reference

**Persistent memory for AI coding agents**

This is the complete technical reference for Engram. For getting started, see the [README](README.md). For per-agent setup, see [Agent Setup](docs/AGENT-SETUP.md).

---

## Quick Navigation

| Section                                                   | What you'll find                                             |
| --------------------------------------------------------- | ------------------------------------------------------------ |
| [Database Schema](#database-schema)                       | Tables, FTS5, SQLite config                                  |
| [HTTP API](#http-api-endpoints)                           | All REST endpoints with request/response details             |
| [MCP Tools](#mcp-tools-24-tools)                          | Detailed reference for all 24 memory tools                   |
| [MCP Project Resolution](#mcp-project-resolution)         | Auto-detection algorithm, response envelope, tool categories |
| [Memory Protocol](#memory-protocol)                       | When/how agents should use the tools                         |
| [Project Name Normalization](#project-name-normalization) | Auto-detection, normalization, similar-project warnings      |
| [Features](#features)                                     | FTS5 search, timeline, privacy, git sync, compression        |
| [TUI](#terminal-ui-tui)                                   | Screens, navigation, architecture                            |
| [Running as a Service](#running-as-a-service)             | systemd setup                                                |
| [Design Decisions](#design-decisions)                     | Why Go, why SQLite, why no raw auto-capture                  |

For other docs:

| Doc                                         | Description                                                                                   |
| ------------------------------------------- | --------------------------------------------------------------------------------------------- |
| [Installation](docs/INSTALLATION.md)        | All install methods + platform support                                                        |
| [Engram Cloud](docs/engram-cloud/README.md) | Cloud landing page, quickstart path, branding, and reference links                            |
| [Agent Setup](docs/AGENT-SETUP.md)          | Per-agent configuration + compaction survival                                                 |
| [Codebase Guide](docs/CODEBASE-GUIDE.md)    | Definitive guide to repository structure, package ownership, flows, and maintainer guardrails |
| [Architecture](docs/ARCHITECTURE.md)        | How it works, session lifecycle, CLI reference, project structure                             |
| [Plugins](docs/PLUGINS.md)                  | OpenCode & Claude Code plugin details                                                         |
| [Team Usage](docs/TEAM-USAGE.md)            | Scope conventions, language strategy, and sync behavior for collaborative teams               |
| [Comparison](docs/COMPARISON.md)            | Why Engram vs claude-mem                                                                      |
| [Binary self-testing](docs/SELF-TESTING.md) | Isolated reliability and performance checks for released binaries                             |
| [Beta Testing](docs/BETA_TESTING.md)        | Isolated beta testing flows and cleanup guidance                                              |

---

## Database Schema

### Tables

- **sessions** — `id` (TEXT PK), `project`, `directory`, `started_at`, `ended_at`, `summary`, `status`
- **observations** — `id` (INTEGER PK AUTOINCREMENT), `session_id` (FK), `type`, `title`, `content`, `tool_name`, `project`, `scope`, `topic_key`, `normalized_hash`, `revision_count`, `duplicate_count`, `last_seen_at`, `created_at`, `updated_at`, `deleted_at`
- **observations_fts** — FTS5 virtual table synced via triggers (`title`, `content`, `tool_name`, `type`, `project`)
- **user_prompts** — frozen local Legacy prompt archive. Existing rows remain byte-stable and are available only through explicit `engram legacy-prompts` inventory/access/export/purge commands; they do not enter FTS, Recall, context, ordinary export/import, sync/cloud, or Obsidian. Purge removes canonical Engram-owned journal/FTS copies transactionally and fails closed if customized FTS ownership prevents complete removal.
- **capture_consents** — local project/content-type consent, optionally scoped to an expiring session, with retention between 1 and 30 days (7 by default)
- **diagnostic_captures** — short-lived, consented local Diagnostic Content. It has no FTS or sync triggers and is excluded from every ordinary Memory and replication surface.
- **sync_chunks** — `target_key` (TEXT), `chunk_id` (TEXT), `imported_at`; composite PK (`target_key`, `chunk_id`) for target-scoped chunk tracking
- **memory_relations** — stores conflict-surfacing verdicts from `mem_judge`; columns include `id` (INTEGER PK AUTOINCREMENT), `sync_id` (TEXT UNIQUE), `source_id`, `target_id`, `relation`, `judgment_status` (`pending` | `judged` | `orphaned` | `ignored`), `reason`, `evidence`, `confidence`, `marked_by_actor`, `marked_by_kind`, `marked_by_model`, `session_id`. The SQLite table does not store a `project` column; project is carried in relation sync payloads and derived from joined observations for project-scoped listing. Syncs across machines via local chunks and via cloud autosync when the project is enrolled.
- **sync_apply_deferred** — holds pulled mutations that could not be applied locally due to a missing FK dependency (e.g. relation references an observation not yet present); columns: `sync_id` (TEXT PK), `entity`, `payload`, `apply_status` (`deferred` | `applied` | `dead`), `retry_count`, `last_error`, `last_attempted_at`, `first_seen_at`. Rows with `apply_status='dead'` have exceeded the retry cap (5 attempts) and will not be retried automatically.
- **memory_checkpoints** — local-only root-turn dispositions keyed by unique `(host, session_id, root_turn_id)`. Stores only opaque identity, `disposition`, the versioned `reason_code`, `reason_version`, and timestamps. It has no Memory FTS or sync triggers and is excluded from Memory search, context, counts, JSON/project/chunk exports, pending mutations, cloud materialization, and Obsidian output. The v1 skip vocabulary contains only `no_durable_knowledge`; integration or processing failures are validation errors, never semantic skips.
- **memory_checkpoint_references** — ordered, typed local-only references from a checkpoint to the immutable ID, sync ID, and project identity of every attached Memory. The table has no sync triggers and is excluded from normal Memory and replication surfaces.
- **memory_proposals** — immutable local checkpoint audit evidence for `needs_review`. Stores only an Engram-derived ID, normalized project, redacted `title` and `content`, and creation time. It is separate from `observations`, has no FTS or sync triggers, and never enters Memory search, context, counts, export/import, sync, cloud, or Obsidian. No workflow converts a proposal into a Memory. Project rename, merge, and delete operations update or remove it together with its checkpoint reference.
- **memory_checkpoint_proposal_references** — one local proposal reference per `needs_review` checkpoint. Stores only checkpoint ID, proposal ID, and normalized project; it is excluded from all Memory and replication surfaces.
- **recall_runs / recall_results / recall_segments** — local-only, content-free operational identity for bounded Recall. Runs retain normalized project/scope authority; results bind opaque result IDs to a selected Memory revision through its semantic revision counter and local apply generation; segments retain only byte positions/limits/truncation. They store neither the query nor Memory content or content-derived hashes and have no FTS, export, sync, cloud, Obsidian, or Content-capture path.

The opt-in Recall baseline does not add a table to this Memory database. Its
versioned, local-only SQLite ledger is
`$ENGRAM_DATA_DIR/recall-baseline-v1.db`; it is excluded by construction from
Memory, FTS, Recall, context, ordinary export/import, sync, cloud, Obsidian,
and evaluation/publishing pipelines. See [Content-free Recall
baseline](docs/RECALL-BASELINE.md).

### SQLite Configuration

- WAL mode for concurrent reads
- Busy timeout 5000ms
- Synchronous NORMAL
- Foreign keys ON

---

## CLI memory contract

The CLI is a supported interface for humans, skills, and scripts. Memory
commands keep human-readable output by default and expose `--json` for clean
automation output. Successful JSON is written to stdout; structured errors use
`{"code","message","details?"}` on stderr with a non-zero exit code.

Core curated-memory operations:

```text
engram search <query> [--project P|--all-projects] [--match-mode all|any] [--json]
engram save <title> <content> [--project P] [--topic-key K] [--json]
engram save --title TITLE --content CONTENT [--project P] [--topic-key K] [--json]
engram get <id> [--json]  # explicit curation; complete Memory and relations
engram get --recall-id ID --result-id ID [--position BYTES]
           [--project P|--all-projects] [--scope project|personal|global] [--json]
engram update <id> [--title V] [--content V] [--type V] [--scope V]
                   [--topic-key V|--clear-topic-key] [--json]
engram review list [--project P|--all-projects] [--limit N] [--json]
engram review mark <id> [--json]
engram pin|unpin <id> [--json]
engram current-project [--json]
engram suggest-topic-key [--type V] [--title V|--content V] [--json]
engram conflicts judge <judgment-id> --relation R [--confidence N] [--json]
engram conflicts compare <id-a> <id-b> --relation R --confidence N --reasoning TEXT [--json]
engram projects merge --from SOURCE [--from SOURCE...] --to TARGET [--dry-run] [--yes] [--json]
engram checkpoint preflight --project PROJECT
                            --memory-json JSON [--memory-json JSON ...] [--json]
engram checkpoint record --host HOST --session-id ID --root-turn-id ID
                         --disposition skipped --reason no_durable_knowledge [--json]
engram checkpoint record --host HOST --session-id ID --root-turn-id ID
                         --disposition saved --project PROJECT
                         [--memory-id ID ...] [--memory-json JSON ...] [--json]
engram checkpoint record --host HOST --session-id ID --root-turn-id ID
                         --disposition needs_review --project PROJECT
                         [--memory-id ID ...] [--memory-json JSON ...]
                         --proposal-json '{"title":"...","content":"..."}' [--json]
engram checkpoint status --host HOST --session-id ID --root-turn-id ID [--json]
engram recall-baseline record|report|power|purge [options]
```

`engram current-project --json` separates discovery from Recall/write authority with
`project_strength` and `implicit_write_allowed`. Weak `git_root`, `git_child`,
and `dir_basename` results remain useful for generic read-only discovery, but
automatic candidate Recall returns no candidates and one warning, while an
implicit write fails with `weak_project_identity` before the store opens.
Supply `--project` (or a documented explicit project boundary) to authorize the
intended target.
`engram search --json` reports the same source, path, strength, and implicit-write
metadata while failing weak candidate Recall open. A weak CLI mutation rejection
always writes the structured error envelope to stderr, even without `--json`;
its `details` contain `project`, `project_source`, `project_path`,
`project_strength`, `implicit_write_allowed`, and the exact `safe_next_action`.
Ambiguous and otherwise failed detection retain `ambiguous_project` and
`project_detection_failed`; `weak_project_identity` is reserved for a resolved
fallback identity whose evidence is too weak to authorize the mutation.

Checkpoint identity values are opaque. If one begins with a hyphen, use the inline
forms `--host=VALUE`, `--session-id=VALUE`, or `--root-turn-id=VALUE` to avoid
ambiguity with CLI options.

Before finalization, repeat `--memory-json` with `checkpoint preflight` to inspect
prospective Memories without persisting anything. The result reuses exact
same-project duplicates and returns at most three full, same-project semantic
candidates across the request. Preflight creates no Memory, proposal,
checkpoint, relation, sync mutation, review state, or retired
candidate-evaluation state.

For `saved`, repeat `--memory-id` to attach Memories already saved during the
turn. Repeat `--memory-json` to create and attach Memories during finalization;
each JSON object accepts `title`, `content`, and the optional `type`, `tool_name`,
`scope`, and `topic_key` fields. Both forms may be combined. All Memories must
belong to `--project`. Creation of the session provenance, Memories, sync
mutations, references, and terminal checkpoint is one transaction.

For `needs_review`, provide exactly one `--proposal-json` object containing only
`title` and `content`, plus the enclosing `--project`. Optional `--memory-id` and
`--memory-json` values preserve independently settled same-project Memories in
the same result; a checkpoint with at least one Memory is Mixed Memory. Engram
redacts private blocks and derives the proposal ID, normalized project, and
creation timestamp inside the same transaction as any inline Memories, ordered
references, proposal reference, sync mutations for the Memories, and checkpoint.
Record, idempotent replay, and status return the immutable `proposal` snapshot
with `id`, `project`, `title`, `content`, and `created_at`. `--proposal-id` and
removed proposal fields are rejected. Proposal fields are also rejected for
`saved` and `skipped`. The proposal creates no Memory, sync mutation, review
workflow, or retired candidate-evaluation state.

`save` exits successfully after the memory is persisted even when its response
contains `judgment_required: true`; callers can resolve each returned candidate
with `conflicts judge`. Review timestamps and pins are local-only. `conflicts
compare` persists a verdict supplied by the caller and never invokes an LLM;
automatic semantic discovery remains under `conflicts scan --semantic`.

## HTTP API Endpoints

Engram exposes two different runtimes. Keep routes split by runtime:

- **Local runtime (`engram serve`, JSON on `127.0.0.1:7437`)**
  - `GET /health` (local service health)
  - includes memory CRUD/search/context endpoints documented below
  - includes `GET /sync/status` (local node sync status)
- **Cloud runtime (`engram cloud serve`)**
  - `GET /health` (cloud service health)
  - `GET /sync/pull`, `GET /sync/pull/{chunkID}`, `POST /sync/push`, `POST /sync/mutations/push`, `GET /sync/mutations/pull` (cloud sync transport)
  - `GET /dashboard/*` HTML routes (browser dashboard)

Dashboard route tree (`engram cloud serve`):

- Public
  - `GET /dashboard/health` — dashboard subsystem health
  - `GET /dashboard/login` — login surface (authenticated mode), redirects to `/dashboard/` when already authenticated
  - `POST /dashboard/login` — login submit (authenticated mode), redirect-only no-op in insecure mode
  - `POST /dashboard/logout` — clear session cookie and redirect to login
  - `GET /dashboard/static/*` — embedded CSS/JS assets
- Protected (requires dashboard session in authenticated mode; open in insecure mode)
  - `GET /dashboard` and `GET /dashboard/` — dashboard overview
  - `GET /dashboard/stats`
  - `GET /dashboard/activity`
  - `GET /dashboard/browser`
  - `GET /dashboard/browser/observations` (`HX-Request: true` returns fragment; plain GET returns full page)
  - `GET /dashboard/browser/sessions` (`HX-Request: true` returns fragment; plain GET returns full page)
  - `GET /dashboard/browser/sessions/{sessionID}`
  - `GET /dashboard/projects`
  - `GET /dashboard/projects/list` — HTMX partial; paginated project list with "Paused" badges
  - `GET /dashboard/projects/{project}`
  - `GET /dashboard/projects/{name}/observations` — HTMX partial for project detail
  - `GET /dashboard/projects/{name}/sessions` — HTMX partial for project detail
  - `GET /dashboard/contributors`
  - `GET /dashboard/contributors/list` — HTMX partial; paginated contributor list
  - `GET /dashboard/contributors/{contributor}`
  - `GET /dashboard/admin` (also requires admin token/session)
  - `GET /dashboard/admin/projects`
  - `GET /dashboard/admin/users` (admin-gated)
  - `GET /dashboard/admin/users/list` (admin-gated; HTMX partial)
  - `GET /dashboard/admin/health` (admin-gated)
  - `POST /dashboard/admin/projects/{name}/sync` (admin-gated; toggle sync enabled/disabled)
  - `GET /dashboard/admin/projects/{name}/sync/form` (admin-gated; HTMX partial)
  - `GET /dashboard/admin/audit-log` (admin-gated)
  - `GET /dashboard/admin/audit-log/list` (admin-gated; HTMX partial)
  - `GET /dashboard/sessions/{project}/{sessionID}` — session detail with observations only
  - `GET /dashboard/observations/{project}/{sessionID}/{syncID}` — observation detail
- Retired Legacy prompt routes (`/dashboard/browser/prompts`, project prompt lists, and prompt detail) return `410 Gone` without querying historical prompt content.

Engram is local-first: local SQLite is authoritative; cloud features are optional replication/shared access and enrollment controls.

### Health

- Local runtime (`engram serve`): `GET /health` — Returns `{"status": "ok", "service": "engram", "version": "0.1.0"}`
- Cloud runtime (`engram cloud serve`): `GET /health` — Returns `{"status": "ok", "service": "engram-cloud"}`

### Sessions

- `POST /sessions` — Create session. Body: `{id, project, directory}`
- `POST /sessions/{id}/end` — End session. Body: `{summary}`
- `GET /sessions/recent` — Recent sessions. Query: `?project=X&limit=N`
- `GET /sessions/{id}` — Get single session by ID
- `DELETE /sessions/{id}` — Delete session
  - `200` when deleted
  - `404` when session does not exist
  - `409` when session still has observations (delete/migrate observations first)
  - For cloud-enrolled projects: returns `200` and additionally enqueues a `session/delete` mutation that propagates the deletion to cloud replicas

### Terminal Memory Checkpoints

- `POST /checkpoints/preflight` — Inspect prospective Memories without writes. Body: `{project, memories}`
- `POST /checkpoints` — Atomically record one root-turn checkpoint. Body: `{host, session_id, root_turn_id, disposition, reason_code?, project?, memory_ids?, memories?, proposal?}`
  - `201` with `{checkpoint, idempotency: "created"}` for a new commit
  - `200` with `{checkpoint, idempotency: "already_recorded"}` for an exact replay
  - `400` for malformed input or invalid identity, disposition, reason, or references
  - `409` when the identity already owns a different terminal result
- `GET /checkpoints/status` — Inspect one exact checkpoint. Query: `?host=X&session_id=Y&root_turn_id=Z`
  - `404` when no checkpoint exists
- Checkpoint errors use `{code, message, details}` so HTTP adapters preserve the same stable domain code exposed by CLI and MCP.

### Observations

- `POST /observations` — Add observation. Body: `{session_id, type, title, content, tool_name?, project?, scope?, topic_key?}`
  - `400` when `title` or `content` is missing, empty, or whitespace-only. The observation-create paths (`engram save`, `mem_save`, `POST /observations`) enforce the same title rule because cloud sync rejects observation upserts without a title, and one rejected mutation blocks every later mutation for the project
- `GET /observations` — Recent observations compatibility endpoint. Query: `?project=X&scope=project|personal|global&limit=N&sort=created_at:desc`
- `GET /observations/recent` — Recent observations. Query: `?project=X&scope=project|personal|global&limit=N`
- `GET /observations/{id}` — Get single observation by ID
- `PATCH /observations/{id}` — Update fields. Body: `{title?, content?, type?, project?, scope?, topic_key?}`
  - `400` when `title` or `content` is provided but empty or whitespace-only. Omitting a field leaves its current value unchanged
- `DELETE /observations/{id}` — Delete observation (`?hard=true` for hard delete, soft delete by default)
  - `200` when deleted
  - `404` when observation does not exist

### Review

- `GET /review` — List observations due for local review. Query: `?project=X&limit=N`
- `POST /review/mark_reviewed` — Reset one observation's local review cycle. Body: `{observation_id}`; legacy `{id}` is accepted.
  - `200` with the refreshed observation payload when marked reviewed
  - `400` when `observation_id`/`id` is missing or the JSON body is invalid
  - `404` when the observation does not exist
  - Local-only: updating `review_after` does not enqueue a sync mutation or propagate to other machines.

### Search

- `GET /search` — FTS5 search. Query: `?q=QUERY&type=TYPE&project=PROJECT&scope=SCOPE&limit=N`
  - `200` with a JSON array of search results
  - No-result example: `GET /search?q=definitely-no-hit` returns `200` with `[]` (never `null`)
- `GET /recall` — Authority-aware bounded candidate Recall used by thin host adapters. Query: `?q=QUERY&type=TYPE&project=PROJECT&project_strength=strong|explicit|weak|aggregate&scope=SCOPE&limit=N&match_mode=all|any&all_projects=BOOL`
  - defaults to five candidates, always enforces a 4 KiB candidate budget, and allows at most ten candidates for deliberate follow-up
  - returns `200` for candidates, an empty result, weak authority, or operational fail-open; warnings and diagnostics remain structured
  - returns `400` for malformed query, match mode, limit, authority strength, or conflicting `project`/`all_projects`
- `GET /recall/content` — Deliberately retrieve one selected Recall result. Required query: `recall_id`, `result_id`. Optional query: `position` (UTF-8 byte position, default `0`), `project`, `scope`, `all_projects`, `project_strength`. A success or fail-open response is `200`; syntactically invalid requests are `400`. Content is capped at 16 KiB and a truncated response exposes `continuation_position` for a new explicit request.

### Timeline

- `GET /timeline` — Chronological context. Query: `?observation_id=N&before=5&after=5`

### Prompts

- `POST /prompts` — Offer prompt content to the Core-owned local Diagnostic capture gate. Body: `{session_id, content, project?}`. Capture is disabled by default and a successful request does not make prompts part of Memory.
- `GET /prompts/recent`, `GET /prompts/search`, and `DELETE /prompts/{id}` are retired Legacy routes and return `410 Gone`. Use the explicit `engram legacy-prompts` CLI for inventory, access, export, or separately confirmed purge.

### Context

- `GET /context` — Manual formatted Memory context scoped by project and optional scope. Query: `?project=X&scope=project|personal|global`. Diagnostic and Legacy prompts are excluded.
- `GET /context/compaction` — Runtime compaction Memory context scoped strictly to one persisted session. Query: `?session_id=X`. The server derives the session project; this endpoint does not accept project or scope selection. Diagnostic and Legacy prompts are excluded.

### Passive Capture

- `POST /observations/passive` — Explicitly extract structured learnings from non-subagent text. Body: `{content, session_id, project?, source?}`. Core rejects every `source` identified as subagent lifecycle output so legacy hooks cannot bypass the typed Diagnostic boundary.

### Export / Import

- `GET /export` — Export all data as JSON
  - Optional `?project=<name>` for project-scoped export
  - `400` when `project` is provided but blank/whitespace
- `POST /import` — Import data from JSON. Body: ExportData JSON

### Stats / Diagnostics

- `GET /stats` — Memory statistics. Diagnostic and Legacy prompt rows are excluded from counts.
- `GET /doctor` — Read-only operational diagnostics. Query: `?project=X&check=CHECK_CODE`
  - Returns the same diagnostic report envelope as `engram doctor --json` and MCP `mem_doctor`
  - `project` and `check` are optional; omitted `project` uses current project detection
  - Unknown explicit projects return `404` with `{error, code:"unknown_project", available_projects:[...]}`

### Project Detection / Migration

- `GET /project/current` — Detect the current project. Query: `?cwd=/path/to/repo`
  - Always returns a success envelope with `{project, project_source, project_path, project_strength, implicit_write_allowed, cwd, available_projects}` plus optional `warning`, `error_hint`, or `safe_next_action`
- `POST /projects/migrate` — Rename a project across local records with `{old_project, new_project}`. It follows the server's optional Bearer authentication policy.
- `POST /projects/rescue-ownership` — Bulk-assign ownership to explicitly selected historical sessions or observations that carry none. The JSON body is limited to 8 KiB: `{target_project, confirmed:true, observation_ids?:[], session_ids?:[], prompt_ids?:[]}`.
  - A configured `ENGRAM_HTTP_TOKEN`, matching `Authorization: Bearer <token>`, `target_project`, `confirmed:true`, and at least one positive observation/prompt ID or non-blank session ID are required. Missing server token returns `503`; missing or wrong credentials return `401`; malformed or invalid requests return `400`.
  - This route is a convenience, not the only repair. `engram projects rescue-ownership --project <name> [--session <id>] [--observation <id>]` performs the same operation against the local store and needs no server token, so ownership stays repairable in a zero-config install. Historical `prompt_ids` remain accepted only to return `legacy_prompt_frozen`; ordinary ownership repair never reclassifies Legacy prompt rows.
  - `200` returns `{status, complete, blocked, target_project, rescued_observations, rescued_sessions, rescued_prompts, conflicting_records, skipped_records, journaled_local, reconciliation_status}`. `rescued_prompts` remains zero for compatibility. Owned records are never reassigned.
  - `status` is `rescued` when `complete` is `true` and everything selected now belongs to `target_project`, or `partially_rescued` when something was left behind. `blocked` then names each item exactly — `{kind, id, reason, owned_by}` with `reason` including `legacy_prompt_frozen` for prompt selections — so a partial outcome is never inferred from counters.
- The whole plan is resolved before anything is written: which sessions and which records will move is decided first, then applied. An unowned session that already parents a record owned by a different project is therefore left in place rather than moved out from under it, in either direction. A blank project is treated exactly like `NULL` — neither identifies an owner — and no sync mutation is ever journaled for a blank-owned record.
- `journaled_local` means a canonical pending local mutation exists after the call, whether inserted by the call or already pending. A local journal is not a cloud acknowledgement; autosync reports subsequent reconciliation state.

### Conflict Audit (admin — local runtime only)

These endpoints are served by `engram serve` on the local runtime only. They are not exposed on the cloud runtime. All routes are additive — no existing routes changed.

#### GET /conflicts

List `memory_relations` rows with optional filters.

Query params: `project` (string), `status` (string — raw `judgment_status`, currently `pending` | `judged` | `orphaned` | `ignored`), `since` (RFC3339), `limit` (int, default 50, max 500 — silently clamped), `offset` (int, default 0).

Response:

```json
{
  "total": 80,
  "limit": 50,
  "offset": 0,
  "relations": [
    {
      "id": 42,
      "sync_id": "rel-abc123",
      "relation": "conflicts_with",
      "judgment_status": "pending",
      "source_id": "obs-source123",
      "source_title": "Original architecture decision",
      "target_id": "obs-target456",
      "target_title": "Updated architecture decision",
      "created_at": "2026-01-15 12:00:00",
      "updated_at": "2026-01-15 12:30:00"
    }
  ]
}
```

#### POST /conflicts/judge

Record a verdict on an existing pending relation surfaced by memory conflict detection.

Body:

```json
{
  "judgment_id": "rel-abc123",
  "relation": "related|compatible|scoped|conflicts_with|supersedes|not_conflict",
  "reason": "optional explanation",
  "evidence": "optional JSON or text evidence",
  "confidence": 0.9,
  "session_id": "optional-session-id"
}
```

Response:

```json
{ "relation": { "sync_id": "rel-abc123", "judgment_status": "judged" } }
```

Status codes:

- `200` when judged
- `400` for invalid JSON, missing required fields, unknown relation, or invalid relation state

#### POST /conflicts/compare

Persist an agent-supplied semantic verdict for two observation IDs.

Body:

```json
{
  "memory_id_a": 5,
  "memory_id_b": 6,
  "relation": "related|compatible|scoped|conflicts_with|supersedes|not_conflict",
  "confidence": 0.99,
  "reasoning": "brief explanation",
  "model": "optional-model-id"
}
```

Response:

```json
{ "sync_id": "rel-abc123" }
```

`not_conflict` is a no-op verdict and returns an empty `sync_id`.

Status codes:

- `200` when accepted
- `400` for invalid JSON, missing required fields, invalid relation, invalid confidence, or cross-project pairs
- `404` when either observation ID does not exist

#### GET /conflicts/{relation_id}

Get full detail for one relation row, including source and target observation snippets.

- `200` with full relation + `source_snippet` + `target_snippet`
- `404` with a JSON `error` containing the not-found message when `relation_id` does not exist
- `400` with JSON error body when `relation_id` is not a valid integer

#### GET /conflicts/stats

Aggregate counts for the project (or global when `project` query param is omitted).

Response:

```json
{
  "project": "my-project",
  "by_relation": {
    "conflicts_with": 3,
    "supersedes": 1
  },
  "by_judgment_status": {
    "pending": 3,
    "judged": 1
  },
  "deferred": 4,
  "dead": 1
}
```

#### POST /conflicts/scan

Run conflict candidate scan for a project. Synchronous.

Request body:

```json
{
  "project": "my-project",
  "limit": 100,
  "apply": false,
  "max_insert": 100,
  "semantic": false,
  "concurrency": 5,
  "timeout_per_call_seconds": 60,
  "max_semantic": 100
}
```

- `limit` — observations per page (default and maximum 100); rows are ordered by observation ID
- `cursor` — optional `next_cursor` from a completed previous page; omit to start the first page
- `apply: false` (default) — dry-run for the non-semantic lexical scan; reports candidates without inserting pending rows
- `apply: true` — non-semantic lexical scan inserts new pending relation rows up to `max_insert` cap (default 100)
- `semantic: true` — after FTS5 lexical scan, run LLM-judge semantic detection on the candidate pairs returned by `FindCandidates`. It does not discover totally lexically unrelated pairs on its own. Requires `ENGRAM_AGENT_CLI` to be set on the server to `claude` or `opencode`.
- Semantic scans can persist non-`not_conflict` judged relations through `JudgeBySemantic` even when `apply: false`; `not_conflict` verdicts are not inserted.
- `concurrency` — worker pool size for parallel LLM calls when `semantic: true` (default 5, range 1–20)
- `timeout_per_call_seconds` — per-LLM-call timeout in seconds when `semantic: true` (default 60, range 1–600)
- `max_semantic` — hard cap on LLM calls per scan (default 100); scan stops collecting new pairs once reached
- Missing `project` field returns `400`
- With `semantic: true`, `concurrency` outside [1, 20] or `timeout_per_call_seconds` outside [1, 600] returns `400`

Response:

```json
{
  "project": "my-project",
  "inspected": 100,
  "ranked_queries": 100,
  "candidates_found": 5,
  "next_cursor": 520,
  "already_related": 2,
  "inserted": 0,
  "capped": false,
  "dry_run": true,
  "semantic_judged": 0,
  "semantic_skipped": 0,
  "semantic_errors": 0
}
```

`semantic_judged`, `semantic_skipped`, and `semantic_errors` are always present (zero when `semantic: false`). `next_cursor` is present only after every candidate for the completed page has been handled. Scans never auto-loop through pages.

When any scan cap is reached, including `max_insert` for lexical apply scans or `max_semantic` for semantic scans, no `next_cursor` is returned. Re-run from the same incoming cursor with a higher cap; the response includes this warning:

```json
{
  "project": "my-project",
  "inspected": 100,
  "candidates_found": 150,
  "already_related": 0,
  "inserted": 50,
  "capped": true,
  "dry_run": false,
  "semantic_judged": 0,
  "semantic_skipped": 0,
  "semantic_errors": 0,
  "warning": "cap reached: this page has no continuation; rerun from the same cursor with a higher applicable cap"
}
```

#### GET /conflicts/deferred

List rows from `sync_apply_deferred`. Query params: `status` (string — `deferred` | `dead` | `applied`), `limit` (int, default 50, max 500), `offset` (int, default 0; accepted for pagination but not echoed in the response envelope).

Response:

```json
{
  "total": 3,
  "limit": 50,
  "rows": [
    {
      "sync_id": "rel-abc123",
      "entity": "relation",
      "payload": {
        "sync_id": "rel-abc123",
        "source_id": "obs-source123",
        "target_id": "obs-target456",
        "relation": "conflicts_with",
        "judgment_status": "pending",
        "project": "my-project",
        "created_at": "2026-01-15 12:00:00",
        "updated_at": "2026-01-15 12:00:00"
      },
      "payload_raw": "{\"sync_id\":\"rel-abc123\",\"source_id\":\"obs-source123\",\"target_id\":\"obs-target456\",\"relation\":\"conflicts_with\",\"judgment_status\":\"pending\",\"project\":\"my-project\",\"created_at\":\"2026-01-15 12:00:00\",\"updated_at\":\"2026-01-15 12:00:00\"}",
      "payload_valid": true,
      "apply_status": "deferred",
      "retry_count": 2,
      "last_error": "source FK not found",
      "last_attempted_at": "2026-01-15 12:05:00",
      "first_seen_at": "2026-01-15 12:00:00"
    }
  ]
}
```

#### POST /conflicts/deferred/replay

Call `ReplayDeferred()` synchronously. Returns counts of rows processed.

Response:

```json
{
  "retried": 4,
  "succeeded": 3,
  "failed": 0,
  "dead": 1
}
```

### Sync Status (local runtime only)

- `GET /sync/status` — Runtime sync-state status for the local node (`engram serve` only).
- In `engram serve`, sync status is wired to persisted SQLite sync state (project-scoped for detected/current project).
- Response fields when provider is injected:
  - `enabled`
  - `phase`
  - `last_error`
  - `consecutive_failures`
  - `backoff_until`
  - `last_sync_at`
  - `reason_code`
  - `reason_message`
  - `deferred_count` — number of pulled mutations awaiting retry (FK dependency not yet local)
  - `dead_count` — number of pulled mutations that exhausted retries (5 failures) and will not be retried
  - `upgrade` (nested object)
    - `stage`
    - `reason_code`
    - `reason_message`
- `enabled` semantics:
  - `true` when cloud runtime is configured for the resolved + enrolled project, or when meaningful persisted sync state exists for that resolved project while runtime is not configured.
  - `false` when no explicit project scope resolves, cloud runtime is malformed/missing, or enrollment/status checks fail.
- Generic/embedded local server usage may return the fallback `enabled=false` response if no provider is injected.

### Environment Variables

| Variable                        | Description                                                                                                                                                                                                                                               | Default              |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| `ENGRAM_DATA_DIR`               | Override data directory                                                                                                                                                                                                                                   | `~/.engram`          |
| `ENGRAM_RECALL_BASELINE`        | Set to `1` to enable content-free local collection for the current Codex Recall baseline. Unset leaves all automatic observers disabled and creates no baseline state.                                                                                     | (unset — disabled)   |
| `ENGRAM_RECALL_BASELINE_RETENTION_DAYS` | Integer retention window from 1 through 30 days for the separate Recall-baseline ledger.                                                                                                                                                           | `7`                  |
| `ENGRAM_PORT`                   | Override HTTP server port                                                                                                                                                                                                                                 | `7437`               |
| `ENGRAM_PROJECT`                | Process-level default project override, applied by every entry point through one precedence rule: **explicit request project** (`engram save --project`, an MCP tool `project` argument) → **process override** (`engram mcp --project`, then `ENGRAM_PROJECT`) → **cwd detection**. For `engram save`: owns the observation and its `manual-save-<project>` session when `--project` is omitted. For `engram serve`: used as the fallback when `GET /sync/status` receives no `project` query param. For `engram mcp`: sets `MCPConfig.DefaultProject`, which takes precedence over cwd detection for all read and write tools (including `mem_update`) for the lifetime of that MCP process. When unset, cwd detection is used as the fallback. | cwd-detected project |
| `ENGRAM_HTTP_TOKEN`             | Optional Bearer auth for the local HTTP server. When set, destructive session/observation routes, `GET /export`, `POST /import`, and `POST /projects/migrate` require `Authorization: Bearer <token>`; retired Legacy prompt routes remain auth-gated before returning `410 Gone`. `POST /projects/rescue-ownership` always requires a configured token and matching Bearer credential. Comparison is constant-time. Token is read at request time (no restart needed). Other routes remain open when unset (zero-config default). Ownership repair never depends on this token: `engram projects rescue-ownership` performs the same repair against the local store. | (unset — HTTP rescue route not served; CLI repair still available) |
| `ENGRAM_TIMEZONE`               | Timezone for timestamp display in the TUI and cloud dashboard. Accepts any IANA zone name (e.g. `America/New_York`, `Europe/Berlin`). Falls back to system local time when unset or invalid.                                                               | system local         |
| `ENGRAM_AGENT_CLI`              | LLM runner name used by `engram conflicts scan --semantic` and the HTTP `/conflicts/scan` endpoint. Accepted values: `claude`, `opencode`.                                                                                                                | (unset)              |
| `ENGRAM_CLOUD_AUTOSYNC`         | Set to `1` to enable background autosync. Requires `ENGRAM_CLOUD_TOKEN` and `ENGRAM_CLOUD_SERVER` to also be set.                                                                                                                                         | (unset — disabled)   |
| `ENGRAM_CLOUD_SERVER`           | Cloud server URL used by the autosync manager and `engram sync --cloud`.                                                                                                                                                                                  | (unset)              |
| `ENGRAM_DATABASE_URL`           | Postgres DSN for `engram cloud serve`.                                                                                                                                                                                                                    | (unset)              |
| `ENGRAM_CLOUD_HOST`             | Bind host for `engram cloud serve`.                                                                                                                                                                                                                       | `127.0.0.1`          |
| `ENGRAM_CLOUD_MAX_PUSH_BYTES`   | Max cloud push payload bytes.                                                                                                                                                                                                                             | `8388608`            |
| `ENGRAM_CLOUD_TOKEN`            | Bearer token required in authenticated `engram cloud serve` mode.                                                                                                                                                                                         | (unset)              |
| `ENGRAM_CLOUD_INSECURE_NO_AUTH` | Set to `1` for local insecure cloud serve (no auth). Cannot be combined with `ENGRAM_CLOUD_TOKEN`.                                                                                                                                                        | (unset)              |
| `ENGRAM_CLOUD_ALLOWED_PROJECTS` | Comma-separated project allowlist enforced by `engram cloud serve`. Required in both token-auth and insecure modes. Use `*` to allow all projects (dev/internal deploys) — bypasses per-project name enforcement while still requiring a non-empty project on each request. | (unset) |
| `ENGRAM_JWT_SECRET`             | Required in authenticated cloud serve mode. Must be explicitly set to a non-default value.                                                                                                                                                                | (unset)              |
| `ENGRAM_CLOUD_ADMIN`            | Optional admin-only dashboard token in authenticated cloud serve mode. Ignored/rejected in insecure mode.                                                                                                                                                 | (unset)              |
| `ENGRAM_CLOUD_TOKEN_PEPPER`     | Dedicated secret used to hash managed cloud tokens. Required both to issue tokens via `engram cloud bootstrap admin --issue-token` (and the admin API/dashboard) and to enable managed-token authentication on `engram cloud serve`. Distinct from `ENGRAM_JWT_SECRET` on purpose — see [Managed users, tokens, and CLI bootstrap](#managed-users-tokens-and-cli-bootstrap). | (unset)              |

### Conflict Audit CLI (admin)

The `engram conflicts` sub-command provides admin/maintainer access to the conflict layer. It is NOT for end users — end users interact with conflicts via the normal agent conversation flow.

When `--project` is omitted, the cwd-detected project is used.

```
engram conflicts list [--project <name>] [--status <pending|judged|orphaned|ignored>] [--since <RFC3339>] [--limit <N>]
```

List `memory_relations` rows. Output: label-colon aligned columns (`id`, `sync_id`, `relation`, `judgment_status`, `source`, `target`, `created_at`).

```
engram conflicts show <relation_id>
```

Show full detail for one relation: relation_id, sync_id, relation, judgment_status, created_at, updated_at, source_id, source_title, target_id, target_title. Exits non-zero when relation_id does not exist.

```
engram conflicts stats [--project <name>]
```

Print aggregate grouped `judgment_status` counts (`pending` | `judged` | `orphaned` | `ignored`) plus deferred and dead queue sizes. When relation counts exist, also prints `By relation type` counts.

```
engram conflicts scan [--project <name>] [--dry-run] [--apply] [--max-insert <N>]
                      [--since <RFC3339>] [--limit <N>] [--cursor <ID>]
                      [--semantic] [--concurrency <N>] [--timeout-per-call <N>]
                      [--max-semantic <N>] [--yes]
```

Walk observations for the project, run FindCandidates, and report or insert new pending relation rows.

- `--dry-run` (default): for non-semantic lexical scans, reports candidates found with 0 pending rows inserted.
- `--apply`: inserts up to `--max-insert` (default 100) new rows; prints WARNING when cap is reached.
- `--since RFC3339`: scan only observations created at or after the timestamp.
- `--limit N`: inspect 1–100 observations per page (default 100), ordered by observation ID.
- `--cursor ID`: resume after a printed `next_cursor`; no automatic follow-up page is run.
- `--semantic`: enable LLM-judge semantic detection on FTS5 candidate pairs returned by `FindCandidates`. It can improve verdict quality for candidates that share lexical terms, but it does not discover totally lexically unrelated pairs on its own. Requires `ENGRAM_AGENT_CLI=claude` or `ENGRAM_AGENT_CLI=opencode`.
- With `--semantic`, non-`not_conflict` verdicts are persisted by `JudgeBySemantic` even in the default `--dry-run` mode; `not_conflict` verdicts remain no-op.
- `--concurrency N`: worker pool size for parallel LLM calls (default 5, max 20).
- `--timeout-per-call N`: per-LLM-call timeout in seconds (default 60).
- `--max-semantic N`: hard cap on LLM calls per scan run (default 100).
- `--yes`: skip the cost-estimate confirmation prompt before LLM calls.

```
engram conflicts deferred [--status <deferred|dead|applied>] [--limit <N>] [--inspect <sync_id>] [--replay]
engram conflicts deferred --recover <sync_id> [--json]
```

Inspect or replay the `sync_apply_deferred` queue.

- Default: list rows with sync_id, apply_status, retry_count, first_seen_at.
- `--inspect <sync_id>`: print full decoded payload for one row; exits non-zero when not found.
- `--replay`: call `ReplayDeferred()` and print retried/succeeded/failed/dead counts.
- `--recover <sync_id>`: immediately revalidate and apply one `dead` relation through the canonical local store path. Recovery preserves remote provenance and never creates an outbound sync mutation. Success atomically retains an `applied` tombstone, so repeating the command is a successful `already_recovered` no-op. A later pulled mutation with the same sync ID starts a new queue episode.
- `--recover` is distinct from `--replay`: replay processes automatic `deferred` retries and deletes rows after successful application; recover targets exactly one `dead` row and retains its applied receipt. It requires no confirmation and may be combined only with `--json`.

### Cloud CLI (opt-in)

- `engram cloud status` — show current cloud config state plus auth/sync readiness without mutating local state. When cloud is configured, also probes the local `engram serve` daemon at `127.0.0.1:7437` (respects `ENGRAM_PORT`) and prints a `Local daemon:` line (`running` / `not running` / `unreachable`) so you can detect a silently dead autosync. Exit code is unaffected; the line is informational
- `engram cloud enroll <project>` — enroll one project for cloud replication
- `engram cloud config --server <url>` — persist an absolute HTTP(S) cloud
  server URL to `~/.engram/cloud.json`; URL userinfo, query delimiters, query
  parameters, and fragments are rejected so credentials cannot be embedded in
  the displayed endpoint
- `engram cloud serve` — run cloud backend API + dashboard (`/dashboard`) using Postgres config from env
- `engram cloud upgrade doctor --project <project>` — deterministic read-only readiness diagnosis (`ready|blocked`, class/reason)
- `engram cloud upgrade repair --project <project> [--dry-run|--apply]` — deterministic local-safe repair planner/apply (no remote mutation)
- `engram cloud upgrade bootstrap --project <project> [--resume]` — resumable checkpointed enroll/push/verify flow
- `engram cloud upgrade status --project <project>` — show upgrade stage/class/reason
- `engram cloud upgrade rollback --project <project>` — restore pre-upgrade local snapshot before `bootstrap_verified`; blocked afterwards
- `engram cloud repair materialize-mutations --project <project> (--dry-run|--apply)` — explicit server-side Postgres repair that backfills existing `cloud_mutations` into compatible `cloud_chunks` without deleting remote data
- `engram cloud bootstrap admin --username <name> [--email <email>] [--grant-project <project>]... [--issue-token [name]]` — create the first managed admin (see [Managed users, tokens, and CLI bootstrap](#managed-users-tokens-and-cli-bootstrap))
- `engram cloud bootstrap recover-token [--name <name>]` — recover the one stranded managed admin token state described below

Cloud client authentication is resolved from `ENGRAM_CLOUD_TOKEN` first, then from
the token already present in the local `cloud.json`; `engram cloud config
--server <url>` changes only the server URL and preserves any saved token. The
cloud serve process itself still reads its server-side bearer credential from
its runtime environment.
Cloud server startup fails closed when the token is missing unless `ENGRAM_CLOUD_INSECURE_NO_AUTH=1` is explicitly set for local insecure development.
`ENGRAM_CLOUD_INSECURE_NO_AUTH=1` cannot be combined with `ENGRAM_CLOUD_TOKEN`.
Cloud server always requires `ENGRAM_CLOUD_ALLOWED_PROJECTS` (comma-separated), including insecure mode, so project scope remains server-enforced.
`ENGRAM_CLOUD_TOKEN` + `ENGRAM_CLOUD_ALLOWED_PROJECTS` are server-side requirements for authenticated mode and must be configured before `engram cloud serve` (or compose startup).
Authenticated mode also requires an explicit non-default `ENGRAM_JWT_SECRET`; implicit development defaults are rejected.
Dashboard requests support browser login in authenticated mode: use `/dashboard/login` to exchange the bearer token for an HttpOnly dashboard cookie scoped to `/dashboard`. Protected `/dashboard/*` HTML routes require that cookie and do **not** treat raw `Authorization: Bearer ...` headers as an authenticated browser session. Sync API routes (`/sync/pull`, `/sync/pull/{chunkID}`, `/sync/push`, `/sync/mutations/push`, `/sync/mutations/pull`) remain header-auth only. In insecure mode (`ENGRAM_CLOUD_INSECURE_NO_AUTH=1` + no `ENGRAM_CLOUD_TOKEN`), dashboard auth is bypassed and `/dashboard/login` redirects to `/dashboard/`.

`ENGRAM_CLOUD_ADMIN` is optional in authenticated mode; when set, `/dashboard/admin` is allowed only for sessions established with that exact token.
`ENGRAM_CLOUD_ADMIN` is rejected in insecure mode (`ENGRAM_CLOUD_INSECURE_NO_AUTH=1`) to avoid an incoherent admin/browser auth path.

Cloud runtime bind host is controlled by `ENGRAM_CLOUD_HOST`:

- default: `127.0.0.1` (local-only, safer default)
- container/compose: set `ENGRAM_CLOUD_HOST=0.0.0.0` so published host ports can reach the cloud server

Cloud runtime envs for `engram cloud serve`:

| Variable                        | Required                 | Notes                                                                                 |
| ------------------------------- | ------------------------ | ------------------------------------------------------------------------------------- |
| `ENGRAM_DATABASE_URL`           | yes                      | Postgres DSN for cloud chunk storage/dashboard read model                             |
| `ENGRAM_PORT`                   | no                       | Runtime port (default `8080`)                                                         |
| `ENGRAM_CLOUD_HOST`             | no                       | Bind host (default `127.0.0.1`; use `0.0.0.0` for containers)                         |
| `ENGRAM_CLOUD_MAX_PUSH_BYTES`   | no                       | Max chunk/mutation push request body bytes (default `8388608`)                        |
| `ENGRAM_CLOUD_ALLOWED_PROJECTS` | yes                      | Comma-separated allowlist; always required (authenticated + insecure modes). Use `*` to allow all projects (dev/internal deploys) — bypasses per-project name enforcement while still requiring a non-empty project on each request. |
| `ENGRAM_CLOUD_TOKEN`            | yes (authenticated mode) | Enables bearer auth mode                                                              |
| `ENGRAM_JWT_SECRET`             | yes (authenticated mode) | Must be explicitly set and non-default when token mode is enabled                     |
| `ENGRAM_CLOUD_INSECURE_NO_AUTH` | no                       | Set to `1` only for local insecure mode; cannot be combined with `ENGRAM_CLOUD_TOKEN` |
| `ENGRAM_CLOUD_ADMIN`            | no                       | Optional admin dashboard token in authenticated mode; rejected in insecure mode       |
| `ENGRAM_CLOUD_TOKEN_PEPPER`     | no (required to enable managed-token auth) | Dedicated managed-token hashing secret. Must differ from `ENGRAM_JWT_SECRET`. Required both by `engram cloud bootstrap admin --issue-token` and by `engram cloud serve` to accept managed tokens at runtime (see below). |

### Managed users, tokens, and CLI bootstrap

`engram cloud bootstrap admin` creates the first **managed admin** — a principal record stored in the cloud Postgres database (`cloud_principals` / `cloud_human_users`), independent from the legacy `ENGRAM_CLOUD_TOKEN` / `ENGRAM_CLOUD_ADMIN` env-token model:

```bash
# Create the first managed admin (safe: refuses to create a duplicate first admin)
engram cloud bootstrap admin --username alice

# Also grant project access and issue a sync token in the same command
engram cloud bootstrap admin --username alice \
  --grant-project my-project \
  --issue-token first-token
```

- `--username` is required; `--email` is optional.
- `--grant-project <project>` may be repeated to grant one or more projects (managed principals are deny-by-default: no grants means no sync access).
- `--issue-token [name]` issues a managed bearer token and prints the **raw token exactly once** in the command output. It is never logged, persisted, or re-printed — store it immediately. Issuing a token requires `ENGRAM_CLOUD_TOKEN_PEPPER` to be set to a dedicated secret (distinct from `ENGRAM_JWT_SECRET`); the command fails clearly, before creating anything, if the pepper is missing.
- Running the command again once a managed admin already exists is rejected (no silent duplicate first-admin creation); the attempt is still recorded as a denied `bootstrap.cli` audit event.
- Every bootstrap attempt (accepted or denied) writes a `bootstrap.cli` audit event to `cloud_auth_audit_log`, with the same non-secret metadata rules (no raw tokens, hashes, or bearer headers) as every other cloud auth audit event.
- Grant/role/duplicate-admin validation reuses the exact same `cloudstore` methods and last-admin guard used by the dashboard's own first-admin bootstrap flow — there is no parallel/looser bootstrap path.

If a historical failed bootstrap left exactly one enabled managed human admin, retained its grants, and created no principal token anywhere in the deployment, run the explicit recovery command:

```bash
engram cloud bootstrap recover-token --name replacement
```

It requires `ENGRAM_CLOUD_TOKEN_PEPPER`, preserves existing grants, and prints the recovered raw token exactly once only after the token and its `bootstrap.cli` recovery audit event commit together. It refuses all other states, including multiple enabled managed human admins or any existing principal token; it does not create users, grants, or partial tokens.

**Runtime authentication:** `engram cloud serve` resolves managed tokens first, then falls back to the legacy env-token credentials (`ENGRAM_CLOUD_TOKEN` for sync, `ENGRAM_CLOUD_ADMIN` for dashboard bootstrap/admin), on every `/sync/*`, `/admin/*`, and dashboard-login request:

- Set `ENGRAM_CLOUD_TOKEN_PEPPER` to enable managed-token authentication. A token issued by `engram cloud bootstrap admin --issue-token` (or by the dashboard/`/admin/*` token-create routes) then authenticates directly against `/sync/*` and `/admin/*`, and can log into the dashboard as its resolved principal/role.
- If `ENGRAM_CLOUD_TOKEN_PEPPER` is not set, managed-token authentication is simply disabled: the server still starts normally, and `ENGRAM_CLOUD_TOKEN` / `ENGRAM_CLOUD_ADMIN` continue to authenticate exactly as before (legacy-only mode).
- Managed principals are deny-by-default for project sync: a managed token only reaches projects explicitly granted via `--grant-project` (or the dashboard/`/admin/*` grant routes). Legacy `ENGRAM_CLOUD_TOKEN` keeps its existing `ENGRAM_CLOUD_ALLOWED_PROJECTS` allowlist behavior, unaffected by managed grants.
- Disabled managed users, revoked managed tokens, and revoked project grants stop authenticating/authorizing on the very next request — no server restart required.
- No rollback action is required to keep using legacy credentials: legacy `ENGRAM_CLOUD_TOKEN` / `ENGRAM_CLOUD_ADMIN` behavior is unchanged and remains fully supported whether or not `ENGRAM_CLOUD_TOKEN_PEPPER` is configured.

Cloud sync is still local-first and explicit:

```bash
# Explicit cloud sync call
engram sync --cloud --project my-project

# Optional env toggle for cloud mode in sync command
ENGRAM_CLOUD_SYNC=1 engram sync --status --project my-project
```

When `engram sync --cloud --project <project>` or autosync hits a known repairable cloud sync/upsert/canonicalization failure, Engram preserves the original error and appends guidance to run:

### Cloud Upgrade Flow

```bash
engram cloud upgrade doctor --project <project>
engram cloud upgrade repair --project <project> --dry-run
engram cloud upgrade repair --project <project> --apply
engram sync --cloud --project <project>
```

Sync/autosync never auto-applies repairs; only the explicit `repair --apply` command mutates local repairable upgrade state.

For cloud servers that already accepted mutation pushes before mutation payloads were materialized into chunk history, run the server-side backfill against the Postgres DSN used by `engram cloud serve`:

```bash
ENGRAM_DATABASE_URL='postgres://...' engram cloud repair materialize-mutations --project <project> --dry-run
ENGRAM_DATABASE_URL='postgres://...' engram cloud repair materialize-mutations --project <project> --apply
```

The backfill is project-scoped, non-destructive, and idempotent: it inserts missing compatible chunks and leaves existing `cloud_mutations` and chunks in place.

`engram cloud serve` also runs this materialization repair automatically for every configured `ENGRAM_CLOUD_ALLOWED_PROJECTS` entry at startup. The explicit repair command remains available for operator verification, dry-runs, and re-running a project after an upgrade.

### Local Cloud Bring-Up (Docker + Postgres)

```bash
# 1) SERVER-SIDE startup requirements (configure before startup)
# docker-compose.cloud.yml includes defaults for browser-demo smoke usage:
# ENGRAM_CLOUD_INSECURE_NO_AUTH=1
# ENGRAM_CLOUD_ALLOWED_PROJECTS=smoke-project
docker compose -f docker-compose.cloud.yml up -d

# source-run flow (without compose): set BOTH token + allowlist before startup
# ENGRAM_DATABASE_URL="postgres://engram:engram_dev@127.0.0.1:5433/engram_cloud?sslmode=disable" \
# ENGRAM_JWT_SECRET="replace-with-32+-byte-random-secret" \
# ENGRAM_CLOUD_TOKEN="your-token" \
# ENGRAM_CLOUD_ALLOWED_PROJECTS="my-project" \
# engram cloud serve

# 2) CLIENT-SIDE CLI setup
# compose runtime flow: published :18080
engram cloud config --server http://127.0.0.1:18080
# compose runtime default is insecure local-dev mode; keep token unset
# client sync preflight only requires the configured cloud server URL; no
# client-side ENGRAM_CLOUD_INSECURE_NO_AUTH flag is required for compose flow
unset ENGRAM_CLOUD_TOKEN

# 3) Enroll project + run explicit cloud sync
engram cloud enroll smoke-project
engram cloud upgrade doctor --project smoke-project
engram cloud upgrade repair --project smoke-project --dry-run
engram cloud upgrade repair --project smoke-project --apply
engram cloud upgrade bootstrap --project smoke-project --resume
engram cloud upgrade status --project smoke-project
engram sync --cloud --status --project smoke-project

# source-run client endpoint (without compose): default :8080
# engram cloud config --server http://127.0.0.1:8080

# cloud mode enforces a single explicit project scope
# engram sync --cloud --all  # blocked by design
```

Deterministic reason codes shared across store/CLI/server:

- `blocked_unenrolled`
- `auth_required`
- `cloud_config_error`
- `policy_forbidden`
- `paused`
- `transport_failed`

### Cloud Status Visibility Matrix

Cloud failure visibility must stay deterministic across supported surfaces:

| Scenario                                                                                               | Expected deterministic reason        | Surfaces                    |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------ | --------------------------- |
| Unconfigured cloud sync preflight (missing server URL)                                                 | `cloud_config_error`                 | CLI stderr                  |
| Cloud runtime not configured in status provider (takes precedence even if project scope is unresolved) | `cloud_not_configured`               | `/sync/status`              |
| `/sync/status` project cannot be resolved (no query/default project) while cloud runtime is configured | `project_required`                   | `/sync/status`              |
| Unenrolled project cloud sync                                                                          | `blocked_unenrolled`                 | CLI stderr + `/sync/status` |
| Runtime auth/policy failure from remote API                                                            | `auth_required` / `policy_forbidden` | CLI stderr + `/sync/status` |
| Explicit paused state                                                                                  | `paused`                             | `/sync/status`              |
| Remote/network failure                                                                                 | `transport_failed`                   | CLI stderr + `/sync/status` |

`engram sync --cloud --status --project <name>` is read-only: it does **not** mutate `/sync/status` lifecycle fields.

Machine-actionable validation/policy failures from cloud sync routes include:

- `error_class` (`repairable` | `blocked` | `policy` | `invalid_request`)
- `error_code` (stable deterministic code)
- `error` (human-readable message)

This envelope is used consistently by `/sync/push` validation/control failures and by `/sync/pull` / `/sync/pull/{chunkID}` project-required or policy failures. `/sync/mutations/push` uses the envelope for empty batches, empty projects, project policy failures, and pause-control failures; relation-payload validation currently returns `error`, `reason_code`, and `invalid` instead. `/sync/mutations/pull` success responses include the project envelope, but internal listing errors currently use plain `http.Error`.

---

## MCP Project Resolution

Engram resolves the project at MCP tool call time. The default source is the **server process working directory** (cwd), not MCP startup state, but some write tools have stronger context: `mem_session_start(directory=...)` resolves from the provided directory, and `mem_save` may use a validated explicit `project` or an existing `session_id` project before falling back to cwd detection. The explicit field is treated as a **validated selection**, not a free-form creation hint. This eliminates project drift caused by agents supplying different names for the same repo.

### Detection algorithm

| Case | Condition                                                                                 | Source            | Strength     | Implicit write | Project                            |
| ---- | ----------------------------------------------------------------------------------------- | ----------------- | ------------ | -------------- | ---------------------------------- |
| 1    | nearest `.engram/config.json` exists within the enclosing git root, or at cwd outside git | `config`          | `strong`     | allowed        | `project_name` from config         |
| 2    | cwd is a git root with `origin` remote                                                    | `git_remote`      | `strong`     | allowed        | repo name from remote URL          |
| 3    | cwd is inside a git repo (subdirectory)                                                   | `git_root`        | `weak`       | rejected       | git root's directory basename      |
| 4    | cwd has exactly one git-repo child                                                        | `git_child`       | `weak`       | rejected       | child repo name (warning included) |
| 5    | cwd has multiple git-repo children                                                        | `ambiguous` error | `unresolved` | rejected       | —                                  |
| 6    | no git repo near cwd                                                                      | `dir_basename`    | `weak`       | rejected       | basename of cwd                    |

CLI explicit scope, `ENGRAM_PROJECT` environment scope, explicit override,
validated ambiguity selection, request-owned project, process override, and
existing session-owned project sources have `explicit`
strength and continue through their existing validation boundaries. The
`all_projects` and `personal_scope` sources are `aggregate`. Unknown future sources fail closed for
implicit writes.

Child scan constraints: depth=1, max 20 entries, 200ms timeout, skips hidden dirs and noise dirs (`node_modules`, `vendor`, `.venv`, `__pycache__`, `target`, `dist`, `build`, `.idea`, `.vscode`).

### Response envelope

Most successful MCP tool responses use this envelope:

```json
{
  "project": "engram",
  "project_source": "git_remote",
  "project_path": "/home/user/engram",
  "project_strength": "strong",
  "implicit_write_allowed": true,
  "result": "...(tool output)..."
}
```

Error responses include `available_projects` when the error is `ambiguous_project` or `unknown_project`.

Exceptions:

- `mem_current_project` returns detection fields directly (`project`, `project_source`, `project_path`, `project_strength`, `implicit_write_allowed`, `cwd`, `available_projects`, optional `warning`, `error_hint`, or `safe_next_action`) and does not wrap them in `result`.
- `mem_doctor` returns the same JSON report shape as `engram doctor --json`; it uses read-project resolution before running diagnostics but does not wrap the report in the common MCP envelope.
- `mem_checkpoint` uses `operation: "preflight"` with an explicit project and prospective Memories for its read-only mode; record mode and `mem_checkpoint_status` use opaque host/session/root-turn identity instead of automatic project resolution. `saved` and `needs_review` writes require an explicit `project`; `skipped` and status do not. Their JSON success and error envelopes match the corresponding CLI commands; tool errors set MCP `isError=true`.

### Write tools (explicit/session/cwd project resolution)

`mem_session_start` honors the MCP process override (`--project` or `ENGRAM_PROJECT`), then resolves from its explicit `directory` argument when supplied, otherwise from cwd. `mem_session_end` uses the project already owned by the persisted session, while `mem_capture_passive` auto-detects from cwd; any `project` argument the LLM sends to them is ignored. `mem_session_summary` supports explicit project override (`project`, `project_choice_reason`, `recovery_token`) matching `mem_save`'s project resolution.

`mem_update` uses ID-based updates and auto-detects project only for response envelope metadata. Its public schema does not expose `project`; raw legacy clients may still send a non-empty `project` argument, and the handler tolerates it as an observation project update for compatibility.

`mem_save` resolves writes by precedence: validated explicit `project`, project already associated with `session_id`, then strong repo/cwd detection (`config` or `git_remote`). Weak repo-root, child-repo, and directory-basename detection is retained for discovery but cannot select the write target.

Guardrails:

- Invalid explicit `project` names fail loudly instead of silently falling back.
- Weak implicit identity fails with `weak_project_identity` and the exact safe next action `provide an explicit project name and retry the write` before any session, observation, prompt, proposal, checkpoint, or sync mutation is created.
- Valid-looking explicit `project` names are accepted only when backed by known context: an existing local project in the store, a matching existing session project, the nearest resolvable `.engram/config.json`, or exact ambiguous-project recovery after the user selected one available project.
- An unbacked explicit `project` fails loudly and does not create a new bucket.
- If a non-empty `session_id` is supplied and no session exists, `mem_save` fails with a structured error and does not write.
- If both explicit `project` and `session_id` are supplied, they must resolve to the same normalized project or `mem_save` fails with a structured error and does not write.
- `project_choice_reason=user_selected_after_ambiguous_project` is only honored when cwd resolution is actually ambiguous. On a non-ambiguous cwd, stale recovery flags do not override explicit-project precedence or session mismatch validation.
- If ambiguous-project recovery is active, `project` must exactly match one of the previously returned `available_projects`; invented or normalized guesses are rejected.
- Exact ambiguous-project choices can still fail with `project_name_collision` when multiple available names collapse to the same stored project bucket after normalization. Rename or disambiguate the colliding projects before retrying.
- Ordinary explicit `mem_save(project=...)` calls can also fail with `project_name_collision` when the raw explicit name collapses into an existing config-backed, session-backed, or store-backed project bucket, such as `foo--bar` colliding with `foo-bar`.

For monorepos, detection now honors the **nearest** `.engram/config.json` at or below the enclosing git root. That lets `repo/backend/.engram/config.json` and `repo/frontend/.engram/config.json` behave as independent projects without letting `~/.engram/config.json` leak into nested workspaces.

`mem_save_prompt` is a Diagnostic capture request, not a Memory write. Capture is
disabled by default and persistence requires explicit local consent for the
resolved project and `prompt` content type. It keeps the older cwd/default
resolution behavior and only uses `project` for the narrow ambiguous-project
recovery override: after a previous `ambiguous_project` error, the agent may
retry with `project=<one of available_projects>` and
`project_choice_reason=user_selected_after_ambiguous_project`.

### Read tools (optional project override)

`mem_context`, `mem_timeline`, `mem_stats`, and `mem_doctor` validate an optional
`project` against the store and return a structured unknown-project error.
`mem_search` instead treats an explicit project as Recall authority and returns
a successful empty candidate set when that project has no Memory.
`mem_get_observation` defaults to an opaque `recall_id`/`result_id` selection,
plus optional `position`, `project`, `scope`, and `all_projects` values that must
preserve the original Recall boundary. Core revalidates the current Memory
revision and authority before returning content. Explicit curation may instead
pass the legacy numeric `id`; it cannot be mixed with an opaque selection.

### Admin tools

`mem_delete` is ID-based and requires `id`; optional `hard_delete=true` permanently deletes the observation. It does not accept or auto-detect `project`.

`mem_merge_projects` requires `from` (comma-separated source project names) and `to` (canonical target project name). It does not accept or auto-detect `project`.

### mem_current_project

Use `mem_current_project` as the first call in a session to inspect the detection result:

```json
{
  "project": "engram",
  "project_source": "git_remote",
  "project_path": "/home/user/engram",
  "project_strength": "strong",
  "implicit_write_allowed": true,
  "cwd": "/home/user/engram",
  "available_projects": [],
  "warning": ""
}
```

Returns success for weak and ambiguous discovery. A weak non-empty project is
read-only evidence; `project_strength: "weak"` and
`implicit_write_allowed: false` signal the caller to request an explicit project
before writing. An empty `project` plus non-empty `available_projects` signals an
explicit ambiguity choice.

---

## MCP Tools (24 tools)

The full registry contains 24 tools. `--tools=agent` selects only the five-tool
default listed in [Default and specialized MCP profiles](#default-and-specialized-mcp-profiles).
Select `curation`, `lifecycle`, `admin`, `all`, or explicit tool names for the
specialized operations documented below.

### mem_checkpoint

Preflight prospective Memories without writes, or record the terminal Memory
checkpoint for one settled root user turn.

- `operation: "preflight"` requires an explicit `project` and one or more inline
  `memories`. It accepts no terminal identity or disposition fields. The result
  contains exact duplicate references and at most three full same-project
  semantic candidates across the request.
- Record mode is the default; `host`, `session_id`, `root_turn_id`, and
  `disposition` are required for a first finalization.

- `disposition: "skipped"` requires `reason: "no_durable_knowledge"` and accepts no Memory references.
- `disposition: "saved"` requires an explicit `project` plus at least one existing `memory_ids` entry or inline `memories` object. The two arrays may be combined. Each inline Memory accepts required `title` and `content`, plus optional `type`, `tool_name`, `scope`, and `topic_key`.
- `disposition: "needs_review"` requires an explicit `project` plus exactly one inline `proposal` object containing only `title` and `content`; zero or more settled `memory_ids` and inline `memories` may be attached.

A saved result exposes an ordered `references` array containing `kind: "memory"`, `memory_id`, `memory_sync_id`, and `project`. Every referenced Memory must exist, remain active, and belong to the same normalized project. Inline Memories, their sync mutations, all references, and the checkpoint commit atomically.

A needs-review result exposes ordered Memory references plus one immutable
`proposal` snapshot containing `id`, `project`, `title`, `content`, and
`created_at`. Engram derives the identity, normalized ownership, and timestamp
while inline Memories, their sync mutations, all references, proposal creation,
and the checkpoint commit atomically. A needs-review checkpoint with at least
one Memory is Mixed Memory. The proposal remains local-only audit evidence and
no review or retired candidate-evaluation workflow runs implicitly.

The first call returns `idempotency: "created"`; replaying the same root-turn identity and disposition returns `idempotency: "already_recorded"` with the original checkpoint, references, proposal snapshot, and timestamps without creating Memories, proposals, or mutations again. Once the identity and disposition match, replay payload fields are ignored rather than revalidated, so retries cannot replace the original references or depend on payload availability. Invalid or empty sets on first finalization fail without changing state. Stable reference-validation codes are `invalid_checkpoint_references`, `checkpoint_memory_not_found`, and `checkpoint_project_mismatch`; terminal changes return `checkpoint_conflict`. Unknown skip reasons, including integration and processing failure labels, return `invalid_checkpoint_reason`.

### mem_checkpoint_status

Inspect one exact checkpoint by `host`, `session_id`, and `root_turn_id`. Missing identity returns `invalid_checkpoint_identity`; an unknown but valid identity returns `checkpoint_not_found`. This operation reads the local checkpoint ledger directly and never queries Memory search or context.

### mem_search

Recall the smallest useful candidate set only when prior decisions, tracked
work, release state, configuration, preferences, or known failures can
materially change the task. Routine self-contained work needs no search.

Automatic project Recall requires strong or explicit identity. Start with one
narrow project search using one to three anchors. The default/initial request
returns at most five candidates and 4 KiB; the same lookup intent may be
reformulated at most once when relevant Memory is reasonably expected. A
deliberate follow-up may request 6 through 10 candidates, but never widens scope
or bypasses the byte budget.

`scope` defaults to `project`. Personal scope or `all_projects: true` is a
deliberate broad request and requires explicit task relevance or user direction.
`project` and `all_projects` are mutually exclusive. An explicit project with no
stored Memory returns an empty success.

Candidates contain bounded summaries rather than full Memory content. Core
returns only active, in-scope, non-deleted, non-superseded Memories. Semantic
relevance/currentness rank first, pins move results only within the same tier,
and recency breaks remaining ties. Pending relations and judged
`conflicts_with` relations appear symmetrically in each candidate's structured
`conflicts`; use `mem_get_observation` only for a selected candidate.

Every success envelope exposes `recall_id`, legacy numeric `result_ids`, opaque
`opaque_result_ids`, `result_count`, `delivered_utf8_bytes`,
`elapsed_monotonic_ms`, and Protocol/binary `provenance` outside prose. Select
bounded content with the candidate's `result_id` or the corresponding
`opaque_result_ids` entry. Empty Recall is successful. Store or transport failure
returns no candidates, one `recall_unavailable` warning, and structured
diagnostics without blocking the caller's task.

### mem_save

Save structured observations. The tool description teaches agents the format:

- **title**: Short, searchable (e.g. "JWT auth middleware")
- **type**: `decision` | `architecture` | `bugfix` | `pattern` | `config` | `discovery` | `learning`
- **scope**: `project` (default) | `personal` | `global` — see [Team Usage](docs/TEAM-USAGE.md) for conventions and sync caveats
- **topic_key**: optional canonical topic id (e.g. `architecture/auth-model`) used to upsert evolving memories
- **capture_prompt**: optional boolean, default `false`. When explicitly true, current process-local prompt context is offered to the Core consent gate; no Diagnostic write occurs without an active local project/content-type grant. The Memory save remains independent if context is unavailable or capture is denied.
- **content**: Structured with `**What**`, `**Why**`, `**Where**`, `**Learned**`; required unless the legacy `observation` alias is provided
- **observation**: backward-compatible alias for `content` for older/raw MCP clients; prefer `content` for new integrations

Exact duplicate saves are deduplicated in a rolling time window using a normalized content hash + project + scope + type + title + tool_name + normalized topic_key. Empty optional identity fields compare as empty values.
When `topic_key` is provided, `mem_save` upserts the latest observation in the same `project + scope + topic_key`, incrementing `revision_count` and attributing it to the latest writer session.
Save responses include lifecycle metadata for the saved observation: computed `state` (`active` or `needs_review`) and `review_after` when the observation type has a review cycle. Content is redacted before the configured storage limit is applied; that limit and truncation metadata (`original_bytes`, `limit_bytes`) are UTF-8 bytes. MCP save/update responses include `truncated`, and warn when truncation occurs.

### mem_update

Update an observation by ID. Public schema supports partial updates for `title`, `content`, `type`, `scope`, and `topic_key`. For legacy/raw MCP clients, a non-empty `project` argument is still tolerated by the handler even though it is not exposed in the schema.

### mem_review

Review observation lifecycle state. Available in the `curation` profile
(`engram mcp --tools=curation`).

Actions:

- `action: "list"` — returns observations whose `review_after` has passed. Optional parameters: `project` and `limit` (default 10).
- `action: "mark_reviewed"` — requires `observation_id`; resets that observation's local review cycle using its type decay policy. The legacy `id` alias is accepted for compatibility.

`mark_reviewed` is local-only for now: `review_after` is intentionally not part of sync payloads in this phase, so resetting the review cycle does not enqueue a sync mutation or propagate to other machines.

### mem_pin

Pin an observation by ID so it appears before recent observations in
`mem_context`. The tool is available in the `curation` profile.

Pins are local to the current device. They are not included in sync payloads and do not propagate to other machines.

### mem_unpin

Remove a local pin by observation ID so the memory returns to normal recency
order in `mem_context`. The tool is available in the `curation` profile.

Like `mem_pin`, unpinning is local-only and is not synced.

### mem_suggest_topic_key

Suggest a stable `topic_key` from `type + title` (or content fallback). Uses family heuristics like `architecture/*`, `bug/*`, `decision/*`, etc. Use before `mem_save` when you want evolving topics to upsert into a single observation.

### mem_delete

Delete an observation by ID. Uses soft-delete by default (`deleted_at`); optional hard-delete for permanent removal.

### mem_save_prompt

Offer a user prompt to the Core-owned local Diagnostic capture gate. Capture is
disabled by default and persistence requires explicit consent scoped to the
project and `prompt` content type. The operation applies post-redaction byte
limits and reports whether content was captured; it never makes a prompt part
of Memory, Recall, context, or Memory statistics.

When persistence is denied, the same MCP process retains the prompt as
short-lived current-prompt activity for a later explicit
`mem_save(capture_prompt=true)` retry. That later request passes through the
same consent gate; `mem_save` succeeds independently when context is unavailable
or capture is denied. A successful `mem_save_prompt` is not cached for a second
capture, and a different MCP process does not inherit the ephemeral activity.

### mem_context

Get recent Memory context from previous sessions — shows sessions and
observations with optional scope filtering. Diagnostic and Legacy prompts are
excluded.

Scope values accepted by the `scope` parameter: `project` (default), `personal`, `global`. When `scope: personal` is passed without an explicit `project` override, the project filter is cleared and personal observations are returned across all projects (cross-project personal scope).

### mem_stats

Show Memory system statistics — sessions, observations, and projects.
Diagnostic and Legacy prompt rows are excluded.

### mem_timeline

Progressive disclosure: after searching, drill into chronological context around a specific observation. Shows N observations before and after within the same session.

### mem_get_observation

Deliberately retrieve one selected `mem_search` result by its `recall_id` and
opaque `result_id`. Each response contains at most 16 KiB of valid UTF-8 Memory
content and reports `original_bytes`, `delivered_utf8_bytes`, `limit_bytes`, and
`truncated`. If truncated, call again with exactly `continuation_position`; no
content is paged automatically, and project/scope authority cannot widen.

Explicit curation workflows may instead pass a numeric observation `id` to
retrieve the legacy complete Memory view and metadata. `id` cannot be combined
with `recall_id` or `result_id`; default agent Recall should use the bounded
opaque-selection path.

### mem_session_summary

Optionally save a curated Session summary when a user or specialized workflow
requests one. It is not an agent-lifecycle completion signal and does not
replace the root turn's terminal Memory checkpoint:

```
## Goal
## Instructions
## Discoveries
## Accomplished
## Next Steps
## Relevant Files
```

### mem_session_start

Register the start of a new coding session.

### mem_session_end

Mark a session as completed with optional summary.

### mem_capture_passive

Extract structured learnings from text output. Looks for `## Key Learnings:` sections and saves each numbered/bulleted item as a separate observation. Duplicates are automatically skipped.

### mem_merge_projects

**Admin tool.** Merge multiple project name variants into a single canonical name. Requires `from` as a comma-separated list of source project names and `to` as the target canonical name. Observations, sessions, and local review state are reassigned; the frozen Legacy prompt archive is never reclassified.

### mem_current_project

Detect the current project from the working directory. Returns `project`, `project_source`, `project_path`, `project_strength`, `implicit_write_allowed`, `cwd`, `available_projects`, and optional recovery metadata. Never returns an error — weak results remain readable and ambiguous cwd returns an empty `project` with non-empty `available_projects`. Recommended as the first call when starting a session.

### mem_doctor

Run read-only operational diagnostics. Returns the same JSON report shape as `engram doctor --json`, with optional `project` and `check` filters. The optional `project` override is validated with read-project resolution before diagnostics run.

### mem_judge

Record a verdict on a pending memory conflict. When `mem_save` returns `candidates[]` and `judgment_required: true`, the agent inspects the candidates and calls `mem_judge` to mark the relation between the saved memory and a candidate.

Parameters:

- **judgment_id** (required): the `judgment_id` returned by `mem_save`
- **relation** (required): `related` | `compatible` | `scoped` | `conflicts_with` | `supersedes` | `not_conflict`
- **reason** (optional): short text explaining the verdict
- **evidence** (optional): free-form text or JSON the agent can use to justify the call (e.g., quoted excerpts from both memories)
- **confidence** (optional, default 1.0): 0.0–1.0; if the value is below 0.7 the agent SHOULD ask the user before calling

Re-judging an existing relation overwrites it (deliberate revision). Two agents judging the same pair persist as separate rows — Phase 1 surfaces both; cross-actor reconciliation is Phase 2.

Recall excludes judged superseded targets and exposes pending or judged
`conflicts_with` relations as structured, symmetric candidate warnings. For
enrolled projects with autosync enabled, judgments propagate through the cloud
mutation pipeline and affect Recall after the relevant mutations are pulled.

### mem_compare

Records a verdict on a semantic comparison between two memories. The agent reads both memories, judges the relationship using its LLM reasoning, and calls `mem_compare` to persist the verdict. Unlike `mem_judge` (which resolves a pre-existing `pending` candidate surfaced by `mem_save`), `mem_compare` creates a new relation row directly — useful for proactive semantic analysis that goes beyond FTS5 lexical matching.

Available in the `curation` profile (`engram mcp --tools=curation`).

Parameters:

- **memory_id_a** (required): int — observation ID of the first memory
- **memory_id_b** (required): int — observation ID of the second memory
- **relation** (required): string — one of `conflicts_with` | `supersedes` | `scoped` | `related` | `compatible` | `not_conflict`
- **confidence** (required): float 0.0..1.0
- **reasoning** (required): string — explanation of the verdict (max 200 chars)
- **model** (optional): string — model name for provenance (e.g. `"claude-haiku-4-5"`)

Behavior:

- Persists a relation row via `JudgeBySemantic` with system provenance (`marked_by_kind="system"`, `marked_by_actor="engram"`)
- Idempotent: the same `(source_id, target_id)` pair updates the existing row rather than inserting a duplicate
- `not_conflict` verdicts are no-ops — acknowledged but not persisted, matching the scan flow contract
- Cross-project relations are rejected with an error

---

## Memory Protocol

The canonical [`engram-memory`](plugin/codex/skills/memory/SKILL.md) skill is the
single source of truth for the root-turn disposition rubric, Recall, compaction,
and terminal finalization. Agent-specific setup projects that skill or its short
activation cue; adapters do not maintain another policy copy.

Normal work ends in one **Terminal Memory commit** after the root user turn and
all causal work settle:

- `saved` atomically commits one or more existing or inline Memories;
- `needs_review` atomically commits zero or more settled Memories plus exactly
  one bounded, redacted proposal; with at least one Memory the result is Mixed;
- `skipped(no_durable_knowledge)` records that the settled turn produced no
  durable result.

Before choosing the terminal disposition for prospective Memories, the agent
runs bounded read-only preflight, reuses exact duplicates, and accounts for at
most three full same-project semantic candidates. Clear low-risk outcomes can
settle directly; ambiguity or a material architecture, policy, or decision
conflict selects `needs_review`.

Selective Recall starts only when prior history can change the task. Its first
project request is bounded to five candidates/4 KiB with one possible
reformulation; limits 6-10 are deliberate follow-up, and personal or
cross-project scope requires explicit relevance. Empty or unavailable Recall
does not block the task.

Independent save is reserved for explicit curation or a long-running,
material loss-risk handoff. `mem_session_summary` is an optional curation
operation. Neither operation replaces the terminal checkpoint.

### Default and specialized MCP profiles

The `agent` profile exposes exactly five tools:

- `mem_current_project`
- `mem_search`
- `mem_get_observation`
- `mem_checkpoint`
- `mem_checkpoint_status`

Use `curation` for independent authoring, optional Session summaries, context,
review, relations, diagnosis, and pins. Use `lifecycle` for host session events,
consent-gated Diagnostic capture requests, and passive Content capture. Use `admin` for destructive and
operational maintenance. `all` and explicit tool-name selection preserve
compatibility for deliberate broad integrations.

A **Memory operation** reads or changes durable Memory or checkpoint state. An
**agent lifecycle operation** reports host activity or captures Content. A
lifecycle operation never selects or implies a Memory disposition.

---

## Project Name Normalization

Engram automatically prevents project name drift — the same project saved under different names (`"engram"` vs `"Engram"` vs `"  ENGRAM  "`) by different clients or users.

### Automatic normalization

All project names are normalized on write and read: **lowercase**, **trimmed**, **collapsed hyphens/underscores**. Hyphens and underscores are not interchangeable, so `"engram-memory"` and `"engram_memory"` are not equivalent. If a name is changed during normalization, a warning is included in the response.

### Auto-detection

MCP tools resolve project names at call time using the shared detection chain:

1. Nearest `.engram/config.json` `project_name` within the enclosing git root, or at cwd outside git
2. Git remote origin URL (extracts repo name)
3. Git repository root directory name
4. Single git-repo child of cwd
5. Multiple git-repo children of cwd returns `ambiguous_project` with `available_projects`
6. Current working directory basename

`engram mcp` accepts a process-level default project via `--project <name>` / `--project=<name>` or `ENGRAM_PROJECT=<name>`. This override takes precedence over cwd detection for all read and write tools — `mem_update` included — throughout the lifetime of that MCP process. It is a trusted startup-time value — use it when the host cannot supply a reliable cwd (VS Code, WSL, CI, Docker).

The same precedence rule is applied by every entry point, so identity never depends on which binary wrote the record: an **explicit request project** (`engram save --project`, an MCP tool `project` argument) wins first, then the **process override** (`engram mcp --project`, then `ENGRAM_PROJECT`), then **cwd detection**.

### Ownership on legacy sessions

A database upgraded from the schema where `sessions.project` was nullable still holds sessions that identify no project. Those sessions keep accepting writes: ownership is established forward rather than demanded retroactively.

- When the write resolves a project through the chain above, its unowned parent session **adopts** that project in the same transaction, and the move is journaled like any other ownership change. The record and its session end up in agreement, so no record is left split from its parent.
- Adoption is refused in one case: an unowned session that already parents a record owned by a *different* project. Claiming it there would split that record from its session, so the write fails with `project_ownership_ambiguous` (HTTP `409`) and the operator resolves it explicitly.
- A write that resolves no project at all against an unowned session fails with `project_ownership_required` (HTTP `409`).

Both errors carry a `remedy` field naming the exact command to run: `engram projects rescue-ownership --project <name> --session <id>`. That command reaches the local store directly and needs no server token, so recovery stays available in a zero-config install. Bulk repair remains available over HTTP through `POST /projects/rescue-ownership` when `ENGRAM_HTTP_TOKEN` is configured.

### Similar-project warnings

When saving to a project that doesn't exist yet, Engram checks for similar existing project names (Levenshtein distance, substring, case-insensitive matching) and warns the agent if a likely variant already exists.

### Retroactive cleanup

Use `engram projects consolidate --project <name>` to interactively merge legacy project names that are equivalent after normalization. In a strong config- or remote-backed repo, `--project` may be omitted. Use `mem_merge_projects` for agent-driven consolidation.

Use `engram projects rescue-ownership --project <name> [--session <id>] [--observation <id>]` to assign ownership to legacy session or observation rows that carry none. It prints what moved and, when anything was left behind, exactly which items and why. The historical `--prompt` input reports `legacy_prompt_frozen` and never reclassifies the archive. The command works against the local store, so it needs no running server and no `ENGRAM_HTTP_TOKEN`.

---

## Features

### Full-Text Search (FTS5)

- Searches across title, content, tool_name, type, and project
- Query sanitization: wraps each word in quotes to avoid FTS5 syntax errors
- Supports type and project filters

### Timeline (Progressive Disclosure)

Three-layer pattern for token-efficient memory retrieval:

1. `mem_search` — Find relevant observations
2. `mem_timeline` — Drill into chronological neighborhood of a result
3. `mem_get_observation` — Get at most 16 KiB for one selected result; continue explicitly by byte position

### Privacy Tags

`<private>...</private>` content is stripped at TWO levels:

1. **Plugin layer** (TypeScript) — Strips before data leaves the process
2. **Store layer** (Go) — `stripPrivateTags()` runs inside Memory and Diagnostic capture write boundaries

Example: `Set up API with <private>sk-abc123</private>` becomes `Set up API with [REDACTED]`

### Diagnostic Capture and Legacy Prompts

Prompt and subagent-output capture are separate local-only Diagnostic
facilities, independently disabled by default and gated by explicit
project/content-type consent. Subagent capture accepts only a bounded JSON
object with `kind="engram_diagnostic"`, non-empty `title` and `learning`, and
an optional `evidence_ref`; it rejects raw transcript/last-message fallback,
unknown fields, invalid UTF-8, and oversized values. Diagnostic capture has no
FTS and never feeds Recall, context, Memory statistics, ordinary export/import,
sync, cloud, or Obsidian. It cannot create Memory, proposals, checkpoints,
Session summaries, or retired evaluation/promotion state. Historical
`user_prompts` rows remain a frozen Legacy archive available only through
explicit `engram legacy-prompts` operations.

### Export / Import

Share memories across machines, backup, or migrate:

- `engram export` — JSON dump of ordinary sessions and observations; Diagnostic and Legacy prompts are excluded
- `engram import <file>` — Load from JSON, sessions use INSERT OR IGNORE (skip duplicates), atomic transaction

### Git Sync (Chunked)

Share memories through git repositories using compressed chunks with a manifest index.

- `engram sync` — Exports new memories as a gzipped JSONL chunk to `.engram/chunks/`
- `engram sync --all` — Exports ALL memories from every project
- `engram sync --import` — Imports chunks listed in the manifest that haven't been imported yet
- `engram sync --status` — Shows how many chunks exist locally vs remotely (filesystem mode)
- `engram sync --cloud --status --project <name>` — Shows local, remote, and pending chunk counts for the specified cloud project
- `engram sync --project NAME` — Filters export to a specific project

```
.engram/
├── manifest.json          <- index of all chunks (small, git-mergeable)
├── chunks/
│   ├── a3f8c1d2.jsonl.gz <- chunk 1 (gzipped JSONL)
│   ├── b7d2e4f1.jsonl.gz <- chunk 2
│   └── ...
└── engram.db              <- local working DB (gitignored)
```

**Why chunks?**

- Each `engram sync` creates a NEW chunk — old chunks are never modified
- No merge conflicts: each dev creates independent chunks, git just adds files
- Chunks are content-hashed (SHA-256 prefix) — each chunk is imported only once
- The manifest is the only file git diffs — it's small and append-only
- Compressed: a chunk with 8 sessions + 10 observations = ~2KB

### Agent-Driven Compression

Instead of a separate LLM service, the agent itself compresses observations. The agent already has the model, context, and API key.

**Two levels:**

- **Per-action** (`mem_save`): Structured summaries (What/Why/Where/Learned)
- **Optional Session summary** (`mem_session_summary`): Explicitly curated Goal/Instructions/Discoveries/Accomplished/Next Steps/Files context

### No Raw Tool-Call Auto-Capture

Engram does not record a firehose of raw tool calls. Raw tool calls (`edit: {file: "foo.go"}`, `bash: {command: "go build"}`) are noisy and pollute FTS5 search. The agent's curated summaries are higher signal, more searchable, and don't bloat the database. Shell history and git provide the raw audit trail.

`mem_save` never captures the current prompt by default. With `capture_prompt=true`, it may offer same-process prompt context to Core, but persistence still requires explicit local consent for that project and content type. Diagnostic capture remains separate from the curated Memory save, and the save still succeeds if context is missing or capture is denied.

Subagent drafts and evidence are likewise transient by default. Independent
`subagent_output` consent permits only the bounded Diagnostic envelope; it does
not grant the subagent Memory authority. The root agent remains the sole actor
that may attach settled Memory or one unresolved proposal to the terminal
checkpoint.

---

## Terminal UI (TUI)

Interactive Bubbletea-based terminal UI. Launch with `engram tui`.

### Screens

| Screen                  | Description                                                       |
| ----------------------- | ----------------------------------------------------------------- |
| **Dashboard**           | Stats overview (sessions, observations, projects) + menu          |
| **Search**              | FTS5 text search with text input                                  |
| **Search Results**      | Browsable results list from search                                |
| **Recent Observations** | Browse all observations, newest first                             |
| **Review memories**     | Verify memories due for local review in the current project      |
| **Observation Detail**  | Full content of a single observation, scrollable                  |
| **Timeline**            | Chronological context around an observation (before/after)        |
| **Sessions**            | Browse all sessions                                               |
| **Session Detail**      | Observations within a specific session                            |
| **Cloud sync settings** | Local-first cloud control center: configure a server, inspect local cloud readiness/sync state, and enroll projects |

### Navigation

- `j/k` or arrow keys — Navigate lists
- `Enter` — Select / drill into detail
- `c` — Copy observation content to clipboard (OSC 52; works in search results, recent list, detail, and session views)
- `t` — View timeline for selected observation
- `r` — Refresh the Review memories queue or supported status screens
- `m` — From a review memory's detail, open the mark-reviewed confirmation
- `p` — From a review memory's detail, pin or unpin it locally
- `s` or `/` — Quick search from any screen
- `Esc` — Go back or cancel an active text field
- `q` — Go back / quit when a text field is not focused; inside search and URL fields it is typed normally
- `Ctrl+C` — Force quit

### Review memories

From the Dashboard, select **Review memories** to inspect memories whose
`review_after` date is due in the project detected from the current working
directory. The queue is oldest-due first and shows each memory's lifecycle and
pin state. It never falls back to an all-project view; if the project or local
store cannot be resolved, the screen reports the error and allows `r` to retry.

Select a memory with `Enter` to read its complete detail. Merely opening it does
not change its lifecycle. Press `m` to open a confirmation prompt, then `y` (or
`Enter`) to confirm that the memory remains current; `n` or `Esc` cancels. A
successful confirmation resets the type-specific local review cycle, removes
the memory from the due queue, and keeps the cursor near the next item.

Press `p` in review detail to pin or unpin the memory. Pinning changes its local
priority in agent context but does not certify the content or remove it from the
review queue. Both review timestamps and pins are local to this device and are
not synchronized. Use `Esc` from detail to defer the decision and return to the
same queue position.

### Cloud control center

From the Dashboard, select **Cloud sync settings**. The control center has three
working flows:

- **Configure server** — Enter an absolute `http://` or `https://` URL and press
  `Enter`. The URL is validated and persisted in the local data directory's
  `cloud.json`; URL userinfo, query delimiters, query parameters, and fragments
  are rejected, and an existing saved token is preserved. This screen does not
  ask for a token. Client authentication is resolved from `ENGRAM_CLOUD_TOKEN`
  first, or from the token already present in `cloud.json` when the environment
  variable is not set. The TUI only shows whether a token is configured; it
  never displays or echoes the secret.
- **View status** — Inspect local configuration and sync readiness: the resolved
  server URL, whether authentication material is present, the local sync
  lifecycle, enrolled project count/list, and any persisted reason code or
  diagnostic message. `r` refreshes this read-only view. “Auth ready” means a
  token is available locally; it does not perform a remote authentication or
  health check.
- **Enroll projects** — Browse project names known by the local store, including
  projects already enrolled. Select an unenrolled project with `Enter` to mark
  it for cloud replication; enrollment backfills its existing local sync
  mutations. Enrollment is idempotent, and the screen does not push data by
  itself. Select an already enrolled project to see that it is already enrolled.
  `r` reloads the local list.

Cloud remains opt-in and local-first: local SQLite is authoritative, enrollment
controls which project may replicate, and an explicit `engram sync --cloud
--project <project>` (or separately configured autosync) performs transport.
The TUI does not unenroll projects, repair upgrade blockers, or run cloud sync.

### Visual Features

- **Catppuccin Mocha** color palette
- **`(active)` badge** — shown next to sessions and observations from active sessions, sorted to top
- **Scroll indicators** — position in long lists (e.g. "showing 1-20 of 50")
- **2-line items** — each observation shows title + content preview

---

## Running as a Service

Without a service supervisor, `engram serve` dies whenever the binary is replaced (e.g. on `brew upgrade engram`) or the host reboots, and autosync stops silently. The templates below restart it automatically. Use `engram cloud status` afterwards to confirm — the `Local daemon:` line should report `running on port 7437`.

### Using systemd (Linux)

1. Move binary to `~/.local/bin` (ensure it's in your `$PATH`)
2. Create directories: `mkdir -p ~/.engram ~/.config/systemd/user`
3. Create `~/.config/systemd/user/engram.service` (see below)
4. `systemctl --user daemon-reload`
5. `systemctl --user enable engram`
6. `systemctl --user start engram`
7. `journalctl --user -u engram -f`

```ini
[Unit]
Description=Engram Memory Server
After=network.target

[Service]
WorkingDirectory=%h
ExecStart=%h/.local/bin/engram serve
Restart=always
RestartSec=3
Environment=ENGRAM_DATA_DIR=%h/.engram

[Install]
WantedBy=default.target
```

### Using launchd (macOS)

This is the recommended setup for Homebrew users on macOS. With `KeepAlive=true`, launchd relaunches `engram serve` automatically after `brew upgrade engram` replaces the binary, so autosync survives upgrades.

1. Find your binary path: `which engram` (typically `/opt/homebrew/bin/engram` on Apple Silicon or `/usr/local/bin/engram` on Intel)
2. Create the data dir if missing: `mkdir -p ~/.engram`
3. Create `~/Library/LaunchAgents/com.yersonargotev.engram.plist` with the contents below — replace `<HOME>` with the absolute path of your home directory (`echo $HOME`) and adjust the binary path if `which engram` returned something different
4. Load it: `launchctl load ~/Library/LaunchAgents/com.yersonargotev.engram.plist`
5. Verify: `launchctl list | grep engram` and `engram cloud status` (the `Local daemon:` line should report `running on port 7437`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.yersonargotev.engram</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/engram</string>
        <string>serve</string>
    </array>
    <key>WorkingDirectory</key>
    <string><HOME></string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>ENGRAM_DATA_DIR</key>
        <string><HOME>/.engram</string>
        <!-- Uncomment and fill these to enable cloud autosync:
        <key>ENGRAM_CLOUD_AUTOSYNC</key>
        <string>1</string>
        <key>ENGRAM_CLOUD_SERVER</key>
        <string>https://your-cloud-host</string>
        <key>ENGRAM_CLOUD_TOKEN</key>
        <string>your-cloud-token</string>
        -->
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string><HOME>/.engram/serve.out.log</string>
    <key>StandardErrorPath</key>
    <string><HOME>/.engram/serve.err.log</string>
</dict>
</plist>
```

To unload (stop and disable): `launchctl unload ~/Library/LaunchAgents/com.yersonargotev.engram.plist`. To reload after editing the plist: unload, then load again.

> **Note on `brew upgrade`:** launchd does not expand `$HOME` or `~` inside plist values, which is why the template uses literal absolute paths.

### Using Windows Task Scheduler

Windows Task Scheduler is the native service equivalent on Windows. It restarts `engram serve` on login and after reboots, keeping autosync alive without a third-party service manager.

**Setup steps:**

1. Confirm `engram.exe` is in your `PATH`: open PowerShell and run `Get-Command engram`.
2. Set `ENGRAM_CLOUD_TOKEN` (and any other cloud vars) as a **user or system environment variable** in System Properties → Advanced → Environment Variables. Task Scheduler does not inherit session environment variables, so tokens set in your shell profile or in `$env:...` within a PowerShell session will not be visible to the scheduled task.
3. Create the scheduled task by running the PowerShell snippet below in an elevated terminal (Run as Administrator), or import it manually through the Task Scheduler GUI.
4. Verify: after the next login (or trigger manually), run `engram cloud status` — the `Local daemon:` line should report `running on port 7437`.

```powershell
$action  = New-ScheduledTaskAction `
    -Execute  "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -Command `"Start-Process engram -ArgumentList 'serve' -NoNewWindow`""

$trigger = New-ScheduledTaskTrigger -AtLogOn

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -RestartCount 5 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName    "EngramMemoryServer" `
    -Action      $action `
    -Trigger     $trigger `
    -Settings    $settings `
    -RunLevel    Limited `
    -Description "Engram persistent memory server (engram serve)"
```

> **Environment variables:** `ENGRAM_CLOUD_TOKEN`, `ENGRAM_CLOUD_SERVER`, `ENGRAM_CLOUD_AUTOSYNC`, and `ENGRAM_DATA_DIR` must be set as persistent user or system environment variables (Control Panel → System → Advanced → Environment Variables) so Task Scheduler can read them. Variables you `export` or set with `$env:` in a terminal session are not visible to scheduled tasks.

> **Logs:** To capture stdout/stderr, redirect output in the PowerShell command string, for example: `... -Command "Start-Process engram -ArgumentList 'serve' -NoNewWindow -RedirectStandardOutput '$env:USERPROFILE\.engram\serve.out.log' -RedirectStandardError '$env:USERPROFILE\.engram\serve.err.log'"`. Ensure the log files are opened with UTF-8 encoding (`-Encoding UTF8`) if you post-process them.

> **Stopping the task:** `Stop-ScheduledTask -TaskName "EngramMemoryServer"` or `Unregister-ScheduledTask -TaskName "EngramMemoryServer" -Confirm:$false` to remove it entirely.

---

## Design Decisions

1. **Go over TypeScript** — Single binary, cross-platform, no runtime. The initial prototype was TS but was rewritten.
2. **SQLite + FTS5 over vector DB** — FTS5 covers 95% of use cases. No ChromaDB/Pinecone complexity.
3. **Agent-agnostic core** — Go binary is the brain, thin plugins per-agent. Not locked to any agent.
4. **Agent-driven compression** — The agent already has an LLM. No separate compression service.
5. **Privacy at two layers** — Strip in plugin AND store. Defense in depth.
6. **Pure Go SQLite (modernc.org/sqlite)** — No CGO means true cross-platform binary distribution.
7. **No raw tool-call auto-capture** — The agent saves curated summaries; an explicit `mem_save(capture_prompt=true)` may request consent-gated local Diagnostic capture of same-process prompt context, but Engram does not ingest raw tool-call firehoses. Shell history and git provide the raw audit trail.
8. **TUI with Bubbletea** — Interactive terminal UI following Gentleman Bubbletea patterns.

---

## Dependencies

### Go

| Package                              | Version | Purpose                        |
| ------------------------------------ | ------- | ------------------------------ |
| `github.com/mark3labs/mcp-go`        | v0.44.0 | MCP protocol implementation    |
| `modernc.org/sqlite`                 | v1.45.0 | Pure Go SQLite driver (no CGO) |
| `github.com/charmbracelet/bubbletea` | v1.3.10 | Terminal UI framework          |
| `github.com/charmbracelet/lipgloss`  | v1.1.0  | Terminal styling               |
| `github.com/charmbracelet/bubbles`   | v1.0.0  | TUI components                 |

### OpenCode Plugin

- `@opencode-ai/plugin` — OpenCode plugin types and helpers
- Runtime: Bun (built into OpenCode)

---

## Dashboard templ regeneration

The cloud dashboard uses [templ](https://templ.guide/) for server-side HTML components. Generated `*_templ.go` files are committed alongside their `.templ` sources. If you modify any `.templ` file in `internal/cloud/dashboard/`, you must regenerate the Go output before committing.

### Prerequisite

Download the pinned templ binary:

```sh
go mod download
```

### Regenerate

```sh
make templ
# or directly:
go tool templ generate ./internal/cloud/dashboard/...
```

The regenerated `components_templ.go`, `layout_templ.go`, and `login_templ.go` must be committed together with the `.templ` source changes. The test `TestTemplGeneratedFilesAreCheckedIn` in `internal/cloud/dashboard/templ_policy_test.go` will fail in CI if generated files are missing or outdated.

**Important**: Always use the pinned version `github.com/a-h/templ v0.3.1001` (already in `go.mod`). Regenerating with a different version produces diff churn in generated output.

---

## Cloud Autosync

Autosync is a background push/pull replication service that keeps your local Engram store in sync with the Engram Cloud server without blocking local writes.

### Enabling Autosync

Autosync is **opt-in**. Set all three environment variables before starting `engram serve` or `engram mcp`:

| Variable                | Required          | Description                                                             |
| ----------------------- | ----------------- | ----------------------------------------------------------------------- |
| `ENGRAM_CLOUD_AUTOSYNC` | Yes (exact `"1"`) | Enables autosync. Any other value disables it.                          |
| `ENGRAM_CLOUD_TOKEN`    | Yes               | Bearer token for the cloud server.                                      |
| `ENGRAM_CLOUD_SERVER`   | Yes               | Base URL of the cloud server (e.g. `https://cloud.engram.example.com`). |

Example:

```sh
ENGRAM_CLOUD_AUTOSYNC=1 \
ENGRAM_CLOUD_TOKEN=your-token \
ENGRAM_CLOUD_SERVER=https://cloud.engram.example.com \
engram serve

# Or, for stdio MCP agents:
ENGRAM_CLOUD_AUTOSYNC=1 \
ENGRAM_CLOUD_TOKEN=your-token \
ENGRAM_CLOUD_SERVER=https://cloud.engram.example.com \
engram mcp
```

Missing `ENGRAM_CLOUD_TOKEN` or `ENGRAM_CLOUD_SERVER` logs an `ERROR` and disables autosync gracefully — `engram serve` or `engram mcp` still starts.

### Autosync Phase Table

| Phase         | Meaning                                | Dashboard Status          |
| ------------- | -------------------------------------- | ------------------------- |
| `idle`        | Loop running, no cycle yet             | running                   |
| `pushing`     | Pushing local mutations to cloud       | running                   |
| `pulling`     | Pulling remote mutations               | running                   |
| `healthy`     | Last cycle succeeded                   | healthy                   |
| `push_failed` | Last push failed                       | degraded                  |
| `pull_failed` | Last pull failed                       | degraded                  |
| `backoff`     | Too many consecutive failures; waiting | degraded                  |
| `disabled`    | Paused by `StopForUpgrade`             | degraded (upgrade_paused) |

### Reason Code Table

`reason_code` appears in `Manager.Status().ReasonCode` and is surfaced via `/sync/status`:

| `reason_code`      | Cause                                                   | Resolution                                                                   |
| ------------------ | ------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `transport_failed` | Network error, server 5xx, or 404 on mutation endpoints | Check server health and network; if 404, see `server_unsupported` note below |
| `auth_required`    | Bearer token rejected (401)                             | Rotate `ENGRAM_CLOUD_TOKEN`                                                  |
| `policy_forbidden` | Project access denied (403)                             | Check `ENGRAM_CLOUD_ALLOWED_PROJECTS` on the server                          |
| `internal_error`   | Panic inside the sync cycle                             | Check logs for stack trace                                                   |
| `upgrade_paused`   | Autosync paused during cloud upgrade (`PhaseDisabled`)  | Call `ResumeAfterUpgrade` or restart                                         |

Note: when the cloud server returns 404 on mutation endpoints, the transport logs `[autosync] cloud mutation endpoint returned 404 (server_unsupported)` and the transport-level `ErrorCode` is `"server_unsupported"`, but the manager surfaces this as `reason_code: transport_failed`.

### Troubleshooting

For a step-by-step recovery guide covering `chunk_id does not match payload content hash`, `session payload directory is required`, and the temporary missing-directory repair helper, see [Engram Cloud Troubleshooting](docs/engram-cloud/troubleshooting.md).

**`transport_failed` with `server_unsupported` in logs**: Older pre-mutation cloud server deployments may not implement `POST /sync/mutations/push` or `GET /sync/mutations/pull`, causing 404 responses from those endpoints. Deploy a server version that includes these routes before enabling `ENGRAM_CLOUD_AUTOSYNC=1`. Check logs for the line containing `server_unsupported`.

**Autosync not starting**: Check that `ENGRAM_CLOUD_AUTOSYNC` is exactly `"1"` (not `"true"` or `"yes"`), and that both `ENGRAM_CLOUD_TOKEN` and `ENGRAM_CLOUD_SERVER` are non-empty. The process logs an `[autosync] ERROR` line explaining which variable is missing.

**Local writes still blocked**: Autosync runs in its own goroutine and never holds locks shared with the local write path. If local writes appear blocked, investigate the SQLite store layer, not the autosync manager.

---

---

## Cloud Sync Audit Log

When project sync is paused and a push is rejected, Engram records an audit entry in `cloud_sync_audit_log`. This gives operators a persistent trail of every rejection event, visible in the admin dashboard under **Admin > Audit Log**.

### Schema

| Column        | Type                      | Description                                                                 |
| ------------- | ------------------------- | --------------------------------------------------------------------------- |
| `id`          | SERIAL PK                 | Auto-incrementing row identifier                                            |
| `occurred_at` | TIMESTAMPTZ DEFAULT NOW() | Timestamp of the rejection event                                            |
| `contributor` | TEXT NOT NULL             | Identity of the caller (from `created_by` field in request, or `"unknown"`) |
| `project`     | TEXT NOT NULL             | Project name that was paused and rejected                                   |
| `action`      | TEXT NOT NULL             | Push type discriminator: `mutation_push` or `chunk_push`                    |
| `outcome`     | TEXT NOT NULL             | Rejection outcome: always `rejected_project_paused` in v1                   |
| `entry_count` | INT DEFAULT 0             | Number of entries in the rejected batch                                     |
| `reason_code` | TEXT                      | Short machine-readable reason code (e.g. `sync-paused`)                     |
| `metadata`    | JSONB                     | Reserved for future structured context; not populated in v1                 |

### Outcome Vocabulary

| Outcome                   | Meaning                                                                           |
| ------------------------- | --------------------------------------------------------------------------------- |
| `rejected_project_paused` | Push was rejected because the project's sync is paused via the admin sync control |

### Action Discriminator

| Action          | Meaning                                                     |
| --------------- | ----------------------------------------------------------- |
| `mutation_push` | Rejection occurred on `POST /sync/mutations/push`           |
| `chunk_push`    | Rejection occurred on `POST /sync/push` (legacy chunk push) |

Pull requests (`GET /sync/mutations/pull`) are never gated on pause status and never emit audit entries. Paused projects continue to serve reads to enrolled contributors without restriction.

### Retention and Pruning

There is no automatic retention policy in v1. Audit rows accumulate indefinitely. To prune entries older than 90 days, connect to Postgres and run:

```sql
DELETE FROM cloud_sync_audit_log
WHERE occurred_at < NOW() - INTERVAL '90 days';
```

Wrap in a transaction and add a `LIMIT` clause if the table is large:

```sql
BEGIN;
DELETE FROM cloud_sync_audit_log
WHERE id IN (
  SELECT id FROM cloud_sync_audit_log
  WHERE occurred_at < NOW() - INTERVAL '90 days'
  LIMIT 10000
);
COMMIT;
```

---

## Next Steps

- [Agent Setup](docs/AGENT-SETUP.md) — connect your agent to Engram
- [Plugins](docs/PLUGINS.md) — what the OpenCode and Claude Code plugins add beyond bare MCP
- [Obsidian Brain](docs/beta/obsidian-brain.md) — visualize memories as a knowledge graph (beta)
- [Contributing](CONTRIBUTING.md) — how to contribute
