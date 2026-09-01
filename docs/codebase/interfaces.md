[← Codebase Guide](../CODEBASE-GUIDE.md) | [← Previous: Memory Core](memory-core.md) | [Next: Sync and Cloud →](sync-and-cloud.md)

# Interfaces

**Engram exposes one local memory core through several interfaces: CLI, MCP, local HTTP API, and TUI.** Keep interface code thin: parse input, call the right package, return a clear response.

## CLI: `cmd/engram`

`cmd/engram/main.go` and neighboring files are the binary entry point. They connect store, HTTP, MCP, TUI, sync, autosync, setup, doctor, conflicts, cloud, and Obsidian.

Do not put core behavior in the command if it can live in a testable package. The command should coordinate, parse flags, and adapt errors for humans.

## MCP: `internal/mcp`

`internal/mcp/mcp.go` exposes Engram to agents over stdio. It has tool profiles:

| Profile | Use |
|---|---|
| `all` | Default for `engram mcp`; registers all tools. |
| `agent` | Exactly current project, search, get observation, checkpoint, and checkpoint status. |
| `curation` | Explicit authoring, optional Session summary, context, review, relations, diagnosis, and pins. |
| `lifecycle` | Host session start/end, prompt save, and passive Content capture. |
| `admin` | Destructive and operational maintenance: delete, stats, timeline, merge. |

Important points:

- `mem_current_project` is the recommended first call to confirm detection.
- `mem_search` is a thin adapter over `internal/memoryops.Recall`: automatic
  project Recall requires strong/explicit identity, returns at most five
  candidates and 4 KiB by default, and fails open with one warning plus
  diagnostics. CLI, MCP, and the HTTP `/recall` adapter preserve the same
  candidate semantics.
- Normal writes should not pass `project` as an arbitrary override.
- `mem_checkpoint` preflight is a bounded read-only operation over one explicit
  project. Record mode uses the same explicit project for `saved` and
  `needs_review` so the core can enforce Memory and proposal ownership
  atomically. Its optional `recall_feedback` sidecar records explicit labels
  for one Recall run bound at search time to that exact root turn without
  changing checkpoint completion.
- `engram recall-feedback report` is a separate aggregate-only CLI read. It
  exposes denominators and unknowns but no raw or salted identity.
- `ambiguous_project` recovery requires the user to choose an exact project.
- If `mem_save` returns conflict candidates, the agent must judge with `mem_judge` or ask when the relationship is sensitive.

For tool parameters and envelopes, use [DOCS.md — MCP Tools](../../DOCS.md#mcp-tools-24-tools).

## Local API: `internal/server`

`internal/server/server.go` is a simple JSON API over the local store. Its
`GET /recall` route projects the shared Recall service for thin host adapters;
legacy `GET /search` remains a generic compatibility search. It also exposes
`GET /sync/status` for autosync/degraded-state visibility.

Use it for plugins, hooks, or local external clients. Do not confuse it with cloud: the cloud runtime has its own server.

For exact local routes, use [DOCS.md — HTTP API Endpoints](../../DOCS.md#http-api-endpoints).

## TUI: `internal/tui`

The TUI uses Bubbletea and reads from the local store. The separation is classic:

| File | Role |
|---|---|
| `internal/tui/model.go` | State, screens, initialization. |
| `internal/tui/update.go` | Input/transitions handling. |
| `internal/tui/view.go` | Screen rendering. |
| `internal/tui/styles.go` | Lipgloss styles. |

## Interface change checklists

### MCP tools

- [ ] The tool uses store as the source of truth.
- [ ] Project resolution respects `.engram/config.json`, cwd, and the `ambiguous_project` flow.
- [ ] The `agent`/`curation`/`lifecycle`/`admin` profile remains intentional.
- [ ] Errors return useful envelopes for agents.
- [ ] Tests in `internal/mcp/*_test.go` cover the contract.
- [ ] `docs/AGENT-SETUP.md`, `docs/ARCHITECTURE.md`, or `DOCS.md` are updated if visible behavior changes.

### Local API

- [ ] The route belongs to `engram serve`, not cloud.
- [ ] `internal/server/server.go` only orchestrates request/response and calls store/services.
- [ ] Status codes and JSON errors are deterministic.
- [ ] Tests in `internal/server/*_test.go` cover errors and success.
- [ ] `DOCS.md#http-api-endpoints` is updated if there is a new/modified public endpoint.

---

[← Previous: Memory Core](memory-core.md) | [Next: Sync and Cloud →](sync-and-cloud.md)
