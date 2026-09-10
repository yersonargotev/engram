# Host conversations and multiple Memory projects

Status: historical, non-normative research retained through
[issue #162](https://github.com/yersonargotev/engram/issues/162).
[Issue #159](https://github.com/yersonargotev/engram/issues/159) tracks the related
design proposal; publishing this evidence does not approve that proposal or
change the current checkpoint contract.

Investigated 2026-09-09 against source commit `5a0f93e` and installed CLI `3.3.1`.

## Findings

A host conversation cannot currently create inline checkpoint Memories for a second project after its session ID has been associated with the first project. This is shared core behavior, not a CLI-only restriction: both [CLI](../../cmd/engram/checkpoint.go) and [MCP](../../internal/mcp/checkpoint.go) delegate to `memoryops.RecordCheckpoint`.

The core [checkpoint implementation](../../internal/store/checkpoint.go) uses the opaque host `session_id` as the Engram session ID when creating inline Memories. `attachCheckpointMemoriesTx` creates or reuses that session, reads its project, and rejects a different project with `ErrCheckpointProjectMismatch`. It then assigns the same session ID and project to every inline Memory. Changing the root turn does not remove this session ownership constraint.

One checkpoint accepts one project. Existing Memory references must all belong to that project. The ledger uniqueness key is `(host, session_id, root_turn_id)`; project is not part of that key. Replaying a completed turn is not a way to append another project. See [store checkpoint validation and schema](../../internal/store/checkpoint.go).

There is an important asymmetry: a references-only checkpoint does not run the inline-session ownership check. A later turn in the same host conversation can therefore reference existing Memories from a different project, provided all references in that checkpoint share its selected project. See `attachCheckpointMemoriesTx` in the same source.

Independent curation saves can span projects. [CLI save](../../cmd/engram/main.go) uses `manual-save-{project}`. [MCP save](../../internal/mcp/mcp.go) without an explicit session ID resolves the most recent active session of the selected project, falling back to `manual-save-{project}`. MCP project overrides are validated against known context; supplying a session owned by another project is rejected. This capability does not make the normal terminal checkpoint operation support multiple projects.

## Runtime reproduction

Used a temporary `ENGRAM_DATA_DIR`, removed after execution; no fixture was saved in the real database. With installed CLI 3.3.1:

| Operation | Result |
| --- | --- |
| Inline saved checkpoint, session S, turn A, project A | `created` |
| Inline saved checkpoint, session S, turn B, project B | `checkpoint_project_mismatch` |
| Independent `save --project project-b` | Memory created under `manual-save-project-b` |
| References-only saved checkpoint, session S, turn C, project B, referencing that Memory | `created` |

MCP conclusions above follow its maintained handler and shared core, rather than a connected live MCP invocation.

## Design implication

Supporting a host conversation across projects requires distinguishing host conversation identity from project-owned persistence sessions. Supporting multiple projects within one root turn additionally requires a checkpoint payload and validation model that can express several projects while preserving one terminal checkpoint per root turn. These are proposed implications, not implemented changes. Independent saves are curation operations, not the default protocol workaround.
