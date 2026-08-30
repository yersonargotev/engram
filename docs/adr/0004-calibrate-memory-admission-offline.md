---
status: superseded by ADR-0008
---

# Validate memory admission through a local non-persisting preview

Engram will validate Memory proposal generation and Admission assessments with a
small end-to-end slice rather than a test-private implementation. Production logic
lives in `internal/memoryops`; separate, versioned executable corpora calibrate
generation and admission; and `engram admission preview` is the first thin adapter.
The command accepts either an explicit, bounded Evidence bundle or an explicit
existing session ID, consults existing local Memories for normalized exact
duplicates, and returns an advisory report. It does not persist proposals or
assessments and does not change `mem_save` behavior.

The v1 baseline is deterministic and model-free. Generation recognizes only exact
English or Spanish explicit-memory prefixes and supported numbered or bulleted
structured sections. Repository signals and tool output are provenance-only and
cannot create proposals. Admission recommends `admit` for explicit requests,
`reject` only for unprotected empty, normalized exact duplicate, or redacted-only
proposals, and `review` for everything else. Protected proposals may never receive
`reject`.

The V2 adapter may acquire evidence from one explicitly selected local session.
Acquisition is owned by `internal/memoryops`; it reads only persisted user prompts
and the newest active `session_summary` Memory, falling back to the session summary
column when necessary. The Evidence bundle remains bounded by the v1 item and byte
limits, preserves stable provenance, and reports source-level availability,
inclusion, omission, and truncation. Session and requested project must match.

Automatic lifecycle execution, proposal or assessment persistence,
MCP/endpoints/plugins/UI, LLM advice, and promotion remain out of scope. Opening the
normal local store may run existing SQLite initialization or migrations;
“non-persisting” means the preview never creates or changes Memories, proposals, or
assessments. Automated shadow collection requires a later decision backed by
calibration and a separate held-out evaluation corpus.

## Auditable corpus freeze

Calibration may change with the implementation. Held-out evaluation may not. Before
adding or executing a held-out corpus, commit a manifest that freezes its exact
SHA-256 and ordered scenario IDs together with the implementation commit,
evidence/generator/policy versions, metric version, label schema, provenance and
consent status, and numeric thresholds. Add the matching corpus in a later commit
without changing that manifest or the frozen policy. Execute held-out tests only
through the dedicated `admission_heldout` build tag; ordinary tests continue to run
calibration and already-observed regression fixtures. A mismatch or later policy,
metric, threshold, or corpus change requires a new version rather than rewriting the
previous evaluation.

The V3 evaluation fixtures entered alongside their policy and thresholds and are
therefore observed regression evidence, not proof of a prior held-out freeze. V4 is
the first corpus using the manifest-first history described above. Synthetic results
remain regression evidence and do not establish production readiness.
