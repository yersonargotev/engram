---
status: accepted
---

# Measure memory admission with explicit local shadow runs

Engram will add an explicitly invoked, local-only shadow workflow after the
non-persisting preview established by ADR-0004. `engram admission shadow` accepts one
existing session and project, reuses the same bounded Evidence acquisition and
deterministic proposal/admission policy, and atomically retains only derived,
redacted proposal and assessment snapshots. The command invocation is the opt-in
boundary; no lifecycle hook invokes it.

`internal/memoryops` owns shadow orchestration, review semantics, and metrics.
`internal/store` owns dedicated SQLite tables for immutable runs and proposal
snapshots plus append-only human corrections. The CLI remains a thin parser and
renderer. Shadow rows have no sync triggers and are excluded from Memory
search/context, FTS, normal export/import, cloud materialization, and Promotion.
Deleting a project also deletes its local shadow rows.
Project migration and merge move shadow runs atomically with the other project-owned
records and report the moved count without producing shadow-data sync mutations.

The retained contract includes local identifiers, project and selected session ID,
separate evidence/generator/policy versions, acquisition counts and diagnostic
codes, redacted proposal text and metadata, reason codes, evidence reference
identifiers restricted to `prompt:<local-id>`, `summary:<local-id>`, or the fixed
`session-summary` fallback, and bounded corrections. Imported sync IDs are not valid
shadow provenance. The retained contract excludes
Evidence bundle content, prompt and summary text, transcripts, repository diffs,
tool output, model prompts, and model responses. `<private>` blocks are redacted at
acquisition and again at the store boundary.

Corrections accept `admit`, `review`, or `reject`. They append events with a
transactionally increasing per-proposal ordinal without mutating the original
assessment; an identical retry of the latest normalized event is idempotent.
Metrics use only the highest-ordinal correction per proposal and report raw
counts and distributions, including protected false rejects by proposal category
and original assessment reason code. Any protected false reject blocks the
automatic-reject readiness gate. Any explicitly marked unsupported proposal or
privacy leak blocks the automatic-promotion readiness gate. The gates are advisory
and enable no automatic action. Safety findings persist when a later correction
omits their flags; only explicit clear flags append a retraction. This prevents a
verdict-only or note-only edit from silently reopening a readiness gate.

Calibration and held-out fixtures remain separate and use content-addressed,
versioned manifests. The original V3 evaluation cases entered with their policy and
thresholds, so they are classified only as observed regression evidence. V4 freezes
its corpus hash/IDs, policy and metric versions, and thresholds before the held-out
corpus lands; its test requires the explicit `admission_heldout` build tag. All
fixtures remain synthetic regression evidence, not a claim of production readiness.
Automatic lifecycle collection, raw-evidence retention,
LLM judgment, MCP/HTTP/plugin/UI integration, automatic retention, automatic
Promotion/rejection, and changes to `save`/`mem_save` remain out of scope.
