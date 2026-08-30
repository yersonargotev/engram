---
status: accepted
---

# Retire the complete Admission experiment

Engram will remove the complete public `engram admission` command family,
including `preview`, together with the dedicated generation, assessment,
Shadow, study, review, omission, and metrics runtime in one declared breaking
change. It will not ship a deprecation window or retain dormant Admission
runtime as a compatibility layer. The real-session evaluation reached a
terminal no-go and established research and safety value rather than a product
path; preserving the experiment would therefore add ongoing public and
persistence complexity without current user value. This decision supersedes
[ADR-0004](0004-calibrate-memory-admission-offline.md),
[ADR-0005](0005-explicit-local-shadow-admission.md), and
[ADR-0007](0007-freeze-attributable-admission-studies.md). The removal defines
Engram v3.0.0.

The independent Memory checkpoint contract remains `saved | skipped |
needs_review`. Its local Memory proposal survives only as checkpoint audit
evidence, not as an inbox or Promotion promise. Callers provide one inline
`{title, content}` object; Engram derives its identity, project, and creation
time, and the checkpoint returns the immutable proposal reference. V3 removes
the public `proposal_id` input and the proposal `type`, `scope`, `category`,
`protected`, `evidence_refs`, and `reason_codes` fields because they have no
consumer independent from Admission. It adds no standalone proposal create,
get, list, review, or Promotion workflow. Instead, `checkpoint record` and
`checkpoint status` return the immutable `{id, project, title, content,
created_at}` proposal snapshot for a `needs_review` result, making the audit
evidence inspectable through the exact checkpoint identity. CLI and MCP expose
the same result.

The v3 migration reconstructs `memory_proposals` with only `id`, `project`,
`title`, `content`, and `created_at`. It preserves proposal IDs so existing
checkpoint references remain valid, verifies referential integrity, and
irreversibly discards the removed metadata.

Opening a v2 database with v3 automatically and irreversibly drops every
dedicated `admission_*` table and its retained experimental data. New databases
do not create those tables. This data loss is an explicit part of the v3
breaking boundary, not a lifecycle cleanup, sync operation, or conversion into
Memories. Because Engram has no versioned migration ledger, every v3 database
open enforces this invariant through one small, permanent, idempotent tombstone
migration. It drops child tables before parents inside one retryable transaction;
failure aborts startup without leaving a partial Admission schema.

V3 removes Admission commands, examples, behavior descriptions, and domain
terms from active product and contributor documentation in the same change as
the code. `Memory proposal` remains in the glossary with its checkpoint-audit
meaning. Superseded ADR-0004, ADR-0005, and ADR-0007, the
[Admission disposition research report](../research/admission-shadow-disposition.md),
historical CHANGELOG entries, and closed Issues remain as explicitly historical
evidence rather than deprecated product guidance.
