---
status: accepted
---

# Retire Task Brief in favor of explicit recall primitives

Engram v3.0.0 will remove Task Brief as a product capability in one declared
breaking change. The `engram context --brief` mode, its `--task`, `--base`, and
`--limit` flags, the `internal/taskbriefing` package, dedicated rendering and
diagnostics, calibration corpus, and agent-skill integration will be deleted.
Engram will not ship a deprecation window, alias, or compatibility layer.

Task Brief is local, deterministic, privacy-bounded, and can improve precision
when a task contains exact identifiers. However, real generic-task cases can
reject a relevant Memory after retrieval, and no held-out comparison establishes
enough incremental value over deliberate `engram search` plus chronological
`engram context` to justify the independent policy and maintenance surface. The
retention research therefore recommends removal with medium confidence.

## Consequences

`engram context` returns chronological session context only. Removed Task Brief
flags are unknown flags rather than deprecated inputs. Agents use one narrow,
project-scoped `engram search` for topical recall and request chronological
context separately when recent session continuity matters.

The removal changes no SQLite schema, sync contract, cloud behavior, or MCP
surface because Task Brief was a CLI-only consumer of existing Store search and
relation APIs. The Task-Brief-only searchable-inventory helper is removed with
the package. Current activation-study runs classify directed search as the recall
operation; frozen v1 contracts and reports may retain their historical Task Brief
event fields without preserving an executable Task Brief path.

ADR-0003 is superseded. Historical research remains as decision evidence, while
current user and contributor documentation describes only the surviving recall
primitives.
