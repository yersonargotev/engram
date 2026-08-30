# Task Brief retention decision

**Status:** accepted and implemented by [ADR-0009](../adr/0009-retire-task-briefing.md).

**Date:** 2026-08-29
**Examined base:** `2de1aafdb63e3d35c5e478ab024ab1cd40c13d85` (`main`, Engram v2.3.0)

## Decision

**REMOVE Task Brief as a product capability. Confidence: medium.**

This recommends removing `engram context --brief` completely in one declared
breaking release, without a deprecation interval or compatibility layer. The
examined agent skill made Task Brief the first recall query, yet the evidence
shows that a focused ordinary `engram search` can recover the intended Memory
when Task Brief is empty or selects generic workflow history. Its
Git/worktree-aware selection is real but not demonstrated to provide enough
incremental recall quality over explicit search plus chronological context to
justify its independent public and maintenance surface.

Revisit this only if a pre-registered held-out corpus demonstrates that Task
Brief materially improves both recall and precision over a defined `search` +
chronological-context baseline, without raising false confidence or requiring
the caller to rewrite its task as a different retrieval query.

## Method and verification

This note uses primary project sources: accepted ADR, current code, the
versioned executable corpus, repository history, merged issues/PRs, and focused
tests. It uses the official SQLite FTS5 documentation only for the capability
already below `engram search`. Live-store examples below are prior read-only
research results, not fixture contracts; no raw local Memory or prompt content
is reproduced.

`go test ./internal/taskbriefing ./cmd/engram -count=1` passed on the examined
worktree. That proves current contract consistency, not comparative relevance
quality.

## What is proven, and what is not

### Current evidence

Task Brief is an opt-in `engram context --brief` mode. It combines an explicit
Task intent with bounded transient Repository signals, selects at most five
complete durable Memories, emits selection/rejection diagnostics and an advisory
search fallback, and never persists Task/Repository input. Chronological
`engram context` remains the default ([ADR-0003](../adr/0003-task-briefing-from-transient-signals.md),
[v2.3.0 CLI](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/cmd/engram/main.go), [README](../../README.md)).

The distinct inputs could help in a narrow feature-branch case: branch, diff,
path, and commit-subject terms can corroborate task evidence. The v1 corpus has
29 synthetic scenarios covering branch-only selection, dirty-worktree signals,
project isolation, typed issue/PR/path/topic identity, lifecycle filtering,
untracked-content privacy, deterministic bounds, and Git-failure degradation
([corpus](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/prototype/testdata/v1/scenarios.json),
[runner](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/corpus_test.go)). PR #75 also reports
isolated runs where exact PR 56 / issue 43 / Codex setup selection displaced
generic workflow history, and PR #76 made early rejection attributable and the
fallback structured but non-executing ([PR #75](https://github.com/yersonargotev/engram/pull/75),
[PR #76](https://github.com/yersonargotev/engram/pull/76)).

### Evidence against normal use

The corpus does not run the same task through Task Brief, `engram search`, and
chronological context; label useful results; or report recall@k/precision@k. It
asserts expected outputs produced by Task Brief's own policy, so it is not an
incremental-value ablation.

Two real, read-only investigations show the opposite result on generic language:

| Case | Task Brief result | Comparator |
| --- | --- | --- |
| PR 56 / issue 43 merge task | An old workflow Memory survived; the exact setup Memory failed the Task gate before scoring. | `engram search 'Codex setup reproducibility' --match-mode any` returned the setup Memory first and delivery Memory second. [Issue #59](https://github.com/yersonargotev/engram/issues/59) |
| Verbose English and Spanish generic-recall tasks | Both were empty although the target entered the 20-result union; the Task gate rejected it. | Compact `Task Brief generic recall fallback` selected it, while the automatic fallback anchors did not. [research](task-brief-generic-recall.md) |

The second result demonstrates a representation conflict: one `--task` field
must be both a truthful work description and a compact lexical retrieval query.
Recovery depends on an agent transforming the request into a search-like
subject phrase, which is already the clearer responsibility of `engram search`.

A current exact-identifier check does show Task Brief's strongest incremental
benefit. For `Codex setup issue 43 PR 56`, it selected the completed triage
summary alone; `engram search 'PR 56 issue 43' --match-mode all` returned that
summary plus the preceding triage summary. For the current generic-recall case,
both Task Brief and a deliberate `Task Brief generic recall` search put the
settled milestone first. This is useful precision, but it is confined to the
case Task Brief was repaired to optimize and does not offset the generic-task
failures.

On the same local store, a warm ten-run `hyperfine` check measured the generic
Task Brief command at 325.7 ms mean and the directed search at 65.1 ms mean.
This approximately fivefold difference is a local operational sample, not a
cross-machine performance guarantee; it is consistent with Task Brief running
multiple bounded retrievals and Git signal collectors instead of one search.

## Alternatives

| Alternative | Evidence of value | Known limitation | Assessment |
| --- | --- | --- | --- |
| Keep unchanged | Deterministic, privacy-bounded branch-aware policy and exact-identifier cases. | Generic false empties; current skill leads with it; worktree state changes selection. | Reject. |
| Keep experimental with a better skill | Compact query recovered one target with no core change. | Moves query construction to callers and preserves a core that can fail on the honest task text. | Reject; do not retain a deprecated path. |
| Simplify to search | Focused search recovered both reported false omissions; Store supports explicit `all`/`any`. | Caller must supply a purposeful query; no automatic Repository evidence. | Preferred task-specific recall primitive. |
| Use chronological context | Existing stable session history, no Repository inspection. | History is not topical retrieval. | Keep as complementary context. |
| Remove Task Brief | Removes misleading first path while retaining search and chronology. | Breaking CLI/docs/skill contract; loses unproven branch-aware benefit. | **Recommend.** |

The conclusion is an **inference**, not a claim that Repository evidence cannot
help. The known false-confidence risk is concrete: a bounded, well-explained
successful answer can look authoritative even when generic terms or changing
worktree state diverted it.

## Consumers and removal blast radius

The direct runtime consumer is the CLI. `cmdContext` parses `--brief`, `--task`,
`--base`, and `--limit`, invokes `internal/taskbriefing`, and renders a dedicated
human/JSON envelope. Without `--brief` it calls the existing chronological
`Store.FormatContext` path ([v2.3.0 CLI](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/cmd/engram/main.go), [renderer](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/cmd/engram/context_briefing.go), [Store](../../internal/store/store.go)).

There is no Task Brief MCP tool, setup registration, or plugin hook. MCP exposes
`mem_search` and chronological `mem_context` instead ([MCP registration](../../internal/mcp/mcp.go)). The main non-CLI caller is
[the Engram Memory CLI skill](../../skills/engram-memory-cli/SKILL.md), which
instructs agents to run one Task Brief before fallback/search. README examples,
usage text, JSON output, privacy/bound behavior, and fallback behavior are
covered by CLI tests ([examples test](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/cmd/engram/main_extra_test.go),
[brief tests](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/cmd/engram/context_briefing_test.go)).

Removal thus breaks documented scripts and consumers of `mode: "brief"`,
pipeline, diagnostics, rejection, and fallback fields. It belongs in a declared
breaking release. Conversely, Task Brief is contained in
`internal/taskbriefing`; Store search/chronology do not depend on it. No schema,
sync, cloud, dashboard, MCP, or plugin migration is needed.

## Measured maintenance and upstream exposure

At the examined base, `internal/taskbriefing` is 3,095 Go LOC: 1,467 production
LOC and 1,628 test LOC. The renderer and its tests add 1,259 LOC (304 production,
955 test). The package also includes a 356-line JSON corpus and a 91-line
calibration note: 4,801 lines in the dedicated implementation, tests, and
calibration assets before shared CLI, Store, docs, skill, and study integration.

The fork has at least ten merged Task-Brief-specific delivery, documentation,
skill, and repair PRs (#19, #21, #22, #24–#27, #29, #75, and #76). #75 changed
ten files and added 549 lines; #76 changed eight files and added 1,100
([PR #75](https://github.com/yersonargotev/engram/pull/75),
[PR #76](https://github.com/yersonargotev/engram/pull/76)). This is direct
evidence of policy/compatibility cost. Upstream lacks the package at the common
base, so upstream changes to CLI parsing, README CLI text, Store
search/context, or project detection are ongoing conflict/regression surfaces.
The future conflict count is unknown and should not be fabricated.

## Constraint alignment

Task Brief is local, deterministic, bounded, explainable, dependency-free, and
does not persist transient input ([ADR-0003](../adr/0003-task-briefing-from-transient-signals.md)). These are safety properties, not utility proof. `engram search`
uses the same local SQLite Store and exposes explicit AND/OR behavior; FTS5 is
local full-text search with documented phrase, Boolean, and ranking facilities
([Store](../../internal/store/store.go), [SQLite FTS5](https://www.sqlite.org/fts5.html)).
Search plus chronological context preserves local-first, privacy, determinism,
and zero dependencies while eliminating Git acquisition and bespoke selection
policy.

## Missing evidence and review condition

The missing evidence is a held-out, human-judged corpus that compares Task Brief,
one declared targeted-search query, and chronological context on the same
inventories. It should include generic tasks and clean-branch tasks and measure
recall@5, precision@5, false-empty, false-inclusion, and time-to-useful-memory.
Its absence lowers confidence but does not justify retaining an unvalidated
product path indefinitely. The remaining uncertainty is whether branch-aware
benefit outweighs the demonstrated generic failure and maintenance burden.

## Implemented disposition

ADR-0009 applies the recommendation to Engram v3.0.0. Task-specific recall now
starts with one deliberate narrow `engram search`, chronological context remains
separate session history, and the flag, package, renderer, corpus, Store helper,
current docs, tests, and activation-study runner integration are removed. Rollback
remains available at the Git/PR level; the shipped product has no deprecated alias
or compatibility path.

## Primary sources

- [ADR-0003](../adr/0003-task-briefing-from-transient-signals.md)
- [Task Brief generator](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/generator.go), [Repository collector](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/repository.go), and [v1 corpus](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/prototype/testdata/v1/scenarios.json)
- [v2.3.0 CLI](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/cmd/engram/main.go), [renderer](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/cmd/engram/context_briefing.go), [Store](../../internal/store/store.go), and [MCP registration](../../internal/mcp/mcp.go)
- [Issue #59](https://github.com/yersonargotev/engram/issues/59), [PR #75](https://github.com/yersonargotev/engram/pull/75), [Issue #60](https://github.com/yersonargotev/engram/issues/60), and [PR #76](https://github.com/yersonargotev/engram/pull/76)
- [Generic-recall research](task-brief-generic-recall.md) and [SQLite FTS5 documentation](https://www.sqlite.org/fts5.html)
