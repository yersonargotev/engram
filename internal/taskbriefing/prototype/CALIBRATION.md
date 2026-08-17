# Task briefing v1 calibration

This directory is retained decision evidence for Task briefing relevance. It is
an executable prototype, not production command behavior: no code here is wired
to `engram context`.

Run the corpus with:

```sh
go test ./internal/taskbriefing/prototype -run '^TestScenarioCorpus$' -count=1 -v
```

The versioned source is [`testdata/v1/scenarios.json`](testdata/v1/scenarios.json).
Every scenario declares memories that must appear and memories that must not
appear, plus the reason for each expectation. The runner uses a real temporary
SQLite/FTS store, requires the expected signal types, and executes every scenario
repeatedly to compare the complete result—including matched terms, matched
fields, order, omissions, and diagnostics—between runs. It also checks that
generation does not add memories.

## Selected defaults

| Setting | Value | Corpus evidence |
|---|---:|---|
| Task intent weight | 12 | `task_intent_overrides_misleading_git` selects only a Task-matching memory when Git is misleading. A supplied Task intent is also a qualifying gate. |
| Branch name weight | 3 | Branch names help corroborate clean work and commit subjects but cannot qualify a generic match alone. |
| Feature-branch diff weight | 5 | `repository_only_clean_branch` uses committed changes without treating a clean worktree as no work. |
| Staged / unstaged diff weights | 6 / 6 | `dirty_worktree_signals` requires both sources to remain independently visible. |
| Affected path weight | 7 | Paths can strongly corroborate a diff while a single generic term remains insufficient. |
| Commit subject weight | 6 | `commit_subjects_contribute` preserves recorded branch intent without using recency. |
| Untracked path weight | 7 | `untracked_paths_without_content` selects from paths alone and proves file contents are unnecessary. |
| Title or topic-key bonus | 2 | Exact domain labels are more precise than content-only overlap. |
| Pin boost | 2 | `pin_boost_requires_relevance` reorders qualifying memories but excludes an irrelevant pin. |
| Inclusion threshold | 10 | Repository-only selection needs corroboration; `precision_first_does_not_fill_limit` leaves weak candidates out. |
| Maximum result count | 5 | `maximum_result_count` deterministically returns five of six equally relevant memories. The value is a cap, not a quota. |
| Total output budget | 4,096 JSON bytes | `whole_memory_output_budget` retains a complete small result and omits an oversized result atomically, including Selection evidence and diagnostics in the measurement. |

## Input bounds

Inputs are normalized and deduplicated before retrieval. The prototype retains
at most 12 Task terms, 6 branch-name terms, 16 terms for each diff source, and
12 terms for each path, commit-subject, or untracked-path source. The
`every_repository_source_is_bounded` scenario crosses every Repository limit;
`task_and_repository_truncation_are_distinct` crosses the Task limit; and
`oversized_input_and_unresolved_base` combines a bounded diff with degraded base
resolution. Each asserts deterministic retained prefixes and per-source omitted
term counts.

## Precision and lifecycle decisions

Repository evidence contributes only when at least two normalized terms from a
source match a memory. When Task intent is supplied, a memory must independently
match at least half of its bounded normalized terms; Repository signals can only
improve qualifying Task matches. A pin is applied only after the threshold is
met. Deleted and superseded memories are ineligible, relations do not expand
candidates, and two selected memories with a judged conflict remain selected
with a diagnostic.

Selection order is score descending, then title and local ID for deterministic
ties. Recency is not relevance evidence. A missing or mismatched repository
project disables Repository signals, while an explicit Task can still select
memories. Optional Git failures degrade instead of failing retrieval, and a
memory-store failure returns a typed error.
