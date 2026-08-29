# Task briefing v1 calibration

This directory retains the versioned decision evidence that calibrated Task
briefing relevance. The production generator now lives at the parent
`internal/taskbriefing` module seam, and the retained corpus verifies that same
interface. The CLI task-only adapter is wired separately from this evidence.

Run the corpus with:

```sh
go test ./internal/taskbriefing -run '^TestScenarioCorpus$' -count=1 -v
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
| Exact identifier strength | 4 per match | `identifier_agreement_ranks_before_title_tie_breaking` places full PR/issue agreement ahead of partial agreement before title or ID ties. |
| Distinctive term strength | 1 per match | `exact_issue_and_pr_identifiers_beat_generic_workflow_terms_task_only` excludes generic workflow overlap while subject terms still refine ranking. |
| Candidate field contribution limit | 6 per signal and field | `candidate_field_contribution_is_bounded` prevents a long content field from outranking concise title evidence merely by containing every bounded Task term. |
| Title or topic-key bonus | 2 | Exact domain labels are more precise than content-only overlap. |
| Pin boost | 2 | `pin_boost_requires_relevance` reorders qualifying memories but excludes an irrelevant pin. |
| Inclusion threshold | 10 | Repository-only selection needs corroboration; `precision_first_does_not_fill_limit` leaves weak candidates out. |
| Maximum result count | 5 | `maximum_result_count` deterministically returns five of six equally relevant memories. The value is a cap, not a quota. |
| Total output budget | 4,096 output bytes | CLI tests retain or omit complete memories atomically while measuring only the selected human or compact JSON stream, including metadata, Selection evidence, diagnostics, and the final newline. |
| Git input ceiling | 1 MiB per command | `TestReadBoundedGitTermsStopsAtDeterministicByteLimit` proves acquisition stops at the calibrated ceiling and exposes incomplete prefix counts. |

## Input bounds

Inputs are normalized and deduplicated before retrieval. The prototype retains
at most 12 Task terms, 6 branch-name terms, 16 terms for each diff source, and
12 terms for each path, commit-subject, or untracked-path source. Only retained
vocabulary is tracked in memory; eligible occurrences after the bound are
counted without retaining their values. Exact identifiers exclude oversized
tokens and the diagnosed omitted tail. Git streams stop at a deterministic
one-MiB acquisition ceiling and mark those prefix counts with
`count_complete: false`. The
`every_repository_source_is_bounded` scenario crosses every Repository limit;
`task_and_repository_truncation_are_distinct` crosses the Task limit; and
`oversized_input_and_unresolved_base` combines a bounded diff with degraded base
resolution. Each asserts deterministic retained prefixes and per-source omitted
term counts.

## Precision and lifecycle decisions

Normalization separates exact compound identifiers, distinctive terms, and
generic words. Issue and PR references, named branches, commit identities,
paths, and topic keys retain typed identity. Common stop words and workflow
labels do not contribute to the authoritative Task denominator. Exact identifier
matches contribute four strength units and distinctive terms contribute one; a
Task candidate must match at least half of that bounded strong evidence. A
Repository source is strong when it supplies an exact identifier or a
distinctive term with corroborating overlap. Generic Repository overlap can
reinforce repository-only selection after another source is strong, but cannot
rescue a candidate when explicit Task evidence is weak.

The calibrated inclusion threshold is evaluated before match-strength bonuses,
so a single weak source cannot become relevant merely because it contains more
words. Ranking then adds exact and distinctive match strength before title and
local-ID tie-breaking. Each candidate field contributes at most six identifiers
or terms for one signal; repeated prose and long bodies cannot create unbounded
authority. A pin is applied only after the threshold is met. Deleted and
superseded memories are ineligible, relations do not expand candidates, and two
selected memories with a judged conflict remain selected with a diagnostic.

Selection order is score descending, then title and local ID for deterministic
ties. Recency is not relevance evidence. A missing or mismatched repository
project disables Repository signals, while an explicit Task can still select
memories. Optional Git failures degrade instead of failing retrieval, and a
memory-store failure returns a typed error.

Identical normalized payloads from multiple signal sources form one retrieval
and ranking group. The group contributes its strongest source weight once, while
Selection evidence still lists every contributing source. The
`duplicate_repository_payload_is_single_scoring_group` scenario prevents
duplicated worktree content from manufacturing relevance.
