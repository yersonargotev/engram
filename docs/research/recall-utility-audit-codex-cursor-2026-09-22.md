# Recall Utility Audit: Codex and Cursor Agent

**Audit date:** 2026-09-22

**Scope:** all locally available Codex sessions since Engram checkpointing began on 2026-08-30, all locally available Cursor Agent transcripts, the complete local Recall ledger, Recall feedback, checkpoints, and current integration status

**Privacy boundary:** this audit used content-free ledger metadata and tool-call metadata. Conversation content was not bulk-exported. Two Cursor retrieval episodes were inspected only to determine whether selected Memory affected the response.

## Executive verdict

Engram Recall is technically fast and conditionally useful, but it is not yet a reliable cross-agent product capability.

- **Codex:** useful when activated, but activated in a minority of attributable sessions and turns. Agent-labelled results are useful 64.3% of the time, but only 51.4% of unique exposures have any label and there are no user or evaluator labels. The evidence supports **promising canary utility**, not general availability.
- **Cursor Agent:** the tool is invoked occasionally and selected Memory has helped in at least two observed sessions, but the current root-bound path produced four empty searches and no explicit utility feedback. The evidence supports **technical viability with insufficient measured utility**.
- **Overall:** the main bottleneck is activation and attribution, not Store latency. Engram cannot yet answer “what proportion of future tasks improved because of Memory?” across either host.

The current state is therefore **partial / canary**. Recall should not be described as proven end-to-end.

## Evidence populations

Three populations must remain separate:

1. **Native sessions:** host transcript/session files. These measure whether the host ran at all.
2. **Checkpoint-attributable sessions and turns:** root turns with a raw host/session/turn identity in `memory_checkpoints`. These permit a trustworthy host join.
3. **Recall and feedback rows:** searches, result exposures, full-content fetches, and explicit labels. Older unbound searches cannot be assigned to Codex or Cursor without inference.

The local Store contains 824 Engram sessions across 21 projects, but the `sessions` table has no host column. Host-level conclusions therefore use checkpoint identities and native host transcripts rather than guessing from session IDs.

## Complete local ledger

| Measure | Value |
|---|---:|
| Recall searches | 80 |
| Search result exposures | 176 |
| Empty searches | 29 |
| Root-bound searches | 47 |
| Unbound searches | 33 |
| Full-content fetches | 22 |
| Unique exposed turn–Memory pairs eligible for feedback | 109 |
| Labelled pairs | 56 (51.4%) |
| Empty runs represented in feedback denominator | 16 |
| Explicit false-empty reviews | 0 |

Thirty-three searches with 66 result exposures predate or omit root-turn binding. Three additional bound searches cannot be joined to a surviving checkpoint. They remain valid operational evidence but cannot support a strict host comparison.

### Aggregate utility

All existing labels are `agent_explicit`; there are no `user_explicit` or evaluator assessments.

| Label | Count | Denominator | Rate |
|---|---:|---:|---:|
| Useful (`decisive` or `orienting`) | 36 | 56 labelled | 64.3% |
| Decisive | 16 | 56 labelled | 28.6% |
| Orienting | 20 | 56 labelled | 35.7% |
| Unused | 18 | 56 labelled | 32.1% |
| Duplicate | 2 | 56 labelled | 3.6% |
| Current quality | 35 | 37 quality-labelled | 94.6% |
| Contradictory quality | 2 | 37 quality-labelled | 5.4% |
| Stale quality | 0 | 37 quality-labelled | 0% |

This is promising but selection-biased. Unlabelled exposure is unknown, not unused or useful. As a conservative lower bound, only 36 of 109 unique exposed pairs (33.0%) are confirmed useful; that is not an unbiased utility rate.

### Performance

| Operation | Events | p50 | p95 |
|---|---:|---:|---:|
| Search | 80 | 2 ms | 11 ms |
| Full fetch | 22 | 0 ms | 1 ms |

Store performance is healthy. Agent-level time-to-useful has a 9 ms p50 but a 16.2 s p95 across 26 complete samples; this includes agent/tool sequencing and has 13 unknown samples. The tail is an orchestration issue, not a database latency issue.

## Codex

### Coverage

Since 2026-08-30, native Codex storage contains 361 sessions: 165 root sessions and 196 subagent sessions. The checkpoint protocol applies only to root agents.

The checkpoint ledger contains:

| Measure | Codex |
|---|---:|
| Checkpoint-attributable sessions | 93 |
| Checkpointed root turns | 324 |
| Saved | 199 |
| Needs review | 13 |
| Skipped | 112 |
| Sessions with bound Recall | 20 |
| Turns with bound Recall | 35 |
| Bound Recall searches | 43 |
| Result exposures | 110 |
| Empty searches | 12 |
| Full-content fetches | 14 |

Recall reached 20 of 93 checkpoint-attributable sessions (21.5%) and 35 of 324 checkpointed root turns (10.8%). Relative to all 165 native root sessions in the same period, the observable session-level Recall coverage is at most 12.1%. The native denominator includes sessions before complete installation and sessions that may not have needed Memory, so it is a coverage bound rather than a false-negative rate.

All 56 feedback labels map to Codex-attributable turns. Codex therefore owns the aggregate labelled-utility result: 36 useful, 18 unused, 2 duplicate, 35 current, and 2 contradictory.

### Version trend

The current Engram 3.4.0 / Protocol 2 subset attributable to Codex has 19 searches, 63 exposures, and 4 empty runs. Earlier attributable protocol-1 subsets account for 24 searches, 47 exposures, and 8 empty runs. The current path returns more candidates and fewer empty runs proportionally, but the sample is small and tasks differ; this is not a causal version comparison.

### Current integration health

`engram setup status codex --json` reports:

- mode `partial_plugin`;
- CLI, plugin, MCP, activation cue, and Stop verifier ready;
- canonical plugin skill ready, with an additional leftover standalone skill;
- overall Protocol compatibility `incompatible` because the Managed Pack declaration is missing;
- lifecycle canary disabled and lifecycle metrics not observed.

### Codex verdict

**Status: useful but under-activated and only partially measured.**

Codex provides the strongest evidence that Engram can retrieve valuable Memory. The conditional usefulness rate is high enough to continue the canary. It does not prove broad task utility because Recall occurs in roughly one fifth of attributable sessions, half of exposures are unlabelled, and all judgements come from the same agent that performed the task.

## Cursor Agent

### Native transcript coverage

The local Cursor project store contains 51 Agent transcripts across three projects. Tool-call metadata shows:

| Measure | Cursor |
|---|---:|
| Transcripts using any `mem_*` tool | 15 |
| Transcripts using `mem_search` | 11 |
| `mem_search` calls | 23 |
| Transcripts fetching complete Memory | 2 |
| `mem_get_observation` calls | 3 |
| Transcripts calling `mem_checkpoint` | 5 |

Recall appeared in 11 of 51 transcripts (21.6%). Only four search calls across three sessions carried complete `host + session_id + root_turn_id` identity. All four identified `host=cursor`.

### Attributable ledger

The checkpoint ledger contains 17 Cursor root turns across 7 session identities: 11 saved and 6 skipped. One session identity is literally `unknown`, which prevents a clean transcript join.

Only one bound Cursor search joins to a surviving checkpoint; it returned zero results. Three additional Protocol-1 / Engram 3.3.1 bound searches are empty and match the other complete Cursor identities, but lack a surviving checkpoint join. Taken together, the native calls and Recall ledger show four current-style Cursor searches and four empty results.

No Cursor exposure has explicit Recall feedback. Empty searches also have no false-empty review, so they cannot be classified as correct abstention or retrieval failure.

### Qualitative utility

The three full fetches occurred in two sessions:

1. An Engram-project request asked for the latest saved Memory. Cursor searched, fetched the release Memory, and correctly answered with the v3.3.1 release, merge, and upgrade details. This is direct, task-relevant utility.
2. An Alina implementation session searched project Memory and fetched two observations before inspecting payment, reservation, and handover code. The final implementation completed with a detailed acceptance mapping and passing integration/E2E evidence. The recalled material was plausibly orienting, but the transcript does not establish the counterfactual that the task would have been worse without it.

These episodes prove that Cursor can use Engram, but two selected sessions cannot estimate a utility rate.

### Current integration health

`engram setup status cursor --json` reports:

- mode `partial`;
- MCP and user hooks ready;
- installed plugin 3.3.1 stale relative to the running Engram 3.4.0 identity;
- installed plugin skill stale relative to the current editorial rubric;
- an additional customized legacy user skill;
- Cursor User Rules unobservable.

### Cursor verdict

**Status: technically functional, operationally stale, and not quantitatively proven.**

Cursor occasionally recalls useful Memory, but the current identity-bound path has only empty results, checkpoint coverage is incomplete, and no utility or false-empty feedback is recorded. The immediate problem is observability and installation coherence before retrieval quality can be judged.

## Cross-host diagnosis

| Dimension | Codex | Cursor | Overall |
|---|---|---|---|
| Store latency | Healthy | Healthy | Not the bottleneck |
| Recall activation | Low | Low | Primary weakness |
| Result utility when labelled | Promising | No labels | Inconclusive cross-host |
| Full-content selection | Present | Rare | Progressive disclosure works |
| False-empty evidence | Missing | Missing | Major blind spot |
| Checkpoint/turn attribution | Partial | Weak | Prevents complete evaluation |
| Installation health | Partially incompatible | Stale/partial | Confounds comparisons |
| Causal task improvement | Not measured | Not measured | Unproven |

The existing Recall feedback report is therefore a **Codex-biased conditional-precision measure**, not a product-wide utility metric.

## Recommended next actions

1. **Restore a comparable installation tuple before another study.** Repair the missing Codex Managed Pack declaration and update Cursor's plugin/skill to the running release. Do not compare hosts while their Protocol tuples differ.
2. **Require root binding on every root-agent search.** Unbound Recall should remain supported for compatibility, but it should be excluded visibly from utility claims.
3. **Record feedback for every exposed result and every reviewed empty run.** The immediate targets are at least 80% label coverage and explicit false-empty assessment. Add user/evaluator labels so agent self-assessment is not the only source.
4. **Measure activation opportunities.** Define when prior Memory could have changed a task, then score whether the host searched. Session coverage alone cannot distinguish correct abstention from missed Recall.
5. **Run a prospective paired Codex/Cursor study.** Use the same projects and future tasks, compare no Recall vs current Recall, and measure task success, correction burden, latency, noise, false empties, and stale/contradictory use.
6. **Keep the current rollout at canary/partial.** The data justifies continued use and instrumentation, not a claim of reliable autonomous memory.

## Reproducibility notes

The audit used these maintained surfaces:

- `engram recall-feedback report --json`
- `engram recall-baseline report --json`
- `engram setup status codex --json`
- `engram setup status cursor --json`
- read-only queries over `memory_checkpoints`, `recall_runs`, `recall_results`, `recall_segments`, and Recall-feedback tables
- HMAC reconstruction using the local Recall-feedback key and the exact Core `turn` digest format, solely to join raw checkpoint identities to content-free Recall attribution keys
- Codex session metadata under `~/.codex/sessions` and `~/.codex/archived_sessions`
- Cursor tool-call metadata under `~/.cursor/projects/*/agent-transcripts`

No database row, agent transcript, plugin installation, or configuration was modified.
