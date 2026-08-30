# Task Brief generic recall without weakening precision

**Status:** historical research; proposal not adopted. [ADR-0009](../adr/0009-retire-task-briefing.md)
retired Task Brief instead. The recommendations and implementation phases below
record the pre-decision alternative and are not current product guidance.

**Date:** 2026-08-29

**Examined base:** `2de1aafdb63e3d35c5e478ab024ab1cd40c13d85`
(`main`, Engram v2.3.0)

## Executive conclusion

Task Brief's remaining weakness is not one problem but three related ones:

1. the caller supplies one free-form `Task intent`, but the core also treats its
   first 12 unique tokens as a retrieval query and gate denominator;
2. the Task gate requires half of all retained non-workflow terms even when a
   smaller cohesive phrase identifies the actual subject; and
3. fallback anchors are selected by signal and appearance order, so branch names
   and opening verbs can displace the subject terms.

The reproduced false empty is primarily a **Task-gate false negative**, not a
failure to find the Memory. The target Memory was present at positions 11 and 12
of the corresponding 20-result FTS task queries, but the Task gate rejected every
retrieved candidate. A compact intent, `Task Brief generic recall fallback`,
selected the target Memory with the current implementation.

This supports a staged response:

1. immediately teach the agent skill to pass a compact, subject-oriented Task
   intent rather than restating the complete user instruction;
2. add a versioned generic-recall corpus before changing selection;
3. add bounded cohesive lexical retrieval and, only if the corpus still requires
   it, a conservative cohesive-evidence path through the Task gate;
4. make caller-provided retrieval anchors an explicit input if cross-language or
   paraphrase recall is a product requirement, rather than hiding translation or
   LLM ranking inside the core; and
5. make fallback anchors prefer Task identity and cohesive Task subject evidence
   over Repository signals and opening verbs.

The proposal keeps Task Brief local, deterministic, explainable, precision-first,
and dependency-free. It does not add embeddings, hidden searches, recency as
relevance, or persistence of Task/Repository signals.

## Reproducible evidence

### Isolated Task-only comparison

The following runs used the installed Engram v2.3.0 and the same local store from
`/tmp`, with explicit project and scope. Running outside a Git checkout removes
Repository ranking evidence; the expected `repository_project_unresolved`
diagnostic is therefore a control, not a failure.

| Task input | Result | Pipeline | Fallback |
| --- | --- | --- | --- |
| `Research and propose how to improve Task Brief retrieval so generic task descriptions recover relevant project memories without requiring distinctive identifiers or manual fallback searches` | No Memory | 24 unique terms, first 12 analyzed; 20 retrieved; 20 Task-gate rejections; 0 qualified | `research propose how`; executing it with `all` returned 0 results |
| `Investiga y propón cómo mejorar Task Brief: recupera bien memorias con identificadores o términos distintivos pero falla con tareas genéricas; el briefing inicial quedó vacío y el fallback encontró la memoria relevante` | No Memory | 28 unique terms, first 12 analyzed; 20 retrieved; 20 Task-gate rejections; 0 qualified | `investiga propón cómo`; executing it with `all` returned 0 results |
| `Task Brief generic recall fallback` | Target Memory #20477 selected | 20 retrieved; 18 Task-gate rejections; 2 qualified | No fallback was needed |

The target was:

```text
#20477 Independent fork value is concentrated in Codex checkpoints and CLI parity
```

Its durable content says that Task Brief is useful for exact intents but still
needs search fallback. The compact Task matched `task`, `brief`, and `fallback`
in that Memory's content and passed the existing gate.

These are point-in-time operational results, not stable fixture IDs or counts.
The store changes as Memories are added, so only a checked-in corpus can become a
regression contract. The exact commands are retained below so the shape can be
rechecked:

```bash
(
  cd /tmp
  engram context engram --brief \
    --task 'Research and propose how to improve Task Brief retrieval so generic task descriptions recover relevant project memories without requiring distinctive identifiers or manual fallback searches' \
    --scope project --limit 5 --json

  engram context engram --brief \
    --task 'Investiga y propón cómo mejorar Task Brief: recupera bien memorias con identificadores o términos distintivos pero falla con tareas genéricas; el briefing inicial quedó vacío y el fallback encontró la memoria relevante' \
    --scope project --limit 5 --json

  engram context engram --brief \
    --task 'Task Brief generic recall fallback' \
    --scope project --limit 5 --json
)
```

### Candidate retrieval did find the target

Running the Store-equivalent `any` searches over each retained 12-term Task
prefix placed #20477 within the 20-result retrieval bound:

| Query language | Target position | Consequence |
| --- | ---: | --- |
| English | 11 | Candidate retrieval did not omit the target |
| Spanish | 12 | Candidate retrieval did not omit the target despite the language mismatch |

The English Task retained ten terms that the current static workflow list treats
as distinctive and therefore required five units of Task evidence. #20477
matched only `task` and `brief`. The Spanish Task treated nearly every retained
Spanish word as distinctive because the workflow-term list is English-only; it
required six units and again matched only `task` and `brief`.

This distinguishes the demonstrated cause from a nearby risk:

- **Demonstrated:** the target reached the candidate union and the Task gate
  rejected it before ranking.
- **Possible under other inventories:** an `any` query may fill its 20-result
  bound before a relevant candidate arrives. The diagnostics correctly report
  that count as incomplete, but this was not the target's omission mechanism in
  the reproduced case.

### Contracts that already work

- [ADR-0003](../adr/0003-task-briefing-from-transient-signals.md)
  requires local, deterministic, explainable, precision-first selection and
  keeps transient signals out of Memories.
- [Issue #59](https://github.com/yersonargotev/engram/issues/59), delivered by
  [PR #75](https://github.com/yersonargotev/engram/pull/75), correctly made exact
  PR, issue, branch, commit, path, and topic identity stronger than generic
  workflow prose.
- [Issue #60](https://github.com/yersonargotev/engram/issues/60), delivered by
  [PR #76](https://github.com/yersonargotev/engram/pull/76), correctly added
  bounded pipeline attribution and a structured, advisory fallback without
  exposing rejected Memory bodies or executing a hidden second search.
- `go test ./internal/taskbriefing ./cmd/engram -count=1` passes on the examined
  base.

The remaining problem does not invalidate #59 or #60. Their attribution makes
this false negative diagnosable.

## Root cause by pipeline stage

| Stage | Current behavior | Effect on generic Task intent |
| --- | --- | --- |
| Task representation | `collectTerms` lowercases, deduplicates, and retains the first 12 alphanumeric terms ([terms.go](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/terms.go#L18-L86)). The same bounded prefix drives retrieval and the gate. | A complete work instruction becomes a positional bag of words. Opening verbs can occupy the bound while a small subject phrase carries most of the useful meaning. |
| Generic-term classification | `distinctiveTerms` removes a fixed English workflow vocabulary ([generator.go](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/generator.go#L936-L945)). | The list is brittle across domains and languages. Spanish connectors and request verbs inflate the gate denominator even when they cannot match English Memories. |
| Retrieval | Every signal group calls Store search with `MatchMode: "any"` and limit 20, then unions observations while discarding Store rank ([generator.go](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/generator.go#L374-L397)). Store uses FTS5 OR semantics and BM25 ordering ([store.go](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/store/store.go#L3703-L3792)). | Broad terms can saturate a lane. In the reproduced case, however, BM25 still placed the target inside the bound, so changing only the retrieval limit would not fix the observed empty result. |
| Task gate | A Task candidate must match at least half the strength of all exact identifiers plus all retained distinctive terms ([generator.go](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/generator.go#L611-L629)). | Two adjacent subject terms such as `task brief` cannot qualify a Memory when unrelated request prose raises the denominator to five or six. |
| Ranking | Only gate-qualified candidates receive signal weight, match-strength contribution, title/topic bonus, and pin boost ([generator.go](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/generator.go#L505-L603)). | BM25 position and final ranking cannot rescue a target rejected earlier. This is why tuning current ranking weights alone is insufficient. |
| Fallback | Fallback takes identifiers from all signals, then the first distinctive terms, and runs `all` search ([generator.go](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/generator.go#L416-L453)). | `main research propose`, `research propose how`, and `investiga propón cómo` are structurally safe but topically poor. All three reproduced fallback shapes returned no target; the two Task-only variants returned no results at all. |

## Design constraints

The proposal applies these constraints before considering implementation:

1. `Task intent` remains the authoritative description of the current work.
2. Repository signals may reinforce Task relevance but cannot manufacture it.
3. A result bound remains a cap, never a quota.
4. Exact identifiers remain stronger than lexical evidence.
5. No single broad term may qualify a Memory.
6. The core must not translate, call an LLM, or execute fallback implicitly.
7. Every derived query, gate path, omission, and rank contribution remains
   deterministic and attributable.
8. Task intent, derived units, caller anchors, and Repository evidence remain
   transient and local.
9. Existing complete-Memory output, lifecycle, scope, privacy, and 4,096-byte
   contracts remain intact.

## Alternatives

| Alternative | Value | Limitation | Decision |
| --- | --- | --- | --- |
| Pass a compact Task intent from the agent skill | Already selected #20477 with no core change; smallest reversible improvement | Overloads `Task intent` as both work description and retrieval query; quality depends on the caller | Adopt immediately as a measured mitigation |
| Increase candidate/result/output limits | May hide some bounded misses | Does not fix a candidate rejected by the gate; increases noise and cost | Reject as a primary fix |
| Add more static stop words | Removes known English or Spanish request verbs | Language/domain-specific and cannot identify useful phrases | Use only for proven corpus negatives, not as the design |
| Add phrase/cohesive lexical lanes | Uses existing local FTS5, preserves explanation, and gives subject combinations a path independent of broad OR retrieval | Needs bounded query construction and a calibrated gate | Recommended core direction |
| Preserve per-lane FTS rank | Reuses an existing relevance signal that Task Brief currently discards | Raw BM25 values are corpus-dependent and do not solve a pre-ranking gate by themselves | Evaluate ordinal rank only after cohesive retrieval |
| Add explicit caller-provided retrieval anchors | Separates work description from lexical lookup and lets an agent bridge paraphrase or language without a model in the core | Extends the public contract and must not contradict exact Task identity | Recommended if cross-language/paraphrase recall is required |
| Add embeddings or a reranker | May bridge wider semantic gaps | Adds model/index lifecycle, dependencies, opacity, and new privacy/versioning concerns | Defer until lexical held-out evidence shows a ceiling |

SQLite already provides phrase, prefix, `NEAR`, Boolean queries, BM25 with
inverse-document-frequency, and index vocabulary access in FTS5
([official FTS5 documentation](https://www.sqlite.org/fts5.html)). Its Porter
stemmer is explicitly English-specific, so changing the tokenizer is not a
general multilingual solution. If later evaluation combines multiple independent
ranked lanes, Reciprocal Rank Fusion is a simple rank-only candidate, but it
should be treated as a calibrated option rather than assumed necessary
([Cormack, Clarke, and Buettcher, SIGIR 2009](https://cormack.uwaterloo.ca/cormacksigir09-rrf.pdf)).

## Recommended phases

### Phase A — improve the caller before changing the core

Update `skills/engram-memory-cli/SKILL.md` so the Task passed to `--task` is a
compact description of the retrieval subject, not a copy of the complete user
instruction:

- preserve exact issue, PR, branch, commit, path, and topic identifiers first;
- prefer domain nouns and noun phrases over control/request verbs;
- keep the Task short enough that its subject is not displaced by the 12-term
  bound;
- do not invent identifiers or facts; and
- continue to inspect diagnostics and execute only the structured fallback when
  one is supplied.

This is a mitigation, not the final domain model. It should be measured with
paired verbose/compact cases because a bad compact query can still select an
unrelated Memory.

### Phase 0 — freeze evaluation before changing selection

Create a separate v2 corpus with at least 24 curated cases:

- at least 8 generic positive pairs, each with verbose and compact forms;
- at least 8 negative workflow cases sharing one or two broad terms;
- English, Spanish, and cross-language cases;
- existing exact-identifier regressions from #59;
- truncation, scope, lifecycle, output-budget, and privacy cases; and
- explicit cases in which no lexical overlap exists, which must remain empty
  unless caller anchors are supplied.

Split calibration and held-out cases before tuning. Compare baseline, caller
mitigation, cohesive retrieval, and cohesive gate on the same fixtures. Report
exact numerators/denominators for recall@5, precision@5, false-empty rate,
false-inclusion rate, fallback success, candidate arrival, gate passage, and
retrieval completeness. Repeat each case three times and require byte-identical
JSON after removing no fields.

### Phase 1 — add bounded cohesive lexical retrieval

Derive a small private set of cohesive units only from the already-retained Task
prefix:

1. existing typed identifiers;
2. up to three ordered two- or three-term phrases/pairs; and
3. the existing broad `any` bag as the final lane.

Run lanes in fixed order under one visible global candidate budget: identity,
cohesive phrase/AND, then broad OR. Record the strategy, limit, returned lower
bound, and completeness for every lane. Reuse FTS5 through a private Store API
that accepts structured query intent; do not expose raw FTS syntax to
`internal/taskbriefing`, and do not change public `engram search` semantics.

Phase 1 must retain the current gate. Its purpose is to measure whether candidate
arrival alone fixes held-out cases. The reproduced #20477 case already arrived,
so this phase is not expected to fix that specific gate failure by itself.

### Phase 2 — add a conservative cohesive Task-gate path if required

Only if Phase 1 held-out results still contain false empties whose target reached
the candidate union, let a candidate satisfy Task relevance through either:

- the current exact-identifier/distinctive-strength rule; or
- one complete two- or three-term cohesive Task unit matched within the same
  Memory field.

The alternate path must never pass on one term, generic workflow evidence alone,
or Repository evidence alone. Exact identifiers keep greater strength. The
historical inclusion threshold, per-field contribution bound, lifecycle rules,
pin behavior, and deterministic tie-breakers remain unchanged until separate
evidence justifies changes.

Expose bounded evidence such as `task_identifier`, `task_cohesive`, or
`task_broad`, plus matched counts. Do not emit raw Task prose, raw Repository
content, or rejected Memory bodies.

### Phase 3 — separate optional retrieval anchors from Task intent

If cross-language or paraphrase recall is a product requirement, add a proposed
domain concept, **Retrieval anchor**: a bounded, caller-supplied lexical hint used
to retrieve and explain candidate Memories for one Task briefing. It is not a
Memory or Repository signal and does not replace Task intent.

A possible additive contract is repeated `--task-anchor` flags and a matching
structured core/MCP field. Bound the count and total terms; expose anchor
provenance in Selection evidence; never persist anchors. Exact identifiers in
Task intent are authoritative if an anchor conflicts. The core does not generate
or translate anchors: an agent caller may provide `task brief`, `generic recall`,
or another project-language phrase explicitly.

Do not add this term to `CONTEXT.md` until the product requirement and semantics
are accepted. Until then it is proposal vocabulary, not a settled domain concept.

### Phase 4 — make fallback subject-directed

Keep fallback advisory and read-only. Select anchors in this order:

1. exact Task identifiers;
2. explicit Retrieval anchors, if the contract exists;
3. cohesive Task units;
4. remaining distinctive Task terms; and
5. Repository identifiers/terms only when no Task anchor is usable.

Named generic branches such as `main` and request verbs must not consume the
three-anchor bound before Task subject evidence. If no defensible Task anchor
exists, omit the command and report a typed `no_distinctive_fallback_anchors`
reason instead of recommending a predictably empty search.

## Component-level change map

| Area | Proposed ownership | Preserved boundary |
| --- | --- | --- |
| `skills/engram-memory-cli/SKILL.md` | Construct and validate a compact Task subject; later provide explicit anchors if accepted | The agent interprets language; the core stays deterministic |
| `internal/taskbriefing/terms.go` | Derive bounded cohesive units from retained Task terms | Never read or retain the truncated tail |
| `internal/taskbriefing/generator.go` | Own retrieval lanes, global candidate budget, Task gates, ranking evidence, and fallback policy | Store remains a search primitive; CLI remains rendering |
| `internal/store` | Execute safe structured phrase/AND/OR FTS queries and return rank/order metadata | No Task-Brief business rules and no change to public search semantics |
| Result model | Add `retrievals[].strategy`, global budget accounting, and bounded cohesive/anchor evidence | Additive JSON, deterministic trimming, 4,096-byte stdout |
| `cmd/engram/context_briefing.go` | Render new attribution fields and preserve budget ordering | No relevance decisions |
| Corpus/tests | Compare verbose/compact, cohesive, multilingual, negative, and no-overlap cases | Keep all v1 scenarios unchanged and green |

This ownership follows ADR-0003: `internal/taskbriefing` owns selection, Store
owns local searchable inventory, and the CLI owns output rendering/budgeting.

## Acceptance criteria

1. Every v1 scenario retains its selection, ordering, diagnostics, privacy, and
   complete-Memory output behavior.
2. A versioned v2 corpus freezes paired verbose/compact generic tasks, negative
   workflow cases, English/Spanish cases, and lexical no-overlap controls before
   weights or thresholds change.
3. The fixture equivalent of #20477 is selected for both verbose English and
   verbose Spanish Tasks when `task brief` is the covered cohesive unit, without
   selecting unrelated client/video briefs that merely contain `brief`.
4. No negative fixture qualifies through one term, a request verb, a generic
   branch, or Repository evidence alone.
5. A cross-language fixture with no shared lexical subject remains an explained
   empty result without explicit anchors and may select only when bounded caller
   anchors are supplied. The core never claims automatic translation.
6. Every retrieval lane and the global union report limit, returned lower bound,
   completeness, strategy, and budget omissions. Unknown totals never become
   known zeros.
7. Fallback never chooses `main`, `research propose how`, or
   `investiga propón cómo` while a usable Task subject unit exists.
8. Human and JSON output remain within 4,096 bytes and never expose raw diff,
   untracked-file content, full Task prose, or rejected Memory bodies.
9. Identical normalized inputs and fixtures produce byte-identical JSON across
   three runs.
10. `go test ./internal/taskbriefing ./cmd/engram -count=1` and the full relevant
    test suite pass; before/after isolated store checks show no Memory, prompt,
    signal, or remote-index writes.
11. Held-out recall@5 improves over baseline while precision@5 and negative-case
    false-inclusion counts do not regress. Publish counts and intervals, not only
    percentages; choose minimum improvements only after the baseline is frozen.

## Recommended decision

Adopt Phase A and Phase 0 first. They are low-risk and the compact-query result
shows immediate value. Then implement Phase 1 and run the frozen comparison.
Proceed to Phase 2 only if held-out evidence confirms candidate-arrived/gate-failed
false empties. Add Phase 3 only if cross-language/paraphrase recall is an explicit
requirement; otherwise cohesive lexical selection is the simpler boundary. Phase
4 should accompany whichever core phase first changes subject representation.

Do not add embeddings, increase broad limits, or relax the Task gate globally as
the first response. The present evidence does not justify their complexity or
precision cost.

## Primary sources

- [ADR-0003: Generate task briefings from transient work signals](../adr/0003-task-briefing-from-transient-signals.md)
- [Issue #59: prioritize exact task identifiers](https://github.com/yersonargotev/engram/issues/59)
  and [PR #75](https://github.com/yersonargotev/engram/pull/75)
- [Issue #60: expose rejection diagnostics and fallback](https://github.com/yersonargotev/engram/issues/60)
  and [PR #76](https://github.com/yersonargotev/engram/pull/76)
- [Current Task Brief generator](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/generator.go)
- [Current bounded tokenizer](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/terms.go)
- [Current Store FTS search](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/store/store.go#L3622-L3792)
- [Executable v1 corpus](https://github.com/yersonargotev/engram/blob/2de1aafdb63e3d35c5e478ab024ab1cd40c13d85/internal/taskbriefing/prototype/testdata/v1/scenarios.json)
- [SQLite FTS5 official documentation](https://www.sqlite.org/fts5.html)
- [Cormack, Clarke, and Buettcher, *Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods*, SIGIR 2009](https://cormack.uwaterloo.ca/cormacksigir09-rrf.pdf)
