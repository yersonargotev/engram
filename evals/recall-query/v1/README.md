# Concept versus identifier Recall v1

This independent, synthetic diagnostic study implements issue #154. Its contract
freezes both corpus digests, authored relevance/applicability judgments, acceptable
sets, query/reformulation policy, factorial variants, metrics and disposition
rules before evaluated execution. `internal/recallquery` contains the compiled
contract trust anchor. Changes to those inputs require a new study version.

Three calibration needs and three separately authored held-out needs compare
English identifiers, concepts and task paraphrases. Each receives exactly one
preregistered same-intent reformulation. Baseline/editorial wording is crossed
with absent/present structural supersession. The 36 rows per cohort are repeated
measurements of three needs, not 36 independent samples. Held-out input content
must not be inspected during calibration or after a prerequisite failure.

The direct Core consumer is `cmd/recall-query-eval`. It uses no host/plugin/model,
private corpus, telemetry, Content capture, network or cloud integration. Every
row creates and removes its own local Store. Candidate queries retain explicit
project authority, project scope, all-term matching, five results, 4096 candidate
JSON bytes and no history. There are no selected-content requests, continuations,
or downstream task observations. Dates are authored fixture facts; relevance is
not inferred from age or active review state.

From a clean checkout at the evaluator commit:

```sh
go test ./internal/recallquery ./internal/recallstudy
go build -o /tmp/engram-recall-query-eval ./cmd/recall-query-eval
/tmp/engram-recall-query-eval -verify
/tmp/engram-recall-query-eval > /tmp/recall-query-calibration.json
/tmp/engram-recall-query-eval -held-out > /tmp/recall-query-publication.json
```

`-verify` reads only contract and calibration. The default executes only
calibration. `-held-out` independently reruns calibration and opens the reserved
file only after all 36 rows pass operational, authority, budget and lifecycle
checks. Quality misses do not block the next cohort. Contract/fixture tampering
fails visibly. Failed execution produces aggregate unavailable-quality evidence;
no operational failure becomes an empty query or a fabricated utility judgment.
The executable requires a clean compiled Git revision and verifies the tested
Core paths against the frozen source revision. Build into `/tmp` to keep the
checkout clean. No installed Compatibility tuple is implied.

`first_search` and `post_reformulation` expose separate denominators, coverage,
acceptable-set successes, mean reciprocal rank, empty/nonempty misses, repeated
candidate exposure, noise/stale/duplicate counts, bytes and latency. Cumulative
coverage deduplicates keys, while exposure counts retain every occurrence.
History duplicates share one equivalence group. The interval around acceptable
success is descriptive Wilson 95% with three needs per cohort/variant/class;
coverage/rank are descriptive means without inferential intervals. Never pool
variants into an inflated sample size. `mean_candidate_discovery_latency_ms` is
only measured search time through first relevant exposure; actual task
time-to-useful is unavailable. All utility/quality labels are unknown, with
explicit `retrieval_only` omission and zero assessments/disagreements.

Spanish cases are omitted in v1 to isolate the original English observation;
this does not evaluate multilingual support. The small purposive synthetic sample
cannot establish a general ranking defect or reproduce the private Packy corpus.
Any ranking investigation needs separate approval. The frozen exposure-strategy
study under `evals/recall-study/v1` remains unchanged, including its invalid
calibration, unopened held-out cohort and `continue_canary` publication.
