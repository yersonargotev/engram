# Authored discovery and supersession interact within the candidate budget

Disposition: **editorial/lifecycle correction indicated** for this synthetic
corpus. No production ranking change is implemented or authorized. The remaining
task-language gap warrants a separately approved investigation; it is not proof
of a ranking defect, a requirement for embeddings, or a reproduced private corpus.

The frozen evaluator at `abf96cf75de0de784eaaeeebc1d6579bc126715f`
ran Core from `5855e1de2a77cd07dbea67a901752b26cd5e9a7f`, Protocol 2,
on Darwin/arm64 with Go 1.27.1. `publication.json` records the executable SHA-256,
contract commitment, exact evaluator revision and aggregate measurements.
All 36 calibration rows passed prerequisites before held-out opened; all 36
held-out rows completed. Each row has two searches, so the execution made 144
bounded calls across six distinct synthetic needs. These are not 72 independent
samples. No selected-content request, real agent task or explicit utility/quality
label was collected.

Held-out acceptable-set successes (a set containing the resolution), first search
and after the single frozen reformulation:

| Variant | Identifier | Concept | Task paraphrase |
| --- | --- | --- | --- |
| Baseline | 3/3 → 3/3 | 0/3 → 0/3 | 0/3 → 0/3 |
| Editorial only | 3/3 → 3/3 | 0/3 → 0/3 | 0/3 → 0/3 |
| Supersession only | 3/3 → 3/3 | 0/3 → 0/3 | 0/3 → 0/3 |
| Editorial + supersession | 3/3 → 3/3 | 3/3 → 3/3 | 0/3 → 0/3 |

Identifier coverage of both applicable Memories was 100%. Baseline concept
coverage was 50%: every first response was nonempty and found the older invariant,
but omitted the resolution. Combined editorial and structural correction raised
concept coverage to 100%. Supersession alone removed the three stale diagnosis
exposures in concept searches but did not add resolution coverage. Editorial
wording alone did not overcome the bounded selection in this corpus. This
interaction is why neither a recency preference nor a wording-only fix follows
from the initial observation.

All three held-out first task paraphrases were empty, and the prescribed
reformulation did not discover useful knowledge. This differs from the nonempty
concept sets missing the resolution. The latter were partially useful, so they
are not counted as `nonempty_missing_useful` (which means zero relevant-current
keys); their missing answer is visible in acceptable-set and coverage metrics.
No reformulation rescued an acceptable set in this held-out sample. Cumulative
exposure still accounts for both calls, including repeated candidates and
historical deliveries sharing one duplicate group.

The publication provides each cohort/variant/query class's coverage, reciprocal
rank, exposure denominators, noise/stale/duplicate counts, bytes, latency and
candidate-discovery latency. Task time-to-useful remains unavailable: search
latency is not observed downstream utility. Every delivered candidate has unknown
utility and quality, zero explicit assessments/disagreements, and an explicit
`retrieval_only` omission. Synthetic relevance judgments do not become agent
feedback labels or a reviewed false-empty judgment.

At n=3, Wilson 95% intervals for acceptable-set success are approximately
43.85–100% for 3/3 and 0–56.15% for 0/3. They are descriptive uncertainty markers,
not population estimates for this purposive sample. Coverage/rank are descriptive
means. Latency depends on this machine, build and cache state; it excludes Store
setup, reading and task execution. There is no language comparison: Spanish was
explicitly omitted to isolate the English observation. Private Packy data was
neither required nor inspected. No conclusion changes the frozen #109/#110 study
or its `continue_canary` disposition.

Reproduce with the clean-checkout commands in [README.md](README.md). Compare
counts, denominators and disposition; timing and executable hashes can vary with
the build platform. A different corpus, judgment, query or metric policy requires
a new version rather than editing this result.

This v2 publication replaces v1 as delivery evidence. v1 artifacts remain archived unchanged. Review corrections added code commitments, explicit unavailable-evidence accounting and numeric report tests; v2 was frozen before evaluation and used a fresh independently authored held-out cohort. Its held-out findings agree with v1, while the revised calibration reformulations exercise successful cumulative discovery.
