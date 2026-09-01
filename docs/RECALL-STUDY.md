# Paired Recall study

The paired Recall study is a frozen, privacy-safe Codex evaluation contract. It
compares three treatments on matched self-contained task blocks:

- broad chronological injection;
- cue-only, agent-initiated targeted Recall;
- cue-only with no Recall.

It is evaluation tooling, not a product switch. The contract keeps
`default_recall_enabled` and `automatic_rollout_enabled` false, and no command
changes those values.

## Frozen v1 surface

The immutable artifacts live in `evals/recall-study/v1/`:

- `contract.json` and `contract.sha256` freeze the model, repository, task
  protocol, label rubric, revisions, sample size, confidence intervals, and all
  general-availability gates;
- `calibration/manifest.json` and its sidecar allocate blocks 1–60;
- `held-out/manifest.json` and its sidecar allocate blocks 61–517.

Every block is paired across all three treatments. The power analysis freezes
517 blocks per treatment and 1551 total cells. Calibration and held-out
metadata use separate namespaces and seeds, and their numeric ranges cannot
overlap.

The source snapshot is
`105778d820029a2326043739fd676647e5c037f6`. The contract separately records
Managed Pack 3.3.0, binary 3.0.0, Codex plugin 0.1.7, Protocol 1,
`recall-baseline-events-v1`, `diagnostic-capture-v1`, `recall-policy-v1`, and
`recall-study-metrics-v1`, all at that revision. The frozen runner uses Codex
0.152.0 with `gpt-5.6-luna` at low reasoning effort against fresh ephemeral
checkouts.

Changing any frozen byte requires a new study version. Do not update v1 after
calibration or held-out results become visible.

## Local evidence files

Compatibility and consent evidence are supplied locally and are ignored by
Git. A Compatibility file has this shape:

```json
{
  "revisions": {
    "managed_pack": {"version": "3.3.0", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "engram_binary": {"version": "3.0.0", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "codex_plugin": {"version": "0.1.7", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "protocol_contract": {"version": "1", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "telemetry_schema": {"version": "recall-baseline-events-v1", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "capture_schema": {"version": "diagnostic-capture-v1", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "policy": {"version": "recall-policy-v1", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "metric": {"version": "recall-study-metrics-v1", "revision": "105778d820029a2326043739fd676647e5c037f6"},
    "source": {"version": "105778d820029a2326043739fd676647e5c037f6", "revision": "105778d820029a2326043739fd676647e5c037f6"}
  },
  "ready": true
}
```

Consent must match the study and carry a SHA-256 commitment. A missing grant
causes verification to fail before any task evidence is collected:

```json
{
  "study_id": "codex-useful-recall",
  "study_version": "v1",
  "calibration_granted": true,
  "held_out_granted": true,
  "proof_sha256": "<64 lowercase hexadecimal characters>"
}
```

These files prove authorization and provenance; they are not committed study
results.

## Commands

Every command takes the same verified metadata:

```bash
COMMON=(
  --contract evals/recall-study/v1/contract.json
  --contract-hash evals/recall-study/v1/contract.sha256
  --calibration-manifest evals/recall-study/v1/calibration/manifest.json
  --calibration-hash evals/recall-study/v1/calibration/manifest.sha256
  --held-out-manifest evals/recall-study/v1/held-out/manifest.json
  --held-out-hash evals/recall-study/v1/held-out/manifest.sha256
  --environment evals/recall-study/v1/environment.json
  --consent evals/recall-study/v1/consent.json
)

engram recall-study verify "${COMMON[@]}" --json
engram recall-study dry-run "${COMMON[@]}" --json
engram recall-study calibrate "${COMMON[@]}" \
  --output evals/recall-study/v1/calibration/run-plan.json --json
```

`verify` and `dry-run` read only frozen metadata. `calibrate` writes a private
`0600` plan for the 180 calibration cells. None accepts a held-out task-input
flag. `run-held-out` is the sole mode that can authorize the private 1371-cell
held-out plan, and only after the same tuple and consent checks:

```bash
engram recall-study run-held-out "${COMMON[@]}" \
  --output evals/recall-study/v1/held-out/run-plan.json --json
```

The v1 command writes protocol identities and authorization state; it does not
open task inputs or execute Codex. Study execution and the immutable disposition
belong to the next delivery stage.

`report` accepts a private strict `recall-study-rows-v1` file and an aggregate
metric-evidence file. It rejects unknown JSON fields and incomplete plans, then
writes only aggregate counts, raw denominators, unknowns, confidence intervals,
and gate results:

```bash
engram recall-study report "${COMMON[@]}" \
  --rows evals/recall-study/v1/calibration/rows.json \
  --metrics evals/recall-study/v1/calibration/metrics.json \
  --output evals/recall-study/v1/calibration/report.json --json
```

Run plans, task inputs, row-level results, label keys, Compatibility evidence,
consent evidence, and metric work files remain local. Shared reports contain no
prompt, query, Memory content, assistant text, transcript path, raw identifier,
or repository diff.

## Frozen gates

The report evaluator applies the preregistered point or confidence-interval
bound for every gate: checkpoint lower bound ≥ −2 percentage points; Stop upper
bound < 1 point; injected-byte reduction lower bound ≥ 30%; startup/compact p95
reduction lower bound ≥ 25%; Recall p95 < 250 ms; utility improvement ≥ 10%
with a non-negative lower bound; noise upper bound < 20% with positive
improvement; harm upper bound ≤ 2% and no worse than baseline; false-empty upper
bound ≤ 5%; and explicit label coverage lower bound ≥ 80%.

Passing gates does not enable rollout. Applying the eventual disposition is a
separate protected delivery stage.
