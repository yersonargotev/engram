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

- `contract.json` and its compiled SHA-256 trust anchor freeze the model,
  repository, task protocol, label rubric, revisions, sample size, confidence
  intervals, and all general-availability gates;
- `policy.json`, `metrics.json`, and their sidecars provide content-addressed
  treatment and analysis specifications;
- `task-protocol.json` and its sidecar freeze task-input canonicalization,
  exact synthetic fixture, instruction, verifier and expected-result templates,
  launch environment, timeout, success observation, failure mapping, and cleanup;
- `calibration/manifest.json` and its sidecar allocate blocks 1–60;
- `held-out/manifest.json` and its sidecar allocate blocks 61–517.

Every block is paired across all three treatments. The power analysis freezes
517 blocks per treatment and 1551 total cells. Calibration and held-out
metadata use separate namespaces and seeds, and their numeric ranges cannot
overlap. Every unit carries separate SHA-256 commitments to its fixture,
instruction, verifier and expected result, plus one commitment to the complete
canonical `recall-task-input-v1`. The manifest hashes bind all 517 memberships;
metadata-only verification validates those commitments and rejects cross-cohort
reuse without materializing held-out input bytes. The consented #110 executor
must provide byte-identical private inputs at the `VerifyTaskInput` seam.

The source snapshot is
`105778d820029a2326043739fd676647e5c037f6`. The contract separately records
Managed Pack 3.3.0, binary 3.0.0, Codex plugin 0.1.7, Protocol 1,
`recall-baseline-events-v1`, and `diagnostic-capture-v1` at that revision.
Policy, metric, and task-protocol v1 are separately bound by their exact
SHA-256 revisions.
The future execution stage uses Codex
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
    "policy": {"version": "recall-policy-v1", "revision": "sha256:9d0d0207d1a90d48f4082bdb543b9a08f83d0b1f5a34af4ceb6cba986a151879"},
    "metric": {"version": "recall-study-metrics-v1", "revision": "sha256:65507cf0f882e4e3c031335d0f41bdb79f908087e9c2288e223fcb9a7d22a589"},
    "source": {"version": "105778d820029a2326043739fd676647e5c037f6", "revision": "105778d820029a2326043739fd676647e5c037f6"}
  },
  "compatibility": {
    "schema_version": "protocol-compatibility-v1",
    "status": "ready",
    "reason_code": "protocol_compatible",
    "reason": "All four attributable Protocol ranges intersect.",
    "axes": [
      {"name":"managed_pack","version":"3.3.0","provenance":"repository:https://github.com/yersonargotev/engram.git#revision:105778d820029a2326043739fd676647e5c037f6","supported_protocol":{"minimum":1,"maximum":1},"status":"ready","reason_code":"managed_pack_ready"},
      {"name":"engram_binary","version":"3.0.0","provenance":"repository:https://github.com/yersonargotev/engram.git#revision:105778d820029a2326043739fd676647e5c037f6","supported_protocol":{"minimum":1,"maximum":1},"status":"ready","reason_code":"engram_binary_ready"},
      {"name":"codex_plugin","version":"0.1.7","provenance":"repository:https://github.com/yersonargotev/engram.git#revision:105778d820029a2326043739fd676647e5c037f6","supported_protocol":{"minimum":1,"maximum":1},"status":"ready","reason_code":"codex_plugin_ready"},
      {"name":"protocol_contract","version":"1","provenance":"engram-core","supported_protocol":{"minimum":1,"maximum":1},"status":"ready","reason_code":"protocol_contract_ready"}
    ],
    "protocol_intersection": {"minimum":1,"maximum":1}
  }
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
  "proof_sha256": "<ConsentCommitment(contract, calibration manifest, held-out manifest)>"
}
```

The proof is not an arbitrary well-formed digest: it is the deterministic
domain-separated commitment to the exact contract and both manifest hashes.
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
  --environment evals/recall-study/v1/private/environment.json
  --consent evals/recall-study/v1/private/consent.json
)

engram recall-study verify "${COMMON[@]}" --json
engram recall-study dry-run "${COMMON[@]}" --json
engram recall-study plan-calibration "${COMMON[@]}" \
  --output evals/recall-study/v1/private/calibration/run-plan.json --json
```

`verify` and `dry-run` read only frozen metadata. `plan-calibration` writes a
private `0600` plan for the 180 calibration cells. No #109 command accepts a
task-input path, opens held-out inputs, authorizes a held-out run, or executes
Codex. Calibration execution, held-out authorization and execution, and the
immutable disposition belong exclusively to issue #110.

The contract nevertheless freezes the execution boundary in advance: the
runner must consume `recall-study-run-plan-v1`, produce private
`recall-study-rows-v1`, keep task inputs outside this command surface, and leave
default Recall and automatic rollout disabled. Issue #110 must implement that
shape rather than revising it after results are visible.

`report` accepts a private strict calibration `recall-study-rows-v1` file. It
rejects unknown JSON fields, incomplete plans, held-out rows, and treatment
contradictions. Points, raw denominators, unknowns, Wilson intervals, and the
frozen deterministic bootstrap intervals are derived from those validated rows;
callers cannot provide metric values or confidence intervals.

The same frozen domain analyzer accepts held-out and `combined-v1` row sets for
issue #110, but this #109 CLI rejects them before analysis so it cannot become a
held-out access path. Its aggregate output includes per-treatment intervals for
all three arms, including distinct duplicate and time-to-useful results; the GA
clauses remain the preregistered paired broad-versus-targeted comparisons.
Completed attempts record a separate `task_outcome`: verifier failures remain
in the paired quality population and in each arm's task-success rate. Harness
failures are operational failures; they must contain no residual quality
evidence, are rejected otherwise, and are reported separately.

```bash
engram recall-study report "${COMMON[@]}" \
  --rows evals/recall-study/v1/private/calibration/rows.json \
  --output evals/recall-study/v1/private/calibration/report.json --json
```

Every local artifact is placed under `evals/recall-study/v1/private/`; Git
ignores that directory as a whole. Run plans, task inputs, row-level results,
label keys, Compatibility evidence, and consent evidence remain local. Shared
reports contain no prompt, query, Memory content, assistant text, transcript
path, raw identifier, or repository diff.

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
