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
The execution stage uses Codex
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
    "policy": {"version": "recall-policy-v1", "revision": "sha256:a5e3aa9c438e2efc3d4bd11e9e636ef231a3e9dc0468af9062e15426f93b9af1"},
    "metric": {"version": "recall-study-metrics-v1", "revision": "sha256:78a701966e15a5bc6fb00ade131f95c49b2ca6af4ce17e06fbef185202c20eaa"},
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
private `0600` plan for the 180 calibration cells. These metadata modes do not
accept a task-input path, open held-out inputs, authorize a held-out run, or
execute Codex.

The executor builds binary 3.0.0 and Codex plugin 0.1.7 from the frozen source
revision, installs them into an isolated Codex home, and copies only the named
authentication file. Each cell gets a fresh ephemeral checkout, isolated
HOME/data/XDG/temp directories, the exact synthetic task input, the frozen
model and reasoning effort, and a 900-second timeout. Only the fixture may
change. Harness failures use frozen operational-failure codes and cannot carry
residual metric or quality evidence. Private progress is rewritten atomically
after every cell; the disposable checkout and local agent state are removed
only after that row is durable. A failed write retains the isolated cell for
diagnosis. Targeted Recall result keys, explicit label sources, disagreements,
unknown label dimensions, and false-empty reviews come from the content-free
Store snapshot. The broad arm separately binds its one deterministic evaluator
assessment to the exact verified synthetic SessionStart context. Global temp
roots are excluded from the model sandbox. A trusted pre-tool guard removes
only the cell's copied authentication file before the first model-requested
tool runs, while the named original remains untouched. The committed project
Memory manifest remains byte- and mode-identical and readable to the model in
every arm. The trusted SessionStart adapter uniformly substitutes a separate
content-free working directory, preventing background import without changing
the model-visible repository. Either cohort can resume from its strict `0600`
`recall-study-rows-v1` file.

```bash
RUNTIME=(
  --source-repo "$PWD"
  --codex-binary "$(command -v codex)"
  --auth-file "$CODEX_AUTH_FILE"
)

engram recall-study run-calibration "${COMMON[@]}" "${RUNTIME[@]}" \
  --output evals/recall-study/v1/private/calibration/rows.json \
  --publication-output evals/recall-study/v1/publication.json --json

engram recall-study run-held-out "${COMMON[@]}" "${RUNTIME[@]}" \
  --calibration-rows evals/recall-study/v1/private/calibration/rows.json \
  --output evals/recall-study/v1/private/held-out/rows.json --json
```

`run-held-out` validates the complete calibration row set before it creates a
runner or materializes held-out task bytes. Operational failures or omissions
in calibration keep held-out execution closed. The runner never reads
unrelated sessions or uses the operator's normal Engram store.

`--publication-output` is used only when calibration cannot validly complete.
In that case the command writes a shared invalid-stage publication with
`continue_canary`, the reason code, every unavailable gate as an evidence gap,
and no fabricated row or report. A missing required targeted Recall operation
is an invalid treatment observation, not a process failure and not an invented
zero-latency Recall.

`report` accepts a private strict calibration `recall-study-rows-v1` file. It
rejects unknown JSON fields, incomplete plans, held-out rows, and treatment
contradictions. Points, raw denominators, unknowns, Wilson intervals, and the
frozen deterministic bootstrap intervals are derived from those validated rows;
callers cannot provide metric values or confidence intervals.

The same frozen domain analyzer accepts held-out and `combined-v1` row sets,
but the standalone `report` CLI still rejects them so it cannot become a
held-out access path. Aggregate output includes per-treatment intervals for all
three arms, including distinct duplicate and time-to-useful results; the GA
clauses remain the preregistered paired broad-versus-targeted comparisons.
Completed attempts record a separate `task_outcome`: verifier failures remain
in the paired quality population and in each arm's task-success rate. Harness
failures are operational failures; they must contain no residual metric or
quality evidence, must use an exactly frozen outcome code, are rejected
otherwise, and are reported separately. A treatment with zero task successes
still reports a 0% success rate; value metrics that have no successful
population carry `available: false`, denominator zero, and all affected runs as
unknown instead of aborting the report.

```bash
engram recall-study report "${COMMON[@]}" \
  --rows evals/recall-study/v1/private/calibration/rows.json \
  --output evals/recall-study/v1/private/calibration/report.json --json
```

Run plans, task inputs, row-level results, label keys, Compatibility evidence,
and consent evidence are placed under `evals/recall-study/v1/private/`; Git
ignores that directory as a whole. The final shared publication is derived only
from complete calibration and held-out row sets:

```bash
engram recall-study publish "${COMMON[@]}" \
  --calibration-rows evals/recall-study/v1/private/calibration/rows.json \
  --held-out-rows evals/recall-study/v1/private/held-out/rows.json \
  --output evals/recall-study/v1/publication.json --json
```

The publication contains aggregate labels, treatment metrics, raw
denominators, unknown counts, confidence intervals, gate clauses, evidence-gap
gate IDs, and exactly one Codex-scoped disposition. It contains no prompt,
query, Memory content, assistant text, transcript path, raw identifier,
repository diff, or row-level material.

## Published v1 result

The consented v1 calibration stopped before accepting its first row because
Codex 0.152.0 did not initiate Recall in the first frozen targeted-treatment
cell. The published result is therefore `valid: false` with reason
`targeted_recall_not_observed`, 0 of 180 calibration rows observed, all ten
gates listed as evidence gaps, and disposition `continue_canary`. No held-out
task input was materialized, and no rollout was enabled. The bound result is
[`evals/recall-study/v1/publication.json`](../evals/recall-study/v1/publication.json).

## Applied distribution outcome

The content-addressed
[`distribution.json`](../evals/recall-study/v1/distribution.json) applies the
immutable `continue_canary` publication without reinterpreting its unavailable
gates. It pins the exact verified canary tuple from source revision
`105778d820029a2326043739fd676647e5c037f6`: Managed Pack `3.3.0`, binary
`3.0.0`, Codex plugin `0.1.7`, and Protocol `1`, all with an inclusive Protocol
range of `1..1`. The four pinned source artifacts carry exact SHA-256 digests.
Their historical bytes are preserved under
[`evals/recall-study/v1/source-snapshot/`](../evals/recall-study/v1/source-snapshot/)
so the frozen outcome remains independently verifiable while the current
Managed Pack and skill continue to evolve.

This outcome is deliberately not a release or a general-availability
qualification. It preserves the legacy-compatible expand path, all old owned
and customized paths, and the Legacy prompt archive. It enables no rollout or
cleanup, and the Recall telemetry and Diagnostic capture schemas remain local,
backward-compatible, and non-participating by default. Held-out inputs remain
unopened.

Verify the publication binding, the outcome trust anchor, the four-axis tuple,
the exact Git revision, and regular-file membership at that revision without
reading private evidence or mutating installation state. The outcome embeds the
exact commit and the minimal Git tree bodies needed to reconstruct each path;
the verifier checks the preserved historical snapshot and recomputes commit,
tree, and blob object IDs plus the independent SHA-256 file digests. It does not
reinterpret the current checkout as the evaluated source. This works from a
shallow checkout or extracted source without fetching history:

```bash
engram recall-study verify-distribution \
  --contract evals/recall-study/v1/contract.json \
  --contract-hash evals/recall-study/v1/contract.sha256 \
  --publication evals/recall-study/v1/publication.json \
  --distribution evals/recall-study/v1/distribution.json \
  --distribution-hash evals/recall-study/v1/distribution.sha256 \
  --source-repo "$PWD" --json
```

Successful output proves only the frozen source outcome. It reports
`source_revision_verified: true`, `source_artifacts_verified: true`, and
`post_install_readiness: not_verified`; it never emits a top-level `ready`
claim. The disposition requires no release, so there is no new release binary
or binary checksum to verify. Run `engram setup status codex --json` separately
to verify the installed binary, Managed Pack, Codex plugin, Protocol ranges,
and post-install capability state.

## Frozen gates

The report evaluator applies the preregistered point or confidence-interval
bound for every gate: checkpoint lower bound ≥ −2 percentage points; Stop upper
bound < 1 point; injected-byte reduction lower bound ≥ 30%; startup/compact p95
reduction lower bound ≥ 25%; Recall p95 < 250 ms; utility improvement ≥ 10%
with a non-negative lower bound; noise upper bound < 20% with positive
improvement; harm upper bound ≤ 2% and no worse than baseline; false-empty upper
bound ≤ 5%; and explicit label coverage lower bound ≥ 80%.

The disposition is `qualify_general_availability` only when every gate passes.
A checkpoint non-inferiority, Stop-growth, or harm failure selects
`rollback_prior_verified_tuple`; other failed gates select `continue_canary`
and are listed as evidence gaps. Every publication keeps `rollout_enabled`
false. Applying any disposition remains a separate protected delivery stage.
