# Useful Recall Study v1

This directory contains the frozen metadata for the paired Codex Recall study.
The contract and both cohort manifests are bound to exact SHA-256 sidecars.
Calibration owns sampling units 1–60; held-out owns 61–517. The manifests carry
unique per-unit commitments to the exact synthetic fixture, instruction,
verifier, expected result, and complete canonical task-input envelope, but never
private task content, prompts, session identifiers, or row-level outcomes. The
separate task-protocol artifact freezes the byte templates and launch contract.

The consented executor runs calibration before held-out, persists resumable
private rows when a cell is valid, and derives the aggregate-only
`publication.json`. An invalid calibration publishes its reason and unavailable
gates without fabricating rows. The v1 result is `continue_canary` because the
first targeted cell did not initiate Recall; held-out remained unopened. The
shared artifact always leaves rollout disabled and does not change any frozen
v1 input or sidecar.

`distribution.json` and its SHA-256 sidecar apply that immutable disposition as
one content-addressed distribution outcome. They pin the verified canary tuple
at source revision `105778d820029a2326043739fd676647e5c037f6`: Managed Pack
`3.3.0`, binary `3.0.0`, Codex plugin `0.1.7`, and Protocol `1`. The selected
action preserves the tuple, every legacy path, and the Legacy prompt archive;
it requires no release, enables no rollout, and keeps the Recall telemetry and
Diagnostic capture schemas non-participating by default.

`engram recall-study verify-distribution` recomputes the pinned commit, minimal
tree chain, and artifact blob IDs from the self-contained membership proof, then
checks each independent SHA-256 digest in the supplied source tree. It needs no
local Git history or network access. This source verification deliberately
reports installed readiness as `not_verified`; use
`engram setup status codex --json` for the separate post-install check. Because
the disposition requires no release, it does not invent a new release artifact
or checksum.

The content-addressed `policy.json` and `metrics.json` define treatment and
analysis semantics. Operator Compatibility, consent, run plans, task inputs,
rows, labels, and other work files live only under `private/`, which is ignored
as a whole by Git. See
[`docs/RECALL-STUDY.md`](../../../docs/RECALL-STUDY.md) for the contract,
privacy boundary, and commands.
