# Useful Recall Study v1

This directory contains the frozen metadata for the paired Codex Recall study.
The contract and both cohort manifests are bound to exact SHA-256 sidecars.
Calibration owns sampling units 1–60; held-out owns 61–517. The manifests carry
unique per-unit commitments to canonical task-input envelopes, but never task
content, prompts, session identifiers, or row-level outcomes. The separate
task-protocol artifact freezes how those envelopes select and launch disposable
fixtures.

The content-addressed `policy.json` and `metrics.json` define treatment and
analysis semantics. Operator Compatibility, consent, run plans, task inputs,
rows, labels, and other work files live only under `private/`, which is ignored
as a whole by Git. See
[`docs/RECALL-STUDY.md`](../../../docs/RECALL-STUDY.md) for the contract,
privacy boundary, and commands.
