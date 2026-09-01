# Useful Recall Study v1

This directory contains the frozen metadata for the paired Codex Recall study.
The contract and both cohort manifests are bound to exact SHA-256 sidecars.
Calibration owns sampling units 1–60; held-out owns 61–517. The manifests carry
only protocol metadata and never task inputs, prompts, session identifiers, or
row-level outcomes.

Operator Compatibility, consent, run plans, task inputs, rows, and metric work
files stay local and are ignored by Git. See
[`docs/RECALL-STUDY.md`](../../../docs/RECALL-STUDY.md) for the contract,
privacy boundary, and commands.
