---
status: accepted
---

# Freeze attributable Admission studies before collection

Real-session Admission evaluation will extend explicit local Shadow admission
runs with an immutable, versioned Admission study contract. The contract freezes
calibration and held-out cohorts with non-overlapping frozen session manifests,
sampling requirements, supported adapter,
project-type, and session-shape dimensions, the label schema, numeric thresholds,
consent attestation, retention, cleanup, and the complete set of aggregate output
sections before a session can join a study. One session may belong to only one
cohort in a study version; an identical retry returns the existing run, while any
changed attribution requires a new version.

Study contracts, attributed runs, reviewer-specific Admission corrections, and
Omission annotations remain in dedicated local SQLite tables with no sync,
search, export, cloud, Obsidian, or Promotion path. Independent reviewers have
separate append-only streams; study metrics use the first review stream as the
primary human verdict and compare every pair of latest reviewer labels for runs
in the declared independent-review subset. Pairwise agreement is descriptive;
its Wilson interval uses proposal-level reviewer unanimity as the independent
unit. The aggregate report exposes counts, distributions, quality proportions,
Wilson 95% intervals, cohort sufficiency,
and frozen gates by type, without proposal content, reviewer identity, Evidence
references, or other row-level material. Automatic Admission remains disabled
regardless of the reported disposition. A `go` disposition requires complete
review coverage and representation of every declared adapter, project type, and
session shape;
protected false rejects, unsupported proposals, and privacy leaks all have
fixed zero-tolerance gates.
