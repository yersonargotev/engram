# Disposition for Admission preview, shadow, and study

Date: 2026-08-29

## Maintainer decision

After reviewing this recommendation, the maintainer chose complete removal:
the entire public `engram admission` family, including `preview`, will leave in
Engram v3.0.0 in one breaking change without a deprecation window. The
independent checkpoint `needs_review` disposition and local Memory proposals
remain as minimal audit evidence: callers provide only inline `{title, content}`
and v3 removes the Admission-derived proposal metadata and public `proposal_id`
input without adding an inbox or Promotion flow. Opening an existing database
with v3 automatically and irreversibly drops every dedicated `admission_*`
table and its experimental data through a permanent idempotent tombstone;
new databases never create them. Existing checkpoint proposals are rebuilt with
only identity, project, title, content, and creation time so their references
remain valid. `checkpoint record` and `checkpoint status` expose that immutable
minimal snapshot for `needs_review`, without a standalone proposal API. Active
product docs and Admission-only glossary terms leave with the code; superseded
ADRs, research, CHANGELOG history, and closed Issues remain as historical
evidence. The accepted decision is recorded in
[ADR-0008](../adr/0008-retire-the-admission-experiment.md); it supersedes the
staged recommendation below.

## Recommendation

**Do not improve the policy or open another Admission study now.** Retire
`admission shadow`, `admission study`, `admission review`, `admission omission`,
and `admission metrics` in a declared breaking/major release, after a deprecation
window. Keep `admission preview` temporarily as an experimental maintainer
diagnostic, with no automatic-promotion claim. Retire it too if the announced window
produces no demonstrated recurring maintainer or user use.

This retires a research line that accomplished its purpose: it established, with
real evidence and without promoting unsafe data, that the policy is not ready. This
does not mean Shadow's safety boundaries failed. Its local-only and privacy
boundaries are deliberate and well defended. It is a product-priority conclusion:
the feature creates no Memories, does not participate in retrieval or Promotion,
and its real-session evaluation programme ended in a terminal no-go.

“Keep it and wait” is not recommended. It preserves a public contract, tables, and
complex tests whose only valid next step requires unavailable resources: a new
prospective corpus and independent human review.

## Question and sources

This report evaluates `engram admission preview|shadow|study`. It uses only primary
sources: current ADRs, code, tests, public-contract documentation, Git history, and
first-party repository Issues. Execution claims are attributed to the Issue comments
that record them, not inferred from code.

## Current state and original purpose

| Surface | Purpose | Verified state |
| --- | --- | --- |
| `preview` | Run deterministic generation and assessment on bounded Evidence without persistence or a `mem_save` change. | Still public and intentionally advisory/non-persisting. [ADR-0004, lines 7-35](../adr/0004-calibrate-memory-admission-offline.md#validate-memory-admission-through-a-local-non-persisting-preview); [DOCS.md, lines 161-173](https://github.com/yersonargotev/engram/blob/acc8c0d0835f1e59431ba3ec38c52dfb9dd53faa/DOCS.md?plain=1#L161-L173). |
| `shadow` + review/metrics | Retain derived local snapshots from one session for human correction and metrics. | Explicit, local, and isolated: no hooks, sync, search, FTS, export, cloud, or Promotion. [ADR-0005, lines 7-21](../adr/0005-explicit-local-shadow-admission.md#measure-memory-admission-with-explicit-local-shadow-runs). |
| `study` + omission | Freeze an attributable contract, calibration/held-out cohorts, and gates before real-session collection. | Immutable contract and aggregate metrics; `automatic_admission_enabled` remains false. [ADR-0007, lines 7-31](../adr/0007-freeze-attributable-admission-studies.md#freeze-attributable-admission-studies-before-collection); [DOCS.md, lines 190-251](https://github.com/yersonargotev/engram/blob/acc8c0d0835f1e59431ba3ec38c52dfb9dd53faa/DOCS.md?plain=1#L190-L251). |

The boundary was sound: V1 policy is deterministic and conservative. It recommends
`admit` only for explicit requests, `reject` for a few unprotected cases, and
`review` otherwise. [ADR-0004, lines 16-22](https://github.com/yersonargotev/engram/blob/acc8c0d0835f1e59431ba3ec38c52dfb9dd53faa/docs/adr/0004-calibrate-memory-admission-offline.md?plain=1#L16-L22).
There is no production write path: `RunAdmissionShadow` calls `PreviewAdmission`
and persists separate snapshots; it neither saves a Memory nor calls Promotion.
[internal/memoryops/admission_shadow.go, lines 76-161](../../internal/memoryops/admission_shadow.go#L76-L161).

## Empirical result: the study answered its question

The roadmap and its children are closed as `wontfix`: [#34](https://github.com/yersonargotev/engram/issues/34),
[#50](https://github.com/yersonargotev/engram/issues/50),
[#51](https://github.com/yersonargotev/engram/issues/51), and
[#52](https://github.com/yersonargotev/engram/issues/52). Their dispositions are
consistent:

1. V3 produced **zero proposals** in calibration. A confirmed omission showed that
   it did not exercise the proposal path and could not meet its minimum. The
   held-out allocation was not run. [#50: V3 calibration result](https://github.com/yersonargotev/engram/issues/50#issuecomment-5464647816)
2. V4 produced eight proposals; primary human review labeled them `5 admit / 1
   review / 2 reject`. The policy nevertheless recommended `review` for all eight
   and emitted no `admit`. The review rate was 100% against a frozen maximum of
   50%, while promotion precision had a zero denominator; both gates fail
   regardless of later human labels. [#50: V4 calibration](https://github.com/yersonargotev/engram/issues/50#issuecomment-5465737780)
   [#50: primary review](https://github.com/yersonargotev/engram/issues/50#issuecomment-5465750613)
3. The minimum independent human review was also unsatisfiable: it remained 0/1,
   and no second reviewer was fabricated. The held-out allocation was neither run
   nor inspected, and the former #61 source was permanently retired from held-out
   use. [#50: final insufficiency](https://github.com/yersonargotev/engram/issues/50#issuecomment-5465806305)
   [#50: closing disposition](https://github.com/yersonargotev/engram/issues/50#issuecomment-5465827213)
4. Consequently, #51 could not freeze an evidence-backed candidate and #52 had no
   valid candidate or manifest to execute. Both reject inferring a policy change
   from insufficient evidence. [#51](https://github.com/yersonargotev/engram/issues/51),
   [#52](https://github.com/yersonargotev/engram/issues/52).

Shadow therefore had research and epistemic-safety value. The result is not “more
data will make it pass”: the policy output already made V4 impossible, while the
prospective design requires a new contract, new sources, and feasible review
conditions.

## Cost and risks of keeping it

The cost is not just the CLI. Two substantial, dedicated changes created this
surface: [`8da8d43`](https://github.com/yersonargotev/engram/commit/8da8d43284f757bf31ab0afa62f063c60b810b78)
introduced opt-in Shadow, and [`9d4efe7`](https://github.com/yersonargotev/engram/commit/9d4efe72321e6166297cd64f60da0c6eb48bf007)
added attributable studies. [`2c47d31`](https://github.com/yersonargotev/engram/commit/2c47d31fa5cc90c1e2f2b4d84d7bd31d29b0fed5)
later connected ambiguous checkpoint knowledge to review; it is related but not a
third dedicated Admission implementation.

The conservative 16-file core set totals approximately **6,826 Go lines** by
`wc -l`: six CLI files (`admission.go`, `admission_shadow.go`,
`admission_study.go`, and their three tests); six `memoryops` files with the same
three implementation/test pairs; and four store files (`admission_shadow.go`,
`admission_study.go`, and their tests). It includes preview because Shadow calls
it, but does not claim every line is exclusive to this feature. The full
admission-specific corpus/store test perimeter is larger, so 6,826 is a
conservative maintenance-cost indicator, not a complete LOC accounting.

The concrete risks are:

- **Product:** metrics and `go` do not change user behavior; the result explicitly
  contains `automatic_admission_enabled: false`.
  [internal/memoryops/admission_study.go, lines 140-152](../../internal/memoryops/admission_study.go#L140-L152)
  and [DOCS.md, lines 238-251](https://github.com/yersonargotev/engram/blob/acc8c0d0835f1e59431ba3ec38c52dfb9dd53faa/DOCS.md?plain=1#L238-L251). Documented commands can imply a
  product capability that does not exist.
- **Maintenance:** versioned contracts, manifests, cohorts, consent, retention,
  two review modes, Wilson metrics, and gates enlarge the regression surface. The
  metric types track distributions across cohort, adapter, project, and session
  shape plus seven gates. [internal/memoryops/admission_study.go, lines 51-152](../../internal/memoryops/admission_study.go#L51-L152).
- **Privacy and support:** the design minimizes data and remains local-only, but it
  retains redacted proposals, references, and corrections. Redaction, project
  deletion, migration, and cleanup must remain correct. [ADR-0005, lines 23-43](https://github.com/yersonargotev/engram/blob/acc8c0d0835f1e59431ba3ec38c52dfb9dd53faa/docs/adr/0005-explicit-local-shadow-admission.md?plain=1#L23-L43);
  [DOCS.md, lines 229-251](https://github.com/yersonargotev/engram/blob/acc8c0d0835f1e59431ba3ec38c52dfb9dd53faa/DOCS.md?plain=1#L229-L251).
- **Research:** a legitimate new study cannot reuse the retired held-out source or
  lower gates retrospectively. It needs a newly consented source, prospective
  contract, candidate policy, and two genuinely independent reviewers. That is a
  new research project, not a small improvement.

## Options compared

| Option | Benefit | Cost/risk | Verdict |
| --- | --- | --- | --- |
| Improve policy now and repeat the study | Could turn `review` proposals into `admit`. | No valid corpus or second reviewer; a policy change requires a new study and does not establish downstream value. | Do not do this. |
| Keep everything frozen | Preserves a tool for a possible future researcher. | Public and privacy debt; no current user value; appears to be an active roadmap. | Do not do this. |
| Retire only Study and keep Shadow | Reduces some complexity. | Shadow without an attributable programme generates retained data without a decision path. | Not recommended. |
| Retire Shadow/Study/Review/Metrics; temporarily downgrade Preview to a diagnostic | Retains the smaller non-persisting seam while removing retention, cohorts, and gates with no current value. | Public CLI break requires a deprecation window and major release. | **Recommended.** |
| Retire the entire Admission family immediately | Maximum simplification. | Also removes the diagnostic seam without an announced observation period. | Reconsider after the Preview window. |

## Action plan

1. Open a retirement issue, not a recalibration issue. State: “Admission research
   closed; no automatic Admission roadmap.” Link #34 and #50-#52.
2. During an announced deprecation window, mark `shadow`, `study`, `review`,
   `omission`, and `metrics` deprecated, with a breaking-release removal version
   and clear warnings. Do not create new studies. Keep only `preview`, marked as an
   experimental maintainer diagnostic.
3. Local-first provides no telemetry that can prove absence of use. Offer an
   explicit count and confirmed cleanup path for existing `admission_*` data
   during the window; keep the retained row-level material inside its current
   local-only boundary. The exit criterion is no *demonstrated recurring*
   maintainer or user use, not a claim that no one uses it.
4. In the declared breaking/major release, remove the Shadow/Study CLI, tables
   from new-database creation, migrations that create them, documentation, and
   tests. Existing SQLite tables must remain inert unless the user explicitly uses
   the confirmed cleanup path; never automatically drop them on startup, sync them,
   or convert them to Memories. Keep ADRs and #50 as historical evidence.
5. Revisit `preview` after the window. If it lacks demonstrated recurring
   maintainer/user use, retain any necessary fixture tests internally and remove
   its public command in a later declared breaking release.

A future restart is justified only by a different product hypothesis: admitting a
Memory must measurably improve retrieval and usefulness in a later task, not merely
pass proposal-label precision. It must start with a new issue, newly consented data,
a prospective corpus that does not reuse #61, a frozen candidate policy, and two
independent reviewers available before calibration begins. Without those
preconditions, more Admission work would repeat the expensive part of the
experiment without changing the demonstrated blocker.
