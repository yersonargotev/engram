# Local shadow admission V3

Date: 2026-08-19

## Recommendation

Ship V3 as an explicitly invoked, local-only measurement workflow:

1. `admission shadow` reuses V2 session acquisition and the V1 deterministic
   generator/policy.
2. One transaction stores an immutable run and immutable proposal/assessment
   snapshots, but no Evidence item content.
3. `admission review mark` appends a human correction; it never changes the
   original recommendation and never promotes a proposal.
4. `admission metrics` derives aggregates from snapshots and each proposal's latest
   correction.

Do not run this from session lifecycle hooks in V3. The explicit command is the
opt-in boundary, keeps failures away from `save`/`mem_save`, and matches the accepted
decision that automatic collection needs a separate retained-data design
([ADR-0004](../adr/0004-calibrate-memory-admission-offline.md),
[original admission research](automatic-memory-admission.md)).

The domain boundary must remain visible: a Memory proposal is not a Memory, an
Admission assessment is advice rather than a decision, and only Promotion may
create a Memory ([domain language](../../CONTEXT.md)).

## What V3 should reuse

The current baseline already provides the right computation seam:

- evidence is bounded to 32 items, 16 KiB per item, and 64 KiB total;
- one explicitly selected session supplies persisted prompts plus its newest active
  session summary, with project matching and source-level coverage diagnostics;
- `<private>` blocks are redacted before proposal generation;
- repository signals and tool output cannot generate proposals;
- proposal and assessment reason codes, categories, protection, and provenance
  references are deterministic; and
- preview reads Memories only for normalized exact duplicate advice and has tests
  proving that it does not mutate sessions, prompts, or Memories.

These are executable properties of the current
[memoryops implementation](../../internal/memoryops/admission.go),
[service tests](../../internal/memoryops/admission_test.go), and
[versioned V1 corpora](../../internal/memoryops/admission_corpus_test.go). V3 should
call that service and snapshot its result rather than fork the grammar or policy.
`admission preview` must continue to use the same service without persistence.

## Retained data contract

Use dedicated ordinary SQLite tables owned by `internal/store`; do not reuse
`observations`, `user_prompts`, FTS tables, sync tables, or relation tables.

| Record | Retain | Never retain |
| --- | --- | --- |
| Shadow run | Local run ID, project, selected session ID, timestamps, evidence/generator/policy versions, acquisition counts, diagnostic codes | Evidence bundle JSON, prompt or summary text, transcript, tool output, repository diff |
| Proposal assessment | Local proposal ID, run ID, redacted title/content, type, scope, category, protected flag, evidence reference IDs, proposal reason codes, recommendation and assessment reason codes | Source excerpts, nearby Memory content, model prompts/responses |
| Human correction | Event ID, proposal ID, verdict, bounded sanitized note, timestamp | Copied source evidence or user identity not needed by the local experiment |

The proposal text is the minimum reviewable derived artifact, but it is still
sensitive data. V3 stores it only after the same `<private>` block redaction used
during acquisition, applies that redaction again at the store boundary, bounds
correction notes, and documents that notes must not contain raw evidence or secrets.
Generic credential-pattern scanning is a future hardening option, not part of this
deterministic V3 contract: pattern coverage is necessarily incomplete. GitHub's
first-party secret-scanning documentation likewise distinguishes provider, generic,
and unstructured patterns rather than promising universal detection
([supported patterns](https://docs.github.com/en/code-security/reference/secret-security/supported-secret-scanning-patterns)).

Evidence references are identifiers, not evidence excerpts. Do not retain hashes of
raw prompts or summaries as a substitute: a hash adds no evaluation value here and
can still enable guessing of low-entropy text.

Local-only must be structural, not a flag that callers must remember. The shadow
tables must have no code path into sync mutations/chunks, cloud materialization,
Memory search/context, FTS, or normal export/import. `internal/memoryops` owns the
workflow and metrics; CLI only parses and renders it.

## Immutable assessments and append-only corrections

Treat the generated proposal and original assessment as historical facts about a
specific versioned policy. Never update them after insertion. A correction is a new
event pointing to a proposal, following the provenance principle that a revision is
derived from, rather than a silent overwrite of, its original entity
([W3C PROV-O](https://www.w3.org/TR/prov-o/)).

For a proposal, order corrections by `(created_at, correction_id)` and use the last
event as the current human verdict. Preserve earlier events for audit. An identical
retry is idempotent only when its normalized `(verdict, note)` equals the latest
event: return that event without inserting. Returning to an older verdict after an
intervening correction is a real new event and must append.

Validate proposal existence, restrict verdicts to `admit | review | reject`, bound
notes, and insert under a transaction. Listing must
use an explicit stable order, such as run time descending then proposal ID ascending;
SQLite does not guarantee row order without `ORDER BY`.

## Metrics and gates

Metrics must be grouped by policy version and project and must report raw numerators
and denominators. Counts alone are valid before review coverage is adequate; do not
turn unreviewed proposals into implicit agreement or rejection.

| Metric | Definition |
| --- | --- |
| Runs / proposals | Immutable run count and proposal count |
| Recommendation distribution | Proposal count by original `admit`, `review`, `reject` |
| Category distribution | Proposal count by category and protected flag |
| Review coverage | Proposals with at least one correction / all proposals |
| Human outcome distribution | Uniquely reviewed proposals by latest human verdict |
| Agreement | Latest human verdict equals original recommendation / uniquely reviewed proposals |
| Disagreement | Latest human verdict differs / uniquely reviewed proposals |
| Protected false rejects | Count of protected proposals originally `reject` whose latest human verdict is not `reject` |
| Reason-code coverage | Proposals with at least one assessment reason code / all proposals, plus counts by reason code |

`protected_false_rejects > 0` blocks any automatic-reject gate. Report this as an
absolute count even when the rate rounds to zero. For proportions, publish a 95%
binomial confidence interval; zero observed errors is not proof of zero population
error ([NIST confidence-interval guidance](https://www.itl.nist.gov/div898/handbook/prc/section2/prc241.htm)).

Do not present one aggregate "accuracy" score. Durable facts are likely rarer than
mundane progress, `review` is intentionally an abstention class, and false rejection
is more costly than false admission. Precision/recall views and category-specific
counts are more informative under class imbalance
([Saito and Rehmsmeier, 2015](https://doi.org/10.1371/journal.pone.0118432)).

V3 runtime corrections can measure recommendation agreement, but they cannot by
themselves establish generator recall: the denominator requires independently
labeled durable facts that the generator may never propose. Likewise, an
unsupported-proposal rate requires a structured gold reason, not an inference from a
free-form note. Those remain release-corpus metrics.

## Calibration and held-out evaluation

Keep calibration and held-out fixtures in different versioned paths and execute them
with different test targets. Each corpus manifest should freeze scenario IDs,
content hashes, label schema, provenance/consent status, and corpus version.

- Calibration data may be inspected and used to change grammar, reason codes,
  thresholds, or policy.
- Before held-out execution, freeze the implementation commit, policy version,
  metric definitions, and numeric go/no-go thresholds in a reviewable manifest.
- Held-out examples and outcomes must not influence those choices. A policy or
  threshold change after evaluation requires a new held-out version.
- Never mix calibration and held-out rows in reported metrics.

Using test data to make model choices leaks evaluation information and produces
optimistic estimates
([scikit-learn's official leakage guidance](https://scikit-learn.org/stable/common_pitfalls.html#data-leakage)).
NIST's AI RMF similarly calls for documented test sets and metrics, evaluation under
deployment-like conditions, privacy measurement, and user feedback/appeal paths
([AI RMF Core](https://airc.nist.gov/airmf-resources/airmf/5-sec-core/)).

The present synthetic V1 scenarios are deterministic regression/calibration
fixtures, not an independent release corpus. A future release claim needs real,
consented, redacted coding sessions labeled independently; a temporal holdout
collected only after policy freeze is preferable when a truly blind checked-in
fixture is impossible.

## Privacy retention and cleanup

Data minimization is the primary privacy control: retain only reviewable proposal
snapshots and provenance IDs, never duplicate the raw sources already present in the
local store. This follows NIST's controls to limit PII retained for testing,
training, and research and to define disposal
([NIST SP 800-53 Rev. 5, SI-12](https://doi.org/10.6028/NIST.SP.800-53r5)).

V3 links cleanup to Engram's existing explicit project deletion: deleting a project
also removes its local shadow runs and all child rows atomically. Automatic finite
retention remains a product decision for a later increment because no review-latency
evidence yet supports a default. A provisional 30-day period is a hypothesis, not a
research-derived threshold. Append-only means no historical rewriting while a run
is retained; it does not prevent an intentional project deletion.

Do not claim secure erasure from SQL `DELETE` alone. SQLite normally marks deleted
space reusable without erasing its old bytes; `secure_delete` or `VACUUM` is needed
to remove forensic traces, with documented performance and virtual-table caveats
([SQLite `secure_delete`](https://sqlite.org/pragma.html#pragma_secure_delete),
[SQLite `VACUUM`](https://sqlite.org/lang_vacuum.html)). V3 should avoid FTS for
shadow text, document logical versus forensic deletion, and avoid promising
cryptographic erasure of database files or backups.

## Non-goals

- Automatic execution at session close or from any plugin/hook.
- Automatic Promotion, automatic rejection, or any change to `save`/`mem_save`.
- MCP, HTTP, plugin, TUI, dashboard, cloud, sync, or normal export/import support.
- LLM generation or judgment.
- Semantic deduplication or relation judgment.
- Retention of raw Evidence for later relabeling.
- A release-readiness claim based only on synthetic fixtures or unreviewed shadow
  rows.
