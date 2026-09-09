# Memory authoring: bounded Recall verification

This editorial audit and synthetic check implement [issue #152](https://github.com/yersonargotev/engram/issues/152), including its approved Agent Brief. They demonstrate prefix placement, not an improvement measured in real-agent sessions.

## Audit and ownership

The canonical `skills/engram-memory/SKILL.md` already required independently useful durable knowledge, causes and rationale, excluded routine activity logs, and supported saved/skipped/needs_review and Mixed outcomes. The gap was how authors order a finding so its reusable lesson appears before delivery bookkeeping.

The new authoring section and four contrasting cases live beside the existing disposition rubric. The Codex, Claude Code, and portable Agent Plugin skill files are byte-identical projections; `go generate ./internal/setup/` refreshes the embedded portable copy. Existing MCP projection and plugin packaging tests enforce parity. The activation cue and root-turn protocol remain unchanged.

The CLI skill was audited as well: its What/Why/Where/Learned command example starts with `<durable result>` and defers to the canonical rubric. The canonical guidance now clarifies that these labels are optional and that the implication and rationale belong in the opening content. The published CLI skill bytes and Pack 3.3.1 compatibility fingerprint remain intact. No host-specific editorial policy, runtime change, version change, persistence gate, or frozen study edit is needed.

## Method

TDD with a new exact-text assertion would not establish editorial quality for this documentation-only change. The approved public verification surface is ordinary CLI Recall; existing automated tests cover projection and packaging contracts. Independent editorial review must assess whether the examples preserve enough knowledge to guide a future decision.

Built the local CLI with `go build -o /tmp/engram-152-verify ./cmd/engram`. Runtime source is unchanged from `5a0f93eabc9750950f6b88e3515210446502fb06`; the tested editorial contents are included in this change. The final PR evidence binds rerun results to the exact candidate SHA.

For each of the bug and architecture examples, joined the canonical blockquote lines with spaces to form the lesson. Added five repetitions of this synthetic delivery paragraph, once before and once after that same lesson:

> Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branch and temporary worktree were removed; the operator checkout was preserved.

Each repetition ends with a space; a newline separates the lesson and the delivery paragraph sequence. Both inputs exceed the candidate prefix limit. Within a disposable `ENGRAM_DATA_DIR`, created two Memories per project with `engram save <title> <content> --project <project> --scope project --json`. Fixture insertion is independent synthetic curation, not a production root-turn checkpoint.

Ran these ordinary queries with that same isolated environment:

```sh
engram search bug --project synthetic-editorial-bug --scope project --limit 5 --json
engram search architecture --project synthetic-editorial-architecture --scope project --limit 5 --json
```

Checked successful exit status, two results per query, both actual summaries, and the reported aggregate UTF-8 bytes. No full retrieval, modified limits, ranking changes, or private corpus was used. The temporary directory was removed after the commands completed and its absence verified.

## Actual returned summaries

### Bug

Reported aggregate response: 1,764 UTF-8 bytes, within 4 KiB.

**bug lesson-first** — 512 summary bytes:

```text
In client v2.4, reuse the idempotency key when retrying a timed-out payment request: the server may have committed before the response was lost. Creating a new key can charge twice. This applies only to endpoints supporting idempotency keys; other endpoints still require reconciliation. Evidence: payment-client PR 42, timeout replay test.
Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branch and temporary worktree were removed; the operat…
```

**bug delivery-first** — 512 summary bytes:

```text
Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branch and temporary worktree were removed; the operator checkout was preserved. Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branch and temporary worktree were removed; the operator checkout was preserved. Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branc…
```

### Architecture

Reported aggregate response: 1,818 UTF-8 bytes, within 4 KiB.

**architecture lesson-first** — 512 summary bytes:

```text
For the single-worker importer, keep the schema cache process-local to avoid cross-worker invalidation state. This assumes one worker owns the whole import; parallel workers would require revisiting ownership before sharing the cache. The decision reduces coordination, not database reads across independent runs. Evidence: importer ADR 7.
Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branch and temporary worktree were removed; the operato…
```

**architecture delivery-first** — 512 summary bytes:

```text
Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branch and temporary worktree were removed; the operator checkout was preserved. Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branch and temporary worktree were removed; the operator checkout was preserved. Delivery record: PR 42 merged after unit tests, end-to-end checks, independent review, and CI passed. The feature branc…
```

## Editorial interpretation and limits

The bug prefix exposes the action (reuse the key), cause (a committed request with a lost response), consequence (duplicate charge), version, and endpoint qualification. A future agent can distinguish safe retry from reconciliation without retrieving the full content merely to find that lesson.

The architecture prefix exposes the cache ownership choice, coordination rationale, single-worker assumption, and the condition that requires reconsideration. It does not promise fewer reads across independent runs. The delivery-first counterparts expose none of these findings despite containing the identical knowledge later.

The routine-delivery and no-durable-knowledge examples are disposition judgments, so they are not inserted as Memories to manufacture Recall results. The vendor case example shows when an external artifact identity itself prevents rediscovery while preserving an unresolved cause. Review should assess all four contrasts independently of the mechanical prefix checks.

These observations establish that the representative long examples expose their actionable lesson and essential qualifications in this ordinary response. They do not guarantee candidate inclusion or summary visibility in every aggregate response, nor establish general gains in agent behavior. A content prefix is not a semantic summary; essential uncertainty and applicability still take precedence over fitting a byte target.
