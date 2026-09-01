# Explicit Recall Feedback

Engram can attach optional, content-free Recall feedback to a terminal Memory
checkpoint. Feedback measures only an exact Recall run and the opaque results
that run exposed in the same exact root user turn. It does not change the
checkpoint disposition or completion.

## Record feedback

Bind Recall when it executes by providing the exact checkpoint identity to
`mem_search` as the all-or-none `host`, `session_id`, and `root_turn_id`
fields. The CLI equivalent is:

```bash
engram search '<query>' --project '<project>' \
  --host '<host>' --session-id '<session>' --root-turn-id '<root turn>' --json
```

Unbound Recall remains available for callers that do not record feedback, but
a later checkpoint cannot claim that run. A different root-turn identity is
rejected visibly while the terminal checkpoint itself remains complete.

Pass `recall_feedback` to `mem_checkpoint`, or the equivalent
`--recall-feedback-json` value to `engram checkpoint record`:

```json
{
  "recall_id": "<exact Recall run>",
  "results": [
    {
      "result_id": "<opaque exposed result>",
      "utility": "decisive",
      "quality": "current",
      "source": "agent_explicit"
    }
  ]
}
```

Every result must belong to that run. `utility` is one of `decisive`,
`orienting`, `duplicate`, or `unused`; `quality` is one of `current`, `stale`,
`contradictory`, or `unknown`. At least one of `utility` or `quality` is
required. `source` is `agent_explicit`, `user_explicit`, or `evaluator`.

An explicitly reviewed zero-result run uses `false_empty` instead of result
labels:

```json
{
  "recall_id": "<exact empty Recall run>",
  "false_empty": {"value": true, "source": "evaluator"}
}
```

Labels are explicit evidence. Omitted feedback remains unknown: it is not
classified as unused, current, false-empty, or failed. Retrieval, ranking,
citation, or checkpoint disposition never implies a label. Use
`agent_explicit` only for the agent's stated assessment, `user_explicit` only
for a direct user assessment, and `evaluator` only for a separately invoked
evaluator.

When a bound Recall completes, Store snapshots only salted run, turn, and
Memory keys plus content-free measurements for every exposed result. It creates
no label. This keeps the unknown denominator stable if a Memory is later hard
deleted; labels remain exclusively explicit.

The terminal checkpoint commits first. The response reports the feedback
sidecar as `recorded`, `already_recorded`, or `failed`; a feedback failure never
reclassifies or rolls back the checkpoint. Exact checkpoint replay may add or
retry the sidecar. The same turn/Memory/source tuple is idempotent when its
labels match and conflicts when they differ. A different source appends a
distinct assessment.

## Local privacy boundary

Core validates raw checkpoint, Recall, and result identities transiently, then
stores only per-install salted turn, run, and Memory keys. Attribution storage has
no raw host/session/turn/Recall/result/Memory identifiers and no prompt, query,
Memory content, assistant response, transcript, or diff.

Feedback and its aggregate report remain outside Memory, FTS, Recall, context,
statistics, ordinary export/import, sync, cloud, Obsidian, Diagnostic capture,
and retired candidate-evaluation or publishing pipelines. The per-install HMAC
key is created locally with owner-only permissions.

## Aggregate report

Run the report separately from Recall and checkpointing:

```bash
engram recall-feedback report
engram recall-feedback report --json
```

The `recall-feedback-report-v1` output is aggregate-only. It exposes no salted
or raw identity. For all successful recorded Recall operations it reports event
counts, search result volume, delivered UTF-8 bytes, and p50/p95 monotonic
latency. Search and first-delivery segment latency is finalized only after its
primary Store persistence succeeds. Missing historical measurements remain
explicit unknown samples.

Let `E` be the distinct `(root turn, Memory)` pairs exposed by root-bound search
runs and `Z` all root-bound zero-result search runs. Repeated exposure of the
same Memory in one turn does not inflate coverage. The report uses these
denominators:

- Label coverage: distinct exposed results with any explicit label divided by
  `E`; unlabelled exposures are unknown.
- Utility: `decisive + orienting` divided by explicit utility labels for one
  source. Noise is `duplicate + unused` over that same denominator, and
  duplicate rate counts only `duplicate`. Exposures without that source's
  utility label are unknown.
- Harm: `stale + contradictory` divided by explicit quality labels for one
  source. Exposures without that source's quality label are unknown.
- False-empty: explicitly true reviews divided by all explicitly reviewed
  empty runs for one source. Unreviewed members of `Z` are unknown.
- Time-to-useful: for each root-bound turn, elapsed time from the first Recall
  start through completion of the earliest run that exposed a Memory later
  labelled `decisive` or `orienting` by that source. This includes one allowed
  reformulation. Turns without a useful label or a complete timeline are
  unknown.

Rates include a 95% Wilson confidence interval when their denominator is
non-zero. Duration percentiles use nearest-rank p50 and p95. The JSON output
always publishes numerator, denominator, and unknown counts so downstream
analysis does not reinterpret missing evidence.
