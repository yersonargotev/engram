---
name: engram-memory-cli
description: Recall and finalize durable project Memory with Engram's CLI. Use for history-dependent work, Terminal Memory commits, explicit curation, or material loss-risk handoffs. Scope this skill to project memory.
metadata:
  version: "3.3.1"
---

# Engram Memory CLI

Engram is local-first, best-effort project memory. Memory can inform or preserve
work; the primary deliverable remains independent from memory availability.

## Best-effort protocol

1. Confirm that `engram` is available. For tasks about Engram itself, keep a CLI
   failure as task evidence and diagnose it within scope. For other tasks,
   continue without memory when the CLI is unavailable or fails.
2. Run `engram current-project --json` before the first project-scoped operation.
3. Treat detection and authority separately. Automatic candidate Recall and
   writes require `project_strength` to be `strong` or `explicit`. Never turn a weak
   `git_root`, `git_child`, or `dir_basename` result into authority by copying it
   into `--project`. Ask the user for the exact project on an explicit memory
   task; otherwise skip the write and continue. When `project` is empty, ask the
   user to select from `available_projects` for an explicit memory task.
4. Pass an exact project to every command that accepts it: use
   `--project <project>` for project-scoped flags and positional `[project]` for
   `engram context`.
5. Use `--json` for agent operations. Parse successful stdout as JSON and
   non-zero stderr as `{"code","message","details?"}`.

Complete this protocol when one exact project is known or memory use has been
skipped without delaying the primary deliverable.

## Recall

Recall only when prior project knowledge could materially change the work.

Use Recall for relevant prior decisions, tracked work, release state,
configuration, preferences, known failures, or an explicit request to remember.
Routine self-contained work needs no search. Personal or cross-project scope
requires explicit task relevance or user direction.

1. Search one lookup intent with one to three distinctive anchors:

   ```bash
   engram search "<narrow query>" --project "<project>" \
     --scope project --match-mode all --limit 5 --json
   ```

2. The initial response is limited to five candidate summaries and 4 KiB. Core
   excludes inactive, deleted, and superseded Memories; relevance/currentness
   rank before pins and recency. Account for every result and explicit conflict.
   Use the response's `recall_id` and one selected candidate's opaque
   `result_id` only when complete content can change the task:

   ```bash
   engram get --recall-id '<recall-id>' --result-id '<result-id>' \
     --project '<project>' --scope project --json
   ```

   One response returns at most 16 KiB of valid UTF-8 content and reports
   `original_bytes`, `delivered_utf8_bytes`, `limit_bytes`, and `truncated`.
   When `truncated` is true, request more only with the exact returned byte
   position; do not infer a position or widen the original scope:

   ```bash
   engram get --recall-id '<recall-id>' --result-id '<result-id>' \
     --position '<continuation_position>' --project '<project>' \
     --scope project --json
   ```
3. If a material memory is expected and the first search is empty or too broad,
   reformulate at most once. Remove generic terms, choose a more distinctive anchor, or
   switch to `--match-mode any`; keep the same lookup intent.
4. A deliberate follow-up may use `--limit 6` through `--limit 10` without
   widening scope or bypassing the 4 KiB candidate budget.
5. Request chronological context separately when recent session continuity can
   materially change the work:

   ```bash
   engram context "<project>" --scope project --json
   ```

6. Treat empty Recall as successful. If Recall is unavailable, continue the
   primary task after reporting the one warning and structured diagnostics.

Use `--all-projects` only for an explicitly cross-project request. Complete
Recall when every relevant result is accounted for, or when the initial search
and its single allowed reformulation are empty.

## Terminal Memory commit

For normal agent work, preserve reusable project knowledge through the root
turn's Terminal Memory commit. Use the opaque identity supplied by the host;
never synthesize a replacement identity.

1. Apply the canonical `engram-memory` disposition rubric after all causal work
   settles.
2. Draft prospective Memories, then run the bounded read-only preflight. Reuse
   exact duplicates and account for every returned candidate (at most three):

   ```bash
   engram checkpoint preflight --project '<project>' \
     --memory-json '{"title":"<concise title>","content":"<durable result>"}' --json
   ```
3. For `saved`, attach existing Memory IDs or create concise Memories atomically
   with repeatable `--memory-json` values:

   ```bash
   engram checkpoint record --host '<host>' --session-id '<session>' \
     --root-turn-id '<root-turn>' --disposition saved --project '<project>' \
     --memory-json '{"title":"<concise title>","content":"What: <durable result>\nWhy: <future value>\nWhere: <subsystem or path>\nLearned: <non-obvious implication>"}' --json
   ```
4. When the rubric finds no durable result, record `skipped` with identity,
   reason, optional Recall feedback, and JSON output:

   ```bash
   engram checkpoint record --host '<host>' --session-id '<session>' \
     --root-turn-id '<root-turn>' --disposition skipped \
     --reason no_durable_knowledge --json
   ```

   Project and Memory reference flags belong to `saved` and `needs_review`, not
   `skipped`.
5. Use `needs_review` with one redacted `--proposal-json` when potentially
   durable knowledge cannot be admitted directly; include any independently
   settled `--memory-id` or `--memory-json` values in the same Mixed Memory
   checkpoint.
6. Normal finalization ends on the `checkpoint record` result. `created` or a
   same-disposition `already_recorded` proves completion. For a structured
   argument or semantic error, correct the input and replay the same identity
   and disposition. Surface conflicts and persistence failures without changing
   the disposition.
7. Reserve `checkpoint status` for explicit inspection or an ambiguous process
   or transport outcome where `checkpoint record` returned no result. In that
   recovery branch, `checkpoint_not_found` means the terminal result remains
   absent, so issue the corrected `checkpoint record` call.

Complete normal preservation when the exact root-turn identity returns `created`
or same-disposition `already_recorded`; the record result is the routine
completion signal.

## Explicit atomic supersession

Apply the canonical `engram-memory` replacement judgment. When the evaluated
preflight candidate exposes `target_version` (Protocol v2), attach a repeatable
`--supersession-json` declaration to the same `checkpoint record` that commits
the replacement:

```bash
engram checkpoint record --host '<host>' --session-id '<session>' \
  --root-turn-id '<root-turn>' --disposition saved --project '<project>' \
  --memory-json '{"title":"<replacement>","content":"<verified current guidance>"}' \
  --supersession-json '{"replacement_input_index":0,"target_memory_id":42,"target_version":"<preflight version>","reason":"<evaluated replacement rationale>"}' --json
```

`replacement_input_index` is zero-based within repeated `--memory-json` values.
For an existing replacement, use `replacement_memory_id` instead and attach the
same ID through `--memory-id`. Use exactly one selector. The target must belong
to the checkpoint project and still match the evaluated version. A stale or
invalid declaration rejects the complete new transaction; reevaluate before
retrying the unchanged root identity. Exact replay returns the original result
without applying new declarations. `needs_review` may include settled
supersessions with its isolated proposal.

Success preserves historical target content and the directed relationship while
ordinary Recall excludes the superseded target. Prose alone does not retire it.
Without `target_version`, report unavailable atomic support; independent curation
is a separate maintenance workflow, not a post-checkpoint completion step.

## Cross-repository checkpoint examples

When a conversation changes repositories, or record returns
`checkpoint_project_mismatch`, apply the canonical `engram-memory` section
**Project binding and cross-repository work** before choosing recovery. The
commands below illustrate that policy through the CLI; they do not grant write
authority. In real work, use the exact host-supplied identity and establish each
project's authority through the normal detection/explicit-intent protocol.

These fixtures use synthetic identities **only inside disposable state**. Run
the block in a subshell so the data-directory override cannot affect later work:

```bash
(
  fixture_dir=$(mktemp -d)
  trap 'rm -rf "$fixture_dir"' EXIT
  export ENGRAM_DATA_DIR="$fixture_dir"

  # Same-project work: both turns return created in project-a.
  engram checkpoint record --host codex --session-id fixture-session \
    --root-turn-id fixture-turn-1 --disposition saved --project project-a \
    --memory-json '{"title":"Fixture A","content":"Synthetic first-project knowledge."}' --json
  engram checkpoint record --host codex --session-id fixture-session \
    --root-turn-id fixture-turn-2 --disposition saved --project project-a \
    --memory-json '{"title":"Fixture A2","content":"Synthetic next-turn knowledge."}' --json

  # Cross-repository task: project-only preflight succeeds for B.
  engram checkpoint preflight --project project-b \
    --memory-json '{"title":"Fixture B","content":"Synthetic destination knowledge."}' --json

  # The same session cannot create inline Memories in B: nonzero exit,
  # checkpoint_project_mismatch. The failed turn remains unfinalized.
  engram checkpoint record --host codex --session-id fixture-session \
    --root-turn-id fixture-turn-3 --disposition saved --project project-b \
    --memory-json '{"title":"Fixture B","content":"Synthetic destination knowledge."}' --json

  # Conditional recovery: assume independently useful A handoff content
  # and authority to store it in A. This does not store the B knowledge.
  engram checkpoint preflight --project project-a \
    --memory-json '{"title":"Fixture handoff","content":"Synthetic project-a handoff about unresolved project-b work."}' --json
  engram checkpoint record --host codex --session-id fixture-session \
    --root-turn-id fixture-turn-3 --disposition saved --project project-a \
    --memory-json '{"title":"Fixture handoff","content":"Synthetic project-a handoff about unresolved project-b work."}' --json

  # Same identity/disposition: already_recorded; no second Memory is added.
  engram checkpoint record --host codex --session-id fixture-session \
    --root-turn-id fixture-turn-3 --disposition saved --project project-a \
    --memory-json '{"title":"Fixture handoff","content":"Synthetic project-a handoff about unresolved project-b work."}' --json
)
```

The rejection above is expected; run the fixture without shell `errexit` so the
recovery commands execute. The handoff is a conditional example, not the
required disposition after every failure. When no legitimate correction exists,
report the incomplete checkpoint rather than substituting a false skip.

For an ambiguous process/transport result only, inspect with
`engram checkpoint status --host '<host>' --session-id '<session>' --root-turn-id '<root-turn>' --json`.
`checkpoint_not_found` permits a corrected retry with that same identity; an
existing terminal result must be respected. A structured ownership rejection
already identifies the failed operation and does not require this extra call.

MCP exposes the same Core restriction through `mem_checkpoint`: preflight uses
`operation: "preflight"`, `project`, and prospective `memories`; record carries
`host`, `session_id`, `root_turn_id`, `disposition`, `project`, and `memories`.
Use `mem_checkpoint_status` only for explicit inspection or ambiguous recovery.
The shell fixtures exercise CLI behavior; shared MCP behavior is supported by
Core delegation and adapter tests, not implied to be a live MCP transcript.

## Independent save

Use `engram save` only for explicit curation or a long-running, material
loss-risk handoff that must preserve knowledge before the root turn settles.
The later Terminal Memory commit still finalizes the root turn and may attach
the saved Memory by ID.

For this branch, read [references/curation.md](references/curation.md) and follow
its save, topic-key, candidate-judgment, and authorization rules.

## Curate

For editing, review, context priority, relations, diagnosis, or project merges,
read [references/curation.md](references/curation.md). Load it only for those
branches and follow every completion and authorization gate it defines.
