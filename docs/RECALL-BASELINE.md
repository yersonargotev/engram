# Content-free Recall baseline

The Recall baseline is an opt-in, local-only operational ledger for measuring
the current Codex Protocol before changing Recall or Capture defaults. It
records bounded counts, monotonic elapsed time, and UTF-8 byte volume. It does
not record the content that produced those measurements.

## Privacy and ownership boundary

Core owns the schema, aggregation, salted linkage, retention, and power
analysis in `internal/recallbaseline`. CLI, MCP, and Codex hooks are thin
observers of existing behavior.

The ledger is stored separately at
`$ENGRAM_DATA_DIR/recall-baseline-v1.db`, with mode `0600`. Its random HMAC key
is held in a separate `recall-baseline-v1.key` file, also with mode `0600`, and
is never embedded in the database or report. Neither file enters the Memory
database, FTS, Recall, context, ordinary export/import, sync, cloud, Obsidian,
or evaluation/publishing pipelines. Do not upload or sync either file.

Operational events contain only:

- `schema_version`, UTC occurrence and expiry timestamps;
- bounded `kind`, `surface`, `operation`, and `outcome` values;
- a per-install HMAC linkage key only for checkpoint coverage;
- optional monotonic latency in microseconds and delivered UTF-8 bytes.

There are no fields for prompts, queries, Memory or assistant text, transcript
paths, repository diffs, credentials, or raw host, session, turn, or Memory
identifiers. When lifecycle correlation is necessary, Core immediately derives
an HMAC-SHA256 key with a random 256-bit per-install secret held outside the
baseline database. The database or report plus ordinary Memory, prompt, sync,
cloud, and export identifiers cannot reproduce or join that key.

## Versioned contracts

| Contract | Version | Purpose |
|---|---|---|
| SQLite schema | `PRAGMA user_version = 1` | Local event and collection-loss storage |
| Event schema | `recall-baseline-events-v1` | Every persisted operational event |
| Report schema | `recall-baseline-report-v1` | Deterministic aggregate output |
| Power schema | `recall-baseline-power-v1` | Prospective controlled-study sizing |

The event vocabulary is closed. Lifecycle kinds are `checkpoint`, `stop`,
`capture`, and `subagent_stop`; operation surfaces are `cli`, `mcp`, and
`lifecycle`. Unknown operation names are rejected so content cannot be hidden
inside a label.

Reports keep lifecycle and operation surfaces separate. They include raw
denominators, unknown latency/byte/outcome counts, and non-blocking collection
loss. Each report embeds `protocol-compatibility-v1` with the exact four-axis
tuple: Managed Pack, Engram binary, Codex plugin, and Protocol contract,
including version and provenance for every axis.

## Collect the current behavior

Collection is disabled unless `ENGRAM_RECALL_BASELINE=1` is present in the
environment of the Engram process and Codex hooks. Enabling it only observes
existing paths; it does not add tools, change lifecycle injection, alter
Capture, run Recall, or change checkpoint decisions.

```bash
export ENGRAM_RECALL_BASELINE=1
export ENGRAM_RECALL_BASELINE_RETENTION_DAYS=7

# Run Codex and Engram normally during the declared baseline window.
# Inspect a content-free aggregate at any point.
engram recall-baseline report --json
```

The default retention is 7 days. `ENGRAM_RECALL_BASELINE_RETENTION_DAYS`
accepts an integer from 1 through 30. Reporting purges expired events first;
an explicit purge is also available:

```bash
engram recall-baseline purge --json
```

Current automatic observers cover checkpoint/Stop outcomes, prompt and
subagent Capture enablement, SubagentStop activity, SessionStart monotonic
latency and exact injected `additionalContext` UTF-8 bytes, every
regular configured CLI command, and every registered MCP tool call. CLI
`search`, `context`, `get`, and `checkpoint verify-stop` record exact delivered
bytes; other CLI commands retain their count and monotonic latency while
reporting bytes as unknown. Config-free self-test, help, version, frozen-study,
and baseline-management commands do not create self-referential events. MCP
observation is bounded and non-blocking; if its queue is full, the report
increments `collection.dropped_events` instead of delaying the tool. Missing
measurements remain visible as unknown counts.

`engram setup status codex [--json]` reads the unexpired aggregate
`lifecycle/session_start` observation through a separate read-only path. It
reports event/sample counts, p50/p95 latency, and total/average injected bytes.
The v1 baseline does not attribute historical events to a treatment, so status
labels the source as an aggregate instead of implying canary causality. If the
ledger is absent, status returns `not_observed` without creating a directory,
database, key, migration, or retention purge.

`engram recall-baseline record` is the stable adapter for a thin integration
that needs to submit one bounded event. Raw identity flags are accepted only
for immediate salted derivation of checkpoint coverage and are rejected for
every other event kind. Never place content in an argument; Core will reject
any unrecognized operation or outcome.

## Reproduce the prospective power analysis

Power analysis accepts only declared numeric assumptions. It has no dataset,
session, transcript, or held-out path input.

```bash
engram recall-baseline power \
  --baseline-rate 0.50 \
  --minimum-detectable-difference 0.10 \
  --alpha 0.05 \
  --power 0.80 \
  --comparisons 3 \
  --treatments 3 \
  --json
```

The v1 method is a conservative, two-sided two-proportion normal
approximation with Bonferroni correction for the declared familywise alpha.
The output repeats every assumption, reports per-comparison alpha and required
sample size per treatment and in total, and always reports
`held_out_accessed: false`.

## End a baseline window

Unset `ENGRAM_RECALL_BASELINE` to stop automatic collection. The local ledger
then remains available until its records expire. To retire the installation
deliberately, remove both `recall-baseline-v1.db` and
`recall-baseline-v1.key` while Engram is not running. Removing the key prevents
future linkage to the deleted window; removing only the database preserves the
same per-install key for a later local window.
