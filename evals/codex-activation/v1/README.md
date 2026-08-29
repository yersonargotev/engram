# Codex Activation Study v1

This directory contains the frozen, fresh-session cohort for measuring whether
repository-scoped guidance changes Codex activation of Engram's user skill and
CLI. It is an activation study, not an Admission study: it observes upstream
skill and command selection without changing Admission policy or enabling any
production integration.

## Frozen cohort

- Source: `6527374bfe7c45f3e54940f7496c05b677f1706c`
- Codex: `0.151.0`, `gpt-5.6-luna`, low reasoning, fresh ephemeral sessions
- Skills: frozen `engram-memory-cli` plus the five system skills bundled into the
  isolated Codex runtime; the exact inventory is verified for every cell
- Treatments: normal Engram, matched Engram with only the two Memory skill-index
  rows removed, and a synthetic neutral Git repository
- Integrations: plugin, MCP, prompt hooks, Stop verifier, apps, and multi-agent
  features disabled
- Runs: two repetitions of six paired synthetic prompt classes per treatment
- Evidence: model-visible skill reads from Codex JSONL plus canonical Engram CLI
  outcomes from a controlled shim

`contract.json` fixes the complete protocol. `contract.sha256` binds its exact
bytes and must be verified before any measured cell runs. A changed contract,
prompt, fixture builder, event schema, or analysis rule requires a new study
version; v1 must never be rewritten in response to visible results.

## Isolation and privacy

Every cell uses a newly copied repository, home, `CODEX_HOME`, Engram data
directory, tool directory, and temporary directory. The runner copies only the
frozen user skill and Codex authentication into that disposable state. It builds
and invokes the real Engram CLI from the frozen source revision against the
isolated store. The real source checkout, user configuration, installed
integrations, and Memory store are never mutated.

Raw Codex JSONL, final responses, shim paths, host session IDs, and local paths
are classified in memory and removed with the cell. The versioned event set
contains only synthetic protocol cell identities and canonical bounded values.
The report contains only aggregates, confidence intervals, omissions, deviations,
and separate answers to the four declared study questions.

## Commands

All commands require the frozen contract and sidecar. `verify` performs runtime,
fixture, treatment-visibility, skill-inventory, and cleanup checks without a
model run:

```bash
go run ./cmd/engram activation-study verify \
  --contract evals/codex-activation/v1/contract.json \
  --contract-hash evals/codex-activation/v1/contract.sha256 \
  --source-repo . \
  --user-skill "$HOME/.agents/skills/engram-memory-cli" \
  --auth-file "$HOME/.codex/auth.json" \
  --json
```

`run` executes missing cells and atomically updates the bounded event set after
each completed cell. Repeating the command with the same output resumes without
rerunning retained cells:

```bash
go run ./cmd/engram activation-study run \
  --contract evals/codex-activation/v1/contract.json \
  --contract-hash evals/codex-activation/v1/contract.sha256 \
  --source-repo . \
  --user-skill "$HOME/.agents/skills/engram-memory-cli" \
  --auth-file "$HOME/.codex/auth.json" \
  --output evals/codex-activation/v1/events.json \
  --json
```

`analyze` rejects incomplete or unverified event sets and deterministically emits
both machine-readable aggregates and the Markdown report:

```bash
go run ./cmd/engram activation-study analyze \
  --contract evals/codex-activation/v1/contract.json \
  --contract-hash evals/codex-activation/v1/contract.sha256 \
  --events evals/codex-activation/v1/events.json \
  --output evals/codex-activation/v1/report.json \
  --markdown-output evals/codex-activation/v1/report.md \
  --json
```

The committed outputs are `events.json`, `report.json`, and `report.md`. They may
be regenerated from the frozen inputs but must not include raw evidence.
