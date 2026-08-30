# Codex Engram activation: MCP/plugin versus CLI + skill

Observation window: `2026-08-29T00:00:00Z` inclusive through
`2026-08-30T00:30:00Z` exclusive.

Status: point-in-time operational research; this is not normative product
documentation.

## Conclusion

The maintainer's observation is **partly confirmed**.

- **Confirmed, moderate confidence:** recent Codex sessions capture Engram's
  lifecycle much more reliably after the plugin/hook repair. Among comparable
  closed sessions, checkpoint coverage changed from `0/1` to `7/7`, while
  combined prompt-plus-checkpoint coverage changed from `0/1` to `6/7`.
- **Confirmed:** Engram MCP is enabled and has been used. Codex's current unified
  tool transport records MCP calls inside `custom_tool_call: exec`, rather than
  as top-level `mcp__engram__*` tool names. Parsing that nested transport found
  30 Engram MCP invocation records across eight rollouts in the observation
  window, including 18 calls across four root rollouts.
- **Not causally established:** MCP cannot receive sole credit for the lifecycle
  improvement. The plugin simultaneously added prompt/session hooks, a mandatory
  root-turn checkpoint cue, Stop verification, and a canonical memory skill.
  Several post-repair delivery sessions completed checkpoints without calling
  MCP, so hooks and the activation cue are sufficient explanations for that
  part of the improvement.

The most defensible interpretation is:

> The complete plugin integration improved reliability. MCP probably increases
> model-visible salience and lowers the friction of memory operations, but the
> current observational sample does not isolate its causal contribution.

This report measures activation and lifecycle capture. It does not measure
Memory quality or justify automatic Admission.

## Sources and boundary

Only primary sources were used:

- Engram's local SQLite tables: `sessions`, `user_prompts`,
  `memory_checkpoints`, and `observations`.
- Local Codex rollout JSONL files from the observation window. The analysis
  published only counts and transport shapes, never prompts, Memory bodies,
  tool arguments, private paths, or opaque identifiers.
- Runtime configuration from `codex plugin list`, `codex mcp list`, and
  `engram setup status codex --json`.
- Engram source and repository documentation:
  [`plugin/codex/hooks/hooks.json`](../../plugin/codex/hooks/hooks.json),
  [`plugin/codex/.mcp.json`](../../plugin/codex/.mcp.json),
  [`plugin/codex/scripts/user-prompt-submit.sh`](../../plugin/codex/scripts/user-prompt-submit.sh),
  [`internal/mcp/mcp.go`](../../internal/mcp/mcp.go), and
  [`docs/AGENT-SETUP.md`](../AGENT-SETUP.md#codex).
- Official OpenAI documentation for [MCP](https://developers.openai.com/codex/mcp/)
  and [skills](https://developers.openai.com/codex/skills/).

The frozen window above prevents later sessions and appended rollout events from
changing the published point-in-time counts. Within that window, the lifecycle
comparison uses **2026-08-29T17:14:22Z** as a conservative repair boundary. The
aggregate report in
[#50](https://github.com/yersonargotev/engram/issues/50#issuecomment-5463765830)
states that the Codex lifecycle integration had been repaired and verified by
then. This boundary separates pre-repair from post-repair behavior; it does not
claim the exact load time of every Codex process.

The former #61 held-out source was retired before this operational analysis. It
was never executed by Admission and can no longer be reused as held-out evidence.

## What “activation” means here

The investigation separates three observable layers:

1. **Lifecycle activation:** a closed Codex session has a captured prompt and a
   root-turn checkpoint.
2. **Agent-initiated Engram use:** a rollout explicitly invokes Engram through
   MCP or CLI.
3. **Skill selection:** Codex explicitly or implicitly selects a skill.

Only the first two have adequate local telemetry. Official OpenAI documentation
states that skills may be invoked explicitly or selected implicitly when the
task matches their description. The rollout format does not provide a stable,
dedicated event proving implicit skill selection, so this report does not invent
a skill-activation rate.

## Lifecycle result

A session counts as closed only when `ended_at` is non-null. A closed session
has a complete captured lifecycle when it has at least one persisted prompt and
at least one checkpoint. A closed zero-prompt session stays in the denominator;
it is an integration failure, not an empty sample to discard.

| Cohort | Sessions | Open/abandoned | Closed | With prompt | With checkpoint | Prompt + checkpoint |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before repair | 2 | 1 | 1 | 1/1 | 0/1 | 0/1 |
| After repair | 7 | 0 | 7 | 6/7 | 7/7 | 6/7 |

Checkpoint coverage changed from `0%` to `100%`; full lifecycle coverage changed
from `0%` to `85.7%`. Prompt capture alone does not prove improvement because
the sole comparable pre-repair session already had a prompt and one post-repair
session did not.

That post-repair failure is the #61 triage session. Keeping it in the denominator
prevents the positive result from hiding the known prompt-telemetry gap.

The sample is small, especially before repair, so these percentages demonstrate
an operational step change rather than a statistically stable rate.

## Why the plugin/hooks explain lifecycle reliability

The enabled Engram plugin declares `SessionStart`, `UserPromptSubmit`, `Stop`,
and `SessionEnd` hooks. Its prompt hook persists the submitted prompt and returns
the root-turn identity as additional context; the Stop hook verifies that the
same turn has a terminal checkpoint. These are exactly the surfaces needed to
explain the new checkpoint coverage.

The current setup snapshot reports the plugin, prompt hook, session hook,
activation cue, and Stop verifier as ready. The plugin manifest also bundles the
canonical memory skill and Engram MCP registration. Therefore “plugin active” is
not one treatment: it combines deterministic lifecycle capture with additional
model guidance and tools.

## MCP is active, visible, and used

Runtime evidence is unambiguous:

- `codex plugin list` reports `engram@engram` installed and enabled.
- `codex mcp list` reports the `engram` server enabled with the `agent` tool
  profile.
- Rollouts contain actual calls such as `tools.mcp__engram__mem_checkpoint(...)`,
  `mem_session_summary`, `mem_search`, and `mem_save` inside the unified
  `custom_tool_call: exec` transport.

A direct-name-only query returns zero because the top-level tool is named
`exec`. That result is a transport artifact, not evidence that MCP was unused.
A nested-call query found:

| Rollout kind | Rollouts with Engram MCP calls | MCP call records |
| --- | ---: | ---: |
| Root | 4 | 18 |
| Subagent | 4 | 12 |
| Total | 8 | 30 |

`engram setup status codex --json` currently labels the MCP configuration
`customized` and its attributable readiness `unavailable` because the configured
command is a development binary rather than the exact supported setup contract.
That diagnostic means “the setup checker cannot establish supported provenance,”
not “Codex cannot run this MCP server.” `codex mcp list` and the observed
invocations are the runtime evidence.

## Why MCP can make activation feel stronger

The mechanism is plausible and documented. Official OpenAI documentation says
Codex reads an MCP server's `instructions` at initialization and uses them as
server-wide guidance alongside its tools. Engram's MCP server provides
instructions that declare nine core memory tools “always available,” require a
session summary before completion, and direct proactive saving after decisions,
bug fixes, discoveries, and conventions. The server registers those instructions
through `server.WithInstructions(serverInstructions)` in
[`internal/mcp/mcp.go`](../../internal/mcp/mcp.go).

By contrast, official OpenAI skill documentation says implicit skill activation
depends on the skill description matching the user's task. A standalone skill
can therefore remain available without being selected. MCP adds persistent
server-level instructions and immediately callable tools, so it has a broader
opportunity to influence behavior than an implicitly matched skill alone.

This supports the user's perception, but it is still mechanism-plus-correlation,
not a controlled causal estimate.

## CLI remains important

CLI did not disappear after MCP became available. A conservative scan found at
least 13 explicit Engram CLI invocations in the two pre-boundary rollouts and 25
in the seven post-boundary rollouts. Recent delivery sessions completed their
checkpoint workflows through CLI without any MCP call in those rollouts.

The observed architecture is therefore additive:

```text
plugin hooks  -> deterministic lifecycle capture and Stop enforcement
activation cue -> mandatory root-turn checkpoint behavior
MCP            -> persistent guidance plus low-friction memory tools
CLI + skill    -> explicit, inspectable fallback and specialized workflow
```

MCP appears to increase salience; it has not replaced CLI.

## Reproducible aggregate checks

The lifecycle query omits identifiers and content:

```bash
ENGRAM_DB_PATH="$HOME/.engram/engram.db"
sqlite3 -header -column "$ENGRAM_DB_PATH" '
WITH per_session AS (
  SELECT s.id, s.started_at, s.ended_at,
         COUNT(DISTINCT p.id) AS prompt_n,
         COUNT(DISTINCT c.id) AS checkpoint_n
  FROM sessions AS s
  LEFT JOIN user_prompts AS p ON p.session_id = s.id
  LEFT JOIN memory_checkpoints AS c ON c.session_id = s.id
  WHERE s.project = "engram"
    AND s.started_at >= "2026-08-29 00:00:00"
    AND s.started_at < "2026-08-30 00:30:00"
  GROUP BY s.id
), cohort AS (
  SELECT CASE WHEN started_at < "2026-08-29 17:14:22"
              THEN "pre-repair" ELSE "post-repair" END AS cohort, *
  FROM per_session
)
SELECT cohort,
       COUNT(*) AS all_sessions,
       SUM(ended_at IS NULL) AS open_sessions,
       SUM(ended_at IS NOT NULL) AS closed_sessions,
       SUM(ended_at IS NOT NULL AND prompt_n > 0) AS closed_with_prompt,
       SUM(ended_at IS NOT NULL AND checkpoint_n > 0) AS closed_with_checkpoint,
       SUM(ended_at IS NOT NULL AND prompt_n > 0 AND checkpoint_n > 0)
         AS closed_full_lifecycle
FROM cohort GROUP BY cohort;'
```

The MCP audit must inspect nested unified-tool inputs while emitting counts only:

```bash
CODEX_SESSION_DIR="${CODEX_HOME:-$HOME/.codex}/sessions/2026/08/29"
rg --files -0 "$CODEX_SESSION_DIR" -g '*.jsonl' |
  xargs -0 -n1 jq -sr \
    --arg start '2026-08-29T00:00:00Z' \
    --arg cutoff '2026-08-30T00:30:00Z' '
    ([.[] | select(.type == "session_meta") |
      .payload.originator][0] // "unknown") as $originator |
    ([.[] |
      select(.timestamp >= $start and .timestamp < $cutoff) |
      select(.type == "response_item" and
        (.payload.type == "function_call" or
         .payload.type == "custom_tool_call")) |
      ((.payload.name // "") + "\n" +
       ((.payload.arguments // .payload.input // "") | tostring)) |
      select(test("tools\\.mcp__engram__mem_[A-Za-z0-9_]+\\s*\\("))
    ] | length) as $calls |
    select($calls > 0) |
    [(if $originator == "codex-tui" then "Root"
      elif $originator == "codex_exec" then "Subagent"
      else "Other" end), $calls] | @tsv' |
  awk -F '\t' '
    { rollouts[$1]++; calls[$1] += $2 }
    END {
      print "Root", rollouts["Root"] + 0, calls["Root"] + 0
      print "Subagent", rollouts["Subagent"] + 0, calls["Subagent"] + 0
      print "Total", rollouts["Root"] + rollouts["Subagent"] + rollouts["Other"],
        calls["Root"] + calls["Subagent"] + calls["Other"]
    }'
```

This reads tool-call envelopes locally and prints only per-rollout counts. It
must not publish arguments or source evidence.

## Limitations and the causal test

- The pre-repair closed cohort contains one session.
- Tasks were not paired, and plugin, hooks, cue, skill, MCP configuration, and
  operator procedure changed close together.
- Some long-lived or resumed rollouts span configuration changes.
- Unified tool transport requires nested-call parsing; direct tool-name counts
  are incomplete.
- Prompt hooks are best-effort, and one post-repair failure remains.
- A checkpoint proves protocol completion, not Memory quality.

A causal test should keep the same plugin, hooks, cue, skill, model, and task
shape, then alternate only MCP availability across paired prospective sessions.
It should compare lifecycle coverage, successful MCP operations, CLI fallback,
and qualitative Memory usefulness using aggregate-only reporting. Until that
test exists, keep the full plugin integration and describe MCP as a plausible
activation amplifier, not the proven cause of the lifecycle improvement.
