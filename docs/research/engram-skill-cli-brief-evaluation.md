# Engram skill, CLI, and Task Brief evaluation

Date: 2026-08-27

## Question

How much of Engram's observed behavior comes from its reusable skill and CLI,
how much comes from Task Brief, and does Codex activate Engram more often when
it runs inside the Engram repository?

This note separates documented behavior, a read-only snapshot of the active
machine, Task Brief implementation evidence, real briefing outputs, and
inference. It does not mutate Codex configuration or Engram memory.

## Executive conclusion

**Yes: Codex has a concrete reason to invoke Engram memory workflows more often
inside this repository.** The root [`AGENTS.md`](../../AGENTS.md#L1-L36) is
loaded as project guidance and explicitly requires Codex to inspect its skill
index, load every matching skill before coding, and use
`engram-memory-cli` whenever prior project knowledge may matter. The same index
also selects the broader `engram-memory-protocol` for decisions, discoveries,
bug fixes, preferences, and session closure.

Outside this repository, the user-installed `engram-memory-cli` skill remains
discoverable, so Engram is not repo-only. Its description is deliberately
selective, however: recall when prior project knowledge may change the work,
and preserve reusable knowledge after the work. That is a narrower activation
contract than Engram's repository instructions.

The active Codex installation does **not** currently have the Engram plugin or
MCP server enabled. Therefore, the extra activation observed in this session is
best explained by the repository's `AGENTS.md` plus the user-level CLI skill,
not by Engram plugin hooks or MCP tools.

The practical result is uneven but useful: **Engram works well here as explicit
memory assisted by a skill and CLI, but not yet as automatic session memory.**
Project detection, storage health, targeted search, and domain-specific briefs
are strong. Generic operational prompts such as “merge this PR and clean the
branch” can surface older workflow memories while omitting the memory for the
actual issue. The current live integration also records no Codex prompts and
has no active lifecycle hooks.

## Activation and discovery evidence

### 1. Repository instructions are automatically scoped by working directory

The [official OpenAI `AGENTS.md` guide](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
documents that Codex builds its instruction chain once per run, reads the
repository-root instructions, then walks toward the current working directory;
closer files appear later and override broader guidance. Consequently, Engram's
root `AGENTS.md` is active when Codex starts anywhere under this repository, but
is not project guidance when Codex starts in an unrelated repository.

Engram's root file adds unusually broad triggers:

- it says to load relevant skills **before writing code** and to follow all
  matched skill rules ([`AGENTS.md:3-10`](../../AGENTS.md#L3-L10));
- `engram-memory-protocol` matches decisions, fixes, discoveries, preferences,
  and session closure ([`AGENTS.md:24`](../../AGENTS.md#L24));
- `engram-memory-cli` matches work that may depend on prior project knowledge,
  durable findings, and memory curation ([`AGENTS.md:25`](../../AGENTS.md#L25)).

The protocol's own scope is broader still: it instructs an agent to search on
the first project-related message and to create a session summary before saying
the work is done
([`skills/memory-protocol/SKILL.md:40-56`](../../skills/memory-protocol/SKILL.md#L40-L56)).
That makes recall and preservation likely on most substantive Engram tasks even
when the user never names a memory skill.

### 2. The repository skills are indexed, not natively repo-discovered

The [official OpenAI skills guide](https://learn.chatgpt.com/docs/build-skills)
documents two activation paths: explicit `$skill-name` invocation and implicit
matching against the skill's `description`. It also documents automatic
repository discovery specifically under `.agents/skills` from the current
directory up to the repository root.

This repository stores its project skills under `skills/`, not
`.agents/skills`. They become effective because `AGENTS.md` explicitly indexes
those paths and tells Codex to read them; they are not evidence that Codex scans
an arbitrary `skills/` directory. This distinction explains why the project
catalog is present inside Engram but does not follow Codex to other repositories.

### 3. One Engram skill is user-scoped and available elsewhere

On this machine, `$HOME/.agents/skills/engram-memory-cli` is a symlink to the
Packy-managed copy of the skill. That is an official Codex user-scope discovery
location, so the skill can appear in any repository. Its current description
matches recall, preservation, and explicit curation
([`skills/engram-memory-cli/SKILL.md:1-4`](../../skills/engram-memory-cli/SKILL.md#L1-L4)).

Once selected, its workflow is intentionally conditional and best-effort:

- recall runs only when prior knowledge could materially change the work
  ([`skills/engram-memory-cli/SKILL.md:30-61`](../../skills/engram-memory-cli/SKILL.md#L30-L61));
- preservation excludes routine, documented, low-value, or non-reusable facts
  ([`skills/engram-memory-cli/SKILL.md:63-96`](../../skills/engram-memory-cli/SKILL.md#L63-L96)).

Thus the same CLI skill can activate elsewhere, but ordinary task matching
should be less aggressive without Engram's root instruction index and memory
protocol.

### 4. The plugin's `ALWAYS ACTIVE` language is not causing this session

The repository's Codex plugin bundles a different `engram-memory` skill whose
description says `ALWAYS ACTIVE` and whose body mandates proactive saves
([`plugin/codex/skills/memory/SKILL.md:1-20`](../../plugin/codex/skills/memory/SKILL.md#L1-L20)).
That phrase is a skill description intended to make implicit matching broad; it
does not override the documented progressive-disclosure model. OpenAI documents
that Codex initially receives skill names and descriptions and reads full
`SKILL.md` content only after selecting a skill.

More importantly, a read-only snapshot on 2026-08-27 showed:

| Check | Observed state |
| --- | --- |
| `codex plugin list` | Marketplace `engram` registered; `engram@engram` **not installed** |
| `codex mcp list` | No `engram` server |
| `~/.codex/config.toml` | Marketplace source present, pointing at the Git repository; no active Engram plugin/MCP entry |

The plugin manifest would bundle hooks and an MCP declaration if installed
([`plugin/codex/.codex-plugin/plugin.json:1-16`](../../plugin/codex/.codex-plugin/plugin.json#L1-L16),
[`plugin/codex/.mcp.json:1-7`](../../plugin/codex/.mcp.json#L1-L7)). Its hook
catalog includes session, prompt, subagent-stop, and session-end events
([`plugin/codex/hooks/hooks.json:1-62`](../../plugin/codex/hooks/hooks.json#L1-L62)).
Because the plugin is not installed and the MCP is absent, those surfaces cannot
explain the current behavior.

## CLI and Task Brief boundary

The user-level skill invokes the CLI explicitly. It first resolves the project
with `engram current-project --json`, then requests one briefing with
`engram context <project> --brief --task ...`
([`skills/engram-memory-cli/SKILL.md:11-39`](../../skills/engram-memory-cli/SKILL.md#L11-L39)).

A read-only comparison confirmed that project resolution depends on the working
directory. The Engram checkout resolves from [`.engram/config.json`](../../.engram/config.json),
not from a hard-coded project exception; the detector then falls back through
Git remote, Git root, child repository, and finally directory basename
([`internal/project/detect.go:75`](../../internal/project/detect.go#L75)):

| Working directory | Resolved project | Source |
| --- | --- | --- |
| Engram repository | `engram` | configured project path |
| Packy repository | `packy` | Git remote |
| `/tmp` | `tmp` | directory basename |

This explains **which memory project** the skill queries. It also exposes a
write-safety gap in the skill contract: `dir_basename` is a valid fallback but
is much weaker evidence than `config` or `git_remote`. From `/tmp`, a blind save
could target a plausible but unintended project named `tmp` even though the
skill says to require an exact project.

An explicit `engram` project requested from `/tmp` still retrieves Engram
memories, but Task Brief reports `repository_project_unresolved` and excludes
repository signals. That behavior provides a useful task-and-memory-only
control for the ranking experiments below.

## Task Brief empirical evaluation

### Method

The evaluation used the installed `engram 2.2.1` against the existing local
database. Briefs were run both in the Engram checkout and from `/tmp` with the
project explicitly set to `engram`. The second form deliberately removes Git
signals. This matters because Task Brief always inspected the current working
tree before building evidence ([`generator.go:186`](https://github.com/yersonargotev/engram/blob/v2.2.1/internal/taskbriefing/generator.go#L186),
[`repository.go:32`](https://github.com/yersonargotev/engram/blob/v2.2.1/internal/taskbriefing/repository.go#L32)).

Creating this report itself changed the in-repository result: its untracked path
contributed `untracked_path` terms such as `engram`, `skill`, `cli`, and `md`.
That is expected behavior, but it means a brief cannot be treated as a stable
memory-only ranking when the worktree changes.

No memory was written for this evaluation. The returned memories and
diagnostics were inspected as structured JSON.

### Results

| Task/query | Returned evidence | Assessment |
| --- | --- | --- |
| `make Codex setup reproducible and ownership-aware for issue 43` from `/tmp` | #146, the distribution-authority decision, and #155, the exact Codex setup reproducibility bugfix | Strong. It recovered both the governing decision and implementation contract. Three qualified results were outside the cap and three more were removed by the output budget. |
| `evaluate how Engram task brief skill and CLI behave in the Engram repository` from `/tmp` | #104, #82, and #80, all about the memory CLI or Task Brief | Relevant, although repetitive. Two qualified results were outside the cap and two more were removed by the output budget. |
| `merge PR 56 for issue 43 and clean only its branch after protected validation` during the real merge | Only old memory #84 survived, describing PR #31 and Codex sync cleanup | Weak topical precision. The exact setup memory #155 was rejected before scoring; nine qualified results missed the five-result cap and four more were removed by the 4096-byte output budget. |
| The same merge task from `/tmp`, after #156 recorded the completed PR #56 delivery | Returned old PR #54 and PR #9 workflow summaries (#150, #151, #55), not #156 | Still weak. Removing Git evidence stopped the old branch/diff boost, but generic workflow matches and deterministic ties still dominated the returned budget. |
| `engram search 'Codex setup reproducibility' --project engram --match-mode any` | #155 first, followed by #156, #146, #144, and #147 | Strong fallback. A compact, distinctive query found the exact implementation and delivery memories immediately. |

### Why the merge brief missed the subject memory

The behavior is deterministic, not random and not an intentional preference for
old memories:

1. Task terms are lowercased and deduplicated by the bounded collector
   ([`terms.go:13`](https://github.com/yersonargotev/engram/blob/v2.2.1/internal/taskbriefing/terms.go#L13)), and the calibrated
   task vocabulary is prefix-capped at 12
   ([`generator.go:88`](https://github.com/yersonargotev/engram/blob/v2.2.1/internal/taskbriefing/generator.go#L88)). The merge
   task had 14 normalized terms, so `protected validation` was omitted.
2. A memory must match at least half of the retained task terms before any
   repository evidence can help it. #84 matched exactly 6 of 12 generic terms:
   `merge`, `pr`, `for`, `issue`, `and`, and `its`. #155 matched only 4:
   `and`, `clean`, `for`, and `only`, so it failed the task gate
   ([`generator.go:237`](https://github.com/yersonargotev/engram/blob/v2.2.1/internal/taskbriefing/generator.go#L237)).
3. #84 then matched branch, branch-diff, affected-path, and commit-subject
   signals and reached a score of 35. The repository evidence was itself heavily
   truncated: 15,037 branch-diff terms and 15 affected-path terms were omitted.
4. Recency is deliberately not a relevance signal
   ([`CALIBRATION.md:55`](https://github.com/yersonargotev/engram/blob/v2.2.1/internal/taskbriefing/prototype/CALIBRATION.md#L55)).
   Once scores tie, the deterministic ordering uses title and ID rather than
   freshness ([`generator.go:275`](https://github.com/yersonargotev/engram/blob/v2.2.1/internal/taskbriefing/generator.go#L275)).
5. The CLI then removes complete, lowest-ranked memories until the full response
   fits 4096 bytes ([`context_briefing.go:56`](https://github.com/yersonargotev/engram/blob/v2.2.1/cmd/engram/context_briefing.go#L56)).

The diagnostics correctly report input truncation, result-cap omissions, and
budget omissions. They do **not** report memories rejected by the task gate,
candidate retrieval, or threshold. Thus `result_limit_omissions` and
`budget_omissions` cannot explain why a known subject memory disappeared.

## CLI health and workflow completeness

The underlying CLI is healthy in this snapshot:

| Check | Result |
| --- | --- |
| Installed version | `engram 2.2.1` |
| `engram doctor --project engram --json` | `ok`: 4/4 checks, no warnings, blocks, or errors; WAL enabled; no observed lock contention |
| Database snapshot | 10 sessions, 155 observations, **0 prompts** |
| User skill drift | None: repo and user-installed `SKILL.md` have the same SHA-256 |
| Brief latency | Approximately 0.29 seconds in five local runs |
| Structured automation surface | JSON is available for project detection, brief, search, save, and diagnostics |

The completeness gap is above the database layer. The installed CLI supports a
generic `save`, but exposes no `session-summary` command. Meanwhile, the older
project memory protocol requires the MCP-specific `mem_session_summary`
operation. With the Engram MCP and plugin absent, a skill+CLI session can save a
normal `summary`, but it cannot execute that lifecycle-specific contract with
semantic parity. The zero prompt count is consistent with the absence of active
Codex prompt/session hooks; it does not indicate a storage failure.

In this implementation session, Engram was additive rather than the sole source
of context. The exact issue brief recovered useful design constraints, but the
conversation, repository files, issue specification, and separate developer
memory also supplied substantial context. The merge brief itself was mostly
noise until a targeted search was used.

## Documented facts versus inference

### Documented or directly observed

- Codex loads repository `AGENTS.md` based on repository root and working
  directory.
- Codex activates skills explicitly or by description match and natively scans
  repo skills under `.agents/skills`.
- Engram's `AGENTS.md` orders Codex to load its indexed project skills.
- The user-level `engram-memory-cli` skill is available outside Engram.
- The current Engram plugin and MCP server are not active in Codex.
- `engram current-project` resolves a different project from each tested working
  directory.
- Task Brief uses task text plus bounded Git/worktree evidence; a changed
  worktree can change selection.
- Exact domain wording retrieved the relevant setup memories, while the generic
  merge wording did not.
- The current database and CLI passed operational diagnostics, but prompt
  lifecycle capture is absent in the active Codex profile.

### Inference

- The higher activation rate inside Engram is primarily caused by the root
  `AGENTS.md` selecting two overlapping memory workflows, while the user-level
  skill alone is selective elsewhere.
- Running inside Engram changes both recall scope and the repository evidence
  available to Task Brief. That can improve specificity when branch and path
  terms are distinctive, or amplify old generic workflow matches when they are
  not.

## Uncertainties and boundaries

- This is a point-in-time local installation snapshot. Installing and enabling
  `engram@engram`, then restarting Codex, would create a materially different
  activation surface and should be evaluated separately.
- No controlled cross-repository transcript set was available here. The causal
  explanation is strong from configuration, but activation frequency should be
  measured with matched prompts from Engram and a neutral repository if a rate
  is needed.
- The accepted checkpoint architecture already resolves the intended end state:
  one canonical detailed skill, one short cue, equivalent CLI/MCP adapters, and
  capability-specific verification. Issues #46 and #48 own delivery and legacy
  protocol retirement; this report does not reopen that design decision.

## Issue tracking and recommended follow-up

The following mapping records issue coverage as of 2026-08-27. “Mapped” means
the tracker contains an explicit contract; it does not mean every open issue is
implemented. Issues #57–#61 were created from this evaluation and remain
`status:needs-review` at this snapshot.

| Evaluation outcome | Prior coverage | Tracking disposition |
| --- | --- | --- |
| Canonicalize the detailed skill and retire repeated protocol prose | [#41](https://github.com/yersonargotev/engram/issues/41) defines the architecture; [#46](https://github.com/yersonargotev/engram/issues/46) owns the canonical skill/cue; [#48](https://github.com/yersonargotev/engram/issues/48) owns duplicate-free retirement | Fully mapped; no new issue |
| Expose equivalent lifecycle semantics through CLI and MCP | #41 selects the checkpoint contract; [#42](https://github.com/yersonargotev/engram/issues/42) delivered `skipped`; [#44](https://github.com/yersonargotev/engram/issues/44) and [#45](https://github.com/yersonargotev/engram/issues/45) own `saved` and `needs_review`; [#46](https://github.com/yersonargotev/engram/issues/46), [#47](https://github.com/yersonargotev/engram/issues/47), and [#48](https://github.com/yersonargotev/engram/issues/48) own activation, enforcement, and migration | Fully mapped; complete the existing issue chain rather than add CLI `session-summary` as a competing contract |
| Make the active Codex integration mode observable without mutation | [#43](https://github.com/yersonargotev/engram/issues/43) delivered separate checks while setup runs, but not a read-only post-install mode inventory | [#57](https://github.com/yersonargotev/engram/issues/57) |
| Treat project-detection source strength as write authority | Earlier CLI and Task Brief issues validate inputs and exact selection, but do not distinguish strong `config`/`git_remote` identity from weak basename fallbacks before mutation | [#58](https://github.com/yersonargotev/engram/issues/58) |
| Improve Task Brief discrimination for exact identifiers and generic workflow prose | [#14](https://github.com/yersonargotev/engram/issues/14)–[#18](https://github.com/yersonargotev/engram/issues/18) delivered the precision-first v1 contract, but their corpus does not cover the PR #56 / issue #43 regression | [#59](https://github.com/yersonargotev/engram/issues/59) |
| Explain gate/threshold rejection and recommend targeted search | #14/#18 expose truncation and late omissions; [#28](https://github.com/yersonargotev/engram/issues/28) documents a manual search fallback, but early candidate rejection remains invisible | [#60](https://github.com/yersonargotev/engram/issues/60) |
| Quantify whether repository guidance increases skill/CLI activation | No existing issue measures matched prompts across normal Engram guidance, a memory-guidance ablation, and a neutral repository; Task Brief and Admission evaluations measure different stages | [#61](https://github.com/yersonargotev/engram/issues/61) |

Issue [#62](https://github.com/yersonargotev/engram/issues/62) tracks
publication of this research baseline. It does not satisfy #61: the current
configuration proves a mechanism and the briefing runs characterize retrieval,
but only the frozen matched-prompt cohort can estimate activation frequency.

The corresponding implementation priorities are:

1. **Make active mode observable (#57).** `doctor` or a setup-status command should
   distinguish manual skill+CLI mode from installed plugin, MCP, prompt hooks,
   activation cue, and lifecycle verifier.
2. **Treat project-source strength explicitly (#58).** The skill should accept
   `config` and `git_remote` as strong project identity, while treating
   `dir_basename` as weak evidence before any write.
3. **Improve Task Brief discrimination (#59).** Weight rare/exact identifiers such as
   issue number, PR number, branch, and topic key; let match strength break
   task-signal ties; prevent generic words from dominating; and bound long
   memory bodies without discarding whole otherwise relevant memories.
4. **Expose rejection telemetry or fallback (#60).** Report gate/threshold/candidate
   rejection counts, or automatically suggest a targeted search when truncation
   and budget diagnostics indicate a lossy brief.
5. **Complete the canonical lifecycle issue chain (#41–#48).** Finish the
   equivalent CLI/MCP checkpoint dispositions, canonical cue, bounded verifier,
   and ownership-aware retirement. Do not introduce generic `save` or session
   summary as a second terminal contract.
6. **Measure activation frequency separately (#61).** Run a matched prompt cohort in
   Engram and a neutral repository. Configuration proves why activation is more
   likely here; only a controlled transcript set can quantify how much more.

## Reproduction commands and snapshot limit

The selected memory IDs and omission counts above are historical observations,
not a frozen fixture. Engram's local database and the repository branch, diff,
paths, commit subjects, and untracked files have changed since the measured
merge turn, and no byte-identical database/worktree pair was retained. The
commands below re-run the same query shapes and reproduce the cwd-sensitive
repository-evidence boundary; they can legitimately return different memories
and counts against current state.

```bash
# Run this block from the Engram checkout.
ENGRAM_RESEARCH_REPO="$(git rev-parse --show-toplevel)"

codex plugin list
codex mcp list
readlink "$HOME/.agents/skills/engram-memory-cli"
engram current-project --json
engram doctor --project engram --json

# Exact issue-specific briefing, isolated from repository evidence.
(
  cd /tmp
  engram context engram --brief \
    --task 'make Codex setup reproducible and ownership-aware for issue 43' \
    --scope project --limit 5 --json
)

# Generic merge briefing with Engram branch/worktree evidence.
(
  cd "$ENGRAM_RESEARCH_REPO"
  engram context engram --brief \
    --task 'merge PR 56 for issue 43 and clean only its branch after protected validation' \
    --scope project --limit 5 --json
)

# Task-and-memory-only control for the same merge intent.
(
  cd /tmp
  engram context engram --brief \
    --task 'merge PR 56 for issue 43 and clean only its branch after protected validation' \
    --scope project --limit 5 --json
)

# Distinctive-anchor fallback; explicit project makes this cwd-independent.
engram search 'Codex setup reproducibility' \
  --project engram --match-mode any --limit 5 --json
```

The two merge briefing commands intentionally use different working directories;
Task Brief includes repository evidence only in the Engram checkout. Exact
reproduction of the historical `#84`, nine result-limit omissions, and four
budget omissions would require both the unretained dated database and the exact
historical Git/worktree state. Treat those values as point-in-time evidence, not
as current expected output. Do not infer plugin activation merely from a
configured marketplace; verify plugin status and MCP server independently.
