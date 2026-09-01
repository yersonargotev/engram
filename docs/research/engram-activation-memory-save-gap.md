# Engram activation and the memory-save gap

Date: 2026-08-26

## Question

Why can Engram look installed and active while producing few or no durable Memories? What does `engram setup` actually activate, and are instructions + MCP + hooks excessive, insufficient, or aimed at the wrong boundary?

This note uses the current repository, tests and accepted ADRs as the primary authority, plus the real-session evidence already recorded in [issue #34](https://github.com/yersonargotev/engram/issues/34). It separates documented behavior from product inferences.

## Executive conclusion

Engram does not have one activation switch. It has a pipeline with independently activated stages:

```text
host integration
  -> lifecycle/evidence capture
  -> Memory proposal generation
  -> Admission assessment
  -> Promotion into a durable Memory
  -> Task briefing/retrieval in later work
```

`engram setup` mainly activates **host integration and evidence plumbing**. Depending on the host, it registers the MCP server, installs or writes persistent instructions, and installs lifecycle hooks/plugins. For Codex specifically, it writes `~/.codex/config.toml`, dedicated model and compaction instruction files, and best-effort installs the Codex plugin; it does **not** currently inject the protocol into `AGENTS.md` ([`internal/setup/agents.go:51-60`](../../internal/setup/agents.go), [`internal/setup/setup.go:1157-1209`](../../internal/setup/setup.go), [`internal/setup/setup.go:1231-1267`](../../internal/setup/setup.go)).

The live Codex home does not have those outputs: Codex 0.150.1 does not list Engram in `codex plugin list`; `~/.codex/config.toml` has no `[mcp_servers.engram]`, `model_instructions_file`, or `experimental_compact_prompt_file`; and neither `~/.codex/engram-instructions.md` nor `~/.codex/engram-compact-prompt.md` exists. This proves that the Engram integration is absent on the Codex surface being experienced.

One Engram capability is nevertheless discoverable there: the user-level `engram-memory-cli` skill is a symlink into the installed Packy bundle. That Pack exports a Codex skill plus an external `engram` executable requirement; it declares no MCP registration or host-setup resource ([`pack.json`](../../pack.json), [`skills/engram-memory-cli/SKILL.md`](../../skills/engram-memory-cli/SKILL.md)). The skill is intentionally best-effort: it activates through normal skill matching, preserves only reusable knowledge, excludes facts already maintained in project documentation, and explicitly permits routine or low-value work to finish without a write. It is therefore a selective CLI workflow, not a replacement for `engram setup codex` lifecycle integration and not an always-running save loop.

An isolated-home execution of `engram setup codex` with the same Codex 0.150.1 did create the config and both instruction files and installed/enabled `engram@engram` 0.1.3. The current installer therefore executes its existing mechanics in that isolated reproduction; this does not validate its now-rejected full-protocol and replacement-instruction design. The live absence is evidence that setup was not executed or its outputs were not persisted in the active Codex home, not evidence that those mechanics failed. It also explains why an installed Engram Pack can appear available while MCP tools and hooks remain absent: those are separate installation products with different contracts.

The isolated config exposed a separate upgrade-safety risk: it wrote the MCP command as the versioned Cask path `/opt/homebrew/Caskroom/engram/2.2.1/engram`. Existing path stabilization recognizes Homebrew `/Cellar/engram/`, not `/Caskroom/engram/`; after a Cask upgrade, the configured executable can disappear even though setup originally succeeded. This risk does not explain the current live absence, because the entire Engram block and instruction configuration are missing there.

That setup is simultaneously:

- **potentially too much in the intended prompt/integration design**: the proactive-save policy is duplicated across persistent instructions, a skill and hook payloads, although the current machine is not receiving the installed Codex integration and two hook payloads are not model-visible under Codex's documented contract;
- **too little at the workflow layer**: nothing connects ordinary lifecycle capture to `admission shadow`, nothing promotes an accepted proposal, and Task briefing is CLI-only rather than an agent-time MCP/lifecycle operation;
- therefore **misoriented as a completeness story**: once correctly installed, setup may increase the probability that an agent voluntarily calls `mem_save`, but it cannot guarantee proposal coverage, Admission, Promotion, or useful later retrieval.

The reported symptom—“everything is considered unfit to save”—is not best explained by a universal quality rejection gate. The current explicit `mem_save` path is authoritative and is not intercepted by Admission preview/shadow. Passive capture has a narrow structural parser, while admission generation has a separate and also narrow deterministic grammar. Both can yield zero output silently or with `no_memory_proposals`, but those are omission/grammar outcomes, not a general judgment that the content is low quality.

## What each stage really does

| Stage | Current trigger | Current output | Important limitation |
| --- | --- | --- | --- |
| Host activation | `engram setup <agent>` and host restart | MCP config, instructions, and sometimes plugin/hooks | Installation can succeed even if plugin installation fails non-fatally; it is not proof that every runtime surface loaded ([`internal/setup/setup.go:1174-1204`](../../internal/setup/setup.go)). Isolated setup succeeded, while the live Codex surface has none of its expected outputs. |
| Evidence/lifecycle capture | Session and prompt hooks; explicit summary/save calls | Sessions, prompts, summaries, explicit Memories | Captured prompts are evidence, not Memories. Codex `UserPromptSubmit` persists prompts in a detached best-effort request ([snapshot `plugin/codex/scripts/user-prompt-submit.sh:32-48`](https://github.com/yersonargotev/engram/blob/a85c054ef062f965a25e0945361cc34bd380391d/plugin/codex/scripts/user-prompt-submit.sh#L32-L48)). |
| Explicit Memory creation | Agent calls `mem_save` or `mem_session_summary` | Durable Memory immediately | Quality depends on agent compliance with the protocol. Admission preview/shadow deliberately does not intercept this path (ADR-0004, lines 7-14, 31-36). |
| Passive capture | Explicit `mem_capture_passive`, or Codex `SubagentStop` posting output | One passive Memory per extracted item | It only recognizes a last valid `Key Learnings`/`Aprendizajes` section, numbered or bulleted items, at least 20 characters and four words ([`internal/store/store.go:6863-6923`](../../internal/store/store.go)). Ordinary prose, decisions embedded elsewhere, and main-agent output without an applicable hook produce nothing. |
| Proposal generation | Explicit `engram admission preview/shadow` | Memory proposals | The v1 grammar only recognizes explicit `Remember this`/`Recuerda esto` requests and supported structured sections. Repository signals and tool output cannot formulate proposals (ADR-0004, lines 16-22; [`README.md:512-520`](../../README.md)). |
| Admission assessment | Same explicit preview/shadow invocation | Advisory `admit`, `review`, or `reject` | It is advice, not persistence. Protected proposals cannot be rejected; ambiguous proposals remain review. |
| Shadow retention/review | Explicit `engram admission shadow`, then review commands | Redacted snapshots, corrections, metrics | No lifecycle hook invokes shadow. Shadow rows are excluded from Memory search/context and Promotion (ADR-0005, lines 7-19). |
| Promotion | No implemented proposal-to-Memory workflow found | N/A | ADR-0005 explicitly keeps Promotion/rejection and changes to `save`/`mem_save` out of scope (lines 45-53). A human verdict of `admit` does not create a Memory. |
| Task briefing | Explicit `engram context <project> --brief --task ...` | Selected existing durable Memories | V1 is CLI-only and precision-first; task-aware MCP is a required follow-up (ADR-0003, lines 7-22). It cannot retrieve a proposal or an unsaved fact. |

## Why saving feels inactive

### 1. The protocol is exhortative, not transactional

The intended installed protocol tells the model to call `mem_save` immediately after decisions, fixes, discoveries, configuration and preferences, and to write a session summary before finishing ([`internal/setup/setup.go:135-199`](../../internal/setup/setup.go)). The Codex plugin also declares an `engram-memory` skill whose description says `ALWAYS ACTIVE`. However, [official Codex skill documentation](https://developers.openai.com/codex/skills) says skills are activated explicitly or implicitly when the request matches the skill `description`; prose inside a skill is available only after activation. `ALWAYS ACTIVE` inside the loaded file therefore does not itself guarantee permanent activation.

The `UserPromptSubmit` hook writes both its first-message ToolSearch request and its later 15-minute reminder as `systemMessage` ([snapshot lines 19-20](https://github.com/yersonargotev/engram/blob/a85c054ef062f965a25e0945361cc34bd380391d/plugin/codex/scripts/user-prompt-submit.sh#L19-L20), [snapshot lines 187-207](https://github.com/yersonargotev/engram/blob/a85c054ef062f965a25e0945361cc34bd380391d/plugin/codex/scripts/user-prompt-submit.sh#L187-L207)). According to the [official Codex hooks contract](https://developers.openai.com/codex/hooks), `systemMessage` is rendered as a UI/event-stream warning. Model-visible context instead requires `hookSpecificOutput.additionalContext`, or plain stdout for compatible hook events. Consequently these two current payloads must not be described as forcing tool loading, calling `mem_context`, or reminding the model; they notify the user interface unless Codex supplies some undocumented behavior.

Documented fact: the MCP configuration can expose tools, persistent model instructions can direct the model, and correctly shaped hook additional context can augment model context. The two current `systemMessage` payloads do not use that model-context channel.

Inference: they cannot observe whether a specific decision was made and atomically require its preservation before task completion. A generic 15-minute nudge also treats “some recent observation exists” as a proxy for “this task's durable findings were saved.” More prompt text can raise salience, but it does not close that semantic gap.

### 2. Passive capture is much narrower than “capture the session”

Only the Codex `SubagentStop` hook posts assistant output to passive capture ([`plugin/codex/hooks/hooks.json:38-48`](../../plugin/codex/hooks/hooks.json), [snapshot `plugin/codex/scripts/subagent-stop.sh:18-39`](https://github.com/yersonargotev/engram/blob/a85c054ef062f965a25e0945361cc34bd380391d/plugin/codex/scripts/subagent-stop.sh#L18-L39)). The extractor ignores prose outside a recognized learning section and drops short items. This explains why valid work can yield no passive Memories without any explicit rejection.

The upstream PR discussion that introduced the four-word passive threshold deliberately removed a server-side `mem_save` quality gate: quality of explicit AI saves belongs in protocol behavior, while passive parsing benefits from a noise floor ([upstream PR #34 discussion](https://github.com/Gentleman-Programming/engram/pull/34#issuecomment-4022495544)). The current code matches that disposition: `minLearningWords` exists only in `ExtractLearnings`, while `mem_save` remains separate.

### 3. Admission is an opt-in experiment, not the automatic save path

ADR-0004 makes preview non-persisting and excludes automatic lifecycle execution, plugins, Promotion, and changes to `mem_save`. ADR-0005 adds retained shadow runs but says the command invocation itself is the opt-in boundary and no lifecycle hook invokes it. The code calls proposal generation only inside explicit preview/shadow orchestration and emits `no_memory_proposals` when the deterministic grammar finds nothing ([`internal/memoryops/admission.go:181-219`](../../internal/memoryops/admission.go)); shadow then stores assessed snapshots, not Memories ([`internal/memoryops/admission_shadow.go:89-137`](../../internal/memoryops/admission_shadow.go)).

Therefore, a session can have all hooks active, persist every prompt, end cleanly, and still never run proposal generation. Even when shadow is run and a reviewer marks a proposal `admit`, current review semantics append a correction and “never promote” the proposal ([`internal/memoryops/admission_shadow.go:171-173`](../../internal/memoryops/admission_shadow.go)).

### 4. Retrieval can fail independently of saving

Issue #34 already contains two distinct real-session cases:

- Useful Packy Memories existed, but a natural-language Task intent raised the half-term match threshold and filtered them. A compact domain-anchored query retrieved them ([real-session retrieval finding](https://github.com/yersonargotev/engram/issues/34#issuecomment-5390295271)).
- For a later vulnerability task, the empty briefing looked like correct abstention because no topical Memory existed; a loose search found an irrelevant `go-git` Memory that should not have been injected ([Packy vulnerability follow-up](https://github.com/yersonargotev/engram/issues/34#issuecomment-5433516510)).

This means “Engram returned nothing” has at least four materially different causes: capture never ran, no proposal matched, no Memory was promoted/saved, or retrieval filtered/abstained. The current user experience does not expose that stage attribution end to end.

## Is setup too much or too little?

### Too much

The intended Codex integration has overlapping surfaces: model instructions, compaction instructions, a plugin skill, session-start/post-compaction injection, first-message ToolSearch text, and a save nudge. They repeat rules but do not share a single observable completion state. In practice, the live Codex home lacks the integration even though isolated setup succeeds; additionally, the ToolSearch and nudge use the wrong documented hook output channel for model context. The `--protocol=slim|full` feature reduces session-start prose for eligible integrations, but it is a verbosity control, not a change in memory semantics ([`internal/setup/protocol.go:25-47`](../../internal/setup/protocol.go)).

### Too little

Setup does not currently establish:

- a reliable end-of-task checkpoint that asks what durable findings occurred and confirms their disposition;
- automatic or scheduled shadow generation from consented sessions;
- a manual Promotion operation from an admitted proposal to a durable Memory;
- task-aware briefing through the same MCP/lifecycle path used by agents;
- an end-to-end diagnostic that says exactly where the pipeline produced zero items.

### Wrong orientation

The strongest conclusion is not “add more instructions” or “remove hooks.” The integration bundle is useful infrastructure, but it is being asked to compensate for a missing domain workflow. The product needs an explicit **memory checkpoint** that connects evidence to proposals, Admission, human disposition and eventual Promotion, while preserving the ADR safety boundary against automatic rejection/promotion.

## Recommended next step

Do **not** design automatic admission yet. Issue #34 explicitly requires
real-session evidence before that step and keeps automatic admission disabled
([issue #34](https://github.com/yersonargotev/engram/issues/34)). Before
interpreting low save rates as an Admission-policy failure, establish a working
host integration and correct the hook contract:

1. Repair the Codex installer before using it on the active home: install and
   verify the plugin/MCP capabilities, emit only the canonical short cue from
   plugin `SessionStart`, and stop claiming `model_instructions_file` as the
   normal activation surface. Remove that key or its generated file only when
   setup can prove it owns the exact current value and content.
2. Correct the Codex hook contract: use
   `hookSpecificOutput.additionalContext` or documented compatible stdout for
   content that must reach the model; reserve `systemMessage` for UI warnings.
   Add contract tests that assert model-visible shape rather than merely valid
   JSON.
3. Stabilize the MCP command for Homebrew Cask so setup prefers a durable
   executable path instead of
   `/opt/homebrew/Caskroom/engram/<version>/engram`; verify the configured
   command survives a simulated version change.
4. Install the repaired setup into the active Codex home, restart Codex, and
   verify plugin identity/trust, MCP availability, cue delivery at startup and
   compaction, and one explicit `mem_save`. Do not rely on `ALWAYS ACTIVE` prose
   inside a progressively loaded skill.
5. Deliver the accepted checkpoint contract in vertical slices: core local
   ledger and reason codes, equivalent CLI/MCP adapters, canonical skill/cue
   generation, then one synchronous Codex `Stop` verifier keyed to the same root
   user turn.
6. Add stage diagnostics and a `session memory status`/equivalent report for one
   session: captured prompt/summary counts, checkpoint outcome, whether shadow
   ran, proposal counts and reasons, pending corrections, promoted/saved Memory
   count, and briefing selection/omission reason codes. Do not include raw
   evidence in shared artifacts.
7. Keep Admission shadow and manual Promotion explicit and local. Promotion
   should require confirmation, deduplication, provenance, idempotency, and
   auditability; a review verdict alone must remain non-promoting.
8. Exercise the repaired activation and checkpoint slices on real sessions and
   feed only consented aggregate evidence into #34. Only then decide whether
   automatic admission deserves a separate design issue.

The immediate work is a setup/hook correctness issue; the later checkpoint remains a scoped design/implementation issue derived from #34. Setup should install and verify capabilities, hooks should use Codex's actual model-context contract, and the checkpoint should own semantic completion. Q10-Q15 were later omitted after the maintainer clarified that Engram already has no runtime dependency on Gentle AI; the concrete setup findings remain maintenance inputs, not a separate independence initiative.

## Maintainer decisions

The maintainer accepted these design directions on 2026-08-26:

1. Engram will guarantee a **Memory checkpoint** outcome for completed work, not
   force one Memory per task. The outcome is `saved`, `skipped` with an explicit
   reason, or `needs_review`.
2. Issue #34 remains the real-session shadow-evaluation track. Activation
   correctness and the later Memory checkpoint are separate, focused work.
3. The current overlapping MCP + plugin + hooks bundle is not an acceptable
   canonical authority. It may be retained only if redesigned so one surface owns
   policy and every remaining integration component has a distinct, non-repeated
   responsibility.
4. The integration choice was reopened because the earlier `skill + CLI + one
   Stop hook` suggestion was only a preliminary hypothesis. The host comparison
   is now complete and ADR-0006 accepts a core checkpoint plus equivalent MCP/CLI
   adapters, one canonical skill/cue, capability-specific verifiers, and plugins
   as packaging only.
5. The current checkpoint-boundary recommendation is once per root user turn,
   rather than once per subagent, tool call, or raw hook event. This direction is
   accepted and was validated against the supported hosts' lifecycle models in
   `memory-checkpoint-integration-surfaces.md`.
6. The current persistence recommendation is a dedicated local checkpoint record:
   `saved` references Memory IDs, `skipped` stores a reason code, and
   `needs_review` references a proposal. Checkpoint records do not enter Memory
   search, context injection, sync, or cloud. This direction is accepted and was
   validated against the existing local-only admission-shadow boundary in
   `memory-checkpoint-integration-surfaces.md`.
7. Architectural recommendations must be preceded by explicit research and must
   identify the evidence, inference, recommendation, and remaining decision.
8. Questions Q10-Q15 are omitted; Engram will not open a separate Gentle AI
   independence initiative from the mistaken premise that setup invokes Gentle
   AI.
9. `yersonargotev/engram` is the sole authority for Engram source, packages,
   marketplace entries, releases, setup artifacts, and integration contracts.
10. For Codex and Claude Code, the short activation cue should use the plugin's
    model-visible `SessionStart` context. Shared `AGENTS.md` or `CLAUDE.md` files
    are fallback surfaces only when no plugin-native or dedicated additive host
    instruction mechanism can satisfy persistent loading.

These decisions are recorded in the domain glossary under **Memory checkpoint**.
The cross-host integration boundary was compared in
`memory-checkpoint-integration-surfaces.md` and accepted in ADR-0006. The setup
authority and persistent-cue precedence were then verified in
`setup-independence.md` and incorporated into that ADR. Remaining migration and
failure-policy decisions must be researched before an implementation brief
freezes the delivery slices.

## Options for the maintainer

| Option | Value | Risk | Recommendation |
| --- | --- | --- | --- |
| Install and verify the active Codex home; repair hook output | Establishes that Engram is genuinely active and ensures intended instructions reach the model | Does not by itself close proposal/Promotion gaps | Yes, immediate prerequisite |
| Stabilize the Homebrew Cask MCP command | Prevents a working setup from breaking after upgrade | Separate from current missing live config | Yes, focused reliability fix |
| Add more mandatory prompt prose | Fast, may improve voluntary `mem_save` frequency | More repeated instructions without stage evidence; still model-dependent | No |
| Auto-run shadow on every session end | Produces evaluation data quickly | Contradicts the explicit opt-in boundary in ADR-0005 and expands privacy/retention scope | No, unless a new ADR reopens the boundary after consent design |
| Add diagnostics only | Makes failures attributable | Does not produce more Memories by itself | Yes, first increment |
| Add opt-in checkpoint + manual Promotion | Closes the missing semantic path while preserving human authority | Requires a careful Promotion contract | Yes, after/alongside diagnostics |
| Proceed to automatic admission | Could reduce model dependence | Issue #34's real-session gates are not yet satisfied | No |

## Uncertainties requiring maintainer decisions

- Is manual Promotion in scope for issue #34's final disposition, or should #34 only gather evidence and open a separate approved Promotion issue?
- What exact migration retires Codex's current full `model_instructions_file`
  injection without deleting or overwriting a user-owned pre-existing value?
- Which failure reasons may a host verifier record when the core, MCP, or CLI
  adapter is unavailable? That policy needs explicit research rather than
  treating an integration failure as `skipped`.

## Evidence summary

- Accepted domain boundaries: [`CONTEXT.md`](../../CONTEXT.md), [ADR-0003](../adr/0003-task-briefing-from-transient-signals.md), [ADR-0004](../adr/0004-calibrate-memory-admission-offline.md), [ADR-0005](../adr/0005-explicit-local-shadow-admission.md).
- Setup and protocol: [`internal/setup/agents.go`](../../internal/setup/agents.go), [`internal/setup/setup.go`](../../internal/setup/setup.go), [`internal/setup/protocol.go`](../../internal/setup/protocol.go).
- Codex lifecycle: [`plugin/codex/hooks/hooks.json`](../../plugin/codex/hooks/hooks.json), [snapshot `plugin/codex/scripts/user-prompt-submit.sh`](https://github.com/yersonargotev/engram/blob/a85c054ef062f965a25e0945361cc34bd380391d/plugin/codex/scripts/user-prompt-submit.sh), [snapshot `plugin/codex/scripts/subagent-stop.sh`](https://github.com/yersonargotev/engram/blob/a85c054ef062f965a25e0945361cc34bd380391d/plugin/codex/scripts/subagent-stop.sh), [`plugin/codex/skills/memory/SKILL.md`](../../plugin/codex/skills/memory/SKILL.md).
- Passive capture and admission code: [`internal/store/store.go`](../../internal/store/store.go), [`internal/memoryops/admission.go`](../../internal/memoryops/admission.go), [`internal/memoryops/admission_shadow.go`](../../internal/memoryops/admission_shadow.go).
- Real-session evidence and declared gates: [issue #34](https://github.com/yersonargotev/engram/issues/34).
- Host activation semantics: [official Codex skills documentation](https://developers.openai.com/codex/skills) and [official Codex hooks documentation](https://developers.openai.com/codex/hooks).
- Current-machine runtime evidence: Codex 0.150.1 live plugin/config/instruction-file inspection and an isolated-home `engram setup codex` reproduction supplied and verified by the coordinating investigation on 2026-08-26. The live integration is absent; isolated setup created the expected outputs and installed/enabled `engram@engram` 0.1.3. The isolated MCP config also exposed the versioned Homebrew Cask command path.
