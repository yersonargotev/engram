---
status: accepted
---

# Own Memory checkpoints in the core and verify them by host capability

Engram's Go core owns the Memory checkpoint state machine, idempotency, and
local audit boundary. MCP and CLI expose the same operation as thin adapters; a
short always-on activation cue announces the invariant; one canonical skill
owns the detailed save, skip, and review rubric; and a host adapter only carries
root-user-turn identity and verifies that a terminal checkpoint exists. Plugins
package those host assets but own no Memory semantics.

`yersonargotev/engram` is the sole distribution authority for Engram source,
packages, marketplace entries, manifests, skills, MCP declarations, hooks, and
generated instruction assets. Host registries and caches are delivery
mechanisms, while other repositories may be upstream or reference inputs; none
of them becomes an authority for the installed Engram contract.

Stable setup installs only release-identified artifacts from that authority and
pins Git-backed inputs to a release tag plus exact commit identity. A moving
branch such as `main` is allowed only in an explicit development workflow, never
as the stable setup default.

The short activation cue uses plugin-native lifecycle context when the host
supports it. Codex and Claude Code both allow a plugin-bundled `SessionStart`
hook to add model-visible context, so their setup must prefer one minimal cue
there over editing shared `AGENTS.md` or `CLAUDE.md`. A dedicated additive host
instruction file is the next fallback. Setup may add a marker-delimited block to
a shared instruction file only when neither plugin-native context nor a
dedicated additive instruction surface can provide persistent loading; it must
never copy the full protocol there. Codex `.rules` files are execution policy,
not behavioral instructions, and `model_instructions_file` has replacement
semantics, so neither is the default activation seam.

Migration is ownership-aware. Setup may remove or replace a legacy instruction
key or generated file only when its current value, path, and known content prove
that Engram created it. Unknown, customized, or user-owned values are preserved
and reported; successful installation of the replacement capability is verified
before any owned legacy activation path is retired.

One checkpoint is finalized per root user turn after all causally related work
settles. Its local-only record is excluded from Memory search, context, sync,
export, and cloud materialization; `saved` references Memories, `needs_review`
references proposals, and `skipped` stores a reason code without transcript or
free-form rationale.

The Checkpoint guarantee is capability-rated rather than universal. Codex and
Claude Code may verify with `Stop`, Gemini CLI with `AfterAgent`, and Pi with
`agent_settled`; OpenCode remains best-effort until a stable continuation gate
is documented and verified. Codex is the first end-to-end proof because the
reported activation failure occurs there and its turn-final contract exposes a
stable turn identifier and controlled continuation.

This rejects MCP-only enforcement, prompt-only guidance, a universal `Stop`
abstraction, plugin-owned policy, moving distribution channels, and repeated
full-protocol injection. The evidence and option comparison are in
[`memory-checkpoint-integration-surfaces.md`](../research/memory-checkpoint-integration-surfaces.md)
and [`setup-independence.md`](../research/setup-independence.md).
