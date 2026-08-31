---
status: accepted
---

# Optimize Recall for bounded utility

Engram will optimize useful Recall subject to explicit noise, latency, volume,
privacy, and checkpoint constraints, with a bias toward precision when coverage
and noise conflict. Historical content is not injected merely because a session
starts, and raw prompts or subagent results require explicit Content capture
consent rather than becoming default Memory inputs.

The default Recall strategy is agent-initiated. It exposes a bounded candidate set
before any deliberate full-content follow-up, keeps Memories advisory to current
user intent and maintained or runtime evidence, surfaces contradictions, and fails
open visibly when Recall itself is unavailable.

Core owns a monotonic Protocol contract of machine-verifiable invariants. One
canonical skill owns the editorial rubric; MCP and CLI remain thin adapters over
Core, host hooks translate lifecycle events without owning Memory policy, and the
default MCP profile exposes only the five semantic tools needed for project
identity, bounded Recall, and terminal checkpointing.

Settled durable knowledge normally enters through one Terminal Memory commit after
the root user turn settles. Its preflight is non-persisting, independent saves are
reserved for explicit curation or material loss-risk handoffs, and mixed terminal
outcomes follow
[ADR-0011](0011-preserve-mixed-terminal-memory-outcomes.md). A missing checkpoint
may cause one identity-preserving continuation; if persistence still fails, the
host surfaces the incomplete result and stops retrying.

That bounded continuation revises
[ADR-0006](0006-own-memory-checkpoints-in-core.md) only where its Checkpoint
guarantee implied an absolute completion barrier. Core ownership, opaque identity,
exact replay, local-only checkpoint state, canonical semantics, and thin adapters
remain accepted.

Capture consent stays separate by project and content type. Diagnostic capture,
Recall feedback, and legacy prompt state remain outside Memory and ordinary Recall;
no proposal, prompt, subagent output, or feedback becomes durable knowledge through
automatic Admission or Promotion.

Default behavior may change only through a prospective controlled comparison with
preregistered utility, noise, harm, latency, volume, and checkpoint gates. Releases
use expand-contract ordering, verify one Compatibility tuple before retiring owned
legacy paths, and roll back to a previously verified tuple as a unit.

The detailed limits, thresholds, migration behavior, and delivery ownership remain
in [specification #98](https://github.com/yersonargotev/engram/issues/98) and its
[delivery graph](../research/codex-mcp-central-thin-hooks-architecture.md#recommended-delivery-flow).
The [architecture research](../research/codex-mcp-central-thin-hooks-architecture.md)
preserves the evidence and alternatives. This ADR does not authorize runtime
implementation by itself.
