---
status: accepted
---

# Generate task briefings from transient work signals

Engram will add an opt-in Task briefing mode that selects durable memories for
the current task using explicit Task intent and transient Repository signals.
Selection remains local, deterministic, explainable, and precision-first;
signals guide retrieval but are never persisted as memories or emitted as raw
briefing content. The behavior belongs behind a small `internal/taskbriefing`
module interface so the CLI stays thin and a later MCP consumer can reuse the
same contract. This deliberately preserves chronological `engram context`,
rejects hidden LLM ranking for v1, and prevents repository signals from crossing
project boundaries.

## Consequences

Ranking weights, relevance thresholds, and input budgets must be validated
against a versioned scenario corpus before implementation. V1 exposes the CLI
mode only; a task-aware MCP operation remains a required follow-up after the CLI
contract is validated.
