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
contract is validated. Identical normalized Repository payloads form one ranking
group using the strongest contributing weight while preserving source-level
evidence. Repository acquisition retains only bounded vocabulary, counts later
eligible occurrences without retaining their values, and stops Git streams at a
deterministic one-MiB ceiling with explicitly incomplete prefix counts. The public output
budget applies to the exact human or compact JSON byte stream written to stdout.
A configured upstream qualifies as a comparison base only when it represents a
lineage distinct from the current branch; same-branch tracking continues to the
remote default because it cannot identify feature-branch changes.
