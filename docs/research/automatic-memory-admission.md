# Automatic Memory proposal generation and admission

Date: 2026-08-18

> The accepted implementation boundary resulting from this research is recorded
> in [ADR-0004](../adr/0004-calibrate-memory-admission-offline.md).

## Decision

**Run a bounded experiment now, but do not ship an automatic reject gate.**

Engram should test two separate capabilities:

1. **Memory proposal generation** from bounded session evidence, to find durable facts that
   the agent did not explicitly save.
2. **Admission advice** that labels a Memory proposal `admit`, `review`, or `reject`
   and records reasons, to reduce noise before a proposal becomes a Memory.

The first production increment should be deterministic, explicitly invoked, and
shadow-only. It should report Memory proposals and Admission assessments through the
real local binary without suppressing or changing the existing `mem_save` path. A
later opt-in collection phase may retain assessments and human/agent corrections
after its retention semantics are designed. An optional LLM may later classify only
ambiguous proposals, using the proposal and minimal provenance rather than a complete
transcript. Explicit user requests to remember something and existing explicit
`mem_save` calls must bypass automatic rejection.

The accepted implementation is `engram admission preview`: production logic in
`internal/memoryops`, two versioned corpora, bounded caller-supplied or explicitly
selected session evidence, and read-only duplicate lookup against local Memories.
Session acquisition reports coverage, omission, and truncation. The command persists
neither proposals nor assessments. See
[ADR-0004](../adr/0004-calibrate-memory-admission-offline.md).

This is a **go for an experiment**, not a go for an autonomous memory gate. Engram
does not yet have an Engram-specific labeled corpus proving that such a gate improves
useful-memory precision without unacceptable recall loss.

## What Engram does today

The current product deliberately begins with an agent deciding that something is
worth remembering. Its documented flow then validates the tool request and persists
the observation in local SQLite; the persistence layer does not reject poorly
structured prose ([memory-core guide](../codebase/memory-core.md#save-and-retrieve-flow),
[memory invariants](../codebase/memory-core.md#memory-invariants)). This creates two
independent failure modes:

- **Omission:** no store-side component can recover a durable fact if the agent never
  calls a write tool.
- **Noise:** `memoryops.Service.Save` checks session and non-empty content, persists
  first, and only then searches for relation candidates; relation-candidate failure
  never rolls back the save
  ([`Service.Save`](../../internal/memoryops/memoryops.go#L67)). Existing relation-
  candidate detection is therefore conflict/relationship discovery, not admission.

Engram already has partial safety nets, but none closes both gaps:

- The installed protocol tells agents when to call `mem_save`, requires an end-of-
  session summary, and describes passive capture as a safety net
  ([protocol](../../internal/setup/setup.go#L135)). These are instructions, so their
  reliability still depends on agent behavior.
- `mem_save` can capture the current user prompt after a successful save, but prompt
  capture is downstream of an explicit save and is best-effort
  ([MCP save handler](../../internal/mcp/mcp.go#L1268)).
- `mem_capture_passive` requires text containing a `Key Learnings` section
  ([handler](../../internal/mcp/mcp.go#L1982)). `Store.PassiveCapture` extracts list
  items under recognized headings and deduplicates exact normalized content before
  writing `passive` observations; it does not infer omitted learnings from arbitrary
  session activity ([`ExtractLearnings`](../../internal/store/store.go#L6713),
  [`PassiveCapture`](../../internal/store/store.go#L6772)).
- Observation insertion already performs deterministic normalized-hash/topic-based
  duplicate handling, while relation discovery uses project/scope-bounded FTS5
  candidates ([`AddObservation`](../../internal/store/store.go#L2256),
  [`FindCandidates`](../../internal/store/relations.go#L342)). Those mechanisms are
  useful admission features, but they do not judge durability or importance.

The architecture has two strong constraints. SQLite is authoritative and adapters
must remain thin ([architecture guardrails](../../skills/architecture-guardrails/SKILL.md#core-guardrails)).
The accepted task-briefing ADR also establishes a nearby precedent: local,
deterministic, explainable, precision-first selection, with a versioned scenario
corpus required before implementation and no hidden LLM ranking in v1
([ADR-0003](../adr/0003-task-briefing-from-transient-signals.md)).

## Evidence from primary research and implementations

### Automatic construction can improve long-term memory, but it is a pipeline

LongMemEval separates long-term memory into information extraction, multi-session
reasoning, temporal reasoning, knowledge updates, and abstention. Its authors report
that evaluated long-context and commercial systems lose substantial accuracy across
sustained histories, and that session decomposition and fact-augmented indexing
improve results ([LongMemEval paper](https://arxiv.org/abs/2410.10813)). This supports
testing extraction instead of assuming a larger context or the current protocol will
prevent omissions.

Mem0 is a concrete first-party implementation of automatic construction. Its current
pipeline retrieves nearby memories, makes an LLM extraction call, parses structured
JSON, and then continues memory processing; an extraction call can fail or produce no
facts, and malformed output is handled explicitly
([Mem0 implementation](https://github.com/mem0ai/mem0/blob/main/mem0/memory/main.py),
[extraction prompt](https://github.com/mem0ai/mem0/blob/main/mem0/configs/prompts.py)).
The accompanying paper reports better LoCoMo results and much lower latency/token
cost than sending full history at answer time, but those measurements evaluate Mem0's
conversation domain and complete pipeline, not coding-agent memory admission
([Mem0 paper](https://arxiv.org/abs/2504.19413)). The transferable lesson is that
extraction and reconciliation are distinct stages; its reported numbers are not proof
that the same thresholds or model will work for Engram.

Generative Agents likewise stores observations, derives higher-level reflections,
and retrieves memories using recency, importance, and relevance. Its ablations show
that observation, reflection, and planning each contributed to its evaluated agent
behavior ([Generative Agents paper](https://arxiv.org/abs/2304.03442)). This supports
generating Memory proposals and keeping provenance, but its goal—believable simulated
behavior—is different from preserving exact engineering decisions.

### An LLM verdict is useful evidence, not a safe deletion oracle

LLM judges exhibit systematic position bias that varies by judge and task, even in a
large controlled comparison study
([Shi et al., 2024](https://arxiv.org/abs/2406.07791)). Engram's own semantic-conflict
feature already treats model execution as a bounded, optional layer above
deterministic FTS5 relation candidates, records model attribution and reasoning, and isolates
shell-out failures rather than replacing the local path
([semantic conflict design](../../openspec/changes/memory-conflict-semantic/design.md),
[`AgentRunner`](../../internal/llm/runner.go#L20),
[`BuildPrompt`](../../internal/llm/prompt.go#L55)). Admission should reuse that
operating pattern, not necessarily its pairwise-conflict prompt or package API.

The consequence is asymmetric: a noisy admitted memory remains visible and can be
reviewed, related, updated, or soft-deleted; an automatically rejected fact may be
unavailable at the later moment when its value becomes clear. Engram's existing
curation research follows the same conservative principle for lifecycle state: a
memory needing review is not invalid and should not be hidden automatically
([curation research](tui-curation-inbox.md#correct-domain-model)). Therefore false
rejection, not aggregate classifier accuracy, is the release-limiting error.

### Evaluation must measure the complete memory outcome

LoCoMo evaluates long-range QA, event summarization, and multimodal dialogue over
human-verified long conversations
([LoCoMo paper](https://aclanthology.org/2024.acl-long.747/)). It is useful as an
external regression benchmark but does not encode Engram concepts such as root
causes, repository invariants, configuration gotchas, or facts already recoverable
from Git. An Engram corpus is required.

NIST's AI RMF calls for validity/reliability, transparency, explainability, privacy
enhancement, and measurement under the conditions in which a system is intended to
operate ([AI RMF 1.0](https://doi.org/10.6028/NIST.AI.100-1),
[AI RMF Core](https://airc.nist.gov/airmf-resources/airmf/5-sec-core/)). For this
feature, that means versioned examples, recorded classifier provenance/reasons,
explicit limitations, and evaluation after model or prompt changes—not a confidence
number accepted at face value.

## Capability decisions

### 1. Memory proposal generation: experiment now

Memory proposal generation addresses omissions; admission alone cannot. The experiment
should consume a **bounded evidence bundle**, not silently record an entire session:

- available user prompts already captured for the session;
- the session summary, when present;
- bounded repository change signals (file paths and compact diff-derived facts, not
  raw unrestricted diffs);
- explicit Memory proposal submissions from agent adapters; and
- provenance for every source fragment.

The generator should output zero or more structured Memory proposals with `title`, `type`,
`content`, `scope`, source references, and a reason the fact appears durable. It must
not write directly to `observations`. Memory proposal generation should be independently
measurable for recall: *of the durable gold facts in a session, how many were
proposed?*

Do not require plugins to classify content. Different adapters have different
session-event access, and putting policy there would duplicate behavior and violate
the thin-plugin boundary. Adapters should only send normalized, bounded evidence or
Memory proposals to a Go-owned contract.

### 2. Admission: deterministic advice now; no automatic rejection

Apply deterministic rules first:

- always admit an explicit user “remember this” request and an explicit existing
  `mem_save` call;
- reject empty content and exact duplicates;
- route project ambiguity through the existing loud project-resolution behavior;
- mark ephemeral progress, raw tool output, secrets, and unsupported claims for
  review/rejection when a deterministic rule can identify them;
- detect likely overlap through existing normalized hashes, topic keys, and FTS5,
  but preserve relation judgment as a separate concern; and
- return `admit | review | reject`, stable reason codes, human-readable reasons, and
  evidence references.

Initially these verdicts are observational only. A `reject` verdict must not delete a
Memory proposal or block an explicit save. Shadow results create the error corpus needed to
decide whether any deterministic rule is safe to enforce.

### 3. Optional LLM: only for the ambiguous middle

After deterministic rules, an LLM may advise on `review` proposals only. Send the
Memory proposal, minimal provenance excerpts, rule results, and a compact list of nearby
Memories. Do not send the whole transcript by default. Bound proposals per session,
tokens, concurrency, and timeout; attribute the model/prompt version and preserve its
reasoning. On unavailable CLI, timeout, malformed output, or unknown verdict, retain
`review` and continue without blocking the save/session-close path.

This keeps the default local-first and zero-model path functional. It also limits
privacy exposure, latency, and cost: deterministic cases make no model call, and the
user's existing authenticated agent CLI can remain an explicit opt-in, matching the
semantic-conflict runner pattern.

## Architectural placement

The durable seam should be a transport-neutral service next to the existing memory
domain operations, conceptually:

```go
type AdmissionRecommendation string // admit | review | reject

type MemoryProposalGenerator interface {
    Generate(context.Context, EvidenceBundle) ([]MemoryProposal, error)
}

type AdmissionPolicy interface {
    Assess(context.Context, MemoryProposal) (AdmissionAssessment, error)
}
```

`internal/memoryops` (or a cohesive child package if the experiment proves large
enough) should orchestrate generation, deterministic policy, optional model advice,
and explicit promotion. `internal/store` should own SQLite persistence/query methods
only. `internal/llm` should own external model execution/parsing if the optional stage
is added. MCP/CLI/plugins should translate inputs and render outputs; cloud should
only replicate durable state after local promotion.

Do **not** insert generated Memory proposals into `observations` and later mark them
rejected: that pollutes search and sync before admission. During the first experiment,
write results to a versioned fixture/report outside the user's memory database. Add a
local proposal table only if an interactive review workflow is approved; it should
be separate from observations and excluded from sync by default until its semantics
are explicitly designed.

## Experiment and go/no-go criteria

### Phase 0 — corpus before runtime behavior

Build a versioned set of real, consented, redacted coding sessions. Label each Memory
proposal independently by at least two reviewers, adjudicating disagreements. Gold
labels must include:

- durable memory category: decision, root cause/bugfix, invariant/constraint,
  convention, configuration, non-recoverable discovery, or explicit preference;
- `admit`, `review`, or `reject`;
- source evidence and whether the fact is recoverable cheaply from maintained code,
  Git, or docs;
- duplicates/updates/conflicts; and
- privacy/secrets flags.

Include adversarially valuable rare facts and large amounts of mundane progress;
otherwise a classifier can appear accurate by rejecting almost everything.

### Phase 1 — offline deterministic baseline

Measure Memory-proposal generation recall separately from admission:

- durable-fact proposal recall by category;
- admitted-memory precision;
- durable-fact false-reject rate (overall and by category);
- duplicate admission rate;
- unsupported/hallucinated proposal rate;
- secret/privacy leakage rate;
- reason-code coverage and reviewer agreement; and
- wall time and incremental bytes/tokens per useful admitted memory.

### Phase 2 — local shadow mode

Run only with explicit opt-in. Existing saves remain authoritative. Show proposed
verdicts in a report or review queue, let users correct them, and log no raw evidence
beyond the configured retention boundary. Compare deterministic-only and
deterministic-plus-LLM arms on the same labeled proposals.

### Go/no-go gates

Set numerical thresholds from the corpus before implementation; do not tune them
after seeing shadow results. At minimum:

- **No-go for automatic promotion** if generated unsupported facts or
  privacy leakage are non-zero in release evaluation.
- **No-go for automatic rejection** if any protected category (explicit user memory,
  root cause, invariant, security/configuration constraint) has a false rejection, or
  if the upper confidence bound is above the predeclared tolerance.
- **No-go for the LLM stage** unless it materially reduces review volume or improves
  precision over deterministic rules without worsening protected-category recall,
  and stays inside declared p95 latency/token budgets.
- **Go for advisory production use** when reviewers can reproduce decisions from
  reason codes/evidence, failure degrades to `review`, and correction/audit paths are
  tested.
- **Go for automatic admit of a rule** only rule-by-rule after shadow evidence shows
  it is deterministic and safe (for example, exact explicit-user requests). Keep
  semantic/importance judgments advisory until substantially stronger evidence
  exists.

## Final recommendation

Implement the **experiment infrastructure and deterministic shadow classifier**, not
the proposed autonomous classifier as a product gate. Start with the corpus and
offline generator, then shadow admission, then an optional LLM arm. Only after those
stages should Engram decide whether any narrow automatic-admit rule is safe.

This sequence directly attacks both weaknesses—Memory proposal generation reduces omission
and admission reduces noise—while preserving the properties that distinguish Engram:
local SQLite remains authoritative, explicit agent/user saves remain reliable, model
use is optional and bounded, adapters stay thin, and every automated judgment is
inspectable and reversible.
