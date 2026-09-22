# Agent Memory in 2026 and the Future of Engram

**Research date:** 2026-09-21

**Scope:** agent memory, context engineering, lifecycle, evaluation, privacy, provenance, portability, and Engram's product direction
**Evidence policy:** primary sources only—official product documentation, specifications, first-party repositories, and original papers. Repository claims are grounded in maintained source and accepted ADRs. Facts and recommendations are separated explicitly.

## Executive conclusion

Engram is aligned with the industry's durable invariants but not yet with its emerging memory architecture.

Its strongest choices are unusually sound: local-first ownership; agent and model neutrality; explicit project scope; bounded, just-in-time Recall; progressive disclosure; contradiction handling; atomic terminal commits; user-visible curation; and an empirical refusal to ship automatic capture without evidence. These choices match the direction visible across Anthropic, OpenAI, Google, Microsoft, LangGraph, and Letta: context is scarce, memory is external and selectively retrieved, write and read paths need separate policy, memory must evolve, and usefulness—not bytes retained—is the objective.

The central risk is not that Engram lacks an embedding database or a knowledge graph. The risk is that its durable unit is still a mostly untyped `{title, content}` observation whose usefulness depends on an agent remembering to write it and later guessing the right lexical query. Engram currently has stronger *commit integrity* than *memory intelligence*. Its own frozen Recall study stopped before one valid observation because Codex did not initiate Recall, so there is not yet causal evidence that normal agents regularly recover and use what Engram stores ([published v1 result](../../evals/recall-study/v1/publication.json), [study design](../RECALL-STUDY.md)). That is the user's largest fear in concrete form: Engram can reliably preserve a note that no future agent ever finds, trusts, or applies.

The recommended future is therefore not “store more.” It is:

> **Make Engram the portable, evidence-aware memory control plane for project agents: it should preserve the smallest set of durable claims, decisions, lessons, and preferences that measurably improve future work, and deliver them through an open, inspectable lifecycle independent of any model vendor.**

This implies a shift from a note store with memory protocol discipline to a typed memory system with provenance, validity, consolidation, feedback, and task-level evaluation. It does **not** imply transcript hoarding, automatic ingestion by default, a proprietary agent runtime, or a speculative graph/embedding rewrite.

## 1. Terms that must not be collapsed

The industry uses “memory” imprecisely. Engram should publish and enforce the following vocabulary.

| Term | Definition | Lifetime | Typical examples | What it is not |
|---|---|---:|---|---|
| **Context** | The tokens and tool-visible material available to the model for one inference or run. | One model call/run | system instructions, recent messages, recalled facts, tool output | Durable storage |
| **State** | Mutable application or workflow data needed to continue execution. It can exist outside the model and may never enter its context. | Step, task, or session | plan status, approval state, sandbox files, counters | Necessarily learned knowledge |
| **History** | The chronological record of messages, events, and tool calls. | Session or longer | chat thread, event log, trace | A curated memory |
| **Memory** | A durable, selected representation derived from experience and intended to improve a later decision or action. | Across turns or sessions | preference, decision and rationale, resolved failure, reusable procedure | The entire history or arbitrary RAG corpus |
| **Knowledge base** | An externally authored or ingested corpus whose truth does not primarily derive from the agent's experience with one user/task. | Durable | code, docs, policies, tickets | Personalization or episodic experience |
| **User profile** | A scoped, mutable semantic model of a person or team's preferences, constraints, roles, and goals. | Cross-session | response style, accessibility constraint, stable role | A chronological diary |

This distinction is already present in first-party systems. OpenAI's Agents SDK calls stored conversation items “session memory,” but separately documents sandbox **agent memory** as distilled lessons from prior runs ([Sessions](https://openai.github.io/openai-agents-python/sessions/), [Agent memory](https://openai.github.io/openai-agents-python/sandbox/memory/)). Google's ADK separates sessions/events/state from Memory Bank ([session service API](https://google.github.io/adk-docs/api-reference/java/com/google/adk/sessions/BaseSessionService.html), [Memory Bank setup](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/memory-bank/set-up)). Microsoft similarly distinguishes an agent thread's history from memory providers that extract and re-inject relevant information ([Semantic Kernel agent memory](https://learn.microsoft.com/en-us/semantic-kernel/frameworks/agent/agent-memory)).

For Engram, the practical rule should be: **history is evidence; memory is a curated claim derived from evidence; context is the delivery channel; state is what lets work resume.**

## 2. Current landscape: facts from primary sources

### 2.1 OpenAI: from explicit notes to background synthesis

ChatGPT originally exposed explicit saved memories, then added reference to chat history in 2025. OpenAI described saved memories and chat-derived insights as distinct systems with independent controls, deletion behavior, and Temporary Chat boundaries ([Memory and controls](https://openai.com/index/memory-and-new-controls-for-chatgpt/), [Memory help](https://help.openai.com/en/articles/8590148-memory-in-chatgpt)). In June 2026, OpenAI disclosed “dreaming,” a background process that synthesizes memory from many conversations to address staleness, correctness, and multi-year scale. The resulting summary is inspectable and editable, and the system continues to coexist with explicit saved memories ([Dreaming](https://openai.com/index/chatgpt-memory-dreaming/)).

This matters strategically: the leading consumer product has moved beyond append-only facts toward periodic consolidation, freshness management, and a reviewable synthesized state. It also confirms that explicit write cues alone under-capture useful information and that static saved facts grow stale.

At the agent-framework layer, the OpenAI Agents SDK treats conversation continuation as a replaceable session backend, offers SQLite, Redis, SQLAlchemy, MongoDB, Dapr, server-managed conversations, encryption/TTL, and automatic Responses compaction ([Sessions](https://openai.github.io/openai-agents-python/sessions/)). Its newer sandbox memory is a distinct two-phase pipeline: extract summaries and raw memories from conversations, then consolidate them into `MEMORY.md` and `memory_summary.md`; reading uses a small injected summary followed by search and selective opening of rollout summaries. Memories are explicitly advisory, and current environment evidence wins when memory is stale ([Agent memory](https://openai.github.io/openai-agents-python/sandbox/memory/)).

The OpenAI design therefore contains four separate layers: conversation history, compaction, durable distilled memory, and progressive disclosure. Engram currently owns only the durable-memory layer and parts of disclosure; it should not confuse this with owning session history or compaction.

### 2.2 Anthropic: context engineering, files, compaction, and versioned stores

Anthropic defines context engineering as selecting the smallest high-signal token set for inference. Its documented long-horizon techniques are compaction, structured note-taking, and multi-agent decomposition. The company explicitly recommends just-in-time retrieval and progressive disclosure over loading everything, while warning that excessive context degrades attention ([Effective context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).

Claude's platform memory tool is file-based and pairs with context editing: the agent can persist notes outside the context window, while tool-result clearing and compaction free active context. Anthropic's managed-agent memory stores make this operational: small text documents are mounted into a session, access can be read-only or read-write, separate stores can represent different owners/lifecycles, and every mutation produces an immutable version for audit/rollback/redaction ([official managed-agent memory guidance](https://github.com/anthropics/skills/blob/main/skills/claude-api/shared/managed-agents-memory.md)). Anthropic also warns never to store credentials because stored text can be replayed verbatim into future contexts.

Anthropic's architectural statement is particularly important for Engram: recoverable session storage is separated from arbitrary context management because future models will require different context strategies ([Managed Agents](https://www.anthropic.com/engineering/managed-agents)). This supports keeping Engram below the harness as a durable control plane rather than trying to own every host's compaction and prompt policy.

### 2.3 Google: exact scopes, configurable extraction, retrieval, and TTL

Google's Vertex AI Memory Bank generates long-term memories from source conversations and retrieves them by immutable exact scope, optionally with semantic similarity. Configuration controls which topics are meaningful enough to persist, can vary by scope, accepts few-shot extraction examples, selects generation and embedding models, and supports TTL policies ([setup and configuration](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/memory-bank/set-up), [retrieval](https://cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/memory-bank/fetch-memories)). ADK can orchestrate this service while sessions remain a separate abstraction.

Google's product demonstrates that memory admission is application-specific, scope is part of correctness rather than a search filter, and forgetting can be policy-driven. Engram has strong project scoping and review dates, but no general TTL/retention policy for durable memories and no typed, configurable extraction topics.

### 2.4 Microsoft: memory as a context provider, not a database choice

AutoGen defines `Memory` as a protocol with `add`, `query`, `update_context`, `clear`, and `close`; implementations choose their storage and retrieval mechanisms. Its basic implementation is chronological, while extensions use ChromaDB or Redis. The key abstraction is not vectors but a component responsible for enriching model context with relevant stored content ([AutoGen Memory protocol](https://microsoft.github.io/autogen/0.4.7/reference/python/autogen_core.memory.html), [Memory and RAG](https://microsoft.github.io/autogen/dev/user-guide/agentchat-user-guide/memory.html)).

Semantic Kernel's experimental agent memory supports a long-term provider (Mem0) and a short-term “whiteboard” that continually extracts requirements, proposals, decisions, and actions so chat history can be truncated without losing task state ([Semantic Kernel agent memory](https://learn.microsoft.com/en-us/semantic-kernel/frameworks/agent/agent-memory)). This reinforces the need to distinguish working state from cross-session memory and suggests a typed extraction vocabulary similar to what Engram already informally encourages.

### 2.5 LangGraph/LangMem: semantic, episodic, and procedural memory

LangMem explicitly separates:

- **Semantic memory:** facts and knowledge, represented either as a bounded profile or a searchable collection.
- **Episodic memory:** prior experiences used as examples of how to handle a situation.
- **Procedural memory:** rules and instructions that change how the agent behaves.

It supports both hot-path, agent-decided writes and background extraction/consolidation; hierarchical namespaces can isolate organization, user, application, or agent; and retrieval combines direct access, semantic search, and metadata filters ([conceptual guide](https://langchain-ai.github.io/langmem/concepts/conceptual_guide/), [memory tools](https://langchain-ai.github.io/langmem/reference/tools/)). The guide states that relevance is more than semantic similarity and may include importance, recency, and usage strength.

This is the clearest mature vocabulary for Engram's next schema. Engram currently has observation `type`, `scope`, `topic_key`, timestamps, duplicate counts, pins, and relations, but no first-class semantic/episodic/procedural kind, no structured profile, and no representation of an episode's situation/action/outcome.

### 2.6 Letta/MemGPT: memory as active context management

MemGPT introduced an operating-system analogy: a fixed context window is fast memory, external stores are slower tiers, and an agent moves information between them ([MemGPT paper](https://arxiv.org/abs/2310.08560)). Letta operationalized this as persistent core memory blocks plus archival and conversation search. Its current MemFS is a git-backed Markdown memory filesystem: memory blocks are kept in context while external memory remains discoverable and loads on demand ([MemFS](https://github.com/letta-ai/letta-docs-md/blob/main/concepts/memfs/index.md), [current agent memory instructions](https://github.com/letta-ai/letta-code/blob/main/src/agent/prompts/letta.md)).

Letta's most useful finding is methodological. A simple filesystem with an agent capable of iterative search achieved 74% on LoCoMo in Letta's test, leading the authors to argue that agent/tool usability can matter more than a sophisticated retrieval substrate ([filesystem benchmark](https://www.letta.com/blog/benchmarking-ai-agent-memory/)). Its separate memory benchmark measures reading, writing, updating, and unnecessary operations, rather than retrieval accuracy alone ([Letta Leaderboard](https://www.letta.com/blog/letta-leaderboard/)). These are first-party results from an interested vendor, not neutral proof, but they directly warn Engram against assuming embeddings or graphs solve activation.

### 2.7 MCP: portable capability transport, not a memory standard

MCP standardizes how hosts connect to servers exposing tools, resources, and prompts, with host-controlled permissions and security boundaries ([2025-11-25 architecture](https://modelcontextprotocol.io/specification/2025-11-25/architecture)). The 2026-07-28 protocol moved to a stateless lifecycle where version and capabilities travel on each request and servers needing cross-call state use explicit handles ([2026 specification schema](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/basic/index.mdx), [draft changelog](https://modelcontextprotocol.io/specification/draft/changelog)).

There is an official example “Memory” server, but it is an application built *over* MCP: a basic local knowledge graph of entities, relations, and observations with dedicated tools ([reference server](https://github.com/modelcontextprotocol/servers/blob/main/src/memory/README.md), [source](https://github.com/modelcontextprotocol/servers/blob/main/src/memory/index.ts)). The core protocol does not define memory semantics, lifecycle, provenance, forgetting, or portability between memory providers.

Therefore MCP is Engram's distribution and interoperation seam, not its domain model. Engram should not wait for MCP to define memory, nor encode product semantics in transport-specific behavior.

## 3. What research says is hard

### 3.1 Long context does not eliminate memory

“Lost in the Middle” found that long-context models' ability to use relevant evidence varies sharply with its position; adding more retrieved documents can saturate or reduce downstream value ([paper](https://aclanthology.org/2024.tacl-1.9/)). Anthropic independently frames context as a finite attention budget with diminishing marginal returns ([context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)). Memory remains a selection problem even when windows grow.

### 3.2 Retrieval benchmarks are necessary but insufficient

LoCoMo evaluates 600-turn, multi-session conversations and includes question answering, temporal/causal reasoning, summarization, and multimodal generation; long-context and RAG systems improve results but remain below human performance ([LoCoMo paper](https://aclanthology.org/2024.acl-long.747/)). LongMemEval measures extraction, multi-session reasoning, temporal reasoning, knowledge updates, and abstention, reporting a roughly 30% accuracy drop for commercial assistants and long-context models over sustained interaction ([LongMemEval](https://arxiv.org/abs/2410.10813)).

Newer work exposes further gaps. LoCoMo-Plus tests implicit constraints whose later cue does not lexically match the original fact, and reports that conventional string matching and explicit task prompting miss these failures ([LoCoMo-Plus](https://aclanthology.org/2026.acl-long.1150/)). AMemGym argues that static, off-policy memory datasets do not measure what an agent chooses to write during live interaction and proposes on-policy evaluation with evolving user state ([AMemGym](https://arxiv.org/abs/2603.01966)).

Engram's future evaluation must therefore cover the entire loop:

1. Did the system identify a future-useful experience?
2. Did it write a faithful, scoped representation?
3. Did it update, consolidate, or forget it correctly?
4. Did the host activate Recall at the right time?
5. Did retrieval return the right evidence without harmful noise?
6. Did the agent use it correctly, defer to newer source/runtime evidence, and complete the task better?
7. Did it avoid writing or recalling when memory had no value?

### 3.3 Useful memory is an evolving model, not an append-only archive

Generative Agents showed that observation, planning, and higher-order reflection each contributed to believable behavior; its architecture ranked retrieval by relevance, recency, and importance, then synthesized experiences into reflections ([original paper](https://arxiv.org/abs/2304.03442)). A-MEM builds linked notes whose attributes and connections evolve when new memory arrives ([A-MEM](https://arxiv.org/abs/2502.12110)). OpenAI's 2026 dreaming system explicitly targets stale and incorrect saved memories through background synthesis ([Dreaming](https://openai.com/index/chatgpt-memory-dreaming/)).

The invariant is not that Engram needs autonomous reflection immediately. It is that a durable system needs lifecycle operations beyond insert/search: validate, supersede, merge, split, decay, delete, and preserve evidence of why the current representation is trusted.

## 4. Invariants likely to outlive today's frameworks

The following are stronger strategic anchors than any current vendor feature:

1. **Context remains scarce.** Model windows will grow, but relevant-token density and attention remain constraints.
2. **Storage and delivery are separate systems.** Persisting something does not make it available, and retrieving it does not make it correctly used.
3. **Write policy and read policy require independent evaluation.** High capture recall can poison later context; high retrieval precision cannot recover facts never written.
4. **Current authoritative evidence wins.** Memory is advisory and must yield to user intent, maintained source, and runtime state.
5. **Scope is a security and correctness boundary.** User, team, project, agent, and task memories cannot safely share one undifferentiated namespace.
6. **Memory needs provenance and time.** A claim without source, observed time, validity interval, and mutation history becomes dangerous as it ages.
7. **Forgetting is a feature.** Deletion, expiration, supersession, compaction, and redaction are core lifecycle operations.
8. **Progressive disclosure beats bulk injection.** A small index/summary should lead to selectively fetched detail.
9. **Model capability is part of the memory system.** Tool affordances, prompts, and host lifecycle can dominate database sophistication.
10. **Evaluation must be end-to-end and on-policy.** Static retrieval scores do not establish task utility.
11. **Simple, legible representations are robust.** Text/files and small tool surfaces are easy for models to learn, inspect, migrate, and debug.
12. **Portability requires semantic contracts above transport.** MCP can carry tools, but it does not standardize memory meaning.

## 5. Audit of Engram today

### 5.1 What Engram gets right

#### A. The write boundary is explicit and atomic

Engram defines one terminal disposition per settled root user turn—`saved`, `skipped(no_durable_knowledge)`, or `needs_review`—and commits Memories plus the checkpoint atomically. The local checkpoint ledger is intentionally excluded from Recall, sync, and export ([canonical skill](../../skills/engram-memory/SKILL.md), [checkpoint store](../../internal/store/checkpoint.go), [ADR-0006](../adr/0006-own-memory-checkpoints-in-core.md)). This is a strong provenance primitive and a better foundation than invisible best-effort extraction.

#### B. Recall is bounded and advisory

The default surface exposes five tools, returns at most five summaries and 4 KiB initially, requires deliberate full-content retrieval, keeps current evidence authoritative, and surfaces conflicts ([memory core](../codebase/memory-core.md), [ADR-0010](../adr/0010-optimize-recall-for-bounded-utility.md)). This is aligned with progressive disclosure and context-budget discipline.

#### C. Privacy boundaries are unusually explicit

Raw prompt/subagent capture is disabled by default, consent is project/content-type scoped, diagnostic capture expires in 7–30 days, and content-free operational telemetry is kept outside Memory, sync, and export ([architecture](../ARCHITECTURE.md), [Recall baseline](../RECALL-BASELINE.md)). This is substantially better than treating all history as latent memory.

#### D. It already contains lifecycle primitives

Engram supports exact deduplication, topic-key upserts, revision and duplicate counts, review dates, pins, soft deletion, semantic relation verdicts, supersession, conflict surfacing, and explicit Recall feedback ([architecture](../ARCHITECTURE.md), [store schema](../../internal/store/store.go), [relations](../../internal/store/relations.go), [feedback](../../internal/store/recall_feedback.go)). These are valuable building blocks for consolidation and freshness.

#### E. Portability and ownership are real product advantages

The single Go binary, SQLite/FTS5 source of truth, CLI/HTTP/MCP adapters, local-first design, and opt-in replication avoid provider lock-in ([README](../../README.md), [memory core](../codebase/memory-core.md)). In an industry where memory is increasingly tied to proprietary agent runtimes, this is Engram's durable strategic moat.

#### F. Engram has shown discipline in killing unproven complexity

The project retired automatic Admission after a terminal no-go and removed Task Brief when generic cases and held-out value were insufficient ([ADR-0008](../adr/0008-retire-the-admission-experiment.md), [ADR-0009](../adr/0009-retire-task-briefing.md)). This is evidence that Engram can evolve without protecting failed experiments.

### 5.2 Where Engram is misaligned or incomplete

#### A. The Memory object is too weak for its intended lifetime

The durable `Observation` has title, free-form content, type, scope, project, topic key, timestamps, counts, and lifecycle metadata, but no first-class:

- memory kind (semantic, episodic, procedural, profile);
- claim/evidence separation;
- source references or evidence timestamps;
- confidence or verification status;
- validity interval or explicit expiry policy;
- owner/subject distinct from project;
- sensitivity/redaction class;
- structured situation/action/outcome for reusable episodes;
- schema/version describing how the memory was derived.

The checkpoint proves *which turn admitted the record*, but not *why the content is true*. Long-lived free text will eventually become ambiguous and stale.

#### B. “Semantic” operations are mostly model judgments around lexical FTS

Normal Recall uses SQLite FTS5/BM25. The schema reserves embedding columns, but the maintained retrieval path is lexical ([store schema](../../internal/store/store.go), [memory core](../codebase/memory-core.md)). This is not inherently wrong—simple lexical search is inspectable and fast—but it cannot reliably bridge cue/trigger semantic disconnect, synonyms, or implicit constraints demonstrated by LoCoMo-Plus. Engram needs a pluggable hybrid retrieval experiment, not an assumption that FTS is sufficient forever.

#### C. Activation remains the most serious product risk

Engram's frozen v1 useful-Recall study observed zero valid calibration rows because Codex did not initiate targeted Recall in the first cell; the publication remained `continue_canary` and did not open held-out evaluation ([study result](../RECALL-STUDY.md), [publication](../../evals/recall-study/v1/publication.json)). The project has excellent machinery for checkpoint activation and compatibility, but Recall still relies primarily on the agent deciding to call it.

This means the strongest current claim is “Engram can expose useful memory safely,” not “Engram makes agents use useful memory.” The difference is existential.

#### D. There is no consolidation loop

Topic-key upsert can replace one evolving record and relations can mark supersession, but Engram has no systematic process that finds fragmented memories, creates a higher-level synthesis, preserves supporting evidence, and retires redundant records. OpenAI dreaming, LangMem background management, Letta sleep-time memory, and Generative Agents reflection all point toward some form of background or explicit consolidation.

Engram should not automatically synthesize by default before it can evaluate faithfulness. But it needs a first-class, reviewable consolidation operation.

#### E. Forgetting is manual and incomplete

Durable Memories can become review-due or be soft/hard deleted, but review dates do not automatically change Recall eligibility in every surface, and there is no general per-kind TTL or “valid until” semantics. A decision superseded by source code differs from an expiring personal preference; both need explicit lifecycle policy.

#### F. Provenance is operational rather than epistemic

Session ID, project, tool name, checkpoint identity, relation provenance, and sync identity provide strong operational traceability. They do not identify the supporting commit, file/line, URL, command output, user assertion, or artifact from which a memory's claim was derived. Future agents cannot efficiently revalidate a memory against its source.

#### G. Cross-agent portability is transport-level, not semantic

Engram works across hosts through MCP/CLI, but host behavior still depends on installed skills, hooks, lifecycle identifiers, and activation cues. A memory written by one agent is textually portable, yet there is no exportable semantic envelope that another memory system could understand without Engram-specific instructions.

#### H. The current taxonomy conflates several jobs

Observation `type` values such as decision, bugfix, discovery, pattern, preference, and summary are useful editorial categories, but they mix content kind, workflow outcome, and intended retrieval behavior. A “bugfix” can be episodic evidence, semantic knowledge, and a procedural lesson simultaneously. The system needs separate axes rather than a larger enum.

### 5.3 Direct answer to the fear: will Engram save useful information for the future?

**Sometimes, by design; not yet by demonstrated system-level evidence.**

Engram's rubric selects exactly the information most likely to remain useful: decisions with reasons, non-obvious root causes, reusable invariants, durable preferences, and significant artifact identities. Its bounded Recall and current-evidence override reduce harm. These are strong priors.

However, future usefulness is a causal property, not a content aesthetic. A Memory is useful only if a later task:

`would have been worse without it` **and** `found it` **and** `trusted it appropriately` **and** `used it correctly`.

Engram currently records explicit Recall feedback (`decisive`, `orienting`, `duplicate`, `unused`; `current`, `stale`, `contradictory`, `unknown`) and false-empty review, which is the right measurement substrate. But no completed controlled study yet establishes the rate at which saved records change real task outcomes. Until that exists, “persistent memory” is proven; “useful future memory” remains the product hypothesis.

## 6. Strategic options

### Option A — Stay a disciplined project note store

Keep the schema and FTS architecture, improve host integrations, and market local portable notes.

**Advantages:** simplest, cheap, stable, private.
**Failure mode:** native platform memories become good enough; Engram remains an extra tool agents inconsistently invoke. Its checkpoint machinery outweighs perceived user value.

### Option B — Become a universal capture and personal-memory system

Ingest transcripts automatically, build user profiles, embeddings/graphs, and background “dreaming.”

**Advantages:** higher capture coverage and consumer personalization.
**Failure mode:** privacy risk, noise, cost, vendor competition, and departure from Engram's project-memory strength. It repeats the retired Admission bet before activation and utility are proven.

### Option C — Become the evidence-aware memory control plane (**recommended**)

Own the durable semantic contract and lifecycle while hosts own execution, history, and compaction. Preserve typed memories with provenance, expose small portable read/write primitives, support optional consolidation and hybrid retrieval behind evaluations, and measure whether later tasks improve.

**Advantages:** builds directly on Engram's strongest assets; remains useful across vendors; differentiates from raw vector stores and proprietary personalization; makes trust, audit, and portability first-class.
**Cost:** requires schema evolution, provenance UX, host conformance work, and rigorous eval investment.

### Option D — Become an agent runtime

Own sessions, compaction, subagents, sandbox state, memory, and orchestration.

**Advantages:** complete control over activation and evaluation.
**Failure mode:** competes with OpenAI, Anthropic, Google, Microsoft, LangGraph, and Letta; destroys the thin, agent-agnostic advantage. Reject.

## 7. Recommended product thesis

### 7.1 Product promise

> Engram remembers the durable project knowledge that future agents need, shows where it came from and whether it is still valid, and delivers only the smallest relevant evidence—locally or shared, across any agent.

The promise has three testable verbs:

- **Remember:** selection and representation are faithful.
- **Trust:** provenance, validity, conflicts, and lifecycle are inspectable.
- **Use:** Recall measurably improves later work under bounded cost/noise.

### 7.2 Principles

1. **Utility over retention.** A low-volume memory that changes work is better than a complete archive.
2. **Evidence before inference.** Preserve source references and observation time; derived summaries must link to evidence.
3. **Current truth wins.** Maintained source, runtime state, and current user intent override Memory.
4. **Typed but text-native.** Keep content readable and portable; add a small stable envelope rather than opaque representations.
5. **Progressive disclosure.** Inject an index or summary, retrieve a candidate, then fetch evidence.
6. **Explicit scopes and owners.** Project, repository, team, user, and agent scope are separate authority domains.
7. **Lifecycle is part of the record.** Validate, supersede, merge, split, expire, redact, and delete.
8. **Local-first, sync optional.** No cloud dependency for correctness.
9. **Host adapters stay thin.** Engram owns memory semantics; hosts own their session and compaction semantics.
10. **Every intelligent default earns rollout through on-policy evals.** No background extraction, embeddings, reranking, or synthesis by fashion.

### 7.3 Core capabilities

#### A. A stable Memory Envelope v2

Add orthogonal fields while retaining a human-readable body:

```text
id, schema_version
kind: semantic | episodic | procedural | profile
category: decision | constraint | preference | lesson | artifact | ...
subject/owner and scope
claim/body
rationale
evidence_refs[]: source kind, locator, observed_at, content hash/revision
derived_from[]: memory IDs or checkpoint/episode IDs
confidence and verification_status
valid_from, valid_until, review_after
sensitivity
created_at, updated_at, superseded_by
```

Not every field should be mandatory. The minimum portable contract should be small; stronger claims such as architecture decisions should require evidence or an explicit “user assertion” source.

#### B. Separate memory forms

- **Semantic claims:** stable facts, constraints, decisions, preferences.
- **Episodes:** situation → action → outcome → lesson, suitable as future examples.
- **Procedures:** verified workflows and agent instructions, never silently promoted into executable policy.
- **Profiles:** bounded structured state for user/team/project, with field-level mutation history.

#### C. Evidence-backed retrieval

Retain FTS as the deterministic baseline. Add a pluggable retrieval pipeline that can combine lexical score, semantic similarity, exact scope, kind/category filters, freshness, pin/importance, prior usefulness, and conflict state. Always return the score components and retrieval policy revision so behavior is explainable.

#### D. Reviewable consolidation

Introduce a dry-run `consolidate` operation that proposes merges, higher-order summaries, profile updates, or supersessions with source links. No automatic promotion until faithfulness and utility gates pass. Preserve source Memories and lineage even when a synthesis becomes the default Recall target.

#### E. Lifecycle and forgetting policy

Allow type/scope-specific review and TTL policies; distinguish “expired, do not recall” from “deleted” and “superseded.” Revalidation should compare evidence revisions where possible—for example, a source file hash or git commit—and route drift to review.

#### F. Activation contracts

Provide three host-independent modes:

1. **Explicit:** user/agent calls Recall.
2. **Cue:** host injects a minimal index of available memory domains, never full content.
3. **Brokered:** a cheap preflight decides whether Recall is likely valuable from content-free/task-safe features, under measurable gates.

The brokered mode should be optional and must prove incremental task utility over explicit/cue modes. The previous Task Brief and Admission results are reasons to test, not reasons never to revisit the problem with a better contract.

#### G. Portable bundles

Define a versioned JSONL/Markdown bundle containing the Memory Envelope, relations, evidence references, and tombstones without requiring Engram's database. MCP tools remain an adapter; the portable bundle is the semantic interchange format.

#### H. A first-class usefulness ledger

Build on existing Recall feedback while preserving privacy. Measure per policy/version:

- save precision and missed-save rate;
- activation rate and false activations;
- Recall precision, false-empty rate, and noise bytes;
- stale/contradictory rate;
- correct-use rate;
- task success delta, latency, and model/tool cost;
- memory operation overhead and user correction burden;
- deletion/redaction completeness.

### 7.4 North Star and guardrail metrics

**North Star:** the proportion of eligible future tasks in which an Engram Memory causes a correct, evidence-backed improvement that would not have occurred without it.

This is a counterfactual task metric, not the number of Memories, searches, tool calls, or tokens retrieved. In controlled evaluation it should be estimated as the paired task-success improvement of memory-enabled treatment over the same host/model/task without Engram Recall. In normal operation, explicit usefulness feedback and verified task outcomes can provide a lower-confidence proxy.

Guardrails must prevent optimization by indiscriminate recall:

- incorrect-use and stale/contradictory-use rate;
- irrelevant Recall rate and bytes injected;
- missed-useful-memory/false-empty rate;
- save precision and sensitive-data incidents;
- user correction/review burden;
- p50/p95 latency and total inference/tool cost;
- deletion and redaction completeness;
- checkpoint coverage and cross-host conformance.

A useful operational equation is:

`net memory value = task improvement − incorrect-memory harm − context noise − latency/cost − human correction burden`.

### 7.5 What Engram should and should not remember

| Save as durable Memory | Keep as history/state or leave in its authoritative source |
|---|---|
| A decision plus rationale, alternatives rejected, scope, and evidence | A decision already clear and current in an ADR unless Memory adds a compact discovery pointer or non-obvious rationale |
| A non-obvious failure signature, root cause, verified fix, and conditions | Raw logs, full tool output, stack traces without a reusable diagnosis |
| A stable invariant, constraint, compatibility boundary, or gotcha | Facts cheaply re-derived from maintained code, tests, configuration, or docs |
| A reusable episode: situation, action, outcome, and lesson | Routine activity, “what I did today,” or a generic session summary |
| A durable, explicitly stated preference or project convention | Inferred sensitive traits, incidental personal facts, or short-lived wishes |
| The identity and result of a significant external artifact/release/incident | Copies of large documents or knowledge-base content that should remain in its source system |
| A bounded procedural lesson proposed for review | Automatically executable instructions inferred from one success or failure |
| An unresolved contradiction as a review proposal, with both sides linked | A confident synthesized claim when sources disagree or evidence is missing |

The rule is: **save the smallest future decision-changing representation, plus enough evidence to verify it; do not save the cheapest path to reconstruct it.**

### 7.6 These gaps are recognized, but not yet ordered by one thesis

The upstream backlog already identifies several of the architectural gaps in this report:

- [#1086](https://github.com/Gentleman-Programming/engram/issues/1086): provenance and confidence;
- [#957](https://github.com/Gentleman-Programming/engram/issues/957): broader `review_after` and `expires_at` lifecycle;
- [#242](https://github.com/Gentleman-Programming/engram/issues/242): atomic consolidation and garbage collection;
- [#233](https://github.com/Gentleman-Programming/engram/issues/233): semantic search;
- [#184](https://github.com/Gentleman-Programming/engram/issues/184): observation version history.

Therefore the diagnosis is not “Engram has ignored modern memory.” The problem is sequencing: these issues can become disconnected features unless they serve one evidence-driven loop. Provenance and lifecycle should precede automatic consolidation; baseline evaluation should precede semantic retrieval rollout; version history should support trustworthy consolidation and revalidation; all of them should be evaluated against the North Star rather than shipped as independent capability parity.

### 7.7 Explicit non-goals

- Store every prompt, tool output, or transcript by default.
- Replace git, documentation, issue trackers, logs, or knowledge bases.
- Own general agent orchestration, sandbox execution, or compaction.
- Build a human-memory simulation.
- Require embeddings, a graph database, or cloud services for core operation.
- Mutate agent instructions automatically from inferred procedural memory.
- Claim “learning” when the system only stores or retrieves text.
- Optimize benchmark QA while ignoring task outcomes and unnecessary memory operations.

## 8. Roadmap by horizon

### Horizon 0 — Establish truth (0–6 weeks)

1. Publish the vocabulary in section 1 as a domain contract.
2. Instrument the existing end-to-end funnel: checkpoint → saved Memory → later Recall activation → candidate exposure → full fetch → explicit usefulness/quality → task outcome where available.
3. Repair the Recall evaluation harness so a missing agent-initiated call is itself a measured activation failure across a complete cohort, rather than preventing all downstream learning.
4. Build a small private, human-reviewed corpus from real Engram project tasks with counterfactual labels: what prior knowledge would have changed the task, which source proves it, and whether abstention is correct.
5. Measure baseline FTS Recall, activation, stale rate, and noise before changing retrieval.

**Exit criterion:** Engram can state, with confidence intervals, where useful memories are lost: admission, activation, retrieval, selection, or application.

### Horizon 1 — Trustworthy records (1–3 months)

1. Design Memory Envelope v2 through an ADR and expand-contract migration.
2. Add memory `kind`, evidence references, observation time, validity/review status, and lineage without breaking existing text records.
3. Add CLI/MCP inspection for “why is this remembered?”, “what supports it?”, and “what replaced it?”.
4. Add explicit expiration/invalidation separate from deletion.
5. Export/import the versioned portable bundle.

**Exit criterion:** a future agent can determine what a Memory asserts, where it came from, its authority scope, and whether it should still be trusted.

### Horizon 2 — Useful retrieval (3–6 months)

1. Create a retrieval interface with FTS as control.
2. Evaluate query expansion, metadata filtering, and optional embeddings/reranking against lexical-only Recall, including implicit-cue cases.
3. Make ranking explainable and policy-versioned.
4. Add small domain/index summaries for progressive disclosure and test host activation variants.
5. Evaluate across at least Codex, Claude Code, Gemini CLI/ADK-compatible flow, and one generic MCP client; do not optimize only one host/model.

**Exit criterion:** a preregistered treatment improves task-level useful Recall without violating noise, latency, privacy, and checkpoint gates.

### Horizon 3 — Memory evolution (6–12 months)

1. Ship reviewable consolidation dry-runs with lineage.
2. Add structured profiles and episodic situation/action/outcome memories as opt-in forms.
3. Add source-drift revalidation for git/file/URL evidence.
4. Implement policy-driven review/TTL by kind and scope.
5. Run longitudinal, on-policy studies that include updates, contradictions, forgetting, and abstention—not only factual lookup.

**Exit criterion:** Engram can keep a memory set smaller and more current over time while improving downstream outcomes.

### Horizon 4 — Ecosystem memory control plane (12–24 months)

1. Stabilize the semantic bundle and provider adapter API.
2. Support read-only and read-write domains with explicit ownership, similar to mounted stores but vendor-neutral.
3. Add team governance: approval policy, redaction audit, retention, access boundaries, and safe shared-memory conflict handling.
4. Explore MCP extensions only after the Engram semantic contract proves itself; keep core tools compatible with ordinary MCP.
5. Publish a reproducible multi-host memory benchmark focused on coding/project work and invite external implementations to compete on the same lifecycle metrics.

**Exit criterion:** memories created in one host remain interpretable, governable, and useful in another without copying the original transcript or depending on a single model.

## 9. What would falsify this strategy?

The recommendation should be abandoned or narrowed if evidence shows any of the following:

1. **Native memory dominance:** major agent hosts expose portable, user-owned, evidence-backed project memory with open export/import and equivalent privacy controls, making Engram a redundant adapter.
2. **No incremental utility:** in preregistered, on-policy coding tasks, Engram Recall fails to improve success, time, correction burden, or cost versus maintained repository docs and native host context.
3. **Activation remains model-bound:** no thin host contract can reliably trigger bounded Recall across vendors without intrusive prompt injection or unacceptable false positives.
4. **Provenance overhead exceeds value:** users and agents systematically omit evidence, and automated evidence capture cannot be made safe or accurate enough to justify schema complexity.
5. **Simple files win consistently:** a repository-native `MEMORY.md` plus normal file tools matches Engram on task utility, privacy, portability, review, and lifecycle at materially lower complexity.
6. **Consolidation harms trust:** synthesized memories introduce enough distortion that immutable atomic notes outperform them after accounting for review cost.
7. **Project scope is wrong:** real value primarily comes from cross-project personal profiles or organization-wide knowledge, and Engram cannot add those authority boundaries without compromising its local/project model.

These are not rhetorical risks. The roadmap should define a kill or pivot threshold for each major capability before implementation.

## 10. The decision

Engram should not chase the industry's visible surface area. It should deepen the part the platforms cannot credibly standardize for users: a local, portable, inspectable contract for durable agent experience.

The near-term priority is not more capture and not a vector database. It is to prove the complete useful-memory loop and strengthen the object being preserved. If Engram can show that an evidence-backed decision or lesson written by one agent is found at the right later moment, survives vendor changes, yields to current source when stale, and measurably improves the task, it will age well even as models, context windows, and host runtimes change.

If it cannot show that, the honest future is a smaller product: an excellent local project notebook. The evaluation must decide.

## Source index

### Industry and specifications

- OpenAI: [ChatGPT Memory](https://openai.com/index/memory-and-new-controls-for-chatgpt/), [Dreaming](https://openai.com/index/chatgpt-memory-dreaming/), [Agents SDK Sessions](https://openai.github.io/openai-agents-python/sessions/), [Sandbox agent memory](https://openai.github.io/openai-agents-python/sandbox/memory/)
- Anthropic: [Effective context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents), [Managed Agents](https://www.anthropic.com/engineering/managed-agents), [memory store guidance](https://github.com/anthropics/skills/blob/main/skills/claude-api/shared/managed-agents-memory.md)
- Google: [Memory Bank setup/configuration](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/memory-bank/set-up), [Memory retrieval](https://cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/memory-bank/fetch-memories), [ADK session API](https://google.github.io/adk-docs/api-reference/java/com/google/adk/sessions/BaseSessionService.html)
- Microsoft: [Semantic Kernel memory](https://learn.microsoft.com/en-us/semantic-kernel/frameworks/agent/agent-memory), [AutoGen Memory protocol](https://microsoft.github.io/autogen/0.4.7/reference/python/autogen_core.memory.html)
- LangChain: [LangMem concepts](https://langchain-ai.github.io/langmem/concepts/conceptual_guide/), [memory tools](https://langchain-ai.github.io/langmem/reference/tools/)
- Letta: [MemFS](https://github.com/letta-ai/letta-docs-md/blob/main/concepts/memfs/index.md), [filesystem benchmark](https://www.letta.com/blog/benchmarking-ai-agent-memory/), [memory benchmark](https://www.letta.com/blog/letta-leaderboard/)
- MCP: [2025 architecture](https://modelcontextprotocol.io/specification/2025-11-25/architecture), [2026 schema](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/basic/index.mdx), [reference Memory server](https://github.com/modelcontextprotocol/servers/blob/main/src/memory/README.md)

### Original research

- [MemGPT](https://arxiv.org/abs/2310.08560)
- [Generative Agents](https://arxiv.org/abs/2304.03442)
- [Lost in the Middle](https://aclanthology.org/2024.tacl-1.9/)
- [LoCoMo](https://aclanthology.org/2024.acl-long.747/)
- [LongMemEval](https://arxiv.org/abs/2410.10813)
- [A-MEM](https://arxiv.org/abs/2502.12110)
- [LoCoMo-Plus](https://aclanthology.org/2026.acl-long.1150/)
- [AMemGym](https://arxiv.org/abs/2603.01966)

### Engram evidence

- [README](../../README.md)
- [Memory core](../codebase/memory-core.md)
- [Architecture](../ARCHITECTURE.md)
- [Canonical Memory skill](../../skills/engram-memory/SKILL.md)
- [Recall study](../RECALL-STUDY.md) and [v1 publication](../../evals/recall-study/v1/publication.json)
- [ADR-0006: core-owned checkpoints](../adr/0006-own-memory-checkpoints-in-core.md)
- [ADR-0008: retire Admission](../adr/0008-retire-the-admission-experiment.md)
- [ADR-0009: retire Task Brief](../adr/0009-retire-task-briefing.md)
- [ADR-0010: bounded useful Recall](../adr/0010-optimize-recall-for-bounded-utility.md)
- [ADR-0011: mixed outcomes](../adr/0011-preserve-mixed-terminal-memory-outcomes.md)
