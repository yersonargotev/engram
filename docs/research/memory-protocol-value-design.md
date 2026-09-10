# Memory protocol value: design interview

Status: paused at the maintainer's request on 2026-09-07. Resume only if the
maintainer requests further work in a later session.

This historical, non-normative record is retained through
[issue #162](https://github.com/yersonargotev/engram/issues/162). Publication
preserves the interview; it does not resume the evaluation or authorize
experiments, telemetry, new infrastructure, or protocol changes.

This document records settled interview decisions
and open questions; it is not an implementation specification or rollout
authorization.

## Settled decisions

The maintainer accepted these decisions on 2026-09-07:

1. The initial evaluation concerns the maintainer using Codex on the same
   repository across sessions separated by days. Other hosts and populations
   require later validation.
2. The protocol must reduce errors caused by missing prior decisions or avoid
   repeated investigation. Memory volume, Recall invocation, checkpoint
   completion, and continuity alone do not demonstrate task benefit.
3. Mandatory per-root-turn checkpointing may be questioned in isolated
   experiments. Normal operation retains the current checkpoint guarantee.
   Capture consent, the separation of transient activity from Memory, and the
   authority of current evidence over Memory remain constraints.
4. Compare three isolated conditions: no persistent Engram Memory, selective
   saving without mandatory checkpoints, and the current protocol. Hold model,
   tools except treatment-specific Memory capabilities, initial code, and
   initial documentation constant. The two Memory conditions share the Recall
   strategy. This separates the value of Memory from the incremental value of
   mandatory checkpoints; exact treatment instructions remain to be defined.
5. Correctness must not worsen. Subject to that constraint, fewer errors or
   less repeated investigation can justify the protocol. Reduced errors may
   justify bounded extra cost; an efficiency-only benefit must compensate for
   the total protocol cost. Numeric limits will be fixed after measuring the
   current baseline and before the comparative experiment.
6. Use separate cost budgets for complete task chains and added delay in simple
   turns. Aggregate task savings cannot compensate for systematically slow
   small interactions. The simple-turn latency budget is finite, not zero.
7. The selective condition uses the same durable-knowledge rubric and save
   mechanism as the current protocol. It may save useful knowledge at task
   completion, but has no obligation to record a disposition for every turn
   and no verifier demanding that closure. A trivial turn may finish without
   a Memory operation. Differences in retention are measured outcomes.
8. Build small, reviewable cases reconstructed from the maintainer's real work
   without copying private conversations. Cover necessary prior decisions,
   knowledge recoverable from maintained sources, obsolete decisions
   contradicted by current evidence, related but irrelevant knowledge, and
   simple turns with nothing durable to preserve. Each case spans an
   information-producing session and a later task. Runs start from equivalent
   state and never share Memories produced by another run. Expected answers
   remain hidden from the evaluated agent.
9. Define correct outcomes before execution and evaluate them with tests or
   observable criteria. Use independent review for non-automatable outcomes,
   blinded to the condition where feasible. The maintainer validates case
   realism and resolves material disagreements. Agent Recall feedback is
   explanatory evidence, not the sole proof of task benefit.
10. Score final correctness separately from corrective work (discarded
    attempts, repairs, and human intervention). Define severe failures for each
    case in advance, including data loss, unauthorized content exposure, and
    violations of critical constraints. Such a failure blocks recommending
    that condition until understood and corrected; faster runs elsewhere do
    not compensate for it. A corrected initial mistake remains added cost
    even when the final answer is correct.
11. Repeated investigation is avoidable reconstruction of an available,
    still-valid conclusion. Checking whether that conclusion remains valid is
    legitimate work. Judge this distinction through case-specific criteria
    and observable actions; fewer reads or tool calls alone do not establish
    efficiency.
12. External environment failures are invalid executions handled under a
    predetermined retry rule. Agent non-use of available Recall and failures
    of the protocol itself remain measured outcomes. Missing measurements
    remain unknown, never zero cost. Insufficient valid evidence yields an
    inconclusive result, preserves normal operation, and requires an explicit
    bounded decision about further experimentation rather than indefinite
    sample expansion.
13. Scope the first delivery to instrumentation, initial measurement of the
    current protocol, and a diagnostic pilot of ten cases (two per category)
    across three conditions: thirty two-session chains. The pilot validates
    isolation, measurements, and evaluation criteria; it cannot establish a
    winning condition. Fix execution, time, and spending limits before running
    it. Pilot cases remain separate from later confirmatory evaluation cases.
14. Start each chain with an empty Memory database. The first session produces
    knowledge and the second tests its preservation and use. All conditions
    have the same maintained code and documentation. Reset conversation
    context between sessions and preserve only permitted artifacts. Obsolete
    knowledge cases introduce changed evidence between sessions. Evaluation
    of a large, accumulated Memory corpus is separate work.
15. Support conclusions separately for each measured dimension. Missing token
    attribution prevents a token-cost conclusion; missing per-turn reply delay
    prevents claiming the simple-turn budget is satisfied. Partial findings
    remain useful, but a general protocol change requires evidence for every
    agreed decision criterion.
16. Separate experiment preparation from execution authorization. Before model
    runs, a reviewable execution configuration must declare the model, maximum
    executions, time limits, retry policy, and a verifiable consumption budget.
    Execution depends on preparation and agreement on that budget. Reaching a
    limit stops execution and produces partial results.
17. For reconstructed experiment cases only, retain locally the observable
    actions, tool results, and agent responses needed for evaluation, excluding
    hidden model reasoning. Restrict access, exclude evidence from Git and
    synchronization, and expire it automatically after seven days. Retain
    publishable cases, rubrics, and a sanitized report durably. This does not
    enable capture of ordinary user sessions.
18. Run the two sessions in fresh processes without shared conversation and
    apply case-defined source changes between them. The pilot tests retention,
    retrieval, and adaptation to changed evidence, not real elapsed days.
    Age-dependent behavior requires explicit temporal tests; usefulness over
    weeks of operation remains a later evaluation.

These decisions set the evaluation direction, not its final design. Changing
the default requires explicit reconsideration of
[ADR-0006](../adr/0006-own-memory-checkpoints-in-core.md) and the controlled
comparison requirements of
[ADR-0010](../adr/0010-optimize-recall-for-bounded-utility.md).

## Existing evidence and measurement limits

- The [published Recall study](../RECALL-STUDY.md#published-v1-result) stopped
  before accepting its first calibration row because targeted Recall did not
  occur. It does not establish comparative task utility.
- [Recall baseline](../RECALL-BASELINE.md) measures local operation counts,
  duration, and available byte volume. Tool duration does not measure all
  agent deliberation or end-to-end reply delay, and bytes are not tokens.
- [Recall feedback](../RECALL-FEEDBACK.md) supports explicit utility and quality
  assessments. Exposure and missing feedback do not imply usefulness or harm.
- The current study runner measures process execution and records that duration
  as `time_to_useful_ms` only for successful tasks. It does not collect actual
  model token usage or per-turn final-response delay, and has no configurable
  cohort-wide cost budget. Future cost comparisons need measurements for failed
  runs as well as successes and a new measurement contract; the frozen v1
  study artifacts must remain unchanged.

## Open design questions

- Exact treatment instructions and permitted inter-session artifacts.
- Case-specific correctness, severity, and repeated-work criteria.
- Numeric latency and model-usage budgets, including simple turns.
- Case allocation and evidence access/consent boundaries.
- Activation checks, measurement attribution, and external-failure retry limits.
- Pilot execution budget and representation of inter-session elapsed time.
- Later prospective statistical design and practical benefit thresholds.
- Numeric decision gates and the budget for follow-up after inconclusive results.

## Proportionality review before specification

After accepting Q16-Q18, the maintainer explicitly raised overengineering risk
and requested evidence from the current session before advancing the design.
The accepted experimental design remains a possible route, not a requirement
to build the entire evaluation system.

A read-only inspection of the twelve preceding root-turn checkpoint identities
in the current session found twelve persisted terminal results: nine `saved`
and three `skipped`. All original record calls returned `created`; no record
failure or verifier recovery was observed in the conversation. This supports
successful checkpoint completion for these turns, not universal reliability
or cross-session task benefit.

Five Memories separately preserve interview rounds Q1-Q15, whose decisions are
also recorded in this document. That is a concrete opportunity to improve
selection and reduce fragmented Memory content using existing mechanisms before
adding protocol features. It is not evidence that those Memories are all
useless, nor authorization to delete or rewrite them.

The available baseline report contains no operational events, so it cannot
measure this session's full checkpoint overhead. The Recall feedback report is
an aggregate and cannot be attributed to this session. Several isolated record
commands observed in the conversation completed in roughly 0.03-0.3 seconds;
this excludes preflight, agent work, and total reply delay. No new telemetry was
enabled and no model experiment was launched for this inspection.

Current recommendation, pending the maintainer's scope decision: retain the
working checkpoint protocol, improve Memory selection, and inspect a small
number of ordinary-session outcomes before commissioning new evaluation
infrastructure. Escalate only for a concrete recurring failure, material delay,
or demonstrated loss of useful knowledge.

The interview precedes a specification and independently verifiable tickets.
At the time of the interview, recording these decisions had not started any
experiment, telemetry collection, public issue, or runtime change. The later
retention issue above authorizes documentation publication only.

The maintainer requested consolidation of the fragmented interview Memories
into one current-state summary pointing here. The detailed decisions and audit
remain in this document; the evaluation itself is deferred while the maintainer
considers whether it is necessary.
