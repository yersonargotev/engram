---
status: accepted
---

# Preserve mixed terminal Memory outcomes without promoting proposals

A Terminal Memory preflight reuses exact duplicates and exposes at most three
semantic candidates without persisting a Memory or checkpoint. Clear low-risk
relations may be resolved before finalization; low confidence or a material
architecture, policy, or decision conflict selects `needs_review`.

A `needs_review` checkpoint may atomically attach zero or more settled Memories
and exactly one Memory proposal. When it attaches at least one Memory it is a Mixed
Memory checkpoint. The proposal remains immutable local audit evidence excluded
from Memory, Recall, sync, export, Admission, and Promotion; `saved` and `skipped`
semantics remain unchanged.

This extends [ADR-0006](0006-own-memory-checkpoints-in-core.md) and the original
`needs_review` contract without changing checkpoint identity, exact replay,
one-checkpoint-per-root-turn semantics, or the prohibition on automatically
turning ambiguous knowledge into a Memory. Detailed behavior remains owned by
[specification #98](https://github.com/yersonargotev/engram/issues/98) and
[delivery ticket #104](https://github.com/yersonargotev/engram/issues/104).
