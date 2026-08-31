const SUMMARY_FIELD_PATHS = [
  ["summary"],
  ["compactedSummary"],
  ["compacted_summary"],
  ["compactSummary"],
  ["compact_summary"],
  ["content"],
  ["text"],
  ["message"],
  ["compacted", "summary"],
  ["compacted", "content"],
  ["compaction", "summary"],
  ["compaction", "content"],
  ["output", "summary"],
  ["output", "content"],
  ["payload", "summary"],
  ["payload", "content"],
  ["data", "summary"],
  ["data", "content"],
];

function getPath(root, path) {
  let current = root;
  for (const key of path) {
    if (!current || typeof current !== "object" || !(key in current)) return undefined;
    current = current[key];
  }
  return current;
}

function normalizeSummary(value) {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Best-effort extraction for Pi compaction event shapes. Unsupported shapes
 * intentionally return undefined instead of throwing.
 */
export function extractCompactedSummary(event) {
  if (!event || typeof event !== "object") return undefined;
  for (const path of SUMMARY_FIELD_PATHS) {
    const summary = normalizeSummary(getPath(event, path));
    if (summary) return summary;
  }
  return undefined;
}

export function recoveryInstruction(project) {
  return (
    `Engram Terminal Memory after compaction:\n` +
    `Continue the same root user turn and reuse its supplied opaque identity. ` +
    `After all remaining causal work settles, make one Terminal Memory commit as saved, needs_review, ` +
    `or skipped(no_durable_knowledge). Project hint: '${project}'.`
  );
}

export function buildRecoveryNotice(project, context) {
  const instruction = recoveryInstruction(project);
  const trimmedContext = typeof context === "string" ? context.trim() : "";
  return trimmedContext ? `${trimmedContext}\n\n${instruction}` : instruction;
}
