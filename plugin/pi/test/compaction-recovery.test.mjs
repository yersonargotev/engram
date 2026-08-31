import test from "node:test";
import assert from "node:assert/strict";
import { buildRecoveryNotice, extractCompactedSummary, recoveryInstruction } from "../compaction-recovery.js";

test("extractCompactedSummary returns undefined for unsupported event shapes", () => {
  assert.equal(extractCompactedSummary(null), undefined);
  assert.equal(extractCompactedSummary({}), undefined);
  assert.equal(extractCompactedSummary({ payload: { unrelated: "value" } }), undefined);
  assert.equal(extractCompactedSummary({ summary: "   " }), undefined);
});

test("extractCompactedSummary supports top-level and nested summary fields", () => {
  assert.equal(extractCompactedSummary({ compactedSummary: "summary text" }), "summary text");
  assert.equal(extractCompactedSummary({ payload: { summary: " nested summary " } }), "nested summary");
  assert.equal(extractCompactedSummary({ compaction: { content: "content summary" } }), "content summary");
});

test("recoveryInstruction keeps the same root-turn Terminal Memory contract", () => {
  const notice = recoveryInstruction("engram");
  assert.match(notice, /Terminal Memory/);
  assert.match(notice, /same root user turn/);
  assert.match(notice, /saved, needs_review, or skipped\(no_durable_knowledge\)/);
  assert.doesNotMatch(notice, /FIRST ACTION REQUIRED|mem_session_summary/);
});

test("buildRecoveryNotice prefixes context when available", () => {
  assert.equal(buildRecoveryNotice("engram", "existing context").startsWith("existing context\n\nEngram Terminal Memory"), true);
  assert.equal(buildRecoveryNotice("engram", "").startsWith("Engram Terminal Memory"), true);
});
