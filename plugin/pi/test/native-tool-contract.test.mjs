import assert from "node:assert/strict";
import { test } from "node:test";
import { importPluginFromSandbox, PLUGIN_ROOT, withPluginSandbox } from "./plugin-sandbox.mjs";

// Contract tests below exercise deferred curation/lifecycle tools explicitly.
process.env.ENGRAM_PI_TOOL_PROFILE = "all";

// The runtime context these fixtures hand the plugin still points at the checkout, because the
// plugin only ever reads `cwd`. The module it loads comes from the sandbox, so nothing under the
// checkout is written to or removed.
const ROOT = PLUGIN_ROOT;

function deferred() {
  let resolve;
  const promise = new Promise((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

// Each sandbox lives at its own path, so every call already loads a fresh module graph and no
// cache-busting query string is needed.
async function loadPluginHarness(sandbox) {
  const registeredTools = new Map();
  const eventHandlers = new Map();
  const registerEngram = await importPluginFromSandbox(sandbox);
  registerEngram({
    registerTool(tool) {
      registeredTools.set(tool.name, tool);
    },
    on(event, handler) {
      eventHandlers.set(event, handler);
    },
  });
  return { registeredTools, eventHandlers };
}

function runtimeContext(sessionId) {
  return {
    cwd: ROOT,
    sessionManager: { getSessionId: () => sessionId },
    ui: { setStatus() {} },
  };
}

// Records every request the extension issues so a test can assert the wire contract the Engram
// HTTP server actually receives, instead of asserting over the extension source text.
function recordingFetch(routes) {
  const calls = [];
  const fetchStub = async (url, init = {}) => {
    const method = init.method ?? "GET";
    const path = new URL(url).pathname + new URL(url).search;
    const body = init.body ? JSON.parse(init.body) : undefined;
    calls.push({ method, path, body });
    const route = routes.find((candidate) => candidate.method === method && path.startsWith(candidate.path));
    const status = route?.status ?? 200;
    const payload = route?.body ?? {};
    return new Response(JSON.stringify(payload), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  };
  return { calls, fetchStub };
}

test("registered Pi-native mem_save_prompt returns content-free consent metadata", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";

  const { calls, fetchStub } = recordingFetch([
    { method: "GET", path: "/health", body: { status: "ok" } },
    { method: "GET", path: "/project/current", body: { project: "paidosdep" } },
    { method: "POST", path: "/sessions", body: { status: "ok" } },
    { method: "POST", path: "/prompts", status: 202, body: { captured: false, reason_code: "consent_disabled" } },
  ]);
  globalThis.fetch = fetchStub;

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
      const { registeredTools } = await loadPluginHarness(sandbox);
      const memSavePrompt = registeredTools.get("mem_save_prompt");
      assert.ok(memSavePrompt, "mem_save_prompt tool should be registered");

      const result = await memSavePrompt.execute(
        "tool-call-prompt",
        { content: "preserve this exact user prompt", project: "paidosdep" },
        undefined,
        undefined,
        runtimeContext("test-session"),
      );

      assert.notEqual(result.isError, true, "an evaluated capture request must not surface as a tool error");

      // The prompt must reach POST /prompts carrying the requested project scope, and the session it
      // references must have been created under that same project first.
      const promptCall = calls.find((call) => call.method === "POST" && call.path === "/prompts");
      assert.ok(promptCall, "mem_save_prompt must POST to /prompts");
      assert.equal(promptCall.body.project, "paidosdep");
      assert.equal(promptCall.body.content, "preserve this exact user prompt");
      assert.ok(promptCall.body.session_id, "the prompt must be attributed to a session");

      const sessionCall = calls.find((call) => call.method === "POST" && call.path === "/sessions");
      assert.ok(sessionCall, "mem_save_prompt must ensure its session exists before writing");
      assert.equal(sessionCall.body.project, "paidosdep");
      assert.equal(sessionCall.body.id, promptCall.body.session_id);
      assert.ok(
        calls.indexOf(sessionCall) < calls.indexOf(promptCall),
        "the session must be created before the prompt that references it",
      );

      assert.deepEqual(result.details.data, { captured: false, reason_code: "consent_disabled" });
      assert.equal(result.details.data.id, undefined, "a Legacy prompt id must not be returned");
      assert.equal(JSON.stringify(result.details.data).includes("preserve this exact user prompt"), false);
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("Pi-native checkpoints use the configured HTTP provider and preserve structured failures", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  const originalBin = process.env.ENGRAM_BIN;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  process.env.ENGRAM_BIN = "/does/not/exist/engram";
  const checkpointCalls = [];
  globalThis.fetch = async (url, init = {}) => {
    const path = new URL(url).pathname;
    if (path === "/health") return new Response(`{"status":"ok"}`);
    if (path === "/project/current") {
      return new Response(JSON.stringify({
        project: "engram", project_source: "git_remote", project_strength: "strong", implicit_write_allowed: true,
      }));
    }
    if (path === "/checkpoints/status") {
      checkpointCalls.push({ method: init.method ?? "GET", path: new URL(url).pathname + new URL(url).search });
      return new Response(JSON.stringify({ checkpoint: { disposition: "skipped" } }));
    }
    if (path === "/checkpoints") {
      const body = JSON.parse(init.body);
      checkpointCalls.push({ method: init.method, path, body });
      if (body.root_turn_id === "turn-conflict") {
        return new Response(JSON.stringify({
          code: "checkpoint_conflict",
          message: "checkpoint already recorded with a different terminal result",
          details: { existing_disposition: "saved" },
        }), { status: 409 });
      }
      if (body.root_turn_id === "turn-invalid") {
        return new Response(JSON.stringify({
          code: "invalid_checkpoint_identity",
          message: "checkpoint identity fields must be non-empty",
          details: { field: "session_id" },
        }), { status: 400 });
      }
      return new Response(JSON.stringify({ idempotency: "created", checkpoint: { disposition: "skipped" } }), { status: 201 });
    }
    return new Response(`{}`, { status: 404 });
  };

  try {
    await withPluginSandbox("engram-pi-checkpoint-provider-", async ({ sandbox }) => {
      const { registeredTools } = await loadPluginHarness(sandbox);
      const checkpoint = registeredTools.get("mem_checkpoint");
      const status = registeredTools.get("mem_checkpoint_status");
      const ctx = runtimeContext("runtime-session");

      const success = await checkpoint.execute("checkpoint-success", {
        host: "pi", session_id: "session-success", root_turn_id: "turn-success",
        disposition: "skipped", reason: "no_durable_knowledge",
      }, undefined, undefined, ctx);
      assert.notEqual(success.isError, true);

      for (const [turn, code, detail] of [
        ["turn-conflict", "checkpoint_conflict", "saved"],
        ["turn-invalid", "invalid_checkpoint_identity", "session_id"],
      ]) {
        const failed = await checkpoint.execute(`checkpoint-${turn}`, {
          host: "pi", session_id: "session-error", root_turn_id: turn,
          disposition: "skipped", reason: "no_durable_knowledge",
        }, undefined, undefined, ctx);
        assert.equal(failed.isError, true);
        assert.equal(failed.details.data.code, code);
        assert.ok(Object.values(failed.details.data.details).includes(detail));
        assert.match(failed.content[0].text, /checkpoint|identity/);
      }

      const inspected = await status.execute("checkpoint-status", {
        host: "pi", session_id: "session-success", root_turn_id: "turn-success",
      }, undefined, undefined, ctx);
      assert.notEqual(inspected.isError, true);
      assert.deepEqual(checkpointCalls.map((call) => call.method), ["POST", "POST", "POST", "GET"]);
      assert.ok(checkpointCalls.every((call) => call.path.startsWith("/checkpoints")));
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
    if (originalBin === undefined) delete process.env.ENGRAM_BIN;
    else process.env.ENGRAM_BIN = originalBin;
  }
});

test("registered Pi-native mem_search fails open on native provider transport failure", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  globalThis.fetch = async () => {
    await new Promise((resolve) => setTimeout(resolve, 5));
    throw new Error("connection refused");
  };

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ dir, sandbox }) => {
      const registeredTools = new Map();
      const registerEngram = await importPluginFromSandbox(sandbox);
      registerEngram({
        registerTool(tool) {
          registeredTools.set(tool.name, tool);
        },
        on() {},
      });

      const memSearch = registeredTools.get("mem_search");
      assert.ok(memSearch, "mem_search tool should be registered");

      const result = await memSearch.execute(
        "tool-call-1",
        { query: "state markers", project: "gentle-agent-state" },
        undefined,
        undefined,
        {
          cwd: dir,
          sessionManager: { getSessionId: () => "test-session" },
          ui: { setStatus() {} },
        },
      );

      assert.notEqual(result.isError, true);
      assert.equal(result.details.data.result_count, 0);
      assert.ok(result.details.data.elapsed_monotonic_ms >= 1);
      assert.equal(result.details.data.warning.code, "recall_unavailable");
      assert.equal(result.details.data.diagnostics[0].code, "recall_transport_failure");

      const contentResult = await registeredTools.get("mem_get_observation").execute(
        "tool-call-content-failure",
        { recall_id: "recall-selected", result_id: "result-selected" },
        undefined,
        undefined,
        runtimeContext("test-session"),
      );
      assert.notEqual(contentResult.isError, true);
      assert.equal(contentResult.details.data.warning.code, "recall_unavailable");
      assert.equal(contentResult.details.data.diagnostics[0].operation, "recall_content");
      assert.equal(contentResult.details.data.memory.content, "");

      const legacyResult = await registeredTools.get("mem_get_observation").execute(
        "tool-call-legacy-content-failure",
        { id: 42 },
        undefined,
        undefined,
        runtimeContext("test-session"),
      );
      assert.equal(legacyResult.isError, true);
      assert.equal(legacyResult.details.data, undefined);
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("Pi personal Recall without an explicit project omits detected project authority", async () => {
	const originalFetch = globalThis.fetch;
	const originalUrl = process.env.ENGRAM_URL;
	process.env.ENGRAM_URL = "http://127.0.0.1:17437";
	const { calls, fetchStub } = recordingFetch([
		{ method: "GET", path: "/health", body: { status: "ok" } },
		{ method: "GET", path: "/project/current", body: { project: "detected-project", project_source: "git_remote", project_strength: "strong" } },
		{ method: "GET", path: "/recall", body: { recall_id: "recall-personal", results: [], result_ids: [], result_count: 0, delivered_utf8_bytes: 2, elapsed_monotonic_ms: 1, provenance: { protocol_version: 1, binary_version: "test" } } },
	]);
	globalThis.fetch = fetchStub;

	try {
		await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
			const { registeredTools } = await loadPluginHarness(sandbox);
			const result = await registeredTools.get("mem_search").execute(
				"tool-call-personal", { query: "preferences", scope: "personal" }, undefined, undefined,
				runtimeContext("personal-session"),
			);
			assert.notEqual(result.isError, true);
			const recallCall = calls.find((call) => call.path.startsWith("/recall?"));
			assert.ok(recallCall);
			const query = new URL(`http://localhost${recallCall.path}`).searchParams;
			assert.equal(query.get("scope"), "personal");
			assert.equal(query.get("project"), null);
			assert.equal(query.get("project_strength"), "aggregate");
		});
	} finally {
		globalThis.fetch = originalFetch;
		if (originalUrl === undefined) delete process.env.ENGRAM_URL;
		else process.env.ENGRAM_URL = originalUrl;
	}
});

test("Pi complete Recall forwards the opaque selection and explicit continuation", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  const { calls, fetchStub } = recordingFetch([
    { method: "GET", path: "/health", body: { status: "ok" } },
    { method: "GET", path: "/project/current", body: { project: "engram", project_source: "git_remote", project_strength: "strong" } },
    { method: "GET", path: "/recall/content", body: { memory: { content: "continued" }, position: 16384, original_bytes: 16400, delivered_utf8_bytes: 16, limit_bytes: 16384, truncated: false } },
  ]);
  globalThis.fetch = fetchStub;

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
      const { registeredTools } = await loadPluginHarness(sandbox);
      const getObservation = registeredTools.get("mem_get_observation");
      assert.ok(getObservation, "mem_get_observation tool should be registered");

      const result = await getObservation.execute(
        "tool-call-content",
        { recall_id: "recall-opaque", result_id: "result-opaque", position: 16384, project: "engram" },
        undefined,
        undefined,
        runtimeContext("recall-content-session"),
      );
      assert.notEqual(result.isError, true);

      const contentCall = calls.find((call) => call.path.startsWith("/recall/content?"));
      assert.ok(contentCall, "mem_get_observation must call the bounded content endpoint");
      const query = new URL(`http://localhost${contentCall.path}`).searchParams;
      assert.equal(query.get("recall_id"), "recall-opaque");
      assert.equal(query.get("result_id"), "result-opaque");
      assert.equal(query.get("position"), "16384");
      assert.equal(query.get("project"), "engram");
      assert.equal(query.get("project_strength"), "explicit");
      assert.equal(query.get("scope"), "project");
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("Pi explicit curation preserves legacy observation ID retrieval", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  const { calls, fetchStub } = recordingFetch([
    { method: "GET", path: "/health", body: { status: "ok" } },
    { method: "GET", path: "/project/current", body: { project: "engram", project_source: "git_remote", project_strength: "strong" } },
    { method: "GET", path: "/observations/42", body: { id: 42, title: "Legacy curation", content: "complete" } },
  ]);
  globalThis.fetch = fetchStub;
  try {
    await withPluginSandbox("engram-pi-legacy-get-", async ({ sandbox }) => {
      const { registeredTools } = await loadPluginHarness(sandbox);
      const result = await registeredTools.get("mem_get_observation").execute(
        "tool-call-legacy-content", { id: 42 }, undefined, undefined, runtimeContext("legacy-content-session"),
      );
      assert.notEqual(result.isError, true);
      assert.ok(calls.some((call) => call.path === "/observations/42"));
      assert.equal(result.details.data.id, 42);
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("weak detected identity remains available to reads but cannot authorize Pi writes", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  let sessionWrites = 0;
  let observationWrites = 0;
  let checkpointWrites = 0;
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path === "/health") return { ok: true, async json() { return { status: "ok" }; } };
    if (path === "/project/current") {
      return {
        ok: true,
        async json() {
          return {
            project: "local-repo",
            project_source: "git_root",
            project_path: ROOT,
            project_strength: "weak",
            implicit_write_allowed: false,
            safe_next_action: "provide an explicit project name and retry the write",
          };
        },
      };
    }
    if (path === "/context") return { ok: true, async json() { return { context: "readable weak context" }; } };
    if (path === "/sessions") sessionWrites += 1;
    if (path === "/observations") observationWrites += 1;
    if (path === "/checkpoints") checkpointWrites += 1;
    return { ok: true, status: 201, async json() { return {}; } };
  };

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
      const { registeredTools } = await loadPluginHarness(sandbox);
      const ctx = runtimeContext("weak-identity-session");

      const read = await registeredTools.get("mem_context").execute("weak-read", {}, undefined, undefined, ctx);
      assert.equal(read.isError, undefined);
      assert.match(read.content[0].text, /readable weak context/);

      const write = await registeredTools.get("mem_save").execute(
        "weak-write",
        { title: "must not persist", content: "weak identity" },
        undefined,
        undefined,
        ctx,
      );
      assert.equal(write.isError, true);
      assert.match(write.content[0].text, /weak_project_identity/);
      assert.match(write.content[0].text, /provide an explicit project name and retry the write/);
      assert.equal(sessionWrites, 0);
      assert.equal(observationWrites, 0);

      const checkpoint = await registeredTools.get("mem_checkpoint").execute(
        "weak-checkpoint",
        {
          host: "pi", session_id: "weak-identity-session", root_turn_id: "turn-weak",
          disposition: "needs_review", proposal: { title: "Review", content: "Must not persist" },
        },
        undefined,
        undefined,
        ctx,
      );
      assert.equal(checkpoint.isError, true);
      assert.match(checkpoint.content[0].text, /weak_project_identity/);
      assert.equal(checkpointWrites, 0, "weak automatic identity must not authorize durable checkpoint content");
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("session-attributed Pi writes bind to acknowledged runtime identity and retry failed registration", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  let registrationAttempts = 0;
  const observationBodies = [];
  const sessionBodies = [];
  globalThis.fetch = async (url, init) => {
    const path = new URL(url).pathname;
    if (path === "/health") return { ok: true, async json() { return { status: "ok" }; } };
    if (path === "/project/current") {
      return { ok: true, async json() { return { project: "pi", project_source: "git_remote", project_path: ROOT, project_strength: "strong", implicit_write_allowed: true }; } };
    }
    if (path === "/sessions") {
      registrationAttempts += 1;
      sessionBodies.push(JSON.parse(init.body));
      if (registrationAttempts === 1) {
        return { ok: false, status: 503, async json() { return { error: "registration unavailable" }; } };
      }
      return { ok: true, status: 201, async json() { return { status: "created" }; } };
    }
    if (path === "/observations") {
      observationBodies.push(JSON.parse(init.body));
      return { ok: true, status: 201, async json() { return { id: observationBodies.length }; } };
    }
    return { ok: true, async json() { return {}; } };
  };

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
      const { registeredTools } = await loadPluginHarness(sandbox);

      const memSave = registeredTools.get("mem_save");
      const ctx = runtimeContext("runtime-session");
      const params = { title: "runtime binding", content: "content", session_id: "model-invented" };

      const failed = await memSave.execute("call-1", params, undefined, undefined, ctx);
      assert.equal(failed.isError, true);
      assert.equal(observationBodies.length, 0, "unacknowledged registration must stop the write");

      const succeeded = await memSave.execute("call-2", params, undefined, undefined, ctx);
      assert.equal(succeeded.isError, undefined);
      assert.equal(registrationAttempts, 2, "failed registration must remain retryable");
      assert.equal(sessionBodies[1].id, "runtime-session");
      assert.equal(observationBodies[0].session_id, "runtime-session");
      assert.notEqual(observationBodies[0].session_id, "model-invented");

      await memSave.execute("call-3", params, undefined, undefined, ctx);
      assert.equal(registrationAttempts, 2, "successful acknowledgement should be cached");

      const noRuntime = await memSave.execute(
        "call-4",
        params,
        undefined,
        undefined,
        { ...ctx, sessionManager: { getSessionId: () => undefined } },
      );
      assert.equal(noRuntime.isError, true);
      assert.match(noRuntime.content[0].text, /Pi runtime session ID is unavailable/);
      assert.equal(registrationAttempts, 2, "missing runtime identity must not synthesize or register a session");
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("parallel first-use writes share one acknowledged registration and keep it cached", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  const registrationGate = deferred();
  let registrationAttempts = 0;
  const writeRequests = [];
  globalThis.fetch = async (url, init) => {
    const path = new URL(url).pathname;
    if (path === "/health") return { ok: true, async json() { return { status: "ok" }; } };
    if (path === "/project/current") {
      return { ok: true, async json() { return { project: "pi", project_source: "git_remote", project_path: ROOT, project_strength: "strong", implicit_write_allowed: true }; } };
    }
    if (path === "/sessions") {
      registrationAttempts += 1;
      await registrationGate.promise;
      return { ok: true, status: 201, async json() { return { status: "created" }; } };
    }
    if (path === "/observations") {
      writeRequests.push(JSON.parse(init.body));
      return { ok: true, status: 201, async json() { return { id: writeRequests.length }; } };
    }
    return { ok: true, async json() { return {}; } };
  };

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
      const { registeredTools, eventHandlers } = await loadPluginHarness(sandbox);
      const memSave = registeredTools.get("mem_save");
      const ctx = runtimeContext("parallel-success-session");
      await eventHandlers.get("session_start")({}, ctx);

      const firstWrite = memSave.execute("parallel-success-1", { title: "first", content: "one" }, undefined, undefined, ctx);
      const secondWrite = memSave.execute("parallel-success-2", { title: "second", content: "two" }, undefined, undefined, ctx);
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(registrationAttempts, 1, "parallel first writes must share one registration request");

      registrationGate.resolve();
      const [firstResult, secondResult] = await Promise.all([firstWrite, secondWrite]);
      assert.equal(firstResult.isError, undefined);
      assert.equal(secondResult.isError, undefined);
      assert.deepEqual(writeRequests.map((request) => request.title).sort(), ["first", "second"]);
      assert.ok(writeRequests.every((request) => request.session_id === "parallel-success-session"));

      await memSave.execute("parallel-success-cached", { title: "cached", content: "three" }, undefined, undefined, ctx);
      assert.equal(registrationAttempts, 1, "acknowledged registration must remain cached");
      assert.equal(writeRequests.length, 3);
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("shared registration failure rejects parallel writes and a later call retries", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  const registrationGate = deferred();
  let registrationAttempts = 0;
  let registrationShouldFail = true;
  const writeRequests = [];
  globalThis.fetch = async (url, init) => {
    const path = new URL(url).pathname;
    if (path === "/health") return { ok: true, async json() { return { status: "ok" }; } };
    if (path === "/project/current") {
      return { ok: true, async json() { return { project: "pi", project_source: "git_remote", project_path: ROOT, project_strength: "strong", implicit_write_allowed: true }; } };
    }
    if (path === "/sessions") {
      registrationAttempts += 1;
      if (registrationShouldFail) {
        await registrationGate.promise;
        return { ok: false, status: 503, async json() { return { error: "registration unavailable" }; } };
      }
      return { ok: true, status: 201, async json() { return { status: "created" }; } };
    }
    if (path === "/observations") {
      writeRequests.push(JSON.parse(init.body));
      return { ok: true, status: 201, async json() { return { id: writeRequests.length }; } };
    }
    return { ok: true, async json() { return {}; } };
  };

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
      const { registeredTools, eventHandlers } = await loadPluginHarness(sandbox);
      const memSave = registeredTools.get("mem_save");
      const ctx = runtimeContext("parallel-failure-session");
      await eventHandlers.get("session_start")({}, ctx);

      const firstWrite = memSave.execute("parallel-failure-1", { title: "first", content: "one" }, undefined, undefined, ctx);
      const secondWrite = memSave.execute("parallel-failure-2", { title: "second", content: "two" }, undefined, undefined, ctx);
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(registrationAttempts, 1, "parallel failed writes must share one registration request");

      registrationGate.resolve();
      const [firstResult, secondResult] = await Promise.all([firstWrite, secondWrite]);
      assert.equal(firstResult.isError, true);
      assert.equal(secondResult.isError, true);
      assert.match(firstResult.content[0].text, /registration unavailable/);
      assert.match(secondResult.content[0].text, /registration unavailable/);
      assert.equal(writeRequests.length, 0, "failed registration must stop every waiting write");

      registrationShouldFail = false;
      const retryResult = await memSave.execute("parallel-failure-retry", { title: "retry", content: "three" }, undefined, undefined, ctx);
      assert.equal(retryResult.isError, undefined);
      assert.equal(registrationAttempts, 2, "a later write must retry failed registration");
      assert.equal(writeRequests.length, 1);

      await memSave.execute("parallel-failure-cached", { title: "cached", content: "four" }, undefined, undefined, ctx);
      assert.equal(registrationAttempts, 2, "successful retry must remain cached");
      assert.equal(writeRequests.length, 2);
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});

test("an opaque runtime session ID stays byte-identical through registration, compaction, and cleanup", async () => {
  const originalFetch = globalThis.fetch;
  const originalUrl = process.env.ENGRAM_URL;
  process.env.ENGRAM_URL = "http://127.0.0.1:17437";
  // Pi hands out an opaque session ID. Surrounding whitespace is part of that
  // identity, so normalizing it anywhere would split registration from
  // compaction and strand the cache entry that shutdown tries to clear.
  const runtimeSessionId = "  pi-runtime-session-id  ";
  const sessionBodies = [];
  const observationBodies = [];
  globalThis.fetch = async (url, init) => {
    const path = new URL(url).pathname;
    if (path === "/health") return { ok: true, async json() { return { status: "ok" }; } };
    if (path === "/project/current") {
      return { ok: true, async json() { return { project: "pi", project_source: "git_remote", project_path: ROOT, project_strength: "strong", implicit_write_allowed: true }; } };
    }
    if (path === "/sessions") {
      sessionBodies.push(JSON.parse(init.body));
      return { ok: true, status: 201, async json() { return { status: "created" }; } };
    }
    if (path === "/observations") {
      observationBodies.push(JSON.parse(init.body));
      return { ok: true, status: 201, async json() { return { id: observationBodies.length }; } };
    }
    if (path === "/context") return { ok: true, async json() { return { context: "" }; } };
    return { ok: true, async json() { return {}; } };
  };

  try {
    await withPluginSandbox("engram-pi-contract-", async ({ sandbox }) => {
      const { registeredTools, eventHandlers } = await loadPluginHarness(sandbox);
      const memSave = registeredTools.get("mem_save");
      const ctx = runtimeContext(runtimeSessionId);
      await eventHandlers.get("session_start")({}, ctx);

      const saved = await memSave.execute("exact-1", { title: "first", content: "one" }, undefined, undefined, ctx);
      assert.equal(saved.isError, undefined);
      assert.equal(sessionBodies.length, 1, "the first write registers the runtime session once");
      assert.equal(sessionBodies[0].id, runtimeSessionId, "registration must use the exact runtime identity");
      assert.equal(observationBodies[0].session_id, runtimeSessionId, "the write must use the exact runtime identity");

      await eventHandlers.get("session_compact")({ summary: "compacted work" }, ctx);
      assert.equal(sessionBodies.length, 1, "compaction must reuse the cached exact identity instead of registering again");
      const compactionSummary = observationBodies.find((body) => body.type === "session_summary");
      assert.ok(compactionSummary, "compaction summary not forwarded");
      assert.equal(compactionSummary.session_id, runtimeSessionId, "compaction must attribute the summary to the exact identity");

      await eventHandlers.get("session_shutdown")({}, ctx);

      const afterShutdown = await memSave.execute("exact-2", { title: "second", content: "two" }, undefined, undefined, ctx);
      assert.equal(afterShutdown.isError, undefined);
      assert.equal(sessionBodies.length, 2, "shutdown must clear the cached entry so nothing is left behind");
      assert.equal(sessionBodies[1].id, runtimeSessionId, "re-registration must still use the exact runtime identity");
    });
  } finally {
    globalThis.fetch = originalFetch;
    if (originalUrl === undefined) delete process.env.ENGRAM_URL;
    else process.env.ENGRAM_URL = originalUrl;
  }
});
