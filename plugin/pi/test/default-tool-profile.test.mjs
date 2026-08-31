import assert from "node:assert/strict";
import { test } from "node:test";
import { importPluginFromSandbox, withPluginSandbox } from "./plugin-sandbox.mjs";

test("public runtime registers exactly the five default Terminal Memory tools", async () => {
  const previousProfile = process.env.ENGRAM_PI_TOOL_PROFILE;
  delete process.env.ENGRAM_PI_TOOL_PROFILE;

  try {
    await withPluginSandbox("engram-pi-default-profile-", async ({ sandbox }) => {
      const registerEngram = await importPluginFromSandbox(sandbox);
      const registeredTools = [];

      registerEngram({
        registerTool(tool) {
          registeredTools.push(tool.name);
        },
        on() {},
      });

      assert.deepEqual(registeredTools, [
        "mem_current_project",
        "mem_search",
        "mem_get_observation",
        "mem_checkpoint",
        "mem_checkpoint_status",
      ]);
    });
  } finally {
    if (previousProfile === undefined) delete process.env.ENGRAM_PI_TOOL_PROFILE;
    else process.env.ENGRAM_PI_TOOL_PROFILE = previousProfile;
  }
});
