import assert from "node:assert/strict";
import test from "node:test";

import {
  adapterCommandForRuntimeId,
  nodeAdvertisesRuntime,
  nodeRuntimeForCreate,
} from "./runtimeAdapterCommand.ts";

// ── adapterCommandForRuntimeId ──────────────────────────────────────────────
//
// MIRRORS the first `commands` entry of each runtime in the Rust
// KNOWN_ACP_RUNTIMES table (discovery.rs:86-205). These assertions pin the
// mapping so a drift from the Rust side is caught here.

test("adapterCommandForRuntimeId mirrors the KNOWN_ACP_RUNTIMES commands", () => {
  assert.equal(adapterCommandForRuntimeId("codex"), "codex-acp");
  assert.equal(adapterCommandForRuntimeId("claude"), "claude-agent-acp");
  assert.equal(adapterCommandForRuntimeId("goose"), "goose");
  assert.equal(adapterCommandForRuntimeId("buzz-agent"), "buzz-agent");
});

test("adapterCommandForRuntimeId trims and rejects unknown ids", () => {
  assert.equal(adapterCommandForRuntimeId("  codex  "), "codex-acp");
  assert.equal(adapterCommandForRuntimeId("custom-harness"), null);
  assert.equal(adapterCommandForRuntimeId(""), null);
});

// ── nodeAdvertisesRuntime ───────────────────────────────────────────────────

test("nodeAdvertisesRuntime matches exact ids and the legacy acp wildcard", () => {
  assert.equal(nodeAdvertisesRuntime(["codex", "goose"], "codex"), true);
  assert.equal(nodeAdvertisesRuntime(["codex", "goose"], "claude"), false);
  // Legacy "acp" wildcard host matches anything.
  assert.equal(nodeAdvertisesRuntime(["acp"], "claude"), true);
  assert.equal(nodeAdvertisesRuntime([], "codex"), false);
});

// ── nodeRuntimeForCreate ────────────────────────────────────────────────────

function catalogEntry(overrides = {}) {
  return {
    id: "codex",
    label: "Codex",
    avatarUrl: "https://runtime/codex.png",
    // Adapter missing locally: command is null, binaryPath is null.
    availability: "adapter_missing",
    command: null,
    binaryPath: null,
    defaultArgs: [],
    mcpCommand: "buzz-dev-mcp",
    modelEnvVar: null,
    providerEnvVar: null,
    thinkingEnvVar: null,
    maxTokensEnvVar: null,
    contextLimitEnvVar: null,
    maxRoundsEnvVar: null,
    installHint: "",
    installInstructionsUrl: "",
    canAutoInstall: false,
    requiresExternalCli: true,
    underlyingCliPath: null,
    nodeRequired: false,
    authStatus: { status: "not_applicable" },
    loginHint: null,
    source: "preset",
    ...overrides,
  };
}

const nodes = [{ nodePubkey: "node-abc", runtimes: ["codex", "goose"] }];

test("nodeRuntimeForCreate synthesizes an available runtime the node advertises", () => {
  const runtime = nodeRuntimeForCreate({
    runtimeId: "codex",
    nodePubkey: "node-abc",
    nodes,
    catalogEntries: [catalogEntry()],
  });
  assert.ok(runtime);
  assert.equal(runtime.availability, "available");
  // Command comes from the adapter map even though the catalog command is null.
  assert.equal(runtime.command, "codex-acp");
  assert.equal(runtime.binaryPath, "");
  // Catalog metadata is preserved.
  assert.equal(runtime.mcpCommand, "buzz-dev-mcp");
  assert.equal(runtime.id, "codex");
});

test("nodeRuntimeForCreate honours the legacy acp wildcard node", () => {
  const runtime = nodeRuntimeForCreate({
    runtimeId: "codex",
    nodePubkey: "node-any",
    nodes: [{ nodePubkey: "node-any", runtimes: ["acp"] }],
    catalogEntries: [catalogEntry()],
  });
  assert.ok(runtime);
  assert.equal(runtime.command, "codex-acp");
});

test("nodeRuntimeForCreate refuses unknown ids, absent nodes, and unadvertised runtimes", () => {
  // Unknown adapter id.
  assert.equal(
    nodeRuntimeForCreate({
      runtimeId: "custom",
      nodePubkey: "node-abc",
      nodes,
      catalogEntries: [catalogEntry({ id: "custom" })],
    }),
    null,
  );
  // Node not in the roster.
  assert.equal(
    nodeRuntimeForCreate({
      runtimeId: "codex",
      nodePubkey: "missing-node",
      nodes,
      catalogEntries: [catalogEntry()],
    }),
    null,
  );
  // Node does not advertise the runtime.
  assert.equal(
    nodeRuntimeForCreate({
      runtimeId: "claude",
      nodePubkey: "node-abc",
      nodes,
      catalogEntries: [catalogEntry({ id: "claude" })],
    }),
    null,
  );
  // No local catalog entry to base metadata on.
  assert.equal(
    nodeRuntimeForCreate({
      runtimeId: "codex",
      nodePubkey: "node-abc",
      nodes,
      catalogEntries: [],
    }),
    null,
  );
});
