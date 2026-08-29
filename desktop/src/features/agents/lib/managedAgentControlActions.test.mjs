import assert from "node:assert/strict";
import test, { beforeEach } from "node:test";

import { ingestNodeEvent, resetNodesStore } from "@/shared/api/nodesStore";
import { KIND_AGENT_ASSIGNMENT } from "@/shared/constants/kinds";
import {
  isNodeHostedAgent,
  startManagedAgentWithRules,
  respawnManagedAgentWithRules,
} from "./managedAgentControlActions.ts";

function assign(agentPubkey, state = "assigned") {
  ingestNodeEvent({
    id: `assign-${agentPubkey}-${state}`,
    kind: KIND_AGENT_ASSIGNMENT,
    pubkey: "owner1",
    created_at: 1,
    tags: [
      ["d", agentPubkey],
      ["node", "node1"],
      ["state", state],
    ],
    sig: "sig",
    content: "encrypted-marker",
  });
}

function agent(overrides = {}) {
  return {
    pubkey: "deadbeef".repeat(8),
    name: "Mesh Agent",
    personaId: null,
    relayUrl: "ws://localhost:3000",
    acpCommand: "buzz-acp",
    agentCommand: "goose",
    agentArgs: [],
    mcpCommand: "",
    turnTimeoutSeconds: 320,
    idleTimeoutSeconds: null,
    maxTurnDurationSeconds: null,
    parallelism: 1,
    systemPrompt: null,
    model: "hf://demo/model.gguf",
    envVars: {},
    status: "stopped",
    pid: null,
    createdAt: new Date(0).toISOString(),
    updatedAt: new Date(0).toISOString(),
    lastStartedAt: null,
    lastStoppedAt: null,
    lastExitCode: null,
    lastError: null,
    logPath: null,
    startOnAppLaunch: false,
    backend: { type: "local" },
    backendAgentId: null,
    respondTo: "owner-only",
    respondToAllowlist: [],
    ...overrides,
  };
}

beforeEach(() => {
  resetNodesStore();
});

// ── isNodeHostedAgent / node-hosted refusal (Phase 4 fix-round-1 Critical) ──
//
// Root cause: a node-hosted agent persists as backend:{type:"local"} —
// indistinguishable from a genuine local agent unless callers explicitly
// check nodesStore's owner-side AGENT_ASSIGNMENT desired state. These pin
// that startManagedAgentWithRules/respawnManagedAgentWithRules refuse before
// ever calling the local start/stop Tauri commands, so every UI surface that
// goes through them (avatar Start/Restart, profile panel primary action,
// bulk respawn, the sidebar's non-pair-scoped fallback) is covered for free.

test("isNodeHostedAgent is true only for a local-backend agent with an active assignment", () => {
  const localAgent = agent();
  assert.equal(isNodeHostedAgent(localAgent), false);
  assign(localAgent.pubkey, "assigned");
  assert.equal(isNodeHostedAgent(localAgent), true);

  // A provider-backend agent is never locally spawned in the first place —
  // an assignment record existing for its pubkey (shouldn't happen, but
  // defense-in-depth) must not make it "node-hosted".
  const providerAgent = agent({
    pubkey: "cafef00d".repeat(8),
    backend: { type: "provider", id: "blox", config: {} },
  });
  assign(providerAgent.pubkey, "assigned");
  assert.equal(isNodeHostedAgent(providerAgent), false);
});

test("unassigning clears the node-hosted gate", () => {
  const localAgent = agent();
  assign(localAgent.pubkey, "assigned");
  assert.equal(isNodeHostedAgent(localAgent), true);
  assign(localAgent.pubkey, "unassigned");
  assert.equal(isNodeHostedAgent(localAgent), false);
});

test("startManagedAgentWithRules refuses a node-hosted agent without touching the local start command", async () => {
  const hostedAgent = agent();
  assign(hostedAgent.pubkey, "assigned");
  let called = false;

  await assert.rejects(
    startManagedAgentWithRules({
      agent: hostedAgent,
      startManagedAgent: async () => {
        called = true;
      },
    }),
    /runs on an execution node/,
  );
  assert.equal(called, false, "the local start command must never fire");
});

test("respawnManagedAgentWithRules refuses a node-hosted agent before stopping or starting", async () => {
  const hostedAgent = agent({ status: "running" });
  assign(hostedAgent.pubkey, "assigned");
  let stopCalled = false;
  let startCalled = false;

  await assert.rejects(
    respawnManagedAgentWithRules({
      agent: hostedAgent,
      stopManagedAgent: async () => {
        stopCalled = true;
      },
      startManagedAgent: async () => {
        startCalled = true;
      },
    }),
    /runs on an execution node/,
  );
  assert.equal(stopCalled, false);
  assert.equal(startCalled, false);
});

test("relay-mesh agents delegate start to the backend preflight", async () => {
  const meshAgent = agent({
    envVars: {
      BUZZ_AGENT_PROVIDER: "openai",
      OPENAI_COMPAT_BASE_URL: "http://127.0.0.1:9337/v1/",
    },
  });

  let calledWith = null;
  await startManagedAgentWithRules({
    agent: meshAgent,
    startManagedAgent: async (pubkey) => {
      calledWith = pubkey;
    },
  });
  assert.equal(calledWith, meshAgent.pubkey);

  // Backend preflight failures (e.g. no live serve target) propagate as-is.
  await assert.rejects(
    startManagedAgentWithRules({
      agent: meshAgent,
      startManagedAgent: async () => {
        throw new Error("no live serve target is available for this model");
      },
    }),
    /no live serve target/,
  );
});

test("ordinary local agents still start normally", async () => {
  let calledWith = null;
  await startManagedAgentWithRules({
    agent: agent(),
    startManagedAgent: async (pubkey) => {
      calledWith = pubkey;
    },
  });
  assert.equal(calledWith, "deadbeef".repeat(8));
});

// --- respawnManagedAgentWithRules: stop→clear→start boundary tests -----------

test("test_respawn_stop_success_start_failure_onStopped_still_fires", async () => {
  // Prove: onStopped fires at the stop-success boundary even when start later
  // throws.  This is the key discriminator: on round-1 code the clear only
  // ran after the full respawn, so a failed start left the badge intact.
  const runningAgent = agent({ status: "running" });
  let onStoppedFired = false;

  await assert.rejects(
    respawnManagedAgentWithRules({
      agent: runningAgent,
      stopManagedAgent: async () => {
        /* stop succeeds */
      },
      startManagedAgent: async () => {
        throw new Error("start failed");
      },
      onStopped: () => {
        onStoppedFired = true;
      },
    }),
    /start failed/,
  );

  assert.ok(
    onStoppedFired,
    "onStopped must fire at stop-success boundary even when start subsequently fails",
  );
});

test("test_respawn_stop_failure_onStopped_not_called", async () => {
  // Prove: onStopped does NOT fire when stop itself throws.  Clearing on a
  // failed stop would remove a badge that is still legitimately active.
  const runningAgent = agent({ status: "running" });
  let onStoppedFired = false;

  await assert.rejects(
    respawnManagedAgentWithRules({
      agent: runningAgent,
      stopManagedAgent: async () => {
        throw new Error("stop failed");
      },
      startManagedAgent: async () => {
        /* should not be reached */
      },
      onStopped: () => {
        onStoppedFired = true;
      },
    }),
    /stop failed/,
  );

  assert.ok(
    !onStoppedFired,
    "onStopped must NOT fire when stop itself fails — badge is still active",
  );
});

test("test_respawn_onStopped_fires_before_start_resolves", async () => {
  // Prove: onStopped fires strictly between stop resolution and start
  // invocation.  A clear that fires after start begins can tombstone genuine
  // new turns from the freshly spawned process.
  const runningAgent = agent({ status: "running" });
  const events = [];

  await respawnManagedAgentWithRules({
    agent: runningAgent,
    stopManagedAgent: async () => {
      events.push("stop");
    },
    startManagedAgent: async () => {
      events.push("start");
    },
    onStopped: () => {
      events.push("onStopped");
    },
  });

  assert.deepEqual(
    events,
    ["stop", "onStopped", "start"],
    "onStopped must fire after stop resolves and before start is called",
  );
});
