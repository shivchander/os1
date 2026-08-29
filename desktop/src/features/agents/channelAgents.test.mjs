import assert from "node:assert/strict";
import test, { afterEach, beforeEach } from "node:test";

import { ingestNodeEvent, resetNodesStore } from "@/shared/api/nodesStore";
import { KIND_AGENT_ASSIGNMENT } from "@/shared/constants/kinds";
import { attachManagedAgentToChannel } from "./channelAgents.ts";

// ── Fixtures ─────────────────────────────────────────────────────────────

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

function rawManagedAgent(pubkey, overrides = {}) {
  return {
    pubkey,
    name: "Mesh Agent",
    persona_id: null,
    runtime: null,
    team_id: null,
    relay_url: "ws://localhost:3000",
    acp_command: "buzz-acp",
    agent_command: "goose",
    agent_command_override: null,
    agent_args: [],
    mcp_command: "",
    turn_timeout_seconds: 320,
    idle_timeout_seconds: null,
    max_turn_duration_seconds: null,
    parallelism: 1,
    system_prompt: null,
    avatar_url: null,
    model: "hf://demo/model.gguf",
    model_source: null,
    provider: null,
    persona_out_of_date: false,
    persona_orphaned: false,
    needs_restart: false,
    restart_diff: [],
    env_vars: {},
    status: "running",
    pid: 123,
    created_at: new Date(0).toISOString(),
    updated_at: new Date(0).toISOString(),
    last_started_at: null,
    last_stopped_at: null,
    last_exit_code: null,
    last_error: null,
    last_error_code: null,
    log_path: null,
    start_on_app_launch: false,
    auto_restart_on_config_change: true,
    backend: { type: "local" },
    backend_agent_id: null,
    respond_to: "owner-only",
    respond_to_allowlist: [],
    ...overrides,
  };
}

// ── Tauri boundary mock ──────────────────────────────────────────────────
//
// `attachManagedAgentToChannel` calls `addChannelMembers`/`startManagedAgent`
// directly (no DI seam) — both route through `@tauri-apps/api/core`'s
// `invoke`, which reads `window.__TAURI_INTERNALS__.invoke` at call time
// (see useArchiveSync.test.mjs / channelHeadCache.test.mjs for the same
// technique). Installing that global here is the smallest way to observe
// exactly which Tauri commands a call issues, in order.

let invokedCommands;
let invokeImpl;

beforeEach(() => {
  resetNodesStore();
  invokedCommands = [];
  invokeImpl = null;
  globalThis.window = {
    __TAURI_INTERNALS__: {
      invoke: async (command, args) => {
        invokedCommands.push(command);
        if (!invokeImpl) {
          throw new Error(`unexpected Tauri command: ${command}`);
        }
        return invokeImpl(command, args);
      },
    },
  };
});

afterEach(() => {
  delete globalThis.window;
});

// ── attachManagedAgentToChannel / node-hosted skip (Phase 4 fix-round-3) ──
//
// Root cause: this function does two things — attach-to-channel (membership)
// and ensure-running (local start) — with zero node-hosted awareness. It is
// reached by 3 callers (useCreatedAgentChannelAttachment's
// presentCreatedAgent, MembersSidebar's "add existing member" flow, and
// ensureChannelAgentPresetInChannel's preset provisioning), and the first
// fires deterministically whenever an agent is created with both a node
// target AND a channel context — this is the core feature path the whole
// node-hosting safety review gates. Membership must still be added (the
// node-hosted agent is a real channel member; the node's own copy
// participates over the relay) — only the local spawn is skipped.

test("attachManagedAgentToChannel adds membership but skips the local start for a node-hosted agent", async () => {
  const hostedAgent = agent({ status: "stopped" });
  assign(hostedAgent.pubkey, "assigned");
  invokeImpl = (command, args) => {
    if (command === "add_channel_members") {
      return { added: args.pubkeys, errors: [] };
    }
    throw new Error(`unexpected command: ${command}`);
  };

  const result = await attachManagedAgentToChannel("channel-1", {
    agent: hostedAgent,
    ensureRunning: true,
  });

  assert.deepEqual(invokedCommands, ["add_channel_members"]);
  assert.equal(result.membershipAdded, true);
  assert.equal(result.started, false);
  assert.equal(result.agent, hostedAgent);
});

test("attachManagedAgentToChannel still starts a non-hosted local agent that is not running", async () => {
  const localAgent = agent({ status: "stopped" });
  invokeImpl = (command, args) => {
    if (command === "add_channel_members") {
      return { added: args.pubkeys, errors: [] };
    }
    if (command === "start_managed_agent") {
      assert.equal(args.pubkey, localAgent.pubkey);
      return rawManagedAgent(localAgent.pubkey, { status: "running" });
    }
    throw new Error(`unexpected command: ${command}`);
  };

  const result = await attachManagedAgentToChannel("channel-1", {
    agent: localAgent,
    ensureRunning: true,
  });

  assert.deepEqual(invokedCommands, [
    "add_channel_members",
    "start_managed_agent",
  ]);
  assert.equal(result.started, true);
  assert.equal(result.agent.status, "running");
});

test("attachManagedAgentToChannel does not restart an already-running non-hosted agent", async () => {
  const runningAgent = agent({ status: "running" });
  invokeImpl = (command, args) => {
    if (command === "add_channel_members") {
      return { added: args.pubkeys, errors: [] };
    }
    throw new Error(`unexpected command: ${command}`);
  };

  const result = await attachManagedAgentToChannel("channel-1", {
    agent: runningAgent,
    ensureRunning: true,
  });

  assert.deepEqual(invokedCommands, ["add_channel_members"]);
  assert.equal(result.started, false);
});

test("attachManagedAgentToChannel unassigning restores the local-start behavior", async () => {
  const formerlyHostedAgent = agent({ status: "stopped" });
  assign(formerlyHostedAgent.pubkey, "assigned");
  assign(formerlyHostedAgent.pubkey, "unassigned");
  invokeImpl = (command, args) => {
    if (command === "add_channel_members") {
      return { added: args.pubkeys, errors: [] };
    }
    if (command === "start_managed_agent") {
      return rawManagedAgent(formerlyHostedAgent.pubkey, { status: "running" });
    }
    throw new Error(`unexpected command: ${command}`);
  };

  const result = await attachManagedAgentToChannel("channel-1", {
    agent: formerlyHostedAgent,
    ensureRunning: true,
  });

  assert.deepEqual(invokedCommands, [
    "add_channel_members",
    "start_managed_agent",
  ]);
  assert.equal(result.started, true);
});
