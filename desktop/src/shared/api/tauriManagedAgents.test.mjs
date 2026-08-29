import assert from "node:assert/strict";
import test, { afterEach, beforeEach } from "node:test";

import { ingestNodeEvent, resetNodesStore } from "@/shared/api/nodesStore";
import { KIND_AGENT_ASSIGNMENT } from "@/shared/constants/kinds";
import { startManagedAgent } from "./tauriManagedAgents.ts";

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

beforeEach(() => {
  resetNodesStore();
});

afterEach(() => {
  delete globalThis.window;
});

// ── startManagedAgent fail-closed backstop (Phase 4 fix-round-3) ──────────
//
// This is the lowest-level wrapper around the `start_managed_agent` Tauri
// command — 7 of the 8 known start paths route through it. Per-caller
// graceful skips (managedAgentControlActions.ts, channelAgents.ts,
// welcomeKickoff.ts, useMentionSendFlow.ts, etc.) already avoid calling it
// for a node-hosted agent, but three straight review rounds each found a new
// ungated caller — this refusal exists so any *future* ungated caller fails
// loudly (a rejected promise a test or bug report can catch) instead of
// silently double-spawning a node-hosted agent's identity/key locally.

test("startManagedAgent refuses a node-hosted agent before touching the Tauri boundary", async () => {
  const pubkey = "deadbeef".repeat(8);
  assign(pubkey, "assigned");

  // Deliberately no `window`/`__TAURI_INTERNALS__` installed at all: if the
  // guard did not fire first, this would instead reject with whatever error
  // the Tauri bridge produces when unavailable (a different message) rather
  // than the guard's own — so matching this exact message proves the
  // refusal happens strictly before any invoke is attempted.
  await assert.rejects(
    startManagedAgent(pubkey),
    /node-hosted agents must not be started locally/,
  );
});

test("startManagedAgent clears once the agent is unassigned", async () => {
  const pubkey = "deadbeef".repeat(8);
  assign(pubkey, "assigned");
  assign(pubkey, "unassigned");
  let invoked = null;
  globalThis.window = {
    __TAURI_INTERNALS__: {
      invoke: async (command, args) => {
        invoked = { command, args };
        return rawManagedAgent(pubkey);
      },
    },
  };

  const result = await startManagedAgent(pubkey);
  assert.equal(invoked.command, "start_managed_agent");
  assert.equal(invoked.args.pubkey, pubkey);
  assert.equal(result.pubkey, pubkey);
});

test("startManagedAgent proceeds to the Tauri bridge for a non-hosted agent", async () => {
  const pubkey = "cafef00d".repeat(8);
  let invoked = null;
  globalThis.window = {
    __TAURI_INTERNALS__: {
      invoke: async (command, args) => {
        invoked = { command, args };
        return rawManagedAgent(pubkey, { status: "running" });
      },
    },
  };

  const result = await startManagedAgent(pubkey, {
    expectedRelayUrl: "ws://localhost:3000",
  });
  assert.equal(invoked.command, "start_managed_agent");
  assert.equal(invoked.args.pubkey, pubkey);
  assert.equal(invoked.args.expectedRelayUrl, "ws://localhost:3000");
  assert.equal(result.status, "running");
});

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
    status: "stopped",
    pid: null,
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
