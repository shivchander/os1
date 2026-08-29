import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";

import {
  getAgentStatus,
  getNodesSnapshot,
  ingestNodeEvent,
  resetNodesStore,
  subscribeNodes,
} from "./nodesStore.ts";
import {
  KIND_AGENT_NODE_STATUS,
  KIND_NODE_ANNOUNCE,
  KIND_PRESENCE_UPDATE,
} from "@/shared/constants/kinds";

function announceEvent(overrides = {}) {
  const nodePubkey = overrides.nodePubkey ?? "n1";
  return {
    id: "announce-1",
    kind: KIND_NODE_ANNOUNCE,
    pubkey: nodePubkey,
    created_at: 1,
    tags: [["d", nodePubkey]],
    sig: "sig",
    content: JSON.stringify({
      format: "buzz-node-v1",
      version: 1,
      node_pubkey: nodePubkey,
      os: overrides.os ?? "macos",
      runtimes: overrides.runtimes ?? ["claude"],
      workspace_root: overrides.workspaceRoot ?? "/x",
    }),
  };
}

function statusEvent(overrides = {}) {
  const nodePubkey = overrides.nodePubkey ?? "n1";
  const agentPubkey = overrides.agentPubkey ?? "a1";
  return {
    id: "status-1",
    kind: KIND_AGENT_NODE_STATUS,
    pubkey: nodePubkey,
    created_at: 2,
    tags: [["d", agentPubkey]],
    sig: "sig",
    content: JSON.stringify({
      // NOTE: buzz-core's AgentNodeStatus format discriminator is
      // "buzz-node-status-v1" — distinct from NodeCapabilities'
      // "buzz-node-v1" (see crates/buzz-core/src/node_status.rs FORMAT).
      format: "buzz-node-status-v1",
      version: 1,
      agent_pubkey: agentPubkey,
      node_pubkey: nodePubkey,
      health: overrides.health ?? "running",
      reason: overrides.reason,
      updated_at: "2026-08-29T00:00:00Z",
    }),
  };
}

function presenceEvent(pubkey, status) {
  return {
    id: `presence-${pubkey}-${status}`,
    kind: KIND_PRESENCE_UPDATE,
    pubkey,
    created_at: 3,
    tags: [],
    sig: "sig",
    content: status,
  };
}

describe("nodesStore", () => {
  beforeEach(() => {
    resetNodesStore();
  });

  it("projects an announce into a NodeView", () => {
    ingestNodeEvent(
      announceEvent({ os: "macos", runtimes: ["claude"], nodePubkey: "n1" }),
    );
    const nodes = getNodesSnapshot();
    assert.equal(nodes.length, 1);
    assert.equal(nodes[0].nodePubkey, "n1");
    assert.equal(nodes[0].os, "macos");
    assert.deepEqual(nodes[0].runtimes, ["claude"]);
    assert.equal(typeof nodes[0].name, "string");
    assert.ok(nodes[0].name.length > 0);
    // No presence observed yet: defaults to offline.
    assert.equal(nodes[0].online, false);
    assert.equal(nodes[0].agentCount, 0);
  });

  it("tracks per-agent status", () => {
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    ingestNodeEvent(
      statusEvent({ nodePubkey: "n1", agentPubkey: "a1", health: "running" }),
    );
    assert.deepEqual(getAgentStatus("a1"), {
      agentPubkey: "a1",
      nodePubkey: "n1",
      health: "running",
      reason: undefined,
    });
  });

  it("counts only running agents toward a node's agentCount", () => {
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    ingestNodeEvent(
      statusEvent({ nodePubkey: "n1", agentPubkey: "a1", health: "running" }),
    );
    ingestNodeEvent(
      statusEvent({ nodePubkey: "n1", agentPubkey: "a2", health: "stopped" }),
    );
    ingestNodeEvent(
      statusEvent({ nodePubkey: "n1", agentPubkey: "a3", health: "running" }),
    );
    const [node] = getNodesSnapshot();
    assert.equal(node.agentCount, 2);
  });

  it("derives online from presence, independent of arrival order", () => {
    // Presence arrives before the announce — the roster must still reflect
    // it once the node is known, since online is derived at read time.
    ingestNodeEvent(presenceEvent("n1", "online"));
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    assert.equal(getNodesSnapshot()[0].online, true);

    ingestNodeEvent(presenceEvent("n1", "offline"));
    assert.equal(getNodesSnapshot()[0].online, false);
  });

  it("ignores presence for pubkeys that are not (yet) announced nodes", () => {
    ingestNodeEvent(presenceEvent("someone-else", "online"));
    assert.equal(getNodesSnapshot().length, 0);
  });

  it("drops an announce whose author does not match its claimed node_pubkey", () => {
    const forged = announceEvent({ nodePubkey: "n1" });
    forged.pubkey = "attacker";
    ingestNodeEvent(forged);
    assert.equal(getNodesSnapshot().length, 0);
  });

  it("drops a status event whose author does not match its claimed node_pubkey", () => {
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    const forged = statusEvent({ nodePubkey: "n1", agentPubkey: "a1" });
    forged.pubkey = "attacker";
    ingestNodeEvent(forged);
    assert.equal(getAgentStatus("a1"), undefined);
  });

  it("drops malformed JSON and format-mismatched payloads without throwing", () => {
    assert.doesNotThrow(() => {
      ingestNodeEvent({
        id: "bad-1",
        kind: KIND_NODE_ANNOUNCE,
        pubkey: "n1",
        created_at: 1,
        tags: [],
        sig: "sig",
        content: "not json",
      });
      ingestNodeEvent({
        id: "bad-2",
        kind: KIND_NODE_ANNOUNCE,
        pubkey: "n1",
        created_at: 1,
        tags: [],
        sig: "sig",
        content: JSON.stringify({ format: "wrong-format-v1" }),
      });
    });
    assert.equal(getNodesSnapshot().length, 0);
  });

  it("notifies subscribers on ingest", () => {
    let notifications = 0;
    const unsubscribe = subscribeNodes(() => {
      notifications += 1;
    });
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    assert.equal(notifications, 1);
    unsubscribe();
    ingestNodeEvent(announceEvent({ nodePubkey: "n2" }));
    assert.equal(notifications, 1, "unsubscribed listener must not fire again");
  });

  it("reset clears nodes, statuses, and presence", () => {
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    ingestNodeEvent(statusEvent({ nodePubkey: "n1", agentPubkey: "a1" }));
    ingestNodeEvent(presenceEvent("n1", "online"));
    resetNodesStore();
    assert.equal(getNodesSnapshot().length, 0);
    assert.equal(getAgentStatus("a1"), undefined);

    // Re-ingesting the same announce after reset must show offline again —
    // proves presence state was actually cleared, not just the node map.
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    assert.equal(getNodesSnapshot()[0].online, false);
  });
});
