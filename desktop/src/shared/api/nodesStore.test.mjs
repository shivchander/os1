import assert from "node:assert/strict";
import { beforeEach, describe, it, mock } from "node:test";

import { relayClient } from "@/shared/api/relayClient";
import {
  KIND_AGENT_ASSIGNMENT,
  KIND_AGENT_NODE_STATUS,
  KIND_NODE_ANNOUNCE,
  KIND_PRESENCE_UPDATE,
} from "@/shared/constants/kinds";
import {
  __setIdentityResolverForTests,
  ensureNodesRelaySubscription,
  getAgentAssignment,
  getAgentStatus,
  getNodesSnapshot,
  ingestNodeEvent,
  isAgentNodeHosted,
  resetNodesStore,
  subscribeNodes,
} from "./nodesStore.ts";

// node:test's async functions always yield at least one microtask/macrotask
// per await even when the awaited value is already resolved — flush lets a
// PresenceSubscriptionReconciler reconcile loop (triggered fire-and-forget
// from ingestNodeEvent) fully settle before the next assertion or the next
// triggering event. Mirrors the same helper in
// shared/api/presenceSubscriptionReconciler.test.mjs.
const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

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

// Monotonic default so sequential assignmentEvent() calls in the same test
// get strictly increasing created_at values (matching realistic in-order
// delivery) unless a test explicitly overrides createdAt to exercise the
// LWW tiebreak with out-of-order delivery.
let nextAssignmentCreatedAt = 4;

function assignmentEvent(overrides = {}) {
  const agentPubkey = overrides.agentPubkey ?? "a1";
  const nodePubkey = overrides.nodePubkey ?? "n1";
  const state = overrides.state ?? "assigned";
  const createdAt = overrides.createdAt ?? nextAssignmentCreatedAt++;
  return {
    id: overrides.id ?? `assignment-${agentPubkey}-${state}-${createdAt}`,
    kind: KIND_AGENT_ASSIGNMENT,
    // Owner-authored, not node- or agent-authored — see
    // crates/buzz-core/src/assignment.rs's build_assignment.
    pubkey: overrides.ownerPubkey ?? "owner1",
    created_at: createdAt,
    tags: [
      ["d", agentPubkey],
      ["node", nodePubkey],
      ["state", state],
    ],
    sig: "sig",
    // Real content is NIP-44-encrypted to the node; this store never
    // decrypts it (see parseAgentAssignmentTags's doc comment) — a marker
    // string is enough to prove content is never inspected.
    content: "encrypted-marker",
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

  // ── AGENT_ASSIGNMENT: the desired-state signal that gates local-only ──────
  // lifecycle controls (isAgentNodeHosted). See the Phase-4 fix-round-1
  // review: nodesStore.getAgentStatus() alone is empty in the window between
  // publishing an assignment and the node's first AGENT_NODE_STATUS, which
  // would leave local Start/Restart live during exactly the gap that
  // matters. getAgentAssignment/isAgentNodeHosted read the OWNER's own
  // desired-state record instead, which is set the instant the assignment
  // publishes.

  it("projects an assigned AGENT_ASSIGNMENT into isAgentNodeHosted", () => {
    const event = assignmentEvent({
      agentPubkey: "a1",
      nodePubkey: "n1",
      state: "assigned",
    });
    ingestNodeEvent(event);
    assert.deepEqual(getAgentAssignment("a1"), {
      agentPubkey: "a1",
      nodePubkey: "n1",
      state: "assigned",
      createdAt: event.created_at,
    });
    assert.equal(isAgentNodeHosted("a1"), true);
  });

  it("an unassigned state is not node-hosted", () => {
    ingestNodeEvent(
      assignmentEvent({
        agentPubkey: "a1",
        nodePubkey: "n1",
        state: "assigned",
      }),
    );
    assert.equal(isAgentNodeHosted("a1"), true);
    ingestNodeEvent(
      assignmentEvent({
        agentPubkey: "a1",
        nodePubkey: "n1",
        state: "unassigned",
      }),
    );
    assert.equal(isAgentNodeHosted("a1"), false);
    assert.equal(getAgentAssignment("a1").state, "unassigned");
  });

  it("an agent with no assignment history is not node-hosted", () => {
    assert.equal(getAgentAssignment("never-assigned"), undefined);
    assert.equal(isAgentNodeHosted("never-assigned"), false);
  });

  it("never reads AGENT_ASSIGNMENT.content — only the public d/node/state tags", () => {
    // assignmentEvent()'s content is an opaque marker string, not valid JSON
    // for any known schema. If ingestion tried to parse/decrypt it, this
    // would throw or silently drop the record; it must do neither.
    assert.doesNotThrow(() => {
      ingestNodeEvent(assignmentEvent({ agentPubkey: "a1" }));
    });
    assert.equal(isAgentNodeHosted("a1"), true);
  });

  it("drops an AGENT_ASSIGNMENT missing a required public tag", () => {
    const missingState = assignmentEvent({ agentPubkey: "a1" });
    missingState.tags = missingState.tags.filter((tag) => tag[0] !== "state");
    ingestNodeEvent(missingState);
    assert.equal(getAgentAssignment("a1"), undefined);
  });

  // ── Last-writer-wins by created_at (fix-round-2 Minor) ────────────────────
  //
  // assignmentByAgent.set() must never let an out-of-order-delivered OLDER
  // event overwrite a NEWER one already applied — an older "unassigned"
  // masking a newer "assigned" would silently re-enable local Start/Restart
  // for a still-node-hosted agent, reopening the round-1 Critical.

  it("an out-of-order older 'unassigned' does not overwrite a newer 'assigned'", () => {
    ingestNodeEvent(
      assignmentEvent({ agentPubkey: "a1", state: "assigned", createdAt: 20 }),
    );
    assert.equal(isAgentNodeHosted("a1"), true);
    // Arrives LATER over the wire but claims an EARLIER wall-clock time —
    // e.g. redelivered from a slower relay connection.
    ingestNodeEvent(
      assignmentEvent({
        agentPubkey: "a1",
        state: "unassigned",
        createdAt: 10,
      }),
    );
    assert.equal(
      isAgentNodeHosted("a1"),
      true,
      "a stale unassigned must not clear an already-applied newer assigned",
    );
    assert.equal(getAgentAssignment("a1").createdAt, 20);
  });

  it("a newer 'unassigned' correctly overrides an older 'assigned' regardless of ingest order", () => {
    ingestNodeEvent(
      assignmentEvent({ agentPubkey: "a1", state: "assigned", createdAt: 10 }),
    );
    ingestNodeEvent(
      assignmentEvent({
        agentPubkey: "a1",
        state: "unassigned",
        createdAt: 20,
      }),
    );
    assert.equal(isAgentNodeHosted("a1"), false);
    assert.equal(getAgentAssignment("a1").createdAt, 20);
  });

  it("a tie (equal created_at) applies the newly-ingested event", () => {
    ingestNodeEvent(
      assignmentEvent({ agentPubkey: "a1", state: "assigned", createdAt: 15 }),
    );
    ingestNodeEvent(
      assignmentEvent({
        agentPubkey: "a1",
        state: "unassigned",
        createdAt: 15,
      }),
    );
    assert.equal(isAgentNodeHosted("a1"), false);
  });

  it("notifies subscribers AND invalidates the roster snapshot reference on an assignment change", () => {
    // Regression guard: useSyncExternalStore(subscribeNodes, getNodesSnapshot)
    // is what AgentPersonaCard/StandaloneAgentCard use to react to
    // isNodeHostedAgent changes (there is no dedicated assignment snapshot).
    // React bails out of re-rendering when getSnapshot() returns an
    // Object.is-equal value even after the subscribed listener fires — so
    // notifyListeners() alone is not sufficient; the cached array reference
    // must actually change too.
    ingestNodeEvent(announceEvent({ nodePubkey: "n1" }));
    const beforeSnapshot = getNodesSnapshot();
    ingestNodeEvent(
      assignmentEvent({
        agentPubkey: "a1",
        nodePubkey: "n1",
        state: "assigned",
      }),
    );
    const afterSnapshot = getNodesSnapshot();
    assert.notEqual(
      afterSnapshot,
      beforeSnapshot,
      "getNodesSnapshot() must return a new reference after an assignment change",
    );
  });

  it("reset also clears assignment desired-state", () => {
    ingestNodeEvent(assignmentEvent({ agentPubkey: "a1", state: "assigned" }));
    assert.equal(isAgentNodeHosted("a1"), true);
    resetNodesStore();
    assert.equal(getAgentAssignment("a1"), undefined);
    assert.equal(isAgentNodeHosted("a1"), false);
  });

  it(
    "requests presence only for announced node pubkeys, author-scoped and " +
      "growing with the roster — never as part of the unscoped roster filter",
    async () => {
      const calls = [];
      mock.method(relayClient, "subscribeLive", (filter, onEvent, onReady) => {
        calls.push({ filter, onEvent });
        // openPresenceSubscription requires EOSE readiness to resolve
        // (see shared/api/presenceRelaySubscription.ts); the roster
        // subscription doesn't pass onReady at all, so this is a no-op there.
        onReady?.("eose");
        return Promise.resolve(async () => {});
      });

      try {
        await ensureNodesRelaySubscription();

        // Only the roster subscription opens up front, and it must not carry
        // an authors scope (NODE_ANNOUNCE/AGENT_NODE_STATUS are addressable
        // and roster-sized — fine unscoped).
        assert.equal(calls.length, 1);
        assert.deepEqual(calls[0].filter.kinds, [
          KIND_NODE_ANNOUNCE,
          KIND_AGENT_NODE_STATUS,
        ]);
        assert.equal(
          "authors" in calls[0].filter,
          false,
          "the roster filter must not scope by authors",
        );
        const rosterOnEvent = calls[0].onEvent;

        // Announcing n1 (via the roster subscription's own onEvent, proving
        // the roster feed still drives this) opens a SEPARATE, author-scoped
        // presence subscription for exactly that one node.
        rosterOnEvent(announceEvent({ nodePubkey: "n1" }));
        await flush();
        let presenceCalls = calls.filter((call) =>
          call.filter.kinds?.includes(KIND_PRESENCE_UPDATE),
        );
        assert.equal(presenceCalls.length, 1);
        assert.deepEqual(presenceCalls[0].filter.authors, ["n1"]);

        // Announcing n2 reconciles onto both authors — still scoped to the
        // roster, never the whole community.
        rosterOnEvent(announceEvent({ nodePubkey: "n2" }));
        await flush();
        presenceCalls = calls.filter((call) =>
          call.filter.kinds?.includes(KIND_PRESENCE_UPDATE),
        );
        assert.equal(presenceCalls.length, 2);
        assert.deepEqual(presenceCalls.at(-1).filter.authors, ["n1", "n2"]);

        // The presence subscription's own events still update the roster via
        // the same ingestNodeEvent reducer.
        presenceCalls.at(-1).onEvent(presenceEvent("n1", "online"));
        assert.equal(
          getNodesSnapshot().find((node) => node.nodePubkey === "n1")?.online,
          true,
        );
      } finally {
        mock.reset();
      }
    },
  );

  // ── Independent-leg retry (fix-round-2 Important) ─────────────────────────
  //
  // Root bug: the original implementation marked the WHOLE subscription
  // "already open" as soon as the roster leg (which needs no identity)
  // succeeded — so once that happened, every later ensureNodesRelaySubscription()
  // call short-circuited and never retried a transiently-failed assignment
  // leg. isAgentNodeHosted would then silently stay false (gate open) for
  // the rest of the community session after one bad IPC call.

  it(
    "retries only the assignment leg after a transient identity failure, " +
      "without re-subscribing an already-succeeded roster leg",
    async () => {
      const calls = [];
      mock.method(relayClient, "subscribeLive", (filter, onEvent, onReady) => {
        calls.push({ filter, onEvent });
        onReady?.("eose");
        return Promise.resolve(async () => {});
      });
      let identityAttempts = 0;
      __setIdentityResolverForTests(async () => {
        identityAttempts += 1;
        if (identityAttempts === 1) {
          throw new Error("transient IPC error");
        }
        return { pubkey: "owner-retry" };
      });

      try {
        await ensureNodesRelaySubscription();
        const rosterCalls1 = calls.filter(
          (call) => !("authors" in call.filter),
        );
        const assignmentCalls1 = calls.filter(
          (call) => "authors" in call.filter,
        );
        assert.equal(rosterCalls1.length, 1, "roster leg subscribes once");
        assert.equal(
          assignmentCalls1.length,
          0,
          "a failed getIdentity must short-circuit before the assignment leg ever calls subscribeLive",
        );
        assert.equal(identityAttempts, 1);
        assert.equal(
          isAgentNodeHosted("a1"),
          false,
          "the gate stays closed (fails open only in the safe direction) while identity resolution is broken",
        );

        // A later call (e.g. a newly-mounted consumer, or the same one
        // re-checking) must retry ONLY the leg that failed.
        await ensureNodesRelaySubscription();
        const rosterCalls2 = calls.filter(
          (call) => !("authors" in call.filter),
        );
        const assignmentCalls2 = calls.filter(
          (call) => "authors" in call.filter,
        );
        assert.equal(
          rosterCalls2.length,
          1,
          "the roster leg must not re-subscribe once it already succeeded",
        );
        assert.equal(
          assignmentCalls2.length,
          1,
          "the assignment leg must retry after a transient identity failure",
        );
        assert.equal(identityAttempts, 2);
        assert.deepEqual(assignmentCalls2[0].filter.authors, ["owner-retry"]);

        // The retried leg is now live: a subsequently-ingested assignment
        // (via its own onEvent, proving the real feed, not a direct
        // ingestNodeEvent call) is honored. Authored by "owner-retry" — the
        // identity the second (successful) resolution returned — since the
        // defense-in-depth author check now requires it.
        assignmentCalls2[0].onEvent(
          assignmentEvent({
            agentPubkey: "a1",
            state: "assigned",
            ownerPubkey: "owner-retry",
          }),
        );
        assert.equal(isAgentNodeHosted("a1"), true);
      } finally {
        mock.reset();
        __setIdentityResolverForTests(null);
      }
    },
  );
});
