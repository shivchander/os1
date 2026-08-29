import { relayClient } from "@/shared/api/relayClient";
import type { RelayEvent } from "@/shared/api/types";
import {
  KIND_AGENT_NODE_STATUS,
  KIND_NODE_ANNOUNCE,
  KIND_PRESENCE_UPDATE,
} from "@/shared/constants/kinds";
import { normalizePubkey, truncatePubkey } from "@/shared/lib/pubkey";

/**
 * Community-scoped live store projecting the execution-node roster from
 * plain (unencrypted) relay events: `NODE_ANNOUNCE` (capabilities),
 * `AGENT_NODE_STATUS` (per-agent health as observed by its node), and
 * presence (kind:20001 — online/offline liveness for any pubkey, including a
 * node's own).
 *
 * Shape mirrors `observerRelayStore.ts`'s listener-Set/notify/reset pattern
 * (module-singleton Maps, a `Set<listener>`, a cached-and-invalidated
 * snapshot, `resetXStore()`). It does NOT reuse observerRelayStore's decrypt
 * + owner-scoped-`#p` + per-agent-eviction machinery: that machinery exists
 * because agent telemetry is per-agent-encrypted and can be high-volume.
 * Node/status/presence events are plaintext and community-wide (small,
 * roster-sized), so the live subscription is wired directly through
 * `relayClient.subscribeLive`, the same primitive `useTeamCatalogRelay.ts`
 * uses for a community-wide catalog.
 */

// Wire-format discriminators, mirrored from buzz-core (crates/buzz-core/src/
// node.rs FORMAT, node_status.rs FORMAT). Deliberately distinct strings —
// status is its own schema, not a NodeCapabilities variant.
const NODE_ANNOUNCE_FORMAT = "buzz-node-v1";
const AGENT_NODE_STATUS_FORMAT = "buzz-node-status-v1";

// Historical backfill depth for the combined announce/status/presence
// subscription. NODE_ANNOUNCE and AGENT_NODE_STATUS are addressable
// (parameterized-replaceable: at most one stored event per author+d-tag), so
// this bounds total roster size (nodes + agents), not a message-volume
// window. Presence is ephemeral and is never included in relay backfill
// regardless of `limit` — only live presence updates arrive.
const NODES_LIVE_SUBSCRIPTION_LIMIT = 500;

export type NodeView = {
  nodePubkey: string;
  /**
   * Display label. `NodeCapabilities` (buzz-core) has no device-name field
   * yet, so this is a truncated pubkey via the canonical `truncatePubkey`
   * helper — never hand-rolled (see `scripts/check-pubkey-truncation.mjs`).
   */
  name: string;
  os: string;
  runtimes: string[];
  online: boolean;
  agentCount: number;
};

export type AgentStatusView = {
  agentPubkey: string;
  nodePubkey: string;
  health: string;
  reason?: string;
};

type NodeAnnounceContent = {
  node_pubkey: string;
  os: string;
  runtimes: string[];
  workspace_root: string;
  max_agents?: number;
};

type PresenceContent = "online" | "away" | "offline";

const nodesByPubkey = new Map<string, NodeAnnounceContent>();
const statusByAgent = new Map<string, AgentStatusView>();
const presenceByPubkey = new Map<string, PresenceContent>();
const listeners = new Set<() => void>();

// Cached snapshot so `useSyncExternalStore` gets a referentially stable
// array between changes; invalidated (set to null) on every ingest/reset and
// lazily rebuilt on next read.
let cachedNodes: NodeView[] | null = null;

let unsubscribeRelay: (() => Promise<void>) | null = null;
let startPromise: Promise<void> | null = null;
// Bumped on every reset so an in-flight subscribe/callback from a prior
// community can never write into the next community's store (mirrors
// observerRelayStore's `generation` guard).
let generation = 0;

function notifyListeners() {
  for (const listener of listeners) {
    listener();
  }
}

function invalidateSnapshot() {
  cachedNodes = null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseJson(content: string): unknown {
  try {
    return JSON.parse(content);
  } catch {
    return null;
  }
}

function parseNodeAnnounce(content: string): NodeAnnounceContent | null {
  const value = parseJson(content);
  if (!isRecord(value) || value.format !== NODE_ANNOUNCE_FORMAT) {
    return null;
  }
  if (
    typeof value.node_pubkey !== "string" ||
    typeof value.os !== "string" ||
    !Array.isArray(value.runtimes) ||
    typeof value.workspace_root !== "string"
  ) {
    return null;
  }
  return {
    node_pubkey: value.node_pubkey,
    os: value.os,
    runtimes: value.runtimes.filter(
      (runtime): runtime is string => typeof runtime === "string",
    ),
    workspace_root: value.workspace_root,
    max_agents:
      typeof value.max_agents === "number" ? value.max_agents : undefined,
  };
}

function parseAgentNodeStatus(content: string): AgentStatusView | null {
  const value = parseJson(content);
  if (!isRecord(value) || value.format !== AGENT_NODE_STATUS_FORMAT) {
    return null;
  }
  if (
    typeof value.agent_pubkey !== "string" ||
    typeof value.node_pubkey !== "string" ||
    typeof value.health !== "string"
  ) {
    return null;
  }
  return {
    agentPubkey: value.agent_pubkey,
    nodePubkey: value.node_pubkey,
    health: value.health,
    reason: typeof value.reason === "string" ? value.reason : undefined,
  };
}

function parsePresence(content: string): PresenceContent | null {
  return content === "online" || content === "away" || content === "offline"
    ? content
    : null;
}

/**
 * Ingest one raw relay event into the store. Exported for tests and for the
 * E2E mock bridge (`__BUZZ_E2E_SEED_NODE_EVENTS__`), which calls this
 * directly to exercise the real reducer rather than stubbing the panel.
 *
 * Unrecognized kinds, malformed payloads, and author-mismatched events are
 * silently dropped — this store has no error-surfacing UI, and a hostile or
 * buggy relay must not be able to crash the roster.
 */
export function ingestNodeEvent(event: RelayEvent): void {
  switch (event.kind) {
    case KIND_NODE_ANNOUNCE: {
      const caps = parseNodeAnnounce(event.content);
      if (!caps) return;
      // Defense-in-depth: the node must sign its own announce (mirrors
      // buzz-core's validate_announce author-binding check). The relay
      // already enforces this, but a compromised relay could misattribute
      // capabilities to a different node pubkey.
      if (normalizePubkey(event.pubkey) !== normalizePubkey(caps.node_pubkey)) {
        return;
      }
      nodesByPubkey.set(normalizePubkey(caps.node_pubkey), caps);
      invalidateSnapshot();
      notifyListeners();
      return;
    }
    case KIND_AGENT_NODE_STATUS: {
      const status = parseAgentNodeStatus(event.content);
      if (!status) return;
      // Defense-in-depth: the reporting node must sign its own status
      // (mirrors buzz-core's validate_status author-binding check).
      if (
        normalizePubkey(event.pubkey) !== normalizePubkey(status.nodePubkey)
      ) {
        return;
      }
      statusByAgent.set(normalizePubkey(status.agentPubkey), status);
      invalidateSnapshot();
      notifyListeners();
      return;
    }
    case KIND_PRESENCE_UPDATE: {
      // Presence is self-signed by its author (the subject is always
      // event.pubkey, never a `p` tag — see features/presence/lib/presence.ts
      // for the same rule on the human-presence path). A node publishes its
      // own presence, so this needs no separate author check.
      const status = parsePresence(event.content);
      if (!status) return;
      presenceByPubkey.set(normalizePubkey(event.pubkey), status);
      invalidateSnapshot();
      notifyListeners();
      return;
    }
    default:
      return;
  }
}

function buildNodeView(caps: NodeAnnounceContent): NodeView {
  const key = normalizePubkey(caps.node_pubkey);
  let agentCount = 0;
  for (const status of statusByAgent.values()) {
    // "Running-agent count": agents this node is actively hosting right now.
    // Starting/stopped/crashed/unschedulable agents are tracked (via
    // getAgentStatus) but not counted here.
    if (
      normalizePubkey(status.nodePubkey) === key &&
      status.health === "running"
    ) {
      agentCount += 1;
    }
  }
  return {
    nodePubkey: caps.node_pubkey,
    name: truncatePubkey(caps.node_pubkey),
    os: caps.os,
    runtimes: caps.runtimes,
    online: presenceByPubkey.get(key) === "online",
    agentCount,
  };
}

export function getNodesSnapshot(): NodeView[] {
  if (cachedNodes) {
    return cachedNodes;
  }
  const nodes = Array.from(nodesByPubkey.values(), buildNodeView);
  cachedNodes = nodes;
  return nodes;
}

export function getAgentStatus(
  agentPubkey: string,
): AgentStatusView | undefined {
  return statusByAgent.get(normalizePubkey(agentPubkey));
}

export function subscribeNodes(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/**
 * Open the live subscription feeding this store (NODE_ANNOUNCE +
 * AGENT_NODE_STATUS + presence) if one isn't already open. Idempotent and
 * safe to call from multiple mounted consumers (mirrors
 * `ensureRelayObserverSubscription`). Errors are logged, not thrown — the
 * panel should still render whatever the store already has.
 */
export function ensureNodesRelaySubscription(): Promise<void> {
  if (unsubscribeRelay) {
    return Promise.resolve();
  }
  if (startPromise) {
    return startPromise;
  }

  const activeGeneration = generation;
  startPromise = relayClient
    .subscribeLive(
      {
        kinds: [
          KIND_NODE_ANNOUNCE,
          KIND_AGENT_NODE_STATUS,
          KIND_PRESENCE_UPDATE,
        ],
        limit: NODES_LIVE_SUBSCRIPTION_LIMIT,
      },
      (event) => {
        if (activeGeneration !== generation) return;
        ingestNodeEvent(event);
      },
    )
    .then((unsubscribe) => {
      if (activeGeneration !== generation) {
        void unsubscribe();
        return;
      }
      unsubscribeRelay = unsubscribe;
    })
    .catch((error) => {
      console.error("Failed to subscribe to the execution-node roster:", error);
    })
    .finally(() => {
      if (activeGeneration === generation) {
        startPromise = null;
      }
    });

  return startPromise;
}

/** Tear down the module-level store. Call from `resetCommunityState()`. */
export function resetNodesStore(): void {
  generation += 1;
  const unsubscribe = unsubscribeRelay;
  unsubscribeRelay = null;
  startPromise = null;
  nodesByPubkey.clear();
  statusByAgent.clear();
  presenceByPubkey.clear();
  invalidateSnapshot();
  notifyListeners();
  void unsubscribe?.();
}
