import { openPresenceSubscription } from "@/shared/api/presenceRelaySubscription";
import { PresenceSubscriptionReconciler } from "@/shared/api/presenceSubscriptionReconciler";
import { relayClient } from "@/shared/api/relayClient";
import { getIdentity } from "@/shared/api/tauriIdentity";
import type { RelayEvent } from "@/shared/api/types";
import {
  KIND_AGENT_ASSIGNMENT,
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
 * NODE_ANNOUNCE/AGENT_NODE_STATUS are plaintext and community-wide but
 * roster-sized (addressable, one event per author+d-tag), so that half of the
 * live subscription is wired directly through `relayClient.subscribeLive`,
 * the same primitive `useTeamCatalogRelay.ts` uses for a community-wide
 * catalog.
 *
 * Presence is different: `KIND_PRESENCE_UPDATE` is NOT author-gated by the
 * relay (unlike `#p`-gated kinds), so an unscoped `{kinds:[...]}` filter would
 * receive every community member's presence heartbeat — population-sized,
 * not roster-sized, and open for the whole app session. Presence is instead
 * requested through the same author-scoped machinery the human-presence path
 * uses (`openPresenceSubscription` + `PresenceSubscriptionReconciler`,
 * `shared/api/presenceRelaySubscription.ts` /
 * `shared/api/presenceSubscriptionReconciler.ts`), reconciled to exactly the
 * set of currently-announced node pubkeys as the roster grows.
 */

// Wire-format discriminators, mirrored from buzz-core (crates/buzz-core/src/
// node.rs FORMAT, node_status.rs FORMAT). Deliberately distinct strings —
// status is its own schema, not a NodeCapabilities variant.
const NODE_ANNOUNCE_FORMAT = "buzz-node-v1";
const AGENT_NODE_STATUS_FORMAT = "buzz-node-status-v1";

// Historical backfill depth for the combined announce/status subscription.
// NODE_ANNOUNCE and AGENT_NODE_STATUS are addressable (parameterized-
// replaceable: at most one stored event per author+d-tag), so this bounds
// total roster size (nodes + agents), not a message-volume window.
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

/**
 * The owner's own desired-state record for an agent, from the public tags of
 * an `AGENT_ASSIGNMENT` event (`d`=agent, `node`=target, `state`). Never the
 * NIP-44-encrypted `content` (nsec + launch contract) — this store has no
 * owner key and cannot decrypt it, and doesn't need to: per
 * `crates/buzz-core/src/assignment.rs`, "the signed outer event exposes only
 * the agent coordinate, target node, and desired lifecycle state as public
 * tags — any of the owner's nodes can decide whether they are the target
 * without decrypting anything."
 */
export type AgentAssignmentView = {
  agentPubkey: string;
  nodePubkey: string;
  state: "assigned" | "unassigned";
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
const assignmentByAgent = new Map<string, AgentAssignmentView>();
const listeners = new Set<() => void>();

// Set once ensureNodesRelaySubscription() resolves the active identity; null
// otherwise (including in pure-reducer tests that call ingestNodeEvent
// directly without ever starting a subscription — see the author check in
// the KIND_AGENT_ASSIGNMENT case below). Cleared on every resetNodesStore().
let currentOwnerPubkey: string | null = null;

// Cached snapshot so `useSyncExternalStore` gets a referentially stable
// array between changes; invalidated (set to null) on every ingest/reset and
// lazily rebuilt on next read.
let cachedNodes: NodeView[] | null = null;

let unsubscribeRelay: (() => Promise<void>) | null = null;
let startPromise: Promise<void> | null = null;
// Reconciles the author-scoped presence subscription onto the current set of
// announced node pubkeys. Created lazily by `ensureNodesRelaySubscription`
// (not eagerly at module load) so pure-reducer tests that only call
// `ingestNodeEvent` directly never spin up a live subscription. `null`
// between `resetNodesStore()` and the next `ensureNodesRelaySubscription()`.
let presenceReconciler: PresenceSubscriptionReconciler | null = null;
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

/**
 * Push the current set of announced node pubkeys to the presence reconciler
 * (if a live subscription session is active). A no-op when
 * `ensureNodesRelaySubscription` hasn't been called — e.g. pure-reducer
 * tests that ingest events directly — since there's no subscription to keep
 * in sync. `PresenceSubscriptionReconciler.setAuthors` itself no-ops when the
 * key is unchanged, so calling this on every announce is cheap.
 */
function reconcilePresenceAuthors(): void {
  presenceReconciler?.setAuthors([...nodesByPubkey.keys()]);
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

function findTagValue(tags: readonly string[][], name: string): string | null {
  for (const tag of tags) {
    if (tag[0] === name) return tag[1] ?? null;
  }
  return null;
}

/**
 * Read an `AGENT_ASSIGNMENT`'s public tags only — `d` (agent), `node`
 * (target), `state` (`assigned`|`unassigned`). Mirrors buzz-core's
 * `validate_envelope` tag set exactly (that function additionally rejects
 * unexpected tags and duplicates; this store, like its announce/status
 * parsers, just drops anything it can't make sense of rather than treating a
 * malformed event as fatal).
 */
function parseAgentAssignmentTags(
  event: RelayEvent,
): AgentAssignmentView | null {
  const agentPubkey = findTagValue(event.tags, "d");
  const nodePubkey = findTagValue(event.tags, "node");
  const state = findTagValue(event.tags, "state");
  if (
    !agentPubkey ||
    !nodePubkey ||
    (state !== "assigned" && state !== "unassigned")
  ) {
    return null;
  }
  return {
    agentPubkey: normalizePubkey(agentPubkey),
    nodePubkey: normalizePubkey(nodePubkey),
    state,
  };
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
      // Keep the author-scoped presence subscription in sync with the
      // roster: a newly-announced node's liveness must start being tracked.
      reconcilePresenceAuthors();
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
      // In production this only ever arrives via the author-scoped presence
      // subscription reconciled onto the known node pubkeys (see
      // `ensureNodesRelaySubscription`/`reconcilePresenceAuthors`) — never
      // through the broad roster filter, which would receive every
      // community member's presence. Presence is self-signed by its author
      // (the subject is always event.pubkey, never a `p` tag — see
      // features/presence/lib/presence.ts for the same rule on the
      // human-presence path), so this needs no separate author check.
      const status = parsePresence(event.content);
      if (!status) return;
      presenceByPubkey.set(normalizePubkey(event.pubkey), status);
      invalidateSnapshot();
      notifyListeners();
      return;
    }
    case KIND_AGENT_ASSIGNMENT: {
      // Defense-in-depth: only accept assignment records authored by the
      // identity this store was told is the current owner (mirrors the
      // announce/status author-binding checks above) — the relay's
      // `authors:[ownerPubkey]` filter already enforces this server-side;
      // this guards a compromised relay. `null` (no subscription started
      // yet — e.g. a pure-reducer test, or the identity fetch failed) skips
      // the check rather than rejecting everything, matching how this store
      // behaves before any live subscription has opened.
      if (
        currentOwnerPubkey &&
        normalizePubkey(event.pubkey) !== currentOwnerPubkey
      ) {
        return;
      }
      const assignment = parseAgentAssignmentTags(event);
      if (!assignment) return;
      assignmentByAgent.set(assignment.agentPubkey, assignment);
      // Assignments don't change the NodeView roster (name/os/runtimes/
      // online/agentCount all derive from announce+status+presence only) —
      // no invalidateSnapshot(), just wake subscribers so
      // getAgentAssignment/isAgentNodeHosted reads see the update.
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

export function getAgentAssignment(
  agentPubkey: string,
): AgentAssignmentView | undefined {
  return assignmentByAgent.get(normalizePubkey(agentPubkey));
}

/**
 * True when the OWNER's own desired-state record for this agent says
 * "assigned" — i.e. some execution node should be running it right now.
 *
 * This is the signal every local-only lifecycle affordance (avatar
 * Start/Restart, the Members-sidebar per-agent and bulk controls, the
 * profile panel's primary action) must gate on before spawning a local
 * process: a node-hosted agent's only start/stop/move is the
 * assignment-based `AgentNodeControls`, never a local spawn — otherwise the
 * desktop and the node both run the same identity/key at once.
 *
 * Deliberately keyed on `getAgentAssignment` (the owner's desired state),
 * NOT `getAgentStatus` (the node's *observed* health): status is empty in
 * the window between publishing an assignment and the node's first
 * `AGENT_NODE_STATUS`, which would leave every local control live during
 * exactly the gap right after create or move — the moment double-running is
 * most likely.
 */
export function isAgentNodeHosted(agentPubkey: string): boolean {
  return getAgentAssignment(agentPubkey)?.state === "assigned";
}

export function subscribeNodes(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/**
 * Open the live subscriptions feeding this store if they aren't already
 * open: the roster subscription (NODE_ANNOUNCE + AGENT_NODE_STATUS, unscoped
 * — both are addressable and roster-sized), the presence reconciler
 * (author-scoped to the announced node pubkeys, growing as the roster
 * grows), and this owner's own AGENT_ASSIGNMENT feed (author-scoped to the
 * resolved identity — that kind carries no `p` tag, so an unscoped filter
 * would leak every community member's assignment records). Idempotent and
 * safe to call from multiple mounted consumers (mirrors
 * `ensureRelayObserverSubscription`). Errors are logged, not thrown — the
 * panel should still render whatever the store already has.
 *
 * The assignment feed's identity resolution is best-effort: if `getIdentity`
 * fails, the roster subscription above is unaffected (it doesn't depend on
 * identity) and `isAgentNodeHosted` simply stays `false` for everything —
 * the same fail-open posture `useCommunityInit.ts` already takes on an
 * identity-resolution failure elsewhere in this app.
 */
export function ensureNodesRelaySubscription(): Promise<void> {
  if (unsubscribeRelay) {
    return Promise.resolve();
  }
  if (startPromise) {
    return startPromise;
  }

  const activeGeneration = generation;

  if (!presenceReconciler) {
    presenceReconciler = new PresenceSubscriptionReconciler({
      open: (authors) =>
        openPresenceSubscription(
          authors,
          (event) => {
            if (activeGeneration !== generation) return;
            ingestNodeEvent(event);
          },
          (...args) => relayClient.subscribeLive(...args),
        ),
    });
    // A node may already have been announced before the live subscription
    // session started (e.g. an earlier ingestNodeEvent call) — reconcile
    // immediately so its presence isn't missed until the next announce.
    reconcilePresenceAuthors();
  }

  startPromise = (async () => {
    const unsubscribers: Array<() => Promise<void>> = [];
    try {
      const unsubscribeRoster = await relayClient.subscribeLive(
        {
          kinds: [KIND_NODE_ANNOUNCE, KIND_AGENT_NODE_STATUS],
          limit: NODES_LIVE_SUBSCRIPTION_LIMIT,
        },
        (event) => {
          if (activeGeneration !== generation) return;
          ingestNodeEvent(event);
        },
      );
      if (activeGeneration !== generation) {
        void unsubscribeRoster();
        return;
      }
      unsubscribers.push(unsubscribeRoster);
      unsubscribeRelay = async () => {
        await Promise.all(unsubscribers.map((unsubscribe) => unsubscribe()));
      };
    } catch (error) {
      console.error("Failed to subscribe to the execution-node roster:", error);
      return;
    }

    try {
      const identity = await getIdentity();
      if (activeGeneration !== generation) return;
      currentOwnerPubkey = normalizePubkey(identity.pubkey);
      const unsubscribeAssignments = await relayClient.subscribeLive(
        {
          kinds: [KIND_AGENT_ASSIGNMENT],
          authors: [currentOwnerPubkey],
          limit: NODES_LIVE_SUBSCRIPTION_LIMIT,
        },
        (event) => {
          if (activeGeneration !== generation) return;
          ingestNodeEvent(event);
        },
      );
      if (activeGeneration !== generation) {
        void unsubscribeAssignments();
        return;
      }
      unsubscribers.push(unsubscribeAssignments);
    } catch (error) {
      // Degrades to isAgentNodeHosted always false — logged, not thrown, and
      // does not tear down the roster subscription established above.
      console.error(
        "Failed to subscribe to this owner's node assignments:",
        error,
      );
    }
  })().finally(() => {
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
  const reconciler = presenceReconciler;
  presenceReconciler = null;
  currentOwnerPubkey = null;
  nodesByPubkey.clear();
  statusByAgent.clear();
  presenceByPubkey.clear();
  assignmentByAgent.clear();
  invalidateSnapshot();
  notifyListeners();
  void unsubscribe?.();
  reconciler?.dispose();
}
