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
  /** The source event's `created_at` — last-writer-wins tiebreak so an
   * out-of-order-delivered older event can never overwrite a newer one. */
  createdAt: number;
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

// The roster (NODE_ANNOUNCE + AGENT_NODE_STATUS) and owner-scoped assignment
// (AGENT_ASSIGNMENT) subscriptions are tracked as two INDEPENDENT idempotent
// legs, each with its own "already subscribed" handle and in-flight promise.
// This is deliberate, not incidental: the assignment leg depends on
// getIdentity() resolving, which can fail transiently (IPC hiccup) even when
// the roster leg (which needs no identity) already succeeded. If a single
// combined flag marked "fully subscribed" once the roster leg alone
// succeeded, every later ensureNodesRelaySubscription() call would
// short-circuit on that flag and never retry the failed assignment leg —
// isNodeHostedAgent would then silently stay false (gate open) for the rest
// of the community session. Splitting them means a fresh call retries
// exactly the leg that previously failed, without re-subscribing the one
// that already succeeded.
let unsubscribeRoster: (() => Promise<void>) | null = null;
let rosterStartPromise: Promise<void> | null = null;
let unsubscribeAssignments: (() => Promise<void>) | null = null;
let assignmentStartPromise: Promise<void> | null = null;
// Test-only seam: production always resolves identity via the real
// getIdentity from @/shared/api/tauriIdentity. Overridable so
// nodesStore.test.mjs can simulate a transient getIdentity failure and its
// retry without ESM module-mocking (this repo's test runner does not enable
// node:test's --experimental-test-module-mocks). Never set outside tests.
let resolveOwnerIdentity: () => Promise<{ pubkey: string }> = getIdentity;
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
    createdAt: event.created_at,
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
      // Last-writer-wins by created_at: an out-of-order-delivered stale
      // "unassigned" must never overwrite a newer "assigned" — that would
      // silently re-enable local Start/Restart for a still-node-hosted
      // agent, undoing the whole point of isNodeHostedAgent. Ties favor the
      // newly-ingested event (consistent with the announce/status reducers
      // above, which have no ordering guard at all — this one is stricter
      // because getting it wrong here reopens the double-spawn hazard, not
      // just a display staleness).
      const existing = assignmentByAgent.get(assignment.agentPubkey);
      if (existing && existing.createdAt > assignment.createdAt) {
        return;
      }
      assignmentByAgent.set(assignment.agentPubkey, assignment);
      // invalidateSnapshot(): NodeView's own fields (name/os/runtimes/online/
      // agentCount) never depend on assignment data, but getNodesSnapshot()'s
      // returned reference is the reactivity signal every
      // useSyncExternalStore(subscribeNodes, getNodesSnapshot) caller relies
      // on (React bails out of re-rendering when getSnapshot() returns an
      // Object.is-equal value even after the listener fires) — including
      // isNodeHostedAgent-gated UI (AgentPersonaCard/StandaloneAgentCard).
      // Without this, notifyListeners() alone does not reliably repaint the
      // gate when an assignment changes. Mirrors why the status case above
      // already invalidates (agentCount derives from status).
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
 * Idempotent roster leg: NODE_ANNOUNCE + AGENT_NODE_STATUS, unscoped (both
 * are addressable and roster-sized), plus the presence reconciler
 * (author-scoped to the announced node pubkeys, growing as the roster
 * grows). Needs no identity, so it has no reason to ever need a retry once
 * it succeeds.
 */
function ensureRosterSubscription(activeGeneration: number): Promise<void> {
  if (unsubscribeRoster) {
    return Promise.resolve();
  }
  if (rosterStartPromise) {
    return rosterStartPromise;
  }

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

  rosterStartPromise = relayClient
    .subscribeLive(
      {
        kinds: [KIND_NODE_ANNOUNCE, KIND_AGENT_NODE_STATUS],
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
      unsubscribeRoster = unsubscribe;
    })
    .catch((error) => {
      console.error("Failed to subscribe to the execution-node roster:", error);
    })
    .finally(() => {
      if (activeGeneration === generation) {
        rosterStartPromise = null;
      }
    });

  return rosterStartPromise;
}

/**
 * Idempotent, independently-retryable assignment leg: this owner's own
 * AGENT_ASSIGNMENT feed (author-scoped to the resolved identity — that kind
 * carries no `p` tag, so an unscoped filter would leak every community
 * member's assignment records). Kept separate from the roster leg so a
 * transient `getIdentity`/`subscribeLive` failure here doesn't get masked by
 * the roster leg's own success — see the module-level comment above
 * `unsubscribeRoster` for why a single combined flag was the bug.
 */
function ensureAssignmentSubscription(activeGeneration: number): Promise<void> {
  if (unsubscribeAssignments) {
    return Promise.resolve();
  }
  if (assignmentStartPromise) {
    return assignmentStartPromise;
  }

  assignmentStartPromise = (async () => {
    try {
      const identity = await resolveOwnerIdentity();
      if (activeGeneration !== generation) return;
      currentOwnerPubkey = normalizePubkey(identity.pubkey);
      const unsubscribe = await relayClient.subscribeLive(
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
        void unsubscribe();
        return;
      }
      unsubscribeAssignments = unsubscribe;
    } catch (error) {
      // Degrades to isAgentNodeHosted staying false — logged, not thrown,
      // and does not touch the roster leg. Left retryable: since
      // unsubscribeAssignments stays null, the NEXT
      // ensureNodesRelaySubscription() call (from any newly-mounted
      // consumer, or a deliberate re-check) re-attempts this leg from
      // scratch, including re-resolving identity.
      console.error(
        "Failed to subscribe to this owner's node assignments:",
        error,
      );
    }
  })().finally(() => {
    if (activeGeneration === generation) {
      assignmentStartPromise = null;
    }
  });

  return assignmentStartPromise;
}

/**
 * Open the live subscriptions feeding this store if they aren't already
 * open. Idempotent and safe to call from multiple mounted consumers (mirrors
 * `ensureRelayObserverSubscription`) — see `ensureRosterSubscription`/
 * `ensureAssignmentSubscription` for why they're two independently-retryable
 * legs rather than one. Errors are logged, not thrown — the panel should
 * still render whatever the store already has.
 */
export async function ensureNodesRelaySubscription(): Promise<void> {
  const activeGeneration = generation;
  await Promise.all([
    ensureRosterSubscription(activeGeneration),
    ensureAssignmentSubscription(activeGeneration),
  ]);
}

/** Tear down the module-level store. Call from `resetCommunityState()`. */
export function resetNodesStore(): void {
  generation += 1;
  const unsubRoster = unsubscribeRoster;
  unsubscribeRoster = null;
  rosterStartPromise = null;
  const unsubAssignments = unsubscribeAssignments;
  unsubscribeAssignments = null;
  assignmentStartPromise = null;
  const reconciler = presenceReconciler;
  presenceReconciler = null;
  currentOwnerPubkey = null;
  nodesByPubkey.clear();
  statusByAgent.clear();
  presenceByPubkey.clear();
  assignmentByAgent.clear();
  invalidateSnapshot();
  notifyListeners();
  void unsubRoster?.();
  void unsubAssignments?.();
  reconciler?.dispose();
}

/**
 * Test-only: override the identity resolver the assignment leg uses, so
 * `nodesStore.test.mjs` can simulate a transient `getIdentity` failure (and
 * its retry on a later `ensureNodesRelaySubscription()` call) without ESM
 * module-mocking. Pass `null` to restore the real `getIdentity`. Never call
 * this outside a test.
 */
export function __setIdentityResolverForTests(
  resolver: (() => Promise<{ pubkey: string }>) | null,
): void {
  resolveOwnerIdentity = resolver ?? getIdentity;
}
