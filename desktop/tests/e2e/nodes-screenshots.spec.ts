/**
 * Screenshot spec for Phase 4 Task 6: the Nodes panel roster and an agent
 * card showing a live node-status badge + Start/Stop/Move controls.
 *
 * The agent card shot targets `StandaloneAgentCard`
 * (`UnifiedAgentsSection.tsx`) — the real, reachable card for a
 * `personaId`-less managed agent. `ManagedAgentRow.tsx` renders an
 * equivalent badge/controls but is not wired into a route yet (see its
 * `agent-error-state-screenshots.spec.ts` header note), so it is not a valid
 * screenshot target.
 */
import { expect, test } from "@playwright/test";

import { waitForAnimations } from "../helpers/animations";
import { installMockBridge, TEST_IDENTITIES } from "../helpers/bridge";

const SHOTS = "test-results/execution-nodes-screenshots";

const KIND_NODE_ANNOUNCE = 39500;
const KIND_AGENT_NODE_STATUS = 39503;
const KIND_PRESENCE_UPDATE = 20001;

const NODE_PUBKEY = "work-node";
const AGENT_PUBKEY = TEST_IDENTITIES.tyler.pubkey;
const AGENT_NAME = "Repo Sync Agent";

function announceEvent(nodePubkey: string, seq: number) {
  return {
    id: `seed-announce-${nodePubkey}`,
    kind: KIND_NODE_ANNOUNCE,
    pubkey: nodePubkey,
    created_at: seq,
    tags: [["d", nodePubkey]],
    sig: "sig",
    content: JSON.stringify({
      format: "buzz-node-v1",
      version: 1,
      node_pubkey: nodePubkey,
      os: "linux",
      runtimes: ["claude", "codex"],
      workspace_root: "/home/node/.buzz-node",
    }),
  };
}

function presenceEvent(nodePubkey: string, seq: number) {
  return {
    id: `seed-presence-${nodePubkey}`,
    kind: KIND_PRESENCE_UPDATE,
    pubkey: nodePubkey,
    created_at: seq,
    tags: [],
    sig: "sig",
    content: "online",
  };
}

function statusEvent(
  nodePubkey: string,
  agentPubkey: string,
  health: string,
  seq: number,
) {
  return {
    id: `seed-status-${seq}`,
    kind: KIND_AGENT_NODE_STATUS,
    pubkey: nodePubkey,
    created_at: seq,
    tags: [["d", agentPubkey]],
    sig: "sig",
    content: JSON.stringify({
      format: "buzz-node-status-v1",
      version: 1,
      agent_pubkey: agentPubkey,
      node_pubkey: nodePubkey,
      health,
      updated_at: "2026-08-29T00:00:00Z",
    }),
  };
}

async function waitForSeedHook(page: import("@playwright/test").Page) {
  await page.waitForFunction(
    () => typeof window.__BUZZ_E2E_SEED_NODE_EVENTS__ === "function",
    null,
    { timeout: 10_000 },
  );
}

test("nodes panel roster", async ({ page }) => {
  await installMockBridge(page);
  await page.goto("/#/nodes", { waitUntil: "domcontentloaded" });
  await waitForSeedHook(page);

  await page.evaluate(
    (events) => {
      window.__BUZZ_E2E_SEED_NODE_EVENTS__?.(events);
    },
    [
      announceEvent(NODE_PUBKEY, 1),
      presenceEvent(NODE_PUBKEY, 2),
      statusEvent(NODE_PUBKEY, AGENT_PUBKEY, "running", 3),
    ],
  );

  const panel = page.getByTestId("nodes-panel");
  await expect(panel.getByTestId(`node-row-${NODE_PUBKEY}`)).toBeVisible();
  await waitForAnimations(page);
  await panel.screenshot({ path: `${SHOTS}/nodes-panel.png` });
});

test("agent card with node status badge and controls", async ({ page }) => {
  await installMockBridge(page, {
    managedAgents: [
      { pubkey: AGENT_PUBKEY, name: AGENT_NAME, backend: { type: "local" } },
    ],
  });
  await page.goto("/");
  await page.getByTestId("open-agents-view").click();
  await waitForSeedHook(page);

  await page.evaluate(
    (events) => {
      window.__BUZZ_E2E_SEED_NODE_EVENTS__?.(events);
    },
    [
      announceEvent(NODE_PUBKEY, 1),
      presenceEvent(NODE_PUBKEY, 2),
      statusEvent(NODE_PUBKEY, AGENT_PUBKEY, "running", 3),
    ],
  );

  const card = page.getByTestId(`managed-agent-${AGENT_PUBKEY}`);
  await expect(
    card.getByTestId(`agent-node-status-${AGENT_PUBKEY}`),
  ).toBeVisible();
  await expect(card.getByTestId(`agent-move-${AGENT_PUBKEY}`)).toBeVisible();
  await waitForAnimations(page);
  await card.screenshot({ path: `${SHOTS}/agent-card-node-status.png` });
});
