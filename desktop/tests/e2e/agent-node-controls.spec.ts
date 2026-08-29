import { expect, test } from "@playwright/test";

import { installMockBridge, TEST_IDENTITIES } from "../helpers/bridge";

/**
 * Agent-card node status + Start/Stop/Move controls (Phase 4 Task 6).
 *
 * `AgentNodeStatusBadge`/`AgentNodeControls` render inside the REAL, live
 * agent card — `StandaloneAgentCard` in `UnifiedAgentsSection.tsx` (a
 * `personaId`-less managed agent lands in the "Custom agents" ungrouped
 * bucket, see `unifiedAgentGroups.ts`). `ManagedAgentRow.tsx` is NOT wired
 * into a reachable route yet (see its own `agent-error-state-screenshots.spec.ts`
 * header comment) — testing against it would pass without proving anything
 * about the shipped app.
 *
 * Kind numbers mirror crates/buzz-core/src/kind.rs / desktop/src/shared/
 * constants/kinds.ts (39500, 39503, 20001) — duplicated as literals here,
 * same convention as nodes-panel.spec.ts.
 */
const KIND_NODE_ANNOUNCE = 39500;
const KIND_AGENT_NODE_STATUS = 39503;
const KIND_PRESENCE_UPDATE = 20001;

// Under 12 chars so truncatePubkey (NodeView.name's source) returns them
// unchanged, keeping text assertions exact.
const CURRENT_NODE = "node-a";
const OTHER_NODE = "node-b";

const AGENT_PUBKEY = TEST_IDENTITIES.tyler.pubkey;
const AGENT_NAME = "Node Agent";

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
      runtimes: ["claude"],
      workspace_root: "/home/node/.buzz-node",
    }),
  };
}

function presenceEvent(
  nodePubkey: string,
  status: "online" | "offline",
  seq: number,
) {
  return {
    id: `seed-presence-${nodePubkey}`,
    kind: KIND_PRESENCE_UPDATE,
    pubkey: nodePubkey,
    created_at: seq,
    tags: [],
    sig: "sig",
    content: status,
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

async function openAgentsViewAndSeed(
  page: import("@playwright/test").Page,
  events: unknown[],
) {
  await page.goto("/");
  await page.getByTestId("open-agents-view").click();
  await waitForSeedHook(page);
  await page.evaluate((seeded) => {
    window.__BUZZ_E2E_SEED_NODE_EVENTS__?.(seeded);
  }, events);
}

test("agent card shows node status and a Move control while running", async ({
  page,
}) => {
  await installMockBridge(page, {
    managedAgents: [
      { pubkey: AGENT_PUBKEY, name: AGENT_NAME, backend: { type: "local" } },
    ],
  });
  await openAgentsViewAndSeed(page, [
    announceEvent(CURRENT_NODE, 1),
    presenceEvent(CURRENT_NODE, "online", 2),
    announceEvent(OTHER_NODE, 3),
    presenceEvent(OTHER_NODE, "online", 4),
    statusEvent(CURRENT_NODE, AGENT_PUBKEY, "running", 5),
  ]);

  const statusBadge = page.getByTestId(`agent-node-status-${AGENT_PUBKEY}`);
  await expect(statusBadge).toBeVisible();
  await expect(statusBadge).toContainText(CURRENT_NODE);
  await expect(statusBadge).toContainText(/running/i);

  // Running: Stop is offered, Start is not.
  await expect(page.getByTestId(`agent-stop-${AGENT_PUBKEY}`)).toBeVisible();
  await expect(page.getByTestId(`agent-start-${AGENT_PUBKEY}`)).toHaveCount(0);

  const moveButton = page.getByTestId(`agent-move-${AGENT_PUBKEY}`);
  await expect(moveButton).toBeVisible();
  await moveButton.click();
  const moveTarget = page.getByRole("menuitem", { name: OTHER_NODE });
  await expect(moveTarget).toBeVisible();
  await moveTarget.click();

  // publishAgentAssignment round-tripped through the real mock command
  // handler (publish_agent_assignment) and the UI reflects success.
  await expect(
    page.locator("[data-sonner-toast]").filter({ hasText: /assigned/i }),
  ).toBeVisible();
});

test("stopping an agent publishes an unassign edit to its current node", async ({
  page,
}) => {
  await installMockBridge(page, {
    managedAgents: [
      { pubkey: AGENT_PUBKEY, name: AGENT_NAME, backend: { type: "local" } },
    ],
  });
  await openAgentsViewAndSeed(page, [
    announceEvent(CURRENT_NODE, 1),
    presenceEvent(CURRENT_NODE, "online", 2),
    statusEvent(CURRENT_NODE, AGENT_PUBKEY, "running", 3),
  ]);

  await page.getByTestId(`agent-stop-${AGENT_PUBKEY}`).click();
  await expect(
    page.locator("[data-sonner-toast]").filter({ hasText: /unassigned/i }),
  ).toBeVisible();
});

test("a stopped agent offers Start instead of Stop", async ({ page }) => {
  await installMockBridge(page, {
    managedAgents: [
      { pubkey: AGENT_PUBKEY, name: AGENT_NAME, backend: { type: "local" } },
    ],
  });
  await openAgentsViewAndSeed(page, [
    announceEvent(CURRENT_NODE, 1),
    presenceEvent(CURRENT_NODE, "online", 2),
    statusEvent(CURRENT_NODE, AGENT_PUBKEY, "stopped", 3),
  ]);

  await expect(
    page.getByTestId(`agent-node-status-${AGENT_PUBKEY}`),
  ).toContainText(/stopped/i);
  await expect(page.getByTestId(`agent-start-${AGENT_PUBKEY}`)).toBeVisible();
  await expect(page.getByTestId(`agent-stop-${AGENT_PUBKEY}`)).toHaveCount(0);
});

test("an agent with no node assignment shows neither status nor controls", async ({
  page,
}) => {
  await installMockBridge(page, {
    managedAgents: [
      { pubkey: AGENT_PUBKEY, name: AGENT_NAME, backend: { type: "local" } },
    ],
  });
  await page.goto("/");
  await page.getByTestId("open-agents-view").click();
  await expect(page.getByTestId(`managed-agent-${AGENT_PUBKEY}`)).toBeVisible();

  await expect(
    page.getByTestId(`agent-node-status-${AGENT_PUBKEY}`),
  ).toHaveCount(0);
  await expect(page.getByTestId(`agent-move-${AGENT_PUBKEY}`)).toHaveCount(0);
});
