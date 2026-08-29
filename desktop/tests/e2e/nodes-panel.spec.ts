import { expect, test } from "@playwright/test";

import { waitForAnimations } from "../helpers/animations";
import { installMockBridge } from "../helpers/bridge";

// Kind numbers mirror crates/buzz-core/src/kind.rs / desktop/src/shared/
// constants/kinds.ts (39500, 39503, 20001). Duplicated as literals here since
// this spec runs outside the app bundle.
const KIND_NODE_ANNOUNCE = 39500;
const KIND_AGENT_NODE_STATUS = 39503;
const KIND_PRESENCE_UPDATE = 20001;

const NODE_PUBKEY = "node1";
const AGENT_PUBKEY = "agent1";

async function waitForSeedHook(page: import("@playwright/test").Page) {
  await page.waitForFunction(
    () => typeof window.__BUZZ_E2E_SEED_NODE_EVENTS__ === "function",
    null,
    { timeout: 10_000 },
  );
}

test("nodes panel renders the roster and the enrollment approval control", async ({
  page,
}) => {
  await installMockBridge(page);
  // Hash-based routing (createHashHistory in app/router.tsx): the static
  // preview server only ever sees "GET /" (the #fragment never reaches it),
  // so deep links are always "/#/<route>" — see workflows.spec.ts /
  // virtualization.spec.ts for the same convention.
  await page.goto("/#/nodes", { waitUntil: "domcontentloaded" });
  await waitForSeedHook(page);

  // Empty state before any node has announced.
  await expect(page.getByTestId("nodes-empty-state")).toBeVisible();

  // Seed real NODE_ANNOUNCE + presence + AGENT_NODE_STATUS events straight
  // into nodesStore's actual reducer (ingestNodeEvent) via
  // __BUZZ_E2E_SEED_NODE_EVENTS__ — this exercises the production ingestion
  // path, not a stubbed panel (mirrors __BUZZ_E2E_SEED_OBSERVER_EVENTS__).
  await page.evaluate(
    ({ nodePubkey, agentPubkey, announceKind, statusKind, presenceKind }) => {
      window.__BUZZ_E2E_SEED_NODE_EVENTS__?.([
        {
          id: "seed-announce-1",
          kind: announceKind,
          pubkey: nodePubkey,
          created_at: 1,
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
        },
        {
          id: "seed-presence-1",
          kind: presenceKind,
          pubkey: nodePubkey,
          created_at: 2,
          tags: [],
          sig: "sig",
          content: "online",
        },
        {
          id: "seed-status-1",
          kind: statusKind,
          pubkey: nodePubkey,
          created_at: 3,
          tags: [["d", agentPubkey]],
          sig: "sig",
          content: JSON.stringify({
            format: "buzz-node-status-v1",
            version: 1,
            agent_pubkey: agentPubkey,
            node_pubkey: nodePubkey,
            health: "running",
            updated_at: "2026-08-29T00:00:00Z",
          }),
        },
      ]);
    },
    {
      nodePubkey: NODE_PUBKEY,
      agentPubkey: AGENT_PUBKEY,
      announceKind: KIND_NODE_ANNOUNCE,
      statusKind: KIND_AGENT_NODE_STATUS,
      presenceKind: KIND_PRESENCE_UPDATE,
    },
  );

  const row = page.getByTestId(`node-row-${NODE_PUBKEY}`);
  await expect(row).toBeVisible();
  await expect(page.getByTestId(`node-online-${NODE_PUBKEY}`)).toBeVisible();
  await expect(row.getByText("linux")).toBeVisible();
  await expect(row.getByText("Online")).toBeVisible();
  await expect(row.getByText("claude, codex")).toBeVisible();
  await expect(page.getByTestId(`node-agent-count-${NODE_PUBKEY}`)).toHaveText(
    "1",
  );
  await expect(page.getByTestId("nodes-empty-state")).toHaveCount(0);

  // Enrollment approval: paste a node pubkey and approve it.
  await page
    .getByTestId("node-enroll-pubkey-input")
    .fill("a-newly-paired-node-pubkey");
  const approveButton = page.getByTestId("node-enroll-approve");
  await expect(approveButton).toBeEnabled();
  await approveButton.click();
  await expect(
    page
      .locator("[data-sonner-toast]")
      .filter({ hasText: /enrollment published/i }),
  ).toBeVisible();

  await waitForAnimations(page);
});
