import { expect, test } from "@playwright/test";

import { installMockBridge } from "../helpers/bridge";

/**
 * Extends the create-agent "Run on" picker (`WhereToRunSection.tsx`) with
 * enrolled, online execution nodes (Phase 4 Task 5). Node data comes from
 * `nodesStore` (now `shared/api/nodesStore.ts`), fed here through the same
 * real-reducer seed hook `nodes-panel.spec.ts` uses
 * (`__BUZZ_E2E_SEED_NODE_EVENTS__` → `ingestNodeEvent`) rather than a
 * fabricated mock-only global — this exercises the production ingestion
 * path, not a stubbed picker.
 *
 * Navigation mirrors `where-to-run-config.spec.ts`'s established,
 * already-pinned conventions: the "Run on" field lives behind the
 * create-agent dialog's "Advanced" disclosure (`#agent-run-on` has count 0
 * until it is expanded), the dialog's stable handle is
 * `getByTestId("persona-dialog")` (not a bare role query), and
 * `PersonaDropdownField` options are `menuitemradio` — a Radix
 * `DropdownMenuRadioGroup`, not a native `<select>` or ARIA listbox.
 *
 * Kind numbers mirror crates/buzz-core/src/kind.rs / desktop/src/shared/
 * constants/kinds.ts (39500, 20001) — duplicated as literals here since this
 * spec runs outside the app bundle (same convention as nodes-panel.spec.ts).
 */
const KIND_NODE_ANNOUNCE = 39500;
const KIND_PRESENCE_UPDATE = 20001;

// Both under 12 chars, so shared/lib/pubkey's truncatePubkey (the source of
// NodeView.name — NodeCapabilities carries no human-readable name field)
// returns them unchanged, keeping the dropdown-option assertions exact.
const ONLINE_NODE_PUBKEY = "node-online";
const OFFLINE_NODE_PUBKEY = "node-offline";

type Page = import("@playwright/test").Page;
type Locator = import("@playwright/test").Locator;

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

async function waitForSeedHook(page: Page) {
  await page.waitForFunction(
    () => typeof window.__BUZZ_E2E_SEED_NODE_EVENTS__ === "function",
    null,
    { timeout: 10_000 },
  );
}

/** Open the create-agent dialog with its "Advanced" section expanded. */
async function openCreateDialogAdvanced(page: Page): Promise<Locator> {
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await page.getByTestId("open-agents-view").click();
  await page.getByTestId("new-agent-card").click();
  const dialog = page.getByTestId("persona-dialog");
  await expect(dialog).toBeVisible({ timeout: 10_000 });
  const advanced = dialog.getByRole("button", {
    name: "Advanced",
    exact: true,
  });
  await expect(dialog.locator("#agent-run-on")).toHaveCount(0);
  await advanced.click();
  await expect(advanced).toHaveAttribute("aria-expanded", "true");
  return dialog;
}

test("run-on picker offers enrolled online nodes and excludes offline ones", async ({
  page,
}) => {
  await installMockBridge(page);
  const dialog = await openCreateDialogAdvanced(page);
  await waitForSeedHook(page);

  await page.evaluate(
    (events) => {
      window.__BUZZ_E2E_SEED_NODE_EVENTS__?.(events);
    },
    [
      announceEvent(ONLINE_NODE_PUBKEY, 1),
      presenceEvent(ONLINE_NODE_PUBKEY, "online", 2),
      announceEvent(OFFLINE_NODE_PUBKEY, 3),
      presenceEvent(OFFLINE_NODE_PUBKEY, "offline", 4),
    ],
  );

  const runOnTrigger = dialog.locator("#agent-run-on");
  await expect(runOnTrigger).toBeVisible();
  await runOnTrigger.press("Enter");

  await expect(
    page.getByRole("menuitemradio", { name: ONLINE_NODE_PUBKEY, exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("menuitemradio", { name: OFFLINE_NODE_PUBKEY }),
  ).toHaveCount(0);

  // Selecting the node surfaces the same key-custody trust warning shape the
  // provider path already uses (spec §12: the node receives the agent's key).
  await page
    .getByRole("menuitemradio", { name: ONLINE_NODE_PUBKEY, exact: true })
    .press("Enter");
  await expect(runOnTrigger).toHaveAttribute("aria-expanded", "false");
  await expect(dialog.getByText("will receive your agent")).toBeVisible();
});

/**
 * Phase 4 fix-round-1 Minor finding: prove the create-with-a-node-target
 * flow end to end — publishAgentAssignment fires with the real command's
 * args right after creation, AND the resulting card's local Start
 * affordance is gated once nodesStore reflects that assignment (the exact
 * gap the fix-round-1 Critical finding was about: a node-hosted agent's
 * avatar Start must never spawn a second, competing local process).
 *
 * The mock `publish_agent_assignment` IPC handler (e2eBridge.ts) does not
 * itself echo the published event back through the live subscription —
 * unlike a real relay, which would return the owner's own event to their
 * own author-scoped subscription. This spec seeds that echo explicitly via
 * `__BUZZ_E2E_SEED_NODE_EVENTS__` (same real-reducer seed hook the rest of
 * this file and nodes-panel.spec.ts use), using the exact agentId/nodePubkey
 * the real create flow just published — it does not fabricate a different
 * scenario, only supplies the round-trip a live relay would provide for
 * free.
 */
test("creating an agent with a node target assigns it and gates the local Start affordance", async ({
  page,
}) => {
  await installMockBridge(page);
  const dialog = await openCreateDialogAdvanced(page);
  await waitForSeedHook(page);

  await page.evaluate(
    (events) => {
      window.__BUZZ_E2E_SEED_NODE_EVENTS__?.(events);
    },
    [
      announceEvent(ONLINE_NODE_PUBKEY, 1),
      presenceEvent(ONLINE_NODE_PUBKEY, "online", 2),
    ],
  );

  const agentName = `Node Target Agent ${Date.now()}`;
  await dialog.locator("#persona-display-name").fill(agentName);

  // Satisfies the definition dialog's local-mode AI-config gate
  // (computeLocalModeGate/customAiPairSatisfied — unrelated to node
  // targeting, required for ANY create submission under the default mock
  // bridge with no baked/global model config) — same selection
  // smoke.spec.ts's chooseSharedComputeProvider makes for the equivalent
  // reason.
  await dialog.getByRole("tab", { name: "Customize for this agent" }).click();
  const llmProvider = dialog.locator("#persona-llm-provider");
  await expect(llmProvider).toBeVisible({ timeout: 10_000 });
  await llmProvider.press("Enter");
  await page
    .getByRole("menuitemradio", { exact: true, name: "Buzz shared compute" })
    .click();

  const runOnTrigger = dialog.locator("#agent-run-on");
  await runOnTrigger.press("Enter");
  await page
    .getByRole("menuitemradio", { name: ONLINE_NODE_PUBKEY, exact: true })
    .press("Enter");
  await expect(runOnTrigger).toContainText(ONLINE_NODE_PUBKEY);

  await dialog.getByTestId("persona-dialog-submit").click();
  await expect(
    page
      .locator("[data-sonner-toast][data-removed='false']")
      .filter({ hasText: "Agent created" }),
  ).toBeVisible({ timeout: 10_000 });
  await expect(page.getByRole("dialog")).toHaveCount(0);

  // publishAgentAssignment fired with the real command's args: the node the
  // user picked, assigned:true, and the launch descriptor's command/args
  // pulled straight off the just-created record (see
  // publishNodeAssignmentForCreatedAgent.ts).
  const assignmentCall = await page.evaluate(() => {
    const log = window.__BUZZ_E2E_COMMAND_LOG__ ?? [];
    return log.find((entry) => entry.command === "publish_agent_assignment")
      ?.payload as
      | {
          agentId: string;
          nodePubkey: string;
          assigned: boolean;
          launch: { command: string };
        }
      | undefined;
  });
  expect(assignmentCall).toBeTruthy();
  expect(assignmentCall?.nodePubkey).toBe(ONLINE_NODE_PUBKEY);
  expect(assignmentCall?.assigned).toBe(true);
  expect(assignmentCall?.launch.command).toBeTruthy();
  const agentId = assignmentCall?.agentId as string;

  // Simulate the relay echoing that just-published assignment back to this
  // same owner-scoped subscription (see this test's header comment). Authored
  // by the mock bridge's default identity pubkey (e2eBridge.ts's
  // DEFAULT_MOCK_IDENTITY) — nodesStore's defense-in-depth author check
  // (mirrors buzz-core's own) drops an AGENT_ASSIGNMENT whose author doesn't
  // match the identity ensureNodesRelaySubscription resolved.
  const MOCK_OWNER_PUBKEY = "deadbeef".repeat(8);
  await page.evaluate(
    ({ nodePubkey, agentPubkey, ownerPubkey }) => {
      window.__BUZZ_E2E_SEED_NODE_EVENTS__?.([
        {
          id: "seed-assignment-1",
          kind: 39502,
          pubkey: ownerPubkey,
          created_at: Date.now(),
          tags: [
            ["d", agentPubkey],
            ["node", nodePubkey],
            ["state", "assigned"],
          ],
          sig: "sig",
          content: "encrypted-marker",
        },
      ]);
    },
    {
      nodePubkey: ONLINE_NODE_PUBKEY,
      agentPubkey: agentId,
      ownerPubkey: MOCK_OWNER_PUBKEY,
    },
  );

  // The Critical fix: the avatar's local Start must now be disabled — the
  // node, not this desktop, owns this agent's process.
  const startButton = page.getByTestId(`agent-runtime-start-${agentId}`);
  await expect(startButton).toBeVisible();
  await expect(startButton).toBeDisabled();
});
