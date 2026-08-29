# Execution Nodes — Phase 4: Desktop App (enroll / assign / status) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the local desktop surfaces to enroll a node, assign an agent to a node, and see per-agent/node status — all by publishing and subscribing to the Phase-1 relay event kinds. The app talks **only** to the relay; signing and key access stay native.

**Architecture:** Two native Tauri commands (`publish_node_enrollment`, `publish_agent_assignment`) build + owner-sign the Phase-1 events (the assignment NIP-44-encrypted to the target node) and publish them via the existing `relay::submit_event_with_keys` path — the frontend never sees a key. A community-scoped live store (`nodesStore`) mirrors `observerRelayStore` to project `NODE_ANNOUNCE` + presence + `AGENT_NODE_STATUS` into the UI. A new **Nodes** panel and an extended **Run on** picker drive those commands; agent cards render status and Start/Stop/Move controls that are just assignment edits.

**Tech Stack:** Tauri 2 (Rust) + React 19 + TypeScript + Biome + Tailwind; Vitest (unit) + Playwright mock-bridge (e2e). buzz-core is imported in `src-tauri` as the crate alias **`buzz_core_pkg`** (see `desktop/src-tauri/src/commands/agent_discovery/relay_directory.rs:361`).

**Spec:** `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` (§10 desktop app changes; §7 kinds; §12 security). Read it alongside this plan. Depends on **Phase 1** (`buzz_core::{assignment, node, node_status}` codecs). Does **not** depend on Phase 2/3 internals — it only speaks the wire contract.

## Global Constraints

- **Signing/keys stay native.** The frontend calls commands and receives already-signed results; a private key never crosses IPC. Owner keys come from `state.keys` (`desktop/src-tauri/src/app_state.rs`); agent keys are resolved from the keyring the way `commands/agents.rs:418` already does.
- **Publish via `relay::submit_event_with_keys(builder, state, signer, None)`** (`desktop/src-tauri/src/relay.rs:624`) — do not open new sockets.
- **No new `unwrap()`/`expect()` in non-test Rust.** Tauri commands return `Result<T, String>` (map errors with `.map_err(|e| e.to_string())`).
- **rem-based Tailwind text tokens only** (`text-sm`, `text-xs`, `text-2xs`…); never `text-[13px]`/px text (CI `pnpm check:px-text`).
- **One public widget per file; ≤1000 lines/file** (`just file-size-check`). Push private sub-widgets into sibling files.
- **Feature modules import only from `shared/` or their own feature.** The new `features/nodes` module must not import from `features/agents` internals; put shared types in `shared/` if needed.
- **Live relay data → feature store** (`nodesStore`, not React Query). **Command/request data → React Query** (`useMutation` on the Tauri commands).
- **Community-scoped singletons MUST reset in `resetCommunityState()`** (`desktop/src/features/communities/useCommunityInit.ts`) — the new `nodesStore` included, or old-community nodes leak.
- **Build e2e with `pnpm build:e2e`** (never `pnpm run build`); every spec calls `installMockBridge(page)`; register smoke specs in `desktop/playwright.config.ts` (`smoke` project `testMatch`).
- **Screenshots** follow AGENTS.md: `just desktop-screenshot` or a spec using `waitForAnimations(page)` before capture; post with `scripts/post-screenshots.sh` (never relay URLs); gate distinct shots on `shasum -a 256` uniqueness.
- **Commit with `git commit -s`** (DCO). Run `cd desktop && pnpm biome check --write src` + `pnpm tsc --noEmit` and, for Rust, `cargo clippy --manifest-path desktop/src-tauri/Cargo.toml` before each task's final commit.

---

### Task 1: `publish_node_enrollment` native command

**Files:**
- Create: `desktop/src-tauri/src/commands/nodes.rs`
- Create: `desktop/src-tauri/src/commands/nodes_tests.rs`
- Modify: `desktop/src-tauri/src/commands/mod.rs` (add `pub mod nodes;` and `#[cfg(test)] mod nodes_tests;`)
- Modify: `desktop/src-tauri/src/lib.rs` (add `commands::nodes::publish_node_enrollment` to the `tauri::generate_handler!` list)

**Interfaces:**
- Consumes: `buzz_core_pkg::node::build_enrollment`, `relay::submit_event_with_keys`, `AppState`.
- Produces: `#[tauri::command] pub async fn publish_node_enrollment(state: State<'_, AppState>, node_pubkey: String) -> Result<String, String>` (returns the published event id hex).

- [ ] **Step 1: Write the failing Rust unit test.**

In `nodes_tests.rs` (mirror the offline unit tests in `desktop/src-tauri/src/commands/agents_tests.rs`):

```rust
use super::nodes::*;
use nostr::{Keys, PublicKey};

#[test]
fn enrollment_builder_is_owner_signed_and_targets_node() {
    let owner = Keys::generate();
    let node = Keys::generate();
    // Pure builder path (no relay): build_enrollment lives in buzz-core.
    let ev = buzz_core_pkg::node::build_enrollment(
        &owner, &node.public_key(), nostr::Timestamp::now().as_secs(),
    )
    .expect("build enrollment");
    let parsed = buzz_core_pkg::node::validate_enrollment(&ev, &owner.public_key()).unwrap();
    assert_eq!(parsed.node_pubkey, node.public_key().to_hex());
    // Wrong owner rejected.
    assert!(buzz_core_pkg::node::validate_enrollment(&ev, &Keys::generate().public_key()).is_err());
    let _ = PublicKey::from_hex(&parsed.node_pubkey).unwrap();
}
```

- [ ] **Step 2: Run to verify it fails.**

Run: `cargo test --manifest-path desktop/src-tauri/Cargo.toml enrollment_builder_is_owner_signed -- --nocapture`
Expected: FAIL to compile (`commands::nodes` does not exist).

- [ ] **Step 3: Implement the command.**

In `nodes.rs`:

```rust
//! Execution-node desktop commands: enrollment + assignment publication.
use nostr::PublicKey;
use tauri::State;

use crate::app_state::AppState;
use crate::relay;

/// Owner-sign and publish a `NODE_ENROLLMENT` authorizing `node_pubkey`.
/// Returns the published event id (hex). Keys never leave the backend.
#[tauri::command]
pub async fn publish_node_enrollment(
    state: State<'_, AppState>,
    node_pubkey: String,
) -> Result<String, String> {
    let node = PublicKey::from_hex(&node_pubkey).map_err(|_| "invalid node pubkey".to_string())?;
    let owner = { state.keys.lock().map_err(|_| "keys lock".to_string())?.clone() };
    let event = buzz_core_pkg::node::build_enrollment(
        &owner,
        &node,
        nostr::Timestamp::now().as_secs(),
    )
    .map_err(|e| e.to_string())?;
    let id = event.id.to_hex();
    relay::submit_event_with_keys(event.into(), &state, &owner, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(id)
}
```

> Note: `submit_event_with_keys` takes an `EventBuilder` in its real-relay usage; if it requires a builder rather than a signed `Event`, replace `build_enrollment` (which signs) with a builder variant, or add `buzz_core::node::enrollment_builder(...) -> EventBuilder` in a tiny Phase-1 follow-up and sign via the relay helper. Confirm the exact arg type at `relay.rs:624` and match it — do not construct a second signing path.

- [ ] **Step 4: Register the command.** Add `pub mod nodes;` (+ `#[cfg(test)] mod nodes_tests;`) to `commands/mod.rs`, and add `commands::nodes::publish_node_enrollment` to the `tauri::generate_handler![…]` macro in `lib.rs` (the ~300-command list).

- [ ] **Step 5: Run test + clippy.**

Run: `cargo test --manifest-path desktop/src-tauri/Cargo.toml nodes -- --nocapture && cargo clippy --manifest-path desktop/src-tauri/Cargo.toml`
Expected: PASS, no warnings.

- [ ] **Step 6: Commit.**
```bash
git add desktop/src-tauri/src/commands/nodes.rs desktop/src-tauri/src/commands/nodes_tests.rs desktop/src-tauri/src/commands/mod.rs desktop/src-tauri/src/lib.rs
git commit -s -m "feat(desktop): publish_node_enrollment command"
```

---

### Task 2: `publish_agent_assignment` native command

**Files:**
- Modify: `desktop/src-tauri/src/commands/nodes.rs`
- Modify: `desktop/src-tauri/src/commands/nodes_tests.rs`
- Modify: `desktop/src-tauri/src/lib.rs` (register `publish_agent_assignment`)

**Interfaces:**
- Consumes: `buzz_core_pkg::{AssignmentSecret, LaunchBlock, AssignState, assignment::build_assignment}`; the agent-keys resolver used at `commands/agents.rs:418` (`(agent_keys, private_key_nsec, …)`).
- Produces:
```rust
#[derive(serde::Deserialize)]
pub struct LaunchInput {
    pub command: String,
    pub args: Vec<String>,
    pub env: std::collections::BTreeMap<String, String>,
    pub policy_env: std::collections::BTreeMap<String, String>,
    pub owner_pubkey: Option<String>,
}
#[tauri::command]
pub async fn publish_agent_assignment(
    state: State<'_, AppState>,
    agent_id: String,        // agent pubkey hex (the `d` coordinate)
    node_pubkey: String,     // target node
    launch: LaunchInput,
    assigned: bool,          // true = assigned, false = unassigned (stop)
) -> Result<String, String>
```

The `launch` block is produced by the **existing desktop launch-data resolver** the local spawn uses (`commands/agents.rs` resolve path around line 418 and the `launch` assembly in `commands/agents_deploy.rs`) — the frontend passes the resolved values through; do not re-derive runtime discovery here (spec §Launch data / D3 seam).

- [ ] **Step 1: Write the failing round-trip test.**

```rust
#[test]
fn assignment_builder_encrypts_to_node_and_round_trips() {
    use buzz_core_pkg::{assignment, AssignState, AssignmentSecret, LaunchBlock};
    use nostr::{Keys, ToBech32};
    use std::collections::BTreeMap;

    let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
    let secret = AssignmentSecret {
        format: "buzz-agent-assignment-v1".into(),
        version: 1,
        agent_pubkey: agent.public_key().to_hex(),
        owner_pubkey: owner.public_key().to_hex(),
        node_pubkey: node.public_key().to_hex(),
        private_key_nsec: agent.secret_key().to_bech32().unwrap(),
        auth_tag: None,
        launch: LaunchBlock {
            command: "claude".into(), args: vec![],
            env: BTreeMap::new(), policy_env: BTreeMap::new(),
            owner_pubkey: Some(owner.public_key().to_hex()),
        },
        env_vars: BTreeMap::new(),
        reap_after_idle_seconds: None,
    };
    let ev = assignment::build_assignment(
        &owner, &node.public_key(), &secret, AssignState::Assigned,
        nostr::Timestamp::now().as_secs(),
    )
    .unwrap();
    // Target node decrypts; a stranger cannot.
    let (_, got) = assignment::decrypt_for_node(&ev, &node, &owner.public_key()).unwrap();
    assert_eq!(got.private_key_nsec, secret.private_key_nsec);
    assert!(assignment::decrypt_for_node(&ev, &Keys::generate(), &owner.public_key()).is_err());
}
```

- [ ] **Step 2: Run to verify it fails.**

Run: `cargo test --manifest-path desktop/src-tauri/Cargo.toml assignment_builder_encrypts_to_node -- --nocapture`
Expected: FAIL to compile (command/types not wired).

- [ ] **Step 3: Implement the command.**

```rust
use std::collections::BTreeMap;

/// Owner-sign and publish an `AGENT_ASSIGNMENT`. The agent's nsec + launch env
/// are NIP-44-encrypted to `node_pubkey`; only that node can read them.
#[tauri::command]
pub async fn publish_agent_assignment(
    state: State<'_, AppState>,
    agent_id: String,
    node_pubkey: String,
    launch: LaunchInput,
    assigned: bool,
) -> Result<String, String> {
    let node = PublicKey::from_hex(&node_pubkey).map_err(|_| "invalid node pubkey".to_string())?;
    let agent_pk =
        PublicKey::from_hex(&agent_id).map_err(|_| "invalid agent pubkey".to_string())?;
    let owner = { state.keys.lock().map_err(|_| "keys lock".to_string())?.clone() };

    // Resolve the agent's nsec from the keyring exactly as the local-spawn path
    // does (commands/agents.rs:418). Extract this into `agent_keys_for` if not
    // already exposed; it must NOT return the key to the frontend.
    let (_agent_keys, private_key_nsec) = crate::commands::agents::agent_keys_for(&state, &agent_id)
        .map_err(|e| e.to_string())?;

    let secret = buzz_core_pkg::AssignmentSecret {
        format: "buzz-agent-assignment-v1".into(),
        version: 1,
        agent_pubkey: agent_pk.to_hex(),
        owner_pubkey: owner.public_key().to_hex(),
        node_pubkey: node.to_hex(),
        private_key_nsec,
        auth_tag: None,
        launch: buzz_core_pkg::LaunchBlock {
            command: launch.command,
            args: launch.args,
            env: launch.env,
            policy_env: launch.policy_env,
            owner_pubkey: launch.owner_pubkey,
        },
        env_vars: BTreeMap::new(),
        reap_after_idle_seconds: None,
    };
    let state_enum = if assigned {
        buzz_core_pkg::AssignState::Assigned
    } else {
        buzz_core_pkg::AssignState::Unassigned
    };
    let event = buzz_core_pkg::assignment::build_assignment(
        &owner, &node, &secret, state_enum, nostr::Timestamp::now().as_secs(),
    )
    .map_err(|e| e.to_string())?;
    let id = event.id.to_hex();
    relay::submit_event_with_keys(event.into(), &state, &owner, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(id)
}
```

> If `agent_keys_for` does not exist, add a small `pub(crate) fn agent_keys_for(state, agent_id) -> Result<(nostr::Keys, String), String>` in `commands/agents.rs` factoring the existing `(agent_keys, private_key_nsec, …)` resolution at line 418, and call it from both sites (DRY — do not duplicate keyring access).

- [ ] **Step 4: Register** `commands::nodes::publish_agent_assignment` in `lib.rs`'s handler list.

- [ ] **Step 5: Run test + clippy.**

Run: `cargo test --manifest-path desktop/src-tauri/Cargo.toml nodes -- --nocapture && cargo clippy --manifest-path desktop/src-tauri/Cargo.toml`
Expected: PASS, no warnings.

- [ ] **Step 6: Commit.**
```bash
git add desktop/src-tauri/src/commands/nodes.rs desktop/src-tauri/src/commands/nodes_tests.rs desktop/src-tauri/src/commands/agents.rs desktop/src-tauri/src/lib.rs
git commit -s -m "feat(desktop): publish_agent_assignment command (nsec encrypted to node)"
```

---

### Task 3: `nodesStore` live store + community reset

**Files:**
- Create: `desktop/src/features/nodes/lib/nodesStore.ts`
- Create: `desktop/src/features/nodes/lib/nodesStore.test.ts`
- Modify: `desktop/src/features/communities/useCommunityInit.ts` (import + call `resetNodesStore()` inside `resetCommunityState`)

**Interfaces:**
- Consumes: the relay subscription helper the app already exposes for live events (mirror `subscribeToAgentObserverFrames` used in `observerRelayStore.ts:3`); kind constants for `NODE_ANNOUNCE`/`AGENT_NODE_STATUS`/presence (add to `desktop/src/shared/constants/kinds.ts` to match `buzz-core/src/kind.rs`).
- Produces:
```ts
export type NodeView = {
  nodePubkey: string; name: string; os: string; runtimes: string[];
  online: boolean; agentCount: number;
};
export type AgentStatusView = { agentPubkey: string; nodePubkey: string; health: string; reason?: string };
export function subscribeNodes(listener: () => void): () => void;
export function getNodesSnapshot(): NodeView[];
export function getAgentStatus(agentPubkey: string): AgentStatusView | undefined;
export function ingestNodeEvent(ev: RelayEvent): void; // exported for tests
export function resetNodesStore(): void;
```

- [ ] **Step 1: Write the failing store test (Vitest).**

```ts
import { describe, it, expect, beforeEach } from "vitest";
import { ingestNodeEvent, getNodesSnapshot, getAgentStatus, resetNodesStore } from "./nodesStore";
import { KIND_NODE_ANNOUNCE, KIND_AGENT_NODE_STATUS } from "@/shared/constants/kinds";

describe("nodesStore", () => {
  beforeEach(() => resetNodesStore());

  it("projects an announce into a NodeView", () => {
    ingestNodeEvent({
      kind: KIND_NODE_ANNOUNCE, pubkey: "n1",
      content: JSON.stringify({ format: "buzz-node-v1", version: 1, node_pubkey: "n1",
        os: "macos", runtimes: ["claude"], workspace_root: "/x" }),
      tags: [["d", "n1"]], created_at: 1,
    } as any);
    const nodes = getNodesSnapshot();
    expect(nodes).toHaveLength(1);
    expect(nodes[0]).toMatchObject({ nodePubkey: "n1", os: "macos", runtimes: ["claude"] });
  });

  it("tracks per-agent status", () => {
    ingestNodeEvent({
      kind: KIND_AGENT_NODE_STATUS, pubkey: "n1",
      content: JSON.stringify({ format: "buzz-node-v1", version: 1, agent_pubkey: "a1",
        node_pubkey: "n1", health: "running", updated_at: "2026-08-29T00:00:00Z" }),
      tags: [["d", "a1"]], created_at: 2,
    } as any);
    expect(getAgentStatus("a1")).toMatchObject({ nodePubkey: "n1", health: "running" });
  });

  it("reset clears everything", () => {
    ingestNodeEvent({ kind: KIND_NODE_ANNOUNCE, pubkey: "n1",
      content: JSON.stringify({ format: "buzz-node-v1", version: 1, node_pubkey: "n1",
        os: "linux", runtimes: [], workspace_root: "/x" }), tags: [["d","n1"]], created_at: 1 } as any);
    resetNodesStore();
    expect(getNodesSnapshot()).toHaveLength(0);
  });
});
```

- [ ] **Step 2: Run to verify it fails.**

Run: `cd desktop && pnpm vitest run src/features/nodes/lib/nodesStore.test.ts`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement `nodesStore.ts`** mirroring the module-singleton shape of `observerRelayStore.ts` (a `Set<listener>`, `Map`s keyed by pubkey, a `notify()` that calls listeners, `subscribe`/`getSnapshot` for `useSyncExternalStore`, and a `reset` that clears the maps + notifies). Reducer: on `KIND_NODE_ANNOUNCE` parse `NodeCapabilities` → upsert `NodeView` (mark `online` from presence, recompute `agentCount` from statuses targeting it); on presence (kind 20001 for a node pubkey) flip `online`; on `KIND_AGENT_NODE_STATUS` parse and upsert `agentStatusByAgent`. Wire the live subscription (mirror `observerRelayStore`'s `subscribeToAgentObserverFrames` call) to feed `ingestNodeEvent`.

- [ ] **Step 4: Register the reset.** In `useCommunityInit.ts`, add `import { resetNodesStore } from "@/features/nodes/lib/nodesStore";` and call `resetNodesStore();` inside `resetCommunityState` (next to `resetAgentObserverStore();`).

- [ ] **Step 5: Run test + typecheck + biome.**

Run: `cd desktop && pnpm vitest run src/features/nodes/lib/nodesStore.test.ts && pnpm tsc --noEmit && pnpm biome check --write src/features/nodes`
Expected: PASS.

- [ ] **Step 6: Commit.**
```bash
git add desktop/src/features/nodes/lib/nodesStore.ts desktop/src/features/nodes/lib/nodesStore.test.ts desktop/src/features/communities/useCommunityInit.ts desktop/src/shared/constants/kinds.ts
git commit -s -m "feat(desktop): nodesStore live store + community reset"
```

---

### Task 4: Nodes panel + enrollment approval

**Files:**
- Create: `desktop/src/features/nodes/ui/NodesPanel.tsx`
- Create: `desktop/src/features/nodes/ui/NodeRow.tsx` (private sub-widget, keeps `NodesPanel` small)
- Create: `desktop/src/shared/api/nodes.ts` (typed `invoke` wrappers: `publishNodeEnrollment`, `publishAgentAssignment`)
- Create: `desktop/tests/e2e/nodes-panel.spec.ts`
- Modify: `desktop/playwright.config.ts` (add `"**/nodes-panel.spec.ts"` to the `smoke` `testMatch`)

**Interfaces:**
- Consumes: `getNodesSnapshot`/`subscribeNodes` (Task 3) via `useSyncExternalStore`; `publishNodeEnrollment` (Task 1 command) via a React Query `useMutation`.
- Produces: `export function NodesPanel(): JSX.Element`.

- [ ] **Step 1: Write the failing Playwright spec.**

```ts
import { test, expect } from "@playwright/test";
import { installMockBridge } from "../helpers/mockBridge";
import { waitForAnimations } from "../helpers/animations";

test("nodes panel lists nodes and shows enrollment", async ({ page }) => {
  await page.addInitScript(() => {
    (window as any).__BUZZ_E2E_MOCK_NODES__ = [
      { nodePubkey: "n1", name: "work-box", os: "linux", runtimes: ["claude","codex"], online: true, agentCount: 2 },
    ];
  });
  await installMockBridge(page);
  await page.goto("/nodes");
  await expect(page.getByText("work-box")).toBeVisible();
  await expect(page.getByText("linux")).toBeVisible();
  await expect(page.getByTestId("node-online-n1")).toBeVisible();
  await waitForAnimations(page);
});
```

- [ ] **Step 2: Build e2e + run to verify it fails.**

Run: `cd desktop && pnpm build:e2e && pnpm exec playwright test tests/e2e/nodes-panel.spec.ts --project=smoke`
Expected: FAIL (route/panel not present).

- [ ] **Step 3: Implement `nodes.ts` invoke wrappers.**

```ts
import { invoke } from "@tauri-apps/api/core";
export const publishNodeEnrollment = (nodePubkey: string) =>
  invoke<string>("publish_node_enrollment", { nodePubkey });
export const publishAgentAssignment = (args: {
  agentId: string; nodePubkey: string;
  launch: { command: string; args: string[]; env: Record<string,string>;
            policyEnv: Record<string,string>; ownerPubkey: string | null };
  assigned: boolean;
}) => invoke<string>("publish_agent_assignment", args);
```

- [ ] **Step 4: Implement `NodesPanel.tsx` + `NodeRow.tsx`.** `NodesPanel` reads nodes via `useSyncExternalStore(subscribeNodes, getNodesSnapshot)`, renders a `NodeRow` per node (name, machine/os, online dot `data-testid={`node-online-${nodePubkey}`}`, runtimes, agent count), and an "Enroll node" affordance that takes a pasted node pubkey and calls a `useMutation(publishNodeEnrollment)`. rem tokens only; add a `/nodes` route in the app router next to the Agents route. Under the E2E mock, seed rows from `window.__BUZZ_E2E_MOCK_NODES__` (mirror how existing panels read mock seed data).

- [ ] **Step 5: Run spec + typecheck + biome.**

Run: `cd desktop && pnpm build:e2e && pnpm exec playwright test tests/e2e/nodes-panel.spec.ts --project=smoke && pnpm tsc --noEmit && pnpm biome check --write src/features/nodes`
Expected: PASS.

- [ ] **Step 6: Commit.**
```bash
git add desktop/src/features/nodes/ui/ desktop/src/shared/api/nodes.ts desktop/tests/e2e/nodes-panel.spec.ts desktop/playwright.config.ts desktop/src/app
git commit -s -m "feat(desktop): Nodes panel + enrollment approval"
```

---

### Task 5: Extend the Run-on picker to assign a node

**Files:**
- Modify: `desktop/src/features/agents/ui/WhereToRunSection.tsx`
- Modify: `desktop/src/features/agents/ui/whereToRunIntent.ts` (add node targets to the draft)
- Create: `desktop/tests/e2e/run-on-node-picker.spec.ts`
- Modify: `desktop/playwright.config.ts` (add the spec to `smoke` `testMatch`)

**Interfaces:**
- Consumes: `getNodesSnapshot`/`subscribeNodes` (Task 3); `publishAgentAssignment` (Task 4 wrapper).
- Produces: an extended `runOnOptions` that includes enrolled **online** nodes; selecting a node target records it in the `WhereToRunDraft` and, on agent save, calls `publishAgentAssignment`.

- [ ] **Step 1: Write the failing spec.**

```ts
import { test, expect } from "@playwright/test";
import { installMockBridge } from "../helpers/mockBridge";

test("run-on picker offers enrolled online nodes", async ({ page }) => {
  await page.addInitScript(() => {
    (window as any).__BUZZ_E2E_MOCK_NODES__ = [
      { nodePubkey: "n1", name: "work-box", os: "linux", runtimes: ["claude"], online: true, agentCount: 0 },
      { nodePubkey: "n2", name: "offline-box", os: "macos", runtimes: [], online: false, agentCount: 0 },
    ];
  });
  await installMockBridge(page);
  await page.goto("/agents");
  await page.getByTestId("create-agent").click();
  await page.getByLabel("Run on").click();
  await expect(page.getByRole("option", { name: /work-box/ })).toBeVisible();     // online node offered
  await expect(page.getByRole("option", { name: /offline-box/ })).toHaveCount(0); // offline excluded
});
```

- [ ] **Step 2: Build e2e + run to verify it fails.**

Run: `cd desktop && pnpm build:e2e && pnpm exec playwright test tests/e2e/run-on-node-picker.spec.ts --project=smoke`
Expected: FAIL (only "This computer" + providers listed).

- [ ] **Step 3: Extend `WhereToRunSection.tsx`.** Add enrolled **online** nodes to `runOnOptions` (label `node.name`, value `node:<nodePubkey>`), read via `useSyncExternalStore(subscribeNodes, getNodesSnapshot)`. Extend `WhereToRunDraft` (`whereToRunIntent.ts`) with a `nodePubkey?: string` when `runOn` is a `node:*` value. Keep the existing provider path untouched. When the agent is saved with a node target, the create/edit flow calls `publishAgentAssignment({ agentId, nodePubkey, launch, assigned: true })` (the `launch` block is the desktop-resolved descriptor the local spawn already builds).

- [ ] **Step 4: Run spec + typecheck + biome + px-text guard.**

Run: `cd desktop && pnpm build:e2e && pnpm exec playwright test tests/e2e/run-on-node-picker.spec.ts --project=smoke && pnpm tsc --noEmit && pnpm biome check --write src/features/agents && pnpm check:px-text`
Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add desktop/src/features/agents/ui/WhereToRunSection.tsx desktop/src/features/agents/ui/whereToRunIntent.ts desktop/tests/e2e/run-on-node-picker.spec.ts desktop/playwright.config.ts
git commit -s -m "feat(desktop): Run-on picker assigns agents to nodes"
```

---

### Task 6: Agent-card status + Start/Stop/Move controls + screenshots

**Files:**
- Create: `desktop/src/features/nodes/ui/AgentNodeStatusBadge.tsx`
- Modify: the agent card component under `desktop/src/features/agents/ui/` (render the badge + controls)
- Create: `desktop/tests/e2e/agent-node-controls.spec.ts`
- Create: `desktop/tests/e2e/nodes-screenshots.spec.ts`
- Modify: `desktop/playwright.config.ts` (add both specs to `smoke` `testMatch`)

**Interfaces:**
- Consumes: `getAgentStatus(agentPubkey)` (Task 3); `publishAgentAssignment` (Task 4).
- Produces: `export function AgentNodeStatusBadge({ agentPubkey }: { agentPubkey: string }): JSX.Element | null`.

- [ ] **Step 1: Write the failing controls spec.**

```ts
import { test, expect } from "@playwright/test";
import { installMockBridge } from "../helpers/mockBridge";

test("agent card shows node status and Move control", async ({ page }) => {
  await page.addInitScript(() => {
    (window as any).__BUZZ_E2E_MOCK_NODES__ = [
      { nodePubkey: "n1", name: "work-box", os: "linux", runtimes: ["claude"], online: true, agentCount: 1 },
    ];
    (window as any).__BUZZ_E2E_MOCK_AGENT_STATUS__ = { a1: { agentPubkey: "a1", nodePubkey: "n1", health: "running" } };
  });
  await installMockBridge(page);
  await page.goto("/agents");
  await expect(page.getByTestId("agent-node-status-a1")).toContainText("work-box");
  await expect(page.getByTestId("agent-node-status-a1")).toContainText(/running/i);
  await expect(page.getByTestId("agent-move-a1")).toBeVisible();
});
```

- [ ] **Step 2: Build e2e + run to verify it fails.**

Run: `cd desktop && pnpm build:e2e && pnpm exec playwright test tests/e2e/agent-node-controls.spec.ts --project=smoke`
Expected: FAIL.

- [ ] **Step 3: Implement the badge + controls.** `AgentNodeStatusBadge` reads `getAgentStatus(agentPubkey)` via `useSyncExternalStore` and renders "running on <node.name> · <health>" (`data-testid={`agent-node-status-${agentPubkey}`}`, rem tokens, health→color). Add Start (assign to last node), Stop (`publishAgentAssignment({…, assigned:false})`), and Move (open node picker → `publishAgentAssignment({…, nodePubkey:newNode, assigned:true})`) controls (`data-testid={`agent-move-${agentPubkey}`}`) wired through React Query mutations.

- [ ] **Step 4: Write the screenshot spec** (`nodes-screenshots.spec.ts`) capturing (a) the Nodes panel and (b) an agent card with a node-status badge, using `waitForAnimations(page)` before each `locator.screenshot()`; write PNGs to `test-results/screenshots/`.

- [ ] **Step 5: Run specs + typecheck + biome + px-text.**

Run: `cd desktop && pnpm build:e2e && pnpm exec playwright test tests/e2e/agent-node-controls.spec.ts tests/e2e/nodes-screenshots.spec.ts --project=smoke && pnpm tsc --noEmit && pnpm biome check --write src && pnpm check:px-text`
Expected: PASS.

- [ ] **Step 6: Verify screenshot distinctness (per AGENTS.md).**

Run: `shasum -a 256 test-results/screenshots/*.png` — Expected: every hash unique (identical hashes mean a spec captured the same state — fix before posting).

- [ ] **Step 7: Commit.**
```bash
git add desktop/src/features/nodes/ui/AgentNodeStatusBadge.tsx desktop/src/features/agents/ui desktop/tests/e2e/agent-node-controls.spec.ts desktop/tests/e2e/nodes-screenshots.spec.ts desktop/playwright.config.ts
git commit -s -m "feat(desktop): agent-card node status + start/stop/move controls"
```

---

## Self-Review

**Spec coverage (§10):**
- Nodes surface (list + enrollment approval) → Task 4 ✓
- Run-on picker → node assignment → Task 5 ✓ (extends `WhereToRunSection.tsx`)
- Status + Start/Stop/Move controls → Task 6 ✓
- "Laptop as a node" (D6): the Nodes panel + enrollment cover approving the laptop-node; the background-service install is a Phase-3 packaging concern (buzz-node daemon), correctly not here.
- Native signing / key never crosses IPC (§12) → Tasks 1–2 ✓ (commands own keys; frontend gets ids).
- Community isolation (reset) → Task 3 ✓.

**Placeholder scan:** No "TBD"/"handle errors"/"similar to". Two explicit *verify-and-match* notes (the `submit_event_with_keys` arg type at `relay.rs:624`; the `agent_keys_for` extraction at `commands/agents.rs:418`) point at exact locations with prescribed actions, not vague gaps.

**Type consistency:** `LaunchInput`/`LaunchBlock` field names (`command`,`args`,`env`,`policy_env`,`owner_pubkey`) match Phase-1's `LaunchBlock` and the `nodes.ts` wrapper (`policyEnv`/`ownerPubkey` are the JS camelCase Tauri auto-maps to `policy_env`/`owner_pubkey`). `AssignmentSecret`/`AssignState`/`assignment::build_assignment`/`assignment::decrypt_for_node` match the Phase-1 interfaces. Store exports (`subscribeNodes`,`getNodesSnapshot`,`getAgentStatus`,`ingestNodeEvent`,`resetNodesStore`) are identical across Task 3's interface, tests, and Tasks 4–6 consumers. `publishNodeEnrollment`/`publishAgentAssignment` names match between the commands (Tasks 1–2), the wrappers (Task 4), and the callers (Tasks 5–6).

**Cross-phase check:** all buzz-core symbols used here (`node::build_enrollment`, `assignment::{build_assignment, decrypt_for_node}`, `AssignmentSecret`, `LaunchBlock`, `AssignState`) are produced by the Phase-1 plan's Tasks 2/3/5 re-exports.

---

## What Phase 5 consumes from this

Phase 5 (move/resilience + two-node e2e) reuses: `publishAgentAssignment(assigned:true|false)` as the **move** primitive (edit `node` → old node stops, new node starts), `nodesStore`/`getAgentStatus` to assert bounded stop-before-start in the UI, and the `nodes-screenshots.spec.ts` harness to capture the move flow. No new desktop command types are required for the move itself — it is an assignment edit.
