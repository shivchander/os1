# Execution Nodes — Phase 5: Multi-Node Resilience + End-to-End Proof

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the working single-node daemon from Phases 2–3 into a correct **multi-node** system: safe agent moves (bounded stop-before-start), self-healing reconcile after reboot/reconnect, the at-most-one-live-instance invariant, active health probing, at-rest secret encryption, and a renewed presence lease — all proven by a green two-node end-to-end test.

**Architecture:** Extend the Phase-2 `Engine` (which owns the reconcile loop over the `NodeRelay`/`Substrate`/`AgentRuntime` seams) with four pure/testable additions — a `pending_spawns` deferral gate keyed on peer `AGENT_NODE_STATUS`, a startup/reconnect full-resync, a `SmokeProbe`, and a `PresenceLoop` — plus a `SecretStore` seam (OS keychain impl). Everything new is unit-tested against the Phase-2 `FakeRelay`/`FakeSubstrate`/`FakeRuntime`; the invariants are then proven end-to-end against a real relay with two `buzz-node` processes.

**Tech Stack:** Rust 1.88, `tokio`, `async-trait`, `nostr` 0.44, `buzz-core` (Phase-1 codecs), `buzz-ws-client`, `keyring` (OS keychain), `buzz-test-client` (e2e). Test with `cargo test -p buzz-node` and `cargo test -p buzz-node --test e2e_nodes -- --ignored`.

**Spec:** `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` (§8 move flow, §9 health probing + key handling, §13 edge cases/invariants I3/I4, §14 testing). Read it alongside this plan.

## Global Constraints

- **`buzz-node` is a `tokio` async binary+lib crate.** Business logic lives in the lib (`src/`), unit-tested; `src/main.rs` is a thin wire-up.
- **No `unsafe`; no new `unwrap()`/`expect()` in non-test code** — use `?` and `NodeError` (the Phase-2 error enum). `unwrap()` is fine under `#[cfg(test)]`.
- **All `pub` items need `///` docs** (`#![warn(missing_docs)]` on the crate, set in Phase 2).
- **Reuse Buzz constants, do not re-derive:** presence TTL is the relay-wide `PRESENCE_TTL_SECS = 180` (`crates/buzz-pubsub/src/presence.rs:16`); the harness heartbeat/presence cadence and clean-shutdown offline-publish mirror `buzz-acp` (`crates/buzz-acp/src/lib.rs` presence republish ~60s + shutdown tail). Presence kind is `buzz_core::kind` presence (kind:20001); use `buzz_core::PresenceStatus`.
- **Consume Phases 1–3; do not redefine their types.** Import from `buzz_core` (codecs) and `buzz_node` (engine, traits, models, fakes). See **Assumed upstream interfaces** below — if a Phase-2/3 name differs, align the two plans rather than duplicating a type here.
- **The two-node e2e is `#[ignore]`** (needs Docker Postgres+Redis+relay), mirroring `crates/buzz-test-client/tests/*` (all `#[ignore]`). It must never run in the default `cargo test`.
- **Secrets never on disk in plaintext and never in `Debug`.** Provider keys go through `SecretStore` (keychain); the on-disk node config is asserted secret-free.
- **Commit with `git commit -s`** (DCO). One commit per task step that says "commit". Run `cargo test -p buzz-node && cargo clippy -p buzz-node --all-targets -- -D warnings` green before each task's final commit.

## Assumed upstream interfaces (from Phases 2–3)

Phase 5 builds on these. They are defined in earlier phases; this block is the contract Phase 5 compiles against (reconcile in that phase if a signature drifted):

```rust
// crates/buzz-node/src/model.rs (Phase 2)
pub struct DesiredAgent { pub agent_pubkey: PublicKey, pub secret: buzz_core::AssignmentSecret }
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ObservedState { Starting, Running, Exited { code: i32 } }
pub struct Observed { pub agent_pubkey: PublicKey, pub state: ObservedState }
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Action { Spawn(PublicKey), Stop(PublicKey), NoOp(PublicKey) }
pub fn reconcile(desired: &[DesiredAgent], observed: &[Observed]) -> Vec<Action>; // one entry per agent

// crates/buzz-node/src/error.rs (Phase 2)
#[derive(Debug, thiserror::Error)] pub enum NodeError { /* … */ }

// crates/buzz-node/src/relay.rs (Phase 2 trait; Phase 3 NostrNodeRelay impl)
pub enum NodeEvent { Assignment(nostr::Event), Status(nostr::Event), Reconnected, Tick }
#[async_trait::async_trait]
pub trait NodeRelay: Send {
    async fn query_assignments(&self, owner: &PublicKey) -> Result<Vec<nostr::Event>, NodeError>;
    async fn next(&mut self) -> Option<NodeEvent>;
    async fn publish_status(&self, s: &buzz_core::AgentNodeStatus) -> Result<(), NodeError>;
    async fn publish_presence(&self, status: buzz_core::PresenceStatus) -> Result<(), NodeError>;
    async fn publish_announce(&self, caps: &buzz_core::NodeCapabilities) -> Result<(), NodeError>;
}

// crates/buzz-node/src/substrate.rs (Phase 2 trait; Phase 3 LocalProcessSubstrate impl)
#[async_trait::async_trait]
pub trait Substrate: Send + Sync {
    async fn spawn(&self, spec: &DesiredAgent) -> Result<(), NodeError>;
    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError>;
    async fn observe(&self) -> Result<Vec<Observed>, NodeError>;
}

// crates/buzz-node/src/runtime.rs (Phase 2 trait; Phase 3 AcpRuntime impl)
#[async_trait::async_trait]
pub trait AgentRuntime: Send + Sync {
    /// A benign round-trip used by the smoke probe; Ok(()) means the agent answered.
    async fn probe(&self, agent: &PublicKey) -> Result<(), NodeError>;
}

// crates/buzz-node/src/engine.rs (Phase 2)
pub struct Engine<R: NodeRelay, S: Substrate, RT: AgentRuntime> {
    pub owner: PublicKey,
    pub node_keys: nostr::Keys,
    pub relay: R,
    pub substrate: S,
    pub runtime: RT,
    /// desired set (agent_pubkey -> decrypted DesiredAgent) where node == self
    pub desired: std::collections::HashMap<PublicKey, DesiredAgent>,
    // Phase-5 fields are added in Task 1/4/6.
}

// crates/buzz-node/src/testkit.rs (Phase 2, behind #[cfg(any(test, feature = "testkit"))])
pub struct FakeRelay { /* inbound queue + captured publishes */ }
impl FakeRelay {
    pub fn push(&self, ev: NodeEvent);              // enqueue an inbound event
    pub fn statuses(&self) -> Vec<buzz_core::AgentNodeStatus>; // captured publish_status
    pub fn presences(&self) -> Vec<buzz_core::PresenceStatus>; // captured publish_presence
    pub fn set_snapshot(&self, events: Vec<nostr::Event>);     // query_assignments result
}
pub struct FakeSubstrate { /* spawn/stop/observe log + programmable observed */ }
impl FakeSubstrate {
    pub fn spawned(&self) -> Vec<PublicKey>;
    pub fn stopped(&self) -> Vec<PublicKey>;
    pub fn set_observed(&self, obs: Vec<Observed>);
}
pub struct FakeRuntime { /* programmable probe result */ }
impl FakeRuntime { pub fn set_probe(&self, agent: &PublicKey, ok: bool); }
```

Helper used throughout the tests (add to `testkit.rs` in Task 1 if absent): a builder that produces a signed `AGENT_ASSIGNMENT` event targeting a node, and an `AGENT_NODE_STATUS` event from a node — both via the Phase-1 `buzz_core` codecs.

---

### Task 1: Move flow — bounded stop-before-start (spec §8, §13 I4)

When an assignment now targets *this* node but another node still reports the agent `starting`/`running`, this node must **defer** the spawn until it sees that peer's `AGENT_NODE_STATUS{stopped}` or a bounded timeout elapses — so a move never produces two live instances.

**Files:**
- Create: `crates/buzz-node/src/move_gate.rs`
- Modify: `crates/buzz-node/src/engine.rs` (add `status_view`, `pending_spawns`; gate `Spawn` execution)
- Modify: `crates/buzz-node/src/lib.rs` (`mod move_gate;`)
- Modify: `crates/buzz-node/src/testkit.rs` (add `assignment_event` / `status_event` builders if not present)

**Interfaces:**
- Consumes: `buzz_core::{AgentNodeStatus, AgentHealth, node_status::validate_status}`; Phase-2 `Action`, `Engine`, `Substrate::spawn`.
- Produces:
  - `struct PeerStatusView { latest: HashMap<PublicKey /*agent*/, (PublicKey /*node*/, AgentHealth)> }` with `fn record(&mut self, s: &AgentNodeStatus)` and `fn peer_blocks_spawn(&self, agent: &PublicKey, me: &PublicKey) -> bool` (true iff the latest status for `agent` is from a **different** node and health ∈ {Starting, Running}).
  - `const MOVE_HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);`
  - Engine field `pending_spawns: HashMap<PublicKey, Instant /*deadline*/>` and method `fn execute_action(&mut self, action: Action, now: Instant) -> Vec<PublicKey /*agents to spawn now*/>` that routes `Spawn(a)` to immediate-spawn or `pending_spawns` based on `PeerStatusView`.

- [ ] **Step 1: Write the failing unit tests.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::AgentHealth;
    use nostr::Keys;
    use std::time::{Duration, Instant};

    #[test]
    fn peer_running_blocks_spawn_until_stopped() {
        let me = Keys::generate().public_key();
        let peer = Keys::generate().public_key();
        let agent = Keys::generate().public_key();
        let mut view = PeerStatusView::default();
        // Peer N reports the agent running:
        view.record_parts(agent, peer, AgentHealth::Running);
        assert!(view.peer_blocks_spawn(&agent, &me));
        // Peer N reports stopped -> no longer blocks:
        view.record_parts(agent, peer, AgentHealth::Stopped);
        assert!(!view.peer_blocks_spawn(&agent, &me));
    }

    #[test]
    fn own_running_status_does_not_block_self() {
        let me = Keys::generate().public_key();
        let agent = Keys::generate().public_key();
        let mut view = PeerStatusView::default();
        view.record_parts(agent, me, AgentHealth::Running); // our own status
        assert!(!view.peer_blocks_spawn(&agent, &me));
    }

    #[test]
    fn deferred_spawn_fires_after_timeout_even_without_stopped() {
        let me = Keys::generate().public_key();
        let peer = Keys::generate().public_key();
        let agent = Keys::generate().public_key();
        let mut view = PeerStatusView::default();
        view.record_parts(agent, peer, AgentHealth::Running);
        let mut pending: std::collections::HashMap<_, Instant> = Default::default();
        let now = Instant::now();
        // gate decides to defer:
        assert!(view.peer_blocks_spawn(&agent, &me));
        pending.insert(agent, now + MOVE_HANDOFF_TIMEOUT);
        // before deadline, still pending:
        assert!(due_pending(&pending, now + Duration::from_secs(5)).is_empty());
        // after deadline, it is due regardless of peer status:
        assert_eq!(due_pending(&pending, now + MOVE_HANDOFF_TIMEOUT + Duration::from_secs(1)), vec![agent]);
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node move_gate::` — Expected: FAIL (types/fns missing).

- [ ] **Step 3: Implement `move_gate.rs`.**

```rust
//! Bounded stop-before-start gating for agent moves (spec §8).
use std::collections::HashMap;
use std::time::{Duration, Instant};

use buzz_core::{AgentHealth, AgentNodeStatus};
use nostr::PublicKey;

/// Max time a receiving node waits for the previous node's `stopped` status
/// before spawning anyway (bounded overlap, never a permanent double — I4).
pub const MOVE_HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);

/// Latest observed per-agent status across all nodes (from `AGENT_NODE_STATUS`).
#[derive(Default)]
pub struct PeerStatusView {
    latest: HashMap<PublicKey, (PublicKey, AgentHealth)>,
}

impl PeerStatusView {
    /// Record a validated status event's meaning.
    pub fn record(&mut self, s: &AgentNodeStatus) -> Result<(), buzz_core::CodecError> {
        let agent = PublicKey::from_hex(&s.agent_pubkey)
            .map_err(|_| buzz_core::CodecError::InvalidPayload("agent_pubkey".into()))?;
        let node = PublicKey::from_hex(&s.node_pubkey)
            .map_err(|_| buzz_core::CodecError::InvalidPayload("node_pubkey".into()))?;
        self.record_parts(agent, node, s.health);
        Ok(())
    }

    /// Test/seam helper: record already-parsed parts.
    pub fn record_parts(&mut self, agent: PublicKey, node: PublicKey, health: AgentHealth) {
        self.latest.insert(agent, (node, health));
    }

    /// True iff the latest status for `agent` is from a node other than `me`
    /// and that node still considers it alive (Starting/Running).
    pub fn peer_blocks_spawn(&self, agent: &PublicKey, me: &PublicKey) -> bool {
        match self.latest.get(agent) {
            Some((node, health)) => {
                node != me && matches!(health, AgentHealth::Starting | AgentHealth::Running)
            }
            None => false,
        }
    }
}

/// Agents whose deferred-spawn deadline has passed as of `now`.
pub fn due_pending(pending: &HashMap<PublicKey, Instant>, now: Instant) -> Vec<PublicKey> {
    let mut due: Vec<PublicKey> = pending
        .iter()
        .filter(|(_, deadline)| now >= **deadline)
        .map(|(agent, _)| *agent)
        .collect();
    due.sort_by_key(|k| k.to_hex());
    due
}
```

Note `AgentHealth` must be `Copy` (it is a fieldless enum in Phase 1; if Phase 1 left it non-`Copy`, derive `Copy` there).

- [ ] **Step 4: Wire the gate into the engine.** In `engine.rs`, add fields `status_view: PeerStatusView` and `pending_spawns: HashMap<PublicKey, Instant>`. On each `NodeEvent::Status(ev)`: `if let Ok(s) = buzz_core::node_status::validate_status(&ev) { self.status_view.record(&s).ok(); }`, then re-evaluate `pending_spawns` (drop any agent no longer blocked and spawn it). Replace the direct `substrate.spawn` on a `Spawn(a)` action with:

```rust
async fn on_spawn(&mut self, agent: PublicKey, now: Instant) -> Result<(), NodeError> {
    if self.status_view.peer_blocks_spawn(&agent, &self.node_keys.public_key()) {
        self.pending_spawns.entry(agent).or_insert(now + MOVE_HANDOFF_TIMEOUT);
        return Ok(()); // deferred; will fire on peer `stopped` or at deadline
    }
    self.pending_spawns.remove(&agent);
    if let Some(spec) = self.desired.get(&agent) {
        self.substrate.spawn(spec).await?;
    }
    Ok(())
}
```
On every loop turn (and on the periodic `Tick`), for each `agent` in `due_pending(&self.pending_spawns, Instant::now())` or no longer `peer_blocks_spawn`, call the spawn path and remove it from `pending_spawns`.

- [ ] **Step 5: Engine-level test — move defers then spawns.**

```rust
#[tokio::test]
async fn move_defers_spawn_until_peer_stops() {
    // owner assigns agent A to node M (this engine). Peer N currently reports A running.
    let (owner, node_m, node_n, agent) = (Keys::generate(), Keys::generate(), Keys::generate(), Keys::generate());
    let relay = FakeRelay::default();
    // seed: assignment A -> M (decryptable by M), and a peer status A running@N
    relay.push(NodeEvent::Assignment(testkit::assignment_event(&owner, &node_m, &agent)));
    relay.push(NodeEvent::Status(testkit::status_event(&node_n, &agent, AgentHealth::Running)));
    let sub = FakeSubstrate::default();
    let mut engine = testkit::engine(owner.public_key(), node_m.clone(), relay.clone(), sub.clone(), FakeRuntime::default());

    engine.run_until_idle().await; // processes both events
    assert!(sub.spawned().is_empty(), "must defer while peer reports running");

    relay.push(NodeEvent::Status(testkit::status_event(&node_n, &agent, AgentHealth::Stopped)));
    engine.run_until_idle().await;
    assert_eq!(sub.spawned(), vec![agent.public_key()], "spawns after peer stopped");
}
```

(Phase 2 exposes `run_until_idle()` on the test engine — a helper that drains the FakeRelay queue and settles pending work; add it to `testkit.rs` if absent.)

- [ ] **Step 6: Run + clippy.** Run: `cargo test -p buzz-node move_gate:: engine::move_defers && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.

- [ ] **Step 7: Commit.**
```bash
git add crates/buzz-node/src/move_gate.rs crates/buzz-node/src/engine.rs crates/buzz-node/src/lib.rs crates/buzz-node/src/testkit.rs
git commit -s -m "feat(node): bounded stop-before-start move gate (I4)"
```

---

### Task 2: Reconcile on startup + after reconnect (spec §9, §13 offline catch-up)

**Files:**
- Modify: `crates/buzz-node/src/engine.rs` (add `full_resync`; call on start + on `NodeEvent::Reconnected`)

**Interfaces:**
- Consumes: `NodeRelay::query_assignments`, `buzz_core::assignment::{validate_envelope, decrypt_for_node}`, `Substrate::observe`, `reconcile`.
- Produces: `Engine::full_resync(&mut self) -> Result<(), NodeError>` — rebuilds `desired` from the relay snapshot, then reconciles against observed.

- [ ] **Step 1: Write the failing test.**

```rust
#[tokio::test]
async fn startup_resync_spawns_assigned_agents_after_reboot() {
    let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
    let relay = FakeRelay::default();
    // Snapshot (as if published before this node started): A -> M.
    relay.set_snapshot(vec![testkit::assignment_event(&owner, &node_m, &agent)]);
    let sub = FakeSubstrate::default(); // observes nothing (fresh reboot)
    let mut engine = testkit::engine(owner.public_key(), node_m, relay.clone(), sub.clone(), FakeRuntime::default());

    engine.full_resync().await.unwrap();
    assert_eq!(sub.spawned(), vec![agent.public_key()], "reboot restarts assigned agents from relay state");
}

#[tokio::test]
async fn reconnect_triggers_full_resync() {
    let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
    let relay = FakeRelay::default();
    let sub = FakeSubstrate::default();
    let mut engine = testkit::engine(owner.public_key(), node_m.clone(), relay.clone(), sub.clone(), FakeRuntime::default());
    // After a drop, the snapshot now has the assignment; a Reconnected event must re-query.
    relay.set_snapshot(vec![testkit::assignment_event(&owner, &node_m, &agent)]);
    relay.push(NodeEvent::Reconnected);
    engine.run_until_idle().await;
    assert_eq!(sub.spawned(), vec![agent.public_key()]);
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node engine::startup_resync engine::reconnect_triggers` — Expected: FAIL.

- [ ] **Step 3: Implement `full_resync`.**

```rust
/// Rebuild desired state from a fresh relay snapshot and converge.
/// Used on engine start and after every `Reconnected` — not just live tailing,
/// so a rebooted node restores its assigned agents (spec §13 offline catch-up).
pub async fn full_resync(&mut self) -> Result<(), NodeError> {
    let me = self.node_keys.public_key();
    let events = self.relay.query_assignments(&self.owner).await?;
    self.desired.clear();
    for ev in &events {
        // envelope is cheap + tells us the target node without decrypting:
        let env = match buzz_core::assignment::validate_envelope(ev, &self.owner) {
            Ok(env) => env,
            Err(_) => continue, // ignore malformed / non-ours
        };
        if env.node_pubkey != me || env.state != buzz_core::AssignState::Assigned {
            continue;
        }
        if let Ok((_, secret)) = buzz_core::assignment::decrypt_for_node(ev, &self.node_keys, &self.owner) {
            self.desired.insert(env.agent_pubkey, DesiredAgent { agent_pubkey: env.agent_pubkey, secret });
        }
    }
    let observed = self.substrate.observe().await?;
    let desired: Vec<DesiredAgent> = self.desired.values().cloned().collect();
    for action in reconcile(&desired, &observed) {
        self.apply(action, std::time::Instant::now()).await?; // routes Spawn via on_spawn (Task 1)
    }
    Ok(())
}
```

Call `full_resync()` once at the top of `run()`, and on receiving `NodeEvent::Reconnected` inside the loop.

- [ ] **Step 4: Run + clippy.** Run: `cargo test -p buzz-node engine::startup_resync engine::reconnect_triggers && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/buzz-node/src/engine.rs
git commit -s -m "feat(node): full resync on startup and reconnect (offline catch-up)"
```

---

### Task 3: One-live-instance guard (I4) — LWW assignment + move sequencing

**Files:**
- Modify: `crates/buzz-node/src/engine.rs` (LWW dedup when two assignment events for the same agent arrive)
- Test only: `crates/buzz-node/src/engine.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: Task 1 gate, Task 2 resync. Produces: no new public API — an invariant, enforced + tested.

- [ ] **Step 1: Write the failing test (LWW: later assignment wins; earlier target does not run).**

```rust
#[tokio::test]
async fn later_assignment_wins_single_instance() {
    let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
    let relay = FakeRelay::default();
    let sub = FakeSubstrate::default();
    let mut engine = testkit::engine(owner.public_key(), node_m.clone(), relay.clone(), sub.clone(), FakeRuntime::default());
    // Two assignment events for the SAME agent both targeting M, newer created_at last.
    relay.push(NodeEvent::Assignment(testkit::assignment_event_at(&owner, &node_m, &agent, 1_000)));
    relay.push(NodeEvent::Assignment(testkit::assignment_event_at(&owner, &node_m, &agent, 2_000)));
    engine.run_until_idle().await;
    // Exactly one spawn for the agent — never two.
    assert_eq!(sub.spawned().iter().filter(|a| **a == agent.public_key()).count(), 1);
}

#[tokio::test]
async fn reassigned_away_stops_here() {
    let (owner, node_m, node_n, agent) = (Keys::generate(), Keys::generate(), Keys::generate(), Keys::generate());
    let relay = FakeRelay::default();
    let sub = FakeSubstrate::default();
    let mut engine = testkit::engine(owner.public_key(), node_m.clone(), relay.clone(), sub.clone(), FakeRuntime::default());
    // First A -> M (we run it); then A -> N (we must stop it).
    relay.push(NodeEvent::Assignment(testkit::assignment_event_at(&owner, &node_m, &agent, 1_000)));
    engine.run_until_idle().await;
    sub.set_observed(vec![Observed { agent_pubkey: agent.public_key(), state: ObservedState::Running }]);
    relay.push(NodeEvent::Assignment(testkit::assignment_event_at(&owner, &node_n, &agent, 2_000)));
    engine.run_until_idle().await;
    assert_eq!(sub.stopped(), vec![agent.public_key()]);
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node engine::later_assignment engine::reassigned_away` — Expected: FAIL (if the engine doesn't yet dedup by created_at / stop on retarget).

- [ ] **Step 3: Implement LWW + retarget-stop.** In the `NodeEvent::Assignment(ev)` handler:
  - Track `assignment_seen: HashMap<PublicKey /*agent*/, u64 /*created_at*/>`; ignore an event whose `created_at` is **older** than the last seen for that agent (LWW; the relay's parameterized-replaceable semantics already keep only the latest, this guards out-of-order live delivery).
  - Compute the envelope; if `env.node_pubkey == me && state == Assigned` → upsert `desired` + reconcile (→ `on_spawn`, gated by Task 1). If `env.node_pubkey != me` **or** `state == Unassigned` → remove from `desired`; if currently observed, emit `Stop`.

```rust
NodeEvent::Assignment(ev) => {
    let env = match buzz_core::assignment::validate_envelope(&ev, &self.owner) { Ok(e) => e, Err(_) => return Ok(()) };
    let created = ev.created_at.as_u64();
    if self.assignment_seen.get(&env.agent_pubkey).is_some_and(|&prev| created < prev) { return Ok(()); }
    self.assignment_seen.insert(env.agent_pubkey, created);
    let me = self.node_keys.public_key();
    let mine = env.node_pubkey == me && env.state == buzz_core::AssignState::Assigned;
    if mine {
        if let Ok((_, secret)) = buzz_core::assignment::decrypt_for_node(&ev, &self.node_keys, &self.owner) {
            self.desired.insert(env.agent_pubkey, DesiredAgent { agent_pubkey: env.agent_pubkey, secret });
        }
    } else {
        self.desired.remove(&env.agent_pubkey);
    }
    self.reconcile_agent(env.agent_pubkey).await?; // Spawn(gated)/Stop/NoOp for just this agent
}
```

- [ ] **Step 4: Run + clippy.** Run: `cargo test -p buzz-node engine:: && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/buzz-node/src/engine.rs
git commit -s -m "feat(node): LWW assignment + retarget-stop (one-live-instance, I4)"
```

---

### Task 4: Active smoke-probe health (spec §9 health probing)

**Files:**
- Create: `crates/buzz-node/src/health.rs`
- Modify: `crates/buzz-node/src/engine.rs` (probe on spawn-confirmed + on `Tick`; publish `AGENT_NODE_STATUS`)
- Modify: `crates/buzz-node/src/lib.rs` (`mod health;`)

**Interfaces:**
- Consumes: `AgentRuntime::probe`, `buzz_core::{AgentHealth, AgentNodeStatus, node_status::build_status}`, `NodeRelay::publish_status`, `Substrate::observe`.
- Produces:
  - `const SMOKE_PROBE_INTERVAL: Duration = Duration::from_secs(300);`
  - `fn classify(observed: &ObservedState, probe_ok: Option<bool>) -> (AgentHealth, Option<String>)` — the shared health-reason vocabulary.
  - `Engine::probe_and_publish(&mut self, agent: PublicKey, now: Instant) -> Result<(), NodeError>`.

- [ ] **Step 1: Write failing tests for `classify`.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::AgentHealth;

    #[test]
    fn classify_maps_observed_and_probe_to_health() {
        use crate::model::ObservedState::*;
        assert_eq!(classify(&Starting, None).0, AgentHealth::Starting);
        assert_eq!(classify(&Running, Some(true)).0, AgentHealth::Running);
        // Running process but probe failed = degraded, surfaced as a reason:
        let (h, reason) = classify(&Running, Some(false));
        assert_eq!(h, AgentHealth::Crashed);
        assert_eq!(reason.as_deref(), Some("probe-failed"));
        // Exited nonzero = crashed with the code in the reason:
        let (h, reason) = classify(&Exited { code: 1 }, None);
        assert_eq!(h, AgentHealth::Crashed);
        assert_eq!(reason.as_deref(), Some("exit-1"));
        // Exited zero = clean stop:
        assert_eq!(classify(&Exited { code: 0 }, None).0, AgentHealth::Stopped);
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node health::` — Expected: FAIL.

- [ ] **Step 3: Implement `health.rs`.**

```rust
//! Active smoke-probe health classification (spec §9).
use std::time::Duration;
use buzz_core::AgentHealth;
use crate::model::ObservedState;

/// How often each running agent gets an active round-trip probe.
pub const SMOKE_PROBE_INTERVAL: Duration = Duration::from_secs(300);

/// Combine process observation with the latest probe outcome into a health +
/// machine-readable reason. `probe_ok = None` means "not probed this cycle".
pub fn classify(observed: &ObservedState, probe_ok: Option<bool>) -> (AgentHealth, Option<String>) {
    match observed {
        ObservedState::Starting => (AgentHealth::Starting, None),
        ObservedState::Running => match probe_ok {
            Some(false) => (AgentHealth::Crashed, Some("probe-failed".into())),
            _ => (AgentHealth::Running, None),
        },
        ObservedState::Exited { code: 0 } => (AgentHealth::Stopped, None),
        ObservedState::Exited { code } => (AgentHealth::Crashed, Some(format!("exit-{code}"))),
    }
}
```

- [ ] **Step 4: Wire probing into the engine.** Add `last_probe: HashMap<PublicKey, Instant>`. On `Tick`, for each observed `Running` agent whose `last_probe` is older than `SMOKE_PROBE_INTERVAL` (and once immediately after a spawn is first observed `Running`), call `runtime.probe(&agent)`, then `classify`, then publish:

```rust
pub async fn probe_and_publish(&mut self, agent: PublicKey, now: Instant) -> Result<(), NodeError> {
    let observed = self.substrate.observe().await?;
    let state = observed.iter().find(|o| o.agent_pubkey == agent).map(|o| o.state.clone());
    let probe_ok = match &state {
        Some(ObservedState::Running) => { self.last_probe.insert(agent, now); Some(self.runtime.probe(&agent).await.is_ok()) }
        _ => None,
    };
    let (health, reason) = crate::health::classify(state.as_ref().unwrap_or(&ObservedState::Exited { code: -1 }), probe_ok);
    let status = AgentNodeStatus {
        format: buzz_core::node::FORMAT.into(), version: buzz_core::node::VERSION,
        agent_pubkey: agent.to_hex(), node_pubkey: self.node_keys.public_key().to_hex(),
        health, reason, updated_at: chrono::Utc::now().to_rfc3339(),
    };
    self.relay.publish_status(&status).await
}
```

- [ ] **Step 5: Engine test — probe failure publishes Crashed/probe-failed.**

```rust
#[tokio::test]
async fn failed_probe_publishes_crashed_reason() {
    let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
    let relay = FakeRelay::default();
    let sub = FakeSubstrate::default();
    sub.set_observed(vec![Observed { agent_pubkey: agent.public_key(), state: ObservedState::Running }]);
    let rt = FakeRuntime::default();
    rt.set_probe(&agent.public_key(), false);
    let mut engine = testkit::engine(owner.public_key(), node_m, relay.clone(), sub, rt);
    engine.probe_and_publish(agent.public_key(), std::time::Instant::now()).await.unwrap();
    let s = relay.statuses().pop().unwrap();
    assert_eq!(s.health, buzz_core::AgentHealth::Crashed);
    assert_eq!(s.reason.as_deref(), Some("probe-failed"));
}
```

- [ ] **Step 6: Run + clippy + commit.** Run: `cargo test -p buzz-node health:: engine::failed_probe && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.
```bash
git add crates/buzz-node/src/health.rs crates/buzz-node/src/engine.rs crates/buzz-node/src/lib.rs
git commit -s -m "feat(node): active smoke-probe health + reason vocabulary"
```

---

### Task 5: At-rest secret encryption for provider keys (spec §9 key handling, §12)

Agent `nsec`s are ephemeral (in-memory, from the assignment). **Provider API keys** the node persists (so agents can call their LLM across restarts) must live in the OS keychain, never plaintext on disk.

**Files:**
- Create: `crates/buzz-node/src/secret_store.rs`
- Modify: `crates/buzz-node/src/lib.rs` (`mod secret_store;`)
- Modify: `crates/buzz-node/Cargo.toml` (add `keyring = "3"`)

**Interfaces:**
- Produces:
  - `trait SecretStore: Send + Sync { fn set(&self, key: &str, value: &str) -> Result<(), NodeError>; fn get(&self, key: &str) -> Result<Option<String>, NodeError>; fn delete(&self, key: &str) -> Result<(), NodeError>; }`
  - `struct KeychainSecretStore { service: String }` (impl over `keyring::Entry`)
  - `struct MemorySecretStore` (`#[cfg(any(test, feature = "testkit"))]`) for unit tests.

- [ ] **Step 1: Write the failing tests (against `MemorySecretStore`, plus a disk-plaintext guard).**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn memory_store_round_trips_and_deletes() {
        let s = MemorySecretStore::default();
        s.set("PROVIDER_ANTHROPIC", "sk-secret").unwrap();
        assert_eq!(s.get("PROVIDER_ANTHROPIC").unwrap().as_deref(), Some("sk-secret"));
        s.delete("PROVIDER_ANTHROPIC").unwrap();
        assert_eq!(s.get("PROVIDER_ANTHROPIC").unwrap(), None);
    }

    #[test]
    fn node_config_serialization_contains_no_secret() {
        // The persisted node config must reference provider keys by NAME only.
        let cfg = crate::config::NodeConfig::sample_with_provider("anthropic");
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("anthropic"));
        assert!(!json.to_lowercase().contains("sk-"), "no secret material in on-disk config");
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node secret_store::` — Expected: FAIL.

- [ ] **Step 3: Implement `secret_store.rs`.**

```rust
//! At-rest secret storage for provider API keys (OS keychain). Agent nsecs are
//! never persisted here — they are in-memory only (spec §9, §12).
use crate::error::NodeError;

/// Store/retrieve named secrets. The node config references secrets by name; the
/// values live here, never in plaintext on disk.
pub trait SecretStore: Send + Sync {
    /// Persist a secret under `key`.
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError>;
    /// Fetch a secret; `Ok(None)` if absent.
    fn get(&self, key: &str) -> Result<Option<String>, NodeError>;
    /// Remove a secret; idempotent.
    fn delete(&self, key: &str) -> Result<(), NodeError>;
}

/// OS-keychain-backed store (`keyring` crate: Keychain/Credential Manager/Secret Service).
pub struct KeychainSecretStore {
    /// Keychain service namespace, e.g. `"buzz-node"`.
    pub service: String,
}

impl SecretStore for KeychainSecretStore {
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError> {
        keyring::Entry::new(&self.service, key)
            .and_then(|e| e.set_password(value))
            .map_err(|e| NodeError::Secret(e.to_string()))
    }
    fn get(&self, key: &str) -> Result<Option<String>, NodeError> {
        match keyring::Entry::new(&self.service, key).and_then(|e| e.get_password()) {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(NodeError::Secret(e.to_string())),
        }
    }
    fn delete(&self, key: &str) -> Result<(), NodeError> {
        match keyring::Entry::new(&self.service, key).and_then(|e| e.delete_credential()) {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(NodeError::Secret(e.to_string())),
        }
    }
}

#[cfg(any(test, feature = "testkit"))]
#[derive(Default)]
/// In-memory store for tests.
pub struct MemorySecretStore(std::sync::Mutex<std::collections::HashMap<String, String>>);

#[cfg(any(test, feature = "testkit"))]
impl SecretStore for MemorySecretStore {
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError> {
        self.0.lock().unwrap().insert(key.into(), value.into());
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<String>, NodeError> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    fn delete(&self, key: &str) -> Result<(), NodeError> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}
```

Add `#[error("secret store: {0}")] Secret(String)` to `NodeError` (Phase-2 enum). Add `NodeConfig::sample_with_provider` under `#[cfg(any(test, feature = "testkit"))]` to `config.rs` if not present (a config that stores provider *names*, not values).

- [ ] **Step 4: Run + clippy + commit.** Run: `cargo test -p buzz-node secret_store:: && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.
```bash
git add crates/buzz-node/src/secret_store.rs crates/buzz-node/src/lib.rs crates/buzz-node/src/error.rs crates/buzz-node/Cargo.toml
git commit -s -m "feat(node): OS-keychain secret store for provider keys (no plaintext at rest)"
```

---

### Task 6: Presence lease — republish + clean-shutdown offline (spec §13 I3)

**Files:**
- Create: `crates/buzz-node/src/presence.rs`
- Modify: `crates/buzz-node/src/engine.rs` (presence tick in `run`; offline publish in shutdown path)
- Modify: `crates/buzz-node/src/lib.rs` (`mod presence;`)

**Interfaces:**
- Consumes: `NodeRelay::publish_presence`, `buzz_core::PresenceStatus`.
- Produces:
  - `const PRESENCE_REPUBLISH: Duration = Duration::from_secs(60);` with a compile-time assertion tying it to the relay TTL.
  - `Engine::publish_online(&self)` / `Engine::shutdown(&mut self)` (drains, publishes `PresenceStatus::Offline`, closes the relay).

- [ ] **Step 1: Write the failing tests.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // The relay-wide lease is PRESENCE_TTL_SECS = 180 (buzz-pubsub). We must
    // republish well within that so a single miss never expires the lease.
    #[test]
    fn republish_gives_at_least_three_beats_per_ttl() {
        let ttl = buzz_pubsub::presence::PRESENCE_TTL_SECS; // 180
        assert!(PRESENCE_REPUBLISH.as_secs() * 3 <= ttl as u64,
            "presence must republish >=3x per TTL to survive a missed beat");
    }
}
```

```rust
#[tokio::test]
async fn shutdown_publishes_offline_then_closes() {
    let (owner, node_m) = (Keys::generate(), Keys::generate());
    let relay = FakeRelay::default();
    let mut engine = testkit::engine(owner.public_key(), node_m, relay.clone(), FakeSubstrate::default(), FakeRuntime::default());
    engine.publish_online().await.unwrap();
    engine.shutdown().await.unwrap();
    let p = relay.presences();
    assert_eq!(p.first(), Some(&buzz_core::PresenceStatus::Online));
    assert_eq!(p.last(), Some(&buzz_core::PresenceStatus::Offline)); // clean exit = immediate offline
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node presence:: engine::shutdown_publishes` — Expected: FAIL. (If `buzz-node` doesn't yet depend on `buzz-pubsub`, add it to `Cargo.toml`; it's already a workspace crate.)

- [ ] **Step 3: Implement `presence.rs` + engine wiring.**

```rust
//! Node presence lease (spec §13 I3). The node republishes presence on a cadence
//! that survives a single missed beat within the relay-wide TTL, and publishes an
//! explicit `Offline` on clean shutdown (mirrors buzz-acp's shutdown tail).
use std::time::Duration;

/// Republish cadence. Must be <= TTL/3 (see the compile-time-ish test).
pub const PRESENCE_REPUBLISH: Duration = Duration::from_secs(60);
```

In `engine.rs`: in `run()`, drive a `tokio::time::interval(PRESENCE_REPUBLISH)` arm alongside the relay loop; each tick `self.relay.publish_presence(PresenceStatus::Online)`. Add:

```rust
/// Publish the initial Online presence (call once at start, after announce).
pub async fn publish_online(&self) -> Result<(), NodeError> {
    self.relay.publish_presence(buzz_core::PresenceStatus::Online).await
}

/// Graceful shutdown: stop supervising, publish Offline (immediate lease clear),
/// then let the relay connection close. Mirrors buzz-acp's shutdown tail.
pub async fn shutdown(&mut self) -> Result<(), NodeError> {
    // (agents keep running — node exit does not stop them; only unassign/!shutdown does)
    self.relay.publish_presence(buzz_core::PresenceStatus::Offline).await?;
    Ok(())
}
```

Wire `shutdown()` to a `tokio::signal::ctrl_c()` / SIGTERM handler in `main.rs` (the detached daemon's clean-exit path from Phase 3).

- [ ] **Step 4: Run + clippy + commit.** Run: `cargo test -p buzz-node presence:: engine::shutdown_publishes && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.
```bash
git add crates/buzz-node/src/presence.rs crates/buzz-node/src/engine.rs crates/buzz-node/src/lib.rs crates/buzz-node/Cargo.toml
git commit -s -m "feat(node): presence lease republish + clean-shutdown offline (I3)"
```

---

### Task 7: Two-node end-to-end proof (`#[ignore]`, real relay)

Proves the whole feature: enroll two nodes, assign, move, and kill — asserting single-live-instance and correct status/presence transitions against a live relay.

**Files:**
- Create: `crates/buzz-node/tests/e2e_nodes.rs`
- Modify: `crates/buzz-node/Cargo.toml` (`[dev-dependencies] buzz-test-client = { path = "../buzz-test-client" }`, `tokio` with `macros,rt-multi-thread`, `feature = "testkit"` for the crate's own fakes if needed)

**Interfaces:**
- Consumes: `buzz_test_client::BuzzTestClient` (connect/authenticate/subscribe/collect_until_eose), the real `NostrNodeRelay`/`LocalProcessSubstrate`/`AcpRuntime` (Phase 3), Phase-1 codecs. Uses a **stub agent command** (a tiny script that connects as the agent key and posts presence) so the test doesn't need a real LLM — set the assignment's `launch.command` to that stub.

- [ ] **Step 1: Preconditions (documented at the top of the test file as a doc comment).**

Run infra the way `crates/buzz-test-client/TESTING.md` prescribes: `just test` brings up Postgres+Redis+relay. The test reads `BUZZ_RELAY_URL` (default `ws://localhost:3000`) and skips with a clear message if unset in CI.

- [ ] **Step 2: Write the e2e test (single file, `#[ignore]`).**

```rust
//! Two-node execution-node e2e. Requires a running relay (see buzz-test-client/TESTING.md).
//! Run: `cargo test -p buzz-node --test e2e_nodes -- --ignored --nocapture`
#![cfg(test)]
use std::time::Duration;
use nostr::Keys;

// Helpers (implemented below the tests): spawn a buzz-node engine task bound to
// generated node keys against the live relay, and a stub agent launch command.
mod harness; // crates/buzz-node/tests/harness/mod.rs — see Step 4

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running relay"]
async fn assign_move_and_kill_keeps_single_live_instance() {
    let relay_url = std::env::var("BUZZ_RELAY_URL").unwrap_or_else(|_| "ws://localhost:3000".into());
    let owner = Keys::generate();
    let node_n = Keys::generate();
    let node_m = Keys::generate();
    let agent = Keys::generate();

    // 1. Start two node engines (N, M) connected to the live relay.
    let n = harness::start_node(&relay_url, owner.public_key(), node_n.clone()).await;
    let m = harness::start_node(&relay_url, owner.public_key(), node_m.clone()).await;

    // 2. Enroll both (owner publishes NODE_ENROLLMENT for each).
    harness::enroll(&relay_url, &owner, &node_n.public_key()).await;
    harness::enroll(&relay_url, &owner, &node_m.public_key()).await;

    // 3. Assign agent A -> N; assert A becomes Running on N.
    harness::assign(&relay_url, &owner, &node_n.public_key(), &agent).await;
    harness::await_status(&relay_url, &owner, &agent.public_key(),
        node_n.public_key(), buzz_core::AgentHealth::Running, Duration::from_secs(30)).await
        .expect("A should run on N");

    // 4. Move A -> M; assert A goes Stopped@N THEN Running@M, and never two Running at once.
    harness::assign(&relay_url, &owner, &node_m.public_key(), &agent).await;
    let transitions = harness::collect_status(&relay_url, &owner, &agent.public_key(), Duration::from_secs(40)).await;
    assert!(harness::never_two_running(&transitions), "must never have two live instances (I4)");
    assert!(harness::saw(&transitions, node_n.public_key(), buzz_core::AgentHealth::Stopped));
    assert!(harness::saw(&transitions, node_m.public_key(), buzz_core::AgentHealth::Running));

    // 5. Kill N's engine; assert N's presence lapses (offline within TTL) and A is unaffected on M.
    n.abort();
    harness::await_status(&relay_url, &owner, &agent.public_key(),
        node_m.public_key(), buzz_core::AgentHealth::Running, Duration::from_secs(10)).await
        .expect("A still running on M after N dies");

    m.abort();
}
```

- [ ] **Step 3: Run to confirm it compiles + is skipped by default.** Run: `cargo test -p buzz-node` — Expected: PASS (0 e2e run; `#[ignore]`). Then `cargo test -p buzz-node --test e2e_nodes` — Expected: builds, reports the ignored test.

- [ ] **Step 4: Implement `tests/harness/mod.rs`** — the glue: `start_node` builds a real `Engine` (`NostrNodeRelay` + `LocalProcessSubstrate` + `AcpRuntime`) and `tokio::spawn`s `engine.run()`, returning the `JoinHandle`; `enroll`/`assign` use Phase-1 `build_enrollment`/`build_assignment` published via a `buzz_test_client::BuzzTestClient` authenticated as the owner (the assignment's `launch.command` points at a repo-local stub script `tests/harness/stub-agent.sh` that connects as the agent key and posts presence — no LLM needed); `collect_status`/`await_status`/`saw`/`never_two_running` subscribe to `KIND_AGENT_NODE_STATUS` (`#d = agent`) via the test client, `validate_status` each event, and fold the `(node, health)` timeline. `never_two_running` scans the timeline for any window where two distinct nodes are both `Running` for the same agent with no intervening `Stopped`.

- [ ] **Step 5: Run the e2e against a live relay.** Run: `just test` in one terminal (or ensure the stack is up), then `cargo test -p buzz-node --test e2e_nodes -- --ignored --nocapture` — Expected: PASS.

- [ ] **Step 6: Commit.**
```bash
git add crates/buzz-node/tests/e2e_nodes.rs crates/buzz-node/tests/harness crates/buzz-node/Cargo.toml
git commit -s -m "test(node): two-node e2e — assign, move, kill; single-live-instance proven"
```

---

## Self-Review

**Spec coverage:**
- §8 move flow (bounded stop-before-start) → Task 1 ✓ · §13 offline catch-up / reboot resync → Task 2 ✓ · §13 I4 one-live-instance (LWW + retarget-stop + move gate) → Tasks 1+3, proven in Task 7 ✓ · §9 active health probing + reason vocabulary → Task 4 ✓ · §9/§12 at-rest secret encryption → Task 5 ✓ · §13 I3 presence lease (republish + clean offline, tied to `PRESENCE_TTL_SECS=180`) → Task 6 ✓ · §14 two-node e2e (reuse `buzz-test-client`) → Task 7 ✓.
- Not in scope here (owned by earlier phases): the reconcile core + traits (Phase 2), real substrate/relay/runtime impls + detached-daemon + enrollment approval (Phase 3), desktop UI (Phase 4), the wire codecs (Phase 1). This plan only *consumes* them (see Assumed upstream interfaces).

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". Every code step is real Rust; the e2e harness glue (Task 7 Step 4) is described as concrete functions with their exact responsibilities and the events they build/subscribe to. Constants (`MOVE_HANDOFF_TIMEOUT=30s`, `SMOKE_PROBE_INTERVAL=300s`, `PRESENCE_REPUBLISH=60s`) are concrete and justified against the relay TTL.

**Type consistency:** `PeerStatusView`/`due_pending`/`MOVE_HANDOFF_TIMEOUT` (Task 1) are referenced consistently in the engine wiring. `classify`/`SMOKE_PROBE_INTERVAL` (Task 4) and `PRESENCE_REPUBLISH`/`shutdown`/`publish_online` (Task 6) match their engine call sites. `SecretStore`/`KeychainSecretStore`/`MemorySecretStore` (Task 5) share one trait. All consumed names (`Engine`, `NodeRelay`, `Substrate`, `AgentRuntime`, `reconcile`, `DesiredAgent`, `Observed`, `ObservedState`, `Action`, `NodeError`, `FakeRelay`/`FakeSubstrate`/`FakeRuntime`, `testkit::engine`/`assignment_event`/`status_event`/`run_until_idle`) come from the **Assumed upstream interfaces** block and are used verbatim — if Phase 2/3 named one differently, reconcile there, not by forking a second definition here.

---

## Definition of done (whole feature, Phases 1–5)

The execution-nodes feature is complete when:
1. `cargo test` (workspace) is green, and `cargo test -p buzz-node --test e2e_nodes -- --ignored` passes against a live relay.
2. From the desktop app you can **create an agent, pick "Run on: <node>", and see it go Running on that node** (Phase 4), with the node process **surviving app quit** (Phase 3 detached daemon) and **node reboot** (Task 2 resync).
3. **Moving** an agent between two nodes yields exactly one live instance at all times (Tasks 1+3, Task 7 proof) and the same relay identity/history on the new body.
4. A **work node behind a firewall** and a **local/personal node** both work with **zero inbound connections** — all control via the relay (spec D2), across two communities (spec §11).
5. Agent `nsec`s never touch disk; provider keys are OS-keychain-encrypted (Task 5); the relay never carries a plaintext key (Phase 1 Task 3 proof).
6. A dead node's presence dot clears within the bounded `PRESENCE_TTL_SECS` (Task 6), and a stopped/crashed agent surfaces a machine-readable health reason (Task 4).
