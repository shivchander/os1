# Execution Nodes — Phase 2: `buzz-node` Reconciler Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the `buzz-node` crate and its pure reconcile brain + async engine loop, all tested against in-memory fakes (no real processes, no real relay), so Phase 3 can plug a real local-process substrate and a real relay client behind stable traits.

**Architecture:** Follows the Kubernetes-controller shape lifted from `buzz-backend-kubernetes`: a **pure** `reconcile(desired, observed) -> Vec<Action>` function (fully unit-testable, one action per agent, deterministic), behind it a `Substrate` trait (start/stop/observe the local process table) and a `NodeRelay` trait (stream desired-state in, publish status/announce/presence out), and an `engine::run()` loop that ties them together (on each new desired-set or periodic tick: observe → reconcile → apply → publish status). Phase 2 ships fakes for both traits; Phase 3 ships the real impls.

**Tech Stack:** Rust 1.88, `tokio` (rt-multi-thread, macros, time, sync), `async-trait`, `buzz-core` (Phase 1 codecs), `thiserror`, `chrono`, `tracing`. Test with `cargo test -p buzz-node`.

**Spec:** `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` — §8 (assign/move/resilience flows), §9 (node internals: reconcile loop, `Substrate` trait, supervision), §13 (edge cases: crash→restart, offline→catch-up), §15 (reuse map).

## Global Constraints

- **New crate `buzz-node`** at `crates/buzz-node/`, added to root `Cargo.toml` `[workspace.members]`.
- **`#![deny(unsafe_code)]`** and **`#![warn(missing_docs)]`** at the crate root — no `unsafe`; every `pub` item documented.
- **No `unwrap()`/`expect()` in non-test code.** Use `?` and `model::NodeError`. `unwrap()` is fine in `#[cfg(test)]` and in fakes gated behind `#[cfg(any(test, feature = "test-utils"))]`.
- **The reconcile function is PURE** — no I/O, no clock, no async. All time/process/network lives behind the `Substrate`/`NodeRelay` traits. This is what makes it exhaustively unit-testable.
- **Consume Phase 1 types** from `buzz_core`: `AssignmentSecret`, `AssignState`, `AgentNodeStatus`, `AgentHealth`, `NodeCapabilities`, `LaunchBlock`, and the `assignment`/`node_status` module consts. Do not redefine wire types here.
- **Reuse workspace dependency versions** — declare deps as `{ workspace = true }` when the crate is already in root `[workspace.dependencies]` (`nostr`, `tokio`, `thiserror`, `serde`, `tracing`, `chrono`). For `async-trait`, reuse the workspace version if present, else pin `async-trait = "0.1"` (and add it to `[workspace.dependencies]`).
- **Commit with `git commit -s`** (DCO). Run `cargo test -p buzz-node` and `cargo clippy -p buzz-node --all-targets -- -D warnings` green before each task's final commit.
- Branch: work on `spec/execution-nodes` (or a child branch); do not commit to `main`.

---

### Task 1: Create the `buzz-node` crate skeleton

**Files:**
- Modify: `Cargo.toml` (root — add `"crates/buzz-node"` to `[workspace.members]`; add `async-trait` to `[workspace.dependencies]` if absent)
- Create: `crates/buzz-node/Cargo.toml`
- Create: `crates/buzz-node/src/lib.rs`
- Create: `crates/buzz-node/src/main.rs`

**Interfaces:**
- Produces: the crate `buzz-node` (lib + bin), compiling empty. Modules `model`, `reconcile`, `substrate`, `relay`, `engine` declared (created in later tasks).

- [ ] **Step 1: Add the crate to the workspace.** Edit root `Cargo.toml` `[workspace.members]` — add the line `"crates/buzz-node",` next to the other `crates/*` members. If `async-trait` is not already under `[workspace.dependencies]`, add `async-trait = "0.1"` there.

- [ ] **Step 2: Create `crates/buzz-node/Cargo.toml`.**

```toml
[package]
name = "buzz-node"
version.workspace = true
edition.workspace = true
license.workspace = true
rust-version.workspace = true

[dependencies]
buzz-core = { path = "../buzz-core" }
nostr = { workspace = true }
tokio = { workspace = true }
async-trait = { workspace = true }
thiserror = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }
chrono = { workspace = true }

[features]
# Exposes the in-memory FakeSubstrate/FakeRelay for cross-crate tests (Phase 5 e2e).
test-utils = []

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 3: Create `crates/buzz-node/src/lib.rs`.**

```rust
#![deny(unsafe_code)]
#![warn(missing_docs)]
//! `buzz-node` — a persistent execution-node daemon that hosts Buzz agents.
//!
//! The node subscribes to the owner's agent→node assignments on the relay and
//! reconciles them against the local process table: it starts assigned agents,
//! stops unassigned ones, restarts crashed ones, and reports observed status —
//! all controlled purely through the relay (no inbound control channel).

/// Pure desired-vs-observed reconciliation.
pub mod reconcile;
/// Domain types: desired agents, observed states, actions, errors.
pub mod model;
/// The substrate abstraction (local process table) + an in-memory fake.
pub mod substrate;
/// The relay abstraction (desired-state in, status out) + an in-memory fake.
pub mod relay;
/// The engine loop tying relay + substrate + reconcile together.
pub mod engine;
```

- [ ] **Step 4: Create a minimal `crates/buzz-node/src/main.rs` stub.**

```rust
#![deny(unsafe_code)]
//! `buzz-node` binary entry point. Real argument parsing, key loading, and the
//! live substrate/relay wiring land in Phase 3; this stub keeps the bin target
//! compiling in Phase 2.

fn main() {
    eprintln!("buzz-node: not yet runnable (Phase 3 wires the live substrate + relay)");
    std::process::exit(1);
}
```

- [ ] **Step 5: Verify the crate compiles.** Run: `cargo build -p buzz-node` — Expected: FAIL (the `pub mod` lines reference modules that don't exist yet). Create empty placeholder files so it builds:

```bash
: > crates/buzz-node/src/model.rs
: > crates/buzz-node/src/reconcile.rs
: > crates/buzz-node/src/substrate.rs
: > crates/buzz-node/src/relay.rs
: > crates/buzz-node/src/engine.rs
```

Run: `cargo build -p buzz-node` — Expected: PASS (empty modules compile).

- [ ] **Step 6: Commit.**
```bash
git add Cargo.toml crates/buzz-node
git commit -s -m "feat(node): scaffold buzz-node crate skeleton"
```

---

### Task 2: Domain model (`model.rs`)

**Files:**
- Create/replace: `crates/buzz-node/src/model.rs`

**Interfaces:**
- Consumes: `buzz_core::{AssignState, AssignmentSecret, LaunchBlock}`, `nostr::{Keys, PublicKey, ToBech32}`.
- Produces:
  - `struct DesiredAgent { pub agent_pubkey: PublicKey, pub secret: AssignmentSecret, pub state: AssignState }` (derives `Debug, Clone, PartialEq`)
  - `enum Observed { Absent, Starting, Running, Stopped, Crashed { code: Option<i32> } }` (derives `Debug, Clone, Copy, PartialEq, Eq`)
  - `enum Action { Start(Box<DesiredAgent>), Stop(PublicKey), Restart(Box<DesiredAgent>), Noop(PublicKey) }` (derives `Debug, Clone, PartialEq`)
  - `enum NodeError { Substrate(String), Relay(String), Spawn { agent: String, source: String }, Config(String) }` (derives `Debug, thiserror::Error`)
  - `#[cfg(test)] pub(crate) fn fake_desired(agent, node, owner, state) -> DesiredAgent`

- [ ] **Step 1: Write the failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use nostr::Keys;

    #[test]
    fn fake_desired_carries_agent_identity_and_state() {
        let (agent, node, owner) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&agent, &node, &owner, buzz_core::AssignState::Assigned);
        assert_eq!(d.agent_pubkey, agent.public_key());
        assert_eq!(d.state, buzz_core::AssignState::Assigned);
        assert_eq!(d.secret.agent_pubkey, agent.public_key().to_hex());
        assert_eq!(d.secret.node_pubkey, node.public_key().to_hex());
    }

    #[test]
    fn node_error_displays() {
        let e = NodeError::Substrate("boom".into());
        assert!(e.to_string().contains("boom"));
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node model::` — Expected: FAIL (types not defined).

- [ ] **Step 3: Implement `model.rs`.**

```rust
//! Domain types for the node engine.
use nostr::PublicKey;

use buzz_core::{AssignState, AssignmentSecret};

/// One agent the relay says should (or should not) run on this node.
#[derive(Debug, Clone, PartialEq)]
pub struct DesiredAgent {
    /// Agent identity (derived from the assignment secret's nsec).
    pub agent_pubkey: PublicKey,
    /// The decrypted assignment secret (nsec + launch env). `Debug` is redacted.
    pub secret: AssignmentSecret,
    /// Whether the assignment wants this agent running (`Assigned`) or stopped.
    pub state: AssignState,
}

/// Observed runtime state of an agent on this node's substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    /// No process/record exists for this agent.
    Absent,
    /// A process exists but the harness has not confirmed startup.
    Starting,
    /// The harness process is up.
    Running,
    /// The process exited cleanly / was stopped.
    Stopped,
    /// The process exited abnormally.
    Crashed {
        /// Exit code if known.
        code: Option<i32>,
    },
}

/// A reconcile decision for a single agent.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Spawn the agent.
    Start(Box<DesiredAgent>),
    /// Stop the agent by pubkey.
    Stop(PublicKey),
    /// Stop then re-spawn a crashed agent.
    Restart(Box<DesiredAgent>),
    /// Nothing to do for this agent.
    Noop(PublicKey),
}

/// Errors surfaced by the node engine and its I/O traits.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The substrate (process table) failed an operation.
    #[error("substrate error: {0}")]
    Substrate(String),
    /// The relay transport failed an operation.
    #[error("relay error: {0}")]
    Relay(String),
    /// Spawning an agent failed.
    #[error("failed to spawn agent {agent}: {source}")]
    Spawn {
        /// Agent pubkey (hex).
        agent: String,
        /// Underlying failure description.
        source: String,
    },
    /// Invalid engine/agent configuration.
    #[error("configuration error: {0}")]
    Config(String),
}

#[cfg(test)]
pub(crate) fn fake_desired(
    agent: &nostr::Keys,
    node: &nostr::Keys,
    owner: &nostr::Keys,
    state: AssignState,
) -> DesiredAgent {
    use nostr::ToBech32;
    use std::collections::BTreeMap;
    let secret = AssignmentSecret {
        format: buzz_core::assignment::FORMAT.to_string(),
        version: buzz_core::assignment::VERSION,
        agent_pubkey: agent.public_key().to_hex(),
        owner_pubkey: owner.public_key().to_hex(),
        node_pubkey: node.public_key().to_hex(),
        private_key_nsec: agent.secret_key().to_bech32().unwrap(),
        auth_tag: None,
        launch: buzz_core::LaunchBlock {
            command: "claude".into(),
            args: vec![],
            env: BTreeMap::new(),
            policy_env: BTreeMap::new(),
            owner_pubkey: Some(owner.public_key().to_hex()),
        },
        env_vars: BTreeMap::new(),
        reap_after_idle_seconds: None,
    };
    DesiredAgent { agent_pubkey: agent.public_key(), secret, state }
}
```

- [ ] **Step 4: Run + clippy.** Run: `cargo test -p buzz-node model:: && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/buzz-node/src/model.rs
git commit -s -m "feat(node): domain model (DesiredAgent, Observed, Action, NodeError)"
```

---

### Task 3: Pure reconcile function (`reconcile.rs`) — the exhaustively-tested brain

**Files:**
- Create/replace: `crates/buzz-node/src/reconcile.rs`

**Interfaces:**
- Consumes: `model::{Action, DesiredAgent, Observed}`, `buzz_core::AssignState`, `nostr::PublicKey`, `std::collections::BTreeMap`.
- Produces: `pub fn reconcile(desired: &[DesiredAgent], observed: &BTreeMap<PublicKey, Observed>) -> Vec<Action>` — deterministic (one action per agent in the union of desired ∪ observed, ordered by pubkey).

Decision table (exhaustive):

| desired intent | observed | action |
|---|---|---|
| Assigned | Absent / Stopped | `Start` |
| Assigned | Crashed | `Restart` |
| Assigned | Starting / Running | `Noop` |
| Unassigned or not-desired | Starting / Running | `Stop` |
| Unassigned or not-desired | Absent / Stopped / Crashed | `Noop` |

- [ ] **Step 1: Write the failing tests (one per transition + ordering + empties).**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{fake_desired, Action, DesiredAgent, Observed};
    use buzz_core::AssignState::{Assigned, Unassigned};
    use nostr::{Keys, PublicKey};
    use std::collections::BTreeMap;

    fn obs(pairs: &[(&Keys, Observed)]) -> BTreeMap<PublicKey, Observed> {
        pairs.iter().map(|(k, o)| (k.public_key(), *o)).collect()
    }

    fn start_of(d: &DesiredAgent) -> Action { Action::Start(Box::new(d.clone())) }
    fn restart_of(d: &DesiredAgent) -> Action { Action::Restart(Box::new(d.clone())) }

    #[test]
    fn assigned_absent_starts() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(reconcile(&[d.clone()], &BTreeMap::new()), vec![start_of(&d)]);
    }

    #[test]
    fn assigned_stopped_starts() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(reconcile(&[d.clone()], &obs(&[(&a, Observed::Stopped)])), vec![start_of(&d)]);
    }

    #[test]
    fn assigned_crashed_restarts() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        let observed = obs(&[(&a, Observed::Crashed { code: Some(1) })]);
        assert_eq!(reconcile(&[d.clone()], &observed), vec![restart_of(&d)]);
    }

    #[test]
    fn assigned_running_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Running)])), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn assigned_starting_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Starting)])), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn unassigned_running_stops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Running)])), vec![Action::Stop(a.public_key())]);
    }

    #[test]
    fn unassigned_starting_stops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Starting)])), vec![Action::Stop(a.public_key())]);
    }

    #[test]
    fn unassigned_absent_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &BTreeMap::new()), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn unassigned_stopped_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Stopped)])), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn not_desired_running_stops() {
        // Agent running on the substrate but no longer in the desired set (moved away).
        let a = Keys::generate();
        assert_eq!(reconcile(&[], &obs(&[(&a, Observed::Running)])), vec![Action::Stop(a.public_key())]);
    }

    #[test]
    fn not_desired_crashed_noops() {
        let a = Keys::generate();
        let observed = obs(&[(&a, Observed::Crashed { code: None })]);
        assert_eq!(reconcile(&[], &observed), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn empty_desired_and_observed_yields_nothing() {
        assert!(reconcile(&[], &BTreeMap::new()).is_empty());
    }

    #[test]
    fn multi_agent_output_is_sorted_by_pubkey() {
        let (n, o) = (Keys::generate(), Keys::generate());
        // Two agents; one assigned+absent (Start), one running-but-not-desired (Stop).
        let mut a1 = Keys::generate();
        let mut a2 = Keys::generate();
        if a1.public_key() > a2.public_key() { std::mem::swap(&mut a1, &mut a2); } // a1 < a2
        let d1 = fake_desired(&a1, &n, &o, Assigned);
        let out = reconcile(&[d1.clone()], &obs(&[(&a2, Observed::Running)]));
        assert_eq!(out, vec![start_of(&d1), Action::Stop(a2.public_key())]);
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node reconcile::` — Expected: FAIL (`reconcile` not defined).

- [ ] **Step 3: Implement `reconcile.rs`.**

```rust
//! Pure desired-vs-observed reconciliation — no I/O, no clock, no async.
use std::collections::{BTreeMap, BTreeSet};

use buzz_core::AssignState;
use nostr::PublicKey;

use crate::model::{Action, DesiredAgent, Observed};

/// Compute the actions that converge `observed` to `desired`.
///
/// Deterministic: emits exactly one [`Action`] per agent in the union of the
/// desired and observed sets, ordered by pubkey. Pure — all effects are the
/// caller's to apply via the [`crate::substrate::Substrate`] trait.
pub fn reconcile(
    desired: &[DesiredAgent],
    observed: &BTreeMap<PublicKey, Observed>,
) -> Vec<Action> {
    let mut want_run: BTreeMap<PublicKey, &DesiredAgent> = BTreeMap::new();
    let mut universe: BTreeSet<PublicKey> = BTreeSet::new();
    for d in desired {
        universe.insert(d.agent_pubkey);
        if d.state == AssignState::Assigned {
            want_run.insert(d.agent_pubkey, d);
        }
    }
    universe.extend(observed.keys().copied());

    let mut actions = Vec::with_capacity(universe.len());
    for pk in universe {
        let obs = observed.get(&pk).copied().unwrap_or(Observed::Absent);
        let action = if let Some(d) = want_run.get(&pk) {
            match obs {
                Observed::Absent | Observed::Stopped => Action::Start(Box::new((*d).clone())),
                Observed::Crashed { .. } => Action::Restart(Box::new((*d).clone())),
                Observed::Starting | Observed::Running => Action::Noop(pk),
            }
        } else {
            match obs {
                Observed::Starting | Observed::Running => Action::Stop(pk),
                Observed::Absent | Observed::Stopped | Observed::Crashed { .. } => Action::Noop(pk),
            }
        };
        actions.push(action);
    }
    actions
}
```

- [ ] **Step 4: Run + clippy.** Run: `cargo test -p buzz-node reconcile:: && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS (13 tests).

- [ ] **Step 5: Commit.**
```bash
git add crates/buzz-node/src/reconcile.rs
git commit -s -m "feat(node): pure reconcile() with exhaustive transition tests"
```

---

### Task 4: `Substrate` trait + `FakeSubstrate` (`substrate.rs`)

**Files:**
- Create/replace: `crates/buzz-node/src/substrate.rs`

**Interfaces:**
- Consumes: `model::{DesiredAgent, NodeError, Observed}`, `async_trait`, `nostr::PublicKey`.
- Produces:
  - `#[async_trait] pub trait Substrate: Send + Sync { async fn observe(&self) -> BTreeMap<PublicKey, Observed>; async fn start(&self, desired: &DesiredAgent) -> Result<(), NodeError>; async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError>; }`
  - `#[cfg(any(test, feature = "test-utils"))] pub struct FakeSubstrate` with `new()`, `set(pk, Observed)`, and public `starts`/`stops` call-logs.

- [ ] **Step 1: Write the failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fake_desired;
    use nostr::Keys;

    #[tokio::test]
    async fn fake_substrate_start_stop_and_observe() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let s = FakeSubstrate::new();
        assert!(s.observe().await.is_empty());
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);
        s.start(&d).await.unwrap();
        assert_eq!(s.observe().await.get(&a.public_key()), Some(&Observed::Running));
        s.stop(&a.public_key()).await.unwrap();
        assert_eq!(s.observe().await.get(&a.public_key()), Some(&Observed::Stopped));
        assert_eq!(*s.starts.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(*s.stops.lock().unwrap(), vec![a.public_key()]);
    }

    #[tokio::test]
    async fn fake_substrate_set_scripts_observed() {
        let a = Keys::generate();
        let s = FakeSubstrate::new();
        s.set(a.public_key(), Observed::Crashed { code: Some(2) });
        assert_eq!(s.observe().await.get(&a.public_key()), Some(&Observed::Crashed { code: Some(2) }));
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node substrate::` — Expected: FAIL.

- [ ] **Step 3: Implement `substrate.rs`.**

```rust
//! The substrate abstraction — how the node observes and controls the local
//! process table — plus an in-memory fake for tests.
use std::collections::BTreeMap;

use async_trait::async_trait;
use nostr::PublicKey;

use crate::model::{DesiredAgent, NodeError, Observed};

/// A place agents run. The real impl (Phase 3) supervises `buzz-acp` child
/// processes; the fake keeps an in-memory map.
#[async_trait]
pub trait Substrate: Send + Sync {
    /// Current observed state of every agent this substrate knows about.
    async fn observe(&self) -> BTreeMap<PublicKey, Observed>;
    /// Start (or ensure started) the given agent.
    async fn start(&self, desired: &DesiredAgent) -> Result<(), NodeError>;
    /// Stop the given agent.
    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError>;
}

/// In-memory [`Substrate`] for tests: `start` marks Running, `stop` marks
/// Stopped, and `set` scripts arbitrary observed states. Call logs are exposed.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
pub struct FakeSubstrate {
    inner: std::sync::Mutex<BTreeMap<PublicKey, Observed>>,
    /// Pubkeys passed to `start`, in order.
    pub starts: std::sync::Mutex<Vec<PublicKey>>,
    /// Pubkeys passed to `stop`, in order.
    pub stops: std::sync::Mutex<Vec<PublicKey>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl FakeSubstrate {
    /// Construct an empty fake substrate.
    pub fn new() -> Self {
        Self::default()
    }
    /// Script an observed state for an agent.
    pub fn set(&self, agent: PublicKey, observed: Observed) {
        self.inner.lock().expect("lock").insert(agent, observed);
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl Substrate for FakeSubstrate {
    async fn observe(&self) -> BTreeMap<PublicKey, Observed> {
        self.inner.lock().expect("lock").clone()
    }
    async fn start(&self, desired: &DesiredAgent) -> Result<(), NodeError> {
        self.starts.lock().expect("lock").push(desired.agent_pubkey);
        self.inner.lock().expect("lock").insert(desired.agent_pubkey, Observed::Running);
        Ok(())
    }
    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError> {
        self.stops.lock().expect("lock").push(*agent);
        self.inner.lock().expect("lock").insert(*agent, Observed::Stopped);
        Ok(())
    }
}
```

- [ ] **Step 4: Run + clippy.** Run: `cargo test -p buzz-node substrate:: && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/buzz-node/src/substrate.rs
git commit -s -m "feat(node): Substrate trait + in-memory FakeSubstrate"
```

---

### Task 5: `NodeRelay` trait + `FakeRelay` (`relay.rs`)

**Files:**
- Create/replace: `crates/buzz-node/src/relay.rs`

**Interfaces:**
- Consumes: `model::{DesiredAgent, NodeError}`, `buzz_core::{AgentNodeStatus, NodeCapabilities}`, `async_trait`.
- Produces:
  - `#[async_trait] pub trait NodeRelay: Send + Sync { async fn next_desired(&mut self) -> Option<Vec<DesiredAgent>>; async fn publish_status(&self, status: &AgentNodeStatus) -> Result<(), NodeError>; async fn publish_announce(&self, caps: &NodeCapabilities) -> Result<(), NodeError>; async fn publish_presence(&self, online: bool) -> Result<(), NodeError>; }`
  - `#[cfg(any(test, feature = "test-utils"))] pub struct FakeRelay` + `pub struct FakeRelayHandle` (shared `Arc<Mutex<..>>` record of published statuses/announces/presence, readable after the relay is moved into `engine::run`). `FakeRelay::new(script) -> (FakeRelay, FakeRelayHandle)`.

- [ ] **Step 1: Write the failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fake_desired;
    use nostr::Keys;

    #[tokio::test]
    async fn fake_relay_streams_script_then_ends() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = vec![fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned)];
        let (mut relay, handle) = FakeRelay::new(vec![d.clone()]);
        assert_eq!(relay.next_desired().await, Some(d));
        assert_eq!(relay.next_desired().await, None);
        relay.publish_presence(true).await.unwrap();
        assert_eq!(*handle.presence.lock().unwrap(), vec![true]);
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node relay::` — Expected: FAIL.

- [ ] **Step 3: Implement `relay.rs`.**

```rust
//! The relay abstraction — desired-state in, node/agent status out — plus an
//! in-memory fake for tests.
use async_trait::async_trait;

use buzz_core::{AgentNodeStatus, NodeCapabilities};

use crate::model::{DesiredAgent, NodeError};

/// The node's only channel to the outside world. The real impl (Phase 3) is a
/// NIP-42 WebSocket client that subscribes to the owner's assignments and
/// publishes signed status/announce/presence events.
#[async_trait]
pub trait NodeRelay: Send + Sync {
    /// Await the next desired-state snapshot for this node. `None` = shut down.
    async fn next_desired(&mut self) -> Option<Vec<DesiredAgent>>;
    /// Publish an observed per-agent status.
    async fn publish_status(&self, status: &AgentNodeStatus) -> Result<(), NodeError>;
    /// Publish this node's capabilities.
    async fn publish_announce(&self, caps: &NodeCapabilities) -> Result<(), NodeError>;
    /// Publish this node's online/offline presence.
    async fn publish_presence(&self, online: bool) -> Result<(), NodeError>;
}

#[cfg(any(test, feature = "test-utils"))]
type Log<T> = std::sync::Arc<std::sync::Mutex<Vec<T>>>;

/// Reader handle to a [`FakeRelay`]'s published-event logs, cloneable and
/// readable after the relay is moved into [`crate::engine::run`].
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone)]
pub struct FakeRelayHandle {
    /// Statuses passed to `publish_status`.
    pub statuses: Log<AgentNodeStatus>,
    /// Capabilities passed to `publish_announce`.
    pub announces: Log<NodeCapabilities>,
    /// Presence flags passed to `publish_presence`.
    pub presence: Log<bool>,
}

/// In-memory [`NodeRelay`]: yields a scripted sequence of desired-sets, then
/// `None`; records everything published.
#[cfg(any(test, feature = "test-utils"))]
pub struct FakeRelay {
    script: std::collections::VecDeque<Vec<DesiredAgent>>,
    handle: FakeRelayHandle,
}

#[cfg(any(test, feature = "test-utils"))]
impl FakeRelay {
    /// Build a fake relay from a script of desired-sets. Returns the relay and a
    /// reader handle to its published-event logs.
    pub fn new(script: Vec<Vec<DesiredAgent>>) -> (Self, FakeRelayHandle) {
        let handle = FakeRelayHandle {
            statuses: Log::default(),
            announces: Log::default(),
            presence: Log::default(),
        };
        (Self { script: script.into(), handle: handle.clone() }, handle)
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl NodeRelay for FakeRelay {
    async fn next_desired(&mut self) -> Option<Vec<DesiredAgent>> {
        self.script.pop_front()
    }
    async fn publish_status(&self, status: &AgentNodeStatus) -> Result<(), NodeError> {
        self.handle.statuses.lock().expect("lock").push(status.clone());
        Ok(())
    }
    async fn publish_announce(&self, caps: &NodeCapabilities) -> Result<(), NodeError> {
        self.handle.announces.lock().expect("lock").push(caps.clone());
        Ok(())
    }
    async fn publish_presence(&self, online: bool) -> Result<(), NodeError> {
        self.handle.presence.lock().expect("lock").push(online);
        Ok(())
    }
}
```

- [ ] **Step 4: Run + clippy.** Run: `cargo test -p buzz-node relay:: && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/buzz-node/src/relay.rs
git commit -s -m "feat(node): NodeRelay trait + in-memory FakeRelay"
```

---

### Task 6: Engine loop (`engine.rs`) — end-to-end against fakes

**Files:**
- Create/replace: `crates/buzz-node/src/engine.rs`

**Interfaces:**
- Consumes: `model::{Action, DesiredAgent, NodeError, Observed}`, `reconcile::reconcile`, `substrate::Substrate`, `relay::NodeRelay`, `buzz_core::{AgentHealth, AgentNodeStatus}`, `nostr::{Keys, PublicKey}`, `chrono::Utc`, `tokio`.
- Produces:
  - `pub struct EngineConfig { pub reconcile_tick: std::time::Duration, pub node_pubkey: PublicKey }`
  - `pub async fn run(substrate: Arc<dyn Substrate>, relay: Box<dyn NodeRelay>, node_keys: Keys, cfg: EngineConfig) -> Result<(), NodeError>`

Loop: publish presence(true) → repeat { on next_desired (None ⇒ break) OR periodic tick: observe → reconcile → apply each action → re-observe → publish one status per known agent } → publish presence(false).

- [ ] **Step 1: Write the failing end-to-end tests.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{fake_desired, Observed};
    use crate::relay::FakeRelay;
    use crate::substrate::FakeSubstrate;
    use buzz_core::AgentHealth;
    use buzz_core::AssignState::{Assigned, Unassigned};
    use nostr::Keys;
    use std::sync::Arc;
    use std::time::Duration;

    fn cfg(node: &Keys) -> EngineConfig {
        // Huge tick ⇒ only the scripted desired-sets drive the loop (deterministic).
        EngineConfig { reconcile_tick: Duration::from_secs(3600), node_pubkey: node.public_key() }
    }

    #[tokio::test]
    async fn assign_starts_agent_and_reports_running() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new(vec![vec![fake_desired(&a, &n, &o, Assigned)]]);
        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n)).await.unwrap();

        assert_eq!(*substrate.starts.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(substrate.observe().await.get(&a.public_key()), Some(&Observed::Running));
        let statuses = handle.statuses.lock().unwrap();
        assert!(statuses.iter().any(|s|
            s.agent_pubkey == a.public_key().to_hex()
                && s.node_pubkey == n.public_key().to_hex()
                && s.health == AgentHealth::Running));
        assert_eq!(*handle.presence.lock().unwrap(), vec![true, false]);
    }

    #[tokio::test]
    async fn unassign_stops_running_agent() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        substrate.set(a.public_key(), Observed::Running);
        let (relay, handle) = FakeRelay::new(vec![vec![fake_desired(&a, &n, &o, Unassigned)]]);
        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n)).await.unwrap();

        assert_eq!(*substrate.stops.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(substrate.observe().await.get(&a.public_key()), Some(&Observed::Stopped));
        assert!(handle.statuses.lock().unwrap().iter().any(|s|
            s.agent_pubkey == a.public_key().to_hex() && s.health == AgentHealth::Stopped));
    }

    #[tokio::test]
    async fn crashed_agent_is_restarted() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        substrate.set(a.public_key(), Observed::Crashed { code: Some(1) });
        let (relay, _handle) = FakeRelay::new(vec![vec![fake_desired(&a, &n, &o, Assigned)]]);
        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n)).await.unwrap();

        assert!(substrate.stops.lock().unwrap().contains(&a.public_key()));
        assert_eq!(*substrate.starts.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(substrate.observe().await.get(&a.public_key()), Some(&Observed::Running));
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node engine::` — Expected: FAIL.

- [ ] **Step 3: Implement `engine.rs`.**

```rust
//! The node engine: on each new desired-set or periodic tick, observe →
//! reconcile → apply → publish status. Controlled entirely via the relay.
use std::sync::Arc;
use std::time::Duration;

use nostr::{Keys, PublicKey};

use buzz_core::{AgentHealth, AgentNodeStatus};

use crate::model::{Action, NodeError, Observed};
use crate::reconcile::reconcile;
use crate::relay::NodeRelay;
use crate::substrate::Substrate;

/// Engine tuning.
pub struct EngineConfig {
    /// How often to reconcile even without a new desired-set (self-heal cadence).
    pub reconcile_tick: Duration,
    /// This node's pubkey (stamped into published status events).
    pub node_pubkey: PublicKey,
}

/// Run the node engine until the relay's desired-state stream ends (`None`).
pub async fn run(
    substrate: Arc<dyn Substrate>,
    mut relay: Box<dyn NodeRelay>,
    node_keys: Keys,
    cfg: EngineConfig,
) -> Result<(), NodeError> {
    // `node_keys` is reserved for Phase 3 (the real relay signs with it); the
    // status payload carries `node_pubkey` from config today.
    let _ = &node_keys;

    let mut current = Vec::new();
    relay.publish_presence(true).await?;

    // First tick fires one period out so tests driven purely by the desired
    // stream see no spurious startup reconcile.
    let start = tokio::time::Instant::now() + cfg.reconcile_tick;
    let mut ticker = tokio::time::interval_at(start, cfg.reconcile_tick);

    loop {
        tokio::select! {
            maybe = relay.next_desired() => match maybe {
                Some(desired) => current = desired,
                None => break,
            },
            _ = ticker.tick() => {}
        }

        let observed = substrate.observe().await;
        for action in reconcile(&current, &observed) {
            match action {
                Action::Start(d) => substrate.start(&d).await?,
                Action::Restart(d) => {
                    substrate.stop(&d.agent_pubkey).await?;
                    substrate.start(&d).await?;
                }
                Action::Stop(pk) => substrate.stop(&pk).await?,
                Action::Noop(_) => {}
            }
        }

        // Report observed status after applying actions.
        let after = substrate.observe().await;
        for (pk, obs) in &after {
            if let Some(health) = health_of(*obs) {
                let status = AgentNodeStatus {
                    format: buzz_core::node_status::FORMAT.to_string(),
                    version: buzz_core::node_status::VERSION,
                    agent_pubkey: pk.to_hex(),
                    node_pubkey: cfg.node_pubkey.to_hex(),
                    health,
                    reason: None,
                    updated_at: chrono::Utc::now().to_rfc3339(),
                };
                relay.publish_status(&status).await?;
            }
        }
    }

    relay.publish_presence(false).await?;
    Ok(())
}

/// Map an [`Observed`] state to a reportable [`AgentHealth`]; `Absent` is not
/// reported.
fn health_of(obs: Observed) -> Option<AgentHealth> {
    match obs {
        Observed::Starting => Some(AgentHealth::Starting),
        Observed::Running => Some(AgentHealth::Running),
        Observed::Stopped => Some(AgentHealth::Stopped),
        Observed::Crashed { .. } => Some(AgentHealth::Crashed),
        Observed::Absent => None,
    }
}
```

- [ ] **Step 4: Run + clippy.** Run: `cargo test -p buzz-node engine:: && cargo clippy -p buzz-node --all-targets -- -D warnings` — Expected: PASS (3 e2e tests).

- [ ] **Step 5: Whole-crate gate.** Run: `cargo test -p buzz-node && cargo clippy -p buzz-node --all-targets -- -D warnings && cargo fmt -p buzz-node -- --check` — Expected: PASS, formatted.

- [ ] **Step 6: Commit.**
```bash
git add crates/buzz-node/src/engine.rs
git commit -s -m "feat(node): engine run loop reconciling relay desired-state against substrate"
```

---

## Self-Review

**Spec coverage:**
- §9 reconcile loop (event-driven + periodic tick, observe→reconcile→apply→status) → Task 6 ✓
- §9 `Substrate` trait with a fake for testability → Task 4 ✓
- §8 assign flow (assigned+absent→Start→status running) → Task 6 test ✓
- §8 move flow's stop half (unassigned/not-desired→Stop) → Task 3 + Task 6 test ✓
- §13 crash recovery (crashed→Restart) → Task 3 + Task 6 test ✓
- §13 offline→catch-up (a fresh desired-set fully reconciles from scratch each tick; the pure function is stateless) → Task 3 ✓
- §15 reuse of the k8s reconcile/classify shape (pure function + fake substrate + ~13 transition tests) → Task 3 ✓
- Presence lease publish/clear (online on start, offline on shutdown) → Task 6 ✓
- **Deferred to Phase 3 (correctly not here):** real local-process substrate, real NIP-42 relay client (`next_desired` decrypting `AGENT_ASSIGNMENT` via `buzz_core::assignment::decrypt_for_node`, `publish_status` signing via `node_keys`), enrollment, detached daemon, key decrypt/zeroize. **Deferred to Phase 5:** bounded stop-before-start on move overlap, active smoke-probe health, the `reason` field population.

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". Every step has real code and a concrete `cargo` command. The `main.rs` stub is explicitly labelled and exits non-zero (Phase 3 replaces it).

**Type consistency:** `reconcile(&[DesiredAgent], &BTreeMap<PublicKey, Observed>) -> Vec<Action>` is identical in Task 3's interface block and its impl and its callsite in Task 6. `Substrate`/`NodeRelay` method signatures match between their trait definitions (Tasks 4/5), the fakes, and the engine's calls (Task 6). `AgentNodeStatus` fields (`format, version, agent_pubkey, node_pubkey, health, reason, updated_at`) match the Phase-1 struct. `AgentHealth` variants (`Starting/Running/Stopped/Crashed`) match Phase-1. `EngineConfig`/`run(...)` signature matches the authoritative contract.

---

## What Phase 3 consumes from this

Phase 3 implements the two traits for real, without touching the engine or the reconcile function:
- **`Substrate` for local processes** — `start` spawns a `buzz-acp` child (injecting the decrypted nsec + `launch` env), `observe` reports container/harness state (`state.running`, not "pid exists"), `stop` does a graceful harness shutdown; plus the crash→`Observed::Crashed` mapping and the circuit-breaker (`SlotCircuit`) wrapper.
- **`NodeRelay` over NIP-42 WebSocket** — `next_desired` subscribes to the owner's `KIND_AGENT_ASSIGNMENT`, filters `node == self`, calls `buzz_core::assignment::decrypt_for_node` to produce `DesiredAgent`s; `publish_status`/`publish_announce`/`publish_presence` build the Phase-1 events (`node_status::build_status` signed with `node_keys`, `node::build_announce`, kind:20001 presence) and send them.
- **`engine::run`, `EngineConfig`, and every `model` type** are reused unchanged; `main.rs` is replaced with real wiring (enrollment, key loading, detached-daemon startup).
