# Execution Nodes — Phase 3: Real Substrate + Relay Wiring (buzz-node) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make **one `buzz-node` daemon run one owner-assigned agent end-to-end against a real relay** — dial out + NIP-42 auth, read the desired-state assignment, decrypt the agent key to this node, spawn the `buzz-acp` harness with the right environment, keep it alive with crash-restart, and publish `AGENT_NODE_STATUS{running}` + node presence back to the relay.

**Architecture:** Phase 2 defined the pure reconciler and the `Substrate` / `NodeRelay` traits against fakes. Phase 3 supplies the **real I/O implementations** behind those same traits (so the Phase-2 `reconcile`/`run` logic is reused unchanged) plus the daemon that hosts them: `AcpRuntime` (spawns the harness, the D7 `AgentRuntime` seam), `LocalProcessSubstrate` (process table + workspaces + circuit breaker), `NostrNodeRelay` (dial-out/NIP-42/subscribe/publish, mirroring `crates/buzz-acp/src/relay.rs`), enrollment, and the `buzz-node` binary (detached background daemon + PID/status singleton guard, mirroring OpenAgents' launcher). Everything is wired so the same code path serves a local laptop node and a remote work node.

**Tech Stack:** Rust 1.88, `tokio` (rt-multi-thread, process, signal, time, sync), `async-trait`, `buzz-ws-client`, `buzz-core` (Phase-1 codecs), `nostr` 0.44, `keyring` (OS keychain), `dirs`, `zeroize`, `thiserror`, `serde`/`serde_json`, `chrono`. Test with `cargo test -p buzz-node`; integration test is `#[ignore]` and needs a running relay (Docker, like `crates/buzz-test-client`).

**Spec:** `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` — §9 (node internals: reconcile loop, agent supervision behind the `AgentRuntime` seam, process persistence, health probing, key handling), §12 (security: NIP-44 key-to-node, owner authorization, at-rest keychain secrets), §15 (reuse map). Read it alongside this plan.

---

## Phase-2 contract (CONSUMED — do not redefine; `use` from the `buzz-node` crate)

Phase 2 created `crates/buzz-node` with these items. Phase 3 imports them and implements the two traits; it MUST NOT redeclare them.

```rust
// crates/buzz-node/src/{types,traits,reconcile,engine}.rs (Phase 2)
use nostr::PublicKey;

/// One agent this node should be running, already decrypted for this node.
pub struct DesiredAgent {
    pub agent_pubkey: PublicKey,
    pub secret: buzz_core::assignment::AssignmentSecret, // nsec, launch, env_vars, reap_after_idle_seconds
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunState { Starting, Running, Stopped, Crashed }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedAgent { pub state: RunState, pub last_error: Option<String> }

#[derive(Clone, Debug, Default)]
pub struct Observed { pub agents: std::collections::BTreeMap<PublicKey, ObservedAgent> }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action { Start(PublicKey), Stop(PublicKey), Restart(PublicKey), NoOp }

/// Pure: diff desired vs observed → ordered actions. (Phase 2, fully tested.)
pub fn reconcile(desired: &[DesiredAgent], observed: &Observed) -> Vec<Action>;

#[async_trait::async_trait]
pub trait Substrate: Send + Sync {
    async fn observe(&self) -> Result<Observed, NodeError>;
    async fn start(&self, desired: &DesiredAgent) -> Result<(), NodeError>;
    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError>;
}

#[async_trait::async_trait]
pub trait NodeRelay: Send + Sync {
    /// Current desired agents for THIS node (already envelope-validated + decrypted).
    async fn next_desired(&self) -> Result<Vec<DesiredAgent>, NodeError>;
    async fn publish_status(&self, status: &buzz_core::node_status::AgentNodeStatus) -> Result<(), NodeError>;
    async fn announce(&self, caps: &buzz_core::node::NodeCapabilities) -> Result<(), NodeError>;
    async fn presence(&self, online: bool) -> Result<(), NodeError>;
}

pub struct EngineConfig {
    pub reconcile_interval: std::time::Duration, // periodic resync (event-driven also drives it)
    pub presence_interval: std::time::Duration,  // republish cadence (default 60s)
}

/// Phase-2 engine loop: on tick/wake → next_desired → reconcile → apply via substrate → publish_status.
pub async fn run(
    substrate: std::sync::Arc<dyn Substrate>,
    relay: std::sync::Arc<dyn NodeRelay>,
    config: EngineConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), NodeError>;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("relay error: {0}")] Relay(String),
    #[error("substrate error: {0}")] Substrate(String),
    #[error("codec error: {0}")] Codec(#[from] buzz_core::CodecError),
    #[error("io error: {0}")] Io(#[from] std::io::Error),
    #[error("config error: {0}")] Config(String),
}
```

---

## Global Constraints

- **New crate `crates/buzz-node`** (created in Phase 2); add it to the workspace `members` if not already. It **may** do I/O (unlike `buzz-core`): `tokio`, processes, keychain, network are allowed here.
- **No `unsafe`.** No new `unwrap()`/`expect()` in non-test code — use `?` and `NodeError`. `unwrap()` allowed under `#[cfg(test)]`.
- **All `pub` items documented** (`///`).
- **Agent identity env keys are authoritative and reserved:** `BUZZ_PRIVATE_KEY`, `NOSTR_PRIVATE_KEY`, `BUZZ_AUTH_TAG`, `BUZZ_RELAY_URL`, `BUZZ_ACP_AGENT_OWNER`. Build them from the decrypted `AssignmentSecret`'s top-level fields; strip these keys from any user-supplied env map before merge (§Reserved-key rule). Mirror the env keys `buzz-acp` reads (`crates/buzz-acp/src/lib.rs:5069-5124` `build_mcp_servers`).
- **Secrets never hit disk in plaintext.** The agent nsec lives only in memory (`zeroize::Zeroizing`) and is passed to the child via env, then dropped. Node keypair and any provider creds are stored in the **OS keychain** via `keyring`, never a plaintext file (the OpenAgents launcher's plaintext `~/.openagents/env/*.env` is the cautionary tale — do not copy it).
- **Relay client mirrors `crates/buzz-acp/src/relay.rs`** for dial-out + NIP-42 + reconnect/backoff (`buzz-ws-client` has no reconnect — the caller owns it). Presence is kind:20001 republished on `presence_interval` (default 60s); the relay expires it at 180s (§I3).
- **Process-group kill on every stop path** (mirror `crates/buzz-dev-mcp/src/shell.rs:682-857` `KillGroup` RAII).
- **Commit with `git commit -s`** (DCO). Run `cargo test -p buzz-node && cargo clippy -p buzz-node --all-targets -- -D warnings && cargo fmt -p buzz-node -- --check` green before each task's final commit. (Activate hermit first: `. ./bin/activate-hermit`.)

---

### Task 1: `AgentRuntime` seam + `AcpRuntime` (`src/runtime.rs`)

Implements decision **D7**: ACP-only now, behind a trait so a non-ACP adapter can drop in later.

**Files:**
- Create: `crates/buzz-node/src/runtime.rs`
- Modify: `crates/buzz-node/src/lib.rs` (`pub mod runtime;`)
- Modify: `crates/buzz-node/Cargo.toml` (deps: `tokio` features `["process","time","macros","rt-multi-thread"]`, `async-trait`, `zeroize`)

**Interfaces:**
- Consumes: `DesiredAgent`, `NodeError` (Phase 2); `buzz_core::assignment::{AssignmentSecret, LaunchBlock}` (Phase 1).
- Produces:
  - `#[async_trait] pub trait AgentRuntime: Send + Sync { async fn spawn(&self, desired: &DesiredAgent, workspace: &std::path::Path, relay_url: &str) -> Result<tokio::process::Child, NodeError>; }`
  - `pub struct AcpRuntime { pub harness_command: String, pub harness_args: Vec<String> }` (defaults: command `"buzz-acp"`, args `[]`; a bundled deployment uses `"sprig"`).
  - `pub fn build_child_env(secret: &AssignmentSecret, relay_url: &str) -> Vec<(String, String)>` (pure, tested).

- [ ] **Step 1: Write the failing env-builder test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::assignment::{AssignmentSecret, LaunchBlock};
    use nostr::Keys;
    use std::collections::BTreeMap;

    fn secret() -> AssignmentSecret {
        let agent = Keys::generate();
        AssignmentSecret {
            format: buzz_core::assignment::FORMAT.into(),
            version: buzz_core::assignment::VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            owner_pubkey: Keys::generate().public_key().to_hex(),
            node_pubkey: Keys::generate().public_key().to_hex(),
            private_key_nsec: "nsec1exampleexampleexample".into(),
            auth_tag: Some("[\"auth\",\"owner\",\"\",\"sig\"]".into()),
            launch: LaunchBlock {
                command: "claude".into(),
                args: vec![],
                env: BTreeMap::from([("GOOSE_MODEL".into(), "sonnet".into())]),
                policy_env: BTreeMap::from([("GOOSE_MODE".into(), "auto".into())]),
                owner_pubkey: Some("ownerhex".into()),
            },
            env_vars: BTreeMap::from([
                ("FOO".into(), "bar".into()),
                ("BUZZ_PRIVATE_KEY".into(), "attacker".into()), // MUST be stripped
            ]),
            reap_after_idle_seconds: None,
        }
    }

    #[test]
    fn env_builder_sets_identity_and_strips_reserved_user_keys() {
        let env: BTreeMap<String, String> =
            build_child_env(&secret(), "wss://relay.example").into_iter().collect();
        assert_eq!(env["BUZZ_PRIVATE_KEY"], "nsec1exampleexampleexample");
        assert_eq!(env["BUZZ_RELAY_URL"], "wss://relay.example");
        assert_eq!(env["NOSTR_PRIVATE_KEY"], "nsec1exampleexampleexample");
        assert_eq!(env["BUZZ_AUTH_TAG"], "[\"auth\",\"owner\",\"\",\"sig\"]");
        assert_eq!(env["FOO"], "bar");            // user env passes through
        assert_eq!(env["GOOSE_MODEL"], "sonnet"); // launch.env
        assert_eq!(env["GOOSE_MODE"], "auto");    // policy_env
        // reserved key supplied by user did NOT override the authoritative nsec:
        assert_ne!(env["BUZZ_PRIVATE_KEY"], "attacker");
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node runtime::tests::env_builder -- --nocapture` — Expected: FAIL (undefined).

- [ ] **Step 3: Implement `build_child_env`.**

```rust
//! The `AgentRuntime` seam (D7) and its ACP implementation.
use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use buzz_core::assignment::AssignmentSecret;
use tokio::process::{Child, Command};
use zeroize::Zeroizing;

use crate::{DesiredAgent, NodeError};

/// Env keys that carry authoritative identity; never overridable by user env.
const RESERVED_ENV_KEYS: &[&str] = &[
    "BUZZ_PRIVATE_KEY", "NOSTR_PRIVATE_KEY", "BUZZ_AUTH_TAG",
    "BUZZ_RELAY_URL", "BUZZ_ACP_AGENT_OWNER",
];

/// Build the child environment with documented precedence (later overrides earlier):
/// policy_env < launch.env < user env_vars < authoritative identity. User-supplied
/// reserved keys are dropped before merge so identity cannot be spoofed.
pub fn build_child_env(secret: &AssignmentSecret, relay_url: &str) -> Vec<(String, String)> {
    let mut env: BTreeMap<String, String> = BTreeMap::new();
    let strip = |m: &BTreeMap<String, String>, env: &mut BTreeMap<String, String>| {
        for (k, v) in m {
            if !RESERVED_ENV_KEYS.contains(&k.as_str()) {
                env.insert(k.clone(), v.clone());
            }
        }
    };
    strip(&secret.launch.policy_env, &mut env);
    strip(&secret.launch.env, &mut env);
    strip(&secret.env_vars, &mut env);
    // Authoritative identity, written last:
    env.insert("BUZZ_PRIVATE_KEY".into(), secret.private_key_nsec.clone());
    env.insert("NOSTR_PRIVATE_KEY".into(), secret.private_key_nsec.clone());
    env.insert("BUZZ_RELAY_URL".into(), relay_url.to_string());
    if let Some(tag) = &secret.auth_tag {
        env.insert("BUZZ_AUTH_TAG".into(), tag.clone());
    }
    if let Some(owner) = &secret.launch.owner_pubkey {
        env.insert("BUZZ_ACP_AGENT_OWNER".into(), owner.clone());
    }
    env.into_iter().collect()
}
```

- [ ] **Step 4: Run env test to green.** Run: `cargo test -p buzz-node runtime::tests::env_builder` — Expected: PASS.

- [ ] **Step 5: Implement the trait + `AcpRuntime::spawn`.**

```rust
/// Spawns the agent process for a desired agent. D7 seam: ACP-only impl in v1.
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    /// Spawn the harness as a detached-from-parent-signals child with the agent
    /// environment. The returned `Child` is owned by the substrate's process table.
    async fn spawn(
        &self,
        desired: &DesiredAgent,
        workspace: &Path,
        relay_url: &str,
    ) -> Result<Child, NodeError>;
}

/// ACP runtime: spawns `buzz-acp` (or `sprig`) with the injected agent env.
pub struct AcpRuntime {
    /// Harness binary name resolved on PATH (default `buzz-acp`).
    pub harness_command: String,
    /// Extra harness args (default empty).
    pub harness_args: Vec<String>,
}

impl Default for AcpRuntime {
    fn default() -> Self {
        Self { harness_command: "buzz-acp".into(), harness_args: Vec::new() }
    }
}

#[async_trait]
impl AgentRuntime for AcpRuntime {
    async fn spawn(
        &self,
        desired: &DesiredAgent,
        workspace: &Path,
        relay_url: &str,
    ) -> Result<Child, NodeError> {
        // Hold the nsec in a zeroizing buffer; env vars are copied into the child.
        let nsec = Zeroizing::new(desired.secret.private_key_nsec.clone());
        let _ = &nsec; // ownership makes the intent explicit; env copy below.
        let mut cmd = Command::new(&self.harness_command);
        cmd.args(&self.harness_args)
            .current_dir(workspace)
            .env_clear()
            .kill_on_drop(false); // the substrate owns termination via process groups
        // Preserve PATH/HOME so the harness resolves its own tools.
        for key in ["PATH", "HOME", "USER", "LANG", "TMPDIR"] {
            if let Ok(v) = std::env::var(key) {
                cmd.env(key, v);
            }
        }
        for (k, v) in build_child_env(&desired.secret, relay_url) {
            cmd.env(k, v);
        }
        // New process group so stop() can signal the whole tree (mirror KillGroup).
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // SAFETY-free: setsid via pre_exec is provided by tokio's std Command;
            // use process_group(0) (Rust 1.64+) to avoid unsafe pre_exec.
            cmd.process_group(0);
        }
        cmd.spawn().map_err(NodeError::Io)
    }
}
```

- [ ] **Step 6: Add a spawn smoke test** (spawns a trivial command, asserts a live child) — put the real binary behind config so the test uses `/bin/sh`:

```rust
#[tokio::test]
async fn acp_runtime_spawns_a_child_in_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let rt = AcpRuntime { harness_command: "/bin/sh".into(), harness_args: vec!["-c".into(), "sleep 5".into()] };
    let desired = crate::test_support::desired_agent_fixture();
    let mut child = rt.spawn(&desired, dir.path(), "wss://relay.example").await.unwrap();
    assert!(child.id().is_some(), "child should be running");
    child.start_kill().ok();
}
```

(Add `tempfile` as a dev-dependency; `crate::test_support::desired_agent_fixture()` is a Phase-2 test helper — if absent, build a `DesiredAgent` inline as in Step 1's `secret()`.)

- [ ] **Step 7: Run + clippy + commit.**
Run: `cargo test -p buzz-node runtime:: && cargo clippy -p buzz-node --all-targets -- -D warnings`
```bash
git add crates/buzz-node/src/runtime.rs crates/buzz-node/src/lib.rs crates/buzz-node/Cargo.toml
git commit -s -m "feat(node): AgentRuntime seam (D7) + AcpRuntime harness spawner"
```

---

### Task 2: `LocalProcessSubstrate` (`src/substrate.rs`)

**Files:**
- Create: `crates/buzz-node/src/substrate.rs`
- Modify: `crates/buzz-node/src/lib.rs` (`pub mod substrate;`)
- Modify: `crates/buzz-node/Cargo.toml` (deps: `dirs`, `nix` [unix signals] or use `libc`; `tokio` feature `signal`)

**Interfaces:**
- Consumes: `Substrate`, `Observed`, `ObservedAgent`, `RunState`, `DesiredAgent`, `NodeError` (Phase 2); `AgentRuntime` (Task 1).
- Produces:
  - `pub struct LocalProcessSubstrate { runtime: Arc<dyn AgentRuntime>, relay_url: String, root: PathBuf, table: Mutex<BTreeMap<PublicKey, AgentProc>>, breaker: Mutex<BTreeMap<PublicKey, Circuit>> }`
  - `pub fn workspace_dir(root: &Path, agent: &PublicKey) -> PathBuf` (→ `<root>/agents/<hex>/workspace`)
  - internal `struct Circuit` implementing the breaker (3 crashes / 60s → 5-min open, half-open probe — mirror `buzz-acp` `SlotCircuit`, `lib.rs:1461-1545`).

- [ ] **Step 1: Write failing lifecycle test with a fake runtime.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::AgentRuntime;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct SleepRuntime; // spawns `sleep 30` so the child stays alive
    #[async_trait]
    impl AgentRuntime for SleepRuntime {
        async fn spawn(&self, _d: &DesiredAgent, ws: &std::path::Path, _r: &str)
            -> Result<tokio::process::Child, NodeError> {
            tokio::process::Command::new("/bin/sh")
                .args(["-c", "sleep 30"]).current_dir(ws)
                .spawn().map_err(NodeError::Io)
        }
    }

    #[tokio::test]
    async fn start_then_observe_running_then_stop() {
        let dir = tempfile::tempdir().unwrap();
        let sub = LocalProcessSubstrate::new(Arc::new(SleepRuntime), "wss://r".into(), dir.path().into());
        let d = crate::test_support::desired_agent_fixture();
        sub.start(&d).await.unwrap();
        let obs = sub.observe().await.unwrap();
        assert_eq!(obs.agents[&d.agent_pubkey].state, RunState::Running);
        // workspace was created:
        assert!(workspace_dir(dir.path(), &d.agent_pubkey).is_dir());
        sub.stop(&d.agent_pubkey).await.unwrap();
        let obs = sub.observe().await.unwrap();
        assert!(matches!(obs.agents.get(&d.agent_pubkey), None | Some(ObservedAgent{state: RunState::Stopped, ..})));
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node substrate::tests::start_then_observe` — Expected: FAIL.

- [ ] **Step 3: Implement `LocalProcessSubstrate`.** Key points (write real code):
  - `AgentProc { child: tokio::process::Child, started: bool }`. `start()`: create the workspace dir (`std::fs::create_dir_all(workspace_dir(...))`), call `runtime.spawn(...)`, insert into `table` with `started=true`.
  - `observe()`: for each entry, `child.try_wait()?` → `None` = alive → `RunState::Running`; `Some(status)` = exited → if intentional-stop flag set `Stopped` else `Crashed` (record `last_error = exit code`) and feed the circuit breaker.
  - `stop()`: mark intentional; process-group kill (Unix: `killpg(child_pgid, SIGTERM)`; wait up to a grace, then `SIGKILL`; mirror `KillGroup`). Windows: `taskkill /T /F` on the pid. Remove from table.
  - Circuit breaker: on a `Crashed` observation, `Circuit::record_crash()`; the Phase-2 `reconcile` will emit `Restart`, but the substrate refuses to restart while the breaker is open (returns quickly / stays Crashed) and allows a half-open probe after the cooldown. Expose `breaker_open(agent) -> bool` used inside `start()` for restarts.
  - `workspace_dir(root, agent)` → `root.join("agents").join(agent.to_hex()).join("workspace")`.

- [ ] **Step 4: Add a crash-restart breaker test.**

```rust
#[tokio::test]
async fn crash_loop_opens_breaker() {
    let dir = tempfile::tempdir().unwrap();
    struct DieRuntime;
    #[async_trait::async_trait]
    impl crate::runtime::AgentRuntime for DieRuntime {
        async fn spawn(&self, _d:&DesiredAgent, ws:&std::path::Path, _r:&str)
            -> Result<tokio::process::Child, NodeError> {
            tokio::process::Command::new("/bin/sh").args(["-c","exit 1"]).current_dir(ws)
                .spawn().map_err(NodeError::Io)
        }
    }
    let sub = LocalProcessSubstrate::new(std::sync::Arc::new(DieRuntime), "wss://r".into(), dir.path().into());
    let d = crate::test_support::desired_agent_fixture();
    for _ in 0..4 {
        let _ = sub.start(&d).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = sub.observe().await;
    }
    assert!(sub.breaker_open(&d.agent_pubkey));
}
```

- [ ] **Step 5: Run + clippy + commit.**
Run: `cargo test -p buzz-node substrate:: && cargo clippy -p buzz-node --all-targets -- -D warnings`
```bash
git add crates/buzz-node/src/substrate.rs crates/buzz-node/src/lib.rs crates/buzz-node/Cargo.toml
git commit -s -m "feat(node): LocalProcessSubstrate with workspaces + crash-restart breaker"
```

---

### Task 3: `NostrNodeRelay` (`src/nostr_relay.rs`)

**Files:**
- Create: `crates/buzz-node/src/nostr_relay.rs`
- Modify: `crates/buzz-node/src/lib.rs` (`pub mod nostr_relay;`)
- Modify: `crates/buzz-node/Cargo.toml` (deps: `buzz-ws-client`, `buzz-core`, `nostr`, `chrono`)

**Interfaces:**
- Consumes: `NodeRelay`, `DesiredAgent`, `NodeError` (Phase 2); `buzz_core::{assignment, node, node_status, kind::KIND_AGENT_ASSIGNMENT}` (Phase 1); `buzz-ws-client` connection API.
- Produces:
  - `pub struct NostrNodeRelay { node_keys: nostr::Keys, owner_pubkey: nostr::PublicKey, relay_url: String, /* conn + inbound cache */ }`
  - `pub fn desired_from_event(event: &nostr::Event, node_keys: &nostr::Keys, owner: &nostr::PublicKey) -> Option<DesiredAgent>` (pure filter/decrypt, tested without a live relay)
  - `impl NodeRelay for NostrNodeRelay { … }`

- [ ] **Step 1: Write the failing pure-decrypt test** (craft a real assignment with Phase-1 `build_assignment`, prove only the target node yields a `DesiredAgent`).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::assignment::{build_assignment, AssignState, AssignmentSecret, LaunchBlock, FORMAT, VERSION};
    use nostr::Keys;
    use std::collections::BTreeMap;

    fn make_assignment(owner:&Keys, agent:&Keys, node:&Keys) -> nostr::Event {
        let secret = AssignmentSecret {
            format: FORMAT.into(), version: VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            owner_pubkey: owner.public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            private_key_nsec: agent.secret_key().to_bech32().unwrap(),
            auth_tag: None,
            launch: LaunchBlock { command:"claude".into(), args:vec![], env:BTreeMap::new(), policy_env:BTreeMap::new(), owner_pubkey:Some(owner.public_key().to_hex()) },
            env_vars: BTreeMap::new(), reap_after_idle_seconds: None,
        };
        build_assignment(owner, &node.public_key(), &secret, AssignState::Assigned, 1_785_780_000).unwrap()
    }

    #[test]
    fn desired_only_for_target_node() {
        use nostr::ToBech32;
        let (owner, agent, node, other) = (Keys::generate(), Keys::generate(), Keys::generate(), Keys::generate());
        let ev = make_assignment(&owner, &agent, &node);
        // target node → Some, correct agent + decrypted nsec derives agent
        let d = desired_from_event(&ev, &node, &owner.public_key()).expect("target node gets desired");
        assert_eq!(d.agent_pubkey, agent.public_key());
        assert_eq!(Keys::parse(&d.secret.private_key_nsec).unwrap().public_key(), agent.public_key());
        // non-target node → None
        assert!(desired_from_event(&ev, &other, &owner.public_key()).is_none());
        // wrong owner → None
        assert!(desired_from_event(&ev, &node, &Keys::generate().public_key()).is_none());
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node nostr_relay::tests::desired_only` — Expected: FAIL.

- [ ] **Step 3: Implement `desired_from_event`.**

```rust
//! Real Nostr relay client for the node (dial-out + NIP-42 + assignment intake).
use buzz_core::assignment::{decrypt_for_node, AssignState};
use crate::{DesiredAgent, NodeError};

/// Validate + decrypt an assignment event; return a `DesiredAgent` only when it
/// targets this node, is authored by the owner, and is in the `Assigned` state.
pub fn desired_from_event(
    event: &nostr::Event,
    node_keys: &nostr::Keys,
    owner: &nostr::PublicKey,
) -> Option<DesiredAgent> {
    // decrypt_for_node validates the owner-signed envelope, requires node == self,
    // decrypts with (node_priv, owner_pub), and cross-checks inner==outer metadata.
    let (envelope, secret) = decrypt_for_node(event, node_keys, owner).ok()?;
    if envelope.state != AssignState::Assigned {
        return None;
    }
    Some(DesiredAgent { agent_pubkey: envelope.agent_pubkey, secret })
}
```

- [ ] **Step 4: Run pure test to green.** Run: `cargo test -p buzz-node nostr_relay::tests::desired_only` — Expected: PASS.

- [ ] **Step 5: Implement `NostrNodeRelay`** (real I/O; mirror `crates/buzz-acp/src/relay.rs`). Write real code for:
  - `connect()`: open a `buzz-ws-client` connection to `relay_url`, complete NIP-42 (`build_auth_event` + `authenticate`); on drop/error, reconnect with exponential backoff (1s→30s) — `buzz-ws-client` has no reconnect, so own it here (mirror `relay.rs`).
  - Background socket task: subscribe with a REQ filter `{ kinds:[KIND_AGENT_ASSIGNMENT], authors:[owner] }` (assignments are owner-signed); on each `EVENT`, run `desired_from_event`; maintain a `BTreeMap<PublicKey, DesiredAgent>` of current desired agents (LWW by the replaceable event — keep the newest `created_at` per agent), behind a `tokio::sync::Mutex`.
  - `next_desired()`: snapshot that map into a `Vec`.
  - `publish_status(status)`: `buzz_core::node_status::build_status(&node_keys, status, now)?` then send EVENT.
  - `announce(caps)`: `buzz_core::node::build_announce(&node_keys, caps, now)?` then send EVENT.
  - `presence(online)`: build a kind:20001 presence event signed by `node_keys` (content `"online"`/`"offline"`) and send; the engine calls this on `presence_interval` and once with `false` on shutdown.

- [ ] **Step 6: Run + clippy + commit.**
Run: `cargo test -p buzz-node nostr_relay:: && cargo clippy -p buzz-node --all-targets -- -D warnings`
```bash
git add crates/buzz-node/src/nostr_relay.rs crates/buzz-node/src/lib.rs crates/buzz-node/Cargo.toml
git commit -s -m "feat(node): NostrNodeRelay — dial-out/NIP-42, assignment intake, status/presence publish"
```

---

### Task 4: Enrollment + node config (`src/enroll.rs`)

**Files:**
- Create: `crates/buzz-node/src/enroll.rs`
- Modify: `crates/buzz-node/src/lib.rs` (`pub mod enroll;`)
- Modify: `crates/buzz-node/Cargo.toml` (deps: `keyring`, `dirs`, `rand`)

**Interfaces:**
- Consumes: `NodeError` (Phase 2); `buzz_core::node::{validate_enrollment, NodeCapabilities}` (Phase 1); `NostrNodeRelay` (Task 3).
- Produces:
  - `pub struct NodeConfig { pub node_pubkey: String, pub owner_pubkey: String, pub relay_url: String, pub workspace_root: PathBuf }` (serde; persisted to `~/.buzz-node/config.json`)
  - `pub fn load_or_create_node_keys() -> Result<nostr::Keys, NodeError>` (OS keychain via `keyring`, service `"buzz-node"`, account `"node-key"`)
  - `pub fn pairing_code() -> String` (8 uppercase base32 chars, no ambiguous chars)
  - `pub async fn enroll(relay: &NostrNodeRelay, caps: &NodeCapabilities) -> Result<NodeConfig, NodeError>` (announce, print code, wait for the owner's `NODE_ENROLLMENT` for this node pubkey, validate, persist config)

- [ ] **Step 1: Write failing tests** (pairing-code shape; config round-trips to JSON).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pairing_code_is_eight_unambiguous_chars() {
        let c = pairing_code();
        assert_eq!(c.len(), 8);
        assert!(c.chars().all(|ch| "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".contains(ch)));
    }
    #[test]
    fn node_config_round_trips() {
        let cfg = NodeConfig {
            node_pubkey: "n".into(), owner_pubkey: "o".into(),
            relay_url: "wss://r".into(), workspace_root: "/tmp/x".into(),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: NodeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.node_pubkey, "n");
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node enroll::` — Expected: FAIL.

- [ ] **Step 3: Implement `pairing_code`, `NodeConfig`, `load_or_create_node_keys`, `enroll`.** Key points:
  - `pairing_code`: sample 8 chars from the unambiguous alphabet with `rand`.
  - `load_or_create_node_keys`: `keyring::Entry::new("buzz-node","node-key")`; on `get_password` hit, `Keys::parse`; on miss, `Keys::generate()`, store nsec via `set_password`. **Never** write the key to a file (§Global Constraints; contrast OpenAgents' plaintext env files).
  - `enroll`: `relay.announce(caps)`, print the code + node pubkey to stderr, then poll the relay for a `KIND_NODE_ENROLLMENT` event whose `d` == node pubkey; `validate_enrollment(&event, &owner)?`; on success persist `NodeConfig` to `~/.buzz-node/config.json` (0600) and return it. (The owner approves in the app, which publishes the enrollment.)
  - Note the keychain call is not unit-tested in CI (no keychain); gate a keychain round-trip test behind `#[ignore]`.

- [ ] **Step 4: Run + clippy + commit.**
Run: `cargo test -p buzz-node enroll:: && cargo clippy -p buzz-node --all-targets -- -D warnings`
```bash
git add crates/buzz-node/src/enroll.rs crates/buzz-node/src/lib.rs crates/buzz-node/Cargo.toml
git commit -s -m "feat(node): enrollment (pairing code, keychain node key, NODE_ENROLLMENT wait)"
```

---

### Task 5: The `buzz-node` daemon binary (`src/daemon.rs` + `src/main.rs`)

**Files:**
- Create: `crates/buzz-node/src/daemon.rs`
- Create: `crates/buzz-node/src/main.rs`
- Modify: `crates/buzz-node/Cargo.toml` (`[[bin]] name = "buzz-node"`; deps: `clap` features `["derive"]`, `serde_json`)

**Interfaces:**
- Consumes: `run`, `EngineConfig` (Phase 2); `LocalProcessSubstrate` (T2), `NostrNodeRelay` (T3), `AcpRuntime` (T1), `enroll::*` (T4).
- Produces: the CLI (`buzz-node up [--foreground]`, `buzz-node enroll`, `buzz-node autostart`, `buzz-node status`) and the singleton/PID guard.

- [ ] **Step 1: Write failing PID-guard test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn live_pid_detected_stale_pid_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("daemon.pid");
        // our own pid is alive:
        std::fs::write(&pidfile, std::process::id().to_string()).unwrap();
        assert!(live_daemon_pid(&pidfile).is_some());
        // a pid that cannot exist is stale:
        std::fs::write(&pidfile, "999999999").unwrap();
        assert!(live_daemon_pid(&pidfile).is_none());
    }
}
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-node daemon::tests::live_pid` — Expected: FAIL.

- [ ] **Step 3: Implement the guard + daemonize + engine wiring.** Key points (real code):
  - `live_daemon_pid(pidfile) -> Option<u32>`: read pid; on Unix check liveness with `kill(pid, 0)` (signal 0) via `nix`/`libc`; cross-check a fresh `daemon.status.json` (mtime < 30s) as a second source (mirror OpenAgents' two-source guard).
  - `daemonize()`: re-exec `buzz-node up --foreground` with `std::process::Command` + `.stdin(Stdio::null()).stdout(logfd).stderr(logfd)`; on Unix `process_group(0)` and do not wait — the parent returns immediately (the OS keeps the child after the app/terminal exits). Write `daemon.pid`. (This is the detached-process persistence from §9; reboot survival is `autostart`.)
  - `up_foreground()`: load `NodeConfig`; `load_or_create_node_keys()`; build `NostrNodeRelay`, `LocalProcessSubstrate::new(Arc::new(AcpRuntime::default()), relay_url, workspace_root)`; a `tokio::sync::watch` shutdown channel wired to `tokio::signal::ctrl_c()` and SIGTERM; call the Phase-2 `run(substrate, relay, EngineConfig{ reconcile_interval: 5s, presence_interval: 60s }, shutdown_rx)`; write `daemon.status.json` every 5s.
  - `autostart()`: install an opt-in OS login item (macOS `launchd` plist in `~/Library/LaunchAgents`, Linux systemd user unit, Windows Startup) that runs `buzz-node up`. Emit the file, do not enable silently beyond the user's command.
  - `main.rs`: `clap` dispatch to the above.

- [ ] **Step 4: Run + clippy + commit.**
Run: `cargo test -p buzz-node daemon:: && cargo clippy -p buzz-node --all-targets -- -D warnings && cargo build -p buzz-node`
```bash
git add crates/buzz-node/src/daemon.rs crates/buzz-node/src/main.rs crates/buzz-node/Cargo.toml
git commit -s -m "feat(node): buzz-node daemon binary — detached up, PID guard, enroll/autostart CLI"
```

---

### Task 6: End-to-end integration test (gated)

**Files:**
- Create: `crates/buzz-node/tests/e2e_node.rs`

**Interfaces:**
- Consumes: everything above + a running relay (Docker, like `crates/buzz-test-client`). Uses `buzz_core::{node::build_enrollment, assignment::build_assignment}` to act as the owner + app.

- [ ] **Step 1: Write the gated e2e test.**

```rust
//! Requires a running relay at BUZZ_TEST_RELAY_URL. Run with:
//!   cargo test -p buzz-node --test e2e_node -- --ignored --nocapture
#[tokio::test]
#[ignore = "requires a running relay (docker compose up) + BUZZ_TEST_RELAY_URL"]
async fn enroll_assign_running() {
    let relay_url = std::env::var("BUZZ_TEST_RELAY_URL").expect("set BUZZ_TEST_RELAY_URL");
    let owner = nostr::Keys::generate();
    let node = nostr::Keys::generate();
    let agent = nostr::Keys::generate();

    // 1) Node announces + relay owner publishes NODE_ENROLLMENT for this node.
    //    (In production the app does step 2 after the human approves a pairing code.)
    // 2) Owner publishes an AGENT_ASSIGNMENT targeting `node`, encrypted to `node`,
    //    carrying `agent`'s nsec and a trivial launch (command that stays alive).
    // 3) Start the node engine (NostrNodeRelay + LocalProcessSubstrate with a
    //    SleepRuntime-style AgentRuntime so no real LLM key is needed).
    // 4) Poll the relay for AGENT_NODE_STATUS{d=agent, health=running} authored by node,
    //    within a 30s deadline. Assert it appears.
    // Full wiring uses build_enrollment/build_assignment + a buzz-ws-client owner
    // connection to publish, and desired_from_event on the node side.
    let _ = (relay_url, owner, node, agent);
    // See helper `spawn_test_node(...)` below; assert on the observed status event.
}
```

- [ ] **Step 2: Implement the test body + a `SleepRuntime` `AgentRuntime`** (spawns `/bin/sh -c 'sleep 60'` so the "agent" is a live process without needing an LLM key). Publish enrollment + assignment as the owner over a second `buzz-ws-client` connection; run the node engine in a task; subscribe as the owner for `KIND_AGENT_NODE_STATUS` with `#d=[agent]` and assert `health == running` within 30s.

- [ ] **Step 3: Document the run recipe** at the top of the file (start relay via the repo's docker compose, export `BUZZ_TEST_RELAY_URL=ws://localhost:3000`).

- [ ] **Step 4: Run (locally, with a relay) + commit.**
Run: `cargo test -p buzz-node --test e2e_node -- --ignored --nocapture` (with a relay up) — Expected: PASS.
```bash
git add crates/buzz-node/tests/e2e_node.rs
git commit -s -m "test(node): gated e2e — enroll, assign, assert agent running via relay status"
```

---

## Self-Review

**Spec coverage (§9, §12, §15):**
- Reconcile loop reuse (§9) → consumes Phase-2 `run`/`reconcile` unchanged; Tasks 2–3 supply real `Substrate`/`NodeRelay` ✓
- Agent supervision behind the `AgentRuntime` seam, ACP-only (D7) → Task 1 ✓
- Process persistence (detached daemon + PID/status guard) → Task 5 ✓
- Health probing / status publish (§9) → Task 3 `publish_status` + Task 6 assertion ✓ (active smoke-probe beyond liveness is Phase 5)
- Workspaces (§9) → Task 2 `workspace_dir` ✓
- Key handling: NIP-44 decrypt-to-node, in-memory zeroized nsec, env injection (§9/§12) → Tasks 1 + 3 ✓
- At-rest keychain secrets, no plaintext (§12) → Task 4 ✓
- Owner authorization (NIP-OA / owner-signed) → Task 3 `desired_from_event` (owner-authored filter + `decrypt_for_node`) + Task 4 `validate_enrollment` ✓
- Relay dial-out/NIP-42/reconnect (§15 reuse of `buzz-acp/relay.rs`) → Task 3 ✓
- **Deferred to Phase 5 (correctly not here):** move flow (bounded stop-before-start), offline catch-up nuance, active smoke-probe health vocabulary, two-node concurrency, at-rest encryption of *provider API keys* (node key is here; provider-key storage lands with the app config in Phase 4/5).

**Placeholder scan:** No "TBD"/"handle edge cases". The two large real-I/O methods (`NostrNodeRelay` connect/loop in T3 Step 5, daemon internals in T5 Step 3) are specified as concrete bullet steps citing the exact Buzz files to mirror, with the pure/testable parts (`build_child_env`, `desired_from_event`, `live_daemon_pid`, breaker) given as full code with tests — matching how Phase 1 handled boilerplate via a named pattern file.

**Type consistency:** `AgentRuntime::spawn(&self, &DesiredAgent, &Path, &str) -> Result<Child, NodeError>` is identical in T1's interface, T1 impl, T2 fakes, and T6's `SleepRuntime`. `desired_from_event(&Event, &Keys, &PublicKey) -> Option<DesiredAgent>` matches between T3 interface, impl, and test. `LocalProcessSubstrate::new(Arc<dyn AgentRuntime>, String, PathBuf)` matches T2 tests and T5 wiring. `workspace_dir`, `live_daemon_pid`, `NodeConfig`, `pairing_code` signatures match their tests. All consumed Phase-2 symbols use the signatures in the contract block above.

---

## What Phase 5 consumes from this

Phase 5 (move/resilience + two-node e2e) builds on: `LocalProcessSubstrate` (adds bounded stop-before-start on move, offline catch-up, active smoke-probe health + reason vocabulary in `publish_status`), `NostrNodeRelay` (handles `AssignState::Unassigned` → stop, and the move handshake watching the old node's `stopped` status), and the daemon (`autostart` hardening). It reuses the Task 6 e2e harness to run **two** node engines and assert single-live-instance + resurrection on move.
