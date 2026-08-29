//! Gated end-to-end test for the `buzz-node` execution-node engine against a
//! real relay: enroll a node with an owner, have the owner assign a trivial
//! always-alive "agent" to it, run the node engine, and confirm the owner
//! observes `AGENT_NODE_STATUS{health: running}` for that agent.
//!
//! This is the only test that exercises the full Phase 3 wiring end to
//! end — enrollment ([`buzz_node::enroll::enroll`]), assignment intake
//! ([`buzz_node::nostr_relay::NostrNodeRelay`]), reconciliation
//! ([`buzz_node::engine::run`]), and process supervision
//! ([`buzz_node::substrate::LocalProcessSubstrate`]) — against a real relay
//! instead of the in-memory fakes the crate's unit tests use. It substitutes
//! a trivial `sleep`-based [`AgentRuntime`] for the real `buzz-acp` harness so
//! it needs no LLM provider key, mirroring `substrate::tests::SleepRuntime`.
//!
//! `#[ignore]`d because it needs a live relay, following the convention this
//! crate's other live tests already use (`enroll::tests::live_enroll_round_trip`,
//! `nostr_relay::tests::live_announce_status_presence_publish_and_subscribe_connects`).
//! Start a relay (`just relay` — `ws://localhost:3000`; see also
//! `crates/buzz-test-client` for the Docker-based alternative), then run:
//!
//! ```text
//! BUZZ_TEST_RELAY_URL=ws://localhost:3000 \
//!     cargo test -p buzz-node --test e2e_node -- --ignored --nocapture
//! ```
//!
//! Note: like `enroll::tests::live_enroll_round_trip`, this drives the real
//! [`buzz_node::enroll::enroll`], which persists `NodeConfig` to this
//! machine's real `~/.buzz-node/config.json` (only the agent workspace root
//! is temp-dir-isolated) — an accepted, pre-existing characteristic of these
//! opt-in live tests, not something introduced here.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nostr::{Alphabet, Filter, Keys, Kind, SingleLetterTag, ToBech32};
use serde_json::json;
use tokio::process::{Child, Command};

use buzz_core::assignment::{build_assignment, AssignmentSecret, LaunchBlock};
use buzz_core::kind::KIND_AGENT_NODE_STATUS;
use buzz_core::node::{build_enrollment, NodeCapabilities};
use buzz_core::node_status::validate_status;
use buzz_core::{AgentHealth, AssignState};
use buzz_ws_client::{NostrWsConnection, RelayMessage, WsClientError};

use buzz_node::engine;
use buzz_node::enroll;
use buzz_node::model::{DesiredAgent, NodeError};
use buzz_node::nostr_relay::NostrNodeRelay;
use buzz_node::relay::NodeRelay;
use buzz_node::runtime::AgentRuntime;
use buzz_node::substrate::{LocalProcessSubstrate, Substrate};

/// Overall deadline for the owner to observe the node's `AGENT_NODE_STATUS`.
const STATUS_DEADLINE: Duration = Duration::from_secs(30);

/// Current Unix time in seconds. A local copy of the identical, private
/// `buzz_node::nostr_relay::now_unix` helper — not exported (`pub(crate)`),
/// so not reachable from this external integration-test crate — rather than
/// a source change just to share three lines.
fn now_unix() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0)
}

/// An [`AgentRuntime`] that spawns a trivial always-alive process
/// (`sleep 60`) instead of a real `buzz-acp` harness, so this test proves the
/// node's enroll → assign → reconcile → observe → publish wiring without
/// needing an LLM provider key. Mirrors `substrate::tests::SleepRuntime`.
struct SleepRuntime;

#[async_trait]
impl AgentRuntime for SleepRuntime {
    async fn spawn(
        &self,
        _desired: &DesiredAgent,
        workspace: &Path,
        _relay_url: &str,
    ) -> Result<Child, NodeError> {
        let mut cmd = Command::new("sleep");
        cmd.arg("60").current_dir(workspace).kill_on_drop(true); // never orphan a sleep if this test's engine task is aborted
                                                                 // Required by the `AgentRuntime` contract: `LocalProcessSubstrate::stop`
                                                                 // signals `child.id()` as a process-*group* id.
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.spawn()
            .map_err(|e| NodeError::Substrate(format!("spawn: {e}")))
    }

    async fn probe(&self, _agent: &nostr::PublicKey) -> Result<(), NodeError> {
        Ok(())
    }
}

/// Build the owner-authored assignment secret for `agent`, targeting `node`.
/// The launch command is never actually run — [`SleepRuntime`] ignores it —
/// so it only needs to be structurally valid.
fn assignment_secret(owner: &Keys, agent: &Keys, node: &Keys) -> AssignmentSecret {
    AssignmentSecret {
        format: buzz_core::assignment::FORMAT.into(),
        version: buzz_core::assignment::VERSION,
        agent_pubkey: agent.public_key().to_hex(),
        owner_pubkey: owner.public_key().to_hex(),
        node_pubkey: node.public_key().to_hex(),
        private_key_nsec: agent.secret_key().to_bech32().expect("encode agent nsec"),
        auth_tag: None,
        launch: LaunchBlock {
            command: "true".into(),
            args: vec![],
            env: std::collections::BTreeMap::new(),
            policy_env: std::collections::BTreeMap::new(),
            owner_pubkey: Some(owner.public_key().to_hex()),
        },
        env_vars: std::collections::BTreeMap::new(),
        reap_after_idle_seconds: None,
    }
}

/// Requires a running relay. Run with:
///   `BUZZ_TEST_RELAY_URL=ws://localhost:3000 cargo test -p buzz-node --test e2e_node -- --ignored --nocapture`
#[tokio::test]
#[ignore = "requires a running relay; set BUZZ_TEST_RELAY_URL (see crates/buzz-test-client)"]
async fn enroll_assign_running() {
    let relay_url = std::env::var("BUZZ_TEST_RELAY_URL").expect("set BUZZ_TEST_RELAY_URL");
    let owner = Keys::generate();
    let node = Keys::generate();
    let agent = Keys::generate();

    // --- 1. Enroll the node with the owner (real handshake over the relay):
    // the node announces + waits, the owner publishes NODE_ENROLLMENT, the
    // node validates it and persists a NodeConfig. Mirrors
    // `enroll::tests::live_enroll_round_trip`.
    let caps = NodeCapabilities {
        format: buzz_core::node::FORMAT.into(),
        version: buzz_core::node::VERSION,
        node_pubkey: node.public_key().to_hex(),
        os: "test".into(),
        runtimes: vec!["acp".into()],
        workspace_root: std::env::temp_dir()
            .join(format!("buzz-node-e2e-{}", node.public_key().to_hex()))
            .to_string_lossy()
            .into_owned(),
        max_agents: None,
    };
    let relay_url_for_enroll = relay_url.clone();
    let node_for_enroll = node.clone();
    let enroll_task = tokio::spawn(async move {
        enroll::enroll(&relay_url_for_enroll, &node_for_enroll, &caps).await
    });

    // Give the node a moment to announce + subscribe before the owner approves.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut owner_conn = NostrWsConnection::connect_authenticated(&relay_url, &owner, None)
        .await
        .expect("owner connect");
    let enrollment_event =
        build_enrollment(&owner, &node.public_key(), now_unix()).expect("build enrollment event");
    let ok = owner_conn
        .send_event(enrollment_event)
        .await
        .expect("publish enrollment");
    assert!(ok.accepted, "relay rejected enrollment: {}", ok.message);

    let cfg = tokio::time::timeout(Duration::from_secs(30), enroll_task)
        .await
        .expect("enroll task timed out")
        .expect("enroll task panicked")
        .expect("enroll failed");

    // --- 2. Start the node engine: the real relay client and process
    // substrate, but a trivial `SleepRuntime` standing in for the `buzz-acp`
    // harness (see its doc comment).
    let substrate: Arc<dyn Substrate> = Arc::new(LocalProcessSubstrate::new(
        Arc::new(SleepRuntime),
        cfg.relay_url.clone(),
        cfg.workspace_root.clone(),
    ));
    let relay: Box<dyn NodeRelay> = Box::new(NostrNodeRelay::new(
        cfg.relay_url.clone(),
        node.clone(),
        owner.public_key(),
    ));
    let engine_cfg = engine::EngineConfig {
        reconcile_tick: Duration::from_secs(2),
        presence_interval: Duration::from_secs(2),
        node_pubkey: node.public_key(),
    };
    // `engine::run` only returns once its `NodeRelay::next_desired` yields
    // `None`, which a live `NostrNodeRelay` never does on its own (see its
    // doc comment) — so this runs for the rest of the test and is aborted at
    // the end rather than awaited.
    let engine_handle = tokio::spawn(engine::run(substrate, relay, node.clone(), engine_cfg));

    // Give the engine a moment to connect + subscribe for AGENT_ASSIGNMENT
    // before the owner publishes one.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- 3. Owner assigns the agent to this node.
    let secret = assignment_secret(&owner, &agent, &node);
    let assignment_event = build_assignment(
        &owner,
        &node.public_key(),
        &secret,
        AssignState::Assigned,
        now_unix(),
    )
    .expect("build assignment event");
    let ok = owner_conn
        .send_event(assignment_event)
        .await
        .expect("publish assignment");
    assert!(ok.accepted, "relay rejected assignment: {}", ok.message);

    // --- 4. Poll for AGENT_NODE_STATUS{agent, health: running} authored by
    // the node, within a deadline.
    let agent_hex = agent.public_key().to_hex();
    let status_sub_id = "e2e-node-status";
    let filter = Filter::new()
        .kind(Kind::Custom(KIND_AGENT_NODE_STATUS as u16))
        .author(node.public_key())
        .custom_tag(SingleLetterTag::lowercase(Alphabet::D), agent_hex.as_str());
    owner_conn
        .send_raw(&json!(["REQ", status_sub_id, filter]))
        .await
        .expect("subscribe for agent node status");

    let observed_running = tokio::time::timeout(STATUS_DEADLINE, async {
        loop {
            match owner_conn.next_event(Duration::from_secs(10)).await {
                Ok(RelayMessage::Event {
                    subscription_id,
                    event,
                }) if subscription_id == status_sub_id => {
                    if let Ok(status) = validate_status(&event) {
                        if status.agent_pubkey == agent_hex && status.health == AgentHealth::Running
                        {
                            return;
                        }
                        // A status for a different agent, or not yet
                        // Running (e.g. Starting) — keep waiting.
                    }
                }
                Ok(_other) => {} // EOSE/OK/NOTICE/AUTH/CLOSED/unrelated EVENT — keep waiting
                Err(WsClientError::Timeout) => {} // no news yet; poll again
                Err(e) => panic!("status stream read failed: {e}"),
            }
        }
    })
    .await;

    engine_handle.abort();

    observed_running.expect(
        "did not observe AGENT_NODE_STATUS{health: running} for the assigned agent within the deadline",
    );
}
