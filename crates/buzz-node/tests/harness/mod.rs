//! Shared glue for `crates/buzz-node/tests/e2e_nodes.rs` (the two-node
//! execution-node end-to-end proof): starting real node engines against a
//! live relay, driving the owner side of enrollment/assignment, and folding
//! the owner's observed `AGENT_NODE_STATUS` stream into a timeline the test
//! can assert the single-live-instance invariant (spec I4) against.
//!
//! Test-only glue: liberal `unwrap`/`expect`/`panic!` throughout is
//! deliberate (matches `tests/e2e_node.rs`'s style), not something that
//! would be accepted in `src/`.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nostr::{Alphabet, Filter, Keys, Kind, PublicKey, SingleLetterTag, ToBech32};

use buzz_core::assignment::{build_assignment, AssignmentSecret, LaunchBlock};
use buzz_core::kind::KIND_AGENT_NODE_STATUS;
use buzz_core::node::build_enrollment;
use buzz_core::node_status::validate_status;
use buzz_core::{AgentHealth, AssignState};
use buzz_test_client::{BuzzTestClient, RelayMessage, TestClientError};

use buzz_node::engine::{self, EngineConfig};
use buzz_node::model::NodeError;
use buzz_node::nostr_relay::NostrNodeRelay;
use buzz_node::relay::NodeRelay;
use buzz_node::runtime::AcpRuntime;
use buzz_node::substrate::{LocalProcessSubstrate, Substrate};

/// Current Unix time in seconds, for event `created_at` fields. A local
/// copy of `buzz_node::nostr_relay::now_unix` (`pub(crate)`, so not
/// reachable from this external integration-test crate) -- mirrors
/// `tests/e2e_node.rs`'s identical copy.
fn now_unix() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0)
}

/// Absolute path to the repo-local stub agent script (see its own doc
/// comment). Resolved via `CARGO_MANIFEST_DIR` rather than a relative path
/// so it doesn't depend on the test binary's working directory.
fn stub_agent_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/harness/stub-agent.sh")
}

/// A running node engine started by [`start_node`].
pub struct NodeHandle {
    /// The spawned `engine::run(...)` task.
    pub task: tokio::task::JoinHandle<Result<(), NodeError>>,
    /// The same substrate the task's engine drives, kept here so the test
    /// can deterministically stop a still-running agent at teardown.
    /// Aborting `task` alone does not stop any agent process it was
    /// supervising: `AcpRuntime::spawn` deliberately does not
    /// `kill_on_drop` its spawned child (see its doc comment -- a real
    /// graceful daemon shutdown must never kill agents), so dropping the
    /// engine's `Arc<dyn Substrate>` on task abort just detaches the
    /// process rather than terminating it. Calling
    /// `substrate.stop(&agent)` explicitly is the real, intentional
    /// termination path.
    pub substrate: Arc<dyn Substrate>,
}

/// Start a real node engine against `relay_url`: the same
/// `NostrNodeRelay`/`LocalProcessSubstrate`/`AcpRuntime`/`engine::run`
/// wiring `crates/buzz-node/src/daemon.rs`'s `up_foreground` uses for a
/// real `buzz-node up`, minus the CLI/PID-file/`NodeConfig` machinery --
/// this harness constructs the engine directly rather than going through
/// `buzz_node::enroll`/`up_foreground`, since the test already knows
/// `relay_url`/`owner`/`node_keys`/`workspace_root` up front.
///
/// `AcpRuntime::harness_command` is pointed at the repo-local
/// `tests/harness/stub-agent.sh` instead of the default `buzz-acp`, so the
/// engine supervises a real, benign, long-lived process without needing an
/// LLM provider key. This is the actual seam that matters: `AcpRuntime`
/// always execs its own configured `harness_command`/`harness_args` and
/// never reads the assignment secret's `launch.command`, so pointing an
/// assignment's `launch.command` at the stub (as the stale plan this batch
/// was drafted from assumed) would have been a no-op.
pub fn start_node(
    relay_url: &str,
    owner: PublicKey,
    node_keys: Keys,
    workspace_root: PathBuf,
) -> NodeHandle {
    let stub_runtime = AcpRuntime {
        harness_command: stub_agent_path().to_string_lossy().into_owned(),
        harness_args: Vec::new(),
    };
    let substrate: Arc<dyn Substrate> = Arc::new(LocalProcessSubstrate::new(
        Arc::new(stub_runtime),
        relay_url.to_string(),
        workspace_root,
    ));
    let relay: Box<dyn NodeRelay> = Box::new(NostrNodeRelay::new(
        relay_url.to_string(),
        node_keys.clone(),
        owner,
    ));
    let cfg = EngineConfig {
        reconcile_tick: Duration::from_secs(1),
        presence_interval: Duration::from_secs(30),
        node_pubkey: node_keys.public_key(),
    };
    let task_substrate = Arc::clone(&substrate);
    let task = tokio::spawn(engine::run(task_substrate, relay, node_keys, cfg));
    NodeHandle { task, substrate }
}

/// Owner publishes a real `NODE_ENROLLMENT` for `node_pubkey`, over
/// `client` (already NIP-42-authenticated as the owner). This drives only
/// the owner side of the real wire event -- unlike `tests/e2e_node.rs`,
/// this harness does not also run the node-side `buzz_node::enroll::enroll`
/// handshake (which blocks waiting for exactly this event and persists a
/// `NodeConfig` to this machine's real `~/.buzz-node/config.json`), since
/// [`start_node`] builds the engine directly instead of through the CLI's
/// enroll/up flow.
pub async fn enroll(client: &mut BuzzTestClient, owner: &Keys, node_pubkey: PublicKey) {
    let event = build_enrollment(owner, &node_pubkey, now_unix()).expect("build enrollment event");
    let ok = client.send_event(event).await.expect("publish enrollment");
    assert!(ok.accepted, "relay rejected enrollment: {}", ok.message);
}

/// Owner publishes a real, NIP-44-encrypted `AGENT_ASSIGNMENT` assigning
/// `agent` to `node_pubkey`, over `client`. Calling this a second time for
/// the same `agent` with a different `node_pubkey` IS the "move" this test
/// proves: the relay keeps only the latest `d=agent` record, but every node
/// that has ever seen ANY assignment for this agent is already
/// live-subscribed to the owner's whole `AGENT_ASSIGNMENT` stream (no
/// per-node filter -- see `nostr_relay::ActorState::ensure_connected`'s
/// owner-only filter), so both the old and new target node observe this
/// same event and react.
pub async fn assign(
    client: &mut BuzzTestClient,
    owner: &Keys,
    node_pubkey: PublicKey,
    agent: &Keys,
) {
    let secret = AssignmentSecret {
        format: buzz_core::assignment::FORMAT.into(),
        version: buzz_core::assignment::VERSION,
        agent_pubkey: agent.public_key().to_hex(),
        owner_pubkey: owner.public_key().to_hex(),
        node_pubkey: node_pubkey.to_hex(),
        private_key_nsec: agent.secret_key().to_bech32().expect("encode agent nsec"),
        auth_tag: None,
        // `command` is inert here: `AcpRuntime` never reads it (see
        // `start_node`'s doc comment) -- it only needs to be structurally
        // valid, mirroring `tests/e2e_node.rs`'s identical `"true"` filler.
        launch: LaunchBlock {
            command: "true".into(),
            args: vec![],
            env: std::collections::BTreeMap::new(),
            policy_env: std::collections::BTreeMap::new(),
            owner_pubkey: Some(owner.public_key().to_hex()),
        },
        env_vars: std::collections::BTreeMap::new(),
        reap_after_idle_seconds: None,
    };
    let event = build_assignment(
        owner,
        &node_pubkey,
        &secret,
        AssignState::Assigned,
        now_unix(),
    )
    .expect("build assignment event");
    let ok = client.send_event(event).await.expect("publish assignment");
    assert!(ok.accepted, "relay rejected assignment: {}", ok.message);
}

/// Subscription id for the observer's `AGENT_NODE_STATUS` tail (see
/// [`subscribe_status`]).
const STATUS_SUB_ID: &str = "e2e-nodes-status";

/// Subscribe `client` to every `AGENT_NODE_STATUS` for `agent`, from any
/// node (deliberately no `author` filter, since the whole point is to see
/// reports from both N and M). Call once, before any assignment that might
/// produce a status for `agent`, so [`await_status`]/[`collect_status`]
/// never miss an early transition.
pub async fn subscribe_status(client: &mut BuzzTestClient, agent: PublicKey) {
    let filter = Filter::new()
        .kind(Kind::Custom(KIND_AGENT_NODE_STATUS as u16))
        .custom_tag(SingleLetterTag::lowercase(Alphabet::D), agent.to_hex());
    client
        .subscribe(STATUS_SUB_ID, vec![filter])
        .await
        .expect("subscribe to agent node status");
}

/// One observed, validated `(node, health)` transition for the agent
/// [`subscribe_status`] was subscribed for.
#[derive(Debug, Clone)]
pub struct StatusEvent {
    /// The node that authored (and cryptographically signed) this status.
    pub node: PublicKey,
    /// The health it reported.
    pub health: AgentHealth,
}

/// Block until `client`'s open status subscription observes `node`
/// reporting `health`, or `deadline` elapses. Panics on timeout or a
/// transport failure -- this is a test assertion, not a library call.
pub async fn await_status(
    client: &mut BuzzTestClient,
    node: PublicKey,
    health: AgentHealth,
    deadline: Duration,
) {
    let end = tokio::time::Instant::now() + deadline;
    loop {
        let remaining = end.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for {health:?}@{}", node.to_hex());
        }
        match client.recv_event(remaining).await {
            Ok(RelayMessage::Event {
                subscription_id,
                event,
            }) if subscription_id == STATUS_SUB_ID => {
                if let Ok(status) = validate_status(&event) {
                    if status.node_pubkey == node.to_hex() && status.health == health {
                        return;
                    }
                }
            }
            Ok(_other) => {} // EOSE/OK/NOTICE/AUTH/CLOSED/unrelated EVENT -- keep waiting
            Err(TestClientError::Timeout) => {} // no news yet; poll again
            Err(e) => panic!("status stream read failed: {e}"),
        }
    }
}

/// Drain `client`'s open status subscription into an ordered `(node,
/// health)` timeline, stopping as soon as `target` is observed reporting
/// `Running`, or once `window` elapses -- whichever comes first. A caller
/// that never sees `target` reach `Running` gets back the full partial
/// timeline collected during `window`, so a genuine correctness failure
/// (the deferred spawn never firing) fails an assertion on that data
/// instead of hanging indefinitely.
pub async fn collect_status(
    client: &mut BuzzTestClient,
    target: PublicKey,
    window: Duration,
) -> Vec<StatusEvent> {
    let end = tokio::time::Instant::now() + window;
    let mut timeline = Vec::new();
    loop {
        let remaining = end.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return timeline;
        }
        match client.recv_event(remaining).await {
            Ok(RelayMessage::Event {
                subscription_id,
                event,
            }) if subscription_id == STATUS_SUB_ID => {
                if let Ok(status) = validate_status(&event) {
                    if let Ok(node) = PublicKey::from_hex(&status.node_pubkey) {
                        let health = status.health;
                        let reached_target = node == target && health == AgentHealth::Running;
                        timeline.push(StatusEvent { node, health });
                        if reached_target {
                            return timeline;
                        }
                    }
                }
            }
            Ok(_other) => {}
            Err(TestClientError::Timeout) => {}
            Err(e) => {
                eprintln!("status stream read failed while collecting: {e}");
                return timeline;
            }
        }
    }
}

/// True iff `timeline` contains at least one `(node, health)` entry.
pub fn saw(timeline: &[StatusEvent], node: PublicKey, health: AgentHealth) -> bool {
    timeline
        .iter()
        .any(|e| e.node == node && e.health == health)
}

/// The I4 proof: true iff `timeline` never shows two distinct nodes both
/// reporting the agent alive (`Starting`/`Running` -- the same "alive" test
/// `move_gate::PeerStatusView::peer_blocks_spawn` uses) with no intervening
/// non-alive report for the first. Tracks, at each point in the timeline,
/// which single node (if any) is the "currently alive" one; a second,
/// different node reporting itself alive while that's still set is a
/// double-live-instance violation.
pub fn never_two_running(timeline: &[StatusEvent]) -> bool {
    fn is_alive(health: AgentHealth) -> bool {
        matches!(health, AgentHealth::Starting | AgentHealth::Running)
    }
    let mut alive_node: Option<PublicKey> = None;
    for event in timeline {
        if is_alive(event.health) {
            if let Some(prev) = alive_node {
                if prev != event.node {
                    return false;
                }
            }
            alive_node = Some(event.node);
        } else if alive_node == Some(event.node) {
            alive_node = None;
        }
    }
    true
}
