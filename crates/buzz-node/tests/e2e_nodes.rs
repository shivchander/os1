//! Two-node execution-node end-to-end proof: enroll two nodes, assign an
//! agent, move it between them, and kill a node -- asserting the
//! at-most-one-live-instance invariant (spec I4) and correct
//! `AGENT_NODE_STATUS` transitions against a REAL relay.
//!
//! This is the multi-node sibling of `tests/e2e_node.rs` (the Phase 3
//! single-node enroll -> assign -> running proof): it drives TWO real node
//! engines -- `NostrNodeRelay` + `LocalProcessSubstrate` + `AcpRuntime` +
//! `engine::run`, exactly the wiring `crates/buzz-node/src/daemon.rs`'s
//! `up_foreground` uses for a real `buzz-node up` -- against one live
//! relay, and proves the Phase 5 bounded stop-before-start move gate
//! (`buzz_node::move_gate`, spec I4) actually prevents two simultaneous
//! live instances during a real owner-initiated move, not just in the
//! in-memory `FakeRelay`/`FakeSubstrate` unit tests already covering it in
//! `crates/buzz-node/src/engine.rs`.
//!
//! See `tests/harness/mod.rs` for the shared glue this test drives:
//! `start_node` (real engine wiring, stub-agent-backed `AcpRuntime`),
//! `enroll`/`assign` (owner-published `NODE_ENROLLMENT`/`AGENT_ASSIGNMENT`
//! via a `buzz_test_client::BuzzTestClient`), and
//! `subscribe_status`/`await_status`/`collect_status`/`saw`/
//! `never_two_running` (the owner's observed `AGENT_NODE_STATUS` timeline
//! and the I4 check over it).
//!
//! ## Preconditions
//!
//! Requires a running relay with Postgres + Redis, exactly like
//! `tests/e2e_node.rs` and `crates/buzz-test-client`'s own `#[ignore]`d e2e
//! suite (see its module for the general live-relay pattern this mirrors):
//!
//! ```text
//! just relay   # brings up Docker Postgres+Redis if needed; serves ws://localhost:3000
//! ```
//!
//! Then, in another terminal:
//!
//! ```text
//! BUZZ_TEST_RELAY_URL=ws://localhost:3000 \
//!     cargo test -p buzz-node --test e2e_nodes -- --ignored --nocapture
//! ```
//!
//! `#[ignore]`d for the same reason as `tests/e2e_node.rs`: it needs a live
//! relay, so it must never run as part of the default `cargo test`/CI.
use std::time::Duration;

use nostr::Keys;

use buzz_core::AgentHealth;
use buzz_test_client::BuzzTestClient;

mod harness;

/// Overall deadline for the owner to observe the agent `Running` on its
/// initially-assigned node, before the move even starts. Mirrors
/// `tests/e2e_node.rs`'s identical `STATUS_DEADLINE`.
const INITIAL_RUNNING_DEADLINE: Duration = Duration::from_secs(30);

/// Upper bound on how long the owner-observed timeline collection waits for
/// the move to complete (`Running` on the new node). Comfortably above
/// `buzz_node::move_gate::MOVE_HANDOFF_TIMEOUT` (30s) plus relay/reconcile
/// latency, so a genuine correctness failure (the deferred spawn never
/// firing) fails an assertion on the partial timeline instead of hanging.
const MOVE_DEADLINE: Duration = Duration::from_secs(40);

/// Deadline to confirm the agent is still `Running` on its new node once
/// its old node's engine is killed.
const POST_KILL_DEADLINE: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running relay; set BUZZ_TEST_RELAY_URL (see crates/buzz-node/tests/e2e_node.rs)"]
async fn assign_move_and_kill_keeps_single_live_instance() {
    let relay_url = std::env::var("BUZZ_TEST_RELAY_URL").expect("set BUZZ_TEST_RELAY_URL");
    let owner = Keys::generate();
    let node_n = Keys::generate();
    let node_m = Keys::generate();
    let agent = Keys::generate();

    let mut owner_client = BuzzTestClient::connect(&relay_url, &owner)
        .await
        .expect("owner connect");
    // Subscribe before anything happens so the timeline below is never
    // missing an early transition.
    harness::subscribe_status(&mut owner_client, agent.public_key()).await;

    // --- 1. Start two real node engines (N, M) against the live relay.
    let n = harness::start_node(
        &relay_url,
        owner.public_key(),
        node_n.clone(),
        std::env::temp_dir().join(format!("buzz-node-e2e-n-{}", node_n.public_key().to_hex())),
    );
    let m = harness::start_node(
        &relay_url,
        owner.public_key(),
        node_m.clone(),
        std::env::temp_dir().join(format!("buzz-node-e2e-m-{}", node_m.public_key().to_hex())),
    );
    // Give both engines a moment to connect and subscribe before the owner
    // publishes anything. Not strictly required for correctness (a fresh
    // relay subscription replays matching backlog to a late subscriber
    // too), but keeps this test's timing close to the real startup order,
    // mirroring `tests/e2e_node.rs`'s identical pre-publish pause.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- 2. Enroll both nodes with the owner.
    harness::enroll(&mut owner_client, &owner, node_n.public_key()).await;
    harness::enroll(&mut owner_client, &owner, node_m.public_key()).await;

    // --- 3. Assign agent A -> N; confirm it comes up Running on N.
    harness::assign(&mut owner_client, &owner, node_n.public_key(), &agent).await;
    harness::await_status(
        &mut owner_client,
        node_n.public_key(),
        AgentHealth::Running,
        INITIAL_RUNNING_DEADLINE,
    )
    .await;

    // --- 4. Move A -> M. The owner-observed timeline must show Stopped@N
    // strictly before Running@M, and never two nodes Running at once (I4).
    harness::assign(&mut owner_client, &owner, node_m.public_key(), &agent).await;
    let timeline =
        harness::collect_status(&mut owner_client, node_m.public_key(), MOVE_DEADLINE).await;

    assert!(
        harness::never_two_running(&timeline),
        "I4 violated: two nodes reported the agent alive with no intervening stop: {timeline:?}"
    );
    assert!(
        harness::saw(&timeline, node_n.public_key(), AgentHealth::Stopped),
        "N must report Stopped once the agent moves away: {timeline:?}"
    );
    assert!(
        harness::saw(&timeline, node_m.public_key(), AgentHealth::Running),
        "M must report Running once the move completes: {timeline:?}"
    );
    let stopped_n_at = timeline
        .iter()
        .position(|e| e.node == node_n.public_key() && e.health == AgentHealth::Stopped)
        .expect("Stopped@N must appear in the timeline");
    let running_m_at = timeline
        .iter()
        .position(|e| e.node == node_m.public_key() && e.health == AgentHealth::Running)
        .expect("Running@M must appear in the timeline");
    assert!(
        stopped_n_at < running_m_at,
        "expected Stopped@N before Running@M, got: {timeline:?}"
    );

    // --- 5. Kill N's engine task; A must stay Running on M.
    n.task.abort();
    harness::await_status(
        &mut owner_client,
        node_m.public_key(),
        AgentHealth::Running,
        POST_KILL_DEADLINE,
    )
    .await;

    // --- Cleanup: stop the still-running stub agent on M explicitly. See
    // `harness::NodeHandle`'s doc comment for why aborting `m.task` alone
    // would leak the spawned stub process instead of terminating it.
    let _ = m.substrate.stop(&agent.public_key()).await;
    m.task.abort();
    let _ = owner_client.disconnect().await;
}
