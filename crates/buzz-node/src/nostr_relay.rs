//! Real Nostr relay client for the node: dial-out + NIP-42 auth, owner-scoped
//! `AGENT_ASSIGNMENT` intake (decrypt + target-node filter), and
//! status/announce/presence publish. Mirrors the dial-out/NIP-42/reconnect
//! pattern in `crates/buzz-acp/src/relay.rs`, built on `buzz-ws-client` (which
//! has no reconnect of its own — this module owns that).
use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use buzz_core::assignment::decrypt_for_node;
use buzz_core::kind::{KIND_AGENT_ASSIGNMENT, KIND_PRESENCE_UPDATE};
use buzz_core::{AgentNodeStatus, NodeCapabilities};
use buzz_ws_client::{NostrWsConnection, RelayMessage, WsClientError};
use nostr::{Event, EventBuilder, Filter, Keys, Kind, PublicKey};
use serde_json::json;
use tokio::sync::Mutex;

use crate::model::{DesiredAgent, NodeError};
use crate::relay::NodeRelay;

/// Validate and target-node-decrypt an `AGENT_ASSIGNMENT` event, returning a
/// [`DesiredAgent`] only when `event` is a well-formed, owner-signed envelope
/// that decrypts under `node_keys` (i.e. this node is its `node` tag).
///
/// Pure and I/O-free: safe to unit test with events built by
/// [`buzz_core::assignment::build_assignment`], no relay required. Both
/// [`buzz_core::AssignState::Assigned`] and `Unassigned` envelopes decrypt to
/// `Some` — the desired-state's `state` field carries the lifecycle intent
/// through to [`crate::reconcile::reconcile`], which is what actually decides
/// to stop an unassigned-but-still-running agent.
pub fn desired_from_event(
    event: &Event,
    node_keys: &Keys,
    owner: &PublicKey,
) -> Option<DesiredAgent> {
    let (envelope, secret) = decrypt_for_node(event, node_keys, owner).ok()?;
    Some(DesiredAgent {
        agent_pubkey: envelope.agent_pubkey,
        secret,
        state: envelope.state,
    })
}

/// Subscription id for this node's `AGENT_ASSIGNMENT` stream. Fixed and
/// process-local: this type holds exactly one such subscription at a time.
const ASSIGNMENT_SUB_ID: &str = "buzz-node-assignments";

/// How long a single read waits for the next relay message before looping
/// back to poll again. Bounded (rather than unbounded/`Duration::MAX`) so
/// the per-read `tokio::time::timeout` deadline computation can never
/// overflow, while still being long enough that idle waiting doesn't spin.
/// The engine's own reconcile ticker can preempt a wait via `select!`
/// regardless of this value — see [`NodeRelay::next_desired`]'s caller in
/// `crate::engine::run`.
const READ_POLL_TIMEOUT: Duration = Duration::from_secs(25);

/// Reconnect backoff ladder in seconds: 1, 2, 4, 8, 16, then capped at 30.
const BACKOFF_LADDER_SECS: [u64; 6] = [1, 2, 4, 8, 16, 30];

/// The backoff delay before the `attempt`-th (0-indexed) reconnect retry.
/// Pure — safe to unit test without a relay.
fn backoff_delay(attempt: usize) -> Duration {
    let idx = attempt.min(BACKOFF_LADDER_SECS.len() - 1);
    Duration::from_secs(BACKOFF_LADDER_SECS[idx])
}

/// Current Unix time in seconds, for the `created_at` the node-event codecs
/// require. Clamped to 0 rather than panicking if the system clock is ever
/// set before the epoch.
fn now_unix() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0)
}

/// A live, authenticated, subscribed connection.
struct Conn {
    ws: NostrWsConnection,
}

/// Real [`NodeRelay`]: dials `relay_url`, NIP-42 authenticates as the node,
/// subscribes to the owner's `AGENT_ASSIGNMENT` stream, and publishes
/// status/announce/presence — all over one lazily-established, reconnecting
/// connection. `buzz-ws-client` itself has no reconnect logic; this type
/// owns that (mirrors `crates/buzz-acp/src/relay.rs`'s dial-out pattern).
pub struct NostrNodeRelay {
    node_keys: Keys,
    owner_pubkey: PublicKey,
    relay_url: String,
    conn: Mutex<Option<Conn>>,
    /// Latest known desired-state per agent (last-writer-wins by the
    /// underlying addressable event's `created_at`, enforced relay-side by
    /// NIP-33). Mutated only from `next_desired`, which holds `&mut self`.
    desired: BTreeMap<PublicKey, DesiredAgent>,
}

impl NostrNodeRelay {
    /// Build a relay client for `node_keys`, scoped to `owner_pubkey`'s
    /// assignments. Dialing happens lazily on first use (`next_desired` or
    /// any `publish_*` call), not in this constructor.
    pub fn new(relay_url: String, node_keys: Keys, owner_pubkey: PublicKey) -> Self {
        Self {
            node_keys,
            owner_pubkey,
            relay_url,
            conn: Mutex::new(None),
            desired: BTreeMap::new(),
        }
    }

    /// Ensure `*guard` holds a live, authenticated, subscribed connection,
    /// (re)connecting with the [`backoff_delay`] ladder until it succeeds.
    /// Never gives up: a down relay is a condition this daemon must ride
    /// out, not a fatal error — there is no shutdown signal wired through
    /// the [`NodeRelay`] trait other than `next_desired` returning `None`,
    /// which this type never does on its own. Agents already running are
    /// unaffected by an outage here (they are owned by the `Substrate`, not
    /// the relay connection), so blocking a `publish_*` call until the
    /// relay comes back does not stop or restart anything.
    async fn ensure_connected(&self, guard: &mut Option<Conn>) {
        if guard.is_some() {
            return;
        }
        let mut attempt = 0usize;
        loop {
            match NostrWsConnection::connect_authenticated(&self.relay_url, &self.node_keys, None)
                .await
            {
                Ok(mut ws) => {
                    let filter = Filter::new()
                        .kind(Kind::Custom(KIND_AGENT_ASSIGNMENT as u16))
                        .author(self.owner_pubkey);
                    match ws
                        .send_raw(&json!(["REQ", ASSIGNMENT_SUB_ID, filter]))
                        .await
                    {
                        Ok(()) => {
                            *guard = Some(Conn { ws });
                            return;
                        }
                        Err(e) => tracing::warn!(
                            error = %e,
                            relay_url = %self.relay_url,
                            "failed to subscribe after connect; retrying"
                        ),
                    }
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    attempt,
                    relay_url = %self.relay_url,
                    "node relay connect failed; retrying"
                ),
            }
            tokio::time::sleep(backoff_delay(attempt)).await;
            attempt += 1;
        }
    }

    /// Publish a pre-built, signed event, retrying transport failures behind
    /// the same reconnect-forever policy as [`Self::ensure_connected`]. An
    /// explicit relay rejection (`OK false`) is NOT retried — that is a
    /// policy/validation outcome retrying can't fix.
    async fn publish(&self, event: Event) -> Result<(), NodeError> {
        loop {
            let mut guard = self.conn.lock().await;
            self.ensure_connected(&mut guard).await;
            let Some(conn) = guard.as_mut() else {
                return Err(NodeError::Relay(
                    "connection unexpectedly absent after connect".into(),
                ));
            };
            match conn.ws.send_event(event.clone()).await {
                Ok(ok) if ok.accepted => return Ok(()),
                Ok(ok) => {
                    return Err(NodeError::Relay(format!(
                        "event rejected by relay: {}",
                        ok.message
                    )))
                }
                Err(e) => {
                    tracing::warn!(error = %e, "publish failed; reconnecting");
                    *guard = None;
                }
            }
        }
    }
}

#[async_trait]
impl NodeRelay for NostrNodeRelay {
    async fn next_desired(&mut self) -> Option<Vec<DesiredAgent>> {
        loop {
            let mut guard = self.conn.lock().await;
            self.ensure_connected(&mut guard).await;
            let Some(conn) = guard.as_mut() else {
                continue;
            };
            match conn.ws.next_event(READ_POLL_TIMEOUT).await {
                Ok(RelayMessage::Event {
                    subscription_id,
                    event,
                }) if subscription_id == ASSIGNMENT_SUB_ID => {
                    let update = desired_from_event(&event, &self.node_keys, &self.owner_pubkey);
                    // Drop the connection lock before touching `self.desired`
                    // — this iteration no longer needs the connection, and
                    // dropping explicitly sidesteps any doubt about whether
                    // the borrow checker would see `conn`/`desired` as
                    // disjoint fields through the `guard` indirection.
                    drop(guard);
                    if let Some(d) = update {
                        self.desired.insert(d.agent_pubkey, d);
                        return Some(self.desired.values().cloned().collect());
                    }
                    // Not decryptable / not ours — keep waiting.
                }
                Ok(_other) => {
                    // EOSE / OK / NOTICE / AUTH / CLOSED / unrelated EVENT —
                    // no desired-state change; keep waiting.
                }
                Err(WsClientError::Timeout) => {
                    // No news within this poll window; loop and read again.
                    // The engine's own `select!` against its reconcile
                    // ticker can still preempt this future at any `.await`.
                }
                Err(e) => {
                    tracing::warn!(error = %e, "assignment stream read failed; reconnecting");
                    *guard = None;
                }
            }
        }
    }

    async fn publish_status(&self, status: &AgentNodeStatus) -> Result<(), NodeError> {
        let event = buzz_core::node_status::build_status(&self.node_keys, status, now_unix())
            .map_err(|e| NodeError::Relay(format!("build status event: {e}")))?;
        self.publish(event).await
    }

    async fn publish_announce(&self, caps: &NodeCapabilities) -> Result<(), NodeError> {
        let event = buzz_core::node::build_announce(&self.node_keys, caps, now_unix())
            .map_err(|e| NodeError::Relay(format!("build announce event: {e}")))?;
        self.publish(event).await
    }

    async fn publish_presence(&self, online: bool) -> Result<(), NodeError> {
        let content = if online { "online" } else { "offline" };
        // Mirrors `crates/buzz-acp/src/lib.rs`'s `publish_presence`: bare
        // status string, no tags — matching the desktop client's format.
        let event = EventBuilder::new(Kind::Custom(KIND_PRESENCE_UPDATE as u16), content)
            .tags([])
            .sign_with_keys(&self.node_keys)
            .map_err(|e| NodeError::Relay(format!("build presence event: {e}")))?;
        self.publish(event).await
    }
}

#[cfg(test)]
mod tests {
    use buzz_core::assignment::{
        build_assignment, AssignState, AssignmentSecret, LaunchBlock, FORMAT, VERSION,
    };
    use nostr::{Keys, ToBech32};
    use std::collections::BTreeMap;

    use super::desired_from_event;

    fn secret_for(owner: &Keys, agent: &Keys, node: &Keys) -> AssignmentSecret {
        AssignmentSecret {
            format: FORMAT.into(),
            version: VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            owner_pubkey: owner.public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            private_key_nsec: agent.secret_key().to_bech32().unwrap(),
            auth_tag: None,
            launch: LaunchBlock {
                command: "claude".into(),
                args: vec![],
                env: BTreeMap::new(),
                policy_env: BTreeMap::new(),
                owner_pubkey: Some(owner.public_key().to_hex()),
            },
            env_vars: BTreeMap::new(),
            reap_after_idle_seconds: None,
        }
    }

    fn make_assignment(
        owner: &Keys,
        agent: &Keys,
        node: &Keys,
        state: AssignState,
    ) -> nostr::Event {
        build_assignment(
            owner,
            &node.public_key(),
            &secret_for(owner, agent, node),
            state,
            1_785_780_000,
        )
        .unwrap()
    }

    #[test]
    fn desired_for_target_node_carries_agent_and_state() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let ev = make_assignment(&owner, &agent, &node, AssignState::Assigned);

        let d =
            desired_from_event(&ev, &node, &owner.public_key()).expect("target node gets desired");
        assert_eq!(d.agent_pubkey, agent.public_key());
        assert_eq!(d.state, AssignState::Assigned);
        assert_eq!(
            Keys::parse(&d.secret.private_key_nsec)
                .unwrap()
                .public_key(),
            agent.public_key()
        );
    }

    #[test]
    fn desired_carries_unassigned_state_through_for_target_node() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let ev = make_assignment(&owner, &agent, &node, AssignState::Unassigned);

        let d = desired_from_event(&ev, &node, &owner.public_key())
            .expect("target node still decrypts an unassigned envelope");
        assert_eq!(d.state, AssignState::Unassigned);
    }

    #[test]
    fn non_target_node_yields_none() {
        let (owner, agent, node, other) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let ev = make_assignment(&owner, &agent, &node, AssignState::Assigned);
        assert!(desired_from_event(&ev, &other, &owner.public_key()).is_none());
    }

    #[test]
    fn wrong_owner_yields_none() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let ev = make_assignment(&owner, &agent, &node, AssignState::Assigned);
        assert!(desired_from_event(&ev, &node, &Keys::generate().public_key()).is_none());
    }

    // --- backoff_delay (pure) ---

    use super::backoff_delay;
    use std::time::Duration;

    #[test]
    fn backoff_delay_ladders_then_caps() {
        assert_eq!(backoff_delay(0), Duration::from_secs(1));
        assert_eq!(backoff_delay(1), Duration::from_secs(2));
        assert_eq!(backoff_delay(2), Duration::from_secs(4));
        assert_eq!(backoff_delay(3), Duration::from_secs(8));
        assert_eq!(backoff_delay(4), Duration::from_secs(16));
        assert_eq!(backoff_delay(5), Duration::from_secs(30));
        assert_eq!(
            backoff_delay(999),
            Duration::from_secs(30),
            "ladder must cap, not index-panic, past its last rung"
        );
    }

    // --- NostrNodeRelay (live I/O — requires a real relay) ---

    use super::NostrNodeRelay;
    use crate::relay::NodeRelay;

    /// Requires a running relay. Run with:
    ///   `BUZZ_TEST_RELAY_URL=ws://localhost:3000 cargo test -p buzz-node --lib -- --ignored nostr_relay::tests::live_`
    #[tokio::test]
    #[ignore = "requires a running relay; set BUZZ_TEST_RELAY_URL (see crates/buzz-test-client)"]
    async fn live_announce_status_presence_publish_and_subscribe_connects() {
        let relay_url = std::env::var("BUZZ_TEST_RELAY_URL").expect("set BUZZ_TEST_RELAY_URL");
        let owner = Keys::generate();
        let node = Keys::generate();
        let mut relay = NostrNodeRelay::new(relay_url, node.clone(), owner.public_key());

        let caps = buzz_core::NodeCapabilities {
            format: buzz_core::node::FORMAT.into(),
            version: buzz_core::node::VERSION,
            node_pubkey: node.public_key().to_hex(),
            os: "test".into(),
            runtimes: vec![],
            workspace_root: "/tmp".into(),
            max_agents: None,
        };
        relay.publish_announce(&caps).await.expect("announce");

        let status = buzz_core::AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: Keys::generate().public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            health: buzz_core::AgentHealth::Running,
            reason: None,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        relay.publish_status(&status).await.expect("status");
        relay.publish_presence(true).await.expect("presence");

        // No assignments published in this smoke test; just prove the
        // subscribe path connects and the read loop is pollable.
        let _ = tokio::time::timeout(Duration::from_secs(2), relay.next_desired()).await;
    }
}
