//! Real Nostr relay client for the node: dial-out + NIP-42 auth, owner-scoped
//! `AGENT_ASSIGNMENT` intake (decrypt + target-node filter), and
//! status/announce/presence publish. Mirrors the dial-out/NIP-42/reconnect
//! pattern in `crates/buzz-acp/src/relay.rs`, built on `buzz-ws-client` (which
//! has no reconnect of its own — this module owns that). Publishes run on a
//! background task (see [`NostrNodeRelay::spawn_publish`]) so a down relay
//! never blocks the node's local reconcile loop.
use std::collections::BTreeMap;
use std::sync::Arc;
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

/// Subscription id for a one-shot resync query (see
/// [`NostrNodeRelay::fetch_assignment_backlog`]) — distinct from
/// [`ASSIGNMENT_SUB_ID`]'s long-lived live tail so the two can never be
/// confused, though `engine::run` only ever calls `query_desired` between
/// `select!` iterations, never concurrently with `next_desired`.
const RESYNC_SUB_ID: &str = "buzz-node-resync";

/// Upper bound on how long a one-shot resync query waits for EOSE, so a
/// relay that never closes the subscription can't hang startup or a
/// post-reconnect resync forever.
const RESYNC_TIMEOUT: Duration = Duration::from_secs(10);

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
/// set before the epoch. `pub(crate)` so `crate::enroll` can reuse it for
/// its own `NODE_ANNOUNCE`/`NODE_ENROLLMENT` timestamps.
pub(crate) fn now_unix() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0)
}

/// A live, authenticated, subscribed connection.
struct Conn {
    ws: NostrWsConnection,
}

/// Shared connection state, wrapped in an `Arc` so the background publish
/// task spawned by [`NostrNodeRelay::spawn_publish`] can outlive the
/// `&self` call that spawned it.
struct Inner {
    node_keys: Keys,
    owner_pubkey: PublicKey,
    relay_url: String,
    conn: Mutex<Option<Conn>>,
    /// Backing flag for [`NostrNodeRelay::take_reconnected`]: set whenever
    /// [`Inner::ensure_connected`] (re)establishes the connection, including
    /// the very first connect — so the engine's first check also drives its
    /// startup resync.
    reconnected: std::sync::atomic::AtomicBool,
}

impl Inner {
    /// Ensure `*guard` holds a live, authenticated, subscribed connection,
    /// (re)connecting with the [`backoff_delay`] ladder until it succeeds.
    /// Never gives up: a down relay is a condition this daemon must ride
    /// out, not a fatal error — there is no shutdown signal wired through
    /// the [`NodeRelay`] trait other than `next_desired` returning `None`,
    /// which this type never does on its own.
    ///
    /// Called from two places with different blocking implications.
    /// `next_desired` awaits this directly, which is safe because
    /// `engine::run` races `next_desired` against its own reconcile ticker
    /// via `select!` — a long reconnect wait there just means the ticker
    /// branch wins that loop iteration instead of the assignment-read
    /// branch. The publish path never awaits this on the caller's task at
    /// all: [`NostrNodeRelay::spawn_publish`] runs it inside a
    /// `tokio::spawn`ed background task precisely so a down relay can't
    /// block `engine::run`'s per-agent `relay.publish_status(...).await?`
    /// — which would otherwise stall the whole reconcile loop before it
    /// ever reaches the next `substrate.observe()`, leaving a mid-outage
    /// agent crash undetected and un-restarted until the relay reconnects.
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
                            self.reconnected
                                .store(true, std::sync::atomic::Ordering::SeqCst);
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

    /// The actual send-with-retry loop: (re)connects via
    /// [`Self::ensure_connected`] and sends `event`, retrying transport
    /// failures behind the same reconnect-forever policy. An explicit relay
    /// rejection (`OK false`) is NOT retried — that is a policy/validation
    /// outcome retrying can't fix. Always runs inside the background task
    /// spawned by [`NostrNodeRelay::spawn_publish`], never on a caller's task.
    async fn publish_with_retry(&self, event: Event) -> Result<(), NodeError> {
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

/// Real [`NodeRelay`]: dials `relay_url`, NIP-42 authenticates as the node,
/// subscribes to the owner's `AGENT_ASSIGNMENT` stream, and publishes
/// status/announce/presence — all over one lazily-established, reconnecting
/// connection. `buzz-ws-client` itself has no reconnect logic; this type
/// owns that (mirrors `crates/buzz-acp/src/relay.rs`'s dial-out pattern).
pub struct NostrNodeRelay {
    inner: Arc<Inner>,
    /// Latest known desired-state per agent plus LWW watermarks. `&self`
    /// interior mutability (a plain `std::sync::Mutex`, never held across an
    /// `.await`): [`NodeRelay::next_desired`] must stay concurrently
    /// pollable with [`NodeRelay::next_status`] in `engine::run`'s
    /// `select!`.
    desired: std::sync::Mutex<DesiredState>,
}

/// Accumulated live desired-state plus last-seen `created_at` per agent.
/// The relay's own NIP-33 replaceable-event semantics already keep only the
/// latest stored copy, but `seen_created_at` defends this client-side
/// accumulation against an older event reaching us after a newer one — e.g.
/// a live-tail event racing a concurrent [`NostrNodeRelay::query_desired`]
/// resync — from clobbering already-applied newer state.
#[derive(Default)]
struct DesiredState {
    desired: BTreeMap<PublicKey, DesiredAgent>,
    seen_created_at: BTreeMap<PublicKey, u64>,
}

/// Apply one `AGENT_ASSIGNMENT` event to `state`, returning `true` iff it
/// changed `state.desired` (the caller uses this to decide whether the
/// change is worth surfacing to the engine). Order of checks:
///
/// 1. Envelope invalid (bad signature, wrong owner, malformed tags) → ignore.
/// 2. Older than the last-seen `created_at` for this agent → ignore (LWW).
/// 3. Envelope targets a different node (including "moved away from us") →
///    remove any existing entry for this agent; changed iff one existed.
/// 4. Envelope targets us → decrypt and upsert (covers both `Assigned` and
///    `Unassigned` — the desired-state's own `state` field carries that
///    through to [`crate::reconcile::reconcile`]).
///
/// Pure and I/O-free (mirrors [`desired_from_event`]): unit-testable with
/// events built by [`buzz_core::assignment::build_assignment`], no relay
/// required.
fn apply_assignment_event(
    state: &mut DesiredState,
    event: &Event,
    node_keys: &Keys,
    owner: &PublicKey,
) -> bool {
    let Ok(envelope) = buzz_core::assignment::validate_envelope(event, owner) else {
        return false;
    };
    let created = event.created_at.as_secs();
    if state
        .seen_created_at
        .get(&envelope.agent_pubkey)
        .is_some_and(|&prev| created < prev)
    {
        return false;
    }
    state.seen_created_at.insert(envelope.agent_pubkey, created);

    if envelope.node_pubkey != node_keys.public_key() {
        return state.desired.remove(&envelope.agent_pubkey).is_some();
    }
    match desired_from_event(event, node_keys, owner) {
        Some(d) => {
            state.desired.insert(d.agent_pubkey, d);
            true
        }
        None => false,
    }
}

impl NostrNodeRelay {
    /// Build a relay client for `node_keys`, scoped to `owner_pubkey`'s
    /// assignments. Dialing happens lazily on first use (`next_desired` or
    /// any `publish_*` call), not in this constructor.
    pub fn new(relay_url: String, node_keys: Keys, owner_pubkey: PublicKey) -> Self {
        Self {
            inner: Arc::new(Inner {
                node_keys,
                owner_pubkey,
                relay_url,
                conn: Mutex::new(None),
                reconnected: std::sync::atomic::AtomicBool::new(true),
            }),
            desired: std::sync::Mutex::new(DesiredState::default()),
        }
    }

    /// Build this node's presence event without publishing it. Shared by the
    /// fire-and-forget [`NodeRelay::publish_presence`] and the awaited
    /// [`Self::publish_presence_awaited`], so the two can never drift apart
    /// on event shape.
    fn presence_event(&self, online: bool) -> Result<Event, NodeError> {
        let content = if online { "online" } else { "offline" };
        // Mirrors `crates/buzz-acp/src/lib.rs`'s `publish_presence`: bare
        // status string, no tags — matching the desktop client's format.
        EventBuilder::new(Kind::Custom(KIND_PRESENCE_UPDATE as u16), content)
            .tags([])
            .sign_with_keys(&self.inner.node_keys)
            .map_err(|e| NodeError::Relay(format!("build presence event: {e}")))
    }

    /// Publish presence and wait for the relay to actually accept it,
    /// instead of enqueueing on a background task like
    /// [`NodeRelay::publish_presence`] does.
    ///
    /// This exists solely for the daemon's shutdown path
    /// (`buzz-node`'s `daemon::run_until_shutdown`). By the time the process
    /// is exiting, a [`Self::spawn_publish`]-style detached background task
    /// can be killed before it ever runs — tokio does not wait for spawned
    /// tasks when the runtime shuts down — which would silently break the
    /// "clean shutdown ⇒ offline presence delivered" guarantee. Callers on
    /// the shutdown path should still wrap this in an external timeout:
    /// [`Inner::publish_with_retry`]'s reconnect-forever policy is correct
    /// for a long-lived background publish but would otherwise hang process
    /// exit indefinitely against a relay that never comes back.
    pub async fn publish_presence_awaited(&self, online: bool) -> Result<(), NodeError> {
        let event = self.presence_event(online)?;
        self.inner.publish_with_retry(event).await
    }

    /// Publish a pre-built, signed event on a background task, returning as
    /// soon as the task is enqueued — NOT once the event is actually
    /// delivered.
    ///
    /// This deliberately does not block the caller on relay reachability.
    /// `engine::run` calls `publish_status` once per agent, sequentially,
    /// inside its main reconcile loop
    /// (`relay.publish_status(&status).await?`); if that awaited delivery
    /// directly, a down relay would stall the loop before it ever reached
    /// its next `substrate.observe()`, so an agent that crashes mid-outage
    /// would go undetected and un-restarted until the relay reconnected —
    /// breaking the node's otherwise relay-independent local crash recovery.
    ///
    /// This is safe because every event `publish_*` builds is NIP-33
    /// addressable (last-writer-wins by `created_at`, which is captured
    /// synchronously *before* this task is spawned, so logical ordering is
    /// preserved even if delivery is delayed or arrives out of order): a
    /// late or retried background publish can only ever be superseded by a
    /// newer one, never corrupt relay state. A background failure (e.g. an
    /// explicit relay rejection) is only logged — there is no caller left
    /// awaiting this task's result to propagate it to.
    fn spawn_publish(&self, event: Event) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            if let Err(e) = inner.publish_with_retry(event).await {
                tracing::warn!(error = %e, "background publish failed");
            }
        });
    }

    /// Lock `self.desired`, recovering from a poisoned mutex rather than
    /// panicking (mirrors `LocalProcessSubstrate`'s `lock_table`/observe`
    /// precedent): another call already panicked while holding it, which
    /// must not additionally crash *this* caller — the desired-state map is
    /// read on `engine::run`'s hot path via `next_desired`, a method with
    /// no `Result` to propagate a poisoning error through anyway.
    fn lock_desired(&self) -> std::sync::MutexGuard<'_, DesiredState> {
        self.desired
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// One-shot query: (re)connect if needed, subscribe under
    /// [`RESYNC_SUB_ID`] to the same owner-scoped `AGENT_ASSIGNMENT` filter
    /// [`Inner::ensure_connected`] uses for the live tail, collect every
    /// backlog event up to EOSE (or [`RESYNC_TIMEOUT`], whichever comes
    /// first), then close the subscription. Used by
    /// [`NodeRelay::query_desired`] to rebuild desired-state from scratch on
    /// startup and after a reconnect.
    async fn fetch_assignment_backlog(&self) -> Result<Vec<Event>, NodeError> {
        let deadline = std::time::Instant::now() + RESYNC_TIMEOUT;
        let mut guard = self.inner.conn.lock().await;
        self.inner.ensure_connected(&mut guard).await;
        let Some(conn) = guard.as_mut() else {
            return Err(NodeError::Relay(
                "connection unexpectedly absent after connect".into(),
            ));
        };
        let filter = Filter::new()
            .kind(Kind::Custom(KIND_AGENT_ASSIGNMENT as u16))
            .author(self.inner.owner_pubkey);
        conn.ws
            .send_raw(&json!(["REQ", RESYNC_SUB_ID, filter]))
            .await
            .map_err(|e| NodeError::Relay(format!("resync subscribe: {e}")))?;

        let mut events = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!("resync query timed out waiting for EOSE; using partial results");
                break;
            }
            match conn.ws.next_event(remaining).await {
                Ok(RelayMessage::Event {
                    subscription_id,
                    event,
                }) if subscription_id == RESYNC_SUB_ID => events.push(*event),
                Ok(RelayMessage::Eose { subscription_id }) if subscription_id == RESYNC_SUB_ID => {
                    break
                }
                Ok(_other) => {
                    // A live-tail event, OK/NOTICE/AUTH, or another
                    // subscription's message — not this query's concern.
                }
                Err(WsClientError::Timeout) => {
                    // Loop; the outer `deadline` (not this per-read timeout)
                    // governs how long the whole query may run.
                }
                Err(e) => {
                    tracing::warn!(error = %e, "resync query read failed");
                    *guard = None;
                    return Err(NodeError::Relay(format!("resync query: {e}")));
                }
            }
        }
        let _ = conn.ws.send_raw(&json!(["CLOSE", RESYNC_SUB_ID])).await;
        Ok(events)
    }
}

#[async_trait]
impl NodeRelay for NostrNodeRelay {
    async fn next_desired(&self) -> Option<Vec<DesiredAgent>> {
        loop {
            let mut guard = self.inner.conn.lock().await;
            self.inner.ensure_connected(&mut guard).await;
            let Some(conn) = guard.as_mut() else {
                continue;
            };
            match conn.ws.next_event(READ_POLL_TIMEOUT).await {
                Ok(RelayMessage::Event {
                    subscription_id,
                    event,
                }) if subscription_id == ASSIGNMENT_SUB_ID => {
                    // Drop the connection lock before touching `self.desired`
                    // — this iteration no longer needs the connection, and
                    // dropping explicitly sidesteps any doubt about whether
                    // the borrow checker would see `conn`/`desired` as
                    // disjoint fields through the `guard` indirection.
                    drop(guard);
                    let mut state = self.lock_desired();
                    let changed = apply_assignment_event(
                        &mut state,
                        &event,
                        &self.inner.node_keys,
                        &self.inner.owner_pubkey,
                    );
                    if changed {
                        return Some(state.desired.values().cloned().collect());
                    }
                    // Stale, malformed, or targets neither us nor an agent
                    // we previously held — no desired-state change to
                    // surface; keep waiting.
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

    async fn query_desired(&self) -> Result<Vec<DesiredAgent>, NodeError> {
        let events = self.fetch_assignment_backlog().await?;
        let mut fresh = DesiredState::default();
        for event in &events {
            apply_assignment_event(
                &mut fresh,
                event,
                &self.inner.node_keys,
                &self.inner.owner_pubkey,
            );
        }
        let out: Vec<DesiredAgent> = fresh.desired.values().cloned().collect();
        *self.lock_desired() = fresh;
        Ok(out)
    }

    async fn next_status(&self) -> Option<AgentNodeStatus> {
        // Phase 5 batch A ships the move gate's pure logic
        // (`crate::move_gate`) and its `FakeRelay` wiring, proven by
        // `engine::run`'s unit tests, but does NOT yet subscribe this real
        // relay client to peer `AGENT_NODE_STATUS` events. Doing so
        // correctly needs a background task multiplexing two live
        // subscriptions over one connection — this method must stay
        // concurrently pollable with `next_desired` in `engine::run`'s
        // `select!`, which the current one-reader-per-call design can't do
        // for two long-lived streams at once. Until that lands, a *real*
        // node's move gate is reachable only via `MOVE_HANDOFF_TIMEOUT`
        // (never via an observed peer `stopped`), which still bounds a
        // move's overlap but not as tightly as intended. Tracked as a
        // follow-up — see the Phase 5 batch A report.
        std::future::pending().await
    }

    fn take_reconnected(&self) -> bool {
        self.inner
            .reconnected
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    async fn publish_status(&self, status: &AgentNodeStatus) -> Result<(), NodeError> {
        let event = buzz_core::node_status::build_status(&self.inner.node_keys, status, now_unix())
            .map_err(|e| NodeError::Relay(format!("build status event: {e}")))?;
        self.spawn_publish(event);
        Ok(())
    }

    async fn publish_announce(&self, caps: &NodeCapabilities) -> Result<(), NodeError> {
        let event = buzz_core::node::build_announce(&self.inner.node_keys, caps, now_unix())
            .map_err(|e| NodeError::Relay(format!("build announce event: {e}")))?;
        self.spawn_publish(event);
        Ok(())
    }

    async fn publish_presence(&self, online: bool) -> Result<(), NodeError> {
        let event = self.presence_event(online)?;
        self.spawn_publish(event);
        Ok(())
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
        make_assignment_at(owner, agent, node, state, 1_785_780_000)
    }

    fn make_assignment_at(
        owner: &Keys,
        agent: &Keys,
        node: &Keys,
        state: AssignState,
        created_at: u64,
    ) -> nostr::Event {
        build_assignment(
            owner,
            &node.public_key(),
            &secret_for(owner, agent, node),
            state,
            created_at,
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

    // --- apply_assignment_event: LWW + retarget-removal (Task 3, I4) ---

    use super::{apply_assignment_event, DesiredState};

    #[test]
    fn first_assignment_to_us_is_applied() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let mut state = DesiredState::default();
        let ev = make_assignment_at(&owner, &agent, &node, AssignState::Assigned, 1_000);
        assert!(apply_assignment_event(
            &mut state,
            &ev,
            &node,
            &owner.public_key()
        ));
        assert_eq!(state.desired.len(), 1);
        assert!(state.desired.contains_key(&agent.public_key()));
    }

    #[test]
    fn later_assignment_wins_regardless_of_arrival_order() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let mut state = DesiredState::default();
        // Apply the NEWER event first, then an OLDER one for the same agent
        // (out-of-order live delivery) — the older one must be ignored.
        let newer = make_assignment_at(&owner, &agent, &node, AssignState::Assigned, 2_000);
        let older = make_assignment_at(&owner, &agent, &node, AssignState::Unassigned, 1_000);
        assert!(apply_assignment_event(
            &mut state,
            &newer,
            &node,
            &owner.public_key()
        ));
        assert!(
            !apply_assignment_event(&mut state, &older, &node, &owner.public_key()),
            "a stale (older) event must not be applied"
        );
        // The newer `Assigned` state must still be in effect.
        assert_eq!(
            state.desired.get(&agent.public_key()).map(|d| d.state),
            Some(AssignState::Assigned)
        );
    }

    #[test]
    fn reassignment_to_another_node_removes_from_desired() {
        let (owner, agent, node_m, node_n) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let mut state = DesiredState::default();
        let to_m = make_assignment_at(&owner, &agent, &node_m, AssignState::Assigned, 1_000);
        assert!(apply_assignment_event(
            &mut state,
            &to_m,
            &node_m,
            &owner.public_key()
        ));
        assert!(state.desired.contains_key(&agent.public_key()));

        // Reassigned to N: from M's point of view the envelope now targets
        // someone else, and M can tell this from the PUBLIC envelope alone
        // (no decryption needed/possible — the ciphertext is encrypted to N).
        let to_n = make_assignment_at(&owner, &agent, &node_n, AssignState::Assigned, 2_000);
        assert!(apply_assignment_event(
            &mut state,
            &to_n,
            &node_m,
            &owner.public_key()
        ));
        assert!(
            !state.desired.contains_key(&agent.public_key()),
            "M must drop an agent reassigned away from it"
        );
    }

    #[test]
    fn stale_reassignment_away_does_not_undo_a_newer_assignment_to_us() {
        let (owner, agent, node_m, node_n) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let mut state = DesiredState::default();
        // A newer assignment to us lands first...
        let to_m = make_assignment_at(&owner, &agent, &node_m, AssignState::Assigned, 2_000);
        assert!(apply_assignment_event(
            &mut state,
            &to_m,
            &node_m,
            &owner.public_key()
        ));
        // ...then a STALE, older "assigned elsewhere" event arrives late.
        let stale_to_n = make_assignment_at(&owner, &agent, &node_n, AssignState::Assigned, 1_000);
        assert!(
            !apply_assignment_event(&mut state, &stale_to_n, &node_m, &owner.public_key()),
            "an out-of-order stale reassignment must not be applied"
        );
        assert!(
            state.desired.contains_key(&agent.public_key()),
            "the newer assignment to us must survive a stale, older, contradicting event"
        );
    }

    #[test]
    fn unrelated_node_assignment_is_a_no_op_not_a_change() {
        // A's assignment was never ours in the first place (e.g. another
        // node's agent on a shared owner-scoped subscription) — must not be
        // reported as a change.
        let (owner, agent, node_m, node_other) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let mut state = DesiredState::default();
        let to_other =
            make_assignment_at(&owner, &agent, &node_other, AssignState::Assigned, 1_000);
        assert!(!apply_assignment_event(
            &mut state,
            &to_other,
            &node_m,
            &owner.public_key()
        ));
        assert!(state.desired.is_empty());
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

    fn sample_status(node: &Keys) -> buzz_core::AgentNodeStatus {
        buzz_core::AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: Keys::generate().public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            health: buzz_core::AgentHealth::Running,
            reason: None,
            updated_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Port 1 on loopback: nothing listens there and nothing (short of
    /// root) ever will, so the TCP connect fails near-instantly
    /// (connection refused) instead of timing out — this proves the
    /// "returns promptly" property below without depending on slow or
    /// sandboxed outbound network access.
    const UNREACHABLE_RELAY_URL: &str = "ws://127.0.0.1:1";

    #[tokio::test]
    async fn publish_status_returns_promptly_when_relay_is_unreachable() {
        let node = Keys::generate();
        let owner = Keys::generate();
        let relay = NostrNodeRelay::new(
            UNREACHABLE_RELAY_URL.into(),
            node.clone(),
            owner.public_key(),
        );
        let status = sample_status(&node);

        // The old (pre-fix) behavior awaited delivery directly, which meant
        // this call would block for as long as the relay stayed
        // unreachable — effectively forever, since `ensure_connected`
        // retries without giving up. A bounded timeout here is a direct
        // regression test for that: it must resolve well within the first
        // couple of backoff rungs, not hang until (or past) them.
        let result = tokio::time::timeout(Duration::from_secs(2), relay.publish_status(&status))
            .await
            .expect("publish_status must return promptly (enqueue-and-return), not block on the relay being unreachable");
        result.expect("building/enqueueing the status event must still succeed synchronously");
    }

    #[tokio::test]
    async fn publish_presence_returns_promptly_when_relay_is_unreachable() {
        let node = Keys::generate();
        let owner = Keys::generate();
        let relay = NostrNodeRelay::new(UNREACHABLE_RELAY_URL.into(), node, owner.public_key());

        let result = tokio::time::timeout(Duration::from_secs(2), relay.publish_presence(true))
            .await
            .expect("publish_presence must return promptly (enqueue-and-return), not block on the relay being unreachable");
        result.expect("building/enqueueing the presence event must still succeed synchronously");
    }

    #[tokio::test]
    async fn publish_presence_awaited_blocks_on_an_unreachable_relay_instead_of_enqueueing() {
        let node = Keys::generate();
        let owner = Keys::generate();
        let relay = NostrNodeRelay::new(UNREACHABLE_RELAY_URL.into(), node, owner.public_key());

        // The inverse of `publish_presence_returns_promptly_...` above: unlike
        // the fire-and-forget trait method, this variant awaits real
        // delivery, so against a relay that will never come up it must NOT
        // resolve within a short window — `Inner::publish_with_retry` never
        // gives up. This is exactly the property the daemon shutdown path
        // depends on: `publish_presence_awaited` returning `Ok` is real
        // evidence of delivery, not just that a background task got
        // scheduled.
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            relay.publish_presence_awaited(false),
        )
        .await;
        assert!(
            result.is_err(),
            "publish_presence_awaited must actually wait for delivery, not enqueue-and-return"
        );
    }

    /// Requires a running relay. Run with:
    ///   `BUZZ_TEST_RELAY_URL=ws://localhost:3000 cargo test -p buzz-node --lib -- --ignored nostr_relay::tests::live_`
    #[tokio::test]
    #[ignore = "requires a running relay; set BUZZ_TEST_RELAY_URL (see crates/buzz-test-client)"]
    async fn live_announce_status_presence_publish_and_subscribe_connects() {
        let relay_url = std::env::var("BUZZ_TEST_RELAY_URL").expect("set BUZZ_TEST_RELAY_URL");
        let owner = Keys::generate();
        let node = Keys::generate();
        let relay = NostrNodeRelay::new(relay_url, node.clone(), owner.public_key());

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
        relay
            .publish_status(&sample_status(&node))
            .await
            .expect("status");
        relay.publish_presence(true).await.expect("presence");

        // No assignments published in this smoke test; just prove the
        // subscribe path connects and the read loop is pollable. Publishes
        // above are now fire-and-forget (see `spawn_publish`), so this also
        // gives the background tasks a moment to actually run against the
        // real relay before the test process exits.
        let _ = tokio::time::timeout(Duration::from_secs(2), relay.next_desired()).await;
    }
}
