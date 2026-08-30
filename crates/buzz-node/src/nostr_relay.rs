//! Real Nostr relay client for the node: dial-out + NIP-42 auth, owner-scoped
//! `AGENT_ASSIGNMENT` intake (decrypt + target-node filter), peer
//! `AGENT_NODE_STATUS` intake (authenticate + `#d`-scoped to learned
//! agents), and status/announce/presence publish. Mirrors the
//! dial-out/NIP-42/reconnect pattern in `crates/buzz-acp/src/relay.rs`,
//! built on `buzz-ws-client` (which has no reconnect of its own — this
//! module owns that). A single background actor (see [`ActorState`],
//! [`run_actor`]) owns the one connection and multiplexes both live
//! subscriptions plus publishes and resync queries over it — see
//! [`ActorState`]'s doc comment for why. Publishes run on their own
//! `tokio::spawn`ed task per key (see [`NostrNodeRelay::spawn_publish`]) so
//! a down relay never blocks the node's local reconcile loop.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use buzz_core::assignment::decrypt_for_node;
use buzz_core::kind::{KIND_AGENT_ASSIGNMENT, KIND_AGENT_NODE_STATUS, KIND_PRESENCE_UPDATE};
use buzz_core::{AgentNodeStatus, NodeCapabilities};
use buzz_ws_client::{NostrWsConnection, RelayMessage, WsClientError};
use nostr::{Alphabet, Event, EventBuilder, Filter, Keys, Kind, PublicKey, SingleLetterTag};
use serde_json::json;
use tokio::sync::{mpsc, oneshot, Mutex, OnceCell};

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

/// Subscription id for this node's `AGENT_NODE_STATUS` live tail (Phase 5
/// batch C2). Scoped by `#d` to the agent pubkeys learned from the
/// `AGENT_ASSIGNMENT` stream (see [`ActorState::track_agent`]) rather than
/// left unscoped — `AGENT_NODE_STATUS` events are node-authored (not
/// owner-authored), so there is no `author` filter that would bound this to
/// "this owner's world" the way [`ASSIGNMENT_SUB_ID`]'s does; the `#d` scope
/// is the substitute.
const STATUS_SUB_ID: &str = "buzz-node-peer-status";

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

/// Distinguishes independent publish streams for [`PublishCoalescer`]: the
/// Nostr event kind plus its NIP-33 `d`-tag identifier, if any (`None` for
/// non-addressable kinds like presence). Two different agents'
/// `AGENT_NODE_STATUS` streams (same kind, different `d`) must stay
/// independent — coalescing across them would silently drop a distinct
/// agent's status update, not just a repeated one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PublishKey {
    kind: u16,
    identifier: Option<String>,
}

impl PublishKey {
    fn for_event(event: &Event) -> Self {
        Self {
            kind: event.kind.as_u16(),
            identifier: event.tags.identifier().map(str::to_string),
        }
    }
}

/// Coalesces repeated publishes of the same logical stream (see
/// [`PublishKey`]) into at most one in-flight background task per key.
///
/// Without this, a long relay outage during which the engine republishes an
/// agent's status on every reconcile tick (any `Observed` state but
/// `Absent` is "worth reporting" — see `crate::health::classify`) would
/// have [`NostrNodeRelay::spawn_publish`] `tokio::spawn` a brand new task
/// on every tick, each sitting parked waiting on its own
/// [`Inner::publish_with_retry`] call — the connection-owning actor
/// (`ActorState::ensure_connected`) is retrying forever, so every one of
/// these tasks queues up behind it — an unbounded number of parked tasks for
/// the duration of the outage (Phase 3 residual). With this, a publish call
/// that arrives while a task is already in flight for its key REPLACES the
/// payload that task will send next, rather than spawning a second one:
/// newest always wins, and any earlier not-yet-sent replacement is simply
/// dropped.
#[derive(Default)]
struct PublishCoalescer {
    /// A present key means a task is currently claimed for it. The value is
    /// the next payload queued for that task to send once its current send
    /// completes (`None` = nothing queued yet — the claiming send is still
    /// in progress).
    inflight: HashMap<PublishKey, Option<Event>>,
}

impl PublishCoalescer {
    /// Offer `event` (whose coalescing key is `key`) for publish. Returns
    /// `Some(event)` if no task is currently running for `key` — the caller
    /// has just claimed the slot and must spawn a task, starting with
    /// `event`. Returns `None` if a task is already in flight for `key`:
    /// `event` has been queued as the payload that task will send next
    /// (replacing any earlier not-yet-sent replacement), and the caller
    /// must NOT spawn anything.
    fn offer(&mut self, key: PublishKey, event: Event) -> Option<Event> {
        match self.inflight.entry(key) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(None);
                Some(event)
            }
            std::collections::hash_map::Entry::Occupied(mut o) => {
                o.insert(Some(event));
                None
            }
        }
    }

    /// Called once an in-flight task finishes sending `key`'s current
    /// event. Returns the next event to send — keep looping the same task —
    /// if one was queued while it worked, or `None` if the slot has been
    /// released (a future `offer` for this key will spawn a fresh task).
    fn next_or_release(&mut self, key: &PublishKey) -> Option<Event> {
        let slot = self.inflight.get_mut(key)?;
        match slot.take() {
            Some(next) => Some(next),
            None => {
                self.inflight.remove(key);
                None
            }
        }
    }
}

/// Shared connection state, wrapped in an `Arc` so the background publish
/// task spawned by [`NostrNodeRelay::spawn_publish`] can outlive the
/// `&self` call that spawned it, and so the connection-owning actor task
/// (see [`ActorState`]) can hold its own clone independent of `NostrNodeRelay`
/// itself.
struct Inner {
    node_keys: Keys,
    owner_pubkey: PublicKey,
    relay_url: String,
    /// Lazily spawns [`run_actor`] on first use (mirrors this module's
    /// existing "dialing happens lazily on first use" contract — see
    /// [`NostrNodeRelay`]'s doc comment). Exactly one actor per `Inner`,
    /// which is exactly one connection per `NostrNodeRelay`: every
    /// trait-method call reaches the SAME actor through this cell.
    actor: OnceCell<ActorHandle>,
    /// Backing flag for [`NostrNodeRelay::take_reconnected`]. Pre-seeded
    /// `true` at construction so the engine's very first check (before any
    /// connection exists) drives the startup resync;
    /// [`ActorState::ensure_connected`] re-arms it on every later reconnect —
    /// but deliberately NOT on the very first successful connect (see
    /// `has_connected_once`), since that first connect happens *inside*
    /// the startup resync's own `query_desired` call: re-arming there too
    /// would make the engine immediately run a second, redundant resync
    /// right after the first.
    reconnected: std::sync::atomic::AtomicBool,
    /// Set on the first successful connect; distinguishes "first ever
    /// connect" (already covered by `reconnected`'s pre-seed) from a real
    /// reconnect for `ensure_connected`'s `reconnected`-arming logic.
    has_connected_once: std::sync::atomic::AtomicBool,
    /// Coalescing state for [`NostrNodeRelay::spawn_publish`] — see
    /// [`PublishCoalescer`].
    coalescer: std::sync::Mutex<PublishCoalescer>,
}

impl Inner {
    /// Lock `self.coalescer`, recovering from a poisoned mutex rather than
    /// panicking (mirrors `NostrNodeRelay::lock_desired`'s precedent):
    /// another call already panicked while holding it, which must not
    /// additionally crash *this* caller.
    fn lock_coalescer(&self) -> std::sync::MutexGuard<'_, PublishCoalescer> {
        self.coalescer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Get (lazily spawning [`run_actor`] on the first call) the handle to
    /// this relay's connection-owning background actor. Every later call —
    /// from any of `next_desired`/`next_status`/`query_desired`/`publish_*`,
    /// on any clone of this `Arc` — reaches the SAME actor and therefore the
    /// SAME one underlying WebSocket connection (Phase 5 batch C2: the
    /// `AGENT_ASSIGNMENT` and `AGENT_NODE_STATUS` live tails are multiplexed
    /// over it, demuxed by [`ActorState::route_or_collect`]).
    ///
    /// `self: &Arc<Self>` (mirrors `buzz_workflow`/`buzz_pubsub`'s precedent
    /// for actor-owning types in this workspace) so the spawned task can
    /// hold its own `Arc::clone` independent of whichever caller happened to
    /// trigger the lazy spawn.
    async fn actor_handle(self: &Arc<Self>) -> &ActorHandle {
        self.actor
            .get_or_init(|| async { ActorHandle::spawn(Arc::clone(self)) })
            .await
    }

    /// Publish `event`, retrying transport failures forever (never gives up
    /// — a down relay is a condition this daemon must ride out, not a fatal
    /// error; see the actor's `ensure_connected` for the same policy applied
    /// to connecting). An explicit relay rejection (`OK false`) is NOT
    /// retried — that is a policy/validation outcome retrying can't fix.
    ///
    /// Talks to the connection-owning actor via [`ActorCommand::Publish`]
    /// rather than touching any connection directly — `Inner` no longer
    /// holds one. Always runs inside the background task spawned by
    /// [`NostrNodeRelay::spawn_publish`], never on a caller's task (except
    /// [`NostrNodeRelay::publish_presence_awaited`], which awaits it
    /// directly by design).
    async fn publish_with_retry(self: &Arc<Self>, event: Event) -> Result<(), NodeError> {
        loop {
            let handle = self.actor_handle().await;
            let (respond_to, rx) = oneshot::channel();
            if handle
                .cmd_tx
                .send(ActorCommand::Publish {
                    event: Box::new(event.clone()),
                    respond_to,
                })
                .is_err()
            {
                return Err(NodeError::Relay("relay actor task is gone".into()));
            }
            match rx.await {
                Ok(PublishAttempt::Accepted) => return Ok(()),
                Ok(PublishAttempt::Rejected(message)) => {
                    return Err(NodeError::Relay(format!(
                        "event rejected by relay: {message}"
                    )))
                }
                // The actor already resets its connection on a transport
                // failure and will reconnect on its next loop iteration
                // (via `ensure_connected`) before picking up another
                // command — resending here just queues behind that.
                Ok(PublishAttempt::Transport) => continue,
                Err(_) => {
                    return Err(NodeError::Relay(
                        "relay actor task dropped the publish response".into(),
                    ))
                }
            }
        }
    }
}

/// Commands the rest of [`NostrNodeRelay`] sends to the connection-owning
/// actor (see [`run_actor`]) — everything that needs exclusive access to the
/// single [`Conn`] goes through this channel instead of touching a
/// connection directly.
enum ActorCommand {
    /// Publish one pre-built, signed event. `event` is boxed: `nostr::Event`
    /// is large enough (id/pubkey/sig are all fixed-size hashes/keys/sigs)
    /// that an unboxed copy would dominate this enum's size next to
    /// `Resync`'s tiny payload — mirrors [`buzz_ws_client::RelayMessage`]'s
    /// own boxing of `Event` for the same reason.
    Publish {
        /// The event to publish.
        event: Box<Event>,
        /// How the attempt went — see [`PublishAttempt`].
        respond_to: oneshot::Sender<PublishAttempt>,
    },
    /// Fetch the full `AGENT_ASSIGNMENT` backlog for `query_desired`'s
    /// startup/reconnect resync (spec §13 offline catch-up).
    Resync {
        /// The collected backlog, or the transport error that cut it short.
        respond_to: oneshot::Sender<Result<Vec<Event>, NodeError>>,
    },
}

/// Outcome of one publish attempt inside the actor (see
/// [`ActorState::handle_publish`]), distinguishing a transport failure the
/// caller should retry (mirrors `ensure_connected`'s "never gives up"
/// policy) from a final, non-retryable answer.
enum PublishAttempt {
    /// The relay accepted the event.
    Accepted,
    /// The relay explicitly rejected the event (its `OK false` message).
    Rejected(String),
    /// Sending failed at the transport level; the actor has already reset
    /// its connection so the next loop iteration reconnects.
    Transport,
}

/// Where one incoming relay message went, decided by its subscription id
/// (see [`ActorState::route_or_collect`]).
enum RouteOutcome {
    /// Forwarded to a live channel, or simply irrelevant (an EOSE/OK/NOTICE,
    /// or an `EVENT` for a subscription this actor doesn't recognize) —
    /// nothing further for the caller to do.
    Other,
    /// A [`RESYNC_SUB_ID`] event, for a caller running [`ActorState::run_resync`]
    /// to collect. Never produced outside that call — the shared demux is
    /// used by both, but only `run_resync` looks for this variant. Boxed for
    /// the same reason as [`ActorCommand::Publish`]'s `event` field — `Event`
    /// is large enough to otherwise dominate this enum's size next to
    /// `Other`/`ResyncEose`'s zero-sized variants.
    ResyncEvent(Box<Event>),
    /// `RESYNC_SUB_ID` reached end-of-stored-events.
    ResyncEose,
}

/// Cloneable/shareable handle to a running [`run_actor`] task: the command
/// channel plus the two live-tail receivers `next_desired`/`next_status`
/// drain. Never itself cloned today (owned once by `Inner::actor`), but see
/// [`FakeRelayHandle`] for the shape this mirrors.
struct ActorHandle {
    cmd_tx: mpsc::UnboundedSender<ActorCommand>,
    /// Raw (not yet [`apply_assignment_event`]-processed) `AGENT_ASSIGNMENT`
    /// events, drained by [`NostrNodeRelay::next_desired`]. `Mutex`-wrapped
    /// so `&self` methods on `NostrNodeRelay` can drain it (mirrors
    /// `FakeRelay`'s `status_rx`/`desired_rx` precedent in `relay.rs`).
    assignment_rx: Mutex<mpsc::UnboundedReceiver<Event>>,
    /// Raw (not yet [`buzz_core::node_status::validate_status`]-checked)
    /// `AGENT_NODE_STATUS` events, drained and authenticated by
    /// [`NostrNodeRelay::next_status`].
    status_rx: Mutex<mpsc::UnboundedReceiver<Event>>,
}

impl ActorHandle {
    /// Spawn [`run_actor`] and return a handle to it. Synchronous — the
    /// spawn itself is instant; the actor's own first `ensure_connected`
    /// runs on the new task, not this call, preserving this module's
    /// "dialing happens lazily on first use, not before" contract even
    /// though the actor task itself now exists eagerly once first touched.
    fn spawn(inner: Arc<Inner>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (assignment_tx, assignment_rx) = mpsc::unbounded_channel();
        let (status_tx, status_rx) = mpsc::unbounded_channel();
        let state = ActorState {
            inner,
            conn: None,
            known_agents: BTreeSet::new(),
            assignment_tx,
            status_tx,
        };
        tokio::spawn(run_actor(state, cmd_rx));
        Self {
            cmd_tx,
            assignment_rx: Mutex::new(assignment_rx),
            status_rx: Mutex::new(status_rx),
        }
    }
}

/// The connection-owning actor's private, single-owner state — never shared
/// or wrapped in a lock: [`run_actor`] is the only task that ever touches
/// it, which is exactly what lets `next_desired` and `next_status` stay
/// concurrently pollable (Phase 5 batch C2's crux) without either blocking
/// the other or a publish on a shared connection mutex.
struct ActorState {
    inner: Arc<Inner>,
    conn: Option<Conn>,
    /// Agent pubkeys learned from the `AGENT_ASSIGNMENT` stream (any
    /// envelope for this owner, regardless of which node it targets — see
    /// [`Self::track_agent`]'s doc comment for why). Scopes
    /// [`STATUS_SUB_ID`]'s `#d` filter and persists across reconnects
    /// (only `conn` resets).
    known_agents: BTreeSet<PublicKey>,
    assignment_tx: mpsc::UnboundedSender<Event>,
    status_tx: mpsc::UnboundedSender<Event>,
}

impl ActorState {
    /// Ensure `self.conn` holds a live, authenticated connection with both
    /// live subscriptions (re)established, (re)connecting with the
    /// [`backoff_delay`] ladder until it succeeds. Never gives up: a down
    /// relay is a condition this daemon must ride out, not a fatal error —
    /// there is no shutdown signal wired through the [`NodeRelay`] trait.
    /// Called at the top of every [`run_actor`] loop iteration; a no-op
    /// once connected.
    async fn ensure_connected(&mut self) {
        if self.conn.is_some() {
            return;
        }
        let mut attempt = 0usize;
        loop {
            match NostrWsConnection::connect_authenticated(
                &self.inner.relay_url,
                &self.inner.node_keys,
                None,
            )
            .await
            {
                Ok(mut ws) => {
                    let filter = Filter::new()
                        .kind(Kind::Custom(KIND_AGENT_ASSIGNMENT as u16))
                        .author(self.inner.owner_pubkey);
                    match ws
                        .send_raw(&json!(["REQ", ASSIGNMENT_SUB_ID, filter]))
                        .await
                    {
                        Ok(()) => {
                            self.conn = Some(Conn { ws });
                            // Re-establish the status tail too, scoped to
                            // whatever agents were already known before this
                            // (re)connect — a no-op while that set is still
                            // empty. Together with the REQ above this
                            // satisfies "reconnect re-establishes both
                            // subscriptions" (Phase 5 batch C2).
                            self.send_status_req().await;
                            // Only a genuine RECONNECT re-arms `reconnected`
                            // — the very first connect is already covered
                            // by its constructor pre-seed, and this first
                            // connect happens inside that very resync's own
                            // query, so re-arming here too would trigger an
                            // immediate, redundant second resync.
                            if self
                                .inner
                                .has_connected_once
                                .swap(true, std::sync::atomic::Ordering::SeqCst)
                            {
                                self.inner
                                    .reconnected
                                    .store(true, std::sync::atomic::Ordering::SeqCst);
                            }
                            return;
                        }
                        Err(e) => tracing::warn!(
                            error = %e,
                            relay_url = %self.inner.relay_url,
                            "failed to subscribe after connect; retrying"
                        ),
                    }
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    attempt,
                    relay_url = %self.inner.relay_url,
                    "node relay connect failed; retrying"
                ),
            }
            tokio::time::sleep(backoff_delay(attempt)).await;
            attempt += 1;
        }
    }

    /// (Re)send the `AGENT_NODE_STATUS` REQ scoped by `#d` to
    /// `self.known_agents`'s current set. A no-op while that set is empty —
    /// an empty `#d` filter matches nothing (see
    /// `buzz_core::filter::filter_match_one`), so subscribing before there
    /// is anything to scope to would just be a wasted round trip, not an
    /// over-subscription hazard either way.
    async fn send_status_req(&mut self) {
        if self.known_agents.is_empty() {
            return;
        }
        let Some(conn) = self.conn.as_mut() else {
            return;
        };
        let values: Vec<String> = self.known_agents.iter().map(PublicKey::to_hex).collect();
        let filter = Filter::new()
            .kind(Kind::Custom(KIND_AGENT_NODE_STATUS as u16))
            .custom_tags(SingleLetterTag::lowercase(Alphabet::D), values);
        if let Err(e) = conn
            .ws
            .send_raw(&json!(["REQ", STATUS_SUB_ID, filter]))
            .await
        {
            tracing::warn!(
                error = %e,
                "failed to (re)subscribe AGENT_NODE_STATUS; reconnecting"
            );
            self.conn = None;
        }
    }

    /// Learn `event`'s agent pubkey into `self.known_agents` when `event` is
    /// a validly owner-signed `AGENT_ASSIGNMENT` envelope, re-issuing the
    /// `AGENT_NODE_STATUS` subscription the first time a given agent is
    /// seen. Ignores anything that fails [`buzz_core::assignment::validate_envelope`]
    /// (forged, malformed, or wrong-kind) — growing the status scope from an
    /// unauthenticated claim would let a malicious relay steer which `#d`
    /// values this node asks about.
    ///
    /// Deliberately keyed on EVERY envelope for this owner, not just ones
    /// that target this node: `next_desired`'s `ASSIGNMENT_SUB_ID` stream
    /// already receives every one of the owner's assignment records
    /// (`Self::ensure_connected`'s REQ has no target-node filter — see
    /// `apply_assignment_event`'s doc comment), so an agent's *first*
    /// assignment (to some other node) is what proactively grows this
    /// node's status scope. That matters because a *later* move of that
    /// same agent to this node needs its peer's prior status already on
    /// hand at the moment the move's assignment arrives — `engine::run`
    /// reconciles synchronously right after `next_desired` yields, with no
    /// time left for a just-in-time subscription update to pay off. Scoping
    /// only to agents already assigned to this node would miss exactly the
    /// first-time-onto-this-node case this batch exists to make fast.
    async fn track_agent(&mut self, event: &Event) {
        if let Ok(envelope) =
            buzz_core::assignment::validate_envelope(event, &self.inner.owner_pubkey)
        {
            if self.known_agents.insert(envelope.agent_pubkey) {
                self.send_status_req().await;
            }
        }
    }

    /// Route one incoming relay message by subscription id — the demux at
    /// the heart of multiplexing both live tails over the one connection.
    /// Forwards `ASSIGNMENT_SUB_ID`/`STATUS_SUB_ID` events to their channel
    /// (learning a new agent along the way for the former); a
    /// `RESYNC_SUB_ID` event or its EOSE is returned to the caller instead
    /// of being handled here, since it's only meaningful to an in-progress
    /// [`Self::run_resync`] call — this same method is shared by the main
    /// loop (which has no such call in flight and simply lets those
    /// outcomes fall on the floor) and `run_resync` (which collects them),
    /// so a live-tail event racing a resync is still delivered, never
    /// dropped.
    async fn route_or_collect(&mut self, msg: RelayMessage) -> RouteOutcome {
        match msg {
            RelayMessage::Event {
                subscription_id,
                event,
            } if subscription_id == ASSIGNMENT_SUB_ID => {
                self.track_agent(&event).await;
                let _ = self.assignment_tx.send(*event);
                RouteOutcome::Other
            }
            RelayMessage::Event {
                subscription_id,
                event,
            } if subscription_id == STATUS_SUB_ID => {
                let _ = self.status_tx.send(*event);
                RouteOutcome::Other
            }
            RelayMessage::Event {
                subscription_id,
                event,
            } if subscription_id == RESYNC_SUB_ID => RouteOutcome::ResyncEvent(event),
            RelayMessage::Eose { subscription_id } if subscription_id == RESYNC_SUB_ID => {
                RouteOutcome::ResyncEose
            }
            // EOSE/OK/NOTICE/AUTH/CLOSED, or an EVENT for a subscription
            // this actor doesn't recognize — no destination.
            _ => RouteOutcome::Other,
        }
    }

    /// Handle one already-established connection's next relay message
    /// (main-loop branch): route it, or reconnect on a transport error.
    /// [`WsClientError::Timeout`] is not an error here — it just means no
    /// news within [`READ_POLL_TIMEOUT`]; the next loop iteration reads
    /// again (`ensure_connected` re-checks first, which is a no-op while
    /// still connected).
    async fn handle_incoming(&mut self, msg: Result<RelayMessage, WsClientError>) {
        match msg {
            Ok(relay_msg) => {
                let _ = self.route_or_collect(relay_msg).await;
            }
            Err(WsClientError::Timeout) => {}
            Err(e) => {
                tracing::warn!(error = %e, "relay read failed; reconnecting");
                self.conn = None;
            }
        }
    }

    /// Read the next relay message off `self.conn`, or pend forever if
    /// somehow disconnected (defensive only: [`run_actor`] always calls
    /// [`Self::ensure_connected`] immediately before this, so `self.conn`
    /// is `Some` here in practice — pending rather than panicking keeps
    /// this method infallible without a spurious busy-loop if that
    /// invariant is ever violated).
    async fn next_relay_message(&mut self) -> Result<RelayMessage, WsClientError> {
        match self.conn.as_mut() {
            Some(conn) => conn.ws.next_event(READ_POLL_TIMEOUT).await,
            None => std::future::pending().await,
        }
    }

    /// Handle [`ActorCommand::Publish`]: one send attempt against the
    /// current connection. A transport failure resets `self.conn` (so the
    /// next loop iteration reconnects) and reports [`PublishAttempt::Transport`]
    /// — [`Inner::publish_with_retry`] is what actually retries, by sending
    /// a fresh command; this method never loops or sleeps itself, so it can
    /// never starve the rest of [`run_actor`]'s select loop for longer than
    /// one attempt.
    async fn handle_publish(
        &mut self,
        event: Box<Event>,
        respond_to: oneshot::Sender<PublishAttempt>,
    ) {
        let Some(conn) = self.conn.as_mut() else {
            // `ensure_connected` just ran; this shouldn't happen. Treat it
            // like a transport failure rather than panicking.
            let _ = respond_to.send(PublishAttempt::Transport);
            return;
        };
        let outcome = match conn.ws.send_event(*event).await {
            Ok(ok) if ok.accepted => PublishAttempt::Accepted,
            Ok(ok) => PublishAttempt::Rejected(ok.message),
            Err(e) => {
                tracing::warn!(error = %e, "publish failed; reconnecting");
                self.conn = None;
                PublishAttempt::Transport
            }
        };
        let _ = respond_to.send(outcome);
    }

    /// Handle [`ActorCommand::Resync`]: fetch the full `AGENT_ASSIGNMENT`
    /// backlog under [`RESYNC_SUB_ID`], bounded by [`RESYNC_TIMEOUT`].
    /// Delegates to [`Self::run_resync`] and forwards its result — split out
    /// so `?` can be used there instead of threading the response sender
    /// through every early return.
    async fn handle_resync(&mut self, respond_to: oneshot::Sender<Result<Vec<Event>, NodeError>>) {
        let result = self.run_resync().await;
        let _ = respond_to.send(result);
    }

    /// The actual one-shot backlog fetch: subscribe under [`RESYNC_SUB_ID`]
    /// to the same owner-scoped `AGENT_ASSIGNMENT` filter the live tail
    /// uses, collect every backlog event up to EOSE (or [`RESYNC_TIMEOUT`],
    /// whichever comes first), then close the subscription. A live-tail
    /// `ASSIGNMENT_SUB_ID`/`STATUS_SUB_ID` event that arrives while this
    /// runs is still routed normally (via [`Self::route_or_collect`]), not
    /// dropped, and does not count towards this call's own collection.
    async fn run_resync(&mut self) -> Result<Vec<Event>, NodeError> {
        let Some(conn) = self.conn.as_mut() else {
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

        let deadline = std::time::Instant::now() + RESYNC_TIMEOUT;
        let mut events = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!("resync query timed out waiting for EOSE; using partial results");
                break;
            }
            let read_result = {
                let Some(conn) = self.conn.as_mut() else {
                    return Err(NodeError::Relay("connection lost during resync".into()));
                };
                conn.ws.next_event(remaining).await
            };
            match read_result {
                Ok(msg) => match self.route_or_collect(msg).await {
                    RouteOutcome::ResyncEvent(event) => events.push(*event),
                    RouteOutcome::ResyncEose => break,
                    RouteOutcome::Other => {}
                },
                Err(WsClientError::Timeout) => {
                    // Loop; the outer `deadline` (not this per-read timeout)
                    // governs how long the whole query may run.
                }
                Err(e) => {
                    tracing::warn!(error = %e, "resync query read failed");
                    self.conn = None;
                    return Err(NodeError::Relay(format!("resync query: {e}")));
                }
            }
        }
        if let Some(conn) = self.conn.as_mut() {
            let _ = conn.ws.send_raw(&json!(["CLOSE", RESYNC_SUB_ID])).await;
        }
        Ok(events)
    }
}

/// The connection-owning actor's main loop: one task, spawned once per
/// [`NostrNodeRelay`] (via [`ActorHandle::spawn`]), that holds the sole
/// `NostrWsConnection` and multiplexes everything over it — the
/// `AGENT_ASSIGNMENT` and `AGENT_NODE_STATUS` live tails (demuxed by
/// [`ActorState::route_or_collect`] into `next_desired`/`next_status`'s
/// channels), one-shot resync queries, and publishes — via `cmd_rx` and its
/// own read loop. `ensure_connected` runs at the top of every iteration
/// (a no-op once connected), so a command handler never needs its own
/// nested reconnect loop: whatever it leaves in `state.conn` (including
/// `None` after a transport failure) is reconciled before the next command
/// or read is attempted.
async fn run_actor(mut state: ActorState, mut cmd_rx: mpsc::UnboundedReceiver<ActorCommand>) {
    loop {
        state.ensure_connected().await;
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                None => return, // every `ActorHandle`/`Inner` clone dropped; nothing left to serve
                Some(ActorCommand::Publish { event, respond_to }) => {
                    state.handle_publish(event, respond_to).await;
                }
                Some(ActorCommand::Resync { respond_to }) => {
                    state.handle_resync(respond_to).await;
                }
            },
            msg = state.next_relay_message() => {
                state.handle_incoming(msg).await;
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
/// resync — from clobbering already-applied newer state. `Clone` so
/// [`NostrNodeRelay::query_desired`] can seed a resync from the current
/// state instead of rebuilding from scratch — see its doc comment.
#[derive(Default, Clone)]
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
                actor: OnceCell::new(),
                reconnected: std::sync::atomic::AtomicBool::new(true),
                has_connected_once: std::sync::atomic::AtomicBool::new(false),
                coalescer: std::sync::Mutex::new(PublishCoalescer::default()),
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
    ///
    /// Coalesced through [`PublishCoalescer`] (Phase 5 cleanup): if a task
    /// is already in flight for `event`'s key (e.g. still retrying against
    /// a down relay), `event` just replaces that task's next payload
    /// instead of spawning a second concurrent one — see
    /// [`PublishCoalescer`]'s doc comment for why an uncoalesced version of
    /// this would accumulate an unbounded number of parked tasks over a
    /// long outage.
    fn spawn_publish(&self, event: Event) {
        let key = PublishKey::for_event(&event);
        let Some(mut current) = self.inner.lock_coalescer().offer(key.clone(), event) else {
            // A task is already in flight for this key; `event` was queued
            // as its next payload above -- nothing more to do here.
            return;
        };
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            loop {
                if let Err(e) = inner.publish_with_retry(current).await {
                    tracing::warn!(error = %e, "background publish failed");
                }
                match inner.lock_coalescer().next_or_release(&key) {
                    Some(next) => current = next,
                    None => break,
                }
            }
        });
    }

    /// Test-only: number of distinct publish keys currently claimed
    /// in-flight by the coalescer. Used to prove the coalescing bound
    /// end-to-end, through the real `spawn_publish`/`tokio::spawn`/
    /// `publish_with_retry` wiring — not just the pure `PublishCoalescer`
    /// unit (see the `PublishCoalescer` test section below).
    #[cfg(test)]
    fn inflight_publish_count(&self) -> usize {
        self.inner.lock_coalescer().inflight.len()
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

    /// One-shot query: ask the connection-owning actor to fetch the full
    /// owner-scoped `AGENT_ASSIGNMENT` backlog (see
    /// [`ActorState::run_resync`] for the actual subscribe/collect/close
    /// sequence). Used by [`NodeRelay::query_desired`] to rebuild
    /// desired-state from scratch on startup and after a reconnect.
    async fn fetch_assignment_backlog(&self) -> Result<Vec<Event>, NodeError> {
        let handle = self.inner.actor_handle().await;
        let (respond_to, rx) = oneshot::channel();
        handle
            .cmd_tx
            .send(ActorCommand::Resync { respond_to })
            .map_err(|_| NodeError::Relay("relay actor task is gone".into()))?;
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(NodeError::Relay(
                "relay actor task dropped the resync response".into(),
            )),
        }
    }
}

/// Drain `rx` until a validly-authenticated `AGENT_NODE_STATUS` event
/// arrives, dropping (and logging) anything that fails
/// [`buzz_core::node_status::validate_status`] along the way — wrong kind,
/// bad signature, or a malformed payload. This is the authenticate-before-
/// yield contract [`NodeRelay::next_status`]'s doc comment requires:
/// [`crate::move_gate::PeerStatusView::record`] only hex-parses its already-
/// `AgentNodeStatus` input, trusting the relay layer (this function) to have
/// verified the event first.
///
/// A free function taking the receiver directly (rather than a method on
/// `NostrNodeRelay`) so it's unit-testable against a hand-fed channel,
/// without spinning up the real connection-owning actor.
async fn next_valid_status(rx: &mut mpsc::UnboundedReceiver<Event>) -> Option<AgentNodeStatus> {
    loop {
        let event = rx.recv().await?;
        match buzz_core::node_status::validate_status(&event) {
            Ok(status) => return Some(status),
            Err(e) => {
                tracing::warn!(error = %e, "dropped an invalid AGENT_NODE_STATUS event");
            }
        }
    }
}

#[async_trait]
impl NodeRelay for NostrNodeRelay {
    async fn next_desired(&self) -> Option<Vec<DesiredAgent>> {
        let handle = self.inner.actor_handle().await;
        loop {
            // The `assignment_rx` lock is released the moment `recv()`
            // resolves (it's a temporary, not bound to a name) — well
            // before `self.lock_desired()` below, so the two never overlap.
            let event = handle.assignment_rx.lock().await.recv().await?;
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
            // Stale, malformed, or targets neither us nor an agent we
            // previously held — no desired-state change to surface; keep
            // waiting.
        }
    }

    async fn query_desired(&self) -> Result<Vec<DesiredAgent>, NodeError> {
        let events = self.fetch_assignment_backlog().await?;
        // Seed from a CLONE of the current live-tail state, not
        // `DesiredState::default()`: an empty base has no `seen_created_at`
        // watermarks to compare backlog events against, so a backlog
        // snapshot that happens to lag the live tail's more current view
        // (e.g. relay read-after-write lag, or a publish landing just after
        // this resync's query started) could apply a stale event over
        // already-applied newer state — a real LWW regression — and/or
        // silently drop an agent this snapshot omits but the live tail
        // already knows is still assigned (which `reconcile` would then
        // wrongly read as "no longer assigned" and `Stop`). Replaying the
        // backlog on top via the same `apply_assignment_event` LWW logic
        // still lets any genuinely newer backlog event win normally; it
        // only stops a resync from being able to regress or drop what the
        // live tail already correctly applied.
        let mut fresh = self.lock_desired().clone();
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
        // Phase 5 batch C2: real `AGENT_NODE_STATUS` subscription, demuxed
        // over the same connection `next_desired` uses (see `ActorState`).
        // `next_valid_status` is the authenticate-before-yield contract
        // `NodeRelay::next_status`'s doc comment requires — split out as a
        // free function so it's unit-testable against a hand-fed channel,
        // without spinning up the real connection-owning actor.
        let handle = self.inner.actor_handle().await;
        // Held for the whole call (unlike `next_desired`'s per-`.recv()`
        // scoping): `next_status` never needs a second lock inside the
        // loop, and `engine::run`'s `select!` is this trait's only caller —
        // never two concurrent polls of the same `NostrNodeRelay` — so
        // there is no one else to block.
        let mut rx = handle.status_rx.lock().await;
        next_valid_status(&mut rx).await
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

    #[test]
    fn tampered_reassignment_from_a_non_owner_does_not_remove_the_existing_entry() {
        // The riskiest spot in this batch: a forged "reassigned away" event
        // must never be able to evict a legitimate assignment.
        // `validate_envelope`'s author check should reject it outright, so
        // `apply_assignment_event` never even reaches the retarget-removal
        // branch. This locks that property in rather than just trusting it.
        let (owner, attacker, agent, node_m, node_n) = (
            Keys::generate(),
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

        // Forged: signed by `attacker`, not `owner`, "reassigning" A to N.
        let forged = make_assignment_at(&attacker, &agent, &node_n, AssignState::Assigned, 2_000);
        assert!(
            !apply_assignment_event(&mut state, &forged, &node_m, &owner.public_key()),
            "a non-owner-signed event must not be applied at all"
        );
        assert!(
            state.desired.contains_key(&agent.public_key()),
            "the legitimate assignment must survive a forged reassignment attempt"
        );
    }

    // --- query_desired's resync seed (Batch A deferred #5: watermark
    // carry-forward) ---
    //
    // `query_desired` itself needs a live relay to exercise end to end (see
    // the "NostrNodeRelay (live I/O)" section below), so these characterize
    // its fix at the level that's actually pure and unit-testable: how
    // `apply_assignment_event` behaves depending on what `DesiredState` a
    // resync is seeded from. `query_desired`'s only change is exactly this
    // seed (`self.lock_desired().clone()` instead of `DesiredState::default()`).

    #[test]
    fn resync_seeded_from_prior_state_does_not_regress_past_a_newer_live_tail_watermark() {
        // Simulate the live tail having already applied a NEWER assignment
        // than anything the upcoming "backlog" (applied directly here) will
        // return -- e.g. a backlog snapshot that lags the live tail's more
        // current view.
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let mut live_tail_state = DesiredState::default();
        let newer = make_assignment_at(&owner, &agent, &node, AssignState::Assigned, 5_000);
        assert!(apply_assignment_event(
            &mut live_tail_state,
            &newer,
            &node,
            &owner.public_key()
        ));

        // The fix: seed the fresh resync state from a CLONE of the current
        // live-tail state, not `DesiredState::default()`, before replaying
        // the backlog on top.
        let mut fresh = live_tail_state.clone();
        // The "backlog" returns only a STALE, older event for the same
        // agent (the regression scenario).
        let stale = make_assignment_at(&owner, &agent, &node, AssignState::Unassigned, 1_000);
        assert!(
            !apply_assignment_event(&mut fresh, &stale, &node, &owner.public_key()),
            "a stale backlog event must not apply over the seeded newer watermark"
        );
        assert_eq!(
            fresh.desired.get(&agent.public_key()).map(|d| d.state),
            Some(AssignState::Assigned),
            "the resync must not regress past state the live tail already applied"
        );
    }

    #[test]
    fn resync_seeded_from_default_would_have_regressed_without_the_fix() {
        // Characterizes the bug this fixes: seeding from
        // `DesiredState::default()` (the old behavior) lets a stale backlog
        // event apply unopposed, because there is no watermark yet to
        // compare it against.
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let mut fresh = DesiredState::default(); // old behavior: empty base
        let stale = make_assignment_at(&owner, &agent, &node, AssignState::Unassigned, 1_000);
        assert!(apply_assignment_event(
            &mut fresh,
            &stale,
            &node,
            &owner.public_key()
        ));
        assert_eq!(
            fresh.desired.get(&agent.public_key()).map(|d| d.state),
            Some(AssignState::Unassigned),
            "demonstrates the pre-fix hazard: an empty-seeded resync has no \
             watermark to reject a stale event with"
        );
    }

    #[test]
    fn resync_seeded_from_prior_state_preserves_an_agent_the_backlog_snapshot_omits() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let mut live_tail_state = DesiredState::default();
        let assigned = make_assignment_at(&owner, &agent, &node, AssignState::Assigned, 5_000);
        assert!(apply_assignment_event(
            &mut live_tail_state,
            &assigned,
            &node,
            &owner.public_key()
        ));

        // The "backlog" this resync fetches happens to return NOTHING at
        // all for this agent (e.g. a momentarily lagging query) -- seeding
        // from the prior state means it survives instead of silently
        // vanishing from the rebuilt desired set (which `reconcile` would
        // otherwise read as "no longer assigned" and wrongly `Stop`).
        let fresh = live_tail_state.clone(); // no backlog events applied on top
        assert_eq!(
            fresh.desired.get(&agent.public_key()).map(|d| d.state),
            Some(AssignState::Assigned)
        );
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

    // --- PublishCoalescer / PublishKey (pure — no relay required) ---

    use super::{PublishCoalescer, PublishKey};

    /// A signed `AGENT_NODE_STATUS` event for `agent`, distinguishable from
    /// another call's event by `created_at` alone — enough to prove
    /// coalescing keeps only the latest without needing a real relay.
    fn status_event(node: &Keys, agent: &Keys, created_at: u64) -> nostr::Event {
        let s = buzz_core::AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            health: buzz_core::AgentHealth::Running,
            reason: None,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        buzz_core::node_status::build_status(node, &s, created_at).expect("build status event")
    }

    #[test]
    fn publish_key_distinguishes_agents_by_d_tag() {
        let node = Keys::generate();
        let (agent_a, agent_b) = (Keys::generate(), Keys::generate());
        let status_a1 = status_event(&node, &agent_a, 1);
        let status_a2 = status_event(&node, &agent_a, 2);
        let status_b = status_event(&node, &agent_b, 1);

        assert_eq!(
            PublishKey::for_event(&status_a1),
            PublishKey::for_event(&status_a2),
            "the SAME agent's status stream must coalesce across ticks regardless of content"
        );
        assert_ne!(
            PublishKey::for_event(&status_a1),
            PublishKey::for_event(&status_b),
            "DIFFERENT agents must never coalesce into each other"
        );
    }

    #[test]
    fn coalescer_bounds_pending_to_one_slot_per_key_under_repeated_offers() {
        let mut c = PublishCoalescer::default();
        let (node, agent) = (Keys::generate(), Keys::generate());
        let first = status_event(&node, &agent, 1);
        let key = PublishKey::for_event(&first);

        assert!(
            c.offer(key.clone(), first).is_some(),
            "the first offer for a key must claim (spawn)"
        );

        // Flood 100 more offers for the SAME key while a task is still
        // claimed (never drained via `next_or_release`, simulating an
        // in-flight send stuck retrying against a down relay) -- every one
        // must coalesce, never claim a second task.
        for i in 0..100u64 {
            let e = status_event(&node, &agent, i + 2);
            assert!(
                c.offer(key.clone(), e).is_none(),
                "must coalesce, not claim, while a task is already in flight for this key"
            );
        }
        assert_eq!(
            c.inflight.len(),
            1,
            "must never accumulate more than one pending slot per key, no matter how many offers arrive"
        );
    }

    #[test]
    fn coalescer_keeps_only_the_latest_queued_replacement() {
        let mut c = PublishCoalescer::default();
        let (node, agent) = (Keys::generate(), Keys::generate());
        let first = status_event(&node, &agent, 1);
        let key = PublishKey::for_event(&first);
        let second = status_event(&node, &agent, 2);
        let third = status_event(&node, &agent, 3);

        assert!(c.offer(key.clone(), first).is_some(), "claims");
        assert!(c.offer(key.clone(), second).is_none(), "coalesced");
        assert!(
            c.offer(key.clone(), third.clone()).is_none(),
            "replaces the earlier queued replacement"
        );

        assert_eq!(
            c.next_or_release(&key),
            Some(third),
            "must return the LATEST offer once drained, dropping the intermediate one"
        );
        assert_eq!(
            c.next_or_release(&key),
            None,
            "the slot must be released once nothing more is queued"
        );
        assert!(c.inflight.is_empty());
    }

    #[test]
    fn coalescer_lets_a_fresh_offer_claim_again_once_released() {
        let mut c = PublishCoalescer::default();
        let (node, agent) = (Keys::generate(), Keys::generate());
        let first = status_event(&node, &agent, 1);
        let key = PublishKey::for_event(&first);

        assert!(c.offer(key.clone(), first).is_some());
        assert_eq!(
            c.next_or_release(&key),
            None,
            "nothing was queued -- releases immediately"
        );

        let later = status_event(&node, &agent, 2);
        assert!(
            c.offer(key, later).is_some(),
            "a fresh offer after release must claim (spawn) again"
        );
    }

    #[test]
    fn coalescer_tracks_independent_keys_independently() {
        let mut c = PublishCoalescer::default();
        let node = Keys::generate();
        let (agent_a, agent_b) = (Keys::generate(), Keys::generate());
        let a = status_event(&node, &agent_a, 1);
        let b = status_event(&node, &agent_b, 1);

        assert!(c.offer(PublishKey::for_event(&a), a).is_some());
        assert!(
            c.offer(PublishKey::for_event(&b), b).is_some(),
            "a different agent's key must claim its own task, not coalesce into agent A's"
        );
        assert_eq!(c.inflight.len(), 2);
    }

    // --- NostrNodeRelay (live I/O — requires a real relay) ---

    use super::NostrNodeRelay;
    use crate::relay::NodeRelay;

    fn sample_status(node: &Keys) -> buzz_core::AgentNodeStatus {
        sample_status_for(node, &Keys::generate())
    }

    /// Like `sample_status`, but for a caller-supplied `agent` instead of a
    /// fresh random one each call — needed by the coalescing test, which
    /// must reuse the SAME agent (and therefore the same `d`-tag/coalescing
    /// key) across repeated publishes.
    fn sample_status_for(node: &Keys, agent: &Keys) -> buzz_core::AgentNodeStatus {
        buzz_core::AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: agent.public_key().to_hex(),
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

    /// The coalescing bound proven end-to-end (Phase 5 cleanup c): the pure
    /// `PublishCoalescer` unit tests above prove the data-structure
    /// invariant in isolation; this drives the REAL
    /// `spawn_publish`/`tokio::spawn`/`publish_with_retry` wiring against
    /// an actually-unreachable relay — the exact "long outage" scenario the
    /// cleanup exists for — and proves the bound holds there too.
    #[tokio::test]
    async fn spawn_publish_coalesces_rapid_repeated_publishes_against_an_unreachable_relay() {
        let node = Keys::generate();
        let owner = Keys::generate();
        let agent = Keys::generate();
        let relay = NostrNodeRelay::new(
            UNREACHABLE_RELAY_URL.into(),
            node.clone(),
            owner.public_key(),
        );

        // Simulates the engine republishing the SAME agent's status on
        // every reconcile tick throughout a long relay outage. Each call
        // enqueues-and-returns immediately (proven above); the first spawns
        // a background task that gets stuck retrying `ensure_connected`
        // against the unreachable relay, so the `inflight` slot for this
        // agent's key is never drained during this loop.
        for _ in 0..50 {
            let status = sample_status_for(&node, &agent);
            relay.publish_status(&status).await.expect("enqueue status");
        }

        assert_eq!(
            relay.inflight_publish_count(),
            1,
            "must coalesce to at most one in-flight task for this agent's status stream, \
             not spawn 50 concurrent retry loops during the outage"
        );
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

    /// Phase 3 G-B deferred: `publish_announce` was previously untested for
    /// this property, unlike its `publish_status`/`publish_presence`
    /// siblings above — all three go through the same
    /// [`NostrNodeRelay::spawn_publish`] enqueue-and-return path.
    #[tokio::test]
    async fn publish_announce_returns_promptly_when_relay_is_unreachable() {
        let node = Keys::generate();
        let owner = Keys::generate();
        let relay = NostrNodeRelay::new(
            UNREACHABLE_RELAY_URL.into(),
            node.clone(),
            owner.public_key(),
        );
        let caps = buzz_core::NodeCapabilities {
            format: buzz_core::node::FORMAT.into(),
            version: buzz_core::node::VERSION,
            node_pubkey: node.public_key().to_hex(),
            os: "test".into(),
            runtimes: vec![],
            workspace_root: "/tmp".into(),
            max_agents: None,
            name: None,
        };

        let result = tokio::time::timeout(Duration::from_secs(2), relay.publish_announce(&caps))
            .await
            .expect("publish_announce must return promptly (enqueue-and-return), not block on the relay being unreachable");
        result.expect("building/enqueueing the announce event must still succeed synchronously");
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
            name: None,
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

    // --- ActorState: demux + agent-tracking (Phase 5 batch C2, pure — no
    // relay required) ---

    use super::{ActorState, Inner, RouteOutcome, ASSIGNMENT_SUB_ID, RESYNC_SUB_ID, STATUS_SUB_ID};
    use buzz_ws_client::RelayMessage;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use tokio::sync::{mpsc, OnceCell};

    /// Build a minimal `Inner` for `ActorState` tests that never actually
    /// dial a relay (`conn` stays `None` throughout, and `relay_url` is
    /// never dialed) — only `owner_pubkey` is exercised, by `track_agent`'s
    /// `validate_envelope` call.
    fn test_inner(owner_pubkey: nostr::PublicKey) -> Arc<Inner> {
        Arc::new(Inner {
            node_keys: Keys::generate(),
            owner_pubkey,
            relay_url: "ws://127.0.0.1:1".into(),
            actor: OnceCell::new(),
            reconnected: std::sync::atomic::AtomicBool::new(true),
            has_connected_once: std::sync::atomic::AtomicBool::new(false),
            coalescer: std::sync::Mutex::new(PublishCoalescer::default()),
        })
    }

    /// Build a disconnected `ActorState` (`conn: None` — re-subscribing is
    /// therefore a safe no-op; the fake-relay-server tests below exercise
    /// the real re-subscribe-over-the-wire behavior) plus receivers for
    /// whatever it forwards to `next_desired`/`next_status`.
    fn test_actor_state(
        owner_pubkey: nostr::PublicKey,
    ) -> (
        ActorState,
        mpsc::UnboundedReceiver<nostr::Event>,
        mpsc::UnboundedReceiver<nostr::Event>,
    ) {
        let (assignment_tx, assignment_rx) = mpsc::unbounded_channel();
        let (status_tx, status_rx) = mpsc::unbounded_channel();
        let state = ActorState {
            inner: test_inner(owner_pubkey),
            conn: None,
            known_agents: BTreeSet::new(),
            assignment_tx,
            status_tx,
        };
        (state, assignment_rx, status_rx)
    }

    fn event_message(subscription_id: &str, event: nostr::Event) -> RelayMessage {
        RelayMessage::Event {
            subscription_id: subscription_id.to_string(),
            event: Box::new(event),
        }
    }

    #[tokio::test]
    async fn route_or_collect_forwards_an_assignment_event_and_learns_its_agent() {
        let (owner, node, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let (mut state, mut assignment_rx, mut status_rx) = test_actor_state(owner.public_key());
        let ev = make_assignment(&owner, &agent, &node, AssignState::Assigned);

        let outcome = state
            .route_or_collect(event_message(ASSIGNMENT_SUB_ID, ev.clone()))
            .await;

        assert!(matches!(outcome, RouteOutcome::Other));
        assert_eq!(assignment_rx.try_recv().unwrap(), ev);
        assert!(
            status_rx.try_recv().is_err(),
            "must not also land on the status channel"
        );
        assert!(state.known_agents.contains(&agent.public_key()));
    }

    #[tokio::test]
    async fn route_or_collect_forwards_a_status_event_without_learning_anything() {
        let (owner, node, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let (mut state, mut assignment_rx, mut status_rx) = test_actor_state(owner.public_key());
        let ev = status_event(&node, &agent, 1);

        let outcome = state
            .route_or_collect(event_message(STATUS_SUB_ID, ev.clone()))
            .await;

        assert!(matches!(outcome, RouteOutcome::Other));
        assert_eq!(status_rx.try_recv().unwrap(), ev);
        assert!(assignment_rx.try_recv().is_err());
        assert!(
            state.known_agents.is_empty(),
            "a status event is node-authored, not owner-signed, and must never grow the scope"
        );
    }

    #[tokio::test]
    async fn route_or_collect_collects_a_resync_event_without_forwarding_it_live() {
        let (owner, node, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let (mut state, mut assignment_rx, mut status_rx) = test_actor_state(owner.public_key());
        let ev = make_assignment(&owner, &agent, &node, AssignState::Assigned);

        let outcome = state
            .route_or_collect(event_message(RESYNC_SUB_ID, ev.clone()))
            .await;

        match outcome {
            RouteOutcome::ResyncEvent(boxed) => assert_eq!(*boxed, ev),
            _ => panic!("expected a ResyncEvent outcome"),
        }
        assert!(
            assignment_rx.try_recv().is_err(),
            "a resync-collected event must not also reach the live tail"
        );
        assert!(status_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn route_or_collect_signals_resync_eose() {
        let owner = Keys::generate();
        let (mut state, _assignment_rx, _status_rx) = test_actor_state(owner.public_key());

        let outcome = state
            .route_or_collect(RelayMessage::Eose {
                subscription_id: RESYNC_SUB_ID.to_string(),
            })
            .await;

        assert!(matches!(outcome, RouteOutcome::ResyncEose));
    }

    #[tokio::test]
    async fn route_or_collect_ignores_an_unrecognized_subscription() {
        let (owner, node, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let (mut state, mut assignment_rx, mut status_rx) = test_actor_state(owner.public_key());
        let ev = make_assignment(&owner, &agent, &node, AssignState::Assigned);

        let outcome = state
            .route_or_collect(event_message("some-other-subscription", ev))
            .await;

        assert!(matches!(outcome, RouteOutcome::Other));
        assert!(assignment_rx.try_recv().is_err());
        assert!(status_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn track_agent_ignores_a_forged_non_owner_assignment_event() {
        let (owner, attacker, node, agent) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let (mut state, ..) = test_actor_state(owner.public_key());
        // Signed by `attacker`, not `owner` -- `validate_envelope` must
        // reject it, so it must never be allowed to steer which `#d`
        // values this node subscribes to.
        let forged = make_assignment(&attacker, &agent, &node, AssignState::Assigned);

        state.track_agent(&forged).await;

        assert!(
            state.known_agents.is_empty(),
            "a forged event must not grow the status scope"
        );
    }

    #[tokio::test]
    async fn track_agent_ignores_a_wrong_kind_event() {
        let owner = Keys::generate();
        let (mut state, ..) = test_actor_state(owner.public_key());
        let not_an_assignment = nostr::EventBuilder::new(nostr::Kind::TextNote, "hi")
            .sign_with_keys(&owner)
            .expect("sign");

        state.track_agent(&not_an_assignment).await;

        assert!(state.known_agents.is_empty());
    }

    #[tokio::test]
    async fn track_agent_does_not_regrow_an_already_known_agent() {
        let (owner, node, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let (mut state, ..) = test_actor_state(owner.public_key());
        let ev = make_assignment(&owner, &agent, &node, AssignState::Assigned);

        state.track_agent(&ev).await;
        state.track_agent(&ev).await;

        assert_eq!(state.known_agents.len(), 1);
    }

    // --- next_valid_status: authenticate-before-yield (Phase 5 batch C2) ---

    use super::next_valid_status;

    #[tokio::test]
    async fn next_valid_status_yields_a_well_formed_status() {
        let node = Keys::generate();
        let agent = Keys::generate();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let status = sample_status_for(&node, &agent);
        tx.send(buzz_core::node_status::build_status(&node, &status, 1_785_780_000).unwrap())
            .unwrap();

        assert_eq!(next_valid_status(&mut rx).await, Some(status));
    }

    #[tokio::test]
    async fn next_valid_status_drops_a_wrong_kind_event_then_yields_the_next_valid_one() {
        let node = Keys::generate();
        let agent = Keys::generate();
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(
            nostr::EventBuilder::new(nostr::Kind::TextNote, "not a status")
                .sign_with_keys(&node)
                .expect("sign"),
        )
        .unwrap();
        let status = sample_status_for(&node, &agent);
        tx.send(buzz_core::node_status::build_status(&node, &status, 1_785_780_000).unwrap())
            .unwrap();

        assert_eq!(
            next_valid_status(&mut rx).await,
            Some(status),
            "a wrong-kind event must be dropped, not returned or fatal"
        );
    }

    #[tokio::test]
    async fn next_valid_status_drops_a_tampered_event() {
        let node = Keys::generate();
        let agent = Keys::generate();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let status = sample_status_for(&node, &agent);
        let mut tampered =
            buzz_core::node_status::build_status(&node, &status, 1_785_780_000).unwrap();
        // Mutate content after signing: `id`/`sig` no longer match what's on
        // the wire, so `validate_status`'s `verify_id`/`verify_signature`
        // check must fail -- this is the "bad-sig" half of the
        // authenticate-before-yield contract.
        tampered.content.push_str("tampered");
        tx.send(tampered).unwrap();
        drop(tx);

        assert_eq!(
            next_valid_status(&mut rx).await,
            None,
            "a tampered event must be dropped; with nothing valid behind it the stream ends"
        );
    }

    #[tokio::test]
    async fn next_valid_status_returns_none_once_the_channel_closes() {
        let (tx, mut rx) = mpsc::unbounded_channel::<nostr::Event>();
        drop(tx);
        assert_eq!(next_valid_status(&mut rx).await, None);
    }

    // --- NostrNodeRelay against a fake in-process relay (Phase 5 batch C2:
    // real dual-subscription multiplexing + reconnect) ---
    //
    // Mirrors `buzz-acp/src/relay.rs`'s `test_ws_pair`/`next_test_frame`
    // pattern: a real `tokio_tungstenite` server socket on localhost, so the
    // REAL `NostrNodeRelay`/`ActorState` wiring (dial, NIP-42 auth, REQ,
    // reconnect) runs end to end without any live external relay. This is
    // the one property in this batch that genuinely needs wire-level
    // observation — the demux/track_agent logic above is proven without a
    // socket at all.

    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    /// A local TCP listener a test can [`Self::accept_authenticated`]
    /// against repeatedly, so one test can script a disconnect + reconnect
    /// without rebinding.
    struct FakeRelayServer {
        listener: TcpListener,
    }

    impl FakeRelayServer {
        async fn bind() -> (String, Self) {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind fake relay listener");
            let addr = listener.local_addr().expect("read fake relay address");
            (format!("ws://{addr}"), Self { listener })
        }

        /// Accept the next incoming connection and play the server side of
        /// the NIP-42 handshake (challenge → AUTH → OK), leaving the
        /// connection ready for the test to script REQ/EVENT/EOSE traffic.
        async fn accept_authenticated(&self) -> WebSocketStream<TcpStream> {
            let (stream, _) = self
                .listener
                .accept()
                .await
                .expect("accept fake relay connection");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("complete server websocket handshake");
            ws.send(Message::Text(
                serde_json::json!(["AUTH", "test-challenge"])
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send AUTH challenge");
            let auth_frame = next_frame(&mut ws).await;
            assert_eq!(auth_frame[0], "AUTH");
            let event_id = auth_frame[1]["id"]
                .as_str()
                .expect("auth event carries an id")
                .to_string();
            ws.send(Message::Text(
                serde_json::json!(["OK", event_id, true, ""])
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send AUTH ok");
            ws
        }
    }

    async fn next_frame(ws: &mut WebSocketStream<TcpStream>) -> serde_json::Value {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for a frame")
            .expect("fake relay stream ended")
            .expect("read fake relay frame");
        serde_json::from_str(msg.to_text().expect("expected a text frame"))
            .expect("parse fake relay frame as json")
    }

    async fn send_event_frame(
        ws: &mut WebSocketStream<TcpStream>,
        sub_id: &str,
        event: &nostr::Event,
    ) {
        ws.send(Message::Text(
            serde_json::json!(["EVENT", sub_id, event])
                .to_string()
                .into(),
        ))
        .await
        .expect("send EVENT frame");
    }

    /// The end-to-end proof for this batch: a real `NostrNodeRelay` learns
    /// an agent from the assignment tail, proactively (re)subscribes
    /// `AGENT_NODE_STATUS` scoped to exactly that agent, demuxes a peer
    /// status to `next_status` while `next_desired` stays independently
    /// pollable, and — after a simulated disconnect — reconnects with BOTH
    /// subscriptions re-established at the same scope.
    #[tokio::test]
    async fn reconnect_reestablishes_both_live_subscriptions_scoped_to_learned_agents() {
        let (url, server) = FakeRelayServer::bind().await;
        let owner = Keys::generate();
        let node = Keys::generate();
        let relay = NostrNodeRelay::new(url, node.clone(), owner.public_key());
        let agent = Keys::generate();
        let agent_pubkey = agent.public_key();

        let server_task = tokio::spawn(async move {
            // --- First connection: only the assignment tail starts out
            // subscribed -- nothing is known to scope status to yet.
            let mut ws = server.accept_authenticated().await;
            let first_req = next_frame(&mut ws).await;
            assert_eq!(first_req[0], "REQ");
            assert_eq!(first_req[1], ASSIGNMENT_SUB_ID);

            // The owner assigns `agent` to `node` -- the actor learns the
            // agent pubkey from this and must proactively subscribe status.
            let assignment = build_assignment(
                &owner,
                &node.public_key(),
                &secret_for(&owner, &agent, &node),
                AssignState::Assigned,
                1_785_780_000,
            )
            .expect("build assignment");
            send_event_frame(&mut ws, ASSIGNMENT_SUB_ID, &assignment).await;

            let status_req = next_frame(&mut ws).await;
            assert_eq!(status_req[0], "REQ");
            assert_eq!(status_req[1], STATUS_SUB_ID);
            assert_eq!(
                status_req[2]["#d"],
                serde_json::json!([agent.public_key().to_hex()]),
                "the status subscription must be scoped to exactly the learned agent"
            );

            // A real peer status flows through the just-opened subscription.
            let peer = Keys::generate();
            let peer_status = status_event(&peer, &agent, 1);
            send_event_frame(&mut ws, STATUS_SUB_ID, &peer_status).await;

            // --- Simulate a disconnect: drop the socket, then accept again.
            drop(ws);
            let mut ws2 = server.accept_authenticated().await;
            let mut reqs = [next_frame(&mut ws2).await, next_frame(&mut ws2).await];
            reqs.sort_by(|a, b| a[1].as_str().cmp(&b[1].as_str()));
            assert_eq!(reqs[0][1], ASSIGNMENT_SUB_ID);
            assert_eq!(reqs[1][1], STATUS_SUB_ID);
            assert_eq!(
                reqs[1][2]["#d"],
                serde_json::json!([agent.public_key().to_hex()]),
                "reconnect must re-scope status to the SAME known agent, not start over empty"
            );
        });

        let desired = tokio::time::timeout(Duration::from_secs(5), relay.next_desired())
            .await
            .expect("next_desired timed out")
            .expect("a desired-set change");
        assert_eq!(desired.len(), 1);
        assert_eq!(desired[0].agent_pubkey, agent_pubkey);

        let status = tokio::time::timeout(Duration::from_secs(5), relay.next_status())
            .await
            .expect("next_status timed out")
            .expect("a peer status");
        assert_eq!(status.agent_pubkey, agent_pubkey.to_hex());

        tokio::time::timeout(Duration::from_secs(10), server_task)
            .await
            .expect("server task timed out")
            .expect("server task panicked");
    }
}
