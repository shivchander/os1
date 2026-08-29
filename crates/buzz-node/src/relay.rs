//! The relay abstraction — desired-state in, node/agent status out — plus an
//! in-memory fake for tests.
use async_trait::async_trait;

use buzz_core::{AgentNodeStatus, NodeCapabilities};

use crate::model::{DesiredAgent, NodeError};

/// The node's only channel to the outside world. The real impl (Phase 3) is a
/// NIP-42 WebSocket client that subscribes to the owner's assignments and
/// publishes signed status/announce/presence events.
///
/// Every method takes `&self` (interior mutability in implementations), not
/// `&mut self`: [`crate::engine::run`] polls [`Self::next_desired`] and
/// [`Self::next_status`] concurrently in one `select!` — a node must react
/// to a peer's status change without blocking on its own desired-state
/// tail, and vice versa — which requires both to be callable through a
/// shared reference at once.
#[async_trait]
pub trait NodeRelay: Send + Sync {
    /// Await the next desired-state snapshot for this node. `None` = shut down.
    async fn next_desired(&self) -> Option<Vec<DesiredAgent>>;
    /// Await the next observed `AGENT_NODE_STATUS`, from any node
    /// (including this one). Feeds
    /// [`crate::move_gate::PeerStatusView`] so a spawn can defer while a
    /// different node still reports the same agent alive (spec I4).
    async fn next_status(&self) -> Option<AgentNodeStatus>;
    /// Publish an observed per-agent status.
    async fn publish_status(&self, status: &AgentNodeStatus) -> Result<(), NodeError>;
    /// Publish this node's capabilities.
    async fn publish_announce(&self, caps: &NodeCapabilities) -> Result<(), NodeError>;
    /// Publish this node's online/offline presence.
    async fn publish_presence(&self, online: bool) -> Result<(), NodeError>;
}

#[cfg(any(test, feature = "test-utils"))]
type Log<T> = std::sync::Arc<std::sync::Mutex<Vec<T>>>;
#[cfg(any(test, feature = "test-utils"))]
type Shared<T> = std::sync::Arc<std::sync::Mutex<T>>;

/// Reader/control handle to a [`FakeRelay`]'s published-event logs and
/// inbound-injection points, cloneable and usable after the relay itself is
/// moved into [`crate::engine::run`].
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone)]
pub struct FakeRelayHandle {
    /// Statuses passed to `publish_status`.
    pub statuses: Log<AgentNodeStatus>,
    /// Capabilities passed to `publish_announce`.
    pub announces: Log<NodeCapabilities>,
    /// Presence flags passed to `publish_presence`.
    pub presence: Log<bool>,
    /// Sends a peer `AGENT_NODE_STATUS` into the running engine's
    /// `next_status` stream.
    status_tx: tokio::sync::mpsc::UnboundedSender<AgentNodeStatus>,
    /// Sends a desired-set appended live, after the relay has already been
    /// moved into [`crate::engine::run`]. Deliberately a channel (not the
    /// constructor's plain `VecDeque` script): a channel send properly wakes
    /// an already-parked `next_desired` await, whereas mutating a `Mutex`
    /// the pending future never re-checks would go unnoticed until some
    /// *other* `select!` branch happened to cycle the loop.
    desired_tx: tokio::sync::mpsc::UnboundedSender<Vec<DesiredAgent>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl FakeRelayHandle {
    /// Inject a peer `AGENT_NODE_STATUS` observation into a running
    /// engine's [`NodeRelay::next_status`] stream — simulates a status
    /// event arriving from (any) node after the relay has been moved into
    /// [`crate::engine::run`].
    pub fn push_status(&self, status: AgentNodeStatus) {
        let _ = self.status_tx.send(status);
    }
    /// Append a desired-set to a running engine's `next_desired` stream —
    /// simulates a fresh assignment event arriving live, after the relay
    /// has already been moved into [`crate::engine::run`]. Consumed after
    /// the constructor's initial script is exhausted; only observed by a
    /// relay built with [`FakeRelay::new_hanging`] (a non-hanging relay's
    /// `next_desired` returns `None` the moment its script empties, without
    /// ever checking for live pushes).
    pub fn push_desired(&self, desired: Vec<DesiredAgent>) {
        let _ = self.desired_tx.send(desired);
    }
}

/// In-memory [`NodeRelay`]: yields a scripted sequence of desired-sets, then
/// `None` (or, if built via [`FakeRelay::new_hanging`], pends forever
/// instead); records everything published; accepts injected peer statuses
/// via its paired [`FakeRelayHandle`].
#[cfg(any(test, feature = "test-utils"))]
pub struct FakeRelay {
    script: Shared<std::collections::VecDeque<Vec<DesiredAgent>>>,
    status_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<AgentNodeStatus>>,
    /// Kept alive so `status_rx.recv()` only ever completes from an
    /// explicit `push_status` (via the handle's clone of this sender), never
    /// spuriously returns `None` because every `FakeRelayHandle` happened to
    /// be dropped elsewhere while the engine is still running.
    _status_tx_keepalive: tokio::sync::mpsc::UnboundedSender<AgentNodeStatus>,
    /// Live-push channel consulted once `script` is exhausted, only when
    /// `hang_when_exhausted` — see [`FakeRelayHandle::push_desired`].
    desired_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<DesiredAgent>>>,
    /// Kept alive for the same reason as `_status_tx_keepalive`.
    _desired_tx_keepalive: tokio::sync::mpsc::UnboundedSender<Vec<DesiredAgent>>,
    handle: FakeRelayHandle,
    /// When `true`, `next_desired` pends forever once `script` is exhausted
    /// instead of returning `None`. Set via [`FakeRelay::new_hanging`] for
    /// tests that need `engine::run`'s loop to keep running — driven only by
    /// its periodic tickers or injected statuses — instead of ending the
    /// moment the scripted desired-sets run out.
    hang_when_exhausted: bool,
}

#[cfg(any(test, feature = "test-utils"))]
impl FakeRelay {
    /// Build a fake relay from a script of desired-sets. Returns the relay and a
    /// reader handle to its published-event logs. `next_desired` returns
    /// `None` once `script` is exhausted, ending [`crate::engine::run`]'s loop.
    pub fn new(script: Vec<Vec<DesiredAgent>>) -> (Self, FakeRelayHandle) {
        Self::build(script, false)
    }

    /// Like [`Self::new`], but `next_desired` pends forever once `script` is
    /// exhausted instead of returning `None`. For tests that need
    /// `engine::run` to stay alive indefinitely (e.g. to observe periodic
    /// ticker behavior over `tokio::time::advance`d virtual time, or to
    /// inject peer statuses after startup); such tests must end the engine
    /// by aborting its task rather than awaiting `run`'s return.
    pub fn new_hanging(script: Vec<Vec<DesiredAgent>>) -> (Self, FakeRelayHandle) {
        Self::build(script, true)
    }

    fn build(script: Vec<Vec<DesiredAgent>>, hang_when_exhausted: bool) -> (Self, FakeRelayHandle) {
        let (status_tx, status_rx) = tokio::sync::mpsc::unbounded_channel();
        let (desired_tx, desired_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = FakeRelayHandle {
            statuses: Log::default(),
            announces: Log::default(),
            presence: Log::default(),
            status_tx: status_tx.clone(),
            desired_tx: desired_tx.clone(),
        };
        (
            Self {
                script: Shared::new(std::sync::Mutex::new(script.into())),
                status_rx: tokio::sync::Mutex::new(status_rx),
                _status_tx_keepalive: status_tx,
                desired_rx: tokio::sync::Mutex::new(desired_rx),
                _desired_tx_keepalive: desired_tx,
                handle: handle.clone(),
                hang_when_exhausted,
            },
            handle,
        )
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl NodeRelay for FakeRelay {
    async fn next_desired(&self) -> Option<Vec<DesiredAgent>> {
        let popped = self.script.lock().expect("lock").pop_front();
        match popped {
            Some(desired) => Some(desired),
            // The initial script is exhausted: for a hanging relay, fall
            // through to the live-push channel (properly wakes on a later
            // `push_desired`, unlike re-polling a plain `Mutex`-guarded
            // queue would); otherwise end the stream.
            None if self.hang_when_exhausted => self.desired_rx.lock().await.recv().await,
            None => None,
        }
    }
    async fn next_status(&self) -> Option<AgentNodeStatus> {
        self.status_rx.lock().await.recv().await
    }
    async fn publish_status(&self, status: &AgentNodeStatus) -> Result<(), NodeError> {
        self.handle
            .statuses
            .lock()
            .expect("lock")
            .push(status.clone());
        Ok(())
    }
    async fn publish_announce(&self, caps: &NodeCapabilities) -> Result<(), NodeError> {
        self.handle
            .announces
            .lock()
            .expect("lock")
            .push(caps.clone());
        Ok(())
    }
    async fn publish_presence(&self, online: bool) -> Result<(), NodeError> {
        self.handle.presence.lock().expect("lock").push(online);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fake_desired;
    use nostr::Keys;

    #[tokio::test]
    async fn fake_relay_streams_script_then_ends() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = vec![fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned)];
        let (relay, handle) = FakeRelay::new(vec![d.clone()]);
        assert_eq!(relay.next_desired().await, Some(d));
        assert_eq!(relay.next_desired().await, None);
        relay.publish_presence(true).await.unwrap();
        assert_eq!(*handle.presence.lock().unwrap(), vec![true]);
    }

    #[tokio::test]
    async fn push_status_is_observed_by_next_status() {
        let (n, a) = (Keys::generate(), Keys::generate());
        let (relay, handle) = FakeRelay::new(vec![]);
        let status = buzz_core::AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: a.public_key().to_hex(),
            node_pubkey: n.public_key().to_hex(),
            health: buzz_core::AgentHealth::Running,
            reason: None,
            updated_at: "2026-08-29T00:00:00Z".into(),
        };
        handle.push_status(status.clone());
        assert_eq!(relay.next_status().await, Some(status));
    }
}
