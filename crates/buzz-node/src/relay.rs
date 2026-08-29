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
        (
            Self {
                script: script.into(),
                handle: handle.clone(),
            },
            handle,
        )
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl NodeRelay for FakeRelay {
    async fn next_desired(&mut self) -> Option<Vec<DesiredAgent>> {
        self.script.pop_front()
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
        let (mut relay, handle) = FakeRelay::new(vec![d.clone()]);
        assert_eq!(relay.next_desired().await, Some(d));
        assert_eq!(relay.next_desired().await, None);
        relay.publish_presence(true).await.unwrap();
        assert_eq!(*handle.presence.lock().unwrap(), vec![true]);
    }
}
