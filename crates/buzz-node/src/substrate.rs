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
        self.inner
            .lock()
            .expect("lock")
            .insert(desired.agent_pubkey, Observed::Running);
        Ok(())
    }
    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError> {
        self.stops.lock().expect("lock").push(*agent);
        self.inner.lock().expect("lock").insert(*agent, Observed::Stopped);
        Ok(())
    }
}

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
