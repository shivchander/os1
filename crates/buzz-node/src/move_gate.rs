//! Bounded stop-before-start gating for agent moves (spec: one-live-instance
//! invariant, I4).
//!
//! When an assignment newly targets this node, a *different* node may still
//! be reporting the same agent as `starting`/`running` — the move hasn't
//! fully handed off yet. Spawning immediately would risk two live instances
//! of the same agent. [`PeerStatusView`] tracks the latest per-agent status
//! seen from any node (via `AGENT_NODE_STATUS`); [`crate::engine::run`] uses
//! it to defer a spawn while a different node still claims the agent alive,
//! firing it once that peer reports `stopped` or [`MOVE_HANDOFF_TIMEOUT`]
//! elapses — whichever comes first.
use std::collections::HashMap;
use std::time::Duration;

use buzz_core::{AgentHealth, AgentNodeStatus, CodecError};
use nostr::PublicKey;
// `tokio::time::Instant`, not `std::time::Instant`: `crate::engine::run`'s
// deadlines must participate in tokio's (pausable, in tests) clock so
// `tokio::time::advance` in paused-clock tests actually moves them.
use tokio::time::Instant;

/// Max time a receiving node waits for the previous node's `stopped` status
/// before spawning anyway (bounded overlap, never a permanent double — I4).
pub const MOVE_HANDOFF_TIMEOUT: Duration = Duration::from_secs(30);

/// Latest observed per-agent status across all nodes, keyed by agent
/// pubkey. Fed by every inbound `AGENT_NODE_STATUS`, from any node
/// (including this one).
#[derive(Default)]
pub struct PeerStatusView {
    latest: HashMap<PublicKey, (PublicKey, AgentHealth)>,
}

impl PeerStatusView {
    /// Record a validated status event's meaning. Fails only if the
    /// status's own pubkey fields are not valid hex — callers that already
    /// ran the status through [`buzz_core::node_status::validate_status`]
    /// should not normally see this.
    pub fn record(&mut self, s: &AgentNodeStatus) -> Result<(), CodecError> {
        let agent = PublicKey::from_hex(&s.agent_pubkey)
            .map_err(|_| CodecError::InvalidPayload("agent_pubkey".into()))?;
        let node = PublicKey::from_hex(&s.node_pubkey)
            .map_err(|_| CodecError::InvalidPayload("node_pubkey".into()))?;
        self.record_parts(agent, node, s.health);
        Ok(())
    }

    /// Test/seam helper: record already-parsed parts directly.
    pub fn record_parts(&mut self, agent: PublicKey, node: PublicKey, health: AgentHealth) {
        self.latest.insert(agent, (node, health));
    }

    /// True iff the latest status for `agent` is from a node other than
    /// `me` and that node still considers it alive (`Starting`/`Running`).
    /// A node's own status can never block its own spawn.
    pub fn peer_blocks_spawn(&self, agent: &PublicKey, me: &PublicKey) -> bool {
        match self.latest.get(agent) {
            Some((node, health)) => {
                node != me && matches!(health, AgentHealth::Starting | AgentHealth::Running)
            }
            None => false,
        }
    }
}

/// Agents whose deferred-spawn deadline has passed as of `now`, sorted by
/// pubkey for deterministic ordering.
pub fn due_pending(pending: &HashMap<PublicKey, Instant>, now: Instant) -> Vec<PublicKey> {
    let mut due: Vec<PublicKey> = pending
        .iter()
        .filter(|(_, deadline)| now >= **deadline)
        .map(|(agent, _)| *agent)
        .collect();
    due.sort_by_key(|k| k.to_hex());
    due
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::AgentHealth;
    use nostr::Keys;
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn peer_running_blocks_spawn_until_stopped() {
        let me = Keys::generate().public_key();
        let peer = Keys::generate().public_key();
        let agent = Keys::generate().public_key();
        let mut view = PeerStatusView::default();
        // Peer N reports the agent running:
        view.record_parts(agent, peer, AgentHealth::Running);
        assert!(view.peer_blocks_spawn(&agent, &me));
        // Peer N reports stopped -> no longer blocks:
        view.record_parts(agent, peer, AgentHealth::Stopped);
        assert!(!view.peer_blocks_spawn(&agent, &me));
    }

    #[test]
    fn own_running_status_does_not_block_self() {
        let me = Keys::generate().public_key();
        let agent = Keys::generate().public_key();
        let mut view = PeerStatusView::default();
        view.record_parts(agent, me, AgentHealth::Running); // our own status
        assert!(!view.peer_blocks_spawn(&agent, &me));
    }

    #[test]
    fn unknown_agent_never_blocks() {
        let me = Keys::generate().public_key();
        let agent = Keys::generate().public_key();
        let view = PeerStatusView::default();
        assert!(!view.peer_blocks_spawn(&agent, &me));
    }

    #[tokio::test]
    async fn deferred_spawn_fires_after_timeout_even_without_stopped() {
        let me = Keys::generate().public_key();
        let peer = Keys::generate().public_key();
        let agent = Keys::generate().public_key();
        let mut view = PeerStatusView::default();
        view.record_parts(agent, peer, AgentHealth::Running);
        let mut pending: HashMap<_, Instant> = HashMap::default();
        let now = Instant::now();
        // gate decides to defer:
        assert!(view.peer_blocks_spawn(&agent, &me));
        pending.insert(agent, now + MOVE_HANDOFF_TIMEOUT);
        // before deadline, still pending:
        assert!(due_pending(&pending, now + Duration::from_secs(5)).is_empty());
        // after deadline, it is due regardless of peer status:
        assert_eq!(
            due_pending(
                &pending,
                now + MOVE_HANDOFF_TIMEOUT + Duration::from_secs(1)
            ),
            vec![agent]
        );
    }

    #[test]
    fn record_rejects_unparseable_hex() {
        let mut view = PeerStatusView::default();
        let bad = AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: "not-hex".into(),
            node_pubkey: Keys::generate().public_key().to_hex(),
            health: AgentHealth::Running,
            reason: None,
            updated_at: "2026-08-29T00:00:00Z".into(),
        };
        assert!(view.record(&bad).is_err());
    }
}
