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
    /// Last-seen `updated_at` per agent, guarding [`Self::record`] against
    /// an out-of-order (older) status undoing a newer one already applied —
    /// the same class of hazard Task 3 closed for assignments. Not
    /// consulted by [`Self::record_parts`], which is a test-only seam with
    /// no timestamp to compare.
    seen_at: HashMap<PublicKey, chrono::DateTime<chrono::FixedOffset>>,
}

impl PeerStatusView {
    /// Record a validated status event's meaning. Ignores (returns `Ok`
    /// without changing anything) a status older than the last one seen
    /// for that agent, per its `updated_at` — reordered delivery must never
    /// let a stale `running` undo a newer `stopped` (or vice versa). Fails
    /// only if the status's own pubkey or timestamp fields are malformed —
    /// callers that already ran the status through
    /// [`buzz_core::node_status::validate_status`] should not normally see
    /// this.
    pub fn record(&mut self, s: &AgentNodeStatus) -> Result<(), CodecError> {
        let agent = PublicKey::from_hex(&s.agent_pubkey)
            .map_err(|_| CodecError::InvalidPayload("agent_pubkey".into()))?;
        let node = PublicKey::from_hex(&s.node_pubkey)
            .map_err(|_| CodecError::InvalidPayload("node_pubkey".into()))?;
        let updated_at = chrono::DateTime::parse_from_rfc3339(&s.updated_at)
            .map_err(|_| CodecError::InvalidPayload("updated_at".into()))?;
        if self
            .seen_at
            .get(&agent)
            .is_some_and(|&prev| updated_at < prev)
        {
            return Ok(()); // stale out-of-order status; ignore
        }
        self.seen_at.insert(agent, updated_at);
        self.record_parts(agent, node, s.health);
        Ok(())
    }

    /// Test/seam helper: record already-parsed parts directly, bypassing
    /// the recency guard [`Self::record`] applies (there is no timestamp to
    /// compare here) — for tests that want to set up state directly.
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

    fn status_at(
        agent: &PublicKey,
        node: &PublicKey,
        health: AgentHealth,
        updated_at: &str,
    ) -> AgentNodeStatus {
        AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: agent.to_hex(),
            node_pubkey: node.to_hex(),
            health,
            reason: None,
            updated_at: updated_at.into(),
        }
    }

    #[test]
    fn record_rejects_unparseable_timestamp() {
        let (node, agent) = (Keys::generate().public_key(), Keys::generate().public_key());
        let mut view = PeerStatusView::default();
        let bad = status_at(&agent, &node, AgentHealth::Running, "not-a-timestamp");
        assert!(view.record(&bad).is_err());
    }

    #[test]
    fn record_applies_in_order_updates() {
        let me = Keys::generate().public_key();
        let (peer, agent) = (Keys::generate().public_key(), Keys::generate().public_key());
        let mut view = PeerStatusView::default();
        view.record(&status_at(
            &agent,
            &peer,
            AgentHealth::Running,
            "2026-08-29T00:00:00Z",
        ))
        .unwrap();
        assert!(view.peer_blocks_spawn(&agent, &me));
        view.record(&status_at(
            &agent,
            &peer,
            AgentHealth::Stopped,
            "2026-08-29T00:01:00Z",
        ))
        .unwrap();
        assert!(!view.peer_blocks_spawn(&agent, &me));
    }

    #[test]
    fn record_ignores_a_stale_out_of_order_status() {
        // A reordered older `running` arriving after a newer `stopped` must
        // not wrongly re-block a spawn -- the same class of hazard Task 3
        // closed for assignments via `seen_created_at`.
        let me = Keys::generate().public_key();
        let (peer, agent) = (Keys::generate().public_key(), Keys::generate().public_key());
        let mut view = PeerStatusView::default();
        view.record(&status_at(
            &agent,
            &peer,
            AgentHealth::Stopped,
            "2026-08-29T00:01:00Z",
        ))
        .unwrap();
        view.record(&status_at(
            &agent,
            &peer,
            AgentHealth::Running,
            "2026-08-29T00:00:00Z",
        ))
        .unwrap();
        assert!(
            !view.peer_blocks_spawn(&agent, &me),
            "a stale, older 'running' must not undo the newer 'stopped'"
        );
    }
}
