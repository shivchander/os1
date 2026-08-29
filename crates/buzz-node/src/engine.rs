//! The node engine: on each new desired-set or periodic tick, observe →
//! reconcile → apply → publish status. Controlled entirely via the relay.
use std::sync::Arc;
use std::time::Duration;

use nostr::{Keys, PublicKey};

use buzz_core::{AgentHealth, AgentNodeStatus};

use crate::model::{Action, NodeError, Observed};
use crate::reconcile::reconcile;
use crate::relay::NodeRelay;
use crate::substrate::Substrate;

/// Engine tuning.
pub struct EngineConfig {
    /// How often to reconcile even without a new desired-set (self-heal cadence).
    pub reconcile_tick: Duration,
    /// This node's pubkey (stamped into published status events).
    pub node_pubkey: PublicKey,
}

/// Run the node engine until the relay's desired-state stream ends (`None`).
pub async fn run(
    substrate: Arc<dyn Substrate>,
    mut relay: Box<dyn NodeRelay>,
    node_keys: Keys,
    cfg: EngineConfig,
) -> Result<(), NodeError> {
    // `node_keys` is reserved for Phase 3 (the real relay signs with it); the
    // status payload carries `node_pubkey` from config today.
    let _ = &node_keys;

    let mut current = Vec::new();
    relay.publish_presence(true).await?;

    // First tick fires one period out so tests driven purely by the desired
    // stream see no spurious startup reconcile.
    let start = tokio::time::Instant::now() + cfg.reconcile_tick;
    let mut ticker = tokio::time::interval_at(start, cfg.reconcile_tick);

    loop {
        tokio::select! {
            maybe = relay.next_desired() => match maybe {
                Some(desired) => current = desired,
                None => break,
            },
            _ = ticker.tick() => {}
        }

        let observed = substrate.observe().await;
        for action in reconcile(&current, &observed) {
            match action {
                Action::Start(d) => substrate.start(&d).await?,
                Action::Restart(d) => {
                    substrate.stop(&d.agent_pubkey).await?;
                    substrate.start(&d).await?;
                }
                Action::Stop(pk) => substrate.stop(&pk).await?,
                Action::Noop(_) => {}
            }
        }

        // Report observed status after applying actions.
        let after = substrate.observe().await;
        for (pk, obs) in &after {
            if let Some(health) = health_of(*obs) {
                let status = AgentNodeStatus {
                    format: buzz_core::node_status::FORMAT.to_string(),
                    version: buzz_core::node_status::VERSION,
                    agent_pubkey: pk.to_hex(),
                    node_pubkey: cfg.node_pubkey.to_hex(),
                    health,
                    reason: None,
                    updated_at: chrono::Utc::now().to_rfc3339(),
                };
                relay.publish_status(&status).await?;
            }
        }
    }

    relay.publish_presence(false).await?;
    Ok(())
}

/// Map an [`Observed`] state to a reportable [`AgentHealth`]; `Absent` is not
/// reported.
fn health_of(obs: Observed) -> Option<AgentHealth> {
    match obs {
        Observed::Starting => Some(AgentHealth::Starting),
        Observed::Running => Some(AgentHealth::Running),
        Observed::Stopped => Some(AgentHealth::Stopped),
        Observed::Crashed { .. } => Some(AgentHealth::Crashed),
        Observed::Absent => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{fake_desired, Observed};
    use crate::relay::FakeRelay;
    use crate::substrate::FakeSubstrate;
    use buzz_core::AgentHealth;
    use buzz_core::AssignState::{Assigned, Unassigned};
    use nostr::Keys;
    use std::sync::Arc;
    use std::time::Duration;

    fn cfg(node: &Keys) -> EngineConfig {
        // Huge tick ⇒ only the scripted desired-sets drive the loop (deterministic).
        EngineConfig {
            reconcile_tick: Duration::from_secs(3600),
            node_pubkey: node.public_key(),
        }
    }

    #[tokio::test]
    async fn assign_starts_agent_and_reports_running() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new(vec![vec![fake_desired(&a, &n, &o, Assigned)]]);
        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n))
            .await
            .unwrap();

        assert_eq!(*substrate.starts.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(
            substrate.observe().await.get(&a.public_key()),
            Some(&Observed::Running)
        );
        let statuses = handle.statuses.lock().unwrap();
        assert!(statuses.iter().any(|s| {
            s.agent_pubkey == a.public_key().to_hex()
                && s.node_pubkey == n.public_key().to_hex()
                && s.health == AgentHealth::Running
        }));
        assert_eq!(*handle.presence.lock().unwrap(), vec![true, false]);
    }

    #[tokio::test]
    async fn unassign_stops_running_agent() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        substrate.set(a.public_key(), Observed::Running);
        let (relay, handle) = FakeRelay::new(vec![vec![fake_desired(&a, &n, &o, Unassigned)]]);
        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n))
            .await
            .unwrap();

        assert_eq!(*substrate.stops.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(
            substrate.observe().await.get(&a.public_key()),
            Some(&Observed::Stopped)
        );
        assert!(
            handle
                .statuses
                .lock()
                .unwrap()
                .iter()
                .any(|s| s.agent_pubkey == a.public_key().to_hex()
                    && s.health == AgentHealth::Stopped)
        );
    }

    #[tokio::test]
    async fn crashed_agent_is_restarted() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        substrate.set(a.public_key(), Observed::Crashed { code: Some(1) });
        let (relay, _handle) = FakeRelay::new(vec![vec![fake_desired(&a, &n, &o, Assigned)]]);
        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n))
            .await
            .unwrap();

        assert!(substrate.stops.lock().unwrap().contains(&a.public_key()));
        assert_eq!(*substrate.starts.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(
            substrate.observe().await.get(&a.public_key()),
            Some(&Observed::Running)
        );
    }
}
