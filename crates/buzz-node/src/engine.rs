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
    /// How often to re-publish online presence while the engine runs.
    ///
    /// The relay stores presence as a Redis key with a TTL (see
    /// `buzz_pubsub::presence::PRESENCE_TTL_SECS`, 180s) that is only reset
    /// by a fresh presence publish. Without a recurring heartbeat, a node
    /// that publishes presence once at startup and then runs longer than
    /// that TTL — i.e. any healthy long-lived node — would look OFFLINE to
    /// the relay (and therefore to owners) despite being perfectly healthy.
    /// Keep this comfortably under the relay's TTL; the default (60s)
    /// mirrors the TTL's own "3x the heartbeat" margin.
    pub presence_interval: Duration,
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

    // Same "first tick one period out" shape: the startup publish above
    // already covers t=0, so the first heartbeat re-publish should land one
    // `presence_interval` later, not immediately.
    let presence_start = tokio::time::Instant::now() + cfg.presence_interval;
    let mut presence_ticker = tokio::time::interval_at(presence_start, cfg.presence_interval);

    loop {
        tokio::select! {
            maybe = relay.next_desired() => match maybe {
                Some(desired) => current = desired,
                None => break,
            },
            _ = ticker.tick() => {}
            _ = presence_ticker.tick() => {
                // A heartbeat, not a reconcile trigger: refresh the relay's
                // presence TTL and go straight back to waiting rather than
                // falling into the observe/reconcile/report pass below.
                relay.publish_presence(true).await?;
                continue;
            }
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
        // Huge ticks ⇒ only the scripted desired-sets drive the loop (deterministic).
        EngineConfig {
            reconcile_tick: Duration::from_secs(3600),
            presence_interval: Duration::from_secs(3600),
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
        // Shape, not exact equality: a healthy engine now re-publishes online
        // presence on a heartbeat cadence (see `presence_heartbeat_republishes_online_on_cadence`
        // below), so the log may carry extra `true`s beyond the one at
        // startup — only the first (startup) and last (shutdown) entries are
        // guaranteed.
        let presence = handle.presence.lock().unwrap().clone();
        assert_eq!(
            presence.first(),
            Some(&true),
            "must announce online at startup"
        );
        assert_eq!(
            presence.last(),
            Some(&false),
            "must announce offline at shutdown"
        );
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

    /// The presence heartbeat re-publishes online on `presence_interval`
    /// cadence, independent of reconcile/desired-state activity.
    ///
    /// Uses `FakeRelay::new_hanging` with an empty script: a plain
    /// `FakeRelay`'s `next_desired` resolves immediately (`Some`/`None`,
    /// never pending), so with any finite script the loop races straight
    /// through it and `break`s before virtual time ever advances, and the
    /// tickers never get a chance to win the `select!`. Hanging forever once
    /// the (empty) script is exhausted keeps the loop alive so it is driven
    /// purely by its tickers, matching a real long-lived, unassigned node —
    /// deterministic under paused time since only the tickers can ever
    /// resolve.
    #[tokio::test(start_paused = true)]
    async fn presence_heartbeat_republishes_online_on_cadence() {
        let n = Keys::generate();
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new_hanging(vec![]);
        let presence_interval = Duration::from_secs(60);
        let engine_cfg = EngineConfig {
            // Long enough it can never fire inside this test's window,
            // isolating the assertions to the presence heartbeat alone.
            reconcile_tick: Duration::from_secs(3600),
            presence_interval,
            node_pubkey: n.public_key(),
        };
        let task = tokio::spawn(run(substrate, Box::new(relay), n.clone(), engine_cfg));

        // Let the spawned task run its synchronous startup
        // (`publish_presence(true)`) and reach the (now-pending) select loop.
        tokio::task::yield_now().await;
        assert_eq!(
            *handle.presence.lock().unwrap(),
            vec![true],
            "must publish online once at startup, before any heartbeat"
        );

        for beats in 1..=3 {
            tokio::time::advance(presence_interval).await;
            tokio::task::yield_now().await;
            assert_eq!(
                handle.presence.lock().unwrap().len(),
                1 + beats,
                "heartbeat must re-publish online every presence_interval"
            );
        }
        assert!(
            handle.presence.lock().unwrap().iter().all(|&online| online),
            "the engine never publishes offline until shutdown, which this test never reaches"
        );

        task.abort();
    }
}
