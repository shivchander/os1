//! The node engine: on each new desired-set, peer status, or periodic tick,
//! observe → reconcile → apply → publish status. Controlled entirely via
//! the relay.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use nostr::{Keys, PublicKey};
// `tokio::time::Instant`, not `std::time::Instant` — see `move_gate`'s doc
// comment on the same import: move-gate deadlines must respect a paused
// tokio clock in tests.
use tokio::time::Instant;

use buzz_core::{AgentNodeStatus, AssignState};

use crate::health;
use crate::model::{Action, DesiredAgent, NodeError, Observed};
use crate::move_gate::{self, PeerStatusView, MOVE_HANDOFF_TIMEOUT};
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

/// Mutable per-agent bookkeeping threaded through `full_resync` /
/// `reconcile_and_apply` / `apply_action` / `start_gated`, bundled into one
/// struct so passing it around doesn't keep growing each function's own
/// parameter list as later batches add more per-agent maps (Batch B review
/// finding — clippy's `too_many_arguments` tripped once a 3rd map joined
/// `pending_spawns`).
#[derive(Default)]
struct LoopState {
    /// Deferred (move-gate-blocked) spawns awaiting either the peer
    /// reporting `stopped` or [`MOVE_HANDOFF_TIMEOUT`] elapsing (spec I4).
    pending_spawns: HashMap<PublicKey, Instant>,
    /// Last time each `Running` agent got an active smoke probe (spec §9);
    /// an agent absent from this map is treated as due immediately, which
    /// is also how a freshly (re)spawned agent gets probed right away
    /// rather than waiting out `SMOKE_PROBE_INTERVAL` — see `start_gated`,
    /// which clears an agent's entry the moment it actually starts.
    last_probe: HashMap<PublicKey, Instant>,
    /// Latched result of each agent's last actual probe (Batch B review
    /// finding): on a reconcile pass where a fresh probe isn't due,
    /// `reconcile_and_apply` feeds this latched result into `classify`
    /// instead of `None` — otherwise a real probe failure would silently
    /// heal back to `Running` on the very next non-probing pass, for lack
    /// of new evidence rather than any actual recovery.
    last_probe_result: HashMap<PublicKey, bool>,
}

/// Run the node engine until the relay's desired-state stream ends (`None`).
pub async fn run(
    substrate: Arc<dyn Substrate>,
    relay: Box<dyn NodeRelay>,
    node_keys: Keys,
    cfg: EngineConfig,
) -> Result<(), NodeError> {
    // `node_keys` is reserved for Phase 3 (the real relay signs with it); the
    // status payload carries `node_pubkey` from config today.
    let _ = &node_keys;
    let me = cfg.node_pubkey;

    let mut current: Vec<DesiredAgent> = Vec::new();
    let mut status_view = PeerStatusView::default();
    let mut state = LoopState::default();

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
        // `take_reconnected` is always true before its first call, so this
        // also performs the startup resync (spec §13 offline catch-up); it
        // fires again after every later reconnect the relay reports.
        if relay.take_reconnected() {
            full_resync(
                relay.as_ref(),
                &substrate,
                &mut current,
                &status_view,
                &mut state,
                me,
            )
            .await?;
        }

        tokio::select! {
            maybe = relay.next_desired() => match maybe {
                Some(desired) => current = desired,
                None => break,
            },
            maybe_status = relay.next_status() => {
                if let Some(s) = maybe_status {
                    if status_view.record(&s).is_err() {
                        tracing::warn!(
                            agent_pubkey = %s.agent_pubkey,
                            node_pubkey = %s.node_pubkey,
                            "dropped a peer status with an unparseable pubkey"
                        );
                    }
                }
            }
            _ = ticker.tick() => {}
            _ = presence_ticker.tick() => {
                // A heartbeat, not a reconcile trigger: refresh the relay's
                // presence TTL and go straight back to waiting rather than
                // falling into the observe/reconcile/report pass below.
                relay.publish_presence(true).await?;
                continue;
            }
        }

        reconcile_and_apply(
            relay.as_ref(),
            &substrate,
            &status_view,
            &mut state,
            me,
            &current,
        )
        .await?;
    }

    relay.publish_presence(false).await?;
    Ok(())
}

/// Rebuild `current` from a fresh relay snapshot and immediately reconcile
/// against observed state. Called at the top of every loop iteration where
/// [`NodeRelay::take_reconnected`] reports a (re)connect since the last
/// check — always true on the very first iteration — so a rebooted or
/// reconnected node restores its assigned agents from the relay instead of
/// waiting on further live updates (spec §13 offline catch-up).
///
/// KNOWN LIMITATION (tracked as a Phase 5 follow-up): this reconciles
/// `current` against [`Substrate::observe`], which for the real
/// `LocalProcessSubstrate` only knows about processes *this OS process*
/// spawned — a fresh `LocalProcessSubstrate` (as constructed after a node
/// restart) starts with an empty process table and does not discover
/// pre-existing agent processes left running by a prior node process (node
/// shutdown deliberately leaves agents running so they survive a node
/// restart; see `crates/buzz-node/src/daemon.rs`). So on a real reboot where
/// the previous agent process is still alive, this resync's `reconcile`
/// sees that agent as `Absent` and emits a second `Start`, spawning a
/// duplicate live instance until the orphaned original is separately
/// reaped — a real dup-spawn hazard against invariant I4. Closing it needs
/// `LocalProcessSubstrate` to discover pre-existing processes on startup
/// (e.g. a per-agent PID file under its workspace dir, checked for liveness
/// the way `crate::daemon::singleton::live_daemon_pid` already does for the
/// node's own PID) — a substantive `Substrate`-level change, intentionally
/// out of scope for this batch; see the Phase 5 batch A report.
///
/// A `query_desired` failure (e.g. a connection reset mid-backlog) does
/// NOT propagate out of this function: every other relay-facing path in
/// this loop is insulated from killing `run()` (`ensure_connected` retries
/// forever, `next_desired` swallows read errors, publishes are
/// fire-and-forget) — a resync is no different, and the alternative of
/// reconciling against a partial/empty snapshot would risk emitting a
/// wrong `Stop` for agents that are actually still assigned to us. On
/// failure this logs and skips the pass entirely, leaving `current`
/// untouched; the next tick, live update, or reconnect gets another
/// chance.
async fn full_resync(
    relay: &dyn NodeRelay,
    substrate: &Arc<dyn Substrate>,
    current: &mut Vec<DesiredAgent>,
    status_view: &PeerStatusView,
    state: &mut LoopState,
    me: PublicKey,
) -> Result<(), NodeError> {
    match relay.query_desired().await {
        Ok(fresh) => *current = fresh,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "full resync failed; leaving desired state unchanged for now"
            );
            return Ok(());
        }
    }
    reconcile_and_apply(relay, substrate, status_view, state, me, current).await
}

/// Observe the substrate, reconcile against `current`, apply the resulting
/// actions — spawns gated through [`start_gated`] — and publish the
/// resulting per-agent status. Shared by the main loop body and
/// [`full_resync`] so both go through the exact same gating/reporting path.
async fn reconcile_and_apply(
    relay: &dyn NodeRelay,
    substrate: &Arc<dyn Substrate>,
    status_view: &PeerStatusView,
    state: &mut LoopState,
    me: PublicKey,
    current: &[DesiredAgent],
) -> Result<(), NodeError> {
    // Prune deferrals for agents no longer desired+Assigned to us. A
    // deferred (peer-blocked, never-started) agent that leaves `current` —
    // reassigned to another node, or set Unassigned — would otherwise
    // never be revisited: it's absent from `observed` (it was never
    // started) and now absent from `current` too, so `reconcile`'s
    // universe (desired ∪ observed) excludes it entirely and no action
    // (Start or Stop) is ever emitted for it again. Its stale
    // `{agent: past-deadline}` entry would then leak for the rest of this
    // process's lifetime. If the SAME agent is later reassigned back here
    // while a *different* node is genuinely still running it, `due_pending`
    // would see that ancient deadline as already elapsed and `start_gated`
    // would spawn immediately — silently bypassing the move gate (a real
    // I4 double-spawn; see batch A review, Task 1 x Task 3 interaction).
    state.pending_spawns.retain(|agent, _| {
        current
            .iter()
            .any(|d| d.agent_pubkey == *agent && d.state == AssignState::Assigned)
    });

    let now = Instant::now();
    let due: HashSet<PublicKey> = move_gate::due_pending(&state.pending_spawns, now)
        .into_iter()
        .collect();

    let observed = substrate.observe().await;
    for action in reconcile(current, &observed) {
        apply_action(substrate, status_view, state, me, &due, action).await?;
    }

    // Report observed status after applying actions — actively probing each
    // Running agent whose last probe is stale (or absent, e.g. just spawned)
    // before classifying (spec §9 active smoke-probe health).
    let after = substrate.observe().await;
    let now = Instant::now();
    for (pk, obs) in &after {
        let probe_ok = if matches!(obs, Observed::Running) {
            let due_for_probe = state
                .last_probe
                .get(pk)
                .is_none_or(|&t| now.saturating_duration_since(t) >= health::SMOKE_PROBE_INTERVAL);
            if due_for_probe {
                let ok = substrate.probe(pk).await.is_ok();
                state.last_probe.insert(*pk, now);
                state.last_probe_result.insert(*pk, ok);
                Some(ok)
            } else {
                // Not due for a fresh probe this cycle: latch the last
                // known result rather than passing `None`, which
                // `classify` treats as "healthy" — without this, a real
                // probe failure would silently heal back to `Running` on
                // the very next non-probing pass, for lack of new
                // evidence rather than any actual recovery (Batch B
                // review finding). Only a truly never-probed agent (no
                // entry yet) falls through to `None`.
                state.last_probe_result.get(pk).copied()
            }
        } else {
            None
        };
        // Non-mutating peek (`breaker_open` itself performs a one-time,
        // consuming open→half-open transition meant for an actual start
        // attempt — see `Substrate::start`; using it here would silently
        // eat that allowance on every reporting pass for a Crashed-and-
        // no-longer-desired agent, since `Noop` never calls `start()` to
        // consume it properly).
        let breaker_open = substrate.breaker_open_peek(pk);
        if let Some((health, reason)) = health::classify(obs, probe_ok, breaker_open) {
            let status = AgentNodeStatus {
                format: buzz_core::node_status::FORMAT.to_string(),
                version: buzz_core::node_status::VERSION,
                agent_pubkey: pk.to_hex(),
                node_pubkey: me.to_hex(),
                health,
                reason,
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            relay.publish_status(&status).await?;
        }
    }
    Ok(())
}

/// Apply one reconcile [`Action`], routing `Start`/`Restart` through
/// [`start_gated`] so a move never produces two live instances (spec I4).
async fn apply_action(
    substrate: &Arc<dyn Substrate>,
    status_view: &PeerStatusView,
    state: &mut LoopState,
    me: PublicKey,
    due: &HashSet<PublicKey>,
    action: Action,
) -> Result<(), NodeError> {
    match action {
        Action::Start(d) => start_gated(substrate, status_view, state, me, due, *d).await,
        Action::Restart(d) => {
            substrate.stop(&d.agent_pubkey).await?;
            start_gated(substrate, status_view, state, me, due, *d).await
        }
        Action::Stop(pk) => {
            // Clear any stale deferral so a later re-assignment of this
            // agent starts its own fresh handoff window rather than
            // inheriting a leftover deadline.
            state.pending_spawns.remove(&pk);
            state.last_probe.remove(&pk);
            state.last_probe_result.remove(&pk);
            substrate.stop(&pk).await
        }
        Action::Noop(_) => Ok(()),
    }
}

/// Spawn `d`, unless a *different* node's latest status still claims the
/// agent alive (`Starting`/`Running`) and the bounded handoff window
/// ([`MOVE_HANDOFF_TIMEOUT`]) hasn't elapsed yet (spec I4, §8 move flow). A
/// blocked spawn is deferred into `pending_spawns` and retried on the next
/// reconcile pass (the next tick, desired update, or peer status) — it
/// fires as soon as the peer reports `stopped`, or unconditionally once
/// `due` (computed from `pending_spawns` by the caller) says the deadline
/// has passed.
async fn start_gated(
    substrate: &Arc<dyn Substrate>,
    status_view: &PeerStatusView,
    state: &mut LoopState,
    me: PublicKey,
    due: &HashSet<PublicKey>,
    d: DesiredAgent,
) -> Result<(), NodeError> {
    let agent = d.agent_pubkey;
    let blocked = status_view.peer_blocks_spawn(&agent, &me) && !due.contains(&agent);
    if blocked {
        state
            .pending_spawns
            .entry(agent)
            .or_insert_with(|| Instant::now() + MOVE_HANDOFF_TIMEOUT);
        return Ok(());
    }
    state.pending_spawns.remove(&agent);
    substrate.start(&d).await?;
    // Force an immediate smoke probe on the next reporting pass rather than
    // waiting out any stale `last_probe` timestamp left over from before a
    // crash/stop — spec §9 wants a real round-trip right after a spawn is
    // first observed running, not just on the periodic cadence.
    state.last_probe.remove(&agent);
    Ok(())
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

    fn status_of(node: &Keys, agent: &Keys, health: AgentHealth) -> AgentNodeStatus {
        AgentNodeStatus {
            format: buzz_core::node_status::FORMAT.into(),
            version: buzz_core::node_status::VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            health,
            reason: None,
            updated_at: "2026-08-29T00:00:00Z".into(),
        }
    }

    // --- Task 1: bounded stop-before-start move gate (I4) ---

    /// A peer node reporting the agent `running` defers this node's spawn
    /// until the peer reports `stopped` — proving the move gate is wired
    /// into the real `run()` loop, not just unit-tested in isolation (see
    /// `move_gate::tests`).
    #[tokio::test(start_paused = true)]
    async fn move_defers_spawn_until_peer_reports_stopped() {
        let (owner, node_m, node_n, agent) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new_hanging(vec![]);
        let task = tokio::spawn(run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            cfg(&node_m),
        ));
        tokio::task::yield_now().await; // startup resync (empty snapshot)

        // Peer N currently reports the agent running elsewhere.
        handle.push_status(status_of(&node_n, &agent, AgentHealth::Running));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // The owner now assigns the agent to M.
        handle.push_desired(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            substrate.starts.lock().unwrap().is_empty(),
            "must defer while a different node reports the agent running"
        );

        // Peer N reports stopped -> the deferred spawn fires.
        handle.push_status(status_of(&node_n, &agent, AgentHealth::Stopped));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            *substrate.starts.lock().unwrap(),
            vec![agent.public_key()],
            "spawns once the peer reports stopped"
        );

        task.abort();
    }

    /// A node's own previously-published status must never block its own
    /// spawn of the same agent (only a *different* node's status can).
    #[tokio::test(start_paused = true)]
    async fn own_status_never_blocks_own_spawn() {
        let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new_hanging(vec![]);
        let task = tokio::spawn(run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            cfg(&node_m),
        ));
        tokio::task::yield_now().await;

        handle.push_status(status_of(&node_m, &agent, AgentHealth::Running));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        handle.push_desired(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            *substrate.starts.lock().unwrap(),
            vec![agent.public_key()],
            "a node's own status must never defer its own spawn"
        );

        task.abort();
    }

    /// The deferred spawn fires once [`MOVE_HANDOFF_TIMEOUT`] elapses even
    /// if the peer never reports `stopped` — a bounded overlap, never a
    /// permanent double (I4).
    #[tokio::test(start_paused = true)]
    async fn deferred_spawn_fires_after_timeout_without_peer_stopping() {
        let (owner, node_m, node_n, agent) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new_hanging(vec![]);
        let engine_cfg = EngineConfig {
            // Short enough to observe within the test's time budget.
            reconcile_tick: Duration::from_secs(1),
            presence_interval: Duration::from_secs(3600),
            node_pubkey: node_m.public_key(),
        };
        let task = tokio::spawn(run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            engine_cfg,
        ));
        tokio::task::yield_now().await;

        handle.push_status(status_of(&node_n, &agent, AgentHealth::Running));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        handle.push_desired(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(substrate.starts.lock().unwrap().is_empty());

        // Advance past the handoff timeout without ever reporting `stopped`;
        // each reconcile tick re-derives `Start` and re-checks the deadline.
        tokio::time::advance(MOVE_HANDOFF_TIMEOUT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            *substrate.starts.lock().unwrap(),
            vec![agent.public_key()],
            "spawns anyway once the bounded handoff window elapses"
        );

        task.abort();
    }

    // --- Task 2: full resync on startup + reconnect (offline catch-up) ---

    #[tokio::test]
    async fn full_resync_spawns_assigned_agents_from_a_fresh_snapshot() {
        let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        // Observes nothing — as a freshly rebooted substrate would.
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new(vec![]);
        handle.set_snapshot(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);

        let substrate_dyn: Arc<dyn Substrate> = substrate.clone();
        let mut current = Vec::new();
        let status_view = PeerStatusView::default();
        let mut state = LoopState::default();
        full_resync(
            &relay,
            &substrate_dyn,
            &mut current,
            &status_view,
            &mut state,
            node_m.public_key(),
        )
        .await
        .unwrap();

        assert_eq!(
            *substrate.starts.lock().unwrap(),
            vec![agent.public_key()],
            "reboot restarts assigned agents from the relay's desired state"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_triggers_full_resync() {
        let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new_hanging(vec![]);
        let engine_cfg = EngineConfig {
            reconcile_tick: Duration::from_millis(50),
            presence_interval: Duration::from_secs(3600),
            node_pubkey: node_m.public_key(),
        };
        let task = tokio::spawn(run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            engine_cfg,
        ));
        tokio::task::yield_now().await; // startup resync against an empty snapshot
        assert!(substrate.starts.lock().unwrap().is_empty());

        // The assignment now exists at the relay, as if published while
        // this node was disconnected; only a reconnect (not the live tail)
        // will pick it up here.
        handle.set_snapshot(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);
        handle.simulate_reconnect();

        // The next reconcile tick notices the reconnect flag and resyncs.
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(*substrate.starts.lock().unwrap(), vec![agent.public_key()]);

        task.abort();
    }

    // --- Task 3: LWW + retarget-stop (one-live-instance, I4) ---
    //
    // The actual defect here was in `NostrNodeRelay::next_desired` (see
    // `nostr_relay::apply_assignment_event` and its tests): it silently kept
    // a stale desired-entry for an agent reassigned away from this node,
    // because decryption fails (by design) once the envelope targets a
    // different node, and the old code only ever acted on decrypt success.
    // The engine loop's own snapshot-replace semantics (`current = desired`
    // on every `next_desired` yield) were already correct — these tests
    // characterize/lock in that engine-level contract against `FakeRelay`.

    #[tokio::test]
    async fn repeated_assignment_for_an_already_running_agent_spawns_once() {
        let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, _handle) = FakeRelay::new(vec![
            vec![fake_desired(&agent, &node_m, &owner, Assigned)],
            vec![fake_desired(&agent, &node_m, &owner, Assigned)],
        ]);
        run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            cfg(&node_m),
        )
        .await
        .unwrap();

        assert_eq!(
            substrate
                .starts
                .lock()
                .unwrap()
                .iter()
                .filter(|a| **a == agent.public_key())
                .count(),
            1,
            "exactly one spawn for the agent — never two"
        );
    }

    #[tokio::test]
    async fn reassigned_away_stops_here() {
        let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        // Second snapshot omits the agent entirely — exactly what a
        // corrected `NostrNodeRelay::next_desired` now yields once the
        // agent's envelope targets a different node (see
        // `nostr_relay::apply_assignment_event`).
        let (relay, _handle) = FakeRelay::new(vec![
            vec![fake_desired(&agent, &node_m, &owner, Assigned)],
            vec![],
        ]);
        run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            cfg(&node_m),
        )
        .await
        .unwrap();

        assert_eq!(*substrate.stops.lock().unwrap(), vec![agent.public_key()]);
    }

    // --- Task 4: active smoke-probe health ---

    #[tokio::test]
    async fn failed_probe_on_running_agent_publishes_crashed_probe_failed() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        substrate.set(a.public_key(), Observed::Running);
        substrate.set_probe(a.public_key(), false);
        let (relay, handle) = FakeRelay::new(vec![]);
        handle.set_snapshot(vec![fake_desired(&a, &n, &o, Assigned)]);

        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n))
            .await
            .unwrap();

        let statuses = handle.statuses.lock().unwrap();
        let s = statuses
            .iter()
            .find(|s| s.agent_pubkey == a.public_key().to_hex())
            .expect("status published for the probed agent");
        assert_eq!(s.health, AgentHealth::Crashed);
        assert_eq!(s.reason.as_deref(), Some("probe-failed"));
    }

    #[tokio::test]
    async fn healthy_running_agent_is_probed_and_published_with_no_reason() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        substrate.set(a.public_key(), Observed::Running);
        let (relay, handle) = FakeRelay::new(vec![]);
        handle.set_snapshot(vec![fake_desired(&a, &n, &o, Assigned)]);

        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n))
            .await
            .unwrap();

        assert_eq!(
            *substrate.probes.lock().unwrap(),
            vec![a.public_key()],
            "a running assigned agent must be actively probed"
        );
        let statuses = handle.statuses.lock().unwrap();
        let s = statuses
            .iter()
            .find(|s| s.agent_pubkey == a.public_key().to_hex())
            .expect("status published");
        assert_eq!(s.health, AgentHealth::Running);
        assert_eq!(s.reason, None);
    }

    /// Batch B review finding: a probe failure must stay latched across a
    /// later reconcile pass where no fresh probe is due, not silently heal
    /// back to `Running` for lack of new evidence. Drives the engine through
    /// TWO reconcile passes for the same still-`Running` agent (a 2-entry
    /// `FakeRelay` script, each entry consumed by one loop iteration) so the
    /// 2nd pass's `probe_ok` comes from the latch, not a fresh probe.
    #[tokio::test]
    async fn probe_failure_health_is_latched_across_non_probing_reconcile_passes() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        substrate.set_probe(a.public_key(), false);
        let (relay, handle) = FakeRelay::new(vec![
            vec![fake_desired(&a, &n, &o, Assigned)],
            vec![fake_desired(&a, &n, &o, Assigned)],
        ]);

        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n))
            .await
            .unwrap();

        let statuses: Vec<_> = handle
            .statuses
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.agent_pubkey == a.public_key().to_hex())
            .cloned()
            .collect();
        assert_eq!(
            statuses.len(),
            2,
            "expected one status publish per reconcile pass"
        );
        for (i, s) in statuses.iter().enumerate() {
            assert_eq!(
                s.health,
                AgentHealth::Crashed,
                "pass {i}: a probe failure must stay latched, not heal back to Running"
            );
            assert_eq!(s.reason.as_deref(), Some("probe-failed"));
        }
        assert_eq!(
            *substrate.probes.lock().unwrap(),
            vec![a.public_key()],
            "the 2nd pass must reuse the latched result, not issue a redundant probe"
        );
    }

    /// The carried Batch A/B review finding, proven end-to-end: a breaker
    /// held open by repeated crashes must report as `Stopped`/"breaker-open",
    /// never as a fresh `Crashed`.
    #[tokio::test]
    async fn breaker_open_agent_reports_stopped_with_cooldown_reason_not_crashed() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        // A stale Crashed record from whatever run tripped the breaker -- the
        // point of this test is that it must not be re-surfaced as a fresh
        // crash while the breaker holds it in cooldown.
        substrate.set(a.public_key(), Observed::Crashed { code: Some(1) });
        substrate.set_breaker_open(a.public_key(), true);
        let (relay, handle) = FakeRelay::new(vec![]);
        handle.set_snapshot(vec![fake_desired(&a, &n, &o, Assigned)]);

        run(substrate.clone(), Box::new(relay), n.clone(), cfg(&n))
            .await
            .unwrap();

        assert!(
            substrate.starts.lock().unwrap().is_empty(),
            "the open breaker must actually have prevented a start"
        );
        let statuses = handle.statuses.lock().unwrap();
        let s = statuses
            .iter()
            .find(|s| s.agent_pubkey == a.public_key().to_hex())
            .expect("status published for the breaker-open agent");
        assert_eq!(
            s.health,
            AgentHealth::Stopped,
            "a breaker cooldown must not be reported as a fresh crash"
        );
        assert_eq!(s.reason.as_deref(), Some("breaker-open"));
    }

    // --- Batch A review fix round: Task 1 x Task 3 interaction ---

    /// A deferred (peer-blocked) spawn whose agent then leaves `current`
    /// entirely (reassigned away, before ever starting here) must not
    /// leave a stale `pending_spawns` deadline behind. If it did, a later
    /// re-assignment of the SAME agent back to this node — while a
    /// *different* node is genuinely still running it — would see that
    /// ancient deadline as already elapsed and bypass the move gate,
    /// spawning a second live instance (a real I4 violation).
    #[tokio::test(start_paused = true)]
    async fn stale_deferred_spawn_does_not_leak_past_reassignment() {
        let (owner, node_m, node_n, agent) = (
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
            Keys::generate(),
        );
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new_hanging(vec![]);
        let task = tokio::spawn(run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            cfg(&node_m),
        ));
        tokio::task::yield_now().await;

        // Peer N reports the agent running; the assignment to M arrives ->
        // deferred (pending_spawns now holds a deadline for this agent).
        handle.push_status(status_of(&node_n, &agent, AgentHealth::Running));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        handle.push_desired(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(substrate.starts.lock().unwrap().is_empty(), "deferred");

        // The agent is reassigned away from M entirely (M's own
        // `next_desired` would now omit it, per `nostr_relay::
        // apply_assignment_event`) -- simulate that snapshot directly,
        // well before the original 30s handoff window would have elapsed.
        handle.push_desired(vec![]);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Real time passes well beyond the original deferred deadline,
        // with no further events -- nothing has any reason to revisit
        // this agent while it's absent from both desired and observed.
        tokio::time::advance(MOVE_HANDOFF_TIMEOUT * 3).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Reassigned back to M -- while N is STILL genuinely running it.
        handle.push_desired(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            substrate.starts.lock().unwrap().is_empty(),
            "a stale pending_spawns deadline from the earlier defer must not \
             bypass the gate -- the peer is still genuinely running the agent"
        );

        task.abort();
    }

    /// A transient `query_desired` failure (e.g. a connection reset
    /// mid-backlog) during a resync must not crash the whole engine loop —
    /// every other relay-facing path here is insulated from killing
    /// `run()`. Proven by observing that a LATER, successful
    /// reconnect-triggered resync still works: if the earlier error had
    /// propagated out of `run()`, nothing below would ever spawn.
    #[tokio::test(start_paused = true)]
    async fn full_resync_error_does_not_abort_the_engine_loop() {
        let (owner, node_m, agent) = (Keys::generate(), Keys::generate(), Keys::generate());
        let substrate = Arc::new(FakeSubstrate::new());
        let (relay, handle) = FakeRelay::new_hanging(vec![]);
        handle.fail_next_query_desired("simulated connection reset");
        let engine_cfg = EngineConfig {
            reconcile_tick: Duration::from_millis(50),
            presence_interval: Duration::from_secs(3600),
            node_pubkey: node_m.public_key(),
        };
        let task = tokio::spawn(run(
            substrate.clone(),
            Box::new(relay),
            node_m.clone(),
            engine_cfg,
        ));
        // The startup resync's query_desired fails.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        handle.set_snapshot(vec![fake_desired(&agent, &node_m, &owner, Assigned)]);
        handle.simulate_reconnect();
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            *substrate.starts.lock().unwrap(),
            vec![agent.public_key()],
            "a transient resync error must not crash the engine loop"
        );

        task.abort();
    }
}
