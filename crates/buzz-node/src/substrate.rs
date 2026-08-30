//! The substrate abstraction — how the node observes and controls the local
//! process table — plus the real [`LocalProcessSubstrate`] and an in-memory
//! fake for tests.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nostr::PublicKey;
use tokio::process::Child;

use crate::model::{DesiredAgent, NodeError, Observed};
use crate::runtime::AgentRuntime;

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
    /// True if `agent`'s crash-restart breaker currently forbids a start —
    /// a deliberate cooldown after repeated crashes, not a fresh unexpected
    /// failure (spec §9). **Consuming**: once the cooldown has elapsed, this
    /// call itself performs the one-time open→half-open transition that
    /// allows exactly one probe start through (see [`Circuit::is_open`]) —
    /// call this ONLY from an actual start attempt
    /// ([`Substrate::start`]). Anything else (e.g. health reporting) must
    /// use [`Substrate::breaker_open_peek`] instead, or it will silently
    /// consume `start`'s one-time allowance without ever attempting a start
    /// (Batch B review finding).
    fn breaker_open(&self, agent: &PublicKey) -> bool;
    /// Non-mutating peek at whether `agent`'s breaker currently reports
    /// open — the same true/false answer as [`Substrate::breaker_open`] at
    /// this instant, but never performs the open→half-open transition. Safe
    /// to call repeatedly and from a path that isn't actually attempting a
    /// start, e.g. [`crate::health::classify`]'s breaker check in the
    /// engine's status-reporting loop.
    fn breaker_open_peek(&self, agent: &PublicKey) -> bool;
    /// Actively probe a running agent for liveness beyond mere OS-process
    /// existence (spec §9 active smoke-probe) — delegates to the underlying
    /// [`crate::runtime::AgentRuntime::probe`].
    async fn probe(&self, agent: &PublicKey) -> Result<(), NodeError>;
}

/// Crash-restart circuit breaker: crashes within this window count toward
/// tripping the breaker (mirrors `buzz-acp`'s `SlotCircuit`).
const BREAKER_THRESHOLD: usize = 3;
/// Sliding window for counting recent crashes.
const BREAKER_WINDOW: Duration = Duration::from_secs(60);
/// How long an opened breaker stays open before allowing one half-open probe.
const BREAKER_COOLDOWN: Duration = Duration::from_secs(300);
/// Grace period between SIGTERM and SIGKILL when stopping a process group.
const STOP_GRACE: Duration = Duration::from_millis(500);
/// Poll interval while waiting for a signaled process group to exit.
const STOP_POLL: Duration = Duration::from_millis(20);
/// File name of the per-agent PID record written on every successful
/// [`Substrate::start`], under the same per-agent directory as
/// [`workspace_dir`]. Contains nothing but a bare pid integer (mirrors
/// `crate::daemon`'s own `daemon.pid`) — never a secret. Consulted by
/// [`LocalProcessSubstrate::adopt_existing`] on the next construction (e.g.
/// after a node/daemon restart) to re-find and adopt a still-running agent.
const AGENT_PID_FILE: &str = "agent.pid";

/// Per-agent crash-restart breaker state. All transitions go through
/// [`Circuit::record_crash`] (called from `observe()` on each newly detected
/// crash) and [`Circuit::is_open`] (called from `start()` before spawning) —
/// callers never touch `crash_times`/`open_until` directly.
#[derive(Default)]
struct Circuit {
    crash_times: Vec<Instant>,
    open_until: Option<Instant>,
}

impl Circuit {
    /// Record a newly observed crash, opening the circuit once
    /// [`BREAKER_THRESHOLD`] crashes land inside [`BREAKER_WINDOW`].
    fn record_crash(&mut self) {
        let now = Instant::now();
        self.crash_times.push(now);
        self.crash_times
            .retain(|&t| now.duration_since(t) < BREAKER_WINDOW);
        if self.crash_times.len() >= BREAKER_THRESHOLD {
            self.open_until = Some(now + BREAKER_COOLDOWN);
        }
    }

    /// True if a start should currently be refused. A cooled-down breaker
    /// transitions to half-open here: it allows exactly one probe start,
    /// pre-seeding `crash_times` so an immediate re-crash reopens the
    /// circuit without waiting for a fresh full window of crashes.
    fn is_open(&mut self) -> bool {
        match self.open_until {
            Some(until) if Instant::now() < until => true,
            Some(_) => {
                self.open_until = None;
                self.crash_times.clear();
                self.crash_times
                    .extend(std::iter::repeat_n(Instant::now(), BREAKER_THRESHOLD - 1));
                false
            }
            None => false,
        }
    }

    /// Non-mutating peek: true iff currently within the cooldown window.
    /// Unlike [`Self::is_open`], never performs the open→half-open
    /// transition — safe to call from a path that isn't actually attempting
    /// a start (see [`Substrate::breaker_open_peek`]).
    fn is_open_peek(&self) -> bool {
        matches!(self.open_until, Some(until) if Instant::now() < until)
    }
}

/// One agent's process-table entry.
enum AgentSlot {
    /// A spawned child being tracked; poll `try_wait` to distinguish Running
    /// from Crashed. `pid` is captured at spawn time — it doubles as the
    /// process-group id, since the child is spawned into its own group — and
    /// stored rather than re-read later because `Child::id()` returns `None`
    /// once the child has been reaped.
    Live {
        child: Child,
        pid: Option<u32>,
        /// Whether this slot's crash has already been recorded into the
        /// breaker, so repeatedly observing the same exited child doesn't
        /// over-count crashes.
        reported_crash: bool,
    },
    /// A process this substrate did NOT spawn itself but discovered, alive,
    /// in a per-agent PID record left by a prior incarnation of this node
    /// process (see [`LocalProcessSubstrate::adopt_existing`]) — closes the
    /// dup-spawn-on-restart hazard (spec §13 I3/I4): `observe()` reports
    /// this as `Running` from the very first call, so `reconcile` emits
    /// `Noop` instead of a second `Start`. There is no [`Child`] handle to
    /// `try_wait`/reap (this OS process never forked it), so liveness is
    /// polled via [`pid_is_alive`] instead — see `observe()`.
    Adopted {
        pid: u32,
        /// Same one-time-recording guard as `Live::reported_crash`, needed
        /// here too: `pid_is_alive` is polled fresh on every `observe()`
        /// call (there is no "already reaped" terminal state to fall back
        /// on the way a real `Child`'s cached exit status provides), so
        /// without this an agent that died once would re-feed the breaker
        /// on every subsequent reconcile pass for as long as it stays
        /// unreplaced.
        reported_crash: bool,
    },
    /// Intentionally stopped; persists until the next `start()` so the
    /// engine's post-action `observe()` can report the terminal status once
    /// (mirrors `FakeSubstrate::stop`, which leaves the same tombstone).
    Stopped,
    /// `start()` could not produce a live child at all — workspace-directory
    /// creation or the `AgentRuntime::spawn` call itself failed (e.g. a
    /// missing binary or an unwritable workspace) — so there is no process
    /// to track. Observed as `Crashed` so reconcile retries it (subject to
    /// the breaker) exactly like a post-spawn crash, and the failure is
    /// contained here rather than propagated out of `start()`, so one bad
    /// agent can never take down the whole node's reconcile loop. The
    /// failure reason is logged at the point of detection (see
    /// `record_start_failure`) rather than stored here, since nothing
    /// downstream currently reads a per-agent error string back out of the
    /// table.
    Failed,
}

/// A real substrate: a local child-process table, one persistent workspace
/// directory per agent, and a per-agent crash-restart circuit breaker.
/// Delegates the actual spawning to an [`AgentRuntime`] (the D7 seam).
pub struct LocalProcessSubstrate {
    runtime: std::sync::Arc<dyn AgentRuntime>,
    relay_url: String,
    root: PathBuf,
    table: std::sync::Mutex<BTreeMap<PublicKey, AgentSlot>>,
    breaker: std::sync::Mutex<BTreeMap<PublicKey, Circuit>>,
}

impl LocalProcessSubstrate {
    /// Build a substrate that spawns through `runtime`, passes `relay_url`
    /// to every spawned agent, and roots per-agent workspaces under `root`.
    ///
    /// Synchronously scans `root` for per-agent PID records left by a prior
    /// incarnation of this process and adopts any still-alive ones (see
    /// [`Self::adopt_existing`]) before returning — so the very first
    /// `observe()` call already reports them, closing the
    /// dup-spawn-on-restart hazard a fresh, always-empty table used to
    /// cause (spec §13 I3/I4; see `crate::engine::full_resync`'s doc
    /// comment).
    pub fn new(
        runtime: std::sync::Arc<dyn AgentRuntime>,
        relay_url: String,
        root: PathBuf,
    ) -> Self {
        let table = Self::adopt_existing(&root);
        Self {
            runtime,
            relay_url,
            root,
            table: std::sync::Mutex::new(table),
            breaker: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    /// Scan `<root>/agents/*/agent.pid` for PID records a prior incarnation
    /// of this process left behind, adopting each still-alive one so this
    /// substrate's very first `observe()` reports it `Running` (spec §13
    /// I3/I4 — see the module-level doc comment on
    /// `crate::engine::full_resync`).
    ///
    /// A record naming a pid nothing runs as (the agent genuinely didn't
    /// survive, was already stopped, or the file is corrupt/unreadable) is
    /// removed and NOT adopted — that agent is simply absent from the
    /// returned table, exactly as it would be today, so `reconcile` spawns
    /// it fresh.
    ///
    /// Best-effort and non-fatal throughout: a missing `agents` directory
    /// (a brand-new node), an unreadable entry, or a malformed directory
    /// name just skips that one entry (logged) rather than failing
    /// construction — adoption exists to improve on the pre-adoption
    /// baseline (an always-empty table), never to make startup less robust
    /// than that baseline.
    ///
    /// SCOPE: liveness is checked via [`pid_is_alive`] (unix `kill(pid,
    /// 0)`, with a Windows `tasklist` fallback), corroborated by
    /// [`is_own_group_leader`] (every agent this substrate spawns is its
    /// own process-group leader by construction) before adopting — this
    /// narrows, but does not fully close, pid-reuse risk (a coincidental
    /// impostor could also be a group leader; see `is_own_group_leader`'s
    /// own doc comment). Accepted for v1 given this feature's target
    /// ("always-on boxes I own", not short-lived/high-churn hosts where pid
    /// reuse between a stop and a much-later restart is more likely); see
    /// `crate::engine::full_resync`'s doc comment for the full limitations
    /// list.
    fn adopt_existing(root: &Path) -> BTreeMap<PublicKey, AgentSlot> {
        let mut table = BTreeMap::new();
        let agents_dir = root.join("agents");
        let entries = match std::fs::read_dir(&agents_dir) {
            Ok(entries) => entries,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        error = %e,
                        dir = %agents_dir.display(),
                        "failed to scan agents dir for pid adoption; starting with no adopted agents"
                    );
                }
                return table;
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "failed to read an entry while scanning agents dir for pid adoption"
                    );
                    continue;
                }
            };
            if !entry.path().is_dir() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                tracing::warn!(
                    dir = ?entry.path(),
                    "skipping non-UTF8 entry under agents dir during pid adoption"
                );
                continue;
            };
            let Ok(agent) = PublicKey::from_hex(name) else {
                tracing::warn!(
                    dir = name,
                    "skipping non-agent entry under agents dir during pid adoption"
                );
                continue;
            };
            let pid_path = entry.path().join(AGENT_PID_FILE);
            let recorded_pid = std::fs::read_to_string(&pid_path)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok());
            match recorded_pid {
                Some(pid) if pid_is_alive(pid) && is_own_group_leader(pid) => {
                    tracing::info!(
                        agent = %agent.to_hex(),
                        pid,
                        "adopted a still-running agent process from a previous run"
                    );
                    table.insert(
                        agent,
                        AgentSlot::Adopted {
                            pid,
                            reported_crash: false,
                        },
                    );
                }
                Some(pid) if pid_is_alive(pid) => {
                    // Alive, but not its own process-group leader: every
                    // genuine agent is spawned into its own group
                    // (`AgentRuntime::spawn`'s contract), so this pid record
                    // now names an unrelated process that happens to have
                    // reused the recorded pid -- a pid-reuse collision, not
                    // our agent. Reject rather than adopt (see
                    // `is_own_group_leader`'s doc comment).
                    tracing::warn!(
                        agent = %agent.to_hex(),
                        pid,
                        "pid record names a live process that is not its own process-group \
                         leader; treating as a pid-reuse collision, not adopting"
                    );
                    let _ = std::fs::remove_file(&pid_path);
                }
                _ => {
                    // Dead pid, unreadable/corrupt record, or none at all:
                    // nothing to adopt. Clean up so a stale file can never
                    // be misread by a later scan. Best-effort: a failed
                    // removal just leaves this agent `Absent`, same as
                    // today, not a new hazard.
                    let _ = std::fs::remove_file(&pid_path);
                }
            }
        }
        table
    }

    /// Lock the process table, mapping mutex poisoning to a [`NodeError`]
    /// instead of panicking. A poisoned lock means some other call already
    /// panicked while holding it; that must not additionally crash *this*
    /// caller (`start`/`stop` are on the same hot path the engine drives
    /// every reconcile tick for every agent).
    fn lock_table(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<PublicKey, AgentSlot>>, NodeError> {
        self.table
            .lock()
            .map_err(|_| NodeError::Substrate("process table lock poisoned".into()))
    }

    /// Lock the breaker map, mapping mutex poisoning to a [`NodeError`].
    fn lock_breaker(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<PublicKey, Circuit>>, NodeError> {
        self.breaker
            .lock()
            .map_err(|_| NodeError::Substrate("breaker lock poisoned".into()))
    }

    /// Record a failed start attempt: workspace creation or the runtime's
    /// `spawn()` call itself failed, so there was never a live child to
    /// track. Marks the slot [`AgentSlot::Failed`] (observed as `Crashed`)
    /// and feeds the crash-restart breaker exactly as a post-spawn crash
    /// would, so a persistently broken agent (missing binary, unwritable
    /// workspace, ...) backs off instead of being retried on every tick.
    ///
    /// This is the containment boundary for [`Substrate::start`]: callers
    /// pass the underlying error's `reason` for a log line, then treat this
    /// call's own (rare — lock poisoning only) failure as the only thing
    /// still worth propagating.
    fn record_start_failure(&self, agent: PublicKey, reason: &str) -> Result<(), NodeError> {
        tracing::warn!(
            agent = %agent.to_hex(),
            reason,
            "agent failed to start; marked Failed and recorded as a breaker crash"
        );
        self.lock_table()?.insert(agent, AgentSlot::Failed);
        self.lock_breaker()?
            .entry(agent)
            .or_default()
            .record_crash();
        Ok(())
    }

    /// Persist `pid` as `agent`'s PID record (see [`AGENT_PID_FILE`]) so a
    /// future [`Self::adopt_existing`] scan (e.g. after a node/daemon
    /// restart) can re-find and adopt this process if it's still alive.
    /// Best-effort: a write failure is only logged, never propagated — the
    /// agent is already successfully spawned by the time this is called, so
    /// failing to persist the adoption record must not be treated as a
    /// start failure; it only means this particular agent won't be adopted
    /// on a future restart (degrading to today's pre-adoption behavior for
    /// that one agent, not a new hazard).
    fn record_pid(&self, agent: &PublicKey, pid: u32) {
        let path = pid_file_path(&self.root, agent);
        if let Err(e) = std::fs::write(&path, pid.to_string()) {
            tracing::warn!(
                agent = %agent.to_hex(),
                error = %e,
                "failed to persist agent pid record; this agent will not be adopted on a future restart"
            );
        }
    }

    /// Remove `agent`'s PID record, if any. Called on every [`Self::stop`]
    /// so a pid this substrate just intentionally terminated is never later
    /// misread as still belonging to that agent if the OS recycles the pid
    /// number before this agent is started again — shrinking (not
    /// eliminating; see [`Self::adopt_existing`]'s doc comment) the
    /// pid-reuse window. A missing file is not an error (never started, or
    /// already cleared).
    fn clear_pid_record(&self, agent: &PublicKey) {
        let path = pid_file_path(&self.root, agent);
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    agent = %agent.to_hex(),
                    error = %e,
                    "failed to clear agent pid record"
                );
            }
        }
    }
}

/// The persistent workspace directory for `agent` under a substrate rooted
/// at `root`: `<root>/agents/<agent-hex>/workspace`.
pub fn workspace_dir(root: &Path, agent: &PublicKey) -> PathBuf {
    root.join("agents").join(agent.to_hex()).join("workspace")
}

/// The PID record path for `agent` under a substrate rooted at `root`:
/// `<root>/agents/<agent-hex>/agent.pid` — a sibling of [`workspace_dir`]'s
/// directory. See [`AGENT_PID_FILE`].
fn pid_file_path(root: &Path, agent: &PublicKey) -> PathBuf {
    root.join("agents")
        .join(agent.to_hex())
        .join(AGENT_PID_FILE)
}

/// True iff a process with pid `pid` currently exists (adoption's liveness
/// check — see [`LocalProcessSubstrate::adopt_existing`]). Mirrors
/// `crate::daemon::singleton::pid_is_alive`'s logic exactly; kept as an
/// independent copy rather than a shared helper because that one lives in
/// the `buzz-node` *binary* crate (for the daemon's own PID file) while this
/// one lives in the *library* crate (for per-agent adoption) — the binary
/// depends on the library, never the reverse, so a three-line platform check
/// can't be shared across that boundary without a new module solely for it.
#[cfg(unix)]
fn pid_is_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    // Signal 0: existence/permission check only -- no signal is delivered.
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

#[cfg(windows)]
fn pid_is_alive(pid: u32) -> bool {
    // No unsafe FFI (`OpenProcess`) allowed here (`#![deny(unsafe_code)]`),
    // so shell out exactly like `crate::daemon::singleton::pid_is_alive`'s
    // Windows arm does.
    std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).contains(&pid.to_string()))
        .unwrap_or(false)
}

#[cfg(not(any(unix, windows)))]
fn pid_is_alive(_pid: u32) -> bool {
    // Can't verify liveness on this target; fail closed (assume alive) so
    // adoption never mistakes a live agent for dead and double-spawns it —
    // mirrors `crate::daemon::singleton::pid_is_alive`'s identical stance
    // for the same reason.
    true
}

/// True iff `pid` is the leader of its own process group (`getpgid(pid) ==
/// pid`) — the invariant every process this substrate spawns satisfies by
/// construction (every [`AgentRuntime::spawn`] implementation is required
/// to call `process_group(0)`). `pid_is_alive` alone only proves SOME
/// process currently has this pid — on a long-lived box, an OS can recycle
/// a pid for a completely unrelated process between this agent stopping
/// and a much-later restart's adoption scan. Corroborating group
/// leadership doesn't fully close that (an impostor could coincidentally
/// also be a group leader), but it's cheap and meaningfully narrows it, and
/// it's the difference between merely mis-observing an impostor as
/// `Running` (bad but contained) and `killpg`-signaling an entirely
/// unrelated process group when that impostor is later "stopped" (much
/// worse — see [`terminate_adopted`]). Used both at adoption time
/// ([`LocalProcessSubstrate::adopt_existing`], to decide whether to adopt
/// at all) and again right before actually signaling
/// ([`terminate_adopted`], defense-in-depth against the narrow window
/// where a pid is reused between those two points).
#[cfg(unix)]
fn is_own_group_leader(pid: u32) -> bool {
    use nix::unistd::{getpgid, Pid};
    let target = Pid::from_raw(pid as i32);
    getpgid(Some(target)) == Ok(target)
}

/// Windows has no `unsafe`-FFI-free equivalent of process-group leadership
/// to check, and [`terminate_adopted`]'s Windows arm doesn't use `killpg`
/// (it targets a specific pid's whole tree via `taskkill /T` instead), so
/// the risk this guards against on unix doesn't apply the same way here —
/// always true so it never blocks Windows adoption/termination.
#[cfg(windows)]
fn is_own_group_leader(_pid: u32) -> bool {
    true
}

#[cfg(not(any(unix, windows)))]
fn is_own_group_leader(_pid: u32) -> bool {
    true
}

/// Terminate the process group rooted at `pid` when there is no [`Child`]
/// handle to poll/reap — i.e. an [`AgentSlot::Adopted`] process this
/// substrate discovered rather than spawned itself. Signals exactly like
/// [`kill_group`] (SIGTERM, a grace period, then SIGKILL), but polls
/// [`pid_is_alive`] instead of `try_wait` since there is no local `Child` to
/// ask. Never reaps: an adopted process was never actually this OS
/// process's child (`wait`/`waitpid` only works on a real parent-child
/// relationship) — whatever process it was reparented to (`init`/`launchd`,
/// once the prior `buzz-node` incarnation that originally spawned it
/// exited) owns that.
#[cfg(unix)]
async fn terminate_adopted(pid: u32) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    // Defense-in-depth re-check (see `is_own_group_leader`'s doc comment):
    // `adopt_existing` already corroborated this at adoption time, but
    // re-verify right before actually signaling in case the pid was reused
    // in the window since. `killpg` on an impostor's group would otherwise
    // signal an unrelated process tree.
    if !is_own_group_leader(pid) {
        tracing::warn!(
            pid,
            "refusing to killpg a pid that is no longer its own process-group leader"
        );
        return;
    }
    let pgid = Pid::from_raw(pid as i32);
    let _ = killpg(pgid, Signal::SIGTERM);
    let deadline = Instant::now() + STOP_GRACE;
    loop {
        if !pid_is_alive(pid) {
            break;
        }
        if Instant::now() >= deadline {
            let _ = killpg(pgid, Signal::SIGKILL);
            break;
        }
        tokio::time::sleep(STOP_POLL).await;
    }
}

/// Windows fallback: `taskkill /T /F` terminates the whole process tree
/// rooted at `pid`, mirroring [`kill_group`]'s Windows arm minus the
/// `Child` reap (see [`terminate_adopted`]'s doc comment for why there is
/// none here).
#[cfg(windows)]
async fn terminate_adopted(pid: u32) {
    let _ = tokio::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .status()
        .await;
}

/// Fallback for other targets: best-effort direct kill, keeping the crate
/// compiling everywhere (mirrors [`kill_group`]'s equivalent fallback arm).
#[cfg(not(any(unix, windows)))]
async fn terminate_adopted(_pid: u32) {}

#[async_trait]
impl Substrate for LocalProcessSubstrate {
    async fn observe(&self) -> BTreeMap<PublicKey, Observed> {
        let mut crashed_now = Vec::new();
        let mut out = BTreeMap::new();
        {
            let mut table = self
                .table
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (pk, slot) in table.iter_mut() {
                let observed = match slot {
                    AgentSlot::Stopped => Observed::Stopped,
                    // No live child was ever produced for this attempt;
                    // report it exactly like a crash so reconcile retries it
                    // (subject to the breaker, which was already fed at the
                    // point of failure in `record_start_failure`).
                    AgentSlot::Failed => Observed::Crashed { code: None },
                    // No `Child` handle to `try_wait` -- poll OS-level
                    // liveness directly instead. Guarded by the same
                    // one-time `reported_crash` flag `Live` uses, since this
                    // poll (unlike `try_wait`) has no "already reaped"
                    // terminal state of its own to stop re-detecting the
                    // same death on every subsequent `observe()` call.
                    //
                    // Also re-corroborates `is_own_group_leader` on every
                    // poll, not just at adopt time: an agent that crashes
                    // while the daemon stays up never goes through `stop()`
                    // (which would clear this slot), so a pid the OS later
                    // recycles for an unrelated process-group leader would
                    // otherwise latch `Running` on that impostor forever —
                    // an even more likely trigger than the daemon-restart
                    // window `adopt_existing`'s own check guards. A mismatch
                    // here is treated exactly like the pid dying: reported
                    // (once) as a crash so `reconcile` retries it, same as
                    // any other `Adopted` slot whose process is gone.
                    AgentSlot::Adopted {
                        pid,
                        reported_crash,
                    } => {
                        if pid_is_alive(*pid) && is_own_group_leader(*pid) {
                            Observed::Running
                        } else {
                            if !*reported_crash {
                                *reported_crash = true;
                                crashed_now.push(*pk);
                            }
                            Observed::Crashed { code: None }
                        }
                    }
                    AgentSlot::Live {
                        child,
                        reported_crash,
                        ..
                    } => match child.try_wait() {
                        Ok(None) => Observed::Running,
                        Ok(Some(status)) => {
                            if !*reported_crash {
                                *reported_crash = true;
                                crashed_now.push(*pk);
                            }
                            Observed::Crashed {
                                code: status.code(),
                            }
                        }
                        Err(_) => {
                            if !*reported_crash {
                                *reported_crash = true;
                                crashed_now.push(*pk);
                            }
                            Observed::Crashed { code: None }
                        }
                    },
                };
                out.insert(*pk, observed);
            }
        }
        if !crashed_now.is_empty() {
            let mut breaker = self
                .breaker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for pk in crashed_now {
                breaker.entry(pk).or_default().record_crash();
            }
        }
        out
    }

    async fn start(&self, desired: &DesiredAgent) -> Result<(), NodeError> {
        if self.breaker_open(&desired.agent_pubkey) {
            // Refuse to spawn while the breaker is open. The engine will
            // call start() again on the next reconcile tick, at which point
            // this check is re-evaluated (and eventually allows a half-open
            // probe once the cooldown elapses).
            return Ok(());
        }
        // Both failure modes below are contained here, not propagated: the
        // engine's run() loop does `substrate.start(&d).await?` for every
        // agent in the same reconcile pass, so a hard `Err` from this method
        // for one broken agent (missing binary, unwritable workspace, ...)
        // would tear down the whole loop and stop *every* agent on this
        // node, not just the broken one.
        let ws = workspace_dir(&self.root, &desired.agent_pubkey);
        if let Err(e) = std::fs::create_dir_all(&ws) {
            return self
                .record_start_failure(desired.agent_pubkey, &format!("create workspace dir: {e}"));
        }
        match self.runtime.spawn(desired, &ws, &self.relay_url).await {
            Ok(child) => {
                let pid = child.id();
                // Persist BEFORE tracking in-memory, so a future restart can
                // adopt this process even if something after this point
                // (there is nothing async left in this function, but keep
                // the ordering robust to later refactors) never runs. Only a
                // real pid is worth persisting; `Child::id()` returning
                // `None` (already reaped) is already handled below exactly
                // like today, without a pid record to leave behind.
                if let Some(pid) = pid {
                    self.record_pid(&desired.agent_pubkey, pid);
                }
                self.lock_table()?.insert(
                    desired.agent_pubkey,
                    AgentSlot::Live {
                        child,
                        pid,
                        reported_crash: false,
                    },
                );
                Ok(())
            }
            Err(e) => self.record_start_failure(desired.agent_pubkey, &e.to_string()),
        }
    }

    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError> {
        let removed = self.lock_table()?.remove(agent);
        match removed {
            Some(AgentSlot::Live { mut child, pid, .. }) => kill_group(pid, &mut child).await,
            Some(AgentSlot::Adopted { pid, .. }) => terminate_adopted(pid).await,
            Some(AgentSlot::Stopped) | Some(AgentSlot::Failed) | None => {}
        }
        // Always clear, regardless of what (if anything) was removed above:
        // an intentional stop must never leave a pid record behind for a
        // future restart to misadopt if the OS later recycles that pid.
        self.clear_pid_record(agent);
        self.lock_table()?.insert(*agent, AgentSlot::Stopped);
        Ok(())
    }

    fn breaker_open(&self, agent: &PublicKey) -> bool {
        self.breaker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(*agent)
            .or_default()
            .is_open()
    }

    fn breaker_open_peek(&self, agent: &PublicKey) -> bool {
        self.breaker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent)
            .is_some_and(Circuit::is_open_peek)
    }

    async fn probe(&self, agent: &PublicKey) -> Result<(), NodeError> {
        self.runtime.probe(agent).await
    }
}

/// Terminate the process group rooted at `pid` (mirrors
/// `buzz-dev-mcp::shell::KillGroup`): SIGTERM, a short grace period polling
/// `try_wait`, then SIGKILL, then reap. A no-op if `pid` is `None`. Always
/// reaps `child` so a killed process never lingers as a zombie.
#[cfg(unix)]
async fn kill_group(pid: Option<u32>, child: &mut Child) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    if let Some(pid) = pid {
        let pgid = Pid::from_raw(pid as i32);
        let _ = killpg(pgid, Signal::SIGTERM);
        let deadline = Instant::now() + STOP_GRACE;
        loop {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = killpg(pgid, Signal::SIGKILL);
                    break;
                }
                Ok(None) => tokio::time::sleep(STOP_POLL).await,
            }
        }
    }
    let _ = child.wait().await;
}

/// Windows fallback: `taskkill /T /F` terminates the whole process tree
/// rooted at `pid` without requiring `unsafe` Job Object FFI.
#[cfg(windows)]
async fn kill_group(pid: Option<u32>, child: &mut Child) {
    if let Some(pid) = pid {
        let _ = tokio::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .await;
    }
    let _ = child.wait().await;
}

/// Fallback for other targets: best-effort direct kill (no descendant
/// cleanup), keeping the crate compiling everywhere.
#[cfg(not(any(unix, windows)))]
async fn kill_group(_pid: Option<u32>, child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
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
    /// Pubkeys passed to `probe`, in order.
    pub probes: std::sync::Mutex<Vec<PublicKey>>,
    /// Agents currently scripted as having an open crash-restart breaker —
    /// see [`Self::set_breaker_open`].
    open_breakers: std::sync::Mutex<std::collections::BTreeSet<PublicKey>>,
    /// Per-agent scripted `probe` outcome; absent = succeeds. See
    /// [`Self::set_probe`].
    probe_results: std::sync::Mutex<BTreeMap<PublicKey, bool>>,
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
    /// Script whether `agent`'s crash-restart breaker reports open —
    /// mirrors [`LocalProcessSubstrate::breaker_open`]'s effect on `start`
    /// (a refused start leaves the process table untouched).
    pub fn set_breaker_open(&self, agent: PublicKey, open: bool) {
        let mut breakers = self.open_breakers.lock().expect("lock");
        if open {
            breakers.insert(agent);
        } else {
            breakers.remove(&agent);
        }
    }
    /// Script the outcome of subsequent `probe` calls for `agent`.
    /// `ok = false` makes `probe` return an `Err`.
    pub fn set_probe(&self, agent: PublicKey, ok: bool) {
        self.probe_results.lock().expect("lock").insert(agent, ok);
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl Substrate for FakeSubstrate {
    async fn observe(&self) -> BTreeMap<PublicKey, Observed> {
        self.inner.lock().expect("lock").clone()
    }
    async fn start(&self, desired: &DesiredAgent) -> Result<(), NodeError> {
        if self.breaker_open(&desired.agent_pubkey) {
            // Mirrors `LocalProcessSubstrate::start`: refuse silently,
            // leaving the process table untouched.
            return Ok(());
        }
        self.starts.lock().expect("lock").push(desired.agent_pubkey);
        self.inner
            .lock()
            .expect("lock")
            .insert(desired.agent_pubkey, Observed::Running);
        Ok(())
    }
    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError> {
        self.stops.lock().expect("lock").push(*agent);
        self.inner
            .lock()
            .expect("lock")
            .insert(*agent, Observed::Stopped);
        Ok(())
    }
    fn breaker_open(&self, agent: &PublicKey) -> bool {
        self.open_breakers.lock().expect("lock").contains(agent)
    }
    fn breaker_open_peek(&self, agent: &PublicKey) -> bool {
        // The fake models breaker state as a simple sticky flag rather than
        // a timed cooldown with a consuming half-open transition, so peek
        // and the consuming check are identical here — the distinction only
        // matters against the real `Circuit`, exercised by
        // `substrate::tests::circuit_peek_does_not_consume_the_half_open_transition`.
        self.open_breakers.lock().expect("lock").contains(agent)
    }
    async fn probe(&self, agent: &PublicKey) -> Result<(), NodeError> {
        self.probes.lock().expect("lock").push(*agent);
        match self.probe_results.lock().expect("lock").get(agent) {
            Some(false) => Err(NodeError::Substrate("simulated probe failure".into())),
            _ => Ok(()),
        }
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
        assert_eq!(
            s.observe().await.get(&a.public_key()),
            Some(&Observed::Running)
        );
        s.stop(&a.public_key()).await.unwrap();
        assert_eq!(
            s.observe().await.get(&a.public_key()),
            Some(&Observed::Stopped)
        );
        assert_eq!(*s.starts.lock().unwrap(), vec![a.public_key()]);
        assert_eq!(*s.stops.lock().unwrap(), vec![a.public_key()]);
    }

    #[tokio::test]
    async fn fake_substrate_set_scripts_observed() {
        let a = Keys::generate();
        let s = FakeSubstrate::new();
        s.set(a.public_key(), Observed::Crashed { code: Some(2) });
        assert_eq!(
            s.observe().await.get(&a.public_key()),
            Some(&Observed::Crashed { code: Some(2) })
        );
    }

    #[tokio::test]
    async fn fake_substrate_scripts_breaker_open_and_probe_outcome() {
        let a = Keys::generate().public_key();
        let s = FakeSubstrate::new();
        assert!(!s.breaker_open(&a), "closed by default");
        s.set_breaker_open(a, true);
        assert!(s.breaker_open(&a));
        s.set_breaker_open(a, false);
        assert!(!s.breaker_open(&a));

        assert!(s.probe(&a).await.is_ok(), "succeeds by default");
        s.set_probe(a, false);
        assert!(s.probe(&a).await.is_err());
        assert_eq!(
            *s.probes.lock().unwrap(),
            vec![a, a],
            "every probe call is logged"
        );
    }

    #[tokio::test]
    async fn fake_substrate_start_refuses_while_breaker_open() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let s = FakeSubstrate::new();
        s.set_breaker_open(a.public_key(), true);
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);

        s.start(&d).await.unwrap();

        assert!(
            s.starts.lock().unwrap().is_empty(),
            "must not record a start while the breaker is open"
        );
        assert_eq!(
            s.observe().await.get(&a.public_key()),
            None,
            "table must be untouched by a refused start"
        );
    }

    // --- LocalProcessSubstrate ---

    struct SleepRuntime;
    #[async_trait::async_trait]
    impl crate::runtime::AgentRuntime for SleepRuntime {
        async fn spawn(
            &self,
            _desired: &DesiredAgent,
            workspace: &std::path::Path,
            _relay_url: &str,
        ) -> Result<tokio::process::Child, NodeError> {
            let mut cmd = tokio::process::Command::new("/bin/sh");
            cmd.args(["-c", "sleep 5"])
                .current_dir(workspace)
                .kill_on_drop(true); // belt-and-suspenders: never orphan on test panic
                                     // Required by the AgentRuntime contract: the substrate's stop()
                                     // signals `child.id()` as a process-*group* id, so every real
                                     // implementation (including test fixtures) must spawn into its
                                     // own group, exactly like `runtime::AcpRuntime` does.
            #[cfg(unix)]
            cmd.process_group(0);
            cmd.spawn()
                .map_err(|e| NodeError::Substrate(format!("spawn: {e}")))
        }
        async fn probe(&self, _agent: &PublicKey) -> Result<(), NodeError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn start_then_observe_running_then_stop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SleepRuntime),
            "wss://r".into(),
            dir.path().into(),
        );
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);

        sub.start(&d).await.expect("start");
        let obs = sub.observe().await;
        assert_eq!(obs.get(&d.agent_pubkey), Some(&Observed::Running));
        // workspace was created:
        assert!(workspace_dir(dir.path(), &d.agent_pubkey).is_dir());

        sub.stop(&d.agent_pubkey).await.expect("stop");
        let obs = sub.observe().await;
        assert_eq!(obs.get(&d.agent_pubkey), Some(&Observed::Stopped));
    }

    #[tokio::test]
    async fn start_persists_a_pid_record_and_stop_clears_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SleepRuntime),
            "wss://r".into(),
            dir.path().into(),
        );
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);
        let pid_path = pid_file_path(dir.path(), &d.agent_pubkey);

        sub.start(&d).await.expect("start");
        let recorded: u32 = std::fs::read_to_string(&pid_path)
            .expect("pid record written on start")
            .trim()
            .parse()
            .expect("pid record holds a valid pid");
        assert!(
            pid_is_alive(recorded),
            "recorded pid must be the live child"
        );

        sub.stop(&d.agent_pubkey).await.expect("stop");
        assert!(
            !pid_path.exists(),
            "stop() must clear the pid record so a later restart can't misadopt a recycled pid"
        );
    }

    // --- Process adoption across a simulated restart (spec §13 I3/I4) ---

    /// Build a runtime factory (fresh `Arc` per call, since
    /// `LocalProcessSubstrate::new` takes ownership) using the REAL
    /// production [`crate::runtime::AcpRuntime`] pointed at a harmless
    /// `sleep` instead of `buzz-acp` — not a bespoke test fixture — so the
    /// adoption test below actually exercises `AcpRuntime`'s real spawn
    /// behavior (notably: no `kill_on_drop`). Review round 1 caught exactly
    /// the gap a fixture would paper over: an earlier version of this test
    /// used its own fixture that already omitted `kill_on_drop`, which
    /// proved adoption's OWN logic worked but never actually verified that
    /// `AcpRuntime` itself left the process alive across the drop --
    /// `AcpRuntime` still had `kill_on_drop(true)` at the time and would
    /// have killed it.
    fn real_agent_runtime() -> std::sync::Arc<dyn crate::runtime::AgentRuntime> {
        std::sync::Arc::new(crate::runtime::AcpRuntime {
            harness_command: "/bin/sh".into(),
            harness_args: vec!["-c".into(), "sleep 30".into()],
            node_env: std::collections::BTreeMap::new(),
        })
    }

    /// Poll `pid_is_alive` across a bounded window instead of checking once:
    /// signal delivery (and, before Batch C1 review round 1's fix, a real
    /// `kill_on_drop(true)`) isn't necessarily synchronous with the
    /// triggering event returning, so a single point-in-time check right
    /// after a drop/signal can observe "alive" even when the process is a
    /// few microseconds from dying (confirmed empirically while writing
    /// `runtime::tests::spawned_child_survives_being_dropped_so_a_graceful_shutdown_never_kills_it`).
    /// Panics with `msg` the first time `pid` is observed dead within the
    /// window; returns normally once the window elapses with it alive the
    /// whole time.
    async fn assert_stays_alive_for(pid: u32, window: Duration, msg: &str) {
        let deadline = Instant::now() + window;
        loop {
            assert!(pid_is_alive(pid), "{msg}");
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The core property this batch closes: a still-running process from a
    /// PRIOR incarnation of this substrate (over the same root dir) is
    /// discovered and adopted by a FRESH `LocalProcessSubstrate::new()` —
    /// so `observe()` reports it `Running`, and a subsequent `reconcile`
    /// against that observation yields `Noop`, never a duplicate `Start`.
    /// Also proves `stop()` can terminate an adopted process (no `Child`
    /// handle) via `terminate_adopted`, cleaning up the real `sleep` this
    /// test spawned. Uses the real `AcpRuntime` (see `real_agent_runtime`),
    /// not a fixture, so it actually exercises the whole
    /// survive-a-graceful-drop -> adopt -> Noop chain end to end.
    #[cfg(unix)]
    #[tokio::test]
    async fn adoption_discovers_a_still_running_process_after_a_simulated_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);

        let pid = {
            // "Incarnation 1": spawn the agent, then drop the substrate --
            // models `daemon::run_until_shutdown`'s `tokio::select!`
            // dropping the losing `engine` future (and therefore this
            // substrate) when the shutdown branch wins a graceful
            // `buzz-node stop`/ctrl-c, without ever calling `stop()`.
            let sub = LocalProcessSubstrate::new(
                real_agent_runtime(),
                "wss://r".into(),
                dir.path().into(),
            );
            sub.start(&d).await.expect("start");
            let recorded: u32 = std::fs::read_to_string(pid_file_path(dir.path(), &d.agent_pubkey))
                .expect("pid file written")
                .trim()
                .parse()
                .expect("pid file holds a valid pid");
            assert!(
                pid_is_alive(recorded),
                "sanity: the freshly spawned pid must be alive"
            );
            recorded
            // `sub` (and its `AgentSlot::Live`'s real `AcpRuntime`-spawned
            // `Child`) drops here.
        };

        // The property review round 1 found broken: a graceful shutdown
        // (substrate drop) must NOT kill the agent. Polled, not a single
        // check -- see `assert_stays_alive_for`'s doc comment.
        assert_stays_alive_for(
            pid,
            Duration::from_millis(300),
            "a graceful daemon shutdown (substrate drop) must NOT kill the agent process",
        )
        .await;

        // "Incarnation 2": a fresh substrate over the SAME root dir, exactly
        // as a restarted daemon would construct one.
        let sub2 =
            LocalProcessSubstrate::new(real_agent_runtime(), "wss://r".into(), dir.path().into());
        let observed = sub2.observe().await;
        assert_eq!(
            observed.get(&d.agent_pubkey),
            Some(&Observed::Running),
            "a still-alive process from a prior incarnation must be adopted as Running"
        );

        // The no-dup-spawn property itself: reconciling `d` (still desired
        // Assigned) against this observation must NOT emit a Start/Restart
        // for an agent that's already (adopted-)Running.
        let actions = crate::reconcile::reconcile(std::slice::from_ref(&d), &observed);
        assert_eq!(
            actions,
            vec![crate::model::Action::Noop(d.agent_pubkey)],
            "an adopted, still-running agent must reconcile to Noop, never a duplicate spawn"
        );

        // Cleanup + a second property: stop() must actually be able to
        // terminate an Adopted slot (no Child handle), not just a Live one.
        sub2.stop(&d.agent_pubkey).await.expect("stop adopted");
        assert!(
            !pid_is_alive(pid),
            "stop() must terminate an adopted process, not just forget it"
        );
    }

    /// The complementary property: a PID record naming a pid nothing runs
    /// as (the agent genuinely didn't survive) must NOT be adopted, and
    /// must be cleaned up so it can never be misread by a later scan.
    #[cfg(unix)]
    #[tokio::test]
    async fn adoption_ignores_and_cleans_up_a_stale_dead_pid_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let agent = Keys::generate().public_key();
        let pid_path = pid_file_path(dir.path(), &agent);
        std::fs::create_dir_all(pid_path.parent().expect("has parent")).expect("create agent dir");
        // A pid essentially guaranteed to name no live process (mirrors
        // `daemon::singleton::tests`' identical stale-pid sentinel).
        std::fs::write(&pid_path, "999999999").expect("write stale pid");

        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SleepRuntime),
            "wss://r".into(),
            dir.path().into(),
        );

        assert!(
            !sub.observe().await.contains_key(&agent),
            "a dead pid record must not be adopted -- the agent stays Absent"
        );
        assert!(
            !pid_path.exists(),
            "a stale dead-pid record must be cleaned up during the adoption scan"
        );
    }

    /// Batch C1 review round 1 IMPORTANT finding: `pid_is_alive` alone
    /// (`kill(pid, 0)` existence) doesn't prove a recorded pid is actually
    /// OUR agent -- a pid-reuse collision with SOME unrelated but currently
    /// alive process would pass it too. Every genuine agent is spawned into
    /// its own process group (`AgentRuntime::spawn`'s contract), so a real
    /// adoption target always satisfies `pid == its own pgid`. Spawns a
    /// real, currently-alive process WITHOUT giving it its own group
    /// (unlike every genuine agent spawn) to model that collision, and
    /// proves it is rejected -- not adopted, and the stale record cleaned
    /// up -- exactly like a dead pid would be.
    #[cfg(unix)]
    #[tokio::test]
    async fn adoption_rejects_a_live_pid_that_is_not_its_own_process_group_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let agent = Keys::generate().public_key();

        let mut impostor = tokio::process::Command::new("/bin/sh")
            .args(["-c", "sleep 5"])
            .kill_on_drop(true)
            .spawn()
            .expect("spawn impostor");
        let pid = impostor.id().expect("pid");

        let pid_path = pid_file_path(dir.path(), &agent);
        std::fs::create_dir_all(pid_path.parent().expect("has parent")).expect("create agent dir");
        std::fs::write(&pid_path, pid.to_string()).expect("write pid record");

        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SleepRuntime),
            "wss://r".into(),
            dir.path().into(),
        );

        assert!(
            !sub.observe().await.contains_key(&agent),
            "a live pid that is not its own process-group leader must not be adopted"
        );
        assert!(
            !pid_path.exists(),
            "the rejected record must be cleaned up like a dead one"
        );

        impostor.start_kill().ok();
        let _ = impostor.wait().await;
    }

    /// The THIRD `is_own_group_leader` call site (Phase 5 batch C2 fold-in),
    /// proven in isolation like the other two: `observe()` must
    /// re-corroborate group leadership on every poll of an `Adopted` slot,
    /// not just once at `adopt_existing` time. Simulates the scenario that
    /// makes this matter -- an agent crashing while the daemon stays up
    /// (never going through `stop()`, so the slot is never cleared) and the
    /// OS later reusing its pid for an unrelated, non-group-leader process
    /// -- by seeding an `Adopted` slot directly rather than waiting on a
    /// real pid-recycle race (not reproducible on demand).
    #[cfg(unix)]
    #[tokio::test]
    async fn observe_rejects_an_adopted_pid_reused_by_a_non_leader_impostor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let agent = Keys::generate().public_key();

        let mut impostor = tokio::process::Command::new("/bin/sh")
            .args(["-c", "sleep 5"])
            .kill_on_drop(true)
            .spawn()
            .expect("spawn impostor");
        let pid = impostor.id().expect("pid");

        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SleepRuntime),
            "wss://r".into(),
            dir.path().into(),
        );
        // As if a previous `observe()` had legitimately adopted this pid
        // (it was alive and its own group leader then) and the underlying
        // process has since been replaced by the impostor above.
        sub.table.lock().unwrap().insert(
            agent,
            AgentSlot::Adopted {
                pid,
                reported_crash: false,
            },
        );

        assert_eq!(
            sub.observe().await.get(&agent),
            Some(&Observed::Crashed { code: None }),
            "a pid reused by a non-group-leader impostor must be observed as crashed, \
             never latched Running just because pid_is_alive alone still says yes"
        );

        impostor.start_kill().ok();
        let _ = impostor.wait().await;
    }

    /// The defensive re-check inside `terminate_adopted` itself, proven in
    /// isolation: even if something bypassed `adopt_existing`'s own gate,
    /// `terminate_adopted` must never `killpg` a pid that isn't its own
    /// process-group leader. Polls across a window (see
    /// `assert_stays_alive_for`) rather than a single check, so this also
    /// catches the delayed-effect case, not just an immediate one.
    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_adopted_refuses_to_signal_a_non_group_leader_pid() {
        let mut impostor = tokio::process::Command::new("/bin/sh")
            .args(["-c", "sleep 5"])
            .kill_on_drop(true)
            .spawn()
            .expect("spawn impostor");
        let pid = impostor.id().expect("pid");

        terminate_adopted(pid).await;

        assert_stays_alive_for(
            pid,
            Duration::from_millis(300),
            "terminate_adopted must refuse to killpg a pid that is not its own process-group leader",
        )
        .await;

        impostor.start_kill().ok();
        let _ = impostor.wait().await;
    }

    #[tokio::test]
    async fn stop_on_never_started_agent_leaves_a_stopped_tombstone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SleepRuntime),
            "wss://r".into(),
            dir.path().into(),
        );
        let agent = Keys::generate().public_key();
        sub.stop(&agent).await.expect("stop never-started agent");
        assert_eq!(sub.observe().await.get(&agent), Some(&Observed::Stopped));
    }

    /// Spawns a shell that forks a real *descendant* (`sleep 30 &`, then
    /// `wait`s on it) instead of a single leaf process, so tests can prove
    /// `stop()`'s `killpg` reaps the whole process tree — not just the one
    /// direct child the substrate's table tracks. Records the spawned pid
    /// (== pgid, since it's spawned into its own group) so the test can
    /// independently check the group afterward.
    #[derive(Default)]
    struct ForkingRuntime {
        last_pid: std::sync::atomic::AtomicU32,
    }
    #[async_trait::async_trait]
    impl crate::runtime::AgentRuntime for ForkingRuntime {
        async fn spawn(
            &self,
            _desired: &DesiredAgent,
            workspace: &std::path::Path,
            _relay_url: &str,
        ) -> Result<tokio::process::Child, NodeError> {
            let mut cmd = tokio::process::Command::new("/bin/sh");
            cmd.args(["-c", "sleep 30 & wait"])
                .current_dir(workspace)
                .kill_on_drop(true);
            #[cfg(unix)]
            cmd.process_group(0);
            let child = cmd
                .spawn()
                .map_err(|e| NodeError::Substrate(format!("spawn: {e}")))?;
            if let Some(pid) = child.id() {
                self.last_pid
                    .store(pid, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(child)
        }
        async fn probe(&self, _agent: &PublicKey) -> Result<(), NodeError> {
            Ok(())
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stop_kills_the_whole_process_group_not_just_the_leaf() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = std::sync::Arc::new(ForkingRuntime::default());
        let sub = LocalProcessSubstrate::new(runtime.clone(), "wss://r".into(), dir.path().into());
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);

        sub.start(&d).await.expect("start");
        // Give the shell a moment to fork+background the `sleep` descendant
        // before we act on it.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            sub.observe().await.get(&d.agent_pubkey),
            Some(&Observed::Running)
        );
        let pgid = runtime.last_pid.load(std::sync::atomic::Ordering::SeqCst);
        assert_ne!(pgid, 0, "fixture must have recorded a pid");

        sub.stop(&d.agent_pubkey).await.expect("stop");

        // The whole process group — `sh` AND the `sleep` descendant it
        // forked — must be gone, not just the direct child the substrate
        // tracked. `killpg(pgid, None)` sends signal 0 (existence check,
        // no actual signal); ESRCH means no process in that group exists.
        // Poll briefly: a descendant that dies from the same SIGTERM as its
        // parent can take a moment to be reaped by the kernel/init.
        use nix::errno::Errno;
        use nix::sys::signal::killpg;
        use nix::unistd::Pid;
        let mut group_empty = false;
        for _ in 0..20 {
            if killpg(Pid::from_raw(pgid as i32), None) == Err(Errno::ESRCH) {
                group_empty = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            group_empty,
            "process group {pgid} still has a member after stop()"
        );
    }

    /// Fails `spawn()` for one specific agent pubkey (simulating e.g. a
    /// missing binary) and spawns a harmless `sleep` for anyone else, so a
    /// single substrate can host both a permanently-broken agent and a
    /// healthy one in the same test.
    struct SelectiveFailRuntime {
        fail_for: PublicKey,
    }
    #[async_trait::async_trait]
    impl crate::runtime::AgentRuntime for SelectiveFailRuntime {
        async fn spawn(
            &self,
            desired: &DesiredAgent,
            workspace: &std::path::Path,
            _relay_url: &str,
        ) -> Result<tokio::process::Child, NodeError> {
            if desired.agent_pubkey == self.fail_for {
                return Err(NodeError::Spawn {
                    agent: desired.agent_pubkey.to_hex(),
                    reason: "simulated: binary not found".into(),
                });
            }
            let mut cmd = tokio::process::Command::new("/bin/sh");
            cmd.args(["-c", "sleep 5"])
                .current_dir(workspace)
                .kill_on_drop(true);
            #[cfg(unix)]
            cmd.process_group(0);
            cmd.spawn()
                .map_err(|e| NodeError::Substrate(format!("spawn: {e}")))
        }
        async fn probe(&self, _agent: &PublicKey) -> Result<(), NodeError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn start_contains_a_spawn_failure_and_other_agents_still_start() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (bad_agent, bad_node, bad_owner) =
            (Keys::generate(), Keys::generate(), Keys::generate());
        let bad = fake_desired(
            &bad_agent,
            &bad_node,
            &bad_owner,
            buzz_core::AssignState::Assigned,
        );
        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SelectiveFailRuntime {
                fail_for: bad.agent_pubkey,
            }),
            "wss://r".into(),
            dir.path().into(),
        );

        // The failing agent's start() must NOT propagate an error — a
        // single broken agent must never tear down the whole node's
        // reconcile loop (engine.rs's run() does `substrate.start(&d).await?`
        // for every agent in the same pass).
        sub.start(&bad)
            .await
            .expect("start() must contain the per-agent spawn failure");
        // ...and it must be observed as Crashed (so reconcile retries it,
        // subject to the breaker), not silently vanish from the table.
        assert!(matches!(
            sub.observe().await.get(&bad.agent_pubkey),
            Some(Observed::Crashed { code: None })
        ));

        // A second, healthy agent must still be able to start on the SAME
        // substrate — the first agent's failure must not have wedged
        // anything shared (table/breaker locks, etc).
        let (good_agent, good_node, good_owner) =
            (Keys::generate(), Keys::generate(), Keys::generate());
        let good = fake_desired(
            &good_agent,
            &good_node,
            &good_owner,
            buzz_core::AssignState::Assigned,
        );
        sub.start(&good)
            .await
            .expect("a healthy agent must still start");
        assert_eq!(
            sub.observe().await.get(&good.agent_pubkey),
            Some(&Observed::Running)
        );
        sub.stop(&good.agent_pubkey).await.expect("cleanup");
    }

    #[tokio::test]
    async fn start_contains_a_workspace_creation_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);

        // Pre-create a plain FILE at the exact path `workspace_dir` needs to
        // create as a DIRECTORY, so `create_dir_all` fails deterministically
        // — unlike a permissions-based failure, this doesn't depend on the
        // test not running as root (common in CI containers, where root
        // bypasses permission checks entirely).
        let agent_dir = dir.path().join("agents").join(d.agent_pubkey.to_hex());
        std::fs::create_dir_all(&agent_dir).expect("create parent");
        std::fs::write(agent_dir.join("workspace"), b"not a directory")
            .expect("seed conflicting file");

        let sub = LocalProcessSubstrate::new(
            std::sync::Arc::new(SleepRuntime),
            "wss://r".into(),
            dir.path().into(),
        );
        sub.start(&d)
            .await
            .expect("start() must contain a workspace-creation failure");
        assert!(matches!(
            sub.observe().await.get(&d.agent_pubkey),
            Some(Observed::Crashed { code: None })
        ));
    }

    #[derive(Default)]
    struct DieRuntime {
        /// Counts actual spawn attempts, so the breaker test can prove a
        /// skipped `start()` really didn't spawn — not just that
        /// `breaker_open()` reports true.
        spawns: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl crate::runtime::AgentRuntime for DieRuntime {
        async fn spawn(
            &self,
            _desired: &DesiredAgent,
            workspace: &std::path::Path,
            _relay_url: &str,
        ) -> Result<tokio::process::Child, NodeError> {
            self.spawns
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut cmd = tokio::process::Command::new("/bin/sh");
            cmd.args(["-c", "exit 1"])
                .current_dir(workspace)
                .kill_on_drop(true);
            #[cfg(unix)]
            cmd.process_group(0);
            cmd.spawn()
                .map_err(|e| NodeError::Substrate(format!("spawn: {e}")))
        }
        async fn probe(&self, _agent: &PublicKey) -> Result<(), NodeError> {
            Ok(())
        }
    }

    /// Records every `probe()` call and can be scripted to fail — proves
    /// `LocalProcessSubstrate::probe` genuinely delegates to the underlying
    /// `AgentRuntime::probe` rather than being a no-op stub.
    #[derive(Default)]
    struct ProbeRuntime {
        probes: std::sync::Mutex<Vec<PublicKey>>,
        fail: std::sync::atomic::AtomicBool,
    }
    #[async_trait::async_trait]
    impl crate::runtime::AgentRuntime for ProbeRuntime {
        async fn spawn(
            &self,
            _desired: &DesiredAgent,
            workspace: &std::path::Path,
            _relay_url: &str,
        ) -> Result<tokio::process::Child, NodeError> {
            let mut cmd = tokio::process::Command::new("/bin/sh");
            cmd.args(["-c", "sleep 5"])
                .current_dir(workspace)
                .kill_on_drop(true);
            #[cfg(unix)]
            cmd.process_group(0);
            cmd.spawn()
                .map_err(|e| NodeError::Substrate(format!("spawn: {e}")))
        }
        async fn probe(&self, agent: &PublicKey) -> Result<(), NodeError> {
            self.probes.lock().expect("lock").push(*agent);
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                Err(NodeError::Substrate("simulated probe failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn substrate_probe_delegates_to_the_agent_runtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = std::sync::Arc::new(ProbeRuntime::default());
        let sub = LocalProcessSubstrate::new(runtime.clone(), "wss://r".into(), dir.path().into());
        let agent = Keys::generate().public_key();

        sub.probe(&agent).await.expect("probe ok");
        assert_eq!(*runtime.probes.lock().unwrap(), vec![agent]);

        runtime
            .fail
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            sub.probe(&agent).await.is_err(),
            "a probe failure must propagate through the substrate"
        );
    }

    #[tokio::test]
    async fn crash_loop_opens_breaker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = std::sync::Arc::new(DieRuntime::default());
        let sub = LocalProcessSubstrate::new(runtime.clone(), "wss://r".into(), dir.path().into());
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, buzz_core::AssignState::Assigned);

        for _ in 0..4 {
            let _ = sub.start(&d).await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = sub.observe().await;
        }
        assert!(sub.breaker_open(&d.agent_pubkey));
        // 3 crashes trip the breaker (BREAKER_THRESHOLD); the 4th start()
        // must have been refused rather than spawning a 4th process.
        assert_eq!(
            runtime.spawns.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "start() must not spawn while the breaker is open"
        );
    }

    /// Batch B review finding: `Circuit::is_open` performs a one-time,
    /// consuming open→half-open transition once the cooldown has elapsed
    /// (clearing `open_until` and pre-seeding `crash_times`) — meant to be
    /// triggered only by an actual start attempt. `is_open_peek` must report
    /// the same answer without ever performing that transition, so a
    /// read-only caller (health reporting) can't silently eat the
    /// allowance a real `start()` was supposed to consume.
    #[test]
    fn circuit_peek_does_not_consume_the_half_open_transition() {
        let mut c = Circuit {
            // An already-expired cooldown, as if BREAKER_COOLDOWN had
            // elapsed in the past.
            open_until: Some(Instant::now() - Duration::from_millis(1)),
            crash_times: Vec::new(),
        };

        assert!(
            !c.is_open_peek(),
            "peek must see an expired cooldown as no longer open"
        );
        assert!(
            !c.is_open_peek(),
            "peek must be repeatable without side effects"
        );
        // `open_until` must be untouched by the peeks above -- if the real
        // (consuming) check still performs its own one-time transition
        // afterward, peek could not have already consumed it.
        assert!(!c.is_open());
        assert_eq!(
            c.crash_times.len(),
            BREAKER_THRESHOLD - 1,
            "is_open()'s own half-open transition must still fire after any number of peeks"
        );
    }
}
