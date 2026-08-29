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
    /// failure. Consulted by [`crate::health::classify`] so a breaker-open
    /// agent reports `Stopped`/`"breaker-open"` rather than `Crashed` (spec
    /// §9; carried Batch A/B review finding).
    fn breaker_open(&self, agent: &PublicKey) -> bool;
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
    pub fn new(
        runtime: std::sync::Arc<dyn AgentRuntime>,
        relay_url: String,
        root: PathBuf,
    ) -> Self {
        Self {
            runtime,
            relay_url,
            root,
            table: std::sync::Mutex::new(BTreeMap::new()),
            breaker: std::sync::Mutex::new(BTreeMap::new()),
        }
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
}

/// The persistent workspace directory for `agent` under a substrate rooted
/// at `root`: `<root>/agents/<agent-hex>/workspace`.
pub fn workspace_dir(root: &Path, agent: &PublicKey) -> PathBuf {
    root.join("agents").join(agent.to_hex()).join("workspace")
}

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
        if let Some(AgentSlot::Live { mut child, pid, .. }) = removed {
            kill_group(pid, &mut child).await;
        }
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
}
