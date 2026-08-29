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

    /// True if `agent`'s crash-restart breaker currently forbids a start.
    /// Consulted by [`Substrate::start`] before spawning.
    pub fn breaker_open(&self, agent: &PublicKey) -> bool {
        self.breaker
            .lock()
            .expect("breaker lock")
            .entry(*agent)
            .or_default()
            .is_open()
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
            let mut table = self.table.lock().expect("table lock");
            for (pk, slot) in table.iter_mut() {
                let observed = match slot {
                    AgentSlot::Stopped => Observed::Stopped,
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
            let mut breaker = self.breaker.lock().expect("breaker lock");
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
        let ws = workspace_dir(&self.root, &desired.agent_pubkey);
        std::fs::create_dir_all(&ws)
            .map_err(|e| NodeError::Substrate(format!("create workspace dir: {e}")))?;
        let child = self.runtime.spawn(desired, &ws, &self.relay_url).await?;
        let pid = child.id();
        self.table.lock().expect("table lock").insert(
            desired.agent_pubkey,
            AgentSlot::Live {
                child,
                pid,
                reported_crash: false,
            },
        );
        Ok(())
    }

    async fn stop(&self, agent: &PublicKey) -> Result<(), NodeError> {
        let removed = self.table.lock().expect("table lock").remove(agent);
        if let Some(AgentSlot::Live { mut child, pid, .. }) = removed {
            kill_group(pid, &mut child).await;
        }
        self.table
            .lock()
            .expect("table lock")
            .insert(*agent, AgentSlot::Stopped);
        Ok(())
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
        self.inner
            .lock()
            .expect("lock")
            .insert(*agent, Observed::Stopped);
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
