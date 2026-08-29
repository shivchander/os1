//! `buzz-node` process/CLI orchestration: subcommands, the detached-daemon
//! lifecycle (spawn, PID/status singleton guard, graceful shutdown), and
//! opt-in OS autostart registration.
//!
//! This module is part of the `buzz-node` *binary* crate (declared via `mod
//! daemon;` in `main.rs`), not the `buzz-node` *library* crate (`src/lib.rs`
//! — `engine`/`enroll`/`model`/`nostr_relay`/`reconcile`/`relay`/`runtime`/
//! `substrate`). It wires those reusable, embeddable pieces into a runnable
//! OS process, which is inherently binary/platform-specific and has no
//! reason to be part of the library's public API.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use buzz_node::engine;
use buzz_node::enroll::{self, KeychainStore, NodeConfig};
use buzz_node::model::NodeError;
use buzz_node::nostr_relay::NostrNodeRelay;
use buzz_node::relay::NodeRelay;
use buzz_node::runtime::AcpRuntime;
use buzz_node::substrate::{LocalProcessSubstrate, Substrate};
use clap::{Parser, Subcommand};
use nostr::PublicKey;
use serde::{Deserialize, Serialize};

/// How often the engine reconciles even without a fresh assignment (self-heal
/// cadence for `up`'s live daemon).
const RECONCILE_TICK: Duration = Duration::from_secs(5);
/// How often `up --foreground` refreshes `daemon.status.json`.
const STATUS_WRITE_INTERVAL: Duration = Duration::from_secs(5);
/// How stale a `daemon.status.json` heartbeat can be before the two-source
/// singleton guard stops trusting it as evidence the daemon is still alive.
const STATUS_FRESHNESS: Duration = Duration::from_secs(30);
/// Upper bound on the final, awaited offline-presence publish during
/// shutdown. [`NostrNodeRelay::publish_presence_awaited`]'s underlying
/// reconnect-forever policy is correct for a long-lived background publish
/// but must not hang process exit indefinitely against a relay that never
/// comes back.
const FINAL_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

// ─── CLI ────────────────────────────────────────────────────────────────

/// `buzz-node` — a persistent execution-node daemon that hosts Buzz agents.
#[derive(Debug, Parser)]
#[command(name = "buzz-node", about = "Buzz execution-node daemon")]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

/// `buzz-node` subcommands. Bare invocation (no subcommand) behaves like
/// [`Command::Status`].
#[derive(Debug, Subcommand)]
enum Command {
    /// Start the node daemon. By default this spawns a detached background
    /// process and returns immediately; the detached process re-execs into
    /// `up --foreground` internally.
    Up {
        /// Run inline instead of spawning a detached background process.
        #[arg(long)]
        foreground: bool,
    },
    /// Enroll this node with an owner: announce, print a pairing code, wait
    /// for the owner's approval, and persist the resulting node config.
    Enroll {
        /// Relay to enroll against. Falls back to `BUZZ_RELAY_URL`.
        #[arg(long)]
        relay_url: Option<String>,
    },
    /// Register this node to start automatically at login (opt-in;
    /// never triggered as a side effect of `up`/`enroll`).
    Autostart,
    /// Print the daemon's current status as JSON.
    Status,
    /// Signal a running detached daemon to shut down gracefully.
    Stop,
}

/// Parse and run a CLI invocation, returning the process exit code.
pub(crate) async fn dispatch(cli: Cli) -> i32 {
    let result = match cli.command.unwrap_or(Command::Status) {
        Command::Up { foreground } => cmd_up(foreground).await,
        Command::Enroll { relay_url } => cmd_enroll(relay_url).await,
        Command::Autostart => cmd_autostart(),
        Command::Status => cmd_status(),
        Command::Stop => cmd_stop(),
    };
    match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("buzz-node: {e}");
            1
        }
    }
}

// ─── Daemon paths + status file ────────────────────────────────────────

/// Files a running daemon reads/writes under its home directory
/// ([`enroll::node_home_dir`]).
struct DaemonPaths {
    pid_file: PathBuf,
    status_file: PathBuf,
    log_file: PathBuf,
}

impl DaemonPaths {
    fn under(dir: &Path) -> Self {
        Self {
            pid_file: dir.join("daemon.pid"),
            status_file: dir.join("daemon.status.json"),
            log_file: dir.join("daemon.log"),
        }
    }

    fn default_paths() -> Result<Self, NodeError> {
        Ok(Self::under(&enroll::node_home_dir()?))
    }
}

/// Heartbeat written to `daemon.status.json` roughly every
/// [`STATUS_WRITE_INTERVAL`] while the daemon is up. Read by
/// [`running_daemon`] (freshness check) and `buzz-node status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DaemonStatus {
    pid: u32,
    node_pubkey: String,
    owner_pubkey: String,
    relay_url: String,
    started_at: String,
    updated_at: String,
}

// ─── Singleton guard (two-source: live PID + fresh status heartbeat) ──

/// Read a pid from `pid_file` and return it iff a process with that pid is
/// currently alive. `None` for a missing/unparsable file or a pid nothing is
/// running as (a stale file left by a daemon that died without cleaning up
/// after itself).
fn live_daemon_pid(pid_file: &Path) -> Option<u32> {
    let text = std::fs::read_to_string(pid_file).ok()?;
    let pid: u32 = text.trim().parse().ok()?;
    pid_is_alive(pid).then_some(pid)
}

#[cfg(unix)]
fn pid_is_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    // Signal 0: existence/permission check only — no signal is delivered.
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

#[cfg(windows)]
fn pid_is_alive(pid: u32) -> bool {
    // No unsafe FFI (`OpenProcess`) allowed here (`#![deny(unsafe_code)]`),
    // so shell out exactly like `substrate::kill_group`'s Windows arm does.
    std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).contains(&pid.to_string()))
        .unwrap_or(false)
}

#[cfg(not(any(unix, windows)))]
fn pid_is_alive(_pid: u32) -> bool {
    // Can't verify liveness on this target; fail closed (assume alive) so
    // the singleton guard never double-spawns rather than silently allowing
    // a second daemon.
    true
}

/// True if `status_file`'s mtime is within [`STATUS_FRESHNESS`] of now.
fn status_is_fresh(status_file: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(status_file) else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    // `duration_since` errs on clock skew (`modified` in the future) — that
    // is treated as NOT fresh (fail closed) rather than panicking.
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age < STATUS_FRESHNESS)
        .unwrap_or(false)
}

/// The full two-source singleton guard: a daemon is considered "running"
/// only when BOTH its PID file names a live process AND that process's
/// status heartbeat is recent. Neither source alone is reliable: a bare live
/// pid can be an unrelated process that reused a stale pid (pids get
/// recycled across a reboot or long uptime), and a bare fresh status file
/// can't prove the pid recorded in it is still that same process. Mirrors
/// the OpenAgents launcher's two-source daemon guard.
fn running_daemon(paths: &DaemonPaths) -> Option<u32> {
    let pid = live_daemon_pid(&paths.pid_file)?;
    status_is_fresh(&paths.status_file).then_some(pid)
}

// ─── Detached spawn ─────────────────────────────────────────────────────

/// Re-exec this same binary as `buzz-node up --foreground`, detached from
/// the launching terminal: stdin is `/dev/null`, stdout/stderr go to the
/// daemon's log file, and (Unix/Windows) the child is placed in its own
/// process group via [`detach`] so job-control signals aimed at the
/// launching shell/console don't reach it. Does not wait for the child —
/// returning immediately, before the child even finishes its own startup, is
/// what makes `buzz-node up` return promptly while the daemon keeps running.
///
/// This does not call `setsid(2)`: full session detachment needs an
/// `unsafe` `pre_exec` hook, which `#![deny(unsafe_code)]` rules out. This is
/// a documented, accepted v1 limitation (mirroring `enroll::accept_enrollment`'s
/// TOFU caveat): the immediate parent (`buzz-node up`) exits right after
/// spawning, and SIGHUP on terminal close targets the terminal's foreground
/// process group, which this child has already left by joining a new one.
fn spawn_detached(paths: &DaemonPaths) -> Result<u32, NodeError> {
    let exe = std::env::current_exe()
        .map_err(|e| NodeError::Config(format!("resolve current exe: {e}")))?;
    let log_out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_file)
        .map_err(|e| NodeError::Config(format!("open log file: {e}")))?;
    let log_err = log_out
        .try_clone()
        .map_err(|e| NodeError::Config(format!("dup log file handle: {e}")))?;

    let mut cmd = std::process::Command::new(exe);
    cmd.arg("up")
        .arg("--foreground")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(log_out))
        .stderr(std::process::Stdio::from(log_err));
    detach(&mut cmd);

    let child = cmd
        .spawn()
        .map_err(|e| NodeError::Config(format!("spawn detached daemon: {e}")))?;
    let pid = child.id();
    // Deliberately not kept: `std::process::Child` never kills its child on
    // `Drop` (unlike `tokio::process::Child` with `kill_on_drop(true)`), so
    // letting `child` fall out of scope here is exactly the "unref" step —
    // the OS keeps running it after this short-lived launcher exits.
    std::fs::write(&paths.pid_file, pid.to_string())
        .map_err(|e| NodeError::Config(format!("write pid file: {e}")))?;
    Ok(pid)
}

#[cfg(unix)]
fn detach(cmd: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    cmd.process_group(0);
}

#[cfg(windows)]
fn detach(cmd: &mut std::process::Command) {
    use std::os::windows::process::CommandExt;
    // From `winbase.h`: don't inherit the parent's console.
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    // From `winbase.h`: start a new process group so console Ctrl events
    // aimed at the launching console don't reach the child.
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
fn detach(_cmd: &mut std::process::Command) {}

// ─── Graceful shutdown ──────────────────────────────────────────────────

/// The real process shutdown signal: SIGINT (ctrl-c) or, on Unix, SIGTERM —
/// whichever arrives first. Nothing else ends `up --foreground` under normal
/// operation: `engine::run`'s [`NodeRelay`] never self-emits a shutdown (see
/// [`NostrNodeRelay::next_desired`]'s doc comment) — it is a live
/// subscription that runs forever absent a process-level signal.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to install SIGTERM handler; ctrl-c only");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Race `engine` against `shutdown`; either way, run `final_publish` —
/// bounded by [`FINAL_PUBLISH_TIMEOUT`] — before returning.
///
/// This is the crux of the daemon's graceful-shutdown guarantee, kept
/// generic over the engine future and shutdown source specifically so it can
/// be unit tested (see `tests::run_until_shutdown_*`) without any real relay
/// or substrate — the interleaving is what matters, not the concrete I/O.
/// `final_publish` always runs, regardless of *why* the race ended
/// (shutdown signal, the engine returning `Ok`, or the engine returning
/// `Err`): the "clean shutdown ⇒ offline presence delivered" guarantee must
/// hold on every exit path, not just the happy one. Against a real
/// [`NostrNodeRelay`], `final_publish` should be
/// [`NostrNodeRelay::publish_presence_awaited`] — unlike `engine::run`'s own
/// end-of-loop `publish_presence(false)` (fire-and-forget; see
/// `NostrNodeRelay::spawn_publish`), this variant blocks until the relay
/// actually accepts the event, so it isn't dropped when the process exits
/// right behind it.
async fn run_until_shutdown(
    engine: impl std::future::Future<Output = Result<(), NodeError>>,
    shutdown: impl std::future::Future<Output = ()>,
    final_publish: impl std::future::Future<Output = Result<(), NodeError>>,
) -> Result<(), NodeError> {
    let engine_result = tokio::select! {
        result = engine => Some(result),
        () = shutdown => None,
    };

    match tokio::time::timeout(FINAL_PUBLISH_TIMEOUT, final_publish).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "final offline-presence publish failed"),
        Err(_) => tracing::warn!("final offline-presence publish timed out"),
    }

    match engine_result {
        Some(result) => result,
        // A shutdown-signal-driven exit is a normal, successful shutdown.
        None => Ok(()),
    }
}

// ─── `up` ───────────────────────────────────────────────────────────────

async fn cmd_up(foreground: bool) -> Result<(), NodeError> {
    let paths = DaemonPaths::default_paths()?;
    let dir = paths
        .pid_file
        .parent()
        .ok_or_else(|| NodeError::Config("pid file path has no parent directory".into()))?;
    std::fs::create_dir_all(dir)
        .map_err(|e| NodeError::Config(format!("create daemon dir: {e}")))?;

    // Checked unconditionally (not just on the detach path) so a
    // launchd/systemd-supervised `up --foreground` invocation is just as
    // protected against double-starting as a manually run `up`.
    if let Some(pid) = running_daemon(&paths) {
        eprintln!("buzz-node: already running (pid {pid})");
        return Ok(());
    }

    if !foreground {
        let pid = spawn_detached(&paths)?;
        eprintln!(
            "buzz-node: started in background (pid {pid}); logs: {}",
            paths.log_file.display()
        );
        return Ok(());
    }

    std::fs::write(&paths.pid_file, std::process::id().to_string())
        .map_err(|e| NodeError::Config(format!("write pid file: {e}")))?;
    let result = up_foreground(&paths).await;
    let _ = std::fs::remove_file(&paths.pid_file);
    result
}

/// Pure check factored out of [`up_foreground`] for testability: the
/// persisted [`NodeConfig`] must describe the same node the keychain
/// currently holds a key for. A mismatch means the keychain and config file
/// drifted apart (e.g. the keychain was reset, or a config from a different
/// machine was copied over) — proceeding anyway would run the engine under
/// the wrong identity.
fn ensure_identity_matches_config(
    node_pubkey_hex: &str,
    cfg: &NodeConfig,
) -> Result<(), NodeError> {
    if cfg.node_pubkey != node_pubkey_hex {
        return Err(NodeError::Config(format!(
            "persisted config's node_pubkey ({}) does not match this node's keychain identity ({node_pubkey_hex})",
            cfg.node_pubkey,
        )));
    }
    Ok(())
}

async fn up_foreground(paths: &DaemonPaths) -> Result<(), NodeError> {
    let node_keys = enroll::load_or_create_node_keys(&KeychainStore)?;
    let cfg = enroll::load_node_config()?.ok_or_else(|| {
        NodeError::Config(
            "no enrollment found; run `buzz-node enroll --relay-url <URL>` first".into(),
        )
    })?;
    ensure_identity_matches_config(&node_keys.public_key().to_hex(), &cfg)?;

    let owner_pubkey = PublicKey::from_hex(&cfg.owner_pubkey)
        .map_err(|e| NodeError::Config(format!("stored owner_pubkey is invalid: {e}")))?;

    let substrate: Arc<dyn Substrate> = Arc::new(LocalProcessSubstrate::new(
        Arc::new(AcpRuntime::default()),
        cfg.relay_url.clone(),
        cfg.workspace_root.clone(),
    ));
    let relay_for_engine: Box<dyn NodeRelay> = Box::new(NostrNodeRelay::new(
        cfg.relay_url.clone(),
        node_keys.clone(),
        owner_pubkey,
    ));
    // A second, independent connection used only for the final, AWAITED
    // offline-presence publish on shutdown — see `run_until_shutdown`'s doc
    // comment for why `engine::run`'s own fire-and-forget end-of-loop
    // publish can't be relied on for this.
    let shutdown_relay =
        NostrNodeRelay::new(cfg.relay_url.clone(), node_keys.clone(), owner_pubkey);

    let engine_cfg = engine::EngineConfig {
        reconcile_tick: RECONCILE_TICK,
        node_pubkey: node_keys.public_key(),
    };

    let status_task = tokio::spawn(status_writer_loop(
        paths.status_file.clone(),
        std::process::id(),
        cfg,
    ));

    let outcome = run_until_shutdown(
        engine::run(substrate, relay_for_engine, node_keys, engine_cfg),
        wait_for_shutdown_signal(),
        async move { shutdown_relay.publish_presence_awaited(false).await },
    )
    .await;

    status_task.abort();
    outcome
}

fn write_status(path: &Path, status: &DaemonStatus) -> Result<(), NodeError> {
    let json = serde_json::to_string_pretty(status)
        .map_err(|e| NodeError::Config(format!("serialize daemon status: {e}")))?;
    std::fs::write(path, json).map_err(|e| NodeError::Config(format!("write daemon status: {e}")))
}

/// Refresh `status_file` every [`STATUS_WRITE_INTERVAL`] for as long as this
/// task runs; the first write happens immediately (`tokio::time::interval`'s
/// first tick never waits), so [`running_daemon`]'s freshness check sees a
/// heartbeat right away rather than only after the first full interval.
async fn status_writer_loop(status_file: PathBuf, pid: u32, cfg: NodeConfig) {
    let started_at = chrono::Utc::now().to_rfc3339();
    let mut ticker = tokio::time::interval(STATUS_WRITE_INTERVAL);
    loop {
        ticker.tick().await;
        let status = DaemonStatus {
            pid,
            node_pubkey: cfg.node_pubkey.clone(),
            owner_pubkey: cfg.owner_pubkey.clone(),
            relay_url: cfg.relay_url.clone(),
            started_at: started_at.clone(),
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        if let Err(e) = write_status(&status_file, &status) {
            tracing::warn!(error = %e, "failed to write daemon status file");
        }
    }
}

// ─── `enroll` ───────────────────────────────────────────────────────────

async fn cmd_enroll(relay_url_flag: Option<String>) -> Result<(), NodeError> {
    let relay_url = relay_url_flag
        .or_else(|| std::env::var("BUZZ_RELAY_URL").ok())
        .ok_or_else(|| {
            NodeError::Config("pass --relay-url or set BUZZ_RELAY_URL to enroll".into())
        })?;
    let node_keys = enroll::load_or_create_node_keys(&KeychainStore)?;
    let workspace_root = enroll::node_home_dir()?;
    let caps = buzz_core::NodeCapabilities {
        format: buzz_core::node::FORMAT.into(),
        version: buzz_core::node::VERSION,
        node_pubkey: node_keys.public_key().to_hex(),
        os: std::env::consts::OS.to_string(),
        runtimes: vec!["acp".to_string()],
        workspace_root: workspace_root.to_string_lossy().into_owned(),
        max_agents: None,
    };
    let cfg = enroll::enroll(&relay_url, &node_keys, &caps).await?;
    eprintln!(
        "buzz-node: enrolled. node_pubkey={} owner_pubkey={}",
        cfg.node_pubkey, cfg.owner_pubkey
    );
    Ok(())
}

// ─── `status` ───────────────────────────────────────────────────────────

fn cmd_status() -> Result<(), NodeError> {
    let paths = DaemonPaths::default_paths()?;
    match running_daemon(&paths) {
        Some(pid) => match std::fs::read_to_string(&paths.status_file) {
            Ok(json) => {
                println!("{json}");
                Ok(())
            }
            Err(_) => {
                println!("{{\"running\": true, \"pid\": {pid}}}");
                Ok(())
            }
        },
        None => {
            println!("{{\"running\": false}}");
            Ok(())
        }
    }
}

// ─── `stop` ─────────────────────────────────────────────────────────────

fn cmd_stop() -> Result<(), NodeError> {
    let paths = DaemonPaths::default_paths()?;
    match live_daemon_pid(&paths.pid_file) {
        Some(pid) => {
            terminate(pid)?;
            eprintln!("buzz-node: sent shutdown signal to pid {pid}");
            Ok(())
        }
        None => {
            eprintln!("buzz-node: not running");
            Ok(())
        }
    }
}

#[cfg(unix)]
fn terminate(pid: u32) -> Result<(), NodeError> {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), Signal::SIGTERM)
        .map_err(|e| NodeError::Config(format!("signal pid {pid}: {e}")))
}

#[cfg(windows)]
fn terminate(pid: u32) -> Result<(), NodeError> {
    // Best-effort: Windows console processes have no direct SIGTERM
    // equivalent reachable without `unsafe` FFI. `taskkill` (without `/F`)
    // requests a graceful close; `up --foreground`'s own ctrl-c handling
    // (`wait_for_shutdown_signal`) is the primary graceful-shutdown path
    // when running attached to a console.
    std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string()])
        .status()
        .map_err(|e| NodeError::Config(format!("taskkill pid {pid}: {e}")))?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn terminate(_pid: u32) -> Result<(), NodeError> {
    Err(NodeError::Config(
        "stop is not supported on this platform".into(),
    ))
}

// ─── `autostart` ────────────────────────────────────────────────────────

/// Install an OS login item that runs `buzz-node up` at login, so the node
/// survives a reboot without the user remembering to start it by hand. Only
/// ever invoked from the explicit `buzz-node autostart` command — `up` and
/// `enroll` never register this as a side effect.
fn cmd_autostart() -> Result<(), NodeError> {
    let exe = std::env::current_exe()
        .map_err(|e| NodeError::Config(format!("resolve current exe: {e}")))?;
    let path = install_autostart(&exe)?;
    eprintln!("buzz-node: autostart registered at {}", path.display());
    Ok(())
}

fn install_autostart(exe: &Path) -> Result<PathBuf, NodeError> {
    autostart_impl::install(exe)
}

#[cfg(target_os = "macos")]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    const LABEL: &str = "com.buzz.node";

    fn agents_dir() -> Result<PathBuf, NodeError> {
        let home = dirs::home_dir().ok_or_else(|| NodeError::Config("no home directory".into()))?;
        Ok(home.join("Library").join("LaunchAgents"))
    }

    fn plist_contents(exe: &Path) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
<dict>\n\
    <key>Label</key><string>{LABEL}</string>\n\
    <key>ProgramArguments</key>\n\
    <array>\n\
        <string>{exe}</string>\n\
        <string>up</string>\n\
    </array>\n\
    <key>RunAtLoad</key><true/>\n\
    <key>KeepAlive</key><false/>\n\
</dict>\n\
</plist>\n",
            exe = exe.display(),
        )
    }

    /// The pure, testable half: write the plist to an arbitrary `dir`.
    pub(super) fn install_to(dir: &Path, exe: &Path) -> Result<PathBuf, NodeError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| NodeError::Config(format!("create LaunchAgents dir: {e}")))?;
        let path = dir.join(format!("{LABEL}.plist"));
        std::fs::write(&path, plist_contents(exe))
            .map_err(|e| NodeError::Config(format!("write launch agent plist: {e}")))?;
        Ok(path)
    }

    pub(super) fn install(exe: &Path) -> Result<PathBuf, NodeError> {
        let path = install_to(&agents_dir()?, exe)?;
        // Best-effort immediate activation. The file alone still guarantees
        // autostart on the NEXT login even if `launchctl` is unavailable
        // (e.g. a sandboxed/headless environment) — that failure is not
        // surfaced as an error for this reason.
        let _ = std::process::Command::new("launchctl")
            .args(["load", "-w"])
            .arg(&path)
            .status();
        Ok(path)
    }
}

#[cfg(target_os = "linux")]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    fn unit_dir() -> Result<PathBuf, NodeError> {
        let home = dirs::home_dir().ok_or_else(|| NodeError::Config("no home directory".into()))?;
        Ok(home.join(".config").join("systemd").join("user"))
    }

    fn unit_contents(exe: &Path) -> String {
        format!(
            "[Unit]\nDescription=Buzz execution node\n\n\
[Service]\nExecStart={} up --foreground\nRestart=on-failure\n\n\
[Install]\nWantedBy=default.target\n",
            exe.display(),
        )
    }

    /// The pure, testable half: write the unit file to an arbitrary `dir`.
    pub(super) fn install_to(dir: &Path, exe: &Path) -> Result<PathBuf, NodeError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| NodeError::Config(format!("create systemd user dir: {e}")))?;
        let path = dir.join("buzz-node.service");
        std::fs::write(&path, unit_contents(exe))
            .map_err(|e| NodeError::Config(format!("write systemd unit: {e}")))?;
        Ok(path)
    }

    pub(super) fn install(exe: &Path) -> Result<PathBuf, NodeError> {
        let path = install_to(&unit_dir()?, exe)?;
        // Best-effort immediate activation; see the macOS arm's comment on
        // why a failure here is not surfaced as an error.
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", "buzz-node.service"])
            .status();
        Ok(path)
    }
}

#[cfg(target_os = "windows")]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    fn startup_dir() -> Result<PathBuf, NodeError> {
        let appdata = std::env::var_os("APPDATA")
            .ok_or_else(|| NodeError::Config("APPDATA is not set".into()))?;
        Ok(PathBuf::from(appdata)
            .join("Microsoft")
            .join("Windows")
            .join("Start Menu")
            .join("Programs")
            .join("Startup"))
    }

    fn script_contents(exe: &Path) -> String {
        format!("@echo off\r\nstart \"\" \"{}\" up\r\n", exe.display())
    }

    /// The pure, testable half: write the startup script to an arbitrary `dir`.
    pub(super) fn install_to(dir: &Path, exe: &Path) -> Result<PathBuf, NodeError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| NodeError::Config(format!("create Startup dir: {e}")))?;
        let path = dir.join("buzz-node.bat");
        std::fs::write(&path, script_contents(exe))
            .map_err(|e| NodeError::Config(format!("write startup script: {e}")))?;
        Ok(path)
    }

    pub(super) fn install(exe: &Path) -> Result<PathBuf, NodeError> {
        // Placing the file in the Startup folder is itself sufficient —
        // nothing further to activate; it takes effect at the next login.
        install_to(&startup_dir()?, exe)
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    pub(super) fn install(_exe: &Path) -> Result<PathBuf, NodeError> {
        Err(NodeError::Config(
            "autostart is not supported on this platform".into(),
        ))
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    // --- CLI parsing ---

    #[test]
    fn cli_bare_invocation_has_no_subcommand() {
        let cli = Cli::parse_from(["buzz-node"]);
        assert!(cli.command.is_none());
    }

    #[test]
    fn cli_parses_up_with_foreground_flag() {
        let cli = Cli::parse_from(["buzz-node", "up", "--foreground"]);
        assert!(matches!(
            cli.command,
            Some(Command::Up { foreground: true })
        ));
    }

    #[test]
    fn cli_parses_up_without_foreground_flag() {
        let cli = Cli::parse_from(["buzz-node", "up"]);
        assert!(matches!(
            cli.command,
            Some(Command::Up { foreground: false })
        ));
    }

    #[test]
    fn cli_parses_enroll_with_relay_url() {
        let cli = Cli::parse_from(["buzz-node", "enroll", "--relay-url", "wss://r"]);
        match cli.command {
            Some(Command::Enroll { relay_url }) => {
                assert_eq!(relay_url.as_deref(), Some("wss://r"))
            }
            other => panic!("expected Enroll, got {other:?}"),
        }
    }

    #[test]
    fn cli_parses_status_autostart_stop() {
        assert!(matches!(
            Cli::parse_from(["buzz-node", "status"]).command,
            Some(Command::Status)
        ));
        assert!(matches!(
            Cli::parse_from(["buzz-node", "autostart"]).command,
            Some(Command::Autostart)
        ));
        assert!(matches!(
            Cli::parse_from(["buzz-node", "stop"]).command,
            Some(Command::Stop)
        ));
    }

    // --- singleton guard ---

    #[test]
    fn live_pid_detected_stale_pid_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pidfile = dir.path().join("daemon.pid");

        std::fs::write(&pidfile, std::process::id().to_string()).expect("write pid");
        assert_eq!(live_daemon_pid(&pidfile), Some(std::process::id()));

        std::fs::write(&pidfile, "999999999").expect("write stale pid");
        assert!(
            live_daemon_pid(&pidfile).is_none(),
            "a pid nothing runs as must be treated as stale, not alive"
        );
    }

    #[test]
    fn live_daemon_pid_missing_file_is_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pidfile = dir.path().join("does-not-exist.pid");
        assert!(live_daemon_pid(&pidfile).is_none());
    }

    #[test]
    fn live_daemon_pid_unparsable_contents_is_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pidfile = dir.path().join("daemon.pid");
        std::fs::write(&pidfile, "not-a-pid").expect("write garbage");
        assert!(live_daemon_pid(&pidfile).is_none());
    }

    #[test]
    fn running_daemon_requires_both_live_pid_and_fresh_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = DaemonPaths::under(dir.path());
        std::fs::write(&paths.pid_file, std::process::id().to_string()).expect("write pid");

        assert!(
            running_daemon(&paths).is_none(),
            "a live pid alone, with no status heartbeat yet, must not count as running"
        );

        std::fs::write(&paths.status_file, "{}").expect("write status");
        assert_eq!(running_daemon(&paths), Some(std::process::id()));
    }

    #[test]
    fn running_daemon_ignores_a_stale_status_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = DaemonPaths::under(dir.path());
        std::fs::write(&paths.pid_file, std::process::id().to_string()).expect("write pid");
        std::fs::write(&paths.status_file, "{}").expect("write status");

        let stale = SystemTime::now() - STATUS_FRESHNESS - Duration::from_secs(5);
        let file = std::fs::File::open(&paths.status_file).expect("open status file");
        file.set_modified(stale).expect("backdate mtime");

        assert!(
            running_daemon(&paths).is_none(),
            "a live pid with a stale heartbeat must not count as running"
        );
    }

    #[test]
    fn running_daemon_rejects_a_pid_nothing_runs_as_even_with_a_fresh_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = DaemonPaths::under(dir.path());
        std::fs::write(&paths.pid_file, "999999999").expect("write stale pid");
        std::fs::write(&paths.status_file, "{}").expect("write fresh status");
        assert!(running_daemon(&paths).is_none());
    }

    // --- identity/config cross-check ---

    fn sample_cfg() -> NodeConfig {
        NodeConfig {
            node_pubkey: "abc".into(),
            owner_pubkey: "o".into(),
            relay_url: "wss://r".into(),
            workspace_root: "/tmp/x".into(),
        }
    }

    #[test]
    fn ensure_identity_matches_config_accepts_a_match() {
        assert!(ensure_identity_matches_config("abc", &sample_cfg()).is_ok());
    }

    #[test]
    fn ensure_identity_matches_config_rejects_a_mismatch() {
        assert!(ensure_identity_matches_config("def", &sample_cfg()).is_err());
    }

    // --- graceful shutdown orchestration ---

    #[tokio::test]
    async fn shutdown_signal_wins_and_still_runs_final_publish() {
        let published = Arc::new(AtomicBool::new(false));
        let published2 = published.clone();

        let result = run_until_shutdown(
            std::future::pending::<Result<(), NodeError>>(),
            async {}, // fires immediately
            async move {
                published2.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert!(result.is_ok());
        assert!(
            published.load(Ordering::SeqCst),
            "final publish must run once shutdown wins the race"
        );
    }

    #[tokio::test]
    async fn engine_finishing_on_its_own_still_runs_final_publish() {
        let published = Arc::new(AtomicBool::new(false));
        let published2 = published.clone();

        let result = run_until_shutdown(
            async { Ok(()) },
            std::future::pending::<()>(), // shutdown never fires
            async move {
                published2.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert!(result.is_ok());
        assert!(published.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn engine_error_propagates_but_final_publish_still_runs() {
        let published = Arc::new(AtomicBool::new(false));
        let published2 = published.clone();

        let result = run_until_shutdown(
            async { Err(NodeError::Substrate("boom".into())) },
            std::future::pending::<()>(),
            async move {
                published2.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert!(result.is_err(), "the engine's error must still surface");
        assert!(
            published.load(Ordering::SeqCst),
            "final publish must run even when the engine errored"
        );
    }

    #[tokio::test]
    async fn final_publish_timeout_does_not_hang_shutdown() {
        let start = tokio::time::Instant::now();

        let result = run_until_shutdown(
            std::future::pending::<Result<(), NodeError>>(),
            async {},                                        // shutdown fires immediately
            std::future::pending::<Result<(), NodeError>>(), // final publish never resolves
        )
        .await;

        assert!(
            result.is_ok(),
            "a stuck final publish must not fail the whole shutdown"
        );
        assert!(
            start.elapsed() < FINAL_PUBLISH_TIMEOUT + Duration::from_secs(2),
            "must return promptly once the final-publish timeout elapses, not hang forever"
        );
    }

    // --- autostart (pure file-writing half; platform-gated) ---

    #[cfg(target_os = "macos")]
    #[test]
    fn autostart_plist_written_and_references_the_given_exe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let exe = PathBuf::from("/usr/local/bin/buzz-node");
        let path = autostart_impl::install_to(dir.path(), &exe).expect("install_to");
        let contents = std::fs::read_to_string(&path).expect("read plist");
        assert!(contents.contains("/usr/local/bin/buzz-node"));
        assert!(contents.contains("RunAtLoad"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn autostart_unit_written_and_references_the_given_exe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let exe = PathBuf::from("/usr/local/bin/buzz-node");
        let path = autostart_impl::install_to(dir.path(), &exe).expect("install_to");
        let contents = std::fs::read_to_string(&path).expect("read unit");
        assert!(contents.contains("/usr/local/bin/buzz-node"));
        assert!(contents.contains("[Service]"));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn autostart_script_written_and_references_the_given_exe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let exe = PathBuf::from(r"C:\buzz-node.exe");
        let path = autostart_impl::install_to(dir.path(), &exe).expect("install_to");
        let contents = std::fs::read_to_string(&path).expect("read script");
        assert!(contents.contains("buzz-node.exe"));
    }
}
