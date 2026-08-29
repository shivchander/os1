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
//!
//! Submodules split out the self-contained pieces: [`cli`] (the argument
//! grammar), [`singleton`] (the PID/`daemon.status.json` guard), and
//! [`autostart`] (the per-OS login-item registration). What's left here is
//! the orchestration that ties them together: command dispatch, detached
//! spawn, and the graceful-shutdown race.
use std::sync::Arc;
use std::time::Duration;

use buzz_node::engine;
use buzz_node::enroll::{self, KeychainStore, NodeConfig};
use buzz_node::model::NodeError;
use buzz_node::nostr_relay::NostrNodeRelay;
use buzz_node::relay::NodeRelay;
use buzz_node::runtime::AcpRuntime;
use buzz_node::substrate::{LocalProcessSubstrate, Substrate};
use nostr::PublicKey;

use autostart::cmd_autostart;
use cli::Command;
use singleton::{live_daemon_pid, running_daemon, status_writer_loop, DaemonPaths};

mod autostart;
mod cli;
mod singleton;

pub(crate) use cli::Cli;

/// How often the engine reconciles even without a fresh assignment (self-heal
/// cadence for `up`'s live daemon).
const RECONCILE_TICK: Duration = Duration::from_secs(5);
/// Upper bound on the final, awaited offline-presence publish during
/// shutdown. [`NostrNodeRelay::publish_presence_awaited`]'s underlying
/// reconnect-forever policy is correct for a long-lived background publish
/// but must not hang process exit indefinitely against a relay that never
/// comes back.
const FINAL_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

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

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

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
}
