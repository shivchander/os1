//! The daemon's two-source singleton guard: a PID file plus a
//! `daemon.status.json` heartbeat, mirroring the OpenAgents launcher.
//!
//! Read half ([`running_daemon`], [`live_daemon_pid`]) and write half
//! ([`status_writer_loop`]) live together because both sides must agree on
//! [`DaemonPaths`]/[`DaemonStatus`]'s shape — splitting them further would
//! just move that coupling across a file boundary instead of removing it.
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use buzz_node::enroll::{self, NodeConfig};
use buzz_node::model::NodeError;
use serde::{Deserialize, Serialize};

/// How often `up --foreground` refreshes `daemon.status.json`.
const STATUS_WRITE_INTERVAL: Duration = Duration::from_secs(5);
/// How stale a `daemon.status.json` heartbeat can be before the two-source
/// singleton guard stops trusting it as evidence the daemon is still alive.
const STATUS_FRESHNESS: Duration = Duration::from_secs(30);

// ─── Daemon paths + status file ────────────────────────────────────────

/// Files a running daemon reads/writes under its home directory
/// ([`enroll::node_home_dir`]).
pub(super) struct DaemonPaths {
    pub(super) pid_file: PathBuf,
    pub(super) status_file: PathBuf,
    pub(super) log_file: PathBuf,
}

impl DaemonPaths {
    fn under(dir: &Path) -> Self {
        Self {
            pid_file: dir.join("daemon.pid"),
            status_file: dir.join("daemon.status.json"),
            log_file: dir.join("daemon.log"),
        }
    }

    pub(super) fn default_paths() -> Result<Self, NodeError> {
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
pub(super) fn live_daemon_pid(pid_file: &Path) -> Option<u32> {
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
pub(super) fn running_daemon(paths: &DaemonPaths) -> Option<u32> {
    let pid = live_daemon_pid(&paths.pid_file)?;
    status_is_fresh(&paths.status_file).then_some(pid)
}

// ─── Status heartbeat writer ────────────────────────────────────────────

fn write_status(path: &Path, status: &DaemonStatus) -> Result<(), NodeError> {
    let json = serde_json::to_string_pretty(status)
        .map_err(|e| NodeError::Config(format!("serialize daemon status: {e}")))?;
    std::fs::write(path, json).map_err(|e| NodeError::Config(format!("write daemon status: {e}")))
}

/// Refresh `status_file` every [`STATUS_WRITE_INTERVAL`] for as long as this
/// task runs; the first write happens immediately (`tokio::time::interval`'s
/// first tick never waits), so [`running_daemon`]'s freshness check sees a
/// heartbeat right away rather than only after the first full interval.
pub(super) async fn status_writer_loop(status_file: PathBuf, pid: u32, cfg: NodeConfig) {
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

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
}
