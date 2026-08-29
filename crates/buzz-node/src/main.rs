#![deny(unsafe_code)]
//! `buzz-node` — the execution-node daemon binary.
//!
//! Argument parsing and all process/daemon orchestration (detached spawn,
//! the PID/status singleton guard, graceful shutdown, autostart) live in
//! `daemon.rs`, kept as part of this binary crate's own test target so that
//! logic stays unit-testable. This file is intentionally a thin shim.
mod daemon;

use clap::Parser;

#[tokio::main]
async fn main() {
    let cli = daemon::Cli::parse();
    std::process::exit(daemon::dispatch(cli).await);
}
