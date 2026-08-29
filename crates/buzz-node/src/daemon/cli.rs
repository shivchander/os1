//! `buzz-node`'s CLI argument grammar: [`Cli`] and [`Command`].
//!
//! Parsing only — routing a parsed [`Command`] to its handler is
//! [`super::dispatch`], which lives in the parent `daemon` module alongside
//! the handlers themselves.
use clap::{Parser, Subcommand};

/// `buzz-node` — a persistent execution-node daemon that hosts Buzz agents.
#[derive(Debug, Parser)]
#[command(name = "buzz-node", about = "Buzz execution-node daemon")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(super) command: Option<Command>,
}

/// `buzz-node` subcommands. Bare invocation (no subcommand) behaves like
/// [`Command::Status`].
#[derive(Debug, Subcommand)]
pub(super) enum Command {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
