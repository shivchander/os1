//! Domain types for the node engine.
use nostr::PublicKey;

use buzz_core::{AssignState, AssignmentSecret};

/// One agent the relay says should (or should not) run on this node.
#[derive(Debug, Clone, PartialEq)]
pub struct DesiredAgent {
    /// Agent identity (derived from the assignment secret's nsec).
    pub agent_pubkey: PublicKey,
    /// The decrypted assignment secret (nsec + launch env). `Debug` is redacted.
    pub secret: AssignmentSecret,
    /// Whether the assignment wants this agent running (`Assigned`) or stopped.
    pub state: AssignState,
}

/// Observed runtime state of an agent on this node's substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    /// No process/record exists for this agent.
    Absent,
    /// A process exists but the harness has not confirmed startup.
    Starting,
    /// The harness process is up.
    Running,
    /// The process exited cleanly / was stopped.
    Stopped,
    /// The process exited abnormally.
    Crashed {
        /// Exit code if known.
        code: Option<i32>,
    },
}

/// A reconcile decision for a single agent.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Spawn the agent.
    Start(Box<DesiredAgent>),
    /// Stop the agent by pubkey.
    Stop(PublicKey),
    /// Stop then re-spawn a crashed agent.
    Restart(Box<DesiredAgent>),
    /// Nothing to do for this agent.
    Noop(PublicKey),
}

/// Errors surfaced by the node engine and its I/O traits.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The substrate (process table) failed an operation.
    #[error("substrate error: {0}")]
    Substrate(String),
    /// The relay transport failed an operation.
    #[error("relay error: {0}")]
    Relay(String),
    /// Spawning an agent failed.
    #[error("failed to spawn agent {agent}: {reason}")]
    Spawn {
        /// Agent pubkey (hex).
        agent: String,
        /// Underlying failure description.
        ///
        /// Named `reason` rather than `source`: `thiserror` treats a field
        /// literally named `source` as the error's `Error::source()`
        /// regardless of attributes, which requires that field's type to
        /// implement `std::error::Error` — `String` does not, so that name
        /// fails to compile here.
        reason: String,
    },
    /// Invalid engine/agent configuration.
    #[error("configuration error: {0}")]
    Config(String),
}

#[cfg(test)]
pub(crate) fn fake_desired(
    agent: &nostr::Keys,
    node: &nostr::Keys,
    owner: &nostr::Keys,
    state: AssignState,
) -> DesiredAgent {
    use nostr::ToBech32;
    use std::collections::BTreeMap;
    let secret = AssignmentSecret {
        format: buzz_core::assignment::FORMAT.to_string(),
        version: buzz_core::assignment::VERSION,
        agent_pubkey: agent.public_key().to_hex(),
        owner_pubkey: owner.public_key().to_hex(),
        node_pubkey: node.public_key().to_hex(),
        private_key_nsec: agent.secret_key().to_bech32().unwrap(),
        auth_tag: None,
        launch: buzz_core::LaunchBlock {
            command: "claude".into(),
            args: vec![],
            env: BTreeMap::new(),
            policy_env: BTreeMap::new(),
            owner_pubkey: Some(owner.public_key().to_hex()),
        },
        env_vars: BTreeMap::new(),
        reap_after_idle_seconds: None,
    };
    DesiredAgent {
        agent_pubkey: agent.public_key(),
        secret,
        state,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::Keys;

    #[test]
    fn fake_desired_carries_agent_identity_and_state() {
        let (agent, node, owner) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&agent, &node, &owner, buzz_core::AssignState::Assigned);
        assert_eq!(d.agent_pubkey, agent.public_key());
        assert_eq!(d.state, buzz_core::AssignState::Assigned);
        assert_eq!(d.secret.agent_pubkey, agent.public_key().to_hex());
        assert_eq!(d.secret.node_pubkey, node.public_key().to_hex());
    }

    #[test]
    fn node_error_displays() {
        let e = NodeError::Substrate("boom".into());
        assert!(e.to_string().contains("boom"));
    }
}
