//! The `AgentRuntime` seam (D7) and its ACP implementation: how the node
//! turns a decrypted [`crate::model::DesiredAgent`] into a running child
//! process.
use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use buzz_core::assignment::AssignmentSecret;
use nostr::PublicKey;
use tokio::process::{Child, Command};
use zeroize::Zeroize;

use crate::model::{DesiredAgent, NodeError};

/// Env keys that carry authoritative agent identity. These are always set
/// from the decrypted [`AssignmentSecret`] and are stripped from any
/// user-supplied environment map before merging, so a careless or malicious
/// `env_vars`/`launch.env`/`launch.policy_env` entry can never spoof the
/// agent's identity to the relay.
const RESERVED_ENV_KEYS: &[&str] = &[
    "BUZZ_PRIVATE_KEY",
    "NOSTR_PRIVATE_KEY",
    "BUZZ_AUTH_TAG",
    "BUZZ_RELAY_URL",
    "BUZZ_ACP_AGENT_OWNER",
];

/// Build the child environment for an agent harness process.
///
/// Precedence (later overrides earlier): `launch.policy_env` < `launch.env`
/// < `secret.env_vars` < authoritative identity. Any user-supplied reserved
/// key (see [`RESERVED_ENV_KEYS`]) is dropped before the merge, so identity
/// can never be overridden by policy/launch/user env.
pub fn build_child_env(secret: &AssignmentSecret, relay_url: &str) -> Vec<(String, String)> {
    let mut env: BTreeMap<String, String> = BTreeMap::new();
    let merge = |m: &BTreeMap<String, String>, env: &mut BTreeMap<String, String>| {
        for (k, v) in m {
            if !RESERVED_ENV_KEYS.contains(&k.as_str()) {
                env.insert(k.clone(), v.clone());
            }
        }
    };
    merge(&secret.launch.policy_env, &mut env);
    merge(&secret.launch.env, &mut env);
    merge(&secret.env_vars, &mut env);

    // Authoritative identity, written last so it always wins.
    env.insert("BUZZ_PRIVATE_KEY".into(), secret.private_key_nsec.clone());
    env.insert("NOSTR_PRIVATE_KEY".into(), secret.private_key_nsec.clone());
    env.insert("BUZZ_RELAY_URL".into(), relay_url.to_string());
    if let Some(tag) = &secret.auth_tag {
        env.insert("BUZZ_AUTH_TAG".into(), tag.clone());
    }
    if let Some(owner) = &secret.launch.owner_pubkey {
        env.insert("BUZZ_ACP_AGENT_OWNER".into(), owner.clone());
    }
    env.into_iter().collect()
}

/// Spawns the agent harness process for a [`DesiredAgent`]. D7 seam:
/// ACP-only implementation in v1 ([`AcpRuntime`]); a future adapter can
/// implement this trait to launch a non-ACP harness without substrate
/// changes.
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    /// Spawn the harness in `workspace` with the agent's environment. The
    /// returned [`Child`] is owned by the caller's process table, which
    /// observes and terminates it.
    ///
    /// Contract: the child **must** be spawned into its own new process
    /// group (Unix: `Command::process_group(0)`, as [`AcpRuntime`] does)
    /// so that `LocalProcessSubstrate::stop`'s `killpg` on `child.id()`
    /// reaches the whole tree instead of a group the child never leads.
    async fn spawn(
        &self,
        desired: &DesiredAgent,
        workspace: &Path,
        relay_url: &str,
    ) -> Result<Child, NodeError>;

    /// Actively probe a previously spawned agent for liveness — a real
    /// round-trip beyond mere OS-process existence (spec §9 active
    /// smoke-probe). `Err` means the probe itself failed (the agent is
    /// unresponsive even though its process may still be running), which
    /// [`crate::health::classify`] surfaces as `AgentHealth::Crashed`/
    /// `"probe-failed"`.
    async fn probe(&self, agent: &PublicKey) -> Result<(), NodeError>;
}

/// ACP runtime: spawns `buzz-acp` (or a bundled `sprig`) with the injected
/// agent environment.
pub struct AcpRuntime {
    /// Harness binary resolved on `PATH` (default `buzz-acp`).
    pub harness_command: String,
    /// Extra harness CLI arguments (default empty).
    pub harness_args: Vec<String>,
}

impl Default for AcpRuntime {
    fn default() -> Self {
        Self {
            harness_command: "buzz-acp".into(),
            harness_args: Vec::new(),
        }
    }
}

#[async_trait]
impl AgentRuntime for AcpRuntime {
    async fn spawn(
        &self,
        desired: &DesiredAgent,
        workspace: &Path,
        relay_url: &str,
    ) -> Result<Child, NodeError> {
        let mut cmd = Command::new(&self.harness_command);
        cmd.args(&self.harness_args)
            .current_dir(workspace)
            .env_clear()
            // Safety net only, for an abnormal drop (panic/bug) before the
            // process table ever registers this child: the *normal* shutdown
            // path is the substrate's graceful process-*group* kill
            // (`substrate::kill_group`), which this cannot replace — it has
            // no knowledge of the child's own descendants.
            .kill_on_drop(true);
        // Preserve enough host env for the harness to resolve its own tools.
        for key in ["PATH", "HOME", "USER", "LANG", "TMPDIR"] {
            if let Ok(v) = std::env::var(key) {
                cmd.env(key, v);
            }
        }
        // Each `v` is our own owned copy of a secret-derived env value
        // (notably the agent nsec, twice). `Command::env` copies it again
        // into its internal env map, so we zeroize our copy immediately
        // after — this cannot scrub the `Command`'s internal copy or the
        // OS's copy of the child's environment block, but it does close the
        // window where our own transient `String` sits in memory unscrubbed.
        for (k, mut v) in build_child_env(&desired.secret, relay_url) {
            cmd.env(&k, v.as_str());
            v.zeroize();
        }

        // New process group so a future stop() can signal the whole tree
        // with one killpg (mirrors `buzz-dev-mcp::shell::KillGroup`).
        #[cfg(unix)]
        cmd.process_group(0);

        cmd.spawn().map_err(|e| NodeError::Spawn {
            agent: desired.agent_pubkey.to_hex(),
            reason: e.to_string(),
        })
    }

    async fn probe(&self, _agent: &PublicKey) -> Result<(), NodeError> {
        // v1 LIMITATION (tracked as a Phase 5 follow-up, not silently
        // papered over): `buzz-acp` exposes no control-channel/health RPC
        // yet, and `AcpRuntime` is stateless — it hands the spawned `Child`
        // to the substrate's process table and keeps no reference to it, so
        // there is no in-process handle here to round-trip against. This
        // reports healthy unconditionally; OS-level liveness is still fully
        // covered by `Substrate::observe`'s `try_wait` polling. Closing this
        // gap needs a real control-channel ping added to `buzz-acp` (e.g. an
        // ACP `session/probe`-style round trip) threaded through here.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::assignment::{AssignmentSecret, LaunchBlock, FORMAT, VERSION};
    use nostr::Keys;
    use std::collections::BTreeMap;

    fn secret() -> AssignmentSecret {
        let agent = Keys::generate();
        AssignmentSecret {
            format: FORMAT.into(),
            version: VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            owner_pubkey: Keys::generate().public_key().to_hex(),
            node_pubkey: Keys::generate().public_key().to_hex(),
            private_key_nsec: "nsec1exampleexampleexample".into(),
            auth_tag: Some("[\"auth\",\"owner\",\"\",\"sig\"]".into()),
            launch: LaunchBlock {
                command: "claude".into(),
                args: vec![],
                env: BTreeMap::from([("GOOSE_MODEL".into(), "sonnet".into())]),
                policy_env: BTreeMap::from([("GOOSE_MODE".into(), "auto".into())]),
                owner_pubkey: Some("ownerhex".into()),
            },
            env_vars: BTreeMap::from([
                ("FOO".into(), "bar".into()),
                ("BUZZ_PRIVATE_KEY".into(), "attacker".into()), // MUST be stripped
            ]),
            reap_after_idle_seconds: None,
        }
    }

    #[test]
    fn env_builder_sets_identity_and_strips_reserved_user_keys() {
        let env: BTreeMap<String, String> = build_child_env(&secret(), "wss://relay.example")
            .into_iter()
            .collect();
        assert_eq!(env["BUZZ_PRIVATE_KEY"], "nsec1exampleexampleexample");
        assert_eq!(env["BUZZ_RELAY_URL"], "wss://relay.example");
        assert_eq!(env["NOSTR_PRIVATE_KEY"], "nsec1exampleexampleexample");
        assert_eq!(env["BUZZ_AUTH_TAG"], "[\"auth\",\"owner\",\"\",\"sig\"]");
        assert_eq!(env["FOO"], "bar"); // user env passes through
        assert_eq!(env["GOOSE_MODEL"], "sonnet"); // launch.env
        assert_eq!(env["GOOSE_MODE"], "auto"); // policy_env
        assert_eq!(env["BUZZ_ACP_AGENT_OWNER"], "ownerhex"); // launch.owner_pubkey
                                                             // reserved key supplied by user did NOT override the authoritative nsec:
        assert_ne!(env["BUZZ_PRIVATE_KEY"], "attacker");
    }

    #[tokio::test]
    async fn acp_runtime_spawns_a_child_in_workspace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rt = AcpRuntime {
            harness_command: "/bin/sh".into(),
            harness_args: vec!["-c".into(), "sleep 5".into()],
        };
        let (agent, node, owner) = (Keys::generate(), Keys::generate(), Keys::generate());
        let desired =
            crate::model::fake_desired(&agent, &node, &owner, buzz_core::AssignState::Assigned);

        let mut child = rt
            .spawn(&desired, dir.path(), "wss://relay.example")
            .await
            .expect("spawn");
        assert!(child.id().is_some(), "child should be running");

        // Clean up deterministically so the test never leaves an orphaned
        // `sleep` process behind.
        child.start_kill().ok();
        let _ = child.wait().await;
    }
}
