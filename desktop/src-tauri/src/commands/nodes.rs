//! Execution-node desktop commands: enrollment + assignment publication.
//!
//! Both commands keep signing and key material entirely native — the
//! frontend calls these and receives only the published event id; a private
//! key (owner or agent) never crosses the Tauri IPC boundary. See
//! `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` §12.

use std::collections::BTreeMap;

use nostr::{Event, Keys, PublicKey};
use tauri::{AppHandle, State};

use crate::app_state::AppState;
use crate::relay;

/// Build the owner-signed `NODE_ENROLLMENT` event authorizing `node_pubkey`.
///
/// Pure/testable: no I/O, no relay. Extracted from [`publish_node_enrollment`]
/// so the event-construction path can be unit tested without a live relay.
fn build_node_enrollment_event(owner: &Keys, node_pubkey: &PublicKey) -> Result<Event, String> {
    buzz_core_pkg::node::build_enrollment(owner, node_pubkey, nostr::Timestamp::now().as_secs())
        .map_err(|error| error.to_string())
}

/// Owner-sign and publish a `NODE_ENROLLMENT` authorizing `node_pubkey`.
///
/// Returns the published event id (hex). The owner's key never leaves this
/// process — it is read from [`AppState::keys`] and used only to sign.
#[tauri::command]
pub async fn publish_node_enrollment(
    node_pubkey: String,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let node =
        PublicKey::from_hex(node_pubkey.trim()).map_err(|_| "invalid node pubkey".to_string())?;
    let owner = state
        .keys
        .lock()
        .map_err(|error| error.to_string())?
        .clone();
    let event = build_node_enrollment_event(&owner, &node)?;
    let event_id = event.id.to_hex();
    relay::submit_signed_event_with_keys(&event, &state, &owner, None)
        .await
        .map_err(|error| format!("failed to publish node enrollment: {error}"))?;
    Ok(event_id)
}

/// Desktop-resolved launch contract for starting an agent process on a node.
///
/// Mirrors [`buzz_core_pkg::LaunchBlock`] field-for-field; kept as a distinct
/// IPC-boundary type (rather than deserializing the buzz-core type directly)
/// so the wire shape at the Tauri command boundary is free to diverge from
/// the wire-encrypted payload shape. `camelCase` on the wire — e.g. the
/// frontend sends `policyEnv`/`ownerPubkey` — maps to these snake_case
/// fields.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchInput {
    /// Executable to launch (e.g. `claude`, `goose`).
    pub command: String,
    /// Explicit CLI arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Plain environment overrides.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Policy-controlled environment overrides.
    #[serde(default)]
    pub policy_env: BTreeMap<String, String>,
    /// Owning user's pubkey, when applicable to the launch contract.
    #[serde(default)]
    pub owner_pubkey: Option<String>,
}

impl From<LaunchInput> for buzz_core_pkg::LaunchBlock {
    fn from(value: LaunchInput) -> Self {
        Self {
            command: value.command,
            args: value.args,
            env: value.env,
            policy_env: value.policy_env,
            owner_pubkey: value.owner_pubkey,
        }
    }
}

/// Resolve an existing managed agent's Nostr nsec from its on-disk record.
///
/// `load_managed_agents` hydrates `private_key_nsec` from the OS keyring (see
/// `managed_agents::storage::hydrate_keys`) — this reuses that existing
/// resolution path rather than touching the keyring directly. Read-only, so
/// it is scoped under the same store lock every other `load_managed_agents`
/// caller uses; the guard is dropped when this function returns, well before
/// the command's later `.await` on the relay publish.
fn resolve_agent_nsec(
    app: &AppHandle,
    state: &AppState,
    agent_pubkey: &str,
) -> Result<String, String> {
    let _store_guard = state
        .managed_agents_store_lock
        .lock()
        .map_err(|error| error.to_string())?;
    let records = crate::managed_agents::load_managed_agents(app)?;
    let record = records
        .into_iter()
        .find(|record| record.pubkey == agent_pubkey)
        .ok_or_else(|| format!("agent {agent_pubkey} not found"))?;
    if record.private_key_nsec.trim().is_empty() {
        return Err(format!(
            "agent {agent_pubkey} key unavailable — the OS keyring may be unreachable"
        ));
    }
    Ok(record.private_key_nsec)
}

/// Build the owner-signed, node-encrypted `AGENT_ASSIGNMENT` event.
///
/// Pure/testable: no I/O, no relay, no keyring — the resolved nsec is passed
/// in rather than looked up. Extracted from [`publish_agent_assignment`] so
/// the encrypt-to-node path can be unit tested without a live relay. Never
/// logs `private_key_nsec` or any part of `secret`.
fn build_agent_assignment_event(
    owner: &Keys,
    agent_pubkey: &PublicKey,
    node_pubkey: &PublicKey,
    private_key_nsec: String,
    launch: LaunchInput,
    assign_state: buzz_core_pkg::AssignState,
) -> Result<Event, String> {
    let secret = buzz_core_pkg::AssignmentSecret {
        format: buzz_core_pkg::assignment::FORMAT.to_string(),
        version: buzz_core_pkg::assignment::VERSION,
        agent_pubkey: agent_pubkey.to_hex(),
        owner_pubkey: owner.public_key().to_hex(),
        node_pubkey: node_pubkey.to_hex(),
        private_key_nsec,
        auth_tag: None,
        launch: launch.into(),
        env_vars: BTreeMap::new(),
        reap_after_idle_seconds: None,
    };
    buzz_core_pkg::assignment::build_assignment(
        owner,
        node_pubkey,
        &secret,
        assign_state,
        nostr::Timestamp::now().as_secs(),
    )
    .map_err(|error| error.to_string())
}

/// Owner-sign and publish an `AGENT_ASSIGNMENT` assigning `agent_id` to
/// `node_pubkey` (or unassigning it, when `assigned` is `false`).
///
/// The agent's nsec and launch contract are NIP-44 encrypted to
/// `node_pubkey` — only that node can decrypt them; the relay and every
/// other party see only the public envelope (agent/node/state tags). Neither
/// the owner's nor the agent's key ever leaves this process — the frontend
/// receives only the published event id.
#[tauri::command]
pub async fn publish_agent_assignment(
    agent_id: String,
    node_pubkey: String,
    launch: LaunchInput,
    assigned: bool,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let agent =
        PublicKey::from_hex(agent_id.trim()).map_err(|_| "invalid agent pubkey".to_string())?;
    let node =
        PublicKey::from_hex(node_pubkey.trim()).map_err(|_| "invalid node pubkey".to_string())?;
    let owner = state
        .keys
        .lock()
        .map_err(|error| error.to_string())?
        .clone();
    let private_key_nsec = resolve_agent_nsec(&app, &state, &agent.to_hex())?;
    let assign_state = if assigned {
        buzz_core_pkg::AssignState::Assigned
    } else {
        buzz_core_pkg::AssignState::Unassigned
    };
    let event = build_agent_assignment_event(
        &owner,
        &agent,
        &node,
        private_key_nsec,
        launch,
        assign_state,
    )?;
    let event_id = event.id.to_hex();
    relay::submit_signed_event_with_keys(&event, &state, &owner, None)
        .await
        .map_err(|error| format!("failed to publish agent assignment: {error}"))?;
    Ok(event_id)
}

#[cfg(test)]
#[path = "nodes_tests.rs"]
mod tests;
