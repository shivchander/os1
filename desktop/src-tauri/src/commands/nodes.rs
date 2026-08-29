//! Execution-node desktop commands: enrollment + assignment publication.
//!
//! Both commands keep signing and key material entirely native — the
//! frontend calls these and receives only the published event id; a private
//! key (owner or agent) never crosses the Tauri IPC boundary. See
//! `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` §12.

use nostr::{Event, Keys, PublicKey};
use tauri::State;

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

#[cfg(test)]
#[path = "nodes_tests.rs"]
mod tests;
