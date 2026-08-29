//! `AGENT_NODE_STATUS` codec: node-authored observed per-agent health.
use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use serde::{Deserialize, Serialize};

use crate::kind::KIND_AGENT_NODE_STATUS;
use crate::node_codec::{parse_canonical_pubkey, parse_rfc3339, parse_strict_json, CodecError};

/// Wire-format discriminator for node status payloads.
pub const FORMAT: &str = "buzz-node-status-v1";
/// Current node status payload schema version.
pub const VERSION: u32 = 1;

/// Observed health of an agent process, as reported by the node running it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentHealth {
    /// The node has launched the agent process but it is not yet ready.
    Starting,
    /// The agent process is running normally.
    Running,
    /// The agent process exited cleanly and is not running.
    Stopped,
    /// The agent process exited unexpectedly.
    Crashed,
    /// The node cannot host this agent (e.g. missing runtime, over capacity).
    Unschedulable,
}

/// Node-authored observed status for a single agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNodeStatus {
    /// Always [`FORMAT`].
    pub format: String,
    /// Always [`VERSION`].
    pub version: u32,
    /// Agent pubkey and event `d` coordinate.
    pub agent_pubkey: String,
    /// Reporting node's pubkey (must equal the signing key).
    pub node_pubkey: String,
    /// Observed health.
    pub health: AgentHealth,
    /// Optional human-readable reason code, e.g. an exit status or error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// RFC3339 bookkeeping timestamp of the observation.
    pub updated_at: String,
}

/// Build a signed `AGENT_NODE_STATUS` event.
pub fn build_status(
    node_keys: &Keys,
    status: &AgentNodeStatus,
    created_at: u64,
) -> Result<Event, CodecError> {
    if status.format != FORMAT || status.version != VERSION {
        return Err(CodecError::InvalidPayload(
            "unsupported format/version".into(),
        ));
    }
    parse_canonical_pubkey("agent_pubkey", &status.agent_pubkey)?;
    parse_canonical_pubkey("node_pubkey", &status.node_pubkey)?;
    parse_rfc3339("updated_at", &status.updated_at)?;
    if status.node_pubkey != node_keys.public_key().to_hex() {
        return Err(CodecError::InvalidPayload(
            "node_pubkey != signing key".into(),
        ));
    }
    let content = serde_json::to_string(status).map_err(|_| CodecError::Encrypt)?;
    EventBuilder::new(Kind::Custom(KIND_AGENT_NODE_STATUS as u16), content)
        .tags([Tag::parse(["d", status.agent_pubkey.as_str()]).map_err(|_| CodecError::Sign)?])
        .custom_created_at(nostr::Timestamp::from(created_at))
        .sign_with_keys(node_keys)
        .map_err(|_| CodecError::Sign)
}

/// Validate an `AGENT_NODE_STATUS` event and return its status.
pub fn validate_status(event: &Event) -> Result<AgentNodeStatus, CodecError> {
    if event.kind.as_u16() as u32 != KIND_AGENT_NODE_STATUS {
        return Err(CodecError::InvalidEnvelope("wrong kind".into()));
    }
    if !event.verify_id() || !event.verify_signature() {
        return Err(CodecError::InvalidEnvelope("invalid id/signature".into()));
    }
    let status: AgentNodeStatus =
        serde_json::from_value(parse_strict_json(event.content.as_bytes())?)
            .map_err(|e| CodecError::InvalidPayload(format!("schema: {e}")))?;
    if status.format != FORMAT || status.version != VERSION {
        return Err(CodecError::InvalidPayload(
            "unsupported format/version".into(),
        ));
    }
    parse_canonical_pubkey("agent_pubkey", &status.agent_pubkey)?;
    parse_canonical_pubkey("node_pubkey", &status.node_pubkey)?;
    parse_rfc3339("updated_at", &status.updated_at)?;
    if status.node_pubkey != event.pubkey.to_hex() {
        return Err(CodecError::InvalidEnvelope(
            "node did not sign its own status".into(),
        ));
    }
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::Keys;
    #[test]
    fn status_round_trips_and_binds_node_author() {
        let (node, agent) = (Keys::generate(), Keys::generate());
        let s = AgentNodeStatus {
            format: FORMAT.into(), version: VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            health: AgentHealth::Running, reason: None,
            updated_at: "2026-08-29T00:00:00Z".into(),
        };
        let ev = build_status(&node, &s, 1_785_780_000).unwrap();
        assert_eq!(validate_status(&ev).unwrap(), s);
    }
    #[test]
    fn status_rejects_non_author_node() {
        let (node, agent) = (Keys::generate(), Keys::generate());
        let s = AgentNodeStatus {
            format: FORMAT.into(), version: VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            node_pubkey: Keys::generate().public_key().to_hex(), // not signer
            health: AgentHealth::Crashed, reason: Some("exit 1".into()),
            updated_at: "2026-08-29T00:00:00Z".into(),
        };
        // `build_status` itself refuses to sign a self-mismatched status, so
        // construct the malformed event directly to exercise `validate_status`'s
        // independent author-binding check (defense in depth: a signer who signs
        // as themselves but claims a different node_pubkey in content).
        let content = serde_json::to_string(&s).unwrap();
        let ev = EventBuilder::new(Kind::Custom(KIND_AGENT_NODE_STATUS as u16), content)
            .tags([Tag::parse(["d", s.agent_pubkey.as_str()]).unwrap()])
            .custom_created_at(nostr::Timestamp::from(1_785_780_000))
            .sign_with_keys(&node)
            .unwrap();
        assert!(matches!(validate_status(&ev), Err(CodecError::InvalidEnvelope(_))));
    }
    #[test]
    fn status_rejects_bad_timestamp() {
        let (node, agent) = (Keys::generate(), Keys::generate());
        let s = AgentNodeStatus {
            format: FORMAT.into(), version: VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            health: AgentHealth::Stopped, reason: None,
            updated_at: "not-a-date".into(),
        };
        assert!(matches!(build_status(&node, &s, 1_785_780_000), Err(CodecError::InvalidPayload(_))));
    }
}
