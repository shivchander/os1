//! `NODE_ANNOUNCE` and `NODE_ENROLLMENT` codecs for execution nodes.
use nostr::{Event, EventBuilder, Keys, Kind, PublicKey, Tag};
use serde::{Deserialize, Serialize};

use crate::kind::{KIND_NODE_ANNOUNCE, KIND_NODE_ENROLLMENT};
use crate::node_codec::{parse_canonical_pubkey, parse_strict_json, CodecError};

/// Wire-format discriminator for node payloads.
pub const FORMAT: &str = "buzz-node-v1";
/// Current node payload schema version.
pub const VERSION: u32 = 1;

/// Node-authored capabilities advertised in `NODE_ANNOUNCE`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeCapabilities {
    /// Always [`FORMAT`].
    pub format: String,
    /// Always [`VERSION`].
    pub version: u32,
    /// Node's own pubkey (must equal the signing key).
    pub node_pubkey: String,
    /// Coarse OS label, e.g. `macos` / `linux` / `windows`.
    pub os: String,
    /// Installed agent runtimes the node can host (registry ids).
    pub runtimes: Vec<String>,
    /// Absolute path under which per-agent workspaces are created.
    pub workspace_root: String,
    /// Optional soft cap on concurrently hosted agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_agents: Option<u32>,
}

/// Owner-signed authorization binding a node pubkey to an owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Enrollment {
    /// Always [`FORMAT`].
    pub format: String,
    /// Always [`VERSION`].
    pub version: u32,
    /// Authorized node pubkey.
    pub node_pubkey: String,
    /// Enrolling owner pubkey (equals the signing key).
    pub owner_pubkey: String,
}

/// Build a signed `NODE_ANNOUNCE` event.
pub fn build_announce(
    node_keys: &Keys,
    caps: &NodeCapabilities,
    created_at: u64,
) -> Result<Event, CodecError> {
    if caps.format != FORMAT || caps.version != VERSION {
        return Err(CodecError::InvalidPayload(
            "unsupported format/version".into(),
        ));
    }
    if caps.node_pubkey != node_keys.public_key().to_hex() {
        return Err(CodecError::InvalidPayload(
            "node_pubkey != signing key".into(),
        ));
    }
    let content = serde_json::to_string(caps).map_err(|_| CodecError::Encrypt)?;
    EventBuilder::new(Kind::Custom(KIND_NODE_ANNOUNCE as u16), content)
        .tags([Tag::parse(["d", caps.node_pubkey.as_str()]).map_err(|_| CodecError::Sign)?])
        .custom_created_at(nostr::Timestamp::from(created_at))
        .sign_with_keys(node_keys)
        .map_err(|_| CodecError::Sign)
}

/// Validate a `NODE_ANNOUNCE` event and return its capabilities.
pub fn validate_announce(event: &Event) -> Result<NodeCapabilities, CodecError> {
    if event.kind.as_u16() as u32 != KIND_NODE_ANNOUNCE {
        return Err(CodecError::InvalidEnvelope("wrong kind".into()));
    }
    if !event.verify_id() || !event.verify_signature() {
        return Err(CodecError::InvalidEnvelope("invalid id/signature".into()));
    }
    let caps: NodeCapabilities =
        serde_json::from_value(parse_strict_json(event.content.as_bytes())?)
            .map_err(|e| CodecError::InvalidPayload(format!("schema: {e}")))?;
    if caps.format != FORMAT || caps.version != VERSION {
        return Err(CodecError::InvalidPayload(
            "unsupported format/version".into(),
        ));
    }
    parse_canonical_pubkey("node_pubkey", &caps.node_pubkey)?;
    if caps.node_pubkey != event.pubkey.to_hex() {
        return Err(CodecError::InvalidEnvelope(
            "node did not sign its own announce".into(),
        ));
    }
    Ok(caps)
}

/// Build an owner-signed `NODE_ENROLLMENT` event.
pub fn build_enrollment(
    owner_keys: &Keys,
    node_pubkey: &PublicKey,
    created_at: u64,
) -> Result<Event, CodecError> {
    let payload = Enrollment {
        format: FORMAT.into(),
        version: VERSION,
        node_pubkey: node_pubkey.to_hex(),
        owner_pubkey: owner_keys.public_key().to_hex(),
    };
    let content = serde_json::to_string(&payload).map_err(|_| CodecError::Encrypt)?;
    EventBuilder::new(Kind::Custom(KIND_NODE_ENROLLMENT as u16), content)
        .tags([Tag::parse(["d", payload.node_pubkey.as_str()]).map_err(|_| CodecError::Sign)?])
        .custom_created_at(nostr::Timestamp::from(created_at))
        .sign_with_keys(owner_keys)
        .map_err(|_| CodecError::Sign)
}

/// Validate a `NODE_ENROLLMENT` event against the expected owner.
pub fn validate_enrollment(
    event: &Event,
    expected_owner: &PublicKey,
) -> Result<Enrollment, CodecError> {
    if event.kind.as_u16() as u32 != KIND_NODE_ENROLLMENT {
        return Err(CodecError::InvalidEnvelope("wrong kind".into()));
    }
    if &event.pubkey != expected_owner {
        return Err(CodecError::InvalidEnvelope(
            "author is not expected owner".into(),
        ));
    }
    if !event.verify_id() || !event.verify_signature() {
        return Err(CodecError::InvalidEnvelope("invalid id/signature".into()));
    }
    let e: Enrollment = serde_json::from_value(parse_strict_json(event.content.as_bytes())?)
        .map_err(|err| CodecError::InvalidPayload(format!("schema: {err}")))?;
    if e.format != FORMAT || e.version != VERSION {
        return Err(CodecError::InvalidPayload(
            "unsupported format/version".into(),
        ));
    }
    parse_canonical_pubkey("node_pubkey", &e.node_pubkey)?;
    if e.owner_pubkey != expected_owner.to_hex() {
        return Err(CodecError::InvalidPayload("owner_pubkey mismatch".into()));
    }
    Ok(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::Keys;

    #[test]
    fn announce_round_trips_and_binds_author() {
        let node = Keys::generate();
        let caps = NodeCapabilities {
            format: FORMAT.into(),
            version: VERSION,
            node_pubkey: node.public_key().to_hex(),
            os: "macos".into(),
            runtimes: vec!["claude".into(), "goose".into()],
            workspace_root: "/Users/x/.buzz-node".into(),
            max_agents: Some(8),
        };
        let ev = build_announce(&node, &caps, 1_785_780_000).unwrap();
        assert_eq!(validate_announce(&ev).unwrap(), caps);
    }

    #[test]
    fn announce_rejects_author_mismatch() {
        let node = Keys::generate();
        let caps = NodeCapabilities {
            format: FORMAT.into(),
            version: VERSION,
            node_pubkey: Keys::generate().public_key().to_hex(), // not the signer
            os: "linux".into(),
            runtimes: vec![],
            workspace_root: "/x".into(),
            max_agents: None,
        };
        // `build_announce` itself refuses to sign a self-mismatched announce, so
        // construct the malformed event directly to exercise `validate_announce`'s
        // independent author-binding check (defense in depth: a signer who signs
        // as themselves but claims a different node_pubkey in content).
        let content = serde_json::to_string(&caps).unwrap();
        let ev = EventBuilder::new(Kind::Custom(KIND_NODE_ANNOUNCE as u16), content)
            .tags([Tag::parse(["d", caps.node_pubkey.as_str()]).unwrap()])
            .custom_created_at(nostr::Timestamp::from(1_785_780_000))
            .sign_with_keys(&node)
            .unwrap();
        assert!(matches!(
            validate_announce(&ev),
            Err(CodecError::InvalidEnvelope(_))
        ));
    }

    #[test]
    fn enrollment_round_trips_and_requires_owner() {
        let owner = Keys::generate();
        let node = Keys::generate();
        let ev = build_enrollment(&owner, &node.public_key(), 1_785_780_000).unwrap();
        let e = validate_enrollment(&ev, &owner.public_key()).unwrap();
        assert_eq!(e.node_pubkey, node.public_key().to_hex());
        assert!(validate_enrollment(&ev, &Keys::generate().public_key()).is_err());
    }
}
