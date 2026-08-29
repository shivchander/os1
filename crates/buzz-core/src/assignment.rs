//! `AGENT_ASSIGNMENT` codec: owner-signed, node-encrypted agent→node assignment.
//!
//! The signed outer event exposes only the agent coordinate, target node, and
//! desired lifecycle state as public tags — any of the owner's nodes can
//! decide whether they are the target without decrypting anything. Only the
//! target node holds the private key needed to decrypt `content`, which
//! carries the agent's nsec and launch contract via NIP-44.
use std::collections::BTreeMap;
use std::fmt;

use nostr::nips::nip44::{self, Version};
use nostr::{Event, EventBuilder, Keys, Kind, PublicKey, Tag};
use serde::{Deserialize, Serialize};

use crate::kind::KIND_AGENT_ASSIGNMENT;
use crate::node_codec::{parse_canonical_pubkey, parse_strict_json, CodecError};

/// Wire-format discriminator for decrypted assignment payloads.
pub const FORMAT: &str = "buzz-agent-assignment-v1";
/// Current assignment payload schema version.
pub const VERSION: u32 = 1;

/// Desktop-resolved launch contract used to start an agent process on a node.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchBlock {
    /// Executable to launch (e.g. `claude`, `goose`).
    pub command: String,
    /// Explicit CLI arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Plain environment overrides.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Policy-controlled environment overrides.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub policy_env: BTreeMap<String, String>,
    /// Owning user's pubkey, when applicable to the launch contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_pubkey: Option<String>,
}

impl fmt::Debug for LaunchBlock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LaunchBlock")
            .field("command", &self.command)
            .field("args", &self.args)
            .field("env", &"<redacted>")
            .field("policy_env", &"<redacted>")
            .field("owner_pubkey", &self.owner_pubkey)
            .finish()
    }
}

/// Decrypted assignment secret: the agent's private key and launch contract,
/// readable only by the target node.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentSecret {
    /// Always [`FORMAT`].
    pub format: String,
    /// Always [`VERSION`].
    pub version: u32,
    /// Agent pubkey and event `d` coordinate.
    pub agent_pubkey: String,
    /// Owner pubkey and signed event author.
    pub owner_pubkey: String,
    /// Target node pubkey; must equal the NIP-44 recipient and `node` tag.
    pub node_pubkey: String,
    /// Agent private key in nsec form.
    pub private_key_nsec: String,
    /// Optional NIP-OA owner attestation JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_tag: Option<String>,
    /// Launch contract used to start the agent process.
    pub launch: LaunchBlock,
    /// Secret environment overrides applied at launch.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env_vars: BTreeMap<String, String>,
    /// Optional idle-reap timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reap_after_idle_seconds: Option<u64>,
}

impl fmt::Debug for AssignmentSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssignmentSecret")
            .field("format", &self.format)
            .field("version", &self.version)
            .field("agent_pubkey", &self.agent_pubkey)
            .field("owner_pubkey", &self.owner_pubkey)
            .field("node_pubkey", &self.node_pubkey)
            .field("private_key_nsec", &"<redacted>")
            .field("auth_tag", &self.auth_tag.as_ref().map(|_| "<redacted>"))
            .field("launch", &self.launch)
            .field("env_vars", &"<redacted>")
            .field("reap_after_idle_seconds", &self.reap_after_idle_seconds)
            .finish()
    }
}

/// Authoritative desired-state repeated in the outer `state` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignState {
    /// The agent is assigned to the target node and should be running.
    Assigned,
    /// The agent has been unassigned from the target node and should stop.
    Unassigned,
}

impl AssignState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Assigned => "assigned",
            Self::Unassigned => "unassigned",
        }
    }
}

/// Validated public metadata from an `AGENT_ASSIGNMENT` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentEnvelope {
    /// Agent pubkey from `d`.
    pub agent_pubkey: PublicKey,
    /// Owner pubkey from the signed event author.
    pub owner_pubkey: PublicKey,
    /// Target node pubkey from `node`.
    pub node_pubkey: PublicKey,
    /// Desired lifecycle state from `state`.
    pub state: AssignState,
}

/// Validate decrypted secret semantics independently of encryption.
fn validate_secret(secret: &AssignmentSecret) -> Result<(), CodecError> {
    if secret.format != FORMAT || secret.version != VERSION {
        return Err(CodecError::InvalidPayload(
            "unsupported format or version".into(),
        ));
    }
    let agent = parse_canonical_pubkey("agent_pubkey", &secret.agent_pubkey)?;
    parse_canonical_pubkey("owner_pubkey", &secret.owner_pubkey)?;
    parse_canonical_pubkey("node_pubkey", &secret.node_pubkey)?;
    let agent_keys = Keys::parse(secret.private_key_nsec.trim())
        .map_err(|_| CodecError::InvalidPayload("invalid agent nsec".into()))?;
    if agent_keys.public_key() != agent {
        return Err(CodecError::InvalidPayload(
            "agent nsec does not derive agent_pubkey".into(),
        ));
    }
    Ok(())
}

/// Build an owner-signed `AGENT_ASSIGNMENT` event, NIP-44 encrypting `secret`
/// to the target node.
pub fn build_assignment(
    owner_keys: &Keys,
    node_pubkey: &PublicKey,
    secret: &AssignmentSecret,
    state: AssignState,
    created_at: u64,
) -> Result<Event, CodecError> {
    validate_secret(secret)?;
    if secret.owner_pubkey != owner_keys.public_key().to_hex() {
        return Err(CodecError::InvalidPayload(
            "owner_pubkey does not match signing key".into(),
        ));
    }
    if secret.node_pubkey != node_pubkey.to_hex() {
        return Err(CodecError::InvalidPayload(
            "node_pubkey does not match target".into(),
        ));
    }
    let plaintext = serde_json::to_string(secret).map_err(|_| CodecError::Encrypt)?;
    let ciphertext = nip44::encrypt(owner_keys.secret_key(), node_pubkey, plaintext, Version::V2)
        .map_err(|_| CodecError::Encrypt)?;
    let tags = vec![
        Tag::parse(["d", secret.agent_pubkey.as_str()]).map_err(|_| CodecError::Sign)?,
        Tag::parse(["node", secret.node_pubkey.as_str()]).map_err(|_| CodecError::Sign)?,
        Tag::parse(["state", state.as_str()]).map_err(|_| CodecError::Sign)?,
    ];
    EventBuilder::new(Kind::Custom(KIND_AGENT_ASSIGNMENT as u16), ciphertext)
        .tags(tags)
        .custom_created_at(nostr::Timestamp::from(created_at))
        .sign_with_keys(owner_keys)
        .map_err(|_| CodecError::Sign)
}

/// Validate a signed outer envelope before any decryption.
pub fn validate_envelope(
    event: &Event,
    expected_owner: &PublicKey,
) -> Result<AssignmentEnvelope, CodecError> {
    if event.kind.as_u16() as u32 != KIND_AGENT_ASSIGNMENT {
        return Err(CodecError::InvalidEnvelope("wrong kind".into()));
    }
    if &event.pubkey != expected_owner {
        return Err(CodecError::InvalidEnvelope(
            "author is not expected owner".into(),
        ));
    }
    if !event.verify_id() || !event.verify_signature() {
        return Err(CodecError::InvalidEnvelope(
            "invalid event id or signature".into(),
        ));
    }

    let mut d = None;
    let mut node = None;
    let mut state = None;
    for tag in event.tags.iter() {
        let parts = tag.as_slice();
        if parts.len() != 2 {
            return Err(CodecError::InvalidEnvelope(
                "every tag must have exactly one value".into(),
            ));
        }
        let slot = match parts[0].as_str() {
            "d" => &mut d,
            "node" => &mut node,
            "state" => &mut state,
            name => return Err(CodecError::InvalidEnvelope(format!("unexpected tag: {name}"))),
        };
        if slot.replace(parts[1].clone()).is_some() {
            return Err(CodecError::InvalidEnvelope(format!(
                "duplicate {} tag",
                parts[0]
            )));
        }
    }

    let agent_pubkey = parse_canonical_pubkey(
        "d",
        d.as_deref()
            .ok_or_else(|| CodecError::InvalidEnvelope("missing d tag".into()))?,
    )?;
    let node_pubkey = parse_canonical_pubkey(
        "node",
        node.as_deref()
            .ok_or_else(|| CodecError::InvalidEnvelope("missing node tag".into()))?,
    )?;
    let state = match state.as_deref() {
        Some("assigned") => AssignState::Assigned,
        Some("unassigned") => AssignState::Unassigned,
        Some(_) => return Err(CodecError::InvalidEnvelope("invalid state tag".into())),
        None => return Err(CodecError::InvalidEnvelope("missing state tag".into())),
    };
    Ok(AssignmentEnvelope {
        agent_pubkey,
        owner_pubkey: *expected_owner,
        node_pubkey,
        state,
    })
}

/// Validate, target-node decrypt, strictly parse, and cross-check an
/// `AGENT_ASSIGNMENT` event. Fails closed unless `node_keys` is the event's
/// target node.
pub fn decrypt_for_node(
    event: &Event,
    node_keys: &Keys,
    expected_owner: &PublicKey,
) -> Result<(AssignmentEnvelope, AssignmentSecret), CodecError> {
    let envelope = validate_envelope(event, expected_owner)?;
    if node_keys.public_key() != envelope.node_pubkey {
        return Err(CodecError::InvalidEnvelope("not the target node".into()));
    }
    let plaintext = nip44::decrypt(node_keys.secret_key(), expected_owner, &event.content)
        .map_err(|_| CodecError::Decrypt)?;
    let value = parse_strict_json(plaintext.as_bytes())?;
    let secret: AssignmentSecret =
        serde_json::from_value(value).map_err(|e| CodecError::InvalidPayload(format!("schema: {e}")))?;
    validate_secret(&secret)?;
    if secret.agent_pubkey != envelope.agent_pubkey.to_hex()
        || secret.owner_pubkey != envelope.owner_pubkey.to_hex()
        || secret.node_pubkey != envelope.node_pubkey.to_hex()
    {
        return Err(CodecError::InvalidPayload(
            "outer/inner metadata mismatch".into(),
        ));
    }
    Ok((envelope, secret))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{Keys, ToBech32};
    use std::collections::BTreeMap;

    fn secret(owner: &Keys, agent: &Keys, node: &Keys) -> AssignmentSecret {
        AssignmentSecret {
            format: FORMAT.into(), version: VERSION,
            agent_pubkey: agent.public_key().to_hex(),
            owner_pubkey: owner.public_key().to_hex(),
            node_pubkey: node.public_key().to_hex(),
            private_key_nsec: agent.secret_key().to_bech32().unwrap(),
            auth_tag: None,
            launch: LaunchBlock {
                command: "claude".into(), args: vec![],
                env: BTreeMap::new(), policy_env: BTreeMap::new(),
                owner_pubkey: Some(owner.public_key().to_hex()),
            },
            env_vars: BTreeMap::from([("FOO".into(), "bar".into())]),
            reap_after_idle_seconds: None,
        }
    }

    #[test]
    fn assignment_round_trips_on_target_node() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let s = secret(&owner, &agent, &node);
        let ev = build_assignment(&owner, &node.public_key(), &s, AssignState::Assigned, 1_785_780_000).unwrap();
        // Any node can read the public envelope:
        let env = validate_envelope(&ev, &owner.public_key()).unwrap();
        assert_eq!(env.node_pubkey, node.public_key());
        assert_eq!(env.state, AssignState::Assigned);
        // Only the target node can decrypt the secret:
        let (_, got) = decrypt_for_node(&ev, &node, &owner.public_key()).unwrap();
        assert_eq!(got, s);
    }

    #[test]
    fn non_target_node_cannot_decrypt() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let ev = build_assignment(&owner, &node.public_key(), &secret(&owner, &agent, &node), AssignState::Assigned, 1_785_780_000).unwrap();
        let stranger = Keys::generate();
        assert!(matches!(decrypt_for_node(&ev, &stranger, &owner.public_key()), Err(CodecError::InvalidEnvelope(_)) | Err(CodecError::Decrypt)));
    }

    #[test]
    fn wrong_owner_rejected() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let ev = build_assignment(&owner, &node.public_key(), &secret(&owner, &agent, &node), AssignState::Assigned, 1_785_780_000).unwrap();
        assert!(validate_envelope(&ev, &Keys::generate().public_key()).is_err());
    }

    #[test]
    fn debug_redacts_nsec() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let s = secret(&owner, &agent, &node);
        let nsec = s.private_key_nsec.clone();
        let dbg = format!("{s:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains(&nsec));
        assert!(!dbg.contains("bar"));
    }

    #[test]
    fn nsec_must_derive_agent_pubkey() {
        let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
        let mut s = secret(&owner, &agent, &node);
        s.private_key_nsec = Keys::generate().secret_key().to_bech32().unwrap();
        let ev = build_assignment(&owner, &node.public_key(), &s, AssignState::Assigned, 1_785_780_000);
        assert!(matches!(ev, Err(CodecError::InvalidPayload(_))));
    }
}
