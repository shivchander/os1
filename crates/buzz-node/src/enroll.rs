//! Node enrollment: keypair bootstrap via an OS-keychain-backed
//! [`SecretStore`], a human-facing pairing code, and the first-enrollment
//! handshake that discovers and pins this node's owner.
//!
//! At enrollment time the node does not yet know its owner's pubkey — that
//! is exactly what enrollment discovers. Trust is established by (a) the
//! cryptographic self-consistency [`accept_enrollment`] checks on the
//! owner-signed `NODE_ENROLLMENT` event, and (b) the human confirming the
//! printed [`pairing_code`] out-of-band between this node and the app. This
//! is why [`enroll`] cannot be built on [`crate::nostr_relay::NostrNodeRelay`]
//! (which requires a known `owner_pubkey` at construction): it opens its own
//! short-lived connection instead.
use std::path::{Path, PathBuf};
use std::time::Duration;

use buzz_core::kind::KIND_NODE_ENROLLMENT;
use buzz_core::node::{validate_enrollment, Enrollment, NodeCapabilities};
use buzz_ws_client::{NostrWsConnection, RelayMessage, WsClientError};
use nostr::{Alphabet, Event, Filter, Keys, Kind, PublicKey, SingleLetterTag, ToBech32};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::model::NodeError;

/// Persisted node identity + relay wiring. Serialized to
/// `~/.buzz-node/config.json` (mode `0600` on Unix) by [`save_node_config`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeConfig {
    /// This node's own pubkey (hex).
    pub node_pubkey: String,
    /// The enrolling owner's pubkey (hex), discovered at enrollment time.
    pub owner_pubkey: String,
    /// The relay this node dials out to.
    pub relay_url: String,
    /// Root directory under which per-agent workspaces are created.
    pub workspace_root: PathBuf,
}

/// Unambiguous alphabet for human-typed pairing codes: excludes `0`/`O` and
/// `1`/`I`/`L` to avoid transcription errors.
const PAIRING_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
/// Number of characters in a generated pairing code.
const PAIRING_CODE_LEN: usize = 8;

/// Generate a human-typeable pairing code for the human to visually confirm
/// between this node's terminal output and the app's enrollment prompt.
///
/// This is a UX guard against approving the wrong physical machine, NOT the
/// cryptographic root of trust — see [`accept_enrollment`] for what actually
/// establishes trust. The code itself is never transmitted to, or checked
/// by, the relay/app/protocol; it exists purely for a human to eyeball on
/// both ends, so it cannot by itself prevent a third party from racing the
/// real owner's approval (see the HARDENING FOLLOW-UP on
/// [`accept_enrollment`]).
pub fn pairing_code() -> String {
    let mut rng = rand::rng();
    (0..PAIRING_CODE_LEN)
        .map(|_| {
            let idx = rng.random_range(0..PAIRING_ALPHABET.len());
            PAIRING_ALPHABET[idx] as char
        })
        .collect()
}

/// Abstraction over persistent secret storage, so tests can substitute an
/// in-memory implementation instead of the real OS keychain — which is
/// unavailable headless/in CI and must never be required by a unit test.
pub trait SecretStore: Send + Sync {
    /// Fetch a previously stored secret, or `Ok(None)` if absent.
    fn get(&self, key: &str) -> Result<Option<String>, NodeError>;
    /// Store (overwriting) a secret.
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError>;
}

/// Keychain service name under which every [`KeychainStore`] entry is filed.
const KEYCHAIN_SERVICE: &str = "buzz-node";

/// The real OS keychain, via the `keyring` crate. Never falls back to a
/// plaintext file — the OpenAgents launcher's plaintext `~/.openagents/env/*.env`
/// is the cautionary tale this deliberately avoids.
pub struct KeychainStore;

impl SecretStore for KeychainStore {
    fn get(&self, key: &str) -> Result<Option<String>, NodeError> {
        let entry = keyring::Entry::new(KEYCHAIN_SERVICE, key)
            .map_err(|e| NodeError::Config(format!("keychain entry for {key}: {e}")))?;
        match entry.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(NodeError::Config(format!("keychain read for {key}: {e}"))),
        }
    }

    fn set(&self, key: &str, value: &str) -> Result<(), NodeError> {
        let entry = keyring::Entry::new(KEYCHAIN_SERVICE, key)
            .map_err(|e| NodeError::Config(format!("keychain entry for {key}: {e}")))?;
        entry
            .set_password(value)
            .map_err(|e| NodeError::Config(format!("keychain write for {key}: {e}")))
    }
}

/// In-memory [`SecretStore`] for tests. Never touches the real keychain, so
/// unit tests can exercise [`load_or_create_node_keys`] headlessly.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
pub struct InMemorySecretStore(std::sync::Mutex<std::collections::BTreeMap<String, String>>);

#[cfg(any(test, feature = "test-utils"))]
impl SecretStore for InMemorySecretStore {
    fn get(&self, key: &str) -> Result<Option<String>, NodeError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned())
    }

    fn set(&self, key: &str, value: &str) -> Result<(), NodeError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
}

/// Keychain/store key under which the node's own Nostr keypair (nsec) lives.
const NODE_KEY_ENTRY: &str = "node-key";

/// Load this node's persistent keypair from `store`, generating and storing
/// a new one on first run. The nsec is held only in memory afterward — it is
/// never written to a file (see the module-level docs).
pub fn load_or_create_node_keys(store: &dyn SecretStore) -> Result<Keys, NodeError> {
    match store.get(NODE_KEY_ENTRY)? {
        Some(nsec) => Keys::parse(nsec.trim())
            .map_err(|e| NodeError::Config(format!("stored node key is invalid: {e}"))),
        None => {
            let keys = Keys::generate();
            let nsec = keys
                .secret_key()
                .to_bech32()
                .map_err(|e| NodeError::Config(format!("encode node nsec: {e}")))?;
            store.set(NODE_KEY_ENTRY, &nsec)?;
            Ok(keys)
        }
    }
}

/// Default path for the persisted [`NodeConfig`]: `~/.buzz-node/config.json`.
pub fn config_path() -> Result<PathBuf, NodeError> {
    let home = dirs::home_dir()
        .ok_or_else(|| NodeError::Config("could not resolve home directory".into()))?;
    Ok(home.join(".buzz-node").join("config.json"))
}

/// Persist `cfg` as JSON to `path` (mode `0600` on Unix), creating parent
/// directories as needed. Takes an explicit path (rather than always using
/// [`config_path`]) so tests can round-trip through a tempdir without
/// touching the real home directory.
pub fn save_node_config_to(path: &Path, cfg: &NodeConfig) -> Result<(), NodeError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| NodeError::Config(format!("create config dir: {e}")))?;
    }
    let json = serde_json::to_string_pretty(cfg)
        .map_err(|e| NodeError::Config(format!("serialize node config: {e}")))?;
    std::fs::write(path, json).map_err(|e| NodeError::Config(format!("write node config: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| NodeError::Config(format!("chmod node config: {e}")))?;
    }
    Ok(())
}

/// Persist `cfg` to the default path ([`config_path`]).
pub fn save_node_config(cfg: &NodeConfig) -> Result<(), NodeError> {
    save_node_config_to(&config_path()?, cfg)
}

/// Load a previously persisted [`NodeConfig`] from `path`, or `Ok(None)` if
/// it does not exist yet (first run). See [`save_node_config_to`] for why
/// this takes an explicit path.
pub fn load_node_config_from(path: &Path) -> Result<Option<NodeConfig>, NodeError> {
    match std::fs::read_to_string(path) {
        Ok(json) => serde_json::from_str(&json)
            .map(Some)
            .map_err(|e| NodeError::Config(format!("parse node config: {e}"))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(NodeError::Config(format!("read node config: {e}"))),
    }
}

/// Load a previously persisted [`NodeConfig`] from the default path
/// ([`config_path`]).
pub fn load_node_config() -> Result<Option<NodeConfig>, NodeError> {
    load_node_config_from(&config_path()?)
}

/// Accept an owner's `NODE_ENROLLMENT` event for this node, discovering and
/// authenticating the owner from the event's own signature.
///
/// The node has no pre-shared owner identity at enrollment time, so this
/// validates the envelope against its own claimed author (`event.pubkey`)
/// rather than an externally-known owner — [`validate_enrollment`] still
/// proves the signature is valid and self-consistent (its `owner_pubkey`
/// field matches the signer), and this additionally checks the `d` tag
/// names this exact node.
///
/// **This is trust-on-first-use (TOFU):** the *first* well-formed,
/// correctly-targeted `NODE_ENROLLMENT` this function accepts wins and pins
/// the owner for good; thereafter the node only acts on commands from that
/// pinned owner. The human confirming the printed [`pairing_code`]
/// out-of-band is a UX guard against approving the wrong physical machine —
/// it is NOT part of this function's trust decision, and the code is never
/// transmitted to or checked here. Concretely, that means any authenticated
/// community member could race a self-signed `NODE_ENROLLMENT` for this
/// node's pubkey before the intended owner approves, and whichever one
/// reaches this function first wins ("first-consistent-event-wins" TOFU).
///
/// **Accepted as a documented v1 risk**, not an oversight: the owner
/// controls community membership, and this matches the design's
/// TOFU/sovereignty stance
/// (`docs/superpowers/specs/2026-08-29-execution-nodes-design.md` §9/§12).
///
/// HARDENING FOLLOW-UP: make the pairing code load-bearing instead of
/// cosmetic — add a code/HMAC field to `NODE_ENROLLMENT`, emitted by the
/// desktop app once the human confirms the code, and have this function
/// reject an otherwise-valid enrollment whose code doesn't match.
///
/// Pure and I/O-free: safe to unit test with events built by
/// [`buzz_core::node::build_enrollment`].
pub fn accept_enrollment(event: &Event, node_pubkey: &PublicKey) -> Result<Enrollment, NodeError> {
    let enrollment = validate_enrollment(event, &event.pubkey)
        .map_err(|e| NodeError::Config(format!("invalid enrollment: {e}")))?;
    if enrollment.node_pubkey != node_pubkey.to_hex() {
        return Err(NodeError::Config(
            "enrollment targets a different node".into(),
        ));
    }
    Ok(enrollment)
}

/// Subscription id for the one-shot enrollment wait in [`enroll`].
const ENROLL_SUB_ID: &str = "buzz-node-enrollment";
/// How long a single read waits before looping back to poll again.
const ENROLL_READ_POLL_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall deadline for [`enroll`]: bounds how long the process blocks
/// waiting for the owner's approval before giving up.
const ENROLLMENT_TIMEOUT: Duration = Duration::from_secs(600);

/// Enroll this node with an owner: publish `NODE_ANNOUNCE`, print a pairing
/// code for the human to confirm in the app, wait (up to
/// [`ENROLLMENT_TIMEOUT`]) for the owner's `NODE_ENROLLMENT`, validate it,
/// and persist the resulting [`NodeConfig`] (via [`save_node_config`]).
///
/// Trust-on-first-use: the first `NODE_ENROLLMENT` [`accept_enrollment`]
/// accepts wins and pins the owner. See that function's doc comment for why
/// this (and the printed pairing code's purely cosmetic role in it) is a
/// deliberate, documented v1 tradeoff rather than an oversight, and for the
/// hardening follow-up that would make the code load-bearing.
///
/// Requires a live relay — this whole function is I/O and is not unit
/// tested; see the `#[ignore]`d `live_enroll_round_trip` test, which drives
/// both the node and owner sides against a real relay. The pure validation
/// it relies on ([`accept_enrollment`]) is fully unit tested.
pub async fn enroll(
    relay_url: &str,
    node_keys: &Keys,
    caps: &NodeCapabilities,
) -> Result<NodeConfig, NodeError> {
    let mut conn = NostrWsConnection::connect_authenticated(relay_url, node_keys, None)
        .await
        .map_err(|e| NodeError::Relay(format!("connect: {e}")))?;

    let announce = buzz_core::node::build_announce(node_keys, caps, crate::nostr_relay::now_unix())
        .map_err(|e| NodeError::Config(format!("build announce event: {e}")))?;
    let ok = conn
        .send_event(announce)
        .await
        .map_err(|e| NodeError::Relay(format!("publish announce: {e}")))?;
    if !ok.accepted {
        return Err(NodeError::Relay(format!(
            "announce rejected by relay: {}",
            ok.message
        )));
    }

    let node_pubkey = node_keys.public_key();
    let node_pubkey_hex = node_pubkey.to_hex();
    eprintln!(
        "Pairing code: {}\nNode pubkey: {node_pubkey_hex}\nApprove this node in the app to continue.",
        pairing_code()
    );

    let filter = Filter::new()
        .kind(Kind::Custom(KIND_NODE_ENROLLMENT as u16))
        .custom_tag(
            SingleLetterTag::lowercase(Alphabet::D),
            node_pubkey_hex.as_str(),
        );
    conn.send_raw(&json!(["REQ", ENROLL_SUB_ID, filter]))
        .await
        .map_err(|e| NodeError::Relay(format!("subscribe for enrollment: {e}")))?;

    // First well-formed, correctly-targeted NODE_ENROLLMENT wins here
    // (trust-on-first-use) — the pairing code printed above is a human UX
    // guard only and is not checked in this loop. See `accept_enrollment`'s
    // doc comment for why that's an accepted v1 risk and the hardening
    // follow-up that would make the code load-bearing instead.
    let wait_for_enrollment = async {
        loop {
            match conn.next_event(ENROLL_READ_POLL_TIMEOUT).await {
                Ok(RelayMessage::Event {
                    subscription_id,
                    event,
                }) if subscription_id == ENROLL_SUB_ID => {
                    if let Ok(enrollment) = accept_enrollment(&event, &node_pubkey) {
                        return Ok(enrollment);
                    }
                    // Malformed, mistargeted, or not-yet-trusted — keep waiting.
                }
                Ok(_other) => {} // EOSE/OK/NOTICE/AUTH/CLOSED/unrelated — keep waiting
                Err(WsClientError::Timeout) => {} // no news yet; poll again
                Err(e) => {
                    return Err(NodeError::Relay(format!(
                        "enrollment stream read failed: {e}"
                    )))
                }
            }
        }
    };
    let enrollment: Enrollment = tokio::time::timeout(ENROLLMENT_TIMEOUT, wait_for_enrollment)
        .await
        .map_err(|_| {
            NodeError::Config("enrollment timed out waiting for owner approval".into())
        })??;

    let cfg = NodeConfig {
        node_pubkey: node_pubkey_hex,
        owner_pubkey: enrollment.owner_pubkey,
        relay_url: relay_url.to_string(),
        workspace_root: PathBuf::from(&caps.workspace_root),
    };
    save_node_config(&cfg)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::node::build_enrollment;

    // --- pairing_code ---

    #[test]
    fn pairing_code_is_eight_unambiguous_chars() {
        let c = pairing_code();
        assert_eq!(c.len(), PAIRING_CODE_LEN);
        assert!(c.chars().all(|ch| PAIRING_ALPHABET.contains(&(ch as u8))));
    }

    #[test]
    fn pairing_code_is_randomized() {
        // Not an RNG-quality test — just proves it is not a fixed constant.
        let codes: std::collections::HashSet<String> = (0..20).map(|_| pairing_code()).collect();
        assert!(codes.len() > 1, "20 draws should not all collide");
    }

    // --- NodeConfig (pure serde) ---

    fn sample_cfg() -> NodeConfig {
        NodeConfig {
            node_pubkey: "n".into(),
            owner_pubkey: "o".into(),
            relay_url: "wss://r".into(),
            workspace_root: "/tmp/x".into(),
        }
    }

    #[test]
    fn node_config_round_trips() {
        let cfg = sample_cfg();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: NodeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cfg);
    }

    // --- NodeConfig file persistence (tempdir — no real home dir touched) ---

    #[test]
    fn save_and_load_node_config_round_trips_via_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("config.json");
        let cfg = sample_cfg();

        save_node_config_to(&path, &cfg).expect("save");
        let loaded = load_node_config_from(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded, cfg);
    }

    #[test]
    fn load_node_config_missing_file_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist.json");
        assert!(load_node_config_from(&path).expect("load").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn save_node_config_is_owner_only_readable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.json");
        save_node_config_to(&path, &sample_cfg()).expect("save");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // --- SecretStore / load_or_create_node_keys (in-memory — no real keychain) ---

    #[test]
    fn load_or_create_node_keys_generates_then_reuses() {
        let store = InMemorySecretStore::default();
        let first = load_or_create_node_keys(&store).expect("generate");
        let second = load_or_create_node_keys(&store).expect("reuse");
        assert_eq!(first.public_key(), second.public_key());
    }

    #[test]
    fn load_or_create_node_keys_rejects_corrupt_stored_key() {
        let store = InMemorySecretStore::default();
        store.set(NODE_KEY_ENTRY, "not-an-nsec").expect("set");
        assert!(load_or_create_node_keys(&store).is_err());
    }

    // --- accept_enrollment (pure) ---

    #[test]
    fn accept_enrollment_for_target_node() {
        let (owner, node) = (Keys::generate(), Keys::generate());
        let ev = build_enrollment(&owner, &node.public_key(), 1_785_780_000).unwrap();
        let e = accept_enrollment(&ev, &node.public_key()).expect("accept");
        assert_eq!(e.owner_pubkey, owner.public_key().to_hex());
        assert_eq!(e.node_pubkey, node.public_key().to_hex());
    }

    #[test]
    fn accept_enrollment_rejects_event_for_a_different_node() {
        let (owner, node, other) = (Keys::generate(), Keys::generate(), Keys::generate());
        let ev = build_enrollment(&owner, &node.public_key(), 1_785_780_000).unwrap();
        assert!(accept_enrollment(&ev, &other.public_key()).is_err());
    }

    // --- enroll (live I/O — requires a real relay) ---

    /// Requires a running relay. Run with:
    ///   `BUZZ_TEST_RELAY_URL=ws://localhost:3000 cargo test -p buzz-node --lib -- --ignored enroll::tests::live_`
    #[tokio::test]
    #[ignore = "requires a running relay; set BUZZ_TEST_RELAY_URL (see crates/buzz-test-client)"]
    async fn live_enroll_round_trip() {
        let relay_url = std::env::var("BUZZ_TEST_RELAY_URL").expect("set BUZZ_TEST_RELAY_URL");
        let node_keys = Keys::generate();
        let owner_keys = Keys::generate();
        let node_pubkey_hex = node_keys.public_key().to_hex();

        let caps = NodeCapabilities {
            format: buzz_core::node::FORMAT.into(),
            version: buzz_core::node::VERSION,
            node_pubkey: node_pubkey_hex.clone(),
            os: "test".into(),
            runtimes: vec![],
            workspace_root: std::env::temp_dir()
                .join("buzz-node-enroll-test")
                .to_string_lossy()
                .into_owned(),
            max_agents: None,
        };

        let relay_url_for_task = relay_url.clone();
        let node_keys_for_task = node_keys.clone();
        let enroll_task =
            tokio::spawn(
                async move { enroll(&relay_url_for_task, &node_keys_for_task, &caps).await },
            );

        // Give the node a moment to announce + subscribe before the owner publishes.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut owner_conn =
            NostrWsConnection::connect_authenticated(&relay_url, &owner_keys, None)
                .await
                .expect("owner connect");
        let enrollment_event = build_enrollment(
            &owner_keys,
            &node_keys.public_key(),
            crate::nostr_relay::now_unix(),
        )
        .expect("build enrollment");
        let ok = owner_conn
            .send_event(enrollment_event)
            .await
            .expect("publish enrollment");
        assert!(ok.accepted, "relay rejected enrollment: {}", ok.message);

        let cfg = tokio::time::timeout(Duration::from_secs(30), enroll_task)
            .await
            .expect("enroll task timed out")
            .expect("enroll task panicked")
            .expect("enroll failed");

        assert_eq!(cfg.node_pubkey, node_pubkey_hex);
        assert_eq!(cfg.owner_pubkey, owner_keys.public_key().to_hex());
    }
}
