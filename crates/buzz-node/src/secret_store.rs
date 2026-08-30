//! At-rest secret storage for provider API keys (OS keychain). Agent nsecs
//! are never persisted here — they stay in memory only, injected into the
//! harness environment straight from the assignment secret at spawn time
//! (spec §9, §12; see `runtime::build_child_env`).
//!
//! Distinct from [`crate::enroll::SecretStore`], which stores this node's own
//! Nostr signing key under one fixed, well-known entry. This store holds an
//! open-ended set of *provider* API keys (Anthropic, OpenAI, ...) referenced
//! by name from the persisted [`crate::enroll::NodeConfig`] — the on-disk
//! config carries only the provider name (see [`provider_secret_key`]); the
//! key value lives here, never on disk.
//!
//! [`resolve_provider_secret_store`] chooses the concrete backend
//! (OS keychain, or a `0600` [`FileProviderSecretStore`] on headless
//! nodes — mirrors [`crate::enroll::resolve_secret_store`]'s exact
//! selection logic), and [`provider_env_var`] maps a provider name to the
//! environment variable an agent harness expects it under.
//! `daemon::up_foreground` is the consumer: it resolves each name in
//! [`crate::enroll::NodeConfig::providers`] back out to its stored secret
//! and folds it into the ACP runtime's `node_env` base layer
//! (`runtime::build_child_env`'s injected environment).
use std::path::PathBuf;

use crate::model::NodeError;

/// Default keychain service namespace for provider secrets — distinct from
/// `enroll`'s `"buzz-node"` (used for the node's own signing key) so the two
/// families of entry can never collide in the OS keychain.
const DEFAULT_SERVICE: &str = "buzz-node-providers";

/// Derive the keychain key under which a named provider's secret is stored,
/// from the provider name persisted in [`crate::enroll::NodeConfig::providers`]
/// (e.g. `"anthropic"` -> `"provider:anthropic"`). Centralizing this mapping
/// here keeps the on-disk config (names only) and the keychain (values) tied
/// together by one canonical scheme instead of an ad hoc one at each call
/// site.
pub fn provider_secret_key(provider: &str) -> String {
    format!("provider:{provider}")
}

/// Store/retrieve named provider secrets (e.g. LLM API keys). The on-disk
/// [`crate::enroll::NodeConfig`] references a secret by name only; the value
/// lives here, never in plaintext on disk.
pub trait ProviderSecretStore: Send + Sync {
    /// Persist a secret under `key`, overwriting any existing value.
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError>;
    /// Fetch a secret; `Ok(None)` if absent.
    fn get(&self, key: &str) -> Result<Option<String>, NodeError>;
    /// Remove a secret; idempotent (`Ok(())` even if it was already absent).
    fn delete(&self, key: &str) -> Result<(), NodeError>;
}

/// OS-keychain-backed store (`keyring` crate: Keychain/Credential
/// Manager/Secret Service — the same backend as
/// [`crate::enroll::KeychainStore`]), one entry per key. Uses its own service
/// namespace so provider keys are never filed alongside the node's own
/// signing key.
#[derive(Debug, Clone)]
pub struct KeychainSecretStore {
    /// Keychain service namespace, e.g. `"buzz-node-providers"`.
    pub service: String,
}

impl Default for KeychainSecretStore {
    /// Build a store under [`DEFAULT_SERVICE`].
    fn default() -> Self {
        Self {
            service: DEFAULT_SERVICE.to_string(),
        }
    }
}

impl ProviderSecretStore for KeychainSecretStore {
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError> {
        let entry = keyring::Entry::new(&self.service, key)
            .map_err(|e| NodeError::Secret(format!("keychain entry for {key}: {e}")))?;
        entry
            .set_password(value)
            .map_err(|e| NodeError::Secret(format!("keychain write for {key}: {e}")))
    }
    fn get(&self, key: &str) -> Result<Option<String>, NodeError> {
        let entry = keyring::Entry::new(&self.service, key)
            .map_err(|e| NodeError::Secret(format!("keychain entry for {key}: {e}")))?;
        match entry.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(NodeError::Secret(format!("keychain read for {key}: {e}"))),
        }
    }
    fn delete(&self, key: &str) -> Result<(), NodeError> {
        let entry = keyring::Entry::new(&self.service, key)
            .map_err(|e| NodeError::Secret(format!("keychain entry for {key}: {e}")))?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(NodeError::Secret(format!("keychain delete for {key}: {e}"))),
        }
    }
}

/// In-memory [`ProviderSecretStore`] for tests. Never touches the real OS
/// keychain. Deliberately does NOT derive/implement `Debug` — it holds
/// secret values, and this store must never be printable (constraint: no
/// secrets in `Debug`).
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
pub struct MemorySecretStore(std::sync::Mutex<std::collections::BTreeMap<String, String>>);

#[cfg(any(test, feature = "test-utils"))]
impl ProviderSecretStore for MemorySecretStore {
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<String>, NodeError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned())
    }
    fn delete(&self, key: &str) -> Result<(), NodeError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
        Ok(())
    }
}

/// A `0600`-file-backed [`ProviderSecretStore`] for headless nodes with no
/// OS secret-service, mirroring [`crate::enroll::FileStore`]'s on-disk
/// posture exactly: each secret is one file, mode `0600`, inside a `0700`
/// directory. Uses the same filename sanitization
/// ([`crate::enroll::file_key_name`]) so a provider secret key (e.g.
/// `"provider:anthropic"`) can never escape the store directory.
pub struct FileProviderSecretStore {
    dir: PathBuf,
}

impl FileProviderSecretStore {
    /// Store secrets as `0600` files under `dir`. Explicit-dir constructor
    /// (mirrors [`crate::enroll::FileStore::new`]) so tests can point this
    /// at a tempdir instead of the real node home directory.
    pub fn in_dir(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Store secrets under this node's default provider-secrets directory:
    /// `<node_home_dir>/provider-secrets`.
    pub fn new() -> Result<Self, NodeError> {
        Ok(Self::in_dir(default_provider_secrets_dir()?))
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.dir.join(crate::enroll::file_key_name(key))
    }
}

impl ProviderSecretStore for FileProviderSecretStore {
    fn set(&self, key: &str, value: &str) -> Result<(), NodeError> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|e| NodeError::Secret(format!("create provider secret dir: {e}")))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Best-effort: keep the secrets directory owner-only.
            let _ = std::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700));
        }
        let path = self.path_for(key);
        std::fs::write(&path, value)
            .map_err(|e| NodeError::Secret(format!("write provider secret file {key}: {e}")))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| NodeError::Secret(format!("chmod provider secret file {key}: {e}")))?;
        }
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>, NodeError> {
        match std::fs::read_to_string(self.path_for(key)) {
            Ok(v) => Ok(Some(v.trim().to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(NodeError::Secret(format!(
                "read provider secret file {key}: {e}"
            ))),
        }
    }

    fn delete(&self, key: &str) -> Result<(), NodeError> {
        match std::fs::remove_file(self.path_for(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(NodeError::Secret(format!(
                "delete provider secret file {key}: {e}"
            ))),
        }
    }
}

/// Directory (under [`crate::enroll::node_home_dir`]) for the file-backed
/// provider-secret store. Mirrors `enroll::file_keystore_dir`.
fn default_provider_secrets_dir() -> Result<PathBuf, NodeError> {
    Ok(crate::enroll::node_home_dir()?.join("provider-secrets"))
}

/// Canary key used only to probe keychain availability in
/// [`resolve_provider_secret_store`] (mirrors
/// [`crate::enroll::resolve_secret_store`]'s probe against its own
/// `NODE_KEY_ENTRY`) — the exact key name is arbitrary, since `Ok(None)`
/// already proves the backend itself is reachable; only `Err` means
/// unavailable.
const KEYCHAIN_PROBE_KEY: &str = "keychain-probe";

/// Choose the node's [`ProviderSecretStore`]. Mirrors
/// [`crate::enroll::resolve_secret_store`]'s exact selection logic: the OS
/// keychain ([`KeychainSecretStore`]) is the preferred default, but this
/// transparently falls back to a `0600` [`FileProviderSecretStore`] under
/// `~/.buzz-node/provider-secrets` when either (a)
/// [`crate::enroll::FILE_KEYSTORE_ENV`] is set, or (b) a probe of the
/// keychain fails (e.g. the "not activatable" DBus error on a headless
/// Linux box).
pub fn resolve_provider_secret_store() -> Result<Box<dyn ProviderSecretStore>, NodeError> {
    if std::env::var_os(crate::enroll::FILE_KEYSTORE_ENV).is_some() {
        return Ok(Box::new(FileProviderSecretStore::new()?));
    }
    let keychain = KeychainSecretStore::default();
    match keychain.get(KEYCHAIN_PROBE_KEY) {
        Ok(_) => Ok(Box::new(keychain)),
        Err(e) => {
            let dir = default_provider_secrets_dir()?;
            eprintln!(
                "buzz-node: OS keychain unavailable ({e}); falling back to 0600 file provider-secret store at {}",
                dir.display()
            );
            Ok(Box::new(FileProviderSecretStore::in_dir(dir)))
        }
    }
}

/// Map a provider name (case-insensitive) to the environment variable an
/// agent harness expects its API key under. `None` for an unrecognized
/// provider — callers should skip injection rather than guess a name.
pub fn provider_env_var(provider: &str) -> Option<&'static str> {
    match provider.to_ascii_lowercase().as_str() {
        "openai" => Some("OPENAI_API_KEY"),
        "anthropic" => Some("ANTHROPIC_API_KEY"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_round_trips_and_deletes() {
        let s = MemorySecretStore::default();
        assert_eq!(s.get("PROVIDER_ANTHROPIC").unwrap(), None);
        s.set("PROVIDER_ANTHROPIC", "sk-secret").unwrap();
        assert_eq!(
            s.get("PROVIDER_ANTHROPIC").unwrap().as_deref(),
            Some("sk-secret")
        );
        s.delete("PROVIDER_ANTHROPIC").unwrap();
        assert_eq!(s.get("PROVIDER_ANTHROPIC").unwrap(), None);
    }

    #[test]
    fn memory_store_delete_is_idempotent() {
        let s = MemorySecretStore::default();
        s.delete("never-set").unwrap(); // must not error
    }

    #[test]
    fn memory_store_set_overwrites() {
        let s = MemorySecretStore::default();
        s.set("k", "v1").unwrap();
        s.set("k", "v2").unwrap();
        assert_eq!(s.get("k").unwrap().as_deref(), Some("v2"));
    }

    #[test]
    fn provider_secret_key_is_stable_and_name_scoped() {
        assert_eq!(provider_secret_key("anthropic"), "provider:anthropic");
        assert_ne!(
            provider_secret_key("anthropic"),
            provider_secret_key("openai")
        );
    }

    /// The whole point of Task 5: the on-disk `NodeConfig` may name a
    /// provider, but must never carry its secret value.
    #[test]
    fn node_config_serialization_contains_no_secret() {
        let cfg = crate::enroll::NodeConfig::sample_with_provider("anthropic");
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(
            json.contains("anthropic"),
            "the provider NAME is expected on disk"
        );
        assert!(
            !json.to_lowercase().contains("sk-"),
            "no secret material may appear in the on-disk config"
        );
    }

    /// End-to-end contract: a provider name recorded in `NodeConfig` maps to
    /// a `ProviderSecretStore` key that actually round-trips a secret.
    #[test]
    fn provider_name_in_config_resolves_to_a_working_secret_store_key() {
        let cfg = crate::enroll::NodeConfig::sample_with_provider("anthropic");
        let store = MemorySecretStore::default();
        let key = provider_secret_key(&cfg.providers[0]);
        store.set(&key, "sk-live-secret").unwrap();
        assert_eq!(store.get(&key).unwrap().as_deref(), Some("sk-live-secret"));
    }

    // --- provider_env_var ---

    #[test]
    fn provider_env_var_maps_known_providers_case_insensitively() {
        assert_eq!(provider_env_var("openai"), Some("OPENAI_API_KEY"));
        assert_eq!(provider_env_var("OpenAI"), Some("OPENAI_API_KEY"));
        assert_eq!(provider_env_var("OPENAI"), Some("OPENAI_API_KEY"));
        assert_eq!(provider_env_var("anthropic"), Some("ANTHROPIC_API_KEY"));
        assert_eq!(provider_env_var("Anthropic"), Some("ANTHROPIC_API_KEY"));
    }

    #[test]
    fn provider_env_var_is_none_for_an_unrecognized_provider() {
        assert_eq!(provider_env_var("someothervendor"), None);
    }

    // --- FileProviderSecretStore (headless fallback — tempdir, no real keychain/home) ---

    #[test]
    fn file_provider_secret_store_round_trips_missing_returns_none_and_is_owner_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileProviderSecretStore::in_dir(dir.path().join("provider-secrets"));
        let key = provider_secret_key("anthropic");

        assert_eq!(store.get(&key).expect("get"), None);
        store.set(&key, "sk-live-secret").expect("set");
        assert_eq!(
            store.get(&key).expect("get").as_deref(),
            Some("sk-live-secret")
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(
                dir.path()
                    .join("provider-secrets")
                    .join(crate::enroll::file_key_name(&key)),
            )
            .expect("metadata")
            .permissions()
            .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn file_provider_secret_store_delete_is_idempotent_and_removes_the_secret() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FileProviderSecretStore::in_dir(dir.path().join("provider-secrets"));
        let key = provider_secret_key("openai");

        store.set(&key, "sk-live-secret").expect("set");
        store.delete(&key).expect("delete");
        assert_eq!(store.get(&key).expect("get"), None);

        // Idempotent: deleting an already-absent secret must not error.
        store.delete(&key).expect("delete again");
    }
}
