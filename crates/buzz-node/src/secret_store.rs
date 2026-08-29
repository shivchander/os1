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
//! v1 LIMITATION: this module only provides the store + the name/key
//! mapping. Nothing yet reads a configured provider's secret back out at
//! agent-spawn time — that consumption (e.g. threading it into
//! `runtime::build_child_env`'s injected environment) is follow-on work.
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
}
