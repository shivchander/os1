//! Shared codec helpers for execution-node event kinds.
use std::collections::HashSet;
use std::fmt;

use nostr::PublicKey;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use thiserror::Error;

/// Errors returned by the execution-node event codecs.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    /// The signed outer event is malformed or has the wrong author/kind/tags.
    #[error("invalid envelope: {0}")]
    InvalidEnvelope(String),
    /// Ciphertext could not be authenticated/decrypted. Deliberately redacted.
    #[error("payload could not be decrypted")]
    Decrypt,
    /// Decrypted or public payload is malformed or semantically invalid.
    #[error("invalid payload: {0}")]
    InvalidPayload(String),
    /// Encryption failed.
    #[error("encryption failed")]
    Encrypt,
    /// Event signing failed.
    #[error("signing failed")]
    Sign,
}

/// Parse an RFC3339 timestamp string, returning an error labeled with `label`.
pub(crate) fn parse_rfc3339(label: &str, value: &str) -> Result<(), CodecError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|_| ())
        .map_err(|_| CodecError::InvalidPayload(format!("{label} must be RFC3339")))
}

/// Parse and validate a canonical (lowercase hex, on-curve) Nostr public key.
pub(crate) fn parse_canonical_pubkey(label: &str, value: &str) -> Result<PublicKey, CodecError> {
    parse_lower_hex_32(label, value)?;
    let key = PublicKey::from_hex(value)
        .map_err(|_| CodecError::InvalidEnvelope(format!("invalid {label}")))?;
    key.xonly()
        .map_err(|_| CodecError::InvalidEnvelope(format!("invalid {label} curve point")))?;
    Ok(key)
}

/// Validate that `value` is exactly 64 lowercase hex characters.
pub(crate) fn parse_lower_hex_32(label: &str, value: &str) -> Result<(), CodecError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(CodecError::InvalidEnvelope(format!(
            "{label} must be 64 lowercase hex chars"
        )));
    }
    Ok(())
}

/// Strictly parse JSON bytes, rejecting duplicate object keys and non-finite floats.
pub(crate) fn parse_strict_json(bytes: &[u8]) -> Result<Value, CodecError> {
    struct StrictValue;
    impl<'de> DeserializeSeed<'de> for StrictValue {
        type Value = Value;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
            d.deserialize_any(self)
        }
    }
    impl<'de> Visitor<'de> for StrictValue {
        type Value = Value;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("valid JSON with unique object keys")
        }
        fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
            Ok(Value::Bool(v))
        }
        fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
            Ok(Value::Number(v.into()))
        }
        fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
            Ok(Value::Number(v.into()))
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Value, E> {
            serde_json::Number::from_f64(v)
                .map(Value::Number)
                .ok_or_else(|| E::custom("non-finite float"))
        }
        fn visit_str<E>(self, v: &str) -> Result<Value, E> {
            Ok(Value::String(v.to_owned()))
        }
        fn visit_string<E>(self, v: String) -> Result<Value, E> {
            Ok(Value::String(v))
        }
        fn visit_unit<E>(self) -> Result<Value, E> {
            Ok(Value::Null)
        }
        fn visit_none<E>(self) -> Result<Value, E> {
            Ok(Value::Null)
        }
        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
            d.deserialize_any(self)
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
            let mut out = Vec::new();
            while let Some(value) = seq.next_element_seed(StrictValue)? {
                out.push(value);
            }
            Ok(Value::Array(out))
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
            let mut seen = HashSet::new();
            let mut out = serde_json::Map::new();
            while let Some(key) = map.next_key::<String>()? {
                if !seen.insert(key.clone()) {
                    return Err(serde::de::Error::custom(format!("duplicate key: {key}")));
                }
                out.insert(key, map.next_value_seed(StrictValue)?);
            }
            Ok(Value::Object(out))
        }
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = StrictValue
        .deserialize(&mut deserializer)
        .map_err(|e| CodecError::InvalidPayload(e.to_string()))?;
    deserializer
        .end()
        .map_err(|e| CodecError::InvalidPayload(e.to_string()))?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::Keys;
    #[test]
    fn canonical_pubkey_accepts_generated_key() {
        let pk = Keys::generate().public_key().to_hex();
        assert!(parse_canonical_pubkey("k", &pk).is_ok());
        assert!(parse_canonical_pubkey("k", "XYZ").is_err());
    }
}
