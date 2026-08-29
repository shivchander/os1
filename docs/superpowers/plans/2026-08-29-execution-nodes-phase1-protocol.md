# Execution Nodes — Phase 1: Protocol Foundation (buzz-core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the four new Nostr event kinds and their zero-I/O payload codecs (node announce, node enrollment, agent→node assignment with node-encrypted key delivery, and per-agent node status) to `buzz-core`, so later phases (the `buzz-node` daemon, relay wiring, and desktop UI) have a verified, tested wire contract to build on.

**Architecture:** Each new kind gets a small module in `crates/buzz-core/src/` following the existing owner-encrypted codec pattern in `private_managed_agent.rs` — a typed payload (`serde`, `deny_unknown_fields`, redacted `Debug` for secrets), a `build_event()` that signs (and, for the assignment, NIP-44-encrypts to the target node), a public `validate_envelope()` that checks the signed outer event before any decryption, and `validate_*` semantic checks. `buzz-core` stays zero-I/O; everything here is pure and unit-testable with `Keys::generate()`.

**Tech Stack:** Rust 1.88, `nostr` 0.44 (features `nip44`, `nip98`), `serde`/`serde_json`, `thiserror`, `sha2`, `chrono`. Test with `cargo test -p buzz-core`.

**Spec:** `docs/superpowers/specs/2026-08-29-execution-nodes-design.md` (§7 event model, §8 flows, §12 security). Read it alongside this plan.

## Global Constraints

- **buzz-core is zero-I/O.** No `tokio`, `sqlx`, `redis`, `axum`, or network/disk. Pure types + crypto only (`crates/buzz-core/Cargo.toml`).
- **`#![deny(unsafe_code)]`** (crate-level, already set) — no `unsafe`.
- **No new `unwrap()`/`expect()` in non-test code.** Use `?` and the module's `Error` enum (mirror `private_managed_agent::Error`). `unwrap()` is fine inside `#[cfg(test)]`.
- **`#![warn(missing_docs)]`** (crate-level) — every `pub` item needs a `///` doc comment.
- **Kinds are `u32`** defined in `crates/buzz-core/src/kind.rs`; build events with `Kind::Custom(KIND_X as u16)`.
- **Follow the codec pattern in `crates/buzz-core/src/private_managed_agent.rs`** — reuse its shape for `build_event`/`validate_envelope`/`validate_and_decrypt`, secret redaction in `Debug`, `#[serde(deny_unknown_fields)]`, and the `#[cfg(test)]` round-trip + fail-closed tests. Copy small helpers (`parse_lower_hex_32`, `parse_canonical_pubkey`, `parse_event_id`, `parse_rfc3339`, `parse_strict_json`) into a shared `node_codec` helper module in Task 1 rather than duplicating per file.
- **Secrets never in `Debug` or plaintext tags.** The agent `nsec` travels only inside NIP-44 ciphertext; its struct has a redacted `Debug`.
- **Commit with `git commit -s`** (DCO). One commit per task step that says "commit".
- Run `cargo test -p buzz-core` and `cargo clippy -p buzz-core --all-targets` green before the task's final commit.

---

### Task 1: Reserve kind constants + shared codec helpers

**Files:**
- Modify: `crates/buzz-core/src/kind.rs` (add four `pub const` + doc comments)
- Create: `crates/buzz-core/src/node_codec.rs` (shared parse/validate helpers)
- Modify: `crates/buzz-core/src/lib.rs` (add `pub mod node_codec;`)

**Interfaces:**
- Produces: `KIND_NODE_ANNOUNCE`, `KIND_NODE_ENROLLMENT`, `KIND_AGENT_ASSIGNMENT`, `KIND_AGENT_NODE_STATUS` (`u32`); helper fns `node_codec::{parse_lower_hex_32, parse_canonical_pubkey, parse_event_id, parse_rfc3339, parse_strict_json, content_sha256}` and a shared `node_codec::CodecError` enum (mirrors `private_managed_agent::Error`).

- [ ] **Step 1: Pick collision-free kind numbers.**

Run: `grep -nE 'pub const KIND_[A-Z_]+: u32 = 39[0-9]{3}' crates/buzz-core/src/kind.rs`
Confirm `39500`–`39503` are unused (Buzz uses 39000/39001/39002 for channel metadata/membership). If any collide, use the next free `395xx` values and keep the four consecutive. Record the chosen numbers here in the plan before continuing. All are **parameterized-replaceable** (30000–39999): announce/enrollment keyed by node pubkey, assignment/status keyed by agent pubkey.

- [ ] **Step 2: Add the constants to `kind.rs`.**

```rust
/// Execution node self-announcement (parameterized replaceable, d = node pubkey).
/// Node-authored capabilities + workspace root. Liveness is separate (kind:20001).
pub const KIND_NODE_ANNOUNCE: u32 = 39500;
/// Owner authorization of an execution node (parameterized replaceable, d = node pubkey).
/// Owner-signed trust link; a node acts only on commands from an enrolling owner.
pub const KIND_NODE_ENROLLMENT: u32 = 39501;
/// Agent→node desired-state assignment (parameterized replaceable, d = agent pubkey).
/// Owner-signed; target node in a public `node` tag; the agent nsec + launch env
/// travel NIP-44-encrypted to the target node in `content`.
pub const KIND_AGENT_ASSIGNMENT: u32 = 39502;
/// Observed per-agent status reported by the node running it (parameterized
/// replaceable, d = agent pubkey). Node-authored health + reason code.
pub const KIND_AGENT_NODE_STATUS: u32 = 39503;
```

- [ ] **Step 3: Write the failing test for the constants.**

Add to the bottom of `kind.rs` (create a `#[cfg(test)] mod node_kind_tests` if no test module exists):

```rust
#[cfg(test)]
mod node_kind_tests {
    use super::*;
    #[test]
    fn node_kinds_are_distinct_param_replaceable() {
        let kinds = [
            KIND_NODE_ANNOUNCE, KIND_NODE_ENROLLMENT,
            KIND_AGENT_ASSIGNMENT, KIND_AGENT_NODE_STATUS,
        ];
        for k in kinds {
            assert!((30000..40000).contains(&k), "{k} must be parameterized-replaceable");
        }
        let mut sorted = kinds;
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 4, "node kinds must be distinct");
    }
}
```

- [ ] **Step 4: Run it.** Run: `cargo test -p buzz-core node_kinds_are_distinct -- --nocapture` — Expected: PASS.

- [ ] **Step 5: Create `node_codec.rs` with shared helpers.**

Copy these helpers verbatim from `private_managed_agent.rs` (they are private there — lift them into `node_codec` as `pub(crate)`): `parse_lower_hex_32`, `parse_canonical_pubkey`, `parse_event_id`, `parse_rfc3339`, `parse_strict_json`, `content_sha256`. Define a shared error:

```rust
//! Shared codec helpers for execution-node event kinds.
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
```

(Reuse the exact bodies of the helper fns from `private_managed_agent.rs`, changing their error type to `CodecError`.)

- [ ] **Step 6: Add `pub mod node_codec;` to `lib.rs`** with a doc comment, next to the other `pub mod` lines.

- [ ] **Step 7: Test a helper round-trips.**

```rust
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
```

- [ ] **Step 8: Run + clippy + commit.**

Run: `cargo test -p buzz-core && cargo clippy -p buzz-core --all-targets` — Expected: PASS, no warnings.
```bash
git add crates/buzz-core/src/kind.rs crates/buzz-core/src/node_codec.rs crates/buzz-core/src/lib.rs
git commit -s -m "feat(core): reserve execution-node kinds + shared codec helpers"
```

---

### Task 2: `NODE_ANNOUNCE` + `NODE_ENROLLMENT` codec (`node.rs`)

**Files:**
- Create: `crates/buzz-core/src/node.rs`
- Modify: `crates/buzz-core/src/lib.rs` (`pub mod node;`)

**Interfaces:**
- Consumes: `kind::{KIND_NODE_ANNOUNCE, KIND_NODE_ENROLLMENT}`, `node_codec::{CodecError, parse_canonical_pubkey, parse_strict_json}`.
- Produces:
  - `NodeCapabilities { format, version, node_pubkey, os, runtimes: Vec<String>, workspace_root: String, max_agents: Option<u32> }`
  - `build_announce(node_keys: &Keys, caps: &NodeCapabilities, created_at: u64) -> Result<Event, CodecError>`
  - `validate_announce(event: &Event) -> Result<NodeCapabilities, CodecError>` (verifies the node signed its own announce: `event.pubkey.to_hex() == caps.node_pubkey`)
  - `Enrollment { format, version, node_pubkey, owner_pubkey }`
  - `build_enrollment(owner_keys: &Keys, node_pubkey: &PublicKey, created_at: u64) -> Result<Event, CodecError>`
  - `validate_enrollment(event: &Event, expected_owner: &PublicKey) -> Result<Enrollment, CodecError>`

- [ ] **Step 1: Write failing round-trip tests.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use nostr::Keys;

    #[test]
    fn announce_round_trips_and_binds_author() {
        let node = Keys::generate();
        let caps = NodeCapabilities {
            format: FORMAT.into(), version: VERSION,
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
        let mut caps = NodeCapabilities {
            format: FORMAT.into(), version: VERSION,
            node_pubkey: Keys::generate().public_key().to_hex(), // not the signer
            os: "linux".into(), runtimes: vec![], workspace_root: "/x".into(), max_agents: None,
        };
        let ev = build_announce(&node, &caps, 1_785_780_000).unwrap();
        assert!(matches!(validate_announce(&ev), Err(CodecError::InvalidEnvelope(_))));
        caps.node_pubkey = node.public_key().to_hex(); // silence unused-mut in strict builds
        let _ = caps;
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
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-core node::` — Expected: FAIL (module/types not defined).

- [ ] **Step 3: Implement `node.rs`.**

```rust
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
        return Err(CodecError::InvalidPayload("unsupported format/version".into()));
    }
    if caps.node_pubkey != node_keys.public_key().to_hex() {
        return Err(CodecError::InvalidPayload("node_pubkey != signing key".into()));
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
    let caps: NodeCapabilities = serde_json::from_value(parse_strict_json(event.content.as_bytes())?)
        .map_err(|e| CodecError::InvalidPayload(format!("schema: {e}")))?;
    if caps.format != FORMAT || caps.version != VERSION {
        return Err(CodecError::InvalidPayload("unsupported format/version".into()));
    }
    parse_canonical_pubkey("node_pubkey", &caps.node_pubkey)?;
    if caps.node_pubkey != event.pubkey.to_hex() {
        return Err(CodecError::InvalidEnvelope("node did not sign its own announce".into()));
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
        return Err(CodecError::InvalidEnvelope("author is not expected owner".into()));
    }
    if !event.verify_id() || !event.verify_signature() {
        return Err(CodecError::InvalidEnvelope("invalid id/signature".into()));
    }
    let e: Enrollment = serde_json::from_value(parse_strict_json(event.content.as_bytes())?)
        .map_err(|err| CodecError::InvalidPayload(format!("schema: {err}")))?;
    if e.format != FORMAT || e.version != VERSION {
        return Err(CodecError::InvalidPayload("unsupported format/version".into()));
    }
    parse_canonical_pubkey("node_pubkey", &e.node_pubkey)?;
    if e.owner_pubkey != expected_owner.to_hex() {
        return Err(CodecError::InvalidPayload("owner_pubkey mismatch".into()));
    }
    Ok(e)
}
```

- [ ] **Step 4: Add `pub mod node;` to `lib.rs`** (with a doc comment).

- [ ] **Step 5: Run + clippy.** Run: `cargo test -p buzz-core node:: && cargo clippy -p buzz-core --all-targets` — Expected: PASS, no warnings.

- [ ] **Step 6: Commit.**
```bash
git add crates/buzz-core/src/node.rs crates/buzz-core/src/lib.rs
git commit -s -m "feat(core): NODE_ANNOUNCE + NODE_ENROLLMENT codecs"
```

---

### Task 3: `AGENT_ASSIGNMENT` codec with node-encrypted key delivery (`assignment.rs`)

This is the core of the phase — mirror `private_managed_agent.rs` closely (envelope + NIP-44 payload + cross-check), but encrypt **to the target node**, not owner-self.

**Files:**
- Create: `crates/buzz-core/src/assignment.rs`
- Modify: `crates/buzz-core/src/lib.rs` (`pub mod assignment;`)

**Interfaces:**
- Consumes: `kind::KIND_AGENT_ASSIGNMENT`, `node_codec::*`.
- Produces:
  - `LaunchBlock { command: String, args: Vec<String>, env: BTreeMap<String,String>, policy_env: BTreeMap<String,String>, owner_pubkey: Option<String> }` (the desktop-resolved launch contract, §Launch data in the base spec)
  - `AssignmentSecret { format, version, agent_pubkey, owner_pubkey, node_pubkey, private_key_nsec, auth_tag: Option<String>, launch: LaunchBlock, env_vars: BTreeMap<String,String>, reap_after_idle_seconds: Option<u64> }` — redacted `Debug`
  - `AssignmentEnvelope { agent_pubkey: PublicKey, owner_pubkey: PublicKey, node_pubkey: PublicKey, state: AssignState }`
  - `enum AssignState { Assigned, Unassigned }`
  - `build_assignment(owner_keys: &Keys, node_pubkey: &PublicKey, secret: &AssignmentSecret, state: AssignState, created_at: u64) -> Result<Event, CodecError>`
  - `validate_envelope(event: &Event, expected_owner: &PublicKey) -> Result<AssignmentEnvelope, CodecError>`
  - `decrypt_for_node(event: &Event, node_keys: &Keys, expected_owner: &PublicKey) -> Result<(AssignmentEnvelope, AssignmentSecret), CodecError>`

**Public tags:** `["d", agent_pubkey]`, `["node", node_pubkey]`, `["state", "assigned"|"unassigned"]`. **Encrypted content:** NIP-44(owner_secret → node_pubkey) of `AssignmentSecret`. Public fields let *any* of the owner's nodes decide `node == self` without decrypting; only the target node can read the nsec.

- [ ] **Step 1: Write the failing tests** (round-trip on target node; wrong node fails; wrong owner fails; nsec redacted; nsec must derive agent_pubkey).

```rust
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
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-core assignment::` — Expected: FAIL.

- [ ] **Step 3: Implement `assignment.rs`.** Follow `private_managed_agent.rs` for structure. Key points:
  - `AssignState::as_str()` → `"assigned"`/`"unassigned"`; parse in `validate_envelope`.
  - Custom `Debug` for `AssignmentSecret` printing `"<redacted>"` for `private_key_nsec`, `auth_tag`, `env_vars`, and `launch.env`/`policy_env` (copy the redaction style from `PrivateConfig`/`PrivateIdentity`).
  - `build_assignment`: `validate_secret(secret)` first (format/version, `agent_pubkey`/`owner_pubkey`/`node_pubkey` canonical, **`Keys::parse(nsec).public_key() == agent_pubkey`**, owner matches signer, node matches target); serialize `secret` to JSON; `nip44::encrypt(owner_keys.secret_key(), node_pubkey, plaintext, Version::V2)`; build event with the three public tags + `custom_created_at`; sign with `owner_keys`.
  - `validate_envelope`: kind check; author == expected_owner; `verify_id`+`verify_signature`; parse exactly the three tags (`d`,`node`,`state`) with the duplicate/arity checks from `private_managed_agent::validate_envelope`; return `AssignmentEnvelope`.
  - `decrypt_for_node`: `validate_envelope` first; **require `node_keys.public_key() == envelope.node_pubkey`** (else `InvalidEnvelope("not the target node")`); `nip44::decrypt(node_keys.secret_key(), &expected_owner, &event.content)` → `Decrypt` on error; `parse_strict_json`; `serde_json::from_value`; `validate_secret`; cross-check inner `agent_pubkey`/`owner_pubkey`/`node_pubkey` == envelope; return `(envelope, secret)`.
  - Use `FORMAT = "buzz-agent-assignment-v1"`, `VERSION = 1`.

- [ ] **Step 4: Add `pub mod assignment;` to `lib.rs`** (doc comment).

- [ ] **Step 5: Run + clippy.** Run: `cargo test -p buzz-core assignment:: && cargo clippy -p buzz-core --all-targets` — Expected: PASS, no warnings.

- [ ] **Step 6: Commit.**
```bash
git add crates/buzz-core/src/assignment.rs crates/buzz-core/src/lib.rs
git commit -s -m "feat(core): AGENT_ASSIGNMENT codec with node-encrypted key delivery"
```

---

### Task 4: `AGENT_NODE_STATUS` codec (`node_status.rs`)

**Files:**
- Create: `crates/buzz-core/src/node_status.rs`
- Modify: `crates/buzz-core/src/lib.rs` (`pub mod node_status;`)

**Interfaces:**
- Consumes: `kind::KIND_AGENT_NODE_STATUS`, `node_codec::*`.
- Produces:
  - `enum AgentHealth { Starting, Running, Stopped, Crashed, Unschedulable }` (serde `rename_all = "lowercase"`)
  - `AgentNodeStatus { format, version, agent_pubkey, node_pubkey, health: AgentHealth, reason: Option<String>, updated_at: String }`
  - `build_status(node_keys: &Keys, status: &AgentNodeStatus, created_at: u64) -> Result<Event, CodecError>`
  - `validate_status(event: &Event) -> Result<AgentNodeStatus, CodecError>` (verifies `event.pubkey.to_hex() == status.node_pubkey`; `updated_at` is RFC3339)

- [ ] **Step 1: Write failing tests.**

```rust
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
        let ev = build_status(&node, &s, 1_785_780_000).unwrap();
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
```

- [ ] **Step 2: Run to confirm failure.** Run: `cargo test -p buzz-core node_status::` — Expected: FAIL.

- [ ] **Step 3: Implement `node_status.rs`** following the `node.rs` shape: `#[serde(deny_unknown_fields)]` struct; `build_status` validates format/version, canonical pubkeys, RFC3339 `updated_at` (via `node_codec::parse_rfc3339`), `node_pubkey == signer`; event tags `["d", agent_pubkey]`; sign with `node_keys`. `validate_status` checks kind, `verify_id`/`verify_signature`, strict JSON parse, format/version, canonical pubkeys, RFC3339, and `node_pubkey == event.pubkey`.

- [ ] **Step 4: Add `pub mod node_status;` to `lib.rs`.**

- [ ] **Step 5: Run + clippy.** Run: `cargo test -p buzz-core node_status:: && cargo clippy -p buzz-core --all-targets` — Expected: PASS.

- [ ] **Step 6: Commit.**
```bash
git add crates/buzz-core/src/node_status.rs crates/buzz-core/src/lib.rs
git commit -s -m "feat(core): AGENT_NODE_STATUS codec"
```

---

### Task 5: Re-exports + whole-crate gate

**Files:**
- Modify: `crates/buzz-core/src/lib.rs` (add `pub use` for the new public types, next to the existing `pub use` block)

**Interfaces:**
- Produces: crate-root re-exports so downstream crates write `buzz_core::AssignmentSecret` etc.

- [ ] **Step 1: Add re-exports.**
```rust
pub use assignment::{AssignState, AssignmentEnvelope, AssignmentSecret, LaunchBlock};
pub use node::{Enrollment, NodeCapabilities};
pub use node_codec::CodecError;
pub use node_status::{AgentHealth, AgentNodeStatus};
```

- [ ] **Step 2: Doc-test / compile check the re-exports** — add a doc example on one re-export or a trivial test in `lib.rs`:
```rust
#[cfg(test)]
mod reexport_tests {
    #[test]
    fn types_are_reachable_from_crate_root() {
        let _ = crate::AgentHealth::Running;
        assert_eq!(crate::node::VERSION, 1);
    }
}
```

- [ ] **Step 3: Full gate.** Run: `cargo test -p buzz-core && cargo clippy -p buzz-core --all-targets -- -D warnings && cargo fmt -p buzz-core -- --check` — Expected: PASS, no warnings, formatted.

- [ ] **Step 4: Commit.**
```bash
git add crates/buzz-core/src/lib.rs
git commit -s -m "feat(core): re-export execution-node codec types"
```

---

## Self-Review

**Spec coverage (§7 event model):**
- `NODE_ANNOUNCE` → Task 2 ✓ · `NODE_ENROLLMENT` → Task 2 ✓ · `AGENT_ASSIGNMENT` (+ `enc_nsec` to node, `launch` block) → Task 3 ✓ · `AGENT_NODE_STATUS` (+ health-reason vocabulary via `AgentHealth`/`reason`) → Task 4 ✓. Owner-authorization (owner-signed envelope, node-target public tag) → Tasks 2–3 ✓. NIP-44 key-to-node delivery (§12) → Task 3 ✓. Kinds in `kind.rs` → Task 1 ✓.
- **Deferred to later phases (correctly not here):** subscription/relay handling of these kinds (Phase 3), the reconcile loop (Phase 2), desktop UI (Phase 4). buzz-core is wire-contract only.

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". Kind integers are concrete with a verify-and-adjust step (Task 1 Step 1). Shared helpers are lifted from a named existing file, not invented.

**Type consistency:** `CodecError` is the single error type across all modules (Task 1). `FORMAT`/`VERSION` are per-module consts (distinct discriminators). `AssignmentSecret`/`LaunchBlock`/`AssignmentEnvelope`/`AssignState` names match between Task 3's interface block, its code, and Task 5's re-exports. `AgentHealth`/`AgentNodeStatus` match between Task 4 and Task 5. `build_*`/`validate_*`/`decrypt_for_node` signatures are identical in the interface blocks and the implementing steps.

---

## What Phase 2 consumes from this

`buzz-node`'s reconciler (Phase 2) will import: `KIND_AGENT_ASSIGNMENT`, `assignment::{validate_envelope, decrypt_for_node, AssignmentEnvelope, AssignmentSecret, AssignState}` (to read desired state and get the nsec + launch), `node::{build_announce, build_enrollment, NodeCapabilities}` (to advertise itself), and `node_status::{build_status, AgentNodeStatus, AgentHealth}` (to report observed state). No relay/DB types leak into buzz-core.
