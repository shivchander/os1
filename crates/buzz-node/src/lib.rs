#![deny(unsafe_code)]
#![warn(missing_docs)]
//! `buzz-node` — a persistent execution-node daemon that hosts Buzz agents.
//!
//! The node subscribes to the owner's agent→node assignments on the relay and
//! reconciles them against the local process table: it starts assigned agents,
//! stops unassigned ones, restarts crashed ones, and reports observed status —
//! all controlled purely through the relay (no inbound control channel).

/// The engine loop tying relay + substrate + reconcile together.
pub mod engine;
/// Node enrollment: keypair bootstrap, keychain-backed secret storage,
/// pairing code, and the owner-discovery handshake.
pub mod enroll;
/// Domain types: desired agents, observed states, actions, errors.
pub mod model;
/// Bounded stop-before-start move gate (spec I4): defers a spawn while a
/// different node still reports the agent alive.
pub mod move_gate;
/// The real Nostr `NodeRelay`: dial-out/NIP-42, assignment intake, publish.
pub mod nostr_relay;
/// Pure desired-vs-observed reconciliation.
pub mod reconcile;
/// The relay abstraction (desired-state in, status out) + an in-memory fake.
pub mod relay;
/// The `AgentRuntime` seam (D7) and its ACP implementation.
pub mod runtime;
/// The substrate abstraction: the real `LocalProcessSubstrate` plus an
/// in-memory fake for tests.
pub mod substrate;
