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
/// Domain types: desired agents, observed states, actions, errors.
pub mod model;
/// Pure desired-vs-observed reconciliation.
pub mod reconcile;
/// The relay abstraction (desired-state in, status out) + an in-memory fake.
pub mod relay;
/// The substrate abstraction (local process table) + an in-memory fake.
pub mod substrate;
