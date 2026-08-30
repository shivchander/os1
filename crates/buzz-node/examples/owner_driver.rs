//! owner_driver — drive the OWNER side of execution-node enrollment +
//! assignment against a live relay, for manual live bring-up on a real node.
//!
//! This is test/operator tooling, NOT production code (liberal expect/unwrap,
//! matching the crate's e2e harness style). It exists so a human can bring a
//! real node up end-to-end without the desktop app: it publishes the exact
//! same `NODE_ENROLLMENT` / `AGENT_ASSIGNMENT` events the desktop's Tauri
//! commands do, over the same `buzz-core` codecs. Unlike the desktop today,
//! it populates `LaunchBlock.env` (agent command + provider key), which is
//! what actually lets a node-hosted agent run against a provider.
//!
//! Stable owner + agent keys are persisted under `~/.buzz-node` so re-runs
//! keep the same identities (a node pins its owner on first enrollment, and
//! `assign`/`observe` must agree on the agent pubkey).
//!
//! Usage:
//!   owner_driver whoami
//!   owner_driver enroll  <relay_url> <node_pubkey_hex>
//!   owner_driver assign  <relay_url> <node_pubkey_hex>   # reads OPENAI_API_KEY from env
//!   owner_driver observe <relay_url> [seconds]           # watch the persisted agent's status

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use buzz_core::assignment::{build_assignment, AssignmentSecret, LaunchBlock};
use buzz_core::kind::KIND_AGENT_NODE_STATUS;
use buzz_core::node::build_enrollment;
use buzz_core::node_status::validate_status;
use buzz_core::AssignState;
use buzz_test_client::{BuzzTestClient, RelayMessage, TestClientError};
use nostr::{Alphabet, Filter, Keys, Kind, PublicKey, SingleLetterTag, ToBech32};

fn now_unix() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0)
}

fn key_dir() -> PathBuf {
    dirs::home_dir().expect("home dir").join(".buzz-node")
}

/// Load a persisted key by filename, generating + saving one on first use.
fn load_or_create(name: &str) -> Keys {
    let path = key_dir().join(name);
    if let Ok(nsec) = std::fs::read_to_string(&path) {
        return Keys::parse(nsec.trim()).expect("parse persisted key");
    }
    let keys = Keys::generate();
    std::fs::create_dir_all(key_dir()).expect("create key dir");
    std::fs::write(&path, keys.secret_key().to_bech32().expect("bech32 nsec"))
        .expect("write key file");
    keys
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("");
    let owner = load_or_create("owner.nsec");
    let agent = load_or_create("agent.nsec");

    match mode {
        "whoami" => {
            println!("owner_pubkey={}", owner.public_key().to_hex());
            println!("agent_pubkey={}", agent.public_key().to_hex());
        }

        "enroll" => {
            let relay = args.get(2).expect("relay_url");
            let node = PublicKey::from_hex(args.get(3).expect("node_pubkey_hex"))
                .expect("node pubkey hex");
            let mut c = BuzzTestClient::connect(relay, &owner)
                .await
                .expect("connect+auth as owner");
            let ev = build_enrollment(&owner, &node, now_unix()).expect("build enrollment");
            let ok = c.send_event(ev).await.expect("send enrollment");
            println!(
                "enrollment: accepted={} msg={:?}\nowner_pubkey={}\nnode_pubkey={}",
                ok.accepted,
                ok.message,
                owner.public_key().to_hex(),
                node.to_hex()
            );
        }

        "assign" => {
            let relay = args.get(2).expect("relay_url");
            let node = PublicKey::from_hex(args.get(3).expect("node_pubkey_hex"))
                .expect("node pubkey hex");
            let openai = std::env::var("OPENAI_API_KEY").expect("set OPENAI_API_KEY in env");

            // The launch env the node threads into the spawned harness. The
            // node's AcpRuntime execs `buzz-acp`, which reads
            // BUZZ_ACP_AGENT_COMMAND to pick the agent (codex here) and passes
            // its env down to the agent process.
            let mut env = BTreeMap::new();
            // codex-acp is the ACP adapter that wraps the codex CLI (npm i -g
            // @agentclientprotocol/codex-acp). buzz-acp's normalize_agent_args
            // strips the legacy "acp" default arg for codex-acp, so command alone suffices.
            env.insert("BUZZ_ACP_AGENT_COMMAND".to_string(), "codex-acp".to_string());
            env.insert("OPENAI_API_KEY".to_string(), openai);

            let secret = AssignmentSecret {
                format: buzz_core::assignment::FORMAT.into(),
                version: buzz_core::assignment::VERSION,
                agent_pubkey: agent.public_key().to_hex(),
                owner_pubkey: owner.public_key().to_hex(),
                node_pubkey: node.to_hex(),
                private_key_nsec: agent.secret_key().to_bech32().expect("agent nsec"),
                auth_tag: None,
                launch: LaunchBlock {
                    // Inert (AcpRuntime never reads launch.command) but kept
                    // structurally valid; the real selector is env's
                    // BUZZ_ACP_AGENT_COMMAND above.
                    command: "codex-acp".into(),
                    args: vec![],
                    env,
                    policy_env: BTreeMap::new(),
                    owner_pubkey: Some(owner.public_key().to_hex()),
                },
                env_vars: BTreeMap::new(),
                reap_after_idle_seconds: None,
            };
            let ev = build_assignment(&owner, &node, &secret, AssignState::Assigned, now_unix())
                .expect("build assignment");
            let mut c = BuzzTestClient::connect(relay, &owner)
                .await
                .expect("connect+auth as owner");
            let ok = c.send_event(ev).await.expect("send assignment");
            println!(
                "assignment: accepted={} msg={:?}\nagent_pubkey={}\nnode_pubkey={}",
                ok.accepted,
                ok.message,
                agent.public_key().to_hex(),
                node.to_hex()
            );
        }

        "observe" => {
            let relay = args.get(2).expect("relay_url");
            let secs: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(90);
            let agent_pk = agent.public_key();
            let mut c = BuzzTestClient::connect(relay, &owner)
                .await
                .expect("connect+auth as owner");
            let filter = Filter::new()
                .kind(Kind::Custom(KIND_AGENT_NODE_STATUS as u16))
                .custom_tag(SingleLetterTag::lowercase(Alphabet::D), agent_pk.to_hex());
            c.subscribe("owner-driver-status", vec![filter])
                .await
                .expect("subscribe status");
            println!(
                "observing AGENT_NODE_STATUS for agent {} for {secs}s...",
                agent_pk.to_hex()
            );
            let end = tokio::time::Instant::now() + Duration::from_secs(secs);
            loop {
                let remaining = end.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match c.recv_event(remaining).await {
                    Ok(RelayMessage::Event { event, .. }) => {
                        if let Ok(s) = validate_status(&event) {
                            println!(
                                "  status: node={} health={:?} reason={:?} updated_at={}",
                                &s.node_pubkey[..8.min(s.node_pubkey.len())],
                                s.health,
                                s.reason,
                                s.updated_at
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(TestClientError::Timeout) => {}
                    Err(e) => {
                        eprintln!("status read failed: {e}");
                        break;
                    }
                }
            }
        }

        other => {
            eprintln!(
                "unknown mode {other:?}\nusage: owner_driver [whoami|enroll <relay> <node_hex>|assign <relay> <node_hex>|observe <relay> [secs]]"
            );
            std::process::exit(2);
        }
    }
}
