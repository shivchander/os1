use super::*;
use nostr::{Keys, ToBech32};
use std::collections::BTreeMap;

#[test]
fn enrollment_builder_is_owner_signed_and_targets_node() {
    let owner = Keys::generate();
    let node = Keys::generate();

    let event = build_node_enrollment_event(&owner, &node.public_key()).expect("build enrollment");

    // Any holder of the event can validate it against the claimed owner.
    let parsed = buzz_core_pkg::node::validate_enrollment(&event, &owner.public_key())
        .expect("validate enrollment");
    assert_eq!(parsed.node_pubkey, node.public_key().to_hex());
    assert_eq!(parsed.owner_pubkey, owner.public_key().to_hex());
    assert_eq!(event.pubkey, owner.public_key());
    assert!(event.verify_id());
    assert!(event.verify_signature());

    // A different claimed owner is rejected.
    assert!(
        buzz_core_pkg::node::validate_enrollment(&event, &Keys::generate().public_key()).is_err()
    );
}

#[test]
fn enrollment_builder_rejects_invalid_hex() {
    // Not exercised through the async command (needs a live AppState), but
    // the hex-parsing guard in `publish_node_enrollment` is the same
    // `PublicKey::from_hex` used here — pin its failure mode directly.
    assert!(PublicKey::from_hex("not-hex").is_err());
}

fn sample_launch(owner: &Keys) -> LaunchInput {
    LaunchInput {
        command: "claude".to_string(),
        args: vec!["--flag".to_string()],
        env: BTreeMap::from([("FOO".to_string(), "bar".to_string())]),
        policy_env: BTreeMap::new(),
        owner_pubkey: Some(owner.public_key().to_hex()),
    }
}

#[test]
fn assignment_builder_encrypts_to_node_and_round_trips() {
    let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
    let agent_nsec = agent.secret_key().to_bech32().unwrap();

    let event = build_agent_assignment_event(
        &owner,
        &agent.public_key(),
        &node.public_key(),
        agent_nsec.clone(),
        sample_launch(&owner),
        buzz_core_pkg::AssignState::Assigned,
    )
    .expect("build assignment");

    assert_eq!(event.pubkey, owner.public_key());
    assert!(event.verify_id());
    assert!(event.verify_signature());

    // The target node decrypts and recovers the agent's nsec + launch contract.
    let (envelope, secret) =
        buzz_core_pkg::assignment::decrypt_for_node(&event, &node, &owner.public_key())
            .expect("target node decrypts");
    assert_eq!(envelope.agent_pubkey, agent.public_key());
    assert_eq!(envelope.owner_pubkey, owner.public_key());
    assert_eq!(envelope.node_pubkey, node.public_key());
    assert_eq!(envelope.state, buzz_core_pkg::AssignState::Assigned);
    assert_eq!(secret.private_key_nsec, agent_nsec);
    assert_eq!(secret.launch.command, "claude");
    assert_eq!(secret.launch.args, vec!["--flag".to_string()]);

    // Nobody else can decrypt: neither a stranger nor the owner itself holds
    // the node's secret key.
    let stranger = Keys::generate();
    assert!(
        buzz_core_pkg::assignment::decrypt_for_node(&event, &stranger, &owner.public_key())
            .is_err()
    );
    assert!(
        buzz_core_pkg::assignment::decrypt_for_node(&event, &owner, &owner.public_key()).is_err()
    );
}

#[test]
fn assignment_builder_supports_unassigned_state() {
    let (owner, agent, node) = (Keys::generate(), Keys::generate(), Keys::generate());
    let event = build_agent_assignment_event(
        &owner,
        &agent.public_key(),
        &node.public_key(),
        agent.secret_key().to_bech32().unwrap(),
        sample_launch(&owner),
        buzz_core_pkg::AssignState::Unassigned,
    )
    .expect("build assignment");

    let envelope = buzz_core_pkg::assignment::validate_envelope(&event, &owner.public_key())
        .expect("validate envelope");
    assert_eq!(envelope.state, buzz_core_pkg::AssignState::Unassigned);
}

#[test]
fn assignment_builder_rejects_nsec_not_matching_agent_pubkey() {
    // Defense in depth: even if a caller mismatched the resolved nsec against
    // the requested agent pubkey, buzz-core's `validate_secret` fails closed
    // rather than publishing a wrong-key assignment.
    let (owner, agent, node, wrong_agent) = (
        Keys::generate(),
        Keys::generate(),
        Keys::generate(),
        Keys::generate(),
    );
    let result = build_agent_assignment_event(
        &owner,
        &agent.public_key(),
        &node.public_key(),
        wrong_agent.secret_key().to_bech32().unwrap(),
        sample_launch(&owner),
        buzz_core_pkg::AssignState::Assigned,
    );
    assert!(result.is_err());
}
