use super::*;
use nostr::Keys;

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
