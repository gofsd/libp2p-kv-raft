//! Unit tests for the wire codec. The cross-language ones -- decoding
//! messages a real Go `pkg/shmevent` produced and re-encoding them to
//! identical bytes -- live in [`go_fixture`] at the bottom, and are what
//! actually pin this module as a byte-compatible mirror rather than a
//! plausible one.

use super::*;
use ed25519_dalek::SigningKey;

// Fixed, deterministic 32-byte seeds -- plain distinct test fixtures, not
// meant to be cryptographically random (no OsRng dependency needed just for
// tests).
fn test_key() -> SigningKey {
    SigningKey::from_bytes(&[7u8; 32])
}
fn other_test_key() -> SigningKey {
    SigningKey::from_bytes(&[9u8; 32])
}

fn data(v: &[u8]) -> DataField {
    Some(v.to_vec())
}

#[test]
fn encode_decode_roundtrip() {
    let signing_key = test_key();
    let verifying_key = signing_key.verifying_key();

    let m = Msg::with_id(
        7,
        Body::SetField {
            source_id: 42,
            value: data(b"world"),
        },
    );

    let buf = encode(&m, Some(&signing_key)).unwrap();
    let (got, crc, sig) = decode(&buf).unwrap();
    assert_eq!(got, m);
    verify(&verifying_key, &got, crc, &sig).unwrap();

    let other_key = other_test_key().verifying_key();
    assert!(verify(&other_key, &got, crc, &sig).is_err());
}

/// Every variant survives a round trip, including the ones this client never
/// sends: a response it has to *read* uses the same codec, and a variant
/// nothing exercises is exactly where a mistranscribed field list hides.
#[test]
fn every_variant_roundtrips() {
    let signing_key = test_key();
    for body in sample_bodies() {
        let m = Msg::with_id(3, body.clone());
        let buf = encode(&m, Some(&signing_key)).unwrap();
        let (got, crc, sig) = decode(&buf).unwrap();
        assert_eq!(got, m, "{} did not survive a round trip", m.name());
        verify(&signing_key.verifying_key(), &got, crc, &sig)
            .unwrap_or_else(|e| panic!("{}: {e}", m.name()));
    }
}

/// One populated instance of each variant, with every field set to something
/// distinguishable, so a round trip actually notices a field written into the
/// wrong slot.
fn sample_bodies() -> Vec<Body> {
    vec![
        Body::SetKey { value: data(b"k") },
        Body::SetField {
            source_id: 1,
            value: data(b"v"),
        },
        Body::GetKey {
            source_id: 2,
            key: data(b"key"),
        },
        Body::GetFieldByRegistry {
            source_id: 3,
            value: data(b"v"),
        },
        Body::GetFieldByKey {
            key: data(b"key"),
            value: data(b"value"),
        },
        Body::GetPublicKey {
            pub_key: data(&[0xab; PUBLIC_KEY_SIZE]),
        },
        Body::GetPrivateKey {
            priv_key: data(&[0xcd; 64]),
        },
        Body::BootstrapOrJoinCluster {
            leader_addr: "/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWleader".into(),
        },
        Body::AddLearner {
            claimed_peer_id: 9,
            addr: "/ip4/5.6.7.8/tcp/4001".into(),
        },
        Body::Set {
            key: data(b"key"),
            value: data(b"value"),
        },
        Body::Execute {
            source_id: 4,
            destination_id: 5,
            sender_peer_id: "12D3KooWsender".into(),
            value: data(b"payload"),
        },
        Body::PollExecute {
            sender_peer_id: "12D3KooWsender".into(),
            value: data(b"payload"),
        },
        Body::ListRange {
            start: data(b"aaa"),
            end: data(b"zzz"),
            key: data(b"mmm"),
            value: data(b"val"),
        },
        Body::LogAppend {
            key: data(b"\x01log-key"),
            value: data(b"record"),
        },
        Body::Leave,
        Body::ExecInviteRedeem {
            source_addr: "/ip4/1.2.3.4/tcp/4001".into(),
            token: data(&[1, 2, 3, 4]),
            instance_id: "deadbeef".into(),
            redeemer_peer_id: "12D3KooWredeemer".into(),
        },
        Body::JoinRequestCreate {
            token: data(&[5, 6]),
        },
        Body::JoinRequestCancel {
            token: data(&[7, 8]),
        },
        Body::Recruit {
            ticket: "/ip4/1.2.3.4/tcp/4001#abcd".into(),
            suffrage: 1,
        },
        Body::GetOwnAddr {
            addr: "/ip4/1.2.3.4/tcp/4001".into(),
        },
        Body::ChannelOpen {
            peer_id: "12D3KooWtarget".into(),
            sender_peer_id: "12D3KooWsender".into(),
        },
        Body::ChannelSend {
            channel_id: "chan-1".into(),
            purpose: 2,
            chunk: data(b"chunk"),
        },
        Body::ChannelPoll {
            channel_id: "chan-1".into(),
            status: 3,
            purpose: 2,
            chunk: data(b"chunk"),
        },
        Body::ChannelListen {
            channel_id: "chan-1".into(),
            remote_peer_id: "12D3KooWremote".into(),
        },
        Body::ChannelClose {
            channel_id: "chan-1".into(),
        },
        Body::ChannelCloseWrite {
            channel_id: "chan-1".into(),
        },
        Body::ChannelDataReady {
            channel_id: "chan-1".into(),
        },
        Body::Kick {
            peer_id: "12D3KooWgone".into(),
        },
        Body::Txn {
            ops: Some(vec![
                TxnOp {
                    op: TXN_OP_SET,
                    key: data(b"a"),
                    value: data(b"1"),
                },
                TxnOp {
                    op: TXN_OP_COMPARE_ABSENT,
                    key: data(b"b"),
                    value: None,
                },
            ]),
        },
        Body::GetVersion {
            commit: "abc1234".into(),
            dirty: true,
            build_time: "2026-08-17T00:00:00Z".into(),
            go_version: "go1.25.13".into(),
            libp2p_version: "v0.38.0".into(),
        },
        Body::PublicAccess {
            target_peer: "12D3KooWtarget".into(),
            note: "hello".into(),
            instance_id: "cafe".into(),
        },
        Body::ExecTicket {
            source_addr: "/ip4/1.2.3.4/tcp/4001".into(),
            token: data(&[9]),
        },
        Body::JoinTicket {
            source_addr: "/ip4/1.2.3.4/tcp/4001".into(),
            token: data(&[10]),
        },
        Body::JoinRequestTicket {
            source_addr: "/ip4/1.2.3.4/tcp/4001".into(),
            token: data(&[11]),
        },
        Body::DialSubmitCommand {
            target_peer: "12D3KooWtarget".into(),
            command_id: "cmd-1".into(),
            inputs_json: r#"{"a":"b"}"#.into(),
            note: "note".into(),
            instance_id: "beef".into(),
        },
        Body::DialQueryCommandLog {
            target_peer: "12D3KooWtarget".into(),
            instance_id: "beef".into(),
            since: 1,
            until: 2,
            limit: 3,
            records: Some(vec![b"rec-1".to_vec(), b"rec-2".to_vec()]),
        },
        Body::Error {
            message: "something went wrong".into(),
        },
        Body::GroupPut {
            id: "grp".into(),
            name: "Group".into(),
            public: true,
        },
        Body::GroupDelete { id: "grp".into() },
        Body::CommandPut {
            id: "cmd".into(),
            name: "Command".into(),
            peer_id: "12D3KooWowner".into(),
            spec: Some(r#"{"fields":[]}"#.into()),
        },
        Body::CommandDelete { id: "cmd".into() },
        Body::StationPut {
            peer_id: "12D3KooWstation".into(),
            name: "Station".into(),
            attrs: r#"{"x":1}"#.into(),
        },
        Body::StationDelete {
            peer_id: "12D3KooWstation".into(),
        },
        Body::GroupCommandPut {
            command_id: "cmd".into(),
            group_id: "grp".into(),
        },
        Body::GroupCommandDelete {
            command_id: "cmd".into(),
            group_id: "grp".into(),
        },
        Body::PeerGroupPut {
            peer_id: "12D3KooWpeer".into(),
            group_id: "grp".into(),
        },
        Body::PeerGroupDelete {
            peer_id: "12D3KooWpeer".into(),
            group_id: "grp".into(),
        },
        Body::PermitRequest {
            kind: system::KIND_BOOTSTRAP_NODE,
            peer_id: "12D3KooWpeer".into(),
            metadata: r#"{"addr":"/ip4/1.2.3.4/tcp/4001"}"#.into(),
        },
        Body::PermitConfirm {
            kind: system::KIND_BOOTSTRAP_NODE,
            peer_id: "12D3KooWpeer".into(),
        },
        Body::PermitRevoke {
            kind: system::KIND_BOOTSTRAP_NODE,
            peer_id: "12D3KooWpeer".into(),
        },
        Body::JoinInviteCreate {
            token: data(&[12, 13]),
            suffrage: 1,
        },
        Body::JoinInviteRevoke { token: data(&[14]) },
        Body::ExecInviteCreate {
            token: data(&[15]),
            command_id: "cmd".into(),
            inputs_json: r#"{"a":"b"}"#.into(),
            ttl_seconds: 3600,
        },
        Body::ExecInviteRevoke { token: data(&[16]) },
    ]
}

#[test]
fn decode_detects_corruption() {
    let signing_key = test_key();
    let value = b"hello-corruption-marker".to_vec();
    let m = Msg::with_id(
        1,
        Body::GetFieldByKey {
            key: Some(value.clone()),
            value: None,
        },
    );
    let mut buf = encode(&m, Some(&signing_key)).unwrap();

    let idx = buf
        .windows(value.len())
        .position(|w| w == value.as_slice())
        .expect("value bytes not found in encoded message");
    buf[idx] ^= 0xff;

    assert!(decode(&buf).is_err());
}

#[test]
fn sign_verify_tamper_detection() {
    let signing_key = test_key();
    let verifying_key = signing_key.verifying_key();
    let m = Msg::with_id(
        99,
        Body::SetKey {
            value: data(b"hello"),
        },
    );
    let crc = crc32_of(&m).unwrap();
    let sig = sign(Some(&signing_key), &m, crc).unwrap();
    verify(&verifying_key, &m, crc, &sig).unwrap();

    let tampered = Msg::with_id(
        99,
        Body::SetKey {
            value: data(b"hellp"),
        },
    );
    assert!(verify(&verifying_key, &tampered, crc, &sig).is_err());

    // A different variant carrying the same bytes is a different message too
    // -- the union discriminant is inside the signed canonical form.
    let other_variant = Msg::with_id(
        99,
        Body::GetFieldByKey {
            key: data(b"hello"),
            value: None,
        },
    );
    assert!(verify(&verifying_key, &other_variant, crc, &sig).is_err());
}

/// The distinction this module's doc comment calls rule 2, pinned: an unset
/// `Data` field and a present-but-empty one are different messages, with
/// different CRCs and different signatures. Collapsing them would make this
/// client reject a real `set` of a key to an empty value.
#[test]
fn null_and_empty_data_are_different_messages() {
    let absent = Msg::new(Body::Set {
        key: data(b"k"),
        value: None,
    });
    let empty = Msg::new(Body::Set {
        key: data(b"k"),
        value: Some(Vec::new()),
    });

    assert_ne!(crc32_of(&absent).unwrap(), crc32_of(&empty).unwrap());

    let key = test_key();
    let absent_buf = encode(&absent, Some(&key)).unwrap();
    let empty_buf = encode(&empty, Some(&key)).unwrap();
    assert_ne!(absent_buf, empty_buf);

    // And each decodes back to the state it was built with, rather than
    // both collapsing to one of them.
    assert_eq!(decode(&absent_buf).unwrap().0, absent);
    assert_eq!(decode(&empty_buf).unwrap().0, empty);
}

/// `CommandPut::spec`'s three states are the same three the Go side's
/// `CommandPut`/`CommandPutWithSpec`/`clearcommandspec` trio encodes: absent
/// preserves a stored spec, present-empty clears it, present-nonempty sets
/// it. All three have to survive the wire distinctly.
#[test]
fn command_put_spec_is_three_state() {
    let key = test_key();
    for spec in [
        None,
        Some(String::new()),
        Some("{\"fields\":[]}".to_string()),
    ] {
        let m = Msg::new(Body::CommandPut {
            id: "cmd".into(),
            name: "Command".into(),
            peer_id: "12D3KooWowner".into(),
            spec: spec.clone(),
        });
        let buf = encode(&m, Some(&key)).unwrap();
        let (got, _, _) = decode(&buf).unwrap();
        assert_eq!(got, m, "spec {spec:?} did not survive");
    }
}

#[test]
fn get_public_private_key_events_sign_with_none_key() {
    let m = Msg::with_id(3, Body::GetPublicKey { pub_key: None });
    let buf = encode(&m, None).unwrap();
    decode(&buf).unwrap();

    let m2 = Msg::with_id(3, Body::GetPrivateKey { priv_key: None });
    encode(&m2, None).unwrap();

    // Everything else needs a key.
    let signed = Msg::with_id(3, Body::SetKey { value: data(b"x") });
    assert!(encode(&signed, None).is_err());
}

/// One ceiling for every variant now (`MaxValueSize` on the Go side), rather
/// than the per-event tiers and padding widths the flat struct needed.
#[test]
fn value_too_long_rejected() {
    let signing_key = test_key();
    let at_ceiling = Msg::new(Body::Set {
        key: data(b"k"),
        value: Some(vec![0u8; MAX_VALUE_SIZE]),
    });
    assert!(encode(&at_ceiling, Some(&signing_key)).is_ok());

    let over = Msg::new(Body::Set {
        key: data(b"k"),
        value: Some(vec![0u8; MAX_VALUE_SIZE + 1]),
    });
    assert!(encode(&over, Some(&signing_key)).is_err());
}

#[test]
fn event_name_round_trip() {
    use std::collections::BTreeMap;
    let mut seen = BTreeMap::new();
    for body in sample_bodies() {
        let name = event_name(&body);
        assert!(
            seen.insert(name, ()).is_none(),
            "two variants share the name {name:?}"
        );
        // The name must be enough to reconstruct the variant (with empty
        // fields), which is what msg_from_json relies on -- except for the
        // one variant that deliberately refuses, matching pkg/e2edata.
        match body_from_fields(name, &BTreeMap::new()) {
            Ok(rebuilt) => assert_eq!(
                event_name(&rebuilt),
                name,
                "{name:?} did not rebuild into its own variant"
            ),
            Err(e) => assert_eq!(
                name, "txn",
                "{name:?} is not rebuildable from its own name: {e}"
            ),
        }
    }
    assert_eq!(seen.len(), sample_bodies().len());
    assert!(body_from_fields("not_a_real_event", &BTreeMap::new()).is_err());
}

#[test]
fn msg_json_human_readable() {
    let m = Msg::with_id(
        7,
        Body::SetField {
            source_id: 100,
            value: data(b"world"),
        },
    );
    let json = msg_to_json(&m).unwrap();
    assert_eq!(
        json,
        r#"{"event":"set_field","id":7,"fields":{"source_id":"100","value":"world"}}"#
    );

    let back = msg_from_json(&json).unwrap();
    assert_eq!(back, m);
}

#[test]
fn msg_json_binary_value_uses_hex_prefix() {
    let raw = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0xff];
    let m = Msg::new(Body::GetPublicKey {
        pub_key: Some(raw.clone()),
    });
    let json = msg_to_json(&m).unwrap();
    assert!(
        json.contains(r#""0xdeadbeef00ff""#),
        "json = {json}, want a 0x-prefixed hex value"
    );

    let back = msg_from_json(&json).unwrap();
    assert_eq!(back, m);
}

#[test]
fn msg_json_omits_unset_fields() {
    let json = msg_to_json(&Msg::new(Body::Leave)).unwrap();
    assert_eq!(json, r#"{"event":"leave"}"#);
}

#[test]
fn msg_from_json_rejects_unknown_event_name() {
    assert!(msg_from_json(r#"{"event":"not_a_real_event"}"#).is_err());
}

#[test]
fn hex_round_trip() {
    let raw = vec![0x00, 0x7f, 0xff, 0xab, 0xcd];
    assert_eq!(hex_decode(&hex_encode(&raw)).unwrap(), raw);
    assert!(hex_decode("abc").is_err()); // odd length
    assert!(hex_decode("zz").is_err()); // invalid digit
}

/// The IPC rings this crate talks to its own Worker over must fit the largest
/// message that traffic realistically carries. They are single-use rings
/// written in full before the other side reads, so "too small" does not
/// surface as a rejected message -- it is a tab that stops responding.
///
/// `MAX_VALUE_SIZE` is deliberately *not* the number checked here: it is the
/// protocol's own 8MB ceiling, far above what any IPC round trip carries, and
/// `pkg/ipc`'s own `capacity` comment makes the same distinction (a
/// channelSend/channelPoll chunk, ~16KB, is the largest regular traffic).
#[test]
fn ipc_ring_fits_a_realistic_message() {
    let signing_key = test_key();
    let chunk = 16 * 1024;
    for body in [
        Body::Set {
            key: data(b"some-key"),
            value: Some(vec![0u8; chunk]),
        },
        Body::ChannelSend {
            channel_id: "chan-1".into(),
            purpose: 1,
            chunk: Some(vec![0u8; chunk]),
        },
        Body::LogAppend {
            key: data(b"\x01log"),
            value: Some(vec![0u8; chunk]),
        },
    ] {
        let m = Msg::new(body);
        let name = m.name();
        let encoded = encode(&m, Some(&signing_key)).unwrap();
        assert!(
            encoded.len() as u64 <= IPC_RING_CAPACITY,
            "{name}: a {chunk}-byte payload encodes to {} bytes, over the {}-byte IPC ring -- raise IPC_RING_CAPACITY",
            encoded.len(),
            IPC_RING_CAPACITY,
        );
    }
}

/// Cross-language checks against messages a real `pkg/shmevent` encoded.
///
/// The fixture is generated by `TestWireFixture` in
/// `pkg/shmevent/wire_fixture_test.go` and committed at
/// `api/shmevent_wire_fixture.json`, next to the schema both implementations
/// compile from. Regenerate it with
/// `go test ./pkg/shmevent -run TestWireFixture -update-wire-fixture`
/// whenever the schema changes; that same Go test fails if the committed
/// fixture no longer matches what Go produces, so neither side can drift
/// silently.
///
/// What these prove, which nothing else here can: that this module's
/// canonical-form CRC and signature payload agree with Go's byte-for-byte. A
/// pure Rust round trip is self-consistent by construction and would pass
/// just as happily with a subtly different canonicalization.
mod go_fixture {
    use super::*;
    use ed25519_dalek::VerifyingKey;

    #[derive(serde::Deserialize)]
    struct Fixture {
        signing_key_seed_hex: String,
        public_key_hex: String,
        cases: Vec<Case>,
    }

    #[derive(serde::Deserialize)]
    struct Case {
        name: String,
        json: String,
        encoded_hex: String,
        crc32: u32,
        signed: bool,
    }

    fn fixture() -> Fixture {
        let raw = include_str!("../../../api/shmevent_wire_fixture.json");
        serde_json::from_str(raw).expect("parse api/shmevent_wire_fixture.json")
    }

    #[test]
    fn go_encoded_messages_decode_and_verify() {
        let f = fixture();
        let pub_bytes: [u8; PUBLIC_KEY_SIZE] = hex_decode(&f.public_key_hex)
            .unwrap()
            .try_into()
            .expect("public key length");
        let verifying_key = VerifyingKey::from_bytes(&pub_bytes).unwrap();

        assert!(!f.cases.is_empty(), "fixture has no cases");
        for case in &f.cases {
            let buf = hex_decode(&case.encoded_hex).unwrap();
            let (msg, crc, sig) = decode(&buf)
                .unwrap_or_else(|e| panic!("{}: decode Go-encoded message: {e}", case.name));

            assert_eq!(crc, case.crc32, "{}: crc32", case.name);

            // The fields Go recorded for this message must be the ones this
            // side reads out of it. Compared in the to-JSON direction: the
            // shape is deliberately lossy for the two list-carrying variants
            // (see body_to_fields), so round-tripping the JSON back into a
            // Body would not reproduce them, while both sides' *rendering*
            // of the same message is exactly comparable.
            assert_eq!(
                msg_to_json(&msg).unwrap(),
                case.json,
                "{}: rendered fields",
                case.name
            );

            // And Go's signature must verify against the payload this side
            // derives -- the actual proof that both canonicalizations agree.
            // Skipped for the two variants a node accepts unsigned, whose
            // signature is 64 zero bytes by construction.
            if case.signed {
                verify(&verifying_key, &msg, crc, &sig)
                    .unwrap_or_else(|e| panic!("{}: verify Go signature: {e}", case.name));
            }
        }
    }

    #[test]
    fn rust_reencodes_go_messages_byte_for_byte() {
        let f = fixture();
        let seed: [u8; PRIVATE_KEY_SIZE] = hex_decode(&f.signing_key_seed_hex)
            .unwrap()
            .try_into()
            .expect("signing key seed length");
        let signing_key = SigningKey::from_bytes(&seed);

        for case in &f.cases {
            let (msg, _, _) = decode(&hex_decode(&case.encoded_hex).unwrap()).unwrap();
            let key = if case.signed {
                Some(&signing_key)
            } else {
                None
            };
            let reencoded = encode(&msg, key).unwrap();
            assert_eq!(
                hex_encode(&reencoded),
                case.encoded_hex,
                "{}: re-encoding a decoded Go message must reproduce it exactly",
                case.name
            );
        }
    }
}
