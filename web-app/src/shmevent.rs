//! Rust port of `pkg/shmevent`: the single wire structure (see
//! `api/shmevent.capnp`) used for every message exchanged between a raft
//! node instance and a local "user" -- here, this tab's main thread
//! talking to its own Worker over `shmring_ipc.rs`, and, since the same
//! relationship holds for a remote browser learner, this same struct over
//! `p2p.rs`'s `CLIENT_PROTOCOL` (`pkg/daemon.ClientProtocolID`). Replaces
//! `ipcproto.rs`.
//!
//! See `api/shmevent.capnp`'s doc comment for the full design rationale
//! (why every message carries exactly one raw value plus two relational
//! id fields, and how Set/Get decompose into short sequences of linked
//! messages); this module is a byte-for-byte-compatible reimplementation
//! of `pkg/shmevent`'s Go side, verified against it by the fact both
//! compile from the identical `api/shmevent.capnp` schema (see
//! `build.rs`).
#![allow(clippy::all)]

pub mod shmevent_capnp {
    include!(concat!(env!("OUT_DIR"), "/shmevent_capnp.rs"));
}

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

/// Event type bytes -- the wire values of `Msg.event_type`. See
/// `api/shmevent.capnp` and `pkg/shmevent`'s doc comment for the
/// SetKey/SetField/GetKey/GetField relational pattern.
pub const EVENT_SET_KEY: u8 = 1;
pub const EVENT_SET_FIELD: u8 = 2;
pub const EVENT_GET_KEY: u8 = 3;
pub const EVENT_GET_FIELD: u8 = 4;
pub const EVENT_GET_PUBLIC_KEY: u8 = 5;
pub const EVENT_GET_PRIVATE_KEY: u8 = 6;
pub const EVENT_ADD: u8 = 7;
/// Response-only; see `pkg/shmevent.EventError`'s doc comment for why
/// this exists even though it isn't part of `api/shmevent.capnp`'s
/// originally specified field set.
pub const EVENT_ERROR: u8 = 255;

/// Maximum length of `Msg.value` this module enforces (a convention, not
/// a capnp schema constraint).
pub const VALUE_SIZE: usize = 512;
pub const SIGNATURE_SIZE: usize = 64;
pub const PUBLIC_KEY_SIZE: usize = 32;
pub const PRIVATE_KEY_SIZE: usize = 32; // ed25519-dalek's SigningKey seed length

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Msg {
    pub event_type: u8,
    pub source_id: u16,
    pub destination_id: u16,
    pub value: Vec<u8>,
    pub id: u16,
}

impl Msg {
    pub fn error(id: u16, message: impl Into<String>) -> Msg {
        let mut value = message.into().into_bytes();
        value.truncate(VALUE_SIZE);
        Msg {
            event_type: EVENT_ERROR,
            source_id: 0,
            destination_id: 0,
            value,
            id,
        }
    }
}

#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shmevent: {}", self.0)
    }
}
impl std::error::Error for Error {}

impl From<capnp::Error> for Error {
    fn from(e: capnp::Error) -> Self {
        Error(e.to_string())
    }
}

/// `RequiresSignature` in `pkg/shmevent`: the two bootstrap events a node
/// accepts unsigned, since fetching one of them is the only way a caller
/// with no key yet obtains one -- see `api/shmevent.capnp`'s doc comment.
pub fn requires_signature(event_type: u8) -> bool {
    event_type != EVENT_GET_PUBLIC_KEY && event_type != EVENT_GET_PRIVATE_KEY
}

/// The fixed-width byte sequence CRC32 and the Ed25519 signature are
/// computed over: event(1) || source_id_BE(2) || destination_id_BE(2) ||
/// value, zero-padded/truncated to VALUE_SIZE || id_BE(2) -- see
/// `api/shmevent.capnp`'s doc comment and `pkg/shmevent`'s
/// `canonicalPayload`, which this matches byte-for-byte.
fn canonical_payload(m: &Msg) -> Vec<u8> {
    let mut buf = vec![0u8; 1 + 2 + 2 + VALUE_SIZE + 2];
    buf[0] = m.event_type;
    buf[1..3].copy_from_slice(&m.source_id.to_be_bytes());
    buf[3..5].copy_from_slice(&m.destination_id.to_be_bytes());
    let n = m.value.len().min(VALUE_SIZE);
    buf[5..5 + n].copy_from_slice(&m.value[..n]);
    buf[5 + VALUE_SIZE..].copy_from_slice(&m.id.to_be_bytes());
    buf
}

fn crc32_of(m: &Msg) -> u32 {
    crc32fast::hash(&canonical_payload(m))
}

/// What `sign`/`verify` actually operate on: the CRC-covered payload plus
/// the CRC itself, big-endian -- matches `pkg/shmevent`'s
/// `signedPayload`.
fn signed_payload(m: &Msg, crc: u32) -> Vec<u8> {
    let mut out = canonical_payload(m);
    out.extend_from_slice(&crc.to_be_bytes());
    out
}

/// Signs `m` (whose crc32 must already be `crc`) with `priv`, returning
/// the 64-byte signature to place in `Event.signature`. `priv` may be
/// `None` only for `EVENT_GET_PUBLIC_KEY`/`EVENT_GET_PRIVATE_KEY` requests
/// -- the two bootstrap events a node accepts unsigned -- in which case
/// this returns a zero-filled signature rather than an error, so
/// `encode`'s call site doesn't need a special case. Matches
/// `pkg/shmevent.Sign`.
pub fn sign(priv_key: Option<&SigningKey>, m: &Msg, crc: u32) -> Result<Vec<u8>, Error> {
    match priv_key {
        None => {
            if !requires_signature(m.event_type) {
                Ok(vec![0u8; SIGNATURE_SIZE])
            } else {
                Err(Error(format!(
                    "signing key required for event {}",
                    m.event_type
                )))
            }
        }
        Some(k) => {
            let sig: Signature = k.sign(&signed_payload(m, crc));
            Ok(sig.to_bytes().to_vec())
        }
    }
}

/// Checks `sig` against `m`/`crc` and `pub_key`. Matches
/// `pkg/shmevent.Verify`.
pub fn verify(pub_key: &VerifyingKey, m: &Msg, crc: u32, sig: &[u8]) -> Result<(), Error> {
    let sig_bytes: [u8; 64] = sig.try_into().map_err(|_| {
        Error(format!(
            "signature must be {SIGNATURE_SIZE} bytes, got {}",
            sig.len()
        ))
    })?;
    let signature = Signature::from_bytes(&sig_bytes);
    pub_key
        .verify(&signed_payload(m, crc), &signature)
        .map_err(|_| {
            Error(format!(
                "signature verification failed for event {} (id {})",
                m.event_type, m.id
            ))
        })
}

/// Serializes `m` to its capnp wire form, computing CRC32 and signing
/// with `priv_key`. Matches `pkg/shmevent.Encode`.
pub fn encode(m: &Msg, priv_key: Option<&SigningKey>) -> Result<Vec<u8>, Error> {
    if m.value.len() > VALUE_SIZE {
        return Err(Error(format!(
            "value too long: {} bytes (max {VALUE_SIZE})",
            m.value.len()
        )));
    }

    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<shmevent_capnp::event::Builder>();
    root.set_event(m.event_type);
    root.set_source_id(m.source_id);
    root.set_destination_id(m.destination_id);
    root.set_value(&m.value);
    root.set_id(m.id);

    let crc = crc32_of(m);
    root.set_crc32(crc);

    let sig = sign(priv_key, m, crc)?;
    root.set_signature(&sig);

    let mut buf = Vec::new();
    capnp::serialize::write_message(&mut buf, &message)?;
    Ok(buf)
}

/// Parses `buf` as a capnp Event message and verifies its CRC32 against
/// the decoded fields. Does not verify the signature -- callers that need
/// authenticity must call `verify` explicitly once they know which public
/// key to check against. Matches `pkg/shmevent.Decode`.
pub fn decode(buf: &[u8]) -> Result<(Msg, u32, Vec<u8>), Error> {
    let message_reader = capnp::serialize::read_message(
        &mut std::io::Cursor::new(buf),
        capnp::message::ReaderOptions::new(),
    )?;
    let root = message_reader.get_root::<shmevent_capnp::event::Reader>()?;

    let m = Msg {
        event_type: root.get_event(),
        source_id: root.get_source_id(),
        destination_id: root.get_destination_id(),
        value: root.get_value()?.to_vec(),
        id: root.get_id(),
    };
    let want_crc = root.get_crc32();
    let got_crc = crc32_of(&m);
    if got_crc != want_crc {
        return Err(Error(format!(
            "crc32 mismatch: got {got_crc:#x}, message says {want_crc:#x}"
        )));
    }
    let sig = root.get_signature()?.to_vec();
    Ok((m, want_crc, sig))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    // Fixed, deterministic 32-byte seeds -- plain distinct test fixtures,
    // not meant to be cryptographically random (no OsRng dependency
    // needed just for tests).
    fn test_key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }
    fn other_test_key() -> SigningKey {
        SigningKey::from_bytes(&[9u8; 32])
    }

    #[test]
    fn encode_decode_roundtrip() {
        let signing_key = test_key();
        let verifying_key = signing_key.verifying_key();

        let m = Msg {
            event_type: EVENT_SET_FIELD,
            source_id: 42,
            destination_id: 0,
            value: b"world".to_vec(),
            id: 7,
        };

        let buf = encode(&m, Some(&signing_key)).unwrap();
        let (got, crc, sig) = decode(&buf).unwrap();
        assert_eq!(got, m);
        verify(&verifying_key, &got, crc, &sig).unwrap();

        let other_key = other_test_key().verifying_key();
        assert!(verify(&other_key, &got, crc, &sig).is_err());
    }

    #[test]
    fn decode_detects_corruption() {
        let signing_key = test_key();
        let value = b"hello-corruption-marker".to_vec();
        let m = Msg {
            event_type: EVENT_GET_FIELD,
            source_id: 0,
            destination_id: 0,
            value: value.clone(),
            id: 1,
        };
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
        let m = Msg {
            event_type: EVENT_SET_KEY,
            source_id: 0,
            destination_id: 0,
            value: b"hello".to_vec(),
            id: 99,
        };
        let crc = crc32_of(&m);
        let sig = sign(Some(&signing_key), &m, crc).unwrap();
        verify(&verifying_key, &m, crc, &sig).unwrap();

        let mut tampered = m.clone();
        tampered.source_id += 1;
        assert!(verify(&verifying_key, &tampered, crc, &sig).is_err());
    }

    #[test]
    fn get_public_private_key_events_sign_with_none_key() {
        let m = Msg {
            event_type: EVENT_GET_PUBLIC_KEY,
            source_id: 0,
            destination_id: 0,
            value: vec![],
            id: 3,
        };
        let buf = encode(&m, None).unwrap();
        decode(&buf).unwrap();

        let m2 = Msg {
            event_type: EVENT_SET_KEY,
            ..m
        };
        assert!(encode(&m2, None).is_err());
    }

    #[test]
    fn value_too_long_rejected() {
        let signing_key = test_key();
        let m = Msg {
            event_type: EVENT_SET_KEY,
            source_id: 0,
            destination_id: 0,
            value: vec![0u8; VALUE_SIZE + 1],
            id: 1,
        };
        assert!(encode(&m, Some(&signing_key)).is_err());
    }
}
