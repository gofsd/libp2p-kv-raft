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
//!
//! `system`/`logpermit`/`catalog_keys` mirror `pkg/shmevent`'s own
//! `system.go`/`logpermit.go`/`catalog.go` split -- the `SystemKeyPrefix`
//! namespace's key-building and payload-framing helpers for permits and
//! the Group/Command ACL catalog, layered on top of the event framing
//! this file defines.
//!
//! # KNOWN BROKEN: out of sync with `api/shmevent.capnp`
//!
//! `api/shmevent.capnp` was rewritten from a flat `Event{event, sourceId,
//! destinationId, value, crc32, signature, id}` struct into a real union
//! (one variant per operation, each with its own named fields -- see that
//! file's own doc comment). This crate, and every hand-packed
//! byte-framing helper in `catalog_keys.rs`/`logpermit.rs`/`system.rs`,
//! still assumes the old flat shape and has **not** been ported to the
//! new one. A browser client built from this crate as it stands cannot
//! talk to the rest of the mesh (every other client -- desktop, Android --
//! was migrated in the same change). Porting this crate is its own
//! follow-up plan, deliberately out of scope for the Go-side rewrite that
//! left it in this state.
#![allow(clippy::all)]

pub mod catalog_keys;
pub mod logpermit;
pub mod system;

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
/// Single-round-trip alternative to the SetKey+SetField pair -- see
/// `pkg/shmevent.EventSet`'s doc comment. `Value` is
/// [`encode_set_payload`]`(key, value)`.
pub const EVENT_SET: u8 = 8;
/// Direct, unreplicated peer-to-peer notification -- see
/// `pkg/shmevent.EventExecute`'s doc comment. Needs a prior
/// `EVENT_SET_KEY` pair registering sender/receiver peer ids under
/// `SourceID`/`DestinationID`.
pub const EVENT_EXECUTE: u8 = 11;
/// Drains one queued `EVENT_EXECUTE` notification addressed to this node,
/// oldest first -- empty `Value` if none queued, otherwise
/// [`encode_execute_notification`].
pub const EVENT_POLL_EXECUTE: u8 = 12;
/// Answers one bounded key-range scan page against the local store --
/// `Value` is [`encode_list_range_query`]`(start, end)`; the response
/// reuses the same framing for `(key, value)`, empty if exhausted. See
/// `pkg/shmevent.EventListRange`'s doc comment: no bulk response, a
/// caller wanting every match loops, narrowing `start` past the last
/// returned key each round.
pub const EVENT_LIST_RANGE: u8 = 14;
/// Writes one `pkg/logrecord` record -- the one legitimate way into that
/// reserved key namespace, `EVENT_SET`/`EVENT_SET_FIELD` refuse it. Same
/// payload framing as `EVENT_SET`.
pub const EVENT_LOG_APPEND: u8 = 15;
/// Lodges a pending `KindLogPermit` record, scoped by an arbitrary
/// `logKind` string in addition to `peerID`. `Value` is
/// [`logpermit::encode_log_permit_request_payload`].
pub const EVENT_LOG_PERMIT_REQUEST: u8 = 16;
/// Promotes a pending log-permit record to confirmed -- raft-voter-only.
/// `Value` is [`logpermit::encode_log_permit_confirm_payload`].
pub const EVENT_LOG_PERMIT_CONFIRM: u8 = 17;
/// Deletes a confirmed log-permit record outright -- raft-voter-only.
/// Same payload shape as `EVENT_LOG_PERMIT_CONFIRM`.
pub const EVENT_LOG_PERMIT_REVOKE: u8 = 18;
/// Asks this node's own current cluster to remove it. **Refused by
/// `pkg/daemon.handleShmEvent` for any remote (`ClientProtocolID`)
/// caller** -- "only this node's own operator decides to leave" -- so
/// this constant exists for wire-table completeness (a message carrying
/// byte 19 should decode to a real name, not "unknown") only; no web
/// client capability is built on it, and none ever legitimately can be.
pub const EVENT_LEAVE: u8 = 19;
/// Atomically applies an ordered op list -- see `pkg/shmevent.EventTxn`.
/// Not sent by this client, but defined so `value_size_for` can name it:
/// its width has to match Go's whether or not this client ever uses it.
pub const EVENT_TXN: u8 = 44;
/// Local-only self-service escalation: dials `Value`'s target address
/// directly (no relay reservation -- see `p2p::Handle::connect`) and
/// submits the always-public `public-access` command there under this
/// tab's own identity, granting it real `ReservedGroupChannel`/
/// `ReservedGroupRelay` standing on that cluster in one raft-committed
/// write. `Value` is `target_addr` or `target_addr#note` (see
/// `app::do_public_access`) -- matches `pkg/shmevent.EventPublicAccess`
/// and `pkg/shmclient.Session.PublicAccess`'s identical `#`-separated
/// convention. Response `Value` is a hex instance id, mirroring
/// `dialAndSubmitPublicAccess`'s return.
pub const EVENT_PUBLIC_ACCESS: u8 = 47;
/// Generic envelope for the group-based ACL catalog's single-step CRUD
/// (Group/Command/GroupCommand/PeerGroup/Station Put+Delete) -- see
/// `pkg/shmevent.EventCatalogPut`'s doc comment. Defined here for wire
/// -table completeness only, same reasoning as `EVENT_TXN`: this client is
/// a non-voting learner and none of the events it generalizes were ever
/// reachable from one.
pub const EVENT_CATALOG_PUT: u8 = 53;
/// `EVENT_CATALOG_PUT`'s delete counterpart -- see
/// `pkg/shmevent.EventCatalogDelete`.
pub const EVENT_CATALOG_DELETE: u8 = 54;
/// Generic envelope for every Permit-style Request/Confirm/Revoke and (on
/// the Go side only, see `shmevent::system`'s doc comment) JoinInvite/
/// ExecInvite Create/Revoke -- see `pkg/shmevent.EventLifecycleWrite`'s doc
/// comment. `Value` is [`system::encode_lifecycle_write_payload`];
/// `app::request_permit` is this client's one real caller, sending a
/// Permit-style `system::LIFECYCLE_ACTION_REQUEST`, the one case this
/// family has ever been reachable from a non-voting learner for.
pub const EVENT_LIFECYCLE_WRITE: u8 = 55;
/// `EVENT_PUBLIC_ACCESS` generalized to an arbitrary command id -- see
/// `pkg/shmevent.EventDialSubmitCommand`'s doc comment. Defined here for
/// wire-table completeness only, same reasoning as `EVENT_TXN`: this
/// client has no `do_dial_submit_command` caller of its own yet.
pub const EVENT_DIAL_SUBMIT_COMMAND: u8 = 56;
/// `EVENT_DIAL_SUBMIT_COMMAND`'s read-back counterpart -- see
/// `pkg/shmevent.EventDialQueryCommandLog`'s doc comment. Defined here for
/// wire-table completeness only, same reasoning as `EVENT_DIAL_SUBMIT_COMMAND`.
pub const EVENT_DIAL_QUERY_COMMAND_LOG: u8 = 57;
/// Response-only; see `pkg/shmevent.EventError`'s doc comment for why
/// this exists even though it isn't part of `api/shmevent.capnp`'s
/// originally specified field set.
pub const EVENT_ERROR: u8 = 255;

/// Maximum length of `Msg.value` for most events (a convention, not a capnp
/// schema constraint), and the fixed width `canonical_payload` zero-pads to
/// before CRC/signing.
pub const VALUE_SIZE: usize = 512;

/// The plain-KV data path's larger ceiling, mirroring
/// `pkg/shmevent.KVValueSize`. **This must stay in lockstep with Go's
/// `valueSizeFor`**: it feeds `canonical_width`, the padding width the
/// signed payload uses, so if the two sides disagree about an event's
/// width they compute different CRCs and different signatures over
/// identical messages, and every such message is rejected as forged. That
/// is not a theoretical risk -- it is what happened when Go gained this
/// constant and this file still padded everything to VALUE_SIZE.
///
/// Note the ceiling alone does not decide the padding width: a value that
/// fits `VALUE_SIZE` keeps being padded to `VALUE_SIZE`, so raising a
/// ceiling never invalidates messages older peers can already verify. See
/// `canonical_width`.
pub const KV_VALUE_SIZE: usize = 4 * 1024;

/// The maximum `Msg.value` length -- and the fixed padding width -- for
/// `event`. Mirrors `pkg/shmevent.valueSizeFor`. Channel events are absent
/// deliberately: this client implements no Channel support, so it never
/// constructs or verifies one (see Go's `ChannelValueSize` doc comment).
pub fn value_size_for(event: u8) -> usize {
    match event {
        EVENT_SET_KEY | EVENT_SET_FIELD | EVENT_SET | EVENT_GET_FIELD | EVENT_TXN
        | EVENT_LOG_APPEND
        // EVENT_CATALOG_PUT's payload can carry a Command's form spec or a
        // Station's attrs -- matches pkg/shmevent.valueSizeFor's identical
        // EventCatalogPut case.
        | EVENT_CATALOG_PUT
        // Request carries a caller-authored inputsJSON blob; response
        // aggregates several log records -- matches
        // pkg/shmevent.valueSizeFor's identical case for both events.
        | EVENT_DIAL_SUBMIT_COMMAND
        | EVENT_DIAL_QUERY_COMMAND_LOG => KV_VALUE_SIZE,
        _ => VALUE_SIZE,
    }
}

/// The width `canonical_payload` zero-pads to -- mirrors
/// `pkg/shmevent.canonicalWidth`, and deliberately is *not* just
/// `value_size_for`. Peers do not upgrade together (a deployed relay, an
/// installed Android build, this tab), so raising an event's ceiling must
/// not change the signed bytes of a message that would have fit the old
/// one: a value that fits `VALUE_SIZE` is always padded to `VALUE_SIZE`,
/// the width every build has ever used for it, and only a value that
/// genuinely needs the larger ceiling uses it (no older peer can have
/// seen such a message at all). See Go's `canonicalWidth` doc comment for
/// what going the other way cost.
fn canonical_width(event: u8, value_len: usize) -> usize {
    if value_len <= VALUE_SIZE {
        return VALUE_SIZE;
    }
    value_size_for(event)
}
/// Payload size of the main-thread <-> Worker IPC rings
/// (`shmring_ipc::CAPACITY`). It lives here, beside the value ceilings it
/// has to keep up with, rather than in that module, for one reason: that
/// module is `cfg(target_arch = "wasm32")` and so cannot be unit-tested on
/// the host, and this constant is exactly the kind that goes stale
/// silently. `ipc_ring_fits_largest_encoded_message` pins it against every
/// tier `value_size_for` defines.
///
/// It was last found stale at 4096 -- the same number `KV_VALUE_SIZE` had
/// just been raised to -- which made a full-size KV message 4208 bytes
/// encoded and unable to fit its own transport. That is a hang, not an
/// error: both writers fill the ring before any reader exists.
pub const IPC_RING_CAPACITY: u64 = 32 * 1024;

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
    // Width is per event type *and* value length (see canonical_width),
    // exactly as Go's canonicalPayload does -- safe because the event type
    // sits at byte 0 and the value's own length is known from the decoded
    // message, both before any padding has to be reproduced.
    let vs = canonical_width(m.event_type, m.value.len());
    let mut buf = vec![0u8; 1 + 2 + 2 + vs + 2];
    buf[0] = m.event_type;
    buf[1..3].copy_from_slice(&m.source_id.to_be_bytes());
    buf[3..5].copy_from_slice(&m.destination_id.to_be_bytes());
    let n = m.value.len().min(vs);
    buf[5..5 + n].copy_from_slice(&m.value[..n]);
    buf[5 + vs..].copy_from_slice(&m.id.to_be_bytes());
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
    let max = value_size_for(m.event_type);
    if m.value.len() > max {
        return Err(Error(format!(
            "value too long: {} bytes (max {max})",
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

/// Human-readable name for `event_type`, matching `pkg/shmevent.EventName`
/// -- "unknown" for anything not defined above.
pub fn event_name(event_type: u8) -> &'static str {
    match event_type {
        EVENT_SET_KEY => "set_key",
        EVENT_SET_FIELD => "set_field",
        EVENT_GET_KEY => "get_key",
        EVENT_GET_FIELD => "get_field",
        EVENT_GET_PUBLIC_KEY => "get_public_key",
        EVENT_GET_PRIVATE_KEY => "get_private_key",
        EVENT_ADD => "add",
        EVENT_SET => "set",
        EVENT_EXECUTE => "execute",
        EVENT_POLL_EXECUTE => "poll_execute",
        EVENT_LIST_RANGE => "list_range",
        EVENT_LOG_APPEND => "log_append",
        EVENT_LOG_PERMIT_REQUEST => "log_permit_request",
        EVENT_LOG_PERMIT_CONFIRM => "log_permit_confirm",
        EVENT_LOG_PERMIT_REVOKE => "log_permit_revoke",
        EVENT_LEAVE => "leave",
        EVENT_PUBLIC_ACCESS => "public_access",
        EVENT_CATALOG_PUT => "catalog_put",
        EVENT_CATALOG_DELETE => "catalog_delete",
        EVENT_LIFECYCLE_WRITE => "lifecycle_write",
        EVENT_DIAL_SUBMIT_COMMAND => "dial_submit_command",
        EVENT_DIAL_QUERY_COMMAND_LOG => "dial_query_command_log",
        EVENT_ERROR => "error",
        _ => "unknown",
    }
}

/// Inverse of [`event_name`], matching `pkg/shmevent.EventFromName`.
pub fn event_from_name(name: &str) -> Option<u8> {
    match name {
        "set_key" => Some(EVENT_SET_KEY),
        "set_field" => Some(EVENT_SET_FIELD),
        "get_key" => Some(EVENT_GET_KEY),
        "get_field" => Some(EVENT_GET_FIELD),
        "get_public_key" => Some(EVENT_GET_PUBLIC_KEY),
        "get_private_key" => Some(EVENT_GET_PRIVATE_KEY),
        "add" => Some(EVENT_ADD),
        "set" => Some(EVENT_SET),
        "execute" => Some(EVENT_EXECUTE),
        "poll_execute" => Some(EVENT_POLL_EXECUTE),
        "list_range" => Some(EVENT_LIST_RANGE),
        "log_append" => Some(EVENT_LOG_APPEND),
        "log_permit_request" => Some(EVENT_LOG_PERMIT_REQUEST),
        "log_permit_confirm" => Some(EVENT_LOG_PERMIT_CONFIRM),
        "log_permit_revoke" => Some(EVENT_LOG_PERMIT_REVOKE),
        "leave" => Some(EVENT_LEAVE),
        "public_access" => Some(EVENT_PUBLIC_ACCESS),
        "catalog_put" => Some(EVENT_CATALOG_PUT),
        "catalog_delete" => Some(EVENT_CATALOG_DELETE),
        "lifecycle_write" => Some(EVENT_LIFECYCLE_WRITE),
        "dial_submit_command" => Some(EVENT_DIAL_SUBMIT_COMMAND),
        "dial_query_command_log" => Some(EVENT_DIAL_QUERY_COMMAND_LOG),
        "error" => Some(EVENT_ERROR),
        _ => None,
    }
}

/// Packs `key` and `value` into a single `EVENT_SET`/`EVENT_LOG_APPEND`
/// `Msg.value`: a 2-byte big-endian length prefix for `key`, then `key`
/// verbatim, then `value` verbatim (the rest of the buffer, no length
/// prefix of its own). Matches `pkg/shmevent.EncodeSetPayload`.
pub fn encode_set_payload(key: &[u8], value: &[u8]) -> Result<Vec<u8>, Error> {
    if key.len() > 0xFFFF {
        return Err(Error(format!(
            "set payload key too long: {} bytes",
            key.len()
        )));
    }
    let mut buf = Vec::with_capacity(2 + key.len() + value.len());
    buf.push((key.len() >> 8) as u8);
    buf.push(key.len() as u8);
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    Ok(buf)
}

/// Inverse of [`encode_set_payload`]. Matches
/// `pkg/shmevent.DecodeSetPayload`.
pub fn decode_set_payload(payload: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "set payload too short: {} bytes",
            payload.len()
        )));
    }
    let key_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    if 2 + key_len > payload.len() {
        return Err(Error(format!(
            "set payload key length {key_len} exceeds payload size {}",
            payload.len()
        )));
    }
    Ok((&payload[2..2 + key_len], &payload[2 + key_len..]))
}

/// Packs `start`/`end` (both inclusive store key bounds) into a single
/// `EVENT_LIST_RANGE` request `Value` -- identical framing to
/// [`encode_set_payload`], reused under a name that reads correctly at a
/// list-range call site. Matches `pkg/shmevent.EncodeListRangeQuery`. Also
/// used, under this same name, to decode/encode an `EVENT_LIST_RANGE`
/// response's `(key, value)` pair -- see that event's doc comment.
pub fn encode_list_range_query(start: &[u8], end: &[u8]) -> Result<Vec<u8>, Error> {
    encode_set_payload(start, end)
}

/// Inverse of [`encode_list_range_query`].
pub fn decode_list_range_query(payload: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    decode_set_payload(payload)
}

/// Packs `sender_peer_id` and `payload` into a single value: a 2-byte
/// big-endian length prefix for `sender_peer_id`, then `sender_peer_id`
/// verbatim, then `payload` (the rest of the buffer). Used both for the
/// wire message a peer-to-peer `Execute` delivery carries and for
/// `EVENT_POLL_EXECUTE`'s response. Matches
/// `pkg/shmevent.EncodeExecuteNotification`.
pub fn encode_execute_notification(sender_peer_id: &[u8], payload: &[u8]) -> Result<Vec<u8>, Error> {
    if sender_peer_id.len() > 0xFFFF {
        return Err(Error(format!(
            "execute notification sender peer id too long: {} bytes",
            sender_peer_id.len()
        )));
    }
    let mut buf = Vec::with_capacity(2 + sender_peer_id.len() + payload.len());
    buf.push((sender_peer_id.len() >> 8) as u8);
    buf.push(sender_peer_id.len() as u8);
    buf.extend_from_slice(sender_peer_id);
    buf.extend_from_slice(payload);
    Ok(buf)
}

/// Inverse of [`encode_execute_notification`].
pub fn decode_execute_notification(data: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    if data.len() < 2 {
        return Err(Error(format!(
            "execute notification too short: {} bytes",
            data.len()
        )));
    }
    let id_len = ((data[0] as usize) << 8) | data[1] as usize;
    if 2 + id_len > data.len() {
        return Err(Error(format!(
            "execute notification sender peer id length {id_len} exceeds payload size {}",
            data.len()
        )));
    }
    Ok((&data[2..2 + id_len], &data[2 + id_len..]))
}

/// Decodes a hex string to raw bytes -- hand-rolled rather than pulling in
/// a `hex` crate dependency for the handful of call sites that need it
/// (a build-time identity seed, and [`value_from_json`]'s "0x..." case).
pub fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex string".to_string());
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    for chunk in bytes.chunks(2) {
        let hi = (chunk[0] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex digit {:?}", chunk[0] as char))?;
        let lo = (chunk[1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex digit {:?}", chunk[1] as char))?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

/// Encodes raw bytes to a lowercase hex string.
pub fn hex_encode(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len() * 2);
    for b in raw {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// `EventJson` is `Msg`'s JSON shape, matching `pkg/e2edata.Event` exactly
/// -- `event` as the name [`event_name`] prints rather than the raw byte,
/// `value` as plain text when it's valid UTF-8 (every KV test key/value in
/// practice) or a "0x"-prefixed hex string otherwise (a raw Ed25519 key, or
/// deliberately-corrupt test bytes) -- see that Go type's doc comment for
/// the full reasoning. This is a pure JSON presentation layer: it changes
/// nothing about the capnp wire structure `encode`/`decode` (de)serialize.
#[derive(serde::Serialize, serde::Deserialize)]
struct EventJson {
    event: String,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    source_id: u16,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    destination_id: u16,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    value: String,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    id: u16,
}

fn is_zero_u16(v: &u16) -> bool {
    *v == 0
}

fn value_to_json(raw: &[u8]) -> String {
    match std::str::from_utf8(raw) {
        Ok(s) if !raw.is_empty() => s.to_string(),
        Ok(_) => String::new(),
        Err(_) => format!("0x{}", hex_encode(raw)),
    }
}

fn value_from_json(s: &str) -> Result<Vec<u8>, Error> {
    if s.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(rest) = s.strip_prefix("0x") {
        return hex_decode(rest).map_err(Error);
    }
    Ok(s.as_bytes().to_vec())
}

/// Serializes `m` to `EventJson`'s shape -- the same human-readable form
/// `pkg/e2edata.Event`/kvctl-cli sendevent use, e.g.
/// `{"event":"get_field","value":"hello"}`.
pub fn msg_to_json(m: &Msg) -> Result<String, Error> {
    let json = EventJson {
        event: event_name(m.event_type).to_string(),
        source_id: m.source_id,
        destination_id: m.destination_id,
        value: value_to_json(&m.value),
        id: m.id,
    };
    serde_json::to_string(&json).map_err(|e| Error(e.to_string()))
}

/// Inverse of [`msg_to_json`].
pub fn msg_from_json(s: &str) -> Result<Msg, Error> {
    let json: EventJson = serde_json::from_str(s).map_err(|e| Error(e.to_string()))?;
    let event_type = event_from_name(&json.event)
        .ok_or_else(|| Error(format!("unknown event name {:?}", json.event)))?;
    let value = value_from_json(&json.value)?;
    Ok(Msg {
        event_type,
        source_id: json.source_id,
        destination_id: json.destination_id,
        value,
        id: json.id,
    })
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

    /// The ceiling `encode` enforces is per event (`value_size_for`), so
    /// this checks both tiers: an event still on `VALUE_SIZE` rejects one
    /// byte past it, and a KV-tier event rejects only one byte past
    /// `KV_VALUE_SIZE` -- while `VALUE_SIZE + 1` is now perfectly legal
    /// for it.
    #[test]
    fn value_too_long_rejected() {
        let signing_key = test_key();
        let too_long = |event: u8, len: usize| {
            let m = Msg {
                event_type: event,
                source_id: 0,
                destination_id: 0,
                value: vec![0u8; len],
                id: 1,
            };
            encode(&m, Some(&signing_key)).is_err()
        };
        assert!(too_long(EVENT_LIFECYCLE_WRITE, VALUE_SIZE + 1));
        assert!(!too_long(EVENT_SET_KEY, VALUE_SIZE + 1));
        assert!(!too_long(EVENT_SET_KEY, KV_VALUE_SIZE));
        assert!(too_long(EVENT_SET_KEY, KV_VALUE_SIZE + 1));

        // EVENT_CATALOG_PUT's payload can carry a Command's form spec or a
        // Station's attrs -- it must inherit their wider KV_VALUE_SIZE
        // ceiling, not fall back to plain VALUE_SIZE. Pins the same
        // regression pkg/shmevent's identical test does on the Go side.
        assert!(!too_long(EVENT_CATALOG_PUT, VALUE_SIZE + 1));
        assert!(!too_long(EVENT_CATALOG_PUT, KV_VALUE_SIZE));
        assert!(too_long(EVENT_CATALOG_PUT, KV_VALUE_SIZE + 1));
        assert!(too_long(EVENT_CATALOG_DELETE, VALUE_SIZE + 1));
    }

    #[test]
    fn event_name_round_trip() {
        for e in [
            EVENT_SET_KEY,
            EVENT_SET_FIELD,
            EVENT_GET_KEY,
            EVENT_GET_FIELD,
            EVENT_GET_PUBLIC_KEY,
            EVENT_GET_PRIVATE_KEY,
            EVENT_ADD,
            EVENT_SET,
            EVENT_EXECUTE,
            EVENT_POLL_EXECUTE,
            EVENT_LIST_RANGE,
            EVENT_LOG_APPEND,
            EVENT_LOG_PERMIT_REQUEST,
            EVENT_LOG_PERMIT_CONFIRM,
            EVENT_LOG_PERMIT_REVOKE,
            EVENT_LEAVE,
            EVENT_PUBLIC_ACCESS,
            EVENT_CATALOG_PUT,
            EVENT_CATALOG_DELETE,
            EVENT_LIFECYCLE_WRITE,
            EVENT_DIAL_SUBMIT_COMMAND,
            EVENT_DIAL_QUERY_COMMAND_LOG,
            EVENT_ERROR,
        ] {
            let name = event_name(e);
            assert_eq!(event_from_name(name), Some(e), "round trip for {name:?}");
        }
        assert_eq!(event_from_name("not_a_real_event"), None);
    }

    #[test]
    fn msg_json_human_readable() {
        let m = Msg {
            event_type: EVENT_SET_FIELD,
            source_id: 100,
            destination_id: 0,
            value: b"world".to_vec(),
            id: 7,
        };
        let json = msg_to_json(&m).unwrap();
        assert_eq!(
            json,
            r#"{"event":"set_field","source_id":100,"value":"world","id":7}"#
        );

        let back = msg_from_json(&json).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn msg_json_binary_value_uses_hex_prefix() {
        let raw = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0xff];
        let m = Msg {
            event_type: EVENT_GET_PUBLIC_KEY,
            source_id: 0,
            destination_id: 0,
            value: raw.clone(),
            id: 0,
        };
        let json = msg_to_json(&m).unwrap();
        assert!(
            json.contains(r#""0xdeadbeef00ff""#),
            "json = {json}, want a 0x-prefixed hex value"
        );

        let back = msg_from_json(&json).unwrap();
        assert_eq!(back.value, raw);
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

    #[test]
    fn set_payload_round_trip() {
        let (key, value) = (b"my-key".as_slice(), b"my-value".as_slice());
        let encoded = encode_set_payload(key, value).unwrap();
        let (got_key, got_value) = decode_set_payload(&encoded).unwrap();
        assert_eq!(got_key, key);
        assert_eq!(got_value, value);
    }

    #[test]
    fn set_payload_empty_key() {
        let encoded = encode_set_payload(b"", b"value-only").unwrap();
        let (got_key, got_value) = decode_set_payload(&encoded).unwrap();
        assert_eq!(got_key, b"");
        assert_eq!(got_value, b"value-only");
    }

    #[test]
    fn set_payload_too_short_rejected() {
        assert!(decode_set_payload(&[0u8]).is_err());
        assert!(decode_set_payload(&[]).is_err());
    }

    #[test]
    fn set_payload_key_length_overflow_rejected() {
        // Claims a 10-byte key but only provides 2 bytes of payload after
        // the length prefix.
        assert!(decode_set_payload(&[0x00, 0x0a, 0x01, 0x02]).is_err());
    }

    #[test]
    fn list_range_query_round_trip() {
        let (start, end) = (b"aaa".as_slice(), b"zzz".as_slice());
        let encoded = encode_list_range_query(start, end).unwrap();
        let (got_start, got_end) = decode_list_range_query(&encoded).unwrap();
        assert_eq!(got_start, start);
        assert_eq!(got_end, end);
    }

    #[test]
    fn execute_notification_round_trip() {
        let (sender, payload) = (b"12D3KooWabc".as_slice(), b"hello-execute".as_slice());
        let encoded = encode_execute_notification(sender, payload).unwrap();
        let (got_sender, got_payload) = decode_execute_notification(&encoded).unwrap();
        assert_eq!(got_sender, sender);
        assert_eq!(got_payload, payload);
    }

    #[test]
    fn execute_notification_too_short_rejected() {
        assert!(decode_execute_notification(&[0u8]).is_err());
    }

    /// The IPC rings this crate talks to its own Worker over must fit the
    /// largest message the protocol lets anyone build, for every event.
    /// They are single-use rings written in full before the other side
    /// exists, so "too small" does not surface as a rejected message --
    /// it is a tab that stops responding.
    #[test]
    fn ipc_ring_fits_largest_encoded_message() {
        let signing_key = test_key();
        for event in [
            EVENT_SET_KEY,
            EVENT_SET_FIELD,
            EVENT_SET,
            EVENT_GET_FIELD,
            EVENT_TXN,
            EVENT_LOG_APPEND,
            EVENT_CATALOG_PUT,
            EVENT_ADD,
            EVENT_PUBLIC_ACCESS,
            EVENT_DIAL_SUBMIT_COMMAND,
            EVENT_DIAL_QUERY_COMMAND_LOG,
        ] {
            let m = Msg {
                event_type: event,
                value: vec![0u8; value_size_for(event)],
                ..Default::default()
            };
            let encoded = encode(&m, Some(&signing_key)).unwrap();
            assert!(
                encoded.len() as u64 <= IPC_RING_CAPACITY,
                "event {event}: a full-size value encodes to {} bytes, over the {}-byte IPC ring -- raise IPC_RING_CAPACITY",
                encoded.len(),
                IPC_RING_CAPACITY,
            );
        }
    }

    /// Mirrors Go's TestCanonicalWidthKeepsHistoricalWidthForSmallValues.
    /// Peers do not upgrade together, so raising an event's ceiling must
    /// not change the signed bytes of a message that would have fit the
    /// old one -- otherwise every peer still on an older build rejects it
    /// as forged, silently (a message that fails to decode gets no reply
    /// at all).
    #[test]
    fn canonical_width_keeps_historical_width_for_small_values() {
        for event in [
            EVENT_SET_KEY,
            EVENT_SET_FIELD,
            EVENT_SET,
            EVENT_GET_FIELD,
            EVENT_TXN,
            EVENT_LOG_APPEND,
            EVENT_CATALOG_PUT,
        ] {
            for len in [0, 1, 200, VALUE_SIZE] {
                assert_eq!(canonical_width(event, len), VALUE_SIZE, "event {event}, len {len}");
            }
            assert_eq!(canonical_width(event, VALUE_SIZE + 1), KV_VALUE_SIZE, "event {event}");
        }
    }

    /// The same property where a peer actually feels it: a small-value
    /// message must be checksummed over the 512-wide payload every build
    /// of this project has always used, whatever the event's current
    /// ceiling is.
    #[test]
    fn small_value_crc_is_width_stable() {
        let m = Msg {
            event_type: EVENT_LOG_APPEND,
            source_id: 0,
            destination_id: 0,
            value: b"a small journal record".to_vec(),
            id: 7,
        };
        // Built by hand rather than via canonical_payload, so this still
        // fails if that function starts agreeing with itself about a
        // different width.
        let mut historical = vec![0u8; 1 + 2 + 2 + VALUE_SIZE + 2];
        historical[0] = m.event_type;
        historical[5..5 + m.value.len()].copy_from_slice(&m.value);
        historical[5 + VALUE_SIZE..].copy_from_slice(&m.id.to_be_bytes());
        assert_eq!(crc32_of(&m), crc32fast::hash(&historical));
    }
}
