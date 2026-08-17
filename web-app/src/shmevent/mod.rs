//! Rust port of `pkg/shmevent`: the single wire structure (see
//! `api/shmevent.capnp`) used for every message exchanged between a raft
//! node instance and a local "user" -- here, this tab's main thread
//! talking to its own Worker over `shmring_ipc.rs`, and, since the same
//! relationship holds for a remote browser learner, this same struct over
//! `p2p.rs`'s `CLIENT_PROTOCOL` (`pkg/daemon.ClientProtocolID`).
//!
//! See `api/shmevent.capnp`'s doc comment for the full design rationale.
//! The shape to understand before reading anything here: three fields are
//! common to every message (`id`, `crc32`, `signature`) and everything else
//! is a union with one variant per logical operation, each carrying its own
//! named, typed fields. [`Body`] is that union, one Rust variant per capnp
//! group, and [`Msg`] is `id` plus a `Body`.
//!
//! # The two rules that make this a byte-compatible mirror
//!
//! **1. CRC and signature cover the message's *canonical* capnp encoding**,
//! not its marshaled bytes: `crc32` is CRC-32/IEEE over the canonical form
//! with `crc32` zeroed and `signature` unset, and the Ed25519 signature is
//! over the same thing with `crc32` filled in. Canonicalization is what
//! makes that well-defined at all -- a message's segment layout depends on
//! how it was built, so two logically identical messages only have
//! identical bytes in canonical form ("The result will be identical for
//! equivalent structs"). This matches `pkg/shmevent/sign.go`'s
//! `marshalWithCrcAndEmptySig` exactly, which is the only reason a
//! signature this file produces verifies on a Go node and vice versa.
//!
//! **2. A null pointer and a present-but-empty one are different messages.**
//! Verified against the Go implementation rather than assumed: canonicalizing
//! the same `setKey` twice, once with `SetSignature(nil)` and once with
//! `SetSignature([]byte{})`, gives different bytes (`0000000000000000` vs
//! `0500000002000000` in the pointer section) and therefore a different CRC
//! and a different signature. So every capnp `Data` and `List` field here is
//! an [`Option`] -- [`DataField`] -- where `None` means "null pointer" and
//! `Some(vec![])` means "present, zero length", and the two are never
//! silently interchanged. `Text` fields are plain [`String`], because Go's
//! `Struct.SetText` maps `""` to a null pointer, so an empty Text is
//! unrepresentable there and `""` <-> null is the faithful mapping (this
//! file writes no Text field whose value is empty, for the same reason).
//!
//! Those two rules are why [`encode`]/[`decode`] rebuild a capnp message
//! from [`Msg`] to compute a signature payload instead of hashing anything
//! this file lays out by hand. An earlier version of this module did lay it
//! out by hand -- a fixed-width `event || source_id || destination_id ||
//! zero-padded value || id` buffer -- which the Go side has since dropped
//! along with the flat struct it described.
//!
//! `system`/`catalog_keys` mirror `pkg/shmevent`'s own `system.go`/
//! `catalog.go` split: the `SystemKeyPrefix`
//! namespace's *store* key-building and stored-record payload framing for
//! permits and the Group/Command ACL catalog. Those payloads are the values
//! records hold in the KV store, which is a separate encoding from the wire
//! events in this file -- catalog and permit *writes* are now ordinary union
//! variants ([`Body::GroupPut`], [`Body::PermitRequest`], ...) with named
//! fields, not hand-packed blobs inside a generic envelope.

pub mod catalog_keys;
pub mod system;

pub mod shmevent_capnp {
    include!(concat!(env!("OUT_DIR"), "/shmevent_capnp.rs"));
}

use capnp::message::{Builder as MessageBuilder, ReaderOptions};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use shmevent_capnp::event;
use std::collections::BTreeMap;

/// Ceiling on any single `Data` field, mirroring `pkg/shmevent.MaxValueSize`.
/// One number for every variant: the union's named fields replaced the old
/// per-event value tiers (`VALUE_SIZE`/`KVValueSize`) and the padding widths
/// that went with them, since nothing is padded to a fixed width any more.
pub const MAX_VALUE_SIZE: usize = 8 << 20;

/// Payload size of the main-thread <-> Worker IPC rings
/// (`shmring_ipc::CAPACITY`), matching `pkg/ipc`'s own `capacity`. It lives
/// here, beside the wire code whose message sizes it has to accommodate,
/// rather than in that module, for one reason: that module is
/// `cfg(target_arch = "wasm32")` and so cannot be unit-tested on the host,
/// and this constant is exactly the kind that goes stale silently. See
/// `tests::ipc_ring_fits_a_realistic_message`.
pub const IPC_RING_CAPACITY: u64 = 32 * 1024;

pub const SIGNATURE_SIZE: usize = 64;
pub const PUBLIC_KEY_SIZE: usize = 32;
pub const PRIVATE_KEY_SIZE: usize = 32; // ed25519-dalek's SigningKey seed length

/// A capnp `Data` (or `List`) field, where the distinction this alias exists
/// to preserve is `None` (null pointer, what Go's `SetData(nil)` and an
/// unset field both produce) versus `Some(vec![])` (present, zero length,
/// what Go's `SetData([]byte{})` produces). Canonical form encodes those
/// differently and the signature covers the difference -- see this module's
/// doc comment. Collapsing them is not a cosmetic simplification: it makes
/// this client reject genuine messages, e.g. a `set` of a key to an empty
/// value.
pub type DataField = Option<Vec<u8>>;

/// One op of a [`Body::Txn`], mirroring `api/shmevent.capnp`'s `TxnOp`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxnOp {
    pub op: u8,
    pub key: DataField,
    pub value: DataField,
}

/// [`TxnOp::op`]'s valid values, mirroring `pkg/shmevent`'s `TxnOp*`
/// constants. Set/Delete are plain writes; Compare/CompareAbsent are
/// preconditions -- see that file's doc comments for why an absent key never
/// satisfies Compare.
pub const TXN_OP_SET: u8 = 1;
pub const TXN_OP_DELETE: u8 = 2;
pub const TXN_OP_COMPARE: u8 = 3;
pub const TXN_OP_COMPARE_ABSENT: u8 = 4;

/// Every variant of `api/shmevent.capnp`'s `Event` union, one per capnp
/// group, with that group's own fields. Most variants serve both directions
/// of one request/response exchange, so a caller fills whichever fields its
/// direction needs and leaves the rest `None`/empty -- e.g. a `GetKey`
/// request sets `source_id`, and the response fills `key`.
///
/// Adding a variant here is not a local change: see `api/shmevent.capnp`'s
/// doc comment on what a new wire variant commits every node in the mesh to,
/// and why a new *task* should be a Command instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    SetKey {
        value: DataField,
    },
    SetField {
        source_id: u16,
        value: DataField,
    },
    GetKey {
        source_id: u16,
        key: DataField,
    },
    GetFieldByRegistry {
        source_id: u16,
        value: DataField,
    },
    GetFieldByKey {
        key: DataField,
        value: DataField,
    },
    GetPublicKey {
        pub_key: DataField,
    },
    GetPrivateKey {
        priv_key: DataField,
    },
    BootstrapOrJoinCluster {
        leader_addr: String,
    },
    AddLearner {
        claimed_peer_id: u16,
        addr: String,
    },
    Set {
        key: DataField,
        value: DataField,
    },
    Execute {
        source_id: u16,
        destination_id: u16,
        sender_peer_id: String,
        value: DataField,
    },
    PollExecute {
        sender_peer_id: String,
        value: DataField,
    },
    ListRange {
        start: DataField,
        end: DataField,
        key: DataField,
        value: DataField,
    },
    LogAppend {
        key: DataField,
        value: DataField,
    },
    Leave,
    ExecInviteRedeem {
        source_addr: String,
        token: DataField,
        instance_id: String,
        redeemer_peer_id: String,
    },
    JoinRequestCreate {
        token: DataField,
    },
    JoinRequestCancel {
        token: DataField,
    },
    Recruit {
        ticket: String,
        suffrage: u8,
    },
    GetOwnAddr {
        addr: String,
    },
    ChannelOpen {
        peer_id: String,
        sender_peer_id: String,
    },
    ChannelSend {
        channel_id: String,
        purpose: u8,
        chunk: DataField,
    },
    ChannelPoll {
        channel_id: String,
        status: u8,
        purpose: u8,
        chunk: DataField,
    },
    ChannelListen {
        channel_id: String,
        remote_peer_id: String,
    },
    ChannelClose {
        channel_id: String,
    },
    ChannelCloseWrite {
        channel_id: String,
    },
    ChannelDataReady {
        channel_id: String,
    },
    Kick {
        peer_id: String,
    },
    Txn {
        ops: Option<Vec<TxnOp>>,
    },
    GetVersion {
        commit: String,
        dirty: bool,
        build_time: String,
        go_version: String,
        libp2p_version: String,
    },
    PublicAccess {
        target_peer: String,
        note: String,
        instance_id: String,
    },
    ExecTicket {
        source_addr: String,
        token: DataField,
    },
    JoinTicket {
        source_addr: String,
        token: DataField,
    },
    JoinRequestTicket {
        source_addr: String,
        token: DataField,
    },
    DialSubmitCommand {
        target_peer: String,
        command_id: String,
        inputs_json: String,
        note: String,
        instance_id: String,
    },
    DialQueryCommandLog {
        target_peer: String,
        instance_id: String,
        since: i64,
        until: i64,
        limit: i32,
        records: Option<Vec<Vec<u8>>>,
    },
    Error {
        message: String,
    },
    GroupPut {
        id: String,
        name: String,
        public: bool,
    },
    GroupDelete {
        id: String,
    },
    /// `spec` is deliberately three-state, matching the capnp field's own
    /// pointer presence and `pkg/shmevent.NewCommandPut`/`NewCommandPutWithSpec`:
    /// `None` preserves whatever spec the stored record already has,
    /// `Some("")` clears it, `Some(json)` sets it.
    CommandPut {
        id: String,
        name: String,
        peer_id: String,
        spec: Option<String>,
    },
    CommandDelete {
        id: String,
    },
    StationPut {
        peer_id: String,
        name: String,
        attrs: String,
    },
    StationDelete {
        peer_id: String,
    },
    GroupCommandPut {
        command_id: String,
        group_id: String,
    },
    GroupCommandDelete {
        command_id: String,
        group_id: String,
    },
    PeerGroupPut {
        peer_id: String,
        group_id: String,
    },
    PeerGroupDelete {
        peer_id: String,
        group_id: String,
    },
    PermitRequest {
        kind: u8,
        peer_id: String,
        metadata: String,
    },
    PermitConfirm {
        kind: u8,
        peer_id: String,
    },
    PermitRevoke {
        kind: u8,
        peer_id: String,
    },
    JoinInviteCreate {
        token: DataField,
        suffrage: u8,
    },
    JoinInviteRevoke {
        token: DataField,
    },
    ExecInviteCreate {
        token: DataField,
        command_id: String,
        inputs_json: String,
        ttl_seconds: u64,
    },
    ExecInviteRevoke {
        token: DataField,
    },
}

/// One wire message: the `id` correlation nonce plus whichever union variant
/// it carries. `crc32`/`signature` are not fields here -- they are computed
/// by [`encode`] and returned by [`decode`], never chosen by a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Msg {
    pub id: u16,
    pub body: Body,
}

impl Msg {
    /// A message with no correlation id -- the common case for a request
    /// whose caller doesn't need to cite it later.
    pub fn new(body: Body) -> Msg {
        Msg { id: 0, body }
    }

    pub fn with_id(id: u16, body: Body) -> Msg {
        Msg { id, body }
    }

    /// The failed-response variant every operation shares, mirroring
    /// `pkg/shmevent.NewError`.
    pub fn error(id: u16, message: impl Into<String>) -> Msg {
        Msg {
            id,
            body: Body::Error {
                message: message.into(),
            },
        }
    }

    /// This message's operation name, the same snake_case string
    /// `pkg/shmevent.EventName` prints.
    pub fn name(&self) -> &'static str {
        event_name(&self.body)
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

impl From<capnp::NotInSchema> for Error {
    fn from(e: capnp::NotInSchema) -> Self {
        Error(format!("unknown event union variant {}", e.0))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error(format!("invalid utf-8 in text field: {e}"))
    }
}

/// `RequiresSignature` in `pkg/shmevent`: every variant except the two
/// bootstrap ones a node accepts unsigned, since fetching one of them is the
/// only way a caller with no key yet obtains one.
pub fn requires_signature(body: &Body) -> bool {
    !matches!(body, Body::GetPublicKey { .. } | Body::GetPrivateKey { .. })
}

/// Writes `body`'s fields into `root`, setting the union discriminant.
///
/// Every `Data`/`List` field is written only when present and every `Text`
/// field only when non-empty -- see this module's doc comment on why "unset"
/// and "set to empty" are different messages, and why `""` is Text's only
/// spelling of unset.
fn write_body(mut root: event::Builder<'_>, body: &Body) -> Result<(), Error> {
    match body {
        Body::SetKey { value } => {
            let mut g = root.reborrow().init_set_key();
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::SetField { source_id, value } => {
            let mut g = root.reborrow().init_set_field();
            g.set_source_id(*source_id);
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::GetKey { source_id, key } => {
            let mut g = root.reborrow().init_get_key();
            g.set_source_id(*source_id);
            if let Some(v) = key {
                g.set_key(v);
            }
        }
        Body::GetFieldByRegistry { source_id, value } => {
            let mut g = root.reborrow().init_get_field_by_registry();
            g.set_source_id(*source_id);
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::GetFieldByKey { key, value } => {
            let mut g = root.reborrow().init_get_field_by_key();
            if let Some(v) = key {
                g.set_key(v);
            }
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::GetPublicKey { pub_key } => {
            let mut g = root.reborrow().init_get_public_key();
            if let Some(v) = pub_key {
                g.set_pub_key(v);
            }
        }
        Body::GetPrivateKey { priv_key } => {
            let mut g = root.reborrow().init_get_private_key();
            if let Some(v) = priv_key {
                g.set_priv_key(v);
            }
        }
        Body::BootstrapOrJoinCluster { leader_addr } => {
            let mut g = root.reborrow().init_bootstrap_or_join_cluster();
            set_text(leader_addr, |v| g.set_leader_addr(v));
        }
        Body::AddLearner {
            claimed_peer_id,
            addr,
        } => {
            let mut g = root.reborrow().init_add_learner();
            g.set_claimed_peer_id(*claimed_peer_id);
            set_text(addr, |v| g.set_addr(v));
        }
        Body::Set { key, value } => {
            let mut g = root.reborrow().init_set();
            if let Some(v) = key {
                g.set_key(v);
            }
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::Execute {
            source_id,
            destination_id,
            sender_peer_id,
            value,
        } => {
            let mut g = root.reborrow().init_execute();
            g.set_source_id(*source_id);
            g.set_destination_id(*destination_id);
            set_text(sender_peer_id, |v| g.set_sender_peer_id(v));
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::PollExecute {
            sender_peer_id,
            value,
        } => {
            let mut g = root.reborrow().init_poll_execute();
            set_text(sender_peer_id, |v| g.set_sender_peer_id(v));
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::ListRange {
            start,
            end,
            key,
            value,
        } => {
            let mut g = root.reborrow().init_list_range();
            if let Some(v) = start {
                g.set_start(v);
            }
            if let Some(v) = end {
                g.set_end(v);
            }
            if let Some(v) = key {
                g.set_key(v);
            }
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::LogAppend { key, value } => {
            let mut g = root.reborrow().init_log_append();
            if let Some(v) = key {
                g.set_key(v);
            }
            if let Some(v) = value {
                g.set_value(v);
            }
        }
        Body::Leave => root.set_leave(()),
        Body::ExecInviteRedeem {
            source_addr,
            token,
            instance_id,
            redeemer_peer_id,
        } => {
            let mut g = root.reborrow().init_exec_invite_redeem();
            set_text(source_addr, |v| g.set_source_addr(v));
            if let Some(v) = token {
                g.set_token(v);
            }
            set_text(instance_id, |v| g.set_instance_id(v));
            set_text(redeemer_peer_id, |v| g.set_redeemer_peer_id(v));
        }
        Body::JoinRequestCreate { token } => {
            let mut g = root.reborrow().init_join_request_create();
            if let Some(v) = token {
                g.set_token(v);
            }
        }
        Body::JoinRequestCancel { token } => {
            let mut g = root.reborrow().init_join_request_cancel();
            if let Some(v) = token {
                g.set_token(v);
            }
        }
        Body::Recruit { ticket, suffrage } => {
            let mut g = root.reborrow().init_recruit();
            set_text(ticket, |v| g.set_ticket(v));
            g.set_suffrage(*suffrage);
        }
        Body::GetOwnAddr { addr } => {
            let mut g = root.reborrow().init_get_own_addr();
            set_text(addr, |v| g.set_addr(v));
        }
        Body::ChannelOpen {
            peer_id,
            sender_peer_id,
        } => {
            let mut g = root.reborrow().init_channel_open();
            set_text(peer_id, |v| g.set_peer_id(v));
            set_text(sender_peer_id, |v| g.set_sender_peer_id(v));
        }
        Body::ChannelSend {
            channel_id,
            purpose,
            chunk,
        } => {
            let mut g = root.reborrow().init_channel_send();
            set_text(channel_id, |v| g.set_channel_id(v));
            g.set_purpose(*purpose);
            if let Some(v) = chunk {
                g.set_chunk(v);
            }
        }
        Body::ChannelPoll {
            channel_id,
            status,
            purpose,
            chunk,
        } => {
            let mut g = root.reborrow().init_channel_poll();
            set_text(channel_id, |v| g.set_channel_id(v));
            g.set_status(*status);
            g.set_purpose(*purpose);
            if let Some(v) = chunk {
                g.set_chunk(v);
            }
        }
        Body::ChannelListen {
            channel_id,
            remote_peer_id,
        } => {
            let mut g = root.reborrow().init_channel_listen();
            set_text(channel_id, |v| g.set_channel_id(v));
            set_text(remote_peer_id, |v| g.set_remote_peer_id(v));
        }
        Body::ChannelClose { channel_id } => {
            let mut g = root.reborrow().init_channel_close();
            set_text(channel_id, |v| g.set_channel_id(v));
        }
        Body::ChannelCloseWrite { channel_id } => {
            let mut g = root.reborrow().init_channel_close_write();
            set_text(channel_id, |v| g.set_channel_id(v));
        }
        Body::ChannelDataReady { channel_id } => {
            let mut g = root.reborrow().init_channel_data_ready();
            set_text(channel_id, |v| g.set_channel_id(v));
        }
        Body::Kick { peer_id } => {
            let mut g = root.reborrow().init_kick();
            set_text(peer_id, |v| g.set_peer_id(v));
        }
        Body::Txn { ops } => {
            let g = root.reborrow().init_txn();
            if let Some(ops) = ops {
                let mut list = g.init_ops(u32::try_from(ops.len()).map_err(|_| {
                    Error(format!("txn: too many ops: {}", ops.len()))
                })?);
                for (i, op) in ops.iter().enumerate() {
                    let mut o = list.reborrow().get(i as u32);
                    o.set_op(op.op);
                    if let Some(v) = &op.key {
                        o.set_key(v);
                    }
                    if let Some(v) = &op.value {
                        o.set_value(v);
                    }
                }
            }
        }
        Body::GetVersion {
            commit,
            dirty,
            build_time,
            go_version,
            libp2p_version,
        } => {
            let mut g = root.reborrow().init_get_version();
            set_text(commit, |v| g.set_commit(v));
            g.set_dirty(*dirty);
            set_text(build_time, |v| g.set_build_time(v));
            set_text(go_version, |v| g.set_go_version(v));
            set_text(libp2p_version, |v| g.set_libp2p_version(v));
        }
        Body::PublicAccess {
            target_peer,
            note,
            instance_id,
        } => {
            let mut g = root.reborrow().init_public_access();
            set_text(target_peer, |v| g.set_target_peer(v));
            set_text(note, |v| g.set_note(v));
            set_text(instance_id, |v| g.set_instance_id(v));
        }
        Body::ExecTicket { source_addr, token } => {
            let mut g = root.reborrow().init_exec_ticket();
            set_text(source_addr, |v| g.set_source_addr(v));
            if let Some(v) = token {
                g.set_token(v);
            }
        }
        Body::JoinTicket { source_addr, token } => {
            let mut g = root.reborrow().init_join_ticket();
            set_text(source_addr, |v| g.set_source_addr(v));
            if let Some(v) = token {
                g.set_token(v);
            }
        }
        Body::JoinRequestTicket { source_addr, token } => {
            let mut g = root.reborrow().init_join_request_ticket();
            set_text(source_addr, |v| g.set_source_addr(v));
            if let Some(v) = token {
                g.set_token(v);
            }
        }
        Body::DialSubmitCommand {
            target_peer,
            command_id,
            inputs_json,
            note,
            instance_id,
        } => {
            let mut g = root.reborrow().init_dial_submit_command();
            set_text(target_peer, |v| g.set_target_peer(v));
            set_text(command_id, |v| g.set_command_id(v));
            set_text(inputs_json, |v| g.set_inputs_json(v));
            set_text(note, |v| g.set_note(v));
            set_text(instance_id, |v| g.set_instance_id(v));
        }
        Body::DialQueryCommandLog {
            target_peer,
            instance_id,
            since,
            until,
            limit,
            records,
        } => {
            let mut g = root.reborrow().init_dial_query_command_log();
            set_text(target_peer, |v| g.set_target_peer(v));
            set_text(instance_id, |v| g.set_instance_id(v));
            g.set_since(*since);
            g.set_until(*until);
            g.set_limit(*limit);
            if let Some(records) = records {
                let mut list = g.init_records(u32::try_from(records.len()).map_err(|_| {
                    Error(format!("dial_query_command_log: too many records: {}", records.len()))
                })?);
                for (i, rec) in records.iter().enumerate() {
                    list.set(i as u32, rec);
                }
            }
        }
        Body::Error { message } => {
            let mut g = root.reborrow().init_error();
            set_text(message, |v| g.set_message(v));
        }
        Body::GroupPut { id, name, public } => {
            let mut g = root.reborrow().init_group_put();
            set_text(id, |v| g.set_id(v));
            set_text(name, |v| g.set_name(v));
            g.set_public(*public);
        }
        Body::GroupDelete { id } => {
            let mut g = root.reborrow().init_group_delete();
            set_text(id, |v| g.set_id(v));
        }
        Body::CommandPut {
            id,
            name,
            peer_id,
            spec,
        } => {
            let mut g = root.reborrow().init_command_put();
            set_text(id, |v| g.set_id(v));
            set_text(name, |v| g.set_name(v));
            set_text(peer_id, |v| g.set_peer_id(v));
            // Three-state, and the one Text field here where "present but
            // empty" is meaningful: it clears a stored spec, where absent
            // preserves it. capnp spells present-empty as a zero-length Text
            // pointer, which init_spec(0) writes and set_spec("") would not.
            match spec.as_deref() {
                None => {}
                Some("") => {
                    g.reborrow().init_spec(0);
                }
                Some(s) => g.set_spec(s),
            }
        }
        Body::CommandDelete { id } => {
            let mut g = root.reborrow().init_command_delete();
            set_text(id, |v| g.set_id(v));
        }
        Body::StationPut {
            peer_id,
            name,
            attrs,
        } => {
            let mut g = root.reborrow().init_station_put();
            set_text(peer_id, |v| g.set_peer_id(v));
            set_text(name, |v| g.set_name(v));
            set_text(attrs, |v| g.set_attrs(v));
        }
        Body::StationDelete { peer_id } => {
            let mut g = root.reborrow().init_station_delete();
            set_text(peer_id, |v| g.set_peer_id(v));
        }
        Body::GroupCommandPut {
            command_id,
            group_id,
        } => {
            let mut g = root.reborrow().init_group_command_put();
            set_text(command_id, |v| g.set_command_id(v));
            set_text(group_id, |v| g.set_group_id(v));
        }
        Body::GroupCommandDelete {
            command_id,
            group_id,
        } => {
            let mut g = root.reborrow().init_group_command_delete();
            set_text(command_id, |v| g.set_command_id(v));
            set_text(group_id, |v| g.set_group_id(v));
        }
        Body::PeerGroupPut { peer_id, group_id } => {
            let mut g = root.reborrow().init_peer_group_put();
            set_text(peer_id, |v| g.set_peer_id(v));
            set_text(group_id, |v| g.set_group_id(v));
        }
        Body::PeerGroupDelete { peer_id, group_id } => {
            let mut g = root.reborrow().init_peer_group_delete();
            set_text(peer_id, |v| g.set_peer_id(v));
            set_text(group_id, |v| g.set_group_id(v));
        }
        Body::PermitRequest {
            kind,
            peer_id,
            metadata,
        } => {
            let mut g = root.reborrow().init_permit_request();
            g.set_kind(*kind);
            set_text(peer_id, |v| g.set_peer_id(v));
            set_text(metadata, |v| g.set_metadata(v));
        }
        Body::PermitConfirm { kind, peer_id } => {
            let mut g = root.reborrow().init_permit_confirm();
            g.set_kind(*kind);
            set_text(peer_id, |v| g.set_peer_id(v));
        }
        Body::PermitRevoke { kind, peer_id } => {
            let mut g = root.reborrow().init_permit_revoke();
            g.set_kind(*kind);
            set_text(peer_id, |v| g.set_peer_id(v));
        }
        Body::JoinInviteCreate { token, suffrage } => {
            let mut g = root.reborrow().init_join_invite_create();
            if let Some(v) = token {
                g.set_token(v);
            }
            g.set_suffrage(*suffrage);
        }
        Body::JoinInviteRevoke { token } => {
            let mut g = root.reborrow().init_join_invite_revoke();
            if let Some(v) = token {
                g.set_token(v);
            }
        }
        Body::ExecInviteCreate {
            token,
            command_id,
            inputs_json,
            ttl_seconds,
        } => {
            let mut g = root.reborrow().init_exec_invite_create();
            if let Some(v) = token {
                g.set_token(v);
            }
            set_text(command_id, |v| g.set_command_id(v));
            set_text(inputs_json, |v| g.set_inputs_json(v));
            g.set_ttl_seconds(*ttl_seconds);
        }
        Body::ExecInviteRevoke { token } => {
            let mut g = root.reborrow().init_exec_invite_revoke();
            if let Some(v) = token {
                g.set_token(v);
            }
        }
    }
    Ok(())
}

/// Applies `set` only for a non-empty string. Go's `Struct.SetText` maps
/// `""` to a null pointer, so writing an empty Text here would produce a
/// message no Go peer can ever produce -- and, because the signature covers
/// the difference, one whose signature a Go peer would compute differently.
fn set_text(value: &str, mut set: impl FnMut(&str)) {
    if !value.is_empty() {
        set(value);
    }
}

/// Reads one `Data` field, preserving the null/empty distinction.
fn read_data(has: bool, r: capnp::Result<capnp::data::Reader<'_>>) -> Result<DataField, Error> {
    if !has {
        return Ok(None);
    }
    Ok(Some(r?.to_vec()))
}

/// Reads one `Text` field. A null pointer reads back as `""`, which is
/// exactly how this module spells "unset" for Text -- see [`set_text`].
fn read_text(r: capnp::Result<capnp::text::Reader<'_>>) -> Result<String, Error> {
    Ok(r?.to_string()?)
}

fn read_body(root: event::Reader<'_>) -> Result<Body, Error> {
    use event::Which;
    Ok(match root.which()? {
        Which::SetKey(g) => Body::SetKey {
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::SetField(g) => Body::SetField {
            source_id: g.get_source_id(),
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::GetKey(g) => Body::GetKey {
            source_id: g.get_source_id(),
            key: read_data(g.has_key(), g.get_key())?,
        },
        Which::GetFieldByRegistry(g) => Body::GetFieldByRegistry {
            source_id: g.get_source_id(),
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::GetFieldByKey(g) => Body::GetFieldByKey {
            key: read_data(g.has_key(), g.get_key())?,
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::GetPublicKey(g) => Body::GetPublicKey {
            pub_key: read_data(g.has_pub_key(), g.get_pub_key())?,
        },
        Which::GetPrivateKey(g) => Body::GetPrivateKey {
            priv_key: read_data(g.has_priv_key(), g.get_priv_key())?,
        },
        Which::BootstrapOrJoinCluster(g) => Body::BootstrapOrJoinCluster {
            leader_addr: read_text(g.get_leader_addr())?,
        },
        Which::AddLearner(g) => Body::AddLearner {
            claimed_peer_id: g.get_claimed_peer_id(),
            addr: read_text(g.get_addr())?,
        },
        Which::Set(g) => Body::Set {
            key: read_data(g.has_key(), g.get_key())?,
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::Execute(g) => Body::Execute {
            source_id: g.get_source_id(),
            destination_id: g.get_destination_id(),
            sender_peer_id: read_text(g.get_sender_peer_id())?,
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::PollExecute(g) => Body::PollExecute {
            sender_peer_id: read_text(g.get_sender_peer_id())?,
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::ListRange(g) => Body::ListRange {
            start: read_data(g.has_start(), g.get_start())?,
            end: read_data(g.has_end(), g.get_end())?,
            key: read_data(g.has_key(), g.get_key())?,
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::LogAppend(g) => Body::LogAppend {
            key: read_data(g.has_key(), g.get_key())?,
            value: read_data(g.has_value(), g.get_value())?,
        },
        Which::Leave(()) => Body::Leave,
        Which::ExecInviteRedeem(g) => Body::ExecInviteRedeem {
            source_addr: read_text(g.get_source_addr())?,
            token: read_data(g.has_token(), g.get_token())?,
            instance_id: read_text(g.get_instance_id())?,
            redeemer_peer_id: read_text(g.get_redeemer_peer_id())?,
        },
        Which::JoinRequestCreate(g) => Body::JoinRequestCreate {
            token: read_data(g.has_token(), g.get_token())?,
        },
        Which::JoinRequestCancel(g) => Body::JoinRequestCancel {
            token: read_data(g.has_token(), g.get_token())?,
        },
        Which::Recruit(g) => Body::Recruit {
            ticket: read_text(g.get_ticket())?,
            suffrage: g.get_suffrage(),
        },
        Which::GetOwnAddr(g) => Body::GetOwnAddr {
            addr: read_text(g.get_addr())?,
        },
        Which::ChannelOpen(g) => Body::ChannelOpen {
            peer_id: read_text(g.get_peer_id())?,
            sender_peer_id: read_text(g.get_sender_peer_id())?,
        },
        Which::ChannelSend(g) => Body::ChannelSend {
            channel_id: read_text(g.get_channel_id())?,
            purpose: g.get_purpose(),
            chunk: read_data(g.has_chunk(), g.get_chunk())?,
        },
        Which::ChannelPoll(g) => Body::ChannelPoll {
            channel_id: read_text(g.get_channel_id())?,
            status: g.get_status(),
            purpose: g.get_purpose(),
            chunk: read_data(g.has_chunk(), g.get_chunk())?,
        },
        Which::ChannelListen(g) => Body::ChannelListen {
            channel_id: read_text(g.get_channel_id())?,
            remote_peer_id: read_text(g.get_remote_peer_id())?,
        },
        Which::ChannelClose(g) => Body::ChannelClose {
            channel_id: read_text(g.get_channel_id())?,
        },
        Which::ChannelCloseWrite(g) => Body::ChannelCloseWrite {
            channel_id: read_text(g.get_channel_id())?,
        },
        Which::ChannelDataReady(g) => Body::ChannelDataReady {
            channel_id: read_text(g.get_channel_id())?,
        },
        Which::Kick(g) => Body::Kick {
            peer_id: read_text(g.get_peer_id())?,
        },
        Which::Txn(g) => Body::Txn {
            ops: if g.has_ops() {
                let list = g.get_ops()?;
                let mut ops = Vec::with_capacity(list.len() as usize);
                for op in list.iter() {
                    ops.push(TxnOp {
                        op: op.get_op(),
                        key: read_data(op.has_key(), op.get_key())?,
                        value: read_data(op.has_value(), op.get_value())?,
                    });
                }
                Some(ops)
            } else {
                None
            },
        },
        Which::GetVersion(g) => Body::GetVersion {
            commit: read_text(g.get_commit())?,
            dirty: g.get_dirty(),
            build_time: read_text(g.get_build_time())?,
            go_version: read_text(g.get_go_version())?,
            libp2p_version: read_text(g.get_libp2p_version())?,
        },
        Which::PublicAccess(g) => Body::PublicAccess {
            target_peer: read_text(g.get_target_peer())?,
            note: read_text(g.get_note())?,
            instance_id: read_text(g.get_instance_id())?,
        },
        Which::ExecTicket(g) => Body::ExecTicket {
            source_addr: read_text(g.get_source_addr())?,
            token: read_data(g.has_token(), g.get_token())?,
        },
        Which::JoinTicket(g) => Body::JoinTicket {
            source_addr: read_text(g.get_source_addr())?,
            token: read_data(g.has_token(), g.get_token())?,
        },
        Which::JoinRequestTicket(g) => Body::JoinRequestTicket {
            source_addr: read_text(g.get_source_addr())?,
            token: read_data(g.has_token(), g.get_token())?,
        },
        Which::DialSubmitCommand(g) => Body::DialSubmitCommand {
            target_peer: read_text(g.get_target_peer())?,
            command_id: read_text(g.get_command_id())?,
            inputs_json: read_text(g.get_inputs_json())?,
            note: read_text(g.get_note())?,
            instance_id: read_text(g.get_instance_id())?,
        },
        Which::DialQueryCommandLog(g) => Body::DialQueryCommandLog {
            target_peer: read_text(g.get_target_peer())?,
            instance_id: read_text(g.get_instance_id())?,
            since: g.get_since(),
            until: g.get_until(),
            limit: g.get_limit(),
            records: if g.has_records() {
                let list = g.get_records()?;
                let mut out = Vec::with_capacity(list.len() as usize);
                for i in 0..list.len() {
                    out.push(list.get(i)?.to_vec());
                }
                Some(out)
            } else {
                None
            },
        },
        Which::Error(g) => Body::Error {
            message: read_text(g.get_message())?,
        },
        Which::GroupPut(g) => Body::GroupPut {
            id: read_text(g.get_id())?,
            name: read_text(g.get_name())?,
            public: g.get_public(),
        },
        Which::GroupDelete(g) => Body::GroupDelete {
            id: read_text(g.get_id())?,
        },
        Which::CommandPut(g) => Body::CommandPut {
            id: read_text(g.get_id())?,
            name: read_text(g.get_name())?,
            peer_id: read_text(g.get_peer_id())?,
            // Presence, not emptiness -- see Body::CommandPut's doc comment.
            spec: if g.has_spec() {
                Some(read_text(g.get_spec())?)
            } else {
                None
            },
        },
        Which::CommandDelete(g) => Body::CommandDelete {
            id: read_text(g.get_id())?,
        },
        Which::StationPut(g) => Body::StationPut {
            peer_id: read_text(g.get_peer_id())?,
            name: read_text(g.get_name())?,
            attrs: read_text(g.get_attrs())?,
        },
        Which::StationDelete(g) => Body::StationDelete {
            peer_id: read_text(g.get_peer_id())?,
        },
        Which::GroupCommandPut(g) => Body::GroupCommandPut {
            command_id: read_text(g.get_command_id())?,
            group_id: read_text(g.get_group_id())?,
        },
        Which::GroupCommandDelete(g) => Body::GroupCommandDelete {
            command_id: read_text(g.get_command_id())?,
            group_id: read_text(g.get_group_id())?,
        },
        Which::PeerGroupPut(g) => Body::PeerGroupPut {
            peer_id: read_text(g.get_peer_id())?,
            group_id: read_text(g.get_group_id())?,
        },
        Which::PeerGroupDelete(g) => Body::PeerGroupDelete {
            peer_id: read_text(g.get_peer_id())?,
            group_id: read_text(g.get_group_id())?,
        },
        Which::PermitRequest(g) => Body::PermitRequest {
            kind: g.get_kind(),
            peer_id: read_text(g.get_peer_id())?,
            metadata: read_text(g.get_metadata())?,
        },
        Which::PermitConfirm(g) => Body::PermitConfirm {
            kind: g.get_kind(),
            peer_id: read_text(g.get_peer_id())?,
        },
        Which::PermitRevoke(g) => Body::PermitRevoke {
            kind: g.get_kind(),
            peer_id: read_text(g.get_peer_id())?,
        },
        Which::JoinInviteCreate(g) => Body::JoinInviteCreate {
            token: read_data(g.has_token(), g.get_token())?,
            suffrage: g.get_suffrage(),
        },
        Which::JoinInviteRevoke(g) => Body::JoinInviteRevoke {
            token: read_data(g.has_token(), g.get_token())?,
        },
        Which::ExecInviteCreate(g) => Body::ExecInviteCreate {
            token: read_data(g.has_token(), g.get_token())?,
            command_id: read_text(g.get_command_id())?,
            inputs_json: read_text(g.get_inputs_json())?,
            ttl_seconds: g.get_ttl_seconds(),
        },
        Which::ExecInviteRevoke(g) => Body::ExecInviteRevoke {
            token: read_data(g.has_token(), g.get_token())?,
        },
    })
}

/// Builds `m` into a capnp message with `crc32` set to `crc` and `signature`
/// left unset, and returns its canonical encoding -- the one primitive
/// [`crc32_of`] and [`signed_payload`] share, mirroring
/// `pkg/shmevent`'s `marshalWithCrcAndEmptySig`.
///
/// "Signature left unset" rather than "cleared": this builds a fresh message
/// instead of mutating one, so the field is simply never written, which is
/// the same null pointer Go's `SetSignature(nil)` produces.
fn canonical_with_crc(m: &Msg, crc: u32) -> Result<Vec<u8>, Error> {
    let mut message = MessageBuilder::new_default();
    {
        let mut root = message.init_root::<event::Builder>();
        root.set_id(m.id);
        root.set_crc32(crc);
        write_body(root.reborrow(), &m.body)?;
    }
    let words = message.into_reader().canonicalize()?;
    Ok(capnp::Word::words_to_bytes(&words).to_vec())
}

/// CRC-32/IEEE over the canonical encoding with `crc32` zeroed and
/// `signature` unset -- `pkg/shmevent.crc32Of`.
pub fn crc32_of(m: &Msg) -> Result<u32, Error> {
    Ok(crc32fast::hash(&canonical_with_crc(m, 0)?))
}

/// What [`sign`]/[`verify`] operate on: the canonical encoding with `crc32`
/// set to `crc` and `signature` unset -- `pkg/shmevent.signedPayload`.
fn signed_payload(m: &Msg, crc: u32) -> Result<Vec<u8>, Error> {
    canonical_with_crc(m, crc)
}

/// Signs `m` (whose crc must already be `crc`) with `priv_key`, returning the
/// 64-byte signature for `Event.signature`. `priv_key` may be `None` only for
/// the two bootstrap variants a node accepts unsigned, in which case this
/// returns a zero-filled signature rather than an error, so [`encode`]'s call
/// site needs no special case. Matches `pkg/shmevent.Sign`.
pub fn sign(priv_key: Option<&SigningKey>, m: &Msg, crc: u32) -> Result<Vec<u8>, Error> {
    match priv_key {
        None => {
            if requires_signature(&m.body) {
                Err(Error(format!(
                    "signing key required for event {}",
                    m.name()
                )))
            } else {
                Ok(vec![0u8; SIGNATURE_SIZE])
            }
        }
        Some(k) => {
            let sig: Signature = k.sign(&signed_payload(m, crc)?);
            Ok(sig.to_bytes().to_vec())
        }
    }
}

/// Checks `sig` against `m`/`crc` and `pub_key`. Matches
/// `pkg/shmevent.Verify`.
pub fn verify(pub_key: &VerifyingKey, m: &Msg, crc: u32, sig: &[u8]) -> Result<(), Error> {
    let sig_bytes: [u8; SIGNATURE_SIZE] = sig.try_into().map_err(|_| {
        Error(format!(
            "signature must be {SIGNATURE_SIZE} bytes, got {}",
            sig.len()
        ))
    })?;
    let signature = Signature::from_bytes(&sig_bytes);
    pub_key
        .verify(&signed_payload(m, crc)?, &signature)
        .map_err(|_| {
            Error(format!(
                "signature verification failed for event {} (id {})",
                m.name(),
                m.id
            ))
        })
}

/// Rejects any `Data` field over [`MAX_VALUE_SIZE`], mirroring the
/// `checkValueSize` call every `pkg/shmevent.NewXxx` constructor makes.
fn check_sizes(body: &Body) -> Result<(), Error> {
    let check = |name: &str, v: &DataField| -> Result<(), Error> {
        if let Some(v) = v {
            if v.len() > MAX_VALUE_SIZE {
                return Err(Error(format!(
                    "{name} too long: {} bytes (max {MAX_VALUE_SIZE})",
                    v.len()
                )));
            }
        }
        Ok(())
    };
    match body {
        Body::SetKey { value }
        | Body::SetField { value, .. }
        | Body::GetFieldByRegistry { value, .. }
        | Body::PollExecute { value, .. }
        | Body::Execute { value, .. } => check("value", value)?,
        Body::GetKey { key, .. } => check("key", key)?,
        Body::GetPublicKey { pub_key } => check("pub_key", pub_key)?,
        Body::GetPrivateKey { priv_key } => check("priv_key", priv_key)?,
        Body::GetFieldByKey { key, value }
        | Body::Set { key, value }
        | Body::LogAppend { key, value } => {
            check("key", key)?;
            check("value", value)?;
        }
        Body::ListRange {
            start,
            end,
            key,
            value,
        } => {
            check("start", start)?;
            check("end", end)?;
            check("key", key)?;
            check("value", value)?;
        }
        Body::ChannelSend { chunk, .. } | Body::ChannelPoll { chunk, .. } => {
            check("chunk", chunk)?
        }
        Body::Txn { ops } => {
            for op in ops.iter().flatten() {
                check("txn op key", &op.key)?;
                check("txn op value", &op.value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Serializes `m` to its capnp wire form, computing CRC32 and signing with
/// `priv_key`. Matches `pkg/shmevent.Encode`.
pub fn encode(m: &Msg, priv_key: Option<&SigningKey>) -> Result<Vec<u8>, Error> {
    check_sizes(&m.body)?;

    let crc = crc32_of(m)?;
    let sig = sign(priv_key, m, crc)?;

    let mut message = MessageBuilder::new_default();
    {
        let mut root = message.init_root::<event::Builder>();
        root.set_id(m.id);
        root.set_crc32(crc);
        // Body first, signature last. Both orders encode the same message
        // and canonicalize identically, but capnp lays sub-objects out in
        // allocation order, and Go's Encode signs a message whose body was
        // already built by a NewXxx constructor -- so writing the signature
        // first would leave this side producing byte-different (though
        // equally valid) serializations of identical messages, and
        // tests::go_fixture compares bytes.
        write_body(root.reborrow(), &m.body)?;
        root.set_signature(&sig);
    }
    let mut buf = Vec::new();
    capnp::serialize::write_message(&mut buf, &message)?;
    Ok(buf)
}

/// Parses `buf` as a capnp Event message and checks its CRC32 against the
/// decoded fields. Does not check the signature -- a caller that needs
/// authenticity calls [`verify`] once it knows which public key to check
/// against. Matches `pkg/shmevent.Decode`.
pub fn decode(buf: &[u8]) -> Result<(Msg, u32, Vec<u8>), Error> {
    let message_reader =
        capnp::serialize::read_message(&mut std::io::Cursor::new(buf), ReaderOptions::new())?;
    let root = message_reader.get_root::<event::Reader>()?;

    let m = Msg {
        id: root.get_id(),
        body: read_body(root)?,
    };
    let want_crc = root.get_crc32();
    let got_crc = crc32_of(&m)?;
    if got_crc != want_crc {
        return Err(Error(format!(
            "crc32 mismatch: got {got_crc:#x}, message says {want_crc:#x}"
        )));
    }
    let sig = if root.has_signature() {
        root.get_signature()?.to_vec()
    } else {
        Vec::new()
    };
    Ok((m, want_crc, sig))
}

/// The operation name for `body`, matching `pkg/shmevent.EventName`'s
/// hand-written snake_case table (deliberately not capnpc's own camelCase
/// variant names, since this project's established convention appears in
/// `test/e2e/testdata.json`, `kvctl-cli sendevent` JSON and log output).
pub fn event_name(body: &Body) -> &'static str {
    match body {
        Body::SetKey { .. } => "set_key",
        Body::SetField { .. } => "set_field",
        Body::GetKey { .. } => "get_key",
        Body::GetFieldByRegistry { .. } => "get_field_by_registry",
        Body::GetFieldByKey { .. } => "get_field_by_key",
        Body::GetPublicKey { .. } => "get_public_key",
        Body::GetPrivateKey { .. } => "get_private_key",
        Body::BootstrapOrJoinCluster { .. } => "bootstrap_or_join_cluster",
        Body::AddLearner { .. } => "add_learner",
        Body::Set { .. } => "set",
        Body::Execute { .. } => "execute",
        Body::PollExecute { .. } => "poll_execute",
        Body::ListRange { .. } => "list_range",
        Body::LogAppend { .. } => "log_append",
        Body::Leave => "leave",
        Body::ExecInviteRedeem { .. } => "exec_invite_redeem",
        Body::JoinRequestCreate { .. } => "join_request_create",
        Body::JoinRequestCancel { .. } => "join_request_cancel",
        Body::Recruit { .. } => "recruit",
        Body::GetOwnAddr { .. } => "get_own_addr",
        Body::ChannelOpen { .. } => "channel_open",
        Body::ChannelSend { .. } => "channel_send",
        Body::ChannelPoll { .. } => "channel_poll",
        Body::ChannelListen { .. } => "channel_listen",
        Body::ChannelClose { .. } => "channel_close",
        Body::ChannelCloseWrite { .. } => "channel_close_write",
        Body::ChannelDataReady { .. } => "channel_data_ready",
        Body::Kick { .. } => "kick",
        Body::Txn { .. } => "txn",
        Body::GetVersion { .. } => "get_version",
        Body::PublicAccess { .. } => "public_access",
        Body::ExecTicket { .. } => "exec_ticket",
        Body::JoinTicket { .. } => "join_ticket",
        Body::JoinRequestTicket { .. } => "join_request_ticket",
        Body::DialSubmitCommand { .. } => "dial_submit_command",
        Body::DialQueryCommandLog { .. } => "dial_query_command_log",
        Body::Error { .. } => "error",
        Body::GroupPut { .. } => "group_put",
        Body::GroupDelete { .. } => "group_delete",
        Body::CommandPut { .. } => "command_put",
        Body::CommandDelete { .. } => "command_delete",
        Body::StationPut { .. } => "station_put",
        Body::StationDelete { .. } => "station_delete",
        Body::GroupCommandPut { .. } => "group_command_put",
        Body::GroupCommandDelete { .. } => "group_command_delete",
        Body::PeerGroupPut { .. } => "peer_group_put",
        Body::PeerGroupDelete { .. } => "peer_group_delete",
        Body::PermitRequest { .. } => "permit_request",
        Body::PermitConfirm { .. } => "permit_confirm",
        Body::PermitRevoke { .. } => "permit_revoke",
        Body::JoinInviteCreate { .. } => "join_invite_create",
        Body::JoinInviteRevoke { .. } => "join_invite_revoke",
        Body::ExecInviteCreate { .. } => "exec_invite_create",
        Body::ExecInviteRevoke { .. } => "exec_invite_revoke",
    }
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
    use std::fmt::Write as _;
    let mut out = String::with_capacity(raw.len() * 2);
    for b in raw {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// `EventJson` is [`Msg`]'s JSON shape, matching `pkg/e2edata.Event`
/// exactly: the operation name, the correlation id, and a flat map of
/// whichever of that variant's fields are set -- e.g.
/// `{"event":"get_field_by_key","fields":{"key":"hello"}}`. A pure
/// presentation layer; it changes nothing about the capnp wire structure
/// [`encode`]/[`decode`] (de)serialize.
#[derive(serde::Serialize, serde::Deserialize)]
struct EventJson {
    event: String,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    id: u16,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    fields: BTreeMap<String, String>,
}

fn is_zero_u16(v: &u16) -> bool {
    *v == 0
}

/// A `Data` field as JSON: plain text when it's valid UTF-8 (every KV key
/// and value in practice), a `"0x"`-prefixed hex string otherwise (a raw
/// Ed25519 key, deliberately-corrupt test bytes) -- `pkg/e2edata.valueToJSON`.
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

/// Collects `body`'s set fields into `pkg/e2edata.EventFromMsg`'s flat map:
/// zero integers, `false` bools, empty Text and absent `Data` are all
/// omitted, so a printed event shows only what it actually carries.
fn body_to_fields(body: &Body) -> BTreeMap<String, String> {
    let mut f = BTreeMap::new();
    let data = |f: &mut BTreeMap<String, String>, name: &str, v: &DataField| {
        if let Some(v) = v {
            if !v.is_empty() {
                f.insert(name.to_string(), value_to_json(v));
            }
        }
    };
    let text = |f: &mut BTreeMap<String, String>, name: &str, v: &str| {
        if !v.is_empty() {
            f.insert(name.to_string(), v.to_string());
        }
    };
    let num = |f: &mut BTreeMap<String, String>, name: &str, v: i64| {
        if v != 0 {
            f.insert(name.to_string(), v.to_string());
        }
    };
    match body {
        Body::SetKey { value } => data(&mut f, "value", value),
        Body::SetField { source_id, value } => {
            num(&mut f, "source_id", *source_id as i64);
            data(&mut f, "value", value);
        }
        Body::GetKey { source_id, key } => {
            num(&mut f, "source_id", *source_id as i64);
            data(&mut f, "key", key);
        }
        Body::GetFieldByRegistry { source_id, value } => {
            num(&mut f, "source_id", *source_id as i64);
            data(&mut f, "value", value);
        }
        Body::GetFieldByKey { key, value } => {
            data(&mut f, "key", key);
            data(&mut f, "value", value);
        }
        Body::GetPublicKey { pub_key } => data(&mut f, "pub_key", pub_key),
        Body::GetPrivateKey { priv_key } => data(&mut f, "priv_key", priv_key),
        Body::BootstrapOrJoinCluster { leader_addr } => text(&mut f, "leader_addr", leader_addr),
        Body::AddLearner {
            claimed_peer_id,
            addr,
        } => {
            num(&mut f, "claimed_peer_id", *claimed_peer_id as i64);
            text(&mut f, "addr", addr);
        }
        Body::Set { key, value } => {
            data(&mut f, "key", key);
            data(&mut f, "value", value);
        }
        Body::Execute {
            source_id,
            destination_id,
            sender_peer_id,
            value,
        } => {
            num(&mut f, "source_id", *source_id as i64);
            num(&mut f, "destination_id", *destination_id as i64);
            text(&mut f, "sender_peer_id", sender_peer_id);
            data(&mut f, "value", value);
        }
        Body::PollExecute {
            sender_peer_id,
            value,
        } => {
            text(&mut f, "sender_peer_id", sender_peer_id);
            data(&mut f, "value", value);
        }
        Body::ListRange {
            start,
            end,
            key,
            value,
        } => {
            data(&mut f, "start", start);
            data(&mut f, "end", end);
            data(&mut f, "key", key);
            data(&mut f, "value", value);
        }
        Body::LogAppend { key, value } => {
            data(&mut f, "key", key);
            data(&mut f, "value", value);
        }
        Body::Leave => {}
        Body::ExecInviteRedeem {
            source_addr,
            token,
            instance_id,
            redeemer_peer_id,
        } => {
            text(&mut f, "source_addr", source_addr);
            data(&mut f, "token", token);
            text(&mut f, "instance_id", instance_id);
            text(&mut f, "redeemer_peer_id", redeemer_peer_id);
        }
        Body::JoinRequestCreate { token } | Body::JoinRequestCancel { token } => {
            data(&mut f, "token", token)
        }
        Body::Recruit { ticket, suffrage } => {
            text(&mut f, "ticket", ticket);
            num(&mut f, "suffrage", *suffrage as i64);
        }
        Body::GetOwnAddr { addr } => text(&mut f, "addr", addr),
        Body::ChannelOpen {
            peer_id,
            sender_peer_id,
        } => {
            text(&mut f, "peer_id", peer_id);
            text(&mut f, "sender_peer_id", sender_peer_id);
        }
        Body::ChannelSend {
            channel_id,
            purpose,
            chunk,
        } => {
            text(&mut f, "channel_id", channel_id);
            num(&mut f, "purpose", *purpose as i64);
            data(&mut f, "chunk", chunk);
        }
        Body::ChannelPoll {
            channel_id,
            status,
            purpose,
            chunk,
        } => {
            text(&mut f, "channel_id", channel_id);
            num(&mut f, "status", *status as i64);
            num(&mut f, "purpose", *purpose as i64);
            data(&mut f, "chunk", chunk);
        }
        Body::ChannelListen {
            channel_id,
            remote_peer_id,
        } => {
            text(&mut f, "channel_id", channel_id);
            text(&mut f, "remote_peer_id", remote_peer_id);
        }
        Body::ChannelClose { channel_id }
        | Body::ChannelCloseWrite { channel_id }
        | Body::ChannelDataReady { channel_id } => text(&mut f, "channel_id", channel_id),
        Body::Kick { peer_id } | Body::StationDelete { peer_id } => {
            text(&mut f, "peer_id", peer_id)
        }
        // ops is a list, which this JSON shape deliberately cannot carry --
        // pkg/e2edata's own txn case records the event name and id and
        // nothing else, and refuses to build one from JSON at all (see
        // body_from_fields). A txn is built by a caller that has real
        // TxnOps, not by hand-writing an event.
        Body::Txn { .. } => {}
        Body::GetVersion {
            commit,
            dirty,
            build_time,
            go_version,
            libp2p_version,
        } => {
            text(&mut f, "commit", commit);
            if *dirty {
                f.insert("dirty".to_string(), "true".to_string());
            }
            text(&mut f, "build_time", build_time);
            text(&mut f, "go_version", go_version);
            text(&mut f, "libp2p_version", libp2p_version);
        }
        Body::PublicAccess {
            target_peer,
            note,
            instance_id,
        } => {
            text(&mut f, "target_peer", target_peer);
            text(&mut f, "note", note);
            text(&mut f, "instance_id", instance_id);
        }
        Body::ExecTicket { source_addr, token }
        | Body::JoinTicket { source_addr, token }
        | Body::JoinRequestTicket { source_addr, token } => {
            text(&mut f, "source_addr", source_addr);
            data(&mut f, "token", token);
        }
        Body::DialSubmitCommand {
            target_peer,
            command_id,
            inputs_json,
            note,
            instance_id,
        } => {
            text(&mut f, "target_peer", target_peer);
            text(&mut f, "command_id", command_id);
            text(&mut f, "inputs_json", inputs_json);
            text(&mut f, "note", note);
            text(&mut f, "instance_id", instance_id);
        }
        Body::DialQueryCommandLog {
            target_peer,
            instance_id,
            since,
            until,
            limit,
            records,
        } => {
            text(&mut f, "target_peer", target_peer);
            text(&mut f, "instance_id", instance_id);
            num(&mut f, "since", *since);
            num(&mut f, "until", *until);
            num(&mut f, "limit", *limit as i64);
            // records is a list -- omitted for the same reason txn's ops
            // are, matching pkg/e2edata.
            let _ = records;
        }
        Body::Error { message } => text(&mut f, "message", message),
        Body::GroupPut { id, name, public } => {
            text(&mut f, "id", id);
            text(&mut f, "name", name);
            if *public {
                f.insert("public".to_string(), "true".to_string());
            }
        }
        Body::GroupDelete { id } | Body::CommandDelete { id } => text(&mut f, "id", id),
        Body::CommandPut {
            id,
            name,
            peer_id,
            spec,
        } => {
            text(&mut f, "id", id);
            text(&mut f, "name", name);
            text(&mut f, "peer_id", peer_id);
            if let Some(spec) = spec {
                f.insert("spec".to_string(), spec.clone());
            }
        }
        Body::StationPut {
            peer_id,
            name,
            attrs,
        } => {
            text(&mut f, "peer_id", peer_id);
            text(&mut f, "name", name);
            text(&mut f, "attrs", attrs);
        }
        Body::GroupCommandPut {
            command_id,
            group_id,
        }
        | Body::GroupCommandDelete {
            command_id,
            group_id,
        } => {
            text(&mut f, "command_id", command_id);
            text(&mut f, "group_id", group_id);
        }
        Body::PeerGroupPut { peer_id, group_id } | Body::PeerGroupDelete { peer_id, group_id } => {
            text(&mut f, "peer_id", peer_id);
            text(&mut f, "group_id", group_id);
        }
        Body::PermitRequest {
            kind,
            peer_id,
            metadata,
        } => {
            num(&mut f, "kind", *kind as i64);
            text(&mut f, "peer_id", peer_id);
            text(&mut f, "metadata", metadata);
        }
        Body::PermitConfirm { kind, peer_id } | Body::PermitRevoke { kind, peer_id } => {
            num(&mut f, "kind", *kind as i64);
            text(&mut f, "peer_id", peer_id);
        }
        Body::JoinInviteCreate { token, suffrage } => {
            data(&mut f, "token", token);
            num(&mut f, "suffrage", *suffrage as i64);
        }
        Body::JoinInviteRevoke { token } | Body::ExecInviteRevoke { token } => {
            data(&mut f, "token", token)
        }
        Body::ExecInviteCreate {
            token,
            command_id,
            inputs_json,
            ttl_seconds,
        } => {
            data(&mut f, "token", token);
            text(&mut f, "command_id", command_id);
            text(&mut f, "inputs_json", inputs_json);
            num(&mut f, "ttl_seconds", *ttl_seconds as i64);
        }
    }
    f
}

/// Builds the [`Body`] named by `event` from a flat field map, the inverse of
/// [`body_to_fields`] and the counterpart of `pkg/e2edata.Event.ToMsg`. A
/// field the map doesn't name is left absent/empty/zero, which is what makes
/// the same JSON usable for a request (naming only request fields) and for
/// recording a response.
fn body_from_fields(event: &str, fields: &BTreeMap<String, String>) -> Result<Body, Error> {
    let data = |name: &str| -> Result<DataField, Error> {
        match fields.get(name) {
            None => Ok(None),
            Some(v) => Ok(Some(value_from_json(v)?)),
        }
    };
    let text = |name: &str| -> String { fields.get(name).cloned().unwrap_or_default() };
    let flag = |name: &str| -> bool { matches!(fields.get(name).map(String::as_str), Some("true")) };
    let int = |name: &str| -> Result<i64, Error> {
        match fields.get(name) {
            None => Ok(0),
            Some(v) => v
                .parse::<i64>()
                .map_err(|e| Error(format!("{event}.{name}: {e}"))),
        }
    };
    let u16f = |name: &str| -> Result<u16, Error> {
        u16::try_from(int(name)?).map_err(|_| Error(format!("{event}.{name}: out of range")))
    };
    let u8f = |name: &str| -> Result<u8, Error> {
        u8::try_from(int(name)?).map_err(|_| Error(format!("{event}.{name}: out of range")))
    };

    Ok(match event {
        "set_key" => Body::SetKey { value: data("value")? },
        "set_field" => Body::SetField {
            source_id: u16f("source_id")?,
            value: data("value")?,
        },
        "get_key" => Body::GetKey {
            source_id: u16f("source_id")?,
            key: data("key")?,
        },
        "get_field_by_registry" => Body::GetFieldByRegistry {
            source_id: u16f("source_id")?,
            value: data("value")?,
        },
        "get_field_by_key" => Body::GetFieldByKey {
            key: data("key")?,
            value: data("value")?,
        },
        "get_public_key" => Body::GetPublicKey {
            pub_key: data("pub_key")?,
        },
        "get_private_key" => Body::GetPrivateKey {
            priv_key: data("priv_key")?,
        },
        "bootstrap_or_join_cluster" => Body::BootstrapOrJoinCluster {
            leader_addr: text("leader_addr"),
        },
        "add_learner" => Body::AddLearner {
            claimed_peer_id: u16f("claimed_peer_id")?,
            addr: text("addr"),
        },
        "set" => Body::Set {
            key: data("key")?,
            value: data("value")?,
        },
        "execute" => Body::Execute {
            source_id: u16f("source_id")?,
            destination_id: u16f("destination_id")?,
            sender_peer_id: text("sender_peer_id"),
            value: data("value")?,
        },
        "poll_execute" => Body::PollExecute {
            sender_peer_id: text("sender_peer_id"),
            value: data("value")?,
        },
        "list_range" => Body::ListRange {
            start: data("start")?,
            end: data("end")?,
            key: data("key")?,
            value: data("value")?,
        },
        "log_append" => Body::LogAppend {
            key: data("key")?,
            value: data("value")?,
        },
        "leave" => Body::Leave,
        "exec_invite_redeem" => Body::ExecInviteRedeem {
            source_addr: text("source_addr"),
            token: data("token")?,
            instance_id: text("instance_id"),
            redeemer_peer_id: text("redeemer_peer_id"),
        },
        "join_request_create" => Body::JoinRequestCreate {
            token: data("token")?,
        },
        "join_request_cancel" => Body::JoinRequestCancel {
            token: data("token")?,
        },
        "recruit" => Body::Recruit {
            ticket: text("ticket"),
            suffrage: u8f("suffrage")?,
        },
        "get_own_addr" => Body::GetOwnAddr { addr: text("addr") },
        "channel_open" => Body::ChannelOpen {
            peer_id: text("peer_id"),
            sender_peer_id: text("sender_peer_id"),
        },
        "channel_send" => Body::ChannelSend {
            channel_id: text("channel_id"),
            purpose: u8f("purpose")?,
            chunk: data("chunk")?,
        },
        "channel_poll" => Body::ChannelPoll {
            channel_id: text("channel_id"),
            status: u8f("status")?,
            purpose: u8f("purpose")?,
            chunk: data("chunk")?,
        },
        "channel_listen" => Body::ChannelListen {
            channel_id: text("channel_id"),
            remote_peer_id: text("remote_peer_id"),
        },
        "channel_close" => Body::ChannelClose {
            channel_id: text("channel_id"),
        },
        "channel_close_write" => Body::ChannelCloseWrite {
            channel_id: text("channel_id"),
        },
        "channel_data_ready" => Body::ChannelDataReady {
            channel_id: text("channel_id"),
        },
        "kick" => Body::Kick {
            peer_id: text("peer_id"),
        },
        // Refused rather than half-built, matching pkg/e2edata.Event.ToMsg
        // word for word: a flat string map has no honest spelling for a
        // list, and silently sending an empty txn would be worse than
        // saying so.
        "txn" => {
            return Err(Error(
                "txn: not representable as a generic event (ops is a list)".into(),
            ))
        }
        "get_version" => Body::GetVersion {
            commit: text("commit"),
            dirty: flag("dirty"),
            build_time: text("build_time"),
            go_version: text("go_version"),
            libp2p_version: text("libp2p_version"),
        },
        "public_access" => Body::PublicAccess {
            target_peer: text("target_peer"),
            note: text("note"),
            instance_id: text("instance_id"),
        },
        "exec_ticket" => Body::ExecTicket {
            source_addr: text("source_addr"),
            token: data("token")?,
        },
        "join_ticket" => Body::JoinTicket {
            source_addr: text("source_addr"),
            token: data("token")?,
        },
        "join_request_ticket" => Body::JoinRequestTicket {
            source_addr: text("source_addr"),
            token: data("token")?,
        },
        "dial_submit_command" => Body::DialSubmitCommand {
            target_peer: text("target_peer"),
            command_id: text("command_id"),
            inputs_json: text("inputs_json"),
            note: text("note"),
            instance_id: text("instance_id"),
        },
        "dial_query_command_log" => Body::DialQueryCommandLog {
            target_peer: text("target_peer"),
            instance_id: text("instance_id"),
            since: int("since")?,
            until: int("until")?,
            limit: i32::try_from(int("limit")?)
                .map_err(|_| Error("dial_query_command_log.limit: out of range".into()))?,
            // A list, and so not part of this shape -- see the txn arm.
            records: None,
        },
        "error" => Body::Error {
            message: text("message"),
        },
        "group_put" => Body::GroupPut {
            id: text("id"),
            name: text("name"),
            public: flag("public"),
        },
        "group_delete" => Body::GroupDelete { id: text("id") },
        "command_put" => Body::CommandPut {
            id: text("id"),
            name: text("name"),
            peer_id: text("peer_id"),
            spec: fields.get("spec").cloned(),
        },
        "command_delete" => Body::CommandDelete { id: text("id") },
        "station_put" => Body::StationPut {
            peer_id: text("peer_id"),
            name: text("name"),
            attrs: text("attrs"),
        },
        "station_delete" => Body::StationDelete {
            peer_id: text("peer_id"),
        },
        "group_command_put" => Body::GroupCommandPut {
            command_id: text("command_id"),
            group_id: text("group_id"),
        },
        "group_command_delete" => Body::GroupCommandDelete {
            command_id: text("command_id"),
            group_id: text("group_id"),
        },
        "peer_group_put" => Body::PeerGroupPut {
            peer_id: text("peer_id"),
            group_id: text("group_id"),
        },
        "peer_group_delete" => Body::PeerGroupDelete {
            peer_id: text("peer_id"),
            group_id: text("group_id"),
        },
        "permit_request" => Body::PermitRequest {
            kind: u8f("kind")?,
            peer_id: text("peer_id"),
            metadata: text("metadata"),
        },
        "permit_confirm" => Body::PermitConfirm {
            kind: u8f("kind")?,
            peer_id: text("peer_id"),
        },
        "permit_revoke" => Body::PermitRevoke {
            kind: u8f("kind")?,
            peer_id: text("peer_id"),
        },
        "join_invite_create" => Body::JoinInviteCreate {
            token: data("token")?,
            suffrage: u8f("suffrage")?,
        },
        "join_invite_revoke" => Body::JoinInviteRevoke {
            token: data("token")?,
        },
        "exec_invite_create" => Body::ExecInviteCreate {
            token: data("token")?,
            command_id: text("command_id"),
            inputs_json: text("inputs_json"),
            ttl_seconds: u64::try_from(int("ttl_seconds")?)
                .map_err(|_| Error("exec_invite_create.ttl_seconds: out of range".into()))?,
        },
        "exec_invite_revoke" => Body::ExecInviteRevoke {
            token: data("token")?,
        },
        other => return Err(Error(format!("unknown event name {other:?}"))),
    })
}

/// Serializes `m` to [`EventJson`]'s shape -- the same human-readable form
/// `pkg/e2edata.Event`/`kvctl-cli sendevent` use, e.g.
/// `{"event":"get_field_by_key","fields":{"key":"hello"}}`.
pub fn msg_to_json(m: &Msg) -> Result<String, Error> {
    let json = EventJson {
        event: event_name(&m.body).to_string(),
        id: m.id,
        fields: body_to_fields(&m.body),
    };
    serde_json::to_string(&json).map_err(|e| Error(e.to_string()))
}

/// Inverse of [`msg_to_json`].
pub fn msg_from_json(s: &str) -> Result<Msg, Error> {
    let json: EventJson = serde_json::from_str(s).map_err(|e| Error(e.to_string()))?;
    Ok(Msg {
        id: json.id,
        body: body_from_fields(&json.event, &json.fields)?,
    })
}

#[cfg(test)]
mod tests;
