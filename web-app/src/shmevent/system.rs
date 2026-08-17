//! Rust port of `pkg/shmevent/system.go`'s client-relevant subset: the
//! `SystemKeyPrefix` reserved key namespace and the kind/status bytes a
//! system record's *store key* is built from.
//!
//! What used to live here too -- hand-packed payloads for permit
//! request/confirm/revoke and the generic lifecycle envelope that wrapped
//! them -- is gone, because those are no longer payloads at all: `api/
//! shmevent.capnp`'s union gives `permitRequest`/`permitConfirm`/
//! `permitRevoke` their own variants with named `kind`/`peerId`/`metadata`
//! fields, so a caller builds the message rather than a blob inside one.
//! `pkg/shmevent` dropped its own `EncodePermitRequestPayload` family in the
//! same change.
//!
//! `KindClusterMember`/`KindClusterJoin` and their `Role*`/`Suffrage*`
//! payload helpers are intentionally not ported -- nothing in scope for this
//! client reaches them (those records are written by `pkg/daemon` itself,
//! never by a caller) -- but their kind bytes and names are included for
//! wire-table completeness.

/// Marks a store key as reserved for internal cluster bookkeeping rather
/// than user data. Matches `pkg/shmevent.SystemKeyPrefix`.
pub const SYSTEM_KEY_PREFIX: u8 = 0x00;

/// Kind bytes -- what a system record (see [`system_key`]) is about.
/// Matches `pkg/shmevent`'s `Kind*` constants.
///
/// 0x01 and 0x04 are deliberately absent: they were `KindPermitPeer`/
/// `KindLogPermit` and are now unassigned again, so nothing here should
/// reuse them without checking `pkg/shmevent/system.go` first.
pub const KIND_BOOTSTRAP_NODE: u8 = 0x02;
pub const KIND_CLUSTER_MEMBER: u8 = 0x03;
pub const KIND_CLUSTER_JOIN: u8 = 0x05;
pub const KIND_GROUP: u8 = 0x06;
pub const KIND_COMMAND: u8 = 0x07;
pub const KIND_GROUP_COMMAND: u8 = 0x08;
pub const KIND_PEER_GROUP: u8 = 0x09;
pub const KIND_JOIN_INVITE: u8 = 0x0A;
pub const KIND_EXEC_INVITE: u8 = 0x0B;
pub const KIND_STATION: u8 = 0x0C;

/// Status bytes -- where a system record is in its two-stage
/// request/confirm lifecycle. Matches `pkg/shmevent`'s `Status*`
/// constants.
pub const STATUS_PENDING: u8 = 0x01;
pub const STATUS_CONFIRMED: u8 = 0x02;

/// Human-readable name for `kind`, matching `pkg/shmevent.KindName` --
/// "unknown(N)" for anything not defined above.
pub fn kind_name(kind: u8) -> String {
    match kind {
        KIND_BOOTSTRAP_NODE => "bootstrap".to_string(),
        KIND_CLUSTER_MEMBER => "cluster-member".to_string(),
        KIND_CLUSTER_JOIN => "cluster-join".to_string(),
        KIND_GROUP => "group".to_string(),
        KIND_COMMAND => "command".to_string(),
        KIND_GROUP_COMMAND => "group-command".to_string(),
        KIND_PEER_GROUP => "peer-group".to_string(),
        KIND_JOIN_INVITE => "join-invite".to_string(),
        KIND_EXEC_INVITE => "exec-invite".to_string(),
        KIND_STATION => "station".to_string(),
        _ => format!("unknown({kind})"),
    }
}

/// Inverse of [`kind_name`], matching `pkg/shmevent.KindFromName`.
pub fn kind_from_name(name: &str) -> Option<u8> {
    match name {
        "bootstrap" => Some(KIND_BOOTSTRAP_NODE),
        "cluster-member" => Some(KIND_CLUSTER_MEMBER),
        "cluster-join" => Some(KIND_CLUSTER_JOIN),
        "group" => Some(KIND_GROUP),
        "command" => Some(KIND_COMMAND),
        "group-command" => Some(KIND_GROUP_COMMAND),
        "peer-group" => Some(KIND_PEER_GROUP),
        "join-invite" => Some(KIND_JOIN_INVITE),
        "exec-invite" => Some(KIND_EXEC_INVITE),
        "station" => Some(KIND_STATION),
        _ => None,
    }
}

/// Builds the store key for a system record: `SYSTEM_KEY_PREFIX`, `kind`,
/// `status`, then `peer_id` verbatim (always last, needs no length
/// prefix). Matches `pkg/shmevent.SystemKey`.
pub fn system_key(kind: u8, status: u8, peer_id: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(3 + peer_id.len());
    key.push(SYSTEM_KEY_PREFIX);
    key.push(kind);
    key.push(status);
    key.extend_from_slice(peer_id);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_name_round_trip() {
        for k in [
            KIND_BOOTSTRAP_NODE,
            KIND_CLUSTER_MEMBER,
            KIND_CLUSTER_JOIN,
            KIND_GROUP,
            KIND_COMMAND,
            KIND_GROUP_COMMAND,
            KIND_PEER_GROUP,
            KIND_JOIN_INVITE,
            KIND_EXEC_INVITE,
            KIND_STATION,
        ] {
            let name = kind_name(k);
            assert_eq!(kind_from_name(&name), Some(k), "round trip for {name:?}");
        }
        assert_eq!(kind_from_name("not_a_real_kind"), None);
    }

    /// The two retired bytes must not silently come back as a name: a
    /// record written under one of them would be unreadable by any current
    /// node, so "unknown" is the honest answer.
    #[test]
    fn retired_kinds_have_no_name() {
        assert_eq!(kind_name(0x01), "unknown(1)");
        assert_eq!(kind_name(0x04), "unknown(4)");
        assert_eq!(kind_from_name("peer"), None);
        assert_eq!(kind_from_name("log-permit"), None);
    }

    #[test]
    fn system_key_layout() {
        let key = system_key(KIND_GROUP, 0x00, b"my-group");
        assert_eq!(key[0], SYSTEM_KEY_PREFIX);
        assert_eq!(key[1], KIND_GROUP);
        assert_eq!(key[2], 0x00);
        assert_eq!(&key[3..], b"my-group");
    }
}
