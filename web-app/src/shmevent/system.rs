//! Rust port of `pkg/shmevent/system.go`'s client-relevant subset: the
//! `SystemKeyPrefix` reserved key namespace and permit payload framing.
//! `KindClusterMember`/`KindClusterJoin` and their `Role*`/`Suffrage*`
//! payload helpers are intentionally not ported here -- nothing in scope
//! for this client reaches them (cluster-member/join records are written
//! by `pkg/daemon` itself, never by a caller) -- but their `Kind` bytes
//! and names are still included below for wire-table completeness, same
//! reasoning as `EVENT_LEAVE` in the parent module.

use super::Error;

/// Marks a store key as reserved for internal cluster bookkeeping rather
/// than user data. Matches `pkg/shmevent.SystemKeyPrefix`.
pub const SYSTEM_KEY_PREFIX: u8 = 0x00;

/// Kind bytes -- what a system record (see [`system_key`]) is about.
/// Matches `pkg/shmevent`'s `Kind*` constants.
pub const KIND_PERMIT_PEER: u8 = 0x01;
pub const KIND_BOOTSTRAP_NODE: u8 = 0x02;
pub const KIND_CLUSTER_MEMBER: u8 = 0x03;
pub const KIND_LOG_PERMIT: u8 = 0x04;
pub const KIND_CLUSTER_JOIN: u8 = 0x05;
pub const KIND_GROUP: u8 = 0x06;
pub const KIND_COMMAND: u8 = 0x07;
pub const KIND_GROUP_COMMAND: u8 = 0x08;
pub const KIND_PEER_GROUP: u8 = 0x09;

/// Status bytes -- where a system record is in its two-stage
/// request/confirm lifecycle. Matches `pkg/shmevent`'s `Status*`
/// constants.
pub const STATUS_PENDING: u8 = 0x01;
pub const STATUS_CONFIRMED: u8 = 0x02;

/// Human-readable name for `kind`, matching `pkg/shmevent.KindName` --
/// "unknown(N)" for anything not defined above.
pub fn kind_name(kind: u8) -> String {
    match kind {
        KIND_PERMIT_PEER => "peer".to_string(),
        KIND_BOOTSTRAP_NODE => "bootstrap".to_string(),
        KIND_CLUSTER_MEMBER => "cluster-member".to_string(),
        KIND_CLUSTER_JOIN => "cluster-join".to_string(),
        KIND_GROUP => "group".to_string(),
        KIND_COMMAND => "command".to_string(),
        KIND_GROUP_COMMAND => "group-command".to_string(),
        KIND_PEER_GROUP => "peer-group".to_string(),
        _ => format!("unknown({kind})"),
    }
}

/// Inverse of [`kind_name`], matching `pkg/shmevent.KindFromName`.
pub fn kind_from_name(name: &str) -> Option<u8> {
    match name {
        "peer" => Some(KIND_PERMIT_PEER),
        "bootstrap" => Some(KIND_BOOTSTRAP_NODE),
        "cluster-member" => Some(KIND_CLUSTER_MEMBER),
        "cluster-join" => Some(KIND_CLUSTER_JOIN),
        "group" => Some(KIND_GROUP),
        "command" => Some(KIND_COMMAND),
        "group-command" => Some(KIND_GROUP_COMMAND),
        "peer-group" => Some(KIND_PEER_GROUP),
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

/// Packs `kind`, `peer_id`, and `metadata` into a single
/// `EVENT_PERMIT_REQUEST` `Msg.value`: `kind`, then a 2-byte big-endian
/// length prefix for `peer_id`, then `peer_id`, then `metadata` verbatim
/// (the rest of the buffer). Matches
/// `pkg/shmevent.EncodePermitRequestPayload`.
pub fn encode_permit_request_payload(
    kind: u8,
    peer_id: &[u8],
    metadata: &[u8],
) -> Result<Vec<u8>, Error> {
    if peer_id.len() > 0xFFFF {
        return Err(Error(format!(
            "permit request peerID too long: {} bytes",
            peer_id.len()
        )));
    }
    let mut buf = Vec::with_capacity(1 + 2 + peer_id.len() + metadata.len());
    buf.push(kind);
    buf.push((peer_id.len() >> 8) as u8);
    buf.push(peer_id.len() as u8);
    buf.extend_from_slice(peer_id);
    buf.extend_from_slice(metadata);
    Ok(buf)
}

/// Inverse of [`encode_permit_request_payload`].
pub fn decode_permit_request_payload(payload: &[u8]) -> Result<(u8, &[u8], &[u8]), Error> {
    if payload.len() < 3 {
        return Err(Error(format!(
            "permit request payload too short: {} bytes",
            payload.len()
        )));
    }
    let kind = payload[0];
    let id_len = ((payload[1] as usize) << 8) | payload[2] as usize;
    if 3 + id_len > payload.len() {
        return Err(Error(format!(
            "permit request peerID length {id_len} exceeds payload size {}",
            payload.len()
        )));
    }
    Ok((kind, &payload[3..3 + id_len], &payload[3 + id_len..]))
}

/// Packs `kind` and `peer_id` (the rest of the buffer) into a single
/// `EVENT_PERMIT_CONFIRM`/`EVENT_PERMIT_REVOKE` `Msg.value` -- no metadata
/// field, since the daemon reads the pending request's own value back out
/// of the store rather than trusting the caller to resend it. Matches
/// `pkg/shmevent.EncodePermitConfirmPayload`.
pub fn encode_permit_confirm_payload(kind: u8, peer_id: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + peer_id.len());
    buf.push(kind);
    buf.extend_from_slice(peer_id);
    buf
}

/// Inverse of [`encode_permit_confirm_payload`].
pub fn decode_permit_confirm_payload(payload: &[u8]) -> Result<(u8, &[u8]), Error> {
    if payload.is_empty() {
        return Err(Error("permit confirm payload too short".to_string()));
    }
    Ok((payload[0], &payload[1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_name_round_trip() {
        for k in [
            KIND_PERMIT_PEER,
            KIND_BOOTSTRAP_NODE,
            KIND_CLUSTER_MEMBER,
            KIND_CLUSTER_JOIN,
            KIND_GROUP,
            KIND_COMMAND,
            KIND_GROUP_COMMAND,
            KIND_PEER_GROUP,
        ] {
            let name = kind_name(k);
            assert_eq!(kind_from_name(&name), Some(k), "round trip for {name:?}");
        }
        assert_eq!(kind_from_name("not_a_real_kind"), None);
    }

    #[test]
    fn system_key_layout() {
        let key = system_key(KIND_GROUP, 0x00, b"my-group");
        assert_eq!(key[0], SYSTEM_KEY_PREFIX);
        assert_eq!(key[1], KIND_GROUP);
        assert_eq!(key[2], 0x00);
        assert_eq!(&key[3..], b"my-group");
    }

    #[test]
    fn permit_request_payload_round_trip() {
        let encoded =
            encode_permit_request_payload(KIND_PERMIT_PEER, b"peer-123", b"some metadata")
                .unwrap();
        let (kind, peer_id, metadata) = decode_permit_request_payload(&encoded).unwrap();
        assert_eq!(kind, KIND_PERMIT_PEER);
        assert_eq!(peer_id, b"peer-123");
        assert_eq!(metadata, b"some metadata");
    }

    #[test]
    fn permit_request_payload_empty_metadata() {
        let encoded = encode_permit_request_payload(KIND_BOOTSTRAP_NODE, b"peer-x", b"").unwrap();
        let (kind, peer_id, metadata) = decode_permit_request_payload(&encoded).unwrap();
        assert_eq!(kind, KIND_BOOTSTRAP_NODE);
        assert_eq!(peer_id, b"peer-x");
        assert_eq!(metadata, b"");
    }

    #[test]
    fn permit_confirm_payload_round_trip() {
        let encoded = encode_permit_confirm_payload(KIND_PERMIT_PEER, b"peer-456");
        let (kind, peer_id) = decode_permit_confirm_payload(&encoded).unwrap();
        assert_eq!(kind, KIND_PERMIT_PEER);
        assert_eq!(peer_id, b"peer-456");
    }

    #[test]
    fn permit_confirm_payload_too_short_rejected() {
        assert!(decode_permit_confirm_payload(&[]).is_err());
    }
}
