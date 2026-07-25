//! Rust port of `pkg/shmevent/logpermit.go`: `KindLogPermit` record
//! key-building and payload framing -- a permit scoped by an arbitrary
//! `logKind` string in addition to `peerID`, unlike [`super::system`]'s
//! fixed-kind-byte permits.

use super::system::{KIND_LOG_PERMIT, SYSTEM_KEY_PREFIX};
use super::Error;

/// Builds the store key for a `KindLogPermit` record: `SYSTEM_KEY_PREFIX`,
/// `KIND_LOG_PERMIT`, `status`, then a 2-byte big-endian length prefix for
/// `log_kind`, then `log_kind`, then `peer_id` verbatim. Matches
/// `pkg/shmevent.LogPermitKey`.
pub fn log_permit_key(status: u8, log_kind: &str, peer_id: &[u8]) -> Result<Vec<u8>, Error> {
    let log_kind_bytes = log_kind.as_bytes();
    if log_kind_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "log permit logKind too long: {} bytes",
            log_kind_bytes.len()
        )));
    }
    let mut key = Vec::with_capacity(3 + 2 + log_kind_bytes.len() + peer_id.len());
    key.push(SYSTEM_KEY_PREFIX);
    key.push(KIND_LOG_PERMIT);
    key.push(status);
    key.push((log_kind_bytes.len() >> 8) as u8);
    key.push(log_kind_bytes.len() as u8);
    key.extend_from_slice(log_kind_bytes);
    key.extend_from_slice(peer_id);
    Ok(key)
}

/// Packs `log_kind`, `peer_id`, and `metadata` into a single
/// `EVENT_LOG_PERMIT_REQUEST` `Msg.value`: a 2-byte big-endian length
/// prefix for `log_kind`, then `log_kind`, then a 2-byte big-endian length
/// prefix for `peer_id`, then `peer_id`, then `metadata` verbatim (the
/// rest of the buffer). Matches
/// `pkg/shmevent.EncodeLogPermitRequestPayload`.
pub fn encode_log_permit_request_payload(
    log_kind: &str,
    peer_id: &[u8],
    metadata: &[u8],
) -> Result<Vec<u8>, Error> {
    let log_kind_bytes = log_kind.as_bytes();
    if log_kind_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "log permit request logKind too long: {} bytes",
            log_kind_bytes.len()
        )));
    }
    if peer_id.len() > 0xFFFF {
        return Err(Error(format!(
            "log permit request peerID too long: {} bytes",
            peer_id.len()
        )));
    }
    let mut buf =
        Vec::with_capacity(2 + log_kind_bytes.len() + 2 + peer_id.len() + metadata.len());
    buf.push((log_kind_bytes.len() >> 8) as u8);
    buf.push(log_kind_bytes.len() as u8);
    buf.extend_from_slice(log_kind_bytes);
    buf.push((peer_id.len() >> 8) as u8);
    buf.push(peer_id.len() as u8);
    buf.extend_from_slice(peer_id);
    buf.extend_from_slice(metadata);
    Ok(buf)
}

/// Inverse of [`encode_log_permit_request_payload`].
pub fn decode_log_permit_request_payload(
    payload: &[u8],
) -> Result<(String, &[u8], &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "log permit request payload too short: {} bytes",
            payload.len()
        )));
    }
    let kind_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    let mut off = 2;
    if off + kind_len > payload.len() {
        return Err(Error(format!(
            "log permit request logKind length {kind_len} exceeds payload size {}",
            payload.len()
        )));
    }
    let log_kind = String::from_utf8_lossy(&payload[off..off + kind_len]).to_string();
    off += kind_len;
    if off + 2 > payload.len() {
        return Err(Error(
            "log permit request payload too short for peerID length".to_string(),
        ));
    }
    let id_len = ((payload[off] as usize) << 8) | payload[off + 1] as usize;
    off += 2;
    if off + id_len > payload.len() {
        return Err(Error(format!(
            "log permit request peerID length {id_len} exceeds payload size {}",
            payload.len()
        )));
    }
    Ok((log_kind, &payload[off..off + id_len], &payload[off + id_len..]))
}

/// Packs `log_kind` and `peer_id` (the rest of the buffer) into a single
/// `EVENT_LOG_PERMIT_CONFIRM`/`EVENT_LOG_PERMIT_REVOKE` `Msg.value` -- no
/// metadata field, mirroring `encode_permit_confirm_payload`'s reasoning.
/// Matches `pkg/shmevent.EncodeLogPermitConfirmPayload`.
pub fn encode_log_permit_confirm_payload(log_kind: &str, peer_id: &[u8]) -> Result<Vec<u8>, Error> {
    let log_kind_bytes = log_kind.as_bytes();
    if log_kind_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "log permit confirm logKind too long: {} bytes",
            log_kind_bytes.len()
        )));
    }
    let mut buf = Vec::with_capacity(2 + log_kind_bytes.len() + peer_id.len());
    buf.push((log_kind_bytes.len() >> 8) as u8);
    buf.push(log_kind_bytes.len() as u8);
    buf.extend_from_slice(log_kind_bytes);
    buf.extend_from_slice(peer_id);
    Ok(buf)
}

/// Inverse of [`encode_log_permit_confirm_payload`].
pub fn decode_log_permit_confirm_payload(payload: &[u8]) -> Result<(String, &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "log permit confirm payload too short: {} bytes",
            payload.len()
        )));
    }
    let kind_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    let off = 2;
    if off + kind_len > payload.len() {
        return Err(Error(format!(
            "log permit confirm logKind length {kind_len} exceeds payload size {}",
            payload.len()
        )));
    }
    let log_kind = String::from_utf8_lossy(&payload[off..off + kind_len]).to_string();
    Ok((log_kind, &payload[off + kind_len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_permit_key_layout() {
        let key = log_permit_key(0x01, "cmdlog", b"peer-abc").unwrap();
        assert_eq!(key[0], SYSTEM_KEY_PREFIX);
        assert_eq!(key[1], KIND_LOG_PERMIT);
        assert_eq!(key[2], 0x01);
        let kind_len = ((key[3] as usize) << 8) | key[4] as usize;
        assert_eq!(kind_len, 6);
        assert_eq!(&key[5..5 + kind_len], b"cmdlog");
        assert_eq!(&key[5 + kind_len..], b"peer-abc");
    }

    #[test]
    fn log_permit_request_payload_round_trip() {
        let encoded =
            encode_log_permit_request_payload("cmdlog", b"peer-123", b"meta").unwrap();
        let (log_kind, peer_id, metadata) = decode_log_permit_request_payload(&encoded).unwrap();
        assert_eq!(log_kind, "cmdlog");
        assert_eq!(peer_id, b"peer-123");
        assert_eq!(metadata, b"meta");
    }

    #[test]
    fn log_permit_confirm_payload_round_trip() {
        let encoded = encode_log_permit_confirm_payload("cmdlog", b"peer-456").unwrap();
        let (log_kind, peer_id) = decode_log_permit_confirm_payload(&encoded).unwrap();
        assert_eq!(log_kind, "cmdlog");
        assert_eq!(peer_id, b"peer-456");
    }

    #[test]
    fn log_permit_confirm_payload_too_short_rejected() {
        assert!(decode_log_permit_confirm_payload(&[0u8]).is_err());
    }
}
