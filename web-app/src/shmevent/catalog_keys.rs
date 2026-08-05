//! Rust port of `pkg/shmevent/catalog.go`: key-building, bounds, and
//! payload framing for the Group/Command ACL catalog. Named
//! `catalog_keys`, not `catalog`, to avoid colliding with the top-level
//! `catalog` module (business logic built on top of these keys).

use super::system::{system_key, KIND_COMMAND, KIND_GROUP, KIND_GROUP_COMMAND, KIND_PEER_GROUP};
use super::Error;

/// `KindGroup`/`KindCommand`/`KindGroupCommand`/`KindPeerGroup` have no
/// pending/confirmed lifecycle -- every record is written and read
/// directly under this fixed placeholder. Matches
/// `pkg/shmevent.catalogStatusPlaceholder`.
const CATALOG_STATUS_PLACEHOLDER: u8 = 0x00;

/// Offset into a Group/Command record's key at which its trailing id
/// field starts (`SYSTEM_KEY_PREFIX`+kind+status = 3 bytes) -- the
/// standing convention (`systemKeyIDOffset` in `pkg/kvctl`/
/// `mobile/kvmobile`) for slicing `key[3:]` to recover the id after a
/// range-scan hit.
pub const SYSTEM_KEY_ID_OFFSET: usize = 3;

/// Builds the store key for a Group record. Matches
/// `pkg/shmevent.GroupKey`.
pub fn group_key(id: &[u8]) -> Vec<u8> {
    system_key(KIND_GROUP, CATALOG_STATUS_PLACEHOLDER, id)
}

/// Builds the store key for a Command record. Matches
/// `pkg/shmevent.CommandKey`.
pub fn command_key(id: &[u8]) -> Vec<u8> {
    system_key(KIND_COMMAND, CATALOG_STATUS_PLACEHOLDER, id)
}

/// Shared bound construction behind [`group_key_bounds`]/
/// [`command_key_bounds`] -- `[lo, hi]` covering every record under a
/// system-key kind+status prefix with one trailing unprefixed id field.
/// Matches `pkg/shmevent.keyListBounds`.
fn key_list_bounds(kind: u8) -> (Vec<u8>, Vec<u8>) {
    let prefix = system_key(kind, CATALOG_STATUS_PLACEHOLDER, &[]);
    let lo = prefix.clone();
    let mut hi = prefix.clone();
    hi.extend(std::iter::repeat(0xFFu8).take(64));
    (lo, hi)
}

/// `[lo, hi]` key range covering every currently-stored Group record.
/// Matches `pkg/shmevent.GroupKeyBounds`.
pub fn group_key_bounds() -> (Vec<u8>, Vec<u8>) {
    key_list_bounds(KIND_GROUP)
}

/// `[lo, hi]` key range covering every currently-stored Command record.
/// Matches `pkg/shmevent.CommandKeyBounds`.
pub fn command_key_bounds() -> (Vec<u8>, Vec<u8>) {
    key_list_bounds(KIND_COMMAND)
}

/// Builds the store key for one Group<->Command relation record:
/// `command_id` first (length-prefixed, prefix-scannable -- see
/// [`group_command_bounds`]), `group_id` last (no prefix needed). Matches
/// `pkg/shmevent.GroupCommandKey`.
pub fn group_command_key(command_id: &[u8], group_id: &[u8]) -> Result<Vec<u8>, Error> {
    if command_id.len() > 0xFFFF {
        return Err(Error(format!(
            "group-command commandID too long: {} bytes",
            command_id.len()
        )));
    }
    let mut key = Vec::with_capacity(3 + 2 + command_id.len() + group_id.len());
    key.push(super::system::SYSTEM_KEY_PREFIX);
    key.push(KIND_GROUP_COMMAND);
    key.push(CATALOG_STATUS_PLACEHOLDER);
    key.push((command_id.len() >> 8) as u8);
    key.push(command_id.len() as u8);
    key.extend_from_slice(command_id);
    key.extend_from_slice(group_id);
    Ok(key)
}

/// `[lo, hi]` key range covering every group linked to `command_id`.
/// Matches `pkg/shmevent.GroupCommandBounds`.
pub fn group_command_bounds(command_id: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let prefix = group_command_key(command_id, &[])?;
    let lo = prefix.clone();
    let mut hi = prefix;
    hi.extend(std::iter::repeat(0xFFu8).take(64));
    Ok((lo, hi))
}

/// Builds the store key for one Peer<->Group relation record: `peer_id`
/// first (length-prefixed, prefix-scannable -- see
/// [`peer_group_bounds`]), `group_id` last. Matches
/// `pkg/shmevent.PeerGroupKey`.
pub fn peer_group_key(peer_id: &[u8], group_id: &[u8]) -> Result<Vec<u8>, Error> {
    if peer_id.len() > 0xFFFF {
        return Err(Error(format!(
            "peer-group peerID too long: {} bytes",
            peer_id.len()
        )));
    }
    let mut key = Vec::with_capacity(3 + 2 + peer_id.len() + group_id.len());
    key.push(super::system::SYSTEM_KEY_PREFIX);
    key.push(KIND_PEER_GROUP);
    key.push(CATALOG_STATUS_PLACEHOLDER);
    key.push((peer_id.len() >> 8) as u8);
    key.push(peer_id.len() as u8);
    key.extend_from_slice(peer_id);
    key.extend_from_slice(group_id);
    Ok(key)
}

/// `[lo, hi]` key range covering every group `peer_id` belongs to.
/// Matches `pkg/shmevent.PeerGroupBounds`.
pub fn peer_group_bounds(peer_id: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let prefix = peer_group_key(peer_id, &[])?;
    let lo = prefix.clone();
    let mut hi = prefix;
    hi.extend(std::iter::repeat(0xFFu8).take(64));
    Ok((lo, hi))
}

/// Inverse of [`group_command_key`]: given a full GroupCommand record key,
/// returns its `command_id`/`group_id` fields. Matches
/// `pkg/shmevent.ParseGroupCommandKey`.
pub fn parse_group_command_key(key: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    if key.len() < 5 || key[0] != super::system::SYSTEM_KEY_PREFIX || key[1] != KIND_GROUP_COMMAND
    {
        return Err(Error("key is not a KindGroupCommand key".to_string()));
    }
    let cmd_len = ((key[3] as usize) << 8) | key[4] as usize;
    if 5 + cmd_len > key.len() {
        return Err(Error("group-command key truncated in commandID".to_string()));
    }
    Ok((&key[5..5 + cmd_len], &key[5 + cmd_len..]))
}

/// Inverse of [`peer_group_key`]. Matches
/// `pkg/shmevent.ParsePeerGroupKey`.
pub fn parse_peer_group_key(key: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    if key.len() < 5 || key[0] != super::system::SYSTEM_KEY_PREFIX || key[1] != KIND_PEER_GROUP {
        return Err(Error("key is not a KindPeerGroup key".to_string()));
    }
    let id_len = ((key[3] as usize) << 8) | key[4] as usize;
    if 5 + id_len > key.len() {
        return Err(Error("peer-group key truncated in peerID".to_string()));
    }
    Ok((&key[5..5 + id_len], &key[5 + id_len..]))
}

/// Bare prefix shared by every GroupCommand record system-wide -- used by
/// the server's cascade-delete when a Group (not a Command) is deleted.
/// Not needed by this client's own logic (cascade-delete is entirely
/// server-side), kept for wire-table completeness/parity with the Go
/// source. Matches `pkg/shmevent.AllGroupCommandsPrefix`.
pub fn all_group_commands_prefix() -> Vec<u8> {
    system_key(KIND_GROUP_COMMAND, CATALOG_STATUS_PLACEHOLDER, &[])
}

/// [`all_group_commands_prefix`]'s PeerGroup counterpart. Matches
/// `pkg/shmevent.AllPeerGroupsPrefix`.
pub fn all_peer_groups_prefix() -> Vec<u8> {
    system_key(KIND_PEER_GROUP, CATALOG_STATUS_PLACEHOLDER, &[])
}

/// Packs a Group record's name into its stored value -- id is already the
/// record's key ([`group_key`]). Matches `pkg/shmevent.EncodeGroupPayload`.
pub fn encode_group_payload(name: &str) -> Vec<u8> {
    name.as_bytes().to_vec()
}

/// Inverse of [`encode_group_payload`].
pub fn decode_group_payload(payload: &[u8]) -> String {
    String::from_utf8_lossy(payload).to_string()
}

/// Packs a Command record's name and peer_id (where it may be executed)
/// into its stored value: a 2-byte big-endian length prefix for name,
/// then name, then peer_id verbatim. Matches
/// `pkg/shmevent.EncodeCommandPayload`.
pub fn encode_command_payload(name: &str, peer_id: &[u8]) -> Result<Vec<u8>, Error> {
    let name_bytes = name.as_bytes();
    if name_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "command name too long: {} bytes",
            name_bytes.len()
        )));
    }
    let mut buf = Vec::with_capacity(2 + name_bytes.len() + peer_id.len());
    buf.push((name_bytes.len() >> 8) as u8);
    buf.push(name_bytes.len() as u8);
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(peer_id);
    Ok(buf)
}

/// Inverse of [`encode_command_payload`], reading either payload version and
/// discarding any spec -- see [`decode_command_payload_full`].
pub fn decode_command_payload(payload: &[u8]) -> Result<(String, &[u8]), Error> {
    let (name, peer_id, _) = decode_command_payload_full(payload)?;
    Ok((name, peer_id))
}

/// Decodes either Command payload version, returning the spec as well (empty
/// for a v1 record, which has none).
///
/// A Command record grew a third field -- the form definition a client renders
/// inputs from -- and v1's layout had no room for one, since `peer_id` is its
/// trailing field and takes the rest of the buffer. v2 is marked by an
/// impossible v1 name length (`0xFFFF`; the whole value is capped far below
/// that) followed by explicitly length-prefixed fields. Go's
/// `shmevent.EncodeCommandPayloadWithSpec` is the writer, and it still emits
/// v1 byte-for-byte whenever there is no spec, so a spec-less command reaching
/// this decoder is unchanged from before the field existed.
pub fn decode_command_payload_full(payload: &[u8]) -> Result<(String, &[u8], &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "command payload too short: {} bytes",
            payload.len()
        )));
    }
    if ((payload[0] as usize) << 8 | payload[1] as usize) == COMMAND_PAYLOAD_V2_SENTINEL {
        let mut off = 2usize;
        let name_len = read_len(payload, &mut off, "command name length")?;
        let name = read_slice(payload, &mut off, name_len, "command name")?;
        let peer_len = read_len(payload, &mut off, "command peer id length")?;
        let peer_id = read_slice(payload, &mut off, peer_len, "command peer id")?;
        return Ok((
            String::from_utf8_lossy(name).to_string(),
            peer_id,
            &payload[off..],
        ));
    }
    let (name, peer_id) = decode_command_payload_v1(payload)?;
    Ok((name, peer_id, &[]))
}

/// An impossible v1 name length, marking a v2 payload. See
/// [`decode_command_payload_full`].
const COMMAND_PAYLOAD_V2_SENTINEL: usize = 0xFFFF;

fn read_len(payload: &[u8], off: &mut usize, what: &str) -> Result<usize, Error> {
    if *off + 2 > payload.len() {
        return Err(Error(format!("command payload truncated reading {what}")));
    }
    let v = (payload[*off] as usize) << 8 | payload[*off + 1] as usize;
    *off += 2;
    Ok(v)
}

fn read_slice<'a>(
    payload: &'a [u8],
    off: &mut usize,
    len: usize,
    what: &str,
) -> Result<&'a [u8], Error> {
    if *off + len > payload.len() {
        return Err(Error(format!(
            "command {what} length {len} exceeds payload size {}",
            payload.len()
        )));
    }
    let s = &payload[*off..*off + len];
    *off += len;
    Ok(s)
}

fn decode_command_payload_v1(payload: &[u8]) -> Result<(String, &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "command payload too short: {} bytes",
            payload.len()
        )));
    }
    let name_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    let off = 2;
    if off + name_len > payload.len() {
        return Err(Error(format!(
            "command name length {name_len} exceeds payload size {}",
            payload.len()
        )));
    }
    let name = String::from_utf8_lossy(&payload[off..off + name_len]).to_string();
    Ok((name, &payload[off + name_len..]))
}

/// Packs `id` and `name` into a single `EVENT_GROUP_PUT` `Msg.value`: a
/// 2-byte big-endian length prefix for id, then id, then name verbatim.
/// Distinct from [`encode_group_payload`] (the record's stored value,
/// keyed separately by [`group_key`]`(id)`). Matches
/// `pkg/shmevent.EncodeGroupPutPayload`.
pub fn encode_group_put_payload(id: &str, name: &str) -> Result<Vec<u8>, Error> {
    let id_bytes = id.as_bytes();
    if id_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "group put id too long: {} bytes",
            id_bytes.len()
        )));
    }
    let name_bytes = name.as_bytes();
    let mut buf = Vec::with_capacity(2 + id_bytes.len() + name_bytes.len());
    buf.push((id_bytes.len() >> 8) as u8);
    buf.push(id_bytes.len() as u8);
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(name_bytes);
    Ok(buf)
}

/// Inverse of [`encode_group_put_payload`].
pub fn decode_group_put_payload(payload: &[u8]) -> Result<(String, String), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "group put payload too short: {} bytes",
            payload.len()
        )));
    }
    let id_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    let off = 2;
    if off + id_len > payload.len() {
        return Err(Error(format!(
            "group put id length {id_len} exceeds payload size {}",
            payload.len()
        )));
    }
    let id = String::from_utf8_lossy(&payload[off..off + id_len]).to_string();
    let name = String::from_utf8_lossy(&payload[off + id_len..]).to_string();
    Ok((id, name))
}

/// Packs `id`, `name`, and `peer_id` into a single `EVENT_COMMAND_PUT`
/// `Msg.value`: 2-byte length prefix + id, then 2-byte length prefix +
/// name, then peer_id verbatim. Distinct from [`encode_command_payload`]
/// (the record's stored value). Matches
/// `pkg/shmevent.EncodeCommandPutPayload`.
pub fn encode_command_put_payload(
    id: &str,
    name: &str,
    peer_id: &[u8],
) -> Result<Vec<u8>, Error> {
    let id_bytes = id.as_bytes();
    if id_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "command put id too long: {} bytes",
            id_bytes.len()
        )));
    }
    let name_bytes = name.as_bytes();
    if name_bytes.len() > 0xFFFF {
        return Err(Error(format!(
            "command put name too long: {} bytes",
            name_bytes.len()
        )));
    }
    let mut buf =
        Vec::with_capacity(2 + id_bytes.len() + 2 + name_bytes.len() + peer_id.len());
    buf.push((id_bytes.len() >> 8) as u8);
    buf.push(id_bytes.len() as u8);
    buf.extend_from_slice(id_bytes);
    buf.push((name_bytes.len() >> 8) as u8);
    buf.push(name_bytes.len() as u8);
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(peer_id);
    Ok(buf)
}

/// Inverse of [`encode_command_put_payload`].
pub fn decode_command_put_payload(payload: &[u8]) -> Result<(String, String, &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "command put payload too short: {} bytes",
            payload.len()
        )));
    }
    let id_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    let mut off = 2;
    if off + id_len > payload.len() {
        return Err(Error(format!(
            "command put id length {id_len} exceeds payload size {}",
            payload.len()
        )));
    }
    let id = String::from_utf8_lossy(&payload[off..off + id_len]).to_string();
    off += id_len;
    if off + 2 > payload.len() {
        return Err(Error(
            "command put payload too short for name length".to_string(),
        ));
    }
    let name_len = ((payload[off] as usize) << 8) | payload[off + 1] as usize;
    off += 2;
    if off + name_len > payload.len() {
        return Err(Error(format!(
            "command put name length {name_len} exceeds payload size {}",
            payload.len()
        )));
    }
    let name = String::from_utf8_lossy(&payload[off..off + name_len]).to_string();
    off += name_len;
    Ok((id, name, &payload[off..]))
}

/// Packs `command_id` and `group_id` into a single
/// `EVENT_GROUP_COMMAND_PUT`/`EVENT_GROUP_COMMAND_DELETE` `Msg.value`: a
/// 2-byte big-endian length prefix for command_id, then command_id, then
/// group_id verbatim -- mirrors [`group_command_key`]'s own field order,
/// so decoding this payload and passing the results straight to
/// `group_command_key` builds the record's key. Matches
/// `pkg/shmevent.EncodeGroupCommandPayload`.
pub fn encode_group_command_payload(command_id: &[u8], group_id: &[u8]) -> Result<Vec<u8>, Error> {
    if command_id.len() > 0xFFFF {
        return Err(Error(format!(
            "group-command commandID too long: {} bytes",
            command_id.len()
        )));
    }
    let mut buf = Vec::with_capacity(2 + command_id.len() + group_id.len());
    buf.push((command_id.len() >> 8) as u8);
    buf.push(command_id.len() as u8);
    buf.extend_from_slice(command_id);
    buf.extend_from_slice(group_id);
    Ok(buf)
}

/// Inverse of [`encode_group_command_payload`].
pub fn decode_group_command_payload(payload: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "group-command payload too short: {} bytes",
            payload.len()
        )));
    }
    let cmd_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    let off = 2;
    if off + cmd_len > payload.len() {
        return Err(Error(format!(
            "group-command commandID length {cmd_len} exceeds payload size {}",
            payload.len()
        )));
    }
    Ok((&payload[off..off + cmd_len], &payload[off + cmd_len..]))
}

/// [`encode_group_command_payload`]'s PeerGroup counterpart: `peer_id`
/// first (length-prefixed), `group_id` last. Used for both
/// `EVENT_PEER_GROUP_PUT` and `EVENT_PEER_GROUP_DELETE`. Matches
/// `pkg/shmevent.EncodePeerGroupPayload`.
pub fn encode_peer_group_payload(peer_id: &[u8], group_id: &[u8]) -> Result<Vec<u8>, Error> {
    if peer_id.len() > 0xFFFF {
        return Err(Error(format!(
            "peer-group peerID too long: {} bytes",
            peer_id.len()
        )));
    }
    let mut buf = Vec::with_capacity(2 + peer_id.len() + group_id.len());
    buf.push((peer_id.len() >> 8) as u8);
    buf.push(peer_id.len() as u8);
    buf.extend_from_slice(peer_id);
    buf.extend_from_slice(group_id);
    Ok(buf)
}

/// Inverse of [`encode_peer_group_payload`].
pub fn decode_peer_group_payload(payload: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    if payload.len() < 2 {
        return Err(Error(format!(
            "peer-group payload too short: {} bytes",
            payload.len()
        )));
    }
    let id_len = ((payload[0] as usize) << 8) | payload[1] as usize;
    let off = 2;
    if off + id_len > payload.len() {
        return Err(Error(format!(
            "peer-group peerID length {id_len} exceeds payload size {}",
            payload.len()
        )));
    }
    Ok((&payload[off..off + id_len], &payload[off + id_len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_key_bounds_prefix_matches_group_key() {
        let (lo, _hi) = group_key_bounds();
        let key = group_key(b"abc");
        assert!(key.starts_with(&lo));
    }

    #[test]
    fn command_key_id_offset() {
        let key = command_key(b"my-command");
        assert_eq!(&key[SYSTEM_KEY_ID_OFFSET..], b"my-command");
    }

    #[test]
    fn group_command_key_round_trip() {
        let key = group_command_key(b"cmd-1", b"grp-1").unwrap();
        let (command_id, group_id) = parse_group_command_key(&key).unwrap();
        assert_eq!(command_id, b"cmd-1");
        assert_eq!(group_id, b"grp-1");
    }

    #[test]
    fn group_command_bounds_prefix_matches_key() {
        let (lo, _hi) = group_command_bounds(b"cmd-1").unwrap();
        let key = group_command_key(b"cmd-1", b"grp-anything").unwrap();
        assert!(key.starts_with(&lo));
    }

    #[test]
    fn peer_group_key_round_trip() {
        let key = peer_group_key(b"peer-1", b"grp-1").unwrap();
        let (peer_id, group_id) = parse_peer_group_key(&key).unwrap();
        assert_eq!(peer_id, b"peer-1");
        assert_eq!(group_id, b"grp-1");
    }

    #[test]
    fn parse_group_command_key_rejects_wrong_kind() {
        let key = peer_group_key(b"peer-1", b"grp-1").unwrap();
        assert!(parse_group_command_key(&key).is_err());
    }

    #[test]
    fn group_payload_round_trip() {
        let encoded = encode_group_payload("My Group");
        assert_eq!(decode_group_payload(&encoded), "My Group");
    }

    #[test]
    fn command_payload_round_trip() {
        let encoded = encode_command_payload("Reboot", b"12D3KooWtarget").unwrap();
        let (name, peer_id) = decode_command_payload(&encoded).unwrap();
        assert_eq!(name, "Reboot");
        assert_eq!(peer_id, b"12D3KooWtarget");
    }

    #[test]
    fn group_put_payload_round_trip() {
        let encoded = encode_group_put_payload("grp-1", "Group One").unwrap();
        let (id, name) = decode_group_put_payload(&encoded).unwrap();
        assert_eq!(id, "grp-1");
        assert_eq!(name, "Group One");
    }

    #[test]
    fn command_put_payload_round_trip() {
        let encoded = encode_command_put_payload("cmd-1", "Reboot", b"peer-xyz").unwrap();
        let (id, name, peer_id) = decode_command_put_payload(&encoded).unwrap();
        assert_eq!(id, "cmd-1");
        assert_eq!(name, "Reboot");
        assert_eq!(peer_id, b"peer-xyz");
    }

    #[test]
    fn group_command_payload_round_trip() {
        let encoded = encode_group_command_payload(b"cmd-1", b"grp-1").unwrap();
        let (command_id, group_id) = decode_group_command_payload(&encoded).unwrap();
        assert_eq!(command_id, b"cmd-1");
        assert_eq!(group_id, b"grp-1");
    }

    #[test]
    fn peer_group_payload_round_trip() {
        let encoded = encode_peer_group_payload(b"peer-1", b"grp-1").unwrap();
        let (peer_id, group_id) = decode_peer_group_payload(&encoded).unwrap();
        assert_eq!(peer_id, b"peer-1");
        assert_eq!(group_id, b"grp-1");
    }

    #[test]
    fn all_prefixes_are_distinct_and_short() {
        assert_ne!(all_group_commands_prefix(), all_peer_groups_prefix());
    }
}
