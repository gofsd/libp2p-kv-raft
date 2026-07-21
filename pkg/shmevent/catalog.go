package shmevent

import "fmt"

// catalogStatusPlaceholder mirrors clusterMemberStatusPlaceholder:
// KindGroup/KindCommand/KindGroupCommand/KindPeerGroup have no
// pending/confirmed lifecycle (see those constants' doc comment in
// system.go) -- every record is written and read directly under this
// fixed placeholder, keeping the key layout uniform with every other
// SystemKey-produced key.
const catalogStatusPlaceholder = 0x00

// GroupKey builds the pkg/store key for a Group record -- same shape as
// ClusterMemberKey: a single variable-length ID field needing no length
// prefix since it's always last.
func GroupKey(id []byte) []byte {
	return SystemKey(KindGroup, catalogStatusPlaceholder, id)
}

// CommandKey builds the pkg/store key for a Command record.
func CommandKey(id []byte) []byte {
	return SystemKey(KindCommand, catalogStatusPlaceholder, id)
}

// keyListBounds returns the [lo, hi] key range covering every record
// under a SystemKey kind+status prefix that has exactly one, last,
// unprefixed variable-length ID field (Group/Command/ClusterMember's
// shape) -- shared bound construction behind GroupKeyBounds/
// CommandKeyBounds, mirroring ClusterMemberKeyBounds' own padding.
func keyListBounds(kind byte) (lo, hi []byte) {
	prefix := SystemKey(kind, catalogStatusPlaceholder, nil)
	lo = prefix
	hi = make([]byte, len(prefix)+64)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi
}

// GroupKeyBounds returns the [lo, hi] key range covering every
// currently-stored Group record -- for a raw range scan (see
// pkg/shmclient.Session.ListRange), the enumeration counterpart to
// GroupKey's single-record lookup.
func GroupKeyBounds() (lo, hi []byte) {
	return keyListBounds(KindGroup)
}

// CommandKeyBounds is GroupKeyBounds' Command counterpart.
func CommandKeyBounds() (lo, hi []byte) {
	return keyListBounds(KindCommand)
}

// GroupCommandKey builds the pkg/store key for one Group<->Command
// relation record: commandID first (length-prefixed, so it alone can be
// prefix-scanned -- see GroupCommandBounds -- to answer "every group this
// command is exposed to", the side of the join scanned at command-execute
// time; a command is expected to be linked to few groups, unlike a peer,
// which may belong to many, so scanning this side first and point-checking
// PeerGroupKey for each hit is the cheaper order), groupID last (needs no
// prefix of its own, mirroring SystemKey's last-field convention). Two
// variable-length fields, same shape as LogPermitKey. Both fields make up
// the record's identity -- creating the same (commandID, groupID) pair
// twice is a plain overwrite, not a duplicate, and deleting needs no
// separate lookup.
func GroupCommandKey(commandID, groupID []byte) ([]byte, error) {
	if len(commandID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: group-command commandID too long: %d bytes", len(commandID))
	}
	key := make([]byte, 3+2+len(commandID)+len(groupID))
	key[0] = SystemKeyPrefix
	key[1] = KindGroupCommand
	key[2] = catalogStatusPlaceholder
	key[3] = byte(len(commandID) >> 8)
	key[4] = byte(len(commandID))
	off := 5
	off += copy(key[off:], commandID)
	copy(key[off:], groupID)
	return key, nil
}

// GroupCommandBounds returns the [lo, hi] key range covering every group
// linked to commandID -- prefix-scannable since commandID is
// GroupCommandKey's first variable field. hi pads well past any real
// group id's byte length, mirroring ClusterMemberKeyBounds' padding.
func GroupCommandBounds(commandID []byte) (lo, hi []byte, err error) {
	prefix, err := GroupCommandKey(commandID, nil)
	if err != nil {
		return nil, nil, err
	}
	lo = prefix
	hi = make([]byte, len(prefix)+64)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi, nil
}

// PeerGroupKey builds the pkg/store key for one Peer<->Group relation
// record: peerID first (length-prefixed, prefix-scannable -- see
// PeerGroupBounds -- to answer "every group this peer belongs to", used
// both for introspection and for the point-check half of the
// command-execute join), groupID last. Same duplicate-proof,
// lookup-free-delete reasoning as GroupCommandKey applies here too.
func PeerGroupKey(peerID, groupID []byte) ([]byte, error) {
	if len(peerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: peer-group peerID too long: %d bytes", len(peerID))
	}
	key := make([]byte, 3+2+len(peerID)+len(groupID))
	key[0] = SystemKeyPrefix
	key[1] = KindPeerGroup
	key[2] = catalogStatusPlaceholder
	key[3] = byte(len(peerID) >> 8)
	key[4] = byte(len(peerID))
	off := 5
	off += copy(key[off:], peerID)
	copy(key[off:], groupID)
	return key, nil
}

// PeerGroupBounds returns the [lo, hi] key range covering every group
// peerID belongs to -- prefix-scannable since peerID is PeerGroupKey's
// first variable field.
func PeerGroupBounds(peerID []byte) (lo, hi []byte, err error) {
	prefix, err := PeerGroupKey(peerID, nil)
	if err != nil {
		return nil, nil, err
	}
	lo = prefix
	hi = make([]byte, len(prefix)+64)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi, nil
}

// ParseGroupCommandKey is the inverse of GroupCommandKey: given a full
// GroupCommand record key, returns its commandID and groupID fields.
func ParseGroupCommandKey(key []byte) (commandID, groupID []byte, err error) {
	if len(key) < 5 || key[0] != SystemKeyPrefix || key[1] != KindGroupCommand {
		return nil, nil, fmt.Errorf("shmevent: key is not a KindGroupCommand key")
	}
	cmdLen := int(key[3])<<8 | int(key[4])
	if 5+cmdLen > len(key) {
		return nil, nil, fmt.Errorf("shmevent: group-command key truncated in commandID")
	}
	return key[5 : 5+cmdLen], key[5+cmdLen:], nil
}

// ParsePeerGroupKey is the inverse of PeerGroupKey.
func ParsePeerGroupKey(key []byte) (peerID, groupID []byte, err error) {
	if len(key) < 5 || key[0] != SystemKeyPrefix || key[1] != KindPeerGroup {
		return nil, nil, fmt.Errorf("shmevent: key is not a KindPeerGroup key")
	}
	idLen := int(key[3])<<8 | int(key[4])
	if 5+idLen > len(key) {
		return nil, nil, fmt.Errorf("shmevent: peer-group key truncated in peerID")
	}
	return key[5 : 5+idLen], key[5+idLen:], nil
}

// AllGroupCommandsPrefix returns the bare prefix shared by every
// GroupCommand record system-wide, with no commandID/groupID fields at
// all -- used by pkg/kvfsm's cascade-delete when a Group (not a Command)
// is deleted: GroupCommandKey has commandID first and groupID last, so
// there's no cheap groupID-scoped prefix scan the way there is for the
// Command-deletion cascade (see GroupCommandKey's own doc comment); this
// broader system-wide scan, filtered by parsing each key with
// ParseGroupCommandKey, is the accepted, rarer-path tradeoff instead.
func AllGroupCommandsPrefix() []byte {
	return SystemKey(KindGroupCommand, catalogStatusPlaceholder, nil)
}

// AllPeerGroupsPrefix is AllGroupCommandsPrefix's PeerGroup counterpart.
func AllPeerGroupsPrefix() []byte {
	return SystemKey(KindPeerGroup, catalogStatusPlaceholder, nil)
}

// EncodeGroupPayload packs a Group record's name into its value -- id is
// already the record's key (GroupKey), so only name needs to travel in
// the payload.
func EncodeGroupPayload(name string) []byte {
	return []byte(name)
}

// DecodeGroupPayload is the inverse of EncodeGroupPayload.
func DecodeGroupPayload(payload []byte) (name string) {
	return string(payload)
}

// EncodeCommandPayload packs a Command record's name and peerID (where it
// may be executed) into its value: a 2-byte big-endian length prefix for
// name, then name, then peerID verbatim (last field, no prefix needed) --
// id is already the record's key (CommandKey).
func EncodeCommandPayload(name string, peerID []byte) ([]byte, error) {
	if len(name) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: command name too long: %d bytes", len(name))
	}
	buf := make([]byte, 2+len(name)+len(peerID))
	buf[0] = byte(len(name) >> 8)
	buf[1] = byte(len(name))
	off := 2
	off += copy(buf[off:], name)
	copy(buf[off:], peerID)
	return buf, nil
}

// DecodeCommandPayload is the inverse of EncodeCommandPayload.
func DecodeCommandPayload(payload []byte) (name string, peerID []byte, err error) {
	if len(payload) < 2 {
		return "", nil, fmt.Errorf("shmevent: command payload too short: %d bytes", len(payload))
	}
	nameLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+nameLen > len(payload) {
		return "", nil, fmt.Errorf("shmevent: command name length %d exceeds payload size %d", nameLen, len(payload))
	}
	name = string(payload[off : off+nameLen])
	off += nameLen
	return name, payload[off:], nil
}

// EncodeGroupPutPayload packs id and name into a single EventGroupPut
// Msg.Value: a 2-byte big-endian length prefix for id, then id, then name
// verbatim (last field, no prefix needed). Distinct from
// EncodeGroupPayload (the record's stored *value*, keyed separately by
// GroupKey(id)): this is the wire-level request, which needs both fields
// in one message.
func EncodeGroupPutPayload(id, name string) ([]byte, error) {
	if len(id) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: group put id too long: %d bytes", len(id))
	}
	buf := make([]byte, 2+len(id)+len(name))
	buf[0] = byte(len(id) >> 8)
	buf[1] = byte(len(id))
	off := 2
	off += copy(buf[off:], id)
	copy(buf[off:], name)
	return buf, nil
}

// DecodeGroupPutPayload is the inverse of EncodeGroupPutPayload.
func DecodeGroupPutPayload(payload []byte) (id, name string, err error) {
	if len(payload) < 2 {
		return "", "", fmt.Errorf("shmevent: group put payload too short: %d bytes", len(payload))
	}
	idLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+idLen > len(payload) {
		return "", "", fmt.Errorf("shmevent: group put id length %d exceeds payload size %d", idLen, len(payload))
	}
	id = string(payload[off : off+idLen])
	off += idLen
	return id, string(payload[off:]), nil
}

// EncodeCommandPutPayload packs id, name, and peerID into a single
// EventCommandPut Msg.Value: 2-byte length prefix + id, then 2-byte
// length prefix + name, then peerID verbatim (last field, no prefix
// needed). Distinct from EncodeCommandPayload (the record's stored
// *value*), same reasoning as EncodeGroupPutPayload.
func EncodeCommandPutPayload(id, name string, peerID []byte) ([]byte, error) {
	if len(id) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: command put id too long: %d bytes", len(id))
	}
	if len(name) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: command put name too long: %d bytes", len(name))
	}
	buf := make([]byte, 2+len(id)+2+len(name)+len(peerID))
	buf[0] = byte(len(id) >> 8)
	buf[1] = byte(len(id))
	off := 2
	off += copy(buf[off:], id)
	buf[off] = byte(len(name) >> 8)
	buf[off+1] = byte(len(name))
	off += 2
	off += copy(buf[off:], name)
	copy(buf[off:], peerID)
	return buf, nil
}

// DecodeCommandPutPayload is the inverse of EncodeCommandPutPayload.
func DecodeCommandPutPayload(payload []byte) (id, name string, peerID []byte, err error) {
	if len(payload) < 2 {
		return "", "", nil, fmt.Errorf("shmevent: command put payload too short: %d bytes", len(payload))
	}
	idLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+idLen > len(payload) {
		return "", "", nil, fmt.Errorf("shmevent: command put id length %d exceeds payload size %d", idLen, len(payload))
	}
	id = string(payload[off : off+idLen])
	off += idLen
	if off+2 > len(payload) {
		return "", "", nil, fmt.Errorf("shmevent: command put payload too short for name length")
	}
	nameLen := int(payload[off])<<8 | int(payload[off+1])
	off += 2
	if off+nameLen > len(payload) {
		return "", "", nil, fmt.Errorf("shmevent: command put name length %d exceeds payload size %d", nameLen, len(payload))
	}
	name = string(payload[off : off+nameLen])
	off += nameLen
	return id, name, payload[off:], nil
}

// EncodeGroupCommandPayload packs commandID and groupID into a single
// EventGroupCommandPut/EventGroupCommandDelete Msg.Value: a 2-byte
// big-endian length prefix for commandID, then commandID, then groupID
// verbatim (last field, no prefix needed) -- mirrors GroupCommandKey's own
// field order exactly, so decoding this payload and passing the results
// straight to GroupCommandKey builds the record's key.
func EncodeGroupCommandPayload(commandID, groupID []byte) ([]byte, error) {
	if len(commandID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: group-command commandID too long: %d bytes", len(commandID))
	}
	buf := make([]byte, 2+len(commandID)+len(groupID))
	buf[0] = byte(len(commandID) >> 8)
	buf[1] = byte(len(commandID))
	off := 2
	off += copy(buf[off:], commandID)
	copy(buf[off:], groupID)
	return buf, nil
}

// DecodeGroupCommandPayload is the inverse of EncodeGroupCommandPayload.
func DecodeGroupCommandPayload(payload []byte) (commandID, groupID []byte, err error) {
	if len(payload) < 2 {
		return nil, nil, fmt.Errorf("shmevent: group-command payload too short: %d bytes", len(payload))
	}
	cmdLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+cmdLen > len(payload) {
		return nil, nil, fmt.Errorf("shmevent: group-command commandID length %d exceeds payload size %d", cmdLen, len(payload))
	}
	return payload[off : off+cmdLen], payload[off+cmdLen:], nil
}

// EncodePeerGroupPayload is EncodeGroupCommandPayload's PeerGroup
// counterpart: peerID first (length-prefixed), groupID last.
func EncodePeerGroupPayload(peerID, groupID []byte) ([]byte, error) {
	if len(peerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: peer-group peerID too long: %d bytes", len(peerID))
	}
	buf := make([]byte, 2+len(peerID)+len(groupID))
	buf[0] = byte(len(peerID) >> 8)
	buf[1] = byte(len(peerID))
	off := 2
	off += copy(buf[off:], peerID)
	copy(buf[off:], groupID)
	return buf, nil
}

// DecodePeerGroupPayload is the inverse of EncodePeerGroupPayload.
func DecodePeerGroupPayload(payload []byte) (peerID, groupID []byte, err error) {
	if len(payload) < 2 {
		return nil, nil, fmt.Errorf("shmevent: peer-group payload too short: %d bytes", len(payload))
	}
	idLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+idLen > len(payload) {
		return nil, nil, fmt.Errorf("shmevent: peer-group peerID length %d exceeds payload size %d", idLen, len(payload))
	}
	return payload[off : off+idLen], payload[off+idLen:], nil
}
