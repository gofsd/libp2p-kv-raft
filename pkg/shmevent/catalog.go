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

// groupPublicByte/groupPrivateByte are EncodeGroupPayload's leading public
// flag -- a fixed-size byte, same idiom this package already uses for
// RoleVoter/RoleLearner and SuffrageVoter/SuffrageLearner, rather than a
// raw literal 0/1 at each call site.
const (
	groupPrivateByte byte = 0x00
	groupPublicByte  byte = 0x01
)

// EncodeGroupPayload packs a Group record's public flag and name into its
// value: public first (fixed size, so it needs no length prefix), then
// name verbatim (last field, no prefix needed) -- id is already the
// record's key (GroupKey), so only these two need to travel in the
// payload. public being true is what lets isPermittedForCommand admit any
// peer to commands linked to this group with no PeerGroup membership
// record at all -- see that function's doc comment.
func EncodeGroupPayload(name string, public bool) []byte {
	buf := make([]byte, 1+len(name))
	if public {
		buf[0] = groupPublicByte
	} else {
		buf[0] = groupPrivateByte
	}
	copy(buf[1:], name)
	return buf
}

// DecodeGroupPayload is the inverse of EncodeGroupPayload.
func DecodeGroupPayload(payload []byte) (name string, public bool, err error) {
	if len(payload) < 1 {
		return "", false, fmt.Errorf("shmevent: group payload must be at least 1 byte, got %d", len(payload))
	}
	return string(payload[1:]), payload[0] == groupPublicByte, nil
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

// EncodeGroupPutPayload packs public, id, and name into a single
// EventGroupPut Msg.Value: public first (fixed size), then a 2-byte
// big-endian length prefix for id, then id, then name verbatim (last
// field, no prefix needed). Distinct from EncodeGroupPayload (the record's
// stored *value*, keyed separately by GroupKey(id)): this is the
// wire-level request, which needs all three fields in one message.
func EncodeGroupPutPayload(id, name string, public bool) ([]byte, error) {
	if len(id) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: group put id too long: %d bytes", len(id))
	}
	buf := make([]byte, 1+2+len(id)+len(name))
	if public {
		buf[0] = groupPublicByte
	} else {
		buf[0] = groupPrivateByte
	}
	buf[1] = byte(len(id) >> 8)
	buf[2] = byte(len(id))
	off := 3
	off += copy(buf[off:], id)
	copy(buf[off:], name)
	return buf, nil
}

// DecodeGroupPutPayload is the inverse of EncodeGroupPutPayload.
func DecodeGroupPutPayload(payload []byte) (id, name string, public bool, err error) {
	if len(payload) < 3 {
		return "", "", false, fmt.Errorf("shmevent: group put payload too short: %d bytes", len(payload))
	}
	public = payload[0] == groupPublicByte
	idLen := int(payload[1])<<8 | int(payload[2])
	off := 3
	if off+idLen > len(payload) {
		return "", "", false, fmt.Errorf("shmevent: group put id length %d exceeds payload size %d", idLen, len(payload))
	}
	id = string(payload[off : off+idLen])
	off += idLen
	return id, string(payload[off:]), public, nil
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

// Reserved Group ids that pkg/daemon creates once, at cluster bootstrap,
// and keeps current automatically from then on -- see that package's
// syncMemberGroups/clearMemberGroups/ensureReservedGroups. Every current
// raft voter or learner belongs to ReservedGroupCluster, and to exactly
// one of ReservedGroupVoter/ReservedGroupLearner matching its current
// suffrage (a raft leader counts as a voter for this purpose); a peer that
// leaves or is kicked is removed from all three. Their own Group records
// (id/name/public) are protected -- pkg/daemon's EventGroupPut/
// EventGroupDelete reject any attempt to create, rename, or delete one of
// these four ids -- and PeerGroup membership in the first three is
// likewise daemon-managed only: EventPeerGroupPut/EventPeerGroupDelete
// reject a manual edit against ReservedGroupCluster/Voter/Learner (see
// IsAutoManagedGroupID).
//
// ReservedGroupChannel, ReservedGroupRelay, ReservedGroupRemote, and
// ReservedGroupExecute are the deliberate exception to that last rule:
// their Group records are equally protected, but their PeerGroup
// membership remains an ordinary, operator-editable grant (mage
// addpeertogroup/removepeerfromgroup) -- the mechanism for letting a peer
// that isn't a cluster member use a raw Channel, this cluster's relay
// service, the generic remote Set/Get/etc. RPC surface, or Execute
// delivery, anyway. pkg/daemon's handleChannelStream/relayACL/
// handleShmEvent/handleExecuteStream each accept access from any peer in
// ReservedGroupCluster or their own respective group (or a pairwise
// personal grant -- see isPeerIdentityGroupID in pkg/daemon) and reject
// everyone else, identically -- see pkg/daemon's isAuthorizedForGatedAccess.
// ReservedGroupRemote/ReservedGroupExecute have no opt-out Config flag,
// same as Channel/Relay: a non-member remote caller's only other door
// into a cluster it doesn't belong to is the narrow, always-on
// SubmitCommand-plus-its-own-log-readback carve-out (see pkg/daemon's
// isCommandLogCarveOut) -- a public command's own execution logic can use
// addpeertogroup to grant a stranger into remote/execute/channel/relay
// from there, but nothing does so automatically.
const (
	ReservedGroupCluster = "cluster"
	ReservedGroupVoter   = "voter"
	ReservedGroupLearner = "learner"
	ReservedGroupChannel = "channel"
	ReservedGroupRelay   = "relay"
	ReservedGroupRemote  = "remote"
	ReservedGroupExecute = "execute"
)

// IsReservedGroupID reports whether id names one of the seven reserved
// groups above -- used to reject EventGroupPut/EventGroupDelete against
// any of them, since their Group records are daemon-managed, created once
// at cluster bootstrap (see pkg/daemon's ensureReservedGroups).
func IsReservedGroupID(id string) bool {
	switch id {
	case ReservedGroupCluster, ReservedGroupVoter, ReservedGroupLearner, ReservedGroupChannel, ReservedGroupRelay, ReservedGroupRemote, ReservedGroupExecute:
		return true
	default:
		return false
	}
}

// IsAutoManagedGroupID reports whether id is one of the three reserved
// groups whose *membership* (not just their Group record) is exclusively
// daemon-managed -- ReservedGroupChannel/ReservedGroupRelay are
// deliberately excluded, see their own doc comment above.
func IsAutoManagedGroupID(id string) bool {
	switch id {
	case ReservedGroupCluster, ReservedGroupVoter, ReservedGroupLearner:
		return true
	default:
		return false
	}
}

// DefaultPublicGroupID/DefaultPublicCommandID name the Group+Command
// pkg/daemon.ensureDefaultPublicCommand creates once, at the same moment
// ensureReservedGroups creates the seven reserved groups above -- a
// ready-made, always-public front door into an otherwise closed cluster.
// Any peer at all, including one with no standing whatsoever, may submit
// this exact command (SubmitCommand, gated by the identical
// IsPermittedForCommand check every command uses, trivially satisfied
// here since DefaultPublicGroupID's own Public flag is true and
// DefaultPublicCommandID is linked to it via GroupCommand) -- and
// pkg/kvfsm.Apply's OpAppendCommandRequest case special-cases this
// specific commandID: submitting it doesn't just log a request, it also
// grants the submitting peer real ReservedGroupChannel and
// ReservedGroupRelay access, atomically, in the same raft-committed
// write (see kvfsm's grantChannelRelayAccess). This is the concrete
// escalation path pkg/daemon's isCommandLogCarveOut doc comment already
// describes: the one thing a completely unknown peer may do to an
// otherwise closed cluster is submit this one command, and doing so is
// itself what grants it more.
//
// Unlike the seven groups above, this Group/Command pair is deliberately
// NOT reserved/protected -- IsReservedGroupID doesn't include
// DefaultPublicGroupID: it's a sensible starting default, not permanent
// infrastructure. An operator who wants to close open self-service
// enrollment can deletegroup/updategroup it (public=false) or unlink it
// from the command via removecommandfromgroup, the same way they already
// control every other ACL decision in this catalog -- voters already
// have full authority over it, same as anything else here.
const (
	DefaultPublicGroupID   = "public"
	DefaultPublicCommandID = "public-access"
)
