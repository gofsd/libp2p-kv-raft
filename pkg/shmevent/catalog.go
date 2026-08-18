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

// StationKey builds the pkg/store key for one KindStation record, keyed by
// the described device's peer id -- same single-trailing-ID-field shape as
// GroupKey/CommandKey, so StationKeyBounds can reuse keyListBounds too.
func StationKey(peerID []byte) []byte {
	return SystemKey(KindStation, catalogStatusPlaceholder, peerID)
}

// StationKeyBounds returns the [lo, hi] range covering every KindStation
// record, for a ScanRange listing.
func StationKeyBounds() (lo, hi []byte) {
	return keyListBounds(KindStation)
}

// EncodeStationPayload packs a Station record's name and attrs into its
// value: a 2-byte big-endian length prefix for name, then name, then attrs
// verbatim (last field, no prefix needed) -- peer id is already the
// record's key (StationKey). attrs is opaque here (JSON, in practice):
// what a station's attributes *mean* is an application's business, and
// pinning a schema in this package would make every new attribute a wire
// change in three languages.
func EncodeStationPayload(name string, attrs []byte) ([]byte, error) {
	if len(name) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: station name too long: %d bytes", len(name))
	}
	buf := make([]byte, 2+len(name)+len(attrs))
	buf[0] = byte(len(name) >> 8)
	buf[1] = byte(len(name))
	off := 2
	off += copy(buf[off:], name)
	copy(buf[off:], attrs)
	return buf, nil
}

// DecodeStationPayload is the inverse of EncodeStationPayload.
func DecodeStationPayload(payload []byte) (name string, attrs []byte, err error) {
	if len(payload) < 2 {
		return "", nil, fmt.Errorf("shmevent: station payload too short: %d bytes", len(payload))
	}
	nameLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+nameLen > len(payload) {
		return "", nil, fmt.Errorf("shmevent: station name length %d exceeds payload size %d", nameLen, len(payload))
	}
	name = string(payload[off : off+nameLen])
	off += nameLen
	return name, payload[off:], nil
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
	if err := checkCommandNameLen(name); err != nil {
		return nil, err
	}
	buf := make([]byte, 2+len(name)+len(peerID))
	buf[0] = byte(len(name) >> 8)
	buf[1] = byte(len(name))
	off := 2
	off += copy(buf[off:], name)
	copy(buf[off:], peerID)
	return buf, nil
}

// commandPayloadV2Sentinel is a name length no v1 payload may carry (0xFFFF),
// used as a version marker in a Command payload's first two bytes: a v1
// payload starts with its name's big-endian length, so any payload beginning
// 0xFF 0xFF is unambiguously v2.
//
// checkCommandNameLen is what makes that true, and it has to be a real check.
// This used to rest on the value ceilings instead -- a 0xFFFF-byte name could
// not fit the 512-byte ValueSize (later 4KB KVValueSize) every Command record
// was capped at, so the length simply could not occur. Those tiers were
// retired for one generous MaxValueSize (see README's "Value size ceilings"),
// which left a 65535-byte name encodable and its v1 payload
// indistinguishable from a v2 one -- it decodes as a v2 record with an empty
// name, an empty peer id, and the whole thing swallowed as a spec. No stored
// record can be in that state (every one of them was written under a ceiling
// far below 0xFFFF), so this is a write-side guard only, with nothing to
// migrate. web-app's Rust mirror (shmevent/catalog_keys.rs) carries the same
// check for the same reason.
//
// This exists because a Command record grew a third field -- Spec, the form
// definition a client renders (see EncodeCommandPayloadWithSpec) -- and v1's
// layout has no room for one: peerID is the trailing field and takes the rest
// of the buffer, so nothing can follow it. Rather than migrate every stored
// record, both formats are readable forever and v1 is still *written*
// whenever there is no spec, which keeps a spec-less command byte-identical
// to what any older reader (including web-app's Rust decoder in
// shmevent/catalog_keys.rs) already understands.
const commandPayloadV2Sentinel = 0xFFFF

// checkCommandNameLen bounds a Command record's name to one byte below
// commandPayloadV2Sentinel -- see that constant's doc comment. Both payload
// writers use it, so whether a name is acceptable never depends on whether a
// spec happens to be present (only the v1 layout can actually collide, but a
// name that encodes with a spec and fails without one would be worse than the
// single shared limit).
func checkCommandNameLen(name string) error {
	if len(name) >= commandPayloadV2Sentinel {
		return fmt.Errorf("shmevent: command name too long: %d bytes (max %d)", len(name), commandPayloadV2Sentinel-1)
	}
	return nil
}

// EncodeCommandPayloadWithSpec is EncodeCommandPayload plus spec, the
// command's form definition -- what a client needs to render inputs for it,
// opaque to this package and to the FSM (JSON, in practice). Emits v1 byte
// -for-byte when spec is empty, so adding this field costs nothing for the
// commands that don't use it.
//
// v2 layout: [0xFF 0xFF][2-byte name len][name][2-byte peerID len][peerID]
// [spec, taking the rest].
func EncodeCommandPayloadWithSpec(name string, peerID, spec []byte) ([]byte, error) {
	if len(spec) == 0 {
		return EncodeCommandPayload(name, peerID)
	}
	if err := checkCommandNameLen(name); err != nil {
		return nil, err
	}
	if len(peerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: command peer id too long: %d bytes", len(peerID))
	}
	buf := make([]byte, 2+2+len(name)+2+len(peerID)+len(spec))
	buf[0], buf[1] = 0xFF, 0xFF
	off := 2
	buf[off] = byte(len(name) >> 8)
	buf[off+1] = byte(len(name))
	off += 2
	off += copy(buf[off:], name)
	buf[off] = byte(len(peerID) >> 8)
	buf[off+1] = byte(len(peerID))
	off += 2
	off += copy(buf[off:], peerID)
	copy(buf[off:], spec)
	return buf, nil
}

// DecodeCommandPayload is the inverse of EncodeCommandPayload, reading either
// format and discarding any spec -- so every caller that only wants the name
// and target peer (dispatch, the ACL checks, the CLI listings) needs no
// change and cannot accidentally depend on a spec being present. Use
// DecodeCommandPayloadFull to read the spec.
func DecodeCommandPayload(payload []byte) (name string, peerID []byte, err error) {
	name, peerID, _, err = DecodeCommandPayloadFull(payload)
	return name, peerID, err
}

// EncodeCommandPayloadClearingSpec writes a v2 payload carrying an
// explicitly *empty* spec -- the one thing EncodeCommandPayloadWithSpec
// cannot express, since it treats an empty spec as "no spec" and falls back
// to v1. The difference matters because a v1 payload now means "leave
// whatever spec is stored alone" (see kvfsm's OpSet case): without this,
// there would be no way to remove a spec once set.
func EncodeCommandPayloadClearingSpec(name string, peerID []byte) ([]byte, error) {
	payload, err := EncodeCommandPayloadWithSpec(name, peerID, []byte{0})
	if err != nil {
		return nil, err
	}
	return payload[:len(payload)-1], nil
}

// CommandPutPayloadHasSpec reports whether an EventCommandPut payload
// carries a spec field at all, the put-payload counterpart of
// DecodeCommandPayloadSpec's `present`. pkg/daemon needs it to tell a put
// that simply didn't mention the spec (leave the stored one alone) from one
// that carried an empty spec deliberately (clear it) -- a distinction that
// disappears if the two are re-encoded through the same helper.
func CommandPutPayloadHasSpec(payload []byte) bool {
	return len(payload) >= 2 && int(payload[0])<<8|int(payload[1]) == commandPayloadV2Sentinel
}

// DecodeCommandPayloadSpec reports whether payload carries a spec field at
// all, distinct from carrying an empty one. A v1 payload has no field
// (present is false); a v2 payload always has one, even when it's
// zero-length. kvfsm's OpSet case needs exactly this distinction to tell
// "the caller didn't mention the spec" from "the caller cleared it".
func DecodeCommandPayloadSpec(payload []byte) (spec []byte, present bool, err error) {
	if len(payload) < 2 {
		return nil, false, fmt.Errorf("shmevent: command payload too short: %d bytes", len(payload))
	}
	if int(payload[0])<<8|int(payload[1]) != commandPayloadV2Sentinel {
		return nil, false, nil
	}
	_, _, spec, err = DecodeCommandPayloadFull(payload)
	if err != nil {
		return nil, false, err
	}
	return spec, true, nil
}

// DecodeCommandPayloadFull decodes either payload format, returning the spec
// as well (nil for a v1 record, which has none).
func DecodeCommandPayloadFull(payload []byte) (name string, peerID, spec []byte, err error) {
	if len(payload) < 2 {
		return "", nil, nil, fmt.Errorf("shmevent: command payload too short: %d bytes", len(payload))
	}
	if int(payload[0])<<8|int(payload[1]) != commandPayloadV2Sentinel {
		nameLen := int(payload[0])<<8 | int(payload[1])
		off := 2
		if off+nameLen > len(payload) {
			return "", nil, nil, fmt.Errorf("shmevent: command name length %d exceeds payload size %d", nameLen, len(payload))
		}
		name = string(payload[off : off+nameLen])
		off += nameLen
		return name, payload[off:], nil, nil
	}

	off := 2
	if off+2 > len(payload) {
		return "", nil, nil, fmt.Errorf("shmevent: command payload truncated reading name length")
	}
	nameLen := int(payload[off])<<8 | int(payload[off+1])
	off += 2
	if off+nameLen+2 > len(payload) {
		return "", nil, nil, fmt.Errorf("shmevent: command name length %d exceeds payload size %d", nameLen, len(payload))
	}
	name = string(payload[off : off+nameLen])
	off += nameLen
	peerLen := int(payload[off])<<8 | int(payload[off+1])
	off += 2
	if off+peerLen > len(payload) {
		return "", nil, nil, fmt.Errorf("shmevent: command peer id length %d exceeds payload size %d", peerLen, len(payload))
	}
	peerID = payload[off : off+peerLen]
	off += peerLen
	return name, peerID, payload[off:], nil
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
// ReservedGroupChannel, ReservedGroupRelay, and ReservedGroupRemote are the
// deliberate exception to that last rule: their Group records are equally
// protected, but their PeerGroup membership remains an ordinary,
// operator-editable grant (mage addpeertogroup/removepeerfromgroup) -- the
// mechanism for letting a peer that isn't a cluster member use a raw
// Channel, this cluster's relay service, or the generic remote Set/Get/etc.
// RPC surface, anyway. pkg/daemon's handleChannelStream/relayACL/
// handleShmEvent each accept access from any peer in ReservedGroupCluster or
// their own respective group (or a pairwise personal grant -- see
// isPeerIdentityGroupID in pkg/daemon) and reject everyone else, identically
// -- see pkg/daemon's isAuthorizedForGatedAccess. ReservedGroupRemote has no
// opt-out Config flag, same as Channel/Relay: a non-member remote caller's
// only other door into a cluster it doesn't belong to is the narrow,
// always-on SubmitCommand-plus-its-own-log-readback carve-out (see
// pkg/daemon's isCommandLogCarveOut) -- a public command's own execution
// logic can use addpeertogroup to grant a stranger into remote/channel/relay
// from there, but nothing does so automatically.
//
// ReservedGroupExecute is kept reserved (its Group record still protected,
// still auto-created by ensureReservedGroups, and old PeerGroup grants into
// it from before this changed still replicate harmlessly) but is no longer
// consulted for authorization: pkg/daemon's handleExecuteStream gates
// EventExecute delivery on current ReservedGroupCluster membership alone,
// not on isAuthorizedForGatedAccess, so a PeerGroup grant into
// ReservedGroupExecute (or a personal grant) no longer admits a non-member
// sender the way it once did.
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

// NewGroupPut builds a groupPut Msg: Group catalog upsert.
func NewGroupPut(id, name string, public bool) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGroupPut()
	grp := m.GroupPut()
	if err := grp.SetId(id); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_group_put: %w", err)
	}
	if err := grp.SetName(name); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_group_put: %w", err)
	}
	grp.SetPublic(public)
	return m, nil
}

// NewGroupDelete builds a groupDelete Msg: Group catalog delete.
func NewGroupDelete(id string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGroupDelete()
	if err := m.GroupDelete().SetId(id); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_group_delete: %w", err)
	}
	return m, nil
}

// NewCommandPut builds a commandPut Msg leaving any existing spec
// unchanged (HasSpec() false on the result) -- use NewCommandPutWithSpec
// to set or clear one.
func NewCommandPut(id, name, peerID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetCommandPut()
	grp := m.CommandPut()
	if err := grp.SetId(id); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_command_put: %w", err)
	}
	if err := grp.SetName(name); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_command_put: %w", err)
	}
	if err := grp.SetPeerId(peerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_command_put: %w", err)
	}
	return m, nil
}

// NewCommandPutWithSpec builds a commandPut Msg carrying spec explicitly
// (HasSpec() true on the result, even when spec == "") -- the only way to
// clear an existing spec, since NewCommandPut leaves it untouched. spec is
// caller-defined opaque JSON (the command's form definition).
func NewCommandPutWithSpec(id, name, peerID, spec string) (Msg, error) {
	if err := checkValueSizeStr(spec); err != nil {
		return Msg{}, err
	}
	m, err := NewCommandPut(id, name, peerID)
	if err != nil {
		return Msg{}, err
	}
	if err := m.CommandPut().SetSpec(spec); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_command_put_with_spec: %w", err)
	}
	return m, nil
}

// NewCommandDelete builds a commandDelete Msg: Command catalog delete.
func NewCommandDelete(id string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetCommandDelete()
	if err := m.CommandDelete().SetId(id); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_command_delete: %w", err)
	}
	return m, nil
}

// NewStationPut builds a stationPut Msg: Station catalog upsert. attrs is
// caller-defined opaque JSON.
func NewStationPut(peerID, name, attrs string) (Msg, error) {
	if err := checkValueSizeStr(attrs); err != nil {
		return Msg{}, err
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetStationPut()
	grp := m.StationPut()
	if err := grp.SetPeerId(peerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_station_put: %w", err)
	}
	if err := grp.SetName(name); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_station_put: %w", err)
	}
	if err := grp.SetAttrs(attrs); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_station_put: %w", err)
	}
	return m, nil
}

// NewStationDelete builds a stationDelete Msg: Station catalog delete.
func NewStationDelete(peerID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetStationDelete()
	if err := m.StationDelete().SetPeerId(peerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_station_delete: %w", err)
	}
	return m, nil
}

// NewGroupCommandPut builds a groupCommandPut Msg: links commandID into
// groupID.
func NewGroupCommandPut(commandID, groupID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGroupCommandPut()
	grp := m.GroupCommandPut()
	if err := grp.SetCommandId(commandID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_group_command_put: %w", err)
	}
	if err := grp.SetGroupId(groupID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_group_command_put: %w", err)
	}
	return m, nil
}

// NewGroupCommandDelete builds a groupCommandDelete Msg: unlinks
// commandID from groupID.
func NewGroupCommandDelete(commandID, groupID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGroupCommandDelete()
	grp := m.GroupCommandDelete()
	if err := grp.SetCommandId(commandID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_group_command_delete: %w", err)
	}
	if err := grp.SetGroupId(groupID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_group_command_delete: %w", err)
	}
	return m, nil
}

// NewPeerGroupPut builds a peerGroupPut Msg: adds peerID to groupID.
func NewPeerGroupPut(peerID, groupID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetPeerGroupPut()
	grp := m.PeerGroupPut()
	if err := grp.SetPeerId(peerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_peer_group_put: %w", err)
	}
	if err := grp.SetGroupId(groupID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_peer_group_put: %w", err)
	}
	return m, nil
}

// NewPeerGroupDelete builds a peerGroupDelete Msg: removes peerID from
// groupID.
func NewPeerGroupDelete(peerID, groupID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetPeerGroupDelete()
	grp := m.PeerGroupDelete()
	if err := grp.SetPeerId(peerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_peer_group_delete: %w", err)
	}
	if err := grp.SetGroupId(groupID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_peer_group_delete: %w", err)
	}
	return m, nil
}
