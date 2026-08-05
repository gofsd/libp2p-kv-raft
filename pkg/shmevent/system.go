package shmevent

import (
	"fmt"
	"time"
)

// SystemKeyPrefix marks a pkg/store key as reserved for internal cluster
// bookkeeping -- permitted peers, bootstrap nodes, and (planned, not yet
// implemented) raft voter/learner add/remove operations -- rather than
// user data. pkg/daemon's EventSetField/EventSet cases reject any
// caller-supplied key starting with this byte, reserving the entire
// namespace for SystemKey's own use.
const SystemKeyPrefix = 0x00

// Kind bytes -- what a system record (see SystemKey) is about. 0x01 and
// 0x04 were formerly KindPermitPeer/KindLogPermit -- permission records
// for a peer to use the generic remote RPC surface, Execute, relay, or a
// specific pkg/logrecord kind -- now unassigned again: that whole family
// of gates was folded into the unconditional group-ACL mechanism
// pkg/daemon's Channel/relay gates already used (see
// shmevent.ReservedGroupRemote/ReservedGroupExecute/ReservedGroupRelay/
// ReservedGroupChannel and pkg/daemon's isAuthorizedForGatedAccess/
// isCommandLogCarveOut), which needs no permit record at all. Values 0x0D
// and above are still unassigned, reserved for future system operations.
const (
	// KindBootstrapNode registers a stable relay/bootstrap point: a known
	// circuit-relay v2 server's multiaddr, plus a failover priority (see
	// EncodeBootstrapNodeMetadata/DecodeBootstrapNodeMetadata in
	// bootstrapnode.go), that pkg/daemon's own newHost draws its relay
	// candidate list from (relayCandidates) alongside any -relay-peer/
	// Config.RelayPeers seed values -- so a node picks up newly
	// registered relays without a restart, and EnableAutoRelayWithStaticRelays
	// can fail over between candidates if one goes down. Two-stage
	// request/confirm/revoke lifecycle (create/activate/delete), the same
	// EventPermitRequest/EventPermitConfirm/EventPermitRevoke machinery
	// KindClusterJoin below also reuses; BootstrapNodeKeyBounds is this
	// kind's read side, for a ListRange scan of every currently-confirmed
	// entry.
	KindBootstrapNode byte = 0x02
	// KindClusterMember records a raft member's public key and current
	// role (RoleVoter/RoleLearner/RoleLeader) -- see ClusterMemberKey/
	// EncodeClusterMemberPayload. Unlike KindBootstrapNode it has no
	// pending/confirmed two-stage lifecycle: it's a live status mirror,
	// always written directly under a fixed status placeholder (see
	// ClusterMemberKey), kept current by pkg/daemon whenever a peer joins
	// or this node's own raft leadership status changes.
	KindClusterMember byte = 0x03
	// KindClusterJoin consumes the 0x05 slot this block's own doc comment
	// reserved for exactly this: a raft voter/learner add, built on the
	// same EventPermitRequest/EventPermitConfirm/EventPermitRevoke
	// pending->confirmed lifecycle as KindBootstrapNode, rather than a new
	// wire protocol. metadata (see EncodeClusterJoinMetadata) carries the
	// joining peer's dialable multiaddr and requested suffrage; a
	// confirm promotes the pending record to confirmed exactly like any
	// other kind, but pkg/daemon's applyConfirm additionally special-
	// cases this one kind to actually call raft.AddVoter/AddNonvoter at
	// that moment (see applyConfirm's doc comment) -- everywhere else in
	// this package and pkg/kvfsm, KindClusterJoin is opaque, handled
	// identically to any other kind.
	KindClusterJoin byte = 0x05
	// KindGroup, KindCommand, KindGroupCommand, and KindPeerGroup
	// implement the group-based ACL catalog (see pkg/kvctl/catalog.go's
	// package doc comment): KindGroup/KindCommand are direct records
	// (GroupKey/CommandKey, one variable ID field, same shape as
	// ClusterMemberKey); KindGroupCommand/KindPeerGroup are many-to-many
	// relation records with no pending/confirmed lifecycle of their own
	// (GroupCommandKey/PeerGroupKey, two variable fields). Unlike
	// KindBootstrapNode/KindClusterJoin, every one of these four is
	// written directly (kvfsm.OpSet/OpDel) by any single current raft
	// voter -- no separate confirmation step -- reusing the existing
	// EventPermitConfirm/EventPermitRevoke voter-gated forwarding
	// machinery (see pkg/daemon's handleConfirmForward) widened to also
	// accept OpSet, rather than the two-stage pending->confirmed pattern
	// every other client-facing kind above uses.
	KindGroup        byte = 0x06
	KindCommand      byte = 0x07
	KindGroupCommand byte = 0x08
	KindPeerGroup    byte = 0x09
	// KindJoinInvite is a one-time raft-join invite: unlike KindClusterJoin
	// (always addressed to a peer id the cluster already knows about
	// before it's redeemed -- either because a live voter is confirming a
	// request that already landed, or a ticket was pre-signed for that
	// specific known peer id), a KindJoinInvite record is keyed by a random
	// token instead of any peer id at all, because the whole point is
	// admitting a device the cluster has never seen before -- there is no
	// peer id to address it to until the moment it shows up and presents
	// the token. Written directly (kvfsm.OpSet), same as KindGroup/
	// KindCommand -- no pending/confirmed lifecycle, since there's nothing
	// to promote; a single current raft voter creating one is the entire
	// authorization step. Redeeming one is different from every other
	// kind's confirm/delete pattern too: kvfsm.OpConsumeInvite reads and
	// deletes it in one atomic Apply (see that op's doc comment), and a
	// join request presenting a still-valid token is admitted immediately
	// -- raft.AddVoter/AddNonvoter -- even when Config.RequireConfirmForJoin
	// is set, which is the entire feature: no live voter needs to be
	// present at the moment the new device actually shows up.
	KindJoinInvite byte = 0x0A
	// KindExecInvite is a one-time execution invite: a raft voter binds a
	// commandID+inputsJSON pair (see pkg/kvctl/dispatch.go's SubmitCommand
	// for what those mean) to a random token, exactly the way
	// KindJoinInvite binds a suffrage grant to one -- keyed by the token
	// alone, written directly (kvfsm.OpSet, same voter-gated
	// handleConfirmForward path KindJoinInvite/KindGroup use), no
	// pending/confirmed lifecycle. Unlike KindJoinInvite, redeeming one
	// (kvfsm.OpConsumeExecInvite) also re-checks the redeeming peer's real
	// Group/Command/PeerGroup ACL standing in the same atomic Apply before
	// deleting it -- see that op's doc comment -- so an unauthorized
	// redemption attempt is rejected without consuming the invite, letting
	// a legitimate peer still redeem it later.
	KindExecInvite byte = 0x0B
	// KindStation describes one device in operational terms -- a human name
	// and whatever attributes an application cares about (location, model,
	// capabilities), as an opaque payload this package never parses --
	// keyed by that device's peer id. It exists because every other record
	// naming a device names it by peer id alone, which is a 52-character
	// base58 string that tells a person nothing: a Command's target, a
	// PeerGroup's member and a CommandRequest's executor are all unreadable
	// without somewhere to look up "which machine is that". This is that
	// somewhere.
	//
	// Deliberately *not* the cluster's membership list: KindClusterMember
	// already records who is in the cluster, replicated by the join path,
	// and a station record neither grants membership nor implies it. A
	// device can be described before it joins (staging a shop floor) and
	// keep its description after it leaves (so historical records still
	// name it). Written through the same voter-gated catalog CRUD path as
	// KindGroup/KindCommand, so a device cannot rename itself.
	KindStation byte = 0x0C
)

// KindName returns a human-readable name for k, for CLI use (mage/
// kvctl-cli's requestpermit/confirmpermit commands) -- "unknown(N)" for
// anything not defined above. Mirrors EventName/EventFromName.
func KindName(k byte) string {
	switch k {
	case KindBootstrapNode:
		return "bootstrap"
	case KindClusterMember:
		return "cluster-member"
	case KindClusterJoin:
		return "cluster-join"
	case KindGroup:
		return "group"
	case KindCommand:
		return "command"
	case KindGroupCommand:
		return "group-command"
	case KindPeerGroup:
		return "peer-group"
	case KindJoinInvite:
		return "join-invite"
	case KindExecInvite:
		return "exec-invite"
	case KindStation:
		return "station"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}

// KindFromName is the inverse of KindName: it returns the kind byte for
// one of the names KindName produces ("bootstrap", "cluster-member",
// "cluster-join"), and false if name isn't recognized.
func KindFromName(name string) (byte, bool) {
	switch name {
	case "bootstrap":
		return KindBootstrapNode, true
	case "cluster-member":
		return KindClusterMember, true
	case "cluster-join":
		return KindClusterJoin, true
	case "group":
		return KindGroup, true
	case "command":
		return KindCommand, true
	case "group-command":
		return KindGroupCommand, true
	case "peer-group":
		return KindPeerGroup, true
	case "station":
		return KindStation, true
	case "join-invite":
		return KindJoinInvite, true
	case "exec-invite":
		return KindExecInvite, true
	default:
		return 0, false
	}
}

// Role bytes -- a raft member's current standing, as recorded in a
// KindClusterMember record's payload (see EncodeClusterMemberPayload).
const (
	RoleVoter   byte = 0x01
	RoleLearner byte = 0x02
	RoleLeader  byte = 0x03
)

// RoleName returns a human-readable name for a KindClusterMember role byte
// -- "unknown(N)" for anything else. Mirrors KindName/EventName, for CLI
// use (mage/kvctl-cli's listnodes command).
func RoleName(role byte) string {
	switch role {
	case RoleVoter:
		return "voter"
	case RoleLearner:
		return "learner"
	case RoleLeader:
		return "leader"
	default:
		return fmt.Sprintf("unknown(%d)", role)
	}
}

// Status bytes -- where a system record is in its two-stage
// request/confirm lifecycle (see SystemKey).
const (
	StatusPending   byte = 0x01
	StatusConfirmed byte = 0x02
)

// Suffrage bytes -- the raft membership shape a KindClusterJoin request
// asks for, packed into EncodeClusterJoinMetadata's payload. Mirrors
// pkg/daemon's parseJoinRequest "voter"/"learner" wire tokens (raft.Voter/
// raft.Nonvoter) in a form this package can encode without importing
// hashicorp/raft.
const (
	SuffrageVoter   byte = 0x01
	SuffrageLearner byte = 0x02
)

// EncodeClusterJoinMetadata packs a joining peer's dialable multiaddr and
// requested suffrage into the metadata argument EventPermitRequest expects
// (see EncodePermitRequestPayload) for a KindClusterJoin request: suffrage
// byte first (fixed size), then addr verbatim as the rest of the buffer --
// no length prefix needed since addr is always last.
func EncodeClusterJoinMetadata(addr string, suffrage byte) []byte {
	buf := make([]byte, 1+len(addr))
	buf[0] = suffrage
	copy(buf[1:], addr)
	return buf
}

// DecodeClusterJoinMetadata is the inverse of EncodeClusterJoinMetadata.
func DecodeClusterJoinMetadata(payload []byte) (addr string, suffrage byte, err error) {
	if len(payload) < 1 {
		return "", 0, fmt.Errorf("shmevent: cluster join metadata too short: %d bytes", len(payload))
	}
	return string(payload[1:]), payload[0], nil
}

// SystemKey builds the pkg/store key for a system record:
// SystemKeyPrefix, kind, status, then peerID verbatim. peerID is always
// the last field, so its length needs no prefix here (contrast
// EncodePermitRequestPayload, where something follows it).
func SystemKey(kind, status byte, peerID []byte) []byte {
	key := make([]byte, 3+len(peerID))
	key[0] = SystemKeyPrefix
	key[1] = kind
	key[2] = status
	copy(key[3:], peerID)
	return key
}

// clusterMemberStatusPlaceholder is the fixed status byte ClusterMemberKey
// uses -- KindClusterMember has no pending/confirmed lifecycle (see that
// constant's doc comment), so unlike SystemKey's other callers this isn't
// StatusPending/StatusConfirmed, just a placeholder keeping the key layout
// uniform with every other SystemKey-produced key.
const clusterMemberStatusPlaceholder = 0x00

// ClusterMemberKey builds the pkg/store key for peerID's KindClusterMember
// record -- see that constant's doc comment.
func ClusterMemberKey(peerID []byte) []byte {
	return SystemKey(KindClusterMember, clusterMemberStatusPlaceholder, peerID)
}

// ClusterMemberKeyBounds returns the [lo, hi] key range covering every
// currently-stored KindClusterMember record -- the enumeration counterpart
// to ClusterMemberKey's single-record lookup, for a raw range scan (see
// pkg/shmclient.Session.ListRange/EventListRange). hi pads well past any
// real peer id's byte length (base58-encoded Ed25519 peer ids run well
// under 64 bytes), mirroring pkg/kvctl's own kindPrefixBounds pattern for
// logrecord kinds.
func ClusterMemberKeyBounds() (lo, hi []byte) {
	prefix := SystemKey(KindClusterMember, clusterMemberStatusPlaceholder, nil)
	lo = prefix
	hi = make([]byte, len(prefix)+64)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi
}

// EncodeClusterMemberPayload packs pub and role into a single
// KindClusterMember record's value: pub is always exactly
// ed25519.PublicKeySize (32) bytes, role is the trailing byte -- both
// fixed-size, so unlike EncodePermitRequestPayload no length prefix is
// needed.
func EncodeClusterMemberPayload(pub PublicKey, role byte) []byte {
	buf := make([]byte, len(pub)+1)
	copy(buf, pub)
	buf[len(pub)] = role
	return buf
}

// DecodeClusterMemberPayload is the inverse of EncodeClusterMemberPayload.
func DecodeClusterMemberPayload(payload []byte) (pub PublicKey, role byte, err error) {
	if len(payload) != PublicKeySize+1 {
		return nil, 0, fmt.Errorf("shmevent: cluster member payload must be %d bytes, got %d", PublicKeySize+1, len(payload))
	}
	pub = PublicKey(payload[:PublicKeySize])
	role = payload[PublicKeySize]
	return pub, role, nil
}

// EncodePermitRequestPayload packs kind, peerID, and metadata into a
// single EventPermitRequest Msg.Value: kind, then a 2-byte big-endian
// length prefix for peerID, then peerID, then metadata verbatim (the
// rest of the buffer, needing no length prefix of its own). metadata is
// opaque to this package -- e.g. a dialable multiaddr for
// KindBootstrapNode.
func EncodePermitRequestPayload(kind byte, peerID, metadata []byte) ([]byte, error) {
	if len(peerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: permit request peerID too long: %d bytes", len(peerID))
	}
	buf := make([]byte, 1+2+len(peerID)+len(metadata))
	buf[0] = kind
	buf[1] = byte(len(peerID) >> 8)
	buf[2] = byte(len(peerID))
	copy(buf[3:], peerID)
	copy(buf[3+len(peerID):], metadata)
	return buf, nil
}

// DecodePermitRequestPayload is the inverse of EncodePermitRequestPayload.
func DecodePermitRequestPayload(payload []byte) (kind byte, peerID, metadata []byte, err error) {
	if len(payload) < 3 {
		return 0, nil, nil, fmt.Errorf("shmevent: permit request payload too short: %d bytes", len(payload))
	}
	kind = payload[0]
	idLen := int(payload[1])<<8 | int(payload[2])
	if 3+idLen > len(payload) {
		return 0, nil, nil, fmt.Errorf("shmevent: permit request peerID length %d exceeds payload size %d", idLen, len(payload))
	}
	return kind, payload[3 : 3+idLen], payload[3+idLen:], nil
}

// RelayLimits is the per-peer circuit-relay v2 resource allotment
// pkg/daemon's newHost hands go-libp2p as v2relay.Resources.MaxCircuits/
// Limit.Data/Limit.Duration/MaxReservationsPerIP/MaxReservationsPerPeer --
// not a per-peer override: go-libp2p's circuitv2 relay applies a single
// Resources value to every ACL-approved peer alike (there is no hook to
// give one peer a bigger allotment than another without forking that
// package), so every peer currently gets the same node-configured values
// here.
type RelayLimits struct {
	MaxCircuitsPerPeer     int32         // concurrent open relayed circuits this peer may hold
	LimitData              int64         // bytes relayed (each direction) before a circuit is reset
	LimitDuration          time.Duration // wall-clock life of a circuit before it's reset
	MaxReservationsPerIP   int32         // active relay-slot reservations allowed from one IP
	MaxReservationsPerPeer int32         // active relay-slot reservations allowed from one peer
}

// Default relay resource values -- pkg/daemon's Config fields fall back to
// these when left at their zero value (see Config.RelayMaxCircuitsPerPeer
// et al.'s doc comments). One concurrent circuit and one reservation per
// peer/IP is deliberately tight (a single-purpose kv-raft client has no
// legitimate need for more); the 1GB/30-day circuit ceiling is loose
// enough not to interrupt a long-lived, low-traffic follower's relayed
// connection under normal operation while still forcing an eventual
// reset.
const (
	DefaultRelayMaxCircuitsPerPeer     int32         = 1
	DefaultRelayLimitData              int64         = 1 << 30 // 1 GB
	DefaultRelayLimitDuration          time.Duration = 30 * 24 * time.Hour
	DefaultRelayMaxReservationsPerIP   int32         = 5
	DefaultRelayMaxReservationsPerPeer int32         = 1
)

// DefaultRelayLimits returns the RelayLimits built from the Default* values
// above.
func DefaultRelayLimits() RelayLimits {
	return RelayLimits{
		MaxCircuitsPerPeer:     DefaultRelayMaxCircuitsPerPeer,
		LimitData:              DefaultRelayLimitData,
		LimitDuration:          DefaultRelayLimitDuration,
		MaxReservationsPerIP:   DefaultRelayMaxReservationsPerIP,
		MaxReservationsPerPeer: DefaultRelayMaxReservationsPerPeer,
	}
}

// EncodePermitConfirmPayload packs kind and peerID (the rest of the
// buffer) into a single EventPermitConfirm Msg.Value -- no metadata
// field, since the daemon reads the pending request's own value back out
// of the store rather than trusting the confirming caller to resend it.
func EncodePermitConfirmPayload(kind byte, peerID []byte) []byte {
	buf := make([]byte, 1+len(peerID))
	buf[0] = kind
	copy(buf[1:], peerID)
	return buf
}

// DecodePermitConfirmPayload is the inverse of EncodePermitConfirmPayload.
func DecodePermitConfirmPayload(payload []byte) (kind byte, peerID []byte, err error) {
	if len(payload) < 1 {
		return 0, nil, fmt.Errorf("shmevent: permit confirm payload too short")
	}
	return payload[0], payload[1:], nil
}
