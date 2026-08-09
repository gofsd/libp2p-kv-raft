// Package shmevent implements the single wire structure (see
// api/shmevent.capnp) used for every message exchanged between a raft node
// instance and a local "user" -- the desktop CLI, the in-process Android
// UI, or a browser tab's main thread -- over shmring shared memory, and
// (per the same struct's remote counterpart) over
// pkg/daemon.ClientProtocolID. It replaces pkg/ipcproto.
//
// See api/shmevent.capnp's doc comment for the full design rationale: a
// real capnp union, one variant per logical operation, each carrying only
// the named/typed fields that operation needs, replacing an earlier design
// where every operation shared one raw `value` field and hand-packed its
// own binary layout inside it.
package shmevent

import (
	"fmt"

	capnp "capnproto.org/go/capnp/v3"
)

// Msg is an alias for the generated capnp Event struct -- there is no
// separate hand-maintained mirror struct: constructors (NewSetKey,
// NewDialSubmitCommand, ...) build one directly, and callers read a
// decoded Msg's fields with the generated per-variant accessors
// (m.SetKey().Value(), m.DialSubmitCommand().TargetPeer(), ...) gated by
// m.Which(). Encode/Decode below are the only place CRC/signature
// computation happens.
type Msg = Event

// EventName returns a human-readable name for w, for logging -- "unknown"
// for anything not defined in api/shmevent.capnp. Kept as a hand-written
// table (rather than Event_Which's own generated String(), which uses
// camelCase variant names) so this project's established snake_case
// convention for event names -- used in test/e2e/testdata.json,
// kvctl-cli's sendevent JSON, and log output -- doesn't depend on
// capnpc-go's own naming choices.
func EventName(w Event_Which) string {
	switch w {
	case Event_Which_setKey:
		return "set_key"
	case Event_Which_setField:
		return "set_field"
	case Event_Which_getKey:
		return "get_key"
	case Event_Which_getFieldByRegistry:
		return "get_field_by_registry"
	case Event_Which_getFieldByKey:
		return "get_field_by_key"
	case Event_Which_getPublicKey:
		return "get_public_key"
	case Event_Which_getPrivateKey:
		return "get_private_key"
	case Event_Which_bootstrapOrJoinCluster:
		return "bootstrap_or_join_cluster"
	case Event_Which_addLearner:
		return "add_learner"
	case Event_Which_set:
		return "set"
	case Event_Which_execute:
		return "execute"
	case Event_Which_pollExecute:
		return "poll_execute"
	case Event_Which_listRange:
		return "list_range"
	case Event_Which_logAppend:
		return "log_append"
	case Event_Which_leave:
		return "leave"
	case Event_Which_execInviteRedeem:
		return "exec_invite_redeem"
	case Event_Which_joinRequestCreate:
		return "join_request_create"
	case Event_Which_joinRequestCancel:
		return "join_request_cancel"
	case Event_Which_recruit:
		return "recruit"
	case Event_Which_getOwnAddr:
		return "get_own_addr"
	case Event_Which_channelOpen:
		return "channel_open"
	case Event_Which_channelSend:
		return "channel_send"
	case Event_Which_channelPoll:
		return "channel_poll"
	case Event_Which_channelListen:
		return "channel_listen"
	case Event_Which_channelClose:
		return "channel_close"
	case Event_Which_channelCloseWrite:
		return "channel_close_write"
	case Event_Which_channelDataReady:
		return "channel_data_ready"
	case Event_Which_kick:
		return "kick"
	case Event_Which_txn:
		return "txn"
	case Event_Which_getVersion:
		return "get_version"
	case Event_Which_publicAccess:
		return "public_access"
	case Event_Which_execTicket:
		return "exec_ticket"
	case Event_Which_joinTicket:
		return "join_ticket"
	case Event_Which_joinRequestTicket:
		return "join_request_ticket"
	case Event_Which_dialSubmitCommand:
		return "dial_submit_command"
	case Event_Which_dialQueryCommandLog:
		return "dial_query_command_log"
	case Event_Which_error:
		return "error"
	case Event_Which_groupPut:
		return "group_put"
	case Event_Which_groupDelete:
		return "group_delete"
	case Event_Which_commandPut:
		return "command_put"
	case Event_Which_commandDelete:
		return "command_delete"
	case Event_Which_stationPut:
		return "station_put"
	case Event_Which_stationDelete:
		return "station_delete"
	case Event_Which_groupCommandPut:
		return "group_command_put"
	case Event_Which_groupCommandDelete:
		return "group_command_delete"
	case Event_Which_peerGroupPut:
		return "peer_group_put"
	case Event_Which_peerGroupDelete:
		return "peer_group_delete"
	case Event_Which_permitRequest:
		return "permit_request"
	case Event_Which_permitConfirm:
		return "permit_confirm"
	case Event_Which_permitRevoke:
		return "permit_revoke"
	case Event_Which_joinInviteCreate:
		return "join_invite_create"
	case Event_Which_joinInviteRevoke:
		return "join_invite_revoke"
	case Event_Which_execInviteCreate:
		return "exec_invite_create"
	case Event_Which_execInviteRevoke:
		return "exec_invite_revoke"
	default:
		return fmt.Sprintf("unknown(%d)", uint16(w))
	}
}

// EventFromName is the inverse of EventName: it returns the Event_Which
// value for one of the names EventName produces ("set_key", "set_field",
// ...), and false if name isn't recognized. Used to parse a human-authored
// event name (e.g. from test/e2e/testdata.json or kvctl-cli sendevent's
// JSON argument) back into the union variant.
func EventFromName(name string) (Event_Which, bool) {
	switch name {
	case "set_key":
		return Event_Which_setKey, true
	case "set_field":
		return Event_Which_setField, true
	case "get_key":
		return Event_Which_getKey, true
	case "get_field_by_registry":
		return Event_Which_getFieldByRegistry, true
	case "get_field_by_key":
		return Event_Which_getFieldByKey, true
	case "get_public_key":
		return Event_Which_getPublicKey, true
	case "get_private_key":
		return Event_Which_getPrivateKey, true
	case "bootstrap_or_join_cluster":
		return Event_Which_bootstrapOrJoinCluster, true
	case "add_learner":
		return Event_Which_addLearner, true
	case "set":
		return Event_Which_set, true
	case "execute":
		return Event_Which_execute, true
	case "poll_execute":
		return Event_Which_pollExecute, true
	case "list_range":
		return Event_Which_listRange, true
	case "log_append":
		return Event_Which_logAppend, true
	case "leave":
		return Event_Which_leave, true
	case "exec_invite_redeem":
		return Event_Which_execInviteRedeem, true
	case "join_request_create":
		return Event_Which_joinRequestCreate, true
	case "join_request_cancel":
		return Event_Which_joinRequestCancel, true
	case "recruit":
		return Event_Which_recruit, true
	case "get_own_addr":
		return Event_Which_getOwnAddr, true
	case "channel_open":
		return Event_Which_channelOpen, true
	case "channel_send":
		return Event_Which_channelSend, true
	case "channel_poll":
		return Event_Which_channelPoll, true
	case "channel_listen":
		return Event_Which_channelListen, true
	case "channel_close":
		return Event_Which_channelClose, true
	case "channel_close_write":
		return Event_Which_channelCloseWrite, true
	case "channel_data_ready":
		return Event_Which_channelDataReady, true
	case "kick":
		return Event_Which_kick, true
	case "txn":
		return Event_Which_txn, true
	case "get_version":
		return Event_Which_getVersion, true
	case "public_access":
		return Event_Which_publicAccess, true
	case "exec_ticket":
		return Event_Which_execTicket, true
	case "join_ticket":
		return Event_Which_joinTicket, true
	case "join_request_ticket":
		return Event_Which_joinRequestTicket, true
	case "dial_submit_command":
		return Event_Which_dialSubmitCommand, true
	case "dial_query_command_log":
		return Event_Which_dialQueryCommandLog, true
	case "error":
		return Event_Which_error, true
	case "group_put":
		return Event_Which_groupPut, true
	case "group_delete":
		return Event_Which_groupDelete, true
	case "command_put":
		return Event_Which_commandPut, true
	case "command_delete":
		return Event_Which_commandDelete, true
	case "station_put":
		return Event_Which_stationPut, true
	case "station_delete":
		return Event_Which_stationDelete, true
	case "group_command_put":
		return Event_Which_groupCommandPut, true
	case "group_command_delete":
		return Event_Which_groupCommandDelete, true
	case "peer_group_put":
		return Event_Which_peerGroupPut, true
	case "peer_group_delete":
		return Event_Which_peerGroupDelete, true
	case "permit_request":
		return Event_Which_permitRequest, true
	case "permit_confirm":
		return Event_Which_permitConfirm, true
	case "permit_revoke":
		return Event_Which_permitRevoke, true
	case "join_invite_create":
		return Event_Which_joinInviteCreate, true
	case "join_invite_revoke":
		return Event_Which_joinInviteRevoke, true
	case "exec_invite_create":
		return Event_Which_execInviteCreate, true
	case "exec_invite_revoke":
		return Event_Which_execInviteRevoke, true
	default:
		return 0, false
	}
}

// SignatureSize is the length of an Ed25519 signature.
const SignatureSize = 64

// PublicKeySize is the length of an Ed25519 public key.
const PublicKeySize = 32

// PrivateKeySize is the length of an Ed25519 private key in stdlib
// crypto/ed25519's format (32-byte seed + 32-byte public key).
const PrivateKeySize = 64

// NewMsg allocates a fresh single-segment capnp message and returns its
// root Event, id/crc32/signature all zero-valued and no union variant
// selected yet. Exported for generic tooling (pkg/e2edata's Event, behind
// kvctl-cli's sendevent/printeventdatamatrix) that builds/reads an
// arbitrary variant's fields directly by name rather than through one of
// this package's per-variant NewXxx constructors, which only ever expose
// a fixed, request-shaped subset. Ordinary callers should prefer the
// NewXxx constructors.
func NewMsg() (Msg, error) {
	return newMsg()
}

// newMsg is NewMsg's unexported core -- every NewXxx constructor in this
// package starts here.
func newMsg() (Msg, error) {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return Msg{}, fmt.Errorf("shmevent: new message: %w", err)
	}
	root, err := NewRootEvent(seg)
	if err != nil {
		return Msg{}, fmt.Errorf("shmevent: new root: %w", err)
	}
	return root, nil
}

// NewSetKey builds a setKey Msg: registers value under this message's own
// id in the node's key registry (see registry.go) -- generic, not
// KV-specific: used both for an actual KV key (ahead of NewSetField) and a
// peer id (ahead of NewAddLearner/NewBootstrapOrJoinCluster).
func NewSetKey(value []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetSetKey()
	if err := m.SetKey().SetValue(value); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_set_key: %w", err)
	}
	return m, nil
}

// NewSetField builds a setField Msg: store.Set(registry[sourceId], value).
func NewSetField(sourceID uint16, value []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetSetField()
	grp := m.SetField()
	grp.SetSourceId(sourceID)
	if err := grp.SetValue(value); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_set_field: %w", err)
	}
	return m, nil
}

// NewGetKey builds a getKey request Msg: reverse registry lookup for
// sourceId. The response reuses this same variant with key filled in.
func NewGetKey(sourceID uint16) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetKey()
	m.GetKey().SetSourceId(sourceID)
	return m, nil
}

// NewGetFieldByRegistry builds a getFieldByRegistry request Msg:
// store.Get(registry[sourceId]). The response reuses this same variant
// with value filled in.
func NewGetFieldByRegistry(sourceID uint16) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetFieldByRegistry()
	m.GetFieldByRegistry().SetSourceId(sourceID)
	return m, nil
}

// NewGetFieldByKey builds a getFieldByKey request Msg: a one-shot
// store.Get(key), no prior registry entry needed. The response reuses
// this same variant with value filled in.
func NewGetFieldByKey(key []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetFieldByKey()
	if err := m.GetFieldByKey().SetKey(key); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_get_field_by_key: %w", err)
	}
	return m, nil
}

// NewGetPublicKey builds a getPublicKey request Msg -- see that event's
// doc comment in api/shmevent.capnp.
func NewGetPublicKey() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetPublicKey()
	return m, nil
}

// NewGetPrivateKey builds a getPrivateKey request Msg -- see that event's
// doc comment in api/shmevent.capnp.
func NewGetPrivateKey() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetPrivateKey()
	return m, nil
}

// NewBootstrapOrJoinCluster builds a bootstrapOrJoinCluster Msg. An empty
// leaderAddr bootstraps this node as sole leader; a non-empty one joins
// that leader's cluster (may carry a "<multiaddr>#<tokenHex>" invite,
// parsed downstream same as today).
func NewBootstrapOrJoinCluster(leaderAddr string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetBootstrapOrJoinCluster()
	if err := m.BootstrapOrJoinCluster().SetLeaderAddr(leaderAddr); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_bootstrap_or_join_cluster: %w", err)
	}
	return m, nil
}

// NewAddLearner builds an addLearner Msg: claimedPeerId is a prior setKey
// registry reference to this node's own peer id (cross-checked against
// the connection's authenticated identity); addr is this node's own
// reachable address.
func NewAddLearner(claimedPeerID uint16, addr string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetAddLearner()
	grp := m.AddLearner()
	grp.SetClaimedPeerId(claimedPeerID)
	if err := grp.SetAddr(addr); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_add_learner: %w", err)
	}
	return m, nil
}

// NewSet builds a set Msg: an ordinary replicated store write, rejected
// by the daemon if key starts with the reserved SystemKeyPrefix.
func NewSet(key, value []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetSet()
	grp := m.Set()
	if err := grp.SetKey(key); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_set: %w", err)
	}
	if err := grp.SetValue(value); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_set: %w", err)
	}
	return m, nil
}

// NewExecute builds an execute Msg: a direct peer-to-peer notification,
// delivered over ExecuteProtocolID, bypassing the store/raft entirely.
// sourceId/destinationId are prior setKey registrations: the sender's own
// peer id and the receiver's.
func NewExecute(sourceID, destinationID uint16, value []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetExecute()
	grp := m.Execute()
	grp.SetSourceId(sourceID)
	grp.SetDestinationId(destinationID)
	if err := grp.SetValue(value); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_execute: %w", err)
	}
	return m, nil
}

// NewExecuteNotification builds an execute Msg in its network-leg shape:
// senderPeerId/value set, sourceId/destinationId left zero -- what
// pkg/daemon's sendExecute actually places on the wire between two daemon
// processes, which share no registry to resolve sourceId/destinationId
// against (see that variant's own doc comment in api/shmevent.capnp).
func NewExecuteNotification(senderPeerID string, value []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetExecute()
	grp := m.Execute()
	if err := grp.SetSenderPeerId(senderPeerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_execute_notification: %w", err)
	}
	if err := grp.SetValue(value); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_execute_notification: %w", err)
	}
	return m, nil
}

// NewChannelOpenHandshake builds a channelOpen Msg in its network-leg
// shape: senderPeerId set, peerId left empty -- what pkg/daemon's
// dispatchChannelOpen actually places on the wire as ChannelProtocolID's
// handshake, naming this node's own identity directly since the receiver
// has no way to look up a dial target's registry entry across processes.
func NewChannelOpenHandshake(senderPeerID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetChannelOpen()
	if err := m.ChannelOpen().SetSenderPeerId(senderPeerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_channel_open_handshake: %w", err)
	}
	return m, nil
}

// NewPollExecute builds a pollExecute request Msg: drains the local
// execute inbox, oldest first. The response reuses this same variant,
// empty if nothing was queued, else with senderPeerId/value filled in.
func NewPollExecute() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetPollExecute()
	return m, nil
}

// NewListRange builds a listRange request Msg: a bounded key-range scan
// (start/end both inclusive), answered directly by whichever node
// receives it. The response reuses this same variant with key/value
// filled in (both empty if nothing remains in range).
func NewListRange(start, end []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetListRange()
	grp := m.ListRange()
	if err := grp.SetStart(start); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_list_range: %w", err)
	}
	if err := grp.SetEnd(end); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_list_range: %w", err)
	}
	return m, nil
}

// NewLogAppend builds a logAppend Msg: a pkg/logrecord append -- key must
// start with the reserved log-key prefix.
func NewLogAppend(key, value []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetLogAppend()
	grp := m.LogAppend()
	if err := grp.SetKey(key); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_log_append: %w", err)
	}
	if err := grp.SetValue(value); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_log_append: %w", err)
	}
	return m, nil
}

// NewLeave builds a leave Msg: local-only, asks this node's own cluster
// to raft.RemoveServer it, resuming its own solo store.
func NewLeave() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetLeave()
	return m, nil
}

// NewKick builds a kick Msg: force-raft.RemoveServer's peerId out of this
// cluster. Remote caller must be a current raft voter.
func NewKick(peerID string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetKick()
	if err := m.Kick().SetPeerId(peerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_kick: %w", err)
	}
	return m, nil
}

// NewGetOwnAddr builds a getOwnAddr request Msg. The response fills addr
// with this node's own current best-advertised address.
func NewGetOwnAddr() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetOwnAddr()
	return m, nil
}

// NewGetVersion builds a getVersion request Msg. The response fills
// commit/dirty/buildTime/goVersion/libp2pVersion.
func NewGetVersion() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetGetVersion()
	return m, nil
}

// NewError builds an error response Msg: any failed request answers with
// this instead of its own variant.
func NewError(message string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetError()
	if err := m.Error().SetMessage_(message); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_error: %w", err)
	}
	return m, nil
}

// Encode serializes m to its capnp wire form, computing CRC32 and signing
// with priv. priv may be nil only for getPublicKey/getPrivateKey requests
// (see Sign's doc comment). m must already have its union variant/fields
// (and, if the caller wants one, its id) set -- see the NewXxx
// constructors above; Encode only sets crc32/signature.
func Encode(m Msg, priv PrivateKey) ([]byte, error) {
	crc, err := crc32Of(m)
	if err != nil {
		return nil, fmt.Errorf("shmevent: crc32: %w", err)
	}
	m.SetCrc32(crc)

	sig, err := Sign(priv, m, crc)
	if err != nil {
		return nil, fmt.Errorf("shmevent: sign: %w", err)
	}
	if err := m.SetSignature(sig); err != nil {
		return nil, fmt.Errorf("shmevent: set signature: %w", err)
	}

	return m.Message().Marshal()
}

// writableCopy returns a deep copy of m in a fresh, growable-arena
// message. capnp.Unmarshal (Decode's own first step) builds its message
// on a fixed-capacity arena wrapping the exact bytes it was given, with
// no spare room and no allocator to grow into -- fine for reading, but
// any later attempt to grow one of its Data/Text fields (crc32Of/
// signedPayload's own clear-then-restore of the signature field, or a
// daemon handler mutating a decoded request into its own response, e.g.
// filling in a previously-empty response field) fails outright with
// "cannot allocate segment in read-only ... arena". Every Msg this
// package hands back to a caller needs to tolerate exactly that kind of
// mutation, so Decode always returns a writable copy instead of the raw
// unmarshaled struct.
func writableCopy(m Msg) (Msg, error) {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return Msg{}, fmt.Errorf("shmevent: writable copy: new message: %w", err)
	}
	root, err := NewRootEvent(seg)
	if err != nil {
		return Msg{}, fmt.Errorf("shmevent: writable copy: new root: %w", err)
	}
	if err := capnp.Struct(root).CopyFrom(capnp.Struct(m)); err != nil {
		return Msg{}, fmt.Errorf("shmevent: writable copy: %w", err)
	}
	return root, nil
}

// Decode parses buf as a capnp Event message and verifies its CRC32
// against the decoded fields. It does not verify the signature -- callers
// that need authenticity (anything but a bootstrap
// GetPublicKey/GetPrivateKey exchange) must call Verify explicitly once
// they know which public key to check against; see this package's doc
// comment on why those two events are the exception.
func Decode(buf []byte) (m Msg, crc uint32, signature []byte, err error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: unmarshal: %w", err)
	}
	decoded, err := ReadRootEvent(msg)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: read root: %w", err)
	}
	// See writableCopy's own doc comment for why this copy exists at all:
	// crc32Of below already needs to mutate-and-restore root's crc32/
	// signature fields, which capnp.Unmarshal's read-only arena cannot
	// support in place.
	root, err := writableCopy(decoded)
	if err != nil {
		return Msg{}, 0, nil, err
	}

	wantCRC := root.Crc32()
	sigBytes, err := root.Signature()
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: signature: %w", err)
	}
	sigCopy := append([]byte(nil), sigBytes...)

	gotCRC, err := crc32Of(root)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: crc32: %w", err)
	}
	if gotCRC != wantCRC {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: crc32 mismatch: got %#x, message says %#x", gotCRC, wantCRC)
	}
	return root, wantCRC, sigCopy, nil
}
