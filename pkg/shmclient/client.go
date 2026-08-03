// Package shmclient implements the caller-side orchestration for
// pkg/shmevent's relational protocol over pkg/ipc: the SetKey+SetField
// message pair a Set needs, the single inline-key GetField a one-shot Get
// needs, and the GetPrivateKey bootstrap every signed call needs first
// (see pkg/shmevent's doc comment on why a local caller signs with the
// same Ed25519 key the node's own identity uses). Used by pkg/kvctl (the
// desktop CLI) and mobile/kvmobile (the in-process Android UI) -- anything
// that drives a node over pkg/ipc rather than pkg/shmevent's wire struct
// directly (as web-app's Rust build does, over ClientProtocolID).
package shmclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Session caches the signing key fetched from peerID for the lifetime of
// the caller's own process/session, so repeated Set/Get/Add calls only
// pay the GetPrivateKey round trip once -- important for a long-lived
// caller like mobile/kvmobile, less so for pkg/kvctl's short-lived CLI
// invocations, which still go through it via the package-level
// convenience functions below.
type Session struct {
	peerID string
	priv   shmevent.PrivateKey

	// channels holds every channel this session has OpenChannel/
	// ListenChannel'd its pkg/chandata data-plane ring pair for -- see
	// setupChannelData/channelPipe.
	channelsMu sync.Mutex
	channels   map[string]*channelPipe
}

// Open fetches peerID's signing key (see pkg/shmevent's doc comment on why
// this is safe/expected for a local, same-machine caller) and returns a
// Session ready for Set/Get/Add.
func Open(ctx context.Context, peerID string) (*Session, error) {
	priv, err := GetPrivateKey(ctx, peerID)
	if err != nil {
		return nil, fmt.Errorf("shmclient: fetch signing key: %w", err)
	}
	return &Session{peerID: peerID, priv: priv, channels: make(map[string]*channelPipe)}, nil
}

// newID returns a random non-zero id for a new message -- 0 is reserved
// meaning "SourceID/DestinationID not used" (see api/shmevent.capnp), so a
// real message's own id avoids it too, even though nothing currently cites
// these particular ids by SourceID.
func newID() uint16 {
	for {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 1
		}
		if id := binary.BigEndian.Uint16(b[:]); id != 0 {
			return id
		}
	}
}

// Set applies key=value through raft on the session's node, in a single
// EventSet round trip (key and value packed together via
// shmevent.EncodeSetPayload) rather than the SetKey+SetField pair --
// see EventSet's doc comment for why: pkg/ipc.Call pays a real,
// non-negligible cost (a fresh shmring segment pair) per round trip, so a
// caller in this package's position halves Set's cost by not needing two.
func (s *Session) Set(ctx context.Context, key, value string) error {
	payload, err := shmevent.EncodeSetPayload([]byte(key), []byte(value))
	if err != nil {
		return fmt.Errorf("shmclient: set: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventSet,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: set: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: set: %s", resp.Value)
	}
	return nil
}

// LogAppend writes one pkg/logrecord record -- key must start with
// logrecord.LogKeyPrefix (typically built via logrecord.BuildKey) and
// value its encoded pkg/logrecord.Record. Unlike Set, key/value are
// []byte rather than string: a log record's key is raw binary (a packed
// length-prefixed kind/unitID plus a binary timestamp and random
// suffix), not text. See shmevent.EventLogAppend's doc comment for why
// this needs its own event rather than reusing EventSet.
func (s *Session) LogAppend(ctx context.Context, key, value []byte) error {
	payload, err := shmevent.EncodeSetPayload(key, value)
	if err != nil {
		return fmt.Errorf("shmclient: log_append: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventLogAppend,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: log_append: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: log_append: %s", resp.Value)
	}
	return nil
}

// Txn atomically applies every op in ops through raft on the session's
// node, in a single EventTxn round trip: either all of them land, or none
// do (see shmevent.EventTxn's doc comment). Each op is a plain Set
// (shmevent.TxnOpSet, key and value both required) or Delete
// (shmevent.TxnOpDelete, value ignored).
func (s *Session) Txn(ctx context.Context, ops []shmevent.TxnOp) error {
	payload, err := shmevent.EncodeTxnPayload(ops)
	if err != nil {
		return fmt.Errorf("shmclient: txn: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventTxn,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: txn: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: txn: %s", resp.Value)
	}
	return nil
}

// Get reads key from the session's node -- a one-shot GetField carrying
// key directly in Value (SourceID left 0), skipping the registry
// round-trip Set needs -- which, like any raft follower's local read, may
// lag a moment behind a Set that just committed on the leader.
func (s *Session) Get(ctx context.Context, key string) (string, error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventGetField,
		Value:     []byte(key),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: get_field: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: get_field: %s", resp.Value)
	}
	return string(resp.Value), nil
}

// Add bootstraps this node as the cluster's sole leader (leaderPeerID ==
// "") or joins it to the cluster led by leaderPeerID as a voter. Returns
// this node's own peer id, mirroring the pre-shmevent
// ipcproto.ActionAdd response. See pkg/shmevent.EventAdd's doc comment for
// the (unused here) learner-join shape a remote browser caller uses
// instead.
func (s *Session) Add(ctx context.Context, leaderPeerID string) (string, error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventAdd,
		Value:     []byte(leaderPeerID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: add: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: add: %s", resp.Value)
	}
	return string(resp.Value), nil
}

// GetOwnAddr returns the session's node's own current best-advertised
// multiaddr (public first, then a relay reservation, then anything else,
// loopback last -- see pkg/daemon's advertisedAddrs) -- queried live, never
// cached, so a node whose Config.RelayPeers reservation completed after
// startup returns the up-to-date circuit address on a later call even if
// an earlier one didn't have it yet.
func (s *Session) GetOwnAddr(ctx context.Context) (string, error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventGetOwnAddr,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: get_own_addr: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: get_own_addr: %s", resp.Value)
	}
	return string(resp.Value), nil
}

// Leave asks the raft cluster the session's node currently belongs to to
// remove it (raft.RemoveServer) -- see shmevent.EventLeave's doc comment.
// Unlike Add, it takes no target: there's only ever one cluster this
// node's own live raft handle currently belongs to.
func (s *Session) Leave(ctx context.Context) error {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventLeave,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: leave: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: leave: %s", resp.Value)
	}
	return nil
}

// Kick asks the raft cluster the session's node currently belongs to to
// force-remove targetPeerID (raft.RemoveServer), without that peer's own
// cooperation -- see shmevent.EventKick's doc comment. Only takes effect
// if the session's node is itself a raft voter (or forwards to one),
// same restriction ConfirmPermit/RevokePermit have.
func (s *Session) Kick(ctx context.Context, targetPeerID string) error {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventKick,
		Value:     []byte(targetPeerID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: kick: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: kick: %s", resp.Value)
	}
	return nil
}

// RequestPermit lodges a pending permit request for peerID (of the given
// kind -- shmevent.KindPermitPeer or shmevent.KindBootstrapNode) on the
// session's node. metadata is opaque, kind-specific data (e.g. a dialable
// multiaddr for KindBootstrapNode). See shmevent.EventPermitRequest's doc
// comment: any raft node may receive and relay this.
func (s *Session) RequestPermit(ctx context.Context, kind byte, peerID, metadata []byte) error {
	payload, err := shmevent.EncodePermitRequestPayload(kind, peerID, metadata)
	if err != nil {
		return fmt.Errorf("shmclient: permit_request: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventPermitRequest,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: permit_request: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: permit_request: %s", resp.Value)
	}
	return nil
}

// ConfirmPermit promotes a pending permit request for peerID (of the
// given kind) from pending to confirmed. See
// shmevent.EventPermitConfirm's doc comment: only a peer that is
// currently a raft voter may confirm -- the session's node will reject
// this (surfaced as an error here) if it forwards to a leader that
// determines the confirming node isn't one.
func (s *Session) ConfirmPermit(ctx context.Context, kind byte, peerID []byte) error {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventPermitConfirm,
		Value:     shmevent.EncodePermitConfirmPayload(kind, peerID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: permit_confirm: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: permit_confirm: %s", resp.Value)
	}
	return nil
}

// RevokePermit deletes a confirmed permit record for peerID (of the given
// kind) outright. See shmevent.EventPermitRevoke's doc comment: only a
// peer that is currently a raft voter may revoke -- the session's node
// will reject this (surfaced as an error here) if it forwards to a
// leader that determines the revoking node isn't one, the same as
// ConfirmPermit.
func (s *Session) RevokePermit(ctx context.Context, kind byte, peerID []byte) error {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventPermitRevoke,
		Value:     shmevent.EncodePermitConfirmPayload(kind, peerID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: permit_revoke: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: permit_revoke: %s", resp.Value)
	}
	return nil
}

// catalogCall is the shared round trip behind every group-based ACL
// catalog Session method below (PutGroup, DeleteGroupCommand, etc.) --
// each of the eight just builds its own payload and event type, then
// reduces to this one send-and-check-for-EventError call.
func (s *Session) catalogCall(ctx context.Context, eventType uint8, payload []byte) error {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: eventType,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: %s: %w", shmevent.EventName(eventType), err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: %s: %s", shmevent.EventName(eventType), resp.Value)
	}
	return nil
}

// PutGroup creates or updates (single-step, no separate create/update --
// see shmevent.KindGroup's doc comment) the Group record id=name on the
// session's node, granting unconditional access to its linked commands to
// any peer if public is true (see pkg/kvfsm's isPermittedForCommand). Only
// a current raft voter may do this -- see shmevent.EventGroupPut's doc
// comment.
func (s *Session) PutGroup(ctx context.Context, id, name string, public bool) error {
	payload, err := shmevent.EncodeGroupPutPayload(id, name, public)
	if err != nil {
		return fmt.Errorf("shmclient: group_put: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventGroupPut, payload)
}

// DeleteGroup deletes the Group record id, cascading to every
// GroupCommand/PeerGroup record referencing it (see
// kvfsm.OpCascadeDelete). Only a current raft voter may do this.
func (s *Session) DeleteGroup(ctx context.Context, id string) error {
	return s.catalogCall(ctx, shmevent.EventGroupDelete, []byte(id))
}

// PutCommand creates or updates the Command record id={name, peerID}
// (peerID is where the command may be executed) on the session's node.
// Only a current raft voter may do this.
func (s *Session) PutCommand(ctx context.Context, id, name string, peerID []byte) error {
	payload, err := shmevent.EncodeCommandPutPayload(id, name, peerID)
	if err != nil {
		return fmt.Errorf("shmclient: command_put: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventCommandPut, payload)
}

// DeleteCommand deletes the Command record id, cascading to every
// GroupCommand record referencing it. Only a current raft voter may do
// this.
func (s *Session) DeleteCommand(ctx context.Context, id string) error {
	return s.catalogCall(ctx, shmevent.EventCommandDelete, []byte(id))
}

// PutGroupCommand links commandID to groupID -- peers in groupID (see
// PutPeerGroup) become permitted to submit/execute commandID. Only a
// current raft voter may do this.
func (s *Session) PutGroupCommand(ctx context.Context, commandID, groupID []byte) error {
	payload, err := shmevent.EncodeGroupCommandPayload(commandID, groupID)
	if err != nil {
		return fmt.Errorf("shmclient: group_command_put: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventGroupCommandPut, payload)
}

// DeleteGroupCommand unlinks commandID from groupID. Only a current raft
// voter may do this.
func (s *Session) DeleteGroupCommand(ctx context.Context, commandID, groupID []byte) error {
	payload, err := shmevent.EncodeGroupCommandPayload(commandID, groupID)
	if err != nil {
		return fmt.Errorf("shmclient: group_command_delete: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventGroupCommandDelete, payload)
}

// PutPeerGroup adds peerID as a member of groupID -- see PutGroupCommand
// for what that grants. Only a current raft voter may do this.
func (s *Session) PutPeerGroup(ctx context.Context, peerID, groupID []byte) error {
	payload, err := shmevent.EncodePeerGroupPayload(peerID, groupID)
	if err != nil {
		return fmt.Errorf("shmclient: peer_group_put: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventPeerGroupPut, payload)
}

// DeletePeerGroup removes peerID from groupID. Only a current raft voter
// may do this.
func (s *Session) DeletePeerGroup(ctx context.Context, peerID, groupID []byte) error {
	payload, err := shmevent.EncodePeerGroupPayload(peerID, groupID)
	if err != nil {
		return fmt.Errorf("shmclient: peer_group_delete: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventPeerGroupDelete, payload)
}

// CreateJoinInvite lodges a one-time shmevent.KindJoinInvite record for
// token, granting suffrage, on the session's node. Only a current raft
// voter may do this -- see shmevent.EventJoinInviteCreate's doc comment.
func (s *Session) CreateJoinInvite(ctx context.Context, token []byte, suffrage byte) error {
	payload, err := shmevent.EncodeJoinInviteCreatePayload(token, suffrage)
	if err != nil {
		return fmt.Errorf("shmclient: join_invite_create: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventJoinInviteCreate, payload)
}

// RevokeJoinInvite deletes the KindJoinInvite record for token outright,
// before it's ever redeemed. Only a current raft voter may do this.
func (s *Session) RevokeJoinInvite(ctx context.Context, token []byte) error {
	return s.catalogCall(ctx, shmevent.EventJoinInviteRevoke, shmevent.EncodeJoinInviteRevokePayload(token))
}

// CreateJoinRequest mints a fresh join-request ticket on the session's own
// node -- the reverse of CreateJoinInvite, for a node with no cluster of
// its own yet to hand some other cluster's voter (see
// shmevent.EventJoinRequestCreate's doc comment). Returns the new token.
func (s *Session) CreateJoinRequest(ctx context.Context) ([]byte, error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventJoinRequestCreate,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return nil, fmt.Errorf("shmclient: join_request_create: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return nil, fmt.Errorf("shmclient: join_request_create: %s", resp.Value)
	}
	return resp.Value, nil
}

// CancelJoinRequest clears the session's own pending join-request ticket
// (a no-op if token no longer matches -- already consumed or superseded).
func (s *Session) CancelJoinRequest(ctx context.Context, token []byte) error {
	return s.catalogCall(ctx, shmevent.EventJoinRequestCancel, shmevent.EncodeJoinRequestCancelPayload(token))
}

// Recruit tells the session's own node (an existing raft voter) to mint a
// normal join invite on its own cluster and hand-deliver it directly to
// the device named in ticket ("<device's own multiaddr>#<tokenHex>", from
// that device's own CreateJoinRequest) -- see shmevent.EventRecruit's doc
// comment. Returns the recruited device's own join result ("<peerID>
// ok"/"<peerID> pending") on success.
func (s *Session) Recruit(ctx context.Context, ticket string, suffrage byte) (string, error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventRecruit,
		Value:     shmevent.EncodeRecruitPayload(ticket, suffrage),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: recruit: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: recruit: %s", resp.Value)
	}
	return string(resp.Value), nil
}

// CreateExecInvite lodges a one-time shmevent.KindExecInvite record for
// token, binding commandID+inputsJSON, on the session's node. Only a
// current raft voter may do this -- see shmevent.EventExecInviteCreate's
// doc comment.
func (s *Session) CreateExecInvite(ctx context.Context, token []byte, commandID, inputsJSON string) error {
	payload, err := shmevent.EncodeExecInviteCreatePayload(token, commandID, inputsJSON)
	if err != nil {
		return fmt.Errorf("shmclient: exec_invite_create: %w", err)
	}
	return s.catalogCall(ctx, shmevent.EventExecInviteCreate, payload)
}

// RevokeExecInvite deletes the KindExecInvite record for token outright,
// before it's ever redeemed. Only a current raft voter may do this.
func (s *Session) RevokeExecInvite(ctx context.Context, token []byte) error {
	return s.catalogCall(ctx, shmevent.EventExecInviteRevoke, shmevent.EncodeExecInviteRevokePayload(token))
}

// RedeemExecInvite tells the session's own node to dial sourceAddr and
// redeem token there on this node's own behalf -- see
// shmevent.EventExecInviteRedeem's doc comment. Returns the new instance
// id on success.
func (s *Session) RedeemExecInvite(ctx context.Context, sourceAddr string, token []byte) (string, error) {
	payload, err := shmevent.EncodeExecInviteRedeemRequest(sourceAddr, token)
	if err != nil {
		return "", fmt.Errorf("shmclient: exec_invite_redeem: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventExecInviteRedeem,
		Value:     payload,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: exec_invite_redeem: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: exec_invite_redeem: %s", resp.Value)
	}
	return string(resp.Value), nil
}

// Execute sends payload as a direct peer-to-peer EventExecute notification
// from the session's own node to destPeerID -- bypassing raft and the
// store entirely, see shmevent.EventExecute's doc comment. Needs two
// EventSetKey round trips first (registering the session's own peer id
// and destPeerID under fresh ids) since dispatchExecute requires both
// SourceID and DestinationID to reference prior registrations, unlike
// Set/Get's single-round-trip forms.
func (s *Session) Execute(ctx context.Context, destPeerID string, payload []byte) error {
	sourceID := newID()
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventSetKey,
		Value:     []byte(s.peerID),
		ID:        sourceID,
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: execute: register source: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: execute: register source: %s", resp.Value)
	}

	destID := newID()
	resp, err = ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventSetKey,
		Value:     []byte(destPeerID),
		ID:        destID,
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: execute: register destination: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: execute: register destination: %s", resp.Value)
	}

	resp, err = ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType:     shmevent.EventExecute,
		SourceID:      sourceID,
		DestinationID: destID,
		Value:         payload,
		ID:            newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: execute: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: execute: %s", resp.Value)
	}
	return nil
}

// PollExecute drains one queued EventExecute notification delivered to
// the session's node -- see shmevent.EventPollExecute's doc comment. ok
// is false if nothing is currently queued.
func (s *Session) PollExecute(ctx context.Context) (senderPeerID string, payload []byte, ok bool, err error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventPollExecute,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", nil, false, fmt.Errorf("shmclient: poll_execute: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", nil, false, fmt.Errorf("shmclient: poll_execute: %s", resp.Value)
	}
	if len(resp.Value) == 0 {
		return "", nil, false, nil
	}
	sender, notifPayload, err := shmevent.DecodeExecuteNotification(resp.Value)
	if err != nil {
		return "", nil, false, fmt.Errorf("shmclient: poll_execute: decode notification: %w", err)
	}
	return string(sender), notifPayload, true, nil
}

// channelPipe is one open channel's pkg/chandata data-plane handles from
// this local caller's own side: up is this side's producer ring (this
// process created it -- see chandata.ChunkWriter.CloseStorage's doc
// comment on why this side also releases its storage), down is this
// side's consumer ring (the daemon created it -- this side only ever
// Close()s its own mapping of it). See setupChannelData.
//
// A shmring Writer/Reader documents itself as usable from only a single
// goroutine at a time, but this package's own callers make no such
// promise back: mobile/kvmobile in particular hands SendChannelData/
// PollChannel-driving code and CloseChannel out to Kotlin, which is free
// to call them from different threads concurrently -- unlike the old
// design, where every call was a self-contained IPC round trip with
// nothing left to race afterward, up/down are now long-lived objects a
// concurrent Close could free out from under an in-flight
// WriteChunk/ReadChunk. ctx/cancel/mu/closed/wg below exist purely to
// make that safe: every Send/Poll call registers itself (enter) before
// touching up/down and unregisters (leave) after, while
// close/closeUpload cancel ctx first (promptly unblocking anything
// currently inside WriteChunk/ReadChunk, which both respect context
// cancellation) and only then wait for every registered call to actually
// finish before touching the underlying rings themselves.
type channelPipe struct {
	up   *chandata.ChunkWriter
	down *chandata.ChunkReader

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func newChannelPipe(up *chandata.ChunkWriter, down *chandata.ChunkReader) *channelPipe {
	ctx, cancel := context.WithCancel(context.Background())
	return &channelPipe{up: up, down: down, ctx: ctx, cancel: cancel}
}

// enter registers one in-flight Send/Poll/closeUpload call against p,
// returning false (caller must not proceed) if p has already started
// closing. Every successful enter must be matched by exactly one leave.
func (p *channelPipe) enter() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.wg.Add(1)
	return true
}

func (p *channelPipe) leave() { p.wg.Done() }

// closeUpload safely closes p's upload ring writer (CloseChannelWrite's
// implementation) -- serialized against any in-flight SendChannel the
// same way close (below) is, but without tearing the whole pipe down:
// PollChannel keeps working against down afterward.
func (p *channelPipe) closeUpload() {
	if !p.enter() {
		return
	}
	defer p.leave()
	p.up.Close()
}

// close marks p closed (any Send/Poll call that hasn't already entered
// will now fail fast rather than touch up/down), cancels ctx to unblock
// whichever calls are currently in flight, waits for them to actually
// return, and only then releases both rings -- see this type's own doc
// comment for why each step matters.
func (p *channelPipe) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cancel()
	p.wg.Wait()
	p.up.CloseStorage()
	p.down.Close()
}

// mergeCancel returns a context cancelled when either a or b is --
// SendChannel/PollChannel bound their WriteChunk/ReadChunk call by both
// the caller's own ctx and the pipe's ctx (cancelled by channelPipe.close/
// closeUpload), so a call blocked on a full/empty ring is unblocked
// promptly by either the caller giving up or this channel being closed
// from a different goroutine, whichever comes first. The returned cancel
// must always be called to release the association context.AfterFunc
// makes internally.
func mergeCancel(a, b context.Context) (context.Context, context.CancelFunc) {
	merged, cancel := context.WithCancel(a)
	stop := context.AfterFunc(b, cancel)
	return merged, func() { stop(); cancel() }
}

// pollWaitCap bounds how long a single PollChannel call blocks inside
// ReadChunk waiting for the next chunk before reporting ChannelNoData --
// deliberately short (not ctx's own, often much longer, deadline): the
// existing callers in pkg/kvctl/mobile/kvmobile expect a poll call to
// return promptly so their own loop can re-check for a signal to stop
// (SIGINT, a caller-cancelled context) between calls, the same
// responsiveness the old per-chunk IPC round trip had by virtue of never
// blocking server-side in the first place. This still gets the ring's own
// efficient backoff wait (see pkg/chandata) instead of returning
// instantly and relying entirely on the caller's own sleep-then-retry, so
// a chunk that arrives mid-wait is delivered with lower latency than
// before, not higher.
const pollWaitCap = 150 * time.Millisecond

// setupChannelData creates/opens channelID's pkg/chandata ring pair and
// registers it in s.channels, then sends shmevent.EventChannelDataReady so
// the daemon opens its own end and starts forwarding -- shared by
// OpenChannel/ListenChannel once each has a channelID in hand. On any
// failure it tears down whatever it already created/opened before
// returning the error, so a caller that gives up on a failed Open/Listen
// doesn't leak a half-set-up ring.
func (s *Session) setupChannelData(ctx context.Context, channelID string) (err error) {
	up, err := chandata.Create(s.peerID, channelID, chandata.DirUp)
	if err != nil {
		return fmt.Errorf("shmclient: create upload ring: %w", err)
	}
	defer func() {
		if err != nil {
			up.CloseStorage()
		}
	}()

	down, err := chandata.Open(ctx, s.peerID, channelID, chandata.DirDown)
	if err != nil {
		return fmt.Errorf("shmclient: open download ring: %w", err)
	}
	defer func() {
		if err != nil {
			down.Close()
		}
	}()

	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventChannelDataReady,
		Value:     []byte(channelID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: channel_data_ready: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: channel_data_ready: %s", resp.Value)
	}

	s.channelsMu.Lock()
	s.channels[channelID] = newChannelPipe(up, down)
	s.channelsMu.Unlock()
	return nil
}

// OpenChannel opens a raw, persistent, bidirectional byte pipe from the
// session's own node to destPeerID -- see shmevent.EventChannelOpen's doc
// comment. Unlike Execute, this needs no prior EventSetKey registration:
// EventChannelOpen's Value is just destPeerID directly. Returns the
// freshly minted channelID every subsequent SendChannel/PollChannel/
// CloseChannel call on this channel needs. Also sets up channelID's
// pkg/chandata data-plane ring pair (setupChannelData) before returning,
// so every channelID this method ever hands back is immediately usable
// with SendChannel/PollChannel's high-throughput ring path.
func (s *Session) OpenChannel(ctx context.Context, destPeerID string) (channelID string, err error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventChannelOpen,
		Value:     []byte(destPeerID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", fmt.Errorf("shmclient: open_channel: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", fmt.Errorf("shmclient: open_channel: %s", resp.Value)
	}
	channelID = string(resp.Value)
	if err := s.setupChannelData(ctx, channelID); err != nil {
		return "", fmt.Errorf("shmclient: open_channel: %w", err)
	}
	return channelID, nil
}

// SendChannel writes one chunk of bytes to channelID, tagged with purpose
// (see shmevent.ChannelPurposeData/Control/Video) -- unlike before, this
// is a pkg/chandata ring write (see OpenChannel/ListenChannel's own doc
// comments), not a per-chunk IPC round trip: it returns once chunk has
// been copied into the ring, which may be before the daemon has actually
// forwarded it onto the wire (see shmevent.EventChannelDataReady's doc
// comment on why CloseChannelWrite, not this call, is where that
// distinction matters).
func (s *Session) SendChannel(ctx context.Context, channelID string, purpose byte, chunk []byte) error {
	pipe, ok := s.channelPipe(channelID)
	if !ok {
		return fmt.Errorf("shmclient: send_channel: no such channel %q", channelID)
	}
	if !pipe.enter() {
		return fmt.Errorf("shmclient: send_channel: channel %q is closing", channelID)
	}
	defer pipe.leave()
	workCtx, cancel := mergeCancel(ctx, pipe.ctx)
	defer cancel()
	if err := pipe.up.WriteChunk(workCtx, purpose, chunk); err != nil {
		return fmt.Errorf("shmclient: send_channel: %w", err)
	}
	return nil
}

// ChannelStatus is PollChannel's three-way result -- see
// shmevent.EventChannelPoll's doc comment.
type ChannelStatus byte

const (
	ChannelNoData ChannelStatus = iota
	ChannelChunk
	ChannelClosed
)

// PollChannel drains one buffered chunk received on channelID since the
// last poll, if any -- a caller loops this (with a short sleep between
// empty polls) to observe a channel's incoming traffic, the same "no push
// transport" shape PollExecute already uses. purpose (see
// shmevent.ChannelPurposeData/Control/Video) is only meaningful when
// status is ChannelChunk. Reads from channelID's pkg/chandata download
// ring (see OpenChannel/ListenChannel), blocking briefly (pollWaitCap) for
// a chunk to arrive rather than returning ChannelNoData instantly.
func (s *Session) PollChannel(ctx context.Context, channelID string) (chunk []byte, purpose byte, status ChannelStatus, err error) {
	pipe, ok := s.channelPipe(channelID)
	if !ok {
		return nil, 0, ChannelNoData, fmt.Errorf("shmclient: poll_channel: no such channel %q", channelID)
	}
	if !pipe.enter() {
		// Closed by a concurrent CloseChannel -- same terminal state a
		// still-open pipe eventually reports via io.EOF below.
		return nil, 0, ChannelClosed, nil
	}
	defer pipe.leave()
	workCtx, wcancel := mergeCancel(ctx, pipe.ctx)
	defer wcancel()
	waitCtx, cancel := context.WithTimeout(workCtx, pollWaitCap)
	defer cancel()
	purpose, chunk, err = pipe.down.ReadChunk(waitCtx)
	if err != nil {
		if err == io.EOF {
			return nil, 0, ChannelClosed, nil
		}
		if ctx.Err() != nil {
			// The caller's own ctx is what actually ran out, not just this
			// call's internal pollWaitCap -- a real error, not "no data
			// yet."
			return nil, 0, ChannelNoData, fmt.Errorf("shmclient: poll_channel: %w", err)
		}
		if pipe.ctx.Err() != nil {
			// Closed by a concurrent CloseChannel mid-wait.
			return nil, 0, ChannelClosed, nil
		}
		return nil, 0, ChannelNoData, nil
	}
	return chunk, purpose, ChannelChunk, nil
}

// ListenChannel claims one pending incoming channel -- see
// shmevent.EventChannelListen's doc comment. ok is false if none are
// currently pending; a caller loops this the same way PollChannel loops
// for incoming traffic. Also sets up channelID's pkg/chandata data-plane
// ring pair (setupChannelData) before returning, same as OpenChannel.
func (s *Session) ListenChannel(ctx context.Context) (channelID, remotePeerID string, ok bool, err error) {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventChannelListen,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %s", resp.Value)
	}
	if len(resp.Value) == 0 {
		return "", "", false, nil
	}
	id, peer, err := shmevent.DecodeChannelAccept(resp.Value)
	if err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: decode: %w", err)
	}
	if err := s.setupChannelData(ctx, string(id)); err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %w", err)
	}
	return string(id), string(peer), true, nil
}

// CloseChannel ends channelID outright -- see shmevent.EventChannelClose's
// doc comment. Also releases channelID's pkg/chandata ring pair, if this
// session ever set one up for it (OpenChannel/ListenChannel) -- this side
// created the upload ring, so it releases its storage outright
// (ChunkWriter.CloseStorage); it only ever opened the download ring as a
// reader, so it just releases its own mapping (ChunkReader.Close). Best-
// effort regardless of whether the EventChannelClose call itself
// succeeds, so a daemon that's already gone doesn't leak this side's own
// ring storage.
func (s *Session) CloseChannel(ctx context.Context, channelID string) error {
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventChannelClose,
		Value:     []byte(channelID),
		ID:        newID(),
	}, s.priv)

	s.channelsMu.Lock()
	pipe, ok := s.channels[channelID]
	delete(s.channels, channelID)
	s.channelsMu.Unlock()
	if ok {
		pipe.close()
	}

	if err != nil {
		return fmt.Errorf("shmclient: close_channel: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: close_channel: %s", resp.Value)
	}
	return nil
}

// CloseChannelWrite half-closes channelID's outgoing direction only --
// "I have nothing more to send," not "end the channel outright" (that's
// CloseChannel). A caller whose own local input source (e.g. os.Stdin)
// reaches a clean EOF should call this rather than CloseChannel, then
// keep polling for whatever the remote peer still has left to send. First
// closes (not releases -- see CloseChannel) this side's own upload ring
// writer, then sends shmevent.EventChannelCloseWrite -- see that event's
// doc comment for why the daemon deliberately delays its response until
// every chunk this call's Close just made visible has actually been
// forwarded onto the wire, so this call returning is still a genuine
// "everything I sent already reached the network" guarantee, the same
// one the old per-chunk-synchronous design had for free.
func (s *Session) CloseChannelWrite(ctx context.Context, channelID string) error {
	if pipe, ok := s.channelPipe(channelID); ok {
		pipe.closeUpload()
	}

	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventChannelCloseWrite,
		Value:     []byte(channelID),
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: close_channel_write: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("shmclient: close_channel_write: %s", resp.Value)
	}
	return nil
}

// channelPipe looks up channelID's data-plane ring pair, set up by
// OpenChannel/ListenChannel.
func (s *Session) channelPipe(channelID string) (*channelPipe, bool) {
	s.channelsMu.Lock()
	defer s.channelsMu.Unlock()
	p, ok := s.channels[channelID]
	return p, ok
}

// ListRange returns the first stored key/value pair with start <= key <=
// end (both inclusive), or ok=false if none remain in that range -- see
// shmevent.EventListRange's doc comment. A caller wanting every match
// calls this in a loop, each time narrowing start to just past the
// previously returned key (e.g. append a 0x00 byte to it), the same
// "loop rather than a bulk response" shape PollExecute already uses.
func (s *Session) ListRange(ctx context.Context, start, end []byte) (key, value []byte, ok bool, err error) {
	query, err := shmevent.EncodeListRangeQuery(start, end)
	if err != nil {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: %w", err)
	}
	resp, err := ipc.Call(ctx, s.peerID, shmevent.Msg{
		EventType: shmevent.EventListRange,
		Value:     query,
		ID:        newID(),
	}, s.priv)
	if err != nil {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: %s", resp.Value)
	}
	if len(resp.Value) == 0 {
		return nil, nil, false, nil
	}
	key, value, err = shmevent.DecodeListRangeQuery(resp.Value)
	if err != nil {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: decode result: %w", err)
	}
	return key, value, true, nil
}

// GetPublicKey fetches peerID's Ed25519 public key -- unsigned, since it's
// one of the two bootstrap events a node accepts without a key to check a
// signature against yet (see pkg/shmevent.RequiresSignature).
func GetPublicKey(ctx context.Context, peerID string) (shmevent.PublicKey, error) {
	resp, err := ipc.Call(ctx, peerID, shmevent.Msg{
		EventType: shmevent.EventGetPublicKey,
		ID:        newID(),
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_public_key: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return nil, fmt.Errorf("shmclient: get_public_key: %s", resp.Value)
	}
	return shmevent.PublicKey(resp.Value), nil
}

// GetPrivateKey fetches peerID's Ed25519 private key -- unsigned, same
// bootstrap exception as GetPublicKey.
func GetPrivateKey(ctx context.Context, peerID string) (shmevent.PrivateKey, error) {
	resp, err := ipc.Call(ctx, peerID, shmevent.Msg{
		EventType: shmevent.EventGetPrivateKey,
		ID:        newID(),
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("shmclient: get_private_key: %w", err)
	}
	if resp.EventType == shmevent.EventError {
		return nil, fmt.Errorf("shmclient: get_private_key: %s", resp.Value)
	}
	return shmevent.PrivateKey(resp.Value), nil
}

// Set is a one-shot convenience wrapper around Open+Session.Set, for a
// short-lived caller (pkg/kvctl) that doesn't need to cache the session
// across multiple calls.
func Set(ctx context.Context, peerID, key, value string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Set(ctx, key, value)
}

// LogAppend is the one-shot convenience wrapper around
// Open+Session.LogAppend.
func LogAppend(ctx context.Context, peerID string, key, value []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.LogAppend(ctx, key, value)
}

// Txn is the one-shot convenience wrapper around Open+Session.Txn.
func Txn(ctx context.Context, peerID string, ops []shmevent.TxnOp) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Txn(ctx, ops)
}

// Get is the one-shot convenience wrapper around Open+Session.Get.
func Get(ctx context.Context, peerID, key string) (string, error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.Get(ctx, key)
}

// Add is the one-shot convenience wrapper around Open+Session.Add.
//
// Bootstrap/first-join is a special case: a brand new node has no signing
// key exchange to do beyond what Open already performs (GetPrivateKey is
// itself unsigned and always available), so this works uniformly whether
// or not the node has ever been added to a cluster before.
func Add(ctx context.Context, peerID, leaderPeerID string) (string, error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.Add(ctx, leaderPeerID)
}

// Leave is the one-shot convenience wrapper around Open+Session.Leave.
func Leave(ctx context.Context, peerID string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Leave(ctx)
}

// Kick is the one-shot convenience wrapper around Open+Session.Kick.
func Kick(ctx context.Context, peerID, targetPeerID string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Kick(ctx, targetPeerID)
}

// RequestPermit is the one-shot convenience wrapper around
// Open+Session.RequestPermit.
func RequestPermit(ctx context.Context, peerID string, kind byte, targetPeerID, metadata []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.RequestPermit(ctx, kind, targetPeerID, metadata)
}

// ConfirmPermit is the one-shot convenience wrapper around
// Open+Session.ConfirmPermit.
func ConfirmPermit(ctx context.Context, peerID string, kind byte, targetPeerID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.ConfirmPermit(ctx, kind, targetPeerID)
}

// RevokePermit is the one-shot convenience wrapper around
// Open+Session.RevokePermit.
func RevokePermit(ctx context.Context, peerID string, kind byte, targetPeerID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.RevokePermit(ctx, kind, targetPeerID)
}

// PutGroup is the one-shot convenience wrapper around Open+Session.PutGroup.
func PutGroup(ctx context.Context, peerID, id, name string, public bool) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutGroup(ctx, id, name, public)
}

// DeleteGroup is the one-shot convenience wrapper around
// Open+Session.DeleteGroup.
func DeleteGroup(ctx context.Context, peerID, id string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeleteGroup(ctx, id)
}

// PutCommand is the one-shot convenience wrapper around
// Open+Session.PutCommand.
func PutCommand(ctx context.Context, peerID, id, name string, targetPeerID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutCommand(ctx, id, name, targetPeerID)
}

// DeleteCommand is the one-shot convenience wrapper around
// Open+Session.DeleteCommand.
func DeleteCommand(ctx context.Context, peerID, id string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeleteCommand(ctx, id)
}

// PutGroupCommand is the one-shot convenience wrapper around
// Open+Session.PutGroupCommand.
func PutGroupCommand(ctx context.Context, peerID string, commandID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutGroupCommand(ctx, commandID, groupID)
}

// DeleteGroupCommand is the one-shot convenience wrapper around
// Open+Session.DeleteGroupCommand.
func DeleteGroupCommand(ctx context.Context, peerID string, commandID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeleteGroupCommand(ctx, commandID, groupID)
}

// PutPeerGroup is the one-shot convenience wrapper around
// Open+Session.PutPeerGroup.
func PutPeerGroup(ctx context.Context, peerID string, targetPeerID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutPeerGroup(ctx, targetPeerID, groupID)
}

// DeletePeerGroup is the one-shot convenience wrapper around
// Open+Session.DeletePeerGroup.
func DeletePeerGroup(ctx context.Context, peerID string, targetPeerID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeletePeerGroup(ctx, targetPeerID, groupID)
}

// Execute is the one-shot convenience wrapper around Open+Session.Execute.
func Execute(ctx context.Context, peerID, destPeerID string, payload []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Execute(ctx, destPeerID, payload)
}

// PollExecute is the one-shot convenience wrapper around
// Open+Session.PollExecute.
func PollExecute(ctx context.Context, peerID string) (senderPeerID string, payload []byte, ok bool, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", nil, false, err
	}
	return s.PollExecute(ctx)
}

// OpenChannel is the one-shot convenience wrapper around
// Open+Session.OpenChannel.
func OpenChannel(ctx context.Context, peerID, destPeerID string) (channelID string, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.OpenChannel(ctx, destPeerID)
}

// Unlike OpenChannel/ListenChannel/CloseChannel above, SendChannel and
// PollChannel have no one-shot convenience wrapper here: both now read
// from/write to a pkg/chandata ring pair that only the Session which
// itself called OpenChannel/ListenChannel for that channelID ever set up
// (see setupChannelData) -- a fresh Open()'d Session has no way to
// rediscover it, so a one-shot wrapper could never do anything but fail.
// A caller needs the same *Session across a channel's Open/Listen,
// Send/Poll, and Close calls regardless, which pkg/kvctl and
// mobile/kvmobile both already do.

// ListenChannel is the one-shot convenience wrapper around
// Open+Session.ListenChannel.
func ListenChannel(ctx context.Context, peerID string) (channelID, remotePeerID string, ok bool, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", "", false, err
	}
	return s.ListenChannel(ctx)
}

// CloseChannel is the one-shot convenience wrapper around
// Open+Session.CloseChannel.
func CloseChannel(ctx context.Context, peerID, channelID string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.CloseChannel(ctx, channelID)
}

// ListRange is the one-shot convenience wrapper around
// Open+Session.ListRange.
func ListRange(ctx context.Context, peerID string, start, end []byte) (key, value []byte, ok bool, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return nil, nil, false, err
	}
	return s.ListRange(ctx, start, end)
}
