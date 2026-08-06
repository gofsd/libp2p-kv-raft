// Package shmevent implements the single wire structure (see
// api/shmevent.capnp) used for every message exchanged between a raft node
// instance and a local "user" -- the desktop CLI, the in-process Android
// UI, or a browser tab's main thread -- over shmring shared memory, and
// (per the same struct's remote counterpart) over
// pkg/daemon.ClientProtocolID. It replaces pkg/ipcproto.
//
// See api/shmevent.capnp's doc comment for the full design rationale: why
// every message carries exactly one raw value plus two relational id
// fields instead of a fixed Key+Value pair, and how Set/Get decompose into
// short sequences of linked messages built around a server-side key
// registry (registry.go).
package shmevent

import (
	"fmt"

	capnp "capnproto.org/go/capnp/v3"
)

// Event type bytes -- the wire values of Msg.EventType. See
// api/shmevent.capnp and this package's doc comment for the
// SetKey/SetField/GetKey/GetField relational pattern.
//
// Before adding a new constant here: every raft node in the mesh --
// including long-lived bootstrap nodes and Android devices nobody
// force-upgrades -- has to agree on what it means, forever, and it needs a
// hand-duplicated codec/dispatch/wrapper added across pkg/daemon,
// web-app/src/shmevent.rs (a hand-ported mirror, not generated), and
// pkg/kvctl/mobile/kvmobile. If what's being added is a new *task* --
// typed inputs, ACL, durable request/response logging, a low-latency
// "ready" poke -- register it as a Command through
// pkg/kvctl/dispatch.go's SubmitCommand/Group-Command catalog instead
// (see that file's doc comment and CommandRequestLogKind below): it
// already dispatches generically over EventGet/EventLogAppend/
// EventListRange/EventExecute, with zero new wire bytes needed per task
// type. Reserve new constants here for genuinely new wire-level
// primitives -- a new relational shape, a new stream/session protocol --
// not new task types.
const (
	// EventSetKey registers Value under this message's own ID in the
	// node's key registry (see registry.go) -- generic, not KV-specific:
	// used both for an actual KV key (ahead of EventSetField) and for a
	// peer id (ahead of EventAdd's learner-join case).
	EventSetKey uint8 = 1
	// EventSetField performs store.Set(registry[SourceID], Value).
	EventSetField uint8 = 2
	// EventGetKey returns registry[SourceID] as Value (reverse lookup).
	EventGetKey uint8 = 3
	// EventGetField performs store.Get(key), where key is
	// registry[SourceID] if SourceID != 0, or Value itself otherwise (a
	// one-shot read needing no prior registry entry).
	EventGetField uint8 = 4
	// EventGetPublicKey returns the node's Ed25519 public key (32 bytes)
	// as Value. Accepted unsigned -- see this package's doc comment.
	EventGetPublicKey uint8 = 5
	// EventGetPrivateKey returns the node's Ed25519 private key (64
	// bytes, stdlib crypto/ed25519 format) as Value. Accepted unsigned --
	// see this package's doc comment.
	EventGetPrivateKey uint8 = 6
	// EventAdd bootstraps this node as the cluster's sole leader (Value
	// empty, SourceID 0), joins as a voter (Value = leader multiaddr to
	// dial, SourceID 0 -- the daemon already knows its own identity, so
	// nothing needs registering first), or -- when SourceID references a
	// prior EventSetKey holding the caller's own peer id -- adds the
	// caller as a non-voting learner at Value (its own reachable
	// address), the shape pkg/daemon.ClientProtocolID's remote browser
	// callers need since the target daemon has no other way to learn a
	// remote caller's identity.
	EventAdd uint8 = 7
	// EventSet is a single-round-trip alternative to the SetKey+SetField
	// pair: Value packs both the key and the value together (see
	// EncodeSetPayload/DecodeSetPayload), so the daemon can perform
	// store.Set(key, value) directly without the registry.Register/
	// Lookup round trip SetKey+SetField needs to relate two separate
	// messages via SourceID. It exists because api/shmevent.capnp's
	// Event has only one `value` field per message -- callers for whom
	// each message doesn't have a meaningful per-round-trip cost (e.g.
	// web-app's Rust client, talking over an already-open network
	// stream) have no reason to switch off SetKey+SetField; EventSet is
	// for pkg/shmclient, which pays a real, non-negligible cost (a fresh
	// shmring segment pair) per round trip. Trade-off: key and value now
	// share one ValueSize (512-byte) budget instead of each getting
	// their own -- SetKey+SetField remains the only option for a
	// combined key+value beyond that.
	EventSet uint8 = 8
	// EventPermitRequest lodges a pending system record -- today, only a
	// request for a bootstrap/relay node to be registered (KindBootstrapNode)
	// -- under a reserved key (see SystemKey) so every raft member ends up
	// knowing about it purely through ordinary FSM replication, the same
	// as any other Set. Value is EncodePermitRequestPayload(kind, peerID,
	// metadata). Any raft node may receive and relay one (it's applied
	// exactly like EventSet, via handleSetForward's existing one-hop
	// forward-to-leader path) -- see EventPermitConfirm's doc comment for
	// the second stage, which is restricted.
	//
	// Deprecated: still fully supported (additive rollout, see
	// EventLifecycleWrite's own doc comment), but new callers should send
	// EventLifecycleWrite{kind, action: LifecycleActionRequest, ...}
	// instead.
	EventPermitRequest uint8 = 9
	// EventPermitConfirm promotes a pending EventPermitRequest record from
	// pending to confirmed (see SystemKey's Status* values). Value is
	// EncodePermitConfirmPayload(kind, peerID) -- no metadata, since the
	// daemon reads the pending record's own value back out of the store
	// rather than trusting the caller to resend it. Unlike
	// EventPermitRequest, only a peer that is currently a raft *voter* may
	// confirm: any raft node can receive/relay the message, but the
	// daemon's forwarding path re-checks the *libp2p-authenticated*
	// identity of whichever node actually originated it against the
	// leader's live raft configuration before applying -- the per-message
	// Ed25519 signature alone does not prove this (see pkg/daemon's
	// handleForwardConfirmStream doc comment).
	//
	// Deprecated: still fully supported, but new callers should send
	// EventLifecycleWrite{kind, action: LifecycleActionConfirm, ...}
	// instead -- see EventLifecycleWrite's own doc comment.
	EventPermitConfirm uint8 = 10
	// EventExecute is a direct, unreplicated peer-to-peer notification: it
	// never touches the store or the raft FSM (unlike every event above),
	// it's just delivered straight to whichever node SourceID/
	// DestinationID name. SourceID references a prior EventSetKey holding
	// the *sending* node's own peer id, DestinationID references one
	// holding the *receiving* node's peer id -- both required, no
	// implicit "this node" default the way EventAdd's SourceID==0 case
	// has one. Value is an arbitrary raw payload, up to ValueSize.
	//
	// A node that receives this locally (over pkg/ipc or
	// pkg/daemon.ClientProtocolID) checks that SourceID's registered peer
	// id is genuinely its own (see handleShmEvent) -- it can only ever be
	// relaying on its own behalf, since the peer-to-peer hop that follows
	// is signed with its own key -- then dials DestinationID's peer
	// directly over pkg/daemon.ExecuteProtocolID (a fresh libp2p stream
	// between the two raft node processes, not going through raft
	// consensus at all) and hands it EncodeExecuteNotification(own peer
	// id, Value). The receiving node verifies that message's signature
	// against the sender peer id's own Ed25519 public key (embedded in
	// the peer id itself, the same trick pkg/daemon.recordClusterMember
	// uses) -- self-contained, not dependent on trusting the stream's
	// connection identity -- then queues it for local delivery: see
	// EventPollExecute.
	EventExecute uint8 = 11
	// EventPollExecute drains one queued EventExecute notification (see
	// that event's doc comment) delivered to this node, oldest first.
	// Value is ignored on the request. The response's Value is empty if
	// nothing is queued, or EncodeExecuteNotification(senderPeerID,
	// payload) if one was -- a local caller polls this in a loop (there's
	// no push transport from daemon to pkg/ipc client -- see pkg/ipc's
	// doc comment on why it's strictly request/response) to observe
	// Execute notifications addressed to this node.
	EventPollExecute uint8 = 12
	// EventPermitRevoke demotes a confirmed pkg/shmevent system record
	// (today, only KindBootstrapNode) straight back out of existence --
	// unlike EventPermitRequest/EventPermitConfirm's pending->confirmed
	// two-stage lifecycle, there's no intermediate status to pass through,
	// it just deletes the confirmed record. Value is
	// EncodePermitConfirmPayload(kind, peerID) -- the same payload shape
	// EventPermitConfirm uses, reused as-is since both only ever need
	// kind+peerID, no metadata. Same restriction as EventPermitConfirm:
	// only a peer that is currently a raft *voter* may revoke, enforced
	// the identical way (see EventPermitConfirm's doc comment and
	// pkg/daemon's handleForwardConfirmStream, which handles both).
	//
	// Deprecated: still fully supported, but new callers should send
	// EventLifecycleWrite{kind, action: LifecycleActionRevoke, ...}
	// instead -- see EventLifecycleWrite's own doc comment.
	EventPermitRevoke uint8 = 13
	// EventListRange answers a bounded key range scan against the local
	// store directly -- pkg/store.Store.ScanRange -- unlike every Set/Get
	// event, which addresses exactly one key. Value is
	// EncodeListRangeQuery(start, end) (start/end both inclusive, the same
	// framing EncodeSetPayload uses since both only ever pack two
	// length-delimited byte strings). Answered locally by whichever node
	// receives it -- like EventGetField, it needs no raft-leader
	// forwarding, since reads don't require leader routing the way writes
	// do. There is no bulk response: the daemon returns only the first
	// matching pair still within [start, end] via
	// EncodeListRangeQuery(key, value) (the identical framing again, just
	// naming the fields differently since this response packs a result
	// pair, not a query bound), or an empty Value if none remain. A local
	// caller wanting every match in the range polls this in a loop, each
	// time narrowing start to just past the previous response's key --
	// see pkg/logrecord for the key scheme this is designed around and
	// EventPollExecute's doc comment for the same "loop rather than a
	// bulk/push response" reasoning applied here.
	EventListRange uint8 = 14
	// EventLogAppend writes one pkg/logrecord record -- a single ordinary
	// Set/handleSetForward call under the hood (the same raft-replicated
	// path EventSet uses), except it addresses pkg/logrecord's reserved
	// LogKeyPrefix namespace, which EventSet/EventSetField deliberately
	// refuse (see pkg/daemon's rejectReservedKey) so an ordinary caller's
	// key can never collide with a log record by accident. This event is
	// that namespace's one legitimate way in -- mirroring how
	// EventPermitRequest is SystemKeyPrefix's, both bypassing the
	// generic EventSet/EventSetField rejection by being a different case
	// entirely, one whose whole job is writing into that specific
	// reserved space. Value is EncodeSetPayload(key, value) (the same
	// framing EventSet already uses), and the daemon rejects it if key
	// doesn't actually start with LogKeyPrefix -- this event isn't a
	// general-purpose Set bypass, only a log-record one.
	EventLogAppend uint8 = 15
	// EventLeave asks the raft cluster this node currently belongs to to
	// remove it (raft.RemoveServer) -- the graceful counterpart to
	// EventAdd's join: unlike EventAdd, which can name any leader/cluster
	// to join, EventLeave takes no Value at all, since there's only ever
	// one cluster this node's own live raft handle currently belongs to.
	// It shrinks the remote cluster's member count but never tears it
	// down -- the remaining voters keep operating normally, the same
	// fault tolerance hashicorp/raft already provides for any minority of
	// members going offline. Applied directly if this node is the
	// leader, or forwarded one hop to whoever is (see
	// pkg/daemon.ForwardLeaveProtocolID) -- mirroring
	// EventPermitConfirm/EventPermitRevoke's forwarding shape, since a
	// leaving node -- unlike a brand new joiner -- is already a member
	// with its own live raft handle and leader-tracking. Local-only: a
	// remote (ClientProtocolID) caller has no legitimate use for it,
	// since only this node's own operator decides to leave.
	EventLeave uint8 = 19
	// EventGroupPut/EventGroupDelete, EventCommandPut/EventCommandDelete,
	// EventGroupCommandPut/EventGroupCommandDelete, and
	// EventPeerGroupPut/EventPeerGroupDelete implement direct,
	// single-step CRUD (create and update both collapse into one Put,
	// overwriting whatever was at that key -- see
	// shmevent.KindGroup's doc comment) for the group-based ACL catalog:
	// any single current raft voter may Put or Delete one of these
	// records unilaterally, no second-voter confirmation, reusing
	// EventPermitConfirm/EventPermitRevoke's existing voter-gated
	// forwarding machinery (pkg/daemon's handleConfirmForward, widened to
	// also accept kvfsm.OpSet) rather than the two-stage
	// pending->confirmed pattern EventPermitRequest/EventPermitConfirm
	// use. Put's Value is EncodeGroupPayload(name)/EncodeCommandPayload
	// (name, peerID) -- Group/Command's key already carries id
	// (GroupKey/CommandKey); GroupCommand/PeerGroup Put carries no
	// payload at all, since both fields of the relation are already in
	// the key (GroupCommandKey(commandID, groupID)/
	// PeerGroupKey(peerID, groupID)) and there's nothing else to store.
	// Delete's Value is the same key-encoding each kind's Put used to
	// build its key from. Deleting a Group or a Command additionally
	// cascades: every GroupCommand/PeerGroup record referencing the
	// deleted id is removed in the same raft Apply (see
	// pkg/kvfsm.OpCascadeDelete), so a delete never leaves a dangling
	// relation behind.
	//
	// Deprecated: all ten of these (through EventPeerGroupDelete below)
	// are still fully supported, but new callers should send
	// EventCatalogPut/EventCatalogDelete{kind: Kind*, ...} instead -- see
	// EventCatalogPut's own doc comment.
	EventGroupPut uint8 = 20
	// Deprecated: see EventGroupPut's doc comment.
	EventGroupDelete uint8 = 21
	// Deprecated: see EventGroupPut's doc comment.
	EventCommandPut uint8 = 22
	// Deprecated: see EventGroupPut's doc comment.
	EventCommandDelete uint8 = 23
	// Deprecated: see EventGroupPut's doc comment.
	EventGroupCommandPut uint8 = 24
	// Deprecated: see EventGroupPut's doc comment.
	EventGroupCommandDelete uint8 = 25
	// Deprecated: see EventGroupPut's doc comment.
	EventPeerGroupPut uint8 = 26
	// Deprecated: see EventGroupPut's doc comment.
	EventPeerGroupDelete uint8 = 27
	// EventJoinInviteCreate lodges a one-time shmevent.KindJoinInvite
	// record (Value: EncodeJoinInviteCreatePayload) -- a direct write, no
	// pending/confirmed lifecycle, same as EventGroupPut, gated on the
	// caller being a current raft voter the same way. EventJoinInviteRevoke
	// deletes one outright (Value: EncodeJoinInviteRevokePayload) before
	// it's ever redeemed, mirroring EventGroupDelete. Neither is what
	// actually admits a new device -- that happens inside handleJoinStream
	// when a join request presents a still-valid token (see
	// pkg/kvfsm.OpConsumeInvite), entirely outside this event pair.
	//
	// Deprecated: both of these are still fully supported, but new callers
	// should send EventLifecycleWrite{kind: KindJoinInvite, action:
	// LifecycleActionRequest or LifecycleActionRevoke, ...} instead -- see
	// EventLifecycleWrite's own doc comment.
	EventJoinInviteCreate uint8 = 28
	// Deprecated: see EventJoinInviteCreate's doc comment.
	EventJoinInviteRevoke uint8 = 29
	// EventExecInviteCreate/EventExecInviteRevoke are
	// EventJoinInviteCreate/EventJoinInviteRevoke's counterpart for a
	// one-time execution invite (shmevent.KindExecInvite): a direct write,
	// voter-gated the identical way, binding a random token to a
	// commandID+inputsJSON pair instead of a suffrage grant. Create's
	// Value is EncodeExecInviteCreatePayload, Revoke's is
	// EncodeExecInviteRevokePayload. Neither actually triggers execution --
	// that's EventExecInviteRedeem below.
	//
	// Deprecated: both of these are still fully supported, but new callers
	// should send EventLifecycleWrite{kind: KindExecInvite, action:
	// LifecycleActionRequest or LifecycleActionRevoke, ...} instead -- see
	// EventLifecycleWrite's own doc comment.
	EventExecInviteCreate uint8 = 30
	// Deprecated: see EventExecInviteCreate's doc comment.
	EventExecInviteRevoke uint8 = 31
	// EventExecInviteRedeem is local-only (pkg/daemon rejects it from a
	// remote/ClientProtocolID caller -- see handleShmEvent): it tells this
	// node's own daemon "dial sourceAddr and redeem token on my behalf,"
	// Value is EncodeExecInviteRedeemRequest(sourceAddr, token). The
	// daemon does the actual network work itself (dialing
	// pkg/daemon.ExecInviteRedeemProtocolID, signing a self-contained
	// redeem message with this node's own key -- see
	// EncodeExecuteNotification, reused for that wire message -- the same
	// way EventExecute's own notification is self-contained rather than
	// depending on the connection's identity) and returns the new
	// instance id as Value on success. The actual atomic ACL-check+consume
	// (pkg/kvfsm.OpConsumeExecInvite) happens on whichever node turns out
	// to be the current raft leader, entirely outside this local event.
	EventExecInviteRedeem uint8 = 32
	// EventJoinRequestCreate/EventJoinRequestCancel are the reverse of
	// EventJoinInviteCreate/Revoke: instead of an existing cluster voter
	// minting a token that admits an outside device, these let a device
	// with no cluster (and possibly no raft instance at all) mint a
	// one-time token naming *itself*, for some other cluster's voter to
	// redeem via EventRecruit -- see that event's doc comment for the full
	// flow. Unlike EventJoinInviteCreate/Revoke, there's no legitimate
	// remote use of either: pkg/daemon rejects both outright from a
	// remote/ClientProtocolID caller, the same way EventExecInviteRedeem
	// is local-only. Neither touches raft or the store -- the token lives
	// only in the daemon process's own memory (see pkg/daemon's
	// joinRequestToken field) until consumed exactly once by
	// handleRecruitStream or explicitly cancelled. Create takes no Value
	// and returns the new token (raw bytes) as Value; Cancel's Value is
	// the token to clear, a no-op if it no longer matches (already
	// consumed or superseded by a later Create).
	EventJoinRequestCreate uint8 = 33
	EventJoinRequestCancel uint8 = 34
	// EventRecruit is local-only (same rejection as EventExecInviteRedeem):
	// it tells this node's own daemon "mint a normal join invite on my own
	// cluster, then dial the device named in ticket and hand it that
	// invite," where ticket is "<device's own multiaddr>#<tokenHex>" -- the
	// EventJoinRequestCreate token above, in the exact same
	// "<addr>#<tokenHex>" shape splitInviteToken already expects
	// everywhere else in this codebase. Value is
	// EncodeRecruitPayload(ticket, suffrage); suffrage is the same
	// raft.Voter/raft.Nonvoter byte EventJoinInviteCreate's suffrage
	// argument already is. The daemon mints the invite itself
	// (mintJoinInvite, the same handleConfirmForward write
	// EventJoinInviteCreate's case makes) and dials
	// pkg/daemon.RecruitProtocolID directly at the device's own address --
	// see dialAndPushRecruit/handleRecruitStream. Returns the redeemed
	// device's own join result ("<peerID> ok"/"<peerID> pending", same as
	// EventAdd) as Value on success.
	EventRecruit uint8 = 35
	// EventGetOwnAddr returns this node's own current best-advertised
	// multiaddr (Value: pkg/daemon's advertisedAddrs()[0] -- public first,
	// then a /p2p-circuit relay reservation, then any other address,
	// loopback last) as a plain UTF-8 string. Queried live on every call,
	// never cached: a node with Config.RelayPeers set gets its circuit
	// reservation asynchronously in the background after startup (see
	// join's own doc comment on awaitRelayAddr), so the address available
	// right after EventJoinRequestCreate may not yet be the relayed one --
	// this exists so an operator building a "<ownAddr>#<tokenHex>" ticket
	// (or any other caller of advertisedAddrs()[0], e.g. printjoininvitedatamatrix's
	// leaderMultiaddr) can just query again a moment later instead of
	// guessing or constructing the circuit address by hand.
	EventGetOwnAddr uint8 = 36
	// EventChannelOpen opens a persistent, unreplicated, bidirectional,
	// multipurpose stream to another peer -- the persistent-session
	// counterpart to EventExecute's one-shot notification (see that
	// event's doc comment for the shared self-contained-signature/
	// permit-gate design this reuses). Value is the target peer id, as a
	// plain string. The daemon dials pkg/daemon.ChannelProtocolID,
	// performs a signed handshake (identical in spirit to
	// EncodeExecuteNotification), and on acceptance registers a live
	// channel session backed by that libp2p stream. Unlike the handshake
	// alone, every message exchanged afterward over that stream is also a
	// full signed shmevent.Event frame -- see EventChannelSend's doc
	// comment. Local-only, like EventLeave/EventRecruit -- a remote
	// (ClientProtocolID) caller has no legitimate use for opening a
	// channel on this node's behalf. Response Value is a freshly minted
	// opaque channelID string: a pure local handle this node chose for
	// its own bookkeeping, never something the two peers need to agree on
	// -- see EventChannelSend/Poll/Close, which all take it as their own
	// Value.
	EventChannelOpen uint8 = 37
	// EventChannelSend writes one chunk of bytes to an already-open
	// channel (see EventChannelOpen), tagged with a purpose (see
	// ChannelPurposeData/Control/Video -- a caller can freely interleave
	// more than one purpose over the same channel, e.g. a control message
	// alongside a bulk data transfer, or a video frame). Value is
	// EncodeChannelSendPayload(channelID, purpose, chunk). Unlike a raw
	// pipe, chunk boundaries are preserved end to end: the daemon signs
	// purpose+chunk (shmevent.SignChannelChunk) and writes them as one
	// length-framed shmevent.EncodeChannelFrame message directly onto the
	// underlying libp2p stream -- the receiving daemon verifies that
	// signature against the sender's own Ed25519 public key
	// (shmevent.VerifyChannelChunk, exactly like EventChannelOpen's
	// handshake verifies its own sender) before ever buffering the chunk
	// for EventChannelPoll to drain, so a forged or corrupted frame is
	// rejected per-message, not just at handshake time. This is this
	// package's own request/response IPC path -- a per-chunk round trip,
	// capped at ChannelValueSize -- kept for any caller that wants a
	// synchronous "this call returning means the daemon has it" guarantee
	// per chunk, or has no reason to bother with pkg/chandata; every
	// pkg/shmclient caller (pkg/kvctl, kvmobile) instead sends
	// EventChannelDataReady once per channel and moves its real chunk
	// traffic onto that package's own much higher-throughput ring pair --
	// see EventChannelDataReady's doc comment. Either path ends up
	// signed/framed onto the wire the same way (channelSession.write is
	// shared), so a peer receiving a channel's traffic never needs to
	// know or care which path the sender chose per chunk. Local-only.
	EventChannelSend uint8 = 38
	// EventChannelPoll drains one buffered chunk received on channelID
	// (Value, a plain string) since the last poll, oldest first -- the
	// same "no push transport, a local caller loops this" design
	// EventPollExecute already uses, for the identical reason (see that
	// event's doc comment). Response Value is
	// EncodeChannelPollResponse(status, purpose, chunk): status is
	// ChannelPollNoData (nothing new yet, channel still open),
	// ChannelPollChunk (purpose and chunk follow), or ChannelPollClosed
	// (the channel has ended -- returned idempotently on every poll after
	// any already-buffered chunks are drained, never an error, so a
	// caller that stops polling immediately after seeing it once doesn't
	// miss anything; purpose is ChannelPurposeData and chunk empty for
	// both ChannelPollNoData and ChannelPollClosed). Local-only.
	EventChannelPoll uint8 = 39
	// EventChannelListen drains one pending *incoming* channel this node
	// has accepted (via another peer's EventChannelOpen dialing in) but
	// that no local caller has claimed yet -- the callee-side counterpart
	// to EventChannelOpen, polled in a loop the same way EventChannelPoll
	// is. Value is ignored on the request. Response Value is empty if
	// none are pending, else EncodeChannelAccept(channelID,
	// remotePeerID): channelID is this node's own local handle (as usual,
	// not shared with the remote peer -- see EventChannelOpen), from here
	// on usable with EventChannelSend/Poll/Close exactly like one this
	// node opened itself. Local-only.
	EventChannelListen uint8 = 40
	// EventChannelClose ends channelID (Value, a plain string) outright:
	// closes the underlying stream (unblocking any goroutine blocked
	// reading it, same as every other stream in this package's
	// `defer s.Close()` idiom) and forgets the session. Not required for
	// a channel that already reached ChannelPollClosed naturally, but
	// frees it immediately instead of waiting for the daemon's own
	// opportunistic idle reap. Local-only.
	EventChannelClose uint8 = 41
	// EventChannelCloseWrite half-closes channelID's (Value, a plain
	// string) *outgoing* direction only -- "I have nothing more to send,"
	// not "end the channel outright" (that's EventChannelClose). Mirrors
	// a raw TCP connection's shutdown(SHUT_WR): the remote peer's own
	// pumpChannelReads sees this as a normal EOF on its read side and
	// reports ChannelPollClosed once its own already-buffered chunks are
	// drained, exactly like a full close would look from that side, but
	// this node can still receive (and its own operator can still be
	// mid-typing when the other side finishes first). A caller whose
	// local input source (e.g. os.Stdin) reaches a clean EOF should send
	// this rather than EventChannelClose, then keep polling for whatever
	// the remote peer still has left to send -- only calling
	// EventChannelClose once its own receiving side has also finished
	// (ChannelPollClosed) or it's giving up early (e.g. SIGINT). Value is
	// ignored on the response. If this channelID has an EventChannelDataReady
	// ring in play, the response is deliberately delayed (not just a fire-
	// and-forget signal) until every chunk already buffered in that ring
	// has actually been forwarded onto the wire -- see
	// EventChannelDataReady's doc comment -- so a caller that only ever
	// sends through the ring can still rely on "this call returned" to
	// mean "everything I wrote is genuinely on the wire," the same
	// guarantee the plain EventChannelSend path always had for free by
	// virtue of being synchronous per chunk.
	EventChannelCloseWrite uint8 = 42
	// EventKick force-removes an arbitrary peer from the raft cluster
	// this node currently belongs to (raft.RemoveServer), without that
	// peer's own cooperation -- the administrative counterpart to
	// EventLeave, for when a member has genuinely gone down for good
	// (crashed, wiped, never coming back) and an operator wants to
	// restore the cluster's ability to make progress rather than wait
	// for it to rejoin on its own (e.g. a lost voter majority). Value is
	// the target peer id (a plain string, not a multiaddr -- there's
	// nothing to dial, just a raft configuration entry to drop). Unlike
	// EventLeave (self-only, no authorization needed), removing
	// *another* peer is privileged: only a currently confirmed raft
	// voter may do it -- enforced the same way EventPermitConfirm's doc
	// comment describes (a local pkg/ipc operator on a voter's own
	// machine is trusted implicitly; a remote ClientProtocolID caller
	// must be an authenticated voter itself). Applied directly if this
	// node is the leader, or forwarded one hop to whoever is
	// (pkg/daemon.ForwardKickProtocolID), mirroring EventLeave's own
	// forwarding shape.
	EventKick uint8 = 43
	// EventTxn atomically applies an ordered list of plain Set/Delete
	// ops -- Value is EncodeTxnPayload(ops) (see TxnOp/EncodeTxnPayload/
	// DecodeTxnPayload) -- as a single raft log entry: every op lands or
	// none do, the same "one log entry -> one kvfsm.Apply call" guarantee
	// that already makes a single EventSet atomic (see kvfsm.OpTxn's doc
	// comment), just widened to more than one key. pkg/daemon rejects the
	// whole request if any op's key starts with a reserved namespace byte
	// (shmevent.SystemKeyPrefix/logrecord.LogKeyPrefix), the same
	// rejectReservedKey check EventSet/EventSetField already apply per
	// key -- EventTxn is a plain-KV feature only, not a way to batch
	// writes into the Group/Command catalog or any other reserved
	// namespace. Applied via handleOpForward exactly like EventSet
	// (forwarded one hop to the leader if this node isn't it). Like
	// EventSet, every op's key and value together share ops' one Value
	// (ValueSize, 512 bytes) budget -- a transaction is a handful of
	// small ops, not a bulk-write mechanism.
	EventTxn uint8 = 44
	// EventChannelDataReady is the handshake that hands an already-open
	// channel (EventChannelOpen/Listen) off to pkg/chandata's high-
	// throughput data plane: Value is channelID (a plain string), sent
	// once the local caller has already itself created its own outgoing
	// shared-memory ring (pkg/chandata.Create with pkg/chandata.DirUp) for
	// this channelID -- see that package's doc comment for the full
	// design, and pkg/daemon.ChannelProtocolID's for why bulk channel
	// traffic moves off this event-request/response IPC path entirely
	// once this handshake completes. The daemon opens that same ring as a
	// reader (pkg/chandata.Open, DirUp) and starts forwarding every chunk
	// it yields onto the underlying libp2p stream (signed, framed,
	// exactly like the direct EventChannelSend path always has) until the
	// ring reaches EOF or the channel closes; the *incoming* direction
	// needs no equivalent handshake, since the daemon always creates that
	// ring (DirDown) itself, synchronously, before EventChannelOpen/
	// EventChannelListen's own response ever reaches the caller with a
	// channelID to open it by. This makes ring readiness a fully
	// synchronous, race-free precondition for EventChannelCloseWrite's
	// drain-then-half-close behavior (see that event's doc comment): by
	// the time this call returns, dispatchChannelCloseWrite can rely on
	// the ring's presence with no ambiguity about whether one is coming.
	// Sending this is optional -- a caller that never does stays on the
	// plain EventChannelSend/Poll path exactly as before -- but
	// pkg/shmclient.Session always sends it immediately after
	// OpenChannel/ListenChannel resolves a channelID. Local-only.
	EventChannelDataReady uint8 = 45
	// EventGetVersion returns this node's own build/version info (Value:
	// EncodeVersionInfo(VersionInfo) -- git commit, dirty flag, build time,
	// Go version, and the pinned github.com/libp2p/go-libp2p dependency
	// version, gathered live from runtime/debug.ReadBuildInfo each call, see
	// pkg/daemon's currentBuildInfo). Answered locally by whichever node
	// receives it, no store/raft access at all -- same "queried live, never
	// cached" reasoning as EventGetOwnAddr, and the same always-open
	// treatment as EventGetPublicKey/EventGetPrivateKey's "Accepted
	// unsigned" bootstrap carve-out, except here there's no secret being
	// handed out, so unlike those two this is deliberately left reachable
	// for a *remote* (ClientProtocolID) caller too, including one with no
	// standing whatsoever: build/version info is exactly the kind of thing
	// shmevent.DefaultPublicGroupID's "public" group already exists to make
	// available to a total stranger with no other door into a cluster (see
	// that constant's doc comment), but unlike shmevent.DefaultPublicCommandID
	// this needs no SubmitCommand/CommandRequest round trip through raft --
	// it's a plain, side-effect-free read of this one node's own local
	// process state, not a cluster fact anything needs to agree on -- so it
	// is answered directly, the same way EventGetOwnAddr already is, rather
	// than added to the Group/Command catalog.
	EventGetVersion uint8 = 46
	// EventPublicAccess asks this node to submit DefaultPublicCommandID --
	// the always-public front door every cluster bootstraps (see that
	// constant's doc comment) -- to *another* cluster, named by Value: a
	// multiaddr, or a bare peer id this machine's local registry can
	// resolve, exactly the address forms EventExecInviteRedeem's own
	// sourceAddr already accepts. Value may optionally carry a "#"-separated
	// second half, an opaque UTF-8 metadata string stored as the request's
	// "note" field, for a submitter that wants to say who/what it is.
	//
	// Local-only, and the mirror image of every other event here: the rest
	// of this protocol acts on the node receiving it, while this one makes
	// the receiving node act as a *client* of a cluster it has no standing
	// in whatsoever. It exists because the escalation path
	// DefaultPublicCommandID was designed for had, until now, no client
	// side anywhere in this repo -- pkg/shmclient.Session only ever talks to
	// its own local daemon, so a device that needs a stranger cluster's
	// relay or Channel service (an app phone reserving a circuit-relay v2
	// slot on a bootstrap node it isn't a member of, the case
	// configs/bootstrap-nodes.json exists for) had no way to ask for it
	// short of an operator running mage addpeertogroup by hand, per device.
	// pkg/daemon's dialAndSubmitPublicAccess is the handler: it connects to
	// the target and sends a single signed EventLogAppend over
	// ClientProtocolID carrying a CommandRequestLogKind record for
	// DefaultPublicCommandID -- byte for byte what SubmitCommand writes
	// locally -- which the target's isCommandLogCarveOut admits from an
	// unknown caller and kvfsm.Apply answers by granting this node
	// ReservedGroupChannel + ReservedGroupRelay membership in that cluster.
	//
	// Response Value is the new dispatch's instance id (hex), the same id
	// SubmitCommand returns, usable against the target's own
	// GetCommandRequest/QueryCommandLog read-back carve-out.
	EventPublicAccess uint8 = 47
	// EventExecTicket never travels over shmring or the network wire the
	// way every event above does -- Encode/Decode/Sign/Verify already
	// work standalone on any Msg, with no live session involved, so this
	// reuses them as the format for a self-contained, offline-verifiable
	// exec-invite ticket instead of inventing a parallel encoding: Value
	// is EncodeExecTicketPayload(sourceAddr, token) (byte-identical to
	// EncodeExecInviteRedeemRequest -- see that function's doc comment),
	// and the whole message is signed exactly like any other Event.
	// pkg/kvctl.CreateExecInviteTicket builds one client-side (the local
	// caller already holds the same signing key the node's own identity
	// uses -- see this package's doc comment) for a DataMatrix code;
	// pkg/kvctl.RedeemExecInviteTicket decodes it, extracts the issuing
	// peer id from sourceAddr's own trailing /p2p/<peerID> component
	// (self-certifying -- no separate issuer field needed), and verifies
	// the signature against that peer id's own public key before ever
	// dialing anything. A daemon that ever received one live would
	// reject it exactly like any other unrecognized event (see
	// pkg/daemon's handleShmEvent default case) -- nothing needs to
	// special-case this type to stay safe.
	EventExecTicket uint8 = 48
	// EventJoinTicket is EventExecTicket's join-invite counterpart: Value
	// is EncodeJoinTicketPayload(sourceAddr, token) (see that function's
	// doc comment), signed and verified exactly the same way. Unlike
	// EventExecTicket, redeeming one isn't a separate IPC call --
	// join-invite redemption is already baked into `mage addfollower`/
	// `mage addnode`/mobile/kvmobile.Join's own daemon-startup flow
	// (pkg/daemon's splitInviteToken), which only ever wants the plain
	// "<addr>#<tokenHex>" string -- so pkg/kvctl.VerifyJoinInviteTicket
	// verifies the signature and hands back exactly that string, unlike
	// RedeemExecInviteTicket, which also dials on the caller's behalf.
	// Never travels over shmring or the network wire, same as
	// EventExecTicket -- see that constant's doc comment for why that's
	// safe by construction.
	EventJoinTicket uint8 = 49
	// EventJoinRequestTicket is EventJoinTicket's reverse-direction
	// counterpart: EventJoinInviteCreate/EventJoinTicket are minted by an
	// existing cluster's voter to admit a device it already knows how to
	// reach, while a join-request ticket is minted by the requesting
	// device itself (CreateJoinRequest) and redeemed by a voter on some
	// other cluster it wants to join (RecruitPeer/EventRecruit) -- so the
	// signature here proves the ticket really was produced by the
	// requesting device, the opposite direction of trust from
	// EventJoinTicket. Value is the same (sourceAddr, token) shape --
	// EncodeJoinTicketPayload/DecodeJoinTicketPayload are reused as-is,
	// since join-request tokens are JoinInviteTokenSize too (see
	// DecodeJoinRequestCancelPayload) and the wire shape is identical;
	// no separate Encode/DecodeJoinRequestTicketPayload pair exists.
	// pkg/kvctl.RedeemJoinRequestTicket verifies then calls
	// sess.Recruit(...) itself, the same "verify then act" shape
	// RedeemExecInviteTicket uses -- unlike VerifyJoinInviteTicket, which
	// only hands back a string for a separate addfollower/addnode call,
	// since RecruitPeer (unlike joining via addfollower) already dials
	// and completes the join in one round trip.
	EventJoinRequestTicket uint8 = 50
	// EventStationPut creates or updates the KindStation record describing
	// one device (Value is EncodeStationPutPayload(peerID, name, attrs)),
	// EventStationDelete removes it (Value is the peer id). Voter-gated and
	// applied through handleConfirmForward exactly like EventCommandPut --
	// this is the same catalog CRUD, widened to one more kind, not a new
	// mechanism. Deleting a station removes only its description: the
	// device's cluster membership (KindClusterMember), group memberships
	// and any command targeting it are untouched, since none of them
	// depend on a station record existing.
	//
	// Deprecated: both of these are still fully supported, but new callers
	// should send EventCatalogPut/EventCatalogDelete{kind: KindStation,
	// ...} instead -- see EventCatalogPut's own doc comment.
	EventStationPut uint8 = 51
	// Deprecated: see EventStationPut's doc comment.
	EventStationDelete uint8 = 52
	// EventCatalogPut/EventCatalogDelete are one generic envelope pair
	// covering the same ground as EventGroupPut/Delete, EventCommandPut/
	// Delete, EventGroupCommandPut/Delete, EventPeerGroupPut/Delete, and
	// EventStationPut/Delete above -- added additively, alongside those
	// ten (unchanged, still fully functional) events, not in place of
	// them; see this const block's own doc comment on why new catalog
	// kinds belong here going forward. Value is
	// EncodeCatalogPayload(kind, inner): kind is one of KindGroup/
	// KindCommand/KindGroupCommand/KindPeerGroup/KindStation (the same
	// byte each record's own SystemKey already carries), inner is that
	// kind's existing Put/Delete payload, completely unchanged --
	// EncodeGroupPutPayload/EncodeCommandPutPayloadWithSpec/
	// EncodeStationPutPayload/EncodeGroupCommandPayload/
	// EncodePeerGroupPayload for EventCatalogPut, and whatever each kind's
	// own Delete case already used as Msg.Value for EventCatalogDelete.
	// Voter-gated and forwarded through handleConfirmForward exactly like
	// the ten events it generalizes -- pkg/daemon's catalogPutSpecs/
	// catalogDeleteSpecs select the right existing decode/key/op by kind,
	// so no per-kind logic is duplicated, only dispatched to.
	EventCatalogPut    uint8 = 53
	EventCatalogDelete uint8 = 54
	// EventLifecycleWrite is one generic envelope covering
	// EventPermitRequest/Confirm/Revoke, EventJoinInviteCreate/Revoke, and
	// EventExecInviteCreate/Revoke -- seven events sharing the identical
	// "decode payload, build one or two SystemKey-shaped keys, apply via
	// handleSetForward or handleConfirmForward" reduction -- added
	// additively, alongside those seven (unchanged, still fully
	// functional) events, not in place of them. EventJoinRequestCreate/
	// Cancel and EventExecInviteRedeem are deliberately NOT folded in
	// here: the first two are local in-memory ticket ops with no key and
	// no raft write at all, and the third is a network dial to another
	// node, not a decode-key-op reduction -- forcing them into this
	// envelope would blur a real semantic difference for no duplication
	// savings.
	//
	// Value is EncodeLifecycleWritePayload(kind, action, inner): kind is
	// KindJoinInvite, KindExecInvite, or (for the Permit-style family,
	// which is generic over kind the same way EventPermitRequest already
	// is) any other kind byte such as KindBootstrapNode/KindClusterJoin;
	// action is LifecycleActionRequest (also doubling as "Create" for
	// JoinInvite/ExecInvite, which have no separate confirm stage),
	// LifecycleActionConfirm (Permit-style only), or
	// LifecycleActionRevoke. inner is that (kind, action) pair's existing
	// payload, completely unchanged -- EncodePermitRequestPayload/
	// EncodePermitConfirmPayload/EncodeJoinInviteCreatePayload/
	// EncodeJoinInviteRevokePayload/EncodeExecInviteCreatePayload/
	// EncodeExecInviteRevokePayload. pkg/daemon's permitLifecycleSpecs/
	// inviteLifecycleSpecs select the right existing decode/key/op by
	// (kind, action), including which of the two forwarding paths
	// applies: EventPermitRequest's own "any raft node may relay, no
	// voter check anywhere" (handleSetForward) for Permit-style Request,
	// versus the voter-gated handleConfirmForward every other action
	// here already used.
	EventLifecycleWrite uint8 = 55
	// EventError is response-only: Value carries a UTF-8 error message,
	// ID echoes the failed request's ID. Not part of the fields the
	// protocol was specified with -- added because the struct has no
	// separate status field, and errors need some way to be reported;
	// see this package's doc comment.
	EventError uint8 = 255
)

// EventName returns a human-readable name for e, for logging -- "unknown"
// for anything not defined above.
func EventName(e uint8) string {
	switch e {
	case EventSetKey:
		return "set_key"
	case EventSetField:
		return "set_field"
	case EventGetKey:
		return "get_key"
	case EventGetField:
		return "get_field"
	case EventGetPublicKey:
		return "get_public_key"
	case EventGetPrivateKey:
		return "get_private_key"
	case EventAdd:
		return "add"
	case EventSet:
		return "set"
	case EventPermitRequest:
		return "permit_request"
	case EventPermitConfirm:
		return "permit_confirm"
	case EventExecute:
		return "execute"
	case EventPollExecute:
		return "poll_execute"
	case EventPermitRevoke:
		return "permit_revoke"
	case EventListRange:
		return "list_range"
	case EventLogAppend:
		return "log_append"
	case EventLeave:
		return "leave"
	case EventGroupPut:
		return "group_put"
	case EventGroupDelete:
		return "group_delete"
	case EventCommandPut:
		return "command_put"
	case EventCommandDelete:
		return "command_delete"
	case EventStationPut:
		return "station_put"
	case EventStationDelete:
		return "station_delete"
	case EventGroupCommandPut:
		return "group_command_put"
	case EventGroupCommandDelete:
		return "group_command_delete"
	case EventPeerGroupPut:
		return "peer_group_put"
	case EventPeerGroupDelete:
		return "peer_group_delete"
	case EventJoinInviteCreate:
		return "join_invite_create"
	case EventJoinInviteRevoke:
		return "join_invite_revoke"
	case EventExecInviteCreate:
		return "exec_invite_create"
	case EventExecInviteRevoke:
		return "exec_invite_revoke"
	case EventExecInviteRedeem:
		return "exec_invite_redeem"
	case EventJoinRequestCreate:
		return "join_request_create"
	case EventJoinRequestCancel:
		return "join_request_cancel"
	case EventRecruit:
		return "recruit"
	case EventGetOwnAddr:
		return "get_own_addr"
	case EventChannelOpen:
		return "channel_open"
	case EventChannelSend:
		return "channel_send"
	case EventChannelPoll:
		return "channel_poll"
	case EventChannelListen:
		return "channel_listen"
	case EventChannelClose:
		return "channel_close"
	case EventChannelCloseWrite:
		return "channel_close_write"
	case EventKick:
		return "kick"
	case EventTxn:
		return "txn"
	case EventChannelDataReady:
		return "channel_data_ready"
	case EventGetVersion:
		return "get_version"
	case EventPublicAccess:
		return "public_access"
	case EventExecTicket:
		return "exec_ticket"
	case EventJoinTicket:
		return "join_ticket"
	case EventJoinRequestTicket:
		return "join_request_ticket"
	case EventCatalogPut:
		return "catalog_put"
	case EventCatalogDelete:
		return "catalog_delete"
	case EventLifecycleWrite:
		return "lifecycle_write"
	case EventError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", e)
	}
}

// EventFromName is the inverse of EventName: it returns the event type byte
// for one of the names EventName produces ("set_key", "set_field", ...),
// and false if name isn't recognized. Used to parse a human-authored event
// name (e.g. from test/e2e/testdata.json or kvctl-cli sendevent's JSON
// argument) back into the wire byte.
func EventFromName(name string) (uint8, bool) {
	switch name {
	case "set_key":
		return EventSetKey, true
	case "set_field":
		return EventSetField, true
	case "get_key":
		return EventGetKey, true
	case "get_field":
		return EventGetField, true
	case "get_public_key":
		return EventGetPublicKey, true
	case "get_private_key":
		return EventGetPrivateKey, true
	case "add":
		return EventAdd, true
	case "set":
		return EventSet, true
	case "permit_request":
		return EventPermitRequest, true
	case "permit_confirm":
		return EventPermitConfirm, true
	case "execute":
		return EventExecute, true
	case "poll_execute":
		return EventPollExecute, true
	case "permit_revoke":
		return EventPermitRevoke, true
	case "list_range":
		return EventListRange, true
	case "log_append":
		return EventLogAppend, true
	case "leave":
		return EventLeave, true
	case "group_put":
		return EventGroupPut, true
	case "group_delete":
		return EventGroupDelete, true
	case "command_put":
		return EventCommandPut, true
	case "command_delete":
		return EventCommandDelete, true
	case "station_put":
		return EventStationPut, true
	case "station_delete":
		return EventStationDelete, true
	case "group_command_put":
		return EventGroupCommandPut, true
	case "group_command_delete":
		return EventGroupCommandDelete, true
	case "peer_group_put":
		return EventPeerGroupPut, true
	case "peer_group_delete":
		return EventPeerGroupDelete, true
	case "join_invite_create":
		return EventJoinInviteCreate, true
	case "join_invite_revoke":
		return EventJoinInviteRevoke, true
	case "exec_invite_create":
		return EventExecInviteCreate, true
	case "exec_invite_revoke":
		return EventExecInviteRevoke, true
	case "exec_invite_redeem":
		return EventExecInviteRedeem, true
	case "join_request_create":
		return EventJoinRequestCreate, true
	case "join_request_cancel":
		return EventJoinRequestCancel, true
	case "recruit":
		return EventRecruit, true
	case "get_own_addr":
		return EventGetOwnAddr, true
	case "channel_open":
		return EventChannelOpen, true
	case "channel_send":
		return EventChannelSend, true
	case "channel_poll":
		return EventChannelPoll, true
	case "channel_listen":
		return EventChannelListen, true
	case "channel_close":
		return EventChannelClose, true
	case "channel_close_write":
		return EventChannelCloseWrite, true
	case "kick":
		return EventKick, true
	case "txn":
		return EventTxn, true
	case "channel_data_ready":
		return EventChannelDataReady, true
	case "get_version":
		return EventGetVersion, true
	case "public_access":
		return EventPublicAccess, true
	case "exec_ticket":
		return EventExecTicket, true
	case "join_ticket":
		return EventJoinTicket, true
	case "join_request_ticket":
		return EventJoinRequestTicket, true
	case "catalog_put":
		return EventCatalogPut, true
	case "catalog_delete":
		return EventCatalogDelete, true
	case "lifecycle_write":
		return EventLifecycleWrite, true
	case "error":
		return EventError, true
	default:
		return 0, false
	}
}

// ValueSize is the maximum length of Msg.Value this package enforces (a
// convention, not a capnp schema constraint -- see api/shmevent.capnp's
// doc comment on Event.value) for every event type except
// EventChannelSend/EventChannelPoll -- see ChannelValueSize.
const ValueSize = 512

// ChannelValueSize is EventChannelSend/EventChannelPoll's own, much
// larger analogue of ValueSize -- a channel chunk needs to move real bulk
// data (a file transfer, a video frame), where every other event's Value
// is a handful of small fields (a key, a peer id, a permit record) that
// have no reason to grow. Kept independent of ValueSize rather than
// raising that shared constant so that:
//
//   - every other event's canonicalPayload (see sign.go) stays exactly
//     as small as before -- no extra CRC32/signing cost, no bigger
//     shmring segment, for the vast majority of traffic that never
//     touches a channel.
//   - web-app's Rust client (web-app/src/shmevent.rs), which mirrors
//     ValueSize=512 for every event it actually sends/verifies (Set,
//     Get, ...) but implements no Channel support at all, needs no
//     matching change -- it never constructs or verifies an
//     EventChannelSend/EventChannelPoll message.
//
// valueSizeFor is what actually applies this per-event-type ceiling; see
// its doc comment for why a fixed (not length-prefixed) width per event
// type is still safe here, the same way ValueSize's fixed width is.
const ChannelValueSize = 16 * 1024

// KVValueSize is the plain-KV data path's own ceiling, sitting between
// ValueSize and ChannelValueSize: EventSetKey/EventSetField/EventSet/
// EventGetField/EventTxn carry whatever a caller chose to store under a
// key, which -- unlike a permit record or a peer id -- is genuinely
// caller-shaped data whose size the caller, not this package, decides.
//
// 512 bytes turned out to be the binding constraint on anything built on
// top of these events rather than a safety margin: a document store
// layering collections over raw keys (see object-history-app's core/jsondb)
// has to round-trip each document in full on every write, since a Set
// replaces a value outright, and a single JSON object with a handful of
// nested fields already exceeds 512 bytes. Raised for these five event
// types only, kept independent of ValueSize for exactly the reasons
// ChannelValueSize's doc comment gives: every *other* event's
// canonicalPayload stays as small as it was, so no extra CRC32/signing
// cost lands on the traffic that never carries user data.
//
// Two things have to move with this constant, and both are silent when
// missed:
//
//   - pkg/ipc's shared-memory capacity has to fit the largest encoded
//     message; the Android transport's (ipc_android.go) is the tighter of
//     the two and had to grow alongside this.
//   - web-app/src/shmevent/mod.rs's own value_size_for must agree event for
//     event, since it feeds the padding width used for CRC/signing (see
//     canonicalWidth): a disagreement doesn't cause a size error, it makes
//     both sides compute different CRCs and signatures over identical
//     messages, and every affected message is rejected as forged. A test
//     in this package (TestValueSizeTiers) lists the widths so a change
//     here is at least visible; nothing can enforce the Rust side from Go,
//     so it has to be updated by hand.
//
// Raising a ceiling does *not* by itself change how an already-valid
// message is signed -- canonicalWidth deliberately keeps the historical
// width for any value that still fits it, so peers on older builds keep
// verifying. See its doc comment; that separation was learned the hard
// way when this constant was introduced.
const KVValueSize = 4 * 1024

// valueSizeFor returns the maximum Msg.Value length for e.
// EventChannelSend/EventChannelPoll (the two event types whose Value
// carries a channel chunk, in the request and response direction
// respectively -- see those events' doc comments) get ChannelValueSize;
// the plain-KV data events get KVValueSize; every other event type keeps
// the original ValueSize.
//
// This is the ceiling only. The width canonicalPayload actually pads to is
// canonicalWidth, which takes both e and the value's real length --
// because raising a ceiling must not change the signed bytes of messages
// that fit the old one.
func valueSizeFor(e uint8) int {
	if e == EventChannelSend || e == EventChannelPoll {
		return ChannelValueSize
	}
	switch e {
	case EventSetKey, EventSetField, EventSet, EventGetField, EventTxn:
		return KVValueSize
	case EventCommandPut, EventStationPut:
		// These two carry caller-authored definitions -- a command's form
		// spec, a station's attributes -- rather than fields this package
		// defines, so they belong with the plain-KV events rather than with
		// the fixed-shape system records they otherwise resemble.
		return KVValueSize
	case EventCatalogPut:
		// EventCatalogPut's payload can be any of EventGroupPut/
		// EventCommandPut/EventGroupCommandPut/EventPeerGroupPut/
		// EventStationPut's own payloads (see EventCatalogPut's doc
		// comment), including the two above -- it has to offer their
		// widest ceiling, not the narrowest, or a Command-with-spec/Station
		// put that fit under EventCommandPut/EventStationPut directly would
		// silently stop fitting once routed through this envelope instead.
		// EventCatalogDelete needs no equivalent case: every kind's own
		// Delete payload (a bare id, or the two relation kinds' small
		// fixed-shape keys) already fit ValueSize before this envelope
		// existed.
		return KVValueSize
	case EventLogAppend:
		// A journal entry is the durable record of something that happened
		// -- a command's narration, an execution's result -- and 512 bytes
		// left roughly 400-470 for Fields+Narrative combined, which is fine
		// for "started"/"finished" and far too small for a real result with
		// a dozen measured values. It is caller-authored data in the same
		// sense the events above are.
		return KVValueSize
	}
	return ValueSize
}

// SignatureSize is the length of an Ed25519 signature.
const SignatureSize = 64

// PublicKeySize is the length of an Ed25519 public key.
const PublicKeySize = 32

// PrivateKeySize is the length of an Ed25519 private key in stdlib
// crypto/ed25519's format (32-byte seed + 32-byte public key).
const PrivateKeySize = 64

// Msg is the Go-friendly form of the capnp Event struct (named Msg, not
// Event, only to avoid colliding with the generated capnp type of that
// name in this same package -- see shmevent.capnp.go): Encode/Decode
// handle capnp (de)serialization plus CRC/signature computation and
// verification, so callers never touch the generated capnp API directly.
type Msg struct {
	EventType     uint8
	SourceID      uint16
	DestinationID uint16
	Value         []byte
	ID            uint16
}

// EncodeSetPayload packs key and value into a single EventSet Msg.Value: a
// 2-byte big-endian length prefix for key, then key verbatim, then value
// verbatim -- the rest of the buffer, with no length prefix of its own,
// since Value's own end marks value's end. See EventSet's doc comment for
// why this packing exists instead of a second capnp field.
func EncodeSetPayload(key, value []byte) ([]byte, error) {
	if len(key) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: set payload key too long: %d bytes", len(key))
	}
	buf := make([]byte, 2+len(key)+len(value))
	buf[0] = byte(len(key) >> 8)
	buf[1] = byte(len(key))
	copy(buf[2:], key)
	copy(buf[2+len(key):], value)
	return buf, nil
}

// DecodeSetPayload is the inverse of EncodeSetPayload.
func DecodeSetPayload(payload []byte) (key, value []byte, err error) {
	if len(payload) < 2 {
		return nil, nil, fmt.Errorf("shmevent: set payload too short: %d bytes", len(payload))
	}
	keyLen := int(payload[0])<<8 | int(payload[1])
	if 2+keyLen > len(payload) {
		return nil, nil, fmt.Errorf("shmevent: set payload key length %d exceeds payload size %d", keyLen, len(payload))
	}
	return payload[2 : 2+keyLen], payload[2+keyLen:], nil
}

// EncodeListRangeQuery packs start and end -- both inclusive store key
// bounds -- into a single EventListRange request Value. Just
// EncodeSetPayload under a name that reads correctly at an
// EventListRange call site (see that event's doc comment): the wire
// framing is identical, since both only ever pack two length-delimited
// byte strings.
func EncodeListRangeQuery(start, end []byte) ([]byte, error) {
	return EncodeSetPayload(start, end)
}

// DecodeListRangeQuery is the inverse of EncodeListRangeQuery. Also used,
// under this same name, to decode an EventListRange response's
// EncodeListRangeQuery(key, value) framing -- see that event's doc
// comment for why the response reuses this pairing rather than a
// separately named encoder.
func DecodeListRangeQuery(payload []byte) (start, end []byte, err error) {
	return DecodeSetPayload(payload)
}

// EncodeExecuteNotification packs senderPeerID and payload into a single
// value: a 2-byte big-endian length prefix for senderPeerID, then
// senderPeerID verbatim, then payload -- the rest of the buffer, with no
// length prefix of its own, mirroring EncodeSetPayload exactly. Used both
// for the wire message pkg/daemon.ExecuteProtocolID's handler sends (where
// SourceID/DestinationID have no cross-node meaning, so the sender's peer
// id travels in Value instead) and for EventPollExecute's response, so a
// local caller draining its queue learns who an Execute notification came
// from.
func EncodeExecuteNotification(senderPeerID, payload []byte) ([]byte, error) {
	if len(senderPeerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: execute notification sender peer id too long: %d bytes", len(senderPeerID))
	}
	buf := make([]byte, 2+len(senderPeerID)+len(payload))
	buf[0] = byte(len(senderPeerID) >> 8)
	buf[1] = byte(len(senderPeerID))
	copy(buf[2:], senderPeerID)
	copy(buf[2+len(senderPeerID):], payload)
	return buf, nil
}

// DecodeExecuteNotification is the inverse of EncodeExecuteNotification.
func DecodeExecuteNotification(data []byte) (senderPeerID, payload []byte, err error) {
	if len(data) < 2 {
		return nil, nil, fmt.Errorf("shmevent: execute notification too short: %d bytes", len(data))
	}
	idLen := int(data[0])<<8 | int(data[1])
	if 2+idLen > len(data) {
		return nil, nil, fmt.Errorf("shmevent: execute notification sender peer id length %d exceeds payload size %d", idLen, len(data))
	}
	return data[2 : 2+idLen], data[2+idLen:], nil
}

// Encode serializes m to its capnp wire form, computing CRC32 and signing
// with priv. priv may be nil only for EventGetPublicKey/EventGetPrivateKey
// requests (see Sign's doc comment). m.Value's ceiling is ValueSize,
// except for EventChannelSend/EventChannelPoll -- see
// ChannelValueSize/valueSizeFor.
func Encode(m Msg, priv PrivateKey) ([]byte, error) {
	if maxSize := valueSizeFor(m.EventType); len(m.Value) > maxSize {
		return nil, fmt.Errorf("shmevent: value too long: %d bytes (max %d)", len(m.Value), maxSize)
	}

	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, fmt.Errorf("shmevent: new message: %w", err)
	}
	root, err := NewRootEvent(seg)
	if err != nil {
		return nil, fmt.Errorf("shmevent: new root: %w", err)
	}
	root.SetEvent(m.EventType)
	root.SetSourceId(m.SourceID)
	root.SetDestinationId(m.DestinationID)
	if err := root.SetValue(m.Value); err != nil {
		return nil, fmt.Errorf("shmevent: set value: %w", err)
	}
	root.SetId(m.ID)

	crc := crc32Of(m)
	root.SetCrc32(crc)

	sig, err := Sign(priv, m, crc)
	if err != nil {
		return nil, fmt.Errorf("shmevent: sign: %w", err)
	}
	if err := root.SetSignature(sig); err != nil {
		return nil, fmt.Errorf("shmevent: set signature: %w", err)
	}

	return msg.Marshal()
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
	root, err := ReadRootEvent(msg)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: read root: %w", err)
	}
	value, err := root.Value()
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: value: %w", err)
	}
	sig, err := root.Signature()
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: signature: %w", err)
	}

	m = Msg{
		EventType:     root.Event(),
		SourceID:      root.SourceId(),
		DestinationID: root.DestinationId(),
		Value:         append([]byte(nil), value...),
		ID:            root.Id(),
	}
	wantCRC := root.Crc32()
	if gotCRC := crc32Of(m); gotCRC != wantCRC {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: crc32 mismatch: got %#x, message says %#x", gotCRC, wantCRC)
	}
	return m, wantCRC, append([]byte(nil), sig...), nil
}
