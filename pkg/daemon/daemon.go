// Package daemon implements the long-running kvnode process: a libp2p host,
// a hashicorp/raft node backed by pkg/kvfsm/pkg/store, and a pkg/ipc server
// that a local mage CLI invocation drives with add/set/get requests.
//
// A daemon always starts "unconfigured": it has an identity and a raft
// instance, but no cluster role until it receives a pkg/shmevent EventAdd
// request telling it whether to bootstrap as the cluster's sole leader or
// to join an existing leader. This lets the same binary serve every
// `mage addnode` case (new leader, new follower, or rejoining after a
// restart) with identical startup code.
package daemon

import (
	"bufio"
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	pbv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// JoinProtocolID is the libp2p protocol a joining node uses to ask an
// existing leader to add it as a voter.
const JoinProtocolID = protocol.ID("/libp2p-kv-raft/join/1.0.0")

// ForwardProtocolID is the libp2p protocol a non-leader node uses to relay
// a Set to the current raft leader on the caller's behalf. Needed because
// pkg/ipc is a same-machine-only protocol -- a node has no other way to
// reach whichever peer is actually the raft leader -- and raft itself
// deliberately has no client-request forwarding built in; that's left to
// the application. In particular, the Android build of this project runs
// as a follower with no separate "find the leader" step available to its
// UI, so every Set it issues needs this to reach the leader at all.
const ForwardProtocolID = protocol.ID("/libp2p-kv-raft/forward-set/1.0.0")

// ForwardConfirmProtocolID is the libp2p protocol a non-leader node uses
// to relay an EventPermitConfirm to the current raft leader, mirroring
// ForwardProtocolID's role for Set. It's a separate protocol (rather than
// overloading ForwardProtocolID's existing OpSet-only handling) because
// its handler, unlike handleForwardSetStream, must check the identity of
// whoever actually opened the stream against the leader's live raft
// configuration before applying -- see handleForwardConfirmStream's doc
// comment.
const ForwardConfirmProtocolID = protocol.ID("/libp2p-kv-raft/forward-confirm/1.0.0")

// ForwardJoinProtocolID is the libp2p protocol a non-leader node uses to
// relay a Join request to the current raft leader on the joining node's
// behalf, mirroring ForwardProtocolID's role for Set and for the same
// reason: raft leadership can move to any voter at any time -- this
// project watched it happen mid-session -- so whichever cluster member a
// new node was told to join through (e.g. a leader address baked into an
// Android build at compile time) may no longer be the leader by the time
// it actually tries.
const ForwardJoinProtocolID = protocol.ID("/libp2p-kv-raft/forward-join/1.0.0")

// ForwardLeaveProtocolID is the libp2p protocol a raft member that isn't
// currently the leader uses to ask the leader to remove it from the raft
// configuration (raft.RemoveServer) -- see leaveCluster/removeServerLine.
// Unlike ForwardJoinProtocolID (a stranger asking to be let in), there's
// no public-facing, non-member-only counterpart: only an existing member
// -- already reachable through the cluster's own transport, with its own
// live raft handle and leader-tracking -- ever needs to leave, so it can
// always determine and dial the current leader itself (see
// resolveWriteTarget) rather than needing an arbitrary node to introduce
// it the way a brand new joiner does. The peer to remove is never a
// payload field: it's whichever peer's libp2p-authenticated connection
// identity opened the stream (see handleForwardLeaveStream) -- a member
// can only ever ask to remove itself.
const ForwardLeaveProtocolID = protocol.ID("/libp2p-kv-raft/forward-leave/1.0.0")

// ForwardKickProtocolID is the libp2p protocol a raft member that isn't
// currently the leader uses to relay an EventKick (see that event's doc
// comment) to whoever is -- forceKick's one-hop forwarding counterpart to
// ForwardLeaveProtocolID, for removing an *arbitrary* peer rather than
// only the requester itself. Unlike ForwardLeaveProtocolID, the peer to
// remove genuinely is a payload field here (there's no other way to name
// someone other than the stream's own authenticated identity) -- so
// handleForwardKickStream re-checks the *forwarding* peer's own identity
// against the leader's voter list before acting on it, the same way
// handleForwardConfirmStream re-authorizes EventPermitConfirm/Revoke at
// this same hop (see that handler's doc comment for why the original
// caller's own check, done once in handleShmEvent, isn't sufficient on
// its own).
const ForwardKickProtocolID = protocol.ID("/libp2p-kv-raft/forward-kick/1.0.0")

// ClientProtocolID is the libp2p protocol a remote client with no local
// pkg/ipc channel of its own speaks directly to any cluster node to issue
// Add/Set/Get requests -- namely the browser build in web-app/ (rust-libp2p
// compiled to wasm), which has no shared process with this daemon the way
// the Android build's in-process kvmobile does, and dials in over the
// WebTransport listener newHost adds for exactly this. ActionAdd here means
// something different than it does over pkg/ipc: a browser tab can never
// accept a raw inbound connection, so it can never be a raft *voter* (a
// voter's transport must be independently dialable by any other voter at
// any time -- see rafttransport's doc comment), but it *can* be dialed
// through a circuit-relay v2 reservation it already holds (the same
// mechanism an Android device behind carrier-grade NAT already relies on --
// see Config.RelayPeers), which makes it a real raft *non-voter* (learner):
// Key carries the browser's own peer id, Value its reserved
// /p2p-circuit multiaddr -- see handleAddLearner.
const ClientProtocolID = protocol.ID("/libp2p-kv-raft/client/1.0.0")

// ExecuteProtocolID is the libp2p protocol one raft node uses to deliver an
// EventExecute notification directly to another, peer-to-peer -- see that
// event's doc comment in pkg/shmevent. Unlike every other protocol in this
// file, the message it carries never touches raft or the store at either
// end: handleExecuteStream just verifies it and queues it in the
// receiving Node's executeInbox for a local caller to drain via
// EventPollExecute.
const ExecuteProtocolID = protocol.ID("/libp2p-kv-raft/execute/1.0.0")

// ChannelProtocolID is the libp2p protocol one raft node uses to open a
// persistent, bidirectional, multipurpose stream directly to another --
// EventChannelOpen/Send/Poll/Listen/Close/DataReady's transport, see
// those events' doc comments in pkg/shmevent. Unlike every other
// protocol in this file (including ExecuteProtocolID), the stream is
// never a single write-then-EOF request/response: after a short signed
// handshake (mirroring ExecuteProtocolID's self-contained-signature
// design, still the ordinary shmevent.Encode/Decode/Verify Msg
// machinery) the stream stays open indefinitely, and every message
// exchanged over it afterward is *also* signed and framed
// (writeFramed/readFramed), but -- unlike version 2.0.0 of this protocol
// -- using shmevent.SignChannelChunk/VerifyChannelChunk/
// EncodeChannelFrame/DecodeChannelFrame's own variable-length scheme
// instead of a Msg's fixed-width one: see channelSession.write and
// pumpChannelReads for where each direction does this, and
// SignChannelChunk's own doc comment for why a fixed-width scheme (2.0.0's
// only option, since it reused Msg/EventChannelSend directly) doesn't fit
// pkg/chandata's own much larger per-chunk ceiling. Every version bump
// here (1.0.0's raw unframed bytes -> 2.0.0's signed-Msg-per-message ->
// 3.0.0's variable-length signed frame) has been a wire-incompatible
// change, so each is a real protocol.ID bump rather than a change to
// 2.0.0's own wire format in place -- an old peer speaking one and a new
// peer expecting the other must never be allowed to negotiate a stream
// together and silently misinterpret each other; the version bump makes
// that mismatch fail cleanly at libp2p's protocol negotiation step
// instead. See handleChannelStream's own doc comment for the full wire
// design. Like ExecuteProtocolID, the traffic it carries never touches
// raft or the store.
const ChannelProtocolID = protocol.ID("/libp2p-kv-raft/channel/3.0.0")

// ExecInviteRedeemProtocolID is the libp2p protocol a redeeming peer's own
// daemon dials directly at a shmevent.KindExecInvite invite's sourceAddr to
// redeem it -- see dialAndRedeemExecInvite. The message it carries is
// self-contained the same way ExecuteProtocolID's is (EncodeExecuteNotification,
// reused here, wraps the redeeming peer's own claimed id + the raw token,
// signed with that peer's own key and verified against that same claimed
// id's extracted pubkey -- see handleExecInviteRedeemStream), not against
// whichever connection happened to carry it -- which is what lets the exact
// same verify-then-apply-or-forward logic (processExecInviteRedeem) run
// unchanged whether this is where the redeeming peer's connection actually
// landed or a one-hop-forwarded relay from that node to the real leader
// (see ForwardExecInviteRedeemProtocolID).
const ExecInviteRedeemProtocolID = protocol.ID("/libp2p-kv-raft/exec-invite-redeem/1.0.0")

// ForwardExecInviteRedeemProtocolID is the libp2p protocol a non-leader node
// uses to relay an already-verified exec-invite redemption to the current
// raft leader, mirroring ForwardJoinProtocolID's single-hop-only role for
// Join. Unlike ForwardConfirmProtocolID, its handler does no additional
// identity check of its own: the redeeming peer's identity was already
// cryptographically pinned by processExecInviteRedeem's signature check
// before either protocol ever sees it, so there's nothing further to
// authenticate at this hop -- only "is this node actually the leader" (see
// handleForwardExecInviteRedeemStream).
const ForwardExecInviteRedeemProtocolID = protocol.ID("/libp2p-kv-raft/forward-exec-invite-redeem/1.0.0")

// RecruitProtocolID is the libp2p protocol an existing cluster voter's own
// daemon dials directly at a device's own address to redeem an
// EventJoinRequestCreate ticket -- the reverse of every other invite/redeem
// pair in this file: everywhere else, the device that ends up admitted is
// the one that dials out (handleAdd/join, dialAndRedeemExecInvite); here
// the *already-clustered* voter dials the device instead, handing it a
// freshly minted join-invite (mintJoinInvite) plus the ticket's own
// correlation token, so the device (handleRecruitStream) can admit itself
// with no further action of its own beyond having minted the ticket in the
// first place. See dialAndPushRecruit for the sending side. The message is
// a single plain-text line, not a capnp shmevent.Msg -- same non-capnp
// framing JoinProtocolID's own reqLine already uses -- since the ticket
// itself (physically scanned, exactly like every other invite token in
// this file) is the entire credential; there's nothing left to sign.
const RecruitProtocolID = protocol.ID("/libp2p-kv-raft/recruit/1.0.0")

// ReadyFileName is written to Config.DataDir once the daemon's host and IPC
// server are up, so the spawning `mage addnode` can learn the node's peer id
// and listen addresses without parsing stdout.
const ReadyFileName = "ready.json"

// Config configures a single node process.
type Config struct {
	DataDir string // root directory for this node's identity, sqlite, and raft data
	KeyPath string // libp2p identity key (already generated by the caller)

	// ListenPort is the TCP/QUIC port to listen on. 0 (the default) picks an
	// ephemeral port, fine for same-machine/LAN use. A publicly reachable
	// deployment should pin this to a known port so it can be opened in a
	// firewall/security group.
	ListenPort int

	// RelayService makes this node act as a circuit-relay v2 point for
	// other nodes that can't be dialed directly (the "worst case" NAT
	// fallback) and forces it to advertise itself as publicly reachable.
	// Only enable this on a node known to actually have a public,
	// unfiltered address -- e.g. the leader deployed on a public VPS.
	// Every node, regardless of this flag, can still *use* a relay
	// (EnableRelay/EnableHolePunching are always on); this flag only
	// controls whether it also serves as one for others.
	RelayService bool

	// RelayPeers seeds the known circuit-relay v2 servers (nodes running
	// with RelayService=true) this node should proactively reserve a
	// relay slot through, so it ends up with a /p2p-circuit address that
	// someone can dial it on even though it has no directly-dialable
	// address of its own -- e.g. a phone on a cellular connection behind
	// carrier-grade NAT, which blocks *inbound* connections entirely.
	// EnableRelay/EnableHolePunching (always on regardless of this field)
	// only let a node *use* a relay connection someone else already set
	// up; without a static relay target, a node that nothing can dial
	// directly has no way to make its own reservation, and ends up
	// advertising only addresses that raft's leader can never use to send
	// it AppendEntries -- leaving it permanently stuck as a voter that
	// never learns who the leader is. Leave empty for a node with a real
	// public or otherwise directly-dialable address, where a reservation
	// would just be wasted overhead.
	//
	// This is only the *seed* list -- typically just enough to reach the
	// cluster for the very first join, before this node has any
	// replicated data of its own yet (cmd/kvnode's -relay-peer flag,
	// mobile/kvmobile's build-time relayMultiaddr). Once running,
	// relayCandidates (see newHost) merges these with every currently-
	// confirmed shmevent.KindBootstrapNode record already in this node's
	// own local store -- see pkg/kvctl's AddRelayNode/ConfirmRelayNode/
	// RemoveRelayNode/ListRelayNodes and mobile/kvmobile's identical
	// bindings -- so the full candidate set can grow (or shrink) across
	// the cluster's lifetime with no restart needed, and
	// EnableAutoRelayWithStaticRelays gets more than one candidate to
	// fail over between if the first one it tries is down. Order here is
	// preference order: entries listed first are tried first, ahead of
	// every KindBootstrapNode entry regardless of that record's own
	// priority value.
	RelayPeers []string

	// Relay resource knobs, only meaningful alongside RelayService. Zero
	// means "use shmevent.DefaultRelay*" (see those constants' doc
	// comments for the reasoning behind each default). These are the same
	// values newHost hands go-libp2p as v2relay.Resources, and the same
	// values EventPermitRequest stamps onto every new KindPermitPeer
	// record for this node (see shmevent.RelayLimits) -- go-libp2p applies
	// one Resources value to every ACL-approved peer alike, so today every
	// permitted peer gets this same node-wide allotment; there is no
	// per-individual-peer override. These are per-circuit/per-reservation
	// static caps, a separate concern from the cumulative Quota* fields
	// below: a peer can be well under RelayMaxCircuitsPerPeer/
	// RelayMaxReservationsPerIP and still be rejected by QuotaRelay*'s
	// rolling event-rate budget, or vice versa.
	//
	// RelayMaxCircuitsPerPeer bounds concurrent open relayed circuits for
	// a single peer (v2relay.Resources.MaxCircuits -- misleadingly named
	// in go-libp2p itself, it's already enforced per source/destination
	// peer, not as one shared global count).
	RelayMaxCircuitsPerPeer int
	// RelayLimitData bounds bytes relayed, each direction, before a
	// circuit is reset (v2relay.RelayLimit.Data).
	RelayLimitData int64
	// RelayLimitDuration bounds a circuit's wall-clock lifetime before
	// it's reset (v2relay.RelayLimit.Duration).
	RelayLimitDuration time.Duration
	// RelayMaxReservationsPerIP bounds active relay-slot reservations
	// from one IP address (v2relay.Resources.MaxReservationsPerIP).
	RelayMaxReservationsPerIP int
	// RelayMaxReservationsPerPeer bounds active relay-slot reservations
	// from one peer (v2relay.Resources.MaxReservationsPerPeer).
	RelayMaxReservationsPerPeer int

	// Quota* fields configure the two quotaTracker instances every node
	// builds regardless of RelayService/Config -- (*Node).channelQuota and
	// relayQuota -- one independent token bucket per peer id and one per
	// remote IP address (see quotaTracker's own doc comment), applied
	// uniformly to every peer/IP rather than differentiated per specific
	// peer. Zero substitutes this package's Default* constants (see
	// quota.go) -- the same zero-means-default pattern relayLimits already
	// uses for RelayLimits, not "unlimited": there is no way to configure
	// an actually-unlimited bucket once past that substitution, matching
	// every other Default*-backed field in this Config. Channel quota is
	// metered in real bytes (every chunk pumpChannelReads/
	// channelSession.write actually moves); relay quota is metered in
	// events -- one debit per AllowReserve/AllowConnect call -- because
	// go-libp2p's circuitv2 relay never reports actual per-circuit byte
	// usage back to relayACL (RelayLimitData above is a static per-circuit
	// cap enforced entirely inside go-libp2p, not a value this quota can
	// see or accumulate against).
	QuotaChannelBytesPerPeerPerSec float64
	QuotaChannelBurstPerPeer       int
	QuotaChannelBytesPerIPPerSec   float64
	QuotaChannelBurstPerIP         int
	QuotaRelayEventsPerPeerPerSec  float64
	QuotaRelayBurstPerPeer         int
	QuotaRelayEventsPerIPPerSec    float64
	QuotaRelayBurstPerIP           int

	// Raft timing knobs. Zero means "use hashicorp/raft's own default"
	// (1s heartbeat/election, 50ms commit, 500ms leader lease) -- values
	// the raft project itself considers safe for real networks, not just
	// a fast LAN/loopback. Override with smaller values only where the
	// network genuinely warrants it (e.g. same-machine integration
	// tests): tightening these for a real WAN deployment is what causes
	// spurious "leadership lost" elections when latency/jitter occasionally
	// exceeds an aggressive timeout, especially in a small cluster where
	// every node's vote is required.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	LeaderLeaseTimeout time.Duration

	// SnapshotThreshold/SnapshotInterval bound how large a long-lived
	// leader's raft log is allowed to grow between compactions. Zero means
	// "use hashicorp/raft's own default" (8192 entries / 120s), which is
	// tuned for a cluster that mostly just needs *a* periodic snapshot, not
	// one where a brand new non-voter might join long after the log has
	// grown into the thousands: every fresh join replays the entire log
	// from index 1 up to the most recent snapshot, one entry at a time, so
	// a leader that's been running a long time without ever snapshotting
	// makes every subsequent join progressively slower -- observed directly
	// against this project's own long-lived e2e deploy target, where a
	// newly-joined browser tab had to replay well over a thousand mostly-
	// empty heartbeat entries before reaching the one write it actually
	// needed. A lower threshold trades more frequent (cheap, incremental)
	// snapshotting for a join's replay being bounded by TrailingLogs
	// instead of the leader's entire lifetime log.
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration

	// TrailingLogs is how many of the most recent log entries a snapshot
	// leaves in place instead of compacting away. Zero means hashicorp/
	// raft's own default (10240), which -- combined with a lowered
	// SnapshotThreshold above -- can still leave a snapshot compacting
	// nothing at all: a log under 10240 entries total has nothing eligible
	// for removal regardless of how often it snapshots, so a fresh non-
	// voter join still replays the whole thing. Set this alongside
	// SnapshotThreshold, not instead of it, for a snapshot to actually
	// shrink what a new join has to replay.
	TrailingLogs uint64

	// The generic remote (ClientProtocolID) RPC surface and
	// EventLogAppend/EventListRange are, like relay/Channel admission below,
	// always gated -- there is no opt-out flag for either. A current cluster
	// member (voter or learner -- see isClusterMember) is trusted implicitly,
	// the same way it already is for every raft-replicated write; a
	// non-member remote caller is rejected for everything except the one
	// deliberate carve-out described on isCommandLogCarveOut: submitting a
	// command linked to a public shmevent Group (SubmitCommand,
	// raft-authoritatively enforced by kvfsm.OpAppendCommandRequest's
	// IsPermittedForCommand) and reading back that same dispatch's own
	// CommandRequest/execution-index/execution-log records. That carve-out
	// -- together with Join/Recruit, which are their own separate,
	// already-gated protocols entirely outside handleShmEvent's remote
	// surface -- is deliberately the *only* door a peer with no other
	// standing has into an otherwise closed cluster; a public command's own
	// execution logic can widen that peer's access further from there (e.g.
	// mage addpeertogroup <peerID> remote/channel/relay), but nothing does
	// so automatically. See handleShmEvent's top-of-function gate and
	// isAuthorizedForGatedAccess/shmevent.ReservedGroupRemote.
	//
	// Relay/Channel admission (reserving a relay slot or opening a relayed
	// circuit, only meaningful alongside RelayService; opening an inbound
	// Channel) are gated the identical unconditional way, against
	// shmevent.ReservedGroupRelay/ReservedGroupChannel respectively -- see
	// relayACL.allow/handleChannelStream/isAuthorizedForGatedAccessSt.
	//
	// EventExecute delivery (handleExecuteStream) is gated more narrowly
	// than any of the above: only a *current* raft cluster member
	// (shmevent.ReservedGroupCluster, not routed through
	// isAuthorizedForGatedAccess at all) may push one. Unlike
	// remote/channel/relay, there is no operator-grantable execute group or
	// personal-grant carve-out that widens this -- see handleExecuteStream's
	// own doc comment.

	// RequireConfirmForJoin gates JoinProtocolID/ForwardJoinProtocolID
	// (handleJoinStream/handleForwardJoinStream) on a two-stage
	// request/confirm workflow instead of today's immediate
	// raft.AddVoter/AddNonvoter: when true, a join request only lodges a
	// pending shmevent.KindClusterJoin system record (replying "PENDING"
	// instead of "OK") and addServerLine only actually runs once a
	// separate confirmed raft voter promotes that record via
	// EventPermitConfirm -- see applyConfirm's KindClusterJoin handling.
	// Defaults to false: today's behavior, where any join request that
	// reaches the leader (directly or forwarded) is admitted immediately
	// with no separate approval step. Turning this on requires an
	// operator on some other already-confirmed voter to run
	// ConfirmPermit(kind="cluster-join", peerID) -- mage
	// confirmpermit/kvctl-cli confirmpermit -- for every new node that
	// wants to join.
	RequireConfirmForJoin bool
}

// Node is a running daemon instance. Its raft/transport fields are nil
// until the first EventAdd request initializes them (see
// initRaft): constructing raft.NewRaft starts its election-timeout loop
// immediately, so doing that unconditionally at process startup -- before
// mage has had a chance to deliver the Add request that decides whether
// this node bootstraps or joins -- creates a race where the node times out
// waiting for a leader, becomes a candidate, and bumps its persisted term.
// That alone makes raft.HasExistingState true and BootstrapCluster then
// fails with "bootstrap only works on new clusters". Deferring
// raft.NewRaft to initRaft, invoked synchronously inside the Add handler,
// closes that window.
type Node struct {
	cfg    Config
	host   lp2phost.Host
	store  *store.Store
	peerID string

	// ed25519Priv/ed25519Pub are this node's libp2p identity key, in
	// stdlib crypto/ed25519's portable raw form -- what
	// EventGetPrivateKey/EventGetPublicKey hand out, and what every
	// pkg/shmevent message this node sends/verifies is signed/checked
	// against. See pkg/shmevent's doc comment on why local callers share
	// this same key rather than provisioning one of their own.
	ed25519Priv shmevent.PrivateKey
	ed25519Pub  shmevent.PublicKey

	// registry backs pkg/shmevent's EventSetKey/EventGetKey and the
	// SourceID-addressed forms of EventSetField/EventGetField/EventAdd.
	registry *shmevent.Registry

	// executeInbox queues EventExecute notifications (see that event's doc
	// comment) delivered to this node over ExecuteProtocolID, for
	// EventPollExecute to drain. Purely in-memory and never persisted --
	// unlike everything else this daemon handles, a queued notification
	// that's lost on restart is an accepted trade-off, not a correctness
	// bug (see executeInbox's own doc comment).
	executeInbox *executeInbox

	// channels holds every live/pending EventChannelOpen session (see
	// that event's doc comment) -- the persistent-session counterpart to
	// executeInbox, same "purely in-memory, never persisted" trade-off.
	channels *channelTable

	// channelQuota meters real per-chunk bytes flowing through every
	// channelSession this node is party to (see pumpChannelReads/
	// channelSession.write), one token bucket per remote peer id and one
	// per remote IP -- see Config.QuotaChannel*/quotaTracker's own doc
	// comments. relayQuota is its relay-side counterpart, metering
	// reservation/connect events rather than bytes -- built alongside
	// channelQuota in start (before newHost, which also hands the same
	// instance to relayACL directly, since relayACL is constructed inside
	// newHost before this Node exists yet).
	channelQuota *quotaTracker
	relayQuota   *quotaTracker

	// joinRequestMu/joinRequestToken hold this node's own single
	// outstanding join-request ticket (see EventJoinRequestCreate) --
	// purely in-memory, mirroring executeInbox's own "never persisted"
	// trade-off, since a ticket this node itself minted and never got
	// redeemed is fine to lose on restart. Deliberately just one at a
	// time: minting a new ticket supersedes whatever was pending before,
	// and handleRecruitStream clears it the moment it's consumed --
	// whether or not the resulting join actually succeeds -- so the same
	// ticket can never be redeemed twice (see consumeJoinRequestToken).
	joinRequestMu    sync.Mutex
	joinRequestToken []byte

	logStore  *raftboltdb.BoltStore
	snapStore raft.SnapshotStore

	// raftLogFile backs raftConf.LogOutput (see initRaft) -- kept here
	// solely so shutdown can close it; nothing else reads this field.
	raftLogFile *os.File

	mu              sync.RWMutex
	raft            *raft.Raft
	transport       *raft.NetworkTransport
	electionTimeout time.Duration // the effective value raft is actually using, set by initRaft

	// leadershipObserver/leadershipObsCh back watchLeadership -- see
	// initRaft's registration of them. Torn down in shutdown so that
	// goroutine doesn't leak past this Node's lifetime (relevant mainly
	// for tests, which construct many short-lived Nodes in one process).
	leadershipObserver *raft.Observer
	leadershipObsCh    chan raft.Observation
}

// setJoinRequestToken replaces this node's outstanding join-request ticket
// (see EventJoinRequestCreate) with token, discarding whatever was pending
// before -- only ever one at a time.
func (n *Node) setJoinRequestToken(token []byte) {
	n.joinRequestMu.Lock()
	defer n.joinRequestMu.Unlock()
	n.joinRequestToken = token
}

// cancelJoinRequestToken clears this node's outstanding ticket (see
// EventJoinRequestCancel) if it still matches token -- a no-op if it's
// already been consumed or superseded by a later create.
func (n *Node) cancelJoinRequestToken(token []byte) {
	n.joinRequestMu.Lock()
	defer n.joinRequestMu.Unlock()
	if bytes.Equal(n.joinRequestToken, token) {
		n.joinRequestToken = nil
	}
}

// consumeJoinRequestToken is handleRecruitStream's single-use check: it
// reports whether token matches the currently pending ticket, and clears
// the ticket either way -- a wrong or replayed correlation token can never
// match again afterward, even on a later, correct attempt.
func (n *Node) consumeJoinRequestToken(token []byte) bool {
	n.joinRequestMu.Lock()
	defer n.joinRequestMu.Unlock()
	match := len(n.joinRequestToken) > 0 && bytes.Equal(n.joinRequestToken, token)
	n.joinRequestToken = nil
	return match
}

// Run starts a node and blocks, serving IPC requests, until ctx is
// cancelled. It always returns a non-nil error except on clean shutdown via
// ctx cancellation.
func Run(ctx context.Context, cfg Config) error {
	n, err := start(cfg)
	if err != nil {
		return err
	}
	defer n.shutdown()

	// A node that already has persisted raft state -- because it was
	// bootstrapped or joined before, e.g. across a restart -- doesn't need
	// (and, since BootstrapCluster now correctly refuses a non-empty log,
	// can't use) an EventAdd to become operational again: raft.NewRaft
	// recovers the last known configuration and log from disk on its own.
	// Resume immediately so Set/Get work right away, with no coordination
	// step at all, as long as this node is still reachable at whatever
	// address that recovered configuration expects (true if -listen-port
	// is pinned across restarts). A caller that also needs to re-announce
	// a changed address to the current leader still sends an EventAdd
	// with a leader address; handleAdd's join path works whether or not
	// this already ran, since initRaft is idempotent.
	hasState, err := raft.HasExistingState(n.logStore, n.logStore, n.snapStore)
	if err != nil {
		return fmt.Errorf("daemon: check existing raft state: %w", err)
	}
	if hasState {
		if _, err := n.initRaft(); err != nil {
			return fmt.Errorf("daemon: resume raft: %w", err)
		}
	}

	if err := n.writeReadyFile(hasState); err != nil {
		return fmt.Errorf("daemon: write ready file: %w", err)
	}

	return ipc.Serve(ctx, n.peerID, cfg.DataDir, n.ed25519Priv, func(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg {
		return n.handleShmEvent(ctx, m, crc, sig, n.localCaller())
	})
}

func start(cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("daemon: create data dir: %w", err)
	}

	priv, err := loadKey(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("daemon: load identity: %w", err)
	}
	peerID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("daemon: derive peer id: %w", err)
	}

	sqliteDir := filepath.Join(cfg.DataDir, "sqlite")
	st, err := store.Open(sqliteDir)
	if err != nil {
		return nil, fmt.Errorf("daemon: open store: %w", err)
	}

	relayQuota := newQuotaTracker(relayQuotaLimits(cfg))
	channelQuota := newQuotaTracker(channelQuotaLimits(cfg))

	h, err := newHost(priv, cfg, st, peerID.String(), relayQuota)
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("daemon: create libp2p host: %w", err)
	}

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: create raft dir: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: open raft log store: %w", err)
	}

	snapDir := filepath.Join(raftDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		st.Close()
		h.Close()
		return nil, err
	}
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 2, io.Discard)
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: open snapshot store: %w", err)
	}

	ed25519Priv, err := priv.Raw()
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: raw identity private key: %w", err)
	}
	ed25519Pub, err := priv.GetPublic().Raw()
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: raw identity public key: %w", err)
	}

	n := &Node{
		cfg:          cfg,
		host:         h,
		store:        st,
		peerID:       peerID.String(),
		ed25519Priv:  ed25519Priv,
		ed25519Pub:   ed25519Pub,
		registry:     shmevent.NewRegistry(),
		executeInbox: newExecuteInbox(),
		channels:     newChannelTable(),
		channelQuota: channelQuota,
		relayQuota:   relayQuota,
		logStore:     logStore,
		snapStore:    snapStore,
	}
	h.SetStreamHandler(JoinProtocolID, n.withStreamRequestDeadline(n.handleJoinStream))
	h.SetStreamHandler(ForwardProtocolID, n.withStreamRequestDeadline(n.handleForwardSetStream))
	h.SetStreamHandler(ForwardConfirmProtocolID, n.withStreamRequestDeadline(n.handleForwardConfirmStream))
	h.SetStreamHandler(ExecuteProtocolID, n.withStreamRequestDeadline(n.handleExecuteStream))
	h.SetStreamHandler(ChannelProtocolID, n.withStreamRequestDeadline(n.handleChannelStream))
	h.SetStreamHandler(ForwardJoinProtocolID, n.withStreamRequestDeadline(n.handleForwardJoinStream))
	h.SetStreamHandler(ForwardLeaveProtocolID, n.withStreamRequestDeadline(n.handleForwardLeaveStream))
	h.SetStreamHandler(ForwardKickProtocolID, n.withStreamRequestDeadline(n.handleForwardKickStream))
	h.SetStreamHandler(ClientProtocolID, n.withStreamRequestDeadline(n.handleClientStream))
	h.SetStreamHandler(ExecInviteRedeemProtocolID, n.withStreamRequestDeadline(n.handleExecInviteRedeemStream))
	h.SetStreamHandler(ForwardExecInviteRedeemProtocolID, n.withStreamRequestDeadline(n.handleForwardExecInviteRedeemStream))
	h.SetStreamHandler(RecruitProtocolID, n.withStreamRequestDeadline(n.handleRecruitStream))

	// forgetTransientPeer's disconnect hook -- see that method's doc
	// comment for why this (plus newHost's ConnectionManager) exists.
	h.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(_ network.Network, c network.Conn) {
			n.forgetTransientPeer(c.RemotePeer())
		},
	})
	return n, nil
}

func loadKey(keyPath string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key file %s: %w", keyPath, err)
	}
	raw, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, fmt.Errorf("decode key file %s: %w", keyPath, err)
	}
	return crypto.UnmarshalPrivateKey(raw)
}

func (n *Node) shutdown() {
	n.mu.Lock()
	rf, transport := n.raft, n.transport
	observer, obsCh := n.leadershipObserver, n.leadershipObsCh
	logFile := n.raftLogFile
	n.mu.Unlock()

	if rf != nil {
		if observer != nil {
			// Deregister before closing: RegisterObserver/DeregisterObserver
			// and observe() share a lock, so once this returns, raft can no
			// longer be mid-send on obsCh, and closing it is safe -- stops
			// watchLeadership's goroutine instead of leaking it past this
			// Node's lifetime (relevant mainly for tests, which construct
			// many short-lived Nodes in one process).
			rf.DeregisterObserver(observer)
			close(obsCh)
		}
		// Waited on, not fired and forgotten: Shutdown is asynchronous, and
		// raft's own goroutines keep writing to the log store until it
		// finishes. Closing that store underneath them is what makes this
		// wait necessary at all -- see the logStore close below.
		if err := rf.Shutdown().Error(); err != nil {
			fmt.Fprintf(os.Stderr, "daemon: shut down raft: %v\n", err)
		}
	}
	if transport != nil {
		transport.Close()
	}
	if logFile != nil {
		logFile.Close()
	}
	// The raft log store holds a bbolt file lock, and bbolt takes that lock
	// with flock(2) -- per process, and with no timeout on the acquiring
	// side. So a store left open here is not merely a leaked file handle:
	// it makes the *next* daemon.Run against this same data directory block
	// forever inside NewBoltStore, before it can report anything or write a
	// ready file, for as long as this process lives.
	//
	// That is only reachable where one process starts a node more than
	// once, which is exactly what the Android app does -- mobile/kvmobile
	// runs the daemon in-process, and Stop/Start, Join, and Start's own
	// self-heal path all restart it. It presented there as a device that
	// worked until the first restart and then failed every launch
	// afterwards with "ready.json: no such file or directory" (kvmobile's
	// waitForReady timing out on a daemon stuck in flock), which reads like
	// a missing file and is really a lock this line releases. A desktop
	// kvnode never hit it: one process, one node, and exit frees the lock.
	if n.logStore != nil {
		if err := n.logStore.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "daemon: close raft log store: %v\n", err)
		}
	}
	if n.store != nil {
		n.store.Close()
	}
	if n.host != nil {
		n.host.Close()
	}
}

// advertisedAddrs returns this node's dialable multiaddrs, each including a
// trailing /p2p/<peer-id> component.
// advertisedAddrs returns this node's own dialable addresses, best first:
// this is what gets baked into raft's persisted cluster configuration
// (index [0], for both a bootstrapping leader's own address and a joining
// follower's self-reported address) and what a peer that has never
// connected to this node before -- e.g. after a restart tears down any
// existing connection -- has to work with to dial it fresh.
//
// n.host.Addrs() carries no ordering guarantee that favors reachability, so
// a multi-homed host (a VPS with both a public IP and a private/VPN
// interface, like the one this project targets for a real remote
// deployment) can easily end up with a private address in [0] purely by
// interface enumeration order. That address is often not just suboptimal
// but entirely undialable by anyone outside that private network -- and
// unlike the peerstore, which can accumulate additional observed addresses
// from a successful connection, raft's configuration stores exactly one
// address per voter, so getting it wrong here is not recoverable by any
// other layer once persisted. Sort public addresses first, then
// private/unspecified, then loopback last (only useful for same-machine
// setups, and worse than everything else for a real multi-host one).
func (n *Node) advertisedAddrs() []string {
	hostAddrs := n.host.Addrs()

	const (
		scorePublic = iota
		scoreRelay
		scoreOther
		scoreLoopback
	)
	score := func(a multiaddr.Multiaddr) int {
		switch {
		case manet.IsPublicAddr(a):
			return scorePublic
		// A /p2p-circuit address is a relay reservation (see Config.RelayPeers):
		// unlike a raw private/NAT address, it's actually dialable by
		// whoever needs to reach this node, so it belongs ahead of the
		// "other" tier even though it isn't a direct address either.
		case strings.Contains(a.String(), "/p2p-circuit"):
			return scoreRelay
		case manet.IsIPLoopback(a):
			return scoreLoopback
		default:
			return scoreOther
		}
	}
	sorted := make([]multiaddr.Multiaddr, len(hostAddrs))
	copy(sorted, hostAddrs)
	sort.SliceStable(sorted, func(i, j int) bool { return score(sorted[i]) < score(sorted[j]) })

	addrs := make([]string, 0, len(sorted))
	for _, a := range sorted {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", a, n.peerID))
	}
	return addrs
}

// relayAddrAwaitTimeout is how long a node waits for a relay reservation to
// complete before it writes its own address into raft's persisted
// configuration -- on a join (handleAdd's join branch) and on a solo
// bootstrap alike.
//
// Measured, not guessed: against this project's own deploy target (well
// under 1 Mbps to it) 15s was not consistently enough for the reservation
// handshake itself, and the address is not revisable afterwards, so the
// wait is sized for the slow link rather than the fast one.
//
// A var rather than a const only so tests can shrink it; nothing changes it
// at runtime.
var relayAddrAwaitTimeout = 45 * time.Second

// awaitRelayAddr waits up to timeout for a /p2p-circuit address -- proof
// one of the relayCandidates reservations configured in newHost has
// completed -- to appear in n.host.Addrs(). A no-op that returns
// immediately when there are no candidates at all, since there's then
// nothing to wait for.
func (n *Node) awaitRelayAddr(timeout time.Duration) bool {
	if len(relayCandidates(n.cfg, n.store)) == 0 {
		return true
	}
	deadline := time.After(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, a := range n.host.Addrs() {
			if strings.Contains(a.String(), "/p2p-circuit") {
				return true
			}
		}
		select {
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}

// ReadyInfo is the content of ReadyFileName: what the spawning `mage
// addnode` needs to learn about a freshly started node before it can
// register it and trigger its EventAdd bootstrap.
type ReadyInfo struct {
	PeerID      string   `json:"peer_id"`
	ListenAddrs []string `json:"listen_addrs"`

	// Resumed reports that this node came up on raft state it already had
	// on disk -- it was bootstrapped or joined in some earlier process and
	// is a member of its cluster already, rather than a blank node waiting
	// for an EventAdd to give it a role.
	//
	// It exists because that distinction decides whether a *failed* join is
	// fatal. A blank node whose join is refused has nothing: it must report
	// the failure. A resumed node's join is only a re-announcement -- Run
	// has already called initRaft and the node is fully operational by the
	// time this file is written -- so a refusal ("ERR: not leader", the
	// answer from a cluster that has lost quorum until this very node
	// rejoins it) means "nothing to re-announce right now", and tearing the
	// node down over it removes the one participant that would have
	// restored the quorum. See mobile/kvmobile's startAgainst, which reads
	// this.
	Resumed bool `json:"resumed"`
}

// ReadReadyFile reads and parses the ReadyFileName written by a node in
// dataDir. It returns an error (wrapping fs.ErrNotExist) if the node hasn't
// written it yet.
func ReadReadyFile(dataDir string) (ReadyInfo, error) {
	var info ReadyInfo
	data, err := os.ReadFile(filepath.Join(dataDir, ReadyFileName))
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return info, fmt.Errorf("daemon: parse ready file: %w", err)
	}
	return info, nil
}

func (n *Node) writeReadyFile(resumed bool) error {
	info := ReadyInfo{PeerID: n.peerID, ListenAddrs: n.advertisedAddrs(), Resumed: resumed}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	path := filepath.Join(n.cfg.DataDir, ReadyFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (n *Node) handleShmEvent(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte, caller callerIdentity) shmevent.Msg {
	which := m.Which()
	if caller.remotePeer != "" && (which == shmevent.Event_Which_getPrivateKey || which == shmevent.Event_Which_getPublicKey) {
		return errorMsg(m.Id(), fmt.Errorf("%s: not available to a remote caller -- bring your own key", shmevent.EventName(which)))
	}
	if caller.remotePeer != "" && which == shmevent.Event_Which_leave {
		return errorMsg(m.Id(), fmt.Errorf("leave: not available to a remote caller -- only this node's own operator decides to leave"))
	}
	if caller.remotePeer != "" && which == shmevent.Event_Which_execInviteRedeem {
		return errorMsg(m.Id(), fmt.Errorf("exec_invite_redeem: not available to a remote caller -- this node dials sourceAddr on its own operator's behalf, never on an arbitrary remote caller's"))
	}
	if caller.remotePeer != "" && (which == shmevent.Event_Which_joinRequestCreate || which == shmevent.Event_Which_joinRequestCancel) {
		return errorMsg(m.Id(), fmt.Errorf("%s: not available to a remote caller -- only this node's own operator mints or cancels its own join-request ticket", shmevent.EventName(which)))
	}
	if caller.remotePeer != "" && which == shmevent.Event_Which_recruit {
		return errorMsg(m.Id(), fmt.Errorf("recruit: not available to a remote caller -- this node dials the ticket's device on its own operator's behalf, never on an arbitrary remote caller's"))
	}
	if caller.remotePeer != "" && which == shmevent.Event_Which_publicAccess {
		return errorMsg(m.Id(), fmt.Errorf("public_access: not available to a remote caller -- this node submits to the target cluster under its own identity, so letting a stranger trigger it would spend this node's standing on that stranger's behalf"))
	}
	if caller.remotePeer != "" && (which == shmevent.Event_Which_dialSubmitCommand || which == shmevent.Event_Which_dialQueryCommandLog) {
		return errorMsg(m.Id(), fmt.Errorf("%s: not available to a remote caller -- this node dials the target under its own identity, so letting a stranger trigger it would spend this node's standing on that stranger's behalf", shmevent.EventName(which)))
	}
	if caller.remotePeer != "" && (which == shmevent.Event_Which_channelOpen || which == shmevent.Event_Which_channelSend ||
		which == shmevent.Event_Which_channelPoll || which == shmevent.Event_Which_channelListen || which == shmevent.Event_Which_channelClose ||
		which == shmevent.Event_Which_channelCloseWrite || which == shmevent.Event_Which_channelDataReady) {
		return errorMsg(m.Id(), fmt.Errorf("%s: not available to a remote caller -- only this node's own operator drives its own channel sessions", shmevent.EventName(which)))
	}
	if shmevent.RequiresSignature(which) {
		if err := shmevent.Verify(caller.verifyPub, m, crc, sig); err != nil {
			return errorMsg(m.Id(), err)
		}
	}
	if caller.remotePeer != "" &&
		// permitRequest/permitConfirm must stay reachable by a remote
		// caller with no standing at all -- a not-yet-permitted peer has
		// no elevated standing to earn otherwise.
		which != shmevent.Event_Which_permitRequest && which != shmevent.Event_Which_permitConfirm &&
		// setKey/getKey never touch the store or any privileged state --
		// just a per-connection numeric-id relay of a value the caller
		// itself already supplied (see set's own doc comment on why this
		// exists at all) -- and bootstrapOrJoinCluster/addLearner are the
		// actual join door for a brand-new peer that owns no standing yet
		// at all (a web-app browser learner's first-ever call over this
		// exact ClientProtocolID surface): handleAddDispatch already
		// self-authenticates that remote case against the stream's own
		// libp2p identity (see its own doc comment), so gating it here too
		// would make joining impossible for the one caller class it exists
		// to admit. getVersion is likewise always open, remote or not --
		// see its own doc comment: build/version info carries no secret
		// and no side effect, so it gets the same always-answered
		// treatment as getOwnAddr rather than gating it behind cluster/
		// remote-group standing.
		which != shmevent.Event_Which_setKey && which != shmevent.Event_Which_getKey &&
		which != shmevent.Event_Which_bootstrapOrJoinCluster && which != shmevent.Event_Which_addLearner &&
		which != shmevent.Event_Which_getVersion &&
		!n.isAuthorizedForGatedAccess(caller.remotePeer, shmevent.ReservedGroupRemote) &&
		!n.isCommandLogCarveOut(m, caller.remotePeer) {
		return errorMsg(m.Id(), fmt.Errorf("%s: not permitted -- not a cluster member, in the remote group, or granted access to %s, and not a recognized public-command submission or its own log readback", caller.remotePeer, n.peerID))
	}

	switch which {
	case shmevent.Event_Which_setKey:
		value, err := m.SetKey().Value()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		n.registry.Register(m.Id(), value)
		return m

	case shmevent.Event_Which_getKey:
		grp := m.GetKey()
		v, ok := n.registry.Lookup(grp.SourceId())
		if !ok {
			return errorMsg(m.Id(), fmt.Errorf("no entry registered under id %d", grp.SourceId()))
		}
		if err := grp.SetKey(v); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_setField:
		grp := m.SetField()
		key, ok := n.registry.Lookup(grp.SourceId())
		if !ok {
			return errorMsg(m.Id(), fmt.Errorf("no key registered under id %d -- send SetKey first", grp.SourceId()))
		}
		if err := rejectReservedKey(key); err != nil {
			return errorMsg(m.Id(), err)
		}
		value, err := grp.Value()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleSetForward(ctx, key, value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_set:
		grp := m.Set()
		key, err := grp.Key()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		value, err := grp.Value()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := rejectReservedKey(key); err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleSetForward(ctx, key, value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_txn:
		ops, err := shmevent.TxnOps(m.Txn())
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		for _, op := range ops {
			if err := rejectReservedKey(op.Key); err != nil {
				return errorMsg(m.Id(), err)
			}
		}
		raftValue, err := shmevent.EncodeTxnPayload(ops)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if _, err := n.handleOpForward(ctx, kvfsm.OpTxn, nil, raftValue, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_leave:
		if err := n.leaveCluster(ctx); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_kick:
		// Same "only a raft voter may act" check permitConfirm's own doc
		// comment explains -- skipped for a local caller (trusted
		// implicitly, see that comment), enforced here for a remote one
		// since this is the only place that authenticates who *actually*
		// asked, not just whichever node last relayed the request one hop
		// closer to the leader (handleForwardKickStream's own check).
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.Id(), fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		peerID, err := m.Kick().PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.kickPeer(ctx, peerID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	// groupPut/groupDelete/commandPut/commandDelete/stationPut/
	// stationDelete/groupCommandPut/groupCommandDelete/peerGroupPut/
	// peerGroupDelete are the group-based ACL catalog's single-step CRUD
	// (see those variants' own doc comments in api/shmevent.capnp) --
	// voter-gated identically regardless of which.
	case shmevent.Event_Which_groupPut:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.GroupPut()
		id, err := grp.Id()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		name, err := grp.Name()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if shmevent.IsReservedGroupID(id) || isPeerIdentityGroupID(id) {
			return errorMsg(m.Id(), fmt.Errorf("group id %q is reserved and managed automatically", id))
		}
		key := shmevent.GroupKey([]byte(id))
		value := shmevent.EncodeGroupPayload(name, grp.Public())
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_groupDelete:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		id, err := m.GroupDelete().Id()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if shmevent.IsReservedGroupID(id) || isPeerIdentityGroupID(id) {
			return errorMsg(m.Id(), fmt.Errorf("group id %q is reserved and cannot be deleted", id))
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpCascadeDelete, shmevent.GroupKey([]byte(id)), nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_commandPut:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.CommandPut()
		id, err := grp.Id()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		name, err := grp.Name()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		peerID, err := grp.PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		var value []byte
		if grp.HasSpec() {
			spec, err := grp.Spec()
			if err != nil {
				return errorMsg(m.Id(), err)
			}
			// "Carried an empty spec" and "didn't mention the spec" must
			// stay distinguishable all the way to the FSM.
			if spec == "" {
				value, err = shmevent.EncodeCommandPayloadClearingSpec(name, []byte(peerID))
			} else {
				value, err = shmevent.EncodeCommandPayloadWithSpec(name, []byte(peerID), []byte(spec))
			}
			if err != nil {
				return errorMsg(m.Id(), err)
			}
		} else {
			value, err = shmevent.EncodeCommandPayload(name, []byte(peerID))
			if err != nil {
				return errorMsg(m.Id(), err)
			}
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, shmevent.CommandKey([]byte(id)), value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_commandDelete:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		id, err := m.CommandDelete().Id()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpCascadeDelete, shmevent.CommandKey([]byte(id)), nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_stationPut:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.StationPut()
		peerID, err := grp.PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if peerID == "" {
			return errorMsg(m.Id(), fmt.Errorf("station peer id must not be empty"))
		}
		name, err := grp.Name()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		attrs, err := grp.Attrs()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		value, err := shmevent.EncodeStationPayload(name, []byte(attrs))
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, shmevent.StationKey([]byte(peerID)), value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_stationDelete:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		peerID, err := m.StationDelete().PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		// Plain OpDel, not OpCascadeDelete: nothing references a station
		// record.
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, shmevent.StationKey([]byte(peerID)), nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_groupCommandPut:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.GroupCommandPut()
		commandID, err := grp.CommandId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		groupID, err := grp.GroupId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		key, err := shmevent.GroupCommandKey([]byte(commandID), []byte(groupID))
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_groupCommandDelete:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.GroupCommandDelete()
		commandID, err := grp.CommandId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		groupID, err := grp.GroupId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		key, err := shmevent.GroupCommandKey([]byte(commandID), []byte(groupID))
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_peerGroupPut:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.PeerGroupPut()
		peerID, err := grp.PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		groupID, err := grp.GroupId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if shmevent.IsAutoManagedGroupID(groupID) {
			return errorMsg(m.Id(), fmt.Errorf("group %q membership is managed automatically", groupID))
		}
		key, err := shmevent.PeerGroupKey([]byte(peerID), []byte(groupID))
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_peerGroupDelete:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.PeerGroupDelete()
		peerID, err := grp.PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		groupID, err := grp.GroupId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if shmevent.IsAutoManagedGroupID(groupID) {
			return errorMsg(m.Id(), fmt.Errorf("group %q membership is managed automatically", groupID))
		}
		key, err := shmevent.PeerGroupKey([]byte(peerID), []byte(groupID))
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	// permitRequest is deliberately ungated (any raft node may relay, no
	// voter check anywhere -- a not-yet-permitted peer has no elevated
	// standing to earn otherwise) and goes through handleSetForward, not
	// handleConfirmForward.
	case shmevent.Event_Which_permitRequest:
		grp := m.PermitRequest()
		peerID, err := grp.PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		metadata, err := grp.Metadata()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		key := shmevent.SystemKey(grp.Kind(), shmevent.StatusPending, []byte(peerID))
		if err := n.handleSetForward(ctx, key, []byte(metadata), true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_permitConfirm:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.PermitConfirm()
		peerID, err := grp.PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		pendingKey := shmevent.SystemKey(grp.Kind(), shmevent.StatusPending, []byte(peerID))
		confirmedKey := shmevent.SystemKey(grp.Kind(), shmevent.StatusConfirmed, []byte(peerID))
		if err := n.handleConfirmForward(ctx, kvfsm.OpConfirm, pendingKey, confirmedKey, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_permitRevoke:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.PermitRevoke()
		peerID, err := grp.PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		key := shmevent.SystemKey(grp.Kind(), shmevent.StatusConfirmed, []byte(peerID))
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_joinInviteCreate:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.JoinInviteCreate()
		token, err := grp.Token()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		value := shmevent.EncodeJoinInviteRecord(grp.Suffrage())
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, shmevent.JoinInviteKey(token), value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_joinInviteRevoke:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		token, err := m.JoinInviteRevoke().Token()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, shmevent.JoinInviteKey(token), nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_execInviteCreate:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.ExecInviteCreate()
		token, err := grp.Token()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		commandID, err := grp.CommandId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		inputsJSON, err := grp.InputsJson()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		// ttlSeconds is a relative duration on the wire; what actually gets
		// committed to the raft log (and so applied identically by every
		// replica -- see kvfsm.OpConsumeExecInvite) is the absolute
		// expiresAtUnix computed here, once, from this node's own clock at
		// the moment it received the request. 0 stays 0 (no expiry).
		var expiresAtUnix uint64
		if ttl := grp.TtlSeconds(); ttl != 0 {
			expiresAtUnix = uint64(time.Now().Unix()) + ttl
		}
		value := shmevent.EncodeExecInviteRecord(commandID, inputsJSON, expiresAtUnix)
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, shmevent.ExecInviteKey(token), value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_execInviteRevoke:
		if err := requireVoter(n, caller); err != nil {
			return errorMsg(m.Id(), err)
		}
		token, err := m.ExecInviteRevoke().Token()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, shmevent.ExecInviteKey(token), nil, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	// joinRequestCreate/joinRequestCancel manage this node's own in-memory
	// join-request ticket -- never raft/store writes, so unlike every case
	// above there's no voter check here at all (this node itself may have
	// no raft instance yet); the remote-caller rejection already happened
	// at the top of this function.
	case shmevent.Event_Which_joinRequestCreate:
		token := make([]byte, shmevent.JoinInviteTokenSize)
		if _, err := cryptorand.Read(token); err != nil {
			return errorMsg(m.Id(), fmt.Errorf("generate join request token: %w", err))
		}
		n.setJoinRequestToken(token)
		if err := m.JoinRequestCreate().SetToken(token); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_joinRequestCancel:
		token, err := m.JoinRequestCancel().Token()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		n.cancelJoinRequestToken(token)
		return m

	// recruit is the reverse of a JoinInvite: this node (already a raft
	// voter, enforced by mintJoinInvite's own handleConfirmForward the
	// same way a JoinInvite's own write is) mints a normal join invite on
	// its own cluster and hand-delivers it directly to the device named in
	// the ticket -- see dialAndPushRecruit/handleRecruitStream for the
	// actual network leg.
	case shmevent.Event_Which_recruit:
		grp := m.Recruit()
		ticket, err := grp.Ticket()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		result, err := n.dialAndPushRecruit(ctx, ticket, grp.Suffrage())
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetTicket(result); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	// execInviteRedeem is local-only (rejected above if
	// caller.remotePeer != ""): it tells this node's own daemon to dial
	// sourceAddr and redeem token there on its own operator's behalf --
	// see dialAndRedeemExecInvite for the actual network leg.
	case shmevent.Event_Which_execInviteRedeem:
		grp := m.ExecInviteRedeem()
		sourceAddr, err := grp.SourceAddr()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		token, err := grp.Token()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		instanceID, err := n.dialAndRedeemExecInvite(ctx, sourceAddr, token)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetInstanceId(instanceID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	// publicAccess is local-only for the same reason execInviteRedeem
	// above is: it makes this node act as a client of somebody else's
	// cluster, under this node's own identity -- see that variant's doc
	// comment and dialAndSubmitPublicAccess.
	case shmevent.Event_Which_publicAccess:
		grp := m.PublicAccess()
		targetPeer, err := grp.TargetPeer()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		note, err := grp.Note()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		instanceID, err := n.dialAndSubmitPublicAccess(ctx, targetPeer, note)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetInstanceId(instanceID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	// dialSubmitCommand/dialQueryCommandLog are local-only for the same
	// reason publicAccess above is -- see their doc comments and
	// dialAndSubmitCommand/dialAndQueryCommandLog.
	case shmevent.Event_Which_dialSubmitCommand:
		grp := m.DialSubmitCommand()
		targetPeer, err := grp.TargetPeer()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		commandID, err := grp.CommandId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		inputsJSON, err := grp.InputsJson()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		note, err := grp.Note()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		instanceID, err := n.dialAndSubmitCommand(ctx, targetPeer, commandID, inputsJSON, note)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetInstanceId(instanceID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_dialQueryCommandLog:
		grp := m.DialQueryCommandLog()
		targetPeer, err := grp.TargetPeer()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		instanceID, err := grp.InstanceId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		since := time.Unix(0, grp.Since())
		until := time.Unix(0, grp.Until())
		records, err := n.dialAndQueryCommandLog(ctx, targetPeer, instanceID, since, until, int(grp.Limit()))
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		resp, err := shmevent.NewDialQueryCommandLogResponse(records)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		resp.SetId(m.Id())
		return resp

	case shmevent.Event_Which_execute:
		if err := n.dispatchExecute(ctx, m); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_pollExecute:
		notif, ok := n.executeInbox.pop()
		if !ok {
			return m
		}
		if err := m.PollExecute().SetSenderPeerId(string(notif.senderPeerID)); err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := m.PollExecute().SetValue(notif.payload); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_channelOpen:
		peerID, err := m.ChannelOpen().PeerId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		channelID, err := n.dispatchChannelOpen(ctx, peerID)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := m.ChannelOpen().SetPeerId(channelID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_channelSend:
		grp := m.ChannelSend()
		channelID, err := grp.ChannelId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		chunk, err := grp.Chunk()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.dispatchChannelSend(channelID, grp.Purpose(), chunk); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_channelPoll:
		grp := m.ChannelPoll()
		channelID, err := grp.ChannelId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		status, purpose, chunk, err := n.dispatchChannelPoll(channelID)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		grp.SetStatus(status)
		grp.SetPurpose(purpose)
		if err := grp.SetChunk(chunk); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_channelListen:
		channelID, remotePeerID, err := n.dispatchChannelListen()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		grp := m.ChannelListen()
		if err := grp.SetChannelId(channelID); err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetRemotePeerId(remotePeerID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_channelClose:
		channelID, err := m.ChannelClose().ChannelId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.dispatchChannelClose(channelID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_channelCloseWrite:
		channelID, err := m.ChannelCloseWrite().ChannelId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.dispatchChannelCloseWrite(ctx, channelID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_channelDataReady:
		channelID, err := m.ChannelDataReady().ChannelId()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := n.dispatchChannelDataReady(ctx, channelID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_getFieldByRegistry:
		grp := m.GetFieldByRegistry()
		key, ok := n.registry.Lookup(grp.SourceId())
		if !ok {
			return errorMsg(m.Id(), fmt.Errorf("no key registered under id %d -- send SetKey first", grp.SourceId()))
		}
		value, err := n.handleGet(key)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetValue(value); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_getFieldByKey:
		grp := m.GetFieldByKey()
		key, err := grp.Key()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		value, err := n.handleGet(key)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetValue(value); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_listRange:
		grp := m.ListRange()
		start, err := grp.Start()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		end, err := grp.End()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		// A non-cluster-member remote caller already had to clear
		// isCommandLogCarveOut above (the top-of-function gate) to reach
		// here at all -- that check re-derives and re-validates the exact
		// same kind-scoping this case would otherwise need to repeat, so
		// there's nothing further to check here; a cluster member or a
		// "remote"-group grantee needs no additional per-kind restriction.
		matches, err := n.store.ScanRange(start, end, 1)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if len(matches) == 0 {
			return m
		}
		if err := grp.SetKey(matches[0].Key); err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := grp.SetValue(matches[0].Value); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_logAppend:
		grp := m.LogAppend()
		key, err := grp.Key()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		value, err := grp.Value()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if len(key) == 0 || key[0] != logrecord.LogKeyPrefix {
			return errorMsg(m.Id(), fmt.Errorf("log_append: key must start with the reserved logrecord prefix"))
		}
		kind, _, _, err := logrecord.ParseKey(key)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		// Same reasoning as listRange above: a non-cluster-member remote
		// caller already had to clear isCommandLogCarveOut to reach here,
		// which only ever admits a CommandRequestLogKind append -- nothing
		// further to check for any other kind, since only a cluster
		// member or a "remote"-group grantee can reach this case with one.
		// A CommandRequest append (SubmitCommand's actual write) gets its
		// own op, OpAppendCommandRequest, instead of the plain OpSet every
		// other log kind uses below -- it's what makes the submitting
		// peer's real Group/GroupCommand/PeerGroup/Public ACL standing a
		// raft-authoritative check (evaluated inside Apply, against every
		// replica's identically-ordered state) rather than a convention
		// only pkg/kvctl/mobile/kvmobile's own SubmitCommand client
		// enforces client-side -- see OpAppendCommandRequest's doc
		// comment. authorPeerID is this call's own connection-authenticated
		// identity (never anything the message itself claims): caller.remotePeer
		// for a remote caller, this node's own peer id for a local one --
		// SubmitCommand's ACL applies uniformly regardless of which. Goes
		// through handleOpForward/ForwardProtocolID, same as the plain
		// OpSet path just below -- NOT handleConfirmForward's voter-gated
		// ForwardConfirmProtocolID -- because a submitting peer need not
		// itself be a raft voter (a web-app browser tab never is, a phone
		// may join as suffrage "learner"): Apply's own ACL check is what
		// authorizes this write, not the identity of whichever node
		// happens to relay it to the leader.
		if _, ok := shmevent.ParseCommandRequestLogKind(kind); ok {
			authorPeerID := n.peerID
			if caller.remotePeer != "" {
				authorPeerID = caller.remotePeer.String()
			}
			payload, err := shmevent.EncodeCommandRequestApplyPayload(authorPeerID, value)
			if err != nil {
				return errorMsg(m.Id(), err)
			}
			if _, err := n.handleOpForward(ctx, kvfsm.OpAppendCommandRequest, key, payload, true); err != nil {
				return errorMsg(m.Id(), err)
			}
			return m
		}
		if err := n.handleSetForward(ctx, key, value, true); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_getPublicKey:
		if err := m.GetPublicKey().SetPubKey(n.ed25519Pub); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_getPrivateKey:
		if err := m.GetPrivateKey().SetPrivKey(n.ed25519Priv); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_getOwnAddr:
		if err := m.GetOwnAddr().SetAddr(n.advertisedAddrs()[0]); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_getVersion:
		resp, err := shmevent.NewGetVersionResponse(currentBuildInfo())
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		resp.SetId(m.Id())
		return resp

	case shmevent.Event_Which_bootstrapOrJoinCluster:
		leaderAddr, err := m.BootstrapOrJoinCluster().LeaderAddr()
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		peerID, err := n.handleAdd(ctx, leaderAddr)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := m.BootstrapOrJoinCluster().SetLeaderAddr(peerID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	case shmevent.Event_Which_addLearner:
		peerID, err := n.handleAddDispatch(ctx, m, caller.remotePeer)
		if err != nil {
			return errorMsg(m.Id(), err)
		}
		if err := m.AddLearner().SetAddr(peerID); err != nil {
			return errorMsg(m.Id(), err)
		}
		return m

	default:
		return errorMsg(m.Id(), fmt.Errorf("unknown event %s", shmevent.EventName(which)))
	}
}

// handleAddDispatch implements addLearner -- see that variant's doc
// comment in api/shmevent.capnp -- and returns this node's own peer id on
// success, mirroring the pre-shmevent ipcproto.ActionAdd response.
// remotePeer is the caller's own libp2p-authenticated identity ("" for a
// local pkg/ipc caller, see callerIdentity) -- checked against the
// claimed peer id below.
func (n *Node) handleAddDispatch(ctx context.Context, m shmevent.Msg, remotePeer peer.ID) (string, error) {
	grp := m.AddLearner()
	// claimedPeerId references a prior setKey holding the caller's own
	// peer id; addr is the caller's own reachable address.
	joinPeerID, ok := n.registry.Lookup(grp.ClaimedPeerId())
	if !ok {
		return "", fmt.Errorf("no peer id registered under id %d -- send SetKey first", grp.ClaimedPeerId())
	}
	// A remote caller could otherwise register any peer id string it
	// likes via setKey -- not necessarily its own -- and get it added to
	// the raft configuration at an address of its choosing. Binding the
	// claim to the stream's own authenticated identity (unforgeable --
	// established by the libp2p handshake, not anything the message
	// itself carries) closes that.
	if remotePeer != "" && string(joinPeerID) != remotePeer.String() {
		return "", fmt.Errorf("add: claimed peer id %q does not match the authenticated connection identity %s", joinPeerID, remotePeer)
	}
	addr, err := grp.Addr()
	if err != nil {
		return "", err
	}
	return n.handleAddLearner(ctx, string(joinPeerID), addr)
}

// splitInviteToken splits a trailing "#<inviteTokenHex>" off addr, if
// present, decoding the hex into raw token bytes -- see handleAdd's doc
// comment on why the token rides along inside the same string every
// existing leaderPeerID/leaderAddr caller already passes through
// unchanged, rather than a new parameter threaded through all of them.
func splitInviteToken(addr string) (cleanAddr string, token []byte, err error) {
	i := strings.IndexByte(addr, '#')
	if i < 0 {
		return addr, nil, nil
	}
	token, err = hex.DecodeString(addr[i+1:])
	if err != nil {
		return "", nil, fmt.Errorf("invalid invite token in %q: %w", addr, err)
	}
	return addr[:i], token, nil
}

func (n *Node) handleAdd(ctx context.Context, leaderPeerID string) (string, error) {
	rf, err := n.initRaft()
	if err != nil {
		return "", fmt.Errorf("init raft: %w", err)
	}

	// A trailing " learner" (peer ids/multiaddrs never contain a space)
	// requests Nonvoter suffrage for this join instead of the default
	// Voter -- rides along inside the same string every existing caller
	// already passes through unchanged (mage addfollower/kvctl.AddNode/
	// kvmobile.Join, EventAdd's own wire payload), same convention
	// splitInviteToken's own "#<token>" suffix already established, so
	// this needs no new parameter threaded through handleAdd's many
	// existing callers. Checked first (before splitInviteToken) since an
	// invite-token join's suffrage is instead decided by the invite
	// itself (consumeJoinInvite) once it reaches the leader -- this
	// marker is only ever meaningful for a plain, non-invite join.
	suffrage := raft.Voter
	if rest, ok := strings.CutSuffix(leaderPeerID, " learner"); ok {
		leaderPeerID = rest
		suffrage = raft.Nonvoter
	}

	if leaderPeerID == "" {
		// The same wait the join branch below does, for the same reason and
		// with the same permanence: whatever address advertisedAddrs picks
		// here is written into raft's persisted configuration, and nothing
		// later revises it. A node that bootstraps before its relay
		// reservation lands therefore names itself by a NAT'd or loopback
		// address forever, and no peer can dial it -- it can still reach
		// out, so replication it initiates works and the cluster looks
		// healthy, while every inbound dial (a follower forwarding a write
		// to its leader, most visibly) fails with "all dials failed".
		//
		// Found on this project's two-device Android rig, where the leader
		// is an app that solo-bootstraps seconds after launch, long before
		// AutoRelay has reserved anything: it recorded
		// /ip4/127.0.0.1/tcp/<port> as its own raft address and the second
		// device's forwarded writes could only ever succeed by reusing a
		// connection the leader itself had opened.
		//
		// Costs nothing where there is nothing to wait for: awaitRelayAddr
		// returns immediately when no relay candidates are configured,
		// which is every desktop node that isn't behind NAT.
		n.awaitRelayAddr(relayAddrAwaitTimeout)

		cfg := raft.Configuration{
			Servers: []raft.Server{{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(n.peerID),
				Address:  raft.ServerAddress(n.advertisedAddrs()[0]),
			}},
		}
		if err := rf.BootstrapCluster(cfg).Error(); err != nil {
			return "", fmt.Errorf("bootstrap: %w", err)
		}
		// BootstrapCluster only guarantees the configuration was persisted,
		// not that self-election has completed yet. A follower's join
		// request (handleJoinStream) requires State() == Leader, so make
		// the Add response wait for that instead of racing a subsequent
		// `mage addnode <leaderPeerID>` against this node's own election.
		// Scale the wait off the actual configured election timeout rather
		// than a fixed constant, so a longer WAN-tuned timeout still gets a
		// comfortable margin instead of being raced against a hardcoded window.
		if _, err := n.awaitLeader(10 * n.electionTimeout); err != nil {
			return "", fmt.Errorf("await self-election: %w", err)
		}
		if err := n.ensureReservedGroups(ctx); err != nil {
			return "", fmt.Errorf("bootstrap reserved groups: %w", err)
		}
		if err := n.ensureDefaultPublicCommand(ctx); err != nil {
			return "", fmt.Errorf("bootstrap default public command: %w", err)
		}
		return n.peerID, nil
	}

	// A trailing "#<inviteTokenHex>" (peer ids/multiaddrs never contain
	// '#') carries a one-time shmevent.KindJoinInvite token -- see
	// EncodeJoinInviteCreatePayload -- packaged into the exact same
	// leaderPeerID string every existing caller already threads through
	// unchanged (mage addfollower/kvctl.AddNode/kvmobile.Join, EventAdd's
	// own wire payload), so redeeming one needs no new API surface
	// anywhere above this function.
	leaderPeerID, inviteToken, err := splitInviteToken(leaderPeerID)
	if err != nil {
		return "", err
	}

	// leaderPeerID is either a full multiaddr (a leader on another machine,
	// e.g. a remote deployment -- there's no shared registry to resolve it
	// from) or a bare peer id created on this same machine, resolved
	// through the local registry.
	leaderAddr := leaderPeerID
	if !registry.IsMultiaddr(leaderPeerID) {
		reg, err := registry.Open()
		if err != nil {
			return "", err
		}
		leaderAddr, err = reg.ResolveAddress(leaderPeerID)
		if err != nil {
			return "", err
		}
	}

	status, err := n.join(ctx, leaderAddr, suffrage, inviteToken)
	if err != nil {
		return "", fmt.Errorf("join: %w", err)
	}
	// status is "ok" (admitted immediately, today's default behavior) or
	// "pending" (Config.RequireConfirmForJoin is set on the target and
	// this join is now a pending shmevent.KindClusterJoin record awaiting
	// a confirmed voter's approval) -- appended as a space-delimited
	// suffix since peer ids (base58, see registry.IsMultiaddr's sibling
	// assumptions elsewhere) never contain whitespace. Bootstrap/learner
	// join above return the bare peer id, implying "ok" for callers that
	// don't split on a suffix at all (e.g. bootUp's existing shmclient.Add
	// callers, which only check the error).
	return n.peerID + " " + status, nil
}

// join asks the leader reachable at leaderAddr to add this node with the
// given suffrage (Voter or Nonvoter -- see handleAdd's " learner" marker
// doc comment for how a caller requests Nonvoter), and returns "ok"
// (admitted immediately) or "pending" (lodged as a pending join request
// awaiting a confirmed voter's approval -- see Config.RequireConfirmForJoin)
// on success. inviteToken, if non-empty, is a shmevent.KindJoinInvite
// token (see EncodeJoinInviteCreatePayload) that -- if it's still valid --
// gets this request admitted immediately regardless of
// Config.RequireConfirmForJoin, with whatever suffrage the invite itself
// grants (consumeJoinInvite), not necessarily the suffrage argument here;
// see admitOrLodgeJoin.
func (n *Node) join(ctx context.Context, leaderAddr string, suffrage raft.ServerSuffrage, inviteToken []byte) (string, error) {
	// If this node needs a relay reservation to be reachable at all (see
	// Config.RelayPeers), give it a moment to complete before doing anything
	// else: AutoRelay's reservation happens asynchronously in the background
	// from newHost, and raft's configuration stores whatever address we send
	// below permanently -- getting it before the /p2p-circuit address exists
	// means the leader can never actually reach this node.
	//
	// This has to happen before opening the join stream, not just before
	// sending on it: host.NewStream returns as soon as Identify has told us
	// the remote supports JoinProtocolID, deferring the actual
	// multistream-select handshake to the stream's first Write (see
	// msmux.NewMSSelect) -- but the remote's own DefaultNegotiationTimeout
	// (10s) starts ticking the moment it sees the raw stream open, not when
	// bytes first arrive on it. Waiting here, before NewStream is ever
	// called, sidesteps that negotiation-timeout risk entirely (an earlier
	// version awaited between NewStream and the first Write instead, and
	// joins reliably failed with StreamProtocolNegotiationFailed whenever
	// RelayPeer was set as a result) -- so this wait is free to be as long
	// as a reservation genuinely needs, not bounded by that 10s window.
	//
	// A reservation that doesn't complete within this wait isn't just a
	// slower join: awaitRelayAddr gives up silently either way, and
	// whatever address n.host.Addrs() has *then* -- a real /p2p-circuit
	// address, or, if the reservation lost the race, only this node's raw
	// (often NAT'd, undialable) addresses -- is what gets sent below and
	// stored in raft's persisted configuration permanently. Get that
	// wrong and no amount of retrying a later read fixes it: the leader
	// keeps trying to deliver AppendEntries to an address that was never
	// reachable, until this node rejoins with a corrected one. Observed
	// directly against a real relay this project's own deploy target
	// (measured well under 1 Mbps to it): 15s was not consistently enough
	// for the reservation handshake itself to complete over a link that
	// slow, and every subsequent read from that follower failed
	// indefinitely as a result -- not a timing issue a retry budget on
	// the read side could ever paper over.
	n.awaitRelayAddr(relayAddrAwaitTimeout)

	maddr, err := multiaddr.NewMultiaddr(leaderAddr)
	if err != nil {
		return "", fmt.Errorf("invalid leader address %q: %w", leaderAddr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("leader address %q missing peer id: %w", leaderAddr, err)
	}
	// A leader that is itself only reachable through a relay (a phone
	// pairing with another phone, or any node whose advertised address is a
	// /p2p-circuit one) can only ever be dialed over a limited connection,
	// so both the connect and the stream have to say that's acceptable --
	// exactly what dialForward already does for the forward-* protocols,
	// and what rafttransport.Dial does for AppendEntries. Without it,
	// libp2p answers a NewStream over a relayed connection by *waiting* for
	// a direct connection that a NATed peer will never provide
	// (swarm.waitForDirectConn), so the join dies on ctx's deadline with no
	// hint that a relay was the problem.
	relayCtx := network.WithAllowLimitedConn(ctx, "join")
	if err := connectWithRetry(relayCtx, n.host, *info); err != nil {
		return "", fmt.Errorf("connect to leader %s: %w", info.ID, err)
	}

	s, err := n.host.NewStream(relayCtx, info.ID, JoinProtocolID)
	if err != nil {
		return "", fmt.Errorf("open join stream to leader %s: %w", info.ID, err)
	}
	defer s.Close()

	suffrageWord := "voter"
	if suffrage == raft.Nonvoter {
		suffrageWord = "learner"
	}
	selfAddr := n.advertisedAddrs()[0]
	reqLine := fmt.Sprintf("%s %s %s", n.peerID, selfAddr, suffrageWord)
	if len(inviteToken) > 0 {
		reqLine += " " + hex.EncodeToString(inviteToken)
	}
	if _, err := fmt.Fprintf(s, "%s\n", reqLine); err != nil {
		return "", fmt.Errorf("send join request: %w", err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("close join request: %w", err)
	}

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("read join response: %w", err)
		}
		return "", fmt.Errorf("no response from leader %s", info.ID)
	}
	line := scanner.Text()
	switch line {
	case "OK":
		return "ok", nil
	case "PENDING":
		return "pending", nil
	default:
		return "", fmt.Errorf("leader rejected join: %s", line)
	}
}

// handleJoinStream is the leader-side handler for JoinProtocolID: it parses
// "<peer-id> <multiaddr> <voter|learner>" from the requester and adds it to
// the raft configuration with the requested suffrage if this node is the
// leader, or forwards the request to whoever currently is (one hop only,
// over ForwardJoinProtocolID -- see handleForwardJoinStream) since the
// joining node has no other way to learn the real leader.
func (n *Node) handleJoinStream(s network.Stream) {
	defer s.Close()

	joinPeerID, joinAddr, suffrage, inviteToken, err := parseJoinRequest(s)
	if err != nil {
		fmt.Fprintf(s, "ERR: malformed join request: %v\n", err)
		return
	}

	rf := n.getRaft()
	if rf != nil && rf.State() == raft.Leader {
		fmt.Fprintf(s, "%s\n", n.admitOrLodgeJoin(context.Background(), rf, joinPeerID, joinAddr, suffrage, inviteToken))
		return
	}

	var leaderID raft.ServerID
	if rf != nil {
		_, leaderID = rf.LeaderWithID()
	}
	if leaderID == "" {
		fmt.Fprintf(s, "ERR: not leader\n")
		return
	}

	line, err := n.forwardJoin(context.Background(), leaderID, joinPeerID, joinAddr, suffrage, inviteToken)
	if err != nil {
		fmt.Fprintf(s, "ERR: forward join: %v\n", err)
		return
	}
	fmt.Fprintf(s, "%s\n", line)
}

// handleForwardJoinStream is the leader-side handler for
// ForwardJoinProtocolID: it adds the requester to the raft configuration
// with the requested suffrage if this node is actually the leader, or
// reports the current leader without forwarding again -- mirroring
// handleForwardSetStream's single-hop guarantee, which rules out a
// forwarding cycle regardless of how leadership bounces around.
func (n *Node) handleForwardJoinStream(s network.Stream) {
	defer s.Close()

	joinPeerID, joinAddr, suffrage, inviteToken, err := parseJoinRequest(s)
	if err != nil {
		fmt.Fprintf(s, "ERR: malformed join request: %v\n", err)
		return
	}

	rf := n.getRaft()
	if rf == nil || rf.State() != raft.Leader {
		var leaderID raft.ServerID
		if rf != nil {
			_, leaderID = rf.LeaderWithID()
		}
		fmt.Fprintf(s, "ERR: not leader; current leader is %s (already forwarded once)\n", leaderID)
		return
	}

	fmt.Fprintf(s, "%s\n", n.admitOrLodgeJoin(context.Background(), rf, joinPeerID, joinAddr, suffrage, inviteToken))
}

// parseJoinRequest reads and parses the single
// "<peer-id> <multiaddr> <voter|learner>" line that is the wire format
// shared by JoinProtocolID and ForwardJoinProtocolID. The suffrage token
// defaults to "voter" if absent, so a line written by an older build of
// this same code (before ClientProtocolID's browser-learner join existed)
// still parses the same way it always has.
func parseJoinRequest(s network.Stream) (peerID, addr string, suffrage raft.ServerSuffrage, inviteToken []byte, err error) {
	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", "", raft.Voter, nil, err
		}
		return "", "", raft.Voter, nil, fmt.Errorf("empty join request")
	}
	var suffrageWord, tokenHex string
	fields := strings.Fields(scanner.Text())
	switch len(fields) {
	case 2:
		peerID, addr = fields[0], fields[1]
		suffrageWord = "voter"
	case 3:
		peerID, addr, suffrageWord = fields[0], fields[1], fields[2]
	case 4:
		peerID, addr, suffrageWord, tokenHex = fields[0], fields[1], fields[2], fields[3]
	default:
		return "", "", raft.Voter, nil, fmt.Errorf("expected \"<peer-id> <multiaddr> [voter|learner] [inviteToken]\", got %q", scanner.Text())
	}
	switch suffrageWord {
	case "voter":
		suffrage = raft.Voter
	case "learner":
		suffrage = raft.Nonvoter
	default:
		return "", "", raft.Voter, nil, fmt.Errorf("unknown suffrage %q", suffrageWord)
	}
	if tokenHex != "" {
		inviteToken, err = hex.DecodeString(tokenHex)
		if err != nil {
			return "", "", raft.Voter, nil, fmt.Errorf("invalid invite token %q: %w", tokenHex, err)
		}
	}
	return peerID, addr, suffrage, inviteToken, nil
}

// admitOrLodgeJoin is handleJoinStream/handleForwardJoinStream's shared
// decision point, called only once rf.State()==Leader is already
// confirmed. A non-empty inviteToken takes priority over everything else:
// if it names a still-valid shmevent.KindJoinInvite record, this admits
// immediately with *that record's own* suffrage (never joinAddr's
// requested one -- see consumeJoinInvite's doc comment on why the
// invite's grant, not the requester's ask, is authoritative) regardless of
// Config.RequireConfirmForJoin, and a malformed/already-consumed token is
// a hard error rather than a silent fall-through to the slower path
// below. With no token, this runs addServerLine immediately (today's
// behavior) unless Config.RequireConfirmForJoin is set, in which case it
// instead lodges a pending shmevent.KindClusterJoin record and replies
// "PENDING" -- the actual raft.AddVoter/AddNonvoter only happens later,
// once some other confirmed voter promotes that record (see
// applyConfirm's KindClusterJoin handling).
func (n *Node) admitOrLodgeJoin(ctx context.Context, rf *raft.Raft, joinPeerID, joinAddr string, suffrage raft.ServerSuffrage, inviteToken []byte) string {
	if len(inviteToken) > 0 {
		grantedSuffrage, err := n.consumeJoinInvite(rf, inviteToken)
		if err != nil {
			return fmt.Sprintf("ERR: %v", err)
		}
		return n.addServerLine(ctx, rf, joinPeerID, joinAddr, grantedSuffrage)
	}
	if !n.cfg.RequireConfirmForJoin {
		return n.addServerLine(ctx, rf, joinPeerID, joinAddr, suffrage)
	}
	return n.lodgeJoinRequest(ctx, joinPeerID, joinAddr, suffrage)
}

// consumeJoinInvite atomically redeems token via kvfsm.OpConsumeInvite
// (rf.Apply directly, not handleConfirmForward -- every call site already
// only reaches here once rf.State()==Leader is confirmed, exactly like
// addServerLine's own precedent) and returns the suffrage it granted.
// Returns an error for a token that doesn't name a currently-valid invite
// (never created, already redeemed, or already revoked) -- deliberately
// never falls back to Config.RequireConfirmForJoin's pending-lodge
// behavior in that case, so a caller that supplied a bad token gets a
// clear rejection instead of silently landing in a different, slower
// admission path it didn't ask for.
func (n *Node) consumeJoinInvite(rf *raft.Raft, token []byte) (raft.ServerSuffrage, error) {
	key := shmevent.JoinInviteKey(token)
	cmd := kvfsm.EncodeCommand(kvfsm.OpConsumeInvite, key, nil)
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return raft.Voter, fmt.Errorf("invite: %w", err)
	}
	res, ok := future.Response().(kvfsm.ApplyResult)
	if !ok {
		return raft.Voter, fmt.Errorf("invite: unexpected apply response type %T", future.Response())
	}
	if res.Err != nil {
		return raft.Voter, fmt.Errorf("invalid or already-used invite: %w", res.Err)
	}
	sf, err := shmevent.DecodeJoinInviteRecord(res.Value)
	if err != nil {
		return raft.Voter, fmt.Errorf("invite: %w", err)
	}
	if sf == shmevent.SuffrageLearner {
		return raft.Nonvoter, nil
	}
	return raft.Voter, nil
}

// dialAndRedeemExecInvite is EventExecInviteRedeem's local-only handler: it
// resolves sourceAddr (a multiaddr, or a bare peer id already known to this
// machine's local registry -- same resolution handleAdd's leaderAddr
// already does), builds a self-contained EventExecInviteRedeem message
// naming this node's own peer id and token (EncodeExecuteNotification,
// reused from EventExecute's identical need), signs it with this node's
// own key (n.ed25519Priv -- the actual "peer signs it" step), and dials
// ExecInviteRedeemProtocolID directly at sourceAddr. Returns the new
// instance id on success.
func (n *Node) dialAndRedeemExecInvite(ctx context.Context, sourceAddr string, token []byte) (string, error) {
	info, err := resolveDialAddr(n.cfg, n.store, sourceAddr)
	if err != nil {
		return "", err
	}
	// sourceAddr comes off a scanned ticket, which for any NATed issuer is
	// a /p2p-circuit address -- see join's comment on why a relayed dial
	// needs to be allowed explicitly on both the connect and the stream.
	relayCtx := network.WithAllowLimitedConn(ctx, "exec_invite_redeem")
	if err := connectWithRetry(relayCtx, n.host, *info); err != nil {
		return "", fmt.Errorf("connect to %s: %w", info.ID, err)
	}

	notif, err := shmevent.NewExecInviteRedeemNotification(n.peerID, token)
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: build notification: %w", err)
	}
	buf, err := shmevent.Encode(notif, n.ed25519Priv)
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: encode message: %w", err)
	}

	s, err := n.host.NewStream(relayCtx, info.ID, ExecInviteRedeemProtocolID)
	if err != nil {
		return "", fmt.Errorf("open exec invite redeem stream to %s: %w", info.ID, err)
	}
	defer s.Close()

	if _, err := s.Write(buf); err != nil {
		return "", fmt.Errorf("exec invite redeem: write to %s: %w", info.ID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("exec invite redeem: close write to %s: %w", info.ID, err)
	}

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("exec invite redeem: read response from %s: %w", info.ID, err)
		}
		return "", fmt.Errorf("no response from %s", info.ID)
	}
	line := scanner.Text()
	if rest, ok := strings.CutPrefix(line, "OK "); ok {
		return rest, nil
	}
	return "", fmt.Errorf("exec invite redeem rejected: %s", strings.TrimPrefix(line, "ERR: "))
}

// resolveDialAddr turns addr -- a multiaddr, or a bare peer id -- into the
// peer.AddrInfo to dial. A bare peer id resolves two ways, tried in
// order: first against this machine's own local registry (an identity
// *this machine* itself created or joined via addnode/addfollower/etc --
// see pkg/registry's doc comment), then, if that fails, as a genuinely
// external peer id reachable only through one of this node's own known
// relay candidates (relayCandidates -- the same Config.RelayPeers/
// confirmed-KindBootstrapNode list newHost's own AutoRelay setup already
// draws from), on the working assumption that a peer worth dialing this
// way holds a reservation on one of those relays the same way every other
// NATed device in this mesh does. Every resolveDialAddr caller (dial-and-
// act helpers like dialAndSubmitCommand/dialAndQueryCommandLog/
// dialAndSubmitPublicAccess/dialAndRedeemExecInvite) benefits from this
// uniformly -- bare-peer-id dialing needs no per-caller opt-in.
func resolveDialAddr(cfg Config, st *store.Store, addr string) (*peer.AddrInfo, error) {
	if registry.IsMultiaddr(addr) {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid address %q: %w", addr, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("address %q missing peer id: %w", addr, err)
		}
		return info, nil
	}

	reg, err := registry.Open()
	if err == nil {
		if resolved, resolveErr := reg.ResolveAddress(addr); resolveErr == nil {
			maddr, err := multiaddr.NewMultiaddr(resolved)
			if err != nil {
				return nil, fmt.Errorf("invalid address %q: %w", resolved, err)
			}
			info, err := peer.AddrInfoFromP2pAddr(maddr)
			if err != nil {
				return nil, fmt.Errorf("address %q missing peer id: %w", resolved, err)
			}
			return info, nil
		}
	}

	targetID, err := peer.Decode(addr)
	if err != nil {
		return nil, fmt.Errorf("%q is neither a multiaddr, a peer id created on this machine, nor a valid peer id: %w", addr, err)
	}
	candidates := relayCandidates(cfg, st)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%q is not a peer id created on this machine, and this node has no relay candidates to try reaching it through", addr)
	}
	info := &peer.AddrInfo{ID: targetID}
	for _, relay := range candidates {
		for _, relayAddr := range relay.Addrs {
			circuit, err := multiaddr.NewMultiaddr(relayAddr.String() + "/p2p/" + relay.ID.String() + "/p2p-circuit/p2p/" + targetID.String())
			if err != nil {
				continue
			}
			info.Addrs = append(info.Addrs, circuit)
		}
	}
	if len(info.Addrs) == 0 {
		return nil, fmt.Errorf("%q is not a peer id created on this machine, and no usable relay-circuit address could be built for it", addr)
	}
	return info, nil
}

// connectRetryAttempts/connectRetryDelay bound connectWithRetry -- mirrors
// web-app/src/p2p.rs's DIAL_RETRY_ATTEMPTS/DIAL_RETRY_DELAY_MS and its own
// reasoning: a single lost TCP handshake on a real WAN link is ordinary
// packet loss on that one attempt, not systemic unreachability, so it's
// worth one immediate retry or two before surfacing an error.
const connectRetryAttempts = 3

const connectRetryDelay = 500 * time.Millisecond

// connectRetryReservationAttempts/connectRetryReservationDelay bound the
// extended retry connectWithRetry falls back to when every attempt above
// fails with a retriable relay-circuit status (see
// isRetriableRelayCircuitError) -- caught live pairing two Android devices
// through the deployed relay: the destination's own AutoRelay was still
// (re-)establishing its relay reservation at the moment of the dial, and
// that reservation round trip was independently observed taking 24-46s
// against the same relay in the same run (this project's own
// RequestRelayAccess step does the identical underlying work) -- far past
// connectRetryAttempts/connectRetryDelay's ~1.5s budget, which exists for
// ordinary packet loss, not a real in-progress reconnect on the *other*
// end. Neither NO_RESERVATION nor CONNECTION_FAILED is evidence the peer
// is unreachable -- both are the relay's own explicit "not right now"
// answers (see isRetriableRelayCircuitError's own doc comment on the
// difference between them) -- so it is worth riding out with a dedicated,
// longer budget instead of failing the join/recruit/exec-invite outright.
//
// A first pass at 6 attempts/8s (~48s total) fixed join() outright
// (previously reproduced 3/3 times) but still lost the same race once at
// a sibling call site (dialAndPushRecruit) in the very next verification
// run -- 24-46s was this project's own observed *range*, not a ceiling,
// so a ~48s budget had no margin left for a slower instance. 90s matches
// recruitJoinTimeout, this codebase's own existing budget for the
// identical class of wait (handleRecruitStream's in-process join call).
// CONNECTION_FAILED was added to isRetriableRelayCircuitError after a
// live verification run hit it (not NO_RESERVATION) on an otherwise
// healthy relay/destination pair -- see that function's own doc comment.
const connectRetryReservationAttempts = 11

const connectRetryReservationDelay = 9 * time.Second

// isRetriableRelayCircuitError reports whether err is (or wraps) a
// circuitv2 client dial failure whose relay-reported status is one this
// project has caught live, against its own real deployed relay, as a
// transient "not right now" rather than genuine unreachability:
//
//   - NO_RESERVATION: the destination has no active reservation on file at
//     the relay -- its own AutoRelay may simply not have (re-)established
//     one yet (see connectRetryReservationAttempts' doc comment).
//   - CONNECTION_FAILED: the relay *does* have a reservation for the
//     destination, but its own attempt to open the hop connection to it
//     (relay.go's Stop-protocol dial, after the reservation check already
//     passed) failed or timed out -- a live, momentary hop failure between
//     relay and destination, not evidence the destination is actually
//     unreachable. Caught live in the very next verification run after
//     NO_RESERVATION was fixed, on an otherwise fully healthy relay and
//     destination.
//
// Deliberately narrow: a status like PERMISSION_DENIED or
// RESERVATION_REFUSED means the relay flatly refuses this peer standing,
// which a longer retry budget cannot fix and should keep failing fast on.
// go-libp2p's own relayError type (p2p/protocol/circuitv2/client/dial.go)
// is unexported with no status accessor, so matching the status's own
// wire name in the formatted error text is the only way to distinguish
// these specific, recoverable cases from any other dial failure.
func isRetriableRelayCircuitError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, pbv2.Status_NO_RESERVATION.String()) ||
		strings.Contains(msg, pbv2.Status_CONNECTION_FAILED.String())
}

// isRetriableDialResetError reports whether err is (or wraps) a bare
// ECONNRESET surfacing anywhere in a dial attempt -- including mid
// security-protocol negotiation, before either side has said anything
// meaningful yet. Caught live running this project's own e2e:all against
// the real deployed bootstrap host, on a *direct* dial (not a
// /p2p-circuit hop, so isRetriableRelayCircuitError's own statuses never
// match it): "failed to negotiate security protocol: read tcp4
// ...->host:4101: read: connection reset by peer", reproduced twice in a
// row, on an independently confirmed-healthy destination (no firewall/
// rate-limit/resource pressure on the remote host, and 5/5 plain TCP
// connects from elsewhere succeeded instantly) -- meaning the reset
// happened somewhere on the path itself, not because either endpoint
// refused anything.
//
// Deliberately as narrow as isRetriableRelayCircuitError is, for the
// identical reason: ECONNRESET is a live, transient network-layer event
// (the multi-round-trip equivalent of one lost packet), not a decision
// either side made -- unlike ECONNREFUSED (nothing listening at all) or
// an actual security-protocol/authentication mismatch, neither of which
// a longer wait ever fixes, so neither should be swept into this.
func isRetriableDialResetError(err error) bool {
	return err != nil && errors.Is(err, syscall.ECONNRESET)
}

// clearDialBackoff drops go-libp2p's own per-peer dial backoff for pid, so
// the very next dial actually reaches the network instead of being refused
// locally with "dial backoff".
//
// That backoff exists to stop a host hammering a peer that keeps failing,
// and it is right for the *background* dialing libp2p does on its own --
// but it is charged per peer, not per caller, so a background failure
// silences an unrelated, explicitly requested dial to the same peer for
// the next 5+ seconds. This project hits that constantly on a fresh
// device: AutoRelay starts dialing the baked-in relay the instant the
// daemon comes up, before any standing exists for it to succeed, and the
// operator's own RequestRelayAccess call moments later -- the very call
// that would grant that standing -- fails with "dial backoff" against a
// peer this process never consciously dialed itself. Caught live on both
// emulators running the two-device pair scenario, and the reason
// pkg/e2erun/android_pair.go grew per-step retries whose only real effect
// was to get a fresh process with an empty backoff table.
//
// Every caller here is an explicit, operator- or protocol-initiated
// request with a target it was handed deliberately (a relay to escalate
// against, a leader to join, a device that just showed its ticket), so
// none of them should inherit a background subsystem's opinion about that
// peer. The type assertion keeps this soft: a host whose network isn't a
// *swarm.Swarm simply skips it.
func clearDialBackoff(h lp2phost.Host, info peer.AddrInfo) {
	type backoffClearer interface{ Backoff() *swarm.DialBackoff }
	sw, ok := h.Network().(backoffClearer)
	if !ok {
		return
	}
	sw.Backoff().Clear(info.ID)
	// A /p2p-circuit address is dialed by first dialing the relay named in
	// its own left-hand side, and that hop carries its own backoff entry.
	// Clearing only the destination therefore fixes nothing for the case
	// this exists for: caught live on device A of the two-device pair
	// scenario, whose recruit dial to a relayed peer failed with "error
	// opening hop stream to relay: ... dial backoff" -- backoff against the
	// relay, armed by AutoRelay moments earlier, while the destination's
	// own entry was clean.
	for _, addr := range info.Addrs {
		multiaddr.ForEach(addr, func(c multiaddr.Component) bool {
			if c.Protocol().Code != multiaddr.P_P2P {
				return true
			}
			if hop, err := peer.IDFromBytes(c.RawValue()); err == nil && hop != info.ID {
				sw.Backoff().Clear(hop)
			}
			return true
		})
	}
}

// connectWithRetry calls h.Connect(ctx, info), clearing any stale dial
// backoff first (see clearDialBackoff) and retrying up to
// connectRetryAttempts times on failure -- see connectRetryAttempts' own
// doc comment. Used by every explicitly-requested dial in this file
// (dialAndSubmitPublicAccess, join, dialAndPushRecruit,
// dialAndRedeemExecInvite), whose callers -- a fresh device with no
// standing anywhere yet, a device redeeming a ticket someone physically
// handed it -- have nothing else to fall back on if this one dial has a
// bad moment: caught live, running this project's own e2e pair scenario, a
// plain unretried Connect intermittently failed with a bare TCP dial
// timeout to a real, otherwise-reachable remote target.
//
// If every attempt in that first, short budget fails with a retriable
// relay-circuit status (see isRetriableRelayCircuitError) or a bare
// connection reset (see isRetriableDialResetError), one more, much longer
// round runs on top of it (see connectRetryReservationAttempts' own doc
// comment) rather than surfacing the error immediately -- none of those
// cases mean the destination is actually unreachable.
func connectWithRetry(ctx context.Context, h lp2phost.Host, info peer.AddrInfo) error {
	lastErr := dialAttempts(ctx, h, info, connectRetryAttempts, connectRetryDelay)
	if lastErr == nil || (!isRetriableRelayCircuitError(lastErr) && !isRetriableDialResetError(lastErr)) {
		return lastErr
	}
	return dialAttempts(ctx, h, info, connectRetryReservationAttempts, connectRetryReservationDelay)
}

// dialAttempts is connectWithRetry's shared retry loop, parameterized on
// attempt count/delay so the same clearDialBackoff-then-Connect sequence
// backs both its short default budget and its longer NO_RESERVATION
// fallback.
func dialAttempts(ctx context.Context, h lp2phost.Host, info peer.AddrInfo, attempts int, delay time.Duration) error {
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		// Cleared on every attempt, not just the first: a failed attempt of
		// our own re-arms it, and the delay below is deliberately shorter
		// than the backoff would be.
		clearDialBackoff(h, info)
		if err := h.Connect(ctx, info); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < attempts {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

// dialAndSubmitCommand is EventDialSubmitCommand's local-only handler, and
// dialAndSubmitPublicAccess's own core logic generalized from one hardcoded
// commandID (DefaultPublicCommandID) to any commandID/inputsJSON pair. It
// dials targetAddr's ClientProtocolID -- the remote-client surface, not a
// local ipc.Call -- and sends one signed EventLogAppend carrying a
// pkg/logrecord record under shmevent.CommandRequestLogKind(commandID),
// attributed to this node's own peer id -- byte for byte what a local
// SubmitCommand would write into its own log, just delivered over the wire
// into a stranger cluster's log instead.
//
// Admission hinges on exactly two things, both of which this builds
// deliberately rather than incidentally: the *key's* logrecord kind, which
// is where the target's kvfsm.OpAppendCommandRequest reads the command id
// back out of (ParseCommandRequestLogKind) before checking
// IsPermittedForCommand -- generic over commandID, so this succeeds for any
// command the target has linked to a public group, and is rejected exactly
// like an unpermitted local SubmitCommand otherwise -- and the message
// signature, which is where the target reads the author peer id its own
// dispatcher will see. inputsJSON rides along as the record's "inputs"
// field, mirroring SubmitCommand's own field name, so the target's
// RunCommandDispatcher (which only ever reads records this shape, whether
// written locally or via this carve-out) can't tell the two apart. What's
// deliberately *not* replicated is SubmitCommand's exec-index and
// Execute-poke bookkeeping, which serves the *caller's* own dispatcher
// bookkeeping (nothing here has one) and has no bearing on the target
// accepting the write; the target's own rescanTicker picks it up instead
// (see pkg/kvctl.RunCommandDispatcher's doc comment).
//
// note, if non-empty, rides along as the request's "note" field -- purely
// informational (who/what asked), never consulted by permission checks.
func (n *Node) dialAndSubmitCommand(ctx context.Context, targetAddr, commandID, inputsJSON, note string) (string, error) {
	info, err := resolveDialAddr(n.cfg, n.store, targetAddr)
	if err != nil {
		return "", err
	}
	if info.ID.String() == n.peerID {
		return "", fmt.Errorf("dial_submit_command: %s is this node itself -- a node already has every standing in its own cluster", n.peerID)
	}
	if err := connectWithRetry(ctx, n.host, *info); err != nil {
		return "", fmt.Errorf("connect to %s: %w", info.ID, err)
	}

	var raw [16]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("dial_submit_command: generate instance id: %w", err)
	}
	instanceID := hex.EncodeToString(raw[:])

	rnd, err := logrecord.NewRand()
	if err != nil {
		return "", fmt.Errorf("dial_submit_command: %w", err)
	}
	kind := shmevent.CommandRequestLogKind(commandID)
	ts := time.Now()
	key, err := logrecord.BuildKey(kind, instanceID, ts, rnd)
	if err != nil {
		return "", fmt.Errorf("dial_submit_command: %w", err)
	}
	fields := map[string]string{"command_id": commandID}
	if inputsJSON != "" {
		fields["inputs"] = inputsJSON
	}
	if note != "" {
		fields["note"] = note
	}
	value, err := logrecord.Record{
		Kind:         kind,
		UnitID:       instanceID,
		Timestamp:    ts,
		AuthorPeerID: n.peerID,
		Fields:       fields,
	}.Encode()
	if err != nil {
		return "", fmt.Errorf("dial_submit_command: encode record: %w", err)
	}
	logMsg, err := shmevent.NewLogAppend(key, value)
	if err != nil {
		return "", fmt.Errorf("dial_submit_command: build message: %w", err)
	}
	buf, err := shmevent.Encode(logMsg, n.ed25519Priv)
	if err != nil {
		return "", fmt.Errorf("dial_submit_command: encode message: %w", err)
	}

	// A target worth dialing directly usually has a real public address,
	// but nothing says the caller can only reach it directly -- allow a
	// limited/relay connection the same way dialForward does, so this works
	// from behind NAT via a relay this node already has.
	s, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "dial_submit_command"), info.ID, ClientProtocolID)
	if err != nil {
		return "", fmt.Errorf("open client stream to %s: %w", info.ID, err)
	}
	defer s.Close()

	if _, err := s.Write(buf); err != nil {
		return "", fmt.Errorf("dial_submit_command: write to %s: %w", info.ID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("dial_submit_command: close write to %s: %w", info.ID, err)
	}
	respBuf, err := io.ReadAll(s)
	if err != nil {
		return "", fmt.Errorf("dial_submit_command: read response from %s: %w", info.ID, err)
	}
	resp, _, _, err := shmevent.Decode(respBuf)
	if err != nil {
		return "", fmt.Errorf("dial_submit_command: decode response from %s: %w", info.ID, err)
	}
	if resp.Which() == shmevent.Event_Which_error {
		errMsg, _ := resp.Error().Message_()
		return "", fmt.Errorf("%s rejected by %s: %s", commandID, info.ID, errMsg)
	}
	return instanceID, nil
}

// dialAndSubmitPublicAccess is EventPublicAccess's local-only handler (see
// that event's doc comment). It dials targetAddr's ClientProtocolID -- the
// remote-client surface, not a local ipc.Call -- and sends one signed
// EventLogAppend carrying a pkg/logrecord record under
// shmevent.CommandRequestLogKind(shmevent.DefaultPublicCommandID),
// attributed to this node's own peer id.
//
// The grant itself hinges on exactly two things, both of which this builds
// deliberately rather than incidentally: the *key's* logrecord kind, which
// is where the target's kvfsm.OpAppendCommandRequest reads the command id
// back out of (ParseCommandRequestLogKind) before matching it against
// DefaultPublicCommandID, and the message signature, which is where it
// gets the author peer id it grants membership to. The record's own
// "command_id"/"note" fields are informational -- they exist so the
// target's GetCommandRequest/ListCommandRequests read this back as an
// ordinary dispatch, indistinguishable from a local SubmitCommand, rather
// than as some bespoke half-record. What's deliberately *not* replicated
// is SubmitCommand's exec-index and Execute-poke bookkeeping, which serves
// the target's own dispatcher (nothing here waits to be dispatched) and
// has no bearing on the grant.
//
// note, if non-empty, rides along as the request's "note" field -- purely
// informational (who/what asked), never consulted by the grant itself.
//
// This is now a thin caller of dialAndSubmitCommand (commandID=
// DefaultPublicCommandID, inputsJSON=""), keeping only the relay-specific
// tail below: waiting for a reservation to land is meaningful for
// public-access (the whole point is becoming reachable *through* the
// target if it's a relay), but would be a non sequitur for an arbitrary
// commandID dialAndSubmitCommand's other callers submit -- see that
// function's doc comment.
func (n *Node) dialAndSubmitPublicAccess(ctx context.Context, targetAddr, note string) (string, error) {
	info, err := resolveDialAddr(n.cfg, n.store, targetAddr)
	if err != nil {
		return "", err
	}
	instanceID, err := n.dialAndSubmitCommand(ctx, targetAddr, shmevent.DefaultPublicCommandID, "", note)
	if err != nil {
		return "", err
	}

	// The grant is committed, but standing alone doesn't make this node
	// reachable -- AutoRelay still has to notice and land a reservation,
	// which it does on its own schedule (relayReserveBackoff), tens of
	// seconds later on a slow device. A caller asking a relay for standing
	// is asking to become reachable *through that relay*, and the only
	// thing it can usefully do next -- hand out its own address -- is wrong
	// until that reservation exists. So wait for it here, rather than
	// returning success and leaving every caller to invent its own
	// poll-and-hope loop: pkg/e2erun/android_pair.go had exactly that, with
	// per-step retries and fixed multi-second settles that were still
	// sometimes too short, and a phone's UI would need the same.
	//
	// Only when the target actually is one of this node's own relay
	// candidates: EventPublicAccess can name any cluster at all, and
	// waiting for a relay address after a grant from a cluster this node
	// doesn't relay through would be waiting for something that call was
	// never going to cause. awaitRelayAddr itself returns immediately if a
	// circuit address is already there, or if this node has no relay
	// candidates configured.
	//
	// A wait that runs out is reported as an error, not swallowed. The
	// grant did commit, but the caller asked to become reachable and
	// isn't: returning success there means the next thing it does --
	// handing out an address that is still only loopback, or letting a
	// peer dial it -- fails somewhere far less legible. Caught exactly
	// that way live: the two-device pair scenario's recruit dial came back
	// "NO_RESERVATION" from the relay, with nothing to say the recruited
	// device's own earlier "relay access granted" had never actually
	// produced a reservation.
	if n.isRelayCandidate(info.ID) && hasPublicAddr(info.Addrs) {
		if !n.awaitRelayAddr(relayAccessAwaitTimeout) && !n.hasConfirmedReachableAddr() {
			return "", fmt.Errorf("public_access: %s granted relay standing, but no relay reservation appeared within %s -- this node is still not reachable through it", info.ID, relayAccessAwaitTimeout)
		}
	}
	return instanceID, nil
}

// dialAndQueryCommandLog is EventDialQueryCommandLog's local-only handler:
// the direct-dial counterpart of LatestCommandLog/QueryCommandLog's local
// ListRange loop, reading back targetAddr's own
// shmevent.CommandExecLogKind(instanceID) entries in [since, until] (up to
// limit records, 0 meaning unbounded) instead of this node's own local
// store. Unlike dialAndSubmitCommand, admission needs no prior standing at
// all: the target's isCommandLogReadableKind returns true unconditionally
// for CommandExecLogKind -- "possessing the instance id is the credential"
// (see that function's doc comment) -- so this only ever fails on
// reachability, never permission.
//
// Connects once, then opens one ClientProtocolID stream per page -- there
// is no persistent session the way pkg/shmclient.Session's local IPC has,
// so each EventListRange round trip is its own stream, mirroring the
// per-call dial pkg/shmclient.Session.ListRange's remote counterpart would
// need if it existed.
func (n *Node) dialAndQueryCommandLog(ctx context.Context, targetAddr, instanceID string, since, until time.Time, limit int) ([]logrecord.Record, error) {
	info, err := resolveDialAddr(n.cfg, n.store, targetAddr)
	if err != nil {
		return nil, err
	}
	if info.ID.String() == n.peerID {
		return nil, fmt.Errorf("dial_query_command_log: %s is this node itself -- read its command log locally instead", n.peerID)
	}
	if err := connectWithRetry(ctx, n.host, *info); err != nil {
		return nil, fmt.Errorf("connect to %s: %w", info.ID, err)
	}

	lo, hi := logrecord.ScanBounds(shmevent.CommandExecLogKind, instanceID, since, until)
	var records []logrecord.Record
	for limit <= 0 || len(records) < limit {
		queryMsg, err := shmevent.NewListRange(lo, hi)
		if err != nil {
			return nil, fmt.Errorf("dial_query_command_log: build query: %w", err)
		}
		buf, err := shmevent.Encode(queryMsg, n.ed25519Priv)
		if err != nil {
			return nil, fmt.Errorf("dial_query_command_log: encode message: %w", err)
		}

		s, err := n.host.NewStream(network.WithAllowLimitedConn(ctx, "dial_query_command_log"), info.ID, ClientProtocolID)
		if err != nil {
			return nil, fmt.Errorf("open client stream to %s: %w", info.ID, err)
		}
		_, writeErr := s.Write(buf)
		closeErr := s.CloseWrite()
		if writeErr != nil {
			s.Close()
			return nil, fmt.Errorf("dial_query_command_log: write to %s: %w", info.ID, writeErr)
		}
		if closeErr != nil {
			s.Close()
			return nil, fmt.Errorf("dial_query_command_log: close write to %s: %w", info.ID, closeErr)
		}
		respBuf, err := io.ReadAll(s)
		s.Close()
		if err != nil {
			return nil, fmt.Errorf("dial_query_command_log: read response from %s: %w", info.ID, err)
		}
		resp, _, _, err := shmevent.Decode(respBuf)
		if err != nil {
			return nil, fmt.Errorf("dial_query_command_log: decode response from %s: %w", info.ID, err)
		}
		if resp.Which() == shmevent.Event_Which_error {
			errMsg, _ := resp.Error().Message_()
			return nil, fmt.Errorf("dial_query_command_log rejected by %s: %s", info.ID, errMsg)
		}
		respGrp := resp.ListRange()
		key, err := respGrp.Key()
		if err != nil {
			return nil, fmt.Errorf("dial_query_command_log: response key: %w", err)
		}
		if len(key) == 0 {
			break
		}
		value, err := respGrp.Value()
		if err != nil {
			return nil, fmt.Errorf("dial_query_command_log: response value: %w", err)
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return nil, fmt.Errorf("dial_query_command_log: decode record: %w", err)
		}
		records = append(records, rec)
		lo = append(append([]byte{}, key...), 0x00)
	}
	return records, nil
}

// hasPublicAddr reports whether any of addrs is internet-routable. It
// gates the reservation wait above entirely: go-libp2p only advertises
// circuit addresses built on a relay's *public* addresses, so a relay
// reachable only on a LAN or loopback address can never produce one
// however well the reservation itself went -- waiting for one would be
// waiting forever, and failing on its absence would be reporting a
// problem that isn't. That is the ordinary case on a single machine,
// including this project's own hermetic relay tests, where the
// reservation demonstrably works (a peer dials the circuit address by
// hand) while no address is ever published.
func hasPublicAddr(addrs []multiaddr.Multiaddr) bool {
	for _, a := range addrs {
		if manet.IsPublicAddr(a) {
			return true
		}
	}
	return false
}

// hasConfirmedReachableAddr reports whether go-libp2p has verified (via
// AutoNAT) that some address of this node is reachable from the outside.
// When it has, this node advertises those addresses and deliberately does
// *not* advertise relay ones (go-libp2p's address manager drops relay
// addresses once any address is confirmed reachable) -- so "no circuit
// address appeared" means the relay isn't needed here, not that anything
// failed. That is the ordinary case for a node on a machine that really is
// directly dialable, including this project's own same-machine tests (see
// TestJoinThroughRelay's note on the same behavior); a phone behind
// carrier NAT never confirms one, which is why it must treat the same
// silence as a failure.
func (n *Node) hasConfirmedReachableAddr() bool {
	type confirmedAddrs interface {
		ConfirmedAddrs() (reachable, unreachable, unknown []multiaddr.Multiaddr)
	}
	h, ok := n.host.(confirmedAddrs)
	if !ok {
		return false
	}
	reachable, _, _ := h.ConfirmedAddrs()
	return len(reachable) > 0
}

// relayAccessAwaitTimeout bounds the reservation wait dialAndSubmitPublicAccess
// does after a successful grant. Generous because the wait is the useful
// part of the call on a fresh device (see that call site's comment) and
// because a real phone through a real relay was measured taking ~30-40s
// from grant to reservation, several times longer than the same sequence
// on a desktop; short enough that a caller whose reservation is never
// going to land still gets its answer back.
const relayAccessAwaitTimeout = 90 * time.Second

// isRelayCandidate reports whether pid is one of the relays this node
// would actually reserve a circuit through (see relayCandidates).
func (n *Node) isRelayCandidate(pid peer.ID) bool {
	for _, candidate := range relayCandidates(n.cfg, n.store) {
		if candidate.ID == pid {
			return true
		}
	}
	return false
}

// recruitJoinTimeout bounds handleRecruitStream's in-process call to
// handleAdd -- generous enough to cover join()'s own worst-case relay-
// reservation wait (up to 45s, see join's doc comment) plus a real margin,
// since the dialing peer (dialAndPushRecruit) is blocked reading this
// stream's response for the entire duration.
const recruitJoinTimeout = 90 * time.Second

// mintJoinInvite generates a fresh, cryptographically random one-time
// shmevent.KindJoinInvite token granting suffrage and lodges it the same
// way EventJoinInviteCreate's own case does (handleConfirmForward) -- the
// token is generated here, server-side, rather than supplied by the
// caller, since EventRecruit's own caller only supplies a ticket +
// suffrage, never a pre-chosen invite token.
func (n *Node) mintJoinInvite(ctx context.Context, suffrage byte) ([]byte, error) {
	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := cryptorand.Read(token); err != nil {
		return nil, fmt.Errorf("generate invite token: %w", err)
	}
	key := shmevent.JoinInviteKey(token)
	if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, shmevent.EncodeJoinInviteRecord(suffrage), true); err != nil {
		return nil, err
	}
	return token, nil
}

// dialAndPushRecruit is EventRecruit's local-only handler: ticket is
// "<device's own multiaddr>#<tokenHex>" (see EventJoinRequestCreate),
// exactly the same "<addr>#<tokenHex>" shape splitInviteToken already
// parses everywhere else in this codebase. It mints a normal join invite on
// this node's own cluster (mintJoinInvite), then dials the device directly
// at its own address over RecruitProtocolID and hands it "<this node's own
// advertised address> <inviteTokenHex> <correlationTokenHex>" -- a plain
// text line, not a signed shmevent.Msg, mirroring JoinProtocolID's own
// reqLine (see join): the ticket's correlation token is itself the
// credential the device already trusts (it minted the token itself, and
// only handed it to whoever physically scanned the resulting barcode), so
// there's nothing left to sign. Returns the device's own join result
// ("<peerID> ok"/"<peerID> pending", the same shape handleAdd itself
// returns) on success.
func (n *Node) dialAndPushRecruit(ctx context.Context, ticket string, suffrage byte) (string, error) {
	deviceAddr, correlationToken, err := splitInviteToken(ticket)
	if err != nil {
		return "", err
	}
	if len(correlationToken) == 0 {
		return "", fmt.Errorf("recruit: ticket %q missing a correlation token", ticket)
	}
	// Checked before anything else is spent on this ticket (a join invite
	// gets minted just below, and a minted invite is a real raft record):
	// a ticket whose address half is empty is a caller-side assembly bug --
	// "<addr>#<token>" built from an address that was never obtained -- and
	// without this it surfaced as a confusing failure much further down,
	// inside address resolution, naming neither the ticket nor the reason.
	if deviceAddr == "" {
		return "", fmt.Errorf("recruit: ticket %q has no device address -- expected \"<multiaddr>#<tokenHex>\"", ticket)
	}

	inviteToken, err := n.mintJoinInvite(ctx, suffrage)
	if err != nil {
		return "", fmt.Errorf("recruit: mint join invite: %w", err)
	}

	addr := deviceAddr
	if !registry.IsMultiaddr(addr) {
		// A bare peer id is only resolvable against this machine's own
		// local registry, which is a desktop notion: on Android there is
		// no registry (and no writable home directory to put one in --
		// registry.Open there fails with a bare "mkdir /sdcard/...:
		// permission denied", which tells a caller nothing about the
		// ticket it actually passed). Say what was wrong with the ticket
		// instead, and keep the resolution path for the desktop callers
		// that genuinely have a registry.
		reg, regErr := registry.Open()
		if regErr != nil {
			return "", fmt.Errorf("recruit: ticket address %q is not a multiaddr, and this device has no local registry to resolve a bare peer id against: %w", deviceAddr, regErr)
		}
		resolved, resolveErr := reg.ResolveAddress(addr)
		if resolveErr != nil {
			return "", fmt.Errorf("recruit: resolve ticket address %q: %w", deviceAddr, resolveErr)
		}
		addr = resolved
	}

	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return "", fmt.Errorf("invalid device address %q: %w", addr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("device address %q missing peer id: %w", addr, err)
	}
	// The device being recruited is, by construction, one that had to hand
	// out an address for someone else to reach it -- on a phone or any
	// other NATed device that address is a /p2p-circuit one, reachable only
	// over a limited connection. See join's identical comment for why both
	// calls have to allow it.
	relayCtx := network.WithAllowLimitedConn(ctx, "recruit")
	if err := connectWithRetry(relayCtx, n.host, *info); err != nil {
		return "", fmt.Errorf("connect to %s: %w", info.ID, err)
	}

	s, err := n.host.NewStream(relayCtx, info.ID, RecruitProtocolID)
	if err != nil {
		return "", fmt.Errorf("open recruit stream to %s: %w", info.ID, err)
	}
	defer s.Close()
	// The read below blocks until the device finishes joining, which can
	// legitimately take most of recruitJoinTimeout -- but a stream read
	// doesn't observe ctx, so without a deadline of its own a device that
	// dies mid-join leaves this waiting forever. Matches the budget the
	// responder gives itself (handleRecruitStream), plus the same margin.
	_ = s.SetDeadline(time.Now().Add(recruitJoinTimeout + n.streamRequestTimeout()))

	reqLine := fmt.Sprintf("%s %s %s", n.advertisedAddrs()[0], hex.EncodeToString(inviteToken), hex.EncodeToString(correlationToken))
	if _, err := fmt.Fprintf(s, "%s\n", reqLine); err != nil {
		return "", fmt.Errorf("recruit: write to %s: %w", info.ID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("recruit: close write to %s: %w", info.ID, err)
	}

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("recruit: read response from %s: %w", info.ID, err)
		}
		return "", fmt.Errorf("no response from %s", info.ID)
	}
	line := scanner.Text()
	if rest, ok := strings.CutPrefix(line, "OK "); ok {
		return rest, nil
	}
	return "", fmt.Errorf("recruit rejected: %s", strings.TrimPrefix(line, "ERR: "))
}

// handleRecruitStream is RecruitProtocolID's receiving side, run on the
// device being recruited (see dialAndPushRecruit for the sending side): it
// parses "<leader multiaddr> <inviteTokenHex> <correlationTokenHex>", checks
// correlationToken against this node's own pending ticket
// (consumeJoinRequestToken -- single-use, cleared either way, so a wrong or
// replayed token can never match again afterward), and on a match calls
// handleAdd directly, in-process, exactly as if this node's own operator
// had just run `mage addfollower "<leaderAddr>#<inviteTokenHex>"`
// themselves. This only works cleanly because this feature is fresh-node
// only (see EventJoinRequestCreate's doc comment): handleAdd's
// non-bootstrap path assumes no raft instance exists yet, which is only
// true for a device that hasn't bootstrapped or joined anywhere before
// minting its ticket.
func (n *Node) handleRecruitStream(s network.Stream) {
	defer s.Close()

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		fmt.Fprintln(s, "ERR: no request line")
		return
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) != 3 {
		fmt.Fprintln(s, "ERR: malformed request")
		return
	}
	leaderAddr, inviteTokenHex, correlationHex := fields[0], fields[1], fields[2]

	correlationToken, err := hex.DecodeString(correlationHex)
	if err != nil {
		fmt.Fprintln(s, "ERR: invalid correlation token")
		return
	}
	if !n.consumeJoinRequestToken(correlationToken) {
		fmt.Fprintln(s, "ERR: unknown or already-used join request ticket")
		return
	}

	// The join below is the one handler in this file that legitimately
	// outlasts streamRequestTimeout: join() alone waits up to 45s for this
	// device's relay reservation before it will send an address raft
	// stores permanently (see its doc comment). withStreamRequestDeadline's
	// 30s budget -- right for a handler that only parses a request and
	// answers -- silently capped that, so a device that hadn't finished
	// reserving yet lost the stream mid-join and the recruiter saw a bare
	// "no response" with nothing to explain it. Extending it here to match
	// recruitJoinTimeout, which was already chosen for exactly this wait,
	// is what makes that timeout mean what its doc comment says.
	_ = s.SetDeadline(time.Now().Add(recruitJoinTimeout + n.streamRequestTimeout()))

	ctx, cancel := context.WithTimeout(context.Background(), recruitJoinTimeout)
	defer cancel()
	result, err := n.handleAdd(ctx, leaderAddr+"#"+inviteTokenHex)
	if err != nil {
		fmt.Fprintf(s, "ERR: %s\n", err)
		return
	}

	n.recordRecruitedMembership(leaderAddr)

	fmt.Fprintf(s, "OK %s\n", result)
}

// recordRecruitedMembership updates this node's own pkg/registry entry
// (ClusterPeerID/LeaderPeerID/Role) after handleRecruitStream's handleAdd
// call succeeds -- normally bootUp does this right after an ordinary join,
// but there is no kvctl process involved in this leg at all, so this node's
// own daemon has to do it itself so a later `mage leave`/`rm`/`listclusters`
// on this machine sees accurate membership. Best-effort: silently does
// nothing if this peer id has no existing registry entry at all (e.g. an
// Android identity, which never maintains a desktop-style multi-node
// registry in the first place).
func (n *Node) recordRecruitedMembership(leaderAddr string) {
	reg, err := registry.Open()
	if err != nil {
		return
	}
	info, ok, err := reg.Get(n.peerID)
	if err != nil || !ok {
		return
	}
	remotePID, err := registry.ExtractPeerID(leaderAddr)
	if err != nil {
		return
	}
	info.ClusterPeerID = remotePID
	info.LeaderPeerID = leaderAddr
	info.Role = registry.RoleFollower
	_ = reg.Put(info)
}

// errExecInviteNotLeader is processExecInviteRedeem's sentinel for "this
// node isn't the raft leader" -- checked via errors.Is by both
// handleExecInviteRedeemStream (which forwards once) and
// handleForwardExecInviteRedeemStream (which doesn't forward again).
var errExecInviteNotLeader = errors.New("exec invite redeem: not leader")

// processExecInviteRedeem decodes and verifies buf (a signed
// EventExecInviteRedeem Msg, as built by dialAndRedeemExecInvite) the same
// self-contained way handleExecuteStream verifies an EventExecute
// notification: against the *claimed* sender peer id's own extracted
// pubkey, not against s.Conn().RemotePeer() -- see
// ExecInviteRedeemProtocolID's doc comment for why that's what lets this
// same function run correctly whether called directly off the client
// stream or after being forwarded once. If this node is currently the raft
// leader, it runs the actual redemption (applyConsumeExecInvite);
// otherwise it returns errExecInviteNotLeader, leaving the forward
// decision to the caller.
func (n *Node) processExecInviteRedeem(ctx context.Context, buf []byte) (string, error) {
	m, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: decode: %w", err)
	}
	if m.Which() != shmevent.Event_Which_execInviteRedeem {
		return "", fmt.Errorf("exec invite redeem: expected execInviteRedeem, got %s", shmevent.EventName(m.Which()))
	}
	grp := m.ExecInviteRedeem()
	redeemerPeerIDStr, err := grp.RedeemerPeerId()
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: redeemer peer id: %w", err)
	}
	token, err := grp.Token()
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: token: %w", err)
	}
	redeemerPeer, err := peer.Decode(redeemerPeerIDStr)
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: invalid redeemer peer id %q: %w", redeemerPeerIDStr, err)
	}
	redeemerPub, err := redeemerPeer.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: extract redeemer public key: %w", err)
	}
	rawRedeemerPub, err := redeemerPub.Raw()
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: redeemer public key raw bytes: %w", err)
	}
	if err := shmevent.Verify(shmevent.PublicKey(rawRedeemerPub), m, crc, sig); err != nil {
		return "", fmt.Errorf("exec invite redeem: %w", err)
	}

	rf, isLeader, _, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return "", err
	}
	if !isLeader {
		return "", errExecInviteNotLeader
	}
	return n.applyConsumeExecInvite(ctx, rf, token, redeemerPeer.String())
}

// applyConsumeExecInvite runs kvfsm.OpConsumeExecInvite (the atomic
// ACL-check+consume) and, only once that succeeds, writes the durable
// CommandRequest record and its exec-index entries and sends the target a
// best-effort Execute poke -- the same "Apply, then do the dependent
// follow-up work outside that Apply" shape applyConfirm's own
// admitClusterJoinIfConfirmed call already uses for KindClusterJoin.
func (n *Node) applyConsumeExecInvite(ctx context.Context, rf *raft.Raft, token []byte, redeemerPeerID string) (string, error) {
	key := shmevent.ExecInviteKey(token)
	cmd := kvfsm.EncodeCommand(kvfsm.OpConsumeExecInvite, key, []byte(redeemerPeerID))
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return "", fmt.Errorf("exec invite: %w", err)
	}
	res, ok := future.Response().(kvfsm.ApplyResult)
	if !ok {
		return "", fmt.Errorf("exec invite: unexpected apply response type %T", future.Response())
	}
	if res.Err != nil {
		return "", fmt.Errorf("invalid, already-used, or unauthorized invite: %w", res.Err)
	}
	commandID, inputsJSON, _, err := shmevent.DecodeExecInviteRecord(res.Value)
	if err != nil {
		return "", fmt.Errorf("exec invite: %w", err)
	}

	instanceID, err := newExecInviteInstanceID()
	if err != nil {
		return "", err
	}

	cmdValue, err := n.store.Get(shmevent.CommandKey([]byte(commandID)))
	if err != nil {
		return "", fmt.Errorf("exec invite: command %s not found: %w", commandID, err)
	}
	_, targetPeerID, err := shmevent.DecodeCommandPayload(cmdValue)
	if err != nil {
		return "", fmt.Errorf("exec invite: decode command %s: %w", commandID, err)
	}
	targetPeerIDStr := string(targetPeerID)

	if err := n.writeExecInviteCommandRequest(rf, instanceID, commandID, inputsJSON, redeemerPeerID, targetPeerIDStr); err != nil {
		return "", err
	}

	if poke, err := json.Marshal(execInviteExecutePoke{Type: "cmd_req", CommandID: commandID, InstanceID: instanceID}); err == nil {
		if dest, err := peer.Decode(targetPeerIDStr); err == nil {
			_ = n.sendExecute(ctx, dest, poke)
		}
	}

	return instanceID, nil
}

// execInviteCommandRequestLogKind/execInviteCommandExecIndexKind mirror
// pkg/kvctl/dispatch.go's commandRequestLogKind/commandExecIndexKind naming
// convention exactly (same "cmdreq:"/"cmdexec:" prefixes, so
// ListCommandRequests/ListExecutionsByPeer see these dispatches too) --
// duplicated here rather than imported, since pkg/daemon sits below
// pkg/kvctl in this project's dependency graph and those helpers are
// unexported anyway. Same accepted precedent as executePoke's own
// duplication between pkg/kvctl and mobile/kvmobile.
func execInviteCommandRequestLogKind(commandID string) string {
	return "cmdreq:" + commandID
}

func execInviteCommandExecIndexKind(peerID string) string {
	return "cmdexec:" + peerID
}

const (
	execInviteIndexRoleRequester = "r"
	execInviteIndexRoleTarget    = "t"
)

// execInviteExecutePoke mirrors pkg/kvctl/dispatch.go's executePoke.
type execInviteExecutePoke struct {
	Type       string `json:"type"`
	CommandID  string `json:"command_id,omitempty"`
	InstanceID string `json:"instance_id"`
}

// newExecInviteInstanceID mirrors pkg/kvctl/dispatch.go's newInstanceID.
func newExecInviteInstanceID() (string, error) {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return "", fmt.Errorf("exec invite: generate instance id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// writeExecInviteCommandRequest writes the durable CommandRequest record
// (pkg/kvctl/dispatch.go's own type, encoded the identical way
// pkg/kvctl's appendRecord does) plus its requester/target exec-index
// entries, via n.applySet -- already-leader at this call site (see
// applyConsumeExecInvite), so no forwarding is needed for any of these
// writes.
func (n *Node) writeExecInviteCommandRequest(rf *raft.Raft, instanceID, commandID, inputsJSON, redeemerPeerID, targetPeerID string) error {
	fields := map[string]string{"command_id": commandID}
	if inputsJSON != "" {
		fields["inputs"] = inputsJSON
	}
	if err := n.appendExecInviteLogRecord(rf, execInviteCommandRequestLogKind(commandID), instanceID, redeemerPeerID, fields, ""); err != nil {
		return fmt.Errorf("exec invite: write command request: %w", err)
	}
	if err := n.appendExecInviteLogRecord(rf, execInviteCommandExecIndexKind(redeemerPeerID), instanceID, redeemerPeerID, map[string]string{
		"command_id": commandID,
		"role":       execInviteIndexRoleRequester,
	}, ""); err != nil {
		return fmt.Errorf("exec invite: write requester index: %w", err)
	}
	if targetPeerID != redeemerPeerID {
		if err := n.appendExecInviteLogRecord(rf, execInviteCommandExecIndexKind(targetPeerID), instanceID, redeemerPeerID, map[string]string{
			"command_id": commandID,
			"role":       execInviteIndexRoleTarget,
		}, ""); err != nil {
			return fmt.Errorf("exec invite: write target index: %w", err)
		}
	}
	return nil
}

// appendExecInviteLogRecord builds one pkg/logrecord entry the identical
// way pkg/kvctl's appendRecord does (logrecord.NewRand + BuildKey +
// Record.Encode) and writes it via n.applySet.
func (n *Node) appendExecInviteLogRecord(rf *raft.Raft, kind, unitID, authorPeerID string, fields map[string]string, narrative string) error {
	rnd, err := logrecord.NewRand()
	if err != nil {
		return err
	}
	ts := time.Now()
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		return err
	}
	rec := logrecord.Record{
		Kind:         kind,
		UnitID:       unitID,
		Timestamp:    ts,
		AuthorPeerID: authorPeerID,
		Fields:       fields,
		Narrative:    narrative,
	}
	value, err := rec.Encode()
	if err != nil {
		return err
	}
	return n.applySet(rf, key, value)
}

// handleExecInviteRedeemStream is the receiving side of
// ExecInviteRedeemProtocolID -- see that protocol's doc comment. Forwards
// once (unmodified bytes, no re-signing needed -- see
// processExecInviteRedeem) to the current leader over
// ForwardExecInviteRedeemProtocolID if this node isn't it.
func (n *Node) handleExecInviteRedeemStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "ERR: exec invite redeem: read: %v\n", err)
		return
	}

	instanceID, err := n.processExecInviteRedeem(context.Background(), buf)
	if err == nil {
		fmt.Fprintf(s, "OK %s\n", instanceID)
		return
	}
	if !errors.Is(err, errExecInviteNotLeader) {
		fmt.Fprintf(s, "ERR: %v\n", err)
		return
	}

	line, err := n.forwardExecInviteRedeem(context.Background(), buf)
	if err != nil {
		fmt.Fprintf(s, "ERR: forward exec invite redeem: %v\n", err)
		return
	}
	fmt.Fprintf(s, "%s\n", line)
}

// forwardExecInviteRedeem relays buf (already signature-verified by
// processExecInviteRedeem) to the current leader over
// ForwardExecInviteRedeemProtocolID, unmodified -- see that protocol's doc
// comment on why no re-signing is needed.
func (n *Node) forwardExecInviteRedeem(ctx context.Context, buf []byte) (string, error) {
	rf := n.getRaft()
	if rf == nil {
		return "", fmt.Errorf("not leader and no leader known")
	}
	_, leaderID := rf.LeaderWithID()
	if leaderID == "" {
		return "", fmt.Errorf("not leader and no leader known")
	}
	s, err := n.dialForward(ctx, leaderID, ForwardExecInviteRedeemProtocolID)
	if err != nil {
		return "", fmt.Errorf("open forward stream to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	if _, err := s.Write(buf); err != nil {
		return "", fmt.Errorf("write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("close write to leader %s: %w", leaderID, err)
	}

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("read response from leader %s: %w", leaderID, err)
		}
		return "", fmt.Errorf("no response from leader %s", leaderID)
	}
	return scanner.Text(), nil
}

// handleForwardExecInviteRedeemStream is the leader-side handler for
// ForwardExecInviteRedeemProtocolID: single-hop only, mirroring every other
// Forward* handler in this file -- if this node also isn't leader (a
// leadership change mid-flight), it fails outward instead of forwarding
// again.
func (n *Node) handleForwardExecInviteRedeemStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "ERR: forward exec invite redeem: read: %v\n", err)
		return
	}

	instanceID, err := n.processExecInviteRedeem(context.Background(), buf)
	if err == nil {
		fmt.Fprintf(s, "OK %s\n", instanceID)
		return
	}
	if errors.Is(err, errExecInviteNotLeader) {
		var leaderID raft.ServerID
		if rf := n.getRaft(); rf != nil {
			_, leaderID = rf.LeaderWithID()
		}
		fmt.Fprintf(s, "ERR: not leader; current leader is %s (already forwarded once)\n", leaderID)
		return
	}
	fmt.Fprintf(s, "ERR: %v\n", err)
}

// lodgeJoinRequest records joinPeerID's join request as a pending
// shmevent.KindClusterJoin system record instead of admitting it
// immediately -- see Config.RequireConfirmForJoin. It goes through
// handleSetForward exactly like EventPermitRequest's own case in
// handleShmEvent does (kind-agnostic replication, no special raft-
// membership awareness needed here at all -- that only happens at
// confirm time).
func (n *Node) lodgeJoinRequest(ctx context.Context, joinPeerID, joinAddr string, suffrage raft.ServerSuffrage) string {
	sf := shmevent.SuffrageVoter
	if suffrage == raft.Nonvoter {
		sf = shmevent.SuffrageLearner
	}
	key := shmevent.SystemKey(shmevent.KindClusterJoin, shmevent.StatusPending, []byte(joinPeerID))
	metadata := shmevent.EncodeClusterJoinMetadata(joinAddr, sf)
	if err := n.handleSetForward(ctx, key, []byte(metadata), true); err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}
	return "PENDING"
}

// addServerLine runs raft.AddVoter or raft.AddNonvoter (per suffrage) for
// joinPeerID/joinAddr and returns the response line to send back over the
// wire: "OK" or "ERR: <reason>". Every call site already only reaches this
// once rf.State()==Leader is confirmed, so on success it also records
// joinPeerID's KindClusterMember system record (pubkey + role) --
// see pkg/shmevent.KindClusterMember's doc comment.
func (n *Node) addServerLine(ctx context.Context, rf *raft.Raft, joinPeerID, joinAddr string, suffrage raft.ServerSuffrage) string {
	var future raft.IndexFuture
	switch suffrage {
	case raft.Nonvoter:
		future = rf.AddNonvoter(raft.ServerID(joinPeerID), raft.ServerAddress(joinAddr), 0, 10*time.Second)
	default:
		future = rf.AddVoter(raft.ServerID(joinPeerID), raft.ServerAddress(joinAddr), 0, 10*time.Second)
	}
	if err := future.Error(); err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}

	role := shmevent.RoleVoter
	if suffrage == raft.Nonvoter {
		role = shmevent.RoleLearner
	}
	if err := n.recordClusterMember(ctx, joinPeerID, role); err != nil {
		// The join itself already succeeded and is committed to the raft
		// configuration -- this registry is a queryable convenience
		// mirror, not something anything else depends on for
		// correctness, so a failure recording it shouldn't fail the join
		// response back to the caller.
		fmt.Fprintf(os.Stderr, "daemon: record cluster member %s: %v\n", joinPeerID, err)
	}
	if err := n.syncMemberGroups(ctx, joinPeerID, role); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: sync reserved groups for %s: %v\n", joinPeerID, err)
	}
	// watchLeadership only re-evaluates transferLeadershipToStableVoter on a
	// leadership *change* -- irrelevant here, since admitting a server
	// doesn't produce one. This is the other half: a relay-only leader with
	// no stable voter yet has nothing to hand off to when it self-elects,
	// but a newly admitted voter is exactly the moment that stops being
	// true. Only Voter matters -- a learner can never receive leadership,
	// and preferredLeaderTransferTarget would just skip it anyway.
	if suffrage != raft.Nonvoter {
		n.transferLeadershipToStableVoter(rf)
	}
	return "OK"
}

// recordClusterMember extracts peerIDStr's own public key -- embedded in
// the peer id itself for this project's Ed25519 identities, see
// pkg/shmevent.KindClusterMember's doc comment -- and writes its
// KindClusterMember record with the given role.
func (n *Node) recordClusterMember(ctx context.Context, peerIDStr string, role byte) error {
	pid, err := peer.Decode(peerIDStr)
	if err != nil {
		return fmt.Errorf("decode peer id: %w", err)
	}
	pub, err := pid.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("extract public key: %w", err)
	}
	raw, err := pub.Raw()
	if err != nil {
		return fmt.Errorf("public key raw bytes: %w", err)
	}
	key := shmevent.ClusterMemberKey([]byte(peerIDStr))
	value := shmevent.EncodeClusterMemberPayload(raw, role)
	return n.handleSetForward(ctx, key, value, true)
}

// syncMemberGroups reconciles peerIDStr's PeerGroup membership in the
// three raft-derived reserved groups (see shmevent.ReservedGroupCluster's
// own doc comment) to match role: always a member of
// ReservedGroupCluster, and of exactly one of ReservedGroupVoter/
// ReservedGroupLearner -- RoleLeader counts as a voter for this purpose,
// since a raft leader is itself always one of the voters. Also ensures
// peerIDStr's own personal Group record exists (ensurePersonalGroup) --
// unrelated to cluster/voter/learner PeerGroup membership, but the same
// call sites (addServerLine, watchLeadership) are exactly when a peer
// first has a writable raft store to create it in, whether by joining an
// existing cluster or solo-bootstrapping its own. Called alongside every
// recordClusterMember write so the two stay in lockstep; deliberately
// stateless/idempotent like recordClusterMember itself -- a redundant
// write is harmless, a missed one self-corrects on the next leadership
// transition.
func (n *Node) syncMemberGroups(ctx context.Context, peerIDStr string, role byte) error {
	if err := n.ensurePersonalGroup(ctx, peerIDStr); err != nil {
		return err
	}
	peerID := []byte(peerIDStr)
	if err := n.setPeerGroup(ctx, peerID, []byte(shmevent.ReservedGroupCluster)); err != nil {
		return err
	}
	voter := []byte(shmevent.ReservedGroupVoter)
	learner := []byte(shmevent.ReservedGroupLearner)
	if role == shmevent.RoleLearner {
		if err := n.setPeerGroup(ctx, peerID, learner); err != nil {
			return err
		}
		return n.deletePeerGroup(ctx, peerID, voter)
	}
	if err := n.setPeerGroup(ctx, peerID, voter); err != nil {
		return err
	}
	return n.deletePeerGroup(ctx, peerID, learner)
}

// clearMemberGroups removes peerIDStr from every raft-derived reserved
// group (ReservedGroupCluster/Voter/Learner) once it's no longer a raft
// member -- called from removeServerLine, alongside its KindClusterMember
// deletion. ReservedGroupChannel/ReservedGroupRelay are deliberately left
// untouched: those grants are independent of cluster membership -- see
// their own doc comment.
func (n *Node) clearMemberGroups(ctx context.Context, peerIDStr string) error {
	peerID := []byte(peerIDStr)
	for _, groupID := range [][]byte{
		[]byte(shmevent.ReservedGroupCluster),
		[]byte(shmevent.ReservedGroupVoter),
		[]byte(shmevent.ReservedGroupLearner),
	} {
		if err := n.deletePeerGroup(ctx, peerID, groupID); err != nil {
			return err
		}
	}
	return nil
}

// setPeerGroup/deletePeerGroup are syncMemberGroups/clearMemberGroups'
// shared PeerGroupKey Put/Del primitives -- routed through
// handleSetForward/handleOpForward exactly like recordClusterMember,
// rather than handleConfirmForward, since a watchLeadership callback can
// fire on a node that's a voter/learner but not the leader, and this
// write must still succeed by forwarding to whoever currently is.
func (n *Node) setPeerGroup(ctx context.Context, peerID, groupID []byte) error {
	key, err := shmevent.PeerGroupKey(peerID, groupID)
	if err != nil {
		return err
	}
	return n.handleSetForward(ctx, key, nil, true)
}

// deletePeerGroup removes peerID's membership of groupID. It forwards
// through handleConfirmForward (ForwardConfirmProtocolID), like every
// other OpDel in this package, and not handleOpForward: that path's
// handler accepts only OpSet and OpAppendCommandRequest -- deliberately,
// since it has no sender-is-a-voter gate (see handleForwardSetStream) --
// so a delete sent through it was rejected outright by the leader, on
// every attempt, with "forward set: expected OpSet or
// OpAppendCommandRequest, got op 2".
//
// That made the *last* step of syncMemberGroups impossible from anywhere
// but the leader: a node that changed role could add its new reserved
// group but never drop the stale one, so a demoted voter kept voter
// standing indefinitely. watchLeadership re-runs that sync on every
// leadership observation, so a real deployment logged the same failure
// continuously (found exactly that way, on this project's own shared e2e
// leader) while the membership it was trying to converge never moved.
func (n *Node) deletePeerGroup(ctx context.Context, peerID, groupID []byte) error {
	key, err := shmevent.PeerGroupKey(peerID, groupID)
	if err != nil {
		return err
	}
	return n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true)
}

// ensurePersonalGroup creates peerIDStr's own personal Group record (see
// isPeerIdentityGroupID's doc comment) -- id and name both peerIDStr,
// public false. Called from syncMemberGroups, so unlike
// ensureReservedGroups' one-time call this runs on every leadership
// transition/join; harmless, since it's a deterministic overwrite of a
// record no one else can rename (EventGroupPut rejects any
// peer-identity-shaped id). Calls handleConfirmForward directly rather
// than going through EventGroupPut, bypassing that event's own
// reserved-id rejection -- the same way ensureReservedGroups does for the
// seven fixed reserved groups.
func (n *Node) ensurePersonalGroup(ctx context.Context, peerIDStr string) error {
	key := shmevent.GroupKey([]byte(peerIDStr))
	value := shmevent.EncodeGroupPayload(peerIDStr, false)
	if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, value, true); err != nil {
		return fmt.Errorf("create personal group %q: %w", peerIDStr, err)
	}
	return nil
}

// ensureReservedGroups creates the seven reserved Group records
// (ReservedGroupCluster/Voter/Learner/Channel/Relay/Remote/Execute)
// exactly once, at the moment a brand new cluster is first bootstrapped
// (handleAdd's
// leaderPeerID=="" branch, right after this node's own self-election
// completes) -- a node that instead *joins* an existing cluster
// replicates these same records ordinarily, the same way it replicates
// every other Group a longer-lived cluster already has. Calls
// handleConfirmForward directly rather than going through EventGroupPut,
// bypassing that event's own reserved-id rejection -- the same way
// recordClusterMember bypasses the ordinary client-facing Set path for
// KindClusterMember.
func (n *Node) ensureReservedGroups(ctx context.Context) error {
	for _, id := range []string{
		shmevent.ReservedGroupCluster,
		shmevent.ReservedGroupVoter,
		shmevent.ReservedGroupLearner,
		shmevent.ReservedGroupChannel,
		shmevent.ReservedGroupRelay,
		shmevent.ReservedGroupRemote,
		shmevent.ReservedGroupExecute,
	} {
		key := shmevent.GroupKey([]byte(id))
		value := shmevent.EncodeGroupPayload(id, false)
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, value, true); err != nil {
			return fmt.Errorf("create reserved group %q: %w", id, err)
		}
	}
	return nil
}

// ensureDefaultPublicCommand creates shmevent.DefaultPublicGroupID (a
// Public Group), shmevent.DefaultPublicCommandID (a Command with no
// execution target -- see below), and the GroupCommand link between them
// -- exactly once, at the same moment and call site ensureReservedGroups
// runs (handleAdd's bootstrap branch, right after self-election
// completes). See DefaultPublicCommandID's own doc comment for what
// submitting it actually does (kvfsm.Apply's OpAppendCommandRequest
// special case). Unlike ensureReservedGroups, these three records are
// ordinary, mutable Group/Command/GroupCommand writes from here on --
// ordinary EventGroupPut/EventCommandPut/EventGroupCommandPut semantics
// apply (catalog.go's IsReservedGroupID deliberately excludes
// DefaultPublicGroupID) -- this only seeds sensible starting values, the
// same way a node that instead *joins* an existing cluster just
// replicates whatever it finds here already, unchanged.
//
// The Command's own peerID field (who executes it -- see
// EncodeCommandPayload) is deliberately left empty rather than pointing
// at this bootstrapping node's own peer id: kvfsm's checkCommandPeerIDUnique
// enforces a global "at most one Command per peerID" constraint, and
// occupying the leader's own slot with this synthetic, natively-handled
// command (its whole point is the FSM side effect, not a real dispatched
// executor -- SubmitCommand's best-effort Execute poke to an empty target
// is simply a harmless no-op) would leave that peer unable to ever be
// assigned a real, meaningfully-executed command of its own.
func (n *Node) ensureDefaultPublicCommand(ctx context.Context) error {
	groupKey := shmevent.GroupKey([]byte(shmevent.DefaultPublicGroupID))
	groupValue := shmevent.EncodeGroupPayload(shmevent.DefaultPublicGroupID, true)
	if err := n.handleConfirmForward(ctx, kvfsm.OpSet, groupKey, groupValue, true); err != nil {
		return fmt.Errorf("create default public group: %w", err)
	}

	commandKey := shmevent.CommandKey([]byte(shmevent.DefaultPublicCommandID))
	commandValue, err := shmevent.EncodeCommandPayload(shmevent.DefaultPublicCommandID, nil)
	if err != nil {
		return fmt.Errorf("encode default public command: %w", err)
	}
	if err := n.handleConfirmForward(ctx, kvfsm.OpSet, commandKey, commandValue, true); err != nil {
		return fmt.Errorf("create default public command: %w", err)
	}

	linkKey, err := shmevent.GroupCommandKey([]byte(shmevent.DefaultPublicCommandID), []byte(shmevent.DefaultPublicGroupID))
	if err != nil {
		return fmt.Errorf("build default public group-command link key: %w", err)
	}
	if err := n.handleConfirmForward(ctx, kvfsm.OpSet, linkKey, nil, true); err != nil {
		return fmt.Errorf("link default public command to its group: %w", err)
	}
	return nil
}

// forwardJoin relays a join request (joinPeerID, joinAddr, suffrage,
// inviteToken) to leaderID over ForwardJoinProtocolID and returns its
// response line verbatim (without the trailing newline). See dialForward
// for how leaderID is actually reached.
func (n *Node) forwardJoin(ctx context.Context, leaderID raft.ServerID, joinPeerID, joinAddr string, suffrage raft.ServerSuffrage, inviteToken []byte) (string, error) {
	s, err := n.dialForward(ctx, leaderID, ForwardJoinProtocolID)
	if err != nil {
		return "", fmt.Errorf("open forward-join stream to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	suffrageWord := "voter"
	if suffrage == raft.Nonvoter {
		suffrageWord = "learner"
	}
	line := fmt.Sprintf("%s %s %s", joinPeerID, joinAddr, suffrageWord)
	if len(inviteToken) > 0 {
		line += " " + hex.EncodeToString(inviteToken)
	}
	if _, err := fmt.Fprintf(s, "%s\n", line); err != nil {
		return "", fmt.Errorf("write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("close write to leader %s: %w", leaderID, err)
	}

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("read response from leader %s: %w", leaderID, err)
		}
		return "", fmt.Errorf("no response from leader %s", leaderID)
	}
	return scanner.Text(), nil
}

// handleClientStream is the leader-or-follower-side handler for
// ClientProtocolID: the remote counterpart of pkg/ipc's local shared
// memory, speaking the exact same pkg/shmevent capnp wire struct -- see
// that package's doc comment and ClientProtocolID's for why a browser
// learner's join (EventAdd) looks the way it does here specifically. A
// capnp message has no fixed size (unlike the ipcproto.Request this
// replaced), so this reads the whole request off the stream before
// decoding, the same way handleForwardSetStream already did for
// kvfsm's variable-length command framing.
//
// Every request here is treated as a remoteCaller (see callerIdentity):
// its signature is checked against its own libp2p-authenticated identity,
// not this node's key -- there is no shared-key bootstrap over this
// protocol (see handleShmEvent's doc comment).
func (n *Node) handleClientStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		return
	}
	m, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		return
	}

	caller, err := remoteCaller(s)
	if err != nil {
		respBuf, encErr := shmevent.Encode(errorMsg(m.Id(), err), n.ed25519Priv)
		if encErr == nil {
			s.Write(respBuf)
		}
		return
	}

	resp := n.handleShmEvent(context.Background(), m, crc, sig, caller)
	respBuf, err := shmevent.Encode(resp, n.ed25519Priv)
	if err != nil {
		return
	}
	s.Write(respBuf)
}

// handleAddLearner adds joinPeerID as a raft non-voter at joinAddr directly
// if this node is the leader, or forwards to whoever currently is (one hop
// only, over ForwardJoinProtocolID, reusing the exact same wire path a
// voter join already forwards through -- see handleJoinStream) since the
// caller has no cheaper way to learn the real leader than any other
// joining node does. Returns this node's own peer id on success, mirroring
// handleAdd's return value.
func (n *Node) handleAddLearner(ctx context.Context, joinPeerID, joinAddr string) (string, error) {
	if joinPeerID == "" || joinAddr == "" {
		return "", fmt.Errorf("client add: missing peer id or multiaddr")
	}

	rf := n.getRaft()
	if rf != nil && rf.State() == raft.Leader {
		if line := n.addServerLine(ctx, rf, joinPeerID, joinAddr, raft.Nonvoter); strings.HasPrefix(line, "ERR: ") {
			return "", fmt.Errorf("%s", strings.TrimPrefix(line, "ERR: "))
		}
		return n.peerID, nil
	}

	var leaderID raft.ServerID
	if rf != nil {
		_, leaderID = rf.LeaderWithID()
	}
	if leaderID == "" {
		return "", fmt.Errorf("client add: not leader and no leader known")
	}

	line, err := n.forwardJoin(ctx, leaderID, joinPeerID, joinAddr, raft.Nonvoter, nil)
	if err != nil {
		return "", fmt.Errorf("client add: forward: %w", err)
	}
	if reason, isErr := strings.CutPrefix(line, "ERR: "); isErr {
		return "", fmt.Errorf("%s", reason)
	}
	return n.peerID, nil
}

func (n *Node) handleGet(key []byte) ([]byte, error) {
	value, err := n.store.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// awaitLeader waits up to timeout for this node to become raft leader and
// returns the raft instance once it has. A freshly bootstrapped
// single-voter cluster elects itself almost immediately; this absorbs that
// startup race instead of failing the first Set issued right after
// `mage addnode`.
func (n *Node) awaitLeader(timeout time.Duration) (*raft.Raft, error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if rf := n.getRaft(); rf != nil && rf.State() == raft.Leader {
			return rf, nil
		}
		select {
		case <-deadline:
			rf := n.getRaft()
			if rf == nil {
				return nil, fmt.Errorf("node has not been added to a cluster yet")
			}
			if _, leaderID := rf.LeaderWithID(); leaderID != "" {
				return nil, fmt.Errorf("not leader; current leader is %s", leaderID)
			}
			return nil, fmt.Errorf("not leader and no leader known")
		case <-ticker.C:
		}
	}
}

// resolveWriteTarget waits (up to timeout) for this node to either become
// raft leader itself or learn who currently is, and reports which. In
// steady state this returns on its very first check, with no waiting at
// all -- the timeout only matters right after bootstrap/join, before
// raft's first election has completed and LeaderWithID is still empty.
func (n *Node) resolveWriteTarget(timeout time.Duration) (rf *raft.Raft, isLeader bool, leaderID raft.ServerID, err error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if rf := n.getRaft(); rf != nil {
			if rf.State() == raft.Leader {
				return rf, true, "", nil
			}
			if _, id := rf.LeaderWithID(); id != "" {
				return rf, false, id, nil
			}
		}
		select {
		case <-deadline:
			if n.getRaft() == nil {
				return nil, false, "", fmt.Errorf("node has not been added to a cluster yet")
			}
			return nil, false, "", fmt.Errorf("not leader and no leader known")
		case <-ticker.C:
		}
	}
}
