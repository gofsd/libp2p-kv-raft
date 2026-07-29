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
	"encoding/binary"
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
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	v2relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/rafttransport"
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
// raw, persistent, bidirectional byte pipe directly to another --
// EventChannelOpen/Send/Poll/Listen/Close's transport, see those events'
// doc comments in pkg/shmevent. Unlike every other protocol in this file
// (including ExecuteProtocolID), the stream is never a single
// write-then-EOF request/response: after a short signed handshake
// (mirroring ExecuteProtocolID's self-contained-signature design) the
// stream stays open indefinitely and carries raw, unframed bytes in both
// directions -- see writeFramed/readFramed (used only for the handshake)
// and handleChannelStream's own doc comment for the full wire design.
// Like ExecuteProtocolID, the traffic it carries never touches raft or
// the store.
const ChannelProtocolID = protocol.ID("/libp2p-kv-raft/channel/1.0.0")

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
	// per-individual-peer override.
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

	// RequirePermitForRemote gates every remote (ClientProtocolID) request
	// other than EventPermitRequest/EventPermitConfirm on the caller
	// having a confirmed KindPermitPeer record (see pkg/shmevent's
	// SystemKey/EventPermitRequest doc comments) for its own
	// libp2p-authenticated peer id. Defaults to false: today's behavior,
	// where any remote caller that signs with its own key (see
	// callerIdentity) is honored with no separate allow-listing. Turning
	// this on requires an operator to RequestPermit+ConfirmPermit every
	// remote peer first -- including test/e2e/web-app callers -- via
	// pkg/kvctl's RequestPermit/ConfirmPermit (mage requestpermit/
	// confirmpermit, kvctl-cli requestpermit/confirmpermit).
	RequirePermitForRemote bool

	// RequirePermitForRelay gates this node's circuit-relay v2 service
	// (only meaningful alongside RelayService) on the reserving/connecting
	// peer having a confirmed KindPermitPeer record -- see that kind's doc
	// comment ("permission for a peer to join/use the cluster's relay").
	// Defaults to false: today's behavior, where any peer can reserve a
	// relay slot or open a relayed circuit through this node with no
	// allow-listing at all. Independent of RequirePermitForRemote, which
	// gates the unrelated shmevent/ClientProtocolID RPC layer -- a node
	// can restrict one without the other. Turning this on requires an
	// operator to RequestPermit+ConfirmPermit every peer that needs relay
	// access first, same as RequirePermitForRemote.
	RequirePermitForRelay bool

	// RequirePermitForExecute gates EventExecute delivery
	// (handleExecuteStream) on the signature-verified sender being either
	// a recorded KindClusterMember (a current raft voter or learner --
	// see isClusterMember) or a confirmed KindPermitPeer (see
	// isPermittedPeer): a cluster member is trusted implicitly, the same
	// way it already is for every raft-replicated write, so it never
	// needs a separate permit just to use Execute; any other peer does.
	// Defaults to false: today's behavior, where handleExecuteStream
	// queues a notification from any peer whose self-contained signature
	// checks out, regardless of standing. Independent of
	// RequirePermitForRelay/RequirePermitForRemote. Turning this on
	// requires RequestPermit+ConfirmPermit (kind "peer") for every
	// non-member peer that needs to send Execute notifications.
	RequirePermitForExecute bool

	// RequirePermitForLog gates EventLogAppend and EventListRange, for a
	// remote caller only (never a local one -- see handleShmEvent's doc
	// comment on why a local caller is always trusted as this node's own
	// operator), on that peer holding a confirmed KindLogPermit record
	// for the specific pkg/logrecord kind being appended/queried (see
	// isPermittedForLogKind). Unlike RequirePermitForExecute, cluster
	// membership is NOT an automatic exemption here -- matches
	// RequirePermitForRemote's existing behavior, which doesn't exempt
	// cluster members either. Defaults to false: today's behavior, where
	// any caller that already clears RequirePermitForRemote (or any local
	// caller, unconditionally) may append/query log records of any kind.
	// Independent of RequirePermitForRelay/RequirePermitForRemote/
	// RequirePermitForExecute. Turning this on requires
	// RequestLogPermit+ConfirmLogPermit for every (peer, kind) pair a
	// remote caller needs.
	RequirePermitForLog bool

	// RequirePermitForChannel gates an *incoming* EventChannelOpen
	// (handleChannelStream) exactly the same way RequirePermitForExecute
	// gates handleExecuteStream -- same signature-verified-sender check,
	// same isClusterMember/isPermittedPeer exemption (a cluster member
	// never needs a separate permit), same confirmed KindPermitPeer
	// record reused as-is, no new permit kind. Defaults to false:
	// today's behavior, where handleChannelStream accepts a channel from
	// any peer whose self-contained handshake signature checks out,
	// regardless of standing. Independent of every other RequirePermitFor*
	// flag. Turning this on requires RequestPermit+ConfirmPermit (kind
	// "peer") for every non-member peer that needs to open a channel to
	// this node.
	RequirePermitForChannel bool

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
	// wants to join, the same way RequirePermitForRemote/RequirePermitForRelay
	// require RequestPermit+ConfirmPermit for their own respective kinds.
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

// maxExecuteInbox bounds executeInbox: a queue nothing ever drains (no
// local caller ever polls) would otherwise grow without limit as long as
// other nodes keep sending EventExecute notifications. Past this many
// pending entries, the oldest is dropped to make room for the newest --
// same trade-off a best-effort notification queue with no persistence
// already implies (see executeInbox's doc comment).
const maxExecuteInbox = 256

// executeNotification is one queued EventExecute delivery: senderPeerID is
// the string pkg/shmevent.DecodeExecuteNotification returned (the sending
// node's own peer id, already signature-verified against it by
// handleExecuteStream before queuing), payload is that same call's payload.
type executeNotification struct {
	senderPeerID []byte
	payload      []byte
}

// executeInbox is a bounded FIFO queue of executeNotification, guarded by
// a mutex -- deliberately the simplest thing that could work rather than
// a channel, since EventPollExecute needs a non-blocking "is anything
// there" drain (a closed/empty channel read blocks or needs a select,
// where a plain slice-under-a-mutex just returns ok=false).
type executeInbox struct {
	mu      sync.Mutex
	entries []executeNotification
}

func newExecuteInbox() *executeInbox {
	return &executeInbox{}
}

func (q *executeInbox) push(senderPeerID, payload []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) >= maxExecuteInbox {
		q.entries = q.entries[1:]
	}
	q.entries = append(q.entries, executeNotification{senderPeerID: senderPeerID, payload: payload})
}

func (q *executeInbox) pop() (executeNotification, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return executeNotification{}, false
	}
	n := q.entries[0]
	q.entries = q.entries[1:]
	return n, true
}

// channelIDLength is how many random bytes back a freshly minted
// channelID (see EventChannelOpen's doc comment) -- hex-encoded, so the
// wire string is twice this. Purely a local handle, never compared
// across the two peers of a channel, so collision resistance only needs
// to hold within one node's own lifetime.
const channelIDLength = 8

func newChannelID() (string, error) {
	buf := make([]byte, channelIDLength)
	if _, err := cryptorand.Read(buf); err != nil {
		return "", fmt.Errorf("channel: generate id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// maxPendingChannels/maxChannelInbox bound channelTable's pending queue
// and each channelSession's own buffered-chunk inbox, same reasoning as
// maxExecuteInbox: a queue nothing ever drains would otherwise grow
// without limit as long as a peer keeps dialing in or sending. Past
// these many entries, the oldest is dropped/evicted to make room for the
// newest. Vars, not consts, so tests can lower them rather than sending
// thousands of real chunks (mirrors pkg/kvfsm.maxSystemListEntries' own
// reason for being a var).
var (
	maxPendingChannels = 64
	maxChannelInbox    = 4096
)

// channelIdleTimeout/channelPendingTimeout bound how long, respectively,
// an established-but-unpolled channel session and an accepted-but-
// unclaimed incoming channel are kept alive with nobody attending to
// them. Reaped opportunistically -- see channelTable.reap -- rather than
// by a dedicated background goroutine, since nothing else in this
// package ties a goroutine to a Node's lifetime beyond raft's own
// internals, and tests construct many short-lived Nodes with no
// goroutine-cancellation plumbing in shutdown to hook into. Vars, not
// consts, so tests can shrink them instead of actually waiting minutes.
var (
	channelIdleTimeout    = 5 * time.Minute
	channelPendingTimeout = 2 * time.Minute
)

// channelReadBufferSize is pumpChannelReads' read buffer size -- large
// enough for reasonable throughput, small enough that one channel can't
// hog an outsized chunk of memory per Read call.
const channelReadBufferSize = 4096

// channelSession is one live EventChannelOpen/handleChannelStream
// session (see ChannelProtocolID's doc comment for the wire design):
// stream carries raw, unframed bytes in both directions once the initial
// handshake completes. inbox buffers bytes pumpChannelReads has already
// read off stream, for EventChannelPoll to drain -- entries are raw
// chunks straight off the wire, chunk boundaries carry no meaning.
// writeMu serializes EventChannelSend's writes to stream (in practice
// only ever this node's own local caller, one at a time, but this
// matches executeInbox's own defensive-mutex style rather than relying
// on that).
type channelSession struct {
	stream       network.Stream
	remotePeerID string
	writeMu      sync.Mutex

	mu           sync.Mutex
	inbox        [][]byte
	closed       bool
	closeReason  string
	lastActivity time.Time
}

func newChannelSession(stream network.Stream, remotePeerID string) *channelSession {
	return &channelSession{stream: stream, remotePeerID: remotePeerID, lastActivity: time.Now()}
}

func (s *channelSession) touch() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

func (s *channelSession) idleFor(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return now.Sub(s.lastActivity)
}

// pushChunk records one chunk pumpChannelReads just read off the wire.
func (s *channelSession) pushChunk(chunk []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()
	if len(s.inbox) >= maxChannelInbox {
		s.inbox = s.inbox[1:]
	}
	s.inbox = append(s.inbox, chunk)
}

// popChunk returns the oldest buffered chunk, if any -- EventChannelPoll's
// read side.
func (s *channelSession) popChunk() ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.inbox) == 0 {
		return nil, false
	}
	c := s.inbox[0]
	s.inbox = s.inbox[1:]
	return c, true
}

// markClosed records that stream has ended (pumpChannelReads hit EOF or
// an error) -- reason is empty for a clean EOF. Idempotent: only the
// first call's reason sticks, matching "the channel ended once, however
// that happened."
func (s *channelSession) markClosed(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.closeReason = reason
	}
}

// status reports whether the channel has ended and why.
func (s *channelSession) status() (closed bool, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed, s.closeReason
}

// write sends chunk directly over stream, with no framing -- see
// ChannelProtocolID's doc comment.
func (s *channelSession) write(chunk []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.stream.Write(chunk); err != nil {
		return err
	}
	s.touch()
	return nil
}

// closeWrite half-closes stream's outgoing direction only -- see
// EventChannelCloseWrite's doc comment.
func (s *channelSession) closeWrite() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.stream.CloseWrite()
}

// pendingChannel is one entry in channelTable's pending queue -- a
// channel handleChannelStream has already accepted and registered, but
// that no local caller has claimed via EventChannelListen yet.
type pendingChannel struct {
	channelID string
	addedAt   time.Time
}

// channelTable holds every live channelSession for this Node, keyed by
// its local channelID, plus the FIFO of pending (accepted-but-unclaimed)
// incoming ones -- the persistent-session counterpart to executeInbox,
// guarded by one mutex for the same "simplest thing that could work"
// reason executeInbox's own doc comment gives.
type channelTable struct {
	mu       sync.Mutex
	sessions map[string]*channelSession
	pending  []pendingChannel
}

func newChannelTable() *channelTable {
	return &channelTable{sessions: make(map[string]*channelSession)}
}

func (t *channelTable) register(channelID string, s *channelSession) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sessions[channelID] = s
}

func (t *channelTable) get(channelID string) (*channelSession, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.sessions[channelID]
	return s, ok
}

// remove deletes channelID's session (if any) from both the live table
// and the pending queue -- CloseChannel's implementation, and the
// reaper's. Does not itself close the underlying stream; callers that
// need that do it before calling remove.
func (t *channelTable) remove(channelID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.sessions, channelID)
	for i, p := range t.pending {
		if p.channelID == channelID {
			t.pending = append(t.pending[:i], t.pending[i+1:]...)
			break
		}
	}
}

// pushPending enqueues channelID for a future EventChannelListen to
// claim, evicting (closing) the oldest still-pending entry first if
// already at maxPendingChannels.
func (t *channelTable) pushPending(channelID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.pending) >= maxPendingChannels {
		stale := t.pending[0]
		t.pending = t.pending[1:]
		if s, ok := t.sessions[stale.channelID]; ok {
			s.stream.Close()
			delete(t.sessions, stale.channelID)
		}
	}
	t.pending = append(t.pending, pendingChannel{channelID: channelID, addedAt: time.Now()})
}

// popPending claims the oldest pending entry, if any -- EventChannelListen's
// implementation.
func (t *channelTable) popPending() (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.pending) == 0 {
		return "", false
	}
	id := t.pending[0].channelID
	t.pending = t.pending[1:]
	return id, true
}

// reap closes and evicts sessions idle past channelIdleTimeout, and
// unclaimed pending entries older than channelPendingTimeout -- see
// channelIdleTimeout's own doc comment for why this runs opportunistically
// (called at the top of dispatchChannelPoll/dispatchChannelListen/
// handleChannelStream) rather than from a dedicated background goroutine.
func (t *channelTable) reap() {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	stillPending := make([]pendingChannel, 0, len(t.pending))
	for _, p := range t.pending {
		if now.Sub(p.addedAt) > channelPendingTimeout {
			if s, ok := t.sessions[p.channelID]; ok {
				s.stream.Close()
				delete(t.sessions, p.channelID)
			}
			continue
		}
		stillPending = append(stillPending, p)
	}
	t.pending = stillPending

	for id, s := range t.sessions {
		if s.idleFor(now) > channelIdleTimeout {
			s.stream.Close()
			delete(t.sessions, id)
		}
	}
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

	if err := n.writeReadyFile(); err != nil {
		return fmt.Errorf("daemon: write ready file: %w", err)
	}

	return ipc.Serve(ctx, n.peerID, n.ed25519Priv, func(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg {
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

	h, err := newHost(priv, cfg, st)
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
		logStore:     logStore,
		snapStore:    snapStore,
	}
	h.SetStreamHandler(JoinProtocolID, withStreamRequestDeadline(n.handleJoinStream))
	h.SetStreamHandler(ForwardProtocolID, withStreamRequestDeadline(n.handleForwardSetStream))
	h.SetStreamHandler(ForwardConfirmProtocolID, withStreamRequestDeadline(n.handleForwardConfirmStream))
	h.SetStreamHandler(ExecuteProtocolID, withStreamRequestDeadline(n.handleExecuteStream))
	h.SetStreamHandler(ChannelProtocolID, withStreamRequestDeadline(n.handleChannelStream))
	h.SetStreamHandler(ForwardJoinProtocolID, withStreamRequestDeadline(n.handleForwardJoinStream))
	h.SetStreamHandler(ForwardLeaveProtocolID, withStreamRequestDeadline(n.handleForwardLeaveStream))
	h.SetStreamHandler(ForwardKickProtocolID, withStreamRequestDeadline(n.handleForwardKickStream))
	h.SetStreamHandler(ClientProtocolID, withStreamRequestDeadline(n.handleClientStream))
	h.SetStreamHandler(ExecInviteRedeemProtocolID, withStreamRequestDeadline(n.handleExecInviteRedeemStream))
	h.SetStreamHandler(ForwardExecInviteRedeemProtocolID, withStreamRequestDeadline(n.handleForwardExecInviteRedeemStream))
	h.SetStreamHandler(RecruitProtocolID, withStreamRequestDeadline(n.handleRecruitStream))

	// forgetTransientPeer's disconnect hook -- see that method's doc
	// comment for why this (plus newHost's ConnectionManager) exists.
	h.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(_ network.Network, c network.Conn) {
			n.forgetTransientPeer(c.RemotePeer())
		},
	})
	return n, nil
}

// streamRequestTimeout bounds how long any stream protocol handler
// registered below waits for its peer to finish sending its request
// before giving up. Every handler reads its request via some blocking
// call (bufio.Scanner.Scan, readFramed, io.ReadAll) with no timeout of
// its own -- a peer that opens the stream and then stalls or dies before
// finishing its request previously left that read blocked, and its
// goroutine leaked, forever. Confirmed as the most likely single cause of
// a real production leak: a node with 14 days of uptime found running at
// 4.95GB RSS against only ~265MB of real on-disk data (raft log/
// snapshots/sqlite) -- collapsing to ~30MB immediately after a clean
// restart with the identical data, and hit by a shared, heavily-reused
// e2e test pipeline whose peers routinely get killed or time out
// mid-request. Generous relative to a same-machine/LAN round trip, short
// relative to "forever": long enough for a legitimate slow/relayed peer,
// short enough that a truly abandoned stream's goroutine exits within
// seconds, not indefinitely.
//
// handleChannelStream/dispatchChannelOpen are the one exception: each
// clears this deadline itself (SetDeadline(time.Time{})) right after its
// initial handshake succeeds, before handing the stream off to
// pumpChannelReads' intentionally long-lived read loop -- a channel is
// meant to sit idle between chunks for arbitrary periods (bounded instead
// by channelIdleTimeout via channelTable.reap), not by this one-shot
// per-request budget.
const streamRequestTimeout = 30 * time.Second

// withStreamRequestDeadline wraps handler so its stream has
// streamRequestTimeout to complete before whatever blocking read it does
// can hang forever on a peer that opened the stream and never finished
// sending anything -- see streamRequestTimeout's doc comment.
func withStreamRequestDeadline(handler network.StreamHandler) network.StreamHandler {
	return func(s network.Stream) {
		_ = s.SetDeadline(time.Now().Add(streamRequestTimeout))
		handler(s)
	}
}

// newHost builds this node's libp2p host. Every node gets relay-client and
// hole-punching capability unconditionally, so it can be dialed through
// (or dial through) a circuit relay when a direct connection isn't
// possible -- the "worst case" NAT fallback. A node only advertises itself
// as a relay *for others* (RelayService) and forces public reachability
// when the caller knows it actually has one, e.g. the leader on a public
// VPS; the resource limits mirror the standalone relay in
// pkg/raft/node.go's StartRelayNode. st is only consulted when
// cfg.RequirePermitForRelay is set (see relayACL); it's threaded in here,
// ahead of any *Node existing, because the ACL closure needs to read
// confirmed KindPermitPeer records live -- one already-open *store.Store,
// not a snapshot taken at host-construction time.
// relayLimits resolves cfg's relay resource fields, substituting
// shmevent.DefaultRelay* for whichever were left at their zero value --
// mirrors the same zero-means-default pattern this Config already uses for
// its raft timing/snapshot fields. Shared by newHost (what go-libp2p
// actually enforces) and handleShmEvent's EventPermitRequest case (what
// gets stamped onto a new KindPermitPeer record), so both always agree on
// what "this node's default relay allotment" currently is.
func relayLimits(cfg Config) shmevent.RelayLimits {
	limits := shmevent.DefaultRelayLimits()
	if cfg.RelayMaxCircuitsPerPeer != 0 {
		limits.MaxCircuitsPerPeer = int32(cfg.RelayMaxCircuitsPerPeer)
	}
	if cfg.RelayLimitData != 0 {
		limits.LimitData = cfg.RelayLimitData
	}
	if cfg.RelayLimitDuration != 0 {
		limits.LimitDuration = cfg.RelayLimitDuration
	}
	if cfg.RelayMaxReservationsPerIP != 0 {
		limits.MaxReservationsPerIP = int32(cfg.RelayMaxReservationsPerIP)
	}
	if cfg.RelayMaxReservationsPerPeer != 0 {
		limits.MaxReservationsPerPeer = int32(cfg.RelayMaxReservationsPerPeer)
	}
	return limits
}

// connManagerLowWater/connManagerHighWater bound this host's simultaneous
// open connections: once above high, go-libp2p's connection manager trims
// the least-useful connections back down toward low (respecting
// connManagerGracePeriod for anything newer than that). Previously
// unbounded -- no libp2p.ConnectionManager option at all -- alongside
// forgetTransientPeer, this is the other confirmed root cause of a real
// production node found running at 4.95GB RSS against ~265MB of real
// on-disk data after 14 days of uptime, hit by a shared, heavily-reused
// e2e test pipeline whose peers connect and disconnect constantly.
// Generous for this project's actual cluster sizes (single digits to low
// tens of members plus occasional relayed clients), not a hard cap on
// legitimate cluster size.
const (
	connManagerLowWater    = 100
	connManagerHighWater   = 400
	connManagerGracePeriod = 30 * time.Second
)

func newHost(priv crypto.PrivKey, cfg Config, st *store.Store) (lp2phost.Host, error) {
	cm, err := connmgr.NewConnManager(connManagerLowWater, connManagerHighWater, connmgr.WithGracePeriod(connManagerGracePeriod))
	if err != nil {
		return nil, fmt.Errorf("daemon: create connection manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ConnectionManager(cm),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", cfg.ListenPort),
			// Shares the quic-v1 UDP port above (WebTransport is a session
			// layered on the same QUIC socket, not a separate listener).
			// The webtransport transport module itself is already part of
			// go-libp2p's default transport set (this Config never calls
			// libp2p.Transport, so DefaultTransports applies); only the
			// listen address was missing, which is why every other node
			// this project has run so far -- none of them reachable from a
			// browser -- never noticed. n.host.Addrs() will report the
			// resulting address with its /certhash component appended
			// automatically, so advertisedAddrs()/ready.json need no
			// change to start including it. See web-app/ for the browser
			// client (js-libp2p, since go-libp2p itself has no usable
			// browser-sandbox transport) that dials this.
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1/webtransport", cfg.ListenPort),
		),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
	}

	if cfg.RelayService {
		limits := relayLimits(cfg)
		rc := v2relay.DefaultResources()
		rc.Limit = &v2relay.RelayLimit{
			Duration: limits.LimitDuration,
			Data:     limits.LimitData,
		}
		rc.ReservationTTL = time.Hour
		rc.MaxReservations = 256
		rc.MaxCircuits = int(limits.MaxCircuitsPerPeer)
		rc.BufferSize = 4096
		rc.MaxReservationsPerIP = int(limits.MaxReservationsPerIP)
		rc.MaxReservationsPerPeer = int(limits.MaxReservationsPerPeer)

		relayOpts := []v2relay.Option{v2relay.WithResources(rc)}
		if cfg.RequirePermitForRelay {
			relayOpts = append(relayOpts, v2relay.WithACL(relayACL{store: st}))
		}
		opts = append(opts,
			libp2p.EnableRelayService(relayOpts...),
			libp2p.ForceReachabilityPublic(),
		)
	}

	if candidates := relayCandidates(cfg, st); len(candidates) > 0 {
		// AutoRelay only actively reserves a relay slot once it believes
		// this host is privately reachable, a judgment it otherwise leaves
		// to AutoNAT -- which can be slow, or simply wrong on a network
		// (like this project's own test environment) that looks publicly
		// dialable but isn't actually reachable by the specific peer that
		// matters (the raft leader). RelayPeers is only ever set (or a
		// KindBootstrapNode record only ever confirmed) by a caller who
		// already knows this node needs a relay to be reached at all (see
		// Config.RelayPeers' doc comment), so force that judgment instead
		// of leaving the reservation -- and therefore the /p2p-circuit
		// address join()'s awaitRelayAddr waits for -- contingent on
		// AutoNAT.
		opts = append(opts,
			libp2p.ForceReachabilityPrivate(),
			libp2p.EnableAutoRelayWithStaticRelays(candidates),
		)
	}

	return libp2p.New(opts...)
}

// relayCandidates builds newHost's full ordered relay candidate list --
// Config.RelayPeers (the seed list a caller already knows about, e.g.
// cmd/kvnode's -relay-peer flag or mobile/kvmobile's build-time
// relayMultiaddr, tried first and in the order given) followed by every
// currently-confirmed shmevent.KindBootstrapNode record already replicated
// into this node's own local store (see pkg/kvctl's AddRelayNode/
// ConfirmRelayNode/ListRelayNodes), sorted by ascending priority (lower
// tried first -- see EncodeBootstrapNodeMetadata). This is a plain local
// store read, no raft/leader round trip needed, the same same-machine
// trust boundary rangescan/Get already rely on -- so it works even before
// this node has (re)joined a cluster, as long as st already holds
// KindBootstrapNode records from a prior session.
//
// libp2p's EnableAutoRelayWithStaticRelays already accepts (and rotates
// reservations across) more than one peer.AddrInfo, so handing it every
// known-good relay here -- rather than just one -- is what gives a node
// failover if its first-choice relay goes down, without any extra
// dial/retry logic of this package's own. A malformed or unparseable
// entry (RelayPeers or KindBootstrapNode alike) is skipped rather than
// failing node startup outright -- one bad relay address shouldn't take
// down every other candidate still worth trying.
func relayCandidates(cfg Config, st *store.Store) []peer.AddrInfo {
	seen := make(map[peer.ID]bool)
	var candidates []peer.AddrInfo
	addCandidate := func(addr string) {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil || seen[info.ID] {
			return
		}
		seen[info.ID] = true
		candidates = append(candidates, *info)
	}

	for _, addr := range cfg.RelayPeers {
		if addr != "" {
			addCandidate(addr)
		}
	}

	if st != nil {
		lo, hi := shmevent.BootstrapNodeKeyBounds()
		matches, err := st.ScanRange(lo, hi, 0)
		if err == nil {
			type bootstrapEntry struct {
				addr     string
				priority uint8
			}
			entries := make([]bootstrapEntry, 0, len(matches))
			for _, kv := range matches {
				addr, priority, err := shmevent.DecodeBootstrapNodeMetadata(kv.Value)
				if err != nil {
					continue
				}
				entries = append(entries, bootstrapEntry{addr: addr, priority: priority})
			}
			sort.SliceStable(entries, func(i, j int) bool { return entries[i].priority < entries[j].priority })
			for _, e := range entries {
				addCandidate(e.addr)
			}
		}
	}

	return candidates
}

// initRaft lazily constructs the raft transport and raft.Raft instance. It
// must be called at most once, synchronously, from the EventAdd handler.
func (n *Node) initRaft() (*raft.Raft, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.raft != nil {
		return n.raft, nil
	}

	transport := rafttransport.NewTransport(n.host, 10*time.Second)

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(n.peerID)
	if n.cfg.HeartbeatTimeout > 0 {
		raftConf.HeartbeatTimeout = n.cfg.HeartbeatTimeout
	}
	if n.cfg.ElectionTimeout > 0 {
		raftConf.ElectionTimeout = n.cfg.ElectionTimeout
	}
	if n.cfg.CommitTimeout > 0 {
		raftConf.CommitTimeout = n.cfg.CommitTimeout
	}
	if n.cfg.LeaderLeaseTimeout > 0 {
		raftConf.LeaderLeaseTimeout = n.cfg.LeaderLeaseTimeout
	}
	if n.cfg.SnapshotThreshold > 0 {
		raftConf.SnapshotThreshold = n.cfg.SnapshotThreshold
	}
	if n.cfg.SnapshotInterval > 0 {
		raftConf.SnapshotInterval = n.cfg.SnapshotInterval
	}
	if n.cfg.TrailingLogs > 0 {
		raftConf.TrailingLogs = n.cfg.TrailingLogs
	}
	if logFile, err := os.OpenFile(filepath.Join(n.cfg.DataDir, "raft.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		raftConf.LogOutput = logFile
	}

	fsm := kvfsm.New(n.store)
	rf, err := raft.NewRaft(raftConf, fsm, n.logStore, n.logStore, n.snapStore, transport)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("daemon: create raft node: %w", err)
	}

	n.raft = rf
	n.transport = transport
	n.electionTimeout = raftConf.ElectionTimeout

	// Registered before this function returns to whichever caller then
	// calls BootstrapCluster (handleAdd's bootstrap branch) -- so this
	// node's very first self-election, not just later re-elections, is
	// caught too.
	obsCh := make(chan raft.Observation, 8)
	observer := raft.NewObserver(obsCh, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	rf.RegisterObserver(observer)
	n.leadershipObserver = observer
	n.leadershipObsCh = obsCh
	go n.watchLeadership(rf, obsCh)

	return rf, nil
}

// watchLeadership reacts to every leadership-change notification (see
// initRaft's Observer registration) by re-asserting this node's own
// current truth in its KindClusterMember record -- not tracking "who used
// to be leader": deliberately stateless/idempotent, so a redundant
// identical write is harmless and a missed one self-corrects on the next
// transition. Returns once ch is closed (shutdown).
func (n *Node) watchLeadership(rf *raft.Raft, ch chan raft.Observation) {
	for range ch {
		role, ok := n.ownCurrentRole(rf)
		if !ok {
			continue
		}
		if err := n.recordClusterMember(context.Background(), n.peerID, role); err != nil {
			fmt.Fprintf(os.Stderr, "daemon: record own cluster member status: %v\n", err)
		}
	}
}

// ownCurrentRole determines this node's own current role: RoleLeader if
// it's currently raft.Leader, else RoleVoter/RoleLearner per its own
// suffrage in the current configuration. ok is false if this node isn't
// (yet) present in the configuration at all.
func (n *Node) ownCurrentRole(rf *raft.Raft) (role byte, ok bool) {
	if rf.State() == raft.Leader {
		return shmevent.RoleLeader, true
	}
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return 0, false
	}
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(n.peerID) {
			if srv.Suffrage == raft.Nonvoter {
				return shmevent.RoleLearner, true
			}
			return shmevent.RoleVoter, true
		}
	}
	return 0, false
}

// getRaft returns the raft instance if initRaft has already run.
func (n *Node) getRaft() *raft.Raft {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft
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
		rf.Shutdown()
	}
	if transport != nil {
		transport.Close()
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

// awaitRelayAddr waits up to timeout for a /p2p-circuit address -- proof
// one of the relayCandidates reservations configured in newHost has
// completed -- to appear in n.host.Addrs(). A no-op that returns
// immediately when there are no candidates at all, since there's then
// nothing to wait for.
func (n *Node) awaitRelayAddr(timeout time.Duration) {
	if len(relayCandidates(n.cfg, n.store)) == 0 {
		return
	}
	deadline := time.After(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, a := range n.host.Addrs() {
			if strings.Contains(a.String(), "/p2p-circuit") {
				return
			}
		}
		select {
		case <-deadline:
			return
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

func (n *Node) writeReadyFile() error {
	info := ReadyInfo{PeerID: n.peerID, ListenAddrs: n.advertisedAddrs()}
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

// handleShmEvent dispatches one decoded pkg/shmevent.Msg to the appropriate
// callerIdentity is who handleShmEvent should treat as the sender of a
// request: the public key its signature must verify against, and -- for a
// genuinely remote, libp2p-authenticated caller -- the peer id that
// identity resolves to. The zero value means "local pkg/ipc caller":
// verify against this node's own shared key, exactly as before (see
// pkg/shmevent's doc comment on the shmring same-machine trust boundary).
// remotePeer being non-empty is also what lets handleShmEvent tell a
// genuinely remote caller apart from a local one for the
// remote-only restrictions below (no key-fetch bootstrap, optional permit
// gate) -- it is never derived from anything the message itself claims.
type callerIdentity struct {
	verifyPub  shmevent.PublicKey
	remotePeer peer.ID // "" for a local caller
}

// localCaller is every pkg/ipc (shmring) request's identity: this node's
// own shared key, the same one it hands out via EventGetPrivateKey to a
// same-machine caller with no key yet -- see pkg/shmevent's doc comment.
func (n *Node) localCaller() callerIdentity {
	return callerIdentity{verifyPub: n.ed25519Pub}
}

// remoteCaller derives a callerIdentity from s's own libp2p-authenticated
// identity (the Noise/TLS handshake's remote public key/peer id) rather
// than anything the message itself claims -- mirroring the RemotePeer()
// pattern handleForwardConfirmStream already uses for its voter-only
// confirm check.
func remoteCaller(s network.Stream) (callerIdentity, error) {
	pub := s.Conn().RemotePublicKey()
	if pub == nil {
		return callerIdentity{}, fmt.Errorf("remote caller: connection has no authenticated public key")
	}
	raw, err := pub.Raw()
	if err != nil {
		return callerIdentity{}, fmt.Errorf("remote caller: %w", err)
	}
	return callerIdentity{verifyPub: raw, remotePeer: s.Conn().RemotePeer()}, nil
}

// isPermittedPeer reports whether id has a confirmed KindPermitPeer
// record. Only consulted when Config.RequirePermitForRemote or
// Config.RequirePermitForExecute is true -- see those fields' doc
// comments.
func (n *Node) isPermittedPeer(id peer.ID) bool {
	return isPermittedPeer(n.store, id)
}

// isPermittedPeer is the package-level form of the check above, taking a
// *store.Store directly rather than a *Node -- needed by relayACL, which
// is constructed inside newHost before a *Node exists yet (see start).
func isPermittedPeer(st *store.Store, id peer.ID) bool {
	_, err := st.Get(shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusConfirmed, []byte(id.String())))
	return err == nil
}

// forgetTransientPeer removes id's peerstore entry (addresses, public key,
// supported-protocol bookkeeping) once every connection to it has closed,
// unless id is a *current* raft voter/learner (checked live against
// n.getRaft's configuration, not isClusterMember's persistent-forever
// KindClusterMember record just below -- that would keep every peer this
// node has ever admitted, exactly the growth this exists to stop). The
// default libp2p peerstore this project constructs (see newHost) never
// evicts protobook/keybook/metadata entries on its own; only the addrbook
// has any TTL, and only for addresses added with one. A long-lived node
// dialed by hundreds of distinct, never-returning peers over weeks (e.g.
// this project's own e2e pipeline, which mints a fresh identity per test
// run) would otherwise keep every single one's peerstore entry forever --
// one of two confirmed root causes (the other: streamRequestTimeout) of a
// real production node found running at 4.95GB RSS against ~265MB of
// real on-disk data after 14 days of uptime.
//
// A current cluster member's entry must survive a disconnect -- raft
// needs to redial it, and dialForward already re-adds its address on
// every forwarded call regardless, so removing it here costs nothing
// except a redial's address lookup that would otherwise be a cache hit.
func (n *Node) forgetTransientPeer(id peer.ID) {
	if n.host.Network().Connectedness(id) == network.Connected {
		// Another connection to this same peer is still open (or one
		// raced back in right as this one closed) -- not actually gone.
		return
	}
	if rf := n.getRaft(); rf != nil {
		if cfgFuture := rf.GetConfiguration(); cfgFuture.Error() == nil {
			for _, srv := range cfgFuture.Configuration().Servers {
				if srv.ID == raft.ServerID(id.String()) {
					return
				}
			}
		}
	}
	n.host.Peerstore().RemovePeer(id)
}

// isClusterMember reports whether id has a KindClusterMember record --
// i.e. is currently (or was, since nothing ever deletes this record) a
// raft voter or learner, per recordClusterMember. Only consulted when
// Config.RequirePermitForExecute is true, to let a cluster member send
// Execute notifications without also needing a separate KindPermitPeer
// permit -- membership already implies stronger standing than a relay
// permit.
func (n *Node) isClusterMember(id peer.ID) bool {
	_, err := n.store.Get(shmevent.ClusterMemberKey([]byte(id.String())))
	return err == nil
}

// isPermittedForLogKind reports whether id has a confirmed KindLogPermit
// record for logKind -- see that kind's doc comment. Only consulted when
// Config.RequirePermitForLog is true. Unlike isClusterMember's role in
// Config.RequirePermitForExecute, cluster membership is deliberately NOT
// checked here as an alternative: this gate matches
// Config.RequirePermitForRemote's existing behavior, which doesn't exempt
// cluster members either -- every peer, member or not, needs its own
// explicit per-kind grant.
func (n *Node) isPermittedForLogKind(logKind string, id peer.ID) bool {
	key, err := shmevent.LogPermitKey(shmevent.StatusConfirmed, logKind, []byte(id.String()))
	if err != nil {
		return false
	}
	_, err = n.store.Get(key)
	return err == nil
}

// rejectReservedKey refuses an ordinary EventSet/EventSetField write
// whose key starts with a byte this codebase reserves for its own
// internal bookkeeping -- shmevent.SystemKeyPrefix (permits/bootstrap
// nodes/cluster membership) or logrecord.LogKeyPrefix (log records) --
// so a caller can never collide with (or corrupt) either namespace by
// chance or malice. Both prefixes are single reserved bytes checked the
// same way; kept as one helper so a third reserved namespace only needs
// one new case here, not a change at every call site.
func rejectReservedKey(key []byte) error {
	if len(key) == 0 {
		return nil
	}
	switch key[0] {
	case shmevent.SystemKeyPrefix:
		return fmt.Errorf("key namespace starting with 0x%02x is reserved for system use", shmevent.SystemKeyPrefix)
	case logrecord.LogKeyPrefix:
		return fmt.Errorf("key namespace starting with 0x%02x is reserved for logrecord use", logrecord.LogKeyPrefix)
	default:
		return nil
	}
}

// relayACL is the v2relay.ACLFilter wired into newHost via
// v2relay.WithACL when Config.RequirePermitForRelay is set -- it's what
// actually makes a confirmed KindPermitPeer record mean "permission for a
// peer to join/use the cluster's relay" (that kind's doc comment) rather
// than just gating the unrelated shmevent RPC layer. Both reservation
// (AllowReserve) and outgoing-connect (AllowConnect) check the peer
// that's trying to use *this* node's relay service -- the destination in
// AllowConnect is who src is dialing through the relay, not itself
// requesting anything, so it's never checked here.
//
// Every peer this gate admits is registered under the same
// shmevent.RelayLimits (EventPermitRequest stamps this node's current
// defaults onto the record at request time -- see relayLimits/
// EncodePermitPeerPayload): go-libp2p's circuitv2 relay hands one
// v2relay.Resources value to every ACL-approved peer alike (see newHost),
// with no hook for a per-individual-peer override, so the gate here is
// binary admission (has this peer been registered at all?), not a
// per-peer quota check -- the actual circuit/data/reservation ceilings
// are enforced uniformly, for every admitted peer, by go-libp2p itself.
type relayACL struct {
	store *store.Store
}

func (a relayACL) AllowReserve(p peer.ID, _ multiaddr.Multiaddr) bool {
	return isPermittedPeer(a.store, p)
}

func (a relayACL) AllowConnect(src peer.ID, _ multiaddr.Multiaddr, _ peer.ID) bool {
	return isPermittedPeer(a.store, src)
}

// raft/store/registry operation and returns the Msg to send back -- the
// single entry point both pkg/ipc.Serve (local shared memory) and
// handleClientStream (ClientProtocolID, the remote equivalent for a
// browser learner) call into. See pkg/shmevent's doc comment for the
// overall protocol design and api/shmevent.capnp for the wire struct.
//
// caller identifies who's asking (see callerIdentity) -- a remote caller
// gets two restrictions a local one doesn't: EventGetPrivateKey/
// EventGetPublicKey are refused outright (a remote caller always has its
// own key already -- see web-app's do_connect -- so the bootstrap
// exception that exists for a same-machine caller with no key yet has no
// legitimate remote use, and serving it remotely would just hand out this
// node's own private key to anyone able to dial it), and, if
// Config.RequirePermitForRemote is set, every event but
// EventPermitRequest/EventPermitConfirm/EventLogPermitRequest/
// EventLogPermitConfirm additionally requires a confirmed KindPermitPeer
// record for the caller's own authenticated peer id. EventLogAppend and
// EventListRange are separately, additionally gated per log kind by
// Config.RequirePermitForLog -- see those cases below.

// logNamespaceLo and logNamespaceHi bound the half-open interval [lo, hi)
// containing exactly every possible key that begins with
// logrecord.LogKeyPrefix, regardless of what follows it or how long it
// is: any such key compares >= []byte{LogKeyPrefix} and <
// []byte{LogKeyPrefix+1} under bytes.Compare (a shared first byte makes
// length/suffix irrelevant to that particular comparison), and no key
// that doesn't start with LogKeyPrefix can fall in that interval either
// way -- see overlapsLogNamespace.
var (
	logNamespaceLo = []byte{logrecord.LogKeyPrefix}
	logNamespaceHi = []byte{logrecord.LogKeyPrefix + 1}
)

// overlapsLogNamespace reports whether the inclusive byte range [start,
// end] could return any pkg/logrecord-prefixed key, for
// EventListRange's Config.RequirePermitForLog gate -- not just whether
// start itself happens to begin there. pkg/store.Store.ScanRange has no
// concept of namespaces: a caller could pick a start from outside the
// logrecord namespace entirely (the empty string, or a key under
// shmevent.SystemKeyPrefix) paired with an end chosen past it, and still
// have logrecord data caught inside that wider range -- a check that only
// inspected start[0] would never trigger for that case, even though the
// scan itself would still return the data.
func overlapsLogNamespace(start, end []byte) bool {
	return bytes.Compare(start, logNamespaceHi) < 0 && bytes.Compare(end, logNamespaceLo) >= 0
}

func (n *Node) handleShmEvent(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte, caller callerIdentity) shmevent.Msg {
	if caller.remotePeer != "" && (m.EventType == shmevent.EventGetPrivateKey || m.EventType == shmevent.EventGetPublicKey) {
		return errorMsg(m.ID, fmt.Errorf("%s: not available to a remote caller -- bring your own key", shmevent.EventName(m.EventType)))
	}
	if caller.remotePeer != "" && m.EventType == shmevent.EventLeave {
		return errorMsg(m.ID, fmt.Errorf("leave: not available to a remote caller -- only this node's own operator decides to leave"))
	}
	if caller.remotePeer != "" && m.EventType == shmevent.EventExecInviteRedeem {
		return errorMsg(m.ID, fmt.Errorf("exec_invite_redeem: not available to a remote caller -- this node dials sourceAddr on its own operator's behalf, never on an arbitrary remote caller's"))
	}
	if caller.remotePeer != "" && (m.EventType == shmevent.EventJoinRequestCreate || m.EventType == shmevent.EventJoinRequestCancel) {
		return errorMsg(m.ID, fmt.Errorf("%s: not available to a remote caller -- only this node's own operator mints or cancels its own join-request ticket", shmevent.EventName(m.EventType)))
	}
	if caller.remotePeer != "" && m.EventType == shmevent.EventRecruit {
		return errorMsg(m.ID, fmt.Errorf("recruit: not available to a remote caller -- this node dials the ticket's device on its own operator's behalf, never on an arbitrary remote caller's"))
	}
	if caller.remotePeer != "" && (m.EventType == shmevent.EventChannelOpen || m.EventType == shmevent.EventChannelSend ||
		m.EventType == shmevent.EventChannelPoll || m.EventType == shmevent.EventChannelListen || m.EventType == shmevent.EventChannelClose ||
		m.EventType == shmevent.EventChannelCloseWrite) {
		return errorMsg(m.ID, fmt.Errorf("%s: not available to a remote caller -- only this node's own operator drives its own channel sessions", shmevent.EventName(m.EventType)))
	}
	if shmevent.RequiresSignature(m.EventType) {
		if err := shmevent.Verify(caller.verifyPub, m, crc, sig); err != nil {
			return errorMsg(m.ID, err)
		}
	}
	if caller.remotePeer != "" && n.cfg.RequirePermitForRemote &&
		m.EventType != shmevent.EventPermitRequest && m.EventType != shmevent.EventPermitConfirm &&
		m.EventType != shmevent.EventLogPermitRequest && m.EventType != shmevent.EventLogPermitConfirm {
		if !n.isPermittedPeer(caller.remotePeer) {
			return errorMsg(m.ID, fmt.Errorf("%s not permitted -- send permit_request and have a raft voter confirm it first", caller.remotePeer))
		}
	}

	switch m.EventType {
	case shmevent.EventSetKey:
		n.registry.Register(m.ID, m.Value)
		return shmevent.Msg{EventType: shmevent.EventSetKey, ID: m.ID, Value: m.Value}

	case shmevent.EventGetKey:
		v, ok := n.registry.Lookup(m.SourceID)
		if !ok {
			return errorMsg(m.ID, fmt.Errorf("no entry registered under id %d", m.SourceID))
		}
		return shmevent.Msg{EventType: shmevent.EventGetKey, ID: m.ID, Value: v}

	case shmevent.EventSetField:
		key, ok := n.registry.Lookup(m.SourceID)
		if !ok {
			return errorMsg(m.ID, fmt.Errorf("no key registered under id %d -- send SetKey first", m.SourceID))
		}
		if err := rejectReservedKey(key); err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleSetForward(ctx, key, m.Value, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventSetField, ID: m.ID}

	case shmevent.EventSet:
		key, value, err := shmevent.DecodeSetPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := rejectReservedKey(key); err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleSetForward(ctx, key, value, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventSet, ID: m.ID}

	case shmevent.EventPermitRequest:
		kind, peerID, metadata, err := shmevent.DecodePermitRequestPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		// A KindPermitPeer request registers peerID with this node's
		// current default relay allotment (see relayLimits/
		// shmevent.RelayLimits) rather than trusting whatever metadata
		// the caller sent -- the whole point is that a peer ends up
		// bound to a standard, node-decided allotment on request, not
		// one it gets to name for itself.
		if kind == shmevent.KindPermitPeer {
			metadata = shmevent.EncodePermitPeerPayload(peerID, relayLimits(n.cfg))
		}
		key := shmevent.SystemKey(kind, shmevent.StatusPending, peerID)
		if err := n.handleSetForward(ctx, key, metadata, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPermitRequest, ID: m.ID}

	case shmevent.EventPermitConfirm:
		// The only place that actually enforces "only a raft voter may
		// confirm" (see shmevent.EventPermitConfirm's doc comment): check
		// the *original* caller's own identity here, once, before doing
		// anything else -- not handleForwardConfirmStream's check, which
		// only ever authenticates whichever node relayed the request one
		// hop closer to the leader (using *its own* libp2p identity), not
		// who actually asked. A remote caller with no standing at all
		// could otherwise dial any legitimate voter follower and have it
		// unwittingly relay the confirm on its behalf, or dial the leader
		// directly (handleConfirmForward's isLeader branch used to apply
		// with no identity check at all -- that reasoning only held when
		// caller and node were the same actor, true for a local pkg/ipc
		// operator but not for a remote caller.Conn()-authenticated key).
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		kind, peerID, err := shmevent.DecodePermitConfirmPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		pendingKey := shmevent.SystemKey(kind, shmevent.StatusPending, peerID)
		confirmedKey := shmevent.SystemKey(kind, shmevent.StatusConfirmed, peerID)
		if err := n.handleConfirmForward(ctx, kvfsm.OpConfirm, pendingKey, confirmedKey, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPermitConfirm, ID: m.ID}

	case shmevent.EventPermitRevoke:
		// Same "only a raft voter may act" enforcement as
		// EventPermitConfirm above, for the identical reason (see that
		// case's comment) -- checked once here against the original
		// caller's own identity, not just relied on downstream.
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		kind, peerID, err := shmevent.DecodePermitConfirmPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		confirmedKey := shmevent.SystemKey(kind, shmevent.StatusConfirmed, peerID)
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, confirmedKey, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPermitRevoke, ID: m.ID}

	case shmevent.EventLogPermitRequest:
		logKind, peerID, metadata, err := shmevent.DecodeLogPermitRequestPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key, err := shmevent.LogPermitKey(shmevent.StatusPending, logKind, peerID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleSetForward(ctx, key, metadata, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventLogPermitRequest, ID: m.ID}

	case shmevent.EventLogPermitConfirm:
		// Same "only a raft voter may act" enforcement as EventPermitConfirm
		// (see that case's comment) -- checked once here against the
		// original caller's own identity.
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		logKind, peerID, err := shmevent.DecodeLogPermitConfirmPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		pendingKey, err := shmevent.LogPermitKey(shmevent.StatusPending, logKind, peerID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		confirmedKey, err := shmevent.LogPermitKey(shmevent.StatusConfirmed, logKind, peerID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpConfirm, pendingKey, confirmedKey, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventLogPermitConfirm, ID: m.ID}

	case shmevent.EventLogPermitRevoke:
		// Same "only a raft voter may act" enforcement as EventPermitConfirm
		// above.
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		logKind, peerID, err := shmevent.DecodeLogPermitConfirmPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		confirmedKey, err := shmevent.LogPermitKey(shmevent.StatusConfirmed, logKind, peerID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, confirmedKey, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventLogPermitRevoke, ID: m.ID}

	case shmevent.EventLeave:
		if err := n.leaveCluster(ctx); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventLeave, ID: m.ID}

	case shmevent.EventKick:
		// Same "only a raft voter may act" check EventPermitConfirm's own
		// doc comment explains -- skipped for a local caller (trusted
		// implicitly, see that comment), enforced here for a remote one
		// since this is the only place that authenticates who *actually*
		// asked, not just whichever node last relayed the request one hop
		// closer to the leader (handleForwardKickStream's own check).
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		if err := n.kickPeer(ctx, string(m.Value)); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventKick, ID: m.ID}

	// EventGroupPut/Delete, EventCommandPut/Delete,
	// EventGroupCommandPut/Delete, and EventPeerGroupPut/Delete implement
	// the group-based ACL catalog's single-step CRUD (see
	// shmevent.KindGroup's doc comment): each does the identical inline
	// "only a raft voter may act" early-reject EventPermitRevoke/
	// EventLogPermitConfirm above already do for a directly-reached remote
	// caller (correctness for every other path -- local, or remote hitting
	// a non-leader node -- still comes from handleConfirmForward's
	// forward-to-leader hop, whose handleForwardConfirmStream re-checks
	// voter status against the authenticated forwarding identity
	// regardless), then decodes its payload, builds the record's key, and
	// applies via handleConfirmForward -- kvfsm.OpSet for Put (a direct
	// overwrite: create and update are the same operation, no separate
	// revision history the way pkg/logrecord keeps), kvfsm.OpDel for a
	// relation Delete, kvfsm.OpCascadeDelete for a Group/Command Delete
	// (which also removes every GroupCommand/PeerGroup record referencing
	// the deleted id -- see kvfsm.Apply's OpCascadeDelete case).
	case shmevent.EventGroupPut:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		id, name, public, err := shmevent.DecodeGroupPutPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key := shmevent.GroupKey([]byte(id))
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, shmevent.EncodeGroupPayload(name, public), true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventGroupPut, ID: m.ID}

	case shmevent.EventGroupDelete:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		key := shmevent.GroupKey(m.Value)
		if err := n.handleConfirmForward(ctx, kvfsm.OpCascadeDelete, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventGroupDelete, ID: m.ID}

	case shmevent.EventCommandPut:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		id, name, peerID, err := shmevent.DecodeCommandPutPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key := shmevent.CommandKey([]byte(id))
		value, err := shmevent.EncodeCommandPayload(name, peerID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, value, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventCommandPut, ID: m.ID}

	case shmevent.EventCommandDelete:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		key := shmevent.CommandKey(m.Value)
		if err := n.handleConfirmForward(ctx, kvfsm.OpCascadeDelete, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventCommandDelete, ID: m.ID}

	case shmevent.EventGroupCommandPut:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		commandID, groupID, err := shmevent.DecodeGroupCommandPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key, err := shmevent.GroupCommandKey(commandID, groupID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventGroupCommandPut, ID: m.ID}

	case shmevent.EventGroupCommandDelete:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		commandID, groupID, err := shmevent.DecodeGroupCommandPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key, err := shmevent.GroupCommandKey(commandID, groupID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventGroupCommandDelete, ID: m.ID}

	case shmevent.EventPeerGroupPut:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		peerID, groupID, err := shmevent.DecodePeerGroupPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key, err := shmevent.PeerGroupKey(peerID, groupID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPeerGroupPut, ID: m.ID}

	case shmevent.EventPeerGroupDelete:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		peerID, groupID, err := shmevent.DecodePeerGroupPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key, err := shmevent.PeerGroupKey(peerID, groupID)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPeerGroupDelete, ID: m.ID}

	// EventJoinInviteCreate/Revoke are direct writes gated the identical
	// "only a raft voter may act" way EventGroupPut/Delete are (see that
	// case's comment) -- creating one is the entire authorization step for
	// a one-time raft join, so it needs the same live-voter check any
	// other privileged direct write gets. Redeeming a token happens
	// entirely inside handleJoinStream/admitOrLodgeJoin instead, not here.
	case shmevent.EventJoinInviteCreate:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		token, suffrage, err := shmevent.DecodeJoinInviteCreatePayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key := shmevent.JoinInviteKey(token)
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, shmevent.EncodeJoinInviteRecord(suffrage), true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventJoinInviteCreate, ID: m.ID}

	case shmevent.EventJoinInviteRevoke:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		token, err := shmevent.DecodeJoinInviteRevokePayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key := shmevent.JoinInviteKey(token)
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventJoinInviteRevoke, ID: m.ID}

	// EventJoinRequestCreate/Cancel manage this node's own in-memory
	// join-request ticket (see that event pair's doc comment) -- never
	// raft/store writes, so unlike every case above there's no voter check
	// here at all (this node itself may have no raft instance yet); the
	// remote-caller rejection already happened at the top of this
	// function.
	case shmevent.EventJoinRequestCreate:
		token := make([]byte, shmevent.JoinInviteTokenSize)
		if _, err := cryptorand.Read(token); err != nil {
			return errorMsg(m.ID, fmt.Errorf("generate join request token: %w", err))
		}
		n.setJoinRequestToken(token)
		return shmevent.Msg{EventType: shmevent.EventJoinRequestCreate, ID: m.ID, Value: token}

	case shmevent.EventJoinRequestCancel:
		token, err := shmevent.DecodeJoinRequestCancelPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		n.cancelJoinRequestToken(token)
		return shmevent.Msg{EventType: shmevent.EventJoinRequestCancel, ID: m.ID}

	// EventRecruit is the reverse of EventJoinInviteCreate: this node
	// (already a raft voter, enforced by mintJoinInvite's own
	// handleConfirmForward the same way EventJoinInviteCreate's write is)
	// mints a normal join invite on its own cluster and hand-delivers it
	// directly to the device named in the ticket -- see
	// dialAndPushRecruit/handleRecruitStream for the actual network leg.
	case shmevent.EventRecruit:
		ticket, suffrage, err := shmevent.DecodeRecruitPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		result, err := n.dialAndPushRecruit(ctx, ticket, suffrage)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventRecruit, ID: m.ID, Value: []byte(result)}

	// EventExecInviteCreate/Revoke are direct writes gated the identical
	// "only a raft voter may act" way EventJoinInviteCreate/Revoke are (see
	// those cases' comments) -- creating one is the entire authorization
	// step for what commandID+inputsJSON a token may trigger. Redeeming a
	// token happens entirely over ExecInviteRedeemProtocolID instead (see
	// dialAndRedeemExecInvite/handleExecInviteRedeemStream), not here.
	case shmevent.EventExecInviteCreate:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		token, commandID, inputsJSON, err := shmevent.DecodeExecInviteCreatePayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key := shmevent.ExecInviteKey(token)
		if err := n.handleConfirmForward(ctx, kvfsm.OpSet, key, shmevent.EncodeExecInviteRecord(commandID, inputsJSON), true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventExecInviteCreate, ID: m.ID}

	case shmevent.EventExecInviteRevoke:
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		token, err := shmevent.DecodeExecInviteRevokePayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key := shmevent.ExecInviteKey(token)
		if err := n.handleConfirmForward(ctx, kvfsm.OpDel, key, nil, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventExecInviteRevoke, ID: m.ID}

	// EventExecInviteRedeem is local-only (rejected above if
	// caller.remotePeer != ""): it tells this node's own daemon to dial
	// sourceAddr and redeem token there on its own operator's behalf -- see
	// dialAndRedeemExecInvite for the actual network leg.
	case shmevent.EventExecInviteRedeem:
		sourceAddr, token, err := shmevent.DecodeExecInviteRedeemRequest(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		instanceID, err := n.dialAndRedeemExecInvite(ctx, sourceAddr, token)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventExecInviteRedeem, ID: m.ID, Value: []byte(instanceID)}

	case shmevent.EventExecute:
		if err := n.dispatchExecute(ctx, m); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventExecute, ID: m.ID}

	case shmevent.EventPollExecute:
		notif, ok := n.executeInbox.pop()
		if !ok {
			return shmevent.Msg{EventType: shmevent.EventPollExecute, ID: m.ID}
		}
		value, err := shmevent.EncodeExecuteNotification(notif.senderPeerID, notif.payload)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPollExecute, Value: value, ID: m.ID}

	case shmevent.EventChannelOpen:
		channelID, err := n.dispatchChannelOpen(ctx, string(m.Value))
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventChannelOpen, ID: m.ID, Value: []byte(channelID)}

	case shmevent.EventChannelSend:
		channelID, chunk, err := shmevent.DecodeChannelSendPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if err := n.dispatchChannelSend(string(channelID), chunk); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventChannelSend, ID: m.ID}

	case shmevent.EventChannelPoll:
		resp, err := n.dispatchChannelPoll(string(m.Value))
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventChannelPoll, ID: m.ID, Value: resp}

	case shmevent.EventChannelListen:
		resp, err := n.dispatchChannelListen()
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventChannelListen, ID: m.ID, Value: resp}

	case shmevent.EventChannelClose:
		if err := n.dispatchChannelClose(string(m.Value)); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventChannelClose, ID: m.ID}

	case shmevent.EventChannelCloseWrite:
		if err := n.dispatchChannelCloseWrite(string(m.Value)); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventChannelCloseWrite, ID: m.ID}

	case shmevent.EventGetField:
		key := m.Value
		if m.SourceID != 0 {
			k, ok := n.registry.Lookup(m.SourceID)
			if !ok {
				return errorMsg(m.ID, fmt.Errorf("no key registered under id %d -- send SetKey first", m.SourceID))
			}
			key = k
		}
		value, err := n.handleGet(key)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventGetField, ID: m.ID, Value: value}

	case shmevent.EventListRange:
		start, end, err := shmevent.DecodeListRangeQuery(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		// Config.RequirePermitForLog only applies to a range that could
		// actually return something inside pkg/logrecord's namespace --
		// anything else (ordinary data, shmevent.SystemKeyPrefix data)
		// falls through unchanged, governed only by the generic
		// RequirePermitForRemote gate above, same as today. This checks
		// for any overlap with that namespace (see overlapsLogNamespace),
		// not just whether start itself begins there: pkg/store.ScanRange
		// is a raw byte range with no concept of namespaces, so a start
		// chosen from outside the namespace (even the empty string) paired
		// with an end chosen past it would still return logrecord data --
		// checking only start[0] would let that through ungated. Both
		// start and end must parse to the same kind once the gate is
		// triggered: checking only start would let a permitted start
		// smuggle in an end that overruns into an unpermitted kind's
		// range.
		if caller.remotePeer != "" && n.cfg.RequirePermitForLog &&
			overlapsLogNamespace(start, end) {
			kindStart, _, _, errStart := logrecord.ParseKey(start)
			kindEnd, _, _, errEnd := logrecord.ParseKey(end)
			if errStart != nil || errEnd != nil || kindStart != kindEnd {
				return errorMsg(m.ID, fmt.Errorf("list_range: range must be scoped to a single well-formed logrecord kind under RequirePermitForLog"))
			}
			if !n.isPermittedForLogKind(kindStart, caller.remotePeer) {
				return errorMsg(m.ID, fmt.Errorf("%s not permitted for log kind %q", caller.remotePeer, kindStart))
			}
		}
		matches, err := n.store.ScanRange(start, end, 1)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if len(matches) == 0 {
			return shmevent.Msg{EventType: shmevent.EventListRange, ID: m.ID}
		}
		value, err := shmevent.EncodeListRangeQuery(matches[0].Key, matches[0].Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventListRange, ID: m.ID, Value: value}

	case shmevent.EventLogAppend:
		key, value, err := shmevent.DecodeSetPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if len(key) == 0 || key[0] != logrecord.LogKeyPrefix {
			return errorMsg(m.ID, fmt.Errorf("log_append: key must start with the reserved logrecord prefix"))
		}
		kind, _, _, err := logrecord.ParseKey(key)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if caller.remotePeer != "" && n.cfg.RequirePermitForLog {
			if !n.isPermittedForLogKind(kind, caller.remotePeer) {
				return errorMsg(m.ID, fmt.Errorf("%s not permitted for log kind %q", caller.remotePeer, kind))
			}
		}
		// A CommandRequest append (SubmitCommand's actual write) gets its
		// own op, OpAppendCommandRequest, instead of the plain OpSet every
		// other log kind uses below -- it's what makes the submitting
		// peer's real Group/GroupCommand/PeerGroup/Public ACL standing a
		// raft-authoritative check (evaluated inside Apply, against every
		// replica's identically-ordered state) rather than a convention
		// only pkg/kvctl/mobile/kvmobile's own SubmitCommand client
		// enforces client-side -- see OpAppendCommandRequest's doc
		// comment. authorPeerID is this call's own connection-authenticated
		// identity (never anything m.Value itself claims): caller.remotePeer
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
				return errorMsg(m.ID, err)
			}
			if _, err := n.handleOpForward(ctx, kvfsm.OpAppendCommandRequest, key, payload, true); err != nil {
				return errorMsg(m.ID, err)
			}
			return shmevent.Msg{EventType: shmevent.EventLogAppend, ID: m.ID}
		}
		if err := n.handleSetForward(ctx, key, value, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventLogAppend, ID: m.ID}

	case shmevent.EventGetPublicKey:
		return shmevent.Msg{EventType: shmevent.EventGetPublicKey, ID: m.ID, Value: n.ed25519Pub}

	case shmevent.EventGetPrivateKey:
		return shmevent.Msg{EventType: shmevent.EventGetPrivateKey, ID: m.ID, Value: n.ed25519Priv}

	case shmevent.EventGetOwnAddr:
		return shmevent.Msg{EventType: shmevent.EventGetOwnAddr, ID: m.ID, Value: []byte(n.advertisedAddrs()[0])}

	case shmevent.EventAdd:
		peerID, err := n.handleAddDispatch(ctx, m, caller.remotePeer)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventAdd, ID: m.ID, Value: []byte(peerID)}

	default:
		return errorMsg(m.ID, fmt.Errorf("unknown event %d", m.EventType))
	}
}

// errorMsg builds the response for a failed request -- see
// shmevent.EventError's doc comment for why this event exists even though
// it isn't part of api/shmevent.capnp's originally specified field set.
func errorMsg(id uint16, err error) shmevent.Msg {
	msg := err.Error()
	if len(msg) > shmevent.ValueSize {
		msg = msg[:shmevent.ValueSize]
	}
	return shmevent.Msg{EventType: shmevent.EventError, ID: id, Value: []byte(msg)}
}

// handleAddDispatch implements EventAdd's three shapes -- see
// EventAdd's doc comment in pkg/shmevent -- and returns this node's own
// peer id on success, mirroring the pre-shmevent ipcproto.ActionAdd
// response. remotePeer is the caller's own libp2p-authenticated identity
// ("" for a local pkg/ipc caller, see callerIdentity) -- checked against
// the learner-join branch's claimed peer id below.
func (n *Node) handleAddDispatch(ctx context.Context, m shmevent.Msg, remotePeer peer.ID) (string, error) {
	// Learner join (remote browser caller, via ClientProtocolID): SourceID
	// references a prior EventSetKey holding the caller's own peer id,
	// Value is the caller's own reachable address.
	if m.SourceID != 0 {
		joinPeerID, ok := n.registry.Lookup(m.SourceID)
		if !ok {
			return "", fmt.Errorf("no peer id registered under id %d -- send SetKey first", m.SourceID)
		}
		// A remote caller could otherwise register any peer id string it
		// likes via EventSetKey -- not necessarily its own -- and get it
		// added to the raft configuration at an address of its choosing.
		// Binding the claim to the stream's own authenticated identity
		// (unforgeable -- established by the libp2p handshake, not
		// anything the message itself carries) closes that.
		if remotePeer != "" && string(joinPeerID) != remotePeer.String() {
			return "", fmt.Errorf("add: claimed peer id %q does not match the authenticated connection identity %s", joinPeerID, remotePeer)
		}
		return n.handleAddLearner(ctx, string(joinPeerID), string(m.Value))
	}
	// Bootstrap (Value empty) or voter join (Value = leader peer id/multiaddr).
	return n.handleAdd(ctx, string(m.Value))
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
	n.awaitRelayAddr(45 * time.Second)

	maddr, err := multiaddr.NewMultiaddr(leaderAddr)
	if err != nil {
		return "", fmt.Errorf("invalid leader address %q: %w", leaderAddr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("leader address %q missing peer id: %w", leaderAddr, err)
	}
	if err := n.host.Connect(ctx, *info); err != nil {
		return "", fmt.Errorf("connect to leader %s: %w", info.ID, err)
	}

	s, err := n.host.NewStream(ctx, info.ID, JoinProtocolID)
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
	addr := sourceAddr
	if !registry.IsMultiaddr(addr) {
		reg, err := registry.Open()
		if err != nil {
			return "", err
		}
		addr, err = reg.ResolveAddress(addr)
		if err != nil {
			return "", err
		}
	}

	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return "", fmt.Errorf("invalid source address %q: %w", addr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("source address %q missing peer id: %w", addr, err)
	}
	if err := n.host.Connect(ctx, *info); err != nil {
		return "", fmt.Errorf("connect to %s: %w", info.ID, err)
	}

	value, err := shmevent.EncodeExecuteNotification([]byte(n.peerID), token)
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: encode notification: %w", err)
	}
	buf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventExecInviteRedeem, Value: value}, n.ed25519Priv)
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: encode message: %w", err)
	}

	s, err := n.host.NewStream(ctx, info.ID, ExecInviteRedeemProtocolID)
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

	inviteToken, err := n.mintJoinInvite(ctx, suffrage)
	if err != nil {
		return "", fmt.Errorf("recruit: mint join invite: %w", err)
	}

	addr := deviceAddr
	if !registry.IsMultiaddr(addr) {
		reg, err := registry.Open()
		if err != nil {
			return "", err
		}
		addr, err = reg.ResolveAddress(addr)
		if err != nil {
			return "", err
		}
	}

	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return "", fmt.Errorf("invalid device address %q: %w", addr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("device address %q missing peer id: %w", addr, err)
	}
	if err := n.host.Connect(ctx, *info); err != nil {
		return "", fmt.Errorf("connect to %s: %w", info.ID, err)
	}

	s, err := n.host.NewStream(ctx, info.ID, RecruitProtocolID)
	if err != nil {
		return "", fmt.Errorf("open recruit stream to %s: %w", info.ID, err)
	}
	defer s.Close()

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
	if m.EventType != shmevent.EventExecInviteRedeem {
		return "", fmt.Errorf("exec invite redeem: expected EventExecInviteRedeem, got %s", shmevent.EventName(m.EventType))
	}
	redeemerPeerIDBytes, token, err := shmevent.DecodeExecuteNotification(m.Value)
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: decode notification: %w", err)
	}
	redeemerPeer, err := peer.Decode(string(redeemerPeerIDBytes))
	if err != nil {
		return "", fmt.Errorf("exec invite redeem: invalid redeemer peer id %q: %w", redeemerPeerIDBytes, err)
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
	commandID, inputsJSON, err := shmevent.DecodeExecInviteRecord(res.Value)
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
	if err := n.handleSetForward(ctx, key, metadata, true); err != nil {
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

// handleSetForward is the entry point for a Set (EventSetField, or
// handleForwardSetStream's leader-side answer to an already-forwarded
// request): it applies directly if this node is the leader, or forwards
// to the leader (one hop only) if not. allowForward is false on the
// already-forwarded path so a request can be forwarded at most once: if
// the node it lands on then *also* turns out not to be leader (a
// leadership change mid-flight), it fails outward with a clear error
// instead of forwarding again, which rules out any forwarding cycle
// regardless of how leadership bounces around. A thin kvfsm.OpSet
// wrapper around handleOpForward -- see that doc comment for why
// OpAppendCommandRequest also goes through here rather than through
// handleConfirmForward.
func (n *Node) handleSetForward(ctx context.Context, key, value []byte, allowForward bool) error {
	_, err := n.handleOpForward(ctx, kvfsm.OpSet, key, value, allowForward)
	return err
}

// handleOpForward is handleSetForward generalized to any op that -- like
// OpSet, and unlike OpConfirm/OpDel/OpCascadeDelete/the catalog's OpSet
// puts -- needs no sender-is-a-raft-voter check at the forwarding hop:
// OpAppendCommandRequest's actual authorization (the submitting peer's
// Group/GroupCommand/PeerGroup/Public ACL standing, checked against
// authorPeerID inside kvfsm.Apply itself) doesn't depend at all on
// whether the *node relaying the write* happens to be a voter -- a
// non-voting learner (every web-app browser tab, or a phone joined with
// suffrage "learner") must still be able to submit a command it's
// permitted for and have it forwarded on to the leader. Routing it
// through the voter-gated ForwardConfirmProtocolID instead would reject
// exactly that legitimate case with "not a current raft voter", even
// though Apply's own ACL check is what's actually supposed to gate it.
// handleOpForward returns the raft log index the write ended up applied
// at (this node's own index if it's the leader, the leader's index if
// forwarded) alongside the usual error -- see forwardOp/waitForLocalApply
// for why a forwarding follower needs that index before it can safely
// report success to its own caller.
func (n *Node) handleOpForward(ctx context.Context, op kvfsm.OpType, key, value []byte, allowForward bool) (uint64, error) {
	// Both wait windows below scale off the actual configured election
	// timeout (not a fixed constant) so a WAN-tuned longer timeout still
	// gets a comfortable margin: Apply itself can legitimately take a full
	// election cycle if the leader steps down and a new one is elected
	// mid-call.
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return 0, err
	}
	if isLeader {
		return n.applyOp(rf, op, key, value)
	}
	if !allowForward {
		return 0, fmt.Errorf("not leader; current leader is %s (already forwarded once)", leaderID)
	}
	index, err := n.forwardOp(ctx, op, leaderID, key, value)
	if err != nil {
		return 0, err
	}
	// The leader's ack only proves the write is durable+applied on the
	// leader -- this node's own copy (which is what any immediately
	// following local Get on this node will read) catches up
	// asynchronously via ordinary AppendEntries replication, on no fixed
	// schedule relative to the ack. Waiting here for our own applied index
	// to reach the leader's closes that read-your-own-writes race for the
	// common case (healthy replication, which is at least as fast as the
	// round trip that already happened to get this ack) without turning a
	// write that already genuinely succeeded into a reported failure: a
	// timeout falls through to returning success anyway, same as before
	// this wait existed, just without the improved freshness guarantee.
	n.waitForLocalApply(rf, index, 10*n.electionTimeout)
	return index, nil
}

func (n *Node) applySet(rf *raft.Raft, key, value []byte) error {
	_, err := n.applyOp(rf, kvfsm.OpSet, key, value)
	return err
}

func (n *Node) applyOp(rf *raft.Raft, op kvfsm.OpType, key, value []byte) (uint64, error) {
	cmd := kvfsm.EncodeCommand(op, key, value)
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return 0, err
	}
	if res, ok := future.Response().(kvfsm.ApplyResult); ok && res.Err != nil {
		return 0, res.Err
	}
	return future.Index(), nil
}

// waitForLocalApply blocks (up to timeout) until this node's own raft FSM
// has applied index -- see handleOpForward's doc comment for why a
// forwarding follower needs this before its own immediately-following
// local reads can be trusted to see the write it just forwarded.
// Best-effort: exceeding timeout is not reported as an error, since the
// write already succeeded on the leader by this point regardless of
// whether this node's own copy has caught up yet.
func (n *Node) waitForLocalApply(rf *raft.Raft, index uint64, timeout time.Duration) {
	if rf.AppliedIndex() >= index {
		return
	}
	deadline := time.After(timeout)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return
		case <-ticker.C:
			if rf.AppliedIndex() >= index {
				return
			}
		}
	}
}

// forwardStatusOK/forwardStatusErr are forwardOp/handleForwardSetStream's
// response framing: a single leading status byte, followed by either an
// 8-byte big-endian raft log index (OK -- the index the write was applied
// at on the leader, which the forwarding follower needs, see
// waitForLocalApply) or a UTF-8 error message (Err). Deliberately
// unambiguous about success -- see handleForwardSetStream's doc comment
// for the failure mode (a relay-adjacent stream reporting every Set as
// successful while nothing was ever persisted) this framing exists to
// rule out: decodeForwardResponse treats anything that isn't exactly this
// shape as an error, never as silent success.
const (
	forwardStatusOK  byte = 0x00
	forwardStatusErr byte = 0x01
)

// dialForward opens a stream to leaderID for one of the forward-*
// protocols below. Every forward-* function used to assume "the libp2p
// host already has an open connection/known address for leaderID -- it's
// the peer this node's own raft transport talks to for AppendEntries --
// so no address resolution is needed beyond the peer id itself", and
// dialed with a bare pid and no relay allowance. That assumption breaks
// exactly when it matters most: a leader only reachable via a relay
// circuit address (see CLAUDE.md's "Known gap" note, found running a
// real 3-node cluster) left every forward-* protocol failing with
// "context deadline exceeded" even though raft's own AppendEntries kept
// replicating fine. The difference is what rafttransport.Dial does that
// this didn't: look up a real address for the peer and allow a
// limited/relay connection for the resulting stream. dialForward closes
// that gap the same way -- pulling leaderID's address from this node's
// own current raft configuration (the same raft.ServerAddress
// rafttransport.Dial already dials successfully for AppendEntries, since
// it's this node's own up-to-date view of the cluster, not a guess) and
// seeding the peerstore with it before dialing, with
// network.WithAllowLimitedConn so a relay/transient connection is
// actually usable for the stream. If no address is on file (e.g. this
// node isn't itself a raft member yet, or a racing configuration read),
// it falls back to the old bare pid dial -- so an already-reachable peer
// never regresses.
func (n *Node) dialForward(ctx context.Context, leaderID raft.ServerID, protoID protocol.ID) (network.Stream, error) {
	pid, err := peer.Decode(string(leaderID))
	if err != nil {
		return nil, fmt.Errorf("invalid leader id %s: %w", leaderID, err)
	}

	if rf := n.getRaft(); rf != nil {
		if cfgFuture := rf.GetConfiguration(); cfgFuture.Error() == nil {
			for _, srv := range cfgFuture.Configuration().Servers {
				if srv.ID != leaderID || srv.Address == "" {
					continue
				}
				maddr, err := multiaddr.NewMultiaddr(string(srv.Address))
				if err != nil {
					break
				}
				info, err := peer.AddrInfoFromP2pAddr(maddr)
				if err != nil || info.ID != pid {
					break
				}
				n.host.Peerstore().AddAddrs(info.ID, info.Addrs, time.Hour)
				connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				n.host.Connect(connectCtx, *info) // best-effort -- NewStream below surfaces the real failure
				cancel()
				break
			}
		}
	}

	return n.host.NewStream(network.WithAllowLimitedConn(ctx, "forward"), pid, protoID)
}

// forwardOp relays op(key, value) to leaderID over ForwardProtocolID and
// returns the raft log index it was applied at -- generalizes what used
// to be a Set-only forwardSet to also carry OpAppendCommandRequest (see
// handleOpForward's doc comment). This is purely internal node-to-node
// machinery (not something a "user" ever speaks -- see pkg/shmevent's doc
// comment), so it reuses kvfsm's own log-command framing directly rather
// than pkg/shmevent's user-facing relational protocol for the request;
// see forwardStatusOK's doc comment for the response framing. See
// dialForward for how leaderID is actually reached.
func (n *Node) forwardOp(ctx context.Context, op kvfsm.OpType, leaderID raft.ServerID, key, value []byte) (uint64, error) {
	s, err := n.dialForward(ctx, leaderID, ForwardProtocolID)
	if err != nil {
		return 0, fmt.Errorf("forward set to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	cmd := kvfsm.EncodeCommand(op, key, value)
	if _, err := s.Write(cmd); err != nil {
		return 0, fmt.Errorf("forward set: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return 0, fmt.Errorf("forward set: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return 0, fmt.Errorf("forward set: read response from leader %s: %w", leaderID, err)
	}
	return decodeForwardResponse(respBuf)
}

// decodeForwardResponse parses forwardOp's response framing -- see
// forwardStatusOK's doc comment for the wire shape and why an
// unrecognized/truncated/empty buf is treated as an error rather than
// success.
func decodeForwardResponse(buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return 0, fmt.Errorf("forward set: empty response from leader")
	}
	switch buf[0] {
	case forwardStatusOK:
		if len(buf) != 9 {
			return 0, fmt.Errorf("forward set: malformed success response (%d bytes)", len(buf))
		}
		return binary.BigEndian.Uint64(buf[1:9]), nil
	case forwardStatusErr:
		return 0, fmt.Errorf("forward set: %s", buf[1:])
	default:
		return 0, fmt.Errorf("forward set: unrecognized response status byte 0x%02x", buf[0])
	}
}

// handleForwardSetStream is the leader-side handler for ForwardProtocolID:
// it decodes a kvfsm-framed command and answers it exactly like a local
// Set/OpAppendCommandRequest would, with forwarding disabled (see
// handleSetForward's allowForward doc). See forwardStatusOK's doc comment
// for the response wire format -- every early return here must write a
// well-formed error frame, never just close the stream: an empty response
// from a read/decode failure would otherwise be indistinguishable from
// genuine success, silently dropping the write while the forwarding
// follower reports it as applied. Found exactly this way -- a follower
// over a relay-adjacent connection (a phone) reporting every Set as
// successful while nothing was ever persisted, anywhere.
// OpAppendCommandRequest is accepted here (not just OpSet) for the same
// reason handleOpForward's doc comment gives: its own ACL check runs
// raft-authoritatively inside kvfsm.Apply, so this hop needs no separate
// sender-is-a-voter gate the way OpConfirm/OpDel/OpCascadeDelete do.
func (n *Node) handleForwardSetStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		writeForwardError(s, fmt.Errorf("forward set: read command: %w", err))
		return
	}
	op, key, value, err := kvfsm.DecodeCommand(buf)
	if err != nil {
		writeForwardError(s, fmt.Errorf("forward set: decode command: %w", err))
		return
	}
	if op != kvfsm.OpSet && op != kvfsm.OpAppendCommandRequest {
		writeForwardError(s, fmt.Errorf("forward set: expected OpSet or OpAppendCommandRequest, got op %d", op))
		return
	}

	index, err := n.handleOpForward(context.Background(), op, key, value, false)
	if err != nil {
		writeForwardError(s, err)
		return
	}
	writeForwardSuccess(s, index)
}

// writeForwardError/writeForwardSuccess write forwardStatusOK/
// forwardStatusErr-framed responses -- see forwardStatusOK's doc comment.
// Errors from s.Write itself are deliberately ignored: the stream is
// already being torn down (deferred s.Close() in the caller) and there's
// no further response channel to report a write failure through.
func writeForwardError(s network.Stream, err error) {
	s.Write(append([]byte{forwardStatusErr}, []byte(err.Error())...))
}

func writeForwardSuccess(s network.Stream, index uint64) {
	buf := make([]byte, 9)
	buf[0] = forwardStatusOK
	binary.BigEndian.PutUint64(buf[1:], index)
	s.Write(buf)
}

// handleConfirmForward is EventPermitConfirm/EventPermitRevoke's shared
// counterpart to handleSetForward -- and, since it's exactly "apply
// directly if I'm the voter-implying leader, else forward to a voter-
// checked leader," also the group-based ACL catalog's Put/Delete events'
// (see shmevent.KindGroup's doc comment): applies directly if this node
// is the leader, or forwards to the leader (one hop only, same
// allowForward-guarded pattern) if not. op is kvfsm.OpConfirm (key1 = the
// pending record's key, key2 = the confirmed record's key it's promoted
// to), kvfsm.OpDel (key1 = the confirmed record's key being revoked/
// deleted outright, key2 unused), kvfsm.OpSet (key1 = key, key2 = value,
// a direct single-step write -- used by EventGroupPut et al., never by
// EventPermitConfirm/EventPermitRevoke), or kvfsm.OpCascadeDelete (key1 =
// the Group/Command record's own key, key2 unused -- see kvfsm.Apply's
// respective cases). When this node *is* the leader, no separate voter
// check is needed here -- hashicorp/raft guarantees only a Voter can ever
// hold leader state, so isLeader==true already implies the caller is a
// voter. The forwarded path's voter check happens in
// handleForwardConfirmStream instead, against the authenticated identity
// of whichever node actually opened the stream -- it applies identically
// regardless of which op is being forwarded.
func (n *Node) handleConfirmForward(ctx context.Context, op kvfsm.OpType, key1, key2 []byte, allowForward bool) error {
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		return n.applyConfirm(ctx, rf, op, key1, key2)
	}
	if !allowForward {
		return fmt.Errorf("not leader; current leader is %s (already forwarded once)", leaderID)
	}
	return n.forwardConfirm(ctx, leaderID, op, key1, key2)
}

func (n *Node) applyConfirm(ctx context.Context, rf *raft.Raft, op kvfsm.OpType, key1, key2 []byte) error {
	cmd := kvfsm.EncodeCommand(op, key1, key2)
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	if res, ok := future.Response().(kvfsm.ApplyResult); ok && res.Err != nil {
		return res.Err
	}
	if op == kvfsm.OpConfirm {
		if err := n.admitClusterJoinIfConfirmed(ctx, rf, key2); err != nil {
			return err
		}
	}
	return nil
}

// admitClusterJoinIfConfirmed is applyConfirm's one special case: every
// other kind's OpConfirm is just a replicated status flip (see
// kvfsm.Apply's OpConfirm case), but promoting a pending
// shmevent.KindClusterJoin record to confirmed must also actually admit
// the joining peer into the raft configuration -- this is the only place
// EventPermitConfirm's workflow ever triggers raft.AddVoter/AddNonvoter,
// as opposed to handleJoinStream's immediate (Config.RequireConfirmForJoin
// == false) path. No-op for every other kind. Called only from
// applyConfirm, always with rf already established as this node's own
// *leader* raft handle (guaranteed by applyConfirm's two call sites in
// handleConfirmForward), so addServerLine can run directly here with no
// further leader resolution or forwarding needed.
func (n *Node) admitClusterJoinIfConfirmed(ctx context.Context, rf *raft.Raft, confirmedKey []byte) error {
	if len(confirmedKey) < 3 || confirmedKey[0] != shmevent.SystemKeyPrefix || confirmedKey[1] != shmevent.KindClusterJoin {
		return nil
	}
	peerID := string(confirmedKey[3:])
	value, err := n.store.Get(confirmedKey)
	if err != nil {
		return fmt.Errorf("cluster join: read confirmed record for %s: %w", peerID, err)
	}
	addr, sf, err := shmevent.DecodeClusterJoinMetadata(value)
	if err != nil {
		return fmt.Errorf("cluster join: decode confirmed record for %s: %w", peerID, err)
	}
	suffrage := raft.Voter
	if sf == shmevent.SuffrageLearner {
		suffrage = raft.Nonvoter
	}
	if line := n.addServerLine(ctx, rf, peerID, addr, suffrage); strings.HasPrefix(line, "ERR:") {
		return fmt.Errorf("cluster join: %s", strings.TrimPrefix(line, "ERR: "))
	}
	return nil
}

// forwardConfirm relays op(key1, key2) to leaderID over
// ForwardConfirmProtocolID, mirroring forwardSet's wire convention
// exactly (kvfsm's own command framing; empty response = success,
// non-empty = the leader's error message). See dialForward for how
// leaderID is actually reached.
func (n *Node) forwardConfirm(ctx context.Context, leaderID raft.ServerID, op kvfsm.OpType, key1, key2 []byte) error {
	s, err := n.dialForward(ctx, leaderID, ForwardConfirmProtocolID)
	if err != nil {
		return fmt.Errorf("forward confirm to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	cmd := kvfsm.EncodeCommand(op, key1, key2)
	if _, err := s.Write(cmd); err != nil {
		return fmt.Errorf("forward confirm: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward confirm: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward confirm: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward confirm: %s", respBuf)
	}
	return nil
}

// handleForwardConfirmStream is the leader-side handler for
// ForwardConfirmProtocolID, shared by EventPermitConfirm (OpConfirm),
// EventPermitRevoke (OpDel), and -- reusing this same machinery wholesale
// rather than building a parallel set of protocols/handlers -- the
// group-based ACL catalog's single-step Put (OpSet)/Delete (OpDel)/
// cascading Delete (OpCascadeDelete) events (EventGroupPut,
// EventGroupCommandDelete, etc.; see shmevent.KindGroup's doc comment).
// Unlike handleForwardSetStream, it checks the stream's
// libp2p-authenticated remote peer -- s.Conn().RemotePeer(), established
// by the connection's own handshake and so unforgeable by whatever a
// caller puts in the message itself -- against the leader's live raft
// configuration before applying anything, rejecting unless that peer is
// currently a Voter. This is the actual enforcement of "only a raft voter
// may confirm/revoke/put/delete" for every op it accepts: the generic
// per-message Ed25519 signature check every event type already gets (see
// handleShmEvent) only proves the message wasn't corrupted and was signed
// with whoever's key it was checked against -- for local same-machine
// shmring IPC that's inherently this same node's own key (see
// pkg/shmevent's doc comment), which doesn't by itself say anything about
// cluster membership. The RemotePeer check here is what does, uniformly
// for every op.
func (n *Node) handleForwardConfirmStream(s network.Stream) {
	defer s.Close()

	remote := s.Conn().RemotePeer()
	rf := n.getRaft()
	if rf == nil || !isVoter(rf, raft.ServerID(remote.String())) {
		fmt.Fprintf(s, "forward confirm: %s is not a current raft voter", remote)
		return
	}

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "forward confirm: read command: %v", err)
		return
	}
	op, key1, key2, err := kvfsm.DecodeCommand(buf)
	if err != nil {
		fmt.Fprintf(s, "forward confirm: decode command: %v", err)
		return
	}
	if op != kvfsm.OpConfirm && op != kvfsm.OpDel && op != kvfsm.OpSet && op != kvfsm.OpCascadeDelete {
		fmt.Fprintf(s, "forward confirm: unsupported op %d", op)
		return
	}

	if err := n.handleConfirmForward(context.Background(), op, key1, key2, false); err != nil {
		s.Write([]byte(err.Error()))
	}
}

// isVoter reports whether id is currently a Voter in rf's configuration.
func isVoter(rf *raft.Raft, id raft.ServerID) bool {
	cfg := rf.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return false
	}
	for _, srv := range cfg.Configuration().Servers {
		if srv.ID == id && srv.Suffrage == raft.Voter {
			return true
		}
	}
	return false
}

// removeServerLine runs raft.RemoveServer for peerID and returns the
// response line: "OK" or "ERR: <reason>" -- addServerLine's counterpart
// for leaving instead of joining. Every call site already only reaches
// this once rf.State()==Leader is confirmed. On success it also deletes
// peerID's KindClusterMember record (reusing applyConfirm's OpDel path,
// the same one EventPermitRevoke uses) -- membership no longer holds, so
// the mirror shouldn't keep claiming it does. Shrinking the configuration
// this way is graceful: the remaining voters keep operating normally,
// exactly like hashicorp/raft already tolerates any minority of members
// being unreachable.
func (n *Node) removeServerLine(ctx context.Context, rf *raft.Raft, peerID string) string {
	future := rf.RemoveServer(raft.ServerID(peerID), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}
	if err := n.applyConfirm(ctx, rf, kvfsm.OpDel, shmevent.ClusterMemberKey([]byte(peerID)), nil); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: remove cluster member record %s: %v\n", peerID, err)
	}
	return "OK"
}

// leaveCluster asks the raft cluster this node currently belongs to
// (identified purely by this node's own live raft handle -- there's
// nothing else to specify) to remove it via raft.RemoveServer: applies
// directly if this node is already the leader, or forwards one hop over
// ForwardLeaveProtocolID otherwise. Mirrors handleConfirmForward's shape
// rather than join's public-facing JoinProtocolID dance, since a leaving
// node -- unlike a brand new joiner -- is already a member with its own
// live raft handle and leader-tracking; it never needs to ask an
// arbitrary stranger node to introduce it.
func (n *Node) leaveCluster(ctx context.Context) error {
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		if line := n.removeServerLine(ctx, rf, n.peerID); strings.HasPrefix(line, "ERR:") {
			return fmt.Errorf("%s", strings.TrimPrefix(line, "ERR: "))
		}
		return nil
	}
	return n.forwardLeave(ctx, leaderID)
}

// forwardLeave relays a leave request to leaderID over
// ForwardLeaveProtocolID, mirroring forwardConfirm's wire convention
// (empty response = success, non-empty = the leader's error message) --
// but with no payload at all: the peer to remove is whichever identity
// the stream itself authenticates as, established by
// handleForwardLeaveStream reading s.Conn().RemotePeer(), not anything
// this side writes.
func (n *Node) forwardLeave(ctx context.Context, leaderID raft.ServerID) error {
	s, err := n.dialForward(ctx, leaderID, ForwardLeaveProtocolID)
	if err != nil {
		return fmt.Errorf("forward leave to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward leave: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward leave: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward leave: %s", respBuf)
	}
	return nil
}

// handleForwardLeaveStream is the leader-side handler for
// ForwardLeaveProtocolID: it removes whichever peer the stream's own
// libp2p-authenticated connection identity names -- s.Conn().RemotePeer(),
// unforgeable by anything a caller could put in a payload -- from the
// raft configuration. There is no payload to parse at all: unlike
// handleForwardConfirmStream (which acts on an arbitrary peerID named in
// the forwarded command, restricted to voters only), a leave request can
// only ever remove the requester itself.
func (n *Node) handleForwardLeaveStream(s network.Stream) {
	defer s.Close()

	remote := s.Conn().RemotePeer()
	rf := n.getRaft()
	if rf == nil || rf.State() != raft.Leader {
		var leaderID raft.ServerID
		if rf != nil {
			_, leaderID = rf.LeaderWithID()
		}
		fmt.Fprintf(s, "not leader; current leader is %s (already forwarded once)", leaderID)
		return
	}
	if line := n.removeServerLine(context.Background(), rf, remote.String()); strings.HasPrefix(line, "ERR:") {
		s.Write([]byte(strings.TrimPrefix(line, "ERR: ")))
	}
}

// kickPeer implements EventKick (see that event's doc comment): removes
// targetPeerID from the raft cluster this node currently belongs to via
// raft.RemoveServer, applying directly if this node is already the
// leader, or forwarding one hop over ForwardKickProtocolID otherwise --
// leaveCluster's shape exactly, generalized from "remove myself" to
// "remove whoever the caller names." Authorization (only a raft voter may
// call this) is checked by handleShmEvent before this is ever reached for
// a remote caller, and re-checked at the forwarding hop by
// handleForwardKickStream -- kickPeer itself does no identity check, the
// same division of responsibility handleConfirmForward/leaveCluster
// already have.
func (n *Node) kickPeer(ctx context.Context, targetPeerID string) error {
	if targetPeerID == "" {
		return fmt.Errorf("kick: missing target peer id")
	}
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		if line := n.removeServerLine(ctx, rf, targetPeerID); strings.HasPrefix(line, "ERR:") {
			return fmt.Errorf("%s", strings.TrimPrefix(line, "ERR: "))
		}
		return nil
	}
	return n.forwardKick(ctx, leaderID, targetPeerID)
}

// forwardKick relays a kick request for targetPeerID to leaderID over
// ForwardKickProtocolID, mirroring forwardLeave/forwardConfirm's wire
// convention (empty response = success, non-empty = the leader's error
// message) -- unlike forwardLeave, targetPeerID genuinely is the payload
// here (see ForwardKickProtocolID's doc comment for why).
func (n *Node) forwardKick(ctx context.Context, leaderID raft.ServerID, targetPeerID string) error {
	s, err := n.dialForward(ctx, leaderID, ForwardKickProtocolID)
	if err != nil {
		return fmt.Errorf("forward kick to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	if _, err := s.Write([]byte(targetPeerID)); err != nil {
		return fmt.Errorf("forward kick: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward kick: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward kick: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward kick: %s", respBuf)
	}
	return nil
}

// handleForwardKickStream is the leader-side handler for
// ForwardKickProtocolID: it reads the target peer id to remove from the
// stream body (unlike handleForwardLeaveStream, which has no payload at
// all -- see ForwardKickProtocolID's doc comment), checks the *forwarding*
// peer's own libp2p-authenticated identity against the leader's voter
// list (handleForwardConfirmStream's same reasoning: the per-message
// signature check alone doesn't establish cluster membership), and
// removes targetPeerID via removeServerLine if authorized.
func (n *Node) handleForwardKickStream(s network.Stream) {
	defer s.Close()

	remote := s.Conn().RemotePeer()
	rf := n.getRaft()
	if rf == nil || !isVoter(rf, raft.ServerID(remote.String())) {
		fmt.Fprintf(s, "forward kick: %s is not a current raft voter", remote)
		return
	}
	if rf.State() != raft.Leader {
		_, leaderID := rf.LeaderWithID()
		fmt.Fprintf(s, "not leader; current leader is %s (already forwarded once)", leaderID)
		return
	}

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "forward kick: read target: %v", err)
		return
	}
	targetPeerID := string(buf)
	if targetPeerID == "" {
		fmt.Fprintf(s, "forward kick: missing target peer id")
		return
	}
	if line := n.removeServerLine(context.Background(), rf, targetPeerID); strings.HasPrefix(line, "ERR:") {
		s.Write([]byte(strings.TrimPrefix(line, "ERR: ")))
	}
}

// dispatchExecute implements EventExecute (see that event's doc comment
// in pkg/shmevent): resolves SourceID/DestinationID against this node's
// own registry, confirms the caller isn't claiming some other node as the
// sender (this node can only ever relay the peer-to-peer hop below under
// its own identity, since that's the key it signs with), then delivers
// it. Never touches n.store or raft.
func (n *Node) dispatchExecute(ctx context.Context, m shmevent.Msg) error {
	senderKey, ok := n.registry.Lookup(m.SourceID)
	if !ok {
		return fmt.Errorf("execute: no peer id registered under source id %d -- send SetKey first", m.SourceID)
	}
	if string(senderKey) != n.peerID {
		return fmt.Errorf("execute: source %q is not this node's own peer id (%s)", senderKey, n.peerID)
	}
	destKey, ok := n.registry.Lookup(m.DestinationID)
	if !ok {
		return fmt.Errorf("execute: no peer id registered under destination id %d -- send SetKey first", m.DestinationID)
	}
	destPeerID, err := peer.Decode(string(destKey))
	if err != nil {
		return fmt.Errorf("execute: invalid destination peer id %q: %w", destKey, err)
	}
	return n.sendExecute(ctx, destPeerID, m.Value)
}

// sendExecute dials dest directly over ExecuteProtocolID -- a fresh
// peer-to-peer libp2p stream between two raft node processes, entirely
// outside raft consensus -- and hands it an EventExecute message carrying
// EncodeExecuteNotification(this node's own peer id, payload), signed
// with this node's own key. See handleExecuteStream for the receiving
// side.
func (n *Node) sendExecute(ctx context.Context, dest peer.ID, payload []byte) error {
	s, err := n.host.NewStream(ctx, dest, ExecuteProtocolID)
	if err != nil {
		return fmt.Errorf("execute: open stream to %s: %w", dest, err)
	}
	defer s.Close()

	value, err := shmevent.EncodeExecuteNotification([]byte(n.peerID), payload)
	if err != nil {
		return fmt.Errorf("execute: encode notification: %w", err)
	}
	buf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventExecute, Value: value}, n.ed25519Priv)
	if err != nil {
		return fmt.Errorf("execute: encode message: %w", err)
	}
	if _, err := s.Write(buf); err != nil {
		return fmt.Errorf("execute: write to %s: %w", dest, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("execute: close write to %s: %w", dest, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("execute: read response from %s: %w", dest, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("execute: %s", respBuf)
	}
	return nil
}

// handleExecuteStream is the receiving side of ExecuteProtocolID: it
// decodes the message (Decode itself checks crc32), extracts the claimed
// sender peer id from the notification payload (see
// EncodeExecuteNotification), and verifies the signature against *that*
// peer id's own Ed25519 public key -- embedded in the peer id itself for
// this project's identities, the same extraction
// pkg/daemon.recordClusterMember uses -- rather than trusting whichever
// address dialed in. That's what makes the signature self-contained
// (matches EventExecute's doc comment: authenticity doesn't depend on the
// stream's own connection identity), unlike handleForwardConfirmStream's
// check, which deliberately does the opposite for a different reason (see
// its own doc comment). Once the signature checks out, applies
// Config.RequirePermitForExecute's gate (a read-only n.store lookup, if
// enabled) against that same verified sender peer id, then queues the
// notification for EventPollExecute. Never writes to n.store or raft
// either way.
func (n *Node) handleExecuteStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "execute: read: %v", err)
		return
	}
	m, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		fmt.Fprintf(s, "execute: decode: %v", err)
		return
	}
	if m.EventType != shmevent.EventExecute {
		fmt.Fprintf(s, "execute: expected EventExecute, got %s", shmevent.EventName(m.EventType))
		return
	}
	senderPeerID, payload, err := shmevent.DecodeExecuteNotification(m.Value)
	if err != nil {
		fmt.Fprintf(s, "execute: decode notification: %v", err)
		return
	}
	senderPeer, err := peer.Decode(string(senderPeerID))
	if err != nil {
		fmt.Fprintf(s, "execute: invalid sender peer id %q: %v", senderPeerID, err)
		return
	}
	senderPub, err := senderPeer.ExtractPublicKey()
	if err != nil {
		fmt.Fprintf(s, "execute: extract sender public key: %v", err)
		return
	}
	rawSenderPub, err := senderPub.Raw()
	if err != nil {
		fmt.Fprintf(s, "execute: sender public key raw bytes: %v", err)
		return
	}
	if err := shmevent.Verify(shmevent.PublicKey(rawSenderPub), m, crc, sig); err != nil {
		fmt.Fprintf(s, "execute: %v", err)
		return
	}
	// Authorization, checked only now that senderPeer is proven authentic
	// (see this function's doc comment on why that's the peer id this
	// gate must check, never s.Conn().RemotePeer()) -- see
	// Config.RequirePermitForExecute's doc comment.
	if n.cfg.RequirePermitForExecute && !n.isClusterMember(senderPeer) && !n.isPermittedPeer(senderPeer) {
		fmt.Fprintf(s, "execute: %s is not a raft cluster member or a permitted peer", senderPeer)
		return
	}
	n.executeInbox.push(senderPeerID, payload)
}

// maxFramedMessage caps readFramed's length prefix before allocating, so
// a peer can't claim an enormous length and force a large allocation.
const maxFramedMessage = 4096

// writeFramed writes a 4-byte big-endian length prefix followed by buf --
// ChannelProtocolID's handshake framing (see that protocol's doc
// comment): every other stream protocol in this file writes one message
// and half-closes, relying on EOF to mark the end (see e.g.
// handleExecuteStream's io.ReadAll), which doesn't work here since the
// stream must stay open afterward carrying raw, unframed bytes.
func writeFramed(s network.Stream, buf []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(buf)))
	if _, err := s.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := s.Write(buf)
	return err
}

// readFramed is the inverse of writeFramed.
func readFramed(s network.Stream) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(s, lenBuf[:]); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf[:])
	if msgLen > maxFramedMessage {
		return nil, fmt.Errorf("channel: framed message too large: %d bytes", msgLen)
	}
	buf := make([]byte, msgLen)
	if _, err := io.ReadFull(s, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// channelAccepted/channelRejected are ChannelProtocolID's second
// handshake frame's status byte -- see writeChannelAccept.
const (
	channelAccepted byte = 0x00
	channelRejected byte = 0x01
)

// writeChannelAccept writes ChannelProtocolID's accept/reject frame: a
// single status byte, then -- only when rejected -- a UTF-8 reason, with
// no length prefix (safe, since the accepter always closes the stream
// immediately after writing a rejection, and an acceptance is never
// followed by anything readChannelAccept would need to distinguish from
// a reason).
func writeChannelAccept(s network.Stream, accepted bool, reason string) error {
	if accepted {
		_, err := s.Write([]byte{channelAccepted})
		return err
	}
	buf := append([]byte{channelRejected}, []byte(reason)...)
	_, err := s.Write(buf)
	return err
}

// readChannelAccept is the inverse of writeChannelAccept.
func readChannelAccept(s network.Stream) (status byte, reason string, err error) {
	var statusBuf [1]byte
	if _, err := io.ReadFull(s, statusBuf[:]); err != nil {
		return 0, "", err
	}
	if statusBuf[0] != channelRejected {
		return statusBuf[0], "", nil
	}
	reasonBuf, err := io.ReadAll(s)
	if err != nil {
		return 0, "", err
	}
	return statusBuf[0], string(reasonBuf), nil
}

// dispatchChannelOpen implements EventChannelOpen: dials destPeerIDStr
// over ChannelProtocolID, performs the signed handshake (mirroring
// sendExecute/handleExecuteStream's self-contained-signature design --
// see ChannelProtocolID's doc comment), and on acceptance registers a
// new live channelSession backed by the resulting stream and starts its
// read-pump goroutine. Returns the freshly minted local channelID.
func (n *Node) dispatchChannelOpen(ctx context.Context, destPeerIDStr string) (string, error) {
	// See handleChannelStream's identical call for why this belongs here
	// too -- this is the only one of channelTable.reap's four call sites
	// that was missing it, leaving a node that mostly *originates*
	// channels (and so never itself calls handleChannelStream/
	// dispatchChannelPoll/dispatchChannelListen) with no path of its own
	// that ever reaps its idle/stale sessions.
	n.channels.reap()

	dest, err := peer.Decode(destPeerIDStr)
	if err != nil {
		return "", fmt.Errorf("channel: invalid destination peer id %q: %w", destPeerIDStr, err)
	}
	s, err := n.host.NewStream(ctx, dest, ChannelProtocolID)
	if err != nil {
		return "", fmt.Errorf("channel: open stream to %s: %w", dest, err)
	}
	// Bounds the handshake below (write + read accept) the same way
	// withStreamRequestDeadline bounds every SetStreamHandler-registered
	// handler -- this dial-out path isn't wrapped by that helper, so it
	// needs the same protection set explicitly. Cleared once the
	// handshake succeeds, before handing the stream to pumpChannelReads'
	// intentionally long-lived read loop -- see streamRequestTimeout's
	// doc comment.
	_ = s.SetDeadline(time.Now().Add(streamRequestTimeout))

	notifValue, err := shmevent.EncodeExecuteNotification([]byte(n.peerID), nil)
	if err != nil {
		s.Close()
		return "", fmt.Errorf("channel: encode handshake: %w", err)
	}
	buf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: notifValue}, n.ed25519Priv)
	if err != nil {
		s.Close()
		return "", fmt.Errorf("channel: encode handshake message: %w", err)
	}
	if err := writeFramed(s, buf); err != nil {
		s.Close()
		return "", fmt.Errorf("channel: write handshake to %s: %w", dest, err)
	}

	status, reason, err := readChannelAccept(s)
	if err != nil {
		s.Close()
		return "", fmt.Errorf("channel: read accept from %s: %w", dest, err)
	}
	if status != channelAccepted {
		s.Close()
		return "", fmt.Errorf("channel: %s rejected: %s", dest, reason)
	}

	channelID, err := newChannelID()
	if err != nil {
		s.Close()
		return "", err
	}
	_ = s.SetDeadline(time.Time{}) // handshake done -- see the SetDeadline call above
	sess := newChannelSession(s, dest.String())
	n.channels.register(channelID, sess)
	go n.pumpChannelReads(sess)
	return channelID, nil
}

// handleChannelStream is the receiving side of ChannelProtocolID: reads
// the framed handshake (readFramed), verifies its self-contained
// signature against the claimed sender peer id exactly the way
// handleExecuteStream does (never s.Conn().RemotePeer() -- see that
// function's doc comment for why), applies Config.RequirePermitForChannel's
// gate, writes back a one-byte accept/reject (writeChannelAccept), and on
// acceptance registers a new channelSession, pushes it onto the pending
// queue for EventChannelListen to claim, and starts its read-pump
// goroutine. Unlike every other stream handler in this file, it does NOT
// close the stream on the accept path -- the stream is handed off to the
// channelSession and stays open for that channel's whole lifetime, closed
// later by EventChannelClose or channelTable.reap.
func (n *Node) handleChannelStream(s network.Stream) {
	buf, err := readFramed(s)
	if err != nil {
		s.Close()
		return
	}
	m, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		s.Close()
		return
	}
	if m.EventType != shmevent.EventChannelOpen {
		s.Close()
		return
	}
	senderPeerID, _, err := shmevent.DecodeExecuteNotification(m.Value)
	if err != nil {
		s.Close()
		return
	}
	senderPeer, err := peer.Decode(string(senderPeerID))
	if err != nil {
		s.Close()
		return
	}
	senderPub, err := senderPeer.ExtractPublicKey()
	if err != nil {
		s.Close()
		return
	}
	rawSenderPub, err := senderPub.Raw()
	if err != nil {
		s.Close()
		return
	}
	if err := shmevent.Verify(shmevent.PublicKey(rawSenderPub), m, crc, sig); err != nil {
		s.Close()
		return
	}
	// Authorization, checked only now that senderPeer is proven authentic
	// -- see handleExecuteStream's identically-shaped comment for why
	// this must be the verified senderPeer, never s.Conn().RemotePeer(),
	// and Config.RequirePermitForChannel's own doc comment.
	if n.cfg.RequirePermitForChannel && !n.isClusterMember(senderPeer) && !n.isPermittedPeer(senderPeer) {
		writeChannelAccept(s, false, fmt.Sprintf("%s is not a raft cluster member or a permitted peer", senderPeer))
		s.Close()
		return
	}

	n.channels.reap()
	channelID, err := newChannelID()
	if err != nil {
		writeChannelAccept(s, false, "internal error minting channel id")
		s.Close()
		return
	}
	if err := writeChannelAccept(s, true, ""); err != nil {
		s.Close()
		return
	}
	// Handshake done -- withStreamRequestDeadline's deadline (set when
	// this handler was entered) must not keep applying to
	// pumpChannelReads' intentionally long-lived read loop below. See
	// streamRequestTimeout's doc comment.
	_ = s.SetDeadline(time.Time{})
	sess := newChannelSession(s, senderPeer.String())
	n.channels.register(channelID, sess)
	n.channels.pushPending(channelID)
	go n.pumpChannelReads(sess)
}

// pumpChannelReads is sess's background read pump: copies bytes off
// sess.stream into sess's inbox for EventChannelPoll to drain, until the
// stream errors/EOFs (the peer closed their write side, or the
// connection dropped) or this node's own EventChannelClose/the reaper
// closes it first -- either way, stream.Read then returns and this marks
// the session closed rather than removing it outright, so any chunks
// already buffered are still readable via a final poll.
func (n *Node) pumpChannelReads(sess *channelSession) {
	buf := make([]byte, channelReadBufferSize)
	for {
		nRead, err := sess.stream.Read(buf)
		if nRead > 0 {
			chunk := make([]byte, nRead)
			copy(chunk, buf[:nRead])
			sess.pushChunk(chunk)
		}
		if err != nil {
			reason := ""
			if err != io.EOF {
				reason = err.Error()
			}
			sess.markClosed(reason)
			return
		}
	}
}

// dispatchChannelSend implements EventChannelSend: writes chunk directly
// to channelID's stream, with no framing. Deliberately does not
// pre-check channelSession.status()'s closed flag: that flag tracks
// whether *this node's own read side* has hit EOF (the peer half-closed
// or fully closed their outgoing direction -- see EventChannelCloseWrite),
// which says nothing about whether writing is still valid -- half-close
// is directional. If this node's own outgoing direction has itself been
// closed (EventChannelCloseWrite, or a full EventChannelClose), the
// underlying stream.Write below fails on its own, which is what actually
// gates this.
func (n *Node) dispatchChannelSend(channelID string, chunk []byte) error {
	sess, ok := n.channels.get(channelID)
	if !ok {
		return fmt.Errorf("channel: no such channel %q", channelID)
	}
	return sess.write(chunk)
}

// dispatchChannelPoll implements EventChannelPoll: pops the oldest
// buffered chunk from channelID's inbox, if any -- see
// shmevent.EncodeChannelPollResponse's status byte for the three-way
// result.
func (n *Node) dispatchChannelPoll(channelID string) ([]byte, error) {
	n.channels.reap()
	sess, ok := n.channels.get(channelID)
	if !ok {
		return nil, fmt.Errorf("channel: no such channel %q", channelID)
	}
	if chunk, ok := sess.popChunk(); ok {
		return shmevent.EncodeChannelPollResponse(shmevent.ChannelPollChunk, chunk), nil
	}
	if closed, _ := sess.status(); closed {
		return shmevent.EncodeChannelPollResponse(shmevent.ChannelPollClosed, nil), nil
	}
	return shmevent.EncodeChannelPollResponse(shmevent.ChannelPollNoData, nil), nil
}

// dispatchChannelListen implements EventChannelListen: claims the oldest
// pending (accepted-but-unclaimed) incoming channel, if any. A nil,nil
// return (empty response) means none pending yet -- a local caller polls
// this in a loop, exactly EventPollExecute's documented convention.
func (n *Node) dispatchChannelListen() ([]byte, error) {
	n.channels.reap()
	channelID, ok := n.channels.popPending()
	if !ok {
		return nil, nil
	}
	sess, ok := n.channels.get(channelID)
	if !ok {
		// Reaped between popPending and here -- vanishingly unlikely, but
		// safe to treat the same as "nothing pending yet."
		return nil, nil
	}
	return shmevent.EncodeChannelAccept(channelID, sess.remotePeerID)
}

// dispatchChannelClose implements EventChannelClose: closes channelID's
// stream (unblocking pumpChannelReads' blocking Read, same idiom as
// every other stream's defer s.Close() in this file) and forgets the
// session. Idempotent -- closing an already-gone or never-existed
// channelID is not an error.
func (n *Node) dispatchChannelClose(channelID string) error {
	sess, ok := n.channels.get(channelID)
	if !ok {
		return nil
	}
	sess.stream.Close()
	n.channels.remove(channelID)
	return nil
}

// dispatchChannelCloseWrite implements EventChannelCloseWrite: half-closes
// channelID's outgoing direction only, leaving the session registered
// (still pollable/receivable) -- see that event's doc comment. A no-op,
// not an error, if channelID is already gone.
func (n *Node) dispatchChannelCloseWrite(channelID string) error {
	sess, ok := n.channels.get(channelID)
	if !ok {
		return nil
	}
	return sess.closeWrite()
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
		respBuf, encErr := shmevent.Encode(errorMsg(m.ID, err), n.ed25519Priv)
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
