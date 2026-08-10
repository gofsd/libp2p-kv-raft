package daemon

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

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

// channelMaxChunkSize bounds how many bytes a single channel chunk may
// carry over the network, in both directions -- independent of (and much
// larger than) shmevent.ChannelValueSize, which only bounds
// EventChannelSend/Poll's own IPC payload on the legacy per-chunk-round-
// trip path (see that constant's doc comment). The primary path -- a local
// caller's pkg/chandata data-plane ring, drained by pumpChannelUpload -- is
// capped at chandata.MaxChunkSize instead (see that constant's doc comment
// on why bigger chunks matter for throughput), and this is set equal to
// it so a chunk that fits through the ring always fits in one wire frame
// with no further splitting. Under ChannelProtocolID's per-message-framed
// design (see that constant's own doc comment), each network read is
// already exactly one signed frame -- there is no raw-byte coalescing left
// to guard against, unlike this package's earlier raw-pipe design -- but the
// cap is still enforced in both directions rather than assumed:
// dispatchChannelSend/pumpChannelUpload reject an oversized chunk before
// it's ever written, and pumpChannelReads closes the session if a peer's
// frame exceeds it -- a peer isn't bound by this package's own Encode-time
// checks and could otherwise hand-craft a frame up to maxFramedMessage
// directly.
const channelMaxChunkSize = chandata.MaxChunkSize

// maxPollChunkSize is the largest chunk channelPoll can hand back over
// local IPC: pkg/ipc's shared-memory ring capacity (16-32KB depending on
// transport) bounds the whole encoded response, not just the chunk field
// itself, so this stays comfortably under that regardless of the small
// fixed overhead (status/purpose bytes, capnp framing, CRC32, signature)
// every response also carries.
//
// It is far below channelMaxChunkSize, and that gap is real rather than
// theoretical: the data plane exists to move 256KB chunks, every one of
// which pumpChannelReads also buffers for this poll path. See
// dispatchChannelPoll for what happens to a buffered chunk that cannot fit
// here, and why it is reported rather than silently swallowed.
const maxPollChunkSize = 16*1024 - 2

// channelChunk is one entry in channelSession.inbox -- a purpose-tagged
// chunk pumpChannelReads has already verified and unwrapped from one
// signed network frame (see ChannelProtocolID's doc comment). Unlike this
// package's earlier raw-pipe design, one channelChunk always corresponds
// to exactly one wire frame -- never a coalesced or split read.
type channelChunk struct {
	purpose byte
	data    []byte
}

// channelSession is one live EventChannelOpen/handleChannelStream
// session (see ChannelProtocolID's doc comment for the wire design):
// stream carries a signed shmevent.Event frame per message in both
// directions once the initial handshake completes. remotePub is the
// remote peer's Ed25519 public key (derived from its peer id, the same
// way the handshake itself verifies -- see dispatchChannelOpen/
// handleChannelStream, both of which already have what's needed to
// compute this at session-creation time), used by pumpChannelReads to
// verify every subsequent frame, not just the handshake. inbox buffers
// chunks pumpChannelReads has already read, verified and unwrapped off
// stream, for the legacy EventChannelPoll path to drain -- down (see
// below) is the primary path new callers use instead. writeMu serializes
// writes to stream: dispatchChannelSend's legacy per-chunk IPC path and
// pumpChannelUpload's ring-drain path (see pkg/chandata's doc comment)
// both call write, potentially from different goroutines, unlike before
// when only ever one local caller wrote at a time.
type channelSession struct {
	stream       network.Stream
	remotePeerID string
	remotePub    shmevent.PublicKey
	writeMu      sync.Mutex

	// remoteIP is the remote peer's IP address at the time this session's
	// stream was opened (extractRemoteIP(s.Conn().RemoteMultiaddr())),
	// "" if unresolvable -- quota's IP-bucket key for both directions of
	// this session's traffic. quota is n.channelQuota, threaded through at
	// construction rather than read off a *Node here, since a
	// channelSession outlives any single dispatch call and the receiving/
	// initiating call sites (handleChannelStream/dispatchChannelOpen) both
	// already have n in hand.
	remoteIP string
	quota    *quotaTracker

	// channelID duplicates channelTable's own map key on the session
	// itself, purely so pumpChannelReads/pumpChannelUpload/
	// dispatchChannelDataReady (all of which already have a *channelSession
	// in hand) can name this channel's chandata rings without a second
	// parameter threaded through every call site.
	channelID string

	// closeCtx/closeCancel bound every chandata call this session's
	// goroutines make (WriteChunk/ReadChunk/Open), so tearing the channel
	// down (dispatchChannelClose, channelTable.reap) promptly unblocks
	// them instead of leaving them waiting on a ring that will never see
	// further activity.
	closeCtx    context.Context
	closeCancel context.CancelFunc

	// down is this node's own outgoing data-plane ring toward the local
	// caller (pkg/chandata.DirDown) -- created synchronously before this
	// channelID is ever handed back to any caller (dispatchChannelOpen/
	// handleChannelStream), so it always exists by the time a local
	// caller could possibly go looking for it. Written to only by
	// pumpChannelReads (a single goroutine), which also owns closing it.
	down *chandata.ChunkWriter

	mu           sync.Mutex
	inbox        []channelChunk
	closed       bool
	closeReason  string
	lastActivity time.Time

	// up is the local caller's own outgoing ring, opened lazily by
	// dispatchChannelDataReady once its handshake
	// (shmevent.EventChannelDataReady) confirms the caller has already
	// created it -- nil until then, guarded by mu since it's set from the
	// IPC dispatch goroutine and read (via hasUploadRing) from
	// dispatchChannelCloseWrite, possibly concurrently.
	up *chandata.ChunkReader
	// uploadDrained is closed by pumpChannelUpload when it returns, for
	// dispatchChannelCloseWrite to wait on (only meaningful once
	// hasUploadRing is true -- see that method's doc comment).
	uploadDrained chan struct{}
}

func newChannelSession(channelID string, stream network.Stream, remotePeerID string, remotePub shmevent.PublicKey, down *chandata.ChunkWriter, quota *quotaTracker, remoteIP string) *channelSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &channelSession{
		channelID:     channelID,
		stream:        stream,
		remotePeerID:  remotePeerID,
		remotePub:     remotePub,
		remoteIP:      remoteIP,
		quota:         quota,
		lastActivity:  time.Now(),
		closeCtx:      ctx,
		closeCancel:   cancel,
		down:          down,
		uploadDrained: make(chan struct{}),
	}
}

// hasUploadRing reports whether dispatchChannelDataReady has already
// confirmed and opened this session's upload ring -- see
// dispatchChannelCloseWrite's doc comment on why this is safe to check
// without a race: a genuine pkg/chandata caller always completes that
// handshake strictly before it could possibly call CloseChannelWrite.
func (s *channelSession) hasUploadRing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.up != nil
}

// setUploadRing records the opened upload ring reader -- called once, by
// dispatchChannelDataReady, before pumpChannelUpload starts draining it.
func (s *channelSession) setUploadRing(r *chandata.ChunkReader) {
	s.mu.Lock()
	s.up = r
	s.mu.Unlock()
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

// pushChunk records one purpose-tagged chunk pumpChannelReads just
// verified and unwrapped off the wire.
func (s *channelSession) pushChunk(purpose byte, chunk []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()
	if len(s.inbox) >= maxChannelInbox {
		s.inbox = s.inbox[1:]
	}
	s.inbox = append(s.inbox, channelChunk{purpose: purpose, data: chunk})
}

// popChunk returns the oldest buffered chunk, if any -- EventChannelPoll's
// read side. Each inbox entry is already bounded to channelMaxChunkSize
// (enforced by pumpChannelReads on the way in), so unlike this package's
// earlier raw-pipe design there is nothing left to split across polls:
// one call pops exactly one whole entry.
func (s *channelSession) popChunk() (purpose byte, chunk []byte, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.inbox) == 0 {
		return 0, nil, false
	}
	c := s.inbox[0]
	s.inbox = s.inbox[1:]
	return c.purpose, c.data, true
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

// write signs and encodes purpose and chunk into one
// shmevent.EncodeChannelFrame wire frame using priv -- always this
// node's own identity key, n.ed25519Priv -- and writes it as one length-
// framed message onto stream -- see ChannelProtocolID's doc comment, and
// shmevent.SignChannelChunk's on why this is a separate, variable-length
// signing scheme rather than the fixed-width Msg one every other event
// type (including the legacy EventChannelSend/Poll IPC path) uses.
func (s *channelSession) write(priv shmevent.PrivateKey, purpose byte, chunk []byte) error {
	if !s.quota.allow(s.remotePeerID, s.remoteIP, len(chunk)) {
		return fmt.Errorf("channel: quota exceeded for %s", s.remotePeerID)
	}
	crc, sig, err := shmevent.SignChannelChunk(priv, purpose, chunk)
	if err != nil {
		return fmt.Errorf("channel: sign frame: %w", err)
	}
	buf := shmevent.EncodeChannelFrame(purpose, crc, sig, chunk)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := writeFramed(s.stream, buf); err != nil {
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
				s.closeCancel()
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
			s.closeCancel()
			delete(t.sessions, id)
		}
	}
}

// maxFramedMessage caps readFramed's length prefix before allocating, so
// a peer can't claim an enormous length and force a large allocation. Must
// comfortably fit a full channelMaxChunkSize-sized EventChannelSend frame
// once capnp/crc32/signature overhead is added (empirically ~112 bytes for
// a full-sized Value) -- sized with generous headroom above that, not tuned
// to the exact byte count.
const maxFramedMessage = channelMaxChunkSize + 8*1024

// writeFramed writes a 4-byte big-endian length prefix followed by buf --
// ChannelProtocolID's framing for every message it carries (see that
// protocol's doc comment), handshake and post-handshake data frames
// alike: every other stream protocol in this file writes one message and
// half-closes, relying on EOF to mark the end (see e.g.
// handleExecuteStream's io.ReadAll), which doesn't work here since the
// stream must stay open afterward carrying many further framed messages.
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
	// ctx here is ipc.Serve's own top-level, whole-process-lifetime
	// context (see Run), not a per-request one -- Serve handles one
	// request at a time synchronously, so an unbounded dial that never
	// succeeds or fails (e.g. a peer whose addresses aren't yet resolvable,
	// such as one that only just joined the cluster) wedges this node's
	// entire IPC Serve loop indefinitely, not just this one call. Bound it
	// to streamRequestTimeout, the same budget the handshake right below
	// already gets.
	dialCtx, dialCancel := context.WithTimeout(ctx, streamRequestTimeout)
	// dest is caller-supplied and not necessarily a raft member -- same
	// reasoning as sendExecute's own relayCtx: without this, a dest
	// reachable only through a /p2p-circuit address hangs until dialCtx's
	// deadline instead of using the relayed connection.
	s, err := n.host.NewStream(network.WithAllowLimitedConn(dialCtx, "channel"), dest, ChannelProtocolID)
	dialCancel()
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

	notif, err := shmevent.NewChannelOpenHandshake(n.peerID)
	if err != nil {
		s.Close()
		return "", fmt.Errorf("channel: build handshake: %w", err)
	}
	buf, err := shmevent.Encode(notif, n.ed25519Priv)
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

	destPub, err := dest.ExtractPublicKey()
	if err != nil {
		s.Close()
		return "", fmt.Errorf("channel: extract public key for %s: %w", dest, err)
	}
	rawDestPub, err := destPub.Raw()
	if err != nil {
		s.Close()
		return "", fmt.Errorf("channel: raw public key for %s: %w", dest, err)
	}

	channelID, err := newChannelID()
	if err != nil {
		s.Close()
		return "", err
	}
	// Created synchronously, before channelID is ever handed back to the
	// caller below -- see pkg/chandata's doc comment and
	// shmevent.EventChannelDataReady's on why this ordering makes the
	// caller's own subsequent chandata.Open race-free.
	down, err := chandata.Create(n.peerID, channelID, chandata.DirDown)
	if err != nil {
		s.Close()
		return "", fmt.Errorf("channel: create data-plane ring: %w", err)
	}
	_ = s.SetDeadline(time.Time{}) // handshake done -- see the SetDeadline call above
	sess := newChannelSession(channelID, s, dest.String(), shmevent.PublicKey(rawDestPub), down, n.channelQuota, extractRemoteIP(s.Conn().RemoteMultiaddr()))
	n.channels.register(channelID, sess)
	go n.pumpChannelReads(sess)
	return channelID, nil
}

// handleChannelStream is the receiving side of ChannelProtocolID: reads
// the framed handshake (readFramed), verifies its self-contained
// signature against the claimed sender peer id exactly the way
// handleExecuteStream does (never s.Conn().RemotePeer() -- see that
// function's doc comment for why), gates on the sender belonging to
// shmevent.ReservedGroupCluster or ReservedGroupChannel (see those
// constants' doc comment), writes back a one-byte accept/reject
// (writeChannelAccept), and on
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
	if m.Which() != shmevent.Event_Which_channelOpen {
		s.Close()
		return
	}
	senderPeerID, err := m.ChannelOpen().SenderPeerId()
	if err != nil {
		s.Close()
		return
	}
	senderPeer, err := peer.Decode(senderPeerID)
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
	// this must be the verified senderPeer, never s.Conn().RemotePeer().
	// Not behind a Config opt-out flag, same as every other gate in this
	// package: a channel is only ever usable by a current cluster member,
	// a peer an operator has explicitly added to shmevent.ReservedGroupChannel
	// (mage addpeertogroup <peerID> channel), or a peer this node has
	// individually granted access to via its own personal group (mage
	// addpeertogroup <peerID> <n.peerID> -- see isPeerIdentityGroupID's doc
	// comment for the pairwise-grant mechanism this enables between any two
	// peers, cluster members or not). relayACL's AllowReserve/AllowConnect
	// and handleShmEvent's top-of-function gate gate the relay service and
	// the generic remote RPC surface the identical way, via
	// isAuthorizedForGatedAccess(St) against
	// shmevent.ReservedGroupRelay/ReservedGroupRemote respectively.
	// handleExecuteStream is the one exception: it gates EventExecute on
	// current ReservedGroupCluster membership alone, not through
	// isAuthorizedForGatedAccess -- see its own doc comment.
	if !n.isAuthorizedForGatedAccess(senderPeer, shmevent.ReservedGroupChannel) {
		writeChannelAccept(s, false, fmt.Sprintf("%s is not a cluster member, in the channel group, or granted access to %s", senderPeer, n.peerID))
		s.Close()
		return
	}
	remoteIP := extractRemoteIP(s.Conn().RemoteMultiaddr())

	n.channels.reap()
	channelID, err := newChannelID()
	if err != nil {
		writeChannelAccept(s, false, "internal error minting channel id")
		s.Close()
		return
	}
	// Created synchronously, before this channelID is ever handed back to
	// a local caller via EventChannelListen -- see the identical ordering
	// reasoning in dispatchChannelOpen just above.
	down, err := chandata.Create(n.peerID, channelID, chandata.DirDown)
	if err != nil {
		writeChannelAccept(s, false, "internal error preparing data plane")
		s.Close()
		return
	}
	if err := writeChannelAccept(s, true, ""); err != nil {
		down.CloseStorage()
		s.Close()
		return
	}
	// Handshake done -- withStreamRequestDeadline's deadline (set when
	// this handler was entered) must not keep applying to
	// pumpChannelReads' intentionally long-lived read loop below. See
	// streamRequestTimeout's doc comment.
	_ = s.SetDeadline(time.Time{})
	sess := newChannelSession(channelID, s, senderPeer.String(), shmevent.PublicKey(rawSenderPub), down, n.channelQuota, remoteIP)
	n.channels.register(channelID, sess)
	n.channels.pushPending(channelID)
	go n.pumpChannelReads(sess)
}

// downRingWriteTimeout bounds each individual attempt pumpChannelReads
// makes to also mirror a received chunk into sess.down (see that field's
// doc comment) -- deliberately short relative to sess.closeCtx's own
// lifetime, not because a slow-but-live local caller should ever actually
// hit it (chandata.Capacity comfortably outpaces one downRingWriteTimeout
// window at any realistic drain rate), but so a caller that never opens
// this ring at all (the legacy EventChannelPoll-only path, which has no
// reason to ever call shmevent.EventChannelDataReady) can't wedge this
// pump's read loop forever once the ring fills up -- sess.inbox above
// already has this chunk buffered for that path regardless, so a dropped
// mirror write here costs nothing but the ring's own throughput advantage
// for a caller that was never going to use it in the first place.
const downRingWriteTimeout = 250 * time.Millisecond

// pumpChannelReads is sess's background read pump: reads one signed
// shmevent.EncodeChannelFrame frame at a time off sess.stream
// (readFramed), decodes and verifies each against sess.remotePub
// (shmevent.DecodeChannelFrame/VerifyChannelChunk) and delivers it two
// ways: pushed onto sess.inbox for the legacy EventChannelPoll path to
// drain, and written into sess.down (see downRingWriteTimeout) for
// pkg/chandata callers --
// until the stream errors/EOFs (the peer closed their write side, or the
// connection dropped), a frame fails to read/decode/verify/fit
// channelMaxChunkSize (treated as fatal to the session -- a peer that
// can't hold up its end of the framing contract can't be trusted for
// anything after that point either), or this node's own
// EventChannelClose/the reaper closes the stream first -- any of which
// marks the session closed rather than removing it outright, so chunks
// already buffered are still readable via a final poll, and releases
// sess.down's storage (pumpChannelReads is that ring's sole writer for its
// whole lifetime -- see ChunkWriter.CloseStorage's doc comment on why
// that's the right owner to release it, regardless of whether every byte
// has actually been drained yet).
func (n *Node) pumpChannelReads(sess *channelSession) {
	defer sess.down.CloseStorage()
	for {
		buf, err := readFramed(sess.stream)
		if err != nil {
			reason := ""
			if err != io.EOF {
				reason = err.Error()
			}
			sess.markClosed(reason)
			return
		}
		purpose, crc, sig, chunk, err := shmevent.DecodeChannelFrame(buf)
		if err != nil {
			sess.markClosed(fmt.Sprintf("decode channel frame: %v", err))
			return
		}
		if err := shmevent.VerifyChannelChunk(sess.remotePub, purpose, chunk, crc, sig); err != nil {
			sess.markClosed(fmt.Sprintf("verify channel frame: %v", err))
			return
		}
		if len(chunk) > channelMaxChunkSize {
			sess.markClosed(fmt.Sprintf("peer sent an oversized channel chunk: %d bytes", len(chunk)))
			return
		}
		if !sess.quota.allow(sess.remotePeerID, sess.remoteIP, len(chunk)) {
			sess.markClosed("channel quota exceeded")
			return
		}
		sess.pushChunk(purpose, chunk)

		writeCtx, writeCancel := context.WithTimeout(sess.closeCtx, downRingWriteTimeout)
		_ = sess.down.WriteChunk(writeCtx, purpose, chunk)
		writeCancel()
	}
}

// pumpChannelUpload is sess's background upload-forward pump: started by
// dispatchChannelDataReady once r (sess's upload ring) is confirmed open,
// it reads one purpose-tagged chunk at a time off r and forwards each
// through sess.write -- the exact same signed-frame path
// dispatchChannelSend's legacy per-chunk IPC calls already use, so both
// can safely interleave on the same stream (writeMu serializes them).
// Returns once r reaches io.EOF (the local caller closed its writer and
// every already-buffered chunk has been forwarded -- see
// shmevent.EventChannelCloseWrite's doc comment on why this is what makes
// its drain-then-half-close guarantee correct), sess.closeCtx is
// cancelled (the channel is being torn down some other way), or a write
// fails (the underlying stream itself is gone, fatal to the session same
// as pumpChannelReads' own read errors). Closes sess.uploadDrained on
// return either way, and releases r -- pumpChannelUpload is r's only
// reader for its whole lifetime, but never owns its storage (the local
// caller created it -- see ChunkWriter.CloseStorage's doc comment).
func (n *Node) pumpChannelUpload(sess *channelSession, r *chandata.ChunkReader) {
	defer close(sess.uploadDrained)
	defer r.Close()
	for {
		purpose, chunk, err := r.ReadChunk(sess.closeCtx)
		if err != nil {
			return
		}
		if err := sess.write(n.ed25519Priv, purpose, chunk); err != nil {
			sess.markClosed(fmt.Sprintf("forward upload chunk: %v", err))
			return
		}
	}
}

// dispatchChannelSend implements EventChannelSend: signs and frames
// purpose+chunk (channelSession.write) onto channelID's stream. Rejects
// chunk outright if it exceeds channelMaxChunkSize, before ever writing
// it -- see that constant's doc comment. Deliberately does not pre-check
// channelSession.status()'s closed flag: that flag tracks whether *this
// node's own read side* has hit EOF (the peer half-closed or fully
// closed their outgoing direction -- see EventChannelCloseWrite), which
// says nothing about whether writing is still valid -- half-close is
// directional. If this node's own outgoing direction has itself been
// closed (EventChannelCloseWrite, or a full EventChannelClose), the
// underlying write below fails on its own, which is what actually gates
// this.
func (n *Node) dispatchChannelSend(channelID string, purpose byte, chunk []byte) error {
	if len(chunk) > channelMaxChunkSize {
		return fmt.Errorf("channel: chunk too large: %d bytes (max %d)", len(chunk), channelMaxChunkSize)
	}
	sess, ok := n.channels.get(channelID)
	if !ok {
		return fmt.Errorf("channel: no such channel %q", channelID)
	}
	return sess.write(n.ed25519Priv, purpose, chunk)
}

// dispatchChannelPoll implements channelPoll: pops the oldest buffered
// chunk from channelID's inbox, if any -- see ChannelPollNoData/Chunk/
// Closed for the three-way result.
func (n *Node) dispatchChannelPoll(channelID string) (status, purpose byte, chunk []byte, err error) {
	n.channels.reap()
	sess, ok := n.channels.get(channelID)
	if !ok {
		return 0, 0, nil, fmt.Errorf("channel: no such channel %q", channelID)
	}
	if purpose, chunk, ok := sess.popChunk(); ok {
		// A chunk the wire accepts (up to channelMaxChunkSize) can be far
		// larger than a poll response can carry. Encoding one anyway
		// produced a response pkg/ipc could not encode ("value too long"),
		// which reached the caller as a bare transport error naming
		// neither the channel nor the size -- and by then popChunk had
		// already removed the entry, so the chunk was gone. Say exactly
		// what happened instead, and point at the path that can carry it:
		// the data-plane ring has this same chunk (pumpChannelReads writes
		// every chunk to both), so nothing is actually lost for a reader
		// on that path. Popping rather than leaving it queued is
		// deliberate -- a chunk this reader can never take would otherwise
		// block every later one behind it forever.
		if len(chunk) > maxPollChunkSize {
			return 0, 0, nil, fmt.Errorf("channel: buffered chunk is %d bytes, larger than a poll response can carry (max %d) -- read this channel through its data-plane ring (see pkg/chandata) instead", len(chunk), maxPollChunkSize)
		}
		return shmevent.ChannelPollChunk, purpose, chunk, nil
	}
	if closed, _ := sess.status(); closed {
		return shmevent.ChannelPollClosed, shmevent.ChannelPurposeData, nil, nil
	}
	return shmevent.ChannelPollNoData, shmevent.ChannelPurposeData, nil, nil
}

// dispatchChannelListen implements channelListen: claims the oldest
// pending (accepted-but-unclaimed) incoming channel, if any. Empty
// channelID means none pending yet -- a local caller polls this in a
// loop, exactly pollExecute's documented convention.
func (n *Node) dispatchChannelListen() (channelID, remotePeerID string, err error) {
	n.channels.reap()
	channelID, ok := n.channels.popPending()
	if !ok {
		return "", "", nil
	}
	sess, ok := n.channels.get(channelID)
	if !ok {
		// Reaped between popPending and here -- vanishingly unlikely, but
		// safe to treat the same as "nothing pending yet."
		return "", "", nil
	}
	return channelID, sess.remotePeerID, nil
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
	sess.closeCancel()
	n.channels.remove(channelID)
	return nil
}

// dispatchChannelDataReady implements EventChannelDataReady: opens
// channelID's upload ring (pkg/chandata.DirUp, already created by the
// caller before sending this -- see that event's doc comment) and starts
// pumpChannelUpload draining it. ctx (the incoming request's) is
// deliberately *not* used to bound the open attempt -- like
// dispatchChannelOpen's own identical reasoning (see that function's
// comment on why its dial gets its own bounded context), ctx here is
// ipc.Serve's whole-process-lifetime context, not a per-request one, and
// Serve handles one request at a time synchronously -- an open that never
// succeeds (a caller that sends this without ever having created the
// ring) would otherwise wedge this node's entire IPC loop indefinitely,
// not just this one call. Bound to streamRequestTimeout instead, same as
// dispatchChannelOpen's own dial+handshake.
func (n *Node) dispatchChannelDataReady(ctx context.Context, channelID string) error {
	sess, ok := n.channels.get(channelID)
	if !ok {
		return fmt.Errorf("channel: no such channel %q", channelID)
	}
	openCtx, cancel := context.WithTimeout(context.Background(), streamRequestTimeout)
	defer cancel()
	r, err := chandata.Open(openCtx, n.peerID, channelID, chandata.DirUp)
	if err != nil {
		return fmt.Errorf("channel: open upload ring: %w", err)
	}
	sess.setUploadRing(r)
	go n.pumpChannelUpload(sess, r)
	return nil
}

// dispatchChannelCloseWrite implements EventChannelCloseWrite: half-closes
// channelID's outgoing direction only, leaving the session registered
// (still pollable/receivable) -- see that event's doc comment. A no-op,
// not an error, if channelID is already gone. If channelID's local caller
// completed the EventChannelDataReady handshake (sess.hasUploadRing),
// this first blocks until pumpChannelUpload's own uploadDrained signal
// fires -- i.e. until every chunk already buffered in the upload ring has
// genuinely been forwarded onto the wire -- before actually half-closing,
// so the caller's "this call returned" still means "everything I sent is
// on the wire," the same guarantee the plain EventChannelSend path gets
// for free by being synchronous per chunk (see EventChannelDataReady's
// doc comment). hasUploadRing is race-free here specifically because a
// genuine pkg/chandata caller always completes that handshake strictly
// before it could possibly reach this call. The wait is bounded by
// streamRequestTimeout, not the incoming ctx -- see
// dispatchChannelDataReady's identical reasoning on why that ctx is
// ipc.Serve's whole-process-lifetime one, not a per-request deadline.
func (n *Node) dispatchChannelCloseWrite(ctx context.Context, channelID string) error {
	sess, ok := n.channels.get(channelID)
	if !ok {
		return nil
	}
	if sess.hasUploadRing() {
		drainCtx, cancel := context.WithTimeout(context.Background(), streamRequestTimeout)
		defer cancel()
		select {
		case <-sess.uploadDrained:
		case <-drainCtx.Done():
			return fmt.Errorf("channel: waiting for upload ring to drain: %w", drainCtx.Err())
		}
	}
	return sess.closeWrite()
}
