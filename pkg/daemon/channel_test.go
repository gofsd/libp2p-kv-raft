package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// channelOpenMsg builds a channelOpen request Msg addressed to peerID.
func channelOpenMsg(t *testing.T, peerID string, id uint16) shmevent.Msg {
	t.Helper()
	m, err := shmevent.NewChannelOpen(peerID)
	if err != nil {
		t.Fatalf("NewChannelOpen: %v", err)
	}
	m.SetId(id)
	return m
}

// channelIDFromOpenResp extracts the freshly minted local channelID from a
// channelOpen response -- the response reuses the request's own peerId
// field to carry it back (see NewChannelOpen's doc comment).
func channelIDFromOpenResp(t *testing.T, resp shmevent.Msg) string {
	t.Helper()
	id, err := resp.ChannelOpen().PeerId()
	if err != nil {
		t.Fatalf("ChannelOpen peer_id: %v", err)
	}
	return id
}

// channelSendMsg builds a channelSend request Msg.
func channelSendMsg(t *testing.T, channelID string, purpose byte, chunk []byte, id uint16) shmevent.Msg {
	t.Helper()
	m, err := shmevent.NewChannelSend(channelID, purpose, chunk)
	if err != nil {
		t.Fatalf("NewChannelSend: %v", err)
	}
	m.SetId(id)
	return m
}

// pollChannel is a small test helper wrapping channelPoll's response
// fields -- used throughout below to avoid repeating the same field reads
// at every call site.
func pollChannel(t *testing.T, ctx context.Context, n *Node, channelID string) (status, purpose byte, chunk []byte) {
	t.Helper()
	m, err := shmevent.NewChannelPoll(channelID)
	if err != nil {
		t.Fatalf("NewChannelPoll: %v", err)
	}
	m.SetId(1)
	resp := callLocal(t, ctx, n, m, n.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_poll rejected: %s", mustErrMessage(t, resp))
	}
	grp := resp.ChannelPoll()
	chunk, err = grp.Chunk()
	if err != nil {
		t.Fatalf("ChannelPoll chunk: %v", err)
	}
	return grp.Status(), grp.Purpose(), chunk
}

// pollChannelUntilChunk polls channelID on n until a chunk arrives or
// deadline passes, mirroring TestExecuteEventDeliversAcrossNodes' own
// poll loop. Discards purpose -- see pollChannelUntilChunkWithPurpose for
// a variant that asserts on it.
func pollChannelUntilChunk(t *testing.T, ctx context.Context, n *Node, channelID string) []byte {
	t.Helper()
	chunk, _ := pollChannelUntilChunkWithPurpose(t, ctx, n, channelID)
	return chunk
}

// pollChannelUntilChunkWithPurpose is pollChannelUntilChunk, but also
// returns the chunk's purpose -- for tests asserting purpose is carried
// through correctly.
func pollChannelUntilChunkWithPurpose(t *testing.T, ctx context.Context, n *Node, channelID string) (chunk []byte, purpose byte) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		status, purpose, chunk := pollChannel(t, ctx, n, channelID)
		if status == shmevent.ChannelPollChunk {
			return chunk, purpose
		}
		select {
		case <-deadline:
			t.Fatal("channel chunk never arrived")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// listenChannelUntilClaimed polls channelListen on n until an incoming
// channel is claimed or deadline passes, returning its local channelID and
// the remote peer id it reports.
func listenChannelUntilClaimed(t *testing.T, ctx context.Context, n *Node) (channelID, remotePeerID string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		m, err := shmevent.NewChannelListen()
		if err != nil {
			t.Fatalf("NewChannelListen: %v", err)
		}
		m.SetId(1)
		resp := callLocal(t, ctx, n, m, n.ed25519Priv)
		if resp.Which() == shmevent.Event_Which_error {
			t.Fatalf("channel_listen rejected: %s", mustErrMessage(t, resp))
		}
		grp := resp.ChannelListen()
		gotID, err := grp.ChannelId()
		if err != nil {
			t.Fatalf("ChannelListen channel_id: %v", err)
		}
		if gotID != "" {
			gotPeer, err := grp.RemotePeerId()
			if err != nil {
				t.Fatalf("ChannelListen remote_peer_id: %v", err)
			}
			return gotID, gotPeer
		}
		select {
		case <-deadline:
			t.Fatal("incoming channel never became listenable")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// grantChannelAccess grants sender's peer id membership in receiver's
// reserved "channel" group directly in receiver's store -- the minimal
// standing handleChannelStream's always-enforced gate now requires
// between two standalone test nodes with no cluster relationship to each
// other (see shmevent.ReservedGroupChannel's own doc comment). Every
// channelOpen in this file dials from a to b, never the other way
// (b's own replies reuse the channel a already opened, via
// channelSend -- see TestChannelOpenBidirectionalSendPoll), so a
// single grant of a into b's store is always what these tests need.
func grantChannelAccess(t *testing.T, sender, receiver *Node) {
	t.Helper()
	senderPeerID, err := peer.Decode(sender.peerID)
	if err != nil {
		t.Fatalf("decode sender peer id: %v", err)
	}
	key, err := shmevent.PeerGroupKey([]byte(senderPeerID.String()), []byte(shmevent.ReservedGroupChannel))
	if err != nil {
		t.Fatalf("build channel PeerGroup key: %v", err)
	}
	if err := receiver.store.Set(key, nil); err != nil {
		t.Fatalf("grant channel access: %v", err)
	}
}

// TestChannelOpenBidirectionalSendPoll is the end-to-end happy path: a
// opens a channel to b, sends a chunk, b claims it via channel_listen and
// receives it via channel_poll; b then sends a chunk back on its own
// (independently minted) channelID, and a receives it too. Mirrors
// TestExecuteEventDeliversAcrossNodes' shape.
func TestChannelOpenBidirectionalSendPoll(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)
	if aChannelID == "" {
		t.Fatal("channel_open returned an empty channelID")
	}

	sendResp := callLocal(t, ctx, a, channelSendMsg(t, aChannelID, shmevent.ChannelPurposeData, []byte("hello from a"), 2), a.ed25519Priv)
	if sendResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_send rejected: %s", mustErrMessage(t, sendResp))
	}

	bChannelID, remotePeerID := listenChannelUntilClaimed(t, ctx, b)
	if remotePeerID != a.peerID {
		t.Fatalf("listen reported remote peer %q, want %q", remotePeerID, a.peerID)
	}

	gotChunk := pollChannelUntilChunk(t, ctx, b, bChannelID)
	if string(gotChunk) != "hello from a" {
		t.Fatalf("b received %q, want %q", gotChunk, "hello from a")
	}

	replyResp := callLocal(t, ctx, b, channelSendMsg(t, bChannelID, shmevent.ChannelPurposeData, []byte("hello from b"), 3), b.ed25519Priv)
	if replyResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("reply channel_send rejected: %s", mustErrMessage(t, replyResp))
	}

	gotReply := pollChannelUntilChunk(t, ctx, a, aChannelID)
	if string(gotReply) != "hello from b" {
		t.Fatalf("a received %q, want %q", gotReply, "hello from b")
	}
}

// TestChannelPurposeCarriedEndToEnd is the central assertion this whole
// framed/purpose-tagged rewrite exists for: a purpose other than the
// default (ChannelPurposeVideo, ChannelPurposeControl) survives
// channelSend -> the signed network frame (EncodeChannelFrame) ->
// channelPoll intact, in both directions.
func TestChannelPurposeCarriedEndToEnd(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)

	sendResp := callLocal(t, ctx, a, channelSendMsg(t, aChannelID, shmevent.ChannelPurposeVideo, []byte("frame bytes"), 2), a.ed25519Priv)
	if sendResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_send rejected: %s", mustErrMessage(t, sendResp))
	}

	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)
	gotChunk, gotPurpose := pollChannelUntilChunkWithPurpose(t, ctx, b, bChannelID)
	if string(gotChunk) != "frame bytes" {
		t.Fatalf("b received %q, want %q", gotChunk, "frame bytes")
	}
	if gotPurpose != shmevent.ChannelPurposeVideo {
		t.Fatalf("b received purpose %d, want %d (ChannelPurposeVideo)", gotPurpose, shmevent.ChannelPurposeVideo)
	}

	replyResp := callLocal(t, ctx, b, channelSendMsg(t, bChannelID, shmevent.ChannelPurposeControl, []byte("ack"), 3), b.ed25519Priv)
	if replyResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("reply channel_send rejected: %s", mustErrMessage(t, replyResp))
	}

	gotReply, gotReplyPurpose := pollChannelUntilChunkWithPurpose(t, ctx, a, aChannelID)
	if string(gotReply) != "ack" {
		t.Fatalf("a received %q, want %q", gotReply, "ack")
	}
	if gotReplyPurpose != shmevent.ChannelPurposeControl {
		t.Fatalf("a received purpose %d, want %d (ChannelPurposeControl)", gotReplyPurpose, shmevent.ChannelPurposeControl)
	}
}

// TestChannelLargeChunkNearChannelMaxSize confirms a channel chunk can
// carry far more than the old, shared shmevent.ValueSize (512 bytes) --
// that ceiling, like every other artificial per-event size cap, is gone
// entirely in the capnp rewrite (no ValueSize/ChannelValueSize exist
// anymore at all), so this now exercises a chunk comfortably bigger than
// that old 512-byte figure (mirrored here as a literal) rather than
// against any current ceiling -- there isn't one to size against. This
// exercises the legacy per-chunk IPC path (raw channelSend/Poll, the same
// as every other test in this file) rather than the much larger
// pkg/chandata ring path (channelMaxChunkSize, see that constant's doc
// comment) -- that path's own near-ceiling coverage lives in
// pkg/shmclient instead, since it has no IPC-request-sized payload to stay
// under in the first place.
func TestChannelLargeChunkNearChannelMaxSize(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)

	// oldValueSize mirrors the deleted shmevent.ValueSize (512 bytes) --
	// this test's whole point is a chunk comfortably bigger than that,
	// which no longer has any real ceiling to bump up against.
	const oldValueSize = 512
	want := make([]byte, oldValueSize*4)
	for i := range want {
		want[i] = byte(i % 256)
	}

	sendResp := callLocal(t, ctx, a, channelSendMsg(t, aChannelID, shmevent.ChannelPurposeData, want, 2), a.ed25519Priv)
	if sendResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_send rejected: %s", mustErrMessage(t, sendResp))
	}

	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)
	got := pollChannelUntilChunk(t, ctx, b, bChannelID)
	if len(got) != len(want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("byte %d mismatch: got %x, want %x", i, got[i], want[i])
		}
	}
}

// TestChannelCloseIsObservedAsClosedByPeer confirms that closing a's own
// end of the channel is observed on b's side as ChannelPollClosed --
// idempotently, on every subsequent poll -- rather than an error or a
// hang.
func TestChannelCloseIsObservedAsClosedByPeer(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)
	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	closeMsg, err := shmevent.NewChannelClose(aChannelID)
	if err != nil {
		t.Fatalf("NewChannelClose: %v", err)
	}
	closeMsg.SetId(2)
	closeResp := callLocal(t, ctx, a, closeMsg, a.ed25519Priv)
	if closeResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_close rejected: %s", mustErrMessage(t, closeResp))
	}

	deadline := time.After(10 * time.Second)
	for {
		status, _, _ := pollChannel(t, ctx, b, bChannelID)
		if status == shmevent.ChannelPollClosed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("b never observed the channel as closed")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Idempotent: polling again must still report closed, not an error.
	status, _, _ := pollChannel(t, ctx, b, bChannelID)
	if status != shmevent.ChannelPollClosed {
		t.Fatalf("second poll after close returned status %d, want ChannelPollClosed", status)
	}
}

// TestChannelCloseWriteLeavesOtherDirectionOpen is the regression test
// for the half-close gap a real two-process CLI smoke test surfaced:
// after a's stdin naturally reaches EOF, half-closing (not fully
// closing) must still let b's still-open direction keep working -- b can
// send a reply, a can still receive it -- and only once b's own
// direction also finishes does a's side see ChannelPollClosed. A full
// channelClose right after a's own EOF would have cut b's reply off
// prematurely.
func TestChannelCloseWriteLeavesOtherDirectionOpen(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)
	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	// a is "done sending" (its stdin hit EOF) -- half-close, not close.
	closeWriteMsg, err := shmevent.NewChannelCloseWrite(aChannelID)
	if err != nil {
		t.Fatalf("NewChannelCloseWrite: %v", err)
	}
	closeWriteMsg.SetId(2)
	closeWriteResp := callLocal(t, ctx, a, closeWriteMsg, a.ed25519Priv)
	if closeWriteResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_close_write rejected: %s", mustErrMessage(t, closeWriteResp))
	}

	// b must still be able to send a reply, and a must still receive it
	// -- the direction b->a was never touched by a's half-close.
	replyResp := callLocal(t, ctx, b, channelSendMsg(t, bChannelID, shmevent.ChannelPurposeData, []byte("still here"), 3), b.ed25519Priv)
	if replyResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_send after peer's half-close rejected: %s", mustErrMessage(t, replyResp))
	}
	if got := pollChannelUntilChunk(t, ctx, a, aChannelID); string(got) != "still here" {
		t.Fatalf("a received %q after its own half-close, want %q", got, "still here")
	}

	// Now b also finishes (its own EOF) -- a full close this time, since
	// b has nothing further to receive either. Only now should a observe
	// ChannelPollClosed.
	bCloseMsg, err := shmevent.NewChannelClose(bChannelID)
	if err != nil {
		t.Fatalf("NewChannelClose: %v", err)
	}
	bCloseMsg.SetId(4)
	bCloseResp := callLocal(t, ctx, b, bCloseMsg, b.ed25519Priv)
	if bCloseResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_close rejected: %s", mustErrMessage(t, bCloseResp))
	}
	deadline := time.After(10 * time.Second)
	for {
		status, _, _ := pollChannel(t, ctx, a, aChannelID)
		if status == shmevent.ChannelPollClosed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("a never observed the channel as closed after b's close")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestChannelStreamRejectsForgedSignature mirrors
// TestExecuteStreamRejectsForgedSignature: a handshake claiming to be
// from a's peer id but actually signed with an unrelated key must be
// rejected outright -- never registered, never listenable.
func TestChannelStreamRejectsForgedSignature(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))

	forgerPriv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate forger key: %v", err)
	}
	forger, err := libp2p.New(libp2p.Identity(forgerPriv))
	if err != nil {
		t.Fatalf("start forger host: %v", err)
	}
	defer forger.Close()

	bAddr := b.advertisedAddrs()[0]
	maddr, err := multiaddr.NewMultiaddr(bAddr)
	if err != nil {
		t.Fatalf("parse b addr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("b addr info: %v", err)
	}
	if err := forger.Connect(ctx, *info); err != nil {
		t.Fatalf("connect forger->b: %v", err)
	}

	handshake, err := shmevent.NewChannelOpenHandshake(a.peerID)
	if err != nil {
		t.Fatalf("NewChannelOpenHandshake: %v", err)
	}
	buf, err := shmevent.Encode(handshake, shmevent.PrivateKey(mustRaw(t, forgerPriv)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	bPeerID, err := peer.Decode(b.peerID)
	if err != nil {
		t.Fatalf("decode b peer id: %v", err)
	}
	s, err := forger.NewStream(ctx, bPeerID, ChannelProtocolID)
	if err != nil {
		t.Fatalf("open stream to b: %v", err)
	}
	if err := writeFramed(s, buf); err != nil {
		t.Fatalf("write handshake: %v", err)
	}

	// b must never accept this: give handleChannelStream a moment to run,
	// then confirm nothing became listenable.
	time.Sleep(200 * time.Millisecond)
	listenMsg, err := shmevent.NewChannelListen()
	if err != nil {
		t.Fatalf("NewChannelListen: %v", err)
	}
	listenMsg.SetId(1)
	listenResp := callLocal(t, ctx, b, listenMsg, b.ed25519Priv)
	if listenResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_listen rejected: %s", mustErrMessage(t, listenResp))
	}
	gotID, err := listenResp.ChannelListen().ChannelId()
	if err != nil {
		t.Fatalf("ChannelListen channel_id: %v", err)
	}
	if gotID != "" {
		t.Fatalf("forged channel_open was accepted and became listenable: %q", gotID)
	}
}

// TestChannelDataFrameForgedAfterHandshakeRejected proves per-frame
// signature verification -- not just handshake-time verification --
// actually gates channel data: after a's real handshake with b succeeds,
// a frame written directly onto the stream but signed with an unrelated
// key must never be delivered via channel_poll. This is the whole point
// of framing every post-handshake message with SignChannelChunk/
// EncodeChannelFrame's own signature scheme (see ChannelProtocolID's doc
// comment) -- without per-frame verification, this same injection would
// have silently appeared as ordinary received bytes under this package's
// earlier raw-pipe design.
func TestChannelDataFrameForgedAfterHandshakeRejected(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)
	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	// Inject a frame directly onto a's side of the stream, signed with an
	// unrelated key instead of a's real identity -- bypassing
	// dispatchChannelSend/channelSession.write entirely, exactly the way
	// a malicious or buggy peer might.
	aSess, ok := a.channels.get(aChannelID)
	if !ok {
		t.Fatal("a's own channel session vanished")
	}
	forgerPriv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate forger key: %v", err)
	}
	forgerRaw := shmevent.PrivateKey(mustRaw(t, forgerPriv))
	crc, sig, err := shmevent.SignChannelChunk(forgerRaw, shmevent.ChannelPurposeData, []byte("forged"))
	if err != nil {
		t.Fatalf("SignChannelChunk: %v", err)
	}
	forgedBuf := shmevent.EncodeChannelFrame(shmevent.ChannelPurposeData, crc, sig, []byte("forged"))
	if err := writeFramed(aSess.stream, forgedBuf); err != nil {
		t.Fatalf("write forged frame: %v", err)
	}

	// b must never deliver "forged" via channel_poll -- it must instead
	// observe the channel closed, since a forged frame is fatal to the
	// session (see pumpChannelReads' doc comment).
	deadline := time.After(10 * time.Second)
	for {
		status, _, chunk := pollChannel(t, ctx, b, bChannelID)
		if string(chunk) == "forged" {
			t.Fatal("b delivered a chunk forged with an unrelated key")
		}
		if status == shmevent.ChannelPollClosed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("b never observed the channel as closed after a forged frame")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestChannelGroupGate mirrors what TestRequirePermitForChannelGate used to
// check against Config.RequirePermitForChannel/KindPermitPeer, updated for
// handleChannelStream's current, always-enforced gate: a sender must
// belong to shmevent.ReservedGroupCluster or ReservedGroupChannel (see
// those constants' doc comment), not hold a KindPermitPeer permit.
func TestChannelGroupGate(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))

	bDir := filepath.Join(tmpDir, "b")
	if err := os.MkdirAll(bDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", bDir, err)
	}
	bKeyPath := filepath.Join(bDir, "identity.key")
	if _, err := p2praft.LoadOrGenerateKey(bKeyPath); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	b, err := start(Config{DataDir: bDir, KeyPath: bKeyPath})
	if err != nil {
		t.Fatalf("start b: %v", err)
	}
	t.Cleanup(b.shutdown)

	connectPeers(t, ctx, a, b)

	open := func(id uint16) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, id), a.ed25519Priv)
	}

	// Neither a cluster member nor in the channel group: rejected.
	// dispatchChannelOpen reads b's reject response synchronously, so the
	// gate's rejection on b surfaces straight back as a local error on a.
	if resp := open(1); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("channel_open from a sender in neither reserved group unexpectedly succeeded")
	}

	aPeerID, err := peer.Decode(a.peerID)
	if err != nil {
		t.Fatalf("decode a peer id: %v", err)
	}

	// Grant a membership in b's "channel" group directly.
	channelGroupKey, err := shmevent.PeerGroupKey([]byte(aPeerID.String()), []byte(shmevent.ReservedGroupChannel))
	if err != nil {
		t.Fatalf("build channel PeerGroup key: %v", err)
	}
	if err := b.store.Set(channelGroupKey, nil); err != nil {
		t.Fatalf("grant channel group membership: %v", err)
	}
	if resp := open(2); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open from a channel-group member rejected: %s", mustErrMessage(t, resp))
	}

	// Revoke it again and confirm "cluster" group membership alone is
	// sufficient, independent of the channel group.
	if err := b.store.Delete(channelGroupKey); err != nil {
		t.Fatalf("revoke channel group membership: %v", err)
	}
	if resp := open(3); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("channel_open from a sender with revoked channel-group membership and no cluster membership unexpectedly succeeded")
	}

	clusterGroupKey, err := shmevent.PeerGroupKey([]byte(aPeerID.String()), []byte(shmevent.ReservedGroupCluster))
	if err != nil {
		t.Fatalf("build cluster PeerGroup key: %v", err)
	}
	if err := b.store.Set(clusterGroupKey, nil); err != nil {
		t.Fatalf("grant cluster group membership: %v", err)
	}
	if resp := open(4); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open from a cluster-group member rejected: %s", mustErrMessage(t, resp))
	}
}

// TestChannelPersonalGroupGate proves the pairwise-grant mechanism
// isPeerIdentityGroupID's doc comment describes: two standalone nodes with
// no cluster relationship to each other at all (neither ever joins the
// other's raft group) can still open a channel in one direction once the
// receiver's own personal group (id == its own peer id) lists the sender
// -- and that grant is one-directional, not implied by its reverse: b
// trusting a doesn't mean a trusts b back, exactly the "peer A should
// have B in its own peer group, and vice versa" requirement a truly
// bidirectional connection needs.
func TestChannelPersonalGroupGate(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	openAtoB := func(id uint16) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, id), a.ed25519Priv)
	}
	openBtoA := func(id uint16) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, b, channelOpenMsg(t, a.peerID, id), b.ed25519Priv)
	}

	// Neither side has granted the other anything yet: both directions
	// are rejected.
	if resp := openAtoB(1); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("a -> b channel_open with no grant either way unexpectedly succeeded")
	}
	if resp := openBtoA(2); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("b -> a channel_open with no grant either way unexpectedly succeeded")
	}

	aPeerID, err := peer.Decode(a.peerID)
	if err != nil {
		t.Fatalf("decode a peer id: %v", err)
	}

	// b grants a access to itself, via b's own personal group -- b's
	// PeerGroupKey(a, b.peerID) record. This must permit a -> b, but must
	// NOT (on its own) permit the reverse b -> a: b hasn't been granted
	// anything in a's own personal group.
	bPersonalGroupKey, err := shmevent.PeerGroupKey([]byte(aPeerID.String()), []byte(b.peerID))
	if err != nil {
		t.Fatalf("build b's personal PeerGroup key: %v", err)
	}
	if err := b.store.Set(bPersonalGroupKey, nil); err != nil {
		t.Fatalf("grant a access to b's personal group: %v", err)
	}
	if resp := openAtoB(3); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("a -> b channel_open after b granted a access rejected: %s", mustErrMessage(t, resp))
	}
	if resp := openBtoA(4); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("b -> a channel_open unexpectedly succeeded from b's one-directional grant of a alone")
	}

	// a now also grants b access to itself, via a's own personal group --
	// the connection is genuinely bidirectional only once both sides have
	// granted each other.
	bPeerID, err := peer.Decode(b.peerID)
	if err != nil {
		t.Fatalf("decode b peer id: %v", err)
	}
	aPersonalGroupKey, err := shmevent.PeerGroupKey([]byte(bPeerID.String()), []byte(a.peerID))
	if err != nil {
		t.Fatalf("build a's personal PeerGroup key: %v", err)
	}
	if err := a.store.Set(aPersonalGroupKey, nil); err != nil {
		t.Fatalf("grant b access to a's personal group: %v", err)
	}
	if resp := openBtoA(5); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("b -> a channel_open after a granted b access rejected: %s", mustErrMessage(t, resp))
	}

	// Revoking b's grant of a leaves a's grant of b (the reverse
	// direction) untouched.
	if err := b.store.Delete(bPersonalGroupKey); err != nil {
		t.Fatalf("revoke a's access to b's personal group: %v", err)
	}
	if resp := openAtoB(6); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("a -> b channel_open after b revoked a's grant unexpectedly succeeded")
	}
	if resp := openBtoA(7); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("b -> a channel_open rejected after only a -> b's grant was revoked: %s", mustErrMessage(t, resp))
	}
}

// TestChannelEventsRejectRemoteCaller mirrors
// TestClientProtocolRejectsRemoteKeyFetch's loop-over-events shape: none
// of the channel events are available to a remote (ClientProtocolID)
// caller, only this node's own local operator.
func TestChannelEventsRejectRemoteCaller(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	builders := []func() (shmevent.Msg, error){
		func() (shmevent.Msg, error) { return shmevent.NewChannelOpen("some-peer") },
		func() (shmevent.Msg, error) {
			return shmevent.NewChannelSend("some-channel", shmevent.ChannelPurposeData, nil)
		},
		func() (shmevent.Msg, error) { return shmevent.NewChannelPoll("some-channel") },
		shmevent.NewChannelListen,
		func() (shmevent.Msg, error) { return shmevent.NewChannelClose("some-channel") },
		func() (shmevent.Msg, error) { return shmevent.NewChannelCloseWrite("some-channel") },
		func() (shmevent.Msg, error) { return shmevent.NewChannelDataReady("some-channel") },
	}
	for _, build := range builders {
		m, err := build()
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		m.SetId(1)
		want := shmevent.EventName(m.Which())
		resp, err := callClientProtocol(ctx, remote, leaderPeerID, m, remotePriv)
		if err != nil {
			t.Fatalf("%s: %v", want, err)
		}
		if resp.Which() != shmevent.Event_Which_error {
			t.Fatalf("%s succeeded remotely, want rejection", want)
		}
	}
}

// TestChannelReapEvictsUnclaimedPendingChannel confirms channelTable.reap's
// opportunistic sweep actually evicts an incoming channel nobody ever
// claimed via channel_listen, once channelPendingTimeout has passed --
// shrinking that timeout instead of waiting the real 2 minutes.
func TestChannelReapEvictsUnclaimedPendingChannel(t *testing.T) {
	origTimeout := channelPendingTimeout
	channelPendingTimeout = 50 * time.Millisecond
	t.Cleanup(func() { channelPendingTimeout = origTimeout })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}

	// Give handleChannelStream a moment to register + queue the incoming
	// channel on b, then wait past the (shrunk) pending timeout without
	// ever calling channel_listen.
	time.Sleep(100 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Any subsequent Poll/Listen call opportunistically reaps -- confirm
	// the never-claimed channel is simply gone (channel_listen reports
	// nothing pending), not delivered late.
	listenMsg, err := shmevent.NewChannelListen()
	if err != nil {
		t.Fatalf("NewChannelListen: %v", err)
	}
	listenMsg.SetId(2)
	listenResp := callLocal(t, ctx, b, listenMsg, b.ed25519Priv)
	if listenResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_listen rejected: %s", mustErrMessage(t, listenResp))
	}
	gotID, err := listenResp.ChannelListen().ChannelId()
	if err != nil {
		t.Fatalf("ChannelListen channel_id: %v", err)
	}
	if gotID != "" {
		t.Fatalf("reaped channel was still delivered via channel_listen: %q", gotID)
	}
}

// TestChannelSessionPushPopPreservesPurposeAndOrder is a deterministic,
// struct-level regression test for channelSession's inbox: pushes several
// purpose-tagged chunks and confirms popChunk drains them in the same
// FIFO order, each with its purpose and data intact and unsplit -- unlike
// this package's earlier raw-pipe design, one pushChunk call always
// corresponds to exactly one popChunk call (see channelMaxChunkSize's doc
// comment for why there's nothing left to split).
func TestChannelSessionPushPopPreservesPurposeAndOrder(t *testing.T) {
	sess := newChannelSession("test-channel", nil, "remote-peer", nil, nil, newQuotaTracker(0, 0, 0, 0), "")

	pushed := []struct {
		purpose byte
		data    []byte
	}{
		{shmevent.ChannelPurposeData, []byte("first")},
		{shmevent.ChannelPurposeControl, []byte("second")},
		{shmevent.ChannelPurposeVideo, []byte("third")},
	}
	for _, p := range pushed {
		sess.pushChunk(p.purpose, p.data)
	}

	for i, want := range pushed {
		purpose, chunk, ok := sess.popChunk()
		if !ok {
			t.Fatalf("popChunk %d: no entry, want %q", i, want.data)
		}
		if purpose != want.purpose {
			t.Fatalf("popChunk %d: purpose %d, want %d", i, purpose, want.purpose)
		}
		if string(chunk) != string(want.data) {
			t.Fatalf("popChunk %d: chunk %q, want %q", i, chunk, want.data)
		}
	}

	if _, _, ok := sess.popChunk(); ok {
		t.Fatal("popChunk unexpectedly returned an entry after the inbox was drained")
	}
}

// TestChannelBurstSendStaysUnderPollResponseLimit fires several
// channelSend calls back-to-back at b with no poll from b in between
// (burst load, receiver polls slower than sender sends), confirming no
// individual poll ever errors and the full concatenated result exactly
// matches what a sent, in order and undamaged -- a general burst/ordering
// regression, independent of any one chunk's size (each send here is
// already its own signed wire frame -- see ChannelProtocolID's doc
// comment -- so there's no raw-byte read-coalescing to guard against the
// way this package's earlier raw-pipe design had to).
func TestChannelBurstSendStaysUnderPollResponseLimit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)

	// Fire every send before b ever polls, each its own frame well within
	// channelMaxChunkSize.
	const sendCount = 12
	const sendSize = 400
	var want []byte
	for i := range sendCount {
		chunk := make([]byte, sendSize)
		for j := range chunk {
			chunk[j] = byte((i*sendSize + j) % 256)
		}
		want = append(want, chunk...)
		sendResp := callLocal(t, ctx, a, channelSendMsg(t, aChannelID, shmevent.ChannelPurposeData, chunk, uint16(2+i)), a.ed25519Priv)
		if sendResp.Which() == shmevent.Event_Which_error {
			t.Fatalf("channel_send %d rejected: %s", i, mustErrMessage(t, sendResp))
		}
	}

	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	var got []byte
	deadline := time.After(10 * time.Second)
	for len(got) < len(want) {
		status, _, chunk := pollChannel(t, ctx, b, bChannelID)
		if len(chunk) > channelMaxChunkSize {
			t.Fatalf("poll returned %d bytes, want <= %d", len(chunk), channelMaxChunkSize)
		}
		got = append(got, chunk...)
		if status == shmevent.ChannelPollChunk {
			continue
		}
		select {
		case <-deadline:
			t.Fatalf("only received %d/%d bytes before deadline", len(got), len(want))
		case <-time.After(5 * time.Millisecond):
		}
	}

	if len(got) != len(want) {
		t.Fatalf("received %d bytes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("byte %d mismatch: got %x, want %x", i, got[i], want[i])
		}
	}
}

// TestChannelQuotaExhaustionClosesSession confirms pumpChannelReads closes
// a channelSession once the receiving node's channelQuota denies an
// inbound chunk -- the group-ACL gate (grantChannelAccess) still passes,
// but a's chunk exceeds b's tiny configured per-peer burst budget, so it
// must still be rejected and the session torn down, exactly like an
// oversized or forged frame already is.
func TestChannelQuotaExhaustionClosesSession(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))

	bDir := filepath.Join(tmpDir, "b")
	if err := os.MkdirAll(bDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", bDir, err)
	}
	bKeyPath := filepath.Join(bDir, "identity.key")
	if _, err := p2praft.LoadOrGenerateKey(bKeyPath); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	b, err := start(Config{
		DataDir: bDir, KeyPath: bKeyPath,
		// A burst well under the chunk size sent below -- AllowN denies
		// outright when cost exceeds the bucket's total capacity,
		// regardless of refill rate, so this trips on the very first
		// oversized chunk with no need to wait out a refill window.
		QuotaChannelBytesPerPeerPerSec: 1,
		QuotaChannelBurstPerPeer:       5,
	})
	if err != nil {
		t.Fatalf("start b: %v", err)
	}
	t.Cleanup(b.shutdown)

	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, channelOpenMsg(t, b.peerID, 1), a.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open rejected: %s", mustErrMessage(t, openResp))
	}
	aChannelID := channelIDFromOpenResp(t, openResp)

	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	// Well over b's 5-byte burst budget -- denied on arrival regardless of
	// a's own (unlimited) outbound quota, which only gates a's own send.
	sendResp := callLocal(t, ctx, a, channelSendMsg(t, aChannelID, shmevent.ChannelPurposeData, []byte("this chunk is over budget"), 2), a.ed25519Priv)
	if sendResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_send rejected on a's own side: %s", mustErrMessage(t, sendResp))
	}

	deadline := time.After(10 * time.Second)
	for {
		sess, ok := b.channels.get(bChannelID)
		if !ok {
			t.Fatal("b's channel session vanished instead of being marked closed")
		}
		if closed, reason := sess.status(); closed {
			if !strings.Contains(reason, "quota") {
				t.Fatalf("session closed for %q, want a reason mentioning quota", reason)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("b's channel session was never closed by the quota gate")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
