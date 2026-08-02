package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// pollChannel is a small test helper wrapping EventChannelPoll's
// decode step -- used throughout below to avoid repeating
// DecodeChannelPollResponse's error handling at every call site.
func pollChannel(t *testing.T, ctx context.Context, n *Node, channelID string) (status, purpose byte, chunk []byte) {
	t.Helper()
	resp := callLocal(t, ctx, n, shmevent.Msg{EventType: shmevent.EventChannelPoll, Value: []byte(channelID), ID: 1}, n.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("channel_poll rejected: %s", resp.Value)
	}
	status, purpose, chunk, err := shmevent.DecodeChannelPollResponse(resp.Value)
	if err != nil {
		t.Fatalf("DecodeChannelPollResponse: %v", err)
	}
	return status, purpose, chunk
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

// listenChannelUntilClaimed polls EventChannelListen on n until an
// incoming channel is claimed or deadline passes, returning its local
// channelID and the remote peer id it reports.
func listenChannelUntilClaimed(t *testing.T, ctx context.Context, n *Node) (channelID, remotePeerID string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		resp := callLocal(t, ctx, n, shmevent.Msg{EventType: shmevent.EventChannelListen, ID: 1}, n.ed25519Priv)
		if resp.EventType == shmevent.EventError {
			t.Fatalf("channel_listen rejected: %s", resp.Value)
		}
		if len(resp.Value) > 0 {
			gotID, gotPeer, err := shmevent.DecodeChannelAccept(resp.Value)
			if err != nil {
				t.Fatalf("DecodeChannelAccept: %v", err)
			}
			return string(gotID), string(gotPeer)
		}
		select {
		case <-deadline:
			t.Fatal("incoming channel never became listenable")
		case <-time.After(20 * time.Millisecond):
		}
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

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)
	if aChannelID == "" {
		t.Fatal("channel_open returned an empty channelID")
	}

	sendResp := callLocal(t, ctx, a, shmevent.Msg{
		EventType: shmevent.EventChannelSend,
		Value:     mustEncodeChannelSend(t, aChannelID, shmevent.ChannelPurposeData, []byte("hello from a")),
		ID:        2,
	}, a.ed25519Priv)
	if sendResp.EventType == shmevent.EventError {
		t.Fatalf("channel_send rejected: %s", sendResp.Value)
	}

	bChannelID, remotePeerID := listenChannelUntilClaimed(t, ctx, b)
	if remotePeerID != a.peerID {
		t.Fatalf("listen reported remote peer %q, want %q", remotePeerID, a.peerID)
	}

	gotChunk := pollChannelUntilChunk(t, ctx, b, bChannelID)
	if string(gotChunk) != "hello from a" {
		t.Fatalf("b received %q, want %q", gotChunk, "hello from a")
	}

	replyResp := callLocal(t, ctx, b, shmevent.Msg{
		EventType: shmevent.EventChannelSend,
		Value:     mustEncodeChannelSend(t, bChannelID, shmevent.ChannelPurposeData, []byte("hello from b")),
		ID:        3,
	}, b.ed25519Priv)
	if replyResp.EventType == shmevent.EventError {
		t.Fatalf("reply channel_send rejected: %s", replyResp.Value)
	}

	gotReply := pollChannelUntilChunk(t, ctx, a, aChannelID)
	if string(gotReply) != "hello from b" {
		t.Fatalf("a received %q, want %q", gotReply, "hello from b")
	}
}

// TestChannelPurposeCarriedEndToEnd is the central assertion this whole
// framed/purpose-tagged rewrite exists for: a purpose other than the
// default (ChannelPurposeVideo, ChannelPurposeControl) survives
// EncodeChannelSendPayload -> the signed network frame
// (EncodeChannelWireChunk) -> EncodeChannelPollResponse intact, in both
// directions.
func TestChannelPurposeCarriedEndToEnd(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)

	sendResp := callLocal(t, ctx, a, shmevent.Msg{
		EventType: shmevent.EventChannelSend,
		Value:     mustEncodeChannelSend(t, aChannelID, shmevent.ChannelPurposeVideo, []byte("frame bytes")),
		ID:        2,
	}, a.ed25519Priv)
	if sendResp.EventType == shmevent.EventError {
		t.Fatalf("channel_send rejected: %s", sendResp.Value)
	}

	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)
	gotChunk, gotPurpose := pollChannelUntilChunkWithPurpose(t, ctx, b, bChannelID)
	if string(gotChunk) != "frame bytes" {
		t.Fatalf("b received %q, want %q", gotChunk, "frame bytes")
	}
	if gotPurpose != shmevent.ChannelPurposeVideo {
		t.Fatalf("b received purpose %d, want %d (ChannelPurposeVideo)", gotPurpose, shmevent.ChannelPurposeVideo)
	}

	replyResp := callLocal(t, ctx, b, shmevent.Msg{
		EventType: shmevent.EventChannelSend,
		Value:     mustEncodeChannelSend(t, bChannelID, shmevent.ChannelPurposeControl, []byte("ack")),
		ID:        3,
	}, b.ed25519Priv)
	if replyResp.EventType == shmevent.EventError {
		t.Fatalf("reply channel_send rejected: %s", replyResp.Value)
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
// carry far more than the old, shared shmevent.ValueSize (512 bytes)
// every other event's Value is still capped at -- proving
// EventChannelSend/EventChannelPoll's own, much larger
// shmevent.ChannelValueSize ceiling (see that constant's doc comment)
// actually applies end to end: local IPC send, the signed network frame,
// and local IPC poll all carry a single chunk right up against
// shmevent.ChannelValueSize with no splitting. This exercises the legacy
// per-chunk IPC path (raw EventChannelSend/Poll, the same as every other
// test in this file) rather than the much larger pkg/chandata ring path
// (channelMaxChunkSize, see that constant's doc comment) -- that path's
// own near-ceiling coverage lives in pkg/shmclient instead, since it has
// no IPC-request-sized payload to stay under in the first place.
func TestChannelLargeChunkNearChannelMaxSize(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)

	// shmevent.ChannelValueSize (see that constant's doc comment) is
	// EventChannelSend/Poll's actual IPC ceiling; EncodeChannelSendPayload
	// adds its own channelID+purpose overhead on top of the raw chunk, so
	// the send payload here is that ceiling minus that overhead -- well
	// beyond the old 512-byte shmevent.ValueSize ceiling either way.
	sendOverhead := 2 + len(aChannelID) + 1 // EncodeChannelSendPayload's own packing
	want := make([]byte, shmevent.ChannelValueSize-sendOverhead)
	for i := range want {
		want[i] = byte(i % 256)
	}
	if len(want) <= shmevent.ValueSize {
		t.Fatalf("test chunk (%d bytes) isn't actually bigger than the old shared ValueSize (%d) -- test no longer proves anything", len(want), shmevent.ValueSize)
	}

	sendResp := callLocal(t, ctx, a, shmevent.Msg{
		EventType: shmevent.EventChannelSend,
		Value:     mustEncodeChannelSend(t, aChannelID, shmevent.ChannelPurposeData, want),
		ID:        2,
	}, a.ed25519Priv)
	if sendResp.EventType == shmevent.EventError {
		t.Fatalf("channel_send rejected: %s", sendResp.Value)
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

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)
	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	closeResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelClose, Value: []byte(aChannelID), ID: 2}, a.ed25519Priv)
	if closeResp.EventType == shmevent.EventError {
		t.Fatalf("channel_close rejected: %s", closeResp.Value)
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
// EventChannelClose right after a's own EOF would have cut b's reply off
// prematurely.
func TestChannelCloseWriteLeavesOtherDirectionOpen(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)
	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	// a is "done sending" (its stdin hit EOF) -- half-close, not close.
	closeWriteResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelCloseWrite, Value: []byte(aChannelID), ID: 2}, a.ed25519Priv)
	if closeWriteResp.EventType == shmevent.EventError {
		t.Fatalf("channel_close_write rejected: %s", closeWriteResp.Value)
	}

	// b must still be able to send a reply, and a must still receive it
	// -- the direction b->a was never touched by a's half-close.
	replyResp := callLocal(t, ctx, b, shmevent.Msg{
		EventType: shmevent.EventChannelSend,
		Value:     mustEncodeChannelSend(t, bChannelID, shmevent.ChannelPurposeData, []byte("still here")),
		ID:        3,
	}, b.ed25519Priv)
	if replyResp.EventType == shmevent.EventError {
		t.Fatalf("channel_send after peer's half-close rejected: %s", replyResp.Value)
	}
	if got := pollChannelUntilChunk(t, ctx, a, aChannelID); string(got) != "still here" {
		t.Fatalf("a received %q after its own half-close, want %q", got, "still here")
	}

	// Now b also finishes (its own EOF) -- a full close this time, since
	// b has nothing further to receive either. Only now should a observe
	// ChannelPollClosed.
	bCloseResp := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventChannelClose, Value: []byte(bChannelID), ID: 4}, b.ed25519Priv)
	if bCloseResp.EventType == shmevent.EventError {
		t.Fatalf("channel_close rejected: %s", bCloseResp.Value)
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

	notifValue, err := shmevent.EncodeExecuteNotification([]byte(a.peerID), nil)
	if err != nil {
		t.Fatalf("EncodeExecuteNotification: %v", err)
	}
	buf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: notifValue}, shmevent.PrivateKey(mustRaw(t, forgerPriv)))
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
	listenResp := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventChannelListen, ID: 1}, b.ed25519Priv)
	if listenResp.EventType == shmevent.EventError {
		t.Fatalf("channel_listen rejected: %s", listenResp.Value)
	}
	if len(listenResp.Value) != 0 {
		t.Fatalf("forged channel_open was accepted and became listenable: %q", listenResp.Value)
	}
}

// TestChannelDataFrameForgedAfterHandshakeRejected proves per-frame
// signature verification -- not just handshake-time verification --
// actually gates channel data: after a's real handshake with b succeeds,
// a frame written directly onto the stream but signed with an unrelated
// key must never be delivered via channel_poll. This is the whole point
// of framing every post-handshake message as a signed shmevent.Event
// instead of raw bytes (see ChannelProtocolID's doc comment) -- without
// per-frame verification, this same injection would have silently
// appeared as ordinary received bytes under this package's earlier
// raw-pipe design.
func TestChannelDataFrameForgedAfterHandshakeRejected(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)
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
	forgedBuf, err := shmevent.Encode(shmevent.Msg{
		EventType: shmevent.EventChannelSend,
		Value:     shmevent.EncodeChannelWireChunk(shmevent.ChannelPurposeData, []byte("forged")),
	}, shmevent.PrivateKey(mustRaw(t, forgerPriv)))
	if err != nil {
		t.Fatalf("Encode forged frame: %v", err)
	}
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

// TestRequirePermitForChannelGate mirrors TestRequirePermitForExecuteGate
// line-for-line, against handleChannelStream's identically-shaped gate
// instead of handleExecuteStream's.
func TestRequirePermitForChannelGate(t *testing.T) {
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
	b, err := start(Config{DataDir: bDir, KeyPath: bKeyPath, RequirePermitForChannel: true})
	if err != nil {
		t.Fatalf("start b: %v", err)
	}
	t.Cleanup(b.shutdown)

	connectPeers(t, ctx, a, b)

	open := func(id uint16) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: id}, a.ed25519Priv)
	}

	// Neither permitted nor a cluster member: dispatchChannelOpen reads
	// b's reject response synchronously, so the gate's rejection on b
	// surfaces straight back as a local error on a.
	if resp := open(1); resp.EventType != shmevent.EventError {
		t.Fatal("channel_open from an unpermitted, non-member sender unexpectedly succeeded")
	}

	// Grant a a confirmed KindPermitPeer record directly in b's store.
	aPeerID, err := peer.Decode(a.peerID)
	if err != nil {
		t.Fatalf("decode a peer id: %v", err)
	}
	permitKey := shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusConfirmed, []byte(aPeerID.String()))
	if err := b.store.Set(permitKey, nil); err != nil {
		t.Fatalf("grant permit: %v", err)
	}
	if resp := open(2); resp.EventType == shmevent.EventError {
		t.Fatalf("channel_open from a permitted sender rejected: %s", resp.Value)
	}

	// Revoke it again and confirm a KindClusterMember record alone is
	// sufficient (the cluster-member exemption), independent of the
	// permit.
	if err := b.store.Delete(permitKey); err != nil {
		t.Fatalf("revoke permit: %v", err)
	}
	if resp := open(3); resp.EventType != shmevent.EventError {
		t.Fatal("channel_open from a sender with a revoked permit and no cluster membership unexpectedly succeeded")
	}

	memberKey := shmevent.ClusterMemberKey([]byte(aPeerID.String()))
	memberPayload := shmevent.EncodeClusterMemberPayload(a.ed25519Pub, shmevent.RoleVoter)
	if err := b.store.Set(memberKey, memberPayload); err != nil {
		t.Fatalf("record cluster member: %v", err)
	}
	if resp := open(4); resp.EventType == shmevent.EventError {
		t.Fatalf("channel_open from a cluster member rejected: %s", resp.Value)
	}
}

// TestChannelEventsRejectRemoteCaller mirrors
// TestClientProtocolRejectsRemoteKeyFetch's loop-over-events shape: none
// of the 5 channel events are available to a remote (ClientProtocolID)
// caller, only this node's own local operator.
func TestChannelEventsRejectRemoteCaller(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	events := []uint8{
		shmevent.EventChannelOpen,
		shmevent.EventChannelSend,
		shmevent.EventChannelPoll,
		shmevent.EventChannelListen,
		shmevent.EventChannelClose,
		shmevent.EventChannelCloseWrite,
		shmevent.EventChannelDataReady,
	}
	for _, evt := range events {
		resp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{EventType: evt, ID: 1}, remotePriv)
		if err != nil {
			t.Fatalf("%s: %v", shmevent.EventName(evt), err)
		}
		if resp.EventType != shmevent.EventError {
			t.Fatalf("%s succeeded remotely, want rejection", shmevent.EventName(evt))
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

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}

	// Give handleChannelStream a moment to register + queue the incoming
	// channel on b, then wait past the (shrunk) pending timeout without
	// ever calling channel_listen.
	time.Sleep(100 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Any subsequent Poll/Listen call opportunistically reaps -- confirm
	// the never-claimed channel is simply gone (channel_listen reports
	// nothing pending), not delivered late.
	listenResp := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventChannelListen, ID: 2}, b.ed25519Priv)
	if listenResp.EventType == shmevent.EventError {
		t.Fatalf("channel_listen rejected: %s", listenResp.Value)
	}
	if len(listenResp.Value) != 0 {
		t.Fatalf("reaped channel was still delivered via channel_listen: %q", listenResp.Value)
	}
}

func mustEncodeChannelSend(t *testing.T, channelID string, purpose byte, chunk []byte) []byte {
	t.Helper()
	payload, err := shmevent.EncodeChannelSendPayload(channelID, purpose, chunk)
	if err != nil {
		t.Fatalf("EncodeChannelSendPayload: %v", err)
	}
	return payload
}

// TestChannelSessionPushPopPreservesPurposeAndOrder is a deterministic,
// struct-level regression test for channelSession's inbox: pushes several
// purpose-tagged chunks and confirms popChunk drains them in the same
// FIFO order, each with its purpose and data intact and unsplit -- unlike
// this package's earlier raw-pipe design, one pushChunk call always
// corresponds to exactly one popChunk call (see channelMaxChunkSize's doc
// comment for why there's nothing left to split).
func TestChannelSessionPushPopPreservesPurposeAndOrder(t *testing.T) {
	sess := newChannelSession("test-channel", nil, "remote-peer", nil, nil)

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
// EventChannelSend calls back-to-back at b with no poll from b in between
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

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)

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
		sendResp := callLocal(t, ctx, a, shmevent.Msg{
			EventType: shmevent.EventChannelSend,
			Value:     mustEncodeChannelSend(t, aChannelID, shmevent.ChannelPurposeData, chunk),
			ID:        uint16(2 + i),
		}, a.ed25519Priv)
		if sendResp.EventType == shmevent.EventError {
			t.Fatalf("channel_send %d rejected: %s", i, sendResp.Value)
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
