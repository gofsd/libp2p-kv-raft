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
func pollChannel(t *testing.T, ctx context.Context, n *Node, channelID string) (status byte, chunk []byte) {
	t.Helper()
	resp := callLocal(t, ctx, n, shmevent.Msg{EventType: shmevent.EventChannelPoll, Value: []byte(channelID), ID: 1}, n.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("channel_poll rejected: %s", resp.Value)
	}
	status, chunk, err := shmevent.DecodeChannelPollResponse(resp.Value)
	if err != nil {
		t.Fatalf("DecodeChannelPollResponse: %v", err)
	}
	return status, chunk
}

// pollChannelUntilChunk polls channelID on n until a chunk arrives or
// deadline passes, mirroring TestExecuteEventDeliversAcrossNodes' own
// poll loop.
func pollChannelUntilChunk(t *testing.T, ctx context.Context, n *Node, channelID string) []byte {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		status, chunk := pollChannel(t, ctx, n, channelID)
		if status == shmevent.ChannelPollChunk {
			return chunk
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
		Value:     mustEncodeChannelSend(t, aChannelID, []byte("hello from a")),
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
		Value:     mustEncodeChannelSend(t, bChannelID, []byte("hello from b")),
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
		status, _ := pollChannel(t, ctx, b, bChannelID)
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
	status, _ := pollChannel(t, ctx, b, bChannelID)
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
		Value:     mustEncodeChannelSend(t, bChannelID, []byte("still here")),
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
		status, _ := pollChannel(t, ctx, a, aChannelID)
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

func mustEncodeChannelSend(t *testing.T, channelID string, chunk []byte) []byte {
	t.Helper()
	payload, err := shmevent.EncodeChannelSendPayload(channelID, chunk)
	if err != nil {
		t.Fatalf("EncodeChannelSendPayload: %v", err)
	}
	return payload
}
