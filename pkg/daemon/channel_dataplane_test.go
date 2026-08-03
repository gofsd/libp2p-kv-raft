package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// startChannelDataplaneTestNode is startExecuteTestNode plus a real
// pkg/ipc.Serve loop -- unlike callLocal (every other test in this
// package), pkg/shmclient.Session's channel methods go over real
// pkg/chandata shared-memory rings and real pkg/ipc named shmring
// segments, so exercising them for real needs an actual Serve loop
// running, not just a direct handleShmEvent call. Registers n's peer id
// against dir in the test registry TestMain points registry.EnvHome at --
// pkg/ipc.Call/CallRaw (what shmclient.Open(ctx, n.peerID) ultimately
// drives) now resolves a peer id to its own local-IPC token via that same
// registry lookup (see pkg/ipc/token.go's tokenForPeer), the same way a
// real daemon spawned through pkg/kvctl.AddNodeWithArgs would already be
// registered by the time anything calls it.
func startChannelDataplaneTestNode(t *testing.T, dir string) *Node {
	t.Helper()
	n := startExecuteTestNode(t, dir)
	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	if err := reg.Put(registry.NodeInfo{PeerID: n.peerID, DataDir: dir}); err != nil {
		t.Fatalf("registry.Put: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ipc.Serve(ctx, n.peerID, dir, n.ed25519Priv, func(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg {
		return n.handleShmEvent(ctx, m, crc, sig, n.localCaller())
	})
	return n
}

// listenChannelSessionUntilClaimed polls sess.ListenChannel until an
// incoming channel is claimed or deadline passes -- the pkg/shmclient
// counterpart to channel_test.go's own listenChannelUntilClaimed (which
// drives dispatchChannelListen directly via callLocal instead).
func listenChannelSessionUntilClaimed(t *testing.T, ctx context.Context, sess *shmclient.Session) (channelID, remotePeerID string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		id, remote, ok, err := sess.ListenChannel(ctx)
		if err != nil {
			t.Fatalf("ListenChannel: %v", err)
		}
		if ok {
			return id, remote
		}
		select {
		case <-deadline:
			t.Fatal("incoming channel never became listenable")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestChannelDataplaneRingBulkTransferAndDrain drives pkg/shmclient's
// ring-backed channel path end to end, against two real nodes each with
// a real IPC loop, proving the properties the pkg/chandata rewrite exists
// for: a payload much larger than the old 16KB IPC ceiling and larger
// than a single pkg/chandata ring's own capacity (so the ring genuinely
// wraps and exercises backpressure, not just a one-shot write) arrives
// byte-for-byte and in order; purpose tags survive; and
// CloseChannelWrite's drain guarantee actually holds -- calling it
// immediately after the last SendChannel, with no artificial delay for
// pumpChannelUpload to "catch up" on its own, must never lose trailing
// data (see shmevent.EventChannelDataReady's doc comment on why that
// guarantee needs dispatchChannelCloseWrite's explicit wait).
func TestChannelDataplaneRingBulkTransferAndDrain(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startChannelDataplaneTestNode(t, filepath.Join(tmpDir, "a"))
	b := startChannelDataplaneTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	sessA, err := shmclient.Open(ctx, a.peerID)
	if err != nil {
		t.Fatalf("shmclient.Open(a): %v", err)
	}
	sessB, err := shmclient.Open(ctx, b.peerID)
	if err != nil {
		t.Fatalf("shmclient.Open(b): %v", err)
	}

	channelID, err := sessA.OpenChannel(ctx, b.peerID)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	bChannelID, remotePeerID := listenChannelSessionUntilClaimed(t, ctx, sessB)
	if remotePeerID != a.peerID {
		t.Fatalf("listen reported remote peer %q, want %q", remotePeerID, a.peerID)
	}

	// Bigger than chandata.Capacity so the ring genuinely wraps at least
	// twice, not just a single buffered write.
	const payloadSize = 3*chandata.Capacity + 12345
	want := make([]byte, payloadSize)
	for i := range want {
		want[i] = byte(i % 256)
	}

	sendDone := make(chan error, 1)
	go func() {
		for sent := 0; sent < len(want); {
			n := chandata.MaxChunkSize
			if remaining := len(want) - sent; n > remaining {
				n = remaining
			}
			purpose := shmevent.ChannelPurposeData
			if sent == 0 {
				purpose = shmevent.ChannelPurposeVideo // exercise a non-default purpose on the first chunk
			}
			if err := sessA.SendChannel(ctx, channelID, purpose, want[sent:sent+n]); err != nil {
				sendDone <- err
				return
			}
			sent += n
		}
		// No artificial delay here -- this is the exact scenario
		// dispatchChannelCloseWrite's drain wait exists for: the upload
		// ring may still have unread bytes in it the instant this runs.
		sendDone <- sessA.CloseChannelWrite(ctx, channelID)
	}()

	var got []byte
	var firstPurpose byte
	sawFirst := false
	deadline := time.After(30 * time.Second)
	for len(got) < len(want) {
		chunk, purpose, status, err := sessB.PollChannel(ctx, bChannelID)
		if err != nil {
			t.Fatalf("PollChannel: %v", err)
		}
		switch status {
		case shmclient.ChannelChunk:
			if !sawFirst {
				firstPurpose = purpose
				sawFirst = true
			}
			got = append(got, chunk...)
		case shmclient.ChannelClosed:
			t.Fatalf("channel closed early after %d/%d bytes", len(got), len(want))
		default:
		}
		select {
		case <-deadline:
			t.Fatalf("only received %d/%d bytes before deadline", len(got), len(want))
		default:
		}
	}

	if err := <-sendDone; err != nil {
		t.Fatalf("send side: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("received %d bytes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("byte %d mismatch: got %x, want %x", i, got[i], want[i])
		}
	}
	if firstPurpose != shmevent.ChannelPurposeVideo {
		t.Fatalf("first chunk purpose = %d, want %d (ChannelPurposeVideo)", firstPurpose, shmevent.ChannelPurposeVideo)
	}

	// a already half-closed (CloseChannelWrite above); b still finishes
	// normally, only now should a observe ChannelPollClosed.
	if err := sessB.CloseChannel(ctx, bChannelID); err != nil {
		t.Fatalf("b CloseChannel: %v", err)
	}
	closeDeadline := time.After(10 * time.Second)
	for {
		_, _, status, err := sessA.PollChannel(ctx, channelID)
		if err != nil {
			t.Fatalf("a PollChannel: %v", err)
		}
		if status == shmclient.ChannelClosed {
			break
		}
		select {
		case <-closeDeadline:
			t.Fatal("a never observed the channel as closed after b's close")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err := sessA.CloseChannel(ctx, channelID); err != nil {
		t.Fatalf("a CloseChannel: %v", err)
	}
}
