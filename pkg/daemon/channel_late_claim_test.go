package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// waitForReadPumpToFinish blocks until n has exactly one channel session
// and that session's read pump has ended -- i.e. the remote peer closed
// its write side and pumpChannelReads returned. That is the moment the
// down ring stops being written to, and the moment this bug used to
// unlink it.
func waitForReadPumpToFinish(t *testing.T, n *Node) *channelSession {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		n.channels.mu.Lock()
		var only *channelSession
		for _, s := range n.channels.sessions {
			only = s
		}
		count := len(n.channels.sessions)
		n.channels.mu.Unlock()

		if count == 1 && only != nil {
			if closed, _ := only.status(); closed {
				return only
			}
		}
		select {
		case <-deadline:
			t.Fatalf("the receiving node's read pump never finished (%d sessions)", count)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestChannelClaimedAfterSenderFinishedStillDelivers is the regression
// test for a race that made a whole class of transfers fail at random.
//
// The receiving side of a channel need not be listening when the sender
// starts: an incoming channel sits in the pending queue until some local
// caller claims it (EventChannelListen). A sender that opens a channel,
// pushes a small payload and half-closes can therefore finish entirely
// before anyone claims it -- which is the *normal* shape of a one-shot
// upload to a service that only starts listening once its own request
// arrives.
//
// pumpChannelReads used to release the down ring's *storage* when it
// returned, which is exactly then. The pending entry stayed claimable,
// so EventChannelListen went on handing out a channel id whose ring had
// already been unlinked, and the claiming caller sat in chandata.Open
// retrying a name that could never resolve until its context expired:
// "chandata: waiting for down ring kvchan-<peer>-<id>-down: context
// deadline exceeded". Whether it happened came down to whether the
// claimer got there before the sender finished; against two local nodes
// it lost that race about half the time.
//
// So: send everything, wait until the pump has genuinely finished, and
// only then claim. What arrived must still be readable, in order, and
// the channel must then report itself closed.
func TestChannelClaimedAfterSenderFinishedStillDelivers(t *testing.T) {
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

	// Comfortably inside one ring's capacity, so nothing is dropped
	// while the ring sits unattended -- this test is about the ring
	// surviving at all, not about what happens when it overflows (see
	// TestChannelRefusesToAttachAfterDroppingData).
	want := make([]byte, 200*1024)
	for i := range want {
		want[i] = byte(i % 251)
	}
	for sent := 0; sent < len(want); sent += 64 * 1024 {
		end := min(sent+64*1024, len(want))
		if err := sessA.SendChannel(ctx, channelID, shmevent.ChannelPurposeData, want[sent:end]); err != nil {
			t.Fatalf("SendChannel: %v", err)
		}
	}
	if err := sessA.CloseChannelWrite(ctx, channelID); err != nil {
		t.Fatalf("CloseChannelWrite: %v", err)
	}

	// The whole point: claim only after the sender is done and gone.
	waitForReadPumpToFinish(t, b)

	bChannelID, remotePeerID := listenChannelSessionUntilClaimed(t, ctx, sessB)
	if remotePeerID != a.peerID {
		t.Fatalf("listen reported remote peer %q, want %q", remotePeerID, a.peerID)
	}

	var got []byte
	deadline := time.After(30 * time.Second)
	for len(got) < len(want) {
		chunk, _, status, err := sessB.PollChannel(ctx, bChannelID)
		if err != nil {
			t.Fatalf("PollChannel: %v", err)
		}
		switch status {
		case shmclient.ChannelChunk:
			got = append(got, chunk...)
		case shmclient.ChannelClosed:
			t.Fatalf("channel reported closed after %d/%d bytes", len(got), len(want))
		}
		select {
		case <-deadline:
			t.Fatalf("only received %d/%d bytes before deadline", len(got), len(want))
		default:
		}
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("byte %d differs: got %d, want %d", i, got[i], want[i])
		}
	}

	// And having drained it, the reader sees the close the sender made
	// long before it ever claimed the channel.
	for {
		_, _, status, err := sessB.PollChannel(ctx, bChannelID)
		if err != nil {
			t.Fatalf("PollChannel after drain: %v", err)
		}
		if status == shmclient.ChannelClosed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("a fully drained channel whose sender closed never reported itself closed")
		default:
		}
	}
}

// TestChannelRingStorageIsReleasedWithTheSession is the other half of the
// same change: the down ring now outlives its writer, so something else
// has to release it. Everything that ends a session goes through
// channelTable.evict, and this pins that a session ended by the local
// caller really does take its shared-memory segment with it -- otherwise
// the fix above would trade a race for a leak.
func TestChannelRingStorageIsReleasedWithTheSession(t *testing.T) {
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
	bChannelID, _ := listenChannelSessionUntilClaimed(t, ctx, sessB)

	// While the session is alive the ring is there to be opened, however
	// many times.
	openCtx, openCancel := context.WithTimeout(ctx, 2*time.Second)
	r, err := chandata.Open(openCtx, b.peerID, bChannelID, chandata.DirDown)
	openCancel()
	if err != nil {
		t.Fatalf("the down ring should be open-able while the session lives: %v", err)
	}
	r.Close()

	if err := sessB.CloseChannel(ctx, bChannelID); err != nil {
		t.Fatalf("CloseChannel: %v", err)
	}

	goneCtx, goneCancel := context.WithTimeout(ctx, 2*time.Second)
	defer goneCancel()
	if r, err := chandata.Open(goneCtx, b.peerID, bChannelID, chandata.DirDown); err == nil {
		r.Close()
		t.Fatal("the down ring's storage should be gone once its session is closed")
	}
	_ = sessA
	_ = channelID
}

// TestChannelRefusesToAttachAfterDroppingData covers what the surviving
// ring cannot fix. A ring holds chandata.Capacity; a sender that outruns
// that while nobody is draining loses the excess (see
// downRingWriteTimeout -- the chunks stay in the session's inbox for the
// legacy poll path, but never reach the ring). A caller attaching
// afterwards would read a stream that skips them and then ends cleanly,
// with nothing to say anything was missing.
//
// So attaching is refused instead, and the refusal says what happened.
// Losing an upload is bad; losing part of one silently is worse.
func TestChannelRefusesToAttachAfterDroppingData(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

	// Half again as much as one ring holds, with nothing claiming the
	// channel to drain it.
	payload := make([]byte, chandata.Capacity+chandata.Capacity/2)
	for sent := 0; sent < len(payload); sent += chandata.MaxChunkSize {
		end := min(sent+chandata.MaxChunkSize, len(payload))
		if err := sessA.SendChannel(ctx, channelID, shmevent.ChannelPurposeData, payload[sent:end]); err != nil {
			t.Fatalf("SendChannel: %v", err)
		}
	}
	if err := sessA.CloseChannelWrite(ctx, channelID); err != nil {
		t.Fatalf("CloseChannelWrite: %v", err)
	}
	sess := waitForReadPumpToFinish(t, b)
	if lost := sess.downWasLost(); lost == 0 {
		t.Fatal("expected the unattended ring to have dropped chunks; it did not, so this test is no longer testing anything")
	}

	_, _, _, err = sessB.ListenChannel(ctx)
	if err == nil {
		t.Fatal("claiming a channel whose data was partly dropped should be refused, not served silently")
	}
	if !containsAll(err.Error(), "dropped", "chunk") {
		t.Fatalf("the refusal should say what happened, got: %v", err)
	}
}

func containsAll(haystack string, needles ...string) bool {
	for _, n := range needles {
		found := false
		for i := 0; i+len(n) <= len(haystack); i++ {
			if haystack[i:i+len(n)] == n {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
