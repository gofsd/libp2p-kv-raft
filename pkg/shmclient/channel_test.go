package shmclient

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
)

// newTestChannelPipe builds a real channelPipe backed by an actual
// pkg/chandata ring pair, plus loopback handles standing in for what
// pkg/daemon normally owns on the other end of each ring (see
// setupChannelData's doc comment for the real (local-caller, daemon)
// pairing this mirrors): upDrain reads whatever SendChannel writes into
// up, and downFeed writes whatever PollChannel is meant to read out of
// down. The three returned cleanup values must all be released by the
// caller once done -- close (if the test calls pipe.close() itself) does
// not touch upDrain/downFeed, only pipe's own up/down.
func newTestChannelPipe(t *testing.T) (pipe *channelPipe, upDrain *chandata.ChunkReader, downFeed *chandata.ChunkWriter) {
	t.Helper()
	peerID := fmt.Sprintf("shmclient-test-%d", time.Now().UnixNano())
	channelID := "chan-1"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stand-in for the daemon's side of the data plane: it creates the
	// down ring and reads (drains) the up ring.
	downFeed, err := chandata.Create(peerID, channelID, chandata.DirDown)
	if err != nil {
		t.Fatalf("create down ring: %v", err)
	}
	t.Cleanup(func() { downFeed.CloseStorage() })

	// This session's own side, exactly as setupChannelData builds it.
	up, err := chandata.Create(peerID, channelID, chandata.DirUp)
	if err != nil {
		t.Fatalf("create up ring: %v", err)
	}
	upDrain, err = chandata.Open(ctx, peerID, channelID, chandata.DirUp)
	if err != nil {
		t.Fatalf("open up drain: %v", err)
	}
	t.Cleanup(func() { upDrain.Close() })

	down, err := chandata.Open(ctx, peerID, channelID, chandata.DirDown)
	if err != nil {
		t.Fatalf("open down: %v", err)
	}

	return newChannelPipe(up, down), upDrain, downFeed
}

// TestMergeCancel checks mergeCancel's three documented properties in
// isolation: the merged context is done when either input is, and the
// returned release function tears down the internal AfterFunc association
// without reaching back to cancel the original contexts -- SendChannel/
// PollChannel both defer this release on every call, so a caller-context
// leak here would show up as a slow context.AfterFunc buildup across the
// lifetime of a long-lived Session.
func TestMergeCancel(t *testing.T) {
	t.Run("cancel via a", func(t *testing.T) {
		a, cancelA := context.WithCancel(context.Background())
		defer cancelA()
		merged, release := mergeCancel(a, context.Background())
		defer release()
		cancelA()
		select {
		case <-merged.Done():
		case <-time.After(time.Second):
			t.Fatal("merged context did not cancel when a was cancelled")
		}
	})

	t.Run("cancel via b", func(t *testing.T) {
		b, cancelB := context.WithCancel(context.Background())
		defer cancelB()
		merged, release := mergeCancel(context.Background(), b)
		defer release()
		cancelB()
		select {
		case <-merged.Done():
		case <-time.After(time.Second):
			t.Fatal("merged context did not cancel when b was cancelled")
		}
	})

	t.Run("release does not cancel the original contexts", func(t *testing.T) {
		a, cancelA := context.WithCancel(context.Background())
		defer cancelA()
		b, cancelB := context.WithCancel(context.Background())
		defer cancelB()
		merged, release := mergeCancel(a, b)
		release()
		if merged.Err() == nil {
			t.Fatal("merged context should be done once released")
		}
		if a.Err() != nil {
			t.Fatal("release() must not cancel a")
		}
		if b.Err() != nil {
			t.Fatal("release() must not cancel b")
		}
	})
}

// TestChannelPipeCloseWaitsForInFlightEnter pins down, deterministically
// (no reliance on the race detector or goroutine scheduling luck), the
// ordering channelPipe.close's own doc comment promises: it must block
// until every already-enter()'d call has left before it touches up/down,
// and any enter() attempted after close has started must fail fast.
func TestChannelPipeCloseWaitsForInFlightEnter(t *testing.T) {
	pipe, _, _ := newTestChannelPipe(t)

	if !pipe.enter() {
		t.Fatal("enter() should succeed before close")
	}

	closeReturned := make(chan struct{})
	go func() {
		pipe.close()
		close(closeReturned)
	}()

	select {
	case <-closeReturned:
		t.Fatal("close() returned before the in-flight enter()'d call left")
	case <-time.After(50 * time.Millisecond):
	}

	// A concurrent enter() while close() is already waiting must fail --
	// close() has already marked the pipe closed even though it hasn't
	// finished tearing it down yet.
	if pipe.enter() {
		t.Fatal("enter() succeeded while close() was still waiting for the earlier call to leave")
	}

	pipe.leave()

	select {
	case <-closeReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("close() did not return after the in-flight call left")
	}

	if pipe.enter() {
		t.Fatal("enter() succeeded after close() completed")
	}
}

// TestChannelPipeSendPollRaceWithClose is the audit's actual target: many
// goroutines calling SendChannel/PollChannel concurrently against one
// channel while another goroutine closes it out from under them. Run with
// -race, this is what would catch a bug in the enter/leave/close
// coordination channelPipe's own doc comment describes -- e.g. close()
// releasing up/down while a Send/Poll call is still using them, or a
// Send/Poll call started after close() began still reaching them. The
// property under test isn't any particular return value (SendChannel/
// PollChannel are expected to start failing once close() begins); it's
// that this runs to completion cleanly under the race detector with no
// panic and no goroutine left hanging.
func TestChannelPipeSendPollRaceWithClose(t *testing.T) {
	peerID := fmt.Sprintf("shmclient-race-%d", time.Now().UnixNano())
	channelID := "chan-1"

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer setupCancel()

	downFeed, err := chandata.Create(peerID, channelID, chandata.DirDown)
	if err != nil {
		t.Fatalf("create down ring: %v", err)
	}
	defer downFeed.CloseStorage()

	up, err := chandata.Create(peerID, channelID, chandata.DirUp)
	if err != nil {
		t.Fatalf("create up ring: %v", err)
	}
	upDrain, err := chandata.Open(setupCtx, peerID, channelID, chandata.DirUp)
	if err != nil {
		t.Fatalf("open up drain: %v", err)
	}
	defer upDrain.Close()

	down, err := chandata.Open(setupCtx, peerID, channelID, chandata.DirDown)
	if err != nil {
		t.Fatalf("open down: %v", err)
	}

	pipe := newChannelPipe(up, down)
	sess := &Session{peerID: peerID, channels: map[string]*channelPipe{channelID: pipe}}

	// Background loopback plumbing so Send/Poll have somewhere to make
	// real progress: drain whatever SendChannel writes, and keep feeding
	// PollChannel something to read.
	feedCtx, feedCancel := context.WithCancel(context.Background())
	defer feedCancel()
	var bgWG sync.WaitGroup
	bgWG.Add(2)
	go func() {
		defer bgWG.Done()
		for {
			if err := downFeed.WriteChunk(feedCtx, 0, []byte("x")); err != nil {
				return
			}
		}
	}()
	go func() {
		defer bgWG.Done()
		for {
			if _, _, err := upDrain.ReadChunk(feedCtx); err != nil {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Exactly one sending goroutine and one polling goroutine -- matching
	// what channelPipe's own doc comment actually claims to make safe
	// ("mobile/kvmobile... hands SendChannelData/PollChannel-driving code
	// and CloseChannel out to Kotlin, which is free to call them from
	// different threads concurrently"): one thread driving each direction
	// plus a concurrent Close, not multiple concurrent writers sharing one
	// ChunkWriter -- chandata.ChunkWriter/ChunkReader are each documented
	// single-goroutine-at-a-time in their own right, a constraint
	// channelPipe was never meant to paper over. (An earlier version of
	// this test ran several concurrent senders against one ChunkWriter and
	// the race detector correctly flagged that as unsafe -- it was a bug
	// in the test's concurrency model, not in channelPipe.)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = sess.SendChannel(context.Background(), channelID, 0, []byte("payload"))
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _, _, _ = sess.PollChannel(context.Background(), channelID)
		}
	}()

	// Let Send/Poll race for a bit, then close the pipe out from under
	// them -- exactly the concurrent-close-vs-in-flight-I/O scenario
	// channelPipe exists to make safe.
	time.Sleep(30 * time.Millisecond)
	pipe.close()

	close(stop)
	wg.Wait()

	feedCancel()
	bgWG.Wait()

	// Every Send/Poll call must have observed the channel as closing by
	// now -- a lingering successful call here would mean close() returned
	// without actually having waited for/blocked new entrants.
	if pipe.enter() {
		t.Fatal("enter() succeeded after the race loop finished and close() had returned")
	}
}

// TestSendPollUnknownChannel checks the ordinary "no such channel" path
// (a caller passing a channelID this Session never set up) doesn't panic
// or block -- the fast-fail half of the same lookup the concurrency tests
// above exercise under contention.
func TestSendPollUnknownChannel(t *testing.T) {
	sess := &Session{channels: map[string]*channelPipe{}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := sess.SendChannel(ctx, "no-such-channel", 0, []byte("x")); err == nil {
		t.Fatal("SendChannel on an unknown channel should fail")
	}
	if _, _, status, err := sess.PollChannel(ctx, "no-such-channel"); err == nil || status != ChannelNoData {
		t.Fatalf("PollChannel on an unknown channel = (status=%v, err=%v), want an error and ChannelNoData", status, err)
	}
}
