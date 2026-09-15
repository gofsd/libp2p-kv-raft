//go:build !android

package ipc

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// A Call must give up when *its own* context does, even if what it is waiting for is the peer's
// caller lock rather than the daemon.
//
// The lock serializes callers against the one fixed-name request channel (see callerLocks), and it
// used to be taken with a plain sync.Mutex.Lock(), which cannot be cancelled. So a caller queued
// behind a slow one sat there past its deadline, and only then ran the first context-aware step --
// reporting "waiting for response channel ...: context deadline exceeded", which blames the daemon
// for time the caller actually spent in the queue, and blames it after the whole budget is gone.
//
// That is not hypothetical. A mes backend polls many commands through one shmclient session, and
// on 2026-09-15 an optical batch lost the `cmd-inventoryDictionary` case exactly this way: the app
// waited its full 90s and the backend log showed a burst of "waiting for response channel" for
// several different message ids, none of which had ever reached the daemon.
func TestCallGivesUpOnItsOwnDeadlineWhileAnotherCallerHoldsTheLock(t *testing.T) {
	peerID := fmt.Sprintf("lockwait-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Stand in for a Call already in flight against this peer, holding the lock far longer than
	// the caller below is willing to wait. No Serve loop runs: the point is that the second
	// caller never gets far enough to need one.
	holdFor := 4 * time.Second
	release := holdForPeer(t, peerID, holdFor)
	defer release()

	req, err := shmevent.NewGetFieldByKey([]byte("some-key"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	req.SetId(11)

	const budget = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	_, err = Call(ctx, peerID, req, priv)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Call unexpectedly succeeded with no daemon serving")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want a context.DeadlineExceeded, got %v", err)
	}
	// The real assertion: it returned on its own deadline rather than when the lock came free.
	if elapsed > holdFor/2 {
		t.Fatalf("Call waited %v for a lock its %v context had already given up on -- "+
			"a caller queued behind a slow one burns its whole budget and then misreports "+
			"the wait as the daemon's fault (err=%v)", elapsed, budget, err)
	}
	// And it should say what it was actually waiting for.
	if strings.Contains(err.Error(), "waiting for response channel") {
		t.Fatalf("error blames the response channel for time spent queued on the caller lock: %v", err)
	}
}

// holdForPeer takes peerID's caller lock and releases it after d, returning a func the test may
// call to release early. Safe to call the returned func after the timer already fired.
func holdForPeer(t *testing.T, peerID string, d time.Duration) func() {
	t.Helper()
	l := callerLock(peerID)
	l <- struct{}{}
	released := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-time.After(d):
		case <-released:
		}
		<-l
	}()
	var stopped bool
	return func() {
		if stopped {
			return
		}
		stopped = true
		close(released)
		<-done
	}
}
