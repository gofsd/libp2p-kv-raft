//go:build !android

package ipc

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// The backoff starts short, doubles, and stops at the old flat interval -- which is still the
// ceiling, so an absent peer costs an idle loop exactly what it always did.
func TestOpenRetryBacksOffFromShortToTheOldInterval(t *testing.T) {
	if got := nextOpenRetry(0); got != openRetryMin {
		t.Fatalf("first wait = %s, want %s", got, openRetryMin)
	}
	seen := nextOpenRetry(0)
	for i := 0; i < 20; i++ {
		next := nextOpenRetry(seen)
		if next < seen {
			t.Fatalf("backoff went backwards: %s then %s", seen, next)
		}
		if next > openRetryInterval {
			t.Fatalf("backoff overshot the ceiling: %s > %s", next, openRetryInterval)
		}
		seen = next
	}
	if seen != openRetryInterval {
		t.Fatalf("backoff settled at %s, want the old interval %s", seen, openRetryInterval)
	}
	if got := nextOpenRetry(time.Hour); got != openRetryInterval {
		t.Fatalf("a wait past the ceiling returned %s, want %s", got, openRetryInterval)
	}
}

// What the backoff is for: a round trip must not pay a fixed sleep on each side when the work
// between them takes microseconds.
//
// Both sides used to wait a flat 20ms before looking for a segment the other end had not created
// yet, and the first look essentially always misses -- so every call paid it twice. Measured on
// the optical rig 2026-09-16, that floor *was* the cost of a dispatcher sweep: 55 commands, ~2
// round trips each, median 3.23s.
//
// The bound here is deliberately loose (it runs on whatever CI machine it lands on) but it is far
// below what the flat interval could produce: 20 sequential round trips could not have come in
// under 400ms before, and 800ms leaves room for a slow host without admitting a regression back to
// the old behaviour.
func TestASequenceOfCallsIsNotPacedBySleep(t *testing.T) {
	peerID := fmt.Sprintf("openretry-test-%d", time.Now().UnixNano())
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	served := make(chan error, 1)
	go func() {
		served <- Serve(ctx, peerID, dataDir, priv, func(_ context.Context, m shmevent.Msg, _ uint32, _ []byte) shmevent.Msg {
			resp, err := shmevent.NewGetFieldByKey([]byte("k"))
			if err != nil {
				panic(err)
			}
			if err := resp.GetFieldByKey().SetValue([]byte("v")); err != nil {
				panic(err)
			}
			resp.SetId(m.Id())
			return resp
		})
	}()
	defer func() { cancel(); <-served }()

	const calls = 20
	// One call first, so the measurement excludes the Serve loop's own start-up.
	warm, err := shmevent.NewGetFieldByKey([]byte("k"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	warm.SetId(1)
	if _, err := Call(ctx, peerID, warm, priv); err != nil {
		t.Fatalf("warm-up call: %v", err)
	}

	started := time.Now()
	for i := 0; i < calls; i++ {
		req, err := shmevent.NewGetFieldByKey([]byte("k"))
		if err != nil {
			t.Fatalf("NewGetFieldByKey: %v", err)
		}
		req.SetId(uint16(i + 2))
		if _, err := Call(ctx, peerID, req, priv); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	elapsed := time.Since(started)
	perCall := elapsed / calls

	t.Logf("%d round trips in %s (%s each); the old flat %s on each side put the floor near %s",
		calls, elapsed.Round(time.Millisecond), perCall.Round(time.Microsecond),
		openRetryInterval, openRetryInterval)

	// 432ms before the backoff, 45ms after, on the machine this was written on. The bound is loose
	// enough for a slow CI host and far below what a flat interval on every round trip produces.
	if elapsed > 800*time.Millisecond {
		t.Fatalf("%d round trips took %s (%s each) -- that is the fixed-sleep floor the backoff "+
			"exists to remove, not the cost of the work", calls, elapsed, perCall)
	}
}
