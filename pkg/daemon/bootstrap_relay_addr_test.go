package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
)

// TestBootstrapWaitsForItsRelayAddress covers a node that bootstraps its own
// single-node cluster while it is only reachable through a relay.
//
// The address a node writes into raft's persisted configuration is written
// once and never revised, so choosing it before the relay reservation lands
// names the node by an address nobody can dial -- permanently. The join path
// has always waited for exactly that reason (see handleAdd's join branch);
// the bootstrap path did not, and a solo-bootstrapped node behind NAT
// recorded its loopback address instead.
//
// What made that hard to notice is that such a cluster looks healthy. The
// leader can still dial *out*, so replication it initiates works and reads
// are correct; only an inbound dial fails, which in practice means a
// follower forwarding a write to its leader -- "all dials failed" against
// the one node everything else was working through. Found on this project's
// two-device Android rig, whose leader is an app that solo-bootstraps
// seconds after launch, long before AutoRelay has reserved anything.
//
// The assertion is that the wait *happens*, not that a reservation
// succeeds: whether AutoRelay completes one depends on the host's own
// reachability detection, which is a property of the machine a test runs on
// rather than of this code (see TestRelayLeaderReplication, which constructs
// a circuit address by hand for the same reason). So this configures a relay
// that will never answer and measures that bootstrap still declines to name
// the node until it has given the reservation its full chance.
func TestBootstrapWaitsForItsRelayAddress(t *testing.T) {
	// Not parallel: it adjusts a package-level timeout.
	const wait = 3 * time.Second
	original := relayAddrAwaitTimeout
	relayAddrAwaitTimeout = wait
	t.Cleanup(func() { relayAddrAwaitTimeout = original })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// An unreachable relay: reserving against it can never succeed, so the
	// whole budget is spent, which is what makes the elapsed time readable.
	const deadRelay = "/ip4/127.0.0.1/tcp/1/p2p/12D3KooWMREsi2ioEE976LPRVP4quStNQodK9b9oBZKj9b5MdnLM"

	bootstrap := func(t *testing.T, name string, relays []string) time.Duration {
		t.Helper()
		tmpDir := t.TempDir()
		keyPath := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(keyPath); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		n, err := start(Config{
			DataDir:            filepath.Join(tmpDir, name),
			KeyPath:            keyPath,
			RelayPeers:         relays,
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			CommitTimeout:      20 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		t.Cleanup(n.shutdown)

		started := time.Now()
		if _, err := n.handleAdd(ctx, ""); err != nil {
			t.Fatalf("bootstrap %s: %v", name, err)
		}
		elapsed := time.Since(started)

		if servers := n.getRaft().GetConfiguration().Configuration().Servers; len(servers) != 1 {
			t.Fatalf("%s bootstrapped %d servers, want 1: %+v", name, len(servers), servers)
		}
		return elapsed
	}

	// A node with a relay to wait for spends the budget waiting.
	if elapsed := bootstrap(t, "relayed", []string{deadRelay}); elapsed < wait {
		t.Errorf("bootstrap with a relay configured took %s, less than the %s reservation budget -- it named this node before its relay address could exist", elapsed, wait)
	}

	// The control, and the reason this costs nothing in the ordinary case:
	// awaitRelayAddr returns immediately when there is no relay to wait
	// for, which is every node that isn't behind NAT.
	if elapsed := bootstrap(t, "direct", nil); elapsed >= wait {
		t.Errorf("bootstrap with no relay configured took %s -- it should not wait at all", elapsed)
	}
}
