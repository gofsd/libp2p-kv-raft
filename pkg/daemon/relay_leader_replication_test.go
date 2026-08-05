package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestRelayLeaderReplicatesToPeerBehindItself covers the topology this
// project's own e2e pipeline runs in but no test did: the raft leader is
// *also* the relay that the follower is only reachable through.
//
// A follower behind a relay is enrolled at its /p2p-circuit address, and
// that address names the relay in its own left-hand side. When some third
// node is the relay, the leader dials an ordinary circuit. When the leader
// *is* the relay, the first hop of that address is the leader itself --
// and whether replication survives that is exactly what nothing checked.
//
// It matters because it is the intended production shape: CLAUDE.md's
// connectivity policy says leadership belongs on the stable public host,
// and the stable public host is the one running -relay-service. This
// project's shared e2e cluster only avoided it by accident, by having
// leadership sit on a laptop; pinning leadership to the relay (where it
// belongs) is what made the browser learner stop receiving anything, with
// no error on either side -- writes committed on the leader, reads on the
// learner returning "key not found" until the retry budget ran out.
func TestRelayLeaderReplicatesToPeerBehindItself(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}
	startNode := func(name string, cfg Config) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		t.Cleanup(n.shutdown)
		return n
	}

	// The leader runs the relay service and bootstraps the cluster -- one
	// node playing both roles, exactly like the deployed bootstrap node.
	leaderCfg := fastRaft
	leaderCfg.RelayService = true
	leader := startNode("leader", leaderCfg)
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	followerCfg := fastRaft
	followerCfg.RelayPeers = []string{leaderAddr}
	follower := startNode("follower", followerCfg)

	// Standing first, then a reservation on the leader's own relay: the
	// follower is reachable at a circuit address whose relay hop is the
	// leader.
	resp := callLocal(t, ctx, follower, shmevent.Msg{EventType: shmevent.EventPublicAccess, Value: []byte(leaderAddr), ID: 1}, follower.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("public_access rejected: %s", resp.Value)
	}
	followerCircuit := leaderAddr + "/p2p-circuit/p2p/" + follower.peerID
	maddr, err := multiaddr.NewMultiaddr(followerCircuit)
	if err != nil {
		t.Fatalf("parse follower circuit addr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("follower circuit addr info: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		dialCtx, dialCancel := context.WithTimeout(network.WithAllowLimitedConn(ctx, "test"), 10*time.Second)
		err := leader.host.Connect(dialCtx, *info)
		dialCancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the leader could never reach the follower through its own relay: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// The follower joins for real first, so it has its own raft instance
	// serving replication -- without that there is nothing to replicate
	// into and this test would "fail" for a reason having nothing to do
	// with relays. That join enrolls it at whatever address it advertises,
	// which on one machine is a direct one.
	// " learner", not a plain join: the peer this models (a browser tab, a
	// phone) is always a non-voter, and a voter here would make the
	// cluster two-voter, so severing its connection below would cost
	// quorum and fail the write for a reason unrelated to what this test
	// is about.
	if _, err := follower.handleAdd(ctx, leaderAddr+" learner"); err != nil {
		t.Fatalf("follower join: %v", err)
	}

	// Now move its raft address to the circuit one -- re-adding the same
	// id at a new address is how raft records an address change, and the
	// circuit is what a genuinely NATed peer would have supplied all
	// along (advertisedAddrs prefers it). From here the leader can only
	// reach this follower through its own relay service.
	if _, err := leader.handleAddLearner(ctx, follower.peerID, followerCircuit); err != nil {
		t.Fatalf("re-enrol follower at its circuit address: %v", err)
	}

	// Force the circuit to be the *only* way the leader can reach the
	// follower. On one machine they can always talk directly, so without
	// this the leader keeps using a direct connection and the circuit is
	// never exercised -- an earlier version of this test passed for
	// exactly that reason while the deployed cluster was failing. Dropping
	// the direct addresses and the connections built from them leaves the
	// follower reachable only as a peer reserved on this very host, which
	// is the real topology.
	leader.host.Peerstore().ClearAddrs(info.ID)
	leader.host.Peerstore().AddAddrs(info.ID, info.Addrs, time.Hour)
	for _, c := range leader.host.Network().ConnsToPeer(info.ID) {
		if !c.Stat().Limited {
			_ = c.Close()
		}
	}

	if err := leader.handleSetForward(ctx, []byte("relay-leader-key"), []byte("replicated"), true); err != nil {
		t.Fatalf("set on leader: %v", err)
	}

	// The write must reach the follower's own store: that is what every
	// read on a learner is served from.
	deadline = time.Now().Add(60 * time.Second)
	for {
		value, err := follower.handleGet([]byte("relay-leader-key"))
		if err == nil && string(value) == "replicated" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the follower never received the leader's write: err=%v value=%q\n"+
				"the leader is the relay this follower is reachable through -- see this test's doc comment", err, value)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
