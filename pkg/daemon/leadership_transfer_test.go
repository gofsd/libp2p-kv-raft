package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	lp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
)

// TestPreferredLeaderTransferTarget covers preferredLeaderTransferTarget's
// pure selection logic in isolation, without the cost/flakiness of a real
// relay reservation -- TestLeadershipTransfersOffRelayOnlyLeader below
// covers the real wiring end to end.
func TestPreferredLeaderTransferTarget(t *testing.T) {
	relayAddr := raft.ServerAddress("/ip4/1.2.3.4/tcp/4001/p2p/relay/p2p-circuit/p2p/self")
	directAddr := raft.ServerAddress("/ip4/5.6.7.8/tcp/4001/p2p/other")

	t.Run("self already stable", func(t *testing.T) {
		cfg := raft.Configuration{Servers: []raft.Server{
			{ID: "self", Address: directAddr, Suffrage: raft.Voter},
			{ID: "other", Address: relayAddr, Suffrage: raft.Voter},
		}}
		if _, _, ok := preferredLeaderTransferTarget(cfg, "self"); ok {
			t.Fatal("expected no transfer target when self already has a direct address")
		}
	})

	t.Run("self relay-routed, a stable voter is available", func(t *testing.T) {
		cfg := raft.Configuration{Servers: []raft.Server{
			{ID: "self", Address: relayAddr, Suffrage: raft.Voter},
			{ID: "other", Address: directAddr, Suffrage: raft.Voter},
		}}
		id, addr, ok := preferredLeaderTransferTarget(cfg, "self")
		if !ok || id != "other" || addr != directAddr {
			t.Fatalf("got id=%q addr=%q ok=%v, want id=\"other\" addr=%q ok=true", id, addr, ok, directAddr)
		}
	})

	t.Run("self relay-routed, no voter is any more stable", func(t *testing.T) {
		cfg := raft.Configuration{Servers: []raft.Server{
			{ID: "self", Address: relayAddr, Suffrage: raft.Voter},
			{ID: "other", Address: relayAddr, Suffrage: raft.Voter},
		}}
		if _, _, ok := preferredLeaderTransferTarget(cfg, "self"); ok {
			t.Fatal("expected no transfer target when no other voter is more stable than self")
		}
	})

	t.Run("only stable server present is a learner", func(t *testing.T) {
		cfg := raft.Configuration{Servers: []raft.Server{
			{ID: "self", Address: relayAddr, Suffrage: raft.Voter},
			{ID: "learner", Address: directAddr, Suffrage: raft.Nonvoter},
		}}
		if _, _, ok := preferredLeaderTransferTarget(cfg, "self"); ok {
			t.Fatal("expected no transfer target when the only stable server is a learner, not a voter")
		}
	})
}

// TestLeadershipTransfersOffRelayOnlyLeader drives the real mechanism this
// task added: CLAUDE.md's connectivity policy says leadership belongs on a
// node with a real, stable address, but until now nothing in the daemon
// actually acted on that -- a relay-only node that happened to bootstrap
// first just stayed leader forever (see relay_leader_replication_test.go's
// doc comment on how that topology was actually reached in production).
//
// This reproduces the shape directly: a node reachable only through a
// relay bootstraps solo (so it is, unavoidably, the first leader), then a
// node with a plain direct address joins as a voter. addServerLine's call
// to transferLeadershipToStableVoter should hand leadership to the new
// voter without any operator intervention.
func TestLeadershipTransfersOffRelayOnlyLeader(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	relay, err := p2praft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		t.Fatalf("start relay: %v", err)
	}
	relayAddr := relay.Addrs[0]

	leaderCfg := fastRaftCfg()
	leaderCfg.RelayPeers = []string{relayAddr}
	relayOnlyLeader := startNATTestNode(t, tmpDir, "relay-only-leader", leaderCfg)

	if _, err := relayOnlyLeader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap relay-only leader: %v", err)
	}
	rf := relayOnlyLeader.getRaft()

	// Wait for the reservation to actually be live by dialing the circuit
	// address from a separate prober host -- the same technique
	// TestRelayFailoverToSecondCandidateWhenFirstIsDown and
	// TestExecInviteRedeemOverRelay use, rather than polling
	// n.host.Addrs()/advertisedAddrs() for a /p2p-circuit entry: on a
	// machine with a Tailscale interface, AutoRelay's own addrsFactory
	// treats that CGNAT (100.64.0.0/10) address as public-reachable
	// (manet.IsPublicAddr) and never adds the circuit address to
	// host.Addrs() at all, even though the reservation itself is live and
	// the circuit is fully dialable -- confirmed directly against this
	// environment. A real NAT'd/relay-only node has no such address, so
	// this gap doesn't exist in the production topology this test models.
	leaderCircuitAddr := relayAddr + "/p2p-circuit/p2p/" + relayOnlyLeader.peerID
	maddr, err := multiaddr.NewMultiaddr(leaderCircuitAddr)
	if err != nil {
		t.Fatalf("parse leader circuit addr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("leader circuit addr info: %v", err)
	}
	prober, err := lp2p.New(lp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create prober host: %v", err)
	}
	defer prober.Close()
	deadline := time.Now().Add(45 * time.Second)
	for {
		clearDialBackoff(prober, *info)
		dialCtx, dialCancel := context.WithTimeout(network.WithAllowLimitedConn(ctx, "test"), 10*time.Second)
		dialErr := prober.Connect(dialCtx, *info)
		dialCancel()
		if dialErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay-only leader never became reachable through its relay: %v", dialErr)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Force this node's own raft-recorded address to the circuit address --
	// the same technique relay_leader_replication_test.go uses for a
	// follower's address, applied to self. This makes the precondition this
	// test is about -- "the leader's own raft address is relay-only" --
	// deterministic rather than depending on advertisedAddrs' address-score
	// ordering having already preferred the circuit address by the moment
	// BootstrapCluster ran a few lines up (see the comment above: on this
	// environment it never would).
	if err := rf.AddVoter(raft.ServerID(relayOnlyLeader.peerID), raft.ServerAddress(leaderCircuitAddr), 0, 10*time.Second).Error(); err != nil {
		t.Fatalf("re-register relay-only leader at its circuit address: %v", err)
	}

	stableVoter := startNATTestNode(t, tmpDir, "stable-voter", fastRaftCfg())
	if _, err := stableVoter.handleAdd(ctx, leaderCircuitAddr); err != nil {
		t.Fatalf("stable voter join through relay-only leader: %v", err)
	}

	xferDeadline := time.Now().Add(30 * time.Second)
	for {
		if stableVoter.getRaft().State() == raft.Leader {
			return
		}
		if time.Now().After(xferDeadline) {
			t.Fatalf("leadership never transferred to the stable voter: relay-only leader state=%s, stable voter state=%s",
				relayOnlyLeader.getRaft().State(), stableVoter.getRaft().State())
		}
		time.Sleep(200 * time.Millisecond)
	}
}
