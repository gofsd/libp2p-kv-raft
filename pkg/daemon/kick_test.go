package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestKickRemovesDownedVoterAndClusterKeepsWorking is a real-cluster test
// for EventKick/kickPeer: it proves a raft voter that has permanently gone
// down (crashed, wiped, never coming back -- not just a transient
// disconnect) can be force-removed by another current voter without its
// own cooperation, and that the cluster keeps accepting writes both before
// and after the removal. This is the "still have quorum among the
// survivors" case (2 of 3 voters alive, still a majority of the
// *configured* 3) -- kickPeer relies on an ordinary raft.RemoveServer
// command committing through normal consensus, so it cannot by itself
// recover a cluster that has already lost majority outright (e.g. a
// 2-voter cluster where the other voter goes down); that scenario needs
// hashicorp/raft's offline RecoverCluster instead, a different and much
// more invasive operation this package doesn't implement.
func TestKickRemovesDownedVoterAndClusterKeepsWorking(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}

	startNode := func(name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg := fastRaft
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	isMember := func(rf *raft.Raft, id string) bool {
		t.Helper()
		cfgFuture := rf.GetConfiguration()
		if err := cfgFuture.Error(); err != nil {
			t.Fatalf("get configuration: %v", err)
		}
		for _, srv := range cfgFuture.Configuration().Servers {
			if srv.ID == raft.ServerID(id) {
				return true
			}
		}
		return false
	}

	leader := startNode("leader")
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	voter2 := startNode("voter2")
	defer voter2.shutdown()
	if _, err := voter2.handleAdd(ctx, leaderAddr); err != nil {
		t.Fatalf("voter2 join: %v", err)
	}

	voter3 := startNode("voter3")
	if _, err := voter3.handleAdd(ctx, leaderAddr); err != nil {
		t.Fatalf("voter3 join: %v", err)
	}
	voter3PeerID := voter3.peerID

	if !isMember(leader.getRaft(), voter3PeerID) {
		t.Fatal("voter3 not in leader's raft configuration before shutdown")
	}

	// Set something before the failure, proving the cluster is healthy at
	// full membership.
	if err := leader.applySet(leader.getRaft(), []byte("before"), []byte("ok")); err != nil {
		t.Fatalf("set before kick: %v", err)
	}

	// voter3 goes down for good -- simulated by shutting it down and
	// never restarting it, the same as a crashed/wiped machine that's
	// never coming back. 2 of the 3 configured voters (leader, voter2)
	// remain, still a majority, so the cluster keeps working without any
	// intervention -- kickPeer isn't needed for availability here, only
	// for restoring the configuration to reflect reality.
	voter3.shutdown()

	if err := leader.applySet(leader.getRaft(), []byte("after-down"), []byte("still-ok")); err != nil {
		t.Fatalf("set with voter3 down: %v", err)
	}

	// A second current voter (not the leader) issues the kick -- proving
	// this isn't leader-only, the same "any current voter" authorization
	// EventKick's doc comment describes.
	if err := voter2.kickPeer(ctx, voter3PeerID); err != nil {
		t.Fatalf("kickPeer: %v", err)
	}

	if isMember(leader.getRaft(), voter3PeerID) {
		t.Fatal("voter3 still in leader's raft configuration after kick")
	}

	if err := leader.applySet(leader.getRaft(), []byte("after-kick"), []byte("still-fine")); err != nil {
		t.Fatalf("set after kick: %v", err)
	}
	got, err := leader.handleGet([]byte("after-kick"))
	if err != nil {
		t.Fatalf("get after kick: %v", err)
	}
	if string(got) != "still-fine" {
		t.Fatalf("get after kick = %q, want %q", got, "still-fine")
	}
}

// TestKickRejectsNonVoterCaller proves a cluster member with no raft-voter
// standing (a learner) cannot kick anyone -- EventKick's authorization
// check, exactly mirroring EventPermitConfirm's own "not a current raft
// voter" rejection (see permit_test.go's equivalent). The learner's call
// is local (n.localCaller()) -- correctness here comes from
// handleForwardKickStream re-checking the *forwarding* node's own
// identity once the learner's request reaches the leader, the same two-
// layer enforcement EventPermitConfirm's doc comment describes, not from
// any check at the local dispatch point (which is deliberately skipped
// for local callers).
func TestKickRejectsNonVoterCaller(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}

	startNode := func(name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg := fastRaft
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	leader := startNode("leader")
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	learner := startNode("learner")
	defer learner.shutdown()
	if _, err := learner.handleAdd(ctx, leaderAddr+" learner"); err != nil {
		t.Fatalf("learner join: %v", err)
	}

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, n.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
	}

	resp := call(learner, shmevent.Msg{EventType: shmevent.EventKick, Value: []byte(leader.peerID), ID: 1})
	if resp.EventType != shmevent.EventError {
		t.Fatalf("learner kick unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not a current raft voter") {
		t.Fatalf("learner kick rejected for the wrong reason: %s", resp.Value)
	}
}
