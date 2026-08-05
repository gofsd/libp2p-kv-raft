package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestDeletePeerGroupFromFollower pins that a non-leader can actually
// remove a peer-group record, which syncMemberGroups depends on and which
// nothing else covered.
//
// Every reserved-group sync ends in a delete: a learner drops its stale
// "voter" membership, a voter drops its stale "learner" one
// (syncMemberGroups), and clearMemberGroups removes all three when a peer
// leaves. On the leader those apply locally and work. From any other node
// they have to be forwarded -- and deletePeerGroup was the one delete in
// this package routed through handleOpForward (ForwardProtocolID) rather
// than handleConfirmForward (ForwardConfirmProtocolID) like every other
// OpDel. That handler accepts only OpSet and OpAppendCommandRequest, so
// the forward was rejected outright, every time, with an error naming an
// op number rather than anything a reader could act on:
//
//	sync own reserved groups: forward set: forward set: expected OpSet or
//	OpAppendCommandRequest, got op 2
//
// Found in the wild, not by reading: the shared e2e leader's daemon.log
// had been repeating that line continuously, because watchLeadership runs
// this sync on every leadership observation and the node forwards whenever
// it isn't currently leader. The visible cost is a follower whose reserved
// -group membership silently stops converging -- a demoted voter keeps its
// voter standing -- plus an endless retry loop.
func TestDeletePeerGroupFromFollower(t *testing.T) {
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
		t.Cleanup(n.shutdown)
		return n
	}

	leader := startNode("leader")
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	follower := startNode("follower")
	if _, err := follower.handleAdd(ctx, leader.advertisedAddrs()[0]); err != nil {
		t.Fatalf("follower join: %v", err)
	}

	// The follower must really be a follower -- otherwise this exercises
	// the local-apply path and proves nothing about forwarding.
	deadline := time.Now().Add(30 * time.Second)
	for {
		rf := follower.getRaft()
		if rf != nil && rf.State() != raft.Leader {
			if _, leaderID := rf.LeaderWithID(); leaderID != "" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never settled into a follower state with a known leader")
		}
		time.Sleep(100 * time.Millisecond)
	}

	peerID := []byte(follower.peerID)
	group := []byte(shmevent.ReservedGroupLearner)
	if err := follower.setPeerGroup(ctx, peerID, group); err != nil {
		t.Fatalf("setPeerGroup from a follower (the OpSet half, which always worked): %v", err)
	}
	key, err := shmevent.PeerGroupKey(peerID, group)
	if err != nil {
		t.Fatalf("PeerGroupKey: %v", err)
	}
	if _, err := leader.store.Get(key); err != nil {
		t.Fatalf("the record never reached the leader, so this test cannot say anything about deleting it: %v", err)
	}

	if err := follower.deletePeerGroup(ctx, peerID, group); err != nil {
		t.Fatalf("deletePeerGroup from a follower: %v", err)
	}

	// Committed on the leader, not merely accepted locally.
	deadline = time.Now().Add(20 * time.Second)
	for {
		if _, err := leader.store.Get(key); err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the peer-group record is still present on the leader after a follower deleted it")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
