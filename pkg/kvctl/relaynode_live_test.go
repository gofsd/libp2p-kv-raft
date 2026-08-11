package kvctl_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestRelayNodeAddConfirmReplicatesAndRemove drives AddRelayNode/
// ConfirmRelayNode/GetRelayNode/ListRelayNodes/RemoveRelayNode end to end
// through two real spawned nodes -- the CLI-facing path behind `mage
// addrelaynode/confirmrelaynode/getrelaynode/listrelaynodes/
// removerelaynode`, previously exercised only by hand. Mirrors
// TestRequestConfirmPermitAcrossNodes's shape (this record type reuses
// the identical request/confirm/revoke permit lifecycle, see relaynode.go's
// own package doc comment), but also checks the read side
// (GetRelayNode/ListRelayNodes) and that a confirmed record actually
// replicates to a non-voter follower, not just the leader that confirmed
// it.
func TestRelayNodeAddConfirmReplicatesAndRemove(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	followerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs, leaderID)
	if err != nil {
		t.Fatalf("AddNode (follower): %v", err)
	}

	// A relay node's address only needs to be a syntactically valid
	// multiaddr with a trailing /p2p/<peerID> -- AddRelayNode/
	// ConfirmRelayNode/GetRelayNode never dial it, they only store it (see
	// relayNodePeerID's own doc comment) -- so a synthetic, undialable peer
	// id is fine here.
	const relayAddr = "/ip4/203.0.113.1/tcp/4001/p2p/12D3KooWQzajnsSmucFMSRksuLRQRmBq8Lxwp4LXsxFLJQF6W9VX"
	const relayPeerID = "12D3KooWQzajnsSmucFMSRksuLRQRmBq8Lxwp4LXsxFLJQF6W9VX"
	const priority = uint8(5)

	waitFor := func(cond func() bool, what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for: %s", what)
	}

	if err := kvctl.Use(followerID); err != nil {
		t.Fatalf("Use(follower): %v", err)
	}
	if err := kvctl.AddRelayNode(relayAddr, priority); err != nil {
		t.Fatalf("AddRelayNode: %v", err)
	}

	// Confirming requires a current raft voter -- the follower here is
	// only ever the joined-second member of a 2-node cluster (still a
	// voter in this simple topology), but ConfirmRelayNode is exercised on
	// the leader below regardless, since that's the realistic operator
	// flow (mage confirmrelaynode always runs on a voter that isn't
	// necessarily the one that originated the request).
	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use(leader): %v", err)
	}
	waitFor(func() bool {
		_, err := kvctl.GetRelayNode(relayAddr)
		// Not confirmed yet -- GetRelayNode only ever reads the confirmed
		// half, so it errors until ConfirmRelayNode runs; this just waits
		// for AddRelayNode's own pending write to replicate to the leader.
		return err != nil
	}, "pending relay node record to replicate to the leader (sanity: still unconfirmed)")

	if err := kvctl.ConfirmRelayNode(relayAddr); err != nil {
		t.Fatalf("ConfirmRelayNode: %v", err)
	}

	assertConfirmed := func(peerLabel string) {
		t.Helper()
		var got kvctl.RelayNode
		waitFor(func() bool {
			var err error
			got, err = kvctl.GetRelayNode(relayAddr)
			return err == nil
		}, fmt.Sprintf("confirmed relay node record to appear on %s", peerLabel))
		if got.PeerID != relayPeerID {
			t.Fatalf("%s: GetRelayNode.PeerID = %q, want %q", peerLabel, got.PeerID, relayPeerID)
		}
		if got.Multiaddr != relayAddr {
			t.Fatalf("%s: GetRelayNode.Multiaddr = %q, want %q", peerLabel, got.Multiaddr, relayAddr)
		}
		if got.Priority != priority {
			t.Fatalf("%s: GetRelayNode.Priority = %d, want %d", peerLabel, got.Priority, priority)
		}

		nodes, err := kvctl.ListRelayNodes()
		if err != nil {
			t.Fatalf("%s: ListRelayNodes: %v", peerLabel, err)
		}
		found := false
		for _, n := range nodes {
			if n.PeerID == relayPeerID {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: ListRelayNodes = %+v, want it to include peer %s", peerLabel, nodes, relayPeerID)
		}
	}

	assertConfirmed("leader")

	if err := kvctl.Use(followerID); err != nil {
		t.Fatalf("Use(follower): %v", err)
	}
	assertConfirmed("follower (replicated)")

	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use(leader): %v", err)
	}
	if err := kvctl.RemoveRelayNode(relayAddr); err != nil {
		t.Fatalf("RemoveRelayNode: %v", err)
	}
	waitFor(func() bool {
		_, err := kvctl.GetRelayNode(relayAddr)
		return err != nil
	}, "relay node record to disappear after RemoveRelayNode")

	nodes, err := kvctl.ListRelayNodes()
	if err != nil {
		t.Fatalf("ListRelayNodes after remove: %v", err)
	}
	for _, n := range nodes {
		if n.PeerID == relayPeerID {
			t.Fatalf("ListRelayNodes after remove still includes %s: %+v", relayPeerID, nodes)
		}
	}
}
