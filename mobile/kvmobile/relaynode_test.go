package kvmobile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestAddConfirmListRemoveRelayNodeThroughKvmobile drives kvmobile's
// AddRelayNode/ConfirmRelayNode/GetRelayNode/ListRelayNodes/RemoveRelayNode
// bindings end to end against a real leader -- the same
// request/confirm/revoke lifecycle TestRequestConfirmRevokePermitThroughKvmobile
// exercises for kind "bootstrap" directly, but through the friendlier
// relay-node-specific wrapper this file adds instead. leaderAddr itself
// (a real, valid /p2p/<peerID> multiaddr) stands in for the relay being
// registered -- this test only checks the record CRUD round trip, not
// that leaderAddr is actually reachable as a relay.
func TestAddConfirmListRemoveRelayNodeThroughKvmobile(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	relayPeerID := peerIDFromMultiaddr(t, leaderAddr)
	const priority = 7

	if err := AddRelayNode(leaderAddr, priority); err != nil {
		t.Fatalf("AddRelayNode: %v", err)
	}
	pendingKey := string(shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusPending, []byte(relayPeerID)))
	if _, err := pollGet(t, pendingKey, 10*time.Second); err != nil {
		t.Fatalf("Get(pendingKey) after AddRelayNode: %v", err)
	}

	// Still pending -- GetRelayNode/ListRelayNodes only ever look at
	// confirmed records, mirroring GetGroup/ListGroups' own "confirmed
	// only" semantics for kinds with no two-stage lifecycle at all.
	if _, err := GetRelayNode(leaderAddr); err == nil {
		t.Fatalf("GetRelayNode: expected error for still-pending record, got none")
	}

	if err := ConfirmRelayNode(leaderAddr); err != nil {
		t.Fatalf("ConfirmRelayNode: %v", err)
	}
	confirmedKey := string(shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusConfirmed, []byte(relayPeerID)))
	if _, err := pollGet(t, confirmedKey, 10*time.Second); err != nil {
		t.Fatalf("Get(confirmedKey) after ConfirmRelayNode: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	var got RelayNode
	for {
		out, err := GetRelayNode(leaderAddr)
		if err == nil {
			if err := json.Unmarshal([]byte(out), &got); err != nil {
				t.Fatalf("unmarshal GetRelayNode result %q: %v", out, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GetRelayNode after ConfirmRelayNode: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got.PeerID != relayPeerID || got.Multiaddr != leaderAddr || got.Priority != priority {
		t.Fatalf("GetRelayNode = %+v, want {PeerID:%s Multiaddr:%s Priority:%d}", got, relayPeerID, leaderAddr, priority)
	}

	listOut, err := ListRelayNodes()
	if err != nil {
		t.Fatalf("ListRelayNodes: %v", err)
	}
	var list []RelayNode
	if err := json.Unmarshal([]byte(listOut), &list); err != nil {
		t.Fatalf("unmarshal ListRelayNodes result %q: %v", listOut, err)
	}
	found := false
	for _, n := range list {
		if n.PeerID == relayPeerID {
			found = true
			if n.Multiaddr != leaderAddr || n.Priority != priority {
				t.Fatalf("ListRelayNodes entry = %+v, want {PeerID:%s Multiaddr:%s Priority:%d}", n, relayPeerID, leaderAddr, priority)
			}
		}
	}
	if !found {
		t.Fatalf("ListRelayNodes %v: missing relay node %s", list, relayPeerID)
	}

	if err := RemoveRelayNode(leaderAddr); err != nil {
		t.Fatalf("RemoveRelayNode: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	var stillPresent bool
	for time.Now().Before(deadline) {
		if _, err := Get(confirmedKey); err != nil {
			stillPresent = false
			break
		}
		stillPresent = true
		time.Sleep(50 * time.Millisecond)
	}
	if stillPresent {
		t.Fatalf("Get(confirmedKey) after RemoveRelayNode: still present, want deleted")
	}
}
