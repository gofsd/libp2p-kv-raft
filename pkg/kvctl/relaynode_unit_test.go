package kvctl

import (
	"strings"
	"testing"
)

// TestRelayNodePeerIDExtractsFromP2pAddr covers relayNodePeerID's
// happy path and its two rejection shapes (a string that isn't a
// multiaddr at all, and a syntactically valid multiaddr missing the
// trailing /p2p/<peerID> component) -- the same validation
// pkg/daemon's newHost/relayCandidates already require of every relay
// address, exercised directly here since AddRelayNode/ConfirmRelayNode/
// RemoveRelayNode/GetRelayNode all funnel through it before ever
// reaching a live daemon.
func TestRelayNodePeerIDExtractsFromP2pAddr(t *testing.T) {
	const wantPeerID = "12D3KooWQzajnsSmucFMSRksuLRQRmBq8Lxwp4LXsxFLJQF6W9VX"

	pid, err := relayNodePeerID("/ip4/203.0.113.1/tcp/4001/p2p/" + wantPeerID)
	if err != nil {
		t.Fatalf("relayNodePeerID: %v", err)
	}
	if pid.String() != wantPeerID {
		t.Fatalf("relayNodePeerID = %q, want %q", pid.String(), wantPeerID)
	}
}

func TestRelayNodePeerIDRejectsNotAMultiaddr(t *testing.T) {
	_, err := relayNodePeerID("not a multiaddr at all")
	if err == nil {
		t.Fatal("relayNodePeerID: expected an error for a malformed address, got nil")
	}
	if !strings.Contains(err.Error(), "invalid relay node address") {
		t.Fatalf("relayNodePeerID error = %q, want it to name the address as invalid", err)
	}
}

func TestRelayNodePeerIDRejectsMissingP2pComponent(t *testing.T) {
	_, err := relayNodePeerID("/ip4/203.0.113.1/tcp/4001")
	if err == nil {
		t.Fatal("relayNodePeerID: expected an error for an address with no /p2p/<peerID>, got nil")
	}
	if !strings.Contains(err.Error(), "missing peer id") {
		t.Fatalf("relayNodePeerID error = %q, want it to name the missing peer id", err)
	}
}
