package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestPublicAccessGrantsChannelRelayAccess is
// TestDefaultPublicCommandGrantsChannelRelayAccess's counterpart for the
// actual client path: instead of a test helper hand-assembling the
// carve-out submission on the wire, a stranger node is driven the way a
// real caller drives it -- one local EventPublicAccess naming the target's
// multiaddr -- and must end up with the same real Channel/relay standing.
// This is the whole point of that event existing (see its doc comment):
// before it, nothing in this repo could submit the public command to a
// cluster it wasn't already part of, so the escalation path the daemon
// enforces had no way to actually be taken by a device that needed it.
func TestPublicAccessGrantsChannelRelayAccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	stranger := startExecuteTestNode(t, filepath.Join(t.TempDir(), "stranger"))
	connectPeers(t, ctx, stranger, leader)

	strangerPeerID, err := peer.Decode(stranger.peerID)
	if err != nil {
		t.Fatalf("decode stranger peer id: %v", err)
	}

	if leader.isAuthorizedForGatedAccess(strangerPeerID, shmevent.ReservedGroupRelay) {
		t.Fatal("stranger unexpectedly already had relay access before requesting it")
	}
	if leader.isAuthorizedForGatedAccess(strangerPeerID, shmevent.ReservedGroupChannel) {
		t.Fatal("stranger unexpectedly already had channel access before requesting it")
	}

	// The address form a real caller has: the target's own advertised
	// multiaddr, plus an optional note.
	accessMsg, err := shmevent.NewPublicAccess(leader.advertisedAddrs()[0], "e2e-test-device")
	if err != nil {
		t.Fatalf("NewPublicAccess: %v", err)
	}
	accessMsg.SetId(1)
	resp := callLocal(t, ctx, stranger, accessMsg, stranger.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("public_access was rejected: %s", mustErrMessage(t, resp))
	}
	instanceID, err := resp.PublicAccess().InstanceId()
	if err != nil {
		t.Fatalf("PublicAccess instance_id: %v", err)
	}
	if instanceID == "" {
		t.Fatal("public_access returned no instance id")
	}

	if !leader.isAuthorizedForGatedAccess(strangerPeerID, shmevent.ReservedGroupRelay) {
		t.Fatal("stranger still has no relay access after a successful public_access request")
	}
	if !leader.isAuthorizedForGatedAccess(strangerPeerID, shmevent.ReservedGroupChannel) {
		t.Fatal("stranger still has no channel access after a successful public_access request")
	}

	// The grant is what unlocks the resource, not just the group record:
	// an actual channelOpen against the leader must now be admitted.
	openResp := callLocal(t, ctx, stranger, channelOpenMsg(t, leader.peerID, 2), stranger.ed25519Priv)
	if openResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open from the stranger was rejected after public_access: %s", mustErrMessage(t, openResp))
	}
}

// TestPublicAccessRejectsRemoteCaller pins EventPublicAccess's local-only
// gate. It makes the receiving node act as a client under its *own*
// identity, so a remote caller triggering it would spend that node's
// standing on a stranger's behalf -- the same reasoning
// EventExecInviteRedeem/EventRecruit are local-only for.
func TestPublicAccessRejectsRemoteCaller(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	accessMsg, err := shmevent.NewPublicAccess("/ip4/127.0.0.1/tcp/1/p2p/"+leader.peerID, "")
	if err != nil {
		t.Fatalf("NewPublicAccess: %v", err)
	}
	accessMsg.SetId(1)
	resp, err := callClientProtocol(ctx, remote, leaderPeerID, accessMsg, remotePriv)
	if err != nil {
		t.Fatalf("public_access call: %v", err)
	}
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("public_access from a remote caller unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "not available to a remote caller") {
		t.Fatalf("public_access rejected for the wrong reason: %s", mustErrMessage(t, resp))
	}
}
