package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestDefaultPublicCommandGrantsChannelRelayAccess proves
// shmevent.DefaultPublicGroupID/DefaultPublicCommandID's whole design end
// to end (see that constant's doc comment and kvfsm's
// grantChannelRelayAccess): a totally unknown peer -- never a cluster
// member, never granted anything -- starts with no Channel or relay
// access at all; submitting the default public command (already
// reachable for it since ensureDefaultPublicCommand links it to a Public
// group) succeeds and leaves a verifiable CommandRequest log entry behind
// (checking success log); and only after that does the same peer gain
// real Channel and relay access, proving the auto-grant side effect
// actually landed rather than just the bare submission succeeding.
func TestDefaultPublicCommandGrantsChannelRelayAccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// leader bootstraps through the real handleAdd("") path, so it already
	// has the default public group/command ensureDefaultPublicCommand
	// creates -- this test is about that real bootstrap behavior, not a
	// hand-assembled fixture.
	leader := startTestLeader(t, ctx, Config{})

	// stranger is a second, fully independent node -- never joined to
	// leader's raft cluster, never granted anything -- playing the "totally
	// unknown peer" role. A bare Node (not just newTestRemoteHost's raw
	// libp2p host) because it also needs to dispatch a real
	// EventChannelOpen against leader below, which only a *Node can do.
	stranger := startExecuteTestNode(t, filepath.Join(t.TempDir(), "stranger"))
	connectPeers(t, ctx, stranger, leader)

	strangerPeerID, err := peer.Decode(stranger.peerID)
	if err != nil {
		t.Fatalf("decode stranger peer id: %v", err)
	}
	leaderPeerID, err := peer.Decode(leader.peerID)
	if err != nil {
		t.Fatalf("decode leader peer id: %v", err)
	}

	openChannel := func(id uint16) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, stranger, channelOpenMsg(t, leader.peerID, id), stranger.ed25519Priv)
	}

	// Before: no standing at all, on either resource.
	if resp := openChannel(1); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("channel_open from a totally unknown peer unexpectedly succeeded before submitting the public command")
	}
	if leader.isAuthorizedForGatedAccess(strangerPeerID, shmevent.ReservedGroupRelay) {
		t.Fatal("stranger unexpectedly already had relay access before submitting the public command")
	}

	// Submit the default public command as a remote, non-member caller --
	// remoteAppend/remoteQuery (command_log_carveout_test.go) work against
	// any lp2phost.Host, including a full *Node's own .host.
	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	instanceID := "default-public-command-instance"
	submitResp := remoteAppend(t, ctx, stranger.host, stranger.ed25519Priv, leaderPeerID, shmevent.CommandRequestLogKind(shmevent.DefaultPublicCommandID), instanceID, base)
	if submitResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("submitting the default public command was rejected: %s", mustErrMessage(t, submitResp))
	}

	// Checking success log: the CommandRequest record must actually be
	// there, readable back through the identical carve-out that let the
	// stranger submit it in the first place.
	lo, hi := logrecord.ScanBounds(shmevent.CommandRequestLogKind(shmevent.DefaultPublicCommandID), instanceID, base.Add(-time.Hour), base.Add(time.Hour))
	queryResp := remoteQuery(t, ctx, stranger.host, stranger.ed25519Priv, leaderPeerID, lo, hi)
	if queryResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("reading back the submitted command's log entry was rejected: %s", mustErrMessage(t, queryResp))
	}
	queryKey, err := queryResp.ListRange().Key()
	if err != nil {
		t.Fatalf("ListRange key: %v", err)
	}
	if len(queryKey) == 0 {
		t.Fatal("no log entry found for the submitted default public command -- submission did not actually land")
	}

	// After: the same peer now has real access to both resources, granted
	// atomically as part of that same submission.
	if resp := openChannel(2); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("channel_open from the stranger was rejected after submitting the public command: %s", mustErrMessage(t, resp))
	}
	if !leader.isAuthorizedForGatedAccess(strangerPeerID, shmevent.ReservedGroupRelay) {
		t.Fatal("stranger still has no relay access after submitting the public command")
	}
}
