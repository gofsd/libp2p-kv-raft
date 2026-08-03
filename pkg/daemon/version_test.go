package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestGetVersionLocal proves a local caller gets back a real, decodable
// VersionInfo. Commit/BuildTime/Dirty depend on the Go toolchain's VCS
// build stamping actually being available in whatever environment runs
// this test (see shmevent.VersionInfo's doc comment on when it isn't), so
// only GoVersion -- always populated from runtime.Version(), no VCS
// dependency -- is asserted non-empty; the point of this test is that the
// event is wired up and decodes cleanly, not this checkout's own git state.
func TestGetVersionLocal(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})

	resp := callLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventGetVersion, ID: 1}, leader.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("get_version rejected: %s", resp.Value)
	}
	info, err := shmevent.DecodeVersionInfo(resp.Value)
	if err != nil {
		t.Fatalf("DecodeVersionInfo: %v", err)
	}
	if info.GoVersion == "" {
		t.Fatal("GoVersion unexpectedly empty")
	}
}

// TestGetVersionOpenToUnauthorizedRemoteStranger proves
// shmevent.EventGetVersion's whole design point (see its own doc comment):
// a totally unknown remote peer -- never a cluster member, never granted
// ReservedGroupRemote or anything else -- can still query build/version
// info directly over ClientProtocolID, with no SubmitCommand/CommandRequest
// round trip needed.
func TestGetVersionOpenToUnauthorizedRemoteStranger(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	stranger := startExecuteTestNode(t, filepath.Join(t.TempDir(), "stranger"))
	connectPeers(t, ctx, stranger, leader)

	leaderPeerID, err := peer.Decode(leader.peerID)
	if err != nil {
		t.Fatalf("decode leader peer id: %v", err)
	}

	resp, err := callClientProtocol(ctx, stranger.host, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventGetVersion,
		ID:        1,
	}, stranger.ed25519Priv)
	if err != nil {
		t.Fatalf("callClientProtocol: %v", err)
	}
	if resp.EventType == shmevent.EventError {
		t.Fatalf("get_version from an unauthorized stranger was rejected: %s", resp.Value)
	}
	info, err := shmevent.DecodeVersionInfo(resp.Value)
	if err != nil {
		t.Fatalf("DecodeVersionInfo: %v", err)
	}
	if info.GoVersion == "" {
		t.Fatal("GoVersion unexpectedly empty")
	}
}
