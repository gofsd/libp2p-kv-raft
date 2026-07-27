package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
)

// TestHandleAddLearnerMarkerJoinsAsNonvoter is a real-cluster test for the
// " learner" marker handleAdd now accepts on leaderPeerID (see its doc
// comment): pkg/e2erun's android row runner appends it to keep the shared,
// long-lived e2e leader's quorum from depending on an ephemeral test
// device's connection, but the mechanism itself is generic to any
// join()/JoinProtocolID caller, not android-specific -- what this proves
// is that the marker actually results in raft.Nonvoter landing in the
// leader's configuration (and is stripped before address parsing), the
// same way TestAddLearnerThroughRelay proves ClientProtocolID's separate
// SetKey+EventAdd learner path does.
func TestHandleAddLearnerMarkerJoinsAsNonvoter(t *testing.T) {
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

	joiner := startNode("joiner")
	defer joiner.shutdown()

	status, err := joiner.handleAdd(ctx, leaderAddr+" learner")
	if err != nil {
		t.Fatalf("joiner handleAdd with learner marker: %v", err)
	}
	if status != joiner.peerID+" ok" {
		t.Fatalf("handleAdd status = %q, want %q", status, joiner.peerID+" ok")
	}

	cfgFuture := leader.getRaft().GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get configuration: %v", err)
	}
	var found *raft.Server
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(joiner.peerID) {
			srv := srv
			found = &srv
			break
		}
	}
	if found == nil {
		t.Fatalf("joiner %s not found in leader's raft configuration", joiner.peerID)
	}
	if found.Suffrage != raft.Nonvoter {
		t.Fatalf("joiner suffrage = %v, want Nonvoter", found.Suffrage)
	}
}
