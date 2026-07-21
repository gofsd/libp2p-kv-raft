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

// TestConfirmGatedJoinRequiresVoterConfirmation is a real-cluster test (no
// mocks) for Config.RequireConfirmForJoin: it proves a join request
// against a leader with that flag set only lodges a pending
// shmevent.KindClusterJoin record (join() gets back "pending", not "ok",
// and the joiner is NOT yet in the leader's raft configuration), and that
// the joiner is only actually admitted (raft.AddVoter, via applyConfirm's
// KindClusterJoin special case) once a *different, non-leader* raft voter
// confirms it -- exercising "any voter, not just the leader" specifically,
// since a leader confirming its own pending record would be the less
// interesting case.
func TestConfirmGatedJoinRequiresVoterConfirmation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:      200 * time.Millisecond,
		ElectionTimeout:       200 * time.Millisecond,
		CommitTimeout:         20 * time.Millisecond,
		LeaderLeaseTimeout:    100 * time.Millisecond,
		RequireConfirmForJoin: true,
	}

	startNode := func(cfg Config, name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	leader := startNode(fastRaft, "leader")
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	// Admit a second, real voter directly (bypassing the join wire
	// protocol) purely as test setup, so there's a non-leader voter
	// available to exercise "any voter, not just the leader" below --
	// the point of this test is confirm's authorization, not join's.
	voter := startNode(fastRaft, "voter")
	defer voter.shutdown()
	if _, err := voter.initRaft(); err != nil {
		t.Fatalf("init voter raft: %v", err)
	}
	if line := leader.addServerLine(ctx, leader.getRaft(), voter.peerID, voter.advertisedAddrs()[0], raft.Voter); line != "OK" {
		t.Fatalf("admit voter directly: %s", line)
	}

	joiner := startNode(fastRaft, "joiner")
	defer joiner.shutdown()

	status, err := joiner.handleAdd(ctx, leaderAddr)
	if err != nil {
		t.Fatalf("joiner handleAdd: %v", err)
	}
	if status != joiner.peerID+" pending" {
		t.Fatalf("handleAdd status = %q, want %q", status, joiner.peerID+" pending")
	}

	isMember := func(rf *raft.Raft, id string) bool {
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

	if isMember(leader.getRaft(), joiner.peerID) {
		t.Fatal("joiner already in leader's raft configuration before any confirmation")
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

	confirmPayload := shmevent.EncodePermitConfirmPayload(shmevent.KindClusterJoin, []byte(joiner.peerID))
	resp := call(voter, shmevent.Msg{EventType: shmevent.EventPermitConfirm, Value: confirmPayload, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("voter confirm rejected: %s", resp.Value)
	}

	if !isMember(leader.getRaft(), joiner.peerID) {
		t.Fatal("joiner not in leader's raft configuration after confirmation")
	}
}

// TestLeaveShrinksClusterGracefully is a real-cluster test for
// shmevent.EventLeave/raft.RemoveServer: a joined voter leaving is
// removed from the leader's raft configuration (and its KindClusterMember
// record deleted), while the leader itself keeps operating normally
// afterward -- the "shrink, don't break" guarantee EventLeave/Rm depend
// on.
func TestLeaveShrinksClusterGracefully(t *testing.T) {
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

	voter := startNode("voter")
	defer voter.shutdown()
	if _, err := voter.handleAdd(ctx, leaderAddr); err != nil {
		t.Fatalf("join voter: %v", err)
	}

	isMember := func(rf *raft.Raft, id string) bool {
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
	if !isMember(leader.getRaft(), voter.peerID) {
		t.Fatal("voter not in leader's raft configuration after joining")
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

	// voter is not the leader, so this exercises leaveCluster's forwarded
	// (ForwardLeaveProtocolID) path, not just the direct-apply one.
	resp := call(voter, shmevent.Msg{EventType: shmevent.EventLeave, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("leave rejected: %s", resp.Value)
	}

	if isMember(leader.getRaft(), voter.peerID) {
		t.Fatal("voter still in leader's raft configuration after leaving")
	}

	memberKey := shmevent.ClusterMemberKey([]byte(voter.peerID))
	getResp := call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: memberKey, ID: 2})
	if getResp.EventType != shmevent.EventError {
		t.Fatal("voter's KindClusterMember record still present after leaving -- should have been deleted")
	}

	// The remaining cluster (just the leader, now the sole voter again)
	// must keep operating normally after the shrink.
	setPayload, err := shmevent.EncodeSetPayload([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	resp = call(leader, shmevent.Msg{EventType: shmevent.EventSet, Value: setPayload, ID: 3})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("set after leave rejected: %s", resp.Value)
	}
}

// TestOriginRejoinsClusterCatchesUpOnMissedWrites is a real-cluster test
// for the "leave, let the cluster you originated carry on without you, then
// come back" round trip: it proves the returning identity is not stuck with
// a stale snapshot of the cluster as it was when it left -- raft's own
// AddVoter + snapshot/log-replication machinery (pkg/kvfsm.FSM's
// Snapshot/Restore, wired via raft.NewFileSnapshotStore in start) brings it
// fully current with every write the cluster committed while it was gone,
// exactly like any other join. This is what `mage leave <peerID>` followed
// later by `mage join <peerID>` gets an operator in production; unlike
// TestLeaveShrinksClusterGracefully (which only checks the shrink side),
// this exercises the return side end to end.
func TestOriginRejoinsClusterCatchesUpOnMissedWrites(t *testing.T) {
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

	startNode := func(cfg Config, name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	// origin bootstraps its own single-node cluster -- the "default
	// cluster" a returning identity should end up back on.
	origin := startNode(fastRaft, "origin")
	originAddr := origin.advertisedAddrs()[0]
	originKey := filepath.Join(tmpDir, "origin.key")
	if _, err := origin.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap origin: %v", err)
	}

	// member joins that cluster, and a write lands before origin ever
	// leaves -- this must survive the round trip too, not just writes made
	// after origin is gone.
	member := startNode(fastRaft, "member")
	defer member.shutdown()
	if _, err := member.handleAdd(ctx, originAddr); err != nil {
		t.Fatalf("join member: %v", err)
	}
	if err := origin.handleSetForward(ctx, []byte("k1"), []byte("v1"), true); err != nil {
		t.Fatalf("set k1 on origin: %v", err)
	}

	// origin leaves the cluster it originated (removes itself while still
	// leader, the same shmevent.EventLeave path `mage leave` drives) and
	// its process is torn down -- mirroring resumeSolo's stop-then-restart,
	// not a raft instance left dangling mid-removal.
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
	resp := call(origin, shmevent.Msg{EventType: shmevent.EventLeave, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("origin leave rejected: %s", resp.Value)
	}
	origin.shutdown()

	// The cluster carries on without origin: member becomes the new
	// leader on its own and accepts further writes, entirely unaware
	// origin will ever come back.
	deadline := time.Now().Add(20 * time.Second)
	for member.getRaft().State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatalf("member never became leader after origin left; state=%s", member.getRaft().State())
		}
		time.Sleep(50 * time.Millisecond)
	}
	memberAddr := member.advertisedAddrs()[0]
	if err := member.handleSetForward(ctx, []byte("k2"), []byte("v2"), true); err != nil {
		t.Fatalf("set k2 on member: %v", err)
	}

	// origin comes back under the exact same identity (same key, fresh
	// data dir -- the kvctl.rejoin equivalent of switching to a new
	// registry.ClusterDataDir) and rejoins the cluster it originated,
	// pointed at whichever member is now its leader.
	returned, err := start(Config{
		DataDir:            filepath.Join(tmpDir, "origin-returned"),
		KeyPath:            originKey,
		HeartbeatTimeout:   fastRaft.HeartbeatTimeout,
		ElectionTimeout:    fastRaft.ElectionTimeout,
		CommitTimeout:      fastRaft.CommitTimeout,
		LeaderLeaseTimeout: fastRaft.LeaderLeaseTimeout,
	})
	if err != nil {
		t.Fatalf("restart origin identity: %v", err)
	}
	defer returned.shutdown()
	if returned.peerID != origin.peerID {
		t.Fatalf("returned node has a different identity: got %s, want %s", returned.peerID, origin.peerID)
	}
	if _, err := returned.handleAdd(ctx, memberAddr); err != nil {
		t.Fatalf("origin rejoin: %v", err)
	}

	// The returning identity must end up with every write the cluster
	// committed while it was away, both from before it left (k1) and
	// after (k2) -- "take all changes of the cluster", not a resurrected
	// snapshot of the cluster as it looked when origin departed.
	for _, kv := range []struct{ key, want string }{{"k1", "v1"}, {"k2", "v2"}} {
		deadline := time.Now().Add(20 * time.Second)
		for {
			value, err := returned.handleGet([]byte(kv.key))
			if err == nil && string(value) == kv.want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("returned origin never caught up on %s: err=%v value=%q, want %q", kv.key, err, value, kv.want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
