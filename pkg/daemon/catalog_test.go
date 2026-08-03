package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestGroupPutRequiresVoter mirrors TestLogPermitConfirmRevokeVoterOnly's
// leader/voter/learner topology for the group-based ACL catalog's
// single-step Put events (see shmevent.KindGroup's doc comment): a
// non-voter learner's EventGroupPut must be rejected outright, while a
// real voter's succeeds and is actually readable afterward -- proving the
// widened handleForwardConfirmStream op check (kvfsm.OpSet, previously
// only OpConfirm/OpDel) didn't loosen the "only a raft voter may act"
// enforcement it shares with EventPermitConfirm/EventLogPermitConfirm.
func TestGroupPutRequiresVoter(t *testing.T) {
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

	learner := startNode("learner")
	defer learner.shutdown()
	if _, err := learner.initRaft(); err != nil {
		t.Fatalf("init learner raft: %v", err)
	}
	if _, err := leader.handleAddLearner(ctx, learner.peerID, learner.advertisedAddrs()[0]); err != nil {
		t.Fatalf("join learner: %v", err)
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

	putPayload, err := shmevent.EncodeGroupPutPayload("grp-voter-only", "Voter Only Group", false)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}

	// A learner (nonvoter) putting a group must be rejected.
	resp := call(learner, shmevent.Msg{EventType: shmevent.EventGroupPut, Value: putPayload, ID: 1})
	if resp.EventType != shmevent.EventError {
		t.Fatal("learner group_put unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not a current raft voter") {
		t.Fatalf("learner group_put rejected for the wrong reason: %s", resp.Value)
	}

	// A real voter putting a group must succeed, and be readable
	// afterward via a plain get_field against shmevent.GroupKey.
	resp = call(voter, shmevent.Msg{EventType: shmevent.EventGroupPut, Value: putPayload, ID: 2})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("voter group_put rejected: %s", resp.Value)
	}

	deadline := time.Now().Add(10 * time.Second)
	groupKey := shmevent.GroupKey([]byte("grp-voter-only"))
	for {
		getResp := call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: groupKey, ID: 3})
		if getResp.EventType != shmevent.EventError {
			if name, _, err := shmevent.DecodeGroupPayload(getResp.Value); err == nil && name == "Voter Only Group" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("group put by voter never became readable: last resp=%+v", getResp)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Deleting it (also voter-gated, via OpCascadeDelete) must likewise be
	// rejected for the learner and succeed for the voter.
	resp = call(learner, shmevent.Msg{EventType: shmevent.EventGroupDelete, Value: []byte("grp-voter-only"), ID: 4})
	if resp.EventType != shmevent.EventError {
		t.Fatal("learner group_delete unexpectedly succeeded")
	}
	resp = call(voter, shmevent.Msg{EventType: shmevent.EventGroupDelete, Value: []byte("grp-voter-only"), ID: 5})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("voter group_delete rejected: %s", resp.Value)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		getResp := call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: groupKey, ID: 6})
		if getResp.EventType == shmevent.EventError {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("group delete by voter never took effect")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestPersonalGroupPutDeleteRejected proves isPeerIdentityGroupID's
// protection: a solo bootstrap leader (a voter, so it would otherwise be
// allowed to Put/Delete any group) is rejected outright when the target
// group id is shaped like a peer id -- whether that's its own personal
// group (ensurePersonalGroup already created one for it, via
// syncMemberGroups reacting to its own self-election) or some other
// peer's, which was never created here at all. Format alone is enough to
// reserve the id; the daemon doesn't need to have actually seen that peer
// before to protect the namespace.
func TestPersonalGroupPutDeleteRejected(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "leader.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate leader key: %v", err)
	}
	leader, err := start(Config{
		DataDir:            filepath.Join(tmpDir, "leader"),
		KeyPath:            key,
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}

	call := func(m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, leader.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return leader.handleShmEvent(ctx, decoded, crc, sig, leader.localCaller())
	}

	// Some arbitrary other peer's id (never seen by this daemon at all --
	// no personal group record for it was ever created here) is rejected
	// purely on format.
	otherKeyPath := filepath.Join(tmpDir, "other.key")
	otherPriv, err := p2praft.LoadOrGenerateKey(otherKeyPath)
	if err != nil {
		t.Fatalf("generate other key: %v", err)
	}
	otherPeerID, err := peer.IDFromPrivateKey(otherPriv)
	if err != nil {
		t.Fatalf("derive other peer id: %v", err)
	}

	for _, id := range []string{leader.peerID, otherPeerID.String()} {
		putPayload, err := shmevent.EncodeGroupPutPayload(id, "renamed", false)
		if err != nil {
			t.Fatalf("EncodeGroupPutPayload: %v", err)
		}
		resp := call(shmevent.Msg{EventType: shmevent.EventGroupPut, Value: putPayload, ID: 1})
		if resp.EventType != shmevent.EventError {
			t.Fatalf("group_put against peer-identity-shaped id %q unexpectedly succeeded", id)
		}
		resp = call(shmevent.Msg{EventType: shmevent.EventGroupDelete, Value: []byte(id), ID: 2})
		if resp.EventType != shmevent.EventError {
			t.Fatalf("group_delete against peer-identity-shaped id %q unexpectedly succeeded", id)
		}
	}
}
