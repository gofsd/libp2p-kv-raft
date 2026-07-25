package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestCommandRequestSubmitFromNonVoterForwardsWithoutVoterCheck proves a
// SubmitCommand dispatch (kvfsm.OpAppendCommandRequest) from a peer with
// no raft voter standing at all -- a non-voting learner, the only
// suffrage a web-app browser tab or a phone joined with suffrage
// "learner" can ever hold -- still reaches the leader and is authorized
// purely on its own real Group/GroupCommand/PeerGroup ACL standing, the
// same way TestGroupPutRequiresVoter proves the group-based ACL catalog's
// Put/Delete events *are* voter-gated. OpAppendCommandRequest must NOT
// share that gate: it's forwarded over the plain (non-voter-gated)
// ForwardProtocolID via handleOpForward, not handleConfirmForward's
// voter-gated ForwardConfirmProtocolID -- routing it through the latter
// would reject every legitimate non-voter submitter with "not a current
// raft voter" before Apply's own ACL check ever runs.
func TestCommandRequestSubmitFromNonVoterForwardsWithoutVoterCheck(t *testing.T) {
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

	const groupID = "grp-nonvoter-submit"
	const commandID = "cmd-nonvoter-submit"

	groupPayload, err := shmevent.EncodeGroupPutPayload(groupID, "Non-Voter Submit Group", false)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}
	if resp := call(leader, shmevent.Msg{EventType: shmevent.EventGroupPut, Value: groupPayload, ID: 1}); resp.EventType == shmevent.EventError {
		t.Fatalf("group_put: %s", resp.Value)
	}

	cmdPayload, err := shmevent.EncodeCommandPutPayload(commandID, "Echo", []byte(leader.peerID))
	if err != nil {
		t.Fatalf("EncodeCommandPutPayload: %v", err)
	}
	if resp := call(leader, shmevent.Msg{EventType: shmevent.EventCommandPut, Value: cmdPayload, ID: 2}); resp.EventType == shmevent.EventError {
		t.Fatalf("command_put: %s", resp.Value)
	}

	gcPayload, err := shmevent.EncodeGroupCommandPayload([]byte(commandID), []byte(groupID))
	if err != nil {
		t.Fatalf("EncodeGroupCommandPayload: %v", err)
	}
	if resp := call(leader, shmevent.Msg{EventType: shmevent.EventGroupCommandPut, Value: gcPayload, ID: 3}); resp.EventType == shmevent.EventError {
		t.Fatalf("group_command_put: %s", resp.Value)
	}

	// Grant the learner -- never a raft voter -- real ACL standing for
	// commandID via the group just created.
	pgPayload, err := shmevent.EncodePeerGroupPayload([]byte(learner.peerID), []byte(groupID))
	if err != nil {
		t.Fatalf("EncodePeerGroupPayload: %v", err)
	}
	if resp := call(leader, shmevent.Msg{EventType: shmevent.EventPeerGroupPut, Value: pgPayload, ID: 4}); resp.EventType == shmevent.EventError {
		t.Fatalf("peer_group_put: %s", resp.Value)
	}

	// Give raft a moment to replicate the ACL state to the learner before
	// it submits (isPermittedForCommand runs against the leader's own
	// state at Apply time, but this also lets AppendEntries settle so the
	// learner isn't racing its own join).
	time.Sleep(300 * time.Millisecond)

	rec := logrecord.Record{
		Kind:         shmevent.CommandRequestLogKind(commandID),
		UnitID:       "inst-1",
		Timestamp:    time.Now(),
		AuthorPeerID: learner.peerID,
		Fields:       map[string]string{"command_id": commandID},
	}
	recValue, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	recKey, err := logrecord.BuildKey(rec.Kind, rec.UnitID, rec.Timestamp, [logrecord.RandSize]byte{})
	if err != nil {
		t.Fatalf("logrecord.BuildKey: %v", err)
	}
	setPayload, err := shmevent.EncodeSetPayload(recKey, recValue)
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}

	// The learner is never a raft voter -- if this were still routed
	// through handleConfirmForward's voter-gated ForwardConfirmProtocolID,
	// it would fail here with "not a current raft voter" instead of
	// succeeding on the merits of the PeerGroup grant above.
	resp := call(learner, shmevent.Msg{EventType: shmevent.EventLogAppend, Value: setPayload, ID: 5})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("learner log_append (command request submit) rejected: %s", resp.Value)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, err := leader.store.Get(recKey); err == nil {
			decoded, err := logrecord.Decode(v)
			if err != nil {
				t.Fatalf("decode replicated record: %v", err)
			}
			if decoded.Fields["command_id"] == commandID {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("command request from learner never became readable on the leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
