package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestPermitRequestConfirmWorkflow is a real-cluster test (a leader, a
// joined voter, and a joined nonvoter/learner, no mocks) for
// EventLifecycleWrite's Permit-style Request/Confirm actions: it proves the
// two-stage pending-then-confirmed record actually gets replicated through
// raft like any other Set, and specifically that Confirm's "only a
// raft voter may confirm" rule is enforced against the real,
// libp2p-authenticated identity of whichever node originates the
// confirm -- not a client-supplied claim -- by exercising the rejection
// path against a genuine joined learner, not a hand-constructed error.
func TestPermitRequestConfirmWorkflow(t *testing.T) {
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

	// Sanity check both landed in the leader's configuration with the
	// suffrage this test's rejection/success expectations depend on.
	cfgFuture := leader.getRaft().GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get leader configuration: %v", err)
	}
	wantSuffrage := map[raft.ServerID]raft.ServerSuffrage{
		raft.ServerID(voter.peerID):   raft.Voter,
		raft.ServerID(learner.peerID): raft.Nonvoter,
	}
	for id, want := range wantSuffrage {
		var found bool
		for _, srv := range cfgFuture.Configuration().Servers {
			if srv.ID == id {
				found = true
				if srv.Suffrage != want {
					t.Fatalf("%s joined with suffrage %v, want %v", id, srv.Suffrage, want)
				}
			}
		}
		if !found {
			t.Fatalf("%s not present in leader's raft configuration", id)
		}
	}

	// call mirrors what pkg/ipc.Serve does for a local caller: sign with
	// n's own key, decode back into the (Msg, crc, sig) triple
	// handleShmEvent expects, and dispatch.
	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, n, m, n.ed25519Priv)
	}

	const targetPeerIDStr = "some-new-node-peer-id"
	targetPeerID := []byte(targetPeerIDStr)
	const metadataStr = "/ip4/127.0.0.1/tcp/4001"

	reqMsg, err := shmevent.NewPermitRequest(shmevent.KindBootstrapNode, targetPeerIDStr, metadataStr)
	if err != nil {
		t.Fatalf("NewPermitRequest: %v", err)
	}
	reqMsg.SetId(1)
	resp := call(leader, reqMsg)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("permit_request rejected: %s", mustErrMessage(t, resp))
	}

	newConfirmMsg := func(id uint16) shmevent.Msg {
		m, err := shmevent.NewPermitConfirm(shmevent.KindBootstrapNode, targetPeerIDStr)
		if err != nil {
			t.Fatalf("NewPermitConfirm: %v", err)
		}
		m.SetId(id)
		return m
	}

	// A learner (nonvoter) confirming must be rejected, and specifically
	// for the voter-only reason -- not because e.g. it couldn't find a
	// leader at all.
	resp = call(learner, newConfirmMsg(2))
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatalf("learner permit_confirm unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "not a current raft voter") {
		t.Fatalf("learner permit_confirm rejected for the wrong reason: %s", mustErrMessage(t, resp))
	}

	// The pending record must still be there -- the rejected confirm must
	// not have consumed or altered it.
	pendingKey := shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusPending, targetPeerID)
	newGetMsg := func(key []byte, id uint16) shmevent.Msg {
		m, err := shmevent.NewGetFieldByKey(key)
		if err != nil {
			t.Fatalf("NewGetFieldByKey: %v", err)
		}
		m.SetId(id)
		return m
	}
	getResp := call(leader, newGetMsg(pendingKey, 3))
	if getResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("pending record missing after rejected learner confirm: %s", mustErrMessage(t, getResp))
	}

	// A real voter confirming must succeed.
	resp = call(voter, newConfirmMsg(4))
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("voter permit_confirm rejected: %s", mustErrMessage(t, resp))
	}

	confirmedKey := shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusConfirmed, targetPeerID)
	getResp = call(leader, newGetMsg(confirmedKey, 5))
	if getResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("confirmed record missing after voter confirm: %s", mustErrMessage(t, getResp))
	}

	// The confirmed record's metadata must be exactly what the original
	// request carried (this codebase no longer stamps anything onto it
	// server-side -- that special case existed only for the now-removed
	// KindPermitPeer's relay allotment).
	confirmedValue, err := getResp.GetFieldByKey().Value()
	if err != nil {
		t.Fatalf("GetFieldByKey value: %v", err)
	}
	if string(confirmedValue) != metadataStr {
		t.Fatalf("confirmed record metadata = %q, want %q", confirmedValue, metadataStr)
	}

	getResp = call(leader, newGetMsg(pendingKey, 6))
	if getResp.Which() != shmevent.Event_Which_error {
		t.Fatal("pending record still present after successful confirm -- should have been consumed")
	}
}

// TestPermitRevokeWorkflow is TestPermitRequestConfirmWorkflow's
// counterpart for the Permit-style Revoke action: same real leader/voter/learner
// cluster, proving revoke is voter-gated the identical way confirm is
// (rejected from the learner, accepted from the voter) and that it
// actually deletes the confirmed record via kvfsm.OpDel rather than just
// reporting success.
func TestPermitRevokeWorkflow(t *testing.T) {
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
		return callLocal(t, ctx, n, m, n.ed25519Priv)
	}

	const targetPeerIDStr = "some-revoked-node-peer-id"
	targetPeerID := []byte(targetPeerIDStr)

	reqMsg, err := shmevent.NewPermitRequest(shmevent.KindBootstrapNode, targetPeerIDStr, "")
	if err != nil {
		t.Fatalf("NewPermitRequest: %v", err)
	}
	reqMsg.SetId(1)
	resp := call(leader, reqMsg)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("permit_request rejected: %s", mustErrMessage(t, resp))
	}

	newConfirmMsg := func(id uint16) shmevent.Msg {
		m, err := shmevent.NewPermitConfirm(shmevent.KindBootstrapNode, targetPeerIDStr)
		if err != nil {
			t.Fatalf("NewPermitConfirm: %v", err)
		}
		m.SetId(id)
		return m
	}
	resp = call(voter, newConfirmMsg(2))
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("voter permit_confirm rejected: %s", mustErrMessage(t, resp))
	}

	confirmedKey := shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusConfirmed, targetPeerID)
	newGetMsg := func(key []byte, id uint16) shmevent.Msg {
		m, err := shmevent.NewGetFieldByKey(key)
		if err != nil {
			t.Fatalf("NewGetFieldByKey: %v", err)
		}
		m.SetId(id)
		return m
	}
	getResp := call(leader, newGetMsg(confirmedKey, 3))
	if getResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("confirmed record missing after voter confirm: %s", mustErrMessage(t, getResp))
	}

	newRevokeMsg := func(id uint16) shmevent.Msg {
		m, err := shmevent.NewPermitRevoke(shmevent.KindBootstrapNode, targetPeerIDStr)
		if err != nil {
			t.Fatalf("NewPermitRevoke: %v", err)
		}
		m.SetId(id)
		return m
	}

	// A learner (nonvoter) revoking must be rejected, for the same
	// voter-only reason confirm is, and must not touch the confirmed
	// record.
	resp = call(learner, newRevokeMsg(4))
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatalf("learner permit_revoke unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "not a current raft voter") {
		t.Fatalf("learner permit_revoke rejected for the wrong reason: %s", mustErrMessage(t, resp))
	}
	getResp = call(leader, newGetMsg(confirmedKey, 5))
	if getResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("confirmed record missing after rejected learner revoke: %s", mustErrMessage(t, getResp))
	}

	// A real voter revoking must succeed and actually delete the
	// confirmed record.
	resp = call(voter, newRevokeMsg(6))
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("voter permit_revoke rejected: %s", mustErrMessage(t, resp))
	}
	getResp = call(leader, newGetMsg(confirmedKey, 7))
	if getResp.Which() != shmevent.Event_Which_error {
		t.Fatal("confirmed record still present after successful revoke -- should have been deleted")
	}
}
