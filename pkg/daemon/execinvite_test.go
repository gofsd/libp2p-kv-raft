package daemon

import (
	"context"
	"crypto/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

func newExecInviteToken(t *testing.T) []byte {
	t.Helper()
	token := make([]byte, shmevent.ExecInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

func execInviteCall(t *testing.T, ctx context.Context, n *Node, m shmevent.Msg) shmevent.Msg {
	t.Helper()
	return callLocal(t, ctx, n, m, n.ed25519Priv)
}

func startExecInviteNode(t *testing.T, tmpDir, name string, cfg Config) *Node {
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

// setUpExecInviteACL creates commandID on leader (executing at leader's own
// peer id) and links it, via a fresh group, to redeemerPeerID -- the
// Group/Command/GroupCommand/PeerGroup wiring isPermittedForCommand
// requires -- using the same ordinary EventCatalogPut calls `mage
// creategroup` et al. drive, not a shortcut.
func setUpExecInviteACL(t *testing.T, ctx context.Context, leader *Node, commandID, groupID, redeemerPeerID string) {
	t.Helper()

	groupMsg, err := shmevent.NewGroupPut(groupID, groupID, false)
	if err != nil {
		t.Fatalf("NewGroupPut: %v", err)
	}
	groupMsg.SetId(101)
	if resp := execInviteCall(t, ctx, leader, groupMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("group_put rejected: %s", mustErrMessage(t, resp))
	}

	commandMsg, err := shmevent.NewCommandPut(commandID, commandID, leader.peerID)
	if err != nil {
		t.Fatalf("NewCommandPut: %v", err)
	}
	commandMsg.SetId(102)
	if resp := execInviteCall(t, ctx, leader, commandMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("command_put rejected: %s", mustErrMessage(t, resp))
	}

	groupCommandMsg, err := shmevent.NewGroupCommandPut(commandID, groupID)
	if err != nil {
		t.Fatalf("NewGroupCommandPut: %v", err)
	}
	groupCommandMsg.SetId(103)
	if resp := execInviteCall(t, ctx, leader, groupCommandMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("group_command_put rejected: %s", mustErrMessage(t, resp))
	}

	peerGroupMsg, err := shmevent.NewPeerGroupPut(redeemerPeerID, groupID)
	if err != nil {
		t.Fatalf("NewPeerGroupPut: %v", err)
	}
	peerGroupMsg.SetId(104)
	if resp := execInviteCall(t, ctx, leader, peerGroupMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("peer_group_put rejected: %s", mustErrMessage(t, resp))
	}
}

// TestExecInviteRedeemByPermittedPeerSucceedsAndIsOneTime is this
// feature's central claim, exercised against a real two-node topology (no
// mocks, a genuine libp2p dial): a peer with real Group/Command ACL
// standing can scan/redeem a one-time execution invite it was handed
// out-of-band, and the very same token presented again afterward -- even
// by the same, still-permitted peer -- is rejected, proving the raft
// FSM's atomic consume actually took effect.
func TestExecInviteRedeemByPermittedPeerSucceedsAndIsOneTime(t *testing.T) {
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

	leader := startExecInviteNode(t, tmpDir, "leader", fastRaft)
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	// The redeemer is a standalone node with its own identity -- it never
	// joins leader's raft cluster at all, matching the real use case of a
	// peer that only has Group/Command ACL standing, not raft membership.
	redeemer := startExecInviteNode(t, tmpDir, "redeemer", fastRaft)
	defer redeemer.shutdown()

	setUpExecInviteACL(t, ctx, leader, "cmd-1", "grp-1", redeemer.peerID)

	token := newExecInviteToken(t)
	createMsg, err := shmevent.NewExecInviteCreate(token, "cmd-1", `{"x":1}`)
	if err != nil {
		t.Fatalf("NewExecInviteCreate: %v", err)
	}
	createMsg.SetId(1)
	if resp := execInviteCall(t, ctx, leader, createMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("exec_invite_create rejected: %s", mustErrMessage(t, resp))
	}

	redeemMsg, err := shmevent.NewExecInviteRedeem(leaderAddr, token)
	if err != nil {
		t.Fatalf("NewExecInviteRedeem: %v", err)
	}
	redeemMsg.SetId(2)
	resp := execInviteCall(t, ctx, redeemer, redeemMsg)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("exec_invite_redeem rejected: %s", mustErrMessage(t, resp))
	}
	instanceID, err := resp.ExecInviteRedeem().InstanceId()
	if err != nil {
		t.Fatalf("ExecInviteRedeem instance_id: %v", err)
	}
	if instanceID == "" {
		t.Fatal("exec_invite_redeem succeeded but returned an empty instance id")
	}

	// The durable CommandRequest record must actually be there, attributed
	// to the redeeming peer -- not the leader/invite-creator.
	deadline := time.Now().Add(10 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		matches, err := leader.store.ScanPrefix(logrecord.KindPrefix("cmdreq:cmd-1"), 0)
		if err == nil {
			for _, m := range matches {
				if strings.Contains(string(m.Value), redeemer.peerID) && strings.Contains(string(m.Value), instanceID) {
					found = true
					break
				}
			}
		}
		if found {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found {
		t.Fatal("no CommandRequest record found on leader attributed to the redeeming peer")
	}

	// Redeeming the identical token again -- even by the same, still
	// permitted peer -- must fail: the invite is gone.
	redeemMsg2, err := shmevent.NewExecInviteRedeem(leaderAddr, token)
	if err != nil {
		t.Fatalf("NewExecInviteRedeem: %v", err)
	}
	redeemMsg2.SetId(3)
	resp = execInviteCall(t, ctx, redeemer, redeemMsg2)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("second exec_invite_redeem with the already-consumed token unexpectedly succeeded")
	}
}

// TestExecInviteRedeemByUnpermittedPeerFailsAndKeepsInvite proves the ACL
// half of the contract end-to-end: a peer with no Group/Command standing
// for the invite's command is rejected, and -- unlike a successful
// redemption -- the invite survives that rejection, so a legitimate peer
// can redeem it afterward.
func TestExecInviteRedeemByUnpermittedPeerFailsAndKeepsInvite(t *testing.T) {
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

	leader := startExecInviteNode(t, tmpDir, "leader2", fastRaft)
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	unpermitted := startExecInviteNode(t, tmpDir, "unpermitted", fastRaft)
	defer unpermitted.shutdown()

	permitted := startExecInviteNode(t, tmpDir, "permitted", fastRaft)
	defer permitted.shutdown()

	// Only "permitted" gets a PeerGroup grant; "unpermitted" has none.
	setUpExecInviteACL(t, ctx, leader, "cmd-2", "grp-2", permitted.peerID)

	token := newExecInviteToken(t)
	createMsg, err := shmevent.NewExecInviteCreate(token, "cmd-2", "")
	if err != nil {
		t.Fatalf("NewExecInviteCreate: %v", err)
	}
	createMsg.SetId(1)
	if resp := execInviteCall(t, ctx, leader, createMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("exec_invite_create rejected: %s", mustErrMessage(t, resp))
	}

	redeemMsg, err := shmevent.NewExecInviteRedeem(leaderAddr, token)
	if err != nil {
		t.Fatalf("NewExecInviteRedeem: %v", err)
	}
	redeemMsg.SetId(2)
	resp := execInviteCall(t, ctx, unpermitted, redeemMsg)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("exec_invite_redeem by an unpermitted peer unexpectedly succeeded")
	}

	// The invite must still be redeemable by a legitimately permitted peer.
	redeemMsg2, err := shmevent.NewExecInviteRedeem(leaderAddr, token)
	if err != nil {
		t.Fatalf("NewExecInviteRedeem: %v", err)
	}
	redeemMsg2.SetId(3)
	resp = execInviteCall(t, ctx, permitted, redeemMsg2)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("exec_invite_redeem by a permitted peer, after an earlier unpermitted attempt, was unexpectedly rejected: %s", mustErrMessage(t, resp))
	}
	instanceID, err := resp.ExecInviteRedeem().InstanceId()
	if err != nil {
		t.Fatalf("ExecInviteRedeem instance_id: %v", err)
	}
	if instanceID == "" {
		t.Fatal("exec_invite_redeem by a permitted peer succeeded but returned an empty instance id")
	}
}
