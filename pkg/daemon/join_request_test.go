package daemon

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestRecruitAdmitsPendingNodeAndTicketIsSingleUse is join-request's central
// claim, exercised against a real two-node cluster (no mocks): a device
// with no raft instance at all yet (b -- exactly AddPending's state, never
// sent EventAdd) mints its own ticket, and some other, already-clustered
// node (a, its own solo 1-node cluster) redeems it via EventRecruit --
// which must both mint a real join invite on a's cluster *and* dial b
// directly to hand it over, ending with b admitted as a genuine raft voter
// of a's cluster, with no action of b's own beyond minting the ticket. Also
// proves the ticket is genuinely one-time: a second recruit attempt against
// the same, already-consumed ticket must be rejected outright.
func TestRecruitAdmitsPendingNodeAndTicketIsSingleUse(t *testing.T) {
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

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, n, m, n.ed25519Priv)
	}

	// recruiter: an existing raft cluster (its own solo leader).
	recruiter := startNode("recruiter")
	defer recruiter.shutdown()
	if _, err := recruiter.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap recruiter: %v", err)
	}

	// device: fresh, pending -- no raft instance at all yet, exactly
	// AddPending's state (never sent EventAdd).
	device := startNode("device")
	defer device.shutdown()
	if device.getRaft() != nil {
		t.Fatal("device unexpectedly already has a raft instance before any join-request activity")
	}

	createMsg, err := shmevent.NewJoinRequestCreate()
	if err != nil {
		t.Fatalf("NewJoinRequestCreate: %v", err)
	}
	createMsg.SetId(1)
	createResp := call(device, createMsg)
	if createResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("join_request_create rejected: %s", mustErrMessage(t, createResp))
	}
	correlationToken, err := createResp.JoinRequestCreate().Token()
	if err != nil {
		t.Fatalf("JoinRequestCreate token: %v", err)
	}
	if len(correlationToken) != shmevent.JoinInviteTokenSize {
		t.Fatalf("got token of length %d, want %d", len(correlationToken), shmevent.JoinInviteTokenSize)
	}
	ticket := device.advertisedAddrs()[0] + "#" + hex.EncodeToString(correlationToken)

	recruitMsg, err := shmevent.NewRecruit(ticket, shmevent.SuffrageVoter)
	if err != nil {
		t.Fatalf("NewRecruit: %v", err)
	}
	recruitMsg.SetId(2)
	recruitResp := call(recruiter, recruitMsg)
	if recruitResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("recruit rejected: %s", mustErrMessage(t, recruitResp))
	}
	result, err := recruitResp.Recruit().Ticket()
	if err != nil {
		t.Fatalf("Recruit ticket: %v", err)
	}
	if !strings.HasSuffix(result, " ok") {
		t.Fatalf("got recruit result %q, want it to end in %q", result, " ok")
	}

	cfgFuture := recruiter.getRaft().GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get recruiter configuration: %v", err)
	}
	var found bool
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(device.peerID) {
			found = true
			if srv.Suffrage != raft.Voter {
				t.Fatalf("device joined with suffrage %v, want Voter", srv.Suffrage)
			}
		}
	}
	if !found {
		t.Fatal("device not present in recruiter's raft configuration after recruit")
	}

	// The ticket must now be consumed: a second recruit attempt against the
	// same (already-used) ticket must fail, even though the underlying
	// join-invite minting machinery itself would otherwise happily mint
	// another fresh invite.
	recruitMsg2, err := shmevent.NewRecruit(ticket, shmevent.SuffrageVoter)
	if err != nil {
		t.Fatalf("NewRecruit: %v", err)
	}
	recruitMsg2.SetId(3)
	recruitResp2 := call(recruiter, recruitMsg2)
	if recruitResp2.Which() != shmevent.Event_Which_error {
		t.Fatal("second recruit against the already-consumed ticket unexpectedly succeeded")
	}
}

// TestJoinRequestCreateAndCancelAreLocalOnly proves EventJoinRequestCreate/
// EventJoinRequestCancel/EventRecruit are all rejected outright for a
// remote caller -- there is no legitimate remote use of any of the three
// (see handleShmEvent's rejection block).
func TestJoinRequestCreateAndCancelAreLocalOnly(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "n.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	n, err := start(Config{DataDir: filepath.Join(tmpDir, "n"), KeyPath: key})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer n.shutdown()

	remoteCaller := callerIdentity{remotePeer: n.host.ID(), verifyPub: n.ed25519Pub}

	builders := []func() (shmevent.Msg, error){
		shmevent.NewJoinRequestCreate,
		func() (shmevent.Msg, error) {
			return shmevent.NewJoinRequestCancel(make([]byte, shmevent.JoinInviteTokenSize))
		},
		func() (shmevent.Msg, error) { return shmevent.NewRecruit("some-ticket", shmevent.SuffrageVoter) },
	}
	for _, build := range builders {
		m, err := build()
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		m.SetId(1)
		want := shmevent.EventName(m.Which())
		resp := n.handleShmEvent(ctx, m, 0, nil, remoteCaller)
		if resp.Which() != shmevent.Event_Which_error {
			t.Fatalf("event %s unexpectedly accepted from a remote caller", want)
		}
	}
}

// TestGetOwnAddrWorksWithoutRaft proves EventGetOwnAddr answers even on a
// node with no raft instance at all yet (exactly AddPending's state) --
// unlike everything else that touches raft, this is pure libp2p host
// state, so a device needs to be able to learn its own address (e.g. to
// build a join-request ticket) before it has ever joined anything.
func TestGetOwnAddrWorksWithoutRaft(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "n.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	n, err := start(Config{DataDir: filepath.Join(tmpDir, "n"), KeyPath: key})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer n.shutdown()

	if n.getRaft() != nil {
		t.Fatal("node unexpectedly already has a raft instance")
	}

	m, err := shmevent.NewGetOwnAddr()
	if err != nil {
		t.Fatalf("NewGetOwnAddr: %v", err)
	}
	m.SetId(1)
	resp := callLocal(t, ctx, n, m, n.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("get_own_addr rejected: %s", mustErrMessage(t, resp))
	}
	want := n.advertisedAddrs()[0]
	got, err := resp.GetOwnAddr().Addr()
	if err != nil {
		t.Fatalf("GetOwnAddr addr: %v", err)
	}
	if got != want {
		t.Fatalf("got addr %q, want %q", got, want)
	}
}

// TestJoinRequestCancelInvalidatesBeforeRedemption proves
// EventJoinRequestCancel actually takes effect: a cancelled ticket must be
// rejected by a subsequent recruit exactly like one that was never
// created.
func TestJoinRequestCancelInvalidatesBeforeRedemption(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, n, m, n.ed25519Priv)
	}

	recruiter := startNode("recruiter")
	defer recruiter.shutdown()
	if _, err := recruiter.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap recruiter: %v", err)
	}

	device := startNode("device")
	defer device.shutdown()

	createMsg, err := shmevent.NewJoinRequestCreate()
	if err != nil {
		t.Fatalf("NewJoinRequestCreate: %v", err)
	}
	createMsg.SetId(1)
	createResp := call(device, createMsg)
	if createResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("join_request_create rejected: %s", mustErrMessage(t, createResp))
	}
	correlationToken, err := createResp.JoinRequestCreate().Token()
	if err != nil {
		t.Fatalf("JoinRequestCreate token: %v", err)
	}
	ticket := device.advertisedAddrs()[0] + "#" + hex.EncodeToString(correlationToken)

	cancelMsg, err := shmevent.NewJoinRequestCancel(correlationToken)
	if err != nil {
		t.Fatalf("NewJoinRequestCancel: %v", err)
	}
	cancelMsg.SetId(2)
	cancelResp := call(device, cancelMsg)
	if cancelResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("join_request_cancel rejected: %s", mustErrMessage(t, cancelResp))
	}

	recruitMsg, err := shmevent.NewRecruit(ticket, shmevent.SuffrageVoter)
	if err != nil {
		t.Fatalf("NewRecruit: %v", err)
	}
	recruitMsg.SetId(3)
	recruitResp := call(recruiter, recruitMsg)
	if recruitResp.Which() != shmevent.Event_Which_error {
		t.Fatal("recruit against a cancelled ticket unexpectedly succeeded")
	}
}
