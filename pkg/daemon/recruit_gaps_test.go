package daemon

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// This file closes gaps left by TestRecruitAdmitsPendingNodeAndTicketIsSingleUse
// (join_request_test.go, direct-dial, always Voter, no RequireConfirmForJoin)
// and TestPairThroughRelayCluster (pair_relay_test.go, relayed, but the same
// single happy-path shape): RequireConfirmForJoin's interaction with a
// recruit-driven join, suffrage override, a superseded/burned ticket not
// corrupting a later legitimate one, and what recruiting an
// already-clustered device actually does today. Every test here goes
// through the same relayed topology pair_relay_test.go established --
// hermetic by default, or against the real deployed VPS relay when
// E2E_REAL_RELAY_ADDR is set (see EnvRealRelayAddr's doc comment) -- since
// a hermetic direct-dial pair (join_request_test.go's shape) would miss the
// relay-circuit dial path recruit's two hops (RecruitProtocolID then
// JoinProtocolID) both depend on in production.

// pairFastRaftConfig is the raft-timing base every node in this file
// starts from. Against a hermetic local relay (the default) this matches
// TestPairThroughRelayCluster's own timings -- fast enough that
// self-election and replication settle well inside these tests' budgets.
// Against the real deployed VPS relay (EnvRealRelayAddr set), a solo
// node's own leader-lease check (LeaderLeaseTimeout) has to survive real
// internet round-trip latency stacked on this process's own CPU
// contention from running several concurrent libp2p hosts -- 100ms turned
// out not to be enough margin even for a strictly solo (zero-follower)
// cluster committing to itself, observed directly as "leadership lost
// while committing log" failures right at bootstrap. These WAN-safe
// values are still far tighter than the real deployed node's own
// (-raft-heartbeat-timeout 4s/-raft-election-timeout 4s/
// -raft-leader-lease-timeout 2500ms, see the VPS's actual kvnode
// invocation) -- tight enough to keep these tests from taking minutes
// each, generous enough to have stopped reproducing the failure across
// repeated runs.
func pairFastRaftConfig() Config {
	if os.Getenv(EnvRealRelayAddr) != "" {
		return Config{
			HeartbeatTimeout:   1500 * time.Millisecond,
			ElectionTimeout:    1500 * time.Millisecond,
			CommitTimeout:      100 * time.Millisecond,
			LeaderLeaseTimeout: 1000 * time.Millisecond,
		}
	}
	return Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}
}

// newRecruitPairFixture stands up a relay (the real deployed VPS bootstrap
// when EnvRealRelayAddr is set, else a fresh hermetic one -- the same
// choice TestPairThroughRelayCluster makes) plus two devices, a and b,
// each already holding a working relay reservation through it. cfgA/cfgB
// are each test's own base Config (see pairFastRaftConfig) with whatever
// it needs to vary (e.g. RequireConfirmForJoin) already set -- this
// fixture only overlays the DataDir/KeyPath/RelayPeers every node here
// needs regardless. Neither device is bootstrapped into a raft cluster of
// its own; that's each test's own next step, since some need a to bootstrap
// immediately and b to stay pending, and one deliberately bootstraps b too.
func newRecruitPairFixture(t *testing.T, ctx context.Context, tmpDir string, cfgA, cfgB Config) (relayAddr string, a, b *Node, aAddr, bAddr string) {
	t.Helper()

	startNode := func(name string, cfg Config) *Node {
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
		t.Cleanup(n.shutdown)
		return n
	}

	relayAddr = os.Getenv(EnvRealRelayAddr)
	if relayAddr == "" {
		relayCfg := pairFastRaftConfig()
		relayCfg.RelayService = true
		relay := startNode("relay", relayCfg)
		if _, err := relay.handleAdd(ctx, ""); err != nil {
			t.Fatalf("bootstrap relay: %v", err)
		}
		relayAddr = relay.advertisedAddrs()[0]
	} else {
		t.Logf("using real relay from %s", EnvRealRelayAddr)
	}
	t.Logf("relay addr: %s", relayAddr)

	cfgA.RelayPeers = []string{relayAddr}
	cfgB.RelayPeers = []string{relayAddr}
	a = startNode("a", cfgA)
	b = startNode("b", cfgB)

	// Both devices need standing with the relay's cluster before it will
	// grant either a reservation -- relayACL refuses reservations from
	// strangers unconditionally. See TestPairThroughRelayCluster's own
	// comment on why a real deployed bootstrap already satisfies this same
	// precondition without a local seed step.
	for name, n := range map[string]*Node{"a": a, "b": b} {
		accessMsg, err := shmevent.NewPublicAccess(relayAddr, "")
		if err != nil {
			t.Fatalf("NewPublicAccess: %v", err)
		}
		accessMsg.SetId(1)
		resp := callLocal(t, ctx, n, accessMsg, n.ed25519Priv)
		if resp.Which() == shmevent.Event_Which_error {
			t.Fatalf("public_access from %s rejected: %s", name, mustErrMessage(t, resp))
		}
	}

	circuitAddr := func(n *Node) string {
		return relayAddr + "/p2p-circuit/p2p/" + n.peerID
	}
	aAddr, bAddr = circuitAddr(a), circuitAddr(b)

	awaitReservation := func(name string, from *Node, addr string) {
		t.Helper()
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			t.Fatalf("parse %s circuit addr: %v", name, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			t.Fatalf("%s circuit addr info: %v", name, err)
		}
		deadline := time.Now().Add(90 * time.Second)
		var lastErr error
		for {
			dialCtx, dialCancel := context.WithTimeout(network.WithAllowLimitedConn(ctx, "test"), 10*time.Second)
			lastErr = from.host.Connect(dialCtx, *info)
			dialCancel()
			if lastErr == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never became reachable through the relay after being granted standing: %v", name, lastErr)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	awaitReservation("b", a, bAddr)
	awaitReservation("a", b, aAddr)

	return relayAddr, a, b, aAddr, bAddr
}

// createJoinRequestTicket runs EventJoinRequestCreate on device and returns
// the assembled "<deviceAddr>#<tokenHex>" ticket a recruiter redeems.
func createJoinRequestTicket(t *testing.T, ctx context.Context, device *Node, deviceAddr string) string {
	t.Helper()
	createMsg, err := shmevent.NewJoinRequestCreate()
	if err != nil {
		t.Fatalf("NewJoinRequestCreate: %v", err)
	}
	createMsg.SetId(1)
	resp := callLocal(t, ctx, device, createMsg, device.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("join_request_create rejected: %s", mustErrMessage(t, resp))
	}
	token, err := resp.JoinRequestCreate().Token()
	if err != nil {
		t.Fatalf("JoinRequestCreate token: %v", err)
	}
	return deviceAddr + "#" + hex.EncodeToString(token)
}

// tryRecruit issues EventRecruit on recruiter and returns the raw response,
// letting the caller decide whether success or failure is expected.
func tryRecruit(t *testing.T, ctx context.Context, recruiter *Node, ticket string, suffrage byte) shmevent.Msg {
	t.Helper()
	m, err := shmevent.NewRecruit(ticket, suffrage)
	if err != nil {
		t.Fatalf("NewRecruit: %v", err)
	}
	m.SetId(2)
	return callLocal(t, ctx, recruiter, m, recruiter.ed25519Priv)
}

// mustRecruit is tryRecruit for the common case where the recruit is
// expected to succeed, returning the "<peerID> ok"/"<peerID> pending"
// result string.
func mustRecruit(t *testing.T, ctx context.Context, recruiter *Node, ticket string, suffrage byte) string {
	t.Helper()
	resp := tryRecruit(t, ctx, recruiter, ticket, suffrage)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("recruit rejected: %s", mustErrMessage(t, resp))
	}
	result, err := resp.Recruit().Ticket()
	if err != nil {
		t.Fatalf("Recruit ticket: %v", err)
	}
	return result
}

// assertRaftMember fails the test unless peerID is present in n's raft
// configuration with exactly wantSuffrage.
func assertRaftMember(t *testing.T, n *Node, peerID string, wantSuffrage raft.ServerSuffrage) {
	t.Helper()
	cfgFuture := n.getRaft().GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get configuration: %v", err)
	}
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(peerID) {
			if srv.Suffrage != wantSuffrage {
				t.Fatalf("%s joined with suffrage %v, want %v", peerID, srv.Suffrage, wantSuffrage)
			}
			return
		}
	}
	t.Fatalf("%s not present in raft configuration", peerID)
}

// assertClusterMemberRecord fails the test unless n's own KindClusterMember
// system record for peerID exists with exactly wantRole -- the durable,
// queryable mirror addServerLine/recordClusterMember write alongside the
// raft.AddVoter/AddNonvoter call itself (see ListClusterMembers' data
// source).
func assertClusterMemberRecord(t *testing.T, n *Node, peerID string, wantRole byte) {
	t.Helper()
	value, err := n.handleGet(shmevent.ClusterMemberKey([]byte(peerID)))
	if err != nil {
		t.Fatalf("get KindClusterMember record for %s: %v", peerID, err)
	}
	_, role, err := shmevent.DecodeClusterMemberPayload(value)
	if err != nil {
		t.Fatalf("decode KindClusterMember record for %s: %v", peerID, err)
	}
	if role != wantRole {
		t.Fatalf("%s KindClusterMember role = %d, want %d", peerID, role, wantRole)
	}
}

// TestRecruitBypassesRequireConfirmForJoin proves admitOrLodgeJoin's
// documented but previously unasserted behavior (daemon.go:3529 doc
// comment): a recruit-driven join always redeems through consumeJoinInvite,
// which admits immediately -- even when the recruiter's own
// Config.RequireConfirmForJoin is set, the setting that makes an *ordinary*
// join (mage addfollower with no ticket) stop short as a pending
// shmevent.KindClusterJoin record awaiting a separate confirmpermit. A
// recruited peer must land as a real raft voter immediately, with no
// pending join record ever created for it.
func TestRecruitBypassesRequireConfirmForJoin(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tmpDir := t.TempDir()

	cfgA := pairFastRaftConfig()
	cfgA.RequireConfirmForJoin = true
	cfgB := pairFastRaftConfig()

	_, a, b, _, bAddr := newRecruitPairFixture(t, ctx, tmpDir, cfgA, cfgB)

	if _, err := a.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap a: %v", err)
	}
	if b.getRaft() != nil {
		t.Fatal("b unexpectedly already has a raft instance before pairing")
	}

	ticket := createJoinRequestTicket(t, ctx, b, bAddr)
	result := mustRecruit(t, ctx, a, ticket, shmevent.SuffrageVoter)
	if !strings.HasSuffix(result, " ok") {
		t.Fatalf("got recruit result %q, want it to end in %q", result, " ok")
	}

	assertRaftMember(t, a, b.peerID, raft.Voter)
	assertClusterMemberRecord(t, a, b.peerID, shmevent.RoleVoter)

	pendingKey := shmevent.SystemKey(shmevent.KindClusterJoin, shmevent.StatusPending, []byte(b.peerID))
	if _, err := a.handleGet(pendingKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected no pending KindClusterJoin record for b after invite-based recruit, got err=%v", err)
	}
}

// TestRecruitGrantsRequestedLearnerSuffrage proves consumeJoinInvite's own
// grant -- not whatever suffrage b's inner join() call happens to request
// -- is authoritative (admitOrLodgeJoin's doc comment). This matters
// because a recruit-driven join's inner join() call, unlike an ordinary
// `mage addfollower ... learner`, never carries the " learner" suffix
// handleAdd otherwise looks for (see splitInviteToken's precedence over
// it) -- so its own locally-requested suffrage is always Voter, and Learner
// only ever happens because a's invite said so.
func TestRecruitGrantsRequestedLearnerSuffrage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tmpDir := t.TempDir()

	_, a, b, _, bAddr := newRecruitPairFixture(t, ctx, tmpDir, pairFastRaftConfig(), pairFastRaftConfig())

	if _, err := a.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap a: %v", err)
	}

	ticket := createJoinRequestTicket(t, ctx, b, bAddr)
	result := mustRecruit(t, ctx, a, ticket, shmevent.SuffrageLearner)
	if !strings.HasSuffix(result, " ok") {
		t.Fatalf("got recruit result %q, want it to end in %q", result, " ok")
	}

	assertRaftMember(t, a, b.peerID, raft.Nonvoter)
	assertClusterMemberRecord(t, a, b.peerID, shmevent.RoleLearner)
}

// TestRecruitStaleTicketAlsoInvalidatesCurrentPendingTicket pins down real,
// verified behavior of consumeJoinRequestToken (daemon.go:989), not a
// hypothesis: its own doc comment says a wrong/replayed correlation token
// "clears the ticket either way" (matched or not) -- which means a *stale*
// recruit attempt (e.g. against a ticket a device already superseded with a
// second CreateJoinRequest, per that call's own "only one at a time"
// behavior) doesn't just fail on its own: it also destroys whatever ticket
// the device currently has pending, as a side effect, with no cooperation
// from the actual intended recruiter at all. This is a real DoS-shaped gap
// -- one mistyped, stale, or replayed ticket is enough to force a device to
// mint an entirely new one -- surfaced by writing this test against the
// documented "single pending ticket" model rather than assuming the
// obvious-looking "stale fails, fresh still works" behavior, which is NOT
// what the code does. dialAndPushRecruit also mints a's real
// shmevent.KindJoinInvite record before ever dialing b, so the doomed
// stale-ticket attempt burns one of those too, on top of clearing b's
// ticket -- neither leftover blocks a subsequent correct attempt.
func TestRecruitStaleTicketAlsoInvalidatesCurrentPendingTicket(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tmpDir := t.TempDir()

	_, a, b, _, bAddr := newRecruitPairFixture(t, ctx, tmpDir, pairFastRaftConfig(), pairFastRaftConfig())

	if _, err := a.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap a: %v", err)
	}

	staleTicket := createJoinRequestTicket(t, ctx, b, bAddr)
	freshTicket := createJoinRequestTicket(t, ctx, b, bAddr)
	if staleTicket == freshTicket {
		t.Fatal("expected two calls to join_request_create to mint distinct tokens")
	}

	if resp := tryRecruit(t, ctx, a, staleTicket, shmevent.SuffrageVoter); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("recruit against a superseded (no longer current) ticket unexpectedly succeeded")
	}
	if b.getRaft() != nil {
		t.Fatal("b unexpectedly gained a raft instance from the rejected superseded-ticket recruit attempt")
	}

	// freshTicket was never presented to anyone, yet the stale attempt
	// above already wiped it out as collateral damage. If this
	// unexpectedly starts succeeding, the gap this test documents has been
	// closed (consumeJoinRequestToken would need to stop clearing on a
	// mismatch) -- update this test's expectations alongside that fix.
	if resp := tryRecruit(t, ctx, a, freshTicket, shmevent.SuffrageVoter); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("fresh, never-presented ticket unexpectedly still valid after an unrelated stale-ticket recruit attempt")
	}
	if b.getRaft() != nil {
		t.Fatal("b unexpectedly gained a raft instance from the rejected fresh-ticket recruit attempt")
	}

	// Only a ticket minted after the damage -- a third CreateJoinRequest --
	// still works.
	recoveryTicket := createJoinRequestTicket(t, ctx, b, bAddr)
	result := mustRecruit(t, ctx, a, recoveryTicket, shmevent.SuffrageVoter)
	if !strings.HasSuffix(result, " ok") {
		t.Fatalf("got recruit result %q, want it to end in %q", result, " ok")
	}
	assertRaftMember(t, a, b.peerID, raft.Voter)
}

// TestRecruitAlreadyClusteredDevice documents -- rather than assumes --
// what actually happens when a's operator points RecruitPeer at a device
// that already has its own raft cluster. RecruitProtocolID's own doc
// comment and README ("reset it with leave/rm first") call this
// unsupported, but nothing in the code path actually detects or rejects
// it: handleRecruitStream's inner handleAdd only ever drives b's
// *client*-side join() -- a network round trip asking a to add b to a's
// own configuration -- which never touches b's own raft instance, so b's
// original cluster's log is not rewritten by the join call itself. What
// this test actually checks is the follow-on risk: once a's AddVoter
// succeeds, a's leader believes b is a live member of *a's* cluster and
// starts replicating AppendEntries to it -- delivered into b's *only*
// raft.Raft instance (one libp2p host, one rafttransport handler), which
// is still b's original, unrelated one. b's own pre-existing data must
// survive this untouched; if it doesn't, that's the corruption risk the
// docs only warn about in prose today made concrete.
func TestRecruitAlreadyClusteredDevice(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tmpDir := t.TempDir()

	_, a, b, _, bAddr := newRecruitPairFixture(t, ctx, tmpDir, pairFastRaftConfig(), pairFastRaftConfig())

	if _, err := a.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap a: %v", err)
	}
	// b bootstraps its own solo cluster first -- the unsupported precondition
	// -- and gets a marker write into it before any recruiting starts.
	if _, err := b.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap b: %v", err)
	}
	if err := b.handleSetForward(ctx, []byte("mine"), []byte("b-only"), true); err != nil {
		t.Fatalf("seed b's own pre-existing data: %v", err)
	}

	ticket := createJoinRequestTicket(t, ctx, b, bAddr)
	resp := tryRecruit(t, ctx, a, ticket, shmevent.SuffrageVoter)
	if resp.Which() == shmevent.Event_Which_error {
		t.Logf("recruit against an already-clustered device was rejected: %s (acceptable -- documents a guard exists)", mustErrMessage(t, resp))
	} else {
		result, err := resp.Recruit().Ticket()
		if err != nil {
			t.Fatalf("Recruit ticket: %v", err)
		}
		t.Logf("recruit against an already-clustered device returned %q (no guard exists today)", result)
	}

	// Whichever way the recruit itself came out, b's own pre-existing data
	// must not have been corrupted or replaced by a's unrelated log.
	deadline := time.Now().Add(15 * time.Second)
	var value []byte
	var getErr error
	for {
		value, getErr = b.handleGet([]byte("mine"))
		if getErr == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if getErr != nil || string(value) != "b-only" {
		t.Fatalf("b's own pre-existing cluster data did not survive being recruited while already clustered: err=%v value=%q", getErr, value)
	}
}
