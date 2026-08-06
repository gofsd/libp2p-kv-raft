package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	lp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// fastRaftCfg is the same low-latency raft tuning every other hermetic
// relay test in this package uses, so a solo bootstrap/election never
// dominates a test's own timeout budget.
func fastRaftCfg() Config {
	return Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}
}

func startNATTestNode(t *testing.T, tmpDir, name string, cfg Config) *Node {
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

// TestForgetTransientPeerKeepsRelayCandidateButDropsStranger is a direct
// regression test for forgetTransientPeer (see its doc comment) -- the
// function has no test anywhere else in this package despite being one of
// two confirmed root causes of a real 4.95GB-RSS production leak, and its
// relay-candidate exemption is the exact fix for the two-device pair
// scenario's disappearing-circuit-address bug ("a device reported a real
// circuit address one moment and a bare loopback one seconds later").
//
// This drives the real wiring end to end -- closing an actual libp2p
// connection and letting the DisconnectedF notify hook (wired in start,
// see its own comment) call forgetTransientPeer on its own -- rather than
// calling the method directly, so a future change to that wiring (not
// just the method body) would also be caught.
func TestForgetTransientPeerKeepsRelayCandidateButDropsStranger(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	relay, err := p2praft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		t.Fatalf("start relay: %v", err)
	}
	stranger, err := p2praft.StartRelayNode(ctx, filepath.Join(tmpDir, "stranger.key"), 0)
	if err != nil {
		t.Fatalf("start stranger: %v", err)
	}

	cfg := fastRaftCfg()
	cfg.RelayPeers = []string{relay.Addrs[0]}
	n := startNATTestNode(t, tmpDir, "n", cfg)

	relayInfo := peer.AddrInfo{ID: relay.Host.ID(), Addrs: relay.Host.Addrs()}
	strangerInfo := peer.AddrInfo{ID: stranger.Host.ID(), Addrs: stranger.Host.Addrs()}

	if err := n.host.Connect(ctx, relayInfo); err != nil {
		t.Fatalf("connect to relay: %v", err)
	}
	if err := n.host.Connect(ctx, strangerInfo); err != nil {
		t.Fatalf("connect to stranger: %v", err)
	}

	// forgetTransientPeer's RemovePeer call clears the KeyBook/ProtoBook/
	// Metadata entries, not the AddrBook (see the function's own doc
	// comment: "only the addrbook has any TTL", i.e. addresses are
	// expected to expire on their own and are deliberately left alone
	// here). ProtoBook (GetProtocols) is the reliable signal of that,
	// populated shortly after connect by identify -- PubKey (KeyBook)
	// looks like the obvious thing to check but isn't: for this
	// project's Ed25519 (identity-inlined) peer IDs,
	// memoryKeyBook.PubKey falls back to peer.ID.ExtractPublicKey()
	// whenever the map has no entry, so it returns non-nil unconditionally
	// regardless of whether RemovePeer ran.
	waitForProtocols := func(name string, id peer.ID) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			protos, _ := n.host.Peerstore().GetProtocols(id)
			if len(protos) > 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s has no ProtoBook entry after connecting (identify never completed?)", name)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitForProtocols("relay", relayInfo.ID)
	waitForProtocols("stranger", strangerInfo.ID)

	// Close both connections -- this fires DisconnectedF for each peer,
	// which calls forgetTransientPeer(id) for real, exactly as an AutoNAT
	// reachability probe or a churny mobile network link would.
	if err := n.host.Network().ClosePeer(relayInfo.ID); err != nil {
		t.Fatalf("close relay conn: %v", err)
	}
	if err := n.host.Network().ClosePeer(strangerInfo.ID); err != nil {
		t.Fatalf("close stranger conn: %v", err)
	}

	protosOf := func(id peer.ID) int {
		p, _ := n.host.Peerstore().GetProtocols(id)
		return len(p)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		relayProtosLeft := protosOf(relayInfo.ID)
		strangerProtosLeft := protosOf(strangerInfo.ID)
		if relayProtosLeft > 0 && strangerProtosLeft == 0 {
			return // exactly the outcome forgetTransientPeer exists to produce
		}
		if time.Now().After(deadline) {
			t.Fatalf("after disconnect: relay ProtoBook entries=%d (want >0, it's a relay candidate), stranger ProtoBook entries=%d (want 0, it's not)",
				relayProtosLeft, strangerProtosLeft)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestClearDialBackoffClearsDestinationAndRelayHopBackoff is a direct
// regression test for clearDialBackoff. This is the fix for a bug caught
// live pairing two real devices: AutoRelay's own doomed startup dials
// (before any relay standing exists) armed backoff against both the
// eventual join/recruit target *and* the relay hop itself, silently
// failing the very RequestRelayAccess call that would have granted that
// standing -- see the function's own doc comment. No test anywhere else
// in this package arms swarm.DialBackoff and checks clearDialBackoff
// actually clears it, for either the destination peer or the relay hop
// embedded in a /p2p-circuit multiaddr.
func TestClearDialBackoffClearsDestinationAndRelayHopBackoff(t *testing.T) {
	t.Parallel()

	h, err := lp2p.New(lp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer h.Close()

	sw, ok := h.Network().(*swarm.Swarm)
	if !ok {
		t.Fatalf("host network is %T, want *swarm.Swarm", h.Network())
	}

	destID := newTestPeerID(t)
	relayHopID := newTestPeerID(t)
	someAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	if err != nil {
		t.Fatalf("parse addr: %v", err)
	}

	// Arm backoff exactly as a doomed AutoRelay startup dial would: once
	// against the destination itself, once against the relay hop it went
	// through to get there.
	sw.Backoff().AddBackoff(destID, someAddr)
	sw.Backoff().AddBackoff(relayHopID, someAddr)
	if !sw.Backoff().Backoff(destID, someAddr) {
		t.Fatal("destination backoff didn't arm -- test setup broken")
	}
	if !sw.Backoff().Backoff(relayHopID, someAddr) {
		t.Fatal("relay hop backoff didn't arm -- test setup broken")
	}

	circuitAddr, err := multiaddr.NewMultiaddr("/p2p/" + relayHopID.String() + "/p2p-circuit/p2p/" + destID.String())
	if err != nil {
		t.Fatalf("build circuit addr: %v", err)
	}
	info := peer.AddrInfo{ID: destID, Addrs: []multiaddr.Multiaddr{circuitAddr}}

	clearDialBackoff(h, info)

	if sw.Backoff().Backoff(destID, someAddr) {
		t.Error("clearDialBackoff left the destination peer's backoff armed")
	}
	if sw.Backoff().Backoff(relayHopID, someAddr) {
		t.Error("clearDialBackoff left the relay hop's backoff armed -- a recruit/join dial through this relay would still fail with \"dial backoff\"")
	}
}

func newTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	return pid
}

// TestHasPublicAddrDistinguishesPublicFromPrivateAddrs is a direct unit
// test of hasPublicAddr, the function that gates whether
// dialAndSubmitPublicAccess bothers waiting for a relay reservation at
// all (see its own doc comment: waiting when the relay could never
// publish a circuit address, because it has no public address of its
// own, would just be waiting forever). It has no test anywhere else in
// this package -- every existing hermetic relay test runs on a single
// machine, where the relay never has a public address and this always
// evaluates false, so the true branch has never been exercised even at
// the pure-function level.
func TestHasPublicAddrDistinguishesPublicFromPrivateAddrs(t *testing.T) {
	t.Parallel()

	mustAddr := func(s string) multiaddr.Multiaddr {
		a, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			t.Fatalf("parse %s: %v", s, err)
		}
		return a
	}

	tests := []struct {
		name  string
		addrs []multiaddr.Multiaddr
		want  bool
	}{
		{"no addrs at all", nil, false},
		{"loopback only", []multiaddr.Multiaddr{mustAddr("/ip4/127.0.0.1/tcp/4001")}, false},
		{"private LAN only", []multiaddr.Multiaddr{mustAddr("/ip4/192.168.1.5/tcp/4001")}, false},
		{"public only", []multiaddr.Multiaddr{mustAddr("/ip4/8.8.8.8/tcp/4001")}, true},
		{"public among private", []multiaddr.Multiaddr{
			mustAddr("/ip4/127.0.0.1/tcp/4001"),
			mustAddr("/ip4/192.168.1.5/tcp/4001"),
			mustAddr("/ip4/8.8.8.8/tcp/4001"),
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasPublicAddr(tc.addrs); got != tc.want {
				t.Errorf("hasPublicAddr(%v) = %v, want %v", tc.addrs, got, tc.want)
			}
		})
	}
}

// TestRelayFailoverToSecondCandidateWhenFirstIsDown is the live-AutoRelay
// counterpart to TestRelayCandidatesMergesSeedsAndStoreSortedByPriority,
// which only proves the *ordered candidate list* relayCandidates builds
// is correct -- not that EnableAutoRelayWithStaticRelays actually fails
// over to the second entry when the first is unreachable.
// CLAUDE.md/README's "Node connectivity policy" section claims this
// failover "already fails over between more than one candidate on its
// own", which is true eventually but was not the whole story: writing
// this test caught a real bug, now fixed alongside it (see newHost's
// autorelay.WithBootDelay comment) -- a dead first candidate never counts
// toward AutoRelay's minCandidates (it fails the live connect-and-probe
// every real candidate must pass), so the live second candidate's
// reservation was stalled behind go-libp2p's default 3-minute bootDelay
// instead of being tried immediately. Confirmed by reproducing the stall
// before the fix (a manual run measured exactly 3m0s) and confirming this
// test passes in well under the deadline below after it.
func TestRelayFailoverToSecondCandidateWhenFirstIsDown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	liveRelay, err := p2praft.StartRelayNode(ctx, filepath.Join(tmpDir, "live-relay.key"), 0)
	if err != nil {
		t.Fatalf("start live relay: %v", err)
	}
	liveRelayAddr := liveRelay.Addrs[0]

	// deadAddr names a real, syntactically valid peer id at a port nothing
	// is listening on -- a relay candidate that was configured once and
	// then genuinely went away, distinct from the malformed-entry case
	// TestRelayCandidatesMergesSeedsAndStoreSortedByPriority already
	// covers.
	deadAddr := newTestPeerAddr(t, 1)

	cfg := fastRaftCfg()
	cfg.RelayPeers = []string{deadAddr, liveRelayAddr}
	n := startNATTestNode(t, tmpDir, "device", cfg)

	circuitAddr := liveRelayAddr + "/p2p-circuit/p2p/" + n.peerID
	maddr, err := multiaddr.NewMultiaddr(circuitAddr)
	if err != nil {
		t.Fatalf("parse circuit addr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("circuit addr info: %v", err)
	}

	// A separate prober host, not the relay itself -- dialing a circuit
	// address only succeeds once the device holds a real reservation with
	// the live relay, exactly the technique TestPairThroughRelayCluster's
	// awaitReservation uses.
	prober, err := lp2p.New(lp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create prober host: %v", err)
	}
	defer prober.Close()

	// With the bootDelay fix, this should land within roughly
	// relayReserveBackoff (10s) of startup, not the pre-fix 3-minute
	// stall -- 40s leaves comfortable margin without reintroducing a slow
	// test.
	deadline := time.Now().Add(40 * time.Second)
	var lastErr error
	for {
		// clearDialBackoff before every attempt for the exact reason its own
		// doc comment gives: a failed dial (expected here, repeatedly, until
		// the device's reservation via the live candidate lands) arms the
		// prober's own swarm backoff against this specific (device, circuit
		// addr) pair, which would otherwise make every later attempt fail
		// instantly with "dial backoff" instead of actually retrying.
		clearDialBackoff(prober, *info)
		dialCtx, dialCancel := context.WithTimeout(network.WithAllowLimitedConn(ctx, "test"), 10*time.Second)
		lastErr = prober.Connect(dialCtx, *info)
		dialCancel()
		if lastErr == nil {
			return // device obtained a working reservation through the second, live candidate
		}
		if time.Now().After(deadline) {
			t.Fatalf("device never became reachable through the live relay despite the first candidate being permanently down: %v", lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestExecInviteRedeemOverRelay closes a gap CLAUDE.md names explicitly:
// dialAndRedeemExecInvite was fixed alongside join and dialAndPushRecruit
// for the identical "no network.WithAllowLimitedConn" bug (a relayed dial
// hanging until the context deadline because libp2p waited for a direct
// connection a NATed peer never provides), but unlike the other two, no
// existing test forces it through an actual /p2p-circuit hop --
// TestExecInviteRedeemByPermittedPeerSucceedsAndIsOneTime only ever dials
// a directly-reachable leader.
func TestExecInviteRedeemOverRelay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	fast := fastRaftCfg()

	relayCfg := fast
	relayCfg.RelayService = true
	relay := startExecInviteNode(t, tmpDir, "relay", relayCfg)
	defer relay.shutdown()
	if _, err := relay.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap relay: %v", err)
	}
	relayAddr := relay.advertisedAddrs()[0]

	deviceCfg := fast
	deviceCfg.RelayPeers = []string{relayAddr}
	leader := startExecInviteNode(t, tmpDir, "leader", deviceCfg)
	defer leader.shutdown()
	redeemer := startExecInviteNode(t, tmpDir, "redeemer", deviceCfg)
	defer redeemer.shutdown()

	// Both leader and redeemer need standing on the relay's cluster:
	// leader to hold a reservation, redeemer to be allowed to connect
	// *through* the relay to reach leader -- relayACL.AllowConnect gates
	// the dialer (src), not just the reservation holder (see that
	// method's own doc comment on daemon.go).
	for name, n := range map[string]*Node{"leader": leader, "redeemer": redeemer} {
		resp := execInviteCall(t, ctx, n, shmevent.Msg{EventType: shmevent.EventPublicAccess, Value: []byte(relayAddr), ID: 1})
		if resp.EventType == shmevent.EventError {
			t.Fatalf("public_access from %s rejected: %s", name, resp.Value)
		}
	}

	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}

	leaderCircuitAddr := relayAddr + "/p2p-circuit/p2p/" + leader.peerID
	maddr, err := multiaddr.NewMultiaddr(leaderCircuitAddr)
	if err != nil {
		t.Fatalf("parse leader circuit addr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("leader circuit addr info: %v", err)
	}

	// Prove leader's reservation actually took effect before exercising
	// the real redeem call below -- dialAndRedeemExecInvite's own
	// connectWithRetry budget is far shorter than a fresh AutoRelay
	// reservation can take to land (see relayReserveBackoff), so without
	// this wait the test would be flaky on exactly the timing this whole
	// feature is about.
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for {
		clearDialBackoff(redeemer.host, *info) // see the failover test's identical comment
		dialCtx, dialCancel := context.WithTimeout(network.WithAllowLimitedConn(ctx, "test"), 10*time.Second)
		lastErr = redeemer.host.Connect(dialCtx, *info)
		dialCancel()
		if lastErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader never became reachable through the relay: %v", lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
	// The real redeem below must establish its own connection through
	// dialAndRedeemExecInvite's own connectWithRetry/NewStream path, not
	// ride the connection just opened to prove reachability.
	_ = redeemer.host.Network().ClosePeer(info.ID)

	setUpExecInviteACL(t, ctx, leader, "cmd-relay", "grp-relay", redeemer.peerID)

	token := newExecInviteToken(t)
	createPayload, err := shmevent.EncodeExecInviteCreatePayload(token, "cmd-relay", "")
	if err != nil {
		t.Fatalf("EncodeExecInviteCreatePayload: %v", err)
	}
	if resp := execInviteCall(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventExecInviteCreate, Value: createPayload, ID: 10}); resp.EventType == shmevent.EventError {
		t.Fatalf("exec_invite_create rejected: %s", resp.Value)
	}

	// leaderCircuitAddr, not leader's real dialable address, is what the
	// redeemer resolves sourceAddr into -- naming it explicitly is what
	// forces dialAndRedeemExecInvite through the relay circuit, the same
	// technique TestPairThroughRelayCluster uses for its own two dials.
	redeemPayload, err := shmevent.EncodeExecInviteRedeemRequest(leaderCircuitAddr, token)
	if err != nil {
		t.Fatalf("EncodeExecInviteRedeemRequest: %v", err)
	}
	resp := execInviteCall(t, ctx, redeemer, shmevent.Msg{EventType: shmevent.EventExecInviteRedeem, Value: redeemPayload, ID: 11})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("exec_invite_redeem over relay rejected: %s", resp.Value)
	}
	if string(resp.Value) == "" {
		t.Fatal("exec_invite_redeem over relay succeeded but returned an empty instance id")
	}

	// Same token again must fail -- the invite is one-time, exactly as it
	// is over a direct connection.
	resp = execInviteCall(t, ctx, redeemer, shmevent.Msg{EventType: shmevent.EventExecInviteRedeem, Value: redeemPayload, ID: 12})
	if resp.EventType != shmevent.EventError {
		t.Fatal("second exec_invite_redeem over relay with an already-consumed token unexpectedly succeeded")
	}
}
