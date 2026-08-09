package daemon

import (
	"context"
	"encoding/hex"
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
)

// TestPairThroughRelayCluster is the hermetic counterpart to what
// `relaytool -mode=pair` drives against the real deployed relay, and to
// what pkg/e2erun/android_pair.go drives across two phones: two devices
// that only reach each other through a third node's relay service pair
// with each other -- one mints a join-request ticket, the other redeems it
// (EventRecruit) and admits it into its own cluster.
//
// Topology, the same shape a real deployment has (see CLAUDE.md's "Node
// connectivity policy"):
//
//	relay -- RelayService, its own solo cluster, directly dialable
//	a     -- RelayPeers=[relay], its own solo cluster, the recruiter
//	b     -- RelayPeers=[relay], no raft instance at all (AddPending's
//	         state), the device being recruited
//
// What this covers that TestRecruitAdmitsPendingNodeAndTicketIsSingleUse
// (same events, plain directly-dialable addresses) cannot:
//
//   - a fresh device has no relay standing at all, and relayACL refuses
//     reservations unconditionally, so both devices go through the real
//     EventPublicAccess self-service escalation first and then have to
//     actually obtain a reservation *in the same process* -- no restart,
//     which is what a real user does. See relayReserveBackoff: AutoRelay's
//     own default backs a refused relay off for an hour, which made that
//     impossible.
//   - the pairing dial itself runs over a relay circuit, in both
//     directions (a dials b's circuit address to hand over the invite; b
//     dials a's circuit address to join). Opening a stream on a limited
//     connection needs network.WithAllowLimitedConn; without it libp2p
//     silently waits for a direct connection a NATed peer will never
//     provide, and the dial dies on the context deadline instead.
//
// The addresses are built explicitly rather than read from
// advertisedAddrs: on a single machine go-libp2p's reachability tracker
// (correctly) confirms these hosts' direct addresses are dialable, so it
// never surfaces a /p2p-circuit address of its own accord -- the same
// caveat TestJoinThroughRelay documents. Naming the circuit address
// directly is what forces the relayed path a real NATed device has no
// choice about.
func TestPairThroughRelayCluster(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}

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

	// The relay bootstraps a cluster of its own so ensureDefaultPublicCommand
	// seeds the always-public command the two devices escalate through
	// below -- see EnvRealRelayAddr's own doc comment (relay_target_test.go)
	// on why a real deployed bootstrap already satisfies this same
	// precondition, so real mode here just needs its address, not a local
	// bootstrap at all.
	relayAddr := os.Getenv(EnvRealRelayAddr)
	if relayAddr == "" {
		relayCfg := fastRaft
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

	deviceCfg := fastRaft
	deviceCfg.RelayPeers = []string{relayAddr}
	a := startNode("a", deviceCfg)
	b := startNode("b", deviceCfg)

	// Both devices ask the relay's cluster for standing, exactly as `mage
	// requestpublicaccess` / kvmobile's RequestRelayAccess does. Until this
	// lands, relayACL refuses their reservations outright.
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

	// circuitAddr is the address a device behind this relay hands out: the
	// relay's own address, the /p2p-circuit hop, then the device.
	circuitAddr := func(n *Node) string {
		return relayAddr + "/p2p-circuit/p2p/" + n.peerID
	}
	aAddr, bAddr := circuitAddr(a), circuitAddr(b)

	// awaitReservation proves the standing granted above actually took
	// effect within this process: dialing a device's circuit address only
	// succeeds once that device holds a real reservation with the relay
	// (the relay answers NO_RESERVATION otherwise), so this is the
	// assertion that AutoRelay retried after its first refusal instead of
	// backing off for an hour.
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

	// a is an existing cluster (its own solo leader); b has no raft
	// instance at all yet, the only state EventRecruit supports.
	if _, err := a.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap a: %v", err)
	}
	if b.getRaft() != nil {
		t.Fatal("b unexpectedly already has a raft instance before pairing")
	}

	createMsg, err := shmevent.NewJoinRequestCreate()
	if err != nil {
		t.Fatalf("NewJoinRequestCreate: %v", err)
	}
	createMsg.SetId(2)
	createResp := callLocal(t, ctx, b, createMsg, b.ed25519Priv)
	if createResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("join_request_create on b rejected: %s", mustErrMessage(t, createResp))
	}
	correlationToken, err := createResp.JoinRequestCreate().Token()
	if err != nil {
		t.Fatalf("JoinRequestCreate token: %v", err)
	}
	ticket := bAddr + "#" + hex.EncodeToString(correlationToken)

	recruitMsg, err := shmevent.NewRecruit(ticket, shmevent.SuffrageVoter)
	if err != nil {
		t.Fatalf("NewRecruit: %v", err)
	}
	recruitMsg.SetId(3)
	recruitResp := callLocal(t, ctx, a, recruitMsg, a.ed25519Priv)
	if recruitResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("recruit of b by a over the relay rejected: %s", mustErrMessage(t, recruitResp))
	}
	result, err := recruitResp.Recruit().Ticket()
	if err != nil {
		t.Fatalf("Recruit ticket: %v", err)
	}
	if !strings.HasSuffix(result, " ok") {
		t.Fatalf("got recruit result %q, want it to end in %q", result, " ok")
	}

	cfgFuture := a.getRaft().GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get a's configuration: %v", err)
	}
	var found bool
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(b.peerID) {
			found = true
			if srv.Suffrage != raft.Voter {
				t.Fatalf("b joined with suffrage %v, want Voter", srv.Suffrage)
			}
		}
	}
	if !found {
		t.Fatal("b not present in a's raft configuration after recruit")
	}

	// Pairing is only real if replication then works between them.
	if err := a.handleSetForward(ctx, []byte("paired"), []byte("yes"), true); err != nil {
		t.Fatalf("set on a: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		value, err := b.handleGet([]byte("paired"))
		if err == nil && string(value) == "yes" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("b never observed a's replicated write after pairing: err=%v value=%q", err, value)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
