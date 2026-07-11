package daemon

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/rafttransport"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestAddLearnerThroughRelay is a real-cluster test for handleAddLearner
// (ClientProtocolID's EventAdd): it spins up a genuine circuit-relay v2
// server, a real leader daemon.Node, and a plain go-libp2p host standing in
// for what web-app/'s rust-libp2p-in-wasm build would be -- a peer with no
// directly-dialable address of its own, reachable only through a relay
// reservation, exactly like an Android device behind carrier-grade NAT (see
// Config.RelayPeer's doc comment). It doesn't exercise the Rust wire codec
// itself (that's covered byte-for-byte by web-app's own raft_wire tests
// against real hashicorp/raft fixtures) -- what it proves is the Go-side
// half: that the full pkg/shmevent handshake (GetPrivateKey bootstrap,
// then a signed SetKey+EventAdd pair) results in AddNonvoter landing in
// the leader's raft configuration at a relay-reserved address, and that
// the leader's own rafttransport.NetworkTransport can subsequently Dial()
// the "browser" through that reservation to deliver a real AppendEntries
// stream, the same way it already does for a relay-joined voter (see
// TestJoinThroughRelay).
func TestAddLearnerThroughRelay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	relay, err := p2praft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer relay.Host.Close()
	if len(relay.Addrs) == 0 {
		t.Fatal("relay has no addresses")
	}
	relayAddr := relay.Addrs[0]
	t.Logf("relay addr: %s", relayAddr)

	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}

	leaderKey := filepath.Join(tmpDir, "leader.key")
	if _, err := p2praft.LoadOrGenerateKey(leaderKey); err != nil {
		t.Fatalf("generate leader key: %v", err)
	}
	leaderCfg := fastRaft
	leaderCfg.DataDir = filepath.Join(tmpDir, "leader")
	leaderCfg.KeyPath = leaderKey
	leader, err := start(leaderCfg)
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	defer leader.shutdown()

	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]
	t.Logf("leader addr: %s", leaderAddr)

	// browser stands in for web-app/'s rust-libp2p-in-wasm build: a peer
	// with no directly-dialable address, reachable only through the relay
	// reservation NewP2PNode already sets up (EnableRelay +
	// ListenAddrStrings("/p2p-circuit") + relayclient.Reserve), exactly
	// the mechanism p2p.rs's reserve_relay_slot performs from inside wasm.
	browser, err := p2praft.NewP2PNode(ctx, relayAddr, filepath.Join(tmpDir, "browser.key"))
	if err != nil {
		t.Fatalf("start browser stand-in: %v", err)
	}
	defer browser.Host.Close()

	// Proves the leader's raft transport can really reach the browser
	// through its relay reservation, not just that AddNonvoter accepted
	// the address: a real AppendEntries (heartbeat) stream should arrive
	// on rafttransport.ProtocolID shortly after it joins the configuration.
	raftStreamCh := make(chan struct{}, 1)
	browser.Host.SetStreamHandler(rafttransport.ProtocolID, func(s network.Stream) {
		defer s.Reset()
		select {
		case raftStreamCh <- struct{}{}:
		default:
		}
	})

	// Same caveat TestJoinThroughRelay documents at length: on this
	// same-machine test topology, go-libp2p's own reachability tracker
	// correctly determines the browser's direct address is in fact
	// dialable (it is, by every other node here), so GetAddress() may
	// return that instead of a /p2p-circuit address. That doesn't weaken
	// what this test actually proves -- handleAddLearner correctly
	// AddNonvoters whatever address it's given, and the leader's raft
	// transport really dials it -- it just means this test alone can't
	// distinguish "dialed directly" from "dialed through the relay
	// circuit" the way a genuinely NATed deployment would force.
	browserAddr := browser.GetAddress()
	t.Logf("browser addr: %s (relay circuit address: %v)", browserAddr, strings.Contains(browserAddr, "/p2p-circuit"))
	if _, err := multiaddr.NewMultiaddr(browserAddr); err != nil {
		t.Fatalf("browser address %q is not a valid multiaddr: %v", browserAddr, err)
	}

	leaderPeerID, err := peer.Decode(leader.peerID)
	if err != nil {
		t.Fatalf("decode leader peer id: %v", err)
	}
	leaderMaddr, err := multiaddr.NewMultiaddr(leaderAddr)
	if err != nil {
		t.Fatalf("parse leader addr: %v", err)
	}
	leaderInfo, err := peer.AddrInfoFromP2pAddr(leaderMaddr)
	if err != nil {
		t.Fatalf("leader addr info: %v", err)
	}
	if err := browser.Host.Connect(ctx, *leaderInfo); err != nil {
		t.Fatalf("browser connect to leader: %v", err)
	}

	// Bootstrap: fetch the shared signing key -- unsigned, per
	// shmevent.RequiresSignature -- exactly like web-app/'s app.rs does
	// before it can sign anything else.
	keyResp, err := callClientProtocol(ctx, browser.Host, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventGetPrivateKey,
		ID:        1,
	}, nil)
	if err != nil {
		t.Fatalf("get_private_key: %v", err)
	}
	if keyResp.EventType == shmevent.EventError {
		t.Fatalf("get_private_key rejected: %s", keyResp.Value)
	}
	browserPriv := shmevent.PrivateKey(keyResp.Value)

	// SetKey registers the browser's own peer id, then EventAdd
	// (SourceID referencing it) supplies the relay-reserved address --
	// see pkg/shmevent's doc comment for why AddNonvoter's two pieces of
	// data need two linked messages.
	const setKeyID = 2
	setKeyResp, err := callClientProtocol(ctx, browser.Host, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventSetKey,
		Value:     []byte(browser.Host.ID().String()),
		ID:        setKeyID,
	}, browserPriv)
	if err != nil {
		t.Fatalf("set_key: %v", err)
	}
	if setKeyResp.EventType == shmevent.EventError {
		t.Fatalf("set_key rejected: %s", setKeyResp.Value)
	}

	addResp, err := callClientProtocol(ctx, browser.Host, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventAdd,
		SourceID:  setKeyID,
		Value:     []byte(browserAddr),
		ID:        3,
	}, browserPriv)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if addResp.EventType == shmevent.EventError {
		t.Fatalf("add-learner rejected: %s", addResp.Value)
	}

	rf := leader.getRaft()
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get leader configuration: %v", err)
	}
	var found bool
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(browser.Host.ID().String()) {
			found = true
			if srv.Suffrage != raft.Nonvoter {
				t.Fatalf("browser added with suffrage %v, want Nonvoter", srv.Suffrage)
			}
			if string(srv.Address) != browserAddr {
				t.Fatalf("browser address in configuration = %q, want %q", srv.Address, browserAddr)
			}
		}
	}
	if !found {
		t.Fatal("browser not present in leader's raft configuration after AddNonvoter")
	}

	select {
	case <-raftStreamCh:
	case <-time.After(20 * time.Second):
		t.Fatal("leader never dialed the browser's relay-reserved address with a raft RPC stream")
	}
}

// callClientProtocol speaks ClientProtocolID for one request/response
// round trip -- the test's stand-in for what web-app/'s p2p.rs
// (call_client_protocol) or a Go pkg/shmclient caller would do.
func callClientProtocol(ctx context.Context, h lp2phost.Host, target peer.ID, m shmevent.Msg, priv shmevent.PrivateKey) (shmevent.Msg, error) {
	s, err := h.NewStream(ctx, target, ClientProtocolID)
	if err != nil {
		return shmevent.Msg{}, err
	}
	defer s.Close()

	buf, err := shmevent.Encode(m, priv)
	if err != nil {
		return shmevent.Msg{}, err
	}
	if _, err := s.Write(buf); err != nil {
		return shmevent.Msg{}, err
	}
	if err := s.CloseWrite(); err != nil {
		return shmevent.Msg{}, err
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return shmevent.Msg{}, err
	}
	resp, _, _, err := shmevent.Decode(respBuf)
	if err != nil {
		return shmevent.Msg{}, err
	}
	return resp, nil
}
