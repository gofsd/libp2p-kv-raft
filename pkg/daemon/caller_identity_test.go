package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// newTestRemoteHost builds a bare, directly-dialable libp2p host (no relay
// needed -- unlike the browser stand-in in add_learner_relay_test.go, these
// tests don't exercise NAT traversal, just callerIdentity's remote-signing
// semantics) and connects it to leader. Returns the host, its own raw
// ed25519 private key (never fetched from the leader -- generated here,
// exactly like web-app's Worker generates its own), and leader's peer id.
func newTestRemoteHost(t *testing.T, ctx context.Context, leader *Node) (lp2phost.Host, shmevent.PrivateKey, peer.ID) {
	t.Helper()

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate remote host key: %v", err)
	}
	rawPriv, err := priv.Raw()
	if err != nil {
		t.Fatalf("remote host key raw bytes: %v", err)
	}

	h, err := libp2p.New(libp2p.Identity(priv))
	if err != nil {
		t.Fatalf("start remote host: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	leaderPeerID, err := peer.Decode(leader.peerID)
	if err != nil {
		t.Fatalf("decode leader peer id: %v", err)
	}
	leaderMaddr, err := multiaddr.NewMultiaddr(leader.advertisedAddrs()[0])
	if err != nil {
		t.Fatalf("parse leader addr: %v", err)
	}
	leaderInfo, err := peer.AddrInfoFromP2pAddr(leaderMaddr)
	if err != nil {
		t.Fatalf("leader addr info: %v", err)
	}
	if err := h.Connect(ctx, *leaderInfo); err != nil {
		t.Fatalf("remote host connect to leader: %v", err)
	}

	return h, shmevent.PrivateKey(rawPriv), leaderPeerID
}

// startTestLeader starts and bootstraps a single-voter leader with fast
// raft timing, mirroring add_learner_relay_test.go's fastRaft/leaderCfg
// setup but without any relay involved.
func startTestLeader(t *testing.T, ctx context.Context, cfg Config) *Node {
	t.Helper()

	tmpDir := t.TempDir()
	cfg.HeartbeatTimeout = 200 * time.Millisecond
	cfg.ElectionTimeout = 200 * time.Millisecond
	cfg.CommitTimeout = 20 * time.Millisecond
	cfg.LeaderLeaseTimeout = 100 * time.Millisecond
	cfg.DataDir = filepath.Join(tmpDir, "leader")
	cfg.KeyPath = filepath.Join(tmpDir, "leader.key")

	if _, err := p2praft.LoadOrGenerateKey(cfg.KeyPath); err != nil {
		t.Fatalf("generate leader key: %v", err)
	}
	leader, err := start(cfg)
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	t.Cleanup(leader.shutdown)

	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	return leader
}

// TestAddLearnerRejectsSpoofedPeerID is a regression test for the identity-
// spoofing gap handleAddDispatch used to have: a remote caller registering
// (via EventSetKey) a peer id different from its own authenticated
// connection identity, then citing it via EventAdd's SourceID, must be
// rejected -- not silently added to the raft configuration under someone
// else's identity and an attacker-chosen address.
func TestAddLearnerRejectsSpoofedPeerID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	const setKeyID = 1
	spoofedPeerID := "not-" + remote.ID().String()
	setKeyResp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventSetKey,
		Value:     []byte(spoofedPeerID),
		ID:        setKeyID,
	}, remotePriv)
	if err != nil {
		t.Fatalf("set_key: %v", err)
	}
	if setKeyResp.EventType == shmevent.EventError {
		t.Fatalf("set_key rejected: %s", setKeyResp.Value)
	}

	addResp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventAdd,
		SourceID:  setKeyID,
		Value:     []byte("/ip4/127.0.0.1/tcp/1/p2p/" + spoofedPeerID),
		ID:        2,
	}, remotePriv)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if addResp.EventType != shmevent.EventError {
		t.Fatalf("add with spoofed peer id succeeded, want rejection")
	}
	if !strings.Contains(string(addResp.Value), "does not match") {
		t.Fatalf("add rejection = %q, want it to mention the identity mismatch", addResp.Value)
	}

	rf := leader.getRaft()
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get leader configuration: %v", err)
	}
	for _, srv := range cfgFuture.Configuration().Servers {
		if string(srv.ID) == spoofedPeerID {
			t.Fatalf("spoofed peer id %q was added to the raft configuration", spoofedPeerID)
		}
	}
}

// TestClientProtocolRejectsRemoteKeyFetch proves the earlier reverted
// fix's intent now holds without breaking anything: a remote
// (ClientProtocolID) caller can no longer fetch this node's own private
// or public key, since it always has its own key already (see
// callerIdentity's doc comment) -- unlike a local pkg/ipc caller, which
// still can (that bootstrap exception is unchanged, see permit_test.go/
// mobile/kvmobile's use of it).
func TestClientProtocolRejectsRemoteKeyFetch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, _, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	for _, evt := range []uint8{shmevent.EventGetPrivateKey, shmevent.EventGetPublicKey} {
		resp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
			EventType: evt,
			ID:        1,
		}, nil)
		if err != nil {
			t.Fatalf("%s: %v", shmevent.EventName(evt), err)
		}
		if resp.EventType != shmevent.EventError {
			t.Fatalf("%s succeeded remotely, want rejection", shmevent.EventName(evt))
		}
	}
}

// TestRequirePermitForRemoteGate exercises Config.RequirePermitForRemote:
// off (the default) leaves an unconfirmed remote caller's Set succeeding
// unchanged; on, the same caller is rejected until a raft voter confirms
// its permit request.
func TestRequirePermitForRemoteGate(t *testing.T) {
	t.Parallel()

	// Each subtest gets its own context/cancel rather than sharing one
	// from the parent: subtests marked t.Parallel() keep running after
	// the parent function body returns, so a parent-scoped defer cancel()
	// would fire -- and cancel every subtest's context -- immediately,
	// before any of them actually ran.
	t.Run("default off: unconfirmed caller succeeds", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		leader := startTestLeader(t, ctx, Config{})
		remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

		setPayload, err := shmevent.EncodeSetPayload([]byte("hello"), []byte("world"))
		if err != nil {
			t.Fatalf("EncodeSetPayload: %v", err)
		}
		resp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
			EventType: shmevent.EventSet,
			Value:     setPayload,
			ID:        1,
		}, remotePriv)
		if err != nil {
			t.Fatalf("set: %v", err)
		}
		if resp.EventType == shmevent.EventError {
			t.Fatalf("set rejected with RequirePermitForRemote unset: %s", resp.Value)
		}
	})

	t.Run("on: rejected until confirmed", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		leader := startTestLeader(t, ctx, Config{RequirePermitForRemote: true})
		remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

		setPayload, err := shmevent.EncodeSetPayload([]byte("hello"), []byte("world"))
		if err != nil {
			t.Fatalf("EncodeSetPayload: %v", err)
		}
		resp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
			EventType: shmevent.EventSet,
			Value:     setPayload,
			ID:        1,
		}, remotePriv)
		if err != nil {
			t.Fatalf("set: %v", err)
		}
		if resp.EventType != shmevent.EventError {
			t.Fatal("set succeeded before any permit was requested/confirmed, want rejection")
		}
		if !strings.Contains(string(resp.Value), "not permitted") {
			t.Fatalf("set rejection = %q, want it to mention not being permitted", resp.Value)
		}

		reqPayload, err := shmevent.EncodePermitRequestPayload(shmevent.KindPermitPeer, []byte(remote.ID().String()), nil)
		if err != nil {
			t.Fatalf("EncodePermitRequestPayload: %v", err)
		}
		reqResp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
			EventType: shmevent.EventPermitRequest,
			Value:     reqPayload,
			ID:        2,
		}, remotePriv)
		if err != nil {
			t.Fatalf("permit_request: %v", err)
		}
		if reqResp.EventType == shmevent.EventError {
			t.Fatalf("permit_request rejected: %s", reqResp.Value)
		}

		// leader is itself the sole raft voter, so it can confirm its own
		// pending record locally -- mirrors permit_test.go's "call"
		// helper for a local caller; the true CLI-facing
		// RequestPermit/ConfirmPermit round trip is covered separately by
		// pkg/kvctl's own test.
		confirmPayload := shmevent.EncodePermitConfirmPayload(shmevent.KindPermitPeer, []byte(remote.ID().String()))
		confirmBuf, err := shmevent.Encode(shmevent.Msg{
			EventType: shmevent.EventPermitConfirm,
			Value:     confirmPayload,
			ID:        3,
		}, leader.ed25519Priv)
		if err != nil {
			t.Fatalf("encode confirm: %v", err)
		}
		decodedConfirm, crc, sig, err := shmevent.Decode(confirmBuf)
		if err != nil {
			t.Fatalf("decode confirm: %v", err)
		}
		confirmResp := leader.handleShmEvent(ctx, decodedConfirm, crc, sig, leader.localCaller())
		if confirmResp.EventType == shmevent.EventError {
			t.Fatalf("permit_confirm rejected: %s", confirmResp.Value)
		}

		resp2, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
			EventType: shmevent.EventSet,
			Value:     setPayload,
			ID:        4,
		}, remotePriv)
		if err != nil {
			t.Fatalf("set (after confirm): %v", err)
		}
		if resp2.EventType == shmevent.EventError {
			t.Fatalf("set rejected after permit confirmed: %s", resp2.Value)
		}
	})
}
