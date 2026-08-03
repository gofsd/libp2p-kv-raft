package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// callLocal signs m with priv (the "local shmring caller" convention --
// see localCaller's doc comment) and drives it through handleShmEvent
// exactly as pkg/ipc.Serve would, via a real Encode/Decode round trip
// rather than hand-computing crc/signature.
func callLocal(t *testing.T, ctx context.Context, n *Node, m shmevent.Msg, priv shmevent.PrivateKey) shmevent.Msg {
	t.Helper()
	buf, err := shmevent.Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
}

// connectPeers connects a's host directly to b, so a.host.NewStream can
// reach b without needing a relay -- this test topology mirrors
// TestRequirePermitForRemoteGate's newTestRemoteHost, just node-to-node
// instead of client-to-node.
func connectPeers(t *testing.T, ctx context.Context, a, b *Node) {
	t.Helper()
	bAddr := b.advertisedAddrs()[0]
	maddr, err := multiaddr.NewMultiaddr(bAddr)
	if err != nil {
		t.Fatalf("parse b addr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("b addr info: %v", err)
	}
	if err := a.host.Connect(ctx, *info); err != nil {
		t.Fatalf("connect a->b: %v", err)
	}
}

// startExecuteTestNode starts a bare daemon.Node (no bootstrap/join --
// EventExecute never touches raft, so this test never calls handleAdd)
// under its own DataDir inside t.TempDir().
func startExecuteTestNode(t *testing.T, dir string) *Node {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	keyPath := filepath.Join(dir, "identity.key")
	if _, err := p2praft.LoadOrGenerateKey(keyPath); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	n, err := start(Config{DataDir: dir, KeyPath: keyPath})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(n.shutdown)
	return n
}

// TestExecuteEventDeliversAcrossNodes is the end-to-end happy path: a
// local caller on node a sends EventExecute addressed (via registry ids,
// per that event's doc comment) at node b; delivery happens over a real
// ExecuteProtocolID libp2p stream, node b never touches its store or
// raft, and a caller on b eventually observes it via EventPollExecute,
// carrying a's peer id and the original payload.
func TestExecuteEventDeliversAcrossNodes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	// Grant a into b's "execute" reserved group -- see
	// TestExecuteGateUnconditional's identical setup. Neither bare test
	// node ever bootstraps raft, so there's no cluster membership to rely
	// on instead; this test is about delivery, not the ACL gate itself.
	executeGroupKey, err := shmevent.PeerGroupKey([]byte(a.peerID), []byte(shmevent.ReservedGroupExecute))
	if err != nil {
		t.Fatalf("PeerGroupKey: %v", err)
	}
	if err := b.store.Set(executeGroupKey, nil); err != nil {
		t.Fatalf("grant execute group: %v", err)
	}

	const sourceID, destID = 1, 2
	a.registry.Register(sourceID, []byte(a.peerID))
	a.registry.Register(destID, []byte(b.peerID))

	payload := []byte("hello from a")
	resp := callLocal(t, ctx, a, shmevent.Msg{
		EventType:     shmevent.EventExecute,
		SourceID:      sourceID,
		DestinationID: destID,
		Value:         payload,
		ID:            7,
	}, a.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("execute rejected: %s", resp.Value)
	}

	deadline := time.After(10 * time.Second)
	for {
		pollResp := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventPollExecute, ID: 1}, b.ed25519Priv)
		if pollResp.EventType == shmevent.EventError {
			t.Fatalf("poll_execute rejected: %s", pollResp.Value)
		}
		if len(pollResp.Value) > 0 {
			sender, gotPayload, err := shmevent.DecodeExecuteNotification(pollResp.Value)
			if err != nil {
				t.Fatalf("DecodeExecuteNotification: %v", err)
			}
			if string(sender) != a.peerID {
				t.Fatalf("notification sender = %q, want %q", sender, a.peerID)
			}
			if string(gotPayload) != string(payload) {
				t.Fatalf("notification payload = %q, want %q", gotPayload, payload)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("execute notification never arrived in b's inbox")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// A second poll must come back empty -- the queue is drained, not
	// re-readable.
	again := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventPollExecute, ID: 2}, b.ed25519Priv)
	if len(again.Value) != 0 {
		t.Fatalf("second poll_execute returned a notification, want empty queue: %q", again.Value)
	}

	if _, err := b.store.Get(payload); err == nil {
		t.Fatal("EventExecute unexpectedly wrote its payload into the store")
	}
}

// TestExecuteEventRejectsSpoofedSource confirms dispatchExecute refuses to
// relay on behalf of a source that isn't this node's own peer id -- since
// the peer-to-peer hop is signed with this node's own key regardless of
// what SourceID claims, honoring a mismatched claim would either silently
// mislabel the sender or (per handleExecuteStream's signature check) just
// fail illegibly on the receiving end instead of with a clear local error.
func TestExecuteEventRejectsSpoofedSource(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	const sourceID, destID = 1, 2
	a.registry.Register(sourceID, []byte("not-really-a"))
	a.registry.Register(destID, []byte(b.peerID))

	resp := callLocal(t, ctx, a, shmevent.Msg{
		EventType:     shmevent.EventExecute,
		SourceID:      sourceID,
		DestinationID: destID,
		Value:         []byte("payload"),
		ID:            1,
	}, a.ed25519Priv)
	if resp.EventType != shmevent.EventError {
		t.Fatal("execute with a spoofed source succeeded, want rejection")
	}
}

// TestExecuteStreamRejectsForgedSignature confirms handleExecuteStream's
// signature check is real: a message claiming to be from a's peer id but
// actually signed with an unrelated key must be rejected and never
// queued, regardless of which connection it arrived over.
func TestExecuteStreamRejectsForgedSignature(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))

	forgerPriv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate forger key: %v", err)
	}
	forger, err := libp2p.New(libp2p.Identity(forgerPriv))
	if err != nil {
		t.Fatalf("start forger host: %v", err)
	}
	defer forger.Close()

	bAddr := b.advertisedAddrs()[0]
	maddr, err := multiaddr.NewMultiaddr(bAddr)
	if err != nil {
		t.Fatalf("parse b addr: %v", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("b addr info: %v", err)
	}
	if err := forger.Connect(ctx, *info); err != nil {
		t.Fatalf("connect forger->b: %v", err)
	}

	value, err := shmevent.EncodeExecuteNotification([]byte(a.peerID), []byte("forged"))
	if err != nil {
		t.Fatalf("EncodeExecuteNotification: %v", err)
	}
	buf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventExecute, Value: value}, shmevent.PrivateKey(mustRaw(t, forgerPriv)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	bPeerID, err := peer.Decode(b.peerID)
	if err != nil {
		t.Fatalf("decode b peer id: %v", err)
	}
	s, err := forger.NewStream(ctx, bPeerID, ExecuteProtocolID)
	if err != nil {
		t.Fatalf("open stream to b: %v", err)
	}
	if _, err := s.Write(buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	s.CloseWrite()

	// b's inbox must never see this: give handleExecuteStream a moment to
	// run, then poll and confirm it's empty.
	time.Sleep(200 * time.Millisecond)
	pollResp := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventPollExecute, ID: 1}, b.ed25519Priv)
	if len(pollResp.Value) != 0 {
		t.Fatalf("forged execute notification was queued: %q", pollResp.Value)
	}
}

// TestExecuteGateUnconditional exercises handleExecuteStream's always-on
// isAuthorizedForGatedAccess(shmevent.ReservedGroupExecute) gate against a
// real two-node topology (mirroring TestExecuteEventDeliversAcrossNodes):
// a sender with neither an "execute" group grant nor "cluster" group
// membership must be rejected, while either one on its own must be
// accepted. Since these bare test nodes never bootstrap raft (see
// startExecuteTestNode), the group records are written directly into the
// receiver's store rather than via the real addpeertogroup/join workflows
// -- those are already covered elsewhere; this test is only about
// handleExecuteStream's gate itself.
func TestExecuteGateUnconditional(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))

	bDir := filepath.Join(tmpDir, "b")
	if err := os.MkdirAll(bDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", bDir, err)
	}
	bKeyPath := filepath.Join(bDir, "identity.key")
	if _, err := p2praft.LoadOrGenerateKey(bKeyPath); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	b, err := start(Config{DataDir: bDir, KeyPath: bKeyPath})
	if err != nil {
		t.Fatalf("start b: %v", err)
	}
	t.Cleanup(b.shutdown)

	connectPeers(t, ctx, a, b)

	const sourceID, destID = 1, 2
	a.registry.Register(sourceID, []byte(a.peerID))
	a.registry.Register(destID, []byte(b.peerID))

	send := func(id uint16) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, a, shmevent.Msg{
			EventType:     shmevent.EventExecute,
			SourceID:      sourceID,
			DestinationID: destID,
			Value:         []byte("payload"),
			ID:            id,
		}, a.ed25519Priv)
	}
	pollEmpty := func() bool {
		t.Helper()
		resp := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventPollExecute, ID: 99}, b.ed25519Priv)
		return len(resp.Value) == 0
	}

	aPeerID, err := peer.Decode(a.peerID)
	if err != nil {
		t.Fatalf("decode a peer id: %v", err)
	}

	// Neither granted nor a cluster member: sendExecute reads b's response
	// synchronously (see that function's doc comment), so the gate's
	// rejection on b surfaces straight back as a local dispatchExecute
	// error on a, not a silent drop.
	if resp := send(1); resp.EventType != shmevent.EventError {
		t.Fatal("execute from an unauthorized, non-member sender unexpectedly succeeded")
	}
	if !pollEmpty() {
		t.Fatal("unauthorized, non-member sender's notification was queued")
	}

	// Grant a into b's "execute" reserved group directly (PeerGroup(a,
	// execute)) -- the same record `mage addpeertogroup <peerID> execute`
	// would write.
	executeGroupKey, err := shmevent.PeerGroupKey([]byte(aPeerID.String()), []byte(shmevent.ReservedGroupExecute))
	if err != nil {
		t.Fatalf("PeerGroupKey: %v", err)
	}
	if err := b.store.Set(executeGroupKey, nil); err != nil {
		t.Fatalf("grant execute group: %v", err)
	}
	if resp := send(2); resp.EventType == shmevent.EventError {
		t.Fatalf("execute from an execute-group-granted sender rejected: %s", resp.Value)
	}
	if pollEmpty() {
		t.Fatal("execute-group-granted sender's notification was not queued")
	}

	// Revoke it again and confirm "cluster" group membership alone is
	// sufficient (the cluster-member exemption), independent of the
	// execute-group grant.
	if err := b.store.Delete(executeGroupKey); err != nil {
		t.Fatalf("revoke execute group: %v", err)
	}
	if resp := send(3); resp.EventType != shmevent.EventError {
		t.Fatal("execute from a sender with a revoked execute grant and no cluster membership unexpectedly succeeded")
	}
	if !pollEmpty() {
		t.Fatal("sender's notification was queued after its execute grant was revoked and it's not a cluster member")
	}

	clusterGroupKey, err := shmevent.PeerGroupKey([]byte(aPeerID.String()), []byte(shmevent.ReservedGroupCluster))
	if err != nil {
		t.Fatalf("PeerGroupKey: %v", err)
	}
	if err := b.store.Set(clusterGroupKey, nil); err != nil {
		t.Fatalf("record cluster group membership: %v", err)
	}
	if resp := send(4); resp.EventType == shmevent.EventError {
		t.Fatalf("execute from a cluster member rejected: %s", resp.Value)
	}
	if pollEmpty() {
		t.Fatal("cluster member's notification was not queued despite having no separate execute grant")
	}
}

func mustRaw(t *testing.T, priv crypto.PrivKey) []byte {
	t.Helper()
	raw, err := priv.Raw()
	if err != nil {
		t.Fatalf("raw private key: %v", err)
	}
	return raw
}
