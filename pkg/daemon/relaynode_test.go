package daemon

import (
	"strconv"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// newTestPeerAddr generates a fresh Ed25519 identity and returns a
// syntactically valid relay multiaddr for it -- relayCandidates requires a
// real, parseable /p2p/<peerID> component (peer.AddrInfoFromP2pAddr), so a
// placeholder string won't do.
func newTestPeerAddr(t *testing.T, port int) string {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	return "/ip4/127.0.0.1/tcp/" + strconv.Itoa(port) + "/p2p/" + pid.String()
}

func mustPeerID(t *testing.T, addr string) string {
	t.Helper()
	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		t.Fatalf("parse multiaddr %s: %v", addr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		t.Fatalf("parse %s: %v", addr, err)
	}
	return info.ID.String()
}

// TestRelayCandidatesMergesSeedsAndStoreSortedByPriority exercises
// relayCandidates directly (no host/raft needed -- it's a plain function
// over Config.RelayPeers and a local store.Store read): Config.RelayPeers
// seeds must come first, in the order given, ahead of every confirmed
// shmevent.KindBootstrapNode record regardless of that record's own
// priority; among the store-derived entries, lower priority must sort
// first; a malformed entry (unparseable multiaddr) must be skipped rather
// than failing the whole call; and a peer id present in both lists must
// only appear once.
func TestRelayCandidatesMergesSeedsAndStoreSortedByPriority(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	seedAddr := newTestPeerAddr(t, 4001)
	dupAddr := newTestPeerAddr(t, 4002) // seeded AND registered as a bootstrap node
	lowPriorityAddr := newTestPeerAddr(t, 4003)
	highPriorityAddr := newTestPeerAddr(t, 4004)

	writeConfirmed := func(addr string, priority uint8) {
		key := shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusConfirmed, []byte(mustPeerID(t, addr)))
		value := shmevent.EncodeBootstrapNodeMetadata(addr, priority)
		if err := st.Set(key, []byte(value)); err != nil {
			t.Fatalf("set %s: %v", addr, err)
		}
	}

	// Registered out of priority order on purpose, to prove sorting -- not
	// insertion order -- decides the result.
	writeConfirmed(highPriorityAddr, 200)
	writeConfirmed(lowPriorityAddr, 1)
	writeConfirmed(dupAddr, 5)

	// A pending (not yet confirmed) record must never be picked up.
	pendingAddr := newTestPeerAddr(t, 4005)
	pendingKey := shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusPending, []byte(mustPeerID(t, pendingAddr)))
	if err := st.Set(pendingKey, []byte(shmevent.EncodeBootstrapNodeMetadata(pendingAddr, 0))); err != nil {
		t.Fatalf("set pending: %v", err)
	}

	cfg := Config{RelayPeers: []string{seedAddr, dupAddr, "/not/a/valid/multiaddr"}}
	candidates := relayCandidates(cfg, st)

	wantIDs := []string{
		mustPeerID(t, seedAddr),
		mustPeerID(t, dupAddr),
		mustPeerID(t, lowPriorityAddr),
		mustPeerID(t, highPriorityAddr),
	}
	if len(candidates) != len(wantIDs) {
		t.Fatalf("relayCandidates returned %d entries, want %d: %+v", len(candidates), len(wantIDs), candidates)
	}
	for i, want := range wantIDs {
		if candidates[i].ID.String() != want {
			t.Fatalf("candidate[%d].ID = %s, want %s (full list: %+v)", i, candidates[i].ID.String(), want, candidates)
		}
	}
}
