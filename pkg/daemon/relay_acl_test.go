package daemon

import (
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// TestRelayACL exercises relayACL.AllowReserve/AllowConnect directly
// against a real *store.Store: both must deny a peer with no
// ReservedGroupRelay (or ReservedGroupCluster, or pairwise personal group)
// PeerGroup record and allow one once such a record exists -- the same
// group-ACL mechanism handleChannelStream uses for Channel, see
// isAuthorizedForGatedAccessSt's doc comment.
func TestRelayACL(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	selfPriv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate self key: %v", err)
	}
	selfID, err := peer.IDFromPrivateKey(selfPriv)
	if err != nil {
		t.Fatalf("self peer id from key: %v", err)
	}

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer id from key: %v", err)
	}

	acl := relayACL{store: st, selfPeerID: selfID.String(), quota: newQuotaTracker(0, 0, 0, 0)}

	if acl.AllowReserve(id, nil) {
		t.Fatal("AllowReserve succeeded for a peer in no relay-gating group")
	}
	if acl.AllowConnect(id, nil, "") {
		t.Fatal("AllowConnect succeeded for a peer in no relay-gating group")
	}

	key, err := shmevent.PeerGroupKey([]byte(id.String()), []byte(shmevent.ReservedGroupRelay))
	if err != nil {
		t.Fatalf("build relay PeerGroup key: %v", err)
	}
	if err := st.Set(key, nil); err != nil {
		t.Fatalf("grant relay group membership: %v", err)
	}

	if !acl.AllowReserve(id, nil) {
		t.Fatal("AllowReserve rejected a peer granted ReservedGroupRelay membership")
	}
	if !acl.AllowConnect(id, nil, "") {
		t.Fatal("AllowConnect rejected a peer granted ReservedGroupRelay membership")
	}
}

// TestRelayACLQuotaExhaustion confirms relayACL denies once the relay
// quota's peer bucket is exhausted, even though the group-ACL check still
// passes -- quota and group admission are independent gates, both must
// allow (see relayACL.allow).
func TestRelayACLQuotaExhaustion(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("peer id from key: %v", err)
	}

	key, err := shmevent.PeerGroupKey([]byte(id.String()), []byte(shmevent.ReservedGroupRelay))
	if err != nil {
		t.Fatalf("build relay PeerGroup key: %v", err)
	}
	if err := st.Set(key, nil); err != nil {
		t.Fatalf("grant relay group membership: %v", err)
	}

	const burst = 2
	acl := relayACL{store: st, selfPeerID: "self", quota: newQuotaTracker(1, burst, 0, 0)}

	for i := 0; i < burst; i++ {
		if !acl.AllowReserve(id, nil) {
			t.Fatalf("AllowReserve %d: unexpectedly denied within burst budget", i)
		}
	}
	if acl.AllowReserve(id, nil) {
		t.Fatal("AllowReserve succeeded past the peer quota's burst budget")
	}
}
