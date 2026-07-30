package kvfsm

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

func TestApplyOpSetRejectsDuplicateCommandPeerID(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	apply := func(key, value []byte) error {
		res, ok := f.Apply(&raft.Log{Data: EncodeCommand(OpSet, key, value)}).(ApplyResult)
		if !ok {
			t.Fatal("Apply did not return ApplyResult")
		}
		return res.Err
	}
	payload := func(name, peerID string) []byte {
		v, err := shmevent.EncodeCommandPayload(name, []byte(peerID))
		if err != nil {
			t.Fatalf("EncodeCommandPayload: %v", err)
		}
		return v
	}

	key1 := shmevent.CommandKey([]byte("cmd-1"))
	if err := apply(key1, payload("Reboot", "peer1")); err != nil {
		t.Fatalf("Apply OpSet cmd-1: %v", err)
	}

	// A different id claiming the same peerID must be rejected, and must
	// not have been written.
	key2 := shmevent.CommandKey([]byte("cmd-2"))
	if err := apply(key2, payload("Shutdown", "peer1")); err == nil {
		t.Fatal("Apply OpSet cmd-2 with a duplicate peerID unexpectedly succeeded")
	}
	if _, err := s.Get(key2); err == nil {
		t.Fatal("cmd-2 was written despite the duplicate-peerID rejection")
	}

	// Re-Putting cmd-1 under its own id, even with the exact same peerID,
	// is not a collision with itself.
	if err := apply(key1, payload("Reboot v2", "peer1")); err != nil {
		t.Fatalf("Apply OpSet cmd-1 (re-Put, same peerID): %v", err)
	}

	// Reassigning cmd-1 to a genuinely new, unused peerID still succeeds.
	if err := apply(key1, payload("Reboot v2", "peer2")); err != nil {
		t.Fatalf("Apply OpSet cmd-1 (reassign to unused peerID): %v", err)
	}

	// Now that cmd-1 no longer holds peer1, cmd-2 may claim it.
	if err := apply(key2, payload("Shutdown", "peer1")); err != nil {
		t.Fatalf("Apply OpSet cmd-2 (peerID freed by cmd-1's reassignment): %v", err)
	}

	// A plain, non-Command key (e.g. a Group record) is entirely
	// unaffected by Command peerID collisions.
	grpKey := shmevent.GroupKey([]byte("grp-1"))
	if err := apply(grpKey, shmevent.EncodeGroupPayload("peer1", false)); err != nil {
		t.Fatalf("Apply OpSet grp-1 sharing a name with a peerID: %v", err)
	}
}
