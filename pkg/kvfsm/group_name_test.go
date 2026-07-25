package kvfsm

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

func TestApplyOpSetRejectsDuplicateGroupName(t *testing.T) {
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

	key1 := shmevent.GroupKey([]byte("grp-1"))
	if err := apply(key1, shmevent.EncodeGroupPayload("Infantry", false)); err != nil {
		t.Fatalf("Apply OpSet grp-1: %v", err)
	}

	// A different id claiming the same name must be rejected, and must
	// not have been written.
	key2 := shmevent.GroupKey([]byte("grp-2"))
	if err := apply(key2, shmevent.EncodeGroupPayload("Infantry", false)); err == nil {
		t.Fatal("Apply OpSet grp-2 with a duplicate name unexpectedly succeeded")
	}
	if _, err := s.Get(key2); err == nil {
		t.Fatal("grp-2 was written despite the duplicate-name rejection")
	}

	// Re-Putting grp-1 under its own id, even with the exact same name, is
	// not a collision with itself.
	if err := apply(key1, shmevent.EncodeGroupPayload("Infantry", true)); err != nil {
		t.Fatalf("Apply OpSet grp-1 (re-Put, same name): %v", err)
	}

	// Renaming grp-1 to a genuinely new, unused name still succeeds.
	if err := apply(key1, shmevent.EncodeGroupPayload("Cavalry", false)); err != nil {
		t.Fatalf("Apply OpSet grp-1 (rename to unused name): %v", err)
	}

	// Now that grp-1 no longer holds "Infantry", grp-2 may claim it.
	if err := apply(key2, shmevent.EncodeGroupPayload("Infantry", false)); err != nil {
		t.Fatalf("Apply OpSet grp-2 (name freed by grp-1's rename): %v", err)
	}

	// A plain, non-Group key (e.g. a Command record) is entirely
	// unaffected by Group name collisions.
	cmdKey := shmevent.CommandKey([]byte("cmd-1"))
	cmdPayload, err := shmevent.EncodeCommandPayload("Infantry", []byte("peer1"))
	if err != nil {
		t.Fatalf("EncodeCommandPayload: %v", err)
	}
	if err := apply(cmdKey, cmdPayload); err != nil {
		t.Fatalf("Apply OpSet cmd-1 sharing a name with a Group: %v", err)
	}
}
