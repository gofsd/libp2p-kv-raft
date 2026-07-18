package kvfsm

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// withLowCap temporarily lowers maxSystemListEntries so a test can hit the
// cap without writing 65000 real rows, restoring it afterward.
func withLowCap(t *testing.T, n int) {
	t.Helper()
	orig := maxSystemListEntries
	maxSystemListEntries = n
	t.Cleanup(func() { maxSystemListEntries = orig })
}

func TestApplyOpSetRejectsSystemKeyAtCapacity(t *testing.T) {
	withLowCap(t, 2)

	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	apply := func(op OpType, key, value []byte) error {
		res, ok := f.Apply(&raft.Log{Data: EncodeCommand(op, key, value)}).(ApplyResult)
		if !ok {
			t.Fatal("Apply did not return ApplyResult")
		}
		return res.Err
	}

	prefix := []byte{0x00, 0x01, 0x01}
	key1 := append(append([]byte{}, prefix...), 'a')
	key2 := append(append([]byte{}, prefix...), 'b')
	key3 := append(append([]byte{}, prefix...), 'c')

	if err := apply(OpSet, key1, []byte("v1")); err != nil {
		t.Fatalf("Apply OpSet key1: %v", err)
	}
	if err := apply(OpSet, key2, []byte("v2")); err != nil {
		t.Fatalf("Apply OpSet key2: %v", err)
	}

	// The list is now at its (lowered) cap of 2; a third, genuinely new
	// key must be rejected.
	if err := apply(OpSet, key3, []byte("v3")); err == nil {
		t.Fatal("Apply OpSet key3 succeeded, want rejection at capacity")
	}
	if _, err := s.Get(key3); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("key3: got %v, want ErrNotFound (nothing should have been written)", err)
	}

	// An overwrite of an already-existing key doesn't grow the list, so
	// it must still succeed even while at capacity.
	if err := apply(OpSet, key1, []byte("v1-updated")); err != nil {
		t.Fatalf("Apply OpSet overwrite of key1 at capacity: %v", err)
	}
	v, err := s.Get(key1)
	if err != nil {
		t.Fatalf("Get key1: %v", err)
	}
	if string(v) != "v1-updated" {
		t.Fatalf("key1 = %q, want %q", v, "v1-updated")
	}

	// A different kind+status prefix is entirely unaffected by this
	// prefix's saturation.
	otherPrefix := []byte{0x00, 0x02, 0x01}
	otherKey := append(append([]byte{}, otherPrefix...), 'a')
	if err := apply(OpSet, otherKey, []byte("v")); err != nil {
		t.Fatalf("Apply OpSet under a different kind's prefix: %v", err)
	}
}

func TestApplyOpConfirmRejectsConfirmedAtCapacity(t *testing.T) {
	withLowCap(t, 1)

	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	apply := func(op OpType, key, value []byte) error {
		res, ok := f.Apply(&raft.Log{Data: EncodeCommand(op, key, value)}).(ApplyResult)
		if !ok {
			t.Fatal("Apply did not return ApplyResult")
		}
		return res.Err
	}

	pendingPrefix := []byte{0x00, 0x01, 0x01}
	confirmedPrefix := []byte{0x00, 0x01, 0x02}

	// Saturate the pending list -- this must NOT block room in the
	// independent confirmed list for the same kind.
	pendingA := append(append([]byte{}, pendingPrefix...), 'a')
	pendingB := append(append([]byte{}, pendingPrefix...), 'b')
	if err := apply(OpSet, pendingA, []byte("meta-a")); err != nil {
		t.Fatalf("Apply OpSet pendingA: %v", err)
	}
	if err := apply(OpSet, pendingB, []byte("meta-b")); err == nil {
		t.Fatal("Apply OpSet pendingB succeeded, want rejection (pending list at capacity 1)")
	}

	confirmedA := append(append([]byte{}, confirmedPrefix...), 'a')
	if err := apply(OpConfirm, pendingA, confirmedA); err != nil {
		t.Fatalf("Apply OpConfirm pendingA->confirmedA: %v (confirmed list should have room)", err)
	}
	v, err := s.Get(confirmedA)
	if err != nil {
		t.Fatalf("Get confirmedA: %v", err)
	}
	if string(v) != "meta-a" {
		t.Fatalf("confirmedA = %q, want %q", v, "meta-a")
	}

	// Now the confirmed list itself is at capacity (1). A fresh pending
	// record promoted to a NEW confirmed key must be rejected, and the
	// pending record must survive (not deleted) since the confirm failed.
	// pendingA was deleted by the successful confirm above, so the
	// pending list (capacity 1) has room again for pendingC.
	pendingC := append(append([]byte{}, pendingPrefix...), 'c')
	if err := apply(OpSet, pendingC, []byte("meta-c")); err != nil {
		t.Fatalf("Apply OpSet pendingC: %v", err)
	}
	confirmedC := append(append([]byte{}, confirmedPrefix...), 'c')
	if err := apply(OpConfirm, pendingC, confirmedC); err == nil {
		t.Fatal("Apply OpConfirm pendingC->confirmedC succeeded, want rejection (confirmed list at capacity 1)")
	}
	if _, err := s.Get(confirmedC); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("confirmedC: got %v, want ErrNotFound (nothing should have been written)", err)
	}
	if _, err := s.Get(pendingC); err != nil {
		t.Fatalf("pendingC should survive a failed confirm: %v", err)
	}
}
