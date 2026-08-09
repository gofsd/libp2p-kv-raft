package kvfsm

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// The compare ops are what makes a safe read-modify-write possible at all
// (see shmevent.TxnOpCompare), and the property that matters is not that a
// failed compare reports an error -- it's that it leaves the store *exactly*
// as it was. A compare that failed after its transaction had already written
// half its keys would be worse than no compare at all: a caller retrying the
// read-modify-write would be building on state it never saw.
func TestApplyOpTxnCompare(t *testing.T) {
	newFSM := func(t *testing.T) (*FSM, func(ops []shmevent.TxnOpSpec) ApplyResult) {
		t.Helper()
		s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		t.Cleanup(func() { s.Close() })

		f := New(s)
		return f, func(ops []shmevent.TxnOpSpec) ApplyResult {
			t.Helper()
			payload, err := shmevent.EncodeTxnPayload(ops)
			if err != nil {
				t.Fatalf("EncodeTxnPayload: %v", err)
			}
			res, ok := f.Apply(&raft.Log{Data: EncodeCommand(OpTxn, nil, payload)}).(ApplyResult)
			if !ok {
				t.Fatal("Apply did not return ApplyResult")
			}
			return res
		}
	}

	t.Run("a matching compare lets the writes land", func(t *testing.T) {
		f, apply := newFSM(t)
		if err := f.Store.Set([]byte("k"), []byte("old")); err != nil {
			t.Fatalf("seed: %v", err)
		}

		res := apply([]shmevent.TxnOpSpec{
			{Op: shmevent.TxnOpCompare, Key: []byte("k"), Value: []byte("old")},
			{Op: shmevent.TxnOpSet, Key: []byte("k"), Value: []byte("new")},
		})
		if res.Err != nil {
			t.Fatalf("apply: %v", res.Err)
		}
		if got, _ := f.Store.Get([]byte("k")); string(got) != "new" {
			t.Fatalf("k = %q, want new", got)
		}
	})

	t.Run("a stale compare writes nothing at all", func(t *testing.T) {
		f, apply := newFSM(t)
		if err := f.Store.Set([]byte("k"), []byte("changed-by-someone-else")); err != nil {
			t.Fatalf("seed: %v", err)
		}

		res := apply([]shmevent.TxnOpSpec{
			{Op: shmevent.TxnOpCompare, Key: []byte("k"), Value: []byte("what-i-read")},
			{Op: shmevent.TxnOpSet, Key: []byte("k"), Value: []byte("my-update")},
			{Op: shmevent.TxnOpSet, Key: []byte("unrelated"), Value: []byte("also-mine")},
		})
		if !errors.Is(res.Err, ErrCompareFailed) {
			t.Fatalf("err = %v, want ErrCompareFailed", res.Err)
		}
		if got, _ := f.Store.Get([]byte("k")); string(got) != "changed-by-someone-else" {
			t.Fatalf("k = %q, want the other writer's value untouched", got)
		}
		// The op the compare guards is not the only one rolled back: every
		// write in the transaction is, including ones naming keys the
		// compare never mentioned.
		if _, err := f.Store.Get([]byte("unrelated")); !errors.Is(err, store.ErrNotFound) {
			t.Fatal("a key written after the failed compare exists; the transaction was not atomic")
		}
	})

	t.Run("an absent key never satisfies a value compare", func(t *testing.T) {
		_, apply := newFSM(t)
		res := apply([]shmevent.TxnOpSpec{
			{Op: shmevent.TxnOpCompare, Key: []byte("missing"), Value: []byte("")},
			{Op: shmevent.TxnOpSet, Key: []byte("missing"), Value: []byte("v")},
		})
		// Empty means "exists and is empty" -- letting it also mean "isn't
		// there" would make every create-if-not-exists silently overwrite a
		// key somebody deliberately set to empty.
		if !errors.Is(res.Err, ErrCompareFailed) {
			t.Fatalf("err = %v, want ErrCompareFailed", res.Err)
		}
	})

	t.Run("compare-absent creates only when nobody got there first", func(t *testing.T) {
		f, apply := newFSM(t)

		res := apply([]shmevent.TxnOpSpec{
			{Op: shmevent.TxnOpCompareAbsent, Key: []byte("new-key")},
			{Op: shmevent.TxnOpSet, Key: []byte("new-key"), Value: []byte("first")},
		})
		if res.Err != nil {
			t.Fatalf("first create: %v", res.Err)
		}

		// The second creator is exactly the racing client this op exists
		// for: it must lose, and must not clobber the winner's value.
		res = apply([]shmevent.TxnOpSpec{
			{Op: shmevent.TxnOpCompareAbsent, Key: []byte("new-key")},
			{Op: shmevent.TxnOpSet, Key: []byte("new-key"), Value: []byte("second")},
		})
		if !errors.Is(res.Err, ErrCompareFailed) {
			t.Fatalf("second create err = %v, want ErrCompareFailed", res.Err)
		}
		if got, _ := f.Store.Get([]byte("new-key")); string(got) != "first" {
			t.Fatalf("new-key = %q, want first", got)
		}
	})

	t.Run("a compare guards keys other than the one it names", func(t *testing.T) {
		// The multi-key shape a plain single-key CAS can't express: publish
		// a document and its index entry together, but only if the index
		// hasn't moved under us.
		f, apply := newFSM(t)
		if err := f.Store.Set([]byte("index"), []byte("a,b")); err != nil {
			t.Fatalf("seed: %v", err)
		}

		res := apply([]shmevent.TxnOpSpec{
			{Op: shmevent.TxnOpCompare, Key: []byte("index"), Value: []byte("a,b")},
			{Op: shmevent.TxnOpSet, Key: []byte("index"), Value: []byte("a,b,c")},
			{Op: shmevent.TxnOpSet, Key: []byte("doc/c"), Value: []byte("{}")},
		})
		if res.Err != nil {
			t.Fatalf("apply: %v", res.Err)
		}
		if got, _ := f.Store.Get([]byte("index")); string(got) != "a,b,c" {
			t.Fatalf("index = %q", got)
		}
		if got, _ := f.Store.Get([]byte("doc/c")); string(got) != "{}" {
			t.Fatalf("doc/c = %q", got)
		}
	})
}
