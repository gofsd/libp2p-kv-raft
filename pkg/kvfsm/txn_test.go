package kvfsm

import (
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

func TestApplyOpTxnAppliesAllOpsAtomically(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	apply := func(op OpType, key, value []byte) ApplyResult {
		res, ok := f.Apply(&raft.Log{Data: EncodeCommand(op, key, value)}).(ApplyResult)
		if !ok {
			t.Fatal("Apply did not return ApplyResult")
		}
		return res
	}

	if err := s.Set([]byte("to-delete"), []byte("gone-soon")); err != nil {
		t.Fatalf("seed to-delete: %v", err)
	}

	ops := []shmevent.TxnOp{
		{Op: shmevent.TxnOpSet, Key: []byte("k1"), Value: []byte("v1")},
		{Op: shmevent.TxnOpSet, Key: []byte("k2"), Value: []byte("v2")},
		{Op: shmevent.TxnOpDelete, Key: []byte("to-delete")},
	}
	payload, err := shmevent.EncodeTxnPayload(ops)
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	if res := apply(OpTxn, nil, payload); res.Err != nil {
		t.Fatalf("Apply OpTxn: %v", res.Err)
	}

	if v, err := s.Get([]byte("k1")); err != nil || string(v) != "v1" {
		t.Fatalf("Get(k1) = %q, %v, want v1, nil", v, err)
	}
	if v, err := s.Get([]byte("k2")); err != nil || string(v) != "v2" {
		t.Fatalf("Get(k2) = %q, %v, want v2, nil", v, err)
	}
	if _, err := s.Get([]byte("to-delete")); err == nil {
		t.Fatal("to-delete still present after txn deleted it")
	}
}

func TestApplyOpTxnRejectsUnknownOpKindWithNoPartialEffect(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	f := New(s)
	apply := func(op OpType, key, value []byte) ApplyResult {
		res, ok := f.Apply(&raft.Log{Data: EncodeCommand(op, key, value)}).(ApplyResult)
		if !ok {
			t.Fatal("Apply did not return ApplyResult")
		}
		return res
	}

	// Two ops, hand-built in EncodeTxnPayload's own wire framing: a valid
	// Set for k1, then a second op with an unrecognized kind byte (99) --
	// EncodeTxnPayload itself refuses to build this, so it's assembled by
	// hand to exercise Apply's own validation independently.
	valid, err := shmevent.EncodeTxnPayload([]shmevent.TxnOp{{Op: shmevent.TxnOpSet, Key: []byte("k1"), Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	malformed := append([]byte{}, valid...)
	malformed[1] = 2 // op count: 1 -> 2
	malformed = append(malformed, 99, 0, 2, 'k', '2', 0, 0)

	if res := apply(OpTxn, nil, malformed); res.Err == nil {
		t.Fatal("Apply OpTxn with an unknown op kind unexpectedly succeeded")
	}
	if _, err := s.Get([]byte("k1")); err == nil {
		t.Fatal("k1 was written despite the whole txn being rejected for an unknown op kind")
	}
}
