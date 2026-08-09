package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// callTxnLocal mirrors catalog_test.go's own "call" helper: encode m with
// n's own signing key, decode it back (recomputing crc/sig the same way a
// real caller's round trip would), then dispatch it straight through
// handleShmEvent as a local caller.
func callTxnLocal(t *testing.T, ctx context.Context, n *Node, m shmevent.Msg) shmevent.Msg {
	t.Helper()
	return callLocal(t, ctx, n, m, n.ed25519Priv)
}

// newRawTxnMsg builds a txn Msg directly through the low-level generated
// accessors, bypassing NewTxn's own ValidTxnOp/empty-key validation --
// mirrors what NewTxn does internally, minus the checks -- so a test can
// put an op onto the wire that NewTxn itself would refuse to build, and
// exercise kvfsm.Apply's own independent validation instead.
func newRawTxnMsg(t *testing.T, ops []shmevent.TxnOpSpec) shmevent.Msg {
	t.Helper()
	m, err := shmevent.NewMsg()
	if err != nil {
		t.Fatalf("NewMsg: %v", err)
	}
	m.SetTxn()
	grp := m.Txn()
	list, err := grp.NewOps(int32(len(ops)))
	if err != nil {
		t.Fatalf("NewOps: %v", err)
	}
	for i, op := range ops {
		dst := list.At(i)
		dst.SetOp(op.Op)
		if err := dst.SetKey(op.Key); err != nil {
			t.Fatalf("SetKey: %v", err)
		}
		if err := dst.SetValue(op.Value); err != nil {
			t.Fatalf("SetValue: %v", err)
		}
	}
	return m
}

// TestEventTxnAppliesAllOpsAtomically proves a multi-op txn -- two Sets and
// a Delete of a key set up ahead of time -- lands as a single unit: every
// op is independently readable afterward via getFieldByKey.
func TestEventTxnAppliesAllOpsAtomically(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := startTestLeader(t, ctx, Config{})

	// Pre-existing key the transaction will delete.
	setMsg, err := shmevent.NewSet([]byte("to-delete"), []byte("gone-soon"))
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	setMsg.SetId(1)
	if resp := callTxnLocal(t, ctx, leader, setMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("seed set: %s", mustErrMessage(t, resp))
	}

	ops := []shmevent.TxnOpSpec{
		{Op: shmevent.TxnOpSet, Key: []byte("k1"), Value: []byte("v1")},
		{Op: shmevent.TxnOpSet, Key: []byte("k2"), Value: []byte("v2")},
		{Op: shmevent.TxnOpDelete, Key: []byte("to-delete")},
	}
	txnMsg, err := shmevent.NewTxn(ops)
	if err != nil {
		t.Fatalf("NewTxn: %v", err)
	}
	txnMsg.SetId(2)
	if resp := callTxnLocal(t, ctx, leader, txnMsg); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("txn: %s", mustErrMessage(t, resp))
	}

	get := func(key string) (string, bool) {
		m, err := shmevent.NewGetFieldByKey([]byte(key))
		if err != nil {
			t.Fatalf("NewGetFieldByKey: %v", err)
		}
		m.SetId(3)
		resp := callTxnLocal(t, ctx, leader, m)
		if resp.Which() == shmevent.Event_Which_error {
			return "", false
		}
		value, err := resp.GetFieldByKey().Value()
		if err != nil {
			t.Fatalf("GetFieldByKey value: %v", err)
		}
		return string(value), true
	}

	if v, ok := get("k1"); !ok || v != "v1" {
		t.Fatalf("get(k1) = %q, %v, want v1, true", v, ok)
	}
	if v, ok := get("k2"); !ok || v != "v2" {
		t.Fatalf("get(k2) = %q, %v, want v2, true", v, ok)
	}
	if _, ok := get("to-delete"); ok {
		t.Fatal("to-delete still readable after txn deleted it")
	}
}

// TestEventTxnRejectsReservedNamespaceKeyWithNoPartialEffect proves a
// transaction naming a shmevent.SystemKeyPrefix key anywhere in its op
// list is rejected outright, and -- since pkg/daemon validates every op's
// key before ever forwarding the request -- none of its other, otherwise
// valid ops land either.
func TestEventTxnRejectsReservedNamespaceKeyWithNoPartialEffect(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := startTestLeader(t, ctx, Config{})

	reservedKey := append([]byte{shmevent.SystemKeyPrefix}, []byte("whatever")...)
	ops := []shmevent.TxnOpSpec{
		{Op: shmevent.TxnOpSet, Key: []byte("ordinary-key"), Value: []byte("ordinary-value")},
		{Op: shmevent.TxnOpSet, Key: reservedKey, Value: []byte("nope")},
	}
	txnMsg, err := shmevent.NewTxn(ops)
	if err != nil {
		t.Fatalf("NewTxn: %v", err)
	}
	txnMsg.SetId(1)
	resp := callTxnLocal(t, ctx, leader, txnMsg)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("txn touching a reserved-namespace key unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "reserved") {
		t.Fatalf("txn rejection = %q, want it to mention the reserved namespace", mustErrMessage(t, resp))
	}

	getMsg, err := shmevent.NewGetFieldByKey([]byte("ordinary-key"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	getMsg.SetId(2)
	getResp := callTxnLocal(t, ctx, leader, getMsg)
	if getResp.Which() != shmevent.Event_Which_error {
		t.Fatal("ordinary-key was written despite the whole txn being rejected")
	}
}

// TestEventTxnRejectsUnknownOpKindWithNoPartialEffect proves an
// unrecognized TxnOpSpec.Op value fails the whole kvfsm.Apply call before
// any of the transaction's ops are written -- kvfsm.OpTxn's own
// validate-then-write discipline, one layer past pkg/daemon's reserved-key
// gate. Built via newRawTxnMsg, since NewTxn itself already refuses an
// invalid op kind client-side (see that constructor's own validation) --
// this test is specifically about kvfsm.Apply's independent check.
func TestEventTxnRejectsUnknownOpKindWithNoPartialEffect(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := startTestLeader(t, ctx, Config{})

	ops := []shmevent.TxnOpSpec{
		{Op: shmevent.TxnOpSet, Key: []byte("k1"), Value: []byte("v1")},
		{Op: 99, Key: []byte("k2"), Value: nil}, // unrecognized op kind
	}
	txnMsg := newRawTxnMsg(t, ops)
	txnMsg.SetId(1)

	resp := callTxnLocal(t, ctx, leader, txnMsg)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("txn with an unknown op kind unexpectedly succeeded")
	}

	getMsg, err := shmevent.NewGetFieldByKey([]byte("k1"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	getMsg.SetId(2)
	getResp := callTxnLocal(t, ctx, leader, getMsg)
	if getResp.Which() != shmevent.Event_Which_error {
		t.Fatal("k1 was written despite the whole txn being rejected for an unknown op kind")
	}
}
