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
	buf, err := shmevent.Encode(m, n.ed25519Priv)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
}

// TestEventTxnAppliesAllOpsAtomically proves a multi-op EventTxn -- two
// Sets and a Delete of a key set up ahead of time -- lands as a single
// unit: every op is independently readable afterward via EventGetField.
func TestEventTxnAppliesAllOpsAtomically(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := startTestLeader(t, ctx, Config{})

	// Pre-existing key the transaction will delete.
	setPayload, err := shmevent.EncodeSetPayload([]byte("to-delete"), []byte("gone-soon"))
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	if resp := callTxnLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventSet, Value: setPayload, ID: 1}); resp.EventType == shmevent.EventError {
		t.Fatalf("seed set: %s", resp.Value)
	}

	ops := []shmevent.TxnOp{
		{Op: shmevent.TxnOpSet, Key: []byte("k1"), Value: []byte("v1")},
		{Op: shmevent.TxnOpSet, Key: []byte("k2"), Value: []byte("v2")},
		{Op: shmevent.TxnOpDelete, Key: []byte("to-delete")},
	}
	txnPayload, err := shmevent.EncodeTxnPayload(ops)
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	if resp := callTxnLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventTxn, Value: txnPayload, ID: 2}); resp.EventType == shmevent.EventError {
		t.Fatalf("txn: %s", resp.Value)
	}

	get := func(key string) (string, bool) {
		resp := callTxnLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: []byte(key), ID: 3})
		if resp.EventType == shmevent.EventError {
			return "", false
		}
		return string(resp.Value), true
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
	ops := []shmevent.TxnOp{
		{Op: shmevent.TxnOpSet, Key: []byte("ordinary-key"), Value: []byte("ordinary-value")},
		{Op: shmevent.TxnOpSet, Key: reservedKey, Value: []byte("nope")},
	}
	txnPayload, err := shmevent.EncodeTxnPayload(ops)
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	resp := callTxnLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventTxn, Value: txnPayload, ID: 1})
	if resp.EventType != shmevent.EventError {
		t.Fatal("txn touching a reserved-namespace key unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "reserved") {
		t.Fatalf("txn rejection = %q, want it to mention the reserved namespace", resp.Value)
	}

	getResp := callTxnLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: []byte("ordinary-key"), ID: 2})
	if getResp.EventType != shmevent.EventError {
		t.Fatal("ordinary-key was written despite the whole txn being rejected")
	}
}

// TestEventTxnRejectsUnknownOpKindWithNoPartialEffect proves an
// unrecognized TxnOp.Op value fails the whole kvfsm.Apply call before any
// of the transaction's ops are written -- kvfsm.OpTxn's own
// validate-then-write discipline, one layer past pkg/daemon's
// reserved-key gate.
func TestEventTxnRejectsUnknownOpKindWithNoPartialEffect(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := startTestLeader(t, ctx, Config{})

	// EncodeTxnPayload itself validates Op, so build the malformed payload
	// by hand to exercise kvfsm.Apply's own validation independently of
	// it.
	validOp, err := shmevent.EncodeTxnPayload([]shmevent.TxnOp{{Op: shmevent.TxnOpSet, Key: []byte("k1"), Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	// Two ops: the valid one above, plus a second with an unknown op byte
	// (99), appended by hand in the same wire framing EncodeTxnPayload
	// itself uses ([1-byte op][2-byte keylen][key][2-byte valuelen][value]).
	malformed := append([]byte{}, validOp...)
	malformed[1] = 2 // op count: 1 -> 2
	malformed = append(malformed, 99, 0, 2, 'k', '2', 0, 0)

	resp := callTxnLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventTxn, Value: malformed, ID: 1})
	if resp.EventType != shmevent.EventError {
		t.Fatal("txn with an unknown op kind unexpectedly succeeded")
	}

	getResp := callTxnLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: []byte("k1"), ID: 2})
	if getResp.EventType != shmevent.EventError {
		t.Fatal("k1 was written despite the whole txn being rejected for an unknown op kind")
	}
}
