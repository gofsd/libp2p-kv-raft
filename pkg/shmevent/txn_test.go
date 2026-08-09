package shmevent

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeTxnPayloadRoundTrip(t *testing.T) {
	cases := [][]TxnOpSpec{
		nil,
		{{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")}},
		{
			{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")},
			{Op: TxnOpDelete, Key: []byte("k2"), Value: []byte{}},
			{Op: TxnOpSet, Key: []byte("k3"), Value: []byte{}},
		},
	}

	for _, ops := range cases {
		payload, err := EncodeTxnPayload(ops)
		if err != nil {
			t.Fatalf("EncodeTxnPayload(%v): %v", ops, err)
		}
		got, err := DecodeTxnPayload(payload)
		if err != nil {
			t.Fatalf("DecodeTxnPayload: %v", err)
		}
		if len(got) == 0 && len(ops) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, ops) {
			t.Fatalf("round trip = %+v, want %+v", got, ops)
		}
	}
}

func TestEncodeTxnPayloadRejectsEmptyKey(t *testing.T) {
	_, err := EncodeTxnPayload([]TxnOpSpec{{Op: TxnOpSet, Key: nil, Value: []byte("v")}})
	if err == nil {
		t.Fatal("EncodeTxnPayload with an empty key unexpectedly succeeded")
	}
}

func TestEncodeTxnPayloadRejectsUnknownOp(t *testing.T) {
	_, err := EncodeTxnPayload([]TxnOpSpec{{Op: 99, Key: []byte("k"), Value: []byte("v")}})
	if err == nil {
		t.Fatal("EncodeTxnPayload with an unknown op kind unexpectedly succeeded")
	}
}

func TestDecodeTxnPayloadRejectsTruncatedPayload(t *testing.T) {
	payload, err := EncodeTxnPayload([]TxnOpSpec{{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	if _, err := DecodeTxnPayload(payload[:len(payload)-1]); err == nil {
		t.Fatal("DecodeTxnPayload of a truncated payload unexpectedly succeeded")
	}
}

func TestDecodeTxnPayloadRejectsTrailingBytes(t *testing.T) {
	payload, err := EncodeTxnPayload([]TxnOpSpec{{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	if _, err := DecodeTxnPayload(append(payload, 0xFF)); err == nil {
		t.Fatal("DecodeTxnPayload with trailing bytes unexpectedly succeeded")
	}
}

func TestParseTxnOpsString(t *testing.T) {
	ops, err := ParseTxnOpsString("k1=v1 k2=with=equals del:k3")
	if err != nil {
		t.Fatalf("ParseTxnOpsString: %v", err)
	}
	want := []TxnOpSpec{
		{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")},
		{Op: TxnOpSet, Key: []byte("k2"), Value: []byte("with=equals")},
		{Op: TxnOpDelete, Key: []byte("k3")},
	}
	if !reflect.DeepEqual(ops, want) {
		t.Fatalf("ParseTxnOpsString = %+v, want %+v", ops, want)
	}
}

// The precondition prefixes share the grammar with plain sets, and the
// prefix check has to come first: `if:k=v` is a compare, not a set of a key
// literally named "if:k". Getting that order wrong would turn every
// precondition into a silent write -- the exact opposite of what it's for.
func TestParseTxnOpsStringParsesPreconditions(t *testing.T) {
	ops, err := ParseTxnOpsString("if:k1=v1 ifabsent:k2 k3=v3 del:k4")
	if err != nil {
		t.Fatalf("ParseTxnOpsString: %v", err)
	}
	want := []TxnOpSpec{
		{Op: TxnOpCompare, Key: []byte("k1"), Value: []byte("v1")},
		{Op: TxnOpCompareAbsent, Key: []byte("k2")},
		{Op: TxnOpSet, Key: []byte("k3"), Value: []byte("v3")},
		{Op: TxnOpDelete, Key: []byte("k4")},
	}
	if !reflect.DeepEqual(ops, want) {
		t.Fatalf("ops = %+v, want %+v", ops, want)
	}

	// A compare's own value keeps the same "split on the first = only" rule
	// a set's does, so a value containing = round-trips.
	ops, err = ParseTxnOpsString("if:k=a=b")
	if err != nil {
		t.Fatalf("ParseTxnOpsString: %v", err)
	}
	if string(ops[0].Value) != "a=b" {
		t.Fatalf("value = %q, want a=b", ops[0].Value)
	}

	if _, err := ParseTxnOpsString("if:novalue"); err == nil {
		t.Fatal("ParseTxnOpsString accepted an if: op with no =value")
	}
	if _, err := ParseTxnOpsString("ifabsent:"); err == nil {
		t.Fatal("ParseTxnOpsString accepted an ifabsent: op with an empty key")
	}
}

// A precondition round-trips through the wire format like any other op --
// it travels in the same length-prefixed op list, and a decoder that dropped
// or reordered it would turn a guarded write into an unguarded one.
func TestTxnPayloadRoundTripsPreconditions(t *testing.T) {
	ops := []TxnOpSpec{
		{Op: TxnOpCompare, Key: []byte("index"), Value: []byte("a,b")},
		// Explicitly empty rather than nil: DecodeTxnPayload always yields a
		// non-nil (possibly zero-length) Value, and an op whose Value is
		// ignored anyway is exactly where that distinction would otherwise
		// make a round-trip assertion fail for no reason that matters.
		{Op: TxnOpCompareAbsent, Key: []byte("doc/c"), Value: []byte{}},
		{Op: TxnOpSet, Key: []byte("doc/c"), Value: []byte("{}")},
	}
	payload, err := EncodeTxnPayload(ops)
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	got, err := DecodeTxnPayload(payload)
	if err != nil {
		t.Fatalf("DecodeTxnPayload: %v", err)
	}
	if !reflect.DeepEqual(got, ops) {
		t.Fatalf("ops = %+v, want %+v", got, ops)
	}
}

func TestParseTxnOpsStringRejectsEmptyString(t *testing.T) {
	if _, err := ParseTxnOpsString(""); err == nil {
		t.Fatal("ParseTxnOpsString(\"\") unexpectedly succeeded")
	}
	if _, err := ParseTxnOpsString("   "); err == nil {
		t.Fatal("ParseTxnOpsString of an all-whitespace string unexpectedly succeeded")
	}
}

func TestParseTxnOpsStringRejectsMalformedToken(t *testing.T) {
	if _, err := ParseTxnOpsString("k1=v1 not-a-valid-op"); err == nil {
		t.Fatal("ParseTxnOpsString with a malformed token unexpectedly succeeded")
	}
}

func TestParseTxnOpsStringRejectsEmptyDeleteKey(t *testing.T) {
	if _, err := ParseTxnOpsString("del:"); err == nil {
		t.Fatal("ParseTxnOpsString(\"del:\") unexpectedly succeeded")
	}
}
