package shmevent

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeTxnPayloadRoundTrip(t *testing.T) {
	cases := [][]TxnOp{
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
	_, err := EncodeTxnPayload([]TxnOp{{Op: TxnOpSet, Key: nil, Value: []byte("v")}})
	if err == nil {
		t.Fatal("EncodeTxnPayload with an empty key unexpectedly succeeded")
	}
}

func TestEncodeTxnPayloadRejectsUnknownOp(t *testing.T) {
	_, err := EncodeTxnPayload([]TxnOp{{Op: 99, Key: []byte("k"), Value: []byte("v")}})
	if err == nil {
		t.Fatal("EncodeTxnPayload with an unknown op kind unexpectedly succeeded")
	}
}

func TestDecodeTxnPayloadRejectsTruncatedPayload(t *testing.T) {
	payload, err := EncodeTxnPayload([]TxnOp{{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("EncodeTxnPayload: %v", err)
	}
	if _, err := DecodeTxnPayload(payload[:len(payload)-1]); err == nil {
		t.Fatal("DecodeTxnPayload of a truncated payload unexpectedly succeeded")
	}
}

func TestDecodeTxnPayloadRejectsTrailingBytes(t *testing.T) {
	payload, err := EncodeTxnPayload([]TxnOp{{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")}})
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
	want := []TxnOp{
		{Op: TxnOpSet, Key: []byte("k1"), Value: []byte("v1")},
		{Op: TxnOpSet, Key: []byte("k2"), Value: []byte("with=equals")},
		{Op: TxnOpDelete, Key: []byte("k3")},
	}
	if !reflect.DeepEqual(ops, want) {
		t.Fatalf("ParseTxnOpsString = %+v, want %+v", ops, want)
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
