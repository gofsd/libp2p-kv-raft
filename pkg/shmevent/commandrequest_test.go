package shmevent

import (
	"bytes"
	"testing"
)

func TestCommandRequestLogKindRoundTrip(t *testing.T) {
	kind := CommandRequestLogKind("cmd-1")
	if kind != "cmdreq:cmd-1" {
		t.Fatalf("got kind %q, want %q", kind, "cmdreq:cmd-1")
	}
	got, ok := ParseCommandRequestLogKind(kind)
	if !ok || got != "cmd-1" {
		t.Fatalf("got commandID=%q ok=%v, want %q true", got, ok, "cmd-1")
	}

	if _, ok := ParseCommandRequestLogKind("cmdlog"); ok {
		t.Fatal("ParseCommandRequestLogKind unexpectedly accepted an unrelated kind")
	}
}

func TestCommandRequestApplyPayloadRoundTrip(t *testing.T) {
	recordValue := []byte(`{"kind":"CommandRequest"}`)
	payload, err := EncodeCommandRequestApplyPayload("12D3KooWtest", recordValue)
	if err != nil {
		t.Fatalf("EncodeCommandRequestApplyPayload: %v", err)
	}
	gotPeerID, gotValue, err := DecodeCommandRequestApplyPayload(payload)
	if err != nil {
		t.Fatalf("DecodeCommandRequestApplyPayload: %v", err)
	}
	if gotPeerID != "12D3KooWtest" {
		t.Fatalf("got peerID %q, want %q", gotPeerID, "12D3KooWtest")
	}
	if !bytes.Equal(gotValue, recordValue) {
		t.Fatalf("got value %q, want %q", gotValue, recordValue)
	}

	if _, _, err := DecodeCommandRequestApplyPayload([]byte{0x00}); err == nil {
		t.Fatal("DecodeCommandRequestApplyPayload unexpectedly accepted a malformed payload")
	}
}
