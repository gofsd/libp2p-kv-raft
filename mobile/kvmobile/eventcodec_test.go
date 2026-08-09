package kvmobile

import (
	"encoding/json"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestDecodeEventNeedsNoDaemon proves DecodeEvent is a pure wire-format
// inspector: it decodes an unsigned get_public_key request built directly
// via pkg/shmevent (never dispatched, never touching a running daemon)
// back into the same JSON shape SendEvent/EncodeEvent use.
func TestDecodeEventNeedsNoDaemon(t *testing.T) {
	msg, err := shmevent.NewGetPublicKey()
	if err != nil {
		t.Fatalf("NewGetPublicKey: %v", err)
	}
	msg.SetId(42)
	raw, err := shmevent.Encode(msg, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	respJSON, err := DecodeEvent(raw)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	var ev e2edata.Event
	if err := json.Unmarshal([]byte(respJSON), &ev); err != nil {
		t.Fatalf("parse DecodeEvent response %q: %v", respJSON, err)
	}
	if ev.Op != "get_public_key" {
		t.Fatalf("Op = %q, want get_public_key", ev.Op)
	}
	if ev.ID != 42 {
		t.Fatalf("ID = %d, want 42", ev.ID)
	}
}

// TestEncodeDecodeTriggerEventRoundTrip exercises the full
// EncodeEvent -> bytes -> DecodeEvent -> JSON -> TriggerEvent loop against
// a real local leader -- the same shape a scan-and-confirm flow drives:
// one device (or form) encodes an event into bytes fit for a DataMatrix
// code, another decodes those bytes back to JSON to show a confirmation
// prompt, then submits it for real.
func TestEncodeDecodeTriggerEventRoundTrip(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	raw, err := EncodeEvent(`{"event":"set_key","id":100,"fields":{"value":"hello"}}`)
	if err != nil {
		t.Fatalf("EncodeEvent: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("EncodeEvent returned no bytes")
	}

	decodedJSON, err := DecodeEvent(raw)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	var decoded e2edata.Event
	if err := json.Unmarshal([]byte(decodedJSON), &decoded); err != nil {
		t.Fatalf("parse DecodeEvent response %q: %v", decodedJSON, err)
	}
	if decoded.Op != "set_key" {
		t.Fatalf("Op = %q, want set_key", decoded.Op)
	}
	if decoded.ID != 100 {
		t.Fatalf("ID = %d, want 100", decoded.ID)
	}
	if decoded.Fields["value"] != "hello" {
		t.Fatalf("Fields[value] = %q, want hello", decoded.Fields["value"])
	}

	respJSON, err := TriggerEvent(decodedJSON)
	if err != nil {
		t.Fatalf("TriggerEvent: %v", err)
	}
	var resp e2edata.Event
	if err := json.Unmarshal([]byte(respJSON), &resp); err != nil {
		t.Fatalf("parse TriggerEvent response %q: %v", respJSON, err)
	}
	if resp.Op == "error" {
		t.Fatalf("TriggerEvent returned an error event: fields=%v", resp.Fields)
	}
}
