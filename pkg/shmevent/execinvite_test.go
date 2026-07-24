package shmevent

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func randomExecInviteToken(t *testing.T) []byte {
	t.Helper()
	token := make([]byte, ExecInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

func TestExecInviteKeyLayout(t *testing.T) {
	token := randomExecInviteToken(t)
	key := ExecInviteKey(token)
	want := SystemKey(KindExecInvite, execInviteStatusPlaceholder, token)
	if !bytes.Equal(key, want) {
		t.Fatalf("got key %x, want %x", key, want)
	}
	if key[0] != SystemKeyPrefix || key[1] != KindExecInvite {
		t.Fatalf("got key prefix %x kind %x, want prefix %x kind %x", key[0], key[1], SystemKeyPrefix, KindExecInvite)
	}
}

func TestExecInviteRecordRoundTrip(t *testing.T) {
	payload := EncodeExecInviteRecord("cmd-1", `{"a":1}`)
	gotCommandID, gotInputs, err := DecodeExecInviteRecord(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteRecord: %v", err)
	}
	if gotCommandID != "cmd-1" || gotInputs != `{"a":1}` {
		t.Fatalf("got commandID=%q inputs=%q, want commandID=%q inputs=%q", gotCommandID, gotInputs, "cmd-1", `{"a":1}`)
	}

	// Empty inputsJSON must round-trip too, since SubmitCommand-style
	// callers may omit inputs entirely.
	payload = EncodeExecInviteRecord("cmd-2", "")
	gotCommandID, gotInputs, err = DecodeExecInviteRecord(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteRecord (empty inputs): %v", err)
	}
	if gotCommandID != "cmd-2" || gotInputs != "" {
		t.Fatalf("got commandID=%q inputs=%q, want commandID=%q inputs=%q", gotCommandID, gotInputs, "cmd-2", "")
	}

	if _, _, err := DecodeExecInviteRecord(nil); err == nil {
		t.Fatal("DecodeExecInviteRecord unexpectedly accepted an empty payload")
	}
	if _, _, err := DecodeExecInviteRecord([]byte{0, 5}); err == nil {
		t.Fatal("DecodeExecInviteRecord unexpectedly accepted a truncated payload")
	}
}

func TestExecInviteCreatePayloadRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	payload, err := EncodeExecInviteCreatePayload(token, "cmd-1", `{"a":1}`)
	if err != nil {
		t.Fatalf("EncodeExecInviteCreatePayload: %v", err)
	}
	gotToken, gotCommandID, gotInputs, err := DecodeExecInviteCreatePayload(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteCreatePayload: %v", err)
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}
	if gotCommandID != "cmd-1" || gotInputs != `{"a":1}` {
		t.Fatalf("got commandID=%q inputs=%q, want commandID=%q inputs=%q", gotCommandID, gotInputs, "cmd-1", `{"a":1}`)
	}

	if _, err := EncodeExecInviteCreatePayload([]byte("too-short"), "cmd-1", ""); err == nil {
		t.Fatal("EncodeExecInviteCreatePayload unexpectedly accepted a wrong-size token")
	}
	if _, _, _, err := DecodeExecInviteCreatePayload([]byte("short")); err == nil {
		t.Fatal("DecodeExecInviteCreatePayload unexpectedly accepted a malformed payload")
	}
}

func TestExecInviteRevokePayloadRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	payload := EncodeExecInviteRevokePayload(token)
	got, err := DecodeExecInviteRevokePayload(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteRevokePayload: %v", err)
	}
	if !bytes.Equal(got, token) {
		t.Fatalf("got token %x, want %x", got, token)
	}

	if _, err := DecodeExecInviteRevokePayload([]byte("wrong size")); err == nil {
		t.Fatal("DecodeExecInviteRevokePayload unexpectedly accepted a malformed payload")
	}
}

func TestExecInviteRedeemRequestRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	payload, err := EncodeExecInviteRedeemRequest("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("EncodeExecInviteRedeemRequest: %v", err)
	}
	gotAddr, gotToken, err := DecodeExecInviteRedeemRequest(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteRedeemRequest: %v", err)
	}
	if gotAddr != "/ip4/127.0.0.1/tcp/4001/p2p/abc" {
		t.Fatalf("got addr %q, want %q", gotAddr, "/ip4/127.0.0.1/tcp/4001/p2p/abc")
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}

	if _, err := EncodeExecInviteRedeemRequest("addr", []byte("too-short")); err == nil {
		t.Fatal("EncodeExecInviteRedeemRequest unexpectedly accepted a wrong-size token")
	}
	if _, _, err := DecodeExecInviteRedeemRequest([]byte("short")); err == nil {
		t.Fatal("DecodeExecInviteRedeemRequest unexpectedly accepted a malformed payload")
	}
}

func TestExecInviteKindNameRoundTrip(t *testing.T) {
	if got := KindName(KindExecInvite); got != "exec-invite" {
		t.Fatalf("got %q, want %q", got, "exec-invite")
	}
	k, ok := KindFromName("exec-invite")
	if !ok || k != KindExecInvite {
		t.Fatalf("got k=%d ok=%v, want k=%d ok=true", k, ok, KindExecInvite)
	}
}

func TestExecInviteEventNameRoundTrip(t *testing.T) {
	for _, e := range []uint8{EventExecInviteCreate, EventExecInviteRevoke, EventExecInviteRedeem} {
		name := EventName(e)
		got, ok := EventFromName(name)
		if !ok || got != e {
			t.Fatalf("event %d: round trip through name %q got %d ok=%v", e, name, got, ok)
		}
		if !RequiresSignature(e) {
			t.Fatalf("event %d (%s) unexpectedly does not require a signature", e, name)
		}
	}
}
