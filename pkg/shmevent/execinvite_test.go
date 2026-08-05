package shmevent

import (
	"bytes"
	"crypto/ed25519"
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

func TestExecTicketEventNameRoundTrip(t *testing.T) {
	name := EventName(EventExecTicket)
	if name != "exec_ticket" {
		t.Fatalf("got name %q, want %q", name, "exec_ticket")
	}
	got, ok := EventFromName(name)
	if !ok || got != EventExecTicket {
		t.Fatalf("round trip through name %q got %d ok=%v, want %d", name, got, ok, EventExecTicket)
	}
	if !RequiresSignature(EventExecTicket) {
		t.Fatalf("EventExecTicket unexpectedly does not require a signature")
	}
}

func TestExecTicketPayloadRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	const addr = "/ip4/127.0.0.1/tcp/4001/p2p/abc"

	payload, err := EncodeExecTicketPayload(addr, token)
	if err != nil {
		t.Fatalf("EncodeExecTicketPayload: %v", err)
	}
	// Byte-identical to EncodeExecInviteRedeemRequest -- see that
	// function's doc comment for why this dual use is deliberate.
	want, err := EncodeExecInviteRedeemRequest(addr, token)
	if err != nil {
		t.Fatalf("EncodeExecInviteRedeemRequest: %v", err)
	}
	if !bytes.Equal(payload, want) {
		t.Fatalf("EncodeExecTicketPayload diverged from EncodeExecInviteRedeemRequest: got %x, want %x", payload, want)
	}

	gotAddr, gotToken, err := DecodeExecTicketPayload(payload)
	if err != nil {
		t.Fatalf("DecodeExecTicketPayload: %v", err)
	}
	if gotAddr != addr {
		t.Fatalf("got addr %q, want %q", gotAddr, addr)
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}
}

// TestExecTicketSignVerifyRoundTrip exercises the actual security property
// EventExecTicket exists for: a ticket signed by one key verifies against
// that key's matching public key, fails against a different one, and any
// tampering with the encoded bytes (as would happen if a scanned
// DataMatrix payload were altered or a different sourceAddr/token were
// substituted in) is caught by Verify -- entirely via Encode/Decode/Verify
// standalone, no shmring session involved, confirming the "no daemon
// changes needed" premise this event type was designed around.
func TestExecTicketSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	token := randomExecInviteToken(t)
	value, err := EncodeExecTicketPayload("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("EncodeExecTicketPayload: %v", err)
	}

	wire, err := Encode(Msg{EventType: EventExecTicket, Value: value}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	m, crc, sig, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if m.EventType != EventExecTicket {
		t.Fatalf("got event type %d, want %d", m.EventType, EventExecTicket)
	}
	gotAddr, gotToken, err := DecodeExecTicketPayload(m.Value)
	if err != nil {
		t.Fatalf("DecodeExecTicketPayload: %v", err)
	}
	if gotAddr != "/ip4/127.0.0.1/tcp/4001/p2p/abc" || !bytes.Equal(gotToken, token) {
		t.Fatalf("decoded payload mismatch: addr=%q token=%x", gotAddr, gotToken)
	}

	if err := Verify(pub, m, crc, sig); err != nil {
		t.Fatalf("Verify with correct key: %v", err)
	}

	otherPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(otherPub, m, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded against the wrong public key")
	}

	// A ticket with a substituted token (e.g. an attacker splicing a
	// different token into an otherwise-genuine ticket) must fail
	// verification against the original signature -- same technique
	// TestSignVerifyTamperDetection uses elsewhere in this package.
	tampered := m
	tamperedToken := append([]byte(nil), token...)
	tamperedToken[0] ^= 0xFF
	tamperedValue, err := EncodeExecTicketPayload("/ip4/127.0.0.1/tcp/4001/p2p/abc", tamperedToken)
	if err != nil {
		t.Fatalf("EncodeExecTicketPayload: %v", err)
	}
	tampered.Value = tamperedValue
	if err := Verify(pub, tampered, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded after tampering with the ticket's token")
	}
}
