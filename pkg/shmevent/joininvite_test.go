package shmevent

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func randomToken(t *testing.T) []byte {
	t.Helper()
	token := make([]byte, JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

func TestJoinInviteKeyLayout(t *testing.T) {
	token := randomToken(t)
	key := JoinInviteKey(token)
	want := SystemKey(KindJoinInvite, joinInviteStatusPlaceholder, token)
	if !bytes.Equal(key, want) {
		t.Fatalf("got key %x, want %x", key, want)
	}
	if key[0] != SystemKeyPrefix || key[1] != KindJoinInvite {
		t.Fatalf("got key prefix %x kind %x, want prefix %x kind %x", key[0], key[1], SystemKeyPrefix, KindJoinInvite)
	}
}

func TestJoinInviteRecordRoundTrip(t *testing.T) {
	payload := EncodeJoinInviteRecord(SuffrageLearner)
	got, err := DecodeJoinInviteRecord(payload)
	if err != nil {
		t.Fatalf("DecodeJoinInviteRecord: %v", err)
	}
	if got != SuffrageLearner {
		t.Fatalf("got suffrage %d, want %d", got, SuffrageLearner)
	}

	if _, err := DecodeJoinInviteRecord(nil); err == nil {
		t.Fatal("DecodeJoinInviteRecord unexpectedly accepted an empty payload")
	}
	if _, err := DecodeJoinInviteRecord([]byte{1, 2}); err == nil {
		t.Fatal("DecodeJoinInviteRecord unexpectedly accepted a 2-byte payload")
	}
}

func TestJoinInviteCreatePayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	payload, err := EncodeJoinInviteCreatePayload(token, SuffrageVoter)
	if err != nil {
		t.Fatalf("EncodeJoinInviteCreatePayload: %v", err)
	}
	gotToken, gotSuffrage, err := DecodeJoinInviteCreatePayload(payload)
	if err != nil {
		t.Fatalf("DecodeJoinInviteCreatePayload: %v", err)
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}
	if gotSuffrage != SuffrageVoter {
		t.Fatalf("got suffrage %d, want %d", gotSuffrage, SuffrageVoter)
	}

	if _, err := EncodeJoinInviteCreatePayload([]byte("too-short"), SuffrageVoter); err == nil {
		t.Fatal("EncodeJoinInviteCreatePayload unexpectedly accepted a wrong-size token")
	}
	if _, _, err := DecodeJoinInviteCreatePayload([]byte("wrong size")); err == nil {
		t.Fatal("DecodeJoinInviteCreatePayload unexpectedly accepted a malformed payload")
	}
}

func TestJoinInviteRevokePayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	payload := EncodeJoinInviteRevokePayload(token)
	got, err := DecodeJoinInviteRevokePayload(payload)
	if err != nil {
		t.Fatalf("DecodeJoinInviteRevokePayload: %v", err)
	}
	if !bytes.Equal(got, token) {
		t.Fatalf("got token %x, want %x", got, token)
	}

	if _, err := DecodeJoinInviteRevokePayload([]byte("wrong size")); err == nil {
		t.Fatal("DecodeJoinInviteRevokePayload unexpectedly accepted a malformed payload")
	}
}

func TestJoinInviteKindNameRoundTrip(t *testing.T) {
	if got := KindName(KindJoinInvite); got != "join-invite" {
		t.Fatalf("got %q, want %q", got, "join-invite")
	}
	k, ok := KindFromName("join-invite")
	if !ok || k != KindJoinInvite {
		t.Fatalf("got k=%d ok=%v, want k=%d ok=true", k, ok, KindJoinInvite)
	}
}

func TestJoinInviteEventNameRoundTrip(t *testing.T) {
	for _, e := range []uint8{EventLifecycleWrite} {
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

func TestJoinTicketEventNameRoundTrip(t *testing.T) {
	name := EventName(EventJoinTicket)
	if name != "join_ticket" {
		t.Fatalf("got name %q, want %q", name, "join_ticket")
	}
	got, ok := EventFromName(name)
	if !ok || got != EventJoinTicket {
		t.Fatalf("round trip through name %q got %d ok=%v, want %d", name, got, ok, EventJoinTicket)
	}
	if !RequiresSignature(EventJoinTicket) {
		t.Fatalf("EventJoinTicket unexpectedly does not require a signature")
	}
}

func TestJoinTicketPayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	const addr = "/ip4/127.0.0.1/tcp/4001/p2p/abc"

	payload, err := EncodeJoinTicketPayload(addr, token)
	if err != nil {
		t.Fatalf("EncodeJoinTicketPayload: %v", err)
	}
	gotAddr, gotToken, err := DecodeJoinTicketPayload(payload)
	if err != nil {
		t.Fatalf("DecodeJoinTicketPayload: %v", err)
	}
	if gotAddr != addr {
		t.Fatalf("got addr %q, want %q", gotAddr, addr)
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}

	if _, err := EncodeJoinTicketPayload(addr, []byte("too-short")); err == nil {
		t.Fatal("EncodeJoinTicketPayload unexpectedly accepted a wrong-size token")
	}
	if _, _, err := DecodeJoinTicketPayload([]byte("short")); err == nil {
		t.Fatal("DecodeJoinTicketPayload unexpectedly accepted a malformed payload")
	}
}

// TestJoinTicketSignVerifyRoundTrip mirrors
// TestExecTicketSignVerifyRoundTrip: a ticket signed by one key verifies
// against that key's matching public key, fails against a different one,
// and a substituted token is caught by Verify against the original
// signature -- entirely via Encode/Decode/Verify standalone, no shmring
// session involved.
func TestJoinTicketSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	token := randomToken(t)
	value, err := EncodeJoinTicketPayload("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("EncodeJoinTicketPayload: %v", err)
	}

	wire, err := Encode(Msg{EventType: EventJoinTicket, Value: value}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	m, crc, sig, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if m.EventType != EventJoinTicket {
		t.Fatalf("got event type %d, want %d", m.EventType, EventJoinTicket)
	}
	gotAddr, gotToken, err := DecodeJoinTicketPayload(m.Value)
	if err != nil {
		t.Fatalf("DecodeJoinTicketPayload: %v", err)
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

	tampered := m
	tamperedToken := append([]byte(nil), token...)
	tamperedToken[0] ^= 0xFF
	tamperedValue, err := EncodeJoinTicketPayload("/ip4/127.0.0.1/tcp/4001/p2p/abc", tamperedToken)
	if err != nil {
		t.Fatalf("EncodeJoinTicketPayload: %v", err)
	}
	tampered.Value = tamperedValue
	if err := Verify(pub, tampered, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded after tampering with the ticket's token")
	}
}
