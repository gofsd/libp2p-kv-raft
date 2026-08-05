package shmevent

import (
	"bytes"
	"crypto/ed25519"
	"testing"
)

func TestJoinRequestCancelPayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	payload := EncodeJoinRequestCancelPayload(token)
	got, err := DecodeJoinRequestCancelPayload(payload)
	if err != nil {
		t.Fatalf("DecodeJoinRequestCancelPayload: %v", err)
	}
	if !bytes.Equal(got, token) {
		t.Fatalf("got token %x, want %x", got, token)
	}

	if _, err := DecodeJoinRequestCancelPayload([]byte("wrong size")); err == nil {
		t.Fatal("DecodeJoinRequestCancelPayload unexpectedly accepted a malformed payload")
	}
}

func TestRecruitPayloadRoundTrip(t *testing.T) {
	ticket := "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWtest#deadbeef"
	payload := EncodeRecruitPayload(ticket, SuffrageVoter)
	gotTicket, gotSuffrage, err := DecodeRecruitPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRecruitPayload: %v", err)
	}
	if gotTicket != ticket {
		t.Fatalf("got ticket %q, want %q", gotTicket, ticket)
	}
	if gotSuffrage != SuffrageVoter {
		t.Fatalf("got suffrage %d, want %d", gotSuffrage, SuffrageVoter)
	}

	if _, _, err := DecodeRecruitPayload(nil); err == nil {
		t.Fatal("DecodeRecruitPayload unexpectedly accepted an empty payload")
	}
}

func TestJoinRequestEventNameRoundTrip(t *testing.T) {
	for _, e := range []uint8{EventJoinRequestCreate, EventJoinRequestCancel, EventRecruit} {
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

func TestJoinRequestTicketEventNameRoundTrip(t *testing.T) {
	name := EventName(EventJoinRequestTicket)
	if name != "join_request_ticket" {
		t.Fatalf("got name %q, want %q", name, "join_request_ticket")
	}
	got, ok := EventFromName(name)
	if !ok || got != EventJoinRequestTicket {
		t.Fatalf("round trip through name %q got %d ok=%v, want %d", name, got, ok, EventJoinRequestTicket)
	}
	if !RequiresSignature(EventJoinRequestTicket) {
		t.Fatalf("EventJoinRequestTicket unexpectedly does not require a signature")
	}
}

// TestJoinRequestTicketSignVerifyRoundTrip mirrors
// TestJoinTicketSignVerifyRoundTrip/TestExecTicketSignVerifyRoundTrip:
// EventJoinRequestTicket reuses EncodeJoinTicketPayload/
// DecodeJoinTicketPayload as-is (same (addr, token) shape, same token
// size -- see EventJoinRequestTicket's doc comment), so this only needs
// to confirm the sign/verify property holds under this event type too,
// not re-prove the payload codec.
func TestJoinRequestTicketSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	token := randomToken(t)
	value, err := EncodeJoinTicketPayload("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("EncodeJoinTicketPayload: %v", err)
	}

	wire, err := Encode(Msg{EventType: EventJoinRequestTicket, Value: value}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	m, crc, sig, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if m.EventType != EventJoinRequestTicket {
		t.Fatalf("got event type %d, want %d", m.EventType, EventJoinRequestTicket)
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
