package shmevent

import (
	"bytes"
	"crypto/ed25519"
	"testing"
)

// The old wire format's EncodeJoinRequestCancelPayload/
// DecodeJoinRequestCancelPayload is gone -- a joinRequestCancel Msg
// carries token as its own typed field (NewJoinRequestCancel,
// Event_joinRequestCancel's Token accessor), with the same
// wrong-size-token rejection.
func TestJoinRequestCancelPayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	m, err := NewJoinRequestCancel(token)
	if err != nil {
		t.Fatalf("NewJoinRequestCancel: %v", err)
	}
	got, err := m.JoinRequestCancel().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if !bytes.Equal(got, token) {
		t.Fatalf("got token %x, want %x", got, token)
	}

	if _, err := NewJoinRequestCancel([]byte("wrong size")); err == nil {
		t.Fatal("NewJoinRequestCancel unexpectedly accepted a malformed token")
	}
}

// The old wire format's EncodeRecruitPayload/DecodeRecruitPayload is gone
// -- a recruit Msg carries ticket/suffrage as separate typed fields
// (NewRecruit, Event_recruit's Ticket/Suffrage accessors).
func TestRecruitPayloadRoundTrip(t *testing.T) {
	ticket := "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWtest#deadbeef"
	m, err := NewRecruit(ticket, SuffrageVoter)
	if err != nil {
		t.Fatalf("NewRecruit: %v", err)
	}
	gotTicket, err := m.Recruit().Ticket()
	if err != nil {
		t.Fatalf("Ticket: %v", err)
	}
	if gotTicket != ticket {
		t.Fatalf("got ticket %q, want %q", gotTicket, ticket)
	}
	if gotSuffrage := m.Recruit().Suffrage(); gotSuffrage != SuffrageVoter {
		t.Fatalf("got suffrage %d, want %d", gotSuffrage, SuffrageVoter)
	}
}

func TestJoinRequestEventNameRoundTrip(t *testing.T) {
	for _, w := range []Event_Which{Event_Which_joinRequestCreate, Event_Which_joinRequestCancel, Event_Which_recruit} {
		name := EventName(w)
		got, ok := EventFromName(name)
		if !ok || got != w {
			t.Fatalf("event %v: round trip through name %q got %v ok=%v", w, name, got, ok)
		}
		if !RequiresSignature(w) {
			t.Fatalf("event %v (%s) unexpectedly does not require a signature", w, name)
		}
	}
}

func TestJoinRequestTicketEventNameRoundTrip(t *testing.T) {
	name := EventName(Event_Which_joinRequestTicket)
	if name != "join_request_ticket" {
		t.Fatalf("got name %q, want %q", name, "join_request_ticket")
	}
	got, ok := EventFromName(name)
	if !ok || got != Event_Which_joinRequestTicket {
		t.Fatalf("round trip through name %q got %v ok=%v, want %v", name, got, ok, Event_Which_joinRequestTicket)
	}
	if !RequiresSignature(Event_Which_joinRequestTicket) {
		t.Fatalf("joinRequestTicket unexpectedly does not require a signature")
	}
}

// TestJoinRequestTicketSignVerifyRoundTrip mirrors
// TestJoinTicketSignVerifyRoundTrip/TestExecTicketSignVerifyRoundTrip:
// joinRequestTicket carries the same (sourceAddr, token) shape as
// joinTicket (see that variant's doc comment in api/shmevent.capnp), so
// this only needs to confirm the sign/verify property holds under this
// event type too, not re-prove the field codec.
func TestJoinRequestTicketSignVerifyRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	token := randomToken(t)
	m, err := NewJoinRequestTicket("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("NewJoinRequestTicket: %v", err)
	}

	wire, err := Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, crc, sig, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Which() != Event_Which_joinRequestTicket {
		t.Fatalf("got Which %v, want %v", decoded.Which(), Event_Which_joinRequestTicket)
	}
	gotAddr, err := decoded.JoinRequestTicket().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	gotToken, err := decoded.JoinRequestTicket().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if gotAddr != "/ip4/127.0.0.1/tcp/4001/p2p/abc" || !bytes.Equal(gotToken, token) {
		t.Fatalf("decoded payload mismatch: addr=%q token=%x", gotAddr, gotToken)
	}

	if err := Verify(pub, decoded, crc, sig); err != nil {
		t.Fatalf("Verify with correct key: %v", err)
	}

	otherPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(otherPub, decoded, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded against the wrong public key")
	}

	// decoded is a capnp struct sharing the underlying segment, so mutating
	// its field in place stands in for the old flat struct's field-copy
	// tamper (see TestSignVerifyTamperDetection).
	tamperedToken := append([]byte(nil), token...)
	tamperedToken[0] ^= 0xFF
	if err := decoded.JoinRequestTicket().SetToken(tamperedToken); err != nil {
		t.Fatalf("SetToken: %v", err)
	}
	if err := Verify(pub, decoded, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded after tampering with the ticket's token")
	}
}
