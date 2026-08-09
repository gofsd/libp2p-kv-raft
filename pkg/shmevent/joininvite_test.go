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

// The old wire format's EncodeJoinInviteCreatePayload/
// DecodeJoinInviteCreatePayload (a hand-packed token+suffrage blob) is
// gone -- a joinInviteCreate Msg carries those as separate typed capnp
// fields (NewJoinInviteCreate, Event_joinInviteCreate's Token/Suffrage
// accessors) instead, with the same wrong-size-token rejection.
func TestJoinInviteCreatePayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	m, err := NewJoinInviteCreate(token, SuffrageVoter)
	if err != nil {
		t.Fatalf("NewJoinInviteCreate: %v", err)
	}
	gotToken, err := m.JoinInviteCreate().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}
	if gotSuffrage := m.JoinInviteCreate().Suffrage(); gotSuffrage != SuffrageVoter {
		t.Fatalf("got suffrage %d, want %d", gotSuffrage, SuffrageVoter)
	}

	if _, err := NewJoinInviteCreate([]byte("too-short"), SuffrageVoter); err == nil {
		t.Fatal("NewJoinInviteCreate unexpectedly accepted a wrong-size token")
	}
}

// The old wire format's EncodeJoinInviteRevokePayload/
// DecodeJoinInviteRevokePayload is gone the same way -- a
// joinInviteRevoke Msg carries token as its own typed field
// (NewJoinInviteRevoke, Event_joinInviteRevoke's Token accessor).
func TestJoinInviteRevokePayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	m, err := NewJoinInviteRevoke(token)
	if err != nil {
		t.Fatalf("NewJoinInviteRevoke: %v", err)
	}
	got, err := m.JoinInviteRevoke().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if !bytes.Equal(got, token) {
		t.Fatalf("got token %x, want %x", got, token)
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

// The old generic EventLifecycleWrite envelope this test used is gone --
// every former (kind,action) pair it carried is now its own top-level
// variant (see this package's migration notes); joinInviteCreate is the
// natural stand-in here, the create side of this same invite lifecycle.
func TestJoinInviteEventNameRoundTrip(t *testing.T) {
	for _, w := range []Event_Which{Event_Which_joinInviteCreate} {
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

func TestJoinTicketEventNameRoundTrip(t *testing.T) {
	name := EventName(Event_Which_joinTicket)
	if name != "join_ticket" {
		t.Fatalf("got name %q, want %q", name, "join_ticket")
	}
	got, ok := EventFromName(name)
	if !ok || got != Event_Which_joinTicket {
		t.Fatalf("round trip through name %q got %v ok=%v, want %v", name, got, ok, Event_Which_joinTicket)
	}
	if !RequiresSignature(Event_Which_joinTicket) {
		t.Fatalf("joinTicket unexpectedly does not require a signature")
	}
}

// The old wire format's EncodeJoinTicketPayload/DecodeJoinTicketPayload is
// gone -- a joinTicket Msg carries sourceAddr/token as separate typed
// fields (NewJoinTicket, Event_joinTicket's SourceAddr/Token accessors).
func TestJoinTicketPayloadRoundTrip(t *testing.T) {
	token := randomToken(t)
	const addr = "/ip4/127.0.0.1/tcp/4001/p2p/abc"

	m, err := NewJoinTicket(addr, token)
	if err != nil {
		t.Fatalf("NewJoinTicket: %v", err)
	}
	gotAddr, err := m.JoinTicket().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	gotToken, err := m.JoinTicket().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if gotAddr != addr {
		t.Fatalf("got addr %q, want %q", gotAddr, addr)
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
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
	m, err := NewJoinTicket("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("NewJoinTicket: %v", err)
	}

	wire, err := Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, crc, sig, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Which() != Event_Which_joinTicket {
		t.Fatalf("got Which %v, want %v", decoded.Which(), Event_Which_joinTicket)
	}
	gotAddr, err := decoded.JoinTicket().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	gotToken, err := decoded.JoinTicket().Token()
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
	if err := decoded.JoinTicket().SetToken(tamperedToken); err != nil {
		t.Fatalf("SetToken: %v", err)
	}
	if err := Verify(pub, decoded, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded after tampering with the ticket's token")
	}
}
