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
	payload := EncodeExecInviteRecord("cmd-1", `{"a":1}`, 0)
	gotCommandID, gotInputs, gotExpiresAt, err := DecodeExecInviteRecord(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteRecord: %v", err)
	}
	if gotCommandID != "cmd-1" || gotInputs != `{"a":1}` || gotExpiresAt != 0 {
		t.Fatalf("got commandID=%q inputs=%q expiresAt=%d, want commandID=%q inputs=%q expiresAt=0", gotCommandID, gotInputs, gotExpiresAt, "cmd-1", `{"a":1}`)
	}

	// Empty inputsJSON must round-trip too, since SubmitCommand-style
	// callers may omit inputs entirely.
	payload = EncodeExecInviteRecord("cmd-2", "", 0)
	gotCommandID, gotInputs, gotExpiresAt, err = DecodeExecInviteRecord(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteRecord (empty inputs): %v", err)
	}
	if gotCommandID != "cmd-2" || gotInputs != "" || gotExpiresAt != 0 {
		t.Fatalf("got commandID=%q inputs=%q expiresAt=%d, want commandID=%q inputs=%q expiresAt=0", gotCommandID, gotInputs, gotExpiresAt, "cmd-2", "")
	}

	if _, _, _, err := DecodeExecInviteRecord(nil); err == nil {
		t.Fatal("DecodeExecInviteRecord unexpectedly accepted an empty payload")
	}
	if _, _, _, err := DecodeExecInviteRecord([]byte{0, 5}); err == nil {
		t.Fatal("DecodeExecInviteRecord unexpectedly accepted a truncated payload")
	}
}

// TestExecInviteRecordTTLRoundTrip exercises the versioned (nonzero
// expiresAtUnix) payload shape: EncodeExecInviteRecord's v1/v2 split must be
// transparent to the caller, and a v1 (no-TTL) record must stay byte-for-byte
// identical to what it was before TTLs existed -- an already-replicated
// pre-TTL record must still decode as expiresAtUnix==0.
func TestExecInviteRecordTTLRoundTrip(t *testing.T) {
	const wantExpiresAt = uint64(1234567890)
	payload := EncodeExecInviteRecord("cmd-1", `{"a":1}`, wantExpiresAt)
	gotCommandID, gotInputs, gotExpiresAt, err := DecodeExecInviteRecord(payload)
	if err != nil {
		t.Fatalf("DecodeExecInviteRecord: %v", err)
	}
	if gotCommandID != "cmd-1" || gotInputs != `{"a":1}` || gotExpiresAt != wantExpiresAt {
		t.Fatalf("got commandID=%q inputs=%q expiresAt=%d, want commandID=%q inputs=%q expiresAt=%d", gotCommandID, gotInputs, gotExpiresAt, "cmd-1", `{"a":1}`, wantExpiresAt)
	}

	noTTL := EncodeExecInviteRecord("cmd-1", `{"a":1}`, 0)
	if len(noTTL) >= len(payload) {
		t.Fatalf("expiresAtUnix=0 payload (%d bytes) should be shorter than a TTL-carrying one (%d bytes)", len(noTTL), len(payload))
	}
	// The exact pre-TTL v1 shape: 2-byte commandID length, commandID,
	// inputsJSON -- see EncodeExecInviteRecord's own doc comment.
	wantV1 := []byte{0, byte(len("cmd-1"))}
	wantV1 = append(wantV1, "cmd-1"...)
	wantV1 = append(wantV1, `{"a":1}`...)
	if !bytes.Equal(noTTL, wantV1) {
		t.Fatalf("expiresAtUnix=0 payload %x diverged from the original v1 encoding %x", noTTL, wantV1)
	}
}

// The old wire format's EncodeExecInviteCreatePayload/
// DecodeExecInviteCreatePayload (a hand-packed token+commandID+inputsJSON
// blob) is gone -- an execInviteCreate Msg carries those as separate typed
// capnp fields (NewExecInviteCreate, Event_execInviteCreate's Token/
// CommandId/InputsJson accessors) instead. NewExecInviteCreate keeps the
// same wrong-size-token rejection the old encoder had.
func TestExecInviteCreatePayloadRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	m, err := NewExecInviteCreate(token, "cmd-1", `{"a":1}`, 3600)
	if err != nil {
		t.Fatalf("NewExecInviteCreate: %v", err)
	}
	gotToken, err := m.ExecInviteCreate().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	gotCommandID, err := m.ExecInviteCreate().CommandId()
	if err != nil {
		t.Fatalf("CommandId: %v", err)
	}
	gotInputs, err := m.ExecInviteCreate().InputsJson()
	if err != nil {
		t.Fatalf("InputsJson: %v", err)
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}
	if gotCommandID != "cmd-1" || gotInputs != `{"a":1}` {
		t.Fatalf("got commandID=%q inputs=%q, want commandID=%q inputs=%q", gotCommandID, gotInputs, "cmd-1", `{"a":1}`)
	}
	if got := m.ExecInviteCreate().TtlSeconds(); got != 3600 {
		t.Fatalf("got ttlSeconds=%d, want 3600", got)
	}

	// ttlSeconds==0 (the default, no expiry) must round-trip too.
	mNoTTL, err := NewExecInviteCreate(token, "cmd-1", `{"a":1}`, 0)
	if err != nil {
		t.Fatalf("NewExecInviteCreate (no TTL): %v", err)
	}
	if got := mNoTTL.ExecInviteCreate().TtlSeconds(); got != 0 {
		t.Fatalf("got ttlSeconds=%d, want 0", got)
	}

	if _, err := NewExecInviteCreate([]byte("too-short"), "cmd-1", "", 0); err == nil {
		t.Fatal("NewExecInviteCreate unexpectedly accepted a wrong-size token")
	}
}

// The old wire format's EncodeExecInviteRevokePayload/
// DecodeExecInviteRevokePayload is gone the same way -- an
// execInviteRevoke Msg carries token as its own typed field
// (NewExecInviteRevoke, Event_execInviteRevoke's Token accessor).
func TestExecInviteRevokePayloadRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	m, err := NewExecInviteRevoke(token)
	if err != nil {
		t.Fatalf("NewExecInviteRevoke: %v", err)
	}
	got, err := m.ExecInviteRevoke().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if !bytes.Equal(got, token) {
		t.Fatalf("got token %x, want %x", got, token)
	}
}

// The old wire format's EncodeExecInviteRedeemRequest/
// DecodeExecInviteRedeemRequest is gone -- an execInviteRedeem Msg (built
// via NewExecInviteRedeem, the local-request shape) carries sourceAddr and
// token as separate typed fields instead, with the same wrong-size-token
// rejection.
func TestExecInviteRedeemRequestRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	m, err := NewExecInviteRedeem("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("NewExecInviteRedeem: %v", err)
	}
	gotAddr, err := m.ExecInviteRedeem().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	gotToken, err := m.ExecInviteRedeem().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if gotAddr != "/ip4/127.0.0.1/tcp/4001/p2p/abc" {
		t.Fatalf("got addr %q, want %q", gotAddr, "/ip4/127.0.0.1/tcp/4001/p2p/abc")
	}
	if !bytes.Equal(gotToken, token) {
		t.Fatalf("got token %x, want %x", gotToken, token)
	}

	if _, err := NewExecInviteRedeem("addr", []byte("too-short")); err == nil {
		t.Fatal("NewExecInviteRedeem unexpectedly accepted a wrong-size token")
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

// The old generic EventLifecycleWrite envelope this test used alongside
// EventExecInviteRedeem is gone -- every former (kind,action) pair the
// envelope carried is now its own top-level variant (see this package's
// migration notes); execInviteCreate is execInviteRedeem's own create-side
// counterpart, so it stands in here.
func TestExecInviteEventNameRoundTrip(t *testing.T) {
	for _, w := range []Event_Which{Event_Which_execInviteCreate, Event_Which_execInviteRedeem} {
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

func TestExecTicketEventNameRoundTrip(t *testing.T) {
	name := EventName(Event_Which_execTicket)
	if name != "exec_ticket" {
		t.Fatalf("got name %q, want %q", name, "exec_ticket")
	}
	got, ok := EventFromName(name)
	if !ok || got != Event_Which_execTicket {
		t.Fatalf("round trip through name %q got %v ok=%v, want %v", name, got, ok, Event_Which_execTicket)
	}
	if !RequiresSignature(Event_Which_execTicket) {
		t.Fatalf("execTicket unexpectedly does not require a signature")
	}
}

// The old wire format's EncodeExecTicketPayload/DecodeExecTicketPayload
// (byte-identical to EncodeExecInviteRedeemRequest, deliberately, since a
// ticket is a pre-signed redeem request) is gone -- execTicket and
// execInviteRedeem are now separate top-level capnp variants, so there's no
// shared byte encoding to compare. What this test pins instead: both
// variants still carry the same sourceAddr/token pair with identical
// semantics, built independently.
func TestExecTicketPayloadRoundTrip(t *testing.T) {
	token := randomExecInviteToken(t)
	const addr = "/ip4/127.0.0.1/tcp/4001/p2p/abc"

	ticket, err := NewExecTicket(addr, token)
	if err != nil {
		t.Fatalf("NewExecTicket: %v", err)
	}
	redeem, err := NewExecInviteRedeem(addr, token)
	if err != nil {
		t.Fatalf("NewExecInviteRedeem: %v", err)
	}

	gotAddr, err := ticket.ExecTicket().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	gotToken, err := ticket.ExecTicket().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	wantAddr, err := redeem.ExecInviteRedeem().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	wantToken, err := redeem.ExecInviteRedeem().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if gotAddr != wantAddr {
		t.Fatalf("execTicket addr %q diverged from execInviteRedeem addr %q", gotAddr, wantAddr)
	}
	if !bytes.Equal(gotToken, wantToken) {
		t.Fatalf("execTicket token %x diverged from execInviteRedeem token %x", gotToken, wantToken)
	}
	if gotAddr != addr || !bytes.Equal(gotToken, token) {
		t.Fatalf("got addr=%q token=%x, want addr=%q token=%x", gotAddr, gotToken, addr, token)
	}
}

// TestExecTicketSignVerifyRoundTrip exercises the actual security property
// EventExecTicket exists for: a ticket signed by one key verifies against
// that key's matching public key, fails against a different one, and any
// tampering with the encoded fields (as would happen if a scanned
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
	m, err := NewExecTicket("/ip4/127.0.0.1/tcp/4001/p2p/abc", token)
	if err != nil {
		t.Fatalf("NewExecTicket: %v", err)
	}

	wire, err := Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, crc, sig, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Which() != Event_Which_execTicket {
		t.Fatalf("got Which %v, want %v", decoded.Which(), Event_Which_execTicket)
	}
	gotAddr, err := decoded.ExecTicket().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	gotToken, err := decoded.ExecTicket().Token()
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

	// A ticket with a substituted token (e.g. an attacker splicing a
	// different token into an otherwise-genuine ticket) must fail
	// verification against the original signature -- same technique
	// TestSignVerifyTamperDetection uses elsewhere in this package: decoded
	// is a capnp struct sharing the underlying segment, so mutating its
	// field in place stands in for the old flat struct's field-copy tamper.
	tamperedToken := append([]byte(nil), token...)
	tamperedToken[0] ^= 0xFF
	if err := decoded.ExecTicket().SetToken(tamperedToken); err != nil {
		t.Fatalf("SetToken: %v", err)
	}
	if err := Verify(pub, decoded, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded after tampering with the ticket's token")
	}
}
