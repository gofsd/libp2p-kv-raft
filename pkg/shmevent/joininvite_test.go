package shmevent

import (
	"bytes"
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
	for _, e := range []uint8{EventJoinInviteCreate, EventJoinInviteRevoke} {
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
