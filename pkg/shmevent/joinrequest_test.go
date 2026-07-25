package shmevent

import (
	"bytes"
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
