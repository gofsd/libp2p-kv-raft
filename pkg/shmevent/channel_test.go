package shmevent

import (
	"bytes"
	"testing"
)

func TestChannelSendPayloadRoundTrip(t *testing.T) {
	payload, err := EncodeChannelSendPayload("chan-1", []byte("hello"))
	if err != nil {
		t.Fatalf("EncodeChannelSendPayload: %v", err)
	}
	gotID, gotChunk, err := DecodeChannelSendPayload(payload)
	if err != nil {
		t.Fatalf("DecodeChannelSendPayload: %v", err)
	}
	if string(gotID) != "chan-1" {
		t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
	}
	if !bytes.Equal(gotChunk, []byte("hello")) {
		t.Fatalf("got chunk %q, want %q", gotChunk, "hello")
	}
}

func TestChannelAcceptRoundTrip(t *testing.T) {
	payload, err := EncodeChannelAccept("chan-1", "12D3KooWtest")
	if err != nil {
		t.Fatalf("EncodeChannelAccept: %v", err)
	}
	gotID, gotPeer, err := DecodeChannelAccept(payload)
	if err != nil {
		t.Fatalf("DecodeChannelAccept: %v", err)
	}
	if string(gotID) != "chan-1" {
		t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
	}
	if string(gotPeer) != "12D3KooWtest" {
		t.Fatalf("got remotePeerID %q, want %q", gotPeer, "12D3KooWtest")
	}
}

func TestChannelPollResponseRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		status byte
		chunk  []byte
	}{
		{"no data", ChannelPollNoData, nil},
		{"chunk", ChannelPollChunk, []byte("some bytes")},
		{"closed", ChannelPollClosed, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload := EncodeChannelPollResponse(c.status, c.chunk)
			gotStatus, gotChunk, err := DecodeChannelPollResponse(payload)
			if err != nil {
				t.Fatalf("DecodeChannelPollResponse: %v", err)
			}
			if gotStatus != c.status {
				t.Fatalf("got status %d, want %d", gotStatus, c.status)
			}
			if !bytes.Equal(gotChunk, c.chunk) {
				t.Fatalf("got chunk %q, want %q", gotChunk, c.chunk)
			}
		})
	}

	if _, _, err := DecodeChannelPollResponse(nil); err == nil {
		t.Fatal("DecodeChannelPollResponse unexpectedly accepted an empty payload")
	}
}

func TestChannelEventNameRoundTrip(t *testing.T) {
	events := []uint8{EventChannelOpen, EventChannelSend, EventChannelPoll, EventChannelListen, EventChannelClose, EventChannelCloseWrite}
	for _, e := range events {
		name := EventName(e)
		got, ok := EventFromName(name)
		if !ok || got != e {
			t.Fatalf("EventFromName(%q) = %d, %v, want %d, true", name, got, ok, e)
		}
	}
}
