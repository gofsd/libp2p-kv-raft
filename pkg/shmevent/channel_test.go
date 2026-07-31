package shmevent

import (
	"bytes"
	"testing"
)

func TestChannelSendPayloadRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		purpose byte
	}{
		{"data", ChannelPurposeData},
		{"control", ChannelPurposeControl},
		{"video", ChannelPurposeVideo},
		{"custom", 200},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload, err := EncodeChannelSendPayload("chan-1", c.purpose, []byte("hello"))
			if err != nil {
				t.Fatalf("EncodeChannelSendPayload: %v", err)
			}
			gotID, gotPurpose, gotChunk, err := DecodeChannelSendPayload(payload)
			if err != nil {
				t.Fatalf("DecodeChannelSendPayload: %v", err)
			}
			if string(gotID) != "chan-1" {
				t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
			}
			if gotPurpose != c.purpose {
				t.Fatalf("got purpose %d, want %d", gotPurpose, c.purpose)
			}
			if !bytes.Equal(gotChunk, []byte("hello")) {
				t.Fatalf("got chunk %q, want %q", gotChunk, "hello")
			}
		})
	}

	if _, _, _, err := DecodeChannelSendPayload(nil); err == nil {
		t.Fatal("DecodeChannelSendPayload unexpectedly accepted an empty payload")
	}
}

func TestChannelWireChunkRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		purpose byte
		chunk   []byte
	}{
		{"data", ChannelPurposeData, []byte("hello")},
		{"control", ChannelPurposeControl, []byte("ping")},
		{"video", ChannelPurposeVideo, []byte{0x00, 0x01, 0x02}},
		{"empty chunk", ChannelPurposeData, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload := EncodeChannelWireChunk(c.purpose, c.chunk)
			gotPurpose, gotChunk, err := DecodeChannelWireChunk(payload)
			if err != nil {
				t.Fatalf("DecodeChannelWireChunk: %v", err)
			}
			if gotPurpose != c.purpose {
				t.Fatalf("got purpose %d, want %d", gotPurpose, c.purpose)
			}
			if !bytes.Equal(gotChunk, c.chunk) {
				t.Fatalf("got chunk %q, want %q", gotChunk, c.chunk)
			}
		})
	}

	if _, _, err := DecodeChannelWireChunk(nil); err == nil {
		t.Fatal("DecodeChannelWireChunk unexpectedly accepted an empty payload")
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
		name    string
		status  byte
		purpose byte
		chunk   []byte
	}{
		{"no data", ChannelPollNoData, ChannelPurposeData, nil},
		{"chunk", ChannelPollChunk, ChannelPurposeData, []byte("some bytes")},
		{"chunk control", ChannelPollChunk, ChannelPurposeControl, []byte("ping")},
		{"chunk video", ChannelPollChunk, ChannelPurposeVideo, []byte{0xDE, 0xAD}},
		{"closed", ChannelPollClosed, ChannelPurposeData, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			payload := EncodeChannelPollResponse(c.status, c.purpose, c.chunk)
			gotStatus, gotPurpose, gotChunk, err := DecodeChannelPollResponse(payload)
			if err != nil {
				t.Fatalf("DecodeChannelPollResponse: %v", err)
			}
			if gotStatus != c.status {
				t.Fatalf("got status %d, want %d", gotStatus, c.status)
			}
			if gotPurpose != c.purpose {
				t.Fatalf("got purpose %d, want %d", gotPurpose, c.purpose)
			}
			if !bytes.Equal(gotChunk, c.chunk) {
				t.Fatalf("got chunk %q, want %q", gotChunk, c.chunk)
			}
		})
	}

	if _, _, _, err := DecodeChannelPollResponse(nil); err == nil {
		t.Fatal("DecodeChannelPollResponse unexpectedly accepted an empty payload")
	}
	if _, _, _, err := DecodeChannelPollResponse([]byte{ChannelPollChunk}); err == nil {
		t.Fatal("DecodeChannelPollResponse unexpectedly accepted a status-only payload")
	}
}

func TestChannelPurposeNameRoundTrip(t *testing.T) {
	named := []byte{ChannelPurposeData, ChannelPurposeControl, ChannelPurposeVideo}
	for _, p := range named {
		name := ChannelPurposeName(p)
		got, ok := ChannelPurposeFromName(name)
		if !ok || got != p {
			t.Fatalf("ChannelPurposeFromName(%q) = %d, %v, want %d, true", name, got, ok, p)
		}
	}

	// A numeric fallback for any purpose beyond the three named ones.
	got, ok := ChannelPurposeFromName("7")
	if !ok || got != 7 {
		t.Fatalf(`ChannelPurposeFromName("7") = %d, %v, want 7, true`, got, ok)
	}
	if name := ChannelPurposeName(7); name != "7" {
		t.Fatalf("ChannelPurposeName(7) = %q, want %q", name, "7")
	}

	// Empty name resolves to the default purpose.
	if got, ok := ChannelPurposeFromName(""); !ok || got != ChannelPurposeData {
		t.Fatalf(`ChannelPurposeFromName("") = %d, %v, want %d, true`, got, ok, ChannelPurposeData)
	}

	if _, ok := ChannelPurposeFromName("not-a-purpose"); ok {
		t.Fatal("ChannelPurposeFromName unexpectedly accepted an unknown name")
	}
	if _, ok := ChannelPurposeFromName("256"); ok {
		t.Fatal("ChannelPurposeFromName unexpectedly accepted an out-of-byte-range number")
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
