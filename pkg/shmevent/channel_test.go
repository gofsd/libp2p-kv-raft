package shmevent

import (
	"bytes"
	"testing"
)

func TestChannelOpenRoundTrip(t *testing.T) {
	m, err := NewChannelOpen("12D3KooWtest")
	if err != nil {
		t.Fatalf("NewChannelOpen: %v", err)
	}
	if m.Which() != Event_Which_channelOpen {
		t.Fatalf("Which() = %v, want channelOpen", m.Which())
	}
	peerID, err := m.ChannelOpen().PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	if peerID != "12D3KooWtest" {
		t.Fatalf("got peerID %q, want %q", peerID, "12D3KooWtest")
	}
}

func TestChannelSendRoundTrip(t *testing.T) {
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
			m, err := NewChannelSend("chan-1", c.purpose, []byte("hello"))
			if err != nil {
				t.Fatalf("NewChannelSend: %v", err)
			}
			if m.Which() != Event_Which_channelSend {
				t.Fatalf("Which() = %v, want channelSend", m.Which())
			}
			grp := m.ChannelSend()
			gotID, err := grp.ChannelId()
			if err != nil {
				t.Fatalf("ChannelId: %v", err)
			}
			if gotID != "chan-1" {
				t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
			}
			if grp.Purpose() != c.purpose {
				t.Fatalf("got purpose %d, want %d", grp.Purpose(), c.purpose)
			}
			gotChunk, err := grp.Chunk()
			if err != nil {
				t.Fatalf("Chunk: %v", err)
			}
			if !bytes.Equal(gotChunk, []byte("hello")) {
				t.Fatalf("got chunk %q, want %q", gotChunk, "hello")
			}
		})
	}
}

func TestChannelPollRoundTrip(t *testing.T) {
	m, err := NewChannelPoll("chan-1")
	if err != nil {
		t.Fatalf("NewChannelPoll: %v", err)
	}
	grp := m.ChannelPoll()
	gotID, err := grp.ChannelId()
	if err != nil {
		t.Fatalf("ChannelId: %v", err)
	}
	if gotID != "chan-1" {
		t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
	}

	// The response reuses the same variant, filling in status/purpose/chunk.
	grp.SetStatus(ChannelPollChunk)
	grp.SetPurpose(ChannelPurposeVideo)
	if err := grp.SetChunk([]byte("some bytes")); err != nil {
		t.Fatalf("SetChunk: %v", err)
	}
	if grp.Status() != ChannelPollChunk {
		t.Fatalf("got status %d, want %d", grp.Status(), ChannelPollChunk)
	}
	if grp.Purpose() != ChannelPurposeVideo {
		t.Fatalf("got purpose %d, want %d", grp.Purpose(), ChannelPurposeVideo)
	}
	gotChunk, err := grp.Chunk()
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	if !bytes.Equal(gotChunk, []byte("some bytes")) {
		t.Fatalf("got chunk %q, want %q", gotChunk, "some bytes")
	}
}

func TestChannelListenRoundTrip(t *testing.T) {
	m, err := NewChannelListen()
	if err != nil {
		t.Fatalf("NewChannelListen: %v", err)
	}
	if m.Which() != Event_Which_channelListen {
		t.Fatalf("Which() = %v, want channelListen", m.Which())
	}

	// The response reuses the same variant, filling in channelId/remotePeerId.
	grp := m.ChannelListen()
	if err := grp.SetChannelId("chan-1"); err != nil {
		t.Fatalf("SetChannelId: %v", err)
	}
	if err := grp.SetRemotePeerId("12D3KooWtest"); err != nil {
		t.Fatalf("SetRemotePeerId: %v", err)
	}
	gotID, err := grp.ChannelId()
	if err != nil {
		t.Fatalf("ChannelId: %v", err)
	}
	if gotID != "chan-1" {
		t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
	}
	gotPeer, err := grp.RemotePeerId()
	if err != nil {
		t.Fatalf("RemotePeerId: %v", err)
	}
	if gotPeer != "12D3KooWtest" {
		t.Fatalf("got remotePeerID %q, want %q", gotPeer, "12D3KooWtest")
	}
}

func TestChannelCloseRoundTrip(t *testing.T) {
	m, err := NewChannelClose("chan-1")
	if err != nil {
		t.Fatalf("NewChannelClose: %v", err)
	}
	gotID, err := m.ChannelClose().ChannelId()
	if err != nil {
		t.Fatalf("ChannelId: %v", err)
	}
	if gotID != "chan-1" {
		t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
	}

	mw, err := NewChannelCloseWrite("chan-2")
	if err != nil {
		t.Fatalf("NewChannelCloseWrite: %v", err)
	}
	gotID, err = mw.ChannelCloseWrite().ChannelId()
	if err != nil {
		t.Fatalf("ChannelId: %v", err)
	}
	if gotID != "chan-2" {
		t.Fatalf("got channelID %q, want %q", gotID, "chan-2")
	}
}

func TestChannelDataReadyRoundTrip(t *testing.T) {
	m, err := NewChannelDataReady("chan-1")
	if err != nil {
		t.Fatalf("NewChannelDataReady: %v", err)
	}
	gotID, err := m.ChannelDataReady().ChannelId()
	if err != nil {
		t.Fatalf("ChannelId: %v", err)
	}
	if gotID != "chan-1" {
		t.Fatalf("got channelID %q, want %q", gotID, "chan-1")
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
	events := []Event_Which{
		Event_Which_channelOpen,
		Event_Which_channelSend,
		Event_Which_channelPoll,
		Event_Which_channelListen,
		Event_Which_channelClose,
		Event_Which_channelCloseWrite,
		Event_Which_channelDataReady,
	}
	for _, e := range events {
		name := EventName(e)
		got, ok := EventFromName(name)
		if !ok || got != e {
			t.Fatalf("EventFromName(%q) = %v, %v, want %v, true", name, got, ok, e)
		}
	}
}
