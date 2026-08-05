package shmevent

import (
	"bytes"
	"testing"
)

// A Command payload gained a third field without a migration, by versioning
// itself with an impossible v1 name length. The property that makes that safe
// -- and that these tests exist to pin -- is that a command with no spec is
// still written as v1, byte for byte, so every reader that predates the field
// (including web-app's Rust decoder) keeps working on the records it already
// understood.
func TestCommandPayloadStaysV1WithoutASpec(t *testing.T) {
	v1, err := EncodeCommandPayload("Inspect", []byte("peer1"))
	if err != nil {
		t.Fatalf("EncodeCommandPayload: %v", err)
	}
	for _, spec := range [][]byte{nil, {}} {
		withSpec, err := EncodeCommandPayloadWithSpec("Inspect", []byte("peer1"), spec)
		if err != nil {
			t.Fatalf("EncodeCommandPayloadWithSpec: %v", err)
		}
		if !bytes.Equal(v1, withSpec) {
			t.Fatalf("spec-less encode = %x, want the v1 encoding %x", withSpec, v1)
		}
	}
}

func TestCommandPayloadRoundTrip(t *testing.T) {
	spec := []byte(`{"fields":[{"name":"serial","kind":"scan"}]}`)
	for _, tc := range []struct {
		name     string
		cmdName  string
		peerID   []byte
		spec     []byte
		wantSpec []byte
	}{
		{"v1, no spec", "Inspect", []byte("peer1"), nil, nil},
		{"v2, with spec", "Inspect", []byte("peer1"), spec, spec},
		{"v2, empty peer id", "Unbound", nil, spec, spec},
		{"v2, empty name", "", []byte("peer1"), spec, spec},
		// A spec containing the sentinel bytes must not confuse the reader:
		// the marker is only ever read from the payload's first two bytes.
		{"v2, spec containing 0xFFFF", "Inspect", []byte("peer1"), []byte{0xFF, 0xFF, 0x01}, []byte{0xFF, 0xFF, 0x01}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := EncodeCommandPayloadWithSpec(tc.cmdName, tc.peerID, tc.spec)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			name, peerID, gotSpec, err := DecodeCommandPayloadFull(payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if name != tc.cmdName {
				t.Fatalf("name = %q, want %q", name, tc.cmdName)
			}
			if !bytes.Equal(peerID, tc.peerID) && !(len(peerID) == 0 && len(tc.peerID) == 0) {
				t.Fatalf("peerID = %q, want %q", peerID, tc.peerID)
			}
			if !bytes.Equal(gotSpec, tc.wantSpec) && !(len(gotSpec) == 0 && len(tc.wantSpec) == 0) {
				t.Fatalf("spec = %q, want %q", gotSpec, tc.wantSpec)
			}

			// The two-value decoder every existing caller uses must read
			// both versions and simply not see the spec.
			name2, peerID2, err := DecodeCommandPayload(payload)
			if err != nil {
				t.Fatalf("DecodeCommandPayload: %v", err)
			}
			if name2 != name || !bytes.Equal(peerID2, peerID) {
				t.Fatalf("two-value decode disagrees: %q/%q vs %q/%q", name2, peerID2, name, peerID)
			}
		})
	}
}

func TestCommandPutPayloadRoundTrip(t *testing.T) {
	spec := []byte(`{"fields":[]}`)

	v1, err := EncodeCommandPutPayload("inspect", "Inspect", []byte("peer1"))
	if err != nil {
		t.Fatalf("EncodeCommandPutPayload: %v", err)
	}
	noSpec, err := EncodeCommandPutPayloadWithSpec("inspect", "Inspect", []byte("peer1"), nil)
	if err != nil {
		t.Fatalf("EncodeCommandPutPayloadWithSpec: %v", err)
	}
	if !bytes.Equal(v1, noSpec) {
		t.Fatalf("spec-less put encode = %x, want v1 %x", noSpec, v1)
	}

	payload, err := EncodeCommandPutPayloadWithSpec("inspect", "Inspect", []byte("peer1"), spec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	id, name, peerID, gotSpec, err := DecodeCommandPutPayloadFull(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if id != "inspect" || name != "Inspect" || string(peerID) != "peer1" || string(gotSpec) != string(spec) {
		t.Fatalf("got %q/%q/%q/%q", id, name, peerID, gotSpec)
	}

	// And the v1 payload still decodes through the full decoder.
	id, name, peerID, gotSpec, err = DecodeCommandPutPayloadFull(v1)
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if id != "inspect" || name != "Inspect" || string(peerID) != "peer1" || len(gotSpec) != 0 {
		t.Fatalf("v1 got %q/%q/%q/%q", id, name, peerID, gotSpec)
	}
}

func TestStationPayloadRoundTrip(t *testing.T) {
	attrs := []byte(`{"location":"line 2","model":"KL4"}`)
	payload, err := EncodeStationPayload("Assembly 1", attrs)
	if err != nil {
		t.Fatalf("EncodeStationPayload: %v", err)
	}
	name, gotAttrs, err := DecodeStationPayload(payload)
	if err != nil {
		t.Fatalf("DecodeStationPayload: %v", err)
	}
	if name != "Assembly 1" || string(gotAttrs) != string(attrs) {
		t.Fatalf("got %q/%q", name, gotAttrs)
	}

	putPayload, err := EncodeStationPutPayload([]byte("peer1"), "Assembly 1", attrs)
	if err != nil {
		t.Fatalf("EncodeStationPutPayload: %v", err)
	}
	peerID, name, gotAttrs, err := DecodeStationPutPayload(putPayload)
	if err != nil {
		t.Fatalf("DecodeStationPutPayload: %v", err)
	}
	if string(peerID) != "peer1" || name != "Assembly 1" || string(gotAttrs) != string(attrs) {
		t.Fatalf("got %q/%q/%q", peerID, name, gotAttrs)
	}
}

// A station key has to be distinguishable from every other catalog key, or a
// listing would return records of the wrong kind.
func TestStationKeyBoundsCoverOnlyStations(t *testing.T) {
	lo, hi := StationKeyBounds()
	key := StationKey([]byte("peer1"))
	if bytes.Compare(key, lo) < 0 || bytes.Compare(key, hi) > 0 {
		t.Fatalf("station key %x outside its own bounds [%x, %x]", key, lo, hi)
	}
	for _, other := range [][]byte{
		CommandKey([]byte("peer1")),
		GroupKey([]byte("peer1")),
	} {
		if bytes.Compare(other, lo) >= 0 && bytes.Compare(other, hi) <= 0 {
			t.Fatalf("non-station key %x falls inside the station bounds", other)
		}
	}
}

// The value ceiling is also the fixed width canonicalPayload pads to before
// CRC and signing, which makes this table part of the wire format rather
// than a mere limit: web-app/src/shmevent/mod.rs's value_size_for has to
// return the identical width for every event, or the two implementations
// compute different CRCs and signatures over identical messages and each
// rejects the other's as forged.
//
// Nothing in Go can enforce the Rust side, so this test exists to make a
// change here visible: if you add an event to a tier, the failure message
// names the file that has to be edited by hand.
func TestValueSizeTiers(t *testing.T) {
	for _, tc := range []struct {
		event uint8
		want  int
		name  string
	}{
		{EventSetKey, KVValueSize, "set_key"},
		{EventSetField, KVValueSize, "set_field"},
		{EventSet, KVValueSize, "set"},
		{EventGetField, KVValueSize, "get_field"},
		{EventTxn, KVValueSize, "txn"},
		{EventLogAppend, KVValueSize, "log_append"},
		{EventCommandPut, KVValueSize, "command_put"},
		{EventStationPut, KVValueSize, "station_put"},
		{EventChannelSend, ChannelValueSize, "channel_send"},
		{EventChannelPoll, ChannelValueSize, "channel_poll"},
		{EventPermitRequest, ValueSize, "permit_request"},
		{EventGroupPut, ValueSize, "group_put"},
		{EventCommandDelete, ValueSize, "command_delete"},
		{EventStationDelete, ValueSize, "station_delete"},
		{EventGetVersion, ValueSize, "get_version"},
	} {
		if got := valueSizeFor(tc.event); got != tc.want {
			t.Errorf("valueSizeFor(%s) = %d, want %d -- if this is intentional, "+
				"web-app/src/shmevent/mod.rs's value_size_for must be changed to match, "+
				"or cross-language CRC/signature verification breaks silently",
				tc.name, got, tc.want)
		}
	}
}
