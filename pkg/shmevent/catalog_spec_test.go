package shmevent

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"hash/crc32"
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
			if !bytes.Equal(peerID, tc.peerID) && (len(peerID) != 0 || len(tc.peerID) != 0) {
				t.Fatalf("peerID = %q, want %q", peerID, tc.peerID)
			}
			if !bytes.Equal(gotSpec, tc.wantSpec) && (len(gotSpec) != 0 || len(tc.wantSpec) != 0) {
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

// EventCatalogPut/EventCatalogDelete's whole point is carrying every
// existing per-kind payload unchanged behind one extra leading kind byte --
// this pins that round trip for all five kinds, including that the wrapped
// bytes come back byte-identical to what the kind's own existing encoder
// produced.
func TestCatalogPayloadRoundTrip(t *testing.T) {
	groupInner, err := EncodeGroupPutPayload("g1", "infantry", true)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}
	commandInner, err := EncodeCommandPutPayload("c1", "resupply", []byte("peer1"))
	if err != nil {
		t.Fatalf("EncodeCommandPutPayload: %v", err)
	}
	stationInner, err := EncodeStationPutPayload([]byte("peer1"), "Assembly 1", []byte("attrs"))
	if err != nil {
		t.Fatalf("EncodeStationPutPayload: %v", err)
	}
	groupCommandInner, err := EncodeGroupCommandPayload([]byte("c1"), []byte("g1"))
	if err != nil {
		t.Fatalf("EncodeGroupCommandPayload: %v", err)
	}
	peerGroupInner, err := EncodePeerGroupPayload([]byte("peer1"), []byte("g1"))
	if err != nil {
		t.Fatalf("EncodePeerGroupPayload: %v", err)
	}

	for _, tc := range []struct {
		name  string
		kind  byte
		inner []byte
	}{
		{"group", KindGroup, groupInner},
		{"command", KindCommand, commandInner},
		{"station", KindStation, stationInner},
		{"group_command", KindGroupCommand, groupCommandInner},
		{"peer_group", KindPeerGroup, peerGroupInner},
		{"empty inner", KindGroup, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := EncodeCatalogPayload(tc.kind, tc.inner)
			gotKind, gotInner, err := DecodeCatalogPayload(wrapped)
			if err != nil {
				t.Fatalf("DecodeCatalogPayload: %v", err)
			}
			if gotKind != tc.kind {
				t.Fatalf("got kind %d, want %d", gotKind, tc.kind)
			}
			if !bytes.Equal(gotInner, tc.inner) {
				t.Fatalf("got inner %x, want %x", gotInner, tc.inner)
			}
		})
	}

	if _, _, err := DecodeCatalogPayload(nil); err == nil {
		t.Fatal("DecodeCatalogPayload unexpectedly accepted an empty payload")
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

// TestCanonicalWidthKeepsHistoricalWidthForSmallValues pins the property
// that makes raising a value ceiling safe *across builds*, not just across
// languages: peers do not upgrade together, so a message that would have
// fit the old ceiling has to keep hashing and signing exactly as it always
// did. Raising the KV events to KVValueSize without this made every one of
// them unverifiable to any peer still on an older build -- and, because a
// message that fails to decode gets no reply at all, it presented as a
// deployed relay silently ignoring requests rather than as any kind of
// version error.
func TestCanonicalWidthKeepsHistoricalWidthForSmallValues(t *testing.T) {
	for _, event := range []uint8{EventSetKey, EventSetField, EventSet, EventGetField, EventTxn, EventLogAppend, EventCommandPut, EventStationPut} {
		for _, valueLen := range []int{0, 1, 200, ValueSize} {
			if got := canonicalWidth(event, valueLen); got != ValueSize {
				t.Errorf("canonicalWidth(%s, %d) = %d, want %d -- a value that fits the historical width must still be padded to it, or every peer on an older build rejects the message as forged",
					EventName(event), valueLen, got, ValueSize)
			}
		}
		// Past the historical width, only new builds can be involved at all,
		// so the raised ceiling is what both ends use.
		if got := canonicalWidth(event, ValueSize+1); got != KVValueSize {
			t.Errorf("canonicalWidth(%s, %d) = %d, want %d", EventName(event), ValueSize+1, got, KVValueSize)
		}
	}

	// Channel events have only ever had one width, at every value length.
	for _, event := range []uint8{EventChannelSend, EventChannelPoll} {
		for _, valueLen := range []int{0, 200, ValueSize, ValueSize + 1, ChannelValueSize} {
			if got := canonicalWidth(event, valueLen); got != ChannelValueSize {
				t.Errorf("canonicalWidth(%s, %d) = %d, want %d", EventName(event), valueLen, got, ChannelValueSize)
			}
		}
	}
}

// TestSmallValueSignatureIsWidthStable is the end-to-end statement of the
// same property, at the level a peer actually experiences it: a real signed
// message with a small value must verify against a signature computed the
// way every build of this project has always computed it -- 512-wide
// padding -- regardless of the event's current ceiling.
func TestSmallValueSignatureIsWidthStable(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	m := Msg{EventType: EventLogAppend, Value: []byte("a small journal record"), ID: 7}
	buf, err := Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, crc, sig, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// Rebuild the historical payload by hand rather than calling
	// canonicalPayload, so this test still fails if that function starts
	// agreeing with itself about a different width.
	historical := make([]byte, 1+2+2+ValueSize+2)
	historical[0] = m.EventType
	copy(historical[5:], m.Value)
	historical[5+ValueSize] = byte(m.ID >> 8)
	historical[5+ValueSize+1] = byte(m.ID)
	wantCRC := crc32.ChecksumIEEE(historical)
	if crc != wantCRC {
		t.Fatalf("crc = %#x, want %#x (the value fits ValueSize, so it must be checksummed over a %d-wide payload)", crc, wantCRC, ValueSize)
	}

	signed := append(append([]byte(nil), historical...), byte(wantCRC>>24), byte(wantCRC>>16), byte(wantCRC>>8), byte(wantCRC))
	if !ed25519.Verify(pub, signed, sig) {
		t.Fatal("signature does not verify against the historical fixed-width payload -- an older peer would reject this message as forged")
	}
	if err := Verify(pub, decoded, crc, sig); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}
