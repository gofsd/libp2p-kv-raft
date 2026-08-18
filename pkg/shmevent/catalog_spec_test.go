package shmevent

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
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

// The old wire format packed a commandPut event's id/name/peerID/spec into
// one hand-rolled byte blob (EncodeCommandPutPayload/
// EncodeCommandPutPayloadWithSpec/DecodeCommandPutPayloadFull), now deleted:
// a commandPut Msg carries those as separate typed capnp fields directly
// (NewCommandPut/NewCommandPutWithSpec, Event_commandPut's Id/Name/PeerId/
// Spec/HasSpec accessors). This pins the same round trip -- including that
// a spec-less put leaves HasSpec false, the wire-level counterpart of
// TestCommandPayloadStaysV1WithoutASpec above -- at the Msg/wire level
// instead of the byte-blob level.
func TestCommandPutPayloadRoundTrip(t *testing.T) {
	spec := `{"fields":[]}`

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	noSpec, err := NewCommandPut("inspect", "Inspect", "peer1")
	if err != nil {
		t.Fatalf("NewCommandPut: %v", err)
	}
	if noSpec.CommandPut().HasSpec() {
		t.Fatal("NewCommandPut must leave HasSpec false")
	}

	withSpec, err := NewCommandPutWithSpec("inspect", "Inspect", "peer1", spec)
	if err != nil {
		t.Fatalf("NewCommandPutWithSpec: %v", err)
	}
	buf, err := Encode(withSpec, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, crc, sig, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if err := Verify(pub, decoded, crc, sig); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if decoded.Which() != Event_Which_commandPut {
		t.Fatalf("Which = %v, want commandPut", decoded.Which())
	}
	grp := decoded.CommandPut()
	id, err := grp.Id()
	if err != nil {
		t.Fatalf("Id: %v", err)
	}
	name, err := grp.Name()
	if err != nil {
		t.Fatalf("Name: %v", err)
	}
	peerID, err := grp.PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	gotSpec, err := grp.Spec()
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}
	if id != "inspect" || name != "Inspect" || peerID != "peer1" || gotSpec != spec || !grp.HasSpec() {
		t.Fatalf("got %q/%q/%q/%q (hasSpec=%v)", id, name, peerID, gotSpec, grp.HasSpec())
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

	// The old wire format's EncodeStationPutPayload/DecodeStationPutPayload
	// (a hand-packed peerID+name+attrs blob) is gone the same way
	// EncodeCommandPutPayload is -- a stationPut Msg carries those as
	// separate typed fields (NewStationPut, Event_stationPut's PeerId/Name/
	// Attrs accessors) instead.
	m, err := NewStationPut("peer1", "Assembly 1", string(attrs))
	if err != nil {
		t.Fatalf("NewStationPut: %v", err)
	}
	peerID, err := m.StationPut().PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	putName, err := m.StationPut().Name()
	if err != nil {
		t.Fatalf("Name: %v", err)
	}
	putAttrs, err := m.StationPut().Attrs()
	if err != nil {
		t.Fatalf("Attrs: %v", err)
	}
	if peerID != "peer1" || putName != "Assembly 1" || putAttrs != string(attrs) {
		t.Fatalf("got %q/%q/%q", peerID, putName, putAttrs)
	}
}

// The old wire format wrapped every Group/Command/Station/GroupCommand/
// PeerGroup "put" payload behind one extra leading kind byte
// (EncodeCatalogPayload/DecodeCatalogPayload), used by EventCatalogPut to
// carry any of the five under one wire event. That wrapper is gone: each
// kind is now its own top-level capnp union variant (groupPut/commandPut/
// stationPut/groupCommandPut/peerGroupPut), so Which() alone is what used
// to be the wrapper's kind byte. This pins the same property the old test
// did -- every kind survives a full sign/encode/decode/verify cycle
// distinguishably from every other kind -- at the variant level instead.
func TestCatalogVariantsRoundTripThroughWire(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	for _, tc := range []struct {
		name  string
		build func() (Msg, error)
		want  Event_Which
	}{
		{"group_put", func() (Msg, error) { return NewGroupPut("g1", "infantry", true) }, Event_Which_groupPut},
		{"command_put", func() (Msg, error) { return NewCommandPut("c1", "resupply", "peer1") }, Event_Which_commandPut},
		{"station_put", func() (Msg, error) { return NewStationPut("peer1", "Assembly 1", "attrs") }, Event_Which_stationPut},
		{"group_command_put", func() (Msg, error) { return NewGroupCommandPut("c1", "g1") }, Event_Which_groupCommandPut},
		{"peer_group_put", func() (Msg, error) { return NewPeerGroupPut("peer1", "g1") }, Event_Which_peerGroupPut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := tc.build()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			buf, err := Encode(m, priv)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			decoded, crc, sig, err := Decode(buf)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if err := Verify(pub, decoded, crc, sig); err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if decoded.Which() != tc.want {
				t.Fatalf("Which = %v, want %v", decoded.Which(), tc.want)
			}
		})
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

// TestValueSizeTiers and TestCanonicalWidthKeepsHistoricalWidthForSmallValues
// and TestSmallValueSignatureIsWidthStable pinned the old wire format's fixed
// per-event canonical padding widths (ValueSize/KVValueSize/ChannelValueSize,
// valueSizeFor, canonicalWidth) that made cross-language and cross-build
// CRC/signature computation agree over a fixed-width payload. None of that
// exists in the capnp rewrite -- there is no canonical padding step and no
// per-event size ceiling at all, so there is nothing left for these tests to
// pin. Dropped rather than adapted; see this package's migration notes and
// event_test.go's note by TestValueTooLongRejected's removal for the same
// reasoning.

// TestSentinelLengthNameRejected pins the write-side guard the v1/v2 Command
// payload split depends on: a name of exactly commandPayloadV2Sentinel bytes
// would encode as a v1 payload whose first two bytes are the v2 marker, and
// decode back as a v2 record with an empty name, an empty peer id, and the
// entire real payload swallowed as a spec. That used to be unreachable
// because the retired value ceilings were far below 0xFFFF; now only
// checkCommandNameLen stops it, in both writers -- so a name is either
// encodable in both formats or neither, never one and not the other.
func TestSentinelLengthNameRejected(t *testing.T) {
	name := string(make([]byte, commandPayloadV2Sentinel))
	peerID := []byte("12D3KooWtarget")

	if _, err := EncodeCommandPayload(name, peerID); err == nil {
		t.Fatal("v1 encode accepted a sentinel-length name -- its payload is indistinguishable from v2")
	}
	if _, err := EncodeCommandPayloadWithSpec(name, peerID, []byte(`{"f":1}`)); err == nil {
		t.Fatal("v2 encode accepted a sentinel-length name the v1 encoder rejects")
	}

	// One byte below the sentinel is still fine, and still reads back as v1.
	ok := string(make([]byte, commandPayloadV2Sentinel-1))
	payload, err := EncodeCommandPayload(ok, peerID)
	if err != nil {
		t.Fatalf("EncodeCommandPayload(len %d): %v", len(ok), err)
	}
	if CommandPutPayloadHasSpec(payload) {
		t.Fatal("a v1 payload one byte below the sentinel was read as v2")
	}
	gotName, gotPeer, spec, err := DecodeCommandPayloadFull(payload)
	if err != nil {
		t.Fatalf("DecodeCommandPayloadFull: %v", err)
	}
	if len(gotName) != len(ok) || !bytes.Equal(gotPeer, peerID) || spec != nil {
		t.Fatalf("round trip = (name %d bytes, peer %q, spec %v), want (%d bytes, %q, nil)",
			len(gotName), gotPeer, spec, len(ok), peerID)
	}
}
