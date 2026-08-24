package logref_test

import (
	"errors"
	"math"
	"strings"
	"testing"

	capnp "capnproto.org/go/capnp/v3"

	"github.com/gofsd/libp2p-kv-raft/pkg/logref"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, id := range []uint64{0, 1, 4417, 1 << 32, math.MaxUint64} {
		buf, err := logref.Encode(id)
		if err != nil {
			t.Fatalf("Encode(%d): %v", id, err)
		}
		got, err := logref.Decode(buf)
		if err != nil {
			t.Fatalf("Decode(Encode(%d)): %v", id, err)
		}
		if got != id {
			t.Fatalf("Decode(Encode(%d)) = %d", id, got)
		}
	}
}

// The record's key is keyed by the *field*, not by the number, so
// UnitID's rendering is part of the wire contract rather than an
// implementation detail: change it and every label already printed stops
// resolving.
func TestUnitIDIsDecimalASCII(t *testing.T) {
	if got := logref.UnitID(4417); got != "4417" {
		t.Fatalf("UnitID(4417) = %q, want \"4417\"", got)
	}
	if got := logref.UnitID(math.MaxUint64); got != "18446744073709551615" {
		t.Fatalf("UnitID(MaxUint64) = %q", got)
	}
	id, err := logref.ParseUnitID("4417")
	if err != nil || id != 4417 {
		t.Fatalf("ParseUnitID(\"4417\") = %d, %v", id, err)
	}
	if _, err := logref.ParseUnitID("4417x"); err == nil {
		t.Fatal("ParseUnitID accepted a non-numeric unit id")
	}
	// A record written by something else under the same kind must not
	// resolve as an id by accident -- ParseUnitID rejecting a leading
	// sign is what keeps "-1" out.
	if _, err := logref.ParseUnitID("-1"); err == nil {
		t.Fatal("ParseUnitID accepted a negative unit id")
	}
}

// A payload from some other application, or from another part of this
// one, has to come back as ErrNotLogRef rather than as a plausible id --
// android-app's scan dispatch tries this decoder alongside RunCode/
// NavCode/shmevent and moves on when it says no.
func TestDecodeRejectsForeignPayloads(t *testing.T) {
	for name, buf := range map[string][]byte{
		"empty":     {},
		"text":      []byte("com.gofsd.kvdemo.nav.group:KV"),
		"truncated": func() []byte { b, _ := logref.Encode(4417); return b[:len(b)/2] }(),
		"random":    {0x00, 0x01, 0x02, 0x03, 0xff, 0xfe, 0xfd, 0xfc, 0x11, 0x22},
	} {
		if _, err := logref.Decode(buf); !errors.Is(err, logref.ErrNotLogRef) {
			t.Fatalf("Decode(%s) error = %v, want ErrNotLogRef", name, err)
		}
	}
}

// The tag is the only thing separating a log reference from a capnp
// message of some other type whose bytes happen to line up -- a message
// built to this exact layout but tagged differently must still be
// refused.
func TestDecodeRejectsWrongTag(t *testing.T) {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	root, err := logref.NewRootLogRef(seg)
	if err != nil {
		t.Fatalf("NewRootLogRef: %v", err)
	}
	if err := root.SetTag("some.other.app/code/v1"); err != nil {
		t.Fatalf("SetTag: %v", err)
	}
	root.SetLogId(4417)
	root.SetFieldCrc32(logref.FieldCRC32(4417))
	buf, err := root.Message().Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := logref.Decode(buf); !errors.Is(err, logref.ErrNotLogRef) {
		t.Fatalf("Decode(wrong tag) error = %v, want ErrNotLogRef", err)
	}
}

// The failure this whole checksum exists for: a decode that survived the
// symbol's own error correction and still names the wrong record. It has
// to be told apart from a foreign code, because the two ask the person
// holding the phone for different things -- scan it again, versus this
// is not one of ours.
func TestDecodeRejectsCorruptedID(t *testing.T) {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	root, err := logref.NewRootLogRef(seg)
	if err != nil {
		t.Fatalf("NewRootLogRef: %v", err)
	}
	if err := root.SetTag(logref.Tag); err != nil {
		t.Fatalf("SetTag: %v", err)
	}
	root.SetLogId(4418) // the id a flipped bit produced ...
	root.SetFieldCrc32(logref.FieldCRC32(4417))
	buf, err := root.Message().Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	_, err = logref.Decode(buf)
	if !errors.Is(err, logref.ErrCorrupt) {
		t.Fatalf("Decode(corrupted) error = %v, want ErrCorrupt", err)
	}
	if errors.Is(err, logref.ErrNotLogRef) {
		t.Fatal("a corrupted log reference must not also report as somebody else's code")
	}
}

// The checksum covers the record field's own bytes, not the id's raw
// little-endian words -- so two ids whose decimal forms differ must
// differ here even when their low bytes do not.
func TestFieldCRC32CoversTheFieldsOwnBytes(t *testing.T) {
	if logref.FieldCRC32(4417) == logref.FieldCRC32(4418) {
		t.Fatal("adjacent ids share a checksum")
	}
	if logref.FieldCRC32(1) == logref.FieldCRC32(1<<32) {
		t.Fatal("ids sharing their low bytes share a checksum")
	}
}

// android-app's DataMatrixCodecTest measured ~40 bytes as the floor below
// which a generated symbol stops decoding reliably off a screen. This is
// the code's whole payload, so it is this package's business to stay
// above it -- see api/logref.capnp on why the tag is what buys the room.
func TestEncodedPayloadClearsTheOpticalFloor(t *testing.T) {
	const minReliableSize = 40
	buf, err := logref.Encode(1)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(buf) < minReliableSize {
		t.Fatalf("encoded log reference is %d bytes, below the %d-byte floor a generated symbol decodes reliably above", len(buf), minReliableSize)
	}
	t.Logf("encoded log reference: %d bytes", len(buf))
}

// The tag is a wire constant: a device running an older build has to
// keep resolving codes a newer one prints, so it may not drift.
func TestTagIsStable(t *testing.T) {
	if logref.Tag != "gofsd.libp2p-kv-raft/logref/v1" {
		t.Fatalf("Tag = %q -- changing it invalidates every label already printed", logref.Tag)
	}
	if !strings.HasSuffix(logref.Tag, "/v1") {
		t.Fatal("Tag should carry its own version")
	}
}
