// Package logref implements the payload of a log-reference Data Matrix
// code (api/logref.capnp): a compact, self-checking name for one
// pkg/logrecord.Record, meant to be printed on a label or shown on a
// phone screen and read by another phone's camera.
//
// # What a log reference is for
//
// Every other code android-app generates tells the scanning device to
// *do* something -- run a command (RunCode), enter a screen (NavCode),
// redeem a ticket (a signed shmevent). A log reference tells it what it
// is *looking at*. The device resolves the id to a record, reads the
// group that record declares, and shows that group's commands; the
// person then picks the command they want and the id is already in the
// form. Scanning the next label re-fills the same open form with the
// next id, so a run of twenty objects costs twenty scans and one tap
// instead of twenty scans and sixty taps.
//
// # How an id resolves to a record
//
// A log reference carries a number, not a key. It resolves because the
// record it names is written under a fixed convention rather than an
// arbitrary one:
//
//	kind   = Kind          ("objcode")
//	unitID = UnitID(logId) (the id's decimal ASCII form)
//
// pkg/logrecord.BuildKey packs kind and unitID into the key ahead of the
// timestamp, so "every record naming this id, newest last" is the
// ordinary bounded range scan logrecord.ScanBounds already builds --
// no new index, no new event, and no daemon change. The group lives in
// the record's own Fields under GroupField, which makes an id's group a
// replicated, rewritable fact rather than something baked into the code:
// re-registering an id under a different group re-points every label
// already printed with it.
//
// Records are ordinary log records in every other respect. An id may be
// registered more than once (a correction, a re-assignment); the newest
// record wins, which is what LatestGroup returns.
package logref

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"

	capnp "capnproto.org/go/capnp/v3"
)

// Kind is the pkg/logrecord.Record kind every log-reference registration
// is written under. Fixed rather than caller-chosen because it is half
// of the addressing convention a scanned id resolves through (see this
// package's doc comment) -- a caller free to pick it would produce codes
// nothing could resolve.
const Kind = "objcode"

// GroupField is the Fields key whose value names the group a log
// reference belongs to. On android-app that group is a CommandCatalog.kt
// category, which is what the scanning device switches to; nothing in
// this package interprets the string beyond requiring it to be
// non-empty.
const GroupField = "group"

// Tag is the constant discriminant every encoded LogRef carries, and the
// only thing separating a log reference from an arbitrary barcode whose
// bytes happen to parse as one -- see api/logref.capnp's doc comment on
// why a binary payload needs the same defence NavCode/RunCode get from
// their string prefixes.
const Tag = "gofsd.libp2p-kv-raft/logref/v1"

// ErrNotLogRef reports a payload that is not a log reference: bytes that
// are not a capnp message at all, or a message whose Tag is not [Tag].
// Callers dispatching a scan (android-app's AppRoot) use it to move on
// to the next decoder rather than to report a failure -- a code meant
// for something else is not an error, it is somebody else's code.
var ErrNotLogRef = errors.New("logref: not a log reference")

// ErrCorrupt reports a message that is a log reference but whose
// fieldCrc32 disagrees with its logId -- a decode that survived the
// symbol's own error correction and still came out wrong. Distinct from
// [ErrNotLogRef] on purpose: this one means "scan it again", and a
// caller should say so rather than silently ignoring the code.
var ErrCorrupt = errors.New("logref: crc32 does not match log id")

// UnitID renders logId into the canonical bytes the record's key carries
// -- its decimal ASCII form, with no padding and no sign. This is the
// "log record field" a log reference is a uint64 representation of, and
// the exact string a caller passes to pkg/logrecord.BuildKey or
// kvmobile.LogQuery as unitID.
func UnitID(logID uint64) string { return strconv.FormatUint(logID, 10) }

// ParseUnitID is UnitID's inverse -- for a caller holding a record's
// parsed unitID (pkg/logrecord.ParseKey) that wants the id back.
func ParseUnitID(unitID string) (uint64, error) {
	id, err := strconv.ParseUint(unitID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("logref: unit id %q is not a log id: %w", unitID, err)
	}
	return id, nil
}

// FieldCRC32 is the checksum a code carries alongside its id: CRC-32
// (IEEE) over [UnitID]'s bytes, not over the id's raw 8 little-endian
// bytes. Checksumming the field's own canonical form is what makes the
// check meaningful -- it verifies the thing the record is actually keyed
// by, and stays correct if the id is ever carried somewhere else in that
// same textual form.
func FieldCRC32(logID uint64) uint32 {
	return crc32.ChecksumIEEE([]byte(UnitID(logID)))
}

// Encode builds the Data Matrix payload naming logID.
func Encode(logID uint64) ([]byte, error) {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, fmt.Errorf("logref: new message: %w", err)
	}
	root, err := NewRootLogRef(seg)
	if err != nil {
		return nil, fmt.Errorf("logref: new root: %w", err)
	}
	if err := root.SetTag(Tag); err != nil {
		return nil, fmt.Errorf("logref: set tag: %w", err)
	}
	root.SetLogId(logID)
	root.SetFieldCrc32(FieldCRC32(logID))
	buf, err := root.Message().Marshal()
	if err != nil {
		return nil, fmt.Errorf("logref: marshal: %w", err)
	}
	return buf, nil
}

// Decode parses buf as a log-reference payload and returns the log id it
// names, having checked both that the payload is one of ours ([Tag]) and
// that its own checksum agrees with its id.
//
// The two failure modes are deliberately different errors, because a
// caller reacts to them differently: [ErrNotLogRef] means try the next
// decoder, [ErrCorrupt] means this *is* a log reference and it did not
// survive the trip. Note that a wrong-but-well-formed decode is only
// detectable at all because of fieldCrc32 -- see api/logref.capnp.
func Decode(buf []byte) (uint64, error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrNotLogRef, err)
	}
	root, err := ReadRootLogRef(msg)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrNotLogRef, err)
	}
	tag, err := root.Tag()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrNotLogRef, err)
	}
	if tag != Tag {
		return 0, fmt.Errorf("%w: tag %q", ErrNotLogRef, tag)
	}
	logID := root.LogId()
	if got, want := root.FieldCrc32(), FieldCRC32(logID); got != want {
		return 0, fmt.Errorf("%w: log id %d carries crc32 %#08x, want %#08x", ErrCorrupt, logID, got, want)
	}
	return logID, nil
}
