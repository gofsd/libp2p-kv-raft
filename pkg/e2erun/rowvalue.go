package e2erun

import (
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// withField returns a copy of ev with field set to value, leaving every
// other field untouched.
func withField(ev e2edata.Event, field, value string) e2edata.Event {
	fields := make(map[string]string, len(ev.Fields)+1)
	for k, v := range ev.Fields {
		fields[k] = v
	}
	fields[field] = value
	return e2edata.Event{Op: ev.Op, ID: ev.ID, Fields: fields}
}

// expandEventFields returns a copy of ev with transform applied to every
// field value it carries -- the per-Event generalization of
// ExpandRowValue/ResolveBootstrapPlaceholder, which only ever operated on
// a single flat value string under the old (pre-union) wire design where
// every event shared one opaque Value field.
func expandEventFields(ev e2edata.Event, transform func(string) string) e2edata.Event {
	if len(ev.Fields) == 0 {
		return ev
	}
	fields := make(map[string]string, len(ev.Fields))
	for k, v := range ev.Fields {
		fields[k] = transform(v)
	}
	return e2edata.Event{Op: ev.Op, ID: ev.ID, Fields: fields}
}

// LargeValueToken is the sentinel a row uses in place of a literal
// multi-kilobyte value -- the same idea as BootstrapToken, for a different
// reason. A value at shmevent.KVValueSize is 4096 bytes; pasted into
// test/e2e/testdata.json it would bury the surrounding rows in a wall of
// filler and defeat the point of that file being readable by a human (see
// its own doc comment). The token keeps the row legible and lets the
// expansion track the constant, so a row authored today still means "the
// largest value the protocol allows" after that ceiling next moves.
const LargeValueToken = "LARGEVALUE"

// ExpandRowValue resolves LargeValueToken in a row's value. Applied to
// every event on every platform, unlike ResolveBootstrapPlaceholder, which
// only makes sense for an EventAdd's leader address.
func ExpandRowValue(value string) string {
	if value == LargeValueToken {
		return LargeValue()
	}
	return value
}

// largeValueSize is the size LargeValueToken expands to -- the old
// plain-KV ceiling (shmevent.KVValueSize) this test was originally built
// around, kept as a literal now that the wire union enforces no ceiling
// of its own: it's still the interesting size to send, since a transport
// this large would have caught a ring sized too small for the framing
// around it (as actually happened once) even though nothing rejects a
// larger value outright anymore.
const largeValueSize = 4096

// LargeValue is what LargeValueToken expands to: a value exactly
// largeValueSize bytes. Everything below it has been exercised since this
// suite existed.
//
// Deliberately repeated readable text rather than random or zero bytes, so
// a failure that surfaces as a truncated or corrupted value shows at a
// glance where it was cut.
func LargeValue() string {
	const filler = "kvraft-large-value-"
	return strings.Repeat(filler, largeValueSize/len(filler)+1)[:largeValueSize]
}
