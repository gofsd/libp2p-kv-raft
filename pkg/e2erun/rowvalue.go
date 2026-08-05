package e2erun

import (
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

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

// LargeValue is what LargeValueToken expands to: a value exactly at the
// plain-KV ceiling (shmevent.KVValueSize), which is the interesting size
// to send. Everything below it has been exercised since this suite
// existed; the top of the range is where a transport that quietly assumed
// the *old* 512-byte ceiling -- or, as actually happened, a ring sized to
// the new ceiling but not to the framing around it -- gives way.
//
// Deliberately repeated readable text rather than random or zero bytes, so
// a failure that surfaces as a truncated or corrupted value shows at a
// glance where it was cut.
func LargeValue() string {
	const filler = "kvraft-large-value-"
	return strings.Repeat(filler, shmevent.KVValueSize/len(filler)+1)[:shmevent.KVValueSize]
}
