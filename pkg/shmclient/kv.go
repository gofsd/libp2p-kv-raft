package shmclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Set applies key=value through raft on the session's node, in a single
// set round trip rather than the setKey+setField pair -- see that
// variant's doc comment in api/shmevent.capnp for why: pkg/ipc.Call pays
// a real, non-negligible cost (a fresh shmring segment pair) per round
// trip, so a caller in this package's position halves Set's cost by not
// needing two.
func (s *Session) Set(ctx context.Context, key, value string) error {
	m, err := shmevent.NewSet([]byte(key), []byte(value))
	if err != nil {
		return fmt.Errorf("shmclient: set: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: set: %w", err)
	}
	return respErr("set", resp)
}

// LogAppend writes one pkg/logrecord record -- key must start with
// logrecord.LogKeyPrefix (typically built via logrecord.BuildKey) and
// value its encoded pkg/logrecord.Record. See logAppend's doc comment in
// api/shmevent.capnp for why this needs its own variant rather than
// reusing set.
func (s *Session) LogAppend(ctx context.Context, key, value []byte) error {
	m, err := shmevent.NewLogAppend(key, value)
	if err != nil {
		return fmt.Errorf("shmclient: log_append: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: log_append: %w", err)
	}
	return respErr("log_append", resp)
}

// ErrCompareFailed is what Txn/CompareAndSwap return when a
// shmevent.TxnOpCompare/TxnOpCompareAbsent precondition didn't hold: the
// transaction wrote nothing, and another writer changed the key first. It's
// the one Txn failure worth handling rather than reporting -- see
// CompareAndSwap, which turns it into a plain false for callers whose whole
// response is to re-read and try again.
var ErrCompareFailed = errors.New("shmclient: " + shmevent.CompareFailedMarker)

// Txn atomically applies every op in ops through raft on the session's
// node, in a single txn round trip: either all of them land, or none do
// (see txn's doc comment in api/shmevent.capnp). Each op is a plain Set
// (shmevent.TxnOpSet, key and value both required), Delete
// (shmevent.TxnOpDelete, value ignored), or one of the two compare
// preconditions (shmevent.TxnOpCompare/TxnOpCompareAbsent) -- a
// transaction whose preconditions don't hold applies nothing and returns
// an error matching [ErrCompareFailed].
func (s *Session) Txn(ctx context.Context, ops []shmevent.TxnOpSpec) error {
	m, err := shmevent.NewTxn(ops)
	if err != nil {
		return fmt.Errorf("shmclient: txn: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: txn: %w", err)
	}
	if resp.Which() == shmevent.Event_Which_error {
		msg, _ := resp.Error().Message_()
		if strings.Contains(msg, shmevent.CompareFailedMarker) {
			return fmt.Errorf("%w: %s", ErrCompareFailed, msg)
		}
		return fmt.Errorf("shmclient: txn: %s", msg)
	}
	return nil
}

// CompareAndSwap writes value to key only if key currently holds expected,
// and reports whether it did. A false return is not an error: it means
// another writer got there first, which is the normal outcome of two
// clients racing and the reason to call this instead of Get-then-Set.
//
// Pass absent=true to require that key does *not* exist instead (expected
// is then ignored) -- the create-if-not-exists half of the same primitive,
// which a Get-then-Set can't express safely at all: two clients that both
// read "not found" will both write, and the loser never learns it lost.
//
// The comparison happens inside the raft FSM's Apply (see kvfsm's OpTxn
// case), so it is serialized against every other write in the cluster;
// nothing this function does client-side has a window in it.
func (s *Session) CompareAndSwap(ctx context.Context, key, expected, value string, absent bool) (bool, error) {
	compare := shmevent.TxnOpSpec{Op: shmevent.TxnOpCompare, Key: []byte(key), Value: []byte(expected)}
	if absent {
		compare = shmevent.TxnOpSpec{Op: shmevent.TxnOpCompareAbsent, Key: []byte(key)}
	}
	err := s.Txn(ctx, []shmevent.TxnOpSpec{
		compare,
		{Op: shmevent.TxnOpSet, Key: []byte(key), Value: []byte(value)},
	})
	if errors.Is(err, ErrCompareFailed) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Get reads key from the session's node -- a one-shot getFieldByKey,
// skipping the registry round-trip Set needs -- which, like any raft
// follower's local read, may lag a moment behind a Set that just
// committed on the leader.
func (s *Session) Get(ctx context.Context, key string) (string, error) {
	m, err := shmevent.NewGetFieldByKey([]byte(key))
	if err != nil {
		return "", fmt.Errorf("shmclient: get_field_by_key: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: get_field_by_key: %w", err)
	}
	if err := respErr("get_field_by_key", resp); err != nil {
		return "", err
	}
	value, err := resp.GetFieldByKey().Value()
	if err != nil {
		return "", fmt.Errorf("shmclient: get_field_by_key: %w", err)
	}
	return string(value), nil
}

// ListRange returns the first stored key/value pair with start <= key <=
// end (both inclusive), or ok=false if none remain in that range -- see
// listRange's doc comment in api/shmevent.capnp. A caller wanting every
// match calls this in a loop, each time narrowing start to just past the
// previously returned key (e.g. append a 0x00 byte to it), the same "loop
// rather than a bulk response" shape PollExecute already uses.
func (s *Session) ListRange(ctx context.Context, start, end []byte) (key, value []byte, ok bool, err error) {
	m, err := shmevent.NewListRange(start, end)
	if err != nil {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: %w", err)
	}
	if err := respErr("list_range", resp); err != nil {
		return nil, nil, false, err
	}
	grp := resp.ListRange()
	key, err = grp.Key()
	if err != nil {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: %w", err)
	}
	if len(key) == 0 {
		return nil, nil, false, nil
	}
	value, err = grp.Value()
	if err != nil {
		return nil, nil, false, fmt.Errorf("shmclient: list_range: %w", err)
	}
	return key, value, true, nil
}

// Set is a one-shot convenience wrapper around Open+Session.Set, for a
// short-lived caller (pkg/kvctl) that doesn't need to cache the session
// across multiple calls.
func Set(ctx context.Context, peerID, key, value string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Set(ctx, key, value)
}

// LogAppend is the one-shot convenience wrapper around
// Open+Session.LogAppend.
func LogAppend(ctx context.Context, peerID string, key, value []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.LogAppend(ctx, key, value)
}

// Txn is the one-shot convenience wrapper around Open+Session.Txn.
func Txn(ctx context.Context, peerID string, ops []shmevent.TxnOpSpec) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Txn(ctx, ops)
}

// CompareAndSwap is the one-shot convenience wrapper around
// Open+Session.CompareAndSwap.
func CompareAndSwap(ctx context.Context, peerID, key, expected, value string, absent bool) (bool, error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return false, err
	}
	return s.CompareAndSwap(ctx, key, expected, value, absent)
}

// Get is the one-shot convenience wrapper around Open+Session.Get.
func Get(ctx context.Context, peerID, key string) (string, error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.Get(ctx, key)
}

// RangePair is one key/value pair returned by [Session.ScanRange].
type RangePair struct {
	Key   []byte
	Value []byte
}

// The two orders [Session.ScanRange] accepts. An empty order means
// [RangeOrderAsc], the order the underlying primitive produces natively.
const (
	RangeOrderAsc  = "asc"
	RangeOrderDesc = "desc"
)

// ScanRange collects every pair in [start, end] (both inclusive) into one
// slice, applying order, then skip, then limit -- the paginated whole-range
// counterpart to [Session.ListRange]'s single pair, and the one
// implementation behind both pkg/kvctl.RangeScan and
// mobile/kvmobile.RangeScan (which each used to carry their own copy of the
// walk, with nothing keeping the two in step).
//
// limit caps how many pairs come back (0 = unlimited); skip drops that many
// from the front *after* ordering, so the two compose the way an
// offset/limit pair is normally expected to: descending with skip 1 limit 2
// returns the second and third pairs counting back from the end of the
// range, not the second and third from its start.
//
// Ascending is what the wire primitive does natively, so this stops as soon
// as it holds skip+limit pairs and never reads the rest of the range.
// Descending cannot: listRange answers with the *first* pair at or after a
// lower bound and has no reverse form, so the last pair in a range is only
// findable by walking to it. A descending scan therefore costs a round trip
// per pair in the whole range even when limit is 1, and the cheap
// alternative -- a `descending` field on listRange's request, letting the
// store's own ORDER BY do it -- is deliberately not taken here: it would
// commit every daemon and the hand-mirrored Rust client (web-app) to a new
// wire field permanently, and an older daemon receiving it would silently
// answer ascending, which is worse than being slow. Revisit that only if a
// descending scan over a genuinely large range ever becomes a real
// workload; the ranges this serves today are bounded by construction.
func (s *Session) ScanRange(ctx context.Context, start, end []byte, limit, skip int, order string) ([]RangePair, error) {
	if limit < 0 {
		return nil, fmt.Errorf("shmclient: scan range: invalid limit %d: must be 0 (unlimited) or positive", limit)
	}
	if skip < 0 {
		return nil, fmt.Errorf("shmclient: scan range: invalid skip %d: must be 0 or positive", skip)
	}
	var descending bool
	switch order {
	case "", RangeOrderAsc:
	case RangeOrderDesc:
		descending = true
	default:
		return nil, fmt.Errorf("shmclient: scan range: invalid order %q: must be %q or %q", order, RangeOrderAsc, RangeOrderDesc)
	}

	// Only meaningful ascending -- see the doc comment. 0 means "read the
	// whole range", which unlimited and descending both need.
	var enough int
	if !descending && limit > 0 {
		enough = skip + limit
	}

	var pairs []RangePair
	lo := start
	for {
		key, value, ok, err := s.ListRange(ctx, lo, end)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		pairs = append(pairs, RangePair{Key: key, Value: value})
		if enough > 0 && len(pairs) >= enough {
			break
		}
		// Just past the key just returned, so the next call answers with the
		// following one -- a 0x00 suffix is the smallest key greater than
		// this one under the byte-wise comparison the store scans with.
		lo = append(append([]byte{}, key...), 0x00)
	}

	if descending {
		for i, j := 0, len(pairs)-1; i < j; i, j = i+1, j-1 {
			pairs[i], pairs[j] = pairs[j], pairs[i]
		}
	}
	if skip >= len(pairs) {
		return nil, nil
	}
	pairs = pairs[skip:]
	if limit > 0 && len(pairs) > limit {
		pairs = pairs[:limit]
	}
	return pairs, nil
}

// ListRange is the one-shot convenience wrapper around
// Open+Session.ListRange.
func ListRange(ctx context.Context, peerID string, start, end []byte) (key, value []byte, ok bool, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return nil, nil, false, err
	}
	return s.ListRange(ctx, start, end)
}
