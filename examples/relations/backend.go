package relations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Backend is the whole of what this package needs from a key/value
// store: one atomic multi-op write, and one ordered range read. Both
// shapes already exist in this repo -- shmevent's txn and listRange --
// which is why the entire relation model below needs no new wire
// primitive of its own (see api/shmevent.capnp's "before adding a new
// variant" note). Node() is the real, raft-replicated implementation;
// Memory() is the same semantics in a map, for tests and for working out
// a schema before any cluster exists.
type Backend interface {
	// Apply applies every op atomically: either all of them land or none
	// do, and a failed OpCompare/OpCompareAbsent precondition means none
	// did (returning an error matching ErrConflict).
	Apply(ctx context.Context, ops []Op) error
	// Scan returns every pair with start <= key <= end, both bounds
	// inclusive, in ascending byte order, at most limit of them (0 =
	// unlimited).
	Scan(ctx context.Context, start, end []byte, limit int) ([]Pair, error)
}

// OpKind is what one Op does. The four map one-for-one onto
// shmevent.TxnOpSet/TxnOpDelete/TxnOpCompare/TxnOpCompareAbsent -- this
// package mirrors them rather than importing them so that Memory can
// implement the same contract without dragging the wire layer in.
type OpKind uint8

const (
	// OpSet writes Value at Key.
	OpSet OpKind = iota + 1
	// OpDelete removes Key. Value is ignored.
	OpDelete
	// OpCompare is a precondition: Key must currently hold exactly
	// Value. A missing key never satisfies it.
	OpCompare
	// OpCompareAbsent is a precondition: Key must not exist. Value is
	// ignored. This is what makes Store.Allocate race-free without a
	// lock -- see its doc comment.
	OpCompareAbsent
)

// Op is one operation of an atomic Apply.
type Op struct {
	Kind  OpKind
	Key   []byte
	Value []byte
}

// Pair is one key/value pair returned by Scan.
type Pair struct {
	Key   []byte
	Value []byte
}

// ErrConflict is what Apply returns when a precondition did not hold:
// somebody else wrote the key first, nothing was applied, and the caller
// should re-read and retry rather than treat this as a failure. It is
// this package's name for shmclient.ErrCompareFailed.
var ErrConflict = errors.New("relations: precondition failed, nothing applied")

// memory is an in-process Backend with the same atomicity and ordering
// contract as the real one. It exists so a schema can be designed, and
// this package's own logic tested, without spawning a node -- not as a
// storage option anyone should ship: it is neither durable nor
// replicated.
type memory struct {
	mu sync.Mutex
	kv map[string][]byte
}

// Memory returns an in-process Backend. See memory's own comment for
// what it is and is not for.
func Memory() Backend { return &memory{kv: make(map[string][]byte)} }

// Apply checks every precondition before performing any write, the same
// validate-then-write discipline pkg/kvfsm's own OpTxn case applies
// inside raft's Apply -- so a caller sees identical semantics against
// either backend.
func (m *memory) Apply(ctx context.Context, ops []Op) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, op := range ops {
		switch op.Kind {
		case OpCompare:
			cur, ok := m.kv[string(op.Key)]
			if !ok || !bytes.Equal(cur, op.Value) {
				return fmt.Errorf("%w: compare on key %x", ErrConflict, op.Key)
			}
		case OpCompareAbsent:
			if _, ok := m.kv[string(op.Key)]; ok {
				return fmt.Errorf("%w: key %x already exists", ErrConflict, op.Key)
			}
		case OpSet, OpDelete:
		default:
			return fmt.Errorf("relations: unknown op kind %d", op.Kind)
		}
	}
	for _, op := range ops {
		switch op.Kind {
		case OpSet:
			m.kv[string(op.Key)] = append([]byte(nil), op.Value...)
		case OpDelete:
			delete(m.kv, string(op.Key))
		case OpCompare, OpCompareAbsent:
			// preconditions only -- already checked above
		}
	}
	return nil
}

func (m *memory) Scan(ctx context.Context, start, end []byte, limit int) ([]Pair, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string
	for k := range m.kv {
		if bytes.Compare([]byte(k), start) >= 0 && bytes.Compare([]byte(k), end) <= 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, Pair{
			Key:   []byte(k),
			Value: append([]byte(nil), m.kv[k]...),
		})
	}
	return pairs, nil
}
