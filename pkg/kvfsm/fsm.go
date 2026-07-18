// Package kvfsm implements the hashicorp/raft FSM for the distributed KV
// store, backed by pkg/store.
package kvfsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// maxSystemListEntries bounds every pkg/shmevent.SystemKey-based list (the
// confirmed/pending halves of KindPermitPeer, KindBootstrapNode,
// KindClusterMember, and any future kind) independently -- each distinct
// kind+status prefix (SystemKey's first 3 bytes) may hold at most this
// many entries. Enforced here, inside Apply, rather than as a pre-check in
// pkg/daemon before calling rf.Apply: Apply is the only place every raft
// replica deterministically agrees on order, so a Go-level pre-check could
// race against a concurrent Apply from another source and let two nodes
// disagree about whether the cap was hit. A var, not a const, so tests can
// temporarily lower it rather than writing 65000 real rows.
var maxSystemListEntries = 65000

// systemKeyPrefixLen is how many leading bytes of a shmevent.SystemKey
// identify its list (kind + status, see that function's doc comment) --
// everything after is the peer id, which varies per entry and so must not
// be part of the count-limiting prefix.
const systemKeyPrefixLen = 3

// checkSystemListCap enforces maxSystemListEntries for key if key is a
// pkg/shmevent.SystemKey (starts with shmevent.SystemKeyPrefix): an
// overwrite of an already-existing key never grows its list, so only a
// genuinely new key is checked against its kind+status prefix's current
// count. Ordinary user keys (anything not starting with SystemKeyPrefix)
// are never counted or capped.
func checkSystemListCap(s *store.Store, key []byte) error {
	if len(key) < systemKeyPrefixLen || key[0] != shmevent.SystemKeyPrefix {
		return nil
	}
	if _, err := s.Get(key); err == nil {
		return nil // overwrite of an existing entry, not a new one
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	prefix := key[:systemKeyPrefixLen]
	count, err := s.CountPrefix(prefix)
	if err != nil {
		return err
	}
	if count >= maxSystemListEntries {
		return fmt.Errorf("kvfsm: system list %x is at capacity (%d entries)", prefix, maxSystemListEntries)
	}
	return nil
}

// OpType identifies the kind of mutation carried by a raft log entry.
type OpType uint8

const (
	OpSet OpType = 1
	OpDel OpType = 2
	// OpConfirm promotes a pending pkg/shmevent system record to
	// confirmed: key is the pending record's own key, value is the
	// confirmed record's key (not a value -- see Apply's OpConfirm case).
	// Reuses EncodeCommand/DecodeCommand's existing key+value framing
	// unchanged; both fields are already opaque byte slices, so no wire
	// format change was needed for this op.
	OpConfirm OpType = 3
)

// EncodeCommand builds the raft log payload for a Set/Delete operation.
// Layout: [1 byte op][4 byte big-endian key len][key][4 byte big-endian value len][value].
func EncodeCommand(op OpType, key, value []byte) []byte {
	buf := make([]byte, 1+4+len(key)+4+len(value))
	buf[0] = byte(op)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(key)))
	off := 5
	off += copy(buf[off:], key)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(value)))
	off += 4
	copy(buf[off:], value)
	return buf
}

// DecodeCommand is the inverse of EncodeCommand -- also used directly by
// pkg/daemon's ForwardProtocolID handling, which forwards a Set to the
// leader using this same op+key+value framing rather than the user-facing
// pkg/shmevent protocol (ForwardProtocolID is internal node-to-node
// machinery, not something a "user" ever speaks).
func DecodeCommand(data []byte) (op OpType, key, value []byte, err error) {
	return decodeCommand(data)
}

func decodeCommand(data []byte) (op OpType, key, value []byte, err error) {
	if len(data) < 5 {
		return 0, nil, nil, fmt.Errorf("kvfsm: command too short")
	}
	op = OpType(data[0])
	klen := binary.BigEndian.Uint32(data[1:5])
	off := 5
	if uint32(len(data[off:])) < klen {
		return 0, nil, nil, fmt.Errorf("kvfsm: truncated key")
	}
	key = data[off : off+int(klen)]
	off += int(klen)
	if len(data[off:]) < 4 {
		return 0, nil, nil, fmt.Errorf("kvfsm: missing value length")
	}
	vlen := binary.BigEndian.Uint32(data[off : off+4])
	off += 4
	if uint32(len(data[off:])) < vlen {
		return 0, nil, nil, fmt.Errorf("kvfsm: truncated value")
	}
	value = data[off : off+int(vlen)]
	return op, key, value, nil
}

// FSM adapts pkg/store to the raft.FSM interface.
type FSM struct {
	Store *store.Store
}

// New returns an FSM backed by s.
func New(s *store.Store) *FSM {
	return &FSM{Store: s}
}

// ApplyResult is returned to the raft ApplyFuture caller.
type ApplyResult struct {
	Err error
}

// Apply implements raft.FSM.
func (f *FSM) Apply(l *raft.Log) any {
	op, key, value, err := decodeCommand(l.Data)
	if err != nil {
		return ApplyResult{Err: err}
	}
	switch op {
	case OpSet:
		if err := checkSystemListCap(f.Store, key); err != nil {
			return ApplyResult{Err: err}
		}
		return ApplyResult{Err: f.Store.Set(key, value)}
	case OpDel:
		return ApplyResult{Err: f.Store.Delete(key)}
	case OpConfirm:
		// Read-modify-write across two different keys, safe and
		// deterministic here because Apply runs exactly once, in raft log
		// order, against each node's own already-caught-up local store --
		// every node ends up with the identical result without needing
		// any separate linearizable-read machinery.
		v, err := f.Store.Get(key)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: confirm: no pending record at key: %w", err)}
		}
		// The cap check applies to the *confirmed* key (value) being
		// promoted to, not the pending key (key) being read from and
		// deleted -- this is what makes a kind's pending and confirmed
		// lists count independently: confirming never touches the
		// pending list's membership count, only the confirmed side's.
		if err := checkSystemListCap(f.Store, value); err != nil {
			return ApplyResult{Err: err}
		}
		if err := f.Store.Set(value, v); err != nil {
			return ApplyResult{Err: err}
		}
		return ApplyResult{Err: f.Store.Delete(key)}
	default:
		return ApplyResult{Err: fmt.Errorf("kvfsm: unknown op %d", op)}
	}
}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.Store}, nil
}

// Restore implements raft.FSM.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.Store.LoadAll(rc)
}

type fsmSnapshot struct {
	store *store.Store
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.store.DumpAll(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
