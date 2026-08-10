package shmevent

import (
	"fmt"
	"strings"
)

// TxnOpSpec.Op's valid values. TxnOpSet/TxnOpDelete are plain key writes
// and deletes; TxnOpCompare/TxnOpCompareAbsent are *preconditions* that
// write nothing and instead decide whether the rest of the transaction
// applies at all -- the compare-and-swap shape this type's doc comment
// used to name as something a real transaction "might eventually grow
// into".
const (
	TxnOpSet    byte = 1
	TxnOpDelete byte = 2
	// TxnOpCompare asserts that Key currently holds exactly Value. A key
	// that does not exist never satisfies it (see TxnOpCompareAbsent for
	// that case) -- pkg/store distinguishes absent from empty, so an
	// empty Value means "exists and is empty", nothing looser.
	TxnOpCompare byte = 3
	// TxnOpCompareAbsent asserts that Key does not exist at all. Value is
	// ignored. This is what makes a create-if-not-exists safe against a
	// concurrent creator: without it, "read, see nothing, write" is a
	// read-modify-write race no ordering of two clients can win.
	TxnOpCompareAbsent byte = 4
)

// CompareFailedMarker is the substring every "a precondition didn't hold"
// error carries, wherever it is produced or inspected. It exists because
// that failure has to survive a boundary Go error values can't cross: the
// FSM raises it (kvfsm.ErrCompareFailed), pkg/daemon forwards its text over
// IPC as an error Msg, and the client turns it back into a typed result
// (pkg/shmclient.ErrCompareFailed, mobile/kvmobile.CompareAndSwap's plain
// false). Matching on a constant both ends import beats each end hard-coding
// the same string and only finding out they disagree when a retry loop
// silently stops retrying.
const CompareFailedMarker = "txn: compare failed"

// TxnOpSpec is one operation within a txn request: Set (Key and Value both
// required), Delete (Key required, Value ignored), or one of the two
// compare preconditions above. Named "Spec" (not "TxnOp") to avoid
// colliding with the generated capnp TxnOp struct type this package's
// NewTxn/TxnOps convert to/from -- see those functions' doc comments.
// See txn's doc comment in api/shmevent.capnp for the atomicity guarantee
// applying a list of these gets, and kvfsm's OpTxn case for the rule that
// every compare is evaluated -- against the committed state, inside the
// same single Apply -- before any write in the same transaction lands.
type TxnOpSpec struct {
	Op    byte
	Key   []byte
	Value []byte
}

// IsCompare reports whether op is a precondition rather than a write.
func (op TxnOpSpec) IsCompare() bool {
	return op.Op == TxnOpCompare || op.Op == TxnOpCompareAbsent
}

// ValidTxnOp reports whether op is one of the four kinds this package
// defines. Kept here rather than duplicated in pkg/daemon's txn handler
// and kvfsm's OpTxn case, which both have to reject an unknown kind and
// would otherwise drift apart as kinds are added.
func ValidTxnOp(op byte) bool {
	switch op {
	case TxnOpSet, TxnOpDelete, TxnOpCompare, TxnOpCompareAbsent:
		return true
	}
	return false
}

// NewTxn builds a txn Msg from ops -- see that variant's doc comment in
// api/shmevent.capnp.
func NewTxn(ops []TxnOpSpec) (Msg, error) {
	if len(ops) > 0xFFFF {
		return Msg{}, fmt.Errorf("shmevent: txn has too many ops: %d", len(ops))
	}
	for _, op := range ops {
		if len(op.Key) == 0 {
			return Msg{}, fmt.Errorf("shmevent: txn op has an empty key")
		}
		if !ValidTxnOp(op.Op) {
			return Msg{}, fmt.Errorf("shmevent: txn op has unknown kind %d", op.Op)
		}
		if err := checkValueSize(op.Value); err != nil {
			return Msg{}, err
		}
	}

	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetTxn()
	grp := m.Txn()
	list, err := grp.NewOps(int32(len(ops)))
	if err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_txn: %w", err)
	}
	for i, op := range ops {
		dst := list.At(i)
		dst.SetOp(op.Op)
		if err := dst.SetKey(op.Key); err != nil {
			return Msg{}, fmt.Errorf("shmevent: new_txn: op %d: %w", i, err)
		}
		if err := dst.SetValue(op.Value); err != nil {
			return Msg{}, fmt.Errorf("shmevent: new_txn: op %d: %w", i, err)
		}
	}
	return m, nil
}

// TxnOps reads a txn Msg's ops back out as []TxnOpSpec -- NewTxn's
// inverse, for a receiver (pkg/daemon) that wants to iterate them as
// plain Go values rather than the generated capnp TxnOp_List directly.
func TxnOps(grp Event_txn) ([]TxnOpSpec, error) {
	list, err := grp.Ops()
	if err != nil {
		return nil, fmt.Errorf("shmevent: txn ops: %w", err)
	}
	out := make([]TxnOpSpec, list.Len())
	for i := range out {
		src := list.At(i)
		key, err := src.Key()
		if err != nil {
			return nil, fmt.Errorf("shmevent: txn op %d key: %w", i, err)
		}
		value, err := src.Value()
		if err != nil {
			return nil, fmt.Errorf("shmevent: txn op %d value: %w", i, err)
		}
		out[i] = TxnOpSpec{Op: src.Op(), Key: key, Value: value}
	}
	return out, nil
}

// EncodeTxnPayload packs ops into a raft log entry value: a 2-byte
// big-endian op count, then each op as [1-byte Op][2-byte key
// length][key][2-byte value length][value]. Internal only -- unlike the
// wire Msg union, a raft log entry is never exchanged with a non-Go peer
// (rafttransport's own msgpack framing wraps this for the AppendEntries
// hop between raft nodes, all of which are this same Go binary), so there
// is no reason to route it through capnp; pkg/daemon's txn handler reads
// the wire Msg via TxnOps then re-encodes with this before forwarding to
// raft (kvfsm.OpTxn), the same way EventSet's key/value already travel as
// plain []byte through handleOpForward rather than as a wire Msg.
func EncodeTxnPayload(ops []TxnOpSpec) ([]byte, error) {
	if len(ops) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: txn payload has too many ops: %d", len(ops))
	}
	size := 2
	for _, op := range ops {
		if len(op.Key) == 0 {
			return nil, fmt.Errorf("shmevent: txn op has an empty key")
		}
		if len(op.Key) > 0xFFFF {
			return nil, fmt.Errorf("shmevent: txn op key too long: %d bytes", len(op.Key))
		}
		if len(op.Value) > 0xFFFF {
			return nil, fmt.Errorf("shmevent: txn op value too long: %d bytes", len(op.Value))
		}
		if !ValidTxnOp(op.Op) {
			return nil, fmt.Errorf("shmevent: txn op has unknown kind %d", op.Op)
		}
		size += 1 + 2 + len(op.Key) + 2 + len(op.Value)
	}

	buf := make([]byte, size)
	buf[0] = byte(len(ops) >> 8)
	buf[1] = byte(len(ops))
	off := 2
	for _, op := range ops {
		buf[off] = op.Op
		off++
		buf[off] = byte(len(op.Key) >> 8)
		buf[off+1] = byte(len(op.Key))
		off += 2
		off += copy(buf[off:], op.Key)
		buf[off] = byte(len(op.Value) >> 8)
		buf[off+1] = byte(len(op.Value))
		off += 2
		off += copy(buf[off:], op.Value)
	}
	return buf, nil
}

// DecodeTxnPayload is the inverse of EncodeTxnPayload.
func DecodeTxnPayload(payload []byte) ([]TxnOpSpec, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("shmevent: txn payload too short: %d bytes", len(payload))
	}
	count := int(payload[0])<<8 | int(payload[1])
	off := 2
	ops := make([]TxnOpSpec, 0, count)
	for i := range count {
		if off+1+2 > len(payload) {
			return nil, fmt.Errorf("shmevent: txn payload truncated at op %d", i)
		}
		op := payload[off]
		off++
		keyLen := int(payload[off])<<8 | int(payload[off+1])
		off += 2
		if off+keyLen+2 > len(payload) {
			return nil, fmt.Errorf("shmevent: txn payload truncated reading op %d key", i)
		}
		key := payload[off : off+keyLen]
		off += keyLen
		valueLen := int(payload[off])<<8 | int(payload[off+1])
		off += 2
		if off+valueLen > len(payload) {
			return nil, fmt.Errorf("shmevent: txn payload truncated reading op %d value", i)
		}
		value := payload[off : off+valueLen]
		off += valueLen
		ops = append(ops, TxnOpSpec{Op: op, Key: key, Value: value})
	}
	if off != len(payload) {
		return nil, fmt.Errorf("shmevent: txn payload has %d trailing bytes", len(payload)-off)
	}
	return ops, nil
}

// ParseTxnOpsString parses a human-typeable transaction description into a
// TxnOpSpec list: a space-separated list of `<key>=<value>` (a Set -- split
// on the first `=` only, so a value itself containing `=` is preserved
// verbatim), `del:<key>` (a Delete), `if:<key>=<value>` (a TxnOpCompare
// precondition) or `ifabsent:<key>` (a TxnOpCompareAbsent precondition)
// tokens. Shared by pkg/kvctl's `mage txn` target and mobile/kvmobile.Txn,
// so both bindings accept the exact same one-string grammar rather than
// each inventing (and drifting from) their own.
//
// Prefixes are checked before the `<key>=<value>` split so that a key
// literally named `if` or `del` can't be mistaken for one -- the cost is
// that those four prefixes are reserved at the *start of a token* only; a
// key containing them anywhere else is unaffected.
func ParseTxnOpsString(ops string) ([]TxnOpSpec, error) {
	fields := strings.Fields(ops)
	if len(fields) == 0 {
		return nil, fmt.Errorf("shmevent: txn: no ops given (want e.g. \"k1=v1 k2=v2 del:k3 if:k4=v4 ifabsent:k5\")")
	}
	parsed := make([]TxnOpSpec, 0, len(fields))
	for _, field := range fields {
		if key, ok := strings.CutPrefix(field, "del:"); ok {
			if key == "" {
				return nil, fmt.Errorf("shmevent: txn: %q has an empty key", field)
			}
			parsed = append(parsed, TxnOpSpec{Op: TxnOpDelete, Key: []byte(key)})
			continue
		}
		if key, ok := strings.CutPrefix(field, "ifabsent:"); ok {
			if key == "" {
				return nil, fmt.Errorf("shmevent: txn: %q has an empty key", field)
			}
			parsed = append(parsed, TxnOpSpec{Op: TxnOpCompareAbsent, Key: []byte(key)})
			continue
		}
		if rest, ok := strings.CutPrefix(field, "if:"); ok {
			key, value, found := strings.Cut(rest, "=")
			if !found || key == "" {
				return nil, fmt.Errorf("shmevent: txn: %q is not if:<key>=<value>", field)
			}
			parsed = append(parsed, TxnOpSpec{Op: TxnOpCompare, Key: []byte(key), Value: []byte(value)})
			continue
		}
		key, value, ok := strings.Cut(field, "=")
		if !ok || key == "" {
			// The most common way a well-formed op ends up here is a value
			// that itself contained whitespace: ops are space-delimited (see
			// this function's own doc comment), so "k=hello world" already
			// split into two fields ("k=hello" and this one, "world") by the
			// time either reaches here. There's no escaping syntax for a
			// space inside a value -- name the likely cause rather than
			// leaving the caller to work out why an apparently-valid op
			// failed.
			return nil, fmt.Errorf("shmevent: txn: %q is neither <key>=<value> nor del:<key> (values can't contain spaces; ops are whitespace-separated)", field)
		}
		parsed = append(parsed, TxnOpSpec{Op: TxnOpSet, Key: []byte(key), Value: []byte(value)})
	}
	return parsed, nil
}
