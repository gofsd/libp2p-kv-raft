package shmevent

import (
	"fmt"
	"strings"
)

// TxnOp.Op's valid values. TxnOpSet/TxnOpDelete are plain key writes and
// deletes; TxnOpCompare/TxnOpCompareAbsent are *preconditions* that write
// nothing and instead decide whether the rest of the transaction applies at
// all -- the compare-and-swap shape this type's doc comment used to name as
// something a real transaction "might eventually grow into".
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
// IPC as an EventError, and the client turns it back into a typed result
// (pkg/shmclient.ErrCompareFailed, mobile/kvmobile.CompareAndSwap's plain
// false). Matching on a constant both ends import beats each end hard-coding
// the same string and only finding out they disagree when a retry loop
// silently stops retrying.
const CompareFailedMarker = "txn: compare failed"

// TxnOp is one operation within an EventTxn request: Set (Key and Value both
// required), Delete (Key required, Value ignored), or one of the two compare
// preconditions above. See EventTxn's doc comment for the atomicity guarantee
// applying a list of these gets, and kvfsm's OpTxn case for the rule that
// every compare is evaluated -- against the committed state, inside the same
// single Apply -- before any write in the same transaction lands.
type TxnOp struct {
	Op    byte
	Key   []byte
	Value []byte
}

// IsCompare reports whether op is a precondition rather than a write.
func (op TxnOp) IsCompare() bool {
	return op.Op == TxnOpCompare || op.Op == TxnOpCompareAbsent
}

// ValidTxnOp reports whether op is one of the four kinds this package
// defines. Kept here rather than duplicated in pkg/daemon's EventTxn
// handler and kvfsm's OpTxn case, which both have to reject an unknown
// kind and would otherwise drift apart as kinds are added.
func ValidTxnOp(op byte) bool {
	switch op {
	case TxnOpSet, TxnOpDelete, TxnOpCompare, TxnOpCompareAbsent:
		return true
	}
	return false
}

// EncodeTxnPayload packs ops into a single EventTxn Msg.Value: a 2-byte
// big-endian op count, then each op as [1-byte Op][2-byte key
// length][key][2-byte value length][value] -- every field explicitly
// length-prefixed (unlike EncodeSetPayload's "last field takes the rest of
// the buffer" trick) since more than one op can follow within the same
// Value.
func EncodeTxnPayload(ops []TxnOp) ([]byte, error) {
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
func DecodeTxnPayload(payload []byte) ([]TxnOp, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("shmevent: txn payload too short: %d bytes", len(payload))
	}
	count := int(payload[0])<<8 | int(payload[1])
	off := 2
	ops := make([]TxnOp, 0, count)
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
		ops = append(ops, TxnOp{Op: op, Key: key, Value: value})
	}
	if off != len(payload) {
		return nil, fmt.Errorf("shmevent: txn payload has %d trailing bytes", len(payload)-off)
	}
	return ops, nil
}

// ParseTxnOpsString parses a human-typeable transaction description into a
// TxnOp list: a space-separated list of `<key>=<value>` (a Set -- split on
// the first `=` only, so a value itself containing `=` is preserved
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
func ParseTxnOpsString(ops string) ([]TxnOp, error) {
	fields := strings.Fields(ops)
	if len(fields) == 0 {
		return nil, fmt.Errorf("shmevent: txn: no ops given (want e.g. \"k1=v1 k2=v2 del:k3 if:k4=v4 ifabsent:k5\")")
	}
	parsed := make([]TxnOp, 0, len(fields))
	for _, field := range fields {
		if key, ok := strings.CutPrefix(field, "del:"); ok {
			if key == "" {
				return nil, fmt.Errorf("shmevent: txn: %q has an empty key", field)
			}
			parsed = append(parsed, TxnOp{Op: TxnOpDelete, Key: []byte(key)})
			continue
		}
		if key, ok := strings.CutPrefix(field, "ifabsent:"); ok {
			if key == "" {
				return nil, fmt.Errorf("shmevent: txn: %q has an empty key", field)
			}
			parsed = append(parsed, TxnOp{Op: TxnOpCompareAbsent, Key: []byte(key)})
			continue
		}
		if rest, ok := strings.CutPrefix(field, "if:"); ok {
			key, value, found := strings.Cut(rest, "=")
			if !found || key == "" {
				return nil, fmt.Errorf("shmevent: txn: %q is not if:<key>=<value>", field)
			}
			parsed = append(parsed, TxnOp{Op: TxnOpCompare, Key: []byte(key), Value: []byte(value)})
			continue
		}
		key, value, ok := strings.Cut(field, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("shmevent: txn: %q is neither <key>=<value> nor del:<key>", field)
		}
		parsed = append(parsed, TxnOp{Op: TxnOpSet, Key: []byte(key), Value: []byte(value)})
	}
	return parsed, nil
}
