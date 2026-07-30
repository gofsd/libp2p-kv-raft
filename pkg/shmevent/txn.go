package shmevent

import "fmt"

// TxnOpSet and TxnOpDelete are TxnOp.Op's two valid values -- EventTxn only
// ever expresses plain key writes/deletes, not the compare-and-swap or
// read-modify-write shapes a real "transaction" might eventually grow into
// (see TxnOp's doc comment).
const (
	TxnOpSet    byte = 1
	TxnOpDelete byte = 2
)

// TxnOp is one operation within an EventTxn request: Set (Key and Value
// both required) or Delete (Key required, Value ignored). See EventTxn's
// doc comment for the atomicity guarantee applying a list of these gets.
type TxnOp struct {
	Op    byte
	Key   []byte
	Value []byte
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
		switch op.Op {
		case TxnOpSet, TxnOpDelete:
		default:
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
