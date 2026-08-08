package shmevent

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// EncodeDialSubmitCommandRequest packs the four fields an
// EventDialSubmitCommand Msg.Value carries: targetAddr, commandID, and
// inputsJSON each 2-byte-length-prefixed (chained the same way
// EncodeExecInviteRecord prefixes commandID before its own trailing
// inputsJSON), then note trailing raw with no prefix of its own -- it's the
// last field, so its length is whatever remains.
func EncodeDialSubmitCommandRequest(targetAddr, commandID, inputsJSON, note string) ([]byte, error) {
	for name, s := range map[string]string{"targetAddr": targetAddr, "commandID": commandID, "inputsJSON": inputsJSON} {
		if len(s) > 0xFFFF {
			return nil, fmt.Errorf("shmevent: dial submit command %s too long: %d bytes", name, len(s))
		}
	}
	buf := make([]byte, 0, 6+len(targetAddr)+len(commandID)+len(inputsJSON)+len(note))
	for _, s := range []string{targetAddr, commandID, inputsJSON} {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
		buf = append(buf, s...)
	}
	buf = append(buf, note...)
	return buf, nil
}

// DecodeDialSubmitCommandRequest is the inverse of
// EncodeDialSubmitCommandRequest.
func DecodeDialSubmitCommandRequest(payload []byte) (targetAddr, commandID, inputsJSON, note string, err error) {
	fields := make([]string, 0, 3)
	off := 0
	for i := 0; i < 3; i++ {
		if off+2 > len(payload) {
			return "", "", "", "", fmt.Errorf("shmevent: dial submit command request too short: %d bytes", len(payload))
		}
		l := int(binary.BigEndian.Uint16(payload[off:]))
		off += 2
		if off+l > len(payload) {
			return "", "", "", "", fmt.Errorf("shmevent: dial submit command request field length %d exceeds remaining payload", l)
		}
		fields = append(fields, string(payload[off:off+l]))
		off += l
	}
	return fields[0], fields[1], fields[2], string(payload[off:]), nil
}

// EncodeDialQueryCommandLogRequest packs targetAddr and instanceID
// length-prefixed (same chained shape as EncodeDialSubmitCommandRequest's
// first two fields), then since/until as big-endian UnixNano int64s and
// limit as a big-endian int32 -- all fixed-size trailing fields, so no
// further prefixing is needed once the two variable-length strings are
// past.
func EncodeDialQueryCommandLogRequest(targetAddr, instanceID string, since, until time.Time, limit int) ([]byte, error) {
	if len(targetAddr) > 0xFFFF || len(instanceID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: dial query command log targetAddr/instanceID too long")
	}
	buf := make([]byte, 0, 4+len(targetAddr)+len(instanceID)+8+8+4)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(targetAddr)))
	buf = append(buf, targetAddr...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(instanceID)))
	buf = append(buf, instanceID...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(since.UnixNano()))
	buf = binary.BigEndian.AppendUint64(buf, uint64(until.UnixNano()))
	buf = binary.BigEndian.AppendUint32(buf, uint32(int32(limit)))
	return buf, nil
}

// DecodeDialQueryCommandLogRequest is the inverse of
// EncodeDialQueryCommandLogRequest. A zero since/until (UnixNano == 0,
// i.e. the Unix epoch) round-trips as time.Time{}'s IsZero() would not --
// callers that care about "no bound given" should treat epoch specially,
// the same convention logrecord.ScanBounds callers already use.
func DecodeDialQueryCommandLogRequest(payload []byte) (targetAddr, instanceID string, since, until time.Time, limit int, err error) {
	off := 0
	strs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		if off+2 > len(payload) {
			return "", "", time.Time{}, time.Time{}, 0, fmt.Errorf("shmevent: dial query command log request too short: %d bytes", len(payload))
		}
		l := int(binary.BigEndian.Uint16(payload[off:]))
		off += 2
		if off+l > len(payload) {
			return "", "", time.Time{}, time.Time{}, 0, fmt.Errorf("shmevent: dial query command log request field length %d exceeds remaining payload", l)
		}
		strs = append(strs, string(payload[off:off+l]))
		off += l
	}
	if off+20 > len(payload) {
		return "", "", time.Time{}, time.Time{}, 0, fmt.Errorf("shmevent: dial query command log request missing since/until/limit")
	}
	since = time.Unix(0, int64(binary.BigEndian.Uint64(payload[off:])))
	off += 8
	until = time.Unix(0, int64(binary.BigEndian.Uint64(payload[off:])))
	off += 8
	limit = int(int32(binary.BigEndian.Uint32(payload[off:])))
	return strs[0], strs[1], since, until, limit, nil
}

// EncodeLogRecordList packs records as a sequence of
// 2-byte-length-prefixed logrecord.Record.Encode() blobs -- the response
// shape for EventDialQueryCommandLog, mirroring how the request side
// chains length-prefixed fields above.
func EncodeLogRecordList(records []logrecord.Record) ([]byte, error) {
	buf := make([]byte, 0, len(records)*64)
	for i, r := range records {
		encoded, err := r.Encode()
		if err != nil {
			return nil, fmt.Errorf("shmevent: encode log record list: record %d: %w", i, err)
		}
		if len(encoded) > 0xFFFF {
			return nil, fmt.Errorf("shmevent: encode log record list: record %d too large: %d bytes", i, len(encoded))
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(encoded)))
		buf = append(buf, encoded...)
	}
	return buf, nil
}

// DecodeLogRecordList is the inverse of EncodeLogRecordList.
func DecodeLogRecordList(payload []byte) ([]logrecord.Record, error) {
	var records []logrecord.Record
	off := 0
	for off < len(payload) {
		if off+2 > len(payload) {
			return nil, fmt.Errorf("shmevent: log record list truncated at offset %d", off)
		}
		l := int(binary.BigEndian.Uint16(payload[off:]))
		off += 2
		if off+l > len(payload) {
			return nil, fmt.Errorf("shmevent: log record list entry length %d exceeds remaining payload at offset %d", l, off)
		}
		rec, err := logrecord.Decode(payload[off : off+l])
		if err != nil {
			return nil, fmt.Errorf("shmevent: decode log record list entry at offset %d: %w", off, err)
		}
		records = append(records, rec)
		off += l
	}
	return records, nil
}
