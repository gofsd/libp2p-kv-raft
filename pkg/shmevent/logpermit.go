package shmevent

import "fmt"

// LogPermitKey builds the pkg/store key for a KindLogPermit record:
// SystemKeyPrefix, KindLogPermit, status, then a 2-byte big-endian length
// prefix for logKind, then logKind, then peerID verbatim. Unlike
// SystemKey, two variable-length fields follow the fixed header here
// (logKind and peerID), so only the last of them -- peerID -- can go
// unprefixed; logKind needs its own length prefix so the two can't blur
// into each other (the same fix pkg/logrecord.BuildKey applies to its own
// kind/unitID fields, for the identical reason).
func LogPermitKey(status byte, logKind string, peerID []byte) ([]byte, error) {
	if len(logKind) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: log permit logKind too long: %d bytes", len(logKind))
	}
	key := make([]byte, 3+2+len(logKind)+len(peerID))
	key[0] = SystemKeyPrefix
	key[1] = KindLogPermit
	key[2] = status
	key[3] = byte(len(logKind) >> 8)
	key[4] = byte(len(logKind))
	off := 5
	off += copy(key[off:], logKind)
	copy(key[off:], peerID)
	return key, nil
}

// EncodeLogPermitRequestPayload packs logKind, peerID, and metadata into a
// single EventLogPermitRequest Msg.Value: a 2-byte big-endian length
// prefix for logKind, then logKind, then a 2-byte big-endian length
// prefix for peerID, then peerID, then metadata verbatim (the rest of the
// buffer, needing no length prefix of its own) -- mirrors
// EncodePermitRequestPayload, except logKind is a string needing its own
// prefix (EncodePermitRequestPayload's kind is a single fixed byte) in
// place of that single kind byte.
func EncodeLogPermitRequestPayload(logKind string, peerID, metadata []byte) ([]byte, error) {
	if len(logKind) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: log permit request logKind too long: %d bytes", len(logKind))
	}
	if len(peerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: log permit request peerID too long: %d bytes", len(peerID))
	}
	buf := make([]byte, 2+len(logKind)+2+len(peerID)+len(metadata))
	buf[0] = byte(len(logKind) >> 8)
	buf[1] = byte(len(logKind))
	off := 2
	off += copy(buf[off:], logKind)
	buf[off] = byte(len(peerID) >> 8)
	buf[off+1] = byte(len(peerID))
	off += 2
	off += copy(buf[off:], peerID)
	copy(buf[off:], metadata)
	return buf, nil
}

// DecodeLogPermitRequestPayload is the inverse of
// EncodeLogPermitRequestPayload.
func DecodeLogPermitRequestPayload(payload []byte) (logKind string, peerID, metadata []byte, err error) {
	if len(payload) < 2 {
		return "", nil, nil, fmt.Errorf("shmevent: log permit request payload too short: %d bytes", len(payload))
	}
	kindLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+kindLen > len(payload) {
		return "", nil, nil, fmt.Errorf("shmevent: log permit request logKind length %d exceeds payload size %d", kindLen, len(payload))
	}
	logKind = string(payload[off : off+kindLen])
	off += kindLen
	if off+2 > len(payload) {
		return "", nil, nil, fmt.Errorf("shmevent: log permit request payload too short for peerID length")
	}
	idLen := int(payload[off])<<8 | int(payload[off+1])
	off += 2
	if off+idLen > len(payload) {
		return "", nil, nil, fmt.Errorf("shmevent: log permit request peerID length %d exceeds payload size %d", idLen, len(payload))
	}
	return logKind, payload[off : off+idLen], payload[off+idLen:], nil
}

// EncodeLogPermitConfirmPayload packs logKind and peerID (the rest of the
// buffer) into a single EventLogPermitConfirm/EventLogPermitRevoke
// Msg.Value -- no metadata field, mirroring EncodePermitConfirmPayload's
// reasoning (the daemon reads the pending record's own value back out of
// the store rather than trusting the caller to resend it), with logKind's
// own length prefix taking the place of EncodePermitConfirmPayload's
// single kind byte.
func EncodeLogPermitConfirmPayload(logKind string, peerID []byte) ([]byte, error) {
	if len(logKind) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: log permit confirm logKind too long: %d bytes", len(logKind))
	}
	buf := make([]byte, 2+len(logKind)+len(peerID))
	buf[0] = byte(len(logKind) >> 8)
	buf[1] = byte(len(logKind))
	off := 2
	off += copy(buf[off:], logKind)
	copy(buf[off:], peerID)
	return buf, nil
}

// DecodeLogPermitConfirmPayload is the inverse of
// EncodeLogPermitConfirmPayload.
func DecodeLogPermitConfirmPayload(payload []byte) (logKind string, peerID []byte, err error) {
	if len(payload) < 2 {
		return "", nil, fmt.Errorf("shmevent: log permit confirm payload too short: %d bytes", len(payload))
	}
	kindLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+kindLen > len(payload) {
		return "", nil, fmt.Errorf("shmevent: log permit confirm logKind length %d exceeds payload size %d", kindLen, len(payload))
	}
	return string(payload[off : off+kindLen]), payload[off+kindLen:], nil
}
