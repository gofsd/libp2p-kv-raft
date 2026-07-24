package shmevent

import "fmt"

// execInviteStatusPlaceholder mirrors joinInviteStatusPlaceholder:
// KindExecInvite has no pending/confirmed lifecycle either -- a record
// simply exists (valid, unredeemed) or doesn't (never created, already
// consumed, or revoked).
const execInviteStatusPlaceholder = 0x00

// ExecInviteTokenSize is every execution-invite token's fixed length in
// bytes -- generated with crypto/rand by whoever creates the invite (see
// EncodeExecInviteCreatePayload), same size and same reasoning as
// JoinInviteTokenSize.
const ExecInviteTokenSize = 16

// ExecInviteKey builds the pkg/store key for a KindExecInvite record: the
// token itself is the trailing (and only variable-length) field, exactly
// like JoinInviteKey.
func ExecInviteKey(token []byte) []byte {
	return SystemKey(KindExecInvite, execInviteStatusPlaceholder, token)
}

// EncodeExecInviteRecord packs the commandID and inputsJSON a KindExecInvite
// record grants into its stored value -- a 2-byte big-endian length prefix
// for commandID, then commandID, then inputsJSON verbatim (trailing field,
// no prefix needed), the same shape EncodeCommandPayload uses for its own
// name+peerID pair.
func EncodeExecInviteRecord(commandID, inputsJSON string) []byte {
	buf := make([]byte, 2+len(commandID)+len(inputsJSON))
	buf[0] = byte(len(commandID) >> 8)
	buf[1] = byte(len(commandID))
	off := 2
	off += copy(buf[off:], commandID)
	copy(buf[off:], inputsJSON)
	return buf
}

// DecodeExecInviteRecord is the inverse of EncodeExecInviteRecord.
func DecodeExecInviteRecord(payload []byte) (commandID, inputsJSON string, err error) {
	if len(payload) < 2 {
		return "", "", fmt.Errorf("shmevent: exec invite record too short: %d bytes", len(payload))
	}
	cmdLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+cmdLen > len(payload) {
		return "", "", fmt.Errorf("shmevent: exec invite record commandID length %d exceeds payload size %d", cmdLen, len(payload))
	}
	commandID = string(payload[off : off+cmdLen])
	off += cmdLen
	return commandID, string(payload[off:]), nil
}

// EncodeExecInviteCreatePayload packs a freshly generated token (see
// ExecInviteTokenSize) and the commandID+inputsJSON it should grant into a
// single EventExecInviteCreate Msg.Value: token first (fixed size, no
// length prefix needed), then EncodeExecInviteRecord's own encoding
// trailing.
func EncodeExecInviteCreatePayload(token []byte, commandID, inputsJSON string) ([]byte, error) {
	if len(token) != ExecInviteTokenSize {
		return nil, fmt.Errorf("shmevent: exec invite token must be %d bytes, got %d", ExecInviteTokenSize, len(token))
	}
	record := EncodeExecInviteRecord(commandID, inputsJSON)
	buf := make([]byte, ExecInviteTokenSize+len(record))
	copy(buf, token)
	copy(buf[ExecInviteTokenSize:], record)
	return buf, nil
}

// DecodeExecInviteCreatePayload is the inverse of
// EncodeExecInviteCreatePayload.
func DecodeExecInviteCreatePayload(payload []byte) (token []byte, commandID, inputsJSON string, err error) {
	if len(payload) < ExecInviteTokenSize {
		return nil, "", "", fmt.Errorf("shmevent: exec invite create payload too short: %d bytes", len(payload))
	}
	token = payload[:ExecInviteTokenSize]
	commandID, inputsJSON, err = DecodeExecInviteRecord(payload[ExecInviteTokenSize:])
	if err != nil {
		return nil, "", "", err
	}
	return token, commandID, inputsJSON, nil
}

// EncodeExecInviteRevokePayload packs token (the invite to revoke) as an
// EventExecInviteRevoke Msg.Value -- the whole payload is the token, no
// other field, mirroring EncodeJoinInviteRevokePayload.
func EncodeExecInviteRevokePayload(token []byte) []byte {
	buf := make([]byte, len(token))
	copy(buf, token)
	return buf
}

// DecodeExecInviteRevokePayload is the inverse of
// EncodeExecInviteRevokePayload.
func DecodeExecInviteRevokePayload(payload []byte) (token []byte, err error) {
	if len(payload) != ExecInviteTokenSize {
		return nil, fmt.Errorf("shmevent: exec invite revoke payload must be %d bytes, got %d", ExecInviteTokenSize, len(payload))
	}
	return payload, nil
}

// EncodeExecInviteRedeemRequest packs sourceAddr and token into a single
// EventExecInviteRedeem Msg.Value -- this is the *local-only* shmring
// payload a redeeming peer's own CLI sends to its own daemon ("dial
// sourceAddr and redeem token"), distinct from the signed, self-contained
// message that daemon then sends over the network (see
// EncodeExecuteNotification, reused as-is for that leg). token is
// fixed-size so it can go first with no length prefix; sourceAddr trails
// as the rest of the buffer.
func EncodeExecInviteRedeemRequest(sourceAddr string, token []byte) ([]byte, error) {
	if len(token) != ExecInviteTokenSize {
		return nil, fmt.Errorf("shmevent: exec invite token must be %d bytes, got %d", ExecInviteTokenSize, len(token))
	}
	buf := make([]byte, ExecInviteTokenSize+len(sourceAddr))
	copy(buf, token)
	copy(buf[ExecInviteTokenSize:], sourceAddr)
	return buf, nil
}

// DecodeExecInviteRedeemRequest is the inverse of
// EncodeExecInviteRedeemRequest.
func DecodeExecInviteRedeemRequest(payload []byte) (sourceAddr string, token []byte, err error) {
	if len(payload) < ExecInviteTokenSize {
		return "", nil, fmt.Errorf("shmevent: exec invite redeem request too short: %d bytes", len(payload))
	}
	return string(payload[ExecInviteTokenSize:]), payload[:ExecInviteTokenSize], nil
}
