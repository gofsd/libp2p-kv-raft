package shmevent

import "fmt"

// joinInviteStatusPlaceholder mirrors catalogStatusPlaceholder:
// KindJoinInvite has no pending/confirmed lifecycle either (see that
// constant's doc comment in system.go) -- a record simply exists (valid,
// unredeemed) or doesn't (never created, or already consumed).
const joinInviteStatusPlaceholder = 0x00

// JoinInviteTokenSize is every invite token's fixed length in bytes --
// generated with crypto/rand by whoever creates the invite (see
// EncodeJoinInviteCreatePayload), never chosen or predictable by the
// device that will eventually redeem it. Fixed-size so the token needs no
// length prefix anywhere it's encoded (JoinInviteKey's trailing field,
// EncodeJoinInviteCreatePayload's leading one) -- 16 bytes (128 bits) is
// the same size crypto/rand-backed UUIDs use, comfortably infeasible to
// guess.
const JoinInviteTokenSize = 16

// JoinInviteKey builds the pkg/store key for a KindJoinInvite record: the
// token itself is the trailing (and only variable-length) field, exactly
// like GroupKey/ClusterMemberKey, but unlike those, token is never a
// peer id -- see KindJoinInvite's doc comment on why redemption has
// nothing else to key on.
func JoinInviteKey(token []byte) []byte {
	return SystemKey(KindJoinInvite, joinInviteStatusPlaceholder, token)
}

// EncodeJoinInviteRecord packs the suffrage a KindJoinInvite record grants
// into its stored value -- just the one byte, since token (the record's
// own key) is the only other field redemption needs, and that's already
// available from the key itself.
func EncodeJoinInviteRecord(suffrage byte) []byte {
	return []byte{suffrage}
}

// DecodeJoinInviteRecord is the inverse of EncodeJoinInviteRecord.
func DecodeJoinInviteRecord(payload []byte) (suffrage byte, err error) {
	if len(payload) != 1 {
		return 0, fmt.Errorf("shmevent: join invite record must be 1 byte, got %d", len(payload))
	}
	return payload[0], nil
}

// EncodeJoinInviteCreatePayload packs a freshly generated token (see
// JoinInviteTokenSize) and the suffrage it should grant into a single
// EventJoinInviteCreate Msg.Value: token first (fixed size, so it needs no
// length prefix), then suffrage as the trailing byte.
func EncodeJoinInviteCreatePayload(token []byte, suffrage byte) ([]byte, error) {
	if len(token) != JoinInviteTokenSize {
		return nil, fmt.Errorf("shmevent: join invite token must be %d bytes, got %d", JoinInviteTokenSize, len(token))
	}
	buf := make([]byte, JoinInviteTokenSize+1)
	copy(buf, token)
	buf[JoinInviteTokenSize] = suffrage
	return buf, nil
}

// DecodeJoinInviteCreatePayload is the inverse of
// EncodeJoinInviteCreatePayload.
func DecodeJoinInviteCreatePayload(payload []byte) (token []byte, suffrage byte, err error) {
	if len(payload) != JoinInviteTokenSize+1 {
		return nil, 0, fmt.Errorf("shmevent: join invite create payload must be %d bytes, got %d", JoinInviteTokenSize+1, len(payload))
	}
	return payload[:JoinInviteTokenSize], payload[JoinInviteTokenSize], nil
}

// EncodeJoinInviteRevokePayload packs token (the invite to revoke) as an
// EventJoinInviteRevoke Msg.Value -- the whole payload is the token, no
// other field, named for symmetry with this package's other
// EncodeXPayload/DecodeXPayload pairs.
func EncodeJoinInviteRevokePayload(token []byte) []byte {
	buf := make([]byte, len(token))
	copy(buf, token)
	return buf
}

// DecodeJoinInviteRevokePayload is the inverse of
// EncodeJoinInviteRevokePayload.
func DecodeJoinInviteRevokePayload(payload []byte) (token []byte, err error) {
	if len(payload) != JoinInviteTokenSize {
		return nil, fmt.Errorf("shmevent: join invite revoke payload must be %d bytes, got %d", JoinInviteTokenSize, len(payload))
	}
	return payload, nil
}
