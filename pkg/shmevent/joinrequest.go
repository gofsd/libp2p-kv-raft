package shmevent

import "fmt"

// EncodeJoinRequestCancelPayload packs token (the join-request ticket to
// clear) as an EventJoinRequestCancel Msg.Value -- same shape as
// EncodeJoinInviteRevokePayload, kept as its own named pair since the two
// events guard unrelated concepts (a raft-backed KindJoinInvite record vs.
// this node's own in-memory pending ticket) even though the wire shape
// happens to match.
func EncodeJoinRequestCancelPayload(token []byte) []byte {
	buf := make([]byte, len(token))
	copy(buf, token)
	return buf
}

// DecodeJoinRequestCancelPayload is the inverse of
// EncodeJoinRequestCancelPayload.
func DecodeJoinRequestCancelPayload(payload []byte) (token []byte, err error) {
	if len(payload) != JoinInviteTokenSize {
		return nil, fmt.Errorf("shmevent: join request cancel payload must be %d bytes, got %d", JoinInviteTokenSize, len(payload))
	}
	return payload, nil
}

// EncodeRecruitPayload packs ticket ("<device's own multiaddr>#<tokenHex>",
// see EventJoinRequestCreate) and the suffrage the redeemed device should be
// admitted with into a single EventRecruit Msg.Value: suffrage first (fixed
// size, so it needs no length prefix), then ticket verbatim as the trailing
// field, mirroring EncodeJoinInviteCreatePayload's token-then-suffrage
// convention with the fixed/variable fields swapped since here it's the
// variable-length field that must be last.
func EncodeRecruitPayload(ticket string, suffrage byte) []byte {
	buf := make([]byte, 1+len(ticket))
	buf[0] = suffrage
	copy(buf[1:], ticket)
	return buf
}

// DecodeRecruitPayload is the inverse of EncodeRecruitPayload.
func DecodeRecruitPayload(payload []byte) (ticket string, suffrage byte, err error) {
	if len(payload) < 1 {
		return "", 0, fmt.Errorf("shmevent: recruit payload must be at least 1 byte, got %d", len(payload))
	}
	return string(payload[1:]), payload[0], nil
}
