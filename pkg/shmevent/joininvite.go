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

// NewJoinInviteCreate builds a joinInviteCreate Msg: mints a one-time
// join-invite token, redeemable by any device presenting it.
func NewJoinInviteCreate(token []byte, suffrage byte) (Msg, error) {
	if len(token) != JoinInviteTokenSize {
		return Msg{}, fmt.Errorf("shmevent: join invite token must be %d bytes, got %d", JoinInviteTokenSize, len(token))
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetJoinInviteCreate()
	grp := m.JoinInviteCreate()
	if err := grp.SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_join_invite_create: %w", err)
	}
	grp.SetSuffrage(suffrage)
	return m, nil
}

// NewJoinInviteRevoke builds a joinInviteRevoke Msg: invalidates a
// still-unredeemed join-invite token.
func NewJoinInviteRevoke(token []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetJoinInviteRevoke()
	if err := m.JoinInviteRevoke().SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_join_invite_revoke: %w", err)
	}
	return m, nil
}

// NewJoinTicket builds a joinTicket Msg: an offline signed-ticket wire
// format (never dispatched live) -- see that variant's doc comment in
// api/shmevent.capnp.
func NewJoinTicket(sourceAddr string, token []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetJoinTicket()
	grp := m.JoinTicket()
	if err := grp.SetSourceAddr(sourceAddr); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_join_ticket: %w", err)
	}
	if err := grp.SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_join_ticket: %w", err)
	}
	return m, nil
}
