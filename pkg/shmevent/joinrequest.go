package shmevent

import "fmt"

// NewJoinRequestCreate builds a joinRequestCreate request Msg: local-only,
// mints a fresh correlation token held purely in this daemon's own
// process memory. The response reuses this same variant with token
// filled in.
func NewJoinRequestCreate() (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetJoinRequestCreate()
	return m, nil
}

// NewJoinRequestCancel builds a joinRequestCancel Msg: invalidates a
// still-pending join-request token.
func NewJoinRequestCancel(token []byte) (Msg, error) {
	if len(token) != JoinInviteTokenSize {
		return Msg{}, fmt.Errorf("shmevent: join request token must be %d bytes, got %d", JoinInviteTokenSize, len(token))
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetJoinRequestCancel()
	if err := m.JoinRequestCancel().SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_join_request_cancel: %w", err)
	}
	return m, nil
}

// NewRecruit builds a recruit Msg: local-only, splits ticket
// ("<device's own multiaddr>#<tokenHex>", see NewJoinRequestCreate),
// mints a normal join invite, and dials the device directly to hand it
// over.
func NewRecruit(ticket string, suffrage byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetRecruit()
	grp := m.Recruit()
	if err := grp.SetTicket(ticket); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_recruit: %w", err)
	}
	grp.SetSuffrage(suffrage)
	return m, nil
}

// NewJoinRequestTicket builds a joinRequestTicket Msg: an offline
// signed-ticket wire format (never dispatched live) -- see that variant's
// doc comment in api/shmevent.capnp.
func NewJoinRequestTicket(sourceAddr string, token []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetJoinRequestTicket()
	grp := m.JoinRequestTicket()
	if err := grp.SetSourceAddr(sourceAddr); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_join_request_ticket: %w", err)
	}
	if err := grp.SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_join_request_ticket: %w", err)
	}
	return m, nil
}
