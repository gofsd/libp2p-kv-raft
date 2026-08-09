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

// NewExecInviteCreate builds an execInviteCreate Msg: mints a one-time
// execution-invite token bound to commandID/inputsJSON.
func NewExecInviteCreate(token []byte, commandID, inputsJSON string) (Msg, error) {
	if len(token) != ExecInviteTokenSize {
		return Msg{}, fmt.Errorf("shmevent: exec invite token must be %d bytes, got %d", ExecInviteTokenSize, len(token))
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetExecInviteCreate()
	grp := m.ExecInviteCreate()
	if err := grp.SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_create: %w", err)
	}
	if err := grp.SetCommandId(commandID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_create: %w", err)
	}
	if err := grp.SetInputsJson(inputsJSON); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_create: %w", err)
	}
	return m, nil
}

// NewExecInviteRevoke builds an execInviteRevoke Msg: invalidates a
// still-unredeemed execution-invite token.
func NewExecInviteRevoke(token []byte) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetExecInviteRevoke()
	if err := m.ExecInviteRevoke().SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_revoke: %w", err)
	}
	return m, nil
}

// NewExecInviteRedeem builds an execInviteRedeem request Msg: local-only,
// tells this node's own daemon to dial sourceAddr and redeem token there
// under this node's own identity. The response reuses this same variant
// with instanceId filled in.
func NewExecInviteRedeem(sourceAddr string, token []byte) (Msg, error) {
	if len(token) != ExecInviteTokenSize {
		return Msg{}, fmt.Errorf("shmevent: exec invite token must be %d bytes, got %d", ExecInviteTokenSize, len(token))
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetExecInviteRedeem()
	grp := m.ExecInviteRedeem()
	if err := grp.SetSourceAddr(sourceAddr); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_redeem: %w", err)
	}
	if err := grp.SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_redeem: %w", err)
	}
	return m, nil
}

// NewExecInviteRedeemNotification builds an execInviteRedeem Msg in its
// network-leg shape: redeemerPeerId/token set, sourceAddr/instanceId left
// empty -- what pkg/daemon's dialAndRedeemExecInvite actually places on
// the wire as ExecInviteRedeemProtocolID's request, naming this node's
// own identity directly since the receiver has no way to look up a
// registry entry across processes. The response on that leg is a bare
// text line, not a shmevent Msg at all.
func NewExecInviteRedeemNotification(redeemerPeerID string, token []byte) (Msg, error) {
	if len(token) != ExecInviteTokenSize {
		return Msg{}, fmt.Errorf("shmevent: exec invite token must be %d bytes, got %d", ExecInviteTokenSize, len(token))
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetExecInviteRedeem()
	grp := m.ExecInviteRedeem()
	if err := grp.SetRedeemerPeerId(redeemerPeerID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_redeem_notification: %w", err)
	}
	if err := grp.SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_invite_redeem_notification: %w", err)
	}
	return m, nil
}

// NewExecTicket builds an execTicket Msg: an offline signed-ticket wire
// format (never dispatched live) -- see that variant's doc comment in
// api/shmevent.capnp.
func NewExecTicket(sourceAddr string, token []byte) (Msg, error) {
	if len(token) != ExecInviteTokenSize {
		return Msg{}, fmt.Errorf("shmevent: exec invite token must be %d bytes, got %d", ExecInviteTokenSize, len(token))
	}
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetExecTicket()
	grp := m.ExecTicket()
	if err := grp.SetSourceAddr(sourceAddr); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_ticket: %w", err)
	}
	if err := grp.SetToken(token); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_exec_ticket: %w", err)
	}
	return m, nil
}
