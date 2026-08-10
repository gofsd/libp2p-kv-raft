package shmevent

import (
	"encoding/binary"
	"fmt"
)

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

// execInviteRecordV2Marker flags the versioned (TTL-carrying) payload shape
// below -- the same "impossible v1 length" trick CreateCommandWithSpec's own
// v2 payload uses (see that function's doc comment): a real commandID never
// approaches 0xFFFF bytes, so a v1 reader would only ever see this value if
// the payload actually is v2.
const execInviteRecordV2Marker = 0xFFFF

// EncodeExecInviteRecord packs the commandID, inputsJSON, and expiresAtUnix
// (0 meaning no expiry) a KindExecInvite record grants into its stored
// value. expiresAtUnix==0 is written in the original v1 shape, byte for
// byte -- a 2-byte big-endian length prefix for commandID, then commandID,
// then inputsJSON verbatim (trailing field, no prefix needed), the same
// shape EncodeCommandPayload uses for its own name+peerID pair -- so an
// invite with no TTL (the default) costs nothing extra on the wire or in
// the store, and every already-replicated pre-TTL record still decodes
// unchanged. A nonzero expiresAtUnix instead writes execInviteRecordV2Marker,
// followed by an 8-byte big-endian expiresAtUnix, then the real 2-byte
// commandID length, then commandID, then inputsJSON.
func EncodeExecInviteRecord(commandID, inputsJSON string, expiresAtUnix uint64) []byte {
	if expiresAtUnix == 0 {
		buf := make([]byte, 2+len(commandID)+len(inputsJSON))
		buf[0] = byte(len(commandID) >> 8)
		buf[1] = byte(len(commandID))
		off := 2
		off += copy(buf[off:], commandID)
		copy(buf[off:], inputsJSON)
		return buf
	}
	buf := make([]byte, 2+8+2+len(commandID)+len(inputsJSON))
	buf[0], buf[1] = 0xFF, 0xFF // execInviteRecordV2Marker
	binary.BigEndian.PutUint64(buf[2:10], expiresAtUnix)
	buf[10] = byte(len(commandID) >> 8)
	buf[11] = byte(len(commandID))
	off := 12
	off += copy(buf[off:], commandID)
	copy(buf[off:], inputsJSON)
	return buf
}

// DecodeExecInviteRecord is the inverse of EncodeExecInviteRecord.
// expiresAtUnix is 0 for a v1 (no-TTL) record.
func DecodeExecInviteRecord(payload []byte) (commandID, inputsJSON string, expiresAtUnix uint64, err error) {
	if len(payload) < 2 {
		return "", "", 0, fmt.Errorf("shmevent: exec invite record too short: %d bytes", len(payload))
	}
	cmdLen := int(payload[0])<<8 | int(payload[1])
	if cmdLen == execInviteRecordV2Marker {
		if len(payload) < 12 {
			return "", "", 0, fmt.Errorf("shmevent: exec invite record too short for v2 header: %d bytes", len(payload))
		}
		expiresAtUnix = binary.BigEndian.Uint64(payload[2:10])
		realCmdLen := int(payload[10])<<8 | int(payload[11])
		off := 12
		if off+realCmdLen > len(payload) {
			return "", "", 0, fmt.Errorf("shmevent: exec invite record commandID length %d exceeds payload size %d", realCmdLen, len(payload))
		}
		commandID = string(payload[off : off+realCmdLen])
		off += realCmdLen
		return commandID, string(payload[off:]), expiresAtUnix, nil
	}
	off := 2
	if off+cmdLen > len(payload) {
		return "", "", 0, fmt.Errorf("shmevent: exec invite record commandID length %d exceeds payload size %d", cmdLen, len(payload))
	}
	commandID = string(payload[off : off+cmdLen])
	off += cmdLen
	return commandID, string(payload[off:]), 0, nil
}

// NewExecInviteCreate builds an execInviteCreate Msg: mints a one-time
// execution-invite token bound to commandID/inputsJSON. ttlSeconds is how
// long the invite stays redeemable from the moment this create is applied,
// 0 meaning no expiry (the default) -- see pkg/daemon's
// Event_Which_execInviteCreate handler for where that gets turned into the
// absolute expiresAtUnix actually persisted (shmevent.EncodeExecInviteRecord),
// and kvfsm.OpConsumeExecInvite for where it's enforced.
func NewExecInviteCreate(token []byte, commandID, inputsJSON string, ttlSeconds uint64) (Msg, error) {
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
	grp.SetTtlSeconds(ttlSeconds)
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
