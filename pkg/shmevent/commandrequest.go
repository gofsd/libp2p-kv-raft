package shmevent

import (
	"fmt"
	"strings"
)

// CommandRequestLogKindPrefix marks a pkg/logrecord Kind as a
// SubmitCommand dispatch request for a specific commandID -- shared
// between pkg/kvctl/mobile/kvmobile (which build and enumerate these
// records) and pkg/daemon/pkg/kvfsm (which need to recognize one to
// authoritatively enforce isPermittedForCommand against it -- see
// kvfsm.OpAppendCommandRequest). Was previously a private literal
// duplicated in both client packages, invisible to the daemon/FSM side.
const CommandRequestLogKindPrefix = "cmdreq:"

// CommandRequestLogKind returns the pkg/logrecord Kind every SubmitCommand
// dispatch of commandID is stored under.
func CommandRequestLogKind(commandID string) string {
	return CommandRequestLogKindPrefix + commandID
}

// ParseCommandRequestLogKind extracts commandID back out of a Kind built
// by CommandRequestLogKind, reporting ok=false for any other kind (an
// ordinary, non-command-dispatch log append).
func ParseCommandRequestLogKind(kind string) (commandID string, ok bool) {
	if !strings.HasPrefix(kind, CommandRequestLogKindPrefix) {
		return "", false
	}
	return strings.TrimPrefix(kind, CommandRequestLogKindPrefix), true
}

// CommandExecIndexKindPrefix marks a pkg/logrecord Kind as
// SubmitCommand's per-peer execution index -- every dispatch a given
// peerID plays requester or target in gets one entry here, letting
// ListExecutionsByPeer(peerID) enumerate them with a single prefix scan
// instead of walking every command's own CommandRequestLogKind. Promoted
// here for the identical reason CommandRequestLogKindPrefix was: it used
// to be a private literal duplicated in pkg/kvctl and mobile/kvmobile,
// invisible to pkg/daemon, which needs to recognize one to let a remote,
// non-cluster-member caller read back its own index (and only its own --
// see pkg/daemon's isCommandLogCarveOut) as part of the public-command
// carve-out, the one door a peer with no other standing has into an
// otherwise closed cluster.
const CommandExecIndexKindPrefix = "cmdexec:"

// CommandExecIndexKind returns the pkg/logrecord Kind peerID's execution
// index is stored under.
func CommandExecIndexKind(peerID string) string {
	return CommandExecIndexKindPrefix + peerID
}

// ParseCommandExecIndexKind extracts peerID back out of a Kind built by
// CommandExecIndexKind, reporting ok=false for any other kind.
func ParseCommandExecIndexKind(kind string) (peerID string, ok bool) {
	if !strings.HasPrefix(kind, CommandExecIndexKindPrefix) {
		return "", false
	}
	return strings.TrimPrefix(kind, CommandExecIndexKindPrefix), true
}

// CommandExecLogKind is the fixed pkg/logrecord Kind every
// AppendCommandLog entry (a command's actual execution output, as the
// target reports it) is stored under, keyed by instance id rather than a
// per-command Kind -- an instance id is already globally unique and a
// caller tracking one dispatch already knows exactly which one it wants.
// Promoted from pkg/kvctl/mobile/kvmobile's own identical private literal
// for the same reason CommandExecIndexKindPrefix was: pkg/daemon's
// isCommandLogCarveOut needs to recognize it too, to let a non-member
// remote caller read back a dispatch's own execution log -- "possessing
// the instance id is the credential" was already this kind's design (see
// pkg/kvctl's GetCommandRequest doc comment), so no further ACL check is
// layered on top of recognizing the kind itself.
const CommandExecLogKind = "cmdlog"

// EncodeCommandRequestApplyPayload packs the authenticated submitting
// peer id and the actual pkg/logrecord.Record value into a single
// kvfsm.OpAppendCommandRequest Apply payload: a 2-byte big-endian length
// prefix for authorPeerID (fixed framing, so it needs a prefix -- unlike
// EncodeGroupPutPayload's trailing field, recordValue here is what's
// last), then authorPeerID, then recordValue verbatim (last field, no
// prefix needed). authorPeerID is the connection-authenticated identity
// (pkg/daemon's caller.remotePeer, or this node's own peer id for a local
// caller) -- never a value the payload itself could self-declare.
func EncodeCommandRequestApplyPayload(authorPeerID string, recordValue []byte) ([]byte, error) {
	if len(authorPeerID) > 0xFFFF {
		return nil, fmt.Errorf("shmevent: command request author peer id too long: %d bytes", len(authorPeerID))
	}
	buf := make([]byte, 2+len(authorPeerID)+len(recordValue))
	buf[0] = byte(len(authorPeerID) >> 8)
	buf[1] = byte(len(authorPeerID))
	off := 2
	off += copy(buf[off:], authorPeerID)
	copy(buf[off:], recordValue)
	return buf, nil
}

// DecodeCommandRequestApplyPayload is the inverse of
// EncodeCommandRequestApplyPayload.
func DecodeCommandRequestApplyPayload(payload []byte) (authorPeerID string, recordValue []byte, err error) {
	if len(payload) < 2 {
		return "", nil, fmt.Errorf("shmevent: command request apply payload too short: %d bytes", len(payload))
	}
	idLen := int(payload[0])<<8 | int(payload[1])
	off := 2
	if off+idLen > len(payload) {
		return "", nil, fmt.Errorf("shmevent: command request author peer id length %d exceeds payload size %d", idLen, len(payload))
	}
	authorPeerID = string(payload[off : off+idLen])
	off += idLen
	return authorPeerID, payload[off:], nil
}
