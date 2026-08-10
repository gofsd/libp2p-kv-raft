package kvfsm

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// OpType identifies the kind of mutation carried by a raft log entry.
type OpType uint8

const (
	OpSet OpType = 1
	OpDel OpType = 2
	// OpConfirm promotes a pending pkg/shmevent system record to
	// confirmed: key is the pending record's own key, value is the
	// confirmed record's key (not a value -- see Apply's OpConfirm case).
	// Reuses EncodeCommand/DecodeCommand's existing key+value framing
	// unchanged; both fields are already opaque byte slices, so no wire
	// format change was needed for this op.
	OpConfirm OpType = 3
	// OpCascadeDelete deletes a Group or Command record and every
	// GroupCommand/PeerGroup relation record referencing it, all inside
	// this single Apply call so every raft replica cascades identically
	// (see Apply's OpCascadeDelete case). key is the Group/Command
	// record's own key (shmevent.GroupKey/CommandKey) -- its own kind
	// byte (SystemKey's second byte) is what selects which cascade runs;
	// value is unused.
	OpCascadeDelete OpType = 4
	// OpConsumeInvite atomically reads and deletes a
	// shmevent.KindJoinInvite record: key is the invite's own key
	// (shmevent.JoinInviteKey), value is unused. On success, the
	// ApplyResult's Value field carries the deleted record's own value
	// (its encoded suffrage byte -- shmevent.EncodeJoinInviteRecord/
	// DecodeJoinInviteRecord) back to the caller (see pkg/daemon's
	// consumeJoinInvite), which is what actually lets a join request
	// bypass Config.RequireConfirmForJoin. Read-then-delete in one Apply
	// call is what makes "one time" real with no extra bookkeeping,
	// exactly like OpConfirm's existing read-then-write-then-delete
	// already guarantees for every pending->confirmed kind: two
	// concurrent redemption attempts for the same token deterministically
	// resolve to exactly one winner, since Apply runs in strict raft log
	// order and whichever entry commits second finds nothing left to read.
	OpConsumeInvite OpType = 5
	// OpConsumeExecInvite atomically reads a shmevent.KindExecInvite
	// record, checks the redeeming peer's real Group/Command/PeerGroup ACL
	// standing against the command it names, and -- only if that check
	// passes -- deletes it, all inside this single Apply call: key is the
	// invite's own key (shmevent.ExecInviteKey), value is the redeeming
	// peer's id (raw bytes, no wrapper needed for one field). Unlike
	// OpConsumeInvite, whose caller (pkg/daemon's consumeJoinInvite) is
	// trusted by construction (only reached once this node's own raft
	// join-request handling has already decided to admit the request),
	// this op's ACL check is the actual, raft-authoritative enforcement
	// point -- see shmevent.KindExecInvite's doc comment on why that
	// matters here specifically (the redeeming peer is a genuinely
	// untrusted remote caller, not a locally-driven client). On success,
	// ApplyResult.Value carries the deleted record's own value back to the
	// caller (see pkg/daemon's applyConsumeExecInvite), same convention as
	// OpConsumeInvite. An ACL failure returns an error without deleting
	// anything -- an unauthorized redemption attempt doesn't burn the
	// ticket, so a legitimate peer can still redeem it later; only a
	// successful, permitted redemption consumes it, which combined with
	// Apply's strict raft log ordering is what makes two concurrent
	// legitimate redemption attempts for the same token still resolve to
	// exactly one winner.
	OpConsumeExecInvite OpType = 6
	// OpAppendCommandRequest is a SubmitCommand dispatch's actual write --
	// the raft-authoritative counterpart to OpConsumeExecInvite, for the
	// ordinary (non-invite) submission path: key is the CommandRequest's
	// own pkg/logrecord key (whose Kind, shmevent.CommandRequestLogKind,
	// names the commandID being targeted), value is
	// shmevent.EncodeCommandRequestApplyPayload(authorPeerID, recordValue)
	// -- authorPeerID the connection-authenticated submitting peer (never
	// a value the payload itself could self-declare), recordValue the
	// actual pkg/logrecord.Record bytes to store. Checks the submitting
	// peer's real Group/GroupCommand/PeerGroup/Public ACL standing against
	// the named commandID before writing anything, same IsPermittedForCommand
	// OpConsumeExecInvite already uses -- this is what closes the gap that
	// existed when only pkg/kvctl/mobile/kvmobile's own SubmitCommand
	// client evaluated that check, client-side, before an ordinary
	// unauthenticated EventLogAppend: nothing stopped a remote caller from
	// skipping that client and forging the write directly. Unlike
	// OpConsumeExecInvite there's no prior record to consume -- an ACL
	// failure here just means nothing is ever written, no cleanup needed.
	OpAppendCommandRequest OpType = 7
	// OpTxn atomically applies an ordered list of plain Set/Delete ops --
	// value is shmevent.EncodeTxnPayload(ops) (key is unused; unlike every
	// op above, a single EncodeCommand key/value pair can't hold more than
	// one op, so OpTxn packs its whole op list into value instead). Every
	// op is validated (a well-formed shmevent.TxnOpSet/TxnOpDelete) before
	// any of them are written, so a malformed op fails the transaction
	// with no partial effect -- the same "check everything first"
	// discipline OpAppendCommandRequest's ACL check already follows.
	// Reserved-namespace keys (shmevent.SystemKeyPrefix/
	// logrecord.LogKeyPrefix) are rejected one layer up, in pkg/daemon's
	// EventTxn handler, before ever reaching here -- OpTxn itself trusts
	// that gate rather than re-checking it (its only call site), unlike
	// OpSet, which also serves the Group/Command catalog's own Puts and
	// so must re-validate at this layer regardless of caller.
	//
	// Atomicity here is still purely "one raft log entry -> one Apply
	// call," not a real Store-level transaction: pkg/store.Store's
	// Set/Delete each autocommit independently (see this file's own doc
	// comment on OpCascadeDelete), so a write failing partway through
	// this op list -- after every op already passed validation above --
	// has no rollback, the same latent gap OpCascadeDelete's multi-write
	// case already has. The write loop below is where a real
	// Store-level BEGIN/COMMIT/ROLLBACK would go if that gap is ever
	// closed; nothing about EventTxn's wire shape or this op's
	// validate-then-write structure would need to change to add it.
	OpTxn OpType = 8
)

// ErrCompareFailed is what an OpTxn whose shmevent.TxnOpCompare/
// TxnOpCompareAbsent precondition didn't hold wraps its ApplyResult.Err in.
// A distinct sentinel rather than a plain error string because a failed
// compare is the one transaction failure that is *expected*, and means
// something specific to the caller: nothing was written, another writer got
// there first, and the right response is to re-read and retry rather than to
// surface an error. Callers reach it through errors.Is -- pkg/daemon
// forwards the wrapped message verbatim to the IPC caller, which is what
// pkg/shmclient's CompareAndSwap turns back into a plain false.
var ErrCompareFailed = errors.New("kvfsm: " + shmevent.CompareFailedMarker)

// EncodeCommand builds the raft log payload for a Set/Delete operation.
// Layout: [1 byte op][4 byte big-endian key len][key][4 byte big-endian value len][value].
func EncodeCommand(op OpType, key, value []byte) []byte {
	buf := make([]byte, 1+4+len(key)+4+len(value))
	buf[0] = byte(op)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(key)))
	off := 5
	off += copy(buf[off:], key)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(value)))
	off += 4
	copy(buf[off:], value)
	return buf
}

// DecodeCommand is the inverse of EncodeCommand -- also used directly by
// pkg/daemon's ForwardProtocolID handling, which forwards a Set to the
// leader using this same op+key+value framing rather than the user-facing
// pkg/shmevent protocol (ForwardProtocolID is internal node-to-node
// machinery, not something a "user" ever speaks).
func DecodeCommand(data []byte) (op OpType, key, value []byte, err error) {
	return decodeCommand(data)
}

func decodeCommand(data []byte) (op OpType, key, value []byte, err error) {
	if len(data) < 5 {
		return 0, nil, nil, fmt.Errorf("kvfsm: command too short")
	}
	op = OpType(data[0])
	klen := binary.BigEndian.Uint32(data[1:5])
	off := 5
	if uint32(len(data[off:])) < klen {
		return 0, nil, nil, fmt.Errorf("kvfsm: truncated key")
	}
	key = data[off : off+int(klen)]
	off += int(klen)
	if len(data[off:]) < 4 {
		return 0, nil, nil, fmt.Errorf("kvfsm: missing value length")
	}
	vlen := binary.BigEndian.Uint32(data[off : off+4])
	off += 4
	if uint32(len(data[off:])) < vlen {
		return 0, nil, nil, fmt.Errorf("kvfsm: truncated value")
	}
	value = data[off : off+int(vlen)]
	return op, key, value, nil
}
