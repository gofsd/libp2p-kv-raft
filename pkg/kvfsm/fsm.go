// Package kvfsm implements the hashicorp/raft FSM for the distributed KV
// store, backed by pkg/store.
//
// This file holds the FSM interface adapter itself (FSM/New/ApplyResult/
// Apply/Snapshot/Restore). The op-kind wire format (OpType and its
// EncodeCommand/DecodeCommand framing) lives in fsm_encoding.go; the
// list-cap/group-name-uniqueness/command-spec-merge validation and ACL
// evaluation Apply calls into live in fsm_acl.go; the Group/Command
// cascade-delete logic lives in fsm_cascade.go.
package kvfsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// FSM adapts pkg/store to the raft.FSM interface.
type FSM struct {
	Store *store.Store
}

// New returns an FSM backed by s.
func New(s *store.Store) *FSM {
	return &FSM{Store: s}
}

// ApplyResult is returned to the raft ApplyFuture caller. Value is only
// ever populated by OpConsumeInvite (the deleted invite record's own
// value) -- every other op's caller already knows what it wrote/deleted
// and has no use for it.
type ApplyResult struct {
	Err   error
	Value []byte
}

// Apply implements raft.FSM.
func (f *FSM) Apply(l *raft.Log) any {
	op, key, value, err := decodeCommand(l.Data)
	if err != nil {
		return ApplyResult{Err: err}
	}
	switch op {
	case OpSet:
		if err := checkSystemListCap(f.Store, key); err != nil {
			return ApplyResult{Err: err}
		}
		if err := checkGroupNameUnique(f.Store, key, value); err != nil {
			return ApplyResult{Err: err}
		}
		value, err := preserveCommandSpec(f.Store, key, value)
		if err != nil {
			return ApplyResult{Err: err}
		}
		return ApplyResult{Err: f.Store.Set(key, value)}
	case OpDel:
		return ApplyResult{Err: f.Store.Delete(key)}
	case OpConfirm:
		// Read-modify-write across two different keys, safe and
		// deterministic here because Apply runs exactly once, in raft log
		// order, against each node's own already-caught-up local store --
		// every node ends up with the identical result without needing
		// any separate linearizable-read machinery.
		v, err := f.Store.Get(key)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: confirm: no pending record at key: %w", err)}
		}
		// The cap check applies to the *confirmed* key (value) being
		// promoted to, not the pending key (key) being read from and
		// deleted -- this is what makes a kind's pending and confirmed
		// lists count independently: confirming never touches the
		// pending list's membership count, only the confirmed side's.
		if err := checkSystemListCap(f.Store, value); err != nil {
			return ApplyResult{Err: err}
		}
		if err := f.Store.Set(value, v); err != nil {
			return ApplyResult{Err: err}
		}
		return ApplyResult{Err: f.Store.Delete(key)}
	case OpCascadeDelete:
		return applyCascadeDelete(f.Store, key)
	case OpConsumeInvite:
		// Read-then-delete, atomic within this single Apply call for the
		// identical reason OpConfirm's read-modify-write is (see that
		// case's comment): this is the only place any raft replica ever
		// mutates a KindJoinInvite record after creation, so there's no
		// concurrent-Apply race to protect against beyond what raft's own
		// strict log ordering already guarantees.
		v, err := f.Store.Get(key)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: consume invite: no such invite: %w", err)}
		}
		if err := f.Store.Delete(key); err != nil {
			return ApplyResult{Err: err}
		}
		return ApplyResult{Value: v}
	case OpConsumeExecInvite:
		v, err := f.Store.Get(key)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: consume exec invite: no such invite: %w", err)}
		}
		commandID, _, expiresAtUnix, err := shmevent.DecodeExecInviteRecord(v)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: consume exec invite: decode record: %w", err)}
		}
		// l.AppendedAt is the leader's clock at the moment this very Apply
		// call's entry was appended -- replicated as part of the log entry
		// itself, so every replica compares against the identical value
		// (see raft.Log.AppendedAt's own doc comment: "Followers will
		// observe the leader's time"), keeping this deterministic the same
		// way every other Apply case here already is. An expired invite is
		// deleted here too, same as a successful redemption -- time only
		// moves forward, so it can never become redeemable again.
		if expiresAtUnix != 0 && uint64(l.AppendedAt.Unix()) >= expiresAtUnix {
			if err := f.Store.Delete(key); err != nil {
				return ApplyResult{Err: err}
			}
			return ApplyResult{Err: fmt.Errorf("kvfsm: consume exec invite: token expired")}
		}
		permitted, err := IsPermittedForCommand(f.Store, []byte(commandID), value)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: consume exec invite: acl check: %w", err)}
		}
		if !permitted {
			return ApplyResult{Err: fmt.Errorf("kvfsm: consume exec invite: %s is not permitted for command %s", value, commandID)}
		}
		if err := f.Store.Delete(key); err != nil {
			return ApplyResult{Err: err}
		}
		return ApplyResult{Value: v}
	case OpAppendCommandRequest:
		authorPeerID, recordValue, err := shmevent.DecodeCommandRequestApplyPayload(value)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: append command request: decode payload: %w", err)}
		}
		kind, _, _, err := logrecord.ParseKey(key)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: append command request: parse key: %w", err)}
		}
		commandID, ok := shmevent.ParseCommandRequestLogKind(kind)
		if !ok {
			return ApplyResult{Err: fmt.Errorf("kvfsm: append command request: kind %q is not a command-request kind", kind)}
		}
		permitted, err := IsPermittedForCommand(f.Store, []byte(commandID), []byte(authorPeerID))
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: append command request: acl check: %w", err)}
		}
		if !permitted {
			return ApplyResult{Err: fmt.Errorf("kvfsm: %s is not permitted to submit command %s", authorPeerID, commandID)}
		}
		if err := checkSystemListCap(f.Store, key); err != nil {
			return ApplyResult{Err: err}
		}
		if err := f.Store.Set(key, recordValue); err != nil {
			return ApplyResult{Err: err}
		}
		// shmevent.DefaultPublicCommandID's own special case -- see that
		// constant's doc comment: submitting it, already gated by the
		// IsPermittedForCommand check just above like any other command,
		// also grants the submitting peer real Channel/relay access,
		// atomically in this same Apply call.
		if commandID == shmevent.DefaultPublicCommandID {
			if err := grantChannelRelayAccess(f.Store, []byte(authorPeerID)); err != nil {
				return ApplyResult{Err: fmt.Errorf("kvfsm: grant channel/relay access to %s: %w", authorPeerID, err)}
			}
		}
		return ApplyResult{}
	case OpTxn:
		ops, err := shmevent.DecodeTxnPayload(value)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: txn: decode payload: %w", err)}
		}
		for i, op := range ops {
			if !shmevent.ValidTxnOp(op.Op) {
				return ApplyResult{Err: fmt.Errorf("kvfsm: txn: op %d has unknown kind %d", i, op.Op)}
			}
		}
		// Every precondition is evaluated before any write in the same
		// transaction lands, so a failed compare leaves the store exactly
		// as it was rather than half-applied. This -- not the compare
		// itself -- is what makes the pair a real compare-and-swap: raft
		// has already serialized this entry against every other write, so
		// between reading a key here and writing it three lines down
		// there is no window another client can act in. The same read
		// done by a client over two IPC round trips has a window that no
		// amount of care on the client side can close.
		for i, op := range ops {
			if !op.IsCompare() {
				continue
			}
			current, err := f.Store.Get(op.Key)
			switch op.Op {
			case shmevent.TxnOpCompareAbsent:
				if err == nil {
					return ApplyResult{Err: fmt.Errorf("%w: op %d: key %q exists", ErrCompareFailed, i, op.Key)}
				}
				if !errors.Is(err, store.ErrNotFound) {
					return ApplyResult{Err: fmt.Errorf("kvfsm: txn: op %d: read key %q: %w", i, op.Key, err)}
				}
			case shmevent.TxnOpCompare:
				if errors.Is(err, store.ErrNotFound) {
					return ApplyResult{Err: fmt.Errorf("%w: op %d: key %q does not exist", ErrCompareFailed, i, op.Key)}
				}
				if err != nil {
					return ApplyResult{Err: fmt.Errorf("kvfsm: txn: op %d: read key %q: %w", i, op.Key, err)}
				}
				if !bytes.Equal(current, op.Value) {
					return ApplyResult{Err: fmt.Errorf("%w: op %d: key %q holds a different value", ErrCompareFailed, i, op.Key)}
				}
			}
		}
		// See OpTxn's own doc comment for why this loop -- not any single
		// Store call -- is where future rollback support would wrap a
		// real Store-level transaction.
		for _, op := range ops {
			if op.IsCompare() {
				continue
			}
			var err error
			if op.Op == shmevent.TxnOpSet {
				err = f.Store.Set(op.Key, op.Value)
			} else {
				err = f.Store.Delete(op.Key)
			}
			if err != nil {
				return ApplyResult{Err: fmt.Errorf("kvfsm: txn: op on key %x: %w", op.Key, err)}
			}
		}
		return ApplyResult{}
	default:
		return ApplyResult{Err: fmt.Errorf("kvfsm: unknown op %d", op)}
	}
}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.Store}, nil
}

// Restore implements raft.FSM.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.Store.LoadAll(rc)
}

type fsmSnapshot struct {
	store *store.Store
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.store.DumpAll(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
