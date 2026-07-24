// Package kvfsm implements the hashicorp/raft FSM for the distributed KV
// store, backed by pkg/store.
package kvfsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// maxSystemListEntries is the default cap for every pkg/shmevent.SystemKey
// -based list (the confirmed/pending halves of KindPermitPeer,
// KindBootstrapNode, KindClusterMember, KindGroupCommand, KindPeerGroup,
// and any future kind not listed in systemListLimits) independently --
// each distinct kind+status prefix (SystemKey's first 3 bytes) may hold at
// most this many entries. Enforced here, inside Apply, rather than as a
// pre-check in pkg/daemon before calling rf.Apply: Apply is the only place
// every raft replica deterministically agrees on order, so a Go-level
// pre-check could race against a concurrent Apply from another source and
// let two nodes disagree about whether the cap was hit. A var, not a
// const, so tests can temporarily lower it rather than writing 65000 real
// rows.
var maxSystemListEntries = 65000

// systemListLimits overrides maxSystemListEntries for specific kinds that
// need a tighter cap than the generous system-wide default -- currently
// just shmevent.KindGroup/KindCommand, whose limits (200/2000) were
// chosen as real, meaningful caps rather than a DoS backstop the way
// maxSystemListEntries is for everything else. A var, not a const, for
// the same test-lowering reason as maxSystemListEntries.
var systemListLimits = map[byte]int{
	shmevent.KindGroup:   200,
	shmevent.KindCommand: 2000,
}

// systemKeyPrefixLen is how many leading bytes of a shmevent.SystemKey
// identify its list (kind + status, see that function's doc comment) --
// everything after is the peer id, which varies per entry and so must not
// be part of the count-limiting prefix.
const systemKeyPrefixLen = 3

// checkSystemListCap enforces key's list cap (systemListLimits[kind], or
// maxSystemListEntries if kind has no override) if key is a
// pkg/shmevent.SystemKey (starts with shmevent.SystemKeyPrefix): an
// overwrite of an already-existing key never grows its list, so only a
// genuinely new key is checked against its kind+status prefix's current
// count. Ordinary user keys (anything not starting with SystemKeyPrefix)
// are never counted or capped.
func checkSystemListCap(s *store.Store, key []byte) error {
	if len(key) < systemKeyPrefixLen || key[0] != shmevent.SystemKeyPrefix {
		return nil
	}
	if _, err := s.Get(key); err == nil {
		return nil // overwrite of an existing entry, not a new one
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	prefix := key[:systemKeyPrefixLen]
	limit := maxSystemListEntries
	if override, ok := systemListLimits[key[1]]; ok {
		limit = override
	}
	count, err := s.CountPrefix(prefix)
	if err != nil {
		return err
	}
	if count >= limit {
		return fmt.Errorf("kvfsm: system list %x is at capacity (%d entries)", prefix, limit)
	}
	return nil
}

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
)

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
		commandID, _, err := shmevent.DecodeExecInviteRecord(v)
		if err != nil {
			return ApplyResult{Err: fmt.Errorf("kvfsm: consume exec invite: decode record: %w", err)}
		}
		permitted, err := isPermittedForCommand(f.Store, []byte(commandID), value)
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
	default:
		return ApplyResult{Err: fmt.Errorf("kvfsm: unknown op %d", op)}
	}
}

// isPermittedForCommand reports whether peerID may redeem/execute
// commandID: true if some group G satisfies both PeerGroupKey(peerID, G)
// and GroupCommandKey(commandID, G). Mirrors pkg/kvctl/catalog.go's
// client-side isPermittedForCommand check exactly (scan the commandID side
// first -- a command is expected to be linked to few groups, unlike a peer,
// which may belong to many -- then point-check PeerGroupKey for each hit),
// but evaluated directly against s: called only from inside Apply (see
// OpConsumeExecInvite), so this is the raft-authoritative counterpart that
// client-side check doesn't have. GroupCommandKey's own first field
// (commandID) is length-prefixed, so the fixed part of
// GroupCommandKey(commandID, nil) is already a safe, unpadded ScanPrefix
// prefix -- the same trick applyCascadeDelete's KindCommand case above
// uses -- no need for GroupCommandBounds' 0xFF-padded range here.
func isPermittedForCommand(s *store.Store, commandID, peerID []byte) (bool, error) {
	prefix, err := shmevent.GroupCommandKey(commandID, nil)
	if err != nil {
		return false, err
	}
	matches, err := s.ScanPrefix(prefix, 0)
	if err != nil {
		return false, err
	}
	for _, m := range matches {
		_, groupID, err := shmevent.ParseGroupCommandKey(m.Key)
		if err != nil {
			return false, err
		}
		peerGroupKey, err := shmevent.PeerGroupKey(peerID, groupID)
		if err != nil {
			return false, err
		}
		if _, err := s.Get(peerGroupKey); err == nil {
			return true, nil
		}
	}
	return false, nil
}

// applyCascadeDelete deletes a Group or Command record (key) and every
// GroupCommand/PeerGroup relation record referencing it, so a delete never
// leaves a dangling relation behind. key's own kind byte (SystemKey's
// second byte) selects which cascade runs: Command deletion prefix-scans
// shmevent.GroupCommandKey(commandID, ...) cleanly, since commandID is
// that key's first variable field; Group deletion has no equally cheap
// prefix scan available (groupID is the *trailing* field of both relation
// kinds -- see GroupCommandKey/PeerGroupKey's own doc comments), so it
// instead scans every GroupCommand/PeerGroup record system-wide and
// filters by parsing each key. Accepted, not fixed with a reverse index:
// deleting a Group is a rare administrative action bounded by
// systemListLimits' own caps, not something that needs to be as cheap as
// the command-execute-time join (pkg/kvctl.isPermittedForCommand), which
// is what actually needed the commandID-first key layout to be fast.
func applyCascadeDelete(s *store.Store, key []byte) ApplyResult {
	if len(key) < systemKeyPrefixLen || key[0] != shmevent.SystemKeyPrefix {
		return ApplyResult{Err: fmt.Errorf("kvfsm: cascade delete: not a system key")}
	}
	switch key[1] {
	case shmevent.KindCommand:
		commandID := key[systemKeyPrefixLen:]
		prefix, err := shmevent.GroupCommandKey(commandID, nil)
		if err != nil {
			return ApplyResult{Err: err}
		}
		matches, err := s.ScanPrefix(prefix, 0)
		if err != nil {
			return ApplyResult{Err: err}
		}
		for _, m := range matches {
			if err := s.Delete(m.Key); err != nil {
				return ApplyResult{Err: err}
			}
		}
		return ApplyResult{Err: s.Delete(key)}
	case shmevent.KindGroup:
		groupID := key[systemKeyPrefixLen:]
		if err := deleteRelationsByGroupID(s, shmevent.AllGroupCommandsPrefix(), groupID, shmevent.ParseGroupCommandKey); err != nil {
			return ApplyResult{Err: err}
		}
		if err := deleteRelationsByGroupID(s, shmevent.AllPeerGroupsPrefix(), groupID, shmevent.ParsePeerGroupKey); err != nil {
			return ApplyResult{Err: err}
		}
		return ApplyResult{Err: s.Delete(key)}
	default:
		return ApplyResult{Err: fmt.Errorf("kvfsm: cascade delete: unsupported kind %d", key[1])}
	}
}

// deleteRelationsByGroupID scans every relation record under prefix
// (shmevent.AllGroupCommandsPrefix()/AllPeerGroupsPrefix()) and deletes
// whichever ones parse (via parse, shmevent.ParseGroupCommandKey/
// ParsePeerGroupKey -- both share the (first, groupID []byte, err error)
// shape) to the given groupID -- the shared scan-and-filter loop behind
// applyCascadeDelete's KindGroup case, since GroupCommand and PeerGroup
// need the identical treatment, just parsed differently.
func deleteRelationsByGroupID(s *store.Store, prefix, groupID []byte, parse func(key []byte) ([]byte, []byte, error)) error {
	matches, err := s.ScanPrefix(prefix, 0)
	if err != nil {
		return err
	}
	for _, m := range matches {
		_, matchGroupID, err := parse(m.Key)
		if err != nil {
			return err
		}
		if !bytes.Equal(matchGroupID, groupID) {
			continue
		}
		if err := s.Delete(m.Key); err != nil {
			return err
		}
	}
	return nil
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
