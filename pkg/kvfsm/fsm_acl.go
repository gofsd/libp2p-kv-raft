package kvfsm

import (
	"bytes"
	"errors"
	"fmt"

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
// shmevent.KindGroup/KindCommand (200/2000) and KindBootstrapNode (50),
// whose limits were chosen as real, meaningful caps rather than a DoS
// backstop the way maxSystemListEntries is for everything else --
// KindBootstrapNode in particular is meant to stay a small, curated
// relay-failover list (see pkg/daemon's relayCandidates), not general
// data. A var, not a const, for the same test-lowering reason as
// maxSystemListEntries.
var systemListLimits = map[byte]int{
	shmevent.KindGroup:         200,
	shmevent.KindCommand:       2000,
	shmevent.KindBootstrapNode: 50,
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
func checkSystemListCap(s store.Accessor, key []byte) error {
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

// checkGroupNameUnique enforces that no two Group records share the same
// name, called from Apply's OpSet case whenever key is a GroupKey --
// before the write actually lands, so the check-and-write is atomic
// within this one raft log entry. Doing this check in pkg/daemon/
// pkg/kvctl before ever reaching Apply would leave a TOCTOU race between
// two concurrent PutGroup calls choosing the same name; evaluating it here
// instead relies on the same guarantee OpConfirm/OpConsumeInvite's own
// read-then-write comments already lean on -- Apply runs exactly once, in
// raft log order, so there's nothing to race against. A group being
// overwritten under its own id (a rename, or a plain re-Put) is not a
// collision with itself -- only a genuinely different id already holding
// that name is rejected. At most systemListLimits' own KindGroup cap (200)
// records exist at once, so a full scan here is cheap and bounded.
func checkGroupNameUnique(s store.Accessor, key, value []byte) error {
	if len(key) < systemKeyPrefixLen || key[0] != shmevent.SystemKeyPrefix || key[1] != shmevent.KindGroup {
		return nil
	}
	name, _, err := shmevent.DecodeGroupPayload(value)
	if err != nil {
		return err
	}
	lo, hi := shmevent.GroupKeyBounds()
	matches, err := s.ScanRange(lo, hi, 0)
	if err != nil {
		return err
	}
	for _, m := range matches {
		if bytes.Equal(m.Key, key) {
			continue
		}
		existingName, _, err := shmevent.DecodeGroupPayload(m.Value)
		if err != nil {
			return err
		}
		if existingName == name {
			return fmt.Errorf("kvfsm: group name %q already used by another group", name)
		}
	}
	return nil
}

// preserveCommandSpec carries an existing Command record's spec forward when
// the incoming write doesn't mention one, returning the value to actually
// store. Every non-Command key, and every write that does carry a spec field
// (including an explicitly emptied one -- see
// shmevent.EncodeCommandPayloadClearingSpec), passes through untouched.
//
// This exists because a spec is a big, human-authored field written once,
// while name and target peer are small ones edited often -- and every
// existing caller of PutCommand sends only the latter. Without this, the
// ordinary act of renaming a command, or an app re-registering its catalog
// on startup, would silently delete a form definition nobody meant to touch:
// the write is a Put, so it replaces the whole record. Making the *absence*
// of a field mean "leave it alone" is what keeps that from being a trap,
// while an explicit empty spec still clears it.
//
// Done here rather than in pkg/daemon before rf.Apply for the same reason
// the group-name check is: Apply runs exactly once, in raft log order,
// against state every replica already agrees on, so reading the previous
// record here has nothing to race against. A pre-check would.
func preserveCommandSpec(s store.Accessor, key, value []byte) ([]byte, error) {
	if len(key) < systemKeyPrefixLen || key[0] != shmevent.SystemKeyPrefix || key[1] != shmevent.KindCommand {
		return value, nil
	}
	if _, present, err := shmevent.DecodeCommandPayloadSpec(value); err != nil {
		return nil, fmt.Errorf("kvfsm: command spec: %w", err)
	} else if present {
		return value, nil
	}

	existing, err := s.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		return value, nil
	}
	if err != nil {
		return nil, fmt.Errorf("kvfsm: command spec: read existing: %w", err)
	}
	oldSpec, _, err := shmevent.DecodeCommandPayloadSpec(existing)
	if err != nil || len(oldSpec) == 0 {
		// A previous record that is itself malformed must not block a write
		// that would replace it -- that would make a bad record permanent.
		return value, nil
	}
	name, peerID, err := shmevent.DecodeCommandPayload(value)
	if err != nil {
		return nil, fmt.Errorf("kvfsm: command spec: decode incoming: %w", err)
	}
	merged, err := shmevent.EncodeCommandPayloadWithSpec(name, peerID, oldSpec)
	if err != nil {
		return nil, fmt.Errorf("kvfsm: command spec: re-encode: %w", err)
	}
	return merged, nil
}

// A note on a rule that used to be here: Apply once enforced that no two
// Command records shared the same target peerID -- "a peer has at most one
// command". It was removed, deliberately, because it made the catalog a 1:1
// device<->command mapping, and so capped a cluster's whole command list at
// its device count. Nothing depended on it: every consumer of a Command
// record looks up command id -> target peer (pkg/daemon's exec-invite path,
// pkg/kvctl/dispatch.go's SubmitCommand, kvmobile's catalog listings), never
// peer -> its command, so no reader had a uniqueness assumption to break. A
// device may now be the target of as many commands as the catalog holds,
// which is what lets a cluster define a real command set rather than one
// entry per station.

// IsPermittedForCommand reports whether peerID may redeem/execute
// commandID: true if some group G linked to commandID (GroupCommandKey(
// commandID, G)) either has its own Public flag set, or satisfies
// PeerGroupKey(peerID, G) -- a public group admits any peer with no
// PeerGroup membership record needed at all, exactly the same way it
// would if that peer held one. Mirrors pkg/kvctl/catalog.go's client-side
// isPermittedForCommand check exactly (scan the commandID side first -- a
// command is expected to be linked to few groups, unlike a peer, which may
// belong to many -- then check each hit's Group record before falling
// back to a PeerGroupKey point-check), but evaluated directly against s:
// called from inside Apply (see OpConsumeExecInvite/OpAppendCommandRequest),
// so this is the raft-authoritative counterpart that client-side check
// doesn't have. GroupCommandKey's own first field (commandID) is
// length-prefixed, so the fixed part of GroupCommandKey(commandID, nil) is
// already a safe, unpadded ScanPrefix prefix -- the same trick
// applyCascadeDelete's KindCommand case above uses -- no need for
// GroupCommandBounds' 0xFF-padded range here.
//
// Exported for pkg/daemon's sake too: isCommandLogCarveOut (handleShmEvent's
// gate for a remote, non-cluster-member caller) reuses this exact check to
// let such a caller read back a CommandRequestLogKind command's own
// request queue, the same authoritative standing that already lets it
// submit one -- see that function's doc comment.
func IsPermittedForCommand(s store.Accessor, commandID, peerID []byte) (bool, error) {
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
		if groupValue, err := s.Get(shmevent.GroupKey(groupID)); err == nil {
			if _, public, err := shmevent.DecodeGroupPayload(groupValue); err == nil && public {
				return true, nil
			}
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

// grantChannelRelayAccess adds peerID into shmevent.ReservedGroupChannel
// and ReservedGroupRelay -- OpAppendCommandRequest's special case for
// shmevent.DefaultPublicCommandID (see that constant's own doc comment
// for the full design). Called only from inside Apply, so this lands in
// the identical raft log entry as the CommandRequest write it's a side
// effect of -- every replica applies both writes together or neither, the
// same all-or-nothing guarantee any other Apply case gets.
func grantChannelRelayAccess(s store.Accessor, peerID []byte) error {
	for _, groupID := range [][]byte{[]byte(shmevent.ReservedGroupChannel), []byte(shmevent.ReservedGroupRelay)} {
		key, err := shmevent.PeerGroupKey(peerID, groupID)
		if err != nil {
			return err
		}
		if err := s.Set(key, nil); err != nil {
			return err
		}
	}
	return nil
}
