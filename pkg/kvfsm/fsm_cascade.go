package kvfsm

import (
	"bytes"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

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
