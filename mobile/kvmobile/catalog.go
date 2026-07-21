package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file implements the group-based ACL catalog: Group (id, name),
// Command (id, name, target_peer_id -- where it may be executed),
// GroupCommand (a many-to-many command<->group link) and PeerGroup (a
// peer's group membership) -- all daemon-enforced shmevent.SystemKeyPrefix
// records (see shmevent.KindGroup's doc comment in pkg/shmevent/system.go),
// the same model desktop's pkg/kvctl/catalog.go uses. Any single current
// raft voter may create/update/delete any of these four kinds directly (no
// second-voter confirmation, see shmevent.EventGroupPut's doc comment) --
// and pkg/daemon itself enforces that, so unlike this file's previous
// pkg/logrecord-based/client-side-only participation scheme, nothing here
// needs to independently gate reads or writes: a Command reachable through
// a Group a peer belongs to is enforced by SubmitCommand's
// isPermittedForCommand check in dispatch.go, not by anything in this file.
//
// dispatch.go's SubmitCommand/CommandRequest/CommandLog machinery is
// unaffected by this file: it's a separate, still pkg/logrecord-based
// mechanism (a durable request+response conversation, not ACL
// configuration) that keys off commandID alone instead of groupID.
//
// Not carried over from the old scheme: Group's Description and Command's
// GroupID/Description/FormSchema fields (a Command may now belong to
// multiple groups via GroupCommand, so a single GroupID field no longer
// makes sense, and the new daemon-enforced records have no room for
// free-form metadata -- shmevent.EncodeCommandPayload only carries name and
// peer_id). A caller that still wants a submission-form schema or
// human-readable description alongside a Command should keep that as its
// own pkg/logrecord entry (see LogAppend/LogQuery) keyed by the command id,
// the same general-purpose mechanism this package already exposes for any
// other structured record.
//
// Also not carried over: ResolveQRGroup/GroupView, the QR-scan convenience
// that resolved a scanned group id straight into its available commands.
// GroupCommand's key is commandID-first (cheap to scan "every group this
// command is linked to", not the reverse), so there is no efficient
// "every command linked to this group" primitive anymore -- the same reason
// desktop's pkg/kvctl deliberately didn't port this either (see CLAUDE.md's
// "Catalog/dispatch targets" section: "getgroup + listcommands already
// cover the same ground"). A caller wanting a QR-driven flow should decode
// the group id itself and call GetGroup + ListCommands (the full catalog,
// filtered client-side if needed).

// maxCatalogIDLen bounds Group/Command ids (validateCatalogID) and every
// pkg/logrecord unitID dispatch.go's still-logrecord-based mechanism
// writes -- kindPrefixBounds's fixed-width upper bound is built from this
// same constant, so it's provably wide enough to cover every possible key
// under a kind regardless of which unitIDs actually exist.
const maxCatalogIDLen = 256

func validateCatalogID(id string) error {
	if id == "" {
		return fmt.Errorf("kvmobile: id must not be empty")
	}
	if len(id) > maxCatalogIDLen {
		return fmt.Errorf("kvmobile: id exceeds %d bytes", maxCatalogIDLen)
	}
	return nil
}

// systemKeyIDOffset is how many leading bytes of a shmevent.SystemKey
// (kind + status placeholder) precede the trailing ID field on a
// GroupKey/CommandKey -- mirrors pkg/kvctl/catalog.go's identical
// constant.
const systemKeyIDOffset = 3

// revisionHistory is scanRevisions' result: a unitID's latest revision,
// plus who/when first created it (kept separately since "latest"
// overwrites Timestamp/AuthorPeerID on every update). Used only by
// dispatch.go's still-logrecord-based CommandRequest/CommandLog machinery
// -- Group/Command themselves no longer use it (see this file's doc
// comment).
type revisionHistory struct {
	latest    logrecord.Record
	createdAt time.Time
	createdBy string
	found     bool
}

// scanRevisions folds every logrecord.Record for (kind, unitID) down to
// its latest revision.
func scanRevisions(ctx context.Context, sess *shmclient.Session, kind, unitID string) (revisionHistory, error) {
	lo, hi := logrecord.ScanBounds(kind, unitID, time.Unix(0, 0), time.Now())
	var h revisionHistory
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return revisionHistory{}, err
		}
		if !ok {
			return h, nil
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return revisionHistory{}, err
		}
		if !h.found {
			h.createdAt = rec.Timestamp
			h.createdBy = rec.AuthorPeerID
		}
		h.latest = rec
		h.found = true
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// kindPrefixBounds returns the [lo, hi] key range covering every record
// of the given kind, across every unitID and timestamp -- the shared
// bound construction behind listUnitIDs and ListExecutionsByPeer's
// per-kind prefix scans.
func kindPrefixBounds(kind string) (lo, hi []byte) {
	prefix := logrecord.KindPrefix(kind)
	lo = prefix
	hi = make([]byte, len(prefix)+2+maxCatalogIDLen+8+8)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi
}

// listUnitIDs enumerates every distinct unitID that has ever logged a
// record of kind (see logrecord.KindPrefix), in ascending key order --
// multiple revisions of the same unitID are deduplicated, keeping
// first-seen order.
func listUnitIDs(ctx context.Context, sess *shmclient.Session, kind string) ([]string, error) {
	lo, hi := kindPrefixBounds(kind)

	seen := map[string]bool{}
	var ids []string
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, err
		}
		if !ok {
			return ids, nil
		}
		_, unitID, _, err := logrecord.ParseKey(key)
		if err != nil {
			return nil, err
		}
		if !seen[unitID] {
			seen[unitID] = true
			ids = append(ids, unitID)
		}
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// appendRecord builds and appends one logrecord.Record, attributed to
// authorPeerID -- the shared tail end every dispatch.go write in this
// package reduces to.
func appendRecord(ctx context.Context, sess *shmclient.Session, kind, unitID, authorPeerID string, fields map[string]string, narrative string) error {
	rnd, err := logrecord.NewRand()
	if err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	ts := time.Now()
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	rec := logrecord.Record{
		Kind:         kind,
		UnitID:       unitID,
		Timestamp:    ts,
		AuthorPeerID: authorPeerID,
		Fields:       fields,
		Narrative:    narrative,
	}
	value, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	if err := sess.LogAppend(ctx, key, value); err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	return nil
}

// Group is a named container Commands can be linked to via
// AddCommandToGroup -- peers become permitted to submit/execute a command
// by being added to a group linked to it (AddPeerToGroup).
type Group struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// CreateGroup defines a new command group under id -- or appends a fresh
// revision over an existing one, the same operation as UpdateGroup, just
// named for intent (see shmevent.EventGroupPut's Put semantics: create and
// update collapse into one call). Only a current raft voter may do this;
// pkg/daemon rejects it otherwise.
func CreateGroup(id, name string) error {
	return putGroup(id, name)
}

// UpdateGroup is CreateGroup's alias for the "this id already exists"
// case -- see CreateGroup's doc comment.
func UpdateGroup(id, name string) error {
	return putGroup(id, name)
}

func putGroup(id, name string) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.PutGroup(ctx, id, name); err != nil {
		return fmt.Errorf("kvmobile: put group: %w", err)
	}
	return nil
}

// DeleteGroup deletes Group id, cascading to every GroupCommand/PeerGroup
// record referencing it (see pkg/kvfsm.OpCascadeDelete). Only a current
// raft voter may do this.
func DeleteGroup(id string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.DeleteGroup(ctx, id); err != nil {
		return fmt.Errorf("kvmobile: delete group: %w", err)
	}
	return nil
}

// GetGroup returns id's current definition as a JSON Group, or an error if
// it doesn't exist.
func GetGroup(id string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	value, err := sess.Get(ctx, string(shmevent.GroupKey([]byte(id))))
	if err != nil {
		return "", fmt.Errorf("kvmobile: group %s not found", id)
	}

	out, err := json.Marshal(Group{ID: id, Name: shmevent.DecodeGroupPayload([]byte(value))})
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode group: %w", err)
	}
	return string(out), nil
}

// ListGroups returns every Group as a JSON array (`"[]"` when none exist).
func ListGroups() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi := shmevent.GroupKeyBounds()
	groups := []Group{}
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list groups: %w", err)
		}
		if !ok {
			break
		}
		if len(key) < systemKeyIDOffset {
			return "", fmt.Errorf("kvmobile: malformed group key %x", key)
		}
		groups = append(groups, Group{ID: string(key[systemKeyIDOffset:]), Name: shmevent.DecodeGroupPayload(value)})
		lo = append(append([]byte{}, key...), 0x00)
	}

	out, err := json.Marshal(groups)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode groups: %w", err)
	}
	return string(out), nil
}

// Command is a single submittable/executable operation: TargetPeerID is
// where it runs, gated by whichever groups it's linked to
// (AddCommandToGroup).
type Command struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	TargetPeerID string `json:"target_peer_id"`
}

// CreateCommand defines commandID, executable by targetPeerID -- see
// AddCommandToGroup for linking it into a group so peers can actually
// submit it. Like CreateGroup/UpdateGroup, this and UpdateCommand are the
// same Put operation, just named for intent. Only a current raft voter may
// do this.
func CreateCommand(id, name, targetPeerID string) error {
	return putCommand(id, name, targetPeerID)
}

// UpdateCommand is CreateCommand's alias for the "this id already exists"
// case -- see CreateCommand's doc comment.
func UpdateCommand(id, name, targetPeerID string) error {
	return putCommand(id, name, targetPeerID)
}

func putCommand(id, name, targetPeerID string) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	if targetPeerID == "" {
		return fmt.Errorf("kvmobile: command target_peer_id must not be empty")
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.PutCommand(ctx, id, name, []byte(targetPeerID)); err != nil {
		return fmt.Errorf("kvmobile: put command: %w", err)
	}
	return nil
}

// DeleteCommand deletes Command id, cascading to every GroupCommand record
// referencing it. Only a current raft voter may do this.
func DeleteCommand(id string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.DeleteCommand(ctx, id); err != nil {
		return fmt.Errorf("kvmobile: delete command: %w", err)
	}
	return nil
}

// GetCommand returns id's current definition as a JSON Command, or an
// error if it doesn't exist.
func GetCommand(id string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	value, err := sess.Get(ctx, string(shmevent.CommandKey([]byte(id))))
	if err != nil {
		return "", fmt.Errorf("kvmobile: command %s not found", id)
	}
	name, targetPeerID, err := shmevent.DecodeCommandPayload([]byte(value))
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode command %s: %w", id, err)
	}

	out, err := json.Marshal(Command{ID: id, Name: name, TargetPeerID: string(targetPeerID)})
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode command: %w", err)
	}
	return string(out), nil
}

// ListCommands returns every Command as a JSON array (`"[]"` when none
// exist) -- the full catalog, not scoped to any one group (see
// AddCommandToGroup/ListGroupsForCommand for that relation).
func ListCommands() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi := shmevent.CommandKeyBounds()
	commands := []Command{}
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list commands: %w", err)
		}
		if !ok {
			break
		}
		if len(key) < systemKeyIDOffset {
			return "", fmt.Errorf("kvmobile: malformed command key %x", key)
		}
		id := string(key[systemKeyIDOffset:])
		name, targetPeerID, err := shmevent.DecodeCommandPayload(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list commands: decode %s: %w", id, err)
		}
		commands = append(commands, Command{ID: id, Name: name, TargetPeerID: string(targetPeerID)})
		lo = append(append([]byte{}, key...), 0x00)
	}

	out, err := json.Marshal(commands)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode commands: %w", err)
	}
	return string(out), nil
}

// AddCommandToGroup links commandID to groupID -- peers added to groupID
// (AddPeerToGroup) become permitted to submit/execute commandID. Only a
// current raft voter may do this.
func AddCommandToGroup(commandID, groupID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.PutGroupCommand(ctx, []byte(commandID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvmobile: add command to group: %w", err)
	}
	return nil
}

// RemoveCommandFromGroup unlinks commandID from groupID.
func RemoveCommandFromGroup(commandID, groupID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.DeleteGroupCommand(ctx, []byte(commandID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvmobile: remove command from group: %w", err)
	}
	return nil
}

// ListGroupsForCommand returns every group id commandID is linked to, as a
// JSON array of strings (`"[]"` when none exist).
func ListGroupsForCommand(commandID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi, err := shmevent.GroupCommandBounds([]byte(commandID))
	if err != nil {
		return "", err
	}
	groupIDs := []string{}
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list groups for command: %w", err)
		}
		if !ok {
			break
		}
		_, groupID, err := shmevent.ParseGroupCommandKey(key)
		if err != nil {
			return "", err
		}
		groupIDs = append(groupIDs, string(groupID))
		lo = append(append([]byte{}, key...), 0x00)
	}

	out, err := json.Marshal(groupIDs)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode group ids: %w", err)
	}
	return string(out), nil
}

// AddPeerToGroup grants peerID membership in groupID. Only a current raft
// voter may do this.
func AddPeerToGroup(peerID, groupID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.PutPeerGroup(ctx, []byte(peerID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvmobile: add peer to group: %w", err)
	}
	return nil
}

// RemovePeerFromGroup revokes peerID's membership in groupID.
func RemovePeerFromGroup(peerID, groupID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.DeletePeerGroup(ctx, []byte(peerID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvmobile: remove peer from group: %w", err)
	}
	return nil
}

// ListGroupsForPeer returns every group id peerID belongs to, as a JSON
// array of strings (`"[]"` when none exist).
func ListGroupsForPeer(peerID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi, err := shmevent.PeerGroupBounds([]byte(peerID))
	if err != nil {
		return "", err
	}
	groupIDs := []string{}
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list groups for peer: %w", err)
		}
		if !ok {
			break
		}
		_, groupID, err := shmevent.ParsePeerGroupKey(key)
		if err != nil {
			return "", err
		}
		groupIDs = append(groupIDs, string(groupID))
		lo = append(append([]byte{}, key...), 0x00)
	}

	out, err := json.Marshal(groupIDs)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode group ids: %w", err)
	}
	return string(out), nil
}

// isPermittedForCommand reports whether peerID may submit/execute
// commandID: true if some group G satisfies both PeerGroup(peerID, G) and
// GroupCommand(commandID, G). Scans GroupCommandBounds(commandID) first (a
// command is expected to be linked to few groups, unlike a peer, which may
// belong to many) and point-checks PeerGroupKey(peerID, group) for each
// hit -- scan the smaller side, point-check the other -- the first match
// short-circuits. Mirrors pkg/kvctl/catalog.go's identical function.
func isPermittedForCommand(ctx context.Context, sess *shmclient.Session, peerID, commandID string) (bool, error) {
	lo, hi, err := shmevent.GroupCommandBounds([]byte(commandID))
	if err != nil {
		return false, err
	}
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		_, groupID, err := shmevent.ParseGroupCommandKey(key)
		if err != nil {
			return false, err
		}
		peerGroupKey, err := shmevent.PeerGroupKey([]byte(peerID), groupID)
		if err != nil {
			return false, err
		}
		if _, err := sess.Get(ctx, string(peerGroupKey)); err == nil {
			return true, nil
		}
		lo = append(append([]byte{}, key...), 0x00)
	}
}
