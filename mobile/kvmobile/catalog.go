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
// Partly carried over from the old scheme: Command's FormSchema came back,
// as Spec (see CreateCommandWithSpec) -- shmevent.EncodeCommandPayloadWithSpec
// carries name, peer_id and an opaque spec, so a submission form once again
// travels with the command it belongs to instead of needing a parallel
// pkg/logrecord entry keyed by the same id. Group's Description and Command's
// GroupID did not come back: a Command may belong to multiple groups via
// GroupCommand, so a single GroupID field no longer makes sense, and a
// human-readable description belongs in the spec if a client wants one.
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
// by being added to a group linked to it (AddPeerToGroup), or
// unconditionally if Public is true (see isPermittedForCommand).
type Group struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Public bool   `json:"public"`
}

// CreateGroup defines a new command group under id -- or appends a fresh
// revision over an existing one, the same operation as UpdateGroup, just
// named for intent (see shmevent.EventGroupPut's Put semantics: create and
// update collapse into one call). public, if true, grants unconditional
// access to this group's linked commands to any peer, with no
// AddPeerToGroup membership needed. Only a current raft voter may do
// this; pkg/daemon rejects it otherwise.
func CreateGroup(id, name string, public bool) error {
	return putGroup(id, name, public)
}

// UpdateGroup is CreateGroup's alias for the "this id already exists"
// case -- see CreateGroup's doc comment.
func UpdateGroup(id, name string, public bool) error {
	return putGroup(id, name, public)
}

func putGroup(id, name string, public bool) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.PutGroup(ctx, id, name, public); err != nil {
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

	name, public, err := shmevent.DecodeGroupPayload([]byte(value))
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode group %s: %w", id, err)
	}
	out, err := json.Marshal(Group{ID: id, Name: name, Public: public})
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
		name, public, err := shmevent.DecodeGroupPayload(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: decode group %x: %w", key, err)
		}
		groups = append(groups, Group{ID: string(key[systemKeyIDOffset:]), Name: name, Public: public})
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
	// Spec is the command's form definition, verbatim as stored, omitted
	// when it has none. See CreateCommandWithSpec.
	Spec string `json:"spec,omitempty"`
}

// Station is a device's operational description -- see
// shmevent.KindStation, and PutStation/ListStations below.
type Station struct {
	PeerID string `json:"peer_id"`
	Name   string `json:"name"`
	Attrs  string `json:"attrs,omitempty"`
}

// CreateCommand defines commandID, executable by targetPeerID -- see
// AddCommandToGroup for linking it into a group so peers can actually
// submit it. Like CreateGroup/UpdateGroup, this and UpdateCommand are the
// same Put operation, just named for intent. Only a current raft voter may
// do this.
func CreateCommand(id, name, targetPeerID string) error {
	return putCommand(id, name, targetPeerID, "")
}

// CreateCommandWithSpec is CreateCommand carrying the command's form
// definition as well -- an opaque string (JSON, in practice) the cluster
// replicates verbatim and every device renders its inputs from. This is what
// lets a new command reach every device without any of them gaining new
// code: the definition travels through raft like any other catalog record.
//
// targetPeerID may be empty here, unlike CreateCommand's original contract: a
// definition can exist before anyone decides which device runs it. Submitting
// such a command fails, naming the missing target.
func CreateCommandWithSpec(id, name, targetPeerID, spec string) error {
	return putCommand(id, name, targetPeerID, spec)
}

// UpdateCommandWithSpec is CreateCommandWithSpec's alias for the "this id
// already exists" case.
func UpdateCommandWithSpec(id, name, targetPeerID, spec string) error {
	return putCommand(id, name, targetPeerID, spec)
}

// UpdateCommand is CreateCommand's alias for the "this id already exists"
// case -- see CreateCommand's doc comment.
func UpdateCommand(id, name, targetPeerID string) error {
	return putCommand(id, name, targetPeerID, "")
}

func putCommand(id, name, targetPeerID, spec string) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.PutCommandWithSpec(ctx, id, name, []byte(targetPeerID), []byte(spec)); err != nil {
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
	name, targetPeerID, spec, err := shmevent.DecodeCommandPayloadFull([]byte(value))
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode command %s: %w", id, err)
	}

	out, err := json.Marshal(Command{ID: id, Name: name, TargetPeerID: string(targetPeerID), Spec: string(spec)})
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
	commands, err := listCommands(ctx, sess)
	if err != nil {
		return "", err
	}

	out, err := json.Marshal(commands)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode commands: %w", err)
	}
	return string(out), nil
}

// listCommands is ListCommands' scan, typed -- for an in-process caller
// (lua.go's runner adapter) that wants the records themselves rather than
// the JSON a gomobile binding has to hand back.
func listCommands(ctx context.Context, sess *shmclient.Session) ([]Command, error) {
	lo, hi := shmevent.CommandKeyBounds()
	commands := []Command{}
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("kvmobile: list commands: %w", err)
		}
		if !ok {
			return commands, nil
		}
		if len(key) < systemKeyIDOffset {
			return nil, fmt.Errorf("kvmobile: malformed command key %x", key)
		}
		id := string(key[systemKeyIDOffset:])
		name, targetPeerID, spec, err := shmevent.DecodeCommandPayloadFull(value)
		if err != nil {
			return nil, fmt.Errorf("kvmobile: list commands: decode %s: %w", id, err)
		}
		commands = append(commands, Command{ID: id, Name: name, TargetPeerID: string(targetPeerID), Spec: string(spec)})
		lo = append(append([]byte{}, key...), 0x00)
	}
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
// commandID: true if some group G linked to commandID (GroupCommand(
// commandID, G)) either has its own Public flag set, or satisfies
// PeerGroup(peerID, G) -- a public group admits any peer with no
// PeerGroup membership record needed at all. Scans GroupCommandBounds(
// commandID) first (a command is expected to be linked to few groups,
// unlike a peer, which may belong to many), checking each hit's Group
// record before falling back to a PeerGroupKey(peerID, group) point-check
// -- scan the smaller side, point-check the other -- the first match
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
		if groupValue, err := sess.Get(ctx, string(shmevent.GroupKey(groupID))); err == nil {
			if _, public, err := shmevent.DecodeGroupPayload([]byte(groupValue)); err == nil && public {
				return true, nil
			}
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

// PutStation creates or updates the description of the device peerID -- a
// human-readable name plus opaque attrs (JSON, in practice). Only a current
// raft voter may do this, so a device cannot rename itself.
//
// This is what turns a 52-character peer id into something a person can read:
// every other record naming a device -- a Command's target, a group
// membership, an execution's executor -- names it by peer id alone.
func PutStation(peerID, name, attrs string) error {
	if peerID == "" {
		return fmt.Errorf("kvmobile: station peer_id must not be empty")
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.PutStation(ctx, []byte(peerID), name, []byte(attrs)); err != nil {
		return fmt.Errorf("kvmobile: put station: %w", err)
	}
	return nil
}

// DeleteStation removes peerID's description. Its cluster membership and
// group memberships are untouched -- this deletes what the device is called,
// not what it is.
func DeleteStation(peerID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.DeleteStation(ctx, []byte(peerID)); err != nil {
		return fmt.Errorf("kvmobile: delete station: %w", err)
	}
	return nil
}

// GetStation returns peerID's station description as a JSON object.
func GetStation(peerID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	value, err := sess.Get(ctx, string(shmevent.StationKey([]byte(peerID))))
	if err != nil {
		return "", fmt.Errorf("kvmobile: station %s not found", peerID)
	}
	name, attrs, err := shmevent.DecodeStationPayload([]byte(value))
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode station %s: %w", peerID, err)
	}
	out, err := json.Marshal(Station{PeerID: peerID, Name: name, Attrs: string(attrs)})
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode station: %w", err)
	}
	return string(out), nil
}

// ListStations returns every station description as a JSON array (`"[]"`
// when none exist).
func ListStations() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi := shmevent.StationKeyBounds()
	stations := []Station{}
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list stations: %w", err)
		}
		if !ok {
			break
		}
		if len(key) < systemKeyIDOffset {
			return "", fmt.Errorf("kvmobile: malformed station key %x", key)
		}
		peerID := string(key[systemKeyIDOffset:])
		name, attrs, err := shmevent.DecodeStationPayload(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list stations: decode %s: %w", peerID, err)
		}
		stations = append(stations, Station{PeerID: peerID, Name: name, Attrs: string(attrs)})
		lo = append(append([]byte{}, key...), 0x00)
	}
	out, err := json.Marshal(stations)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode stations: %w", err)
	}
	return string(out), nil
}
