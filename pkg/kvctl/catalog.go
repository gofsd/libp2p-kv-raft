package kvctl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file is the desktop counterpart of mobile/kvmobile/catalog.go: a
// caller-defined catalog of "groups" (named containers, publicly
// listable) and "commands" (actionable operations scoped to a group,
// each naming a TargetPeerID that executes it and a FormSchema
// describing the inputs its submission form should collect), giving
// `mage` parity with the Android bindings for this layer. The logic is
// intentionally a close port, not a shared import, of kvmobile's copy --
// see openCurrentSession's doc comment for why -- but it returns native
// Go values (Group/Command/[]FormField) rather than kvmobile's JSON
// strings, matching this package's own existing convention (e.g.
// LogQuery returns []logrecord.Record, not a JSON string) since kvctl is
// consumed by Go callers (magefile.go, tests), not gomobile bindings
// that need string-only I/O.
//
// Both Group and Command are stored as pkg/logrecord.Record chains --
// append-only, with "update" meaning a fresh record under the same ID
// and "delete" meaning a tombstone record (Fields["deleted"] == "true");
// readers always fold a unitID's full revision history down to its
// latest entry (see scanRevisions). This reuses pkg/logrecord's own
// replication/durability and needs no new capnp wire schema -- every
// operation here is a plain EventLogAppend/EventListRange call, exactly
// like LogAppend/LogQuery.
//
// "Participant of group G" is a confirmed pkg/shmevent KindLogPermit
// record for logKind commandLogKind(G) -- see IsGroupParticipant.
// Deliberately the *same* string Commands are stored under, so
// participation and command-namespace access are one fact rather than
// two that could drift apart. Enforced client-side only, in kvctl
// itself: nothing in pkg/daemon independently blocks a local caller from
// reading or writing its own already-replicated store, so this check is
// only as strong as every caller actually going through these bindings
// rather than around them -- the same caveat kvmobile's copy documents.

// openCurrentSession opens a pkg/shmclient.Session for the registry's
// current node, alongside its own peer id (needed as AuthorPeerID for
// every write below) -- the registry.Open+reg.Current()+shmclient.Open
// sequence every other kvctl function already repeats per call, factored
// out here because catalog.go/dispatch.go's functions call it many times
// each. A *shmclient.Session has no Close/teardown to worry about (see
// its own doc comment) -- it's just a signing key holder for per-call
// shmring round trips, safe to open fresh on every call the way kvctl's
// existing functions already do.
func openCurrentSession(ctx context.Context) (sess *shmclient.Session, selfPeerID string, err error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, "", err
	}
	peerID, err := reg.Current()
	if err != nil {
		return nil, "", err
	}
	sess, err = shmclient.Open(ctx, peerID)
	if err != nil {
		return nil, "", fmt.Errorf("kvctl: open session: %w", err)
	}
	return sess, peerID, nil
}

// logGroupKind is the fixed pkg/logrecord Kind every Group definition is
// stored under. Unlike Command, Group listing/reading has no
// participation gate (see CreateGroup's doc comment: it's a public
// catalog), so it doesn't need a per-group Kind the way commandLogKind
// does.
const logGroupKind = "group"

// commandLogKind returns the pkg/logrecord Kind every Command belonging
// to groupID is stored under, *and* the RequestLogPermit/
// ConfirmLogPermit/RevokeLogPermit logKind that gates participation in
// that same group -- see this file's doc comment for why those are
// deliberately the same string.
func commandLogKind(groupID string) string {
	return "command:" + groupID
}

// maxCatalogIDLen bounds Group/Command IDs, enforced by
// validateCatalogID -- kindPrefixBounds's fixed-width upper bound is
// built from this same constant, so it's provably wide enough to cover
// every possible key under a kind regardless of which unitIDs actually
// exist.
const maxCatalogIDLen = 256

func validateCatalogID(id string) error {
	if id == "" {
		return fmt.Errorf("kvctl: id must not be empty")
	}
	if len(id) > maxCatalogIDLen {
		return fmt.Errorf("kvctl: id exceeds %d bytes", maxCatalogIDLen)
	}
	return nil
}

// IsGroupParticipant implements `mage isgroupparticipant <groupID>`:
// reports whether the current node's own peer id currently holds a
// confirmed permit for groupID (see commandLogKind) -- "is a participant
// of group G". Every Command CRUD/list/get binding below requires this
// before proceeding; Group create/read/list do not (a public catalog) --
// only UpdateGroup/DeleteGroup do.
func IsGroupParticipant(groupID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, selfPeerID, err := openCurrentSession(ctx)
	if err != nil {
		return false, err
	}

	key, err := shmevent.LogPermitKey(shmevent.StatusConfirmed, commandLogKind(groupID), []byte(selfPeerID))
	if err != nil {
		return false, fmt.Errorf("kvctl: is group participant: %w", err)
	}
	if _, err := sess.Get(ctx, string(key)); err != nil {
		return false, nil
	}
	return true, nil
}

// RequestGroupParticipation implements `mage requestgroupparticipation
// <groupID> <peerID> <metadata>`: lodges a pending request for
// targetPeerID to participate in groupID -- a thin, group-scoped wrapper
// over RequestLogPermit(commandLogKind(groupID), ...) so callers don't
// need to know that naming convention themselves.
func RequestGroupParticipation(groupID, targetPeerID, metadata string) error {
	return RequestLogPermit(commandLogKind(groupID), []byte(targetPeerID), []byte(metadata))
}

// ConfirmGroupParticipation implements `mage confirmgroupparticipation
// <groupID> <peerID>`: promotes a pending participation request for
// targetPeerID in groupID to confirmed. Only takes effect if the current
// node is itself a raft voter (see ConfirmLogPermit's doc comment).
func ConfirmGroupParticipation(groupID, targetPeerID string) error {
	return ConfirmLogPermit(commandLogKind(groupID), []byte(targetPeerID))
}

// RevokeGroupParticipation implements `mage revokegroupparticipation
// <groupID> <peerID>`: deletes a confirmed participation record for
// targetPeerID in groupID outright.
func RevokeGroupParticipation(groupID, targetPeerID string) error {
	return RevokeLogPermit(commandLogKind(groupID), []byte(targetPeerID))
}

func requireGroupParticipant(groupID string) error {
	ok, err := IsGroupParticipant(groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("kvctl: not a participant of group %s", groupID)
	}
	return nil
}

// revisionHistory is scanRevisions' result: a unitID's latest revision,
// plus who/when first created it (kept separately since "latest"
// overwrites Timestamp/AuthorPeerID on every update).
type revisionHistory struct {
	latest    logrecord.Record
	createdAt time.Time
	createdBy string
	found     bool
}

// scanRevisions folds every logrecord.Record for (kind, unitID) down to
// its latest revision -- Group/Command's "current state" under the
// append-only/latest-wins scheme this file's doc comment describes.
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
// per-kind prefix scans. hi is sized from maxCatalogIDLen so it's
// provably wide enough to cover any unitID this package itself ever
// writes under kind, regardless of which ones actually exist.
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
// authorPeerID -- the shared tail end every Group/Command/dispatch write
// in this package reduces to.
func appendRecord(ctx context.Context, sess *shmclient.Session, kind, unitID, authorPeerID string, fields map[string]string, narrative string) error {
	rnd, err := logrecord.NewRand()
	if err != nil {
		return fmt.Errorf("kvctl: %w", err)
	}
	ts := time.Now()
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		return fmt.Errorf("kvctl: %w", err)
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
		return fmt.Errorf("kvctl: %w", err)
	}
	if err := sess.LogAppend(ctx, key, value); err != nil {
		return fmt.Errorf("kvctl: %w", err)
	}
	return nil
}

// Group is a caller-defined command group: a named, publicly listable
// container for a set of Commands. Participation (see
// IsGroupParticipant) gates Command access within it, not the Group
// definition itself.
type Group struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func recordToGroup(h revisionHistory) Group {
	return Group{
		ID:          h.latest.UnitID,
		Name:        h.latest.Fields["name"],
		Description: h.latest.Narrative,
		CreatedBy:   h.createdBy,
		CreatedAt:   h.createdAt,
		UpdatedAt:   h.latest.Timestamp,
	}
}

// CreateGroup implements `mage creategroup <id> <name> <description>`:
// defines a new command group under id (or appends a fresh revision over
// an existing/deleted one -- see UpdateGroup, the same operation under a
// different name). Unlike UpdateGroup/DeleteGroup, this has no
// participation requirement: Groups are a public catalog, so any cluster
// member may propose one.
func CreateGroup(id, name, description string) error {
	return putGroup(id, name, description, false, false)
}

// UpdateGroup implements `mage updategroup <id> <name> <description>`:
// appends a new revision for id's name/description. Requires the caller
// to already be a participant of id, unlike CreateGroup.
func UpdateGroup(id, name, description string) error {
	return putGroup(id, name, description, false, true)
}

// DeleteGroup implements `mage deletegroup <id>`: appends a tombstone
// revision for id -- GetGroup/ListGroups exclude it afterward. Existing
// Command records under it aren't themselves deleted, just unreachable
// through the catalog. Requires the caller to already be a participant
// of id.
func DeleteGroup(id string) error {
	return putGroup(id, "", "", true, true)
}

func putGroup(id, name, description string, deleted, requireParticipant bool) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	if requireParticipant {
		if err := requireGroupParticipant(id); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, selfPeerID, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}

	fields := map[string]string{"name": name}
	if deleted {
		fields["deleted"] = "true"
	}
	if err := appendRecord(ctx, sess, logGroupKind, id, selfPeerID, fields, description); err != nil {
		return fmt.Errorf("kvctl: put group: %w", err)
	}
	return nil
}

// GetGroup implements `mage getgroup <id>`: returns id's current
// definition, or an error if it doesn't exist or was deleted.
func GetGroup(id string) (Group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return Group{}, err
	}

	h, err := scanRevisions(ctx, sess, logGroupKind, id)
	if err != nil {
		return Group{}, fmt.Errorf("kvctl: get group: %w", err)
	}
	if !h.found || h.latest.Fields["deleted"] == "true" {
		return Group{}, fmt.Errorf("kvctl: group %s not found", id)
	}
	return recordToGroup(h), nil
}

// ListGroups implements `mage listgroups`: returns every non-deleted
// Group (nil, not an error, when none exist). No participation check --
// see CreateGroup's doc comment.
func ListGroups() ([]Group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}

	ids, err := listUnitIDs(ctx, sess, logGroupKind)
	if err != nil {
		return nil, fmt.Errorf("kvctl: list groups: %w", err)
	}

	var groups []Group
	for _, id := range ids {
		h, err := scanRevisions(ctx, sess, logGroupKind, id)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list groups: %w", err)
		}
		if !h.found || h.latest.Fields["deleted"] == "true" {
			continue
		}
		groups = append(groups, recordToGroup(h))
	}
	return groups, nil
}

// FormField describes one input a Command's submission form should
// collect -- purely descriptive metadata for the calling UI to render a
// form from; kvctl does not validate submitted values against it.
type FormField struct {
	Name     string   `json:"name"`
	Label    string   `json:"label"`
	Type     string   `json:"type"`
	Required bool     `json:"required,omitempty"`
	Options  []string `json:"options,omitempty"`
}

// Command is a single actionable operation belonging to a Group,
// executed by TargetPeerID and described by FormSchema for the calling
// UI to render an input form from -- kvctl does not itself interpret or
// run a Command, only defines/discovers it (see SubmitCommand in
// dispatch.go for dispatching and auditing one).
type Command struct {
	ID           string      `json:"id"`
	GroupID      string      `json:"group_id"`
	TargetPeerID string      `json:"target_peer_id"`
	Name         string      `json:"name"`
	Description  string      `json:"description"`
	FormSchema   []FormField `json:"form_schema,omitempty"`
	CreatedBy    string      `json:"created_by"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

func recordToCommand(h revisionHistory) (Command, error) {
	rec := h.latest
	c := Command{
		ID:           rec.UnitID,
		GroupID:      rec.Fields["group_id"],
		TargetPeerID: rec.Fields["target_peer_id"],
		Name:         rec.Fields["name"],
		Description:  rec.Narrative,
		CreatedBy:    h.createdBy,
		CreatedAt:    h.createdAt,
		UpdatedAt:    rec.Timestamp,
	}
	if schemaJSON := rec.Fields["form_schema"]; schemaJSON != "" {
		if err := json.Unmarshal([]byte(schemaJSON), &c.FormSchema); err != nil {
			return Command{}, fmt.Errorf("kvctl: decode form schema: %w", err)
		}
	}
	return c, nil
}

// CreateCommand implements `mage createcommand <id> <groupID>
// <targetPeerID> <name> <description> <formSchemaJSON>`: defines
// commandID as belonging to groupID, executable by targetPeerID and
// described by formSchema (nil for none) -- the calling UI renders its
// submission form from that schema. Like CreateGroup/UpdateGroup, this
// and UpdateCommand are the same append operation, just named for
// intent. Requires the caller to already be a participant of groupID
// (see IsGroupParticipant) -- unlike CreateGroup, Command writes are
// always gated.
func CreateCommand(id, groupID, targetPeerID, name, description string, formSchema []FormField) error {
	return putCommand(id, groupID, targetPeerID, name, description, formSchema, false)
}

// UpdateCommand is CreateCommand's alias for the "this id already
// exists" case -- see CreateCommand's doc comment.
func UpdateCommand(id, groupID, targetPeerID, name, description string, formSchema []FormField) error {
	return putCommand(id, groupID, targetPeerID, name, description, formSchema, false)
}

// DeleteCommand implements `mage deletecommand <groupID> <id>`: appends
// a tombstone revision for id within groupID -- GetCommand/ListCommands
// exclude it afterward. Requires the caller to already be a participant
// of groupID.
func DeleteCommand(groupID, id string) error {
	return putCommand(id, groupID, "", "", "", nil, true)
}

func putCommand(id, groupID, targetPeerID, name, description string, formSchema []FormField, deleted bool) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	if groupID == "" {
		return fmt.Errorf("kvctl: command group_id must not be empty")
	}
	if err := requireGroupParticipant(groupID); err != nil {
		return err
	}
	if !deleted && targetPeerID == "" {
		return fmt.Errorf("kvctl: command target_peer_id must not be empty")
	}

	fields := map[string]string{
		"group_id":       groupID,
		"target_peer_id": targetPeerID,
		"name":           name,
	}
	if deleted {
		fields["deleted"] = "true"
	} else if len(formSchema) > 0 {
		schemaJSON, err := json.Marshal(formSchema)
		if err != nil {
			return fmt.Errorf("kvctl: encode form schema: %w", err)
		}
		fields["form_schema"] = string(schemaJSON)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, selfPeerID, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := appendRecord(ctx, sess, commandLogKind(groupID), id, selfPeerID, fields, description); err != nil {
		return fmt.Errorf("kvctl: put command: %w", err)
	}
	return nil
}

// GetCommand implements `mage getcommand <groupID> <id>`: returns
// commandID's current definition within groupID, or an error if it
// doesn't exist, was deleted, or the caller isn't a participant of
// groupID.
func GetCommand(groupID, id string) (Command, error) {
	if err := requireGroupParticipant(groupID); err != nil {
		return Command{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return Command{}, err
	}

	h, err := scanRevisions(ctx, sess, commandLogKind(groupID), id)
	if err != nil {
		return Command{}, fmt.Errorf("kvctl: get command: %w", err)
	}
	if !h.found || h.latest.Fields["deleted"] == "true" {
		return Command{}, fmt.Errorf("kvctl: command %s not found in group %s", id, groupID)
	}

	cmd, err := recordToCommand(h)
	if err != nil {
		return Command{}, fmt.Errorf("kvctl: get command: %w", err)
	}
	return cmd, nil
}

// ListCommands implements `mage listcommands <groupID>`: returns every
// non-deleted Command currently defined under groupID (nil, not an
// error, when none exist), or an error if the caller isn't a participant
// of groupID -- the binding behind "if the current node is a participant
// of this group, it sees the list of available commands."
func ListCommands(groupID string) ([]Command, error) {
	if err := requireGroupParticipant(groupID); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}

	ids, err := listUnitIDs(ctx, sess, commandLogKind(groupID))
	if err != nil {
		return nil, fmt.Errorf("kvctl: list commands: %w", err)
	}

	var commands []Command
	for _, id := range ids {
		h, err := scanRevisions(ctx, sess, commandLogKind(groupID), id)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list commands: %w", err)
		}
		if !h.found || h.latest.Fields["deleted"] == "true" {
			continue
		}
		cmd, err := recordToCommand(h)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list commands: %w", err)
		}
		commands = append(commands, cmd)
	}
	return commands, nil
}
