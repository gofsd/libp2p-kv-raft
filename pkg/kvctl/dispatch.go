package kvctl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// This file is the desktop counterpart of mobile/kvmobile/dispatch.go:
// SubmitCommand dispatches a catalog.go Command as a durable, replicated
// request plus a low-latency Execute poke to whoever executes it, and
// QueryCommandLog/LatestCommandLog read back the execution log the
// target writes with AppendCommandLog as it works. Like catalog.go,
// every operation here is a plain EventGet/EventLogAppend/
// EventListRange/EventExecute call -- no new capnp wire schema.
//
// kvctl only dispatches and records; it never interprets or runs a
// Command itself -- that's the target's own application logic, watching
// for requests (ListCommandRequests as a catch-up fallback if an Execute
// poke never arrives -- see SubmitCommand's doc comment) and reporting
// back via AppendCommandLog.
//
// Deliberately not ported: kvmobile's ResolveQRGroup (a QR-scan
// convenience with no CLI equivalent -- `mage getgroup`+`mage
// listcommands` already cover the same ground for an operator who
// already knows the group id) and WatchCommandLog/StopWatchCommandLog
// (a callback-driven background poll loop that doesn't fit a one-shot
// mage invocation -- the same accepted gap kvmobile's WatchExecute
// already has no `mage` binding for; use `mage querycommandlog` with a
// `since` bound, or `mage latestcommandlog`, and poll it yourself if a
// script needs to watch).

// logCommandExecKind is the fixed pkg/logrecord Kind every
// AppendCommandLog entry is stored under, keyed by instance id (globally
// unique, not scoped to a group -- see newInstanceID) rather than a
// per-group Kind the way Command/CommandRequest are, since a caller
// tracking one dispatch already knows exactly which instance id it
// wants, with no need to enumerate "every log entry in group G".
const logCommandExecKind = "cmdlog"

// commandRequestLogKind returns the pkg/logrecord Kind every
// SubmitCommand dispatch (CommandRequest) belonging to groupID is stored
// under -- same per-group-namespacing reasoning as commandLogKind in
// catalog.go, so ListCommandRequests can enumerate a group's pending
// requests with one prefix scan.
func commandRequestLogKind(groupID string) string {
	return "cmdreq:" + groupID
}

// commandExecIndexKind returns the pkg/logrecord Kind SubmitCommand
// indexes a dispatch under for peerID's sake, once per role (requester,
// target) peerID plays in it -- see ListExecutionsByPeer, which this
// makes a single per-peer prefix scan instead of iterating every group's
// ListCommandRequests looking for peerID's dispatches.
func commandExecIndexKind(peerID string) string {
	return "cmdexec:" + peerID
}

// execIndexRoleRequester/execIndexRoleTarget are commandExecIndexKind
// entries' "role" field values -- kept to one byte (see
// appendCommandExecIndex's doc comment on why this index is deliberately
// thin) rather than the human-readable "requester"/"target" strings
// ListExecutionsByPeer's CommandExecution.Role actually returns.
const (
	execIndexRoleRequester = "r"
	execIndexRoleTarget    = "t"
)

// appendCommandExecIndex writes one commandExecIndexKind(peerID) entry
// for instanceID, naming groupID/commandID and peerID's role in this
// dispatch (execIndexRoleRequester/execIndexRoleTarget), attributed to
// requesterPeerID -- SubmitCommand calls this once per role peerID plays
// in a dispatch.
//
// Deliberately thin: it stores only what ListExecutionsByPeer can't
// otherwise derive. It does not store requesterPeerID as a Fields entry
// (that's already the record's own AuthorPeerID) or targetPeerID
// (redundant with peerID itself when role is target; ListExecutionsByPeer
// looks it up via GetCommand for a role-requester entry instead). This
// matters because commandExecIndexKind(peerID) already embeds a full
// peer id in the pkg/logrecord key (see BuildKey), and every record here
// shares pkg/shmevent.ValueSize's single 512-byte budget across key *and*
// value combined -- an earlier version of this (ported from kvmobile's
// own first draft) stored requested_by/target_peer_id directly and blew
// that budget the moment two real peer ids (~52 bytes each) were
// involved at once -- see mobile/kvmobile/dispatch.go's identical doc
// comment for the full story.
func appendCommandExecIndex(ctx context.Context, sess *shmclient.Session, peerID, instanceID, groupID, commandID, requesterPeerID, role string) error {
	fields := map[string]string{
		"group_id":   groupID,
		"command_id": commandID,
		"role":       role,
	}
	return appendRecord(ctx, sess, commandExecIndexKind(peerID), instanceID, requesterPeerID, fields, "")
}

// executePoke is the small JSON envelope SubmitCommand/AppendCommandLog
// send over Execute as an optional low-latency nudge -- see
// mobile/kvmobile/dispatch.go's identical type for the full design (kept
// as a distinct copy, not a shared import, so the two clients don't need
// to agree on a shared internal package for a private wire detail
// neither exposes outside this file).
type executePoke struct {
	Type       string `json:"type"`
	GroupID    string `json:"group_id,omitempty"`
	CommandID  string `json:"command_id,omitempty"`
	InstanceID string `json:"instance_id"`
}

// newInstanceID returns a fresh random hex id for one SubmitCommand
// dispatch -- globally unique (not scoped to a group) since
// GetCommandRequest/QueryCommandLog all key off it alone.
func newInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("kvctl: generate instance id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// CommandRequest is SubmitCommand's durable record of one dispatch --
// GetCommandRequest/ListCommandRequests read it back. There's no
// update/delete for these, only the single revision SubmitCommand
// writes.
type CommandRequest struct {
	InstanceID  string    `json:"instance_id"`
	GroupID     string    `json:"group_id"`
	CommandID   string    `json:"command_id"`
	RequestedBy string    `json:"requested_by"`
	Inputs      string    `json:"inputs,omitempty"` // caller-defined JSON, opaque to kvctl
	RequestedAt time.Time `json:"requested_at"`
}

func recordToCommandRequest(h revisionHistory) CommandRequest {
	return CommandRequest{
		InstanceID:  h.latest.UnitID,
		GroupID:     h.latest.Fields["group_id"],
		CommandID:   h.latest.Fields["command_id"],
		RequestedBy: h.latest.AuthorPeerID,
		Inputs:      h.latest.Fields["inputs"],
		RequestedAt: h.latest.Timestamp,
	}
}

// SubmitCommand implements `mage submitcommand <groupID> <commandID>
// <inputsJSON>`: dispatches commandID (which must already exist in
// groupID -- see CreateCommand) with inputsJSON (caller-defined, opaque
// to kvctl -- typically the JSON object a form built from the Command's
// FormSchema produced) as a durable, replicated CommandRequest, then
// sends the command's TargetPeerID a low-latency Execute poke naming the
// new instance id (best-effort: a failed poke doesn't fail the dispatch,
// since the durable request is the real source of truth -- see
// ListCommandRequests for the target's catch-up path if the poke never
// arrives). Returns the instance id, which the caller uses with
// GetCommandRequest/QueryCommandLog/LatestCommandLog to track this
// specific dispatch. Requires the caller to already be a participant of
// groupID.
//
// kvctl only dispatches and records the request; actually running
// commandID is the target's own application logic (see AppendCommandLog
// for how it reports back).
func SubmitCommand(groupID, commandID, inputsJSON string) (string, error) {
	if err := requireGroupParticipant(groupID); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, requesterPeerID, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}

	h, err := scanRevisions(ctx, sess, commandLogKind(groupID), commandID)
	if err != nil {
		return "", fmt.Errorf("kvctl: submit command: %w", err)
	}
	if !h.found || h.latest.Fields["deleted"] == "true" {
		return "", fmt.Errorf("kvctl: command %s not found in group %s", commandID, groupID)
	}
	targetPeerID := h.latest.Fields["target_peer_id"]

	instanceID, err := newInstanceID()
	if err != nil {
		return "", err
	}

	fields := map[string]string{
		"group_id":   groupID,
		"command_id": commandID,
	}
	if inputsJSON != "" {
		fields["inputs"] = inputsJSON
	}
	if err := appendRecord(ctx, sess, commandRequestLogKind(groupID), instanceID, requesterPeerID, fields, ""); err != nil {
		return "", fmt.Errorf("kvctl: submit command: %w", err)
	}

	if err := appendCommandExecIndex(ctx, sess, requesterPeerID, instanceID, groupID, commandID, requesterPeerID, execIndexRoleRequester); err != nil {
		return "", fmt.Errorf("kvctl: submit command: %w", err)
	}
	if targetPeerID != requesterPeerID {
		if err := appendCommandExecIndex(ctx, sess, targetPeerID, instanceID, groupID, commandID, requesterPeerID, execIndexRoleTarget); err != nil {
			return "", fmt.Errorf("kvctl: submit command: %w", err)
		}
	}

	if poke, err := json.Marshal(executePoke{Type: "cmd_req", GroupID: groupID, CommandID: commandID, InstanceID: instanceID}); err == nil {
		_ = Execute(targetPeerID, string(poke))
	}

	return instanceID, nil
}

// GetCommandRequest implements `mage getcommandrequest <groupID>
// <instanceID>`: returns instanceID's dispatch record within groupID, or
// an error if it doesn't exist or the caller isn't a participant of
// groupID. Typically used by the target after receiving a "cmd_req"
// Execute poke (see executePoke), to learn which command and inputs it
// names.
func GetCommandRequest(groupID, instanceID string) (CommandRequest, error) {
	if err := requireGroupParticipant(groupID); err != nil {
		return CommandRequest{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return CommandRequest{}, err
	}

	h, err := scanRevisions(ctx, sess, commandRequestLogKind(groupID), instanceID)
	if err != nil {
		return CommandRequest{}, fmt.Errorf("kvctl: get command request: %w", err)
	}
	if !h.found {
		return CommandRequest{}, fmt.Errorf("kvctl: command request %s not found in group %s", instanceID, groupID)
	}
	return recordToCommandRequest(h), nil
}

// ListCommandRequests implements `mage listcommandrequests <groupID>`:
// returns every dispatch request currently recorded for groupID (nil,
// not an error, when none exist), oldest first. How a target catches up
// on requests it might have missed an Execute poke for -- pokes are
// unreplicated and dropped if the target wasn't running to receive them
// (see SubmitCommand's doc comment) -- e.g. on startup, or polled
// periodically as a reliability fallback.
func ListCommandRequests(groupID string) ([]CommandRequest, error) {
	if err := requireGroupParticipant(groupID); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}

	ids, err := listUnitIDs(ctx, sess, commandRequestLogKind(groupID))
	if err != nil {
		return nil, fmt.Errorf("kvctl: list command requests: %w", err)
	}

	var requests []CommandRequest
	for _, id := range ids {
		h, err := scanRevisions(ctx, sess, commandRequestLogKind(groupID), id)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list command requests: %w", err)
		}
		if !h.found {
			continue
		}
		requests = append(requests, recordToCommandRequest(h))
	}
	return requests, nil
}

// maxExecutionsByPeer bounds ListExecutionsByPeer's result to the 200
// most recent executions touching a peer -- enough to render a
// meaningful recent-activity view without pulling in a peer's entire
// dispatch history over shmring on every call. Matches
// mobile/kvmobile/dispatch.go's own bound.
const maxExecutionsByPeer = 200

// CommandExecution is one SubmitCommand dispatch as it appears from
// peerID's point of view (see ListExecutionsByPeer) -- Role is
// "requester" or "target" depending on which side of the dispatch
// peerID was on. The same instance appears twice, once under each role's
// peer, if requester and target differ. TargetPeerID is "" for a
// requester-role entry if this node could not resolve it (see
// targetPeerIDForCommand) -- e.g. the command was since deleted, or this
// node isn't a participant of that group.
type CommandExecution struct {
	InstanceID   string    `json:"instance_id"`
	GroupID      string    `json:"group_id"`
	CommandID    string    `json:"command_id"`
	RequestedBy  string    `json:"requested_by"`
	TargetPeerID string    `json:"target_peer_id"`
	Role         string    `json:"role"`
	RequestedAt  time.Time `json:"requested_at"`
}

// targetPeerIDForCommand best-effort resolves commandID's current
// TargetPeerID within groupID -- ListExecutionsByPeer's fallback for a
// role-requester index entry, which (see appendCommandExecIndex's doc
// comment on why the index is deliberately thin) doesn't store
// target_peer_id itself. Returns "" rather than an error if the command
// was since deleted or this node isn't (or is no longer) a participant
// of groupID -- a missing detail on one history entry shouldn't fail the
// whole list.
func targetPeerIDForCommand(groupID, commandID string) string {
	cmd, err := GetCommand(groupID, commandID)
	if err != nil {
		return ""
	}
	return cmd.TargetPeerID
}

// ListExecutionsByPeer implements `mage listexecutions <peerID>`:
// returns up to the maxExecutionsByPeer most recent SubmitCommand
// dispatches touching peerID, as either requester or target, most
// recent first -- the binding behind "show me every command execution
// involving this peer, across every group, without me iterating
// ListCommandRequests per group myself." Backed by the dedicated
// per-peer index SubmitCommand writes at dispatch time (see
// commandExecIndexKind/appendCommandExecIndex), so this costs one prefix
// scan over peerID's own dispatch history, not O(groups) -- plus one
// GetCommand lookup per requester-role entry to resolve TargetPeerID
// (see targetPeerIDForCommand), since the index itself doesn't carry it.
//
// There is no reverse-scan primitive anywhere in this stack
// (pkg/store.ScanRange is `ORDER BY key ASC` only, and
// pkg/shmevent.EventListRange/shmclient.Session.ListRange inherit that),
// so "most recent" still costs walking peerID's whole index ascending
// and keeping a sliding window of the last maxExecutionsByPeer entries
// seen -- bounded by this one peer's own dispatch count, not a cheap
// tail read.
func ListExecutionsByPeer(peerID string) ([]CommandExecution, error) {
	if peerID == "" {
		return nil, fmt.Errorf("kvctl: ListExecutionsByPeer: peerID must not be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}
	lo, hi := kindPrefixBounds(commandExecIndexKind(peerID))

	var window []CommandExecution
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list executions by peer: %w", err)
		}
		if !ok {
			break
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list executions by peer: decode: %w", err)
		}
		groupID, commandID := rec.Fields["group_id"], rec.Fields["command_id"]

		exec := CommandExecution{
			InstanceID:  rec.UnitID,
			GroupID:     groupID,
			CommandID:   commandID,
			RequestedBy: rec.AuthorPeerID,
			RequestedAt: rec.Timestamp,
		}
		if rec.Fields["role"] == execIndexRoleTarget {
			exec.Role = "target"
			exec.TargetPeerID = peerID
		} else {
			exec.Role = "requester"
			exec.TargetPeerID = targetPeerIDForCommand(groupID, commandID)
		}

		window = append(window, exec)
		if len(window) > maxExecutionsByPeer {
			window = window[1:]
		}
		lo = append(append([]byte{}, key...), 0x00)
	}

	for i, j := 0, len(window)-1; i < j; i, j = i+1, j-1 {
		window[i], window[j] = window[j], window[i]
	}
	return window, nil
}

// AppendCommandLog implements `mage appendcommandlog <requesterPeerID>
// <instanceID> <fieldsJSON> <narrative>`: writes one execution-log entry
// for instanceID -- SubmitCommand's target calls this as it works
// through a command, and QueryCommandLog/LatestCommandLog is how the
// requester (and anyone else who knows instanceID) reads it back. Also
// sends requesterPeerID a low-latency Execute poke, best-effort (see
// SubmitCommand's doc comment on why a failed poke doesn't fail the
// call) -- requesterPeerID normally comes from
// GetCommandRequest(...).RequestedBy. Pass "" for requesterPeerID to
// skip the poke.
func AppendCommandLog(requesterPeerID, instanceID string, fields map[string]string, narrative string) error {
	if instanceID == "" {
		return fmt.Errorf("kvctl: instance id must not be empty")
	}
	if err := LogAppend(logCommandExecKind, instanceID, fields, narrative); err != nil {
		return err
	}

	if requesterPeerID != "" {
		if poke, err := json.Marshal(executePoke{Type: "cmd_log", InstanceID: instanceID}); err == nil {
			_ = Execute(requesterPeerID, string(poke))
		}
	}
	return nil
}

// QueryCommandLog implements `mage querycommandlog <instanceID> <since>
// <until> <limit>`: lists every AppendCommandLog entry for instanceID
// with a timestamp in [start, end], oldest first, up to limit records
// (limit <= 0 means unlimited) -- a thin wrapper over
// LogQuery(logCommandExecKind, instanceID, ...) so callers don't need to
// know that Kind convention themselves.
func QueryCommandLog(instanceID string, start, end time.Time, limit int) ([]logrecord.Record, error) {
	return LogQuery(logCommandExecKind, instanceID, start, end, limit)
}

// LatestCommandLog implements `mage latestcommandlog <instanceID>`:
// returns instanceID's single most recent AppendCommandLog entry -- its
// Fields and Narrative, i.e. the command's output as of now. Returns an
// error if instanceID has no log entries yet. The result is always well
// within pkg/shmevent.ValueSize (512 bytes): every AppendCommandLog
// entry is individually bound to that same wire limit at write time
// (LogAppend -> shmclient.LogAppend -> shmevent.Encode), so there is
// nothing here that could ever exceed it -- no separate truncation
// needed on the read side.
//
// Like ListExecutionsByPeer, there is no reverse-scan primitive in this
// stack, so "latest" costs a full walk of instanceID's own log range
// (bounded to just that one instance, not the whole cmdlog kind) rather
// than a cheap tail read -- a caller that already tracks the last
// timestamp it saw should keep using QueryCommandLog's start parameter
// instead of polling this repeatedly.
func LatestCommandLog(instanceID string) (logrecord.Record, error) {
	if instanceID == "" {
		return logrecord.Record{}, fmt.Errorf("kvctl: LatestCommandLog: instanceID must not be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return logrecord.Record{}, err
	}
	lo, hi := logrecord.ScanBounds(logCommandExecKind, instanceID, time.Unix(0, 0), time.Now())

	var latest logrecord.Record
	found := false
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return logrecord.Record{}, fmt.Errorf("kvctl: latest command log: %w", err)
		}
		if !ok {
			break
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return logrecord.Record{}, fmt.Errorf("kvctl: latest command log: decode: %w", err)
		}
		latest = rec
		found = true
		lo = append(append([]byte{}, key...), 0x00)
	}
	if !found {
		return logrecord.Record{}, fmt.Errorf("kvctl: no command log entries for instance %s", instanceID)
	}
	return latest, nil
}
