package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// This file ties the catalog -> dispatch -> execution-log flow together:
// SubmitCommand dispatches a catalog.go Command as a durable, replicated
// request plus a low-latency Execute poke to whoever executes it, and
// WatchCommandLog/QueryCommandLog read back the execution log the target
// device writes with AppendCommandLog as it works. Like catalog.go, every
// operation here is a plain EventGet/EventLogAppend/EventListRange/
// EventExecute call -- no new capnp wire schema. This is the mobile
// counterpart of desktop's pkg/kvctl/dispatch.go, and stays deliberately
// close to it.
//
// kvmobile only dispatches and records; it never interprets or runs a
// Command itself -- that's the target device's own application logic,
// watching for requests (WatchExecute, or ListCommandRequests as a
// catch-up fallback -- see SubmitCommand's doc comment on why Execute
// delivery alone isn't reliable enough to be the only path) and reporting
// back via AppendCommandLog.

// logCommandExecKind is the fixed pkg/logrecord Kind every
// AppendCommandLog entry is stored under, keyed by instance id (globally
// unique, not scoped to a command -- see newInstanceID) rather than a
// per-command Kind the way CommandRequest is, since a caller tracking one
// dispatch already knows exactly which instance id it wants, with no need
// to enumerate "every log entry for command C".
const logCommandExecKind = "cmdlog"

// commandRequestLogKind returns the pkg/logrecord Kind every SubmitCommand
// dispatch (CommandRequest) of commandID is stored under, so
// ListCommandRequests can enumerate a command's pending requests with one
// prefix scan.
func commandRequestLogKind(commandID string) string {
	return "cmdreq:" + commandID
}

// commandExecIndexKind returns the pkg/logrecord Kind SubmitCommand
// indexes a dispatch under for peerID's sake, once per role (requester,
// target) peerID plays in it -- see ListExecutionsByPeer, which this
// makes a single per-peer prefix scan instead of iterating every command's
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
// for instanceID, naming commandID and peerID's role in this dispatch
// (execIndexRoleRequester/execIndexRoleTarget), attributed to
// requesterPeerID -- SubmitCommand calls this once per role peerID plays
// in a dispatch.
//
// Deliberately thin: it stores only what ListExecutionsByPeer can't
// otherwise derive. It does not store requesterPeerID as a Fields entry
// (that's already the record's own AuthorPeerID) or targetPeerID
// (redundant with peerID itself when role is target; ListExecutionsByPeer
// looks it up via GetCommand for a role-requester entry instead). This
// matters because commandExecIndexKind(peerID) already embeds a full peer
// id in the pkg/logrecord key (see BuildKey), and every record here shares
// pkg/shmevent.ValueSize's single 512-byte budget across key *and* value
// combined -- an earlier version of this function also stored
// requested_by/target_peer_id directly and blew that budget the moment
// two real peer ids (~52 bytes each) were involved at once.
func appendCommandExecIndex(ctx context.Context, sess *shmclient.Session, peerID, instanceID, commandID, requesterPeerID, role string) error {
	fields := map[string]string{
		"command_id": commandID,
		"role":       role,
	}
	return appendRecord(ctx, sess, commandExecIndexKind(peerID), instanceID, requesterPeerID, fields, "")
}

// executePoke is the small JSON envelope SubmitCommand/AppendCommandLog
// send over Execute as an optional low-latency nudge -- see
// WatchCommandLog's doc comment for what it's for and why WatchCommandLog
// itself doesn't depend on receiving it. Type is "cmd_req" (a new
// SubmitCommand dispatch) or "cmd_log" (a new AppendCommandLog entry); an
// app with its own WatchExecute callback can decode this itself to decide
// what to react to.
type executePoke struct {
	Type       string `json:"type"`
	CommandID  string `json:"command_id,omitempty"`
	InstanceID string `json:"instance_id"`
}

// newInstanceID returns a fresh random hex id for one SubmitCommand
// dispatch -- globally unique (not scoped to a command) since
// GetCommandRequest/QueryCommandLog/WatchCommandLog all key off it alone.
func newInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("kvmobile: generate instance id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// CommandRequest is SubmitCommand's durable record of one dispatch --
// GetCommandRequest/ListCommandRequests read it back. There's no
// update/delete for these, only the single revision SubmitCommand writes.
type CommandRequest struct {
	InstanceID  string    `json:"instance_id"`
	CommandID   string    `json:"command_id"`
	RequestedBy string    `json:"requested_by"`
	Inputs      string    `json:"inputs,omitempty"` // caller-defined JSON, opaque to kvmobile
	RequestedAt time.Time `json:"requested_at"`
}

func recordToCommandRequest(h revisionHistory) CommandRequest {
	return CommandRequest{
		InstanceID:  h.latest.UnitID,
		CommandID:   h.latest.Fields["command_id"],
		RequestedBy: h.latest.AuthorPeerID,
		Inputs:      h.latest.Fields["inputs"],
		RequestedAt: h.latest.Timestamp,
	}
}

// SubmitCommand dispatches commandID (which must already exist -- see
// CreateCommand) with inputsJSON (caller-defined, opaque to kvmobile --
// typically the JSON object a form built around the command's known
// inputs produced) as a durable, replicated CommandRequest, then sends the
// command's TargetPeerID a low-latency Execute poke naming the new
// instance id (best-effort: a failed poke doesn't fail the dispatch,
// since the durable request is the real source of truth -- see
// ListCommandRequests for the target's catch-up path if the poke never
// arrives). Returns the instance id, which the caller uses with
// GetCommandRequest/QueryCommandLog/WatchCommandLog to track this specific
// dispatch.
//
// Requires this device's own current peer id to be permitted for
// commandID (isPermittedForCommand: some group both commandID is linked
// to via AddCommandToGroup and this device is a member of via
// AddPeerToGroup) -- see catalog.go's doc comment for the full ACL model.
// Unlike the group-participation check this replaces,
// CreateGroup/CreateCommand/AddCommandToGroup/AddPeerToGroup themselves
// are pkg/daemon-enforced (voter-gated), but this specific check -- "is
// the submitting peer currently entitled to this command" -- is still
// evaluated here in kvmobile, not independently inside pkg/daemon's
// generic EventLogAppend handling, so it's only as strong as every caller
// actually going through SubmitCommand rather than writing a
// commandRequestLogKind record directly.
//
// kvmobile only dispatches and records the request; actually running
// commandID is the target device's own application logic (see
// AppendCommandLog for how it reports back).
func SubmitCommand(commandID, inputsJSON string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	requesterPeerID := PeerID()

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	ok, err := isPermittedForCommand(ctx, sess, requesterPeerID, commandID)
	if err != nil {
		return "", fmt.Errorf("kvmobile: submit command: %w", err)
	}
	if !ok {
		return "", fmt.Errorf("kvmobile: %s is not permitted to submit command %s", requesterPeerID, commandID)
	}

	cmdJSON, err := GetCommand(commandID)
	if err != nil {
		return "", err
	}
	var cmd Command
	if err := json.Unmarshal([]byte(cmdJSON), &cmd); err != nil {
		return "", fmt.Errorf("kvmobile: submit command: decode %s: %w", commandID, err)
	}
	targetPeerID := cmd.TargetPeerID

	instanceID, err := newInstanceID()
	if err != nil {
		return "", err
	}

	fields := map[string]string{
		"command_id": commandID,
	}
	if inputsJSON != "" {
		fields["inputs"] = inputsJSON
	}
	if err := appendRecord(ctx, sess, commandRequestLogKind(commandID), instanceID, requesterPeerID, fields, ""); err != nil {
		return "", fmt.Errorf("kvmobile: submit command: %w", err)
	}

	if err := appendCommandExecIndex(ctx, sess, requesterPeerID, instanceID, commandID, requesterPeerID, execIndexRoleRequester); err != nil {
		return "", fmt.Errorf("kvmobile: submit command: %w", err)
	}
	if targetPeerID != requesterPeerID {
		if err := appendCommandExecIndex(ctx, sess, targetPeerID, instanceID, commandID, requesterPeerID, execIndexRoleTarget); err != nil {
			return "", fmt.Errorf("kvmobile: submit command: %w", err)
		}
	}

	if poke, err := json.Marshal(executePoke{Type: "cmd_req", CommandID: commandID, InstanceID: instanceID}); err == nil {
		_ = Execute(targetPeerID, string(poke))
	}

	return instanceID, nil
}

// GetCommandRequest returns instanceID's dispatch record for commandID as
// a JSON CommandRequest, or an error if it doesn't exist. commandID is
// needed to know which storage namespace to look in
// (commandRequestLogKind) -- typically already known to the caller, since
// it's also named in the "cmd_req" Execute poke that usually prompts this
// call (see executePoke).
func GetCommandRequest(commandID, instanceID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	h, err := scanRevisions(ctx, sess, commandRequestLogKind(commandID), instanceID)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get command request: %w", err)
	}
	if !h.found {
		return "", fmt.Errorf("kvmobile: command request %s not found for command %s", instanceID, commandID)
	}

	out, err := json.Marshal(recordToCommandRequest(h))
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode command request: %w", err)
	}
	return string(out), nil
}

// ListCommandRequests returns every dispatch request currently recorded
// for commandID as a JSON array of CommandRequest (`"[]"` when none
// exist), oldest first. How a target device catches up on requests it
// might have missed an Execute poke for -- pokes are unreplicated and
// dropped if the device wasn't running to receive them (see
// SubmitCommand's doc comment) -- e.g. on app startup, or polled
// periodically alongside WatchExecute as a reliability fallback.
func ListCommandRequests(commandID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	ids, err := listUnitIDs(ctx, sess, commandRequestLogKind(commandID))
	if err != nil {
		return "", fmt.Errorf("kvmobile: list command requests: %w", err)
	}

	requests := []CommandRequest{}
	for _, id := range ids {
		h, err := scanRevisions(ctx, sess, commandRequestLogKind(commandID), id)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list command requests: %w", err)
		}
		if !h.found {
			continue
		}
		requests = append(requests, recordToCommandRequest(h))
	}

	out, err := json.Marshal(requests)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode command requests: %w", err)
	}
	return string(out), nil
}

// maxExecutionsByPeer bounds ListExecutionsByPeer's result to the 200
// most recent executions touching a peer -- enough for a device to
// render a meaningful recent-activity view without pulling in a peer's
// entire dispatch history over shmring on every call.
const maxExecutionsByPeer = 200

// CommandExecution is one SubmitCommand dispatch as it appears from
// peerID's point of view (see ListExecutionsByPeer) -- Role is
// "requester" or "target" depending on which side of the dispatch
// peerID was on. The same instance appears twice, once under each role's
// peer, if requester and target differ. TargetPeerID is "" for a
// requester-role entry if this device could not resolve it (see
// targetPeerIDForCommand) -- e.g. the command was since deleted.
type CommandExecution struct {
	InstanceID   string    `json:"instance_id"`
	CommandID    string    `json:"command_id"`
	RequestedBy  string    `json:"requested_by"`
	TargetPeerID string    `json:"target_peer_id"`
	Role         string    `json:"role"`
	RequestedAt  time.Time `json:"requested_at"`
}

// targetPeerIDForCommand best-effort resolves commandID's current
// TargetPeerID -- ListExecutionsByPeer's fallback for a role-requester
// index entry, which (see appendCommandExecIndex's doc comment on why the
// index is deliberately thin) doesn't store target_peer_id itself.
// Returns "" rather than an error if the command was since deleted -- a
// missing detail on one history entry shouldn't fail the whole list.
func targetPeerIDForCommand(commandID string) string {
	out, err := GetCommand(commandID)
	if err != nil {
		return ""
	}
	var cmd Command
	if json.Unmarshal([]byte(out), &cmd) != nil {
		return ""
	}
	return cmd.TargetPeerID
}

// ListExecutionsByPeer returns up to the maxExecutionsByPeer most recent
// SubmitCommand dispatches touching peerID, as either requester or
// target, most recent first -- the binding behind "show me every command
// execution involving this peer, without me iterating
// ListCommandRequests per command myself." Backed by the dedicated
// per-peer index SubmitCommand writes at dispatch time (see
// commandExecIndexKind/appendCommandExecIndex), so this costs one prefix
// scan over peerID's own dispatch history, not O(commands) -- plus one
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
func ListExecutionsByPeer(peerID string) (string, error) {
	if peerID == "" {
		return "", fmt.Errorf("kvmobile: ListExecutionsByPeer: peerID must not be empty")
	}
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi := kindPrefixBounds(commandExecIndexKind(peerID))

	var window []CommandExecution
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list executions by peer: %w", err)
		}
		if !ok {
			break
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list executions by peer: decode: %w", err)
		}
		commandID := rec.Fields["command_id"]

		exec := CommandExecution{
			InstanceID:  rec.UnitID,
			CommandID:   commandID,
			RequestedBy: rec.AuthorPeerID,
			RequestedAt: rec.Timestamp,
		}
		if rec.Fields["role"] == execIndexRoleTarget {
			exec.Role = "target"
			exec.TargetPeerID = peerID
		} else {
			exec.Role = "requester"
			exec.TargetPeerID = targetPeerIDForCommand(commandID)
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
	if window == nil {
		window = []CommandExecution{}
	}

	out, err := json.Marshal(window)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode executions: %w", err)
	}
	return string(out), nil
}

// AppendCommandLog writes one execution-log entry for instanceID --
// SubmitCommand's target device calls this as it works through a
// command, and QueryCommandLog/WatchCommandLog is how the requester (and
// anyone else who knows instanceID) reads it back. Also sends
// requesterPeerID a low-latency Execute poke, best-effort (see
// SubmitCommand's doc comment on why a failed poke doesn't fail the
// call) -- requesterPeerID normally comes from
// GetCommandRequest(...).RequestedBy. Pass "" for requesterPeerID to skip
// the poke.
func AppendCommandLog(requesterPeerID, instanceID, fieldsJSON, narrative string) error {
	if instanceID == "" {
		return fmt.Errorf("kvmobile: instance id must not be empty")
	}
	if err := LogAppend(logCommandExecKind, instanceID, fieldsJSON, narrative); err != nil {
		return err
	}

	if requesterPeerID != "" {
		if poke, err := json.Marshal(executePoke{Type: "cmd_log", InstanceID: instanceID}); err == nil {
			_ = Execute(requesterPeerID, string(poke))
		}
	}
	return nil
}

// QueryCommandLog lists every AppendCommandLog entry for instanceID with
// a timestamp in [since, until], oldest first, up to limit records -- a
// thin wrapper over LogQuery(logCommandExecKind, instanceID, ...) so
// callers don't need to know that Kind convention themselves. since/until
// are RFC3339 or "" (since "" = unbounded, until "" = now); limit is a
// count or "" (no limit).
func QueryCommandLog(instanceID, since, until, limit string) (string, error) {
	return LogQuery(logCommandExecKind, instanceID, since, until, limit)
}

// LatestCommandLog returns instanceID's single most recent
// AppendCommandLog entry -- its Fields and Narrative, i.e. the command's
// output as of now -- as a JSON pkg/logrecord.Record. Returns an error if
// instanceID has no log entries yet. The result is always well within
// pkg/shmevent.ValueSize (512 bytes): every AppendCommandLog entry is
// individually bound to that same wire limit at write time (LogAppend ->
// shmclient.LogAppend -> shmevent.Encode), so there is nothing here that
// could ever exceed it -- no separate truncation needed on the read
// side.
//
// Like ListExecutionsByPeer, there is no reverse-scan primitive in this
// stack, so "latest" costs a full walk of instanceID's own log range
// (bounded to just that one instance, not the whole cmdlog kind) rather
// than a cheap tail read -- callers that already track the last
// timestamp they saw (e.g. WatchCommandLog) should keep using
// QueryCommandLog's since parameter instead of polling this repeatedly.
func LatestCommandLog(instanceID string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("kvmobile: LatestCommandLog: instanceID must not be empty")
	}
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi := logrecord.ScanBounds(logCommandExecKind, instanceID, time.Unix(0, 0), time.Now())

	var latest logrecord.Record
	found := false
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: latest command log: %w", err)
		}
		if !ok {
			break
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: latest command log: decode: %w", err)
		}
		latest = rec
		found = true
		lo = append(append([]byte{}, key...), 0x00)
	}
	if !found {
		return "", fmt.Errorf("kvmobile: no command log entries for instance %s", instanceID)
	}

	out, err := json.Marshal(latest)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode command log: %w", err)
	}
	return string(out), nil
}

// LogCallback is a gomobile-bindable interface Kotlin implements to
// receive WatchCommandLog's periodic updates -- the same reverse-binding
// pattern ExecuteCallback uses.
type LogCallback interface {
	// OnRecords is called whenever a poll finds new records since the
	// last one, as a JSON array (never called with an empty array). Runs
	// on WatchCommandLog's own goroutine, never the caller's.
	OnRecords(recordsJSON string)
}

// watchCommandLogPollInterval bounds how often runCommandLogWatch
// re-queries QueryCommandLog. Unlike WatchExecute's drain of the
// in-memory executeInbox, this is a real replicated-store read each tick
// (see QueryCommandLog), so it's deliberately longer than
// watchExecutePollInterval.
const watchCommandLogPollInterval = 1500 * time.Millisecond

// commandLogWatch is one active WatchCommandLog loop's stop handle.
type commandLogWatch struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var (
	commandLogWatchMu sync.Mutex
	commandLogWatches = map[string]commandLogWatch{}
)

// WatchCommandLog polls QueryCommandLog(instanceID, ...) on a timer and
// invokes cb.OnRecords with whatever's new since the last poll, until
// StopWatchCommandLog(instanceID) is called. A second WatchCommandLog
// call for the same instanceID replaces the first (stopping it first);
// different instanceIDs run independently and concurrently.
//
// Unlike WatchExecute, this is timer-based, not driven by EventExecute
// delivery: PollExecute's queue (executeInbox in pkg/daemon) has exactly
// one consumer slot per device -- a second independent drainer here would
// race WatchExecute for the same notifications and silently steal ones
// meant for it (see that field's own doc comment). AppendCommandLog does
// still send a low-latency Execute poke to the requester as an optional
// accelerant for a caller that's *also* running its own WatchExecute and
// wants to react to a "cmd_log" notification (see executePoke) by
// triggering an immediate QueryCommandLog itself, but WatchCommandLog
// doesn't depend on that -- it works standalone, at the cost of up to
// watchCommandLogPollInterval of extra latency versus a genuine push.
func WatchCommandLog(instanceID string, cb LogCallback) error {
	if cb == nil {
		return fmt.Errorf("kvmobile: WatchCommandLog: cb must not be nil")
	}
	if instanceID == "" {
		return fmt.Errorf("kvmobile: WatchCommandLog: instanceID must not be empty")
	}

	commandLogWatchMu.Lock()
	defer commandLogWatchMu.Unlock()
	stopCommandLogWatchLocked(instanceID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	commandLogWatches[instanceID] = commandLogWatch{cancel: cancel, done: done}

	go runCommandLogWatch(ctx, done, instanceID, cb)
	return nil
}

// StopWatchCommandLog stops instanceID's watcher, if any, and waits for
// it to actually exit before returning. Safe to call when nothing is
// running for it (a no-op).
func StopWatchCommandLog(instanceID string) {
	commandLogWatchMu.Lock()
	defer commandLogWatchMu.Unlock()
	stopCommandLogWatchLocked(instanceID)
}

// stopCommandLogWatchLocked requires commandLogWatchMu already held.
func stopCommandLogWatchLocked(instanceID string) {
	w, ok := commandLogWatches[instanceID]
	if !ok {
		return
	}
	w.cancel()
	<-w.done
	delete(commandLogWatches, instanceID)
}

// runCommandLogWatch is WatchCommandLog's background loop body.
func runCommandLogWatch(ctx context.Context, done chan struct{}, instanceID string, cb LogCallback) {
	defer close(done)

	// since tracks the timestamp just past the newest record already
	// delivered to cb, so each round only asks for what's new.
	var since time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchCommandLogPollInterval):
		}

		sinceStr := ""
		if !since.IsZero() {
			sinceStr = since.Format(time.RFC3339Nano)
		}
		out, err := QueryCommandLog(instanceID, sinceStr, "", "")
		if err != nil {
			continue
		}
		var records []logrecord.Record
		if err := json.Unmarshal([]byte(out), &records); err != nil || len(records) == 0 {
			continue
		}

		cb.OnRecords(out)
		since = records[len(records)-1].Timestamp.Add(time.Nanosecond)
	}
}
