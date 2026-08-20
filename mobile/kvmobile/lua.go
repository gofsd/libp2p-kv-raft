package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// This file is the mobile counterpart of the lua commands on kvctl-cli: a
// command whose body is a Lua script, stored in the journal and registered
// in the catalog like any other command. It is the same examples/luacmd
// package on both sides -- what differs is only which client the scripts
// and the runner reach the cluster through.
//
// # Why a phone is a reasonable place to run one
//
// Unlike the desktop, where a runner and any other CLI call against the
// same node are two OS processes contending for one IPC channel (see
// examples/luacmd's own note, and pkg/ipc's), everything here shares one
// process with the daemon: pkg/ipc's caller lock serialises the runner's
// polling, a script's own waiting, and whatever the UI is doing, without
// any of them starving the others.
//
// That matters more than it sounds, because the interesting scripts are
// the ones that dispatch other commands and wait for them. A run that
// waits for a child served by this same device needs the loop that notices
// the child to keep running -- which is exactly what luacmd's own runner
// provides and the shared RunCommandDispatcher does not.
//
// # What it does not change
//
// Standing. A script submits under *this device's* peer id, and the FSM
// checks that peer against the command's groups exactly as it would check
// a human's. Writing a command in Lua grants nothing, and neither does
// running the interpreter.
//
// Everything else here is JSON in and JSON out, gomobile's only real
// option: no maps, no structs, no slices across the binding.

var (
	luaMu     sync.Mutex
	luaCancel context.CancelFunc
	luaDone   chan struct{}
)

// LuaListener receives what a running script does, for a UI that wants to
// show it live. Every method is called from the runner's own goroutines,
// so a Kotlin implementation must post to the main thread itself rather
// than touching views directly.
//
// This is the *hosting* device's view -- what this device is running for
// whoever asked. A device that merely submitted a command watches that
// instance's log instead (WatchCommandLog), which is the same thing it
// would do for any other command.
type LuaListener interface {
	// OnStart reports a run beginning.
	OnStart(commandID, instanceID string)
	// OnLog reports one live line a script wrote, after it is durably
	// recorded -- so a listener never shows a line the log does not have.
	OnLog(commandID, instanceID, narrative string)
	// OnFinish reports the terminal result: status is "ok" or "error".
	OnFinish(commandID, instanceID, status, narrative string)
	// OnError reports a transient failure of the runner itself. None of
	// them stops it.
	OnError(message string)
}

// LuaPut stores a script in the journal without registering a command for
// it -- for editing a script whose command already exists, or for staging
// one on a device that is not a voter and so may not register it.
//
// Every put is a new revision; earlier ones stay readable (LuaHistory).
// The script must compile, which is checked here so a syntax error is
// caught while somebody is looking at the editor rather than later, on
// whichever device would have run it.
//
// Note that a Command pins the hash of the script it was registered with,
// so putting a new revision does *not* silently change what an existing
// command runs -- that takes a voter re-registering it (LuaCreateCommand).
func LuaPut(id, name, code string) error {
	ctx, cancel := luaContext()
	defer cancel()
	catalog, err := luaCatalog()
	if err != nil {
		return err
	}
	if _, err := catalog.Put(ctx, luacmd.Script{ID: id, Name: name, Code: code}); err != nil {
		return err
	}
	return nil
}

// LuaCreateCommand is the one-call form the editor screen uses: store the
// script, register it as a Lua command targeting *this* device, and link
// it to groupID (empty to skip the link).
//
// Registering and linking are voter-gated by the daemon, so on a learner
// this stores the script and then reports the refusal. That is recoverable
// rather than wasted: a voter can register the same id later, against the
// script this call already wrote.
//
// The group is what decides who may run it -- public group, anybody;
// private group, its members.
func LuaCreateCommand(id, name, groupID, code string) error {
	ctx, cancel := luaContext()
	defer cancel()
	catalog, err := luaCatalog()
	if err != nil {
		return err
	}
	self := PeerID()
	if self == "" {
		return fmt.Errorf("kvmobile: LuaCreateCommand: Start has not completed successfully yet")
	}
	_, err = luacmd.Register(ctx, catalog, luaDevice{}, self,
		luacmd.Script{ID: id, Name: name, Code: code}, groupID)
	return err
}

// LuaGet returns a script's current revision as JSON, source included.
func LuaGet(id string) (string, error) {
	ctx, cancel := luaContext()
	defer cancel()
	catalog, err := luaCatalog()
	if err != nil {
		return "", err
	}
	script, err := catalog.Get(ctx, id)
	if err != nil {
		return "", err
	}
	return luaJSON(script)
}

// LuaList returns every script that currently exists as a JSON array,
// latest revision each, ascending by id, deleted ones left out.
func LuaList() (string, error) {
	ctx, cancel := luaContext()
	defer cancel()
	catalog, err := luaCatalog()
	if err != nil {
		return "", err
	}
	scripts, err := catalog.List(ctx)
	if err != nil {
		return "", err
	}
	return luaJSON(scripts)
}

// LuaHistory returns every revision ever written for a script as a JSON
// array, oldest first, tombstones included.
func LuaHistory(id string) (string, error) {
	ctx, cancel := luaContext()
	defer cancel()
	catalog, err := luaCatalog()
	if err != nil {
		return "", err
	}
	revisions, err := catalog.History(ctx, id)
	if err != nil {
		return "", err
	}
	return luaJSON(revisions)
}

// LuaDelete writes a tombstone revision, after which the script no longer
// exists to a reader. Its source stays readable through LuaHistory, and
// the catalog Command pointing at it is left alone -- delete that with
// DeleteCommand if it should go too.
func LuaDelete(id string) error {
	ctx, cancel := luaContext()
	defer cancel()
	catalog, err := luaCatalog()
	if err != nil {
		return err
	}
	return catalog.Delete(ctx, id)
}

// LuaRun submits a Lua command and returns the instance id it was recorded
// under, without waiting for it. Watch that instance's log
// (WatchCommandLog) to show the run as it happens -- the same thing an app
// does for any other command.
//
// Submits under this device's own peer id, so this device needs standing
// for the command like any other submitter.
func LuaRun(commandID, inputsJSON string) (string, error) {
	ctx, cancel := luaContext()
	defer cancel()
	return luaDevice{}.Submit(ctx, commandID, inputsJSON)
}

// LuaLogs returns one run's log entries as a JSON array.
func LuaLogs(instanceID string) (string, error) {
	ctx, cancel := luaContext()
	defer cancel()
	entries, err := luaDevice{}.QueryLog(ctx, instanceID)
	if err != nil {
		return "", err
	}
	return luaJSON(entries)
}

// LuaLastLog returns the most recent run of commandID as JSON --
// {"instance_id": ..., "entries": [...]}.
//
// It exists because an instance id is not always available to whoever
// wants the log: a command submitted from another device, or by a
// schedule, hands its id back to the submitter and to nobody else. "The
// last run of this command" is the question a person actually has, and is
// what the optical test rig asks, since it cannot feed an instance id back
// into a generated code.
func LuaLastLog(commandID string) (string, error) {
	ctx, cancel := luaContext()
	defer cancel()
	instanceID, entries, err := luacmd.LastRun(ctx, luaDevice{}, commandID)
	if err != nil {
		return "", err
	}
	return luaJSON(struct {
		InstanceID string            `json:"instance_id"`
		Entries    []luacmd.LogEntry `json:"entries"`
	}{InstanceID: instanceID, Entries: entries})
}

// LuaServe starts serving this device's Lua commands, until StopLuaServe.
//
// intervalSeconds is how often to look for new requests (0 means the
// package default, 1.5s) and concurrency how many scripts may run at once
// (0 means 4). Calling it again replaces whatever was running.
//
// Only this device's own Lua commands are served -- ones whose target peer
// id is this device. A command targeting somebody else is that device's
// work, and running a second runner for it elsewhere would race.
//
// Like this package's other loops, the runner outlives Stop deliberately:
// it resolves the session per call, so a torn-down daemon makes a pass
// fail and be reported rather than making the loop exit, and a later Start
// picks it straight back up.
func LuaServe(intervalSeconds, concurrency int) error {
	return luaServe(intervalSeconds, concurrency, nil)
}

// LuaServeWithListener is LuaServe with a listener attached, for a UI that
// wants to show runs as they happen.
func LuaServeWithListener(intervalSeconds, concurrency int, listener LuaListener) error {
	if listener == nil {
		return fmt.Errorf("kvmobile: LuaServeWithListener: listener must not be nil -- use LuaServe")
	}
	return luaServe(intervalSeconds, concurrency, listener)
}

func luaServe(intervalSeconds, concurrency int, listener LuaListener) error {
	if intervalSeconds < 0 {
		return fmt.Errorf("kvmobile: LuaServe: intervalSeconds must not be negative, got %d", intervalSeconds)
	}
	if concurrency < 0 {
		return fmt.Errorf("kvmobile: LuaServe: concurrency must not be negative, got %d", concurrency)
	}

	opts := luacmd.ServeOptions{Concurrency: concurrency}
	if intervalSeconds > 0 {
		opts.Interval = time.Duration(intervalSeconds) * time.Second
	}
	if listener != nil {
		opts.Listener = luaListenerAdapter{listener}
	}
	catalog, err := luaCatalog()
	if err != nil {
		return err
	}

	luaMu.Lock()
	defer luaMu.Unlock()
	stopLuaLocked()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	luaCancel, luaDone = cancel, done
	go func() {
		defer close(done)
		luacmd.NewRunner(luaDevice{}, catalog, opts).Serve(ctx)
	}()
	return nil
}

// StopLuaServe stops the runner and waits for it -- including for whatever
// scripts were mid-run, each of which still records its own result. Safe
// to call when nothing is running (a no-op).
func StopLuaServe() {
	luaMu.Lock()
	defer luaMu.Unlock()
	stopLuaLocked()
}

// LuaServing reports whether this device is running Lua commands -- what a
// settings screen shows a switch from.
func LuaServing() bool {
	luaMu.Lock()
	defer luaMu.Unlock()
	return luaDone != nil
}

// stopLuaLocked requires luaMu already held.
func stopLuaLocked() {
	if luaDone == nil {
		return
	}
	luaCancel()
	<-luaDone
	luaCancel, luaDone = nil, nil
}

// luaListenerAdapter turns the gomobile-facing listener into the one
// examples/luacmd expects. Kept separate so the exported interface can
// stay gomobile-shaped (no non-basic parameter types) whatever the
// package's own listener grows into.
type luaListenerAdapter struct{ listener LuaListener }

func (a luaListenerAdapter) OnStart(commandID, instanceID string) {
	a.listener.OnStart(commandID, instanceID)
}

func (a luaListenerAdapter) OnLog(commandID, instanceID, narrative string) {
	a.listener.OnLog(commandID, instanceID, narrative)
}

func (a luaListenerAdapter) OnFinish(commandID, instanceID, status, narrative string) {
	a.listener.OnFinish(commandID, instanceID, status, narrative)
}

func (a luaListenerAdapter) OnError(message string) { a.listener.OnError(message) }

// luaDevice implements examples/luacmd's Cluster and Registrar through
// this package's own daemon, the way examples/luacmd's Kvctl does through
// pkg/kvctl on the desktop.
//
// Every method resolves the session itself rather than holding one: Stop
// tears the session down and a later Start makes a new one, and a runner
// that outlives both must not be left holding the old one.
type luaDevice struct{}

func (luaDevice) SelfPeerID(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	self := PeerID()
	if self == "" {
		return "", fmt.Errorf("kvmobile: Start has not completed successfully yet")
	}
	return self, nil
}

func (luaDevice) ListCommands(ctx context.Context) ([]luacmd.Command, error) {
	sess, err := currentSession()
	if err != nil {
		return nil, err
	}
	found, err := listCommands(ctx, sess)
	if err != nil {
		return nil, err
	}
	commands := make([]luacmd.Command, 0, len(found))
	for _, c := range found {
		commands = append(commands, luacmd.Command{
			ID: c.ID, Name: c.Name, PeerID: c.TargetPeerID, Spec: c.Spec,
		})
	}
	return commands, nil
}

func (luaDevice) ListRequests(ctx context.Context, commandID string) ([]luacmd.Request, error) {
	sess, err := currentSession()
	if err != nil {
		return nil, err
	}
	found, err := listCommandRequests(ctx, sess, commandID)
	if err != nil {
		return nil, err
	}
	requests := make([]luacmd.Request, 0, len(found))
	for _, r := range found {
		requests = append(requests, luacmd.Request{
			InstanceID:  r.InstanceID,
			CommandID:   r.CommandID,
			RequestedBy: r.RequestedBy,
			Inputs:      r.Inputs,
			RequestedAt: r.RequestedAt,
		})
	}
	return requests, nil
}

func (luaDevice) QueryLog(ctx context.Context, instanceID string) ([]luacmd.LogEntry, error) {
	sess, err := currentSession()
	if err != nil {
		return nil, err
	}
	records, err := logQuery(ctx, sess, logCommandExecKind, instanceID, time.Unix(0, 0), time.Now(), 0)
	if err != nil {
		return nil, err
	}
	entries := make([]luacmd.LogEntry, 0, len(records))
	for _, r := range records {
		entries = append(entries, luacmd.LogEntry{
			InstanceID: instanceID,
			Timestamp:  r.Timestamp,
			Fields:     r.Fields,
			Narrative:  r.Narrative,
		})
	}
	return entries, nil
}

func (luaDevice) Progress(ctx context.Context, requesterPeerID, instanceID string, fields map[string]string, narrative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := luaFieldsJSON(fields)
	if err != nil {
		return err
	}
	return ReportProgress(requesterPeerID, instanceID, encoded, narrative)
}

func (luaDevice) Append(ctx context.Context, requesterPeerID, instanceID string, fields map[string]string, narrative string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := luaFieldsJSON(fields)
	if err != nil {
		return err
	}
	return AppendCommandLog(requesterPeerID, instanceID, encoded, narrative)
}

func (luaDevice) Submit(ctx context.Context, commandID, inputsJSON string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return SubmitCommand(commandID, inputsJSON)
}

func (luaDevice) PutCommand(ctx context.Context, id, name, peerID, spec string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return CreateCommandWithSpec(id, name, peerID, spec)
}

func (luaDevice) LinkGroup(ctx context.Context, commandID, groupID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return AddCommandToGroup(commandID, groupID)
}

// luaCatalog resolves the session per call rather than holding one, for
// the same reason cronStore does: a Stop/Start cycle underneath a running
// runner should be a failed pass, not a wedged loop.
func luaCatalog() (*luacmd.Catalog, error) {
	self := PeerID()
	if self == "" {
		return nil, fmt.Errorf("kvmobile: Start has not completed successfully yet")
	}
	journal := luacmd.Sessions(func(context.Context) (*shmclient.Session, error) {
		return currentSession()
	})
	return luacmd.NewCatalog(journal, self), nil
}

func luaContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), callTimeout)
}

// luaFieldsJSON encodes a fields map for the JSON-shaped AppendCommandLog/
// ReportProgress bindings. Empty stays empty rather than becoming "null",
// which those two read as "no fields".
func luaFieldsJSON(fields map[string]string) (string, error) {
	if len(fields) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return "", fmt.Errorf("kvmobile: lua: encode fields: %w", err)
	}
	return string(encoded), nil
}

func luaJSON(v any) (string, error) {
	out, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("kvmobile: lua: encode result: %w", err)
	}
	return string(out), nil
}
