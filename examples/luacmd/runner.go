package luacmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// The runner: the process on the device that owns a Lua command and turns
// requests for it into runs.
//
// This is the piece that could not simply reuse pkg/kvctl.RunCommandDispatcher
// or mobile/kvmobile.RunCommandDispatcher. Both of those call their handler
// synchronously, inside the loop that scans for pending requests -- which
// is free for every handler written before this one, because none of them
// dispatched work back into their own device. A Lua script doing
// kv.run("inner", ...) against a command this same runner serves is
// exactly that: the wait would block the loop that has to notice the
// child, and the two would sit there until the deadline. So each request
// runs in its own goroutine here, and the scan loop stays free to pick up
// what a running script asks for.

// Serve defaults. The interval is the whole latency budget for noticing a
// request, since there is no low-latency poke on this path: a script that
// dispatches a child waits at least this long for it to start.
const (
	DefaultServeInterval   = 1500 * time.Millisecond
	DefaultCatalogRefresh  = 30 * time.Second
	DefaultConcurrency     = 4
	DefaultCommandNarrated = "started"
)

// Spec is what a catalog Command's spec field carries for a Lua command:
// which script it runs and what that script's bytes must hash to.
//
// The hash is the point. Script source lives in the journal, which any
// local caller of a node can append to, while the Command carrying this
// spec is a voter-gated record. Pinning the hash here means changing what
// an already-registered command *does* takes a voter, not journal access
// -- see the package doc.
type Spec struct {
	// Runtime must be SpecRuntime for this package to claim the command.
	Runtime string `json:"runtime"`
	// ScriptID names the script in the journal.
	ScriptID string `json:"script_id"`
	// SHA256 is the script source's hash, as Sum returns it.
	SHA256 string `json:"sha256"`
	// TimeoutSeconds overrides the runner's own per-run timeout for this
	// one command, for a script that legitimately waits on something slow.
	// Zero means the runner's default.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}

// Encode renders the spec for storing on a Command record.
func (s Spec) Encode() (string, error) {
	encoded, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("luacmd: encode spec: %w", err)
	}
	return string(encoded), nil
}

// NewSpec builds the spec for script, pinning the hash it currently has.
func NewSpec(script Script) Spec {
	return Spec{Runtime: SpecRuntime, ScriptID: script.ID, SHA256: script.SHA256}
}

// ParseSpec reads a Command's spec field, reporting false for anything
// that is not a Lua command -- an unparseable spec, an empty one, or one
// naming a different runtime. Not an error: most commands in a catalog are
// not Lua commands, and a runner walking past them is the normal case.
func ParseSpec(spec string) (Spec, bool) {
	if spec == "" {
		return Spec{}, false
	}
	var parsed Spec
	if err := json.Unmarshal([]byte(spec), &parsed); err != nil {
		return Spec{}, false
	}
	if parsed.Runtime != SpecRuntime || parsed.ScriptID == "" {
		return Spec{}, false
	}
	return parsed, true
}

// Command is one catalog Command as the runner needs to see it.
type Command struct {
	ID     string
	Name   string
	PeerID string
	Spec   string
}

// Request is one dispatch waiting to be served.
type Request struct {
	InstanceID  string
	CommandID   string
	RequestedBy string
	Inputs      string
	RequestedAt time.Time
}

// Cluster is everything the runner needs from the node it runs on. Its
// real implementations are thin adapters over pkg/kvctl and
// mobile/kvmobile; a test supplies its own, which is what lets everything
// below be exercised without a daemon.
type Cluster interface {
	// SelfPeerID is the identity this runner serves commands for: a
	// command targeting any other peer is not this runner's work.
	SelfPeerID(ctx context.Context) (string, error)
	// ListCommands returns the whole catalog; the runner filters it.
	ListCommands(ctx context.Context) ([]Command, error)
	// ListRequests returns every dispatch recorded for commandID.
	ListRequests(ctx context.Context, commandID string) ([]Request, error)
	// QueryLog returns instanceID's log entries in write order.
	QueryLog(ctx context.Context, instanceID string) ([]LogEntry, error)
	// Progress records a non-terminal entry, stamped StatusRunning.
	Progress(ctx context.Context, requesterPeerID, instanceID string, fields map[string]string, narrative string) error
	// Append records a terminal entry.
	Append(ctx context.Context, requesterPeerID, instanceID string, fields map[string]string, narrative string) error
	// Submit dispatches another command under this device's own identity.
	Submit(ctx context.Context, commandID, inputsJSON string) (string, error)
}

// Listener is an optional live view of what a runner is doing, for a UI
// that wants to show runs as they happen. Every method is called from the
// runner's own goroutines, so an implementation that touches a UI must
// post to its own thread.
type Listener interface {
	OnStart(commandID, instanceID string)
	// OnLog is every live line a running script writes, as it writes it --
	// what a device that *hosts* commands shows while one is running. A
	// device that merely submitted one watches that instance's log
	// instead; this is for the other end.
	OnLog(commandID, instanceID, narrative string)
	OnFinish(commandID, instanceID, status, narrative string)
	OnError(message string)
}

// ServeOptions configures a Runner.
type ServeOptions struct {
	// Interval is how often to look for new requests.
	Interval time.Duration
	// CatalogRefresh is how often to re-read which commands are this
	// device's Lua commands, so one registered while the runner is up is
	// picked up without a restart. Zero or less re-reads every pass.
	CatalogRefresh time.Duration
	// Concurrency caps how many scripts run at once.
	Concurrency int
	// Timeout is the per-run wall clock, unless a command's own spec
	// overrides it. Zero means DefaultTimeout.
	Timeout time.Duration
	// Run carries the rest of the per-run caps (submits, log lines,
	// depth, result size). Env, identity and Inputs are filled in by the
	// runner and anything set for them here is ignored.
	Run      Options
	Listener Listener
}

func (o ServeOptions) withDefaults() ServeOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultServeInterval
	}
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	return o
}

// Runner serves this device's Lua commands.
type Runner struct {
	cluster Cluster
	scripts *Catalog
	opts    ServeOptions

	// slots caps concurrent runs; wg tracks them so a stopped runner can
	// wait for what it started rather than abandoning it mid-write.
	slots chan struct{}
	wg    sync.WaitGroup

	// inFlight is what this process is already running, by instance id.
	// It is what keeps the claim (a StatusRunning entry) from reading as
	// "somebody died mid-run, retry it" on this runner's very next pass --
	// see claimed's doc comment.
	mu       sync.Mutex
	inFlight map[string]bool

	// commands is the filtered catalog, refreshed on CatalogRefresh.
	cmdMu       sync.Mutex
	commands    []Command
	lastRefresh time.Time
	selfPeerID  string
}

// NewRunner returns a Runner serving whichever Lua commands target the
// node cluster speaks for, reading their source through scripts.
func NewRunner(cluster Cluster, scripts *Catalog, opts ServeOptions) *Runner {
	opts = opts.withDefaults()
	return &Runner{
		cluster:  cluster,
		scripts:  scripts,
		opts:     opts,
		slots:    make(chan struct{}, opts.Concurrency),
		inFlight: map[string]bool{},
	}
}

// Serve runs until ctx is done, then waits for whatever it already
// started. An immediate first pass, so a request already pending when a
// runner starts is not held up for a whole interval.
//
// Every error along the way is reported to the Listener and retried on the
// next pass rather than ending the loop: a torn-down session (see
// Sessions) or a daemon restarting is a transient condition a long-running
// runner is expected to sit through, exactly as the watch loops in
// mobile/kvmobile do.
func (r *Runner) Serve(ctx context.Context) {
	r.Once(ctx)

	ticker := time.NewTicker(r.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.wg.Wait()
			return
		case <-ticker.C:
			r.Once(ctx)
		}
	}
}

// Once is a single scan pass: find this device's Lua commands, and start
// whatever they have pending that nothing has answered yet. It returns as
// soon as the runs are started, not when they finish -- which is the whole
// point (see this file's own doc comment).
func (r *Runner) Once(ctx context.Context) {
	commands, err := r.luaCommands(ctx)
	if err != nil {
		r.report(fmt.Errorf("luacmd: list commands: %w", err))
		return
	}
	for _, command := range commands {
		spec, ok := ParseSpec(command.Spec)
		if !ok {
			continue
		}
		requests, err := r.cluster.ListRequests(ctx, command.ID)
		if err != nil {
			r.report(fmt.Errorf("luacmd: list requests for %s: %w", command.ID, err))
			continue
		}
		for _, request := range requests {
			r.maybeStart(ctx, command, spec, request)
		}
	}
}

// luaCommands returns this device's own Lua commands, re-reading the
// catalog no more often than CatalogRefresh.
func (r *Runner) luaCommands(ctx context.Context) ([]Command, error) {
	r.cmdMu.Lock()
	defer r.cmdMu.Unlock()

	fresh := r.opts.CatalogRefresh > 0 && time.Since(r.lastRefresh) < r.opts.CatalogRefresh
	if fresh && r.commands != nil {
		return r.commands, nil
	}

	self, err := r.cluster.SelfPeerID(ctx)
	if err != nil {
		return nil, err
	}
	all, err := r.cluster.ListCommands(ctx)
	if err != nil {
		return nil, err
	}

	mine := make([]Command, 0, len(all))
	for _, command := range all {
		if command.PeerID != self {
			continue
		}
		if _, ok := ParseSpec(command.Spec); ok {
			mine = append(mine, command)
		}
	}
	r.commands = mine
	r.selfPeerID = self
	r.lastRefresh = time.Now()
	return mine, nil
}

// maybeStart decides whether request still needs running and, if it does,
// claims it and starts it.
func (r *Runner) maybeStart(ctx context.Context, command Command, spec Spec, request Request) {
	if r.claimed(request.InstanceID) {
		return
	}
	handled, err := r.alreadyHandled(ctx, request.InstanceID)
	if err != nil {
		r.report(fmt.Errorf("luacmd: check %s: %w", request.InstanceID, err))
		return
	}
	if handled {
		return
	}

	// Claim before starting, so a person watching sees the run begin and
	// so an interrupted run is distinguishable from one nothing ever
	// picked up. Not a lock: two runners serving the same command could
	// still both get here, which is why a command has one target peer.
	if err := r.cluster.Progress(ctx, request.RequestedBy, request.InstanceID, nil, DefaultCommandNarrated); err != nil {
		r.report(fmt.Errorf("luacmd: claim %s: %w", request.InstanceID, err))
		return
	}
	r.hold(request.InstanceID)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer r.release(request.InstanceID)

		select {
		case r.slots <- struct{}{}:
			defer func() { <-r.slots }()
		case <-ctx.Done():
			return
		}
		r.serveOne(ctx, command, spec, request)
	}()
}

// alreadyHandled is the same rule pkg/kvctl's dispatcher uses: an instance
// with no entries, or whose latest entry is still StatusRunning, is not
// handled. The second half is what makes a run whose process died get
// picked up again -- and is exactly why a runner must also remember what
// *it* is currently running (see claimed), since its own claim looks
// identical to a dead process's.
func (r *Runner) alreadyHandled(ctx context.Context, instanceID string) (bool, error) {
	entries, err := r.cluster.QueryLog(ctx, instanceID)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}
	return entries[len(entries)-1].Done(), nil
}

func (r *Runner) claimed(instanceID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inFlight[instanceID]
}

func (r *Runner) hold(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight[instanceID] = true
}

func (r *Runner) release(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inFlight, instanceID)
}

// serveOne loads the script, checks it is the one the command was
// registered with, runs it, and records a terminal entry either way.
func (r *Runner) serveOne(ctx context.Context, command Command, spec Spec, request Request) {
	r.notifyStart(command.ID, request.InstanceID)

	script, err := r.verifiedScript(ctx, spec)
	if err != nil {
		r.finish(ctx, command, request, nil, err)
		return
	}

	timeout := r.opts.Timeout
	if spec.TimeoutSeconds > 0 {
		timeout = time.Duration(spec.TimeoutSeconds) * time.Second
	}

	opts := r.opts.Run
	opts.Env = &runEnv{
		cluster:         r.cluster,
		requesterPeerID: request.RequestedBy,
		instanceID:      request.InstanceID,
		commandID:       command.ID,
		onLog:           r.notifyLog,
	}
	opts.InstanceID = request.InstanceID
	opts.CommandID = command.ID
	opts.RequestedBy = request.RequestedBy
	opts.ScriptID = spec.ScriptID
	opts.Inputs = request.Inputs
	opts.Depth = DepthOf(request.Inputs)
	opts.Timeout = timeout

	result, err := Run(ctx, script.Code, opts)
	r.finish(ctx, command, request, &result, err)
}

// verifiedScript reads the script the spec names and refuses it unless its
// bytes still hash to what the Command pinned.
//
// A mismatch is not an error to be retried: the journal now holds
// something other than what a voter registered, and running it would be
// running code nobody approved. It is recorded as a failed run so that
// whoever submitted the command finds out, rather than watching a request
// that never completes.
func (r *Runner) verifiedScript(ctx context.Context, spec Spec) (Script, error) {
	script, err := r.scripts.Get(ctx, spec.ScriptID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Script{}, fmt.Errorf("luacmd: script %s is not in the journal", spec.ScriptID)
		}
		return Script{}, err
	}
	if spec.SHA256 == "" {
		return Script{}, fmt.Errorf("luacmd: command's spec pins no hash for script %s, so what it runs cannot be verified", spec.ScriptID)
	}
	if actual := Sum(script.Code); actual != spec.SHA256 {
		return Script{}, fmt.Errorf("luacmd: script %s is %s but its command pins %s -- refusing to run code the catalog did not register", spec.ScriptID, actual, spec.SHA256)
	}
	return script, nil
}

// finish records the one terminal entry every run gets, whatever happened.
//
// Always one, and always terminal: an instance nothing ever wrote a final
// result for is retried forever by every future pass, and a run that
// failed generally should not be silently re-run with its side effects.
// A script failure is recorded as the Lua message alone -- the stack
// traceback goes in a field, where it is available without pushing the
// actual message out of a log row on a phone (see ScriptError).
func (r *Runner) finish(ctx context.Context, command Command, request Request, result *Result, runErr error) {
	fields := map[string]string{}
	narrative := ""

	switch {
	case runErr != nil:
		fields["status"] = "error"
		narrative = runErr.Error()
		var scriptErr *ScriptError
		if errors.As(runErr, &scriptErr) {
			narrative = scriptErr.Message
			if scriptErr.Traceback != "" {
				fields["traceback"] = scriptErr.Traceback
			}
		}
	case result != nil:
		for key, value := range result.Fields {
			fields[key] = value
		}
		narrative = result.Narrative
		if fields["status"] == "" {
			// A script that said nothing about how it went succeeded:
			// anything but StatusRunning is terminal either way, but a
			// stated status is what a log list can colour and a caller
			// can branch on.
			fields["status"] = "ok"
		}
	}

	// The run's own context may already be done (a stopped runner); the
	// terminal entry still has to be written, or the request looks
	// unhandled forever.
	writeCtx := ctx
	if writeCtx.Err() != nil {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
	}
	if err := r.cluster.Append(writeCtx, request.RequestedBy, request.InstanceID, fields, narrative); err != nil {
		r.report(fmt.Errorf("luacmd: record result for %s: %w", request.InstanceID, err))
	}
	r.notifyFinish(command.ID, request.InstanceID, fields["status"], narrative)
}

func (r *Runner) report(err error) {
	if err == nil || r.opts.Listener == nil {
		return
	}
	r.opts.Listener.OnError(err.Error())
}

func (r *Runner) notifyStart(commandID, instanceID string) {
	if r.opts.Listener != nil {
		r.opts.Listener.OnStart(commandID, instanceID)
	}
}

func (r *Runner) notifyLog(commandID, instanceID, narrative string) {
	if r.opts.Listener != nil {
		r.opts.Listener.OnLog(commandID, instanceID, narrative)
	}
}

func (r *Runner) notifyFinish(commandID, instanceID, status, narrative string) {
	if r.opts.Listener != nil {
		r.opts.Listener.OnFinish(commandID, instanceID, status, narrative)
	}
}

// runEnv is one run's view of the cluster: the same Cluster, with this
// dispatch's requester and instance id already bound, so a script can
// write to its own log and nobody else's.
type runEnv struct {
	cluster         Cluster
	requesterPeerID string
	instanceID      string
	commandID       string
	// onLog, if set, is told about each line after it is durably written
	// -- after, so a listener never shows a line the log does not have.
	onLog func(commandID, instanceID, narrative string)
}

func (e *runEnv) Submit(ctx context.Context, commandID, inputsJSON string) (string, error) {
	return e.cluster.Submit(ctx, commandID, inputsJSON)
}

func (e *runEnv) QueryLog(ctx context.Context, instanceID string) ([]LogEntry, error) {
	return e.cluster.QueryLog(ctx, instanceID)
}

func (e *runEnv) Progress(ctx context.Context, fields map[string]string, narrative string) error {
	if err := e.cluster.Progress(ctx, e.requesterPeerID, e.instanceID, fields, narrative); err != nil {
		return err
	}
	if e.onLog != nil {
		e.onLog(e.commandID, e.instanceID, narrative)
	}
	return nil
}
