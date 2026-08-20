package luacmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	lua "github.com/yuin/gopher-lua"
)

// One run of one script, and the kv table it reaches the cluster through.
//
// Everything a script can do to the outside world goes through Env, which
// is an interface for two reasons: it keeps this file free of any client
// (so the same runtime serves the desktop CLI and the Android app), and it
// lets every behaviour here be tested without a daemon, a catalog or a
// command -- the same seam examples/croncmd draws with its Submitter.

// StatusRunning marks a log entry as not yet terminal. It must stay equal
// to pkg/kvctl.CommandStatusRunning and mobile/kvmobile's copy of it: the
// dispatchers there decide whether a request still needs handling by
// comparing against that exact string, and a run whose progress entries
// said anything else would look finished the moment it started. A test
// asserts the equality rather than an import doing it, so that this file
// stays free of a client dependency.
const StatusRunning = "running"

// depthKey is the reserved inputs key carrying how many Lua runs deep a
// dispatch is. It travels in the inputs JSON because that is the only
// thing that reaches a child dispatch, and it is stripped before a script
// sees its own inputs (see inputsToLua) so it cannot be read as data or
// forged as one.
const depthKey = "_lua_depth"

// Defaults for Options. Deliberately modest: these bound what one
// dispatch can do to a device that may be a phone, and a script needing
// more than any of them is better rewritten as several commands.
const (
	DefaultTimeout      = 5 * time.Minute
	DefaultPollInterval = 250 * time.Millisecond
	DefaultMaxSubmits   = 32
	DefaultMaxLogLines  = 256
	DefaultMaxLogReads  = 500
	DefaultMaxDepth     = 3
	DefaultMaxResult    = 16 << 10
)

// LogEntry is one record from a command's execution log, as a script sees
// it through kv.wait/kv.logs.
type LogEntry struct {
	InstanceID string            `json:"instance_id"`
	Timestamp  time.Time         `json:"timestamp"`
	Fields     map[string]string `json:"fields,omitempty"`
	Narrative  string            `json:"narrative,omitempty"`
}

// Status is the entry's own status field, empty if it has none.
func (e LogEntry) Status() string { return e.Fields["status"] }

// Done reports whether this entry is terminal -- anything but
// StatusRunning, including no status at all, which is what a handler
// written before progress reporting existed produces.
func (e LogEntry) Done() bool { return e.Status() != StatusRunning }

// Env is everything a running script can reach outside its own VM.
//
// Every method is called with the run's own context, so a torn-down run
// stops waiting rather than finishing on a dead deadline. An
// implementation is expected to enforce nothing: the runtime applies the
// caps (see Options), and the cluster applies the permissions.
type Env interface {
	// Submit dispatches commandID with inputsJSON under the running
	// device's own identity, returning the instance id it was recorded
	// under.
	Submit(ctx context.Context, commandID, inputsJSON string) (string, error)
	// QueryLog returns every log entry recorded for instanceID so far, in
	// the order they were written.
	QueryLog(ctx context.Context, instanceID string) ([]LogEntry, error)
	// Progress records a non-terminal entry in *this* run's own log --
	// the live line a person watching the log list sees. Implementations
	// stamp StatusRunning; the runtime never asks for a terminal entry,
	// since that is the runner's to write once Run returns.
	Progress(ctx context.Context, fields map[string]string, narrative string) error
}

// Result is what a script produced: exactly the (fields, narrative) pair
// the command log takes, so a runner records it without interpreting it.
type Result struct {
	Fields    map[string]string
	Narrative string
}

// Options configures one run.
type Options struct {
	// Env is required.
	Env Env
	// Identity of the dispatch being served, surfaced to the script as
	// kv.instance_id / kv.command_id / kv.requested_by / kv.script_id.
	InstanceID  string
	CommandID   string
	RequestedBy string
	ScriptID    string
	// Inputs is the submitter's inputs JSON, verbatim.
	Inputs string
	// Depth is how many Lua runs deep this one already is, read from the
	// inputs by the runner (see DepthOf). A run deeper than MaxDepth is
	// refused before the script starts.
	Depth int

	// Timeout bounds the whole run in wall-clock time. It is enforced
	// between instructions and while waiting, and it is the only time
	// bound there is -- see newState's own note on what is not enforced.
	Timeout time.Duration
	// PollInterval is how often kv.wait re-reads a child's log.
	PollInterval time.Duration
	// MaxSubmits caps how many commands one run may dispatch, MaxLogLines
	// how many lines it may write, MaxLogReads how many of a child's
	// entries one kv.logs call returns, MaxDepth how deep the chain may
	// go, and MaxResultBytes how large its final result may be.
	MaxSubmits     int
	MaxLogLines    int
	MaxLogReads    int
	MaxDepth       int
	MaxResultBytes int
}

func (o Options) withDefaults() Options {
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.MaxSubmits <= 0 {
		o.MaxSubmits = DefaultMaxSubmits
	}
	if o.MaxLogLines <= 0 {
		o.MaxLogLines = DefaultMaxLogLines
	}
	if o.MaxLogReads <= 0 {
		o.MaxLogReads = DefaultMaxLogReads
	}
	if o.MaxDepth <= 0 {
		o.MaxDepth = DefaultMaxDepth
	}
	if o.MaxResultBytes <= 0 {
		o.MaxResultBytes = DefaultMaxResult
	}
	return o
}

// ErrTooDeep is what Run returns for a dispatch already deeper than
// MaxDepth -- checked before the script is even compiled, so a runaway
// chain stops at a known depth rather than at whatever the device gives
// out first.
var ErrTooDeep = errors.New("luacmd: dispatch chain is too deep")

// Run compiles and runs code, and returns what the script produced.
//
// The error return means the *script* failed -- it did not compile, it
// raised, it ran past its deadline, or it broke one of the caps. A child
// command failing is not that: kv.wait hands the child's terminal record
// back as a value and the script decides what it means, which is what lets
// one script produce both a clean success and a clean failure depending
// only on its inputs. A runner records either outcome as a terminal log
// entry; see the package doc.
func Run(ctx context.Context, code string, opts Options) (Result, error) {
	opts = opts.withDefaults()
	if opts.Env == nil {
		return Result{}, fmt.Errorf("luacmd: run: an Env is required")
	}
	if opts.Depth > opts.MaxDepth {
		return Result{}, fmt.Errorf("%w: depth %d exceeds the limit of %d", ErrTooDeep, opts.Depth, opts.MaxDepth)
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	state := newState(ctx)
	defer state.Close()

	r := &run{opts: opts, ctx: ctx}
	if err := r.installKV(state); err != nil {
		return Result{}, err
	}

	fn, err := state.LoadString(code)
	if err != nil {
		return Result{}, fmt.Errorf("luacmd: compile script: %w", err)
	}
	state.Push(fn)
	if err := state.PCall(0, 1, nil); err != nil {
		// A deadline reaches the script as a raised Lua error (see
		// removedGlobals on why nothing can catch it), so the error text
		// alone would not say which of the two happened.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Result{}, fmt.Errorf("luacmd: script stopped after %s: %w", opts.Timeout, ctxErr)
		}
		return Result{}, scriptError(err)
	}

	result, err := resultFromLua(state.Get(-1))
	if err != nil {
		return Result{}, err
	}
	if err := r.checkResultSize(result); err != nil {
		return Result{}, err
	}
	return result, nil
}

// ScriptError is what Run returns for a script that raised: the Lua error
// message on its own, and the stack traceback separately.
//
// They are separate because they have different readers. A runner records
// Message as the failed run's narrative -- it is one line, it names the
// line number, and it is what the person who wrote the script needs to see
// in a log list on a phone. Traceback is for whoever is debugging, and
// putting it in the narrative would push the actual message off the top of
// every log row that mattered. Error() still returns both, so a caller
// that just prints the error loses nothing.
type ScriptError struct {
	// Message is the Lua error, position prefix included, e.g.
	// `<string>:2: inner refused: e2e-fail`.
	Message string
	// Traceback is gopher-lua's stack traceback, empty if it had none.
	Traceback string
}

func (e *ScriptError) Error() string {
	if e.Traceback == "" {
		return "luacmd: script failed: " + e.Message
	}
	return "luacmd: script failed: " + e.Message + "\n" + e.Traceback
}

// scriptError turns gopher-lua's own error into a ScriptError, splitting
// the message from the traceback it carries alongside.
func scriptError(err error) error {
	var apiErr *lua.ApiError
	if errors.As(err, &apiErr) {
		message := err.Error()
		if apiErr.Object != nil && apiErr.Object != lua.LNil {
			message = apiErr.Object.String()
		}
		return &ScriptError{Message: message, Traceback: apiErr.StackTrace}
	}
	return &ScriptError{Message: err.Error()}
}

// DepthOf reads the reserved depth key out of an inputs JSON object --
// what a runner calls to learn how deep the dispatch it is about to serve
// already is. Absent, blank, or not an object all mean zero: a dispatch
// submitted by a person rather than by another script.
func DepthOf(inputsJSON string) int {
	if inputsJSON == "" {
		return 0
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(inputsJSON), &decoded); err != nil {
		return 0
	}
	depth, ok := decoded[depthKey].(float64)
	if !ok || depth < 0 {
		return 0
	}
	return int(depth)
}

// withDepth returns inputsJSON with the reserved depth key set to depth.
//
// Refuses anything but a JSON object, which also settles what a script may
// pass to kv.submit: a table of named values. An array or a bare scalar
// has nowhere to carry the depth, and a chain that silently lost it would
// have no limit at all.
func withDepth(inputsJSON string, depth int) (string, error) {
	decoded := map[string]any{}
	if inputsJSON != "" {
		if err := json.Unmarshal([]byte(inputsJSON), &decoded); err != nil {
			return "", fmt.Errorf("luacmd: inputs must be a table of named values: %w", err)
		}
	}
	decoded[depthKey] = depth
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return "", fmt.Errorf("luacmd: encode inputs: %w", err)
	}
	return string(encoded), nil
}

// run is one script's execution state: the caps it is spending and the
// context it must stop for.
type run struct {
	opts     Options
	ctx      context.Context
	submits  int
	logLines int
}

// installKV builds the kv table and points print at kv.log.
func (r *run) installKV(state *lua.LState) error {
	inputs, err := inputsToLua(state, r.opts.Inputs)
	if err != nil {
		return err
	}

	kv := state.NewTable()
	kv.RawSetString("inputs", inputs)
	kv.RawSetString("instance_id", lua.LString(r.opts.InstanceID))
	kv.RawSetString("command_id", lua.LString(r.opts.CommandID))
	kv.RawSetString("requested_by", lua.LString(r.opts.RequestedBy))
	kv.RawSetString("script_id", lua.LString(r.opts.ScriptID))
	kv.RawSetString("depth", lua.LNumber(r.opts.Depth))

	logFn := state.NewFunction(r.luaLog)
	kv.RawSetString("log", logFn)
	kv.RawSetString("submit", state.NewFunction(r.luaSubmit))
	kv.RawSetString("wait", state.NewFunction(r.luaWait))
	kv.RawSetString("run", state.NewFunction(r.luaRun))
	kv.RawSetString("logs", state.NewFunction(r.luaLogs))
	kv.RawSetString("sleep", state.NewFunction(r.luaSleep))

	state.SetGlobal("kv", kv)
	// print is the reflex of anyone who has written Lua before, and there
	// is no stdout worth writing to on a phone -- so it writes a log line,
	// which is what a person watching a run actually wants to see.
	state.SetGlobal("print", state.NewFunction(r.luaPrint))
	return nil
}

// luaLog implements kv.log(text, fields?).
func (r *run) luaLog(state *lua.LState) int {
	text := state.CheckString(1)
	fields, err := luaToFields(state.Get(2))
	if err != nil {
		state.RaiseError("%s", err.Error())
		return 0
	}
	r.progress(state, fields, text)
	return 0
}

// luaPrint implements print(...), joining its arguments the way Lua's own
// print does and recording the line.
func (r *run) luaPrint(state *lua.LState) int {
	parts := make([]byte, 0, 64)
	for i := 1; i <= state.GetTop(); i++ {
		if i > 1 {
			parts = append(parts, '\t')
		}
		parts = append(parts, state.ToStringMeta(state.Get(i)).String()...)
	}
	r.progress(state, nil, string(parts))
	return 0
}

// progress records one line against the log-line cap, raising in the
// script if it is spent or the write fails.
func (r *run) progress(state *lua.LState, fields map[string]string, text string) {
	if r.logLines >= r.opts.MaxLogLines {
		state.RaiseError("luacmd: this run has already written its %d log lines", r.opts.MaxLogLines)
		return
	}
	r.logLines++
	if err := r.opts.Env.Progress(r.ctx, fields, text); err != nil {
		state.RaiseError("luacmd: write log line: %s", err.Error())
	}
}

// luaSubmit implements kv.submit(command_id, inputs?) -> instance_id.
func (r *run) luaSubmit(state *lua.LState) int {
	instanceID := r.submit(state, state.CheckString(1), state.Get(2))
	state.Push(lua.LString(instanceID))
	return 1
}

// submit is the shared body of kv.submit and kv.run: spend a submit,
// carry the depth, dispatch, and record the child in this run's own log so
// that the parent's log names every execution it caused without the script
// having to remember to (see the package doc).
func (r *run) submit(state *lua.LState, commandID string, inputs lua.LValue) string {
	if commandID == "" {
		state.RaiseError("luacmd: kv.submit needs a command id")
		return ""
	}
	if r.submits >= r.opts.MaxSubmits {
		state.RaiseError("luacmd: this run has already dispatched its %d commands", r.opts.MaxSubmits)
		return ""
	}
	if r.opts.Depth+1 > r.opts.MaxDepth {
		state.RaiseError("luacmd: %s: a script %d deep may not dispatch another", ErrTooDeep.Error(), r.opts.Depth)
		return ""
	}

	inputsJSON, err := luaToInputs(inputs)
	if err != nil {
		state.RaiseError("%s", err.Error())
		return ""
	}
	inputsJSON, err = withDepth(inputsJSON, r.opts.Depth+1)
	if err != nil {
		state.RaiseError("%s", err.Error())
		return ""
	}

	r.submits++
	instanceID, err := r.opts.Env.Submit(r.ctx, commandID, inputsJSON)
	if err != nil {
		state.RaiseError("luacmd: submit %s: %s", commandID, err.Error())
		return ""
	}

	r.progress(state, map[string]string{
		"child_command":  commandID,
		"child_instance": instanceID,
	}, fmt.Sprintf("submitted %s as %s", commandID, instanceID))
	return instanceID
}

// luaWait implements kv.wait(instance_id, seconds?) -> record.
func (r *run) luaWait(state *lua.LState) int {
	instanceID := state.CheckString(1)
	seconds := float64(state.OptNumber(2, 0))
	state.Push(r.wait(state, instanceID, seconds))
	return 1
}

// luaRun implements kv.run(command_id, inputs?, seconds?) -> instance_id,
// record.
func (r *run) luaRun(state *lua.LState) int {
	commandID := state.CheckString(1)
	inputs := state.Get(2)
	seconds := float64(state.OptNumber(3, 0))

	instanceID := r.submit(state, commandID, inputs)
	record := r.wait(state, instanceID, seconds)
	state.Push(lua.LString(instanceID))
	state.Push(record)
	return 2
}

// wait polls instanceID's log until a terminal entry lands or the wait
// runs out, and returns the record table a script reads.
//
// Running out is a value, not an error: the record comes back with
// done=false and timed_out=true, so a script waiting on something slow
// decides for itself whether that is a failure -- the same reasoning that
// makes a *failed* child a value (see Run's doc comment).
func (r *run) wait(state *lua.LState, instanceID string, seconds float64) lua.LValue {
	if instanceID == "" {
		state.RaiseError("luacmd: kv.wait needs an instance id")
		return lua.LNil
	}

	ctx := r.ctx
	if seconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(r.ctx, time.Duration(seconds*float64(time.Second)))
		defer cancel()
	}

	for {
		entries, err := r.opts.Env.QueryLog(ctx, instanceID)
		if err == nil && len(entries) > 0 {
			if last := entries[len(entries)-1]; last.Done() {
				return r.recordToLua(state, last, true, false)
			}
		}
		if err != nil && ctx.Err() == nil {
			state.RaiseError("luacmd: read log for %s: %s", instanceID, err.Error())
			return lua.LNil
		}

		select {
		case <-ctx.Done():
			// The run's own deadline is a failure; this wait's own
			// timeout is an answer.
			if r.ctx.Err() != nil {
				state.RaiseError("luacmd: %s", r.ctx.Err().Error())
				return lua.LNil
			}
			return r.recordToLua(state, LogEntry{InstanceID: instanceID}, false, true)
		case <-time.After(r.opts.PollInterval):
		}
	}
}

// luaLogs implements kv.logs(instance_id, limit?) -> array of records.
func (r *run) luaLogs(state *lua.LState) int {
	instanceID := state.CheckString(1)
	limit := int(state.OptNumber(2, 0))

	entries, err := r.opts.Env.QueryLog(r.ctx, instanceID)
	if err != nil {
		state.RaiseError("luacmd: read log for %s: %s", instanceID, err.Error())
		return 0
	}

	capped := r.opts.MaxLogReads
	if limit > 0 && limit < capped {
		capped = limit
	}
	if len(entries) > capped {
		// Never silently: a script folding a child's log into its own
		// would otherwise report a partial copy as a whole one.
		r.progress(state, map[string]string{"child_instance": instanceID},
			fmt.Sprintf("read the first %d of %s's %d log entries", capped, instanceID, len(entries)))
		entries = entries[:capped]
	}

	list := state.NewTable()
	for i, entry := range entries {
		list.RawSetInt(i+1, r.recordToLua(state, entry, entry.Done(), false))
	}
	state.Push(list)
	return 1
}

// luaSleep implements kv.sleep(seconds), stopping early -- and failing the
// run -- if the deadline arrives first.
func (r *run) luaSleep(state *lua.LState) int {
	seconds := float64(state.CheckNumber(1))
	if seconds <= 0 {
		return 0
	}
	timer := time.NewTimer(time.Duration(seconds * float64(time.Second)))
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		state.RaiseError("luacmd: %s", r.ctx.Err().Error())
	case <-timer.C:
	}
	return 0
}

// recordToLua is the table shape every record a script sees has.
func (r *run) recordToLua(state *lua.LState, entry LogEntry, done, timedOut bool) *lua.LTable {
	record := state.NewTable()
	record.RawSetString("instance_id", lua.LString(entry.InstanceID))
	record.RawSetString("status", lua.LString(entry.Status()))
	record.RawSetString("narrative", lua.LString(entry.Narrative))
	record.RawSetString("fields", fieldsToLua(state, entry.Fields))
	record.RawSetString("done", lua.LBool(done))
	record.RawSetString("timed_out", lua.LBool(timedOut))
	if !entry.Timestamp.IsZero() {
		record.RawSetString("timestamp", lua.LString(entry.Timestamp.Format(time.RFC3339Nano)))
	}
	return record
}

// checkResultSize bounds what one run can add to the log in its final
// entry, which is replicated to every node and kept.
func (r *run) checkResultSize(result Result) error {
	size := len(result.Narrative)
	for key, value := range result.Fields {
		size += len(key) + len(value)
	}
	if size > r.opts.MaxResultBytes {
		return fmt.Errorf("luacmd: result is %d bytes, over the %d byte limit", size, r.opts.MaxResultBytes)
	}
	return nil
}

// resultFromLua turns what a script returned into a Result.
//
// A table is the full form and must say which half is which; a bare string
// is the common case of having nothing structured to report; returning
// nothing at all is legitimate and records an empty entry. A table with
// neither key is refused rather than guessed at, because guessing would
// silently file somebody's intended narrative under a field name or lose
// their fields entirely.
func resultFromLua(v lua.LValue) (Result, error) {
	switch value := v.(type) {
	case *lua.LNilType:
		return Result{}, nil
	case lua.LString:
		return Result{Narrative: string(value)}, nil
	case lua.LNumber, lua.LBool:
		return Result{Narrative: value.String()}, nil
	case *lua.LTable:
		narrative := value.RawGetString("narrative")
		rawFields := value.RawGetString("fields")
		if narrative == lua.LNil && rawFields == lua.LNil {
			return Result{}, fmt.Errorf("luacmd: a script returning a table must return {fields = {...}} or {narrative = \"...\"} (or both)")
		}
		if narrative != lua.LNil && narrative.Type() != lua.LTString {
			return Result{}, fmt.Errorf("luacmd: narrative must be a string, got %s", narrative.Type().String())
		}
		fields, err := luaToFields(rawFields)
		if err != nil {
			return Result{}, err
		}
		return Result{Fields: fields, Narrative: narrative.String()}, nil
	default:
		return Result{}, fmt.Errorf("luacmd: a script may return a table, a string or nothing, not a %s", v.Type().String())
	}
}
