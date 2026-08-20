package luacmd_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// fakeEnv stands in for the cluster: it records what a script dispatched
// and logged, and answers reads from whatever the test put in front of it.
// Submitting is a function so a test can decide what a child does --
// including, in TestOuterInnerChain, actually running the child script.
type fakeEnv struct {
	mu        sync.Mutex
	submits   []submitCall
	lines     []luacmd.LogEntry
	logs      map[string][]luacmd.LogEntry
	submitFn  func(ctx context.Context, commandID, inputsJSON string) (string, error)
	queryFn   func(ctx context.Context, instanceID string) ([]luacmd.LogEntry, error)
	failEvery bool
}

type submitCall struct {
	CommandID string
	Inputs    string
}

func newFakeEnv() *fakeEnv {
	return &fakeEnv{logs: map[string][]luacmd.LogEntry{}}
}

func (f *fakeEnv) Submit(ctx context.Context, commandID, inputsJSON string) (string, error) {
	f.mu.Lock()
	f.submits = append(f.submits, submitCall{CommandID: commandID, Inputs: inputsJSON})
	n := len(f.submits)
	fn := f.submitFn
	f.mu.Unlock()

	if fn != nil {
		return fn(ctx, commandID, inputsJSON)
	}
	return fmt.Sprintf("instance-%d", n), nil
}

func (f *fakeEnv) QueryLog(ctx context.Context, instanceID string) ([]luacmd.LogEntry, error) {
	if f.queryFn != nil {
		return f.queryFn(ctx, instanceID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logs[instanceID], nil
}

func (f *fakeEnv) Progress(ctx context.Context, fields map[string]string, narrative string) error {
	if f.failEvery {
		return errors.New("progress is unavailable")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lines = append(f.lines, luacmd.LogEntry{Fields: fields, Narrative: narrative})
	return nil
}

func (f *fakeEnv) setLog(instanceID string, entries ...luacmd.LogEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs[instanceID] = entries
}

func (f *fakeEnv) narratives() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.lines))
	for _, l := range f.lines {
		out = append(out, l.Narrative)
	}
	return out
}

func (f *fakeEnv) submitted() []submitCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]submitCall{}, f.submits...)
}

// testOptions keeps every run in these tests fast: a short deadline and a
// poll interval well under it, so a case that waits on something that
// never arrives finishes in milliseconds rather than minutes.
func testOptions(env luacmd.Env) luacmd.Options {
	return luacmd.Options{
		Env:          env,
		InstanceID:   "inst-1",
		CommandID:    "outer",
		RequestedBy:  "12D3KooWSubmitter",
		ScriptID:     "outer",
		Timeout:      2 * time.Second,
		PollInterval: 5 * time.Millisecond,
	}
}

func run(t *testing.T, env luacmd.Env, code string) (luacmd.Result, error) {
	t.Helper()
	return luacmd.Run(context.Background(), code, testOptions(env))
}

func mustRun(t *testing.T, env luacmd.Env, code string) luacmd.Result {
	t.Helper()
	result, err := run(t, env, code)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return result
}

// StatusRunning is duplicated rather than imported (see its doc comment),
// so something has to hold the two together. This is that something: if
// pkg/kvctl ever changes what a non-terminal entry says, every Lua run's
// progress lines would start looking terminal and dispatchers would stop
// retrying interrupted runs.
func TestStatusRunningMatchesTheDispatchersDefinition(t *testing.T) {
	if luacmd.StatusRunning != kvctl.CommandStatusRunning {
		t.Errorf("luacmd.StatusRunning is %q but kvctl.CommandStatusRunning is %q", luacmd.StatusRunning, kvctl.CommandStatusRunning)
	}
}

func TestRunReturnsWhatTheScriptReturned(t *testing.T) {
	result := mustRun(t, newFakeEnv(), `return {fields = {status = "ok", count = 3}, narrative = "all done"}`)
	if result.Narrative != "all done" {
		t.Errorf("narrative = %q", result.Narrative)
	}
	if result.Fields["status"] != "ok" || result.Fields["count"] != "3" {
		t.Errorf("fields = %v, want status ok and count stringified to 3", result.Fields)
	}
}

func TestRunAcceptsAStringOrNothingAsAResult(t *testing.T) {
	if got := mustRun(t, newFakeEnv(), `return "just words"`); got.Narrative != "just words" {
		t.Errorf("string result gave %+v", got)
	}
	if got := mustRun(t, newFakeEnv(), `local x = 1`); got.Narrative != "" || got.Fields != nil {
		t.Errorf("a script returning nothing gave %+v, want an empty result", got)
	}
}

func TestRunRejectsAResultItWouldHaveToGuessAt(t *testing.T) {
	_, err := run(t, newFakeEnv(), `return {status = "ok"}`)
	if err == nil {
		t.Fatal("Run accepted a table with neither fields nor narrative")
	}
	if !strings.Contains(err.Error(), "narrative") {
		t.Errorf("error %q does not tell the author what shape to return", err)
	}
}

func TestKvLogWritesLiveLines(t *testing.T) {
	env := newFakeEnv()
	mustRun(t, env, `
kv.log("hello from outer begin")
kv.log("with fields", {step = "1"})
return "done"
`)
	lines := env.narratives()
	if len(lines) != 2 || lines[0] != "hello from outer begin" || lines[1] != "with fields" {
		t.Fatalf("recorded lines %v", lines)
	}
	if env.lines[1].Fields["step"] != "1" {
		t.Errorf("fields on the second line = %v", env.lines[1].Fields)
	}
}

// print is a script author's reflex and there is no useful stdout on the
// devices this runs on, so it has to end up in the same place kv.log does.
func TestPrintWritesALogLine(t *testing.T) {
	env := newFakeEnv()
	mustRun(t, env, `print("hello", 42, true)`)
	lines := env.narratives()
	if len(lines) != 1 || lines[0] != "hello\t42\ttrue" {
		t.Errorf("print recorded %v, want one tab-joined line", lines)
	}
}

func TestKvInputsAreVisibleAndTheDepthKeyIsNot(t *testing.T) {
	env := newFakeEnv()
	opts := testOptions(env)
	opts.Inputs = `{"who":"e2e-ok","mode":"ok","nested":{"n":1},"list":[1,2]}`
	opts.Depth = 1

	result, err := luacmd.Run(context.Background(), `
local seen = {}
for k in pairs(kv.inputs) do seen[#seen + 1] = k end
table.sort(seen)
return {
  fields = {
    who = kv.inputs.who,
    nested = kv.inputs.nested.n,
    second = kv.inputs.list[2],
    keys = table.concat(seen, ","),
    depth = kv.depth,
  },
}
`, opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Fields["who"] != "e2e-ok" || result.Fields["nested"] != "1" || result.Fields["second"] != "2" {
		t.Errorf("inputs did not arrive intact: %v", result.Fields)
	}
	if result.Fields["keys"] != "list,mode,nested,who" {
		t.Errorf("kv.inputs keys are %q -- the reserved depth key must not be visible as data", result.Fields["keys"])
	}
	if result.Fields["depth"] != "1" {
		t.Errorf("kv.depth = %q, want 1", result.Fields["depth"])
	}
}

func TestKvSubmitDispatchesCarriesDepthAndLogsTheChild(t *testing.T) {
	env := newFakeEnv()
	mustRun(t, env, `
local id = kv.submit("inner", {who = "e2e-ok"})
return {fields = {child = id}}
`)

	calls := env.submitted()
	if len(calls) != 1 || calls[0].CommandID != "inner" {
		t.Fatalf("submitted %+v", calls)
	}
	var inputs map[string]any
	if err := json.Unmarshal([]byte(calls[0].Inputs), &inputs); err != nil {
		t.Fatalf("child inputs are not an object: %v", err)
	}
	if inputs["who"] != "e2e-ok" {
		t.Errorf("child inputs lost the script's own values: %v", inputs)
	}
	if luacmd.DepthOf(calls[0].Inputs) != 1 {
		t.Errorf("child inputs carry depth %d, want 1", luacmd.DepthOf(calls[0].Inputs))
	}

	// The parent's own log must name the child without the script asking.
	lines := env.narratives()
	if len(lines) != 1 || !strings.Contains(lines[0], "inner") || !strings.Contains(lines[0], "instance-1") {
		t.Fatalf("parent log lines %v, want one naming the child command and instance", lines)
	}
	if env.lines[0].Fields["child_instance"] != "instance-1" {
		t.Errorf("child instance is not in the parent line's fields: %v", env.lines[0].Fields)
	}
}

func TestKvWaitReturnsTheChildsTerminalRecord(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1",
		luacmd.LogEntry{InstanceID: "instance-1", Fields: map[string]string{"status": luacmd.StatusRunning}, Narrative: "working"},
		luacmd.LogEntry{InstanceID: "instance-1", Fields: map[string]string{"status": "ok"}, Narrative: "hello from inner: e2e-ok"},
	)

	result := mustRun(t, env, `
local id = kv.submit("inner", {who = "e2e-ok"})
local res = kv.wait(id, 1)
return {fields = {status = res.status, done = res.done}, narrative = res.narrative}
`)
	if result.Narrative != "hello from inner: e2e-ok" {
		t.Errorf("narrative = %q, want the child's own", result.Narrative)
	}
	if result.Fields["status"] != "ok" || result.Fields["done"] != "true" {
		t.Errorf("fields = %v, want the terminal record", result.Fields)
	}
}

// D7: a child that failed comes back as a value the script inspects. If it
// raised instead, "the child failed and I handled it" and "my own script
// is broken" would be indistinguishable in the log.
func TestAFailedChildIsAValueNotAnError(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields:     map[string]string{"status": "error"},
		Narrative:  "inner refused: e2e-fail",
	})

	result, err := run(t, env, `
local id, res = kv.run("inner", {mode = "fail"}, 1)
if res.status ~= "ok" then
  return {fields = {status = "error", child = id}, narrative = "hello from outer failed: " .. res.narrative}
end
return {fields = {status = "ok"}, narrative = "hello from outer end"}
`)
	if err != nil {
		t.Fatalf("a failing child made the whole run fail: %v", err)
	}
	if result.Fields["status"] != "error" {
		t.Errorf("fields = %v, want the script's own error handling to have run", result.Fields)
	}
	if !strings.Contains(result.Narrative, "inner refused: e2e-fail") {
		t.Errorf("narrative = %q, want the child's error text carried up", result.Narrative)
	}
}

// A wait that runs out is also a value: done=false, timed_out=true. The
// run's own deadline is the thing that is fatal, not this.
func TestAWaitThatRunsOutComesBackAsAnUnfinishedRecord(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1", luacmd.LogEntry{
		InstanceID: "instance-1",
		Fields:     map[string]string{"status": luacmd.StatusRunning},
		Narrative:  "still working",
	})

	result, err := run(t, env, `
local id = kv.submit("inner")
local res = kv.wait(id, 0.05)
return {fields = {done = res.done, timed_out = res.timed_out}}
`)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Fields["done"] != "false" || result.Fields["timed_out"] != "true" {
		t.Errorf("fields = %v, want done=false timed_out=true", result.Fields)
	}
}

func TestKvLogsReturnsAChildsEntries(t *testing.T) {
	env := newFakeEnv()
	env.setLog("instance-1",
		luacmd.LogEntry{InstanceID: "instance-1", Fields: map[string]string{"status": luacmd.StatusRunning}, Narrative: "hello from inner: e2e-ok"},
		luacmd.LogEntry{InstanceID: "instance-1", Fields: map[string]string{"status": "ok"}, Narrative: "inner done"},
	)

	result := mustRun(t, env, `
local id = kv.submit("inner")
local out = {}
for _, r in ipairs(kv.logs(id)) do out[#out + 1] = r.narrative end
return {narrative = table.concat(out, " | ")}
`)
	if result.Narrative != "hello from inner: e2e-ok | inner done" {
		t.Errorf("narrative = %q", result.Narrative)
	}
}

// The deadline is enforced between instructions, and nothing in the
// sandbox can catch it -- pcall is gone precisely so that this test cannot
// be defeated by wrapping the loop in one (see removedGlobals).
func TestAnEndlessScriptIsStoppedByItsDeadline(t *testing.T) {
	env := newFakeEnv()
	opts := testOptions(env)
	opts.Timeout = 150 * time.Millisecond

	started := time.Now()
	_, err := luacmd.Run(context.Background(), `while true do end`, opts)
	if err == nil {
		t.Fatal("an endless script finished")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error is %v, want a deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Errorf("took %s to stop an endless script", elapsed)
	}
}

func TestASandboxedScriptCannotReachTheHost(t *testing.T) {
	// Each of these is nil rather than present-and-refusing, so the check
	// is that the name does not exist at all.
	for _, name := range []string{
		"io", "os", "package", "debug", "coroutine", "require", "module",
		"dofile", "loadfile", "load", "loadstring", "getfenv", "setfenv",
		"newproxy", "collectgarbage",
	} {
		t.Run(name, func(t *testing.T) {
			result := mustRun(t, newFakeEnv(), fmt.Sprintf(`return {fields = {present = tostring(%s ~= nil)}}`, name))
			if result.Fields["present"] != "false" {
				t.Errorf("%s is reachable from a script", name)
			}
		})
	}
}

func TestWhatAScriptDoesKeep(t *testing.T) {
	result := mustRun(t, newFakeEnv(), `
local parts = {}
for _, name in ipairs({"assert", "error", "ipairs", "pairs", "select", "tonumber", "tostring", "type", "unpack", "setmetatable"}) do
  if _G[name] == nil then parts[#parts + 1] = name end
end
if string.upper("a") ~= "A" then parts[#parts + 1] = "string.upper" end
if math.floor(1.5) ~= 1 then parts[#parts + 1] = "math.floor" end
if table.concat({"a"}) ~= "a" then parts[#parts + 1] = "table.concat" end
return {narrative = table.concat(parts, ",")}
`)
	if result.Narrative != "" {
		t.Errorf("these should still work inside the sandbox but do not: %s", result.Narrative)
	}
}

// pcall stays available, and this is why that is safe: gopher-lua returns
// out of its interpreter loop when the context is spent rather than only
// raising, so no depth of pcall gets another instruction to run. This is
// the escape attempt that looks most likely to work -- every loop in it,
// inner and outer, is inside a pcall.
func TestPcallCannotOutliveTheDeadline(t *testing.T) {
	env := newFakeEnv()
	opts := testOptions(env)
	opts.Timeout = 200 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := luacmd.Run(context.Background(), `
pcall(function()
  while true do
    pcall(function() while true do end end)
  end
end)
return "survived"
`, opts)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a script wrapped entirely in pcall outlived its deadline and returned normally")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("stopped with %v, want a deadline", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a script wrapped entirely in pcall is still running well past its 200ms deadline")
	}
}

func TestPcallStillWorksForOrdinaryErrorHandling(t *testing.T) {
	result := mustRun(t, newFakeEnv(), `
local ok, err = pcall(function() error("deliberate") end)
return {fields = {caught = tostring(not ok)}, narrative = tostring(err)}
`)
	if result.Fields["caught"] != "true" {
		t.Errorf("pcall did not catch a raised error: %+v", result)
	}
	if !strings.Contains(result.Narrative, "deliberate") {
		t.Errorf("pcall lost the error message: %q", result.Narrative)
	}
}

func TestAScriptErrorIsReportedAsOne(t *testing.T) {
	_, err := run(t, newFakeEnv(), `error("inner refused: e2e-fail")`)
	if err == nil {
		t.Fatal("Run returned no error for a script that raised")
	}
	if !strings.Contains(err.Error(), "inner refused: e2e-fail") {
		t.Errorf("error %q does not carry the script's own message", err)
	}
}

func TestRunRefusesADispatchThatIsAlreadyTooDeep(t *testing.T) {
	env := newFakeEnv()
	opts := testOptions(env)
	opts.Depth = 4
	opts.MaxDepth = 3

	_, err := luacmd.Run(context.Background(), `return "never runs"`, opts)
	if !errors.Is(err, luacmd.ErrTooDeep) {
		t.Fatalf("Run returned %v, want ErrTooDeep", err)
	}
	if len(env.submitted()) != 0 || len(env.narratives()) != 0 {
		t.Error("a refused run still touched the cluster")
	}
}

func TestASubmitThatWouldExceedTheDepthLimitIsRefused(t *testing.T) {
	env := newFakeEnv()
	opts := testOptions(env)
	opts.Depth = 3
	opts.MaxDepth = 3

	_, err := luacmd.Run(context.Background(), `return kv.submit("inner")`, opts)
	if err == nil {
		t.Fatal("a script at the depth limit still dispatched")
	}
	if !strings.Contains(err.Error(), "too deep") {
		t.Errorf("error %q does not say why", err)
	}
	if len(env.submitted()) != 0 {
		t.Error("the dispatch happened anyway")
	}
}

func TestTheCapsAreEnforced(t *testing.T) {
	t.Run("submits", func(t *testing.T) {
		env := newFakeEnv()
		opts := testOptions(env)
		opts.MaxSubmits = 2
		_, err := luacmd.Run(context.Background(), `
for i = 1, 5 do kv.submit("inner") end
`, opts)
		if err == nil {
			t.Fatal("a script dispatched past its cap")
		}
		if got := len(env.submitted()); got != 2 {
			t.Errorf("%d dispatches got through a cap of 2", got)
		}
	})

	t.Run("log lines", func(t *testing.T) {
		env := newFakeEnv()
		opts := testOptions(env)
		opts.MaxLogLines = 3
		_, err := luacmd.Run(context.Background(), `
for i = 1, 50 do kv.log("line " .. i) end
`, opts)
		if err == nil {
			t.Fatal("a script logged past its cap")
		}
		if got := len(env.narratives()); got != 3 {
			t.Errorf("%d lines got through a cap of 3", got)
		}
	})

	t.Run("result size", func(t *testing.T) {
		env := newFakeEnv()
		opts := testOptions(env)
		opts.MaxResultBytes = 64
		_, err := luacmd.Run(context.Background(), `return {narrative = string.rep("x", 500)}`, opts)
		if err == nil {
			t.Fatal("an oversized result was accepted")
		}
		if !strings.Contains(err.Error(), "over the 64 byte limit") {
			t.Errorf("error %q does not say what the limit was", err)
		}
	})
}

func TestValuesThatCannotCrossAreRefusedClearly(t *testing.T) {
	cases := []struct {
		name string
		code string
		want string
	}{
		{"self-referential inputs", `local t = {} t.self = t return kv.submit("inner", t)`, "refers to itself"},
		{"a function as an input", `return kv.submit("inner", {f = function() end})`, "cannot convert"},
		{"a nested field", `return {fields = {nested = {1, 2}}}`, "fields hold only"},
		{"a non-string field name", `return {fields = {[1] = "x"}}`, "field names must be strings"},
		{"inputs that are not named values", `return kv.submit("inner", {1, 2, 3})`, "named values"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := run(t, newFakeEnv(), tc.code)
			if err == nil {
				t.Fatalf("accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestAFailingEnvFailsTheRunRatherThanBeingSwallowed(t *testing.T) {
	env := newFakeEnv()
	env.failEvery = true
	_, err := run(t, env, `kv.log("this cannot be written") return "finished anyway"`)
	if err == nil {
		t.Fatal("Run succeeded even though its log write failed")
	}
	if !strings.Contains(err.Error(), "progress is unavailable") {
		t.Errorf("error %q does not carry the underlying failure", err)
	}
}

// The scenario the two-device optical test drives, run here against a fake
// cluster: outer logs, dispatches inner, folds inner's log into its own,
// and ends -- with success and failure decided only by the inputs. Proving
// it at this level is what stops the rig from being where these scripts
// are debugged.
func TestOuterInnerChain(t *testing.T) {
	const innerScript = `
local who = kv.inputs.who or "nobody"
kv.log("hello from inner: " .. who)
if kv.inputs.mode == "fail" then
  error("inner refused: " .. who)
end
return {fields = {status = "ok"}, narrative = "hello from inner: " .. who}
`
	const outerScript = `
kv.log("hello from outer begin")

local id, res = kv.run("inner", {who = kv.inputs.who, mode = kv.inputs.mode}, 5)
for _, r in ipairs(kv.logs(id)) do
  kv.log("inner[" .. id .. "] " .. (r.narrative or ""), {child_instance = id})
end

if res.status ~= "ok" then
  return {
    fields = {status = "error", child_instance = id},
    narrative = "hello from outer failed: " .. (res.narrative or ""),
  }
end

return {
  fields = {status = "ok", child_instance = id},
  narrative = "hello from outer end",
}
`

	for _, tc := range []struct {
		mode          string
		wantStatus    string
		wantNarrative string
		wantInLog     string
	}{
		{"ok", "ok", "hello from outer end", "hello from inner: e2e-ok"},
		// The child's message arrives with gopher-lua's own position
		// prefix ("<string>:5: inner refused: ..."), which is worth
		// keeping: it is what tells whoever wrote the script where it
		// stopped.
		{"fail", "error", "hello from outer failed: <string>:5: inner refused: e2e-fail", "hello from inner: e2e-fail"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			who := "e2e-" + tc.mode
			env := newFakeEnv()

			// Submitting "inner" runs the inner script for real, against
			// its own env, and files the result as that instance's log --
			// which is what a device serving both commands actually does.
			env.submitFn = func(ctx context.Context, commandID, inputsJSON string) (string, error) {
				if commandID != "inner" {
					return "", fmt.Errorf("unexpected command %q", commandID)
				}
				instanceID := "inner-instance"
				innerEnv := newFakeEnv()
				innerOpts := testOptions(innerEnv)
				innerOpts.CommandID = "inner"
				innerOpts.InstanceID = instanceID
				innerOpts.Inputs = inputsJSON
				innerOpts.Depth = luacmd.DepthOf(inputsJSON)

				entries := []luacmd.LogEntry{}
				result, err := luacmd.Run(ctx, innerScript, innerOpts)
				for _, line := range innerEnv.lines {
					line.InstanceID = instanceID
					line.Fields = map[string]string{"status": luacmd.StatusRunning}
					entries = append(entries, line)
				}
				if err != nil {
					entries = append(entries, luacmd.LogEntry{
						InstanceID: instanceID,
						Fields:     map[string]string{"status": "error"},
						Narrative:  errorNarrative(err),
					})
				} else {
					entries = append(entries, luacmd.LogEntry{
						InstanceID: instanceID,
						Fields:     result.Fields,
						Narrative:  result.Narrative,
					})
				}
				env.setLog(instanceID, entries...)
				return instanceID, nil
			}

			opts := testOptions(env)
			opts.Inputs = fmt.Sprintf(`{"who":%q,"mode":%q}`, who, tc.mode)
			result, err := luacmd.Run(context.Background(), outerScript, opts)
			if err != nil {
				t.Fatalf("outer failed as a run, which it must not: %v", err)
			}

			if result.Fields["status"] != tc.wantStatus {
				t.Errorf("status = %q, want %q", result.Fields["status"], tc.wantStatus)
			}
			if result.Narrative != tc.wantNarrative {
				t.Errorf("narrative = %q, want %q", result.Narrative, tc.wantNarrative)
			}
			if result.Fields["child_instance"] != "inner-instance" {
				t.Errorf("the child's execution id is not in the result: %v", result.Fields)
			}

			lines := strings.Join(env.narratives(), "\n")
			if !strings.Contains(lines, "hello from outer begin") {
				t.Errorf("outer's own opening line is missing:\n%s", lines)
			}
			if !strings.Contains(lines, tc.wantInLog) {
				t.Errorf("inner's line was not folded into outer's log:\n%s", lines)
			}
			if !strings.Contains(lines, "inner-instance") {
				t.Errorf("the child's execution id never reached outer's log:\n%s", lines)
			}
		})
	}
}

// errorNarrative is what a runner records for a script that raised: the
// Lua message alone. Taking err.Error() instead would file gopher-lua's
// whole stack traceback as the narrative -- which is exactly why Run
// separates the two (see ScriptError).
func errorNarrative(err error) string {
	var scriptErr *luacmd.ScriptError
	if errors.As(err, &scriptErr) {
		return scriptErr.Message
	}
	return err.Error()
}
