package luacmd_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
)

const selfPeer = "12D3KooWThisDevice"

// fakeCluster is the node a Runner serves: a catalog, a request queue per
// command, and a log per instance.
//
// Submit appends a real request to that queue, which is what makes
// TestAScriptCanDispatchIntoItsOwnRunner a genuine test of the deadlock
// this package's own dispatch loop exists to avoid -- the child a script
// waits for has to be picked up by the very runner whose script is
// waiting.
type fakeCluster struct {
	mu       sync.Mutex
	commands []luacmd.Command
	requests map[string][]luacmd.Request
	logs     map[string][]luacmd.LogEntry
	next     int
	// queryFn, when set, answers QueryLog instead of the stored entries --
	// for a test that needs a read to fail.
	queryFn func(context.Context, string) ([]luacmd.LogEntry, error)

	// live counts runs between their script's own "begin" and "end"
	// lines, and maxLive the high-water mark -- how the concurrency cap is
	// observed without reaching inside the runner.
	live, maxLive int
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		requests: map[string][]luacmd.Request{},
		logs:     map[string][]luacmd.LogEntry{},
	}
}

func (c *fakeCluster) SelfPeerID(context.Context) (string, error) { return selfPeer, nil }

func (c *fakeCluster) ListCommands(context.Context) ([]luacmd.Command, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]luacmd.Command{}, c.commands...), nil
}

func (c *fakeCluster) ListRequests(_ context.Context, commandID string) ([]luacmd.Request, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]luacmd.Request{}, c.requests[commandID]...), nil
}

func (c *fakeCluster) QueryLog(ctx context.Context, instanceID string) ([]luacmd.LogEntry, error) {
	if c.queryFn != nil {
		return c.queryFn(ctx, instanceID)
	}
	return c.QueryLogDirect(instanceID), nil
}

func (c *fakeCluster) Progress(_ context.Context, _, instanceID string, fields map[string]string, narrative string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch narrative {
	case "begin":
		c.live++
		if c.live > c.maxLive {
			c.maxLive = c.live
		}
	case "end":
		c.live--
	}
	stamped := map[string]string{"status": luacmd.StatusRunning}
	for k, v := range fields {
		stamped[k] = v
	}
	c.logs[instanceID] = append(c.logs[instanceID], luacmd.LogEntry{
		InstanceID: instanceID, Fields: stamped, Narrative: narrative, Timestamp: time.Now(),
	})
	return nil
}

func (c *fakeCluster) Append(_ context.Context, _, instanceID string, fields map[string]string, narrative string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs[instanceID] = append(c.logs[instanceID], luacmd.LogEntry{
		InstanceID: instanceID, Fields: fields, Narrative: narrative, Timestamp: time.Now(),
	})
	return nil
}

func (c *fakeCluster) Submit(_ context.Context, commandID, inputsJSON string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	instanceID := fmt.Sprintf("inst-%d", c.next)
	c.requests[commandID] = append(c.requests[commandID], luacmd.Request{
		InstanceID: instanceID, CommandID: commandID, RequestedBy: selfPeer,
		Inputs: inputsJSON, RequestedAt: time.Now(),
	})
	return instanceID, nil
}

// addCommand registers a Lua command for a script already in the journal.
func (c *fakeCluster) addCommand(id string, spec luacmd.Spec) {
	encoded, err := spec.Encode()
	if err != nil {
		panic(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = append(c.commands, luacmd.Command{ID: id, Name: id, PeerID: selfPeer, Spec: encoded})
}

func (c *fakeCluster) addRawCommand(cmd luacmd.Command) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = append(c.commands, cmd)
}

// request queues a dispatch the way a person tapping submit would.
func (c *fakeCluster) request(commandID, inputs string) string {
	instanceID, err := c.Submit(context.Background(), commandID, inputs)
	if err != nil {
		panic(err)
	}
	return instanceID
}

// QueryLogDirect reads the stored entries without going through queryFn --
// what a queryFn override calls once it has decided to answer normally.
func (c *fakeCluster) QueryLogDirect(instanceID string) []luacmd.LogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]luacmd.LogEntry{}, c.logs[instanceID]...)
}

func (c *fakeCluster) entries(instanceID string) []luacmd.LogEntry {
	entries, _ := c.QueryLog(context.Background(), instanceID)
	return entries
}

func (c *fakeCluster) narrativesFor(instanceID string) string {
	var lines []string
	for _, e := range c.entries(instanceID) {
		lines = append(lines, e.Narrative)
	}
	return strings.Join(lines, "\n")
}

// terminal returns instanceID's final entry, or false while it is still
// running or has none.
func (c *fakeCluster) terminal(instanceID string) (luacmd.LogEntry, bool) {
	entries := c.entries(instanceID)
	if len(entries) == 0 {
		return luacmd.LogEntry{}, false
	}
	last := entries[len(entries)-1]
	return last, last.Done()
}

func (c *fakeCluster) highWater() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxLive
}

// recordingListener collects what a UI would have been shown.
type recordingListener struct {
	mu       sync.Mutex
	starts   []string
	lines    []string
	finishes []string
	errors   []string
}

func (l *recordingListener) OnStart(commandID, instanceID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.starts = append(l.starts, commandID+"/"+instanceID)
}

func (l *recordingListener) OnLog(commandID, instanceID, narrative string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, commandID+"/"+instanceID+"/"+narrative)
}

func (l *recordingListener) OnFinish(commandID, instanceID, status, narrative string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishes = append(l.finishes, commandID+"/"+instanceID+"/"+status+"/"+narrative)
}

func (l *recordingListener) OnError(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, message)
}

func (l *recordingListener) snapshot() ([]string, []string, []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.starts...), append([]string{}, l.finishes...), append([]string{}, l.errors...)
}

func (l *recordingListener) liveLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.lines...)
}

// newRig wires a journal, a catalog, a fake cluster and a runner together.
func newRig(t *testing.T, opts luacmd.ServeOptions) (*fakeCluster, *luacmd.Catalog, *luacmd.Runner) {
	t.Helper()
	cluster := newFakeCluster()
	scripts := luacmd.NewCatalog(luacmd.Memory(), selfPeer)
	if opts.Run.PollInterval == 0 {
		opts.Run.PollInterval = 5 * time.Millisecond
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.Interval == 0 {
		opts.Interval = 10 * time.Millisecond
	}
	return cluster, scripts, luacmd.NewRunner(cluster, scripts, opts)
}

// register puts a script in the journal and registers a command pinning
// the hash it just got -- what Phase 4's CLI and Phase 5's bindings do in
// one call.
func register(t *testing.T, cluster *fakeCluster, scripts *luacmd.Catalog, id, code string) luacmd.Script {
	t.Helper()
	stored, err := scripts.Put(context.Background(), luacmd.Script{ID: id, Name: id, Code: code})
	if err != nil {
		t.Fatalf("Put(%s): %v", id, err)
	}
	cluster.addCommand(id, luacmd.NewSpec(stored))
	return stored
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSpecRoundTripsAndIgnoresWhatIsNotOurs(t *testing.T) {
	encoded, err := luacmd.Spec{Runtime: "lua", ScriptID: "outer", SHA256: "abc", TimeoutSeconds: 30}.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	parsed, ok := luacmd.ParseSpec(encoded)
	if !ok || parsed.ScriptID != "outer" || parsed.SHA256 != "abc" || parsed.TimeoutSeconds != 30 {
		t.Fatalf("ParseSpec(%s) = %+v, %v", encoded, parsed, ok)
	}

	for _, spec := range []string{"", "not json", `{"runtime":"wasm","script_id":"x"}`, `{"runtime":"lua"}`} {
		if _, ok := luacmd.ParseSpec(spec); ok {
			t.Errorf("ParseSpec claimed %q", spec)
		}
	}
}

func TestOnceRunsAPendingRequestAndRecordsTheResult(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	register(t, cluster, scripts, "hello", `
kv.log("hello from outer begin")
return {fields = {status = "ok"}, narrative = "hello from outer end"}
`)
	instanceID := cluster.request("hello", "")

	runner.Once(ctx)
	waitFor(t, "the run to finish", func() bool { _, done := cluster.terminal(instanceID); return done })

	last, _ := cluster.terminal(instanceID)
	if last.Fields["status"] != "ok" || last.Narrative != "hello from outer end" {
		t.Errorf("terminal entry = %+v", last)
	}
	if !strings.Contains(cluster.narrativesFor(instanceID), "hello from outer begin") {
		t.Errorf("the script's live line is missing:\n%s", cluster.narrativesFor(instanceID))
	}
	// The claim has to come first, so a person watching sees the run start.
	if first := cluster.entries(instanceID)[0]; first.Fields["status"] != luacmd.StatusRunning {
		t.Errorf("first entry is %+v, want a running claim", first)
	}
}

func TestARequestThatAlreadyFinishedIsLeftAlone(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	register(t, cluster, scripts, "hello", `kv.log("ran") return "done"`)
	instanceID := cluster.request("hello", "")

	if err := cluster.Append(ctx, selfPeer, instanceID, map[string]string{"status": "ok"}, "already handled"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	runner.Once(ctx)
	time.Sleep(50 * time.Millisecond)

	if got := cluster.narrativesFor(instanceID); strings.Contains(got, "ran") {
		t.Errorf("the script ran again for an instance that already had a result:\n%s", got)
	}
}

// A process that died mid-run leaves a StatusRunning entry behind, and the
// convention every dispatcher in this repo shares is that such an instance
// is retryable. This is that case: nothing is in flight here, so the
// entry belongs to somebody who is gone.
func TestAnInterruptedRunIsPickedUpAgain(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	register(t, cluster, scripts, "hello", `kv.log("ran") return "done"`)
	instanceID := cluster.request("hello", "")

	if err := cluster.Progress(ctx, selfPeer, instanceID, nil, "started by a process that then died"); err != nil {
		t.Fatalf("Progress: %v", err)
	}
	runner.Once(ctx)
	waitFor(t, "the retry to finish", func() bool { _, done := cluster.terminal(instanceID); return done })

	if got := cluster.narrativesFor(instanceID); !strings.Contains(got, "ran") {
		t.Errorf("an interrupted run was never retried:\n%s", got)
	}
}

// The other half of that rule: this runner's *own* claim looks exactly
// like a dead process's, so without remembering what it is running, the
// very next pass would start the same script again.
func TestARunInProgressIsNotStartedTwice(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	register(t, cluster, scripts, "slow", `
kv.log("ran")
kv.sleep(0.25)
return "done"
`)
	instanceID := cluster.request("slow", "")

	for i := 0; i < 5; i++ {
		runner.Once(ctx)
		time.Sleep(10 * time.Millisecond)
	}
	waitFor(t, "the run to finish", func() bool { _, done := cluster.terminal(instanceID); return done })

	if got := strings.Count(cluster.narrativesFor(instanceID), "ran"); got != 1 {
		t.Errorf("the script ran %d times across repeated passes, want 1:\n%s", got, cluster.narrativesFor(instanceID))
	}
}

// The whole reason this package has its own dispatch loop: outer waits for
// inner, and inner can only run if the loop that must notice it is not the
// one being blocked.
//
// Confirmed to have teeth rather than assumed: with maybeStart changed to
// call serveOne synchronously -- the shape pkg/kvctl and mobile/kvmobile's
// dispatchers have -- this fails with outer recorded as
// "script stopped after 5s: context deadline exceeded", which is the
// deadlock itself.
func TestAScriptCanDispatchIntoItsOwnRunner(t *testing.T) {
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

local id, res = kv.run("inner", {who = kv.inputs.who, mode = kv.inputs.mode}, 8)
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
		{"fail", "error", "hello from outer failed: ", "hello from inner: e2e-fail"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
			register(t, cluster, scripts, "inner", innerScript)
			register(t, cluster, scripts, "outer", outerScript)

			done := make(chan struct{})
			go func() { defer close(done); runner.Serve(ctx) }()

			who := "e2e-" + tc.mode
			outerInstance := cluster.request("outer", fmt.Sprintf(`{"who":%q,"mode":%q}`, who, tc.mode))
			waitFor(t, "outer to finish", func() bool { _, ok := cluster.terminal(outerInstance); return ok })

			last, _ := cluster.terminal(outerInstance)
			if last.Fields["status"] != tc.wantStatus {
				t.Errorf("outer status = %q, want %q (narrative %q)", last.Fields["status"], tc.wantStatus, last.Narrative)
			}
			if !strings.HasPrefix(last.Narrative, tc.wantNarrative) {
				t.Errorf("outer narrative = %q, want it to start with %q", last.Narrative, tc.wantNarrative)
			}

			childInstance := last.Fields["child_instance"]
			if childInstance == "" {
				t.Fatalf("outer's result does not name the child it ran: %+v", last)
			}
			outerLog := cluster.narrativesFor(outerInstance)
			if !strings.Contains(outerLog, tc.wantInLog) {
				t.Errorf("inner's line was not folded into outer's log:\n%s", outerLog)
			}
			if !strings.Contains(outerLog, childInstance) {
				t.Errorf("the child's execution id never reached outer's log:\n%s", outerLog)
			}
			if inner := cluster.narrativesFor(childInstance); !strings.Contains(inner, tc.wantInLog) {
				t.Errorf("inner's own log does not have its line:\n%s", inner)
			}

			cancel()
			<-done
		})
	}
}

// The hash is what makes journal access insufficient to change what an
// approved command does, so the refusal has to be visible rather than a
// silent skip.
func TestAScriptThatDoesNotMatchItsPinnedHashIsRefused(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	stored := register(t, cluster, scripts, "hello", `kv.log("original") return "done"`)

	// Somebody with journal access rewrites the script; the Command still
	// pins what a voter registered.
	if _, err := scripts.Put(ctx, luacmd.Script{ID: "hello", Name: "hello", Code: `kv.log("substituted") return "done"`}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	instanceID := cluster.request("hello", "")

	runner.Once(ctx)
	waitFor(t, "the refusal to be recorded", func() bool { _, done := cluster.terminal(instanceID); return done })

	last, _ := cluster.terminal(instanceID)
	if last.Fields["status"] != "error" {
		t.Errorf("terminal entry = %+v, want an error", last)
	}
	if !strings.Contains(last.Narrative, stored.SHA256) {
		t.Errorf("the refusal does not say what was pinned: %q", last.Narrative)
	}
	if got := cluster.narrativesFor(instanceID); strings.Contains(got, "substituted") {
		t.Error("the substituted script ran")
	}
}

func TestAMissingScriptIsRecordedRatherThanRetriedForever(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	register(t, cluster, scripts, "hello", `return "done"`)
	if err := scripts.Delete(ctx, "hello"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	instanceID := cluster.request("hello", "")

	runner.Once(ctx)
	waitFor(t, "the failure to be recorded", func() bool { _, done := cluster.terminal(instanceID); return done })

	last, _ := cluster.terminal(instanceID)
	if last.Fields["status"] != "error" || !strings.Contains(last.Narrative, "not in the journal") {
		t.Errorf("terminal entry = %+v", last)
	}
}

func TestARunnerOnlyServesItsOwnLuaCommands(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	stored, err := scripts.Put(ctx, luacmd.Script{ID: "hello", Name: "hello", Code: `kv.log("ran") return "done"`})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	elsewhere, err := luacmd.NewSpec(stored).Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	cluster.addRawCommand(luacmd.Command{ID: "other-device", PeerID: "12D3KooWSomeoneElse", Spec: elsewhere})
	cluster.addRawCommand(luacmd.Command{ID: "plain", PeerID: selfPeer, Spec: ""})
	cluster.addRawCommand(luacmd.Command{ID: "wasm", PeerID: selfPeer, Spec: `{"runtime":"wasm","script_id":"hello"}`})

	other := cluster.request("other-device", "")
	plain := cluster.request("plain", "")
	wasm := cluster.request("wasm", "")

	runner.Once(ctx)
	time.Sleep(80 * time.Millisecond)

	for name, instanceID := range map[string]string{"another device's": other, "a non-Lua": plain, "another runtime's": wasm} {
		if entries := cluster.entries(instanceID); len(entries) != 0 {
			t.Errorf("the runner touched %s command: %+v", name, entries)
		}
	}
}

func TestAScriptFailureIsRecordedWithItsMessageAndTraceback(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	register(t, cluster, scripts, "boom", `error("inner refused: e2e-fail")`)
	instanceID := cluster.request("boom", "")

	runner.Once(ctx)
	waitFor(t, "the failure to be recorded", func() bool { _, done := cluster.terminal(instanceID); return done })

	last, _ := cluster.terminal(instanceID)
	if last.Fields["status"] != "error" {
		t.Errorf("status = %q, want error", last.Fields["status"])
	}
	if !strings.Contains(last.Narrative, "inner refused: e2e-fail") {
		t.Errorf("narrative = %q, want the script's own message", last.Narrative)
	}
	if strings.Contains(last.Narrative, "stack traceback") {
		t.Errorf("narrative carries the traceback, which belongs in a field: %q", last.Narrative)
	}
	if !strings.Contains(last.Fields["traceback"], "stack traceback") {
		t.Errorf("the traceback was dropped entirely: %v", last.Fields)
	}
}

func TestConcurrencyIsCapped(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{Concurrency: 2})
	register(t, cluster, scripts, "slow", `
kv.log("begin")
kv.sleep(0.15)
kv.log("end")
return "done"
`)
	var instances []string
	for i := 0; i < 5; i++ {
		instances = append(instances, cluster.request("slow", ""))
	}

	runner.Once(ctx)
	waitFor(t, "all five runs to finish", func() bool {
		for _, id := range instances {
			if _, done := cluster.terminal(id); !done {
				return false
			}
		}
		return true
	})

	if got := cluster.highWater(); got > 2 {
		t.Errorf("%d scripts ran at once under a cap of 2", got)
	}
	if got := cluster.highWater(); got < 2 {
		t.Errorf("high water mark was %d, so the runs never overlapped and this proves nothing about the cap", got)
	}
}

func TestServeWaitsForWhatItStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{})
	register(t, cluster, scripts, "slow", `
kv.log("begin")
kv.sleep(0.2)
return "done"
`)
	instanceID := cluster.request("slow", "")

	done := make(chan struct{})
	go func() { defer close(done); runner.Serve(ctx) }()

	waitFor(t, "the run to start", func() bool { return strings.Contains(cluster.narrativesFor(instanceID), "begin") })
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve never returned after its context was cancelled")
	}

	// Whatever the outcome, a terminal entry must exist: a stopped runner
	// that left an instance mid-run would have it retried forever.
	if _, ok := cluster.terminal(instanceID); !ok {
		t.Errorf("a run interrupted by shutdown left no terminal entry:\n%s", cluster.narrativesFor(instanceID))
	}
}

func TestTheListenerSeesEveryRun(t *testing.T) {
	ctx := context.Background()
	listener := &recordingListener{}
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{Listener: listener})
	register(t, cluster, scripts, "hello", `
kv.log("hello from outer begin")
return {fields = {status = "ok"}, narrative = "hello from outer end"}
`)
	instanceID := cluster.request("hello", "")

	runner.Once(ctx)
	waitFor(t, "the run to finish", func() bool { _, done := cluster.terminal(instanceID); return done })

	starts, finishes, errs := listener.snapshot()
	if len(starts) != 1 || !strings.Contains(starts[0], instanceID) {
		t.Errorf("starts = %v", starts)
	}
	if len(finishes) != 1 || !strings.Contains(finishes[0], "ok") || !strings.Contains(finishes[0], "hello from outer end") {
		t.Errorf("finishes = %v", finishes)
	}
	if len(errs) != 0 {
		t.Errorf("errors = %v", errs)
	}

	// A device that hosts a command shows its lines as they are written;
	// only a device that merely submitted one has to watch the log. The
	// runner's own claim entry is not one of them -- that is bookkeeping,
	// already reported as OnStart, and a UI showing it as a script line
	// would be showing something nobody wrote.
	lines := listener.liveLines()
	if len(lines) != 1 || !strings.Contains(lines[0], "hello from outer begin") {
		t.Errorf("live lines = %v, want exactly the script's own line", lines)
	}
}

func TestASpecCanGiveItsOwnCommandALongerDeadline(t *testing.T) {
	ctx := context.Background()
	cluster, scripts, runner := newRig(t, luacmd.ServeOptions{Timeout: 50 * time.Millisecond})
	stored, err := scripts.Put(ctx, luacmd.Script{ID: "slow", Name: "slow", Code: `
kv.sleep(0.2)
return "finished anyway"
`})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	spec := luacmd.NewSpec(stored)
	spec.TimeoutSeconds = 5
	cluster.addCommand("slow", spec)
	instanceID := cluster.request("slow", "")

	runner.Once(ctx)
	waitFor(t, "the run to finish", func() bool { _, done := cluster.terminal(instanceID); return done })

	last, _ := cluster.terminal(instanceID)
	if last.Narrative != "finished anyway" {
		t.Errorf("terminal entry = %+v, want the run to have outlived the runner's own 50ms default", last)
	}
}
