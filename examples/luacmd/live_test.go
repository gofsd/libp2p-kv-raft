package luacmd_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// repoRoot/killAllRegistered/fastRaftArgs are the same local copies of
// pkg/kvctl's test helpers examples/genealogy, examples/relations and
// examples/relations/journalcmd each carry, duplicated for the same
// reason: a worked example is not a consumer of another package's test
// infrastructure.

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func killAllRegistered(t *testing.T, reg *registry.Registry) {
	t.Helper()
	nodes, err := reg.List()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, info := range nodes {
		if info.PID == 0 {
			continue
		}
		proc, err := os.FindProcess(info.PID)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(proc *os.Process) {
			defer wg.Done()
			_ = proc.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() {
				proc.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = proc.Kill()
			}
		}(proc)
	}
	wg.Wait()
}

var fastRaftArgs = []string{
	"-raft-heartbeat-timeout", "300ms",
	"-raft-election-timeout", "300ms",
	"-raft-leader-lease-timeout", "250ms",
}

const (
	liveGroupID     = "lua-live"
	liveInnerScript = `
local who = kv.inputs.who or "nobody"
kv.log("hello from inner: " .. who)
if kv.inputs.mode == "fail" then
  error("inner refused: " .. who)
end
return {fields = {status = "ok"}, narrative = "hello from inner: " .. who}
`
	liveOuterScript = `
kv.log("hello from outer begin")

local id, res = kv.run("inner", {who = kv.inputs.who, mode = kv.inputs.mode}, 30)
for _, r in ipairs(kv.logs(id)) do
  kv.log("inner[" .. id .. "] " .. (r.narrative or ""), {child_instance = id})
end

if res.status ~= "ok" then
  return {fields = {status = "error", child_instance = id},
          narrative = "hello from outer failed: " .. (res.narrative or "")}
end
return {fields = {status = "ok", child_instance = id}, narrative = "hello from outer end"}
`
)

// TestLiveLuaChain is this package end to end against a real node: two
// scripts registered as ordinary catalog commands, a runner serving them,
// and a submitted command that dispatches the other one back into the same
// device, waits for it, folds its log into its own, and finishes.
//
// The same scenario the two-device optical rig drives, run here so that
// the rig confirms something already known to work rather than being where
// it is discovered. And the same scenario mobile/kvmobile's own
// TestLuaCommandChainFromADevice covers on the other client -- this is the
// desktop adapter's turn: pkg/kvctl for the catalog and the log,
// pkg/registry for the identity, a real daemon behind both.
//
// One process throughout, deliberately. pkg/ipc's request channel is
// single-in-flight across processes (its caller lock is Go-level only), so
// a runner and a separate CLI talking to one daemon starve each other --
// see the note at the top of mage_lua.go. Everything here shares one
// process, which is also the shape the Android app has.
func TestLiveLuaChain(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	t.Setenv(registry.EnvHome, t.TempDir())

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	peerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// The catalog: one group, this node in it, and the two scripts
	// registered against it. Nothing Lua-specific about the permission --
	// it is the ordinary group model, and the FSM is what enforces it.
	if err := kvctl.PutGroup(liveGroupID, "Lua live", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.AddPeerToGroup(peerID, liveGroupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}

	scripts, catalogPeerID, err := luacmd.CurrentCatalog()
	if err != nil {
		t.Fatalf("CurrentCatalog: %v", err)
	}
	if catalogPeerID != peerID {
		t.Fatalf("CurrentCatalog is bound to %s, want the node just created (%s)", catalogPeerID, peerID)
	}
	device := luacmd.Kvctl()
	for _, script := range []luacmd.Script{
		{ID: "inner", Name: "Inner", Code: liveInnerScript},
		{ID: "outer", Name: "Outer", Code: liveOuterScript},
	} {
		if _, err := luacmd.Register(ctx, scripts, device, peerID, script, liveGroupID); err != nil {
			t.Fatalf("Register(%s): %v", script.ID, err)
		}
	}
	pollUntilTrue(t, 15*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("inner")
		return err == nil, nil
	})

	runnerCtx, stop := context.WithCancel(ctx)
	defer stop()
	runner := luacmd.NewRunner(device, scripts, luacmd.ServeOptions{
		Interval: 300 * time.Millisecond,
		Listener: testListener{t},
	})
	done := make(chan struct{})
	go func() { defer close(done); runner.Serve(runnerCtx) }()
	t.Cleanup(func() {
		stop()
		<-done
	})

	for _, tc := range []struct {
		mode          string
		wantStatus    string
		wantNarrative string
	}{
		{"ok", "ok", "hello from outer end"},
		{"fail", "error", "hello from outer failed: "},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			who := "live-" + tc.mode
			instanceID, err := device.Submit(ctx, "outer", `{"who":"`+who+`","mode":"`+tc.mode+`"}`)
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}

			followCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			var lines []string
			last, err := luacmd.Follow(followCtx, device, instanceID, 300*time.Millisecond, func(entry luacmd.LogEntry) {
				lines = append(lines, entry.Narrative)
			})
			if err != nil {
				t.Fatalf("Follow: %v", err)
			}

			if last.Fields["status"] != tc.wantStatus {
				t.Errorf("status = %q, want %q (narrative %q)", last.Fields["status"], tc.wantStatus, last.Narrative)
			}
			if !strings.HasPrefix(last.Narrative, tc.wantNarrative) {
				t.Errorf("narrative = %q, want it to start with %q", last.Narrative, tc.wantNarrative)
			}

			childInstance := last.Fields["child_instance"]
			if childInstance == "" {
				t.Fatalf("the result does not name the child it ran: %+v", last.Fields)
			}
			log := strings.Join(lines, "\n")
			if !strings.Contains(log, "hello from outer begin") {
				t.Errorf("outer's own opening line is missing:\n%s", log)
			}
			if !strings.Contains(log, "hello from inner: "+who) {
				t.Errorf("inner's line was not folded into outer's log:\n%s", log)
			}
			if !strings.Contains(log, childInstance) {
				t.Errorf("the child's execution id never reached outer's log:\n%s", log)
			}

			// LastRun is what a caller with no instance id asks -- the
			// optical rig's own read-back, since it cannot feed one into a
			// generated code.
			gotID, entries, err := luacmd.LastRun(ctx, device, "outer")
			if err != nil {
				t.Fatalf("LastRun: %v", err)
			}
			if gotID != instanceID {
				t.Errorf("LastRun returned %s, want the run just submitted (%s)", gotID, instanceID)
			}
			if len(entries) == 0 {
				t.Error("LastRun returned no entries")
			}
		})
	}
}

// TestLiveRunnerRefusesASubstitutedScript is the hash pin doing its job
// against a real cluster: the journal is writable by any local caller, the
// Command that pins the hash is not, and a script swapped underneath an
// already-registered command must not run.
func TestLiveRunnerRefusesASubstitutedScript(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	t.Setenv(registry.EnvHome, t.TempDir())

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	peerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := kvctl.PutGroup(liveGroupID, "Lua live", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.AddPeerToGroup(peerID, liveGroupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}

	scripts, _, err := luacmd.CurrentCatalog()
	if err != nil {
		t.Fatalf("CurrentCatalog: %v", err)
	}
	device := luacmd.Kvctl()
	registered, err := luacmd.Register(ctx, scripts, device, peerID,
		luacmd.Script{ID: "pinned", Name: "Pinned", Code: `return {fields = {status = "ok"}, narrative = "the registered script"}`},
		liveGroupID)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	pollUntilTrue(t, 15*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("pinned")
		return err == nil, nil
	})

	// Somebody with nothing but local journal access rewrites the script.
	if _, err := scripts.Put(ctx, luacmd.Script{
		ID: "pinned", Name: "Pinned", Code: `return {fields = {status = "ok"}, narrative = "the substituted script"}`,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	runnerCtx, stop := context.WithCancel(ctx)
	defer stop()
	runner := luacmd.NewRunner(device, scripts, luacmd.ServeOptions{
		Interval: 300 * time.Millisecond,
		Listener: testListener{t},
	})
	done := make(chan struct{})
	go func() { defer close(done); runner.Serve(runnerCtx) }()
	t.Cleanup(func() {
		stop()
		<-done
	})

	instanceID, err := device.Submit(ctx, "pinned", "")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	followCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	last, err := luacmd.Follow(followCtx, device, instanceID, 300*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}

	if last.Fields["status"] != "error" {
		t.Fatalf("terminal entry = %+v, want a refusal", last)
	}
	if !strings.Contains(last.Narrative, registered.SHA256) {
		t.Errorf("the refusal does not name the hash the command pins: %q", last.Narrative)
	}
	if strings.Contains(last.Narrative, "substituted script") {
		t.Error("the substituted script ran")
	}
}

// testListener surfaces the runner's own errors in the test log, where a
// hang is otherwise hard to explain.
type testListener struct{ t *testing.T }

func (l testListener) OnStart(commandID, instanceID string) {
	l.t.Logf("runner: start %s %s", commandID, instanceID)
}

func (l testListener) OnLog(commandID, instanceID, narrative string) {
	l.t.Logf("runner: log %s: %s", commandID, narrative)
}

func (l testListener) OnFinish(commandID, instanceID, status, narrative string) {
	l.t.Logf("runner: finish %s %s: %s", commandID, status, narrative)
}

func (l testListener) OnError(message string) { l.t.Logf("runner: %s", message) }

func pollUntilTrue(t *testing.T, timeout time.Duration, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if ok {
			return
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("condition never became true within %s (last error: %v)", timeout, lastErr)
}
