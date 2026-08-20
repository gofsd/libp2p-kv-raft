package kvmobile

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
)

// recordingLuaListener is a Go-side LuaListener for tests -- Kotlin would
// implement the same interface through gomobile's reverse binding; from Go
// it is an ordinary type to satisfy, the same way recordingCronListener
// and recordingDispatchHandler are.
type recordingLuaListener struct {
	mu       sync.Mutex
	starts   []string
	lines    []string
	finishes []string
	errs     []string
}

func (l *recordingLuaListener) OnStart(commandID, instanceID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.starts = append(l.starts, commandID+" "+instanceID)
}

func (l *recordingLuaListener) OnLog(commandID, instanceID, narrative string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, commandID+" "+narrative)
}

func (l *recordingLuaListener) OnFinish(commandID, instanceID, status, narrative string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishes = append(l.finishes, commandID+" "+status+" "+narrative)
}

func (l *recordingLuaListener) OnError(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, message)
}

func (l *recordingLuaListener) snapshot() (starts, lines, finishes []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.starts...), append([]string{}, l.lines...), append([]string{}, l.finishes...)
}

const (
	luaInnerScript = `
local who = kv.inputs.who or "nobody"
kv.log("hello from inner: " .. who)
if kv.inputs.mode == "fail" then
  error("inner refused: " .. who)
end
return {fields = {status = "ok"}, narrative = "hello from inner: " .. who}
`
	luaOuterScript = `
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

// TestLuaCommandChainFromADevice drives the whole thing the way the app
// will: this device creates two Lua commands in one group, runs the
// runner, and submits the outer one -- which dispatches the inner one back
// into this same device, waits for it, folds its log into its own, and
// finishes. Success and failure are decided only by the inputs.
//
// This is the in-process counterpart of the two-device optical scenario,
// and the one place the mobile adapter is exercised against a real daemon
// rather than a fake: the catalog scan, the request queue, the command log
// and the journal are all the real ones here.
func TestLuaCommandChainFromADevice(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	selfPeerID := PeerID()

	const groupID = "grp-lua"
	if err := LuaCreateCommand("inner", "Inner", "", luaInnerScript); err != nil {
		t.Fatalf("LuaCreateCommand(inner): %v", err)
	}
	if err := LuaCreateCommand("outer", "Outer", "", luaOuterScript); err != nil {
		t.Fatalf("LuaCreateCommand(outer): %v", err)
	}
	// One group for both, and this device in it -- the ordinary
	// public/private group model, with nothing Lua-specific about it.
	grantCommandAccess(t, "outer", groupID, selfPeerID)
	if err := AddCommandToGroup("inner", groupID); err != nil {
		t.Fatalf("AddCommandToGroup(inner): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand("inner")
		return err == nil, nil
	})

	listener := &recordingLuaListener{}
	if err := LuaServeWithListener(1, 2, listener); err != nil {
		t.Fatalf("LuaServeWithListener: %v", err)
	}
	t.Cleanup(StopLuaServe)
	if !LuaServing() {
		t.Fatal("LuaServing reported no runner after starting one")
	}

	t.Run("success", func(t *testing.T) {
		instanceID, err := LuaRun("outer", `{"who":"e2e-ok","mode":"ok"}`)
		if err != nil {
			t.Fatalf("LuaRun: %v", err)
		}
		entry := awaitLuaTerminal(t, instanceID)
		if entry.Fields["status"] != "ok" {
			t.Errorf("status = %q, want ok (narrative %q)", entry.Fields["status"], entry.Narrative)
		}
		if entry.Narrative != "hello from outer end" {
			t.Errorf("narrative = %q", entry.Narrative)
		}
		if entry.Fields["child_instance"] == "" {
			t.Errorf("the result does not name the child it ran: %+v", entry.Fields)
		}

		log := luaNarratives(t, instanceID)
		if !strings.Contains(log, "hello from outer begin") {
			t.Errorf("outer's own opening line is missing:\n%s", log)
		}
		if !strings.Contains(log, "hello from inner: e2e-ok") {
			t.Errorf("inner's line was not folded into outer's log:\n%s", log)
		}
		if !strings.Contains(log, entry.Fields["child_instance"]) {
			t.Errorf("the child's execution id never reached outer's log:\n%s", log)
		}
	})

	t.Run("failure", func(t *testing.T) {
		instanceID, err := LuaRun("outer", `{"who":"e2e-fail","mode":"fail"}`)
		if err != nil {
			t.Fatalf("LuaRun: %v", err)
		}
		entry := awaitLuaTerminal(t, instanceID)
		if entry.Fields["status"] != "error" {
			t.Errorf("status = %q, want error (narrative %q)", entry.Fields["status"], entry.Narrative)
		}
		if !strings.Contains(entry.Narrative, "hello from outer failed") {
			t.Errorf("narrative = %q", entry.Narrative)
		}
		if !strings.Contains(entry.Narrative, "inner refused: e2e-fail") {
			t.Errorf("the child's own message did not reach the parent's result: %q", entry.Narrative)
		}
	})

	// LuaLastLog is what the optical rig reads, since it cannot feed an
	// instance id back into a generated code.
	raw, err := LuaLastLog("outer")
	if err != nil {
		t.Fatalf("LuaLastLog: %v", err)
	}
	var last struct {
		InstanceID string            `json:"instance_id"`
		Entries    []luacmd.LogEntry `json:"entries"`
	}
	if err := json.Unmarshal([]byte(raw), &last); err != nil {
		t.Fatalf("LuaLastLog returned %q: %v", raw, err)
	}
	if last.InstanceID == "" || len(last.Entries) == 0 {
		t.Fatalf("LuaLastLog = %+v, want the most recent run and its entries", last)
	}
	if !strings.Contains(raw, "hello from inner: e2e-fail") {
		t.Errorf("LuaLastLog does not carry the run's folded-in child line:\n%s", raw)
	}

	starts, lines, finishes := listener.snapshot()
	if len(starts) < 4 {
		t.Errorf("listener saw %d starts, want at least 4 (two outers and their two inners)", len(starts))
	}
	if !luaAnyContains(lines, "hello from outer begin") {
		t.Errorf("listener never saw a live line: %v", lines)
	}
	if !luaAnyContains(finishes, "hello from outer end") {
		t.Errorf("listener never saw the successful finish: %v", finishes)
	}

	StopLuaServe()
	if LuaServing() {
		t.Fatal("LuaServing still reported a runner after StopLuaServe")
	}
}

// TestLuaScriptsAreVersionedAndRefusedWhenBroken covers the editor's own
// path: a syntax error is caught while somebody is looking at it, and a
// second put keeps the first readable.
func TestLuaScriptsAreVersionedAndRefusedWhenBroken(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := LuaPut("draft", "Draft", `if then end`); err == nil {
		t.Fatal("LuaPut accepted a script that does not compile")
	}
	if err := LuaPut("draft", "Draft", `return {narrative = "v1"}`); err != nil {
		t.Fatalf("LuaPut: %v", err)
	}
	if err := LuaPut("draft", "Draft", `return {narrative = "v2"}`); err != nil {
		t.Fatalf("LuaPut: %v", err)
	}

	raw, err := LuaGet("draft")
	if err != nil {
		t.Fatalf("LuaGet: %v", err)
	}
	var current luacmd.Script
	if err := json.Unmarshal([]byte(raw), &current); err != nil {
		t.Fatalf("LuaGet returned %q: %v", raw, err)
	}
	if !strings.Contains(current.Code, "v2") {
		t.Errorf("LuaGet returned %q, want the latest revision", current.Code)
	}

	raw, err = LuaHistory("draft")
	if err != nil {
		t.Fatalf("LuaHistory: %v", err)
	}
	var revisions []luacmd.Script
	if err := json.Unmarshal([]byte(raw), &revisions); err != nil {
		t.Fatalf("LuaHistory returned %q: %v", raw, err)
	}
	if len(revisions) != 2 {
		t.Fatalf("LuaHistory returned %d revisions, want 2", len(revisions))
	}

	if err := LuaDelete("draft"); err != nil {
		t.Fatalf("LuaDelete: %v", err)
	}
	if _, err := LuaGet("draft"); err == nil {
		t.Error("LuaGet still returned a deleted script")
	}
	raw, err = LuaList()
	if err != nil {
		t.Fatalf("LuaList: %v", err)
	}
	if strings.Contains(raw, `"draft"`) {
		t.Errorf("LuaList still lists a deleted script: %s", raw)
	}
}

// awaitLuaTerminal waits for instanceID's run to record a terminal entry
// and returns it.
func awaitLuaTerminal(t *testing.T, instanceID string) luacmd.LogEntry {
	t.Helper()
	var terminal luacmd.LogEntry
	pollUntilTrue(t, 60*time.Second, func() (bool, error) {
		raw, err := LuaLogs(instanceID)
		if err != nil {
			return false, err
		}
		var entries []luacmd.LogEntry
		if err := json.Unmarshal([]byte(raw), &entries); err != nil {
			return false, err
		}
		if len(entries) == 0 {
			return false, nil
		}
		last := entries[len(entries)-1]
		if !last.Done() {
			return false, nil
		}
		terminal = last
		return true, nil
	})
	return terminal
}

func luaNarratives(t *testing.T, instanceID string) string {
	t.Helper()
	raw, err := LuaLogs(instanceID)
	if err != nil {
		t.Fatalf("LuaLogs: %v", err)
	}
	var entries []luacmd.LogEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		t.Fatalf("LuaLogs returned %q: %v", raw, err)
	}
	var lines []string
	for _, entry := range entries {
		lines = append(lines, entry.Narrative)
	}
	return strings.Join(lines, "\n")
}

func luaAnyContains(lines []string, want string) bool {
	for _, line := range lines {
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}
