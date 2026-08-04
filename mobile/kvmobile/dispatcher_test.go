package kvmobile

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

// recordingDispatchHandler is a Go-side CommandDispatchHandler
// implementation for tests -- Kotlin would implement the same interface
// via gomobile's reverse binding, but from Go the interface is just an
// ordinary type to satisfy (same reasoning recordingCallback's own doc
// comment in watch_execute_test.go gives).
type recordingDispatchHandler struct {
	calls     int32
	panicOn   string // Inputs value that triggers a panic, if non-empty
	resultFor func(instanceID, commandID, requestedBy, inputs string) string
}

func (h *recordingDispatchHandler) Handle(instanceID, commandID, requestedBy, inputs string) string {
	atomic.AddInt32(&h.calls, 1)
	if h.panicOn != "" && inputs == h.panicOn {
		panic("boom")
	}
	if h.resultFor != nil {
		return h.resultFor(instanceID, commandID, requestedBy, inputs)
	}
	return `{"fields":{"status":"ok"},"narrative":"handled"}`
}

// TestRunCommandDispatcherHandlesRequestExactlyOnce drives a real
// SubmitCommand dispatch through a real RunCommandDispatcher loop (self
// target, matching TestSubmitCommandIndexesExecutionsByPeer's sibling
// self-target pattern) and checks three things: the handler actually
// runs and its returned JSON lands in AppendCommandLog, the handler is
// never invoked more than once for the same instance id even across
// RunCommandDispatcher's own periodic rescan, and StopCommandDispatcher
// actually stops the loop.
func TestRunCommandDispatcherHandlesRequestExactlyOnce(t *testing.T) {
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

	const commandID = "cmd-dispatcher"
	const groupID = "grp-dispatcher"
	if err := CreateCommand(commandID, "Echo", selfPeerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(commandID)
		return err == nil, nil
	})
	grantCommandAccess(t, commandID, groupID, selfPeerID)

	handler := &recordingDispatchHandler{
		resultFor: func(instanceID, gotCommandID, requestedBy, inputs string) string {
			out, _ := json.Marshal(map[string]any{
				"fields":    map[string]string{"status": "ok", "seen_inputs": inputs},
				"narrative": "handled",
			})
			return string(out)
		},
	}
	if err := RunCommandDispatcher(commandID, handler); err != nil {
		t.Fatalf("RunCommandDispatcher: %v", err)
	}
	t.Cleanup(func() { StopCommandDispatcher(commandID) })

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = SubmitCommand(commandID, `{"echo":"hi"}`)
		return err == nil, err
	})

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(instanceID)
		if err != nil {
			return false, nil
		}
		var rec struct {
			Fields    map[string]string `json:"fields"`
			Narrative string            `json:"narrative"`
		}
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			return false, nil
		}
		return rec.Fields["status"] == "ok" && rec.Fields["seen_inputs"] == `{"echo":"hi"}` && rec.Narrative == "handled", nil
	})

	// Give RunCommandDispatcher's own periodic rescan (dispatcherPollInterval,
	// currently watchCommandLogPollInterval == 1.5s) a real chance to run at
	// least once more before checking the handler wasn't invoked again for
	// the same, already-handled instance.
	time.Sleep(2 * dispatcherPollInterval)
	if got := atomic.LoadInt32(&handler.calls); got != 1 {
		t.Fatalf("handler invoked %d times for one instance id, want exactly 1 (dedup via QueryCommandLog should have prevented a re-run on rescan)", got)
	}

	StopCommandDispatcher(commandID)
	// A stopped dispatcher must not react to a later SubmitCommand either.
	var secondInstanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		secondInstanceID, err = SubmitCommand(commandID, `{"echo":"bye"}`)
		return err == nil, err
	})
	time.Sleep(2 * dispatcherPollInterval)
	if _, err := LatestCommandLog(secondInstanceID); err == nil {
		t.Fatal("a stopped RunCommandDispatcher still handled a request submitted after StopCommandDispatcher")
	}
}

// TestRunCommandDispatcherRecoversHandlerPanic checks that a handler
// panic on one request doesn't take down the loop: an error result still
// gets recorded for the panicking instance, and a later, unrelated
// instance still gets handled normally afterward.
func TestRunCommandDispatcherRecoversHandlerPanic(t *testing.T) {
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

	const commandID = "cmd-dispatcher-panic"
	const groupID = "grp-dispatcher-panic"
	if err := CreateCommand(commandID, "Panics", selfPeerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(commandID)
		return err == nil, nil
	})
	grantCommandAccess(t, commandID, groupID, selfPeerID)

	handler := &recordingDispatchHandler{panicOn: "panic"}
	if err := RunCommandDispatcher(commandID, handler); err != nil {
		t.Fatalf("RunCommandDispatcher: %v", err)
	}
	t.Cleanup(func() { StopCommandDispatcher(commandID) })

	var panicInstanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		panicInstanceID, err = SubmitCommand(commandID, "panic")
		return err == nil, err
	})

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(panicInstanceID)
		if err != nil {
			return false, nil
		}
		var rec struct {
			Fields map[string]string `json:"fields"`
		}
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			return false, nil
		}
		return rec.Fields["status"] == "error", nil
	})

	// The loop must still be alive and functional after the panic -- a
	// normal request submitted afterward must still be handled.
	var normalInstanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		normalInstanceID, err = SubmitCommand(commandID, "fine")
		return err == nil, err
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(normalInstanceID)
		if err != nil {
			return false, nil
		}
		var rec struct {
			Fields map[string]string `json:"fields"`
		}
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			return false, nil
		}
		return rec.Fields["status"] == "ok", nil
	})
}

// logEntry is QueryCommandLog's per-record JSON shape, decoded locally so
// these tests don't need pkg/logrecord as a direct import.
type logEntry struct {
	Fields    map[string]string `json:"fields"`
	Narrative string            `json:"narrative"`
}

func queryCommandLogEntries(t *testing.T, instanceID string) []logEntry {
	t.Helper()
	out, err := QueryCommandLog(instanceID, "", "", "")
	if err != nil {
		return nil
	}
	var entries []logEntry
	if err := json.Unmarshal([]byte(out), &entries); err != nil {
		t.Fatalf("decode QueryCommandLog result: %v", err)
	}
	return entries
}

// TestRunCommandDispatcherHandlerReportsProgress drives a handler that
// calls ReportProgress once before returning its real result, and checks
// that QueryCommandLog surfaces both entries in order (the progress update
// first, the terminal result second) -- the live-progress pattern
// ReportProgress exists for -- while the handler is still only invoked
// exactly once for the instance.
func TestRunCommandDispatcherHandlerReportsProgress(t *testing.T) {
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

	const commandID = "cmd-dispatcher-progress"
	const groupID = "grp-dispatcher-progress"
	if err := CreateCommand(commandID, "LongRunning", selfPeerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(commandID)
		return err == nil, nil
	})
	grantCommandAccess(t, commandID, groupID, selfPeerID)

	handler := &recordingDispatchHandler{
		resultFor: func(instanceID, gotCommandID, requestedBy, inputs string) string {
			if err := ReportProgress(requestedBy, instanceID, `{"step":"1/2"}`, "started"); err != nil {
				t.Errorf("ReportProgress: %v", err)
			}
			out, _ := json.Marshal(map[string]any{
				"fields":    map[string]string{"status": CommandStatusSuccess},
				"narrative": "done",
			})
			return string(out)
		},
	}
	if err := RunCommandDispatcher(commandID, handler); err != nil {
		t.Fatalf("RunCommandDispatcher: %v", err)
	}
	t.Cleanup(func() { StopCommandDispatcher(commandID) })

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = SubmitCommand(commandID, "")
		return err == nil, err
	})

	var entries []logEntry
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		entries = queryCommandLogEntries(t, instanceID)
		return len(entries) == 2, nil
	})
	if len(entries) != 2 {
		t.Fatalf("got %d command log entries for instance %s, want 2 (one progress, one terminal)", len(entries), instanceID)
	}
	if entries[0].Fields["status"] != CommandStatusRunning || entries[0].Narrative != "started" {
		t.Fatalf("first entry = %+v, want the ReportProgress update", entries[0])
	}
	if entries[1].Fields["status"] != CommandStatusSuccess || entries[1].Narrative != "done" {
		t.Fatalf("second entry = %+v, want the handler's terminal result", entries[1])
	}

	time.Sleep(2 * dispatcherPollInterval)
	if got := atomic.LoadInt32(&handler.calls); got != 1 {
		t.Fatalf("handler invoked %d times for one instance id, want exactly 1", got)
	}
}

// TestReportProgressLeavesInstancePendingForAFreshDispatcher simulates a
// dispatcher that reported progress and then died before ever writing a
// terminal result (ReportProgress called directly, no RunCommandDispatcher
// loop involved), then starts a real RunCommandDispatcher with a normal
// handler and checks it still picks the instance up -- proving
// commandRequestAlreadyHandled's dedup check treats a latest
// CommandStatusRunning entry as not yet handled.
func TestReportProgressLeavesInstancePendingForAFreshDispatcher(t *testing.T) {
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

	const commandID = "cmd-dispatcher-resume"
	const groupID = "grp-dispatcher-resume"
	if err := CreateCommand(commandID, "Resumable", selfPeerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(commandID)
		return err == nil, nil
	})
	grantCommandAccess(t, commandID, groupID, selfPeerID)

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = SubmitCommand(commandID, "")
		return err == nil, err
	})

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		err := ReportProgress(selfPeerID, instanceID, `{"step":"1/2"}`, "started, then died")
		return err == nil, err
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(instanceID)
		if err != nil {
			return false, nil
		}
		var rec logEntry
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			return false, nil
		}
		return rec.Fields["status"] == CommandStatusRunning, nil
	})

	handler := &recordingDispatchHandler{
		resultFor: func(instanceID, gotCommandID, requestedBy, inputs string) string {
			out, _ := json.Marshal(map[string]any{
				"fields":    map[string]string{"status": CommandStatusSuccess},
				"narrative": "resumed",
			})
			return string(out)
		},
	}
	if err := RunCommandDispatcher(commandID, handler); err != nil {
		t.Fatalf("RunCommandDispatcher: %v", err)
	}
	t.Cleanup(func() { StopCommandDispatcher(commandID) })

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(instanceID)
		if err != nil {
			return false, nil
		}
		var rec logEntry
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			return false, nil
		}
		return rec.Fields["status"] == CommandStatusSuccess && rec.Narrative == "resumed", nil
	})
	if got := atomic.LoadInt32(&handler.calls); got != 1 {
		t.Fatalf("handler invoked %d times, want exactly 1", got)
	}

	entries := queryCommandLogEntries(t, instanceID)
	if len(entries) != 2 {
		t.Fatalf("got %d command log entries, want 2 (the earlier progress entry preserved, plus the new terminal one)", len(entries))
	}
	if entries[0].Fields["status"] != CommandStatusRunning {
		t.Fatalf("first entry = %+v, want the earlier progress update, untouched", entries[0])
	}
}
