package kvctl_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestRunCommandDispatcherHandlesRequestExactlyOnce drives a real
// SubmitCommand dispatch through a real RunCommandDispatcher loop (self
// target, single node -- same accepted pattern
// TestSubmitCommandSelfTargetWritesOneIndexEntry already uses) and checks
// three things: the handler actually runs and its return value lands in
// AppendCommandLog, the handler is never invoked more than once for the
// same instance id even across RunCommandDispatcher's own periodic
// rescan, and stop actually stops the loop.
func TestRunCommandDispatcherHandlesRequestExactlyOnce(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const groupID = "grp-dispatcher"
	const commandID = "cmd-dispatcher"

	if err := kvctl.PutGroup(groupID, "Dispatcher Group", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand(commandID, "Echo", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(commandID, groupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, groupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand(commandID)
		return err == nil, nil
	})

	var callCount int32
	handler := func(req kvctl.CommandRequest) (map[string]string, string) {
		atomic.AddInt32(&callCount, 1)
		return map[string]string{"status": "ok", "seen_inputs": req.Inputs}, "handled"
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	var errs []error
	var errsMu sync.Mutex
	go func() {
		defer close(done)
		kvctl.RunCommandDispatcher(commandID, handler, stop, func(err error) {
			errsMu.Lock()
			errs = append(errs, err)
			errsMu.Unlock()
		})
	}()

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.SubmitCommand(commandID, `{"echo":"hi"}`)
		return err == nil, err
	})

	var latest struct {
		found bool
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(instanceID)
		if err != nil {
			return false, nil
		}
		latest.found = rec.Fields["status"] == "ok" && rec.Fields["seen_inputs"] == `{"echo":"hi"}` && rec.Narrative == "handled"
		return latest.found, nil
	})
	if !latest.found {
		t.Fatal("RunCommandDispatcher never recorded the expected handler result")
	}

	// Give RunCommandDispatcher's own periodic rescan (defaultDispatchRescanInterval,
	// currently 3s) a real chance to run at least once more before checking
	// the handler wasn't invoked again for the same, already-handled instance.
	time.Sleep(4 * time.Second)
	if got := atomic.LoadInt32(&callCount); got != 1 {
		t.Fatalf("handler invoked %d times for one instance id, want exactly 1 (dedup via QueryCommandLog should have prevented a re-run on rescan)", got)
	}

	close(stop)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunCommandDispatcher did not return after stop was closed")
	}

	errsMu.Lock()
	defer errsMu.Unlock()
	for _, err := range errs {
		t.Errorf("RunCommandDispatcher reported unexpected error: %v", err)
	}
}

// TestRunCommandDispatcherRecoversHandlerPanic checks that a handler panic
// on one request doesn't take down the loop: an error result still gets
// recorded for the panicking instance, and a later, unrelated instance
// still gets handled normally afterward.
func TestRunCommandDispatcherRecoversHandlerPanic(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const groupID = "grp-dispatcher-panic"
	const commandID = "cmd-dispatcher-panic"

	if err := kvctl.PutGroup(groupID, "Dispatcher Panic Group", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand(commandID, "Panics", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(commandID, groupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, groupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand(commandID)
		return err == nil, nil
	})

	handler := func(req kvctl.CommandRequest) (map[string]string, string) {
		if req.Inputs == `"panic"` {
			panic("boom")
		}
		return map[string]string{"status": "ok"}, "handled"
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		kvctl.RunCommandDispatcher(commandID, handler, stop, nil)
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})

	var panicInstanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		panicInstanceID, err = kvctl.SubmitCommand(commandID, `"panic"`)
		return err == nil, err
	})

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(panicInstanceID)
		if err != nil {
			return false, nil
		}
		return rec.Fields["status"] == "error", nil
	})

	// The loop must still be alive and functional after the panic -- a
	// normal request submitted afterward must still be handled.
	var normalInstanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		normalInstanceID, err = kvctl.SubmitCommand(commandID, `"fine"`)
		return err == nil, err
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(normalInstanceID)
		if err != nil {
			return false, nil
		}
		return rec.Fields["status"] == "ok", nil
	})
}

// TestRunCommandDispatcherHandlerReportsProgress drives a handler that
// calls ReportProgress once before returning its real result, and checks
// that QueryCommandLog surfaces both entries in order (the progress update
// first, the terminal result second) -- the live-progress pattern
// ReportProgress exists for -- while the handler is still only invoked
// exactly once for the instance (RunCommandDispatcher's own dedup must not
// mistake a self-reported progress update, made from inside the same call,
// for a second, unrelated request needing its own separate invocation).
func TestRunCommandDispatcherHandlerReportsProgress(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const groupID = "grp-dispatcher-progress"
	const commandID = "cmd-dispatcher-progress"

	if err := kvctl.PutGroup(groupID, "Dispatcher Progress Group", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand(commandID, "LongRunning", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(commandID, groupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, groupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand(commandID)
		return err == nil, nil
	})

	var callCount int32
	handler := func(req kvctl.CommandRequest) (map[string]string, string) {
		atomic.AddInt32(&callCount, 1)
		if err := kvctl.ReportProgress(req.RequestedBy, req.InstanceID, map[string]string{"step": "1/2"}, "started"); err != nil {
			t.Errorf("ReportProgress: %v", err)
		}
		return map[string]string{"status": kvctl.CommandStatusSuccess}, "done"
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		kvctl.RunCommandDispatcher(commandID, handler, stop, func(err error) {
			t.Errorf("RunCommandDispatcher reported unexpected error: %v", err)
		})
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.SubmitCommand(commandID, "")
		return err == nil, err
	})

	var entries []logrecord.Record
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		entries, err = kvctl.QueryCommandLog(instanceID, time.Unix(0, 0), time.Now().Add(time.Minute), 0)
		return err == nil && len(entries) == 2, nil
	})
	if len(entries) != 2 {
		t.Fatalf("got %d command log entries for instance %s, want 2 (one progress, one terminal)", len(entries), instanceID)
	}
	if entries[0].Fields["status"] != kvctl.CommandStatusRunning || entries[0].Narrative != "started" {
		t.Fatalf("first entry = %+v, want the ReportProgress update", entries[0])
	}
	if entries[1].Fields["status"] != kvctl.CommandStatusSuccess || entries[1].Narrative != "done" {
		t.Fatalf("second entry = %+v, want the handler's terminal result", entries[1])
	}

	// Give a rescan a real chance to run before checking the handler wasn't
	// invoked again for this now-terminal instance.
	time.Sleep(4 * time.Second)
	if got := atomic.LoadInt32(&callCount); got != 1 {
		t.Fatalf("handler invoked %d times for one instance id, want exactly 1", got)
	}
}

// TestReportProgressLeavesInstancePendingForAFreshDispatcher simulates a
// dispatcher process that reported progress and then died before ever
// writing a terminal result (ReportProgress called directly, with no
// RunCommandDispatcher loop involved at all), then starts a real
// RunCommandDispatcher with a normal handler and checks it still picks the
// instance up -- proving commandRequestAlreadyHandled's dedup check
// correctly treats a latest CommandStatusRunning entry as not yet handled,
// not as an already-completed request to skip forever.
func TestReportProgressLeavesInstancePendingForAFreshDispatcher(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const groupID = "grp-dispatcher-resume"
	const commandID = "cmd-dispatcher-resume"

	if err := kvctl.PutGroup(groupID, "Dispatcher Resume Group", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand(commandID, "Resumable", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(commandID, groupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, groupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand(commandID)
		return err == nil, nil
	})

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.SubmitCommand(commandID, "")
		return err == nil, err
	})

	// Simulate the crashed dispatcher: only a progress entry ever gets
	// written, no terminal result.
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		err := kvctl.ReportProgress(leaderID, instanceID, map[string]string{"step": "1/2"}, "started, then died")
		return err == nil, err
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(instanceID)
		return err == nil && rec.Fields["status"] == kvctl.CommandStatusRunning, nil
	})

	var callCount int32
	handler := func(req kvctl.CommandRequest) (map[string]string, string) {
		atomic.AddInt32(&callCount, 1)
		return map[string]string{"status": kvctl.CommandStatusSuccess}, "resumed"
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		kvctl.RunCommandDispatcher(commandID, handler, stop, func(err error) {
			t.Errorf("RunCommandDispatcher reported unexpected error: %v", err)
		})
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(instanceID)
		if err != nil {
			return false, nil
		}
		return rec.Fields["status"] == kvctl.CommandStatusSuccess && rec.Narrative == "resumed", nil
	})
	if got := atomic.LoadInt32(&callCount); got != 1 {
		t.Fatalf("handler invoked %d times, want exactly 1", got)
	}

	entries, err := kvctl.QueryCommandLog(instanceID, time.Unix(0, 0), time.Now().Add(time.Minute), 0)
	if err != nil {
		t.Fatalf("QueryCommandLog: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d command log entries, want 2 (the earlier progress entry preserved, plus the new terminal one)", len(entries))
	}
	if entries[0].Fields["status"] != kvctl.CommandStatusRunning {
		t.Fatalf("first entry = %+v, want the earlier progress update, untouched", entries[0])
	}
}
