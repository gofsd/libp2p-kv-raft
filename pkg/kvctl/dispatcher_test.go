package kvctl_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
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

	if err := kvctl.PutGroup(groupID, "Dispatcher Group"); err != nil {
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

	if err := kvctl.PutGroup(groupID, "Dispatcher Panic Group"); err != nil {
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
