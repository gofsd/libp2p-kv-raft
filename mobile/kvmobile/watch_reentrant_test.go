package kvmobile

import (
	"testing"
	"time"
)

// stopFromCallback is what a watcher for a *finite* piece of work looks
// like: the callback is the thing that sees the record saying the run is
// over, so the callback is what stops the watch. android-app's LuaWatch is
// exactly this.
type stopFromCallback struct {
	instanceID string
	got        chan string
}

func (s *stopFromCallback) OnRecords(recordsJSON string) {
	StopWatchCommandLog(s.instanceID)
	select {
	case s.got <- recordsJSON:
	default:
	}
}

// TestStoppingACommandLogWatchFromItsOwnCallbackDoesNotDeadlock is the
// regression test for a deadlock that disabled this project's Android Lua
// path entirely: the second "Lua: Run" of an app session hung forever, and
// nothing in the logs named a watcher.
//
// StopWatchCommandLog waits for the watch loop to exit before returning.
// Called from inside OnRecords that is a wait on the caller's own
// goroutine -- it can never finish. The thread parked there kept holding
// both this package's watch mutex and the app-side lock its callback had
// been invoked under, so the next call to start a watch blocked on that
// lock and never came back.
//
// The test needs no daemon: what deadlocked was the stop handshake, not
// anything the poll does. Driving the callback directly is also what makes
// the test fast and deterministic, where reproducing it through a real run
// takes a two-device rig and four minutes.
func TestStoppingACommandLogWatchFromItsOwnCallbackDoesNotDeadlock(t *testing.T) {
	const instanceID = "inst-reentrant"
	cb := &stopFromCallback{instanceID: instanceID, got: make(chan string, 1)}

	commandLogWatchMu.Lock()
	w := &commandLogWatch{cancel: func() {}, done: make(chan struct{})}
	commandLogWatches[instanceID] = w
	commandLogWatchMu.Unlock()
	t.Cleanup(func() {
		commandLogWatchMu.Lock()
		delete(commandLogWatches, instanceID)
		commandLogWatchMu.Unlock()
	})

	// Stand in for the loop reaching its callback: the flag is set exactly
	// as runCommandLogWatch sets it, and `done` is deliberately never
	// closed, because the real loop cannot close it until this callback
	// returns.
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.inCallback.Store(true)
		cb.OnRecords(`[{"narrative":"finished"}]`)
		w.inCallback.Store(false)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a callback that stopped its own watch never returned -- StopWatchCommandLog is waiting for the goroutine that is calling it")
	}

	// Stopping still means stopped: the entry is gone, so a later start
	// for the same instance is a fresh watch rather than a replacement.
	commandLogWatchMu.Lock()
	_, still := commandLogWatches[instanceID]
	commandLogWatchMu.Unlock()
	if still {
		t.Error("the watch was left registered after stopping itself")
	}
}

// The wait is the whole value of Stop for an ordinary caller -- "stopped"
// has to mean "no more callbacks" -- so only the re-entrant case may skip
// it. This pins that an ordinary stop still blocks until the loop is gone.
func TestStoppingACommandLogWatchFromOutsideStillWaitsForIt(t *testing.T) {
	const instanceID = "inst-ordinary"
	w := &commandLogWatch{cancel: func() {}, done: make(chan struct{})}

	commandLogWatchMu.Lock()
	commandLogWatches[instanceID] = w
	commandLogWatchMu.Unlock()

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		StopWatchCommandLog(instanceID)
	}()

	select {
	case <-returned:
		t.Fatal("StopWatchCommandLog returned while the watch loop was still running")
	case <-time.After(200 * time.Millisecond):
	}

	close(w.done) // the loop exits
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("StopWatchCommandLog never returned after its watch loop exited")
	}
}
