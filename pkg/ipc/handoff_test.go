package ipc

import (
	"testing"
	"time"
)

// A caller that walked away must still leave Serve free to continue.
//
// Before releaseAbandoned existed, Call's ctx.Done path returned without ever closing the ack.
// Serve's send lands in a buffered channel, so it succeeded anyway and Serve then blocked on
// `<-ack` with only its own (process-lifetime) context to release it: that peer's IPC was dead
// from then on, and the response segment's fd went with it. This is that handshake, without the
// shared memory.
func TestReleaseAbandonedAcksAHandoffNobodyWillRead(t *testing.T) {
	respChan := make(chan respHandoff, 1)
	ack := make(chan struct{})

	// Serve's side: park the handoff, then wait to be acked.
	respChan <- respHandoff{fd: 7, ack: ack}

	go releaseAbandoned(respChan)

	select {
	case <-ack:
	case <-time.After(ackGrace + time.Second):
		t.Fatal("the abandoned handoff was never acked -- Serve would block here for the life of the process")
	}
}

// And it must not park a goroutine forever when Serve never sends at all, which several of
// Serve's own error paths do by `continue`ing past the handoff entirely.
func TestReleaseAbandonedGivesUpWhenNoHandoffArrives(t *testing.T) {
	respChan := make(chan respHandoff, 1)
	done := make(chan struct{})
	go func() { releaseAbandoned(respChan); close(done) }()

	select {
	case <-done:
	case <-time.After(ackGrace + 2*time.Second):
		t.Fatal("releaseAbandoned never returned -- it would leak one goroutine per abandoned call")
	}
}

// TestAwaitHandoffAckReturnsOnTheAck is the ordinary case: Call dup'd the fd and said so, and Serve
// may release its own immediately.
func TestAwaitHandoffAckReturnsOnTheAck(t *testing.T) {
	ack := make(chan struct{})
	done := make(chan struct{})
	go func() { awaitHandoffAck(ack); close(done) }()
	close(ack)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("awaitHandoffAck did not return once the ack was closed")
	}
}

// TestAwaitHandoffAckTakesNoContext is the regression for
// "ipc: open response shm: backend: dup fd N: bad file descriptor".
//
// Serve used to select on its daemon's ctx here, and released the response segment the instant that
// ctx was cancelled -- with the fd already sitting in Call's channel, where Call was free to dup it.
// The test cannot pass a context to prove the branch is gone, which is the point: the wait's
// signature is the guarantee. What it can check is that a wait already in progress is not shortened
// by anything other than the ack, so a caller about to dup keeps a live fd for at least the grace.
func TestAwaitHandoffAckTakesNoContext(t *testing.T) {
	ack := make(chan struct{})
	done := make(chan struct{})
	go func() { awaitHandoffAck(ack); close(done) }()

	// Whatever else happens in the process -- a cancelled daemon, a closed context -- this wait is
	// still running, because the only things it listens to are the ack and its own grace.
	select {
	case <-done:
		t.Fatal("awaitHandoffAck returned without an ack, long before ackGrace: a caller about to " +
			"dup the parked fd would find it closed")
	case <-time.After(100 * time.Millisecond):
	}
	close(ack)
	<-done
}
