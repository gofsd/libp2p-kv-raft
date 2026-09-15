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
