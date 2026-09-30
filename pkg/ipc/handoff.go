// The response handoff between Call and Serve on the Android transport.
//
// Deliberately build-tag-free, unlike ipc_android.go which is its only caller: what lives here is
// a concurrency contract -- who acks, who waits, and for how long -- and the failure it guards
// against is a deadlock, which is exactly the kind of thing that has to be testable on a plain
// host rather than only on a phone. See handoff_test.go.
package ipc

import "time"

// respHandoff carries the response segment's fd back to Call, plus an ack
// Call closes once it has opened (dup'd) that fd. Serve must wait for that
// ack before it may CloseStorage its own writer: an ASharedMemory fd is
// the only thing keeping the region alive until the other side dups it, so
// closing it any earlier would free memory Call hasn't attached to yet
// (shmring.OpenAndroidSharedMemory dups on entry specifically so each side
// ends up with an independent fd/mapping safe to close on its own schedule
// -- but only once that dup has actually happened).
type respHandoff struct {
	fd  int
	ack chan struct{}
}

// ackGrace bounds how long either side waits on the handoff ack.
//
// The ack is closed the instant Call has dup'd the fd, so in the ordinary case this timer never
// fires -- it exists only for the case where the other side has *gone*, and before it existed that
// case was unrecoverable. Serve's send lands in a buffered channel, so it succeeds even when the
// caller has already given up and returned; Serve then blocked on `<-ack` with only its own
// context to release it, which is the daemon's whole lifetime. So one Call that timed out after
// its request had been picked up wedged that peer's Serve loop permanently -- every later Call
// blocked on the unbuffered mailbox until its own deadline -- and stranded the response segment's
// fd for good, one per occurrence.
//
// Five seconds rather than something tighter because the wait is genuinely bounded work (a dup and
// a channel close) and a timer that fires early would free a segment the caller is still attaching
// to, which is the corruption this handshake exists to prevent.
const ackGrace = 5 * time.Second

// awaitHandoffAck waits for Call to say it has dup'd the parked fd, or for [ackGrace] to expire.
//
// **It deliberately takes no context, and that is the whole point of it being a function.** Once the
// fd is in Call's channel, Call may read it and dup it at any moment, and the only thing that makes
// that dup safe is Serve still holding the fd open. A cancellation arriving in between is not a
// reason to free the segment early -- it is exactly the case [ackGrace]'s own comment describes as
// the corruption this handshake exists to prevent, arriving with a timer that fires at zero.
//
// Serve used to select on `ctx.Done()` here alongside the ack and the grace, and released the
// segment the instant its daemon was cancelled. Measured on object-history-app's optical rig,
// 2026-09-30: a phone whose remembered cluster named a destroyed peer had its daemon torn down by a
// failed join while `requestRelayAccess` was in flight, and that call answered
// `ipc: open response shm: backend: dup fd 198: bad file descriptor` -- unix.Dup on an fd this
// branch had just closed underneath it.
//
// The cost of not honouring cancellation here is bounded and small: at most [ackGrace] on a shutdown
// where the caller never acks, which is the same bound the abandoned case already accepts. The cost
// of honouring it was a caller mapping memory that had been freed, or -- if the fd number had been
// reused by then -- mapping somebody else's.
func awaitHandoffAck(ack <-chan struct{}) {
	t := time.NewTimer(ackGrace)
	defer t.Stop()
	select {
	case <-ack:
	case <-t.C:
		// The caller gave up between handing us its request and reading our answer.
	}
}

// releaseAbandoned takes a response handoff nobody is going to read and acks it, so Serve may
// release the segment and move on. Bounded, because Serve is not obliged to send at all: it
// `continue`s past several error paths without ever reaching the handoff, and a goroutine parked
// forever on that channel would be the leak this is here to stop.
func releaseAbandoned(respChan chan respHandoff) {
	t := time.NewTimer(ackGrace)
	defer t.Stop()
	select {
	case rh := <-respChan:
		close(rh.ack)
	case <-t.C:
	}
}
