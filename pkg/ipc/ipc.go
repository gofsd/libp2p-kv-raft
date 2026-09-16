//go:build !android

// Package ipc implements local (same-machine) request/response IPC between
// the short-lived mage CLI process and a long-running kvnode daemon, over
// github.com/gofsd/shmring shared-memory ring buffers, carrying
// pkg/shmevent's capnp-encoded Event struct.
//
// This file is the desktop (linux/darwin/windows) transport, built on
// shmring's name-based CreateShm/OpenShm. GOOS=android inherits Go's
// "linux" build tag (a long-standing special case in the toolchain's
// build-constraint matching), so it needs excluding explicitly here, same
// as shmring's own backend does for the same reason -- see ipc_android.go
// for the real Android transport and why it has to be a different design
// entirely (ASharedMemory, which is what Android actually provides, has no
// name-based rendezvous at all).
//
// # Design
//
// shmring ring buffers are single-producer/single-consumer for their whole
// lifetime: whoever calls CreateShm owns header initialization and, later,
// removal (CloseStorage); the other side OpenShm's the same name as a
// read-only consumer. That is a poor fit for "one long-running daemon, many
// independent short-lived clients" unless every request/response gets a
// fresh pair of segments. So each call to Call:
//
//  1. CreateShm's the node's request channel (client is producer), writes
//     one capnp-encoded pkg/shmevent.Msg, and Close()s it. Unlike the
//     fixed-size ipcproto.Request this replaced, a capnp message has no
//     fixed size, so the write is however many bytes Encode produces, and
//     the read side (readAll below) reads until EOF rather than a known
//     length.
//  2. Waits for a response channel named after that same message's ID to
//     appear (OpenShm with retry) and reads one capnp-encoded Msg from it.
//  3. Removes the request segment (CloseStorage) now that the response
//     proves the daemon already read it.
//
// Serve mirrors this on the daemon side: OpenShm the (fixed-name) request
// channel (blocking/retrying between commands), read exactly one request,
// handle it, CreateShm a response channel named after the request's ID and
// write the response, then loop back to wait for the next request -- once
// it sees one with a different ID than the round it just answered, it
// knows the client has moved on and can safely remove the previous round's
// response segment.
//
// # Why the response channel is named per request, not fixed per node
//
// It used to be a single fixed name, reused every round. That was a
// genuine, silent-request-loss bug: the daemon only removed the *previous*
// round's response segment once it had separately confirmed (via its own
// polling, up to openRetryInterval later) that the client had torn down
// the previous round's request segment. A client that issued a second
// Call immediately after the first -- no human typing a pause between two
// `mage` commands, e.g. an automated Set immediately followed by a Get, or
// two nodes bootstrapping back to back -- could start polling for its
// *own* response before that stale segment was gone, OpenShm it by the
// (same, reused) name, read the *previous* round's response, and mistake
// that for proof its own (actually still unread) request had been
// handled. It would then remove its own request segment out from under
// the daemon -- silently dropping the real request while reporting false
// success back to the caller. Naming the response channel after an id the
// daemon never reuses within one round trip makes that impossible by
// construction: a client can never open a segment it didn't itself just
// ask the daemon to create for this exact round. See pkg/shmevent's doc
// comment for why a Msg's id -- chosen by the caller, not this package --
// is safe to use for this: a caller that wants to cite an id later via a
// variant's own sourceId/destinationId field has its own reason to pick a
// fresh one per logical operation anyway.
//
// # Why the request channel is fixed per node, and how replay is avoided
// without a wait
//
// The daemon needs a well-known rendezvous point to discover a request it
// hasn't seen yet, so the request channel name stays fixed per node
// (derived from its peer id and its own local-IPC token, see token.go) and
// is re-created fresh by the client on every round trip. A shmring.Reader
// always starts reading from offset 0
// of whatever segment it opens, with no memory of where a previous Reader
// on the same name left off -- so if Serve looped straight back to
// opening the request channel by name before the client had torn down the
// segment from the round it just handled, it would reopen that same
// still-alive segment and read the same bytes again.
//
// An earlier version of this code handled that by blocking until the name
// disappeared before looking for the next request. That introduced its
// own deadlock: the client creates the *next* round's request segment
// (same fixed name) as soon as its previous Call returns, which can race
// ahead of the daemon's own polling confirmation that the *previous*
// segment is gone; the daemon's wait would then mistake the brand new
// segment for the still-lingering old one and block on it forever,
// since the daemon itself is what would need to read it to make it go
// away. Serve instead just re-reads whatever it finds and compares the
// decoded message's ID against the last one it actually handled: a match
// means this is the same request it already answered (the client hasn't
// removed it yet), so it waits a beat and rereads rather than
// reprocessing; any other ID is a genuinely new request, safe to handle
// immediately regardless of whether the segment is "new" or one the
// daemon simply hasn't noticed disappear yet.
//
// Together this only supports one in-flight request per node at a time --
// adequate for a single operator driving commands sequentially from a
// CLI.
package ipc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gofsd/shmring"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// callerLocks serializes every Call/CallRaw round trip against a given
// peerID's request channel, one at a time -- enforcing this package's own
// documented constraint ("one in-flight request per node at a time," see
// above) for callers *within the same process*, not just the single
// sequential CLI invocation that constraint originally assumed. shmring's
// CreateShm on the fixed-name request channel is single-producer: two
// genuinely concurrent callers (e.g. two goroutines racing to Call the
// same peerID at once -- a background command-dispatch loop alongside
// ordinary foreground calls, say) would otherwise race to create/write/
// tear down the same segment and corrupt each other rather than queue
// safely. This does nothing for two separate OS processes calling at
// once, which was never safe and still isn't (no cross-process lock is
// taken) -- only Go-level concurrency within one process is fixed here.
//
// A buffered channel rather than a sync.Mutex, because a mutex cannot be cancelled: Lock() ignores
// the caller's context entirely. A caller queued behind a slow one therefore waited past its own
// deadline and only *then* ran its first context-aware step, reporting "waiting for response
// channel ...: context deadline exceeded" -- blaming the daemon for time spent in this queue, and
// blaming it after the whole budget was already gone. A mes backend polls many commands through
// one session, so one slow handler produced a burst of those errors for requests that had never
// reached the daemon at all; it cost an optical batch its cmd-inventoryDictionary case on
// 2026-09-15. See caller_lock_test.go.
var (
	callerLocksMu sync.Mutex
	callerLocks   = map[string]chan struct{}{}
)

func callerLock(peerID string) chan struct{} {
	callerLocksMu.Lock()
	defer callerLocksMu.Unlock()
	l, ok := callerLocks[peerID]
	if !ok {
		l = make(chan struct{}, 1)
		callerLocks[peerID] = l
	}
	return l
}

// acquireCaller takes peerID's caller lock, or gives up when ctx does, saying which it was
// waiting for. The returned func releases it and must be called exactly once.
func acquireCaller(ctx context.Context, peerID string) (func(), error) {
	l := callerLock(peerID)
	select {
	case l <- struct{}{}:
		return func() { <-l }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("ipc: waiting for the caller lock on %s: %w", peerID, ctx.Err())
	}
}

// capacity is the shared-memory data region size for both channels. It must
// be a power of two and comfortably fit the largest encoded request/
// response this package ever carries -- today, a channelSend/channelPoll
// chunk (up to ~16KB in practice) is the largest regular traffic; most
// other variants carry only a handful of small fields. Each shmring
// segment this package creates is transient (opened, used for one round
// trip, then CloseStorage'd -- see Call/Serve), so sizing this for the
// rare large message costs a bigger but short-lived mmap on every call,
// never a long-lived allocation, even for a call that only carries a few
// bytes.
const capacity = 32 * 1024

const (
	minPoll = 200 * time.Microsecond
	maxPoll = 5 * time.Millisecond

	// openRetryInterval is the *ceiling* on how long either side waits between attempts to open a
	// segment the other side has not created yet; openRetryMin is where that wait starts.
	//
	// It used to be a flat 20ms on both sides, and both sides are on the round trip: the daemon
	// waits to notice the request, the client waits to notice the response. The first attempt of
	// each essentially always fails -- the other end has not written yet -- so every round trip
	// paid the interval twice however fast the work between them was, and the work between them is
	// usually a SQLite read measured in microseconds.
	//
	// That floor is what made a scan expensive rather than the scanning. Measured on the optical
	// rig 2026-09-16: a dispatcher sweep of 55 commands, about two round trips each, had a median
	// of 3.23s -- ~110 x ~30ms, which is this constant and almost nothing else -- and one
	// command's request listing exhausted kvctl's whole 10s budget across roughly fifty
	// individually-fast calls, none of them slow enough to be worth a line. The backoff keeps the
	// old ceiling for a genuinely absent peer, so an idle daemon still costs the same, and hands
	// the common case back the ~40ms it was spending on sleep.
	openRetryMin      = 200 * time.Microsecond
	openRetryInterval = 20 * time.Millisecond
)

// nextOpenRetry returns the wait after one that lasted d, doubling from [openRetryMin] up to
// [openRetryInterval]. d == 0 means "no wait yet", so the first is the shortest.
func nextOpenRetry(d time.Duration) time.Duration {
	if d <= 0 {
		return openRetryMin
	}
	if d >= openRetryInterval {
		return openRetryInterval
	}
	if d *= 2; d > openRetryInterval {
		return openRetryInterval
	}
	return d
}

// reqChannel/respChannel fold token (see token.go's doc comment) into the
// segment name itself, not just something checked after the fact: without
// it, either name is derivable from peerID alone (public -- it's what
// every remote address and registry.json entry already advertises), so
// any co-resident process able to attach to a POSIX shared-memory segment
// by name at all (shmring's backend grants owner+group rw -- see
// loadOrGenerateToken) could open a legitimate node's request channel
// itself, race the real client to create it, or read/forge a response.
// Requiring the token to even *construct* the right name closes that: a
// caller with no read access to the token file cannot address the
// channel, cannot attach to it, and cannot observe its traffic, rather
// than merely failing some check after having already done so.
func reqChannel(peerID, token string) string { return "kvipc-" + peerID + "-" + token + "-req" }

// respChannel is unique per round trip (see package doc comment): id is
// the originating message's ID, which the daemon echoes into its response.
func respChannel(peerID, token string, id uint16) string {
	return fmt.Sprintf("kvipc-%s-%s-resp-%d", peerID, token, id)
}

// Call sends m (with m.ID already set by the caller -- see pkg/shmevent's
// doc comment on why the caller, not this package, chooses it) to the
// daemon serving peerID, signed with priv (nil only for
// EventGetPublicKey/EventGetPrivateKey -- see shmevent.Sign), and returns
// its response. It blocks until the daemon replies or ctx is done.
func Call(ctx context.Context, peerID string, m shmevent.Msg, priv shmevent.PrivateKey) (shmevent.Msg, error) {
	// Timed phase by phase, and reported on the failure paths as well as the slow ones -- see
	// [callTiming]. Off unless KVRAFT_IPC_CALL_LOG is set.
	timing := startCallTiming()
	var callErr error
	defer func() { timing.report(peerID, m.Id(), m.Which().String(), callErr) }()

	release, err := acquireCaller(ctx, peerID)
	timing.done(phaseLock)
	if err != nil {
		callErr = err
		return shmevent.Msg{}, err
	}
	defer release()

	token, err := tokenForPeer(peerID)
	if err != nil {
		callErr = err
		return shmevent.Msg{}, err
	}

	rn := reqChannel(peerID, token)
	w, err := shmring.CreateShm(rn, capacity, shmring.WithPollInterval(minPoll, maxPoll))
	timing.done(phaseOpenRequest)
	if err != nil {
		callErr = fmt.Errorf("ipc: create request channel: %w", err)
		return shmevent.Msg{}, callErr
	}

	buf, err := shmevent.Encode(m, priv)
	if err != nil {
		w.CloseStorage()
		callErr = fmt.Errorf("ipc: encode request: %w", err)
		return shmevent.Msg{}, callErr
	}
	if _, err := w.WriteContext(ctx, buf); err != nil {
		w.CloseStorage()
		callErr = fmt.Errorf("ipc: write request: %w", err)
		return shmevent.Msg{}, callErr
	}
	if err := w.Close(); err != nil {
		w.CloseStorage()
		callErr = fmt.Errorf("ipc: close request writer: %w", err)
		return shmevent.Msg{}, callErr
	}
	timing.done(phaseWriteRequest)

	r, err := openRespWithRetry(ctx, peerID, token, m.Id())
	timing.done(phaseAwaitResponse)
	if err != nil {
		w.CloseStorage()
		callErr = err
		return shmevent.Msg{}, err
	}

	respBuf, err := readAll(ctx, r)
	r.Close()
	timing.done(phaseReadResponse)
	if err != nil {
		w.CloseStorage()
		callErr = fmt.Errorf("ipc: read response: %w", err)
		return shmevent.Msg{}, callErr
	}

	// The response proves the daemon already fully read the request; safe
	// to remove the request segment now.
	w.CloseStorage()

	// Decode checks respBuf's CRC32 (corruption) but, per its own doc
	// comment, never the signature (authenticity) -- and this response leg
	// doesn't call shmevent.Verify either, unlike every other Decode call
	// site that goes on to check a signature against a known key. This is
	// deliberate, not an oversight: shmring is same-machine-only (see
	// pkg/shmevent's own doc comment on that trust boundary), so only a
	// co-resident process holding the daemon's own key could forge a
	// response in the first place -- a threat Verify wouldn't add any
	// defense against here. A caller relying on Call still gets the
	// request leg's Verify (inside the daemon's own Serve loop) checking
	// *it*, since that's the leg crossing a real trust boundary (an
	// arbitrary local caller, not necessarily the daemon's own key holder).
	resp, _, _, err := shmevent.Decode(respBuf)
	if err != nil {
		return shmevent.Msg{}, err
	}
	return resp, nil
}

// CallRaw is Call's pass-through counterpart: encoded is already a complete,
// signed shmevent.Encode output -- built earlier, possibly by a different
// signer, and handed to this call verbatim -- rather than a Msg this
// package signs itself. It's what lets a one-time ticket (e.g. a
// pre-signed EventPermitConfirm built and barcoded well before whoever
// redeems it runs this) actually reach the daemon with its original
// signature intact: Call always re-signs with priv, which would discard
// that signature and substitute this caller's own key instead. Decoding
// encoded here only reads m.ID back out (to address the response channel)
// and validates the framing; it never re-encodes or re-signs it.
func CallRaw(ctx context.Context, peerID string, encoded []byte) (shmevent.Msg, error) {
	release, err := acquireCaller(ctx, peerID)
	if err != nil {
		return shmevent.Msg{}, err
	}
	defer release()

	m, _, _, err := shmevent.Decode(encoded)
	if err != nil {
		return shmevent.Msg{}, fmt.Errorf("ipc: decode raw request: %w", err)
	}

	token, err := tokenForPeer(peerID)
	if err != nil {
		return shmevent.Msg{}, err
	}

	rn := reqChannel(peerID, token)
	w, err := shmring.CreateShm(rn, capacity, shmring.WithPollInterval(minPoll, maxPoll))
	if err != nil {
		return shmevent.Msg{}, fmt.Errorf("ipc: create request channel: %w", err)
	}

	if _, err := w.WriteContext(ctx, encoded); err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: write request: %w", err)
	}
	if err := w.Close(); err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: close request writer: %w", err)
	}

	r, err := openRespWithRetry(ctx, peerID, token, m.Id())
	if err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, err
	}

	respBuf, err := readAll(ctx, r)
	r.Close()
	if err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: read response: %w", err)
	}

	w.CloseStorage()

	// No shmevent.Verify on this leg either -- see Call's identical
	// response decode above for why.
	resp, _, _, err := shmevent.Decode(respBuf)
	if err != nil {
		return shmevent.Msg{}, err
	}
	return resp, nil
}

func openRespWithRetry(ctx context.Context, peerID, token string, id uint16) (*shmring.Reader, error) {
	name := respChannel(peerID, token, id)
	var wait time.Duration
	for {
		r, err := shmring.OpenShm(name, capacity, shmring.WithPollInterval(minPoll, maxPoll))
		if err == nil {
			return r, nil
		}
		wait = nextOpenRetry(wait)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("ipc: waiting for response channel %s: %w", name, ctx.Err())
		case <-time.After(wait):
		}
	}
}

// readAll reads r until EOF -- a capnp message has no fixed size (unlike
// the ipcproto.Request/Response this transport used to carry), so callers
// can no longer read a known number of bytes up front.
func readAll(ctx context.Context, r *shmring.Reader) ([]byte, error) {
	var out []byte
	chunk := make([]byte, 4096)
	for {
		n, err := r.ReadContext(ctx, chunk)
		if n > 0 {
			out = append(out, chunk[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				return out, nil
			}
			return out, err
		}
	}
}

// Handler processes one decoded request (m, with crc/sig as decoded off
// the wire for the handler to verify -- see shmevent.Verify) and returns
// the response Msg to send back.
type Handler func(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg

// Serve runs the daemon side of the protocol for peerID: it repeatedly waits
// for a request, dispatches it to handle, and sends back the response,
// signed with priv. It blocks until ctx is done. dataDir is this node's own
// data directory -- Serve loads (or, on a brand new node, generates) its
// local-IPC token there (see token.go's doc comment) before it starts
// listening; pkg/daemon.Run already writes ready.json from the same
// directory only after this point, so a client's waitForReady is
// guaranteed to see the token file already in place.
func Serve(ctx context.Context, peerID, dataDir string, priv shmevent.PrivateKey, handle Handler) error {
	token, err := loadOrGenerateToken(dataDir)
	if err != nil {
		return err
	}
	name := reqChannel(peerID, token)

	var lastID uint16
	var haveLastID bool
	var dedupWait time.Duration
	var pendingResp *shmring.Writer
	cleanupPending := func() {
		if pendingResp != nil {
			pendingResp.CloseStorage()
			pendingResp = nil
		}
	}
	defer cleanupPending()

	for {
		r, err := openReqWithRetry(ctx, name)
		if err != nil {
			return err
		}

		reqBuf, err := readAll(ctx, r)
		r.Close()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		m, crc, sig, err := shmevent.Decode(reqBuf)
		if err != nil {
			continue
		}

		if haveLastID && m.Id() == lastID {
			// The same request segment we already answered, reopened before the client has torn it
			// down -- see the package doc comment on why we reread and dedup by ID instead of
			// blocking for the name to disappear. Give the client a beat to catch up and try again.
			//
			// **This is the sleep that paced the whole transport**, not the two open-retry loops
			// that look like they would. The request channel has a fixed name, so after answering
			// one call the daemon comes straight back round and re-opens the segment the client is
			// still reading its response out of -- the dedup fires on essentially every call, and a
			// flat wait here was a flat wait per round trip. Backed off from [openRetryMin] the
			// same way, which took a 20-call sequence from 432ms to the tens of milliseconds and
			// is the whole reason a scan of fifty records could exhaust a ten-second budget.
			dedupWait = nextOpenRetry(dedupWait)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(dedupWait):
			}
			continue
		}
		// A new request: the client has moved on, so the next echo starts its backoff afresh.
		dedupWait = 0

		// A genuinely new request only appears once the client's previous
		// Call has returned (single in-flight caller), which only happens
		// after it read our previous response -- safe to remove it now.
		cleanupPending()

		resp := handle(ctx, m, crc, sig)
		resp.SetId(m.Id())
		respWriter, err := sendResponse(ctx, peerID, token, resp, priv)
		if err != nil {
			return err
		}
		pendingResp = respWriter
		lastID = m.Id()
		haveLastID = true
	}
}

func openReqWithRetry(ctx context.Context, name string) (*shmring.Reader, error) {
	var wait time.Duration
	for {
		r, err := shmring.OpenShm(name, capacity, shmring.WithPollInterval(minPoll, maxPoll))
		if err == nil {
			return r, nil
		}
		// Backed off the same way as the response side, and for the same reason: this wait is on
		// the round trip too. A daemon that sleeps 20ms before noticing a request it could have
		// served in microseconds adds that to every call, and this loop is also what an *idle*
		// daemon sits in -- which is why the ceiling stays where it was.
		wait = nextOpenRetry(wait)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// sendResponse creates the response channel (named after resp.ID, which the
// caller must have set to the originating request's ID), writes resp
// signed with priv, and closes the writer (marking it done, but not yet
// removing the segment). The caller must CloseStorage the returned writer
// once it has independently confirmed the client has read the response
// (Serve does this lazily, once it sees the next round's distinct request
// ID).
func sendResponse(ctx context.Context, peerID, token string, resp shmevent.Msg, priv shmevent.PrivateKey) (*shmring.Writer, error) {
	w, err := shmring.CreateShm(respChannel(peerID, token, resp.Id()), capacity, shmring.WithPollInterval(minPoll, maxPoll))
	if err != nil {
		return nil, fmt.Errorf("ipc: create response channel: %w", err)
	}
	buf, err := shmevent.Encode(resp, priv)
	if err != nil {
		w.CloseStorage()
		return nil, fmt.Errorf("ipc: encode response: %w", err)
	}
	if _, err := w.WriteContext(ctx, buf); err != nil {
		w.CloseStorage()
		return nil, fmt.Errorf("ipc: write response: %w", err)
	}
	if err := w.Close(); err != nil {
		w.CloseStorage()
		return nil, fmt.Errorf("ipc: close response writer: %w", err)
	}
	return w, nil
}
