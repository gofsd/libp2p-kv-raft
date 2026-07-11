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
// comment for why Msg.ID -- chosen by the caller, not this package -- is
// safe to use for this: a caller that wants to cite an id later via
// SourceID/DestinationID has its own reason to pick a fresh one per
// logical operation anyway.
//
// # Why the request channel is fixed per node, and how replay is avoided
// without a wait
//
// The daemon needs a well-known rendezvous point to discover a request it
// hasn't seen yet, so the request channel name stays fixed per node
// (derived from its peer id) and is re-created fresh by the client on
// every round trip. A shmring.Reader always starts reading from offset 0
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
	"time"

	"github.com/gofsd/shmring"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// capacity is the shared-memory data region size for both channels. It must
// be a power of two and comfortably fit the larger of an encoded
// request/response (see pkg/shmevent.ValueSize).
const capacity = 4096

const (
	minPoll = 200 * time.Microsecond
	maxPoll = 5 * time.Millisecond

	openRetryInterval = 20 * time.Millisecond
)

func reqChannel(peerID string) string { return "kvipc-" + peerID + "-req" }

// respChannel is unique per round trip (see package doc comment): id is
// the originating message's ID, which the daemon echoes into its response.
func respChannel(peerID string, id uint16) string {
	return fmt.Sprintf("kvipc-%s-resp-%d", peerID, id)
}

// Call sends m (with m.ID already set by the caller -- see pkg/shmevent's
// doc comment on why the caller, not this package, chooses it) to the
// daemon serving peerID, signed with priv (nil only for
// EventGetPublicKey/EventGetPrivateKey -- see shmevent.Sign), and returns
// its response. It blocks until the daemon replies or ctx is done.
func Call(ctx context.Context, peerID string, m shmevent.Msg, priv shmevent.PrivateKey) (shmevent.Msg, error) {
	rn := reqChannel(peerID)
	w, err := shmring.CreateShm(rn, capacity, shmring.WithPollInterval(minPoll, maxPoll))
	if err != nil {
		return shmevent.Msg{}, fmt.Errorf("ipc: create request channel: %w", err)
	}

	buf, err := shmevent.Encode(m, priv)
	if err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: encode request: %w", err)
	}
	if _, err := w.WriteContext(ctx, buf); err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: write request: %w", err)
	}
	if err := w.Close(); err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: close request writer: %w", err)
	}

	r, err := openRespWithRetry(ctx, peerID, m.ID)
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

	// The response proves the daemon already fully read the request; safe
	// to remove the request segment now.
	w.CloseStorage()

	resp, _, _, err := shmevent.Decode(respBuf)
	if err != nil {
		return shmevent.Msg{}, err
	}
	return resp, nil
}

func openRespWithRetry(ctx context.Context, peerID string, id uint16) (*shmring.Reader, error) {
	name := respChannel(peerID, id)
	for {
		r, err := shmring.OpenShm(name, capacity, shmring.WithPollInterval(minPoll, maxPoll))
		if err == nil {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("ipc: waiting for response channel %s: %w", name, ctx.Err())
		case <-time.After(openRetryInterval):
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
// signed with priv. It blocks until ctx is done.
func Serve(ctx context.Context, peerID string, priv shmevent.PrivateKey, handle Handler) error {
	name := reqChannel(peerID)

	var lastID uint16
	var haveLastID bool
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

		if haveLastID && m.ID == lastID {
			// The same request segment we already answered, reopened
			// before the client has torn it down -- see the package doc
			// comment on why we reread and dedup by ID instead of
			// blocking for the name to disappear. Give the client a beat
			// to catch up and try again.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(openRetryInterval):
			}
			continue
		}

		// A genuinely new request only appears once the client's previous
		// Call has returned (single in-flight caller), which only happens
		// after it read our previous response -- safe to remove it now.
		cleanupPending()

		resp := handle(ctx, m, crc, sig)
		resp.ID = m.ID
		respWriter, err := sendResponse(ctx, peerID, resp, priv)
		if err != nil {
			return err
		}
		pendingResp = respWriter
		lastID = m.ID
		haveLastID = true
	}
}

func openReqWithRetry(ctx context.Context, name string) (*shmring.Reader, error) {
	for {
		r, err := shmring.OpenShm(name, capacity, shmring.WithPollInterval(minPoll, maxPoll))
		if err == nil {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(openRetryInterval):
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
func sendResponse(ctx context.Context, peerID string, resp shmevent.Msg, priv shmevent.PrivateKey) (*shmring.Writer, error) {
	w, err := shmring.CreateShm(respChannel(peerID, resp.ID), capacity, shmring.WithPollInterval(minPoll, maxPoll))
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
