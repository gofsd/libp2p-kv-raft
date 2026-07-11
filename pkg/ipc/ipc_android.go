//go:build android

// Package ipc, on Android, implements the same Call/Serve contract as the
// desktop transport (ipc.go) but over shmring's ASharedMemory backend
// instead of name-based CreateShm/OpenShm.
//
// ASharedMemory has no name-based rendezvous at all -- see
// shmring/backend/android.go's doc comment -- so a segment can only be
// shared by handing over its raw file descriptor directly. That only works
// within a single process (or across processes via Binder, which is
// app-specific plumbing this package doesn't attempt -- see
// shmring/mobile's doc comment). Consequently this transport only supports
// a client and a Serve loop running in the same process, which is exactly
// this project's Android build: the follower daemon and the UI's Set/Get
// calls both run inside the one app process (see the mobile package),
// genuinely exercising shmring's shared-memory ring buffer even though no
// OS process boundary is crossed.
//
// Each Call hands its request segment's fd to the matching Serve loop
// through an in-process Go channel (a "mailbox" keyed by peerID, since
// there's still conceptually one daemon per peer id, same as desktop).
// Serve replies by creating a fresh response segment and handing *its* fd
// back the same way. Because every round trip gets its own fresh segments
// -- never reused, unlike desktop's fixed-name request channel -- there is
// no equivalent of the desktop transport's stale-segment race (see ipc.go)
// to guard against here by construction.
package ipc

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/gofsd/shmring"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// capacity is the shared-memory data region size for both channels. It must
// be a power of two and comfortably fit the larger of an encoded
// request/response (see pkg/shmevent.ValueSize).
const capacity = 4096

// call is what Call hands to Serve via the per-peer mailbox.
type call struct {
	reqFD    int
	respChan chan respHandoff
}

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

var (
	mailboxMu sync.Mutex
	mailboxes = map[string]chan call{}
)

// mailboxFor returns the (lazily created) channel Call and Serve rendezvous
// on for peerID. Unbuffered: a Call's send only completes once a Serve
// loop for the same peerID is actively waiting for it.
func mailboxFor(peerID string) chan call {
	mailboxMu.Lock()
	defer mailboxMu.Unlock()
	ch, ok := mailboxes[peerID]
	if !ok {
		ch = make(chan call)
		mailboxes[peerID] = ch
	}
	return ch
}

// Call sends m (with m.ID already set by the caller -- see pkg/shmevent's
// doc comment on why the caller, not this package, chooses it) to the
// in-process follower daemon serving peerID, signed with priv (nil only
// for EventGetPublicKey/EventGetPrivateKey -- see shmevent.Sign), and
// returns its response. It blocks until the daemon replies or ctx is done.
func Call(ctx context.Context, peerID string, m shmevent.Msg, priv shmevent.PrivateKey) (shmevent.Msg, error) {
	w, fd, err := shmring.CreateAndroidSharedMemory("kvipc-req", capacity)
	if err != nil {
		return shmevent.Msg{}, fmt.Errorf("ipc: create request shm: %w", err)
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

	respChan := make(chan respHandoff, 1)
	select {
	case mailboxFor(peerID) <- call{reqFD: fd, respChan: respChan}:
	case <-ctx.Done():
		w.CloseStorage()
		return shmevent.Msg{}, ctx.Err()
	}

	var rh respHandoff
	select {
	case rh = <-respChan:
	case <-ctx.Done():
		w.CloseStorage()
		return shmevent.Msg{}, ctx.Err()
	}

	r, openErr := shmring.OpenAndroidSharedMemory(rh.fd, capacity)
	close(rh.ack)
	if openErr != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: open response shm: %w", openErr)
	}

	respBuf, err := readAll(ctx, r)
	r.Close()
	if err != nil {
		w.CloseStorage()
		return shmevent.Msg{}, fmt.Errorf("ipc: read response: %w", err)
	}

	// The response proves the daemon already fully read the request (same
	// reasoning as desktop Call, and unconditionally true here since every
	// round trip gets its own fresh segments -- see package doc comment):
	// safe to release the request segment now.
	w.CloseStorage()

	resp, _, _, err := shmevent.Decode(respBuf)
	if err != nil {
		return shmevent.Msg{}, err
	}
	return resp, nil
}

// readAll reads r until EOF -- a capnp message has no fixed size (unlike
// the ipcproto.Request/Response this transport used to carry).
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

// Serve runs the in-process daemon side of the protocol for peerID: it
// repeatedly waits for a Call on the peer's mailbox, dispatches it to
// handle, and hands the response segment's fd back. Responses are signed
// with priv. It blocks until ctx is done.
func Serve(ctx context.Context, peerID string, priv shmevent.PrivateKey, handle Handler) error {
	ch := mailboxFor(peerID)
	for {
		var c call
		select {
		case c = <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}

		r, err := shmring.OpenAndroidSharedMemory(c.reqFD, capacity)
		if err != nil {
			continue
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

		resp := handle(ctx, m, crc, sig)
		resp.ID = m.ID

		w, respFD, err := shmring.CreateAndroidSharedMemory("kvipc-resp", capacity)
		if err != nil {
			continue
		}
		respBuf, err := shmevent.Encode(resp, priv)
		if err != nil {
			w.CloseStorage()
			continue
		}
		if _, err := w.WriteContext(ctx, respBuf); err != nil {
			w.CloseStorage()
			continue
		}
		if err := w.Close(); err != nil {
			w.CloseStorage()
			continue
		}

		ack := make(chan struct{})
		select {
		case c.respChan <- respHandoff{fd: respFD, ack: ack}:
		case <-ctx.Done():
			w.CloseStorage()
			return ctx.Err()
		}
		select {
		case <-ack:
		case <-ctx.Done():
			w.CloseStorage()
			return ctx.Err()
		}
		w.CloseStorage()
	}
}
