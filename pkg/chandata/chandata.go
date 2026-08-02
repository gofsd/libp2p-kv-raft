// Package chandata is pkg/daemon.ChannelProtocolID's high-throughput data
// plane: once a channel's control-plane handshake completes
// (shmevent.EventChannelOpen/Listen, plus shmevent.EventChannelDataReady --
// see that event's doc comment), the actual chunk traffic in both
// directions moves off pkg/ipc's request/response protocol and onto a pair
// of long-lived github.com/gofsd/shmring ring buffers instead: one this
// node's local caller writes into and the daemon drains onto the libp2p
// stream (DirUp), one the daemon writes into (as it reads the stream) and
// the local caller drains (DirDown).
//
// # Why request/response IPC doesn't scale for bulk data
//
// pkg/ipc.Call pays a real cost per round trip -- on desktop, a fresh named
// shmring segment pair (CreateShm/OpenShm, each a real shm_open+mmap and
// later shm_unlink+munmap) plus a capnp Encode/Decode/CRC/Ed25519-sign/
// verify, all to move a single chunk capped at shmevent.ChannelValueSize.
// That's fine for the occasional Set/Get or channel control op, but for
// bulk data it means the sender's stdin-reading loop can't get more than
// one chunk ahead of the network write it's waiting on -- there is no
// pipelining, so throughput is bounded by round-trip latency divided into
// chunk size, not by the actual libp2p connection's bandwidth.
//
// A shmring ring buffer, opened once for the channel's whole lifetime
// instead of once per chunk, removes both costs: TryWrite/TryRead are
// non-blocking memory operations (a poll-with-backoff loop bridges the two
// processes -- there's no cross-process wakeup primitive to block on, see
// shmring's own doc comment), so a producer can run arbitrarily far ahead
// of its consumer, bounded only by the ring's capacity, which is exactly
// the pipelining a bulk transfer needs to actually saturate the
// connection.
//
// # Framing
//
// A shmring ring is a raw byte stream with no message boundaries of its
// own (unlike a single pkg/ipc.Call, which is naturally one message). Each
// logical chunk this package's callers hand to WriteChunk is therefore
// prefixed with a small local-only header -- one purpose byte, then a
// 4-byte big-endian length -- before being written to the ring; ReadChunk
// is the inverse. This header exists purely to recover chunk boundaries
// and purpose tags on the same machine, between two processes that already
// fully trust each other (see pkg/shmevent's doc comment on the shmring
// same-machine trust boundary); it is never sent over the network and
// carries no CRC or signature of its own -- that protection is exactly as
// strong as it needs to be one layer up, where pkg/daemon signs and frames
// each chunk again (shmevent.Encode/EncodeChannelWireChunk) before it ever
// leaves this machine.
//
// # Platform split
//
// chandata_desktop.go (built everywhere except android) names rings after
// (peerID, channelID, dir) and rendezvous the same way pkg/ipc.go's own
// named CreateShm/OpenShm does. chandata_android.go backs the identical
// Create/Open signatures with shmring's ASharedMemory transport and an
// in-process fd handoff, mirroring pkg/ipc/ipc_android.go's own reasoning
// for why Android needs a different rendezvous entirely (no OS-level named
// shared memory) -- see that file's doc comment.
package chandata

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gofsd/shmring"
)

// Dir names which direction of a channel a ring carries, from this node's
// own daemon's perspective -- the shared vocabulary Create/Open (desktop
// and android alike) and the naming/registry scheme built on top of it
// use, so the two platform backends and every caller agree on the same two
// strings.
type Dir string

const (
	// DirUp is the local caller's outgoing ring: the caller creates it
	// (chandata.Create), the daemon opens it as a reader
	// (chandata.Open) and forwards every chunk it yields onto the
	// network.
	DirUp Dir = "up"
	// DirDown is the daemon's outgoing ring: the daemon creates it
	// (chandata.Create) as soon as a channel session exists, the local
	// caller opens it as a reader (chandata.Open) once it has a
	// channelID in hand.
	DirDown Dir = "down"
)

// Capacity is the shared-memory data region size for every data-plane
// ring. Must be a power of two (shmring's own constraint). Sized well
// beyond MaxChunkSize so a producer can queue several chunks ahead of its
// consumer -- the actual pipelining depth this package exists to provide --
// without either side blocking on every single call.
const Capacity = 1 << 20 // 1 MiB

// MaxChunkSize bounds a single WriteChunk call's chunk -- independent of
// (and much larger than) shmevent.ChannelValueSize, which only ever bounded
// EventChannelSend/Poll's IPC payload; a chunk moving through this
// package's rings never touches that transport, so it has no reason to
// share that ceiling. Bigger chunks mean fewer, larger signed wire frames
// per byte transferred once pkg/daemon forwards them (see that package's
// pumpChannelUpload/pumpChannelReads), which is the single biggest lever on
// throughput: Ed25519 signing and the network write syscall both have a
// mostly-fixed per-frame cost, so amortizing it over more bytes matters far
// more than shrinking per-chunk latency does for a bulk transfer.
const MaxChunkSize = 256 * 1024

// chunkHeaderSize is one purpose byte plus a 4-byte big-endian length --
// see this package's doc comment on framing.
const chunkHeaderSize = 1 + 4

// ChunkWriter is the producer side of one data-plane ring, framing each
// WriteChunk call as its own self-delimited record (see this package's doc
// comment). Must only be used from a single goroutine at a time, same
// constraint shmring.Writer itself documents.
type ChunkWriter struct {
	w *shmring.Writer
}

// WriteChunk blocks until purpose+chunk has been written in full, or ctx is
// done first. chunk must not exceed MaxChunkSize.
func (c *ChunkWriter) WriteChunk(ctx context.Context, purpose byte, chunk []byte) error {
	if len(chunk) > MaxChunkSize {
		return fmt.Errorf("chandata: chunk too large: %d bytes (max %d)", len(chunk), MaxChunkSize)
	}
	var hdr [chunkHeaderSize]byte
	hdr[0] = purpose
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(chunk)))
	if _, err := c.w.WriteContext(ctx, hdr[:]); err != nil {
		return fmt.Errorf("chandata: write chunk header: %w", err)
	}
	if len(chunk) == 0 {
		return nil
	}
	if _, err := c.w.WriteContext(ctx, chunk); err != nil {
		return fmt.Errorf("chandata: write chunk payload: %w", err)
	}
	return nil
}

// Close marks this ring closed -- see shmring.Writer.Close's own doc
// comment: already-written chunks remain available for ReadChunk to drain,
// and only once fully drained does the reader start observing io.EOF.
func (c *ChunkWriter) Close() error {
	return c.w.Close()
}

// CloseStorage marks this ring closed and releases its underlying storage
// -- call this once this side is done *writing* to it (not once the other
// side is done reading -- see this package's doc comment on the shmring
// same-machine trust boundary and shmring.Writer.CloseStorage's own doc
// comment on why releasing storage the other process still has mapped is
// safe).
func (c *ChunkWriter) CloseStorage() error {
	return c.w.CloseStorage()
}

// ChunkReader is the consumer side of one data-plane ring. Must only be
// used from a single goroutine at a time, same constraint shmring.Reader
// itself documents.
type ChunkReader struct {
	r *shmring.Reader
}

// ReadChunk blocks until one full purpose-tagged chunk is available, ctx is
// done, or the ring has been closed and fully drained (io.EOF).
func (c *ChunkReader) ReadChunk(ctx context.Context) (purpose byte, chunk []byte, err error) {
	var hdr [chunkHeaderSize]byte
	if err := readFullContext(ctx, c.r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > MaxChunkSize {
		return 0, nil, fmt.Errorf("chandata: peer announced an oversized chunk: %d bytes", n)
	}
	if n == 0 {
		return hdr[0], nil, nil
	}
	buf := make([]byte, n)
	if err := readFullContext(ctx, c.r, buf); err != nil {
		return 0, nil, err
	}
	return hdr[0], buf, nil
}

// readFullContext reads exactly len(buf) bytes from r, blocking (with
// ctx's own cancellation) across as many ReadContext calls as it takes --
// a ring's TryRead/ReadContext may return fewer bytes than requested even
// when more is coming, unlike a fixed-size single-shot channel.
func readFullContext(ctx context.Context, r *shmring.Reader, buf []byte) error {
	for read := 0; read < len(buf); {
		n, err := r.ReadContext(ctx, buf[read:])
		read += n
		if err != nil {
			if err == io.EOF && read == len(buf) {
				return nil
			}
			return err
		}
	}
	return nil
}

// Close releases this side's handle on the ring without affecting the
// writer's data or the other side's own mapping -- call this once this
// side is done *reading* it (see ChunkWriter.CloseStorage's doc comment on
// why only the creator releases storage).
func (c *ChunkReader) Close() error {
	return c.r.Close()
}
