//go:build !android

package chandata

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gofsd/shmring"
)

// newTestRingPair builds a real (peerID, channelID, DirUp) ring and both
// its ends -- w is the producer WriteChunk callers under test use, r is
// the consumer ReadChunk callers under test use. Direction is arbitrary
// for these tests (framing logic doesn't care which of Up/Down it's
// carrying); DirUp is used throughout for concreteness.
func newTestRingPair(t *testing.T) (w *ChunkWriter, r *ChunkReader) {
	t.Helper()
	peerID := fmt.Sprintf("chandata-test-%d", time.Now().UnixNano())
	channelID := "chan-1"

	w, err := Create(peerID, channelID, DirUp)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { w.CloseStorage() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err = Open(ctx, peerID, channelID, DirUp)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })

	return w, r
}

// TestWriteReadChunkRoundTrip is the framing contract's basic promise:
// purpose and payload both survive exactly, chunk boundaries are
// recovered from the ring's otherwise-undelimited byte stream, and
// several chunks written ahead of the reader (the whole point of this
// package over per-chunk IPC, per its own doc comment) come back out in
// the same order with no bytes dropped, merged, or split.
func TestWriteReadChunkRoundTrip(t *testing.T) {
	w, r := newTestRingPair(t)

	type chunk struct {
		purpose byte
		data    []byte
	}
	chunks := []chunk{
		{purpose: 1, data: []byte("first")},
		{purpose: 2, data: []byte("second, a bit longer")},
		{purpose: 3, data: []byte("x")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, c := range chunks {
		if err := w.WriteChunk(ctx, c.purpose, c.data); err != nil {
			t.Fatalf("WriteChunk(%d, %q): %v", c.purpose, c.data, err)
		}
	}

	for i, want := range chunks {
		purpose, data, err := r.ReadChunk(ctx)
		if err != nil {
			t.Fatalf("ReadChunk #%d: %v", i, err)
		}
		if purpose != want.purpose {
			t.Fatalf("chunk #%d purpose = %d, want %d", i, purpose, want.purpose)
		}
		if string(data) != string(want.data) {
			t.Fatalf("chunk #%d data = %q, want %q", i, data, want.data)
		}
	}
}

// TestWriteReadZeroLengthChunk checks the zero-length short-circuit both
// WriteChunk and ReadChunk document: a purpose-only chunk with no payload
// round-trips as an empty (nil) chunk with its purpose byte intact,
// rather than being conflated with "no chunk at all" (io.EOF) or blocking
// waiting for a payload that was never coming.
func TestWriteReadZeroLengthChunk(t *testing.T) {
	w, r := newTestRingPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const purpose = 7
	if err := w.WriteChunk(ctx, purpose, nil); err != nil {
		t.Fatalf("WriteChunk with nil chunk: %v", err)
	}
	gotPurpose, gotChunk, err := r.ReadChunk(ctx)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if gotPurpose != purpose {
		t.Fatalf("purpose = %d, want %d", gotPurpose, purpose)
	}
	if len(gotChunk) != 0 {
		t.Fatalf("chunk = %q, want empty", gotChunk)
	}

	// A real chunk sent right after must still be recovered correctly --
	// the zero-length short-circuit must not desynchronize framing for
	// whatever comes next.
	if err := w.WriteChunk(ctx, 9, []byte("after-empty")); err != nil {
		t.Fatalf("WriteChunk after empty chunk: %v", err)
	}
	gotPurpose, gotChunk, err = r.ReadChunk(ctx)
	if err != nil {
		t.Fatalf("ReadChunk after empty chunk: %v", err)
	}
	if gotPurpose != 9 || string(gotChunk) != "after-empty" {
		t.Fatalf("got (purpose=%d, chunk=%q), want (purpose=9, chunk=%q)", gotPurpose, gotChunk, "after-empty")
	}
}

// TestWriteChunkRejectsOversizedChunk checks WriteChunk's own guard: a
// chunk over MaxChunkSize is rejected immediately, before anything is
// written to the ring, rather than silently truncated or written in a way
// that would desync the reader's framing.
func TestWriteChunkRejectsOversizedChunk(t *testing.T) {
	w, r := newTestRingPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oversized := make([]byte, MaxChunkSize+1)
	if err := w.WriteChunk(ctx, 1, oversized); err == nil {
		t.Fatal("WriteChunk accepted a chunk over MaxChunkSize, want an error")
	}

	// Nothing should have reached the ring -- a legitimate chunk sent
	// right after must be the first (and only) thing ReadChunk sees.
	if err := w.WriteChunk(ctx, 2, []byte("ok")); err != nil {
		t.Fatalf("WriteChunk after rejected oversized chunk: %v", err)
	}
	purpose, data, err := r.ReadChunk(ctx)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if purpose != 2 || string(data) != "ok" {
		t.Fatalf("got (purpose=%d, data=%q), want (purpose=2, data=%q) -- the rejected oversized WriteChunk call must not have written a partial header", purpose, data, "ok")
	}
}

// TestReadChunkRejectsOversizedAnnouncedLength checks ReadChunk's own,
// independent guard against a chunk header that *announces* a length over
// MaxChunkSize -- WriteChunk can never produce one (see
// TestWriteChunkRejectsOversizedChunk), so this can only be reached by a
// header this side of the trust boundary didn't write itself: a
// corrupted ring, or (per this package's doc comment on the shmring
// same-machine trust boundary) a fellow local process writing directly to
// a ring name it has no business touching. ReadChunk must reject it
// outright rather than attempt to allocate/read a buffer of the
// attacker-controlled size.
func TestReadChunkRejectsOversizedAnnouncedLength(t *testing.T) {
	peerID := fmt.Sprintf("chandata-oversize-test-%d", time.Now().UnixNano())
	channelID := "chan-1"

	rawWriter, err := shmring.CreateShm(ringName(peerID, channelID, DirUp), Capacity)
	if err != nil {
		t.Fatalf("CreateShm: %v", err)
	}
	defer rawWriter.CloseStorage()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := Open(ctx, peerID, channelID, DirUp)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Hand-craft a header announcing a chunk larger than MaxChunkSize --
	// exactly the shape WriteChunk itself refuses to ever produce.
	var hdr [5]byte
	hdr[0] = 1 // purpose
	binary.BigEndian.PutUint32(hdr[1:], MaxChunkSize+1)
	if _, err := rawWriter.WriteContext(ctx, hdr[:]); err != nil {
		t.Fatalf("write forged header: %v", err)
	}

	_, _, err = r.ReadChunk(ctx)
	if err == nil {
		t.Fatal("ReadChunk accepted a header announcing a chunk over MaxChunkSize, want an error")
	}
}

// TestWriterCloseLeavesBufferedChunksReadable pins ChunkWriter.Close's own
// documented contract: closing the writer (not releasing its storage)
// still lets the reader drain whatever was already written, and only
// once fully drained does the reader observe io.EOF -- the property a
// sender relies on when it has nothing more to say but the receiver still
// has catching up to do.
func TestWriterCloseLeavesBufferedChunksReadable(t *testing.T) {
	w, r := newTestRingPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.WriteChunk(ctx, 1, []byte("buffered")); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	purpose, data, err := r.ReadChunk(ctx)
	if err != nil {
		t.Fatalf("ReadChunk of already-buffered chunk after Close: %v", err)
	}
	if purpose != 1 || string(data) != "buffered" {
		t.Fatalf("got (purpose=%d, data=%q), want (purpose=1, data=%q)", purpose, data, "buffered")
	}

	if _, _, err := r.ReadChunk(ctx); err != io.EOF {
		t.Fatalf("ReadChunk after drain = %v, want io.EOF", err)
	}
}
