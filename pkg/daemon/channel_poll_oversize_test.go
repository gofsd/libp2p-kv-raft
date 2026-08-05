package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestChannelPollDeliversAChunkTheDataPlaneAllows pins the one size
// relationship this package's two channel paths have to agree on, and
// which nothing else checks.
//
// A chunk may be as large as channelMaxChunkSize (chandata.MaxChunkSize,
// 256KB) on the wire -- that is the whole point of the data plane, and
// pumpChannelUpload really does forward chunks that size. Every such chunk
// is also pushed into the session's poll inbox (pumpChannelReads calls
// pushChunk unconditionally), and EventChannelPoll hands one whole inbox
// entry back per call. But a poll *response* is an ordinary shmevent.Msg,
// so its Value is bounded by valueSizeFor(EventChannelPoll) =
// ChannelValueSize, 16KB.
//
// So a peer sending a chunk between those two limits produces a response
// its own transport cannot encode: pkg/ipc.sendResponse fails with "value
// too long", the local caller gets an error instead of its data, and --
// the part that makes this worse than a clear rejection -- popChunk has
// already removed the entry, so the chunk is gone. Retrying polls past it.
//
// Android is where this actually bites: pkg/chandata's rings need named
// shared memory, which ipc_android.go exists precisely because Android
// lacks, so a phone reads a channel through this poll path and no other
// (kvmobile's WatchChannel). A desktop peer using the data plane at its
// documented chunk size therefore sends a phone chunks it can never read.
//
// What this asserts is the invariant: whatever a peer is allowed to put on
// the wire, the poll path either delivers it or says precisely why it
// cannot -- never emits a response its own transport will refuse, and
// never blocks every later chunk behind one it will never take.
func TestChannelPollDeliversAChunkTheDataPlaneAllows(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	a := startExecuteTestNode(t, filepath.Join(tmpDir, "a"))
	b := startExecuteTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)
	grantChannelAccess(t, a, b)

	openResp := callLocal(t, ctx, a, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(b.peerID), ID: 1}, a.ed25519Priv)
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	aChannelID := string(openResp.Value)

	// Sent through dispatchChannelSend directly, not a local IPC call: this
	// is the same sess.write pumpChannelUpload uses for a chunk that came
	// off a chandata ring, and it is the only way to put a chunk this size
	// on the wire (an IPC EventChannelSend would be capped by its own
	// encode long before reaching here). A local caller cannot cause this;
	// a peer can, which is the point.
	const oversize = shmevent.ChannelValueSize + 1
	big := make([]byte, oversize)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	if err := a.dispatchChannelSend(aChannelID, shmevent.ChannelPurposeData, big); err != nil {
		t.Fatalf("send a %d-byte chunk (channelMaxChunkSize is %d, so this is legal): %v", oversize, channelMaxChunkSize, err)
	}
	// A small chunk right behind it: whatever happens to the big one, this
	// must still arrive. An oversized chunk left sitting at the head of the
	// inbox would starve every chunk after it.
	small := []byte("readable by any reader")
	if err := a.dispatchChannelSend(aChannelID, shmevent.ChannelPurposeData, small); err != nil {
		t.Fatalf("send the follow-up chunk: %v", err)
	}

	bChannelID, _ := listenChannelUntilClaimed(t, ctx, b)

	// b polls exactly as a phone's WatchChannel does, and every response
	// has to survive the encode its own transport performs
	// (pkg/ipc.sendResponse) -- callLocal stops one step short of that, so
	// check it here.
	deadline := time.After(20 * time.Second)
	var sawOversizeReport bool
	for {
		resp := callLocal(t, ctx, b, shmevent.Msg{EventType: shmevent.EventChannelPoll, Value: []byte(bChannelID), ID: 2}, b.ed25519Priv)
		if resp.EventType == shmevent.EventError {
			if !strings.Contains(string(resp.Value), "larger than a poll response can carry") {
				t.Fatalf("channel_poll failed for an unexpected reason: %s", resp.Value)
			}
			sawOversizeReport = true
			continue
		}
		if _, err := shmevent.Encode(resp, b.ed25519Priv); err != nil {
			t.Fatalf("channel_poll produced a response its own transport cannot encode: %v -- see this test's doc comment", err)
		}
		status, _, chunk, err := shmevent.DecodeChannelPollResponse(resp.Value)
		if err != nil {
			t.Fatalf("DecodeChannelPollResponse: %v", err)
		}
		if status == shmevent.ChannelPollChunk {
			if string(chunk) == string(small) {
				if !sawOversizeReport {
					t.Fatal("the oversized chunk was skipped silently -- a poll reader has no way to know a chunk it will never receive went past")
				}
				return
			}
			t.Fatalf("unexpected chunk of %d bytes", len(chunk))
		}
		select {
		case <-deadline:
			t.Fatalf("the follow-up chunk never arrived (last status %d) -- an undeliverable chunk is blocking the ones behind it", status)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
