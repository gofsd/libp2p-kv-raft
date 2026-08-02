package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestChannelDataplaneDesktopThroughputBenchmark measures the Channel data
// plane's own throughput ceiling between two real, separate desktop nodes
// on this machine -- no Android emulator, no JNI/gomobile boundary, no
// base64 -- as the diagnostic baseline for isolating how much of the
// mobile leg's own measured throughput (see
// pkg/e2erun/android_channel_transfer.go's doc comment, and README's "Raw
// Channel"/"Data plane: pkg/chandata" sections for the feature this
// exercises) is the data plane itself versus that platform's own
// additional overhead (base64 inflation/CPU, the gomobile JNI boundary,
// and the emulator's own virtual networking stack).
//
// One channel, dialed only by a, carries both directions in sequence --
// mirrors android_channel_transfer.go's own design (see that file's doc
// comment for why a single dial, reused both ways via CloseChannelWrite's
// half-close, rather than two independent dials): this test's own
// connectPeers call (a.host.Connect(ctx, b's real address)) only
// establishes a *direct* connection from a's side, and there's no
// guarantee b's own peerstore ends up with a dialable address back --
// exactly the asymmetry confirmed live against a real desktop+Android
// pair, so this benchmark plays it safe rather than assuming symmetric
// dialing works here just because both nodes happen to be real desktop
// processes this time.
//
// Skipped under -short (mage test's own default): a real ~1GiB two-way
// transfer takes real wall-clock time, not appropriate for the routine
// fast unit test suite. Run explicitly:
//
//	go test -run TestChannelDataplaneDesktopThroughputBenchmark -v ./pkg/daemon/...
func TestChannelDataplaneDesktopThroughputBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("bulk-transfer throughput benchmark; run without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tmpDir := t.TempDir()
	a := startChannelDataplaneTestNode(t, filepath.Join(tmpDir, "a"))
	b := startChannelDataplaneTestNode(t, filepath.Join(tmpDir, "b"))
	connectPeers(t, ctx, a, b)

	sessA, err := shmclient.Open(ctx, a.peerID)
	if err != nil {
		t.Fatalf("shmclient.Open(a): %v", err)
	}
	sessB, err := shmclient.Open(ctx, b.peerID)
	if err != nil {
		t.Fatalf("shmclient.Open(b): %v", err)
	}

	// 1GiB -- same size the original ask was framed around; unlike the
	// Android emulator's own run, this host has no meaningful storage
	// constraint forcing a smaller size.
	const sizeBytes = 1 << 30

	sourcePath, wantHash, err := writeDeterministicBenchFile(sizeBytes)
	if err != nil {
		t.Fatalf("generate payload: %v", err)
	}
	defer os.Remove(sourcePath)
	t.Logf("generated a %d-byte deterministic payload (sha256 %s)", sizeBytes, wantHash)

	channelID, err := sessA.OpenChannel(ctx, b.peerID)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	bChannelID, remotePeerID := listenChannelSessionUntilClaimed(t, ctx, sessB)
	if remotePeerID != a.peerID {
		t.Fatalf("listen reported remote peer %q, want %q", remotePeerID, a.peerID)
	}

	t.Log("a -> b...")
	aToB, err := runChannelDirectionBench(ctx, sessA, channelID, sessB, bChannelID, sourcePath, sizeBytes)
	if err != nil {
		t.Fatalf("a -> b: %v", err)
	}
	if aToB.hash != wantHash {
		t.Fatalf("a -> b: received hash %s, want %s", aToB.hash, wantHash)
	}
	logChannelThroughput(t, "a -> b", sizeBytes, aToB.elapsed)

	t.Log("b -> a...")
	bToA, err := runChannelDirectionBench(ctx, sessB, bChannelID, sessA, channelID, sourcePath, sizeBytes)
	if err != nil {
		t.Fatalf("b -> a: %v", err)
	}
	if bToA.hash != wantHash {
		t.Fatalf("b -> a: received hash %s, want %s", bToA.hash, wantHash)
	}
	logChannelThroughput(t, "b -> a", sizeBytes, bToA.elapsed)

	logChannelThroughput(t, "combined (both directions, sequential)", 2*sizeBytes, aToB.elapsed+bToA.elapsed)

	if err := sessB.CloseChannel(ctx, bChannelID); err != nil {
		t.Fatalf("b CloseChannel: %v", err)
	}
	if err := sessA.CloseChannel(ctx, channelID); err != nil {
		t.Fatalf("a CloseChannel: %v", err)
	}
}

// channelDirectionResult is runChannelDirectionBench's own outcome: the
// received payload's SHA-256 and how long the exchange took, start (the
// sender's first SendChannel call) to finish (the receiver observing
// ChannelClosed, which only happens once CloseChannelWrite's own drain
// guarantee has already been satisfied server-side -- see that event's
// doc comment -- so elapsed genuinely covers "every byte reached the
// wire and was drained," not just "the sender's loop returned").
type channelDirectionResult struct {
	hash    string
	elapsed time.Duration
}

// runChannelDirectionBench streams sourcePath from (fromSess,
// fromChannelID) to (toSess, toChannelID) -- send and receive running
// concurrently, exactly like a real caller (pkg/kvctl.pumpChannel/
// android's own ChannelFileTransferTest.kt) would, not a send-then-poll
// sequence that would just measure round-trip latency instead of
// throughput.
func runChannelDirectionBench(ctx context.Context, fromSess *shmclient.Session, fromChannelID string, toSess *shmclient.Session, toChannelID string, sourcePath string, sizeBytes int64) (channelDirectionResult, error) {
	start := time.Now()
	sendDone := make(chan error, 1)
	go func() {
		if err := sendFileOverChannelBench(ctx, fromSess, fromChannelID, sourcePath); err != nil {
			sendDone <- err
			return
		}
		sendDone <- fromSess.CloseChannelWrite(ctx, fromChannelID)
	}()

	h := sha256.New()
	var total int64
	for {
		chunk, _, status, err := toSess.PollChannel(ctx, toChannelID)
		if err != nil {
			return channelDirectionResult{}, fmt.Errorf("poll channel: %w", err)
		}
		switch status {
		case shmclient.ChannelChunk:
			h.Write(chunk)
			total += int64(len(chunk))
		case shmclient.ChannelClosed:
			elapsed := time.Since(start)
			if err := <-sendDone; err != nil {
				return channelDirectionResult{}, fmt.Errorf("send side: %w", err)
			}
			if total != sizeBytes {
				return channelDirectionResult{}, fmt.Errorf("received %d bytes, want %d", total, sizeBytes)
			}
			return channelDirectionResult{hash: hex.EncodeToString(h.Sum(nil)), elapsed: elapsed}, nil
		}
	}
}

// sendFileOverChannelBench reads path in chandata.MaxChunkSize pieces,
// sending each through sess.SendChannel -- the same shape
// pkg/e2erun/android_channel_transfer.go's own sendFileOverChannel uses
// (duplicated here rather than shared: that one lives in a different
// package, _test.go files can't be imported, and it's a handful of lines).
func sendFileOverChannelBench(ctx context.Context, sess *shmclient.Session, channelID, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, chandata.MaxChunkSize)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if sendErr := sess.SendChannel(ctx, channelID, shmevent.ChannelPurposeData, buf[:n]); sendErr != nil {
				return fmt.Errorf("send channel data: %w", sendErr)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read %s: %w", path, err)
		}
	}
}

// channelBenchPatternBufSize is how large a buffer
// writeDeterministicBenchFile fills once and reuses for every write --
// same reasoning as pkg/e2erun.channelTransferPatternBufSize (256 evenly
// divides it, so one buffer filled once up front is byte-for-byte
// identical for every aligned write).
const channelBenchPatternBufSize = 1 << 20

// writeDeterministicBenchFile creates a temp file and fills it with
// exactly sizeBytes of the same deterministic byte(i % 256) pattern
// pkg/e2erun.writeDeterministicTempFile and android-app's
// ChannelFileTransferTest.kt both use, returning its path and
// lowercase-hex SHA-256.
func writeDeterministicBenchFile(sizeBytes int64) (path string, hashHex string, err error) {
	f, err := os.CreateTemp("", "kvraft-channel-bench-*.bin")
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	buf := make([]byte, channelBenchPatternBufSize)
	for i := range buf {
		buf[i] = byte(i % 256)
	}

	h := sha256.New()
	var written int64
	for written < sizeBytes {
		n := int64(len(buf))
		if remaining := sizeBytes - written; n > remaining {
			n = remaining
		}
		if _, err := f.Write(buf[:n]); err != nil {
			os.Remove(f.Name())
			return "", "", err
		}
		h.Write(buf[:n])
		written += n
	}
	return f.Name(), hex.EncodeToString(h.Sum(nil)), nil
}

// logChannelThroughput logs label's throughput in MiB/s for sizeBytes
// moved in elapsed.
func logChannelThroughput(t *testing.T, label string, sizeBytes int64, elapsed time.Duration) {
	t.Helper()
	mib := float64(sizeBytes) / (1024 * 1024)
	t.Logf("%s: %d bytes in %s (%.1f MiB/s)", label, sizeBytes, elapsed.Round(time.Millisecond), mib/elapsed.Seconds())
}
