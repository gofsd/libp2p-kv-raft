package e2erun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// ip4TCPAddrRe matches a plain (no /p2p-circuit relay hop, no /webtransport
// suffix) "/ip4/<host>/tcp/<port>/p2p/<peerid>" address, capturing the port
// and peer id -- unlike android_pair.go's loopbackTCPAddrRe, this
// deliberately doesn't anchor <host> to 127.0.0.1 -- see this file's own
// use of it (RunChannelFileTransferScenario) for why only the port matters
// here.
var ip4TCPAddrRe = regexp.MustCompile(`^/ip4/[^/]+/tcp/(\d+)/p2p/(.+)$`)

// channelTransferChunkSize matches android-app's ChannelFileTransferTest.kt
// (CHUNK_SIZE) and pkg/chandata.MaxChunkSize -- the most a single
// SendChannel call/signed wire frame may carry. Using anything smaller
// just means more, smaller calls for no benefit.
const channelTransferChunkSize = 256 * 1024

// channelTransferTimeoutSeconds bounds how long
// ChannelFileTransferTest.kt waits for desktop's own send direction to
// finish before giving up -- passed through as its "timeoutSeconds"
// instrumentation arg. Generous for a 1GB transfer even at a deliberately
// throttled emulator/loopback rate; this bounds the Kotlin side's own
// internal wait, not the `adb shell am instrument` call itself (see
// channelTransferInstrumentTimeout for that).
const channelTransferTimeoutSeconds = 900

// channelTransferInstrumentTimeout bounds the host-side `adb shell am
// instrument` invocation itself, and the desktop-side context every
// pkg/shmclient call in this file shares -- belt-and-suspenders alongside
// the Kotlin-side timeoutSeconds above: nothing here should ever block
// this host process indefinitely just because a device-side call hung.
const channelTransferInstrumentTimeout = 20 * time.Minute

// channelTransferAndroidResultsPath is where ChannelFileTransferTest.kt
// writes its result -- same external-files-dir convention (no run-as
// needed to adb pull) every other instrumented test in this package uses.
func channelTransferAndroidResultsPath() string {
	return mustResolveAndroidTarget().deviceResultsPath("channel_transfer_result.json")
}

// channelTransferResult mirrors ChannelFileTransferTest.kt's own written
// JSON shape.
type channelTransferResult struct {
	SizeBytes    int64  `json:"sizeBytes"`
	Pass         bool   `json:"pass"`
	Error        string `json:"error"`
	ReceivedHash string `json:"receivedHash"`
	SourceHash   string `json:"sourceHash"`
}

// RunChannelFileTransferScenario proves the Raw Channel feature
// (README.md's "Raw Channel"/"Data plane: pkg/chandata" sections) actually
// sustains a real, large, bidirectional transfer between a genuine desktop
// kvnode and a genuine Android emulator/device -- not just the small
// synthetic chunks pkg/daemon/pkg/kvctl/mobile/kvmobile's own unit and
// instrumented smoke tests use. sizeBytes is the exact payload size to
// generate/verify each direction (both directions use the same size, and
// so -- since the payload is a pure function of size, see
// writeDeterministicTempFile -- the same expected hash).
//
// Mechanism, in order:
//  1. Spin up a brand-new, fully local desktop kvnode (kvctl.AddNode,
//     bootstrapped as its own solo leader -- Channel never touches raft, so
//     there is no cluster to actually join, just a real daemon process to
//     dial) under an isolated registry.EnvHome, so this never touches an
//     operator's real ~/.libp2p-kv-raft nodes.
//  2. Build+install a kvmobile AAR baked with a fresh android identity and
//     this desktop node's own address, rewritten for 10.0.2.2 (see
//     pkg/e2erun/android_pair.go's forwardLoopbackAddr doc comment on why
//     that alias, not the address's own literal host, is what an emulator
//     can actually dial) -- joinSuffrage=learner (buildAndroidAAR's usual
//     default) so android's own Kvmobile.start populates *its own*
//     peerstore with desktop's address the moment it joins.
//  3. android (ChannelFileTransferTest.kt) dials desktop -- and only ever
//     android, never the other way: confirmed live that desktop's own
//     peerstore has no address for android after the join (the join
//     populates *android's* peerstore with desktop's address, not the
//     reverse, and there is no "connect to this explicit address"
//     primitive in pkg/kvctl/mobile/kvmobile independent of a raft join/
//     Channel dial to inject one) -- desktop claims the incoming channel
//     (ListenChannel) and both directions of the transfer run over that
//     *one* channel: desktop sends its generated file first
//     (SendChannel/CloseChannelWrite), then android, having observed that
//     half-close (its own onClosed callback -- the exact half-close
//     property TestChannelCloseWriteLeavesOtherDirectionOpen proves
//     daemon-side), streams its own generated copy of the same
//     deterministic payload back over the same channel while desktop
//     drains it via PollChannel. See ChannelFileTransferTest.kt's own doc
//     comment for the full mobile-side sequence.
//  4. Every file either side created (desktop's generated source file and
//     received copy, android's own generated source file and received
//     copy) is deleted before returning, pass or fail -- see the deferred
//     cleanups below and ChannelFileTransferTest.kt's own doc comment for
//     the Android side. The desktop node itself is also stopped and its
//     data dir removed -- unlike this package's other e2e nodes
//     (deliberately never torn down automatically, see README's own e2e
//     section), this one is purely a throwaway fixture for this one
//     scenario, not a node an operator might want to keep poking at
//     afterward.
func RunChannelFileTransferScenario(repoRoot string, sizeBytes int64) error {
	if reason := androidUnavailable(); reason != "" {
		return fmt.Errorf("e2erun: channel file transfer: skipped: %s", reason)
	}
	serials, err := connectedAndroidSerials()
	if err != nil {
		return fmt.Errorf("e2erun: channel file transfer: %w", err)
	}
	if len(serials) == 0 {
		return fmt.Errorf("e2erun: channel file transfer: no connected android device/emulator")
	}
	serial := serials[0]

	e2eHome, err := os.MkdirTemp("", "kvraft-e2e-channel-transfer-registry-")
	if err != nil {
		return fmt.Errorf("e2erun: channel file transfer: %w", err)
	}
	defer os.RemoveAll(e2eHome)
	prevHome := os.Getenv(registry.EnvHome)
	os.Setenv(registry.EnvHome, e2eHome)
	defer os.Setenv(registry.EnvHome, prevHome)

	fmt.Println("📡 channel file transfer: bootstrapping a fresh local desktop node...")
	desktopPeerID, err := kvctl.AddNode(repoRoot)
	if err != nil {
		return fmt.Errorf("e2erun: channel file transfer: bootstrap desktop node: %w", err)
	}
	defer stopDesktopNode(e2eHome, desktopPeerID)

	desktopAddr, err := kvctl.GetOwnAddr()
	if err != nil {
		return fmt.Errorf("e2erun: channel file transfer: get desktop own addr: %w", err)
	}
	// Unlike android_pair.go's forwardLoopbackAddr (bridging two emulators,
	// both network-isolated from each other, and so specifically matching
	// only a 127.0.0.1 loopback address -- the only kind either of *its*
	// throwaway devices ever has), this desktop node's own daemon listens
	// on every interface (see pkg/daemon's own listen address), so the
	// *host* part of whatever GetOwnAddr returns doesn't actually matter
	// here -- confirmed live: on this development machine specifically,
	// GetOwnAddr's own "best advertised address" logic picked a real
	// Tailscale interface IP over loopback, not the 127.0.0.1
	// loopbackTCPAddrRe alone expects. Only the *port* matters: every
	// emulator can already reach the host machine directly via the fixed
	// alias 10.0.2.2, on whatever port the daemon is actually listening
	// on, regardless of which specific host address was advertised.
	m := ip4TCPAddrRe.FindStringSubmatch(desktopAddr)
	if m == nil {
		return fmt.Errorf("e2erun: channel file transfer: desktop address %q is not a plain /ip4/.../tcp/<port>/p2p/<id> address this scenario knows how to bridge to an emulator", desktopAddr)
	}
	desktopAddrForAndroid := fmt.Sprintf("/ip4/10.0.2.2/tcp/%s/p2p/%s", m[1], m[2])
	fmt.Printf("📡 desktop node %s reachable from the emulator at %s\n", desktopPeerID, desktopAddrForAndroid)

	_, androidPriv, err := e2edata.GenerateIdentity()
	if err != nil {
		return fmt.Errorf("e2erun: channel file transfer: generate android identity: %w", err)
	}
	androidPeerID, err := e2edata.PeerIDFromPrivateKey(androidPriv)
	if err != nil {
		return fmt.Errorf("e2erun: channel file transfer: %w", err)
	}
	androidNode := e2edata.Node{PeerID: androidPeerID, PrivateKey: hex.EncodeToString(androidPriv)}

	fmt.Println("📡 channel file transfer: building + installing the android app...")
	if err := buildAndroidAAR(repoRoot, androidNode, desktopAddrForAndroid, serial); err != nil {
		return fmt.Errorf("e2erun: channel file transfer: %w", err)
	}
	if err := gradleInstall(repoRoot, serial); err != nil {
		return fmt.Errorf("e2erun: channel file transfer: %w", err)
	}

	sourcePath, expectedHash, err := writeDeterministicTempFile(sizeBytes)
	if err != nil {
		return fmt.Errorf("e2erun: channel file transfer: generate payload: %w", err)
	}
	defer os.Remove(sourcePath)
	fmt.Printf("📡 generated a %d-byte deterministic payload (sha256 %s)\n", sizeBytes, expectedHash)

	ctx, cancel := context.WithTimeout(context.Background(), channelTransferInstrumentTimeout)
	defer cancel()

	_ = exec.Command("adb", "shell", "rm", "-f", channelTransferAndroidResultsPath()).Run()

	androidDone := make(chan struct {
		resultJSON []byte
		err        error
	}, 1)
	go func() {
		resultJSON, err := runChannelTransferInstrumentedTest(serial, map[string]string{
			"peerID":         desktopPeerID,
			"sizeBytes":      strconv.FormatInt(sizeBytes, 10),
			"expectedHash":   expectedHash,
			"timeoutSeconds": strconv.Itoa(channelTransferTimeoutSeconds),
		})
		androidDone <- struct {
			resultJSON []byte
			err        error
		}{resultJSON, err}
	}()

	fmt.Println("📡 desktop: waiting for android to dial in...")
	if err := runDuplexTransfer(ctx, desktopPeerID, sourcePath, sizeBytes, expectedHash); err != nil {
		<-androidDone // best-effort: let the background invocation finish/fail on its own terms too
		return fmt.Errorf("e2erun: channel file transfer: desktop side: %w", err)
	}
	fmt.Println("✅ desktop -> android: hash verified, desktop's own files deleted")

	androidResult := <-androidDone
	if androidResult.err != nil {
		return fmt.Errorf("e2erun: channel file transfer: android side: %w", androidResult.err)
	}
	var result channelTransferResult
	if err := json.Unmarshal(androidResult.resultJSON, &result); err != nil {
		return fmt.Errorf("e2erun: channel file transfer: parse android result: %w", err)
	}
	if !result.Pass {
		return fmt.Errorf("e2erun: channel file transfer: android: %s", result.Error)
	}
	if result.ReceivedHash != expectedHash {
		return fmt.Errorf("e2erun: channel file transfer: android received hash %s, want %s", result.ReceivedHash, expectedHash)
	}
	if result.SourceHash != expectedHash {
		return fmt.Errorf("e2erun: channel file transfer: android's own generated payload hashed to %s, want %s (its deterministic generator disagrees with this host's)", result.SourceHash, expectedHash)
	}
	fmt.Println("✅ android -> desktop: hash verified, android's own files deleted")

	return nil
}

// runDuplexTransfer is desktop's own half of the single-channel sequence
// ChannelFileTransferTest.kt drives from the android side (see
// RunChannelFileTransferScenario's own doc comment for the full picture):
// claims the incoming channel android dials in on, sends sourcePath over it
// (SendChannel/CloseChannelWrite), then drains android's own reply
// (PollChannel) into a temp file plus a running SHA-256 until the channel
// reports closed, verifying it against wantHash/sizeBytes. Both of
// desktop's own files (its outgoing source, its received copy) are read/
// written directly -- nothing here touches android's files, which
// ChannelFileTransferTest.kt cleans up on its own side.
func runDuplexTransfer(ctx context.Context, desktopPeerID, sourcePath string, sizeBytes int64, wantHash string) error {
	sess, err := shmclient.Open(ctx, desktopPeerID)
	if err != nil {
		return fmt.Errorf("open desktop session: %w", err)
	}

	var channelID string
	for {
		if ctx.Err() != nil {
			return fmt.Errorf("listen for incoming channel: %w", ctx.Err())
		}
		id, _, ok, err := sess.ListenChannel(ctx)
		if err != nil {
			return fmt.Errorf("listen channel: %w", err)
		}
		if ok {
			channelID = id
			break
		}
	}
	defer sess.CloseChannel(ctx, channelID)

	if err := sendFileOverChannel(ctx, sess, channelID, sourcePath); err != nil {
		return err
	}
	if err := sess.CloseChannelWrite(ctx, channelID); err != nil {
		return fmt.Errorf("close channel write: %w", err)
	}

	recvFile, err := os.CreateTemp("", "kvraft-e2e-channel-transfer-recv-*.bin")
	if err != nil {
		return err
	}
	recvPath := recvFile.Name()
	defer os.Remove(recvPath)
	defer recvFile.Close()

	h := sha256.New()
	var total int64
	for {
		chunk, _, status, err := sess.PollChannel(ctx, channelID)
		if err != nil {
			return fmt.Errorf("poll channel: %w", err)
		}
		switch status {
		case shmclient.ChannelChunk:
			if _, err := recvFile.Write(chunk); err != nil {
				return fmt.Errorf("write received chunk: %w", err)
			}
			h.Write(chunk)
			total += int64(len(chunk))
		case shmclient.ChannelClosed:
			if total != sizeBytes {
				return fmt.Errorf("received %d bytes from android, want %d", total, sizeBytes)
			}
			gotHash := hex.EncodeToString(h.Sum(nil))
			if gotHash != wantHash {
				return fmt.Errorf("received payload hash %s from android, want %s", gotHash, wantHash)
			}
			return nil
		}
	}
}

// sendFileOverChannel reads path in channelTransferChunkSize pieces,
// sending each through sess.SendChannel -- desktop's own real-file-backed
// equivalent of pkg/kvctl.pumpChannelSend's stdin loop (see that function's
// doc comment; this scenario drives pkg/shmclient directly rather than
// through pkg/kvctl.OpenChannel/mage openchannel's own os.Stdin-bound
// signature, since a long-running orchestrator process has no clean way to
// swap its own os.Stdin for the duration of one call the way a short-lived
// mage/CLI invocation naturally can).
func sendFileOverChannel(ctx context.Context, sess *shmclient.Session, channelID, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, channelTransferChunkSize)
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

// runChannelTransferInstrumentedTest triggers ChannelFileTransferTest via
// `adb shell am instrument`, then pulls and returns its
// channel_transfer_result.json -- mirrors android.go's own
// runInstrumentedTest/runUICommandTest shape (see those functions' doc
// comments), just with this test's own instrumentation args instead of
// "rows"/"cases".
func runChannelTransferInstrumentedTest(serial string, instrumentationArgs map[string]string) ([]byte, error) {
	target := mustResolveAndroidTarget()
	args := []string{"shell", "am", "instrument", "-w", "-e", "class", target.channelTestClass()}
	for k, v := range instrumentationArgs {
		args = append(args, "-e", k, v)
	}
	args = append(args, target.runner())
	cmd := exec.Command("adb", args...)
	withSerial(cmd, serial)
	if err := runCaptured(cmd, "am instrument ChannelFileTransferTest"); err != nil {
		return nil, err
	}

	deviceResultsPath := channelTransferAndroidResultsPath()
	localResultsPath := filepath.Join(os.TempDir(), "kvraft-e2e-channel-transfer-result.json")
	defer os.Remove(localResultsPath)
	pull := exec.Command("adb", "pull", deviceResultsPath, localResultsPath)
	withSerial(pull, serial)
	if out, err := pull.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("pull channel transfer results: %w: %s", err, out)
	}
	return os.ReadFile(localResultsPath)
}

// channelTransferPatternBufSize is how large a buffer
// writeDeterministicTempFile fills once and reuses for every write -- see
// that function's own doc comment on why one fixed, 256-divisible buffer
// is enough for the whole file regardless of size.
const channelTransferPatternBufSize = 1 << 20

// writeDeterministicTempFile creates a temp file and fills it with exactly
// sizeBytes of the same deterministic byte(i % 256) pattern
// ChannelFileTransferTest.kt's own writeDeterministicFile uses, returning
// its path and lowercase-hex SHA-256. Because 256 evenly divides
// channelTransferPatternBufSize, one buffer filled once up front is
// byte-for-byte identical for every aligned write -- no per-byte modulo
// work on the hot path, and (since content is a pure function of size) the
// exact same hash this function returns for size N is what a receiver on
// *either* platform, given the same N, independently arrives at too.
func writeDeterministicTempFile(sizeBytes int64) (path string, hashHex string, err error) {
	f, err := os.CreateTemp("", "kvraft-e2e-channel-transfer-send-*.bin")
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	buf := make([]byte, channelTransferPatternBufSize)
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

// stopDesktopNode kills the local kvnode process RunChannelFileTransferScenario
// bootstrapped (looked up by PID from the isolated registry it ran under --
// e2eHome, see registry.NodeInfo.PID) and removes its data directory --
// unlike this package's other e2e desktop nodes, this one is a pure
// throwaway fixture for one scenario (see RunChannelFileTransferScenario's
// own doc comment on why), not left running for an operator to inspect
// afterward. Best-effort: logs rather than fails the scenario if any step
// here has trouble, since by this point the scenario's own pass/fail
// already reflects what actually matters.
func stopDesktopNode(e2eHome, peerID string) {
	reg, err := registry.Open()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: channel file transfer: cleanup: open registry: %v\n", err)
		return
	}
	info, ok, err := reg.Get(peerID)
	if err != nil || !ok {
		fmt.Fprintf(os.Stderr, "e2erun: channel file transfer: cleanup: registry has no entry for %s\n", peerID)
		return
	}
	if info.PID != 0 {
		if proc, err := os.FindProcess(info.PID); err == nil {
			_ = proc.Kill()
		}
	}
	if info.DataDir != "" {
		_ = os.RemoveAll(info.DataDir)
	}
	_ = e2eHome // data dir removal above already covers the node's own files; e2eHome itself is removed by the caller's own defer
}
