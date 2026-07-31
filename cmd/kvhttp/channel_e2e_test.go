package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestChannelOpenBidirectionalSendPollOverHTTP is the end-to-end happy
// path for the "Raw Channel" feature (see README.md's "Raw Channel"
// section) driven entirely through kvhttp's HTTP bridge, rather than
// pkg/daemon's own in-process TestChannelOpenBidirectionalSendPoll (which
// calls handleShmEvent directly and never touches kvctl-cli or HTTP at
// all): two real kvnode processes, a real kvctl-cli binary, and a real
// http.Handler wired up exactly like cmd/kvhttp's own main() does.
//
// This intentionally does not re-derive channel semantics pkg/daemon's own
// suite already covers at the daemon level (permit gating, forged-signature
// rejection, remote-caller rejection, reap eviction) -- kvhttp is a
// transparent proxy onto kvctl-cli sendevent (see this package's own doc
// comment), so none of that behavior differs by going through it. What
// this test actually proves is the bridge itself: that a caller who only
// has each node's bearer token (no local kvctl-cli invocation, no shared
// filesystem access) can drive a full open/send/poll/close channel
// lifecycle purely over HTTP, and that kvhttp correctly scopes each
// request to the right node via its token.
func TestChannelOpenBidirectionalSendPollOverHTTP(t *testing.T) {
	// No t.Parallel(): this test needs its own KVSTORE_HOME via t.Setenv,
	// which the testing package forbids combining with parallel subtests
	// (they'd share the one process-wide environment).
	root := repoRoot(t)
	t.Setenv(registry.EnvHome, t.TempDir())
	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	// fastRaftArgs shortens hashicorp/raft's WAN-appropriate default
	// timeouts for a same-machine test -- mirrors pkg/kvctl/catalog_test.go's
	// identical constant; channels themselves never touch raft, but B's
	// join (which is what actually connects the two libp2p hosts this test
	// needs) does.
	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	peerA, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("bootstrap node A: %v", err)
	}
	peerB, err := kvctl.AddNodeWithArgs(root, fastRaftArgs, peerA)
	if err != nil {
		t.Fatalf("join node B to A: %v", err)
	}

	kvctlBin := buildKvctlCLI(t, root)
	h := &handler{reg: reg, kvctlBin: kvctlBin}
	srv := httptest.NewServer(http.HandlerFunc(h.handleCommand))
	t.Cleanup(srv.Close)

	tokenA, err := reg.AccessToken(peerA)
	if err != nil {
		t.Fatalf("AccessToken(A): %v", err)
	}
	tokenB, err := reg.AccessToken(peerB)
	if err != nil {
		t.Fatalf("AccessToken(B): %v", err)
	}

	// A opens a channel to B.
	openResp := postEvent(t, srv, tokenA, shmevent.Msg{EventType: shmevent.EventChannelOpen, Value: []byte(peerB)})
	if openResp.EventType == shmevent.EventError {
		t.Fatalf("channel_open rejected: %s", openResp.Value)
	}
	channelIDA := string(openResp.Value)
	if channelIDA == "" {
		t.Fatal("channel_open returned an empty channel id")
	}

	// B claims the incoming channel via channel_listen.
	channelIDB, remotePeer := listenChannelUntilClaimed(t, srv, tokenB)
	if remotePeer != peerA {
		t.Fatalf("channel_listen reported remote peer %q, want %q", remotePeer, peerA)
	}

	// A -> B.
	payloadAtoB, err := shmevent.EncodeChannelSendPayload(channelIDA, shmevent.ChannelPurposeData, []byte("hello from A"))
	if err != nil {
		t.Fatalf("EncodeChannelSendPayload(A->B): %v", err)
	}
	sendResp := postEvent(t, srv, tokenA, shmevent.Msg{EventType: shmevent.EventChannelSend, Value: payloadAtoB})
	if sendResp.EventType == shmevent.EventError {
		t.Fatalf("channel_send (A->B) rejected: %s", sendResp.Value)
	}
	if got := pollChannelUntilChunk(t, srv, tokenB, channelIDB); string(got) != "hello from A" {
		t.Fatalf("B received %q, want %q", got, "hello from A")
	}

	// B -> A, on B's own independently minted channel id -- proving this
	// is a genuine bidirectional pipe, not just A's own request echoed
	// back.
	payloadBtoA, err := shmevent.EncodeChannelSendPayload(channelIDB, shmevent.ChannelPurposeData, []byte("hello from B"))
	if err != nil {
		t.Fatalf("EncodeChannelSendPayload(B->A): %v", err)
	}
	sendResp = postEvent(t, srv, tokenB, shmevent.Msg{EventType: shmevent.EventChannelSend, Value: payloadBtoA})
	if sendResp.EventType == shmevent.EventError {
		t.Fatalf("channel_send (B->A) rejected: %s", sendResp.Value)
	}
	if got := pollChannelUntilChunk(t, srv, tokenA, channelIDA); string(got) != "hello from B" {
		t.Fatalf("A received %q, want %q", got, "hello from B")
	}

	// A closes its end outright; B must observe it as closed on its next
	// poll rather than hanging or erroring.
	closeResp := postEvent(t, srv, tokenA, shmevent.Msg{EventType: shmevent.EventChannelClose, Value: []byte(channelIDA)})
	if closeResp.EventType == shmevent.EventError {
		t.Fatalf("channel_close rejected: %s", closeResp.Value)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, _, _ := pollChannel(t, srv, tokenB, channelIDB)
		if status == shmevent.ChannelPollClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("B never observed the channel as closed after A's channel_close")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// postEvent POSTs req (as the same e2edata.Event JSON shape kvctl-cli
// sendevent/kvhttp's own doc comment describes) to srv with token as the
// bearer, and decodes the response back into a shmevent.Msg -- the HTTP
// counterpart to pkg/daemon/channel_test.go's callLocal.
func postEvent(t *testing.T, srv *httptest.Server, token string, req shmevent.Msg) shmevent.Msg {
	t.Helper()
	body, err := json.Marshal(e2edata.EventFromMsg(req))
	if err != nil {
		t.Fatalf("encode request event: %v", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, srv.URL+"/command", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST /command: %v", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	var ev e2edata.Event
	if err := json.Unmarshal(respBody, &ev); err != nil {
		t.Fatalf("decode response event (status %d, body %q): %v", resp.StatusCode, respBody, err)
	}
	return ev.ToMsg()
}

// pollChannel is postEvent's EventChannelPoll-specific counterpart,
// mirroring pkg/daemon/channel_test.go's identically named helper.
func pollChannel(t *testing.T, srv *httptest.Server, token, channelID string) (status, purpose byte, chunk []byte) {
	t.Helper()
	resp := postEvent(t, srv, token, shmevent.Msg{EventType: shmevent.EventChannelPoll, Value: []byte(channelID)})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("channel_poll rejected: %s", resp.Value)
	}
	status, purpose, chunk, err := shmevent.DecodeChannelPollResponse(resp.Value)
	if err != nil {
		t.Fatalf("DecodeChannelPollResponse: %v", err)
	}
	return status, purpose, chunk
}

// pollChannelUntilChunk polls channelID via srv until a chunk arrives or
// the deadline passes -- also asserts its purpose round-trips as
// ChannelPurposeData, since every send in this file uses that purpose.
func pollChannelUntilChunk(t *testing.T, srv *httptest.Server, token, channelID string) []byte {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, purpose, chunk := pollChannel(t, srv, token, channelID)
		if status == shmevent.ChannelPollChunk {
			if purpose != shmevent.ChannelPurposeData {
				t.Fatalf("got purpose %d, want %d (ChannelPurposeData)", purpose, shmevent.ChannelPurposeData)
			}
			return chunk
		}
		if time.Now().After(deadline) {
			t.Fatal("channel chunk never arrived")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// listenChannelUntilClaimed polls EventChannelListen via srv until an
// incoming channel is claimed or the deadline passes, returning its local
// channelID and the remote peer id it reports.
func listenChannelUntilClaimed(t *testing.T, srv *httptest.Server, token string) (channelID, remotePeerID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp := postEvent(t, srv, token, shmevent.Msg{EventType: shmevent.EventChannelListen})
		if resp.EventType == shmevent.EventError {
			t.Fatalf("channel_listen rejected: %s", resp.Value)
		}
		if len(resp.Value) > 0 {
			gotID, gotPeer, err := shmevent.DecodeChannelAccept(resp.Value)
			if err != nil {
				t.Fatalf("DecodeChannelAccept: %v", err)
			}
			return string(gotID), string(gotPeer)
		}
		if time.Now().After(deadline) {
			t.Fatal("incoming channel never became listenable")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// buildKvctlCLI builds a real kvctl-cli binary from source into a temp
// dir, with cmd.Dir pinned to root -- unlike resolveKvctlBin's own
// `go build ./cmd/kvctl-cli` (main.go), which assumes the *process's*
// working directory is already the repo root (true for cmd/kvhttp's real
// main() and for mage-invoked contexts, but not for `go test
// ./cmd/kvhttp`, whose cwd is this package's own directory) -- caught
// directly: resolveKvctlBin("") failed here with "directory not found"
// before this existed.
func buildKvctlCLI(t *testing.T, root string) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "kvctl-cli")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/kvctl-cli")
	cmd.Dir = root
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build kvctl-cli: %v", err)
	}
	return out
}

// repoRoot walks up from this test file's location to the module root
// (which contains go.mod and cmd/kvnode/cmd/kvctl-cli), so kvctl can
// `go build` those regardless of the working directory `go test` uses --
// mirrors pkg/kvctl/kvctl_test.go's identically named, unexported helper
// (can't be imported across packages, so duplicated here as the same
// handful of lines rather than exported just for this).
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// killAllRegistered terminates every OS process the registry currently
// knows about -- mirrors pkg/kvctl/kvctl_test.go's identically named
// helper (see that file for why this is one up-front t.Cleanup rather than
// per-node cleanup after each successful AddNode).
func killAllRegistered(t *testing.T, reg *registry.Registry) {
	t.Helper()
	nodes, err := reg.List()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, info := range nodes {
		if info.PID == 0 {
			continue
		}
		proc, err := os.FindProcess(info.PID)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(proc *os.Process) {
			defer wg.Done()
			_ = proc.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() {
				proc.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = proc.Kill()
			}
		}(proc)
	}
	wg.Wait()
}
