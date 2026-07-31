package kvmobile

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// channelEvent is one ChannelCallback call recorded by
// recordingChannelCallback -- either an OnData chunk (with its purpose)
// or the terminal OnClosed, distinguished by closed.
type channelEvent struct {
	purpose string
	chunk   string
	closed  bool
	reason  string
}

// recordingChannelCallback is a Go-side ChannelCallback implementation
// for tests -- Kotlin would implement the same interface via gomobile's
// reverse binding, but from Go the interface is just an ordinary type to
// satisfy. Mirrors watch_execute_test.go's recordingCallback shape.
type recordingChannelCallback struct {
	ch chan channelEvent
}

func newRecordingChannelCallback() *recordingChannelCallback {
	return &recordingChannelCallback{ch: make(chan channelEvent, 16)}
}

func (r *recordingChannelCallback) OnData(purpose, chunk string) {
	r.ch <- channelEvent{purpose: purpose, chunk: chunk}
}

func (r *recordingChannelCallback) OnClosed(reason string) {
	r.ch <- channelEvent{closed: true, reason: reason}
}

func (r *recordingChannelCallback) next(t *testing.T, timeout time.Duration) channelEvent {
	t.Helper()
	select {
	case e := <-r.ch:
		return e
	case <-time.After(timeout):
		t.Fatal("timed out waiting for a channel callback event")
		return channelEvent{}
	}
}

// expectNone fails if an event arrives within window -- confirms
// delivery actually stopped (StopChannel), not just that it hasn't
// happened yet.
func (r *recordingChannelCallback) expectNone(t *testing.T, window time.Duration) {
	t.Helper()
	select {
	case e := <-r.ch:
		t.Fatalf("unexpected channel event: purpose=%q chunk=%q closed=%v reason=%q", e.purpose, e.chunk, e.closed, e.reason)
	case <-time.After(window):
	}
}

// TestOpenChannelDeliversAndSends drives OpenChannel end to end: this
// device opens a channel to a real (non-kvmobile) leader, the leader
// sends a chunk back which must reach the callback, and a chunk this
// device sends must reach the leader's own PollChannel.
func TestOpenChannelDeliversAndSends(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cb := newRecordingChannelCallback()
	channelID, err := OpenChannel(leaderPeerID, cb)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { StopChannel(channelID) })

	// The leader must see this device's own channel as an incoming,
	// listenable one.
	leaderChannelID, leaderRemote, ok, err := listenChannelUntil(t, leaderPeerID, 10*time.Second)
	if err != nil {
		t.Fatalf("shmclient.ListenChannel: %v", err)
	}
	if !ok {
		t.Fatal("leader never saw the incoming channel as listenable")
	}
	if leaderRemote != PeerID() {
		t.Fatalf("leader's listen reported remote peer %s, want %s", leaderRemote, PeerID())
	}

	// This device -> leader, tagged with a non-default purpose --
	// confirms purpose survives SendChannelData -> the signed network
	// frame -> the leader's own PollChannel intact.
	if err := SendChannelData(channelID, "video", base64.StdEncoding.EncodeToString([]byte("from device"))); err != nil {
		t.Fatalf("SendChannelData: %v", err)
	}
	gotOnLeader, gotOnLeaderPurpose := pollChannelUntilChunk(t, leaderPeerID, leaderChannelID, 10*time.Second)
	if string(gotOnLeader) != "from device" {
		t.Fatalf("leader received %q, want %q", gotOnLeader, "from device")
	}
	if gotOnLeaderPurpose != shmevent.ChannelPurposeVideo {
		t.Fatalf("leader received purpose %d, want %d (ChannelPurposeVideo)", gotOnLeaderPurpose, shmevent.ChannelPurposeVideo)
	}

	// Leader -> this device, tagged with a different non-default purpose.
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sendCancel()
	if err := shmclient.SendChannel(sendCtx, leaderPeerID, leaderChannelID, shmevent.ChannelPurposeControl, []byte("from leader")); err != nil {
		t.Fatalf("shmclient.SendChannel: %v", err)
	}
	e := cb.next(t, 10*time.Second)
	if e.closed {
		t.Fatalf("unexpected OnClosed(%q) before any data", e.reason)
	}
	if e.purpose != shmevent.ChannelPurposeName(shmevent.ChannelPurposeControl) {
		t.Fatalf("device received purpose %q, want %q", e.purpose, shmevent.ChannelPurposeName(shmevent.ChannelPurposeControl))
	}
	gotOnDevice, err := base64.StdEncoding.DecodeString(e.chunk)
	if err != nil {
		t.Fatalf("decode base64 chunk: %v", err)
	}
	if string(gotOnDevice) != "from leader" {
		t.Fatalf("device received %q, want %q", gotOnDevice, "from leader")
	}
}

// TestListenChannelClaimsIncomingChannel drives the callee-side path: a
// real (non-kvmobile) leader opens a channel *into* this device, and
// kvmobile's own ListenChannel must claim it, returning the JSON
// envelope channelAcceptResult's shape requires (gomobile only supports
// one non-error return value -- see that type's doc comment), and start
// delivering the leader's data to the callback.
func TestListenChannelClaimsIncomingChannel(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	leaderChannelID, err := shmclient.OpenChannel(openCtx, leaderPeerID, PeerID())
	openCancel()
	if err != nil {
		t.Fatalf("shmclient.OpenChannel: %v", err)
	}

	cb := newRecordingChannelCallback()
	listenDone := make(chan struct{})
	var resultJSON string
	var listenErr error
	go func() {
		defer close(listenDone)
		resultJSON, listenErr = ListenChannel(cb)
	}()

	select {
	case <-listenDone:
	case <-time.After(10 * time.Second):
		t.Fatal("ListenChannel never claimed the incoming channel")
	}
	if listenErr != nil {
		t.Fatalf("ListenChannel: %v", listenErr)
	}
	var result channelAcceptResult
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		t.Fatalf("unmarshal ListenChannel result %q: %v", resultJSON, err)
	}
	if result.RemotePeerID != leaderPeerID {
		t.Fatalf("ListenChannel remote peer = %s, want %s", result.RemotePeerID, leaderPeerID)
	}
	t.Cleanup(func() { StopChannel(result.ChannelID) })

	sendCtx, sendCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sendCancel()
	if err := shmclient.SendChannel(sendCtx, leaderPeerID, leaderChannelID, shmevent.ChannelPurposeData, []byte("hello device")); err != nil {
		t.Fatalf("shmclient.SendChannel: %v", err)
	}
	e := cb.next(t, 10*time.Second)
	if e.closed {
		t.Fatalf("unexpected OnClosed(%q) before any data", e.reason)
	}
	got, err := base64.StdEncoding.DecodeString(e.chunk)
	if err != nil {
		t.Fatalf("decode base64 chunk: %v", err)
	}
	if string(got) != "hello device" {
		t.Fatalf("device received %q, want %q", got, "hello device")
	}
}

// TestCloseChannelStopsDeliveryAndNotifiesPeer confirms CloseChannel both
// stops this device's own callback loop and is observed by the peer as
// ChannelPollClosed.
func TestCloseChannelStopsDeliveryAndNotifiesPeer(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cb := newRecordingChannelCallback()
	channelID, err := OpenChannel(leaderPeerID, cb)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}

	leaderChannelID, _, ok, err := listenChannelUntil(t, leaderPeerID, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("shmclient.ListenChannel: ok=%v err=%v", ok, err)
	}

	if err := CloseChannel(channelID); err != nil {
		t.Fatalf("CloseChannel: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, status, err := shmclient.PollChannel(ctx, leaderPeerID, leaderChannelID)
		cancel()
		if err != nil {
			t.Fatalf("shmclient.PollChannel: %v", err)
		}
		if status == shmclient.ChannelClosed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("leader never observed the device's channel as closed")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestStopChannelStopsLocalDeliveryWithoutClosingChannel confirms
// StopChannel's documented scope: it stops this device's own OnData/
// OnClosed delivery, but does NOT close the channel server-side -- the
// leader can still send, and the channel is still poll-able directly via
// shmclient (this device's own local callback loop just isn't watching
// it anymore). Mirrors TestStopWatchExecuteStopsDelivery's shape.
func TestStopChannelStopsLocalDeliveryWithoutClosingChannel(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cb := newRecordingChannelCallback()
	channelID, err := OpenChannel(leaderPeerID, cb)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shmclient.CloseChannel(ctx, leaderPeerID, channelID)
	})

	leaderChannelID, _, ok, err := listenChannelUntil(t, leaderPeerID, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("shmclient.ListenChannel: ok=%v err=%v", ok, err)
	}

	send := func(value string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shmclient.SendChannel(ctx, leaderPeerID, leaderChannelID, shmevent.ChannelPurposeData, []byte(value)); err != nil {
			t.Fatalf("shmclient.SendChannel: %v", err)
		}
	}

	send("before-stop")
	e := cb.next(t, 10*time.Second)
	if e.closed {
		t.Fatalf("unexpected OnClosed(%q) before-stop", e.reason)
	}
	gotBefore, err := base64.StdEncoding.DecodeString(e.chunk)
	if err != nil {
		t.Fatalf("decode base64 chunk: %v", err)
	}
	if string(gotBefore) != "before-stop" {
		t.Fatalf("got %q, want %q", gotBefore, "before-stop")
	}

	StopChannel(channelID)
	send("after-stop")
	cb.expectNone(t, 2*time.Second)

	// The channel itself is still alive -- SendChannelData (going through
	// this device's own daemon, independent of the now-stopped local
	// delivery loop) must still work.
	if err := SendChannelData(channelID, "", base64.StdEncoding.EncodeToString([]byte("still open"))); err != nil {
		t.Fatalf("SendChannelData after StopChannel: %v", err)
	}
	if got, _ := pollChannelUntilChunk(t, leaderPeerID, leaderChannelID, 10*time.Second); string(got) != "still open" {
		t.Fatalf("leader received %q, want %q", got, "still open")
	}
}

func listenChannelUntil(t *testing.T, peerID string, timeout time.Duration) (channelID, remotePeerID string, ok bool, err error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		id, remote, gotOne, listenErr := shmclient.ListenChannel(ctx, peerID)
		cancel()
		if listenErr != nil {
			return "", "", false, listenErr
		}
		if gotOne {
			return id, remote, true, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", "", false, nil
}

func pollChannelUntilChunk(t *testing.T, peerID, channelID string, timeout time.Duration) (chunk []byte, purpose byte) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		chunk, purpose, status, err := shmclient.PollChannel(ctx, peerID, channelID)
		cancel()
		if err != nil {
			t.Fatalf("shmclient.PollChannel: %v", err)
		}
		if status == shmclient.ChannelChunk {
			return chunk, purpose
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("channel chunk never arrived")
	return nil, 0
}
