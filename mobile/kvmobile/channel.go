package kvmobile

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// ChannelCallback is a gomobile-bindable interface Kotlin implements to
// receive a raw channel's incoming data -- OpenChannel/ListenChannel's
// reverse-binding counterpart to ExecuteCallback/CommandDispatchHandler
// (see kvmobile.go). See pkg/shmevent.EventChannelOpen's doc comment for
// the underlying wire design: a persistent, unreplicated, bidirectional
// byte pipe to another peer, the mobile port of desktop's `mage
// openchannel`/`listenchannel`.
type ChannelCallback interface {
	// OnData delivers one received chunk, base64-encoded -- gomobile's
	// bindings are string-only across the boundary (see this package's
	// other reverse-binding interfaces), and unlike ExecuteCallback's raw
	// UTF-8 string (fine for Execute's typical text payloads), arbitrary
	// binary channel data would be mangled by JNI string marshaling.
	// Called on the channel's own pump goroutine, never the caller's --
	// see ExecuteCallback.OnNotification's identical threading note.
	OnData(chunk string)
	// OnClosed is called exactly once, when the channel itself ends
	// (either side closed it, or an error occurred) -- reason is empty
	// for a clean close. Never called just because StopChannel stopped
	// this device's own local delivery loop; that's a local-only action,
	// not necessarily the channel actually ending (see StopChannel's doc
	// comment).
	OnClosed(reason string)
}

// channelPollInterval/channelPollCallTimeout mirror
// watchExecutePollInterval/watchExecutePollCallTimeout's own reasoning in
// kvmobile.go (PollChannel is a local, non-blocking, in-memory check,
// not a store read).
const (
	channelPollInterval    = 200 * time.Millisecond
	channelPollCallTimeout = 2 * time.Second
)

// channelWatch is one active channel's pump loop stop handle -- mirrors
// dispatch.go's commandDispatch shape.
type channelWatch struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var (
	channelMu      sync.Mutex
	channelWatches = map[string]channelWatch{}
)

// OpenChannel opens a raw, persistent, bidirectional byte pipe from this
// device to peerID (see shmevent.EventChannelOpen's doc comment) and
// starts a background pump loop delivering incoming data to cb.OnData/
// cb.OnClosed until StopChannel(channelID) is called or the channel ends
// on its own. Returns the freshly minted channelID every subsequent
// SendChannelData/CloseChannel/StopChannel call needs.
func OpenChannel(peerID string, cb ChannelCallback) (channelID string, err error) {
	if cb == nil {
		return "", fmt.Errorf("kvmobile: OpenChannel: cb must not be nil")
	}
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	channelID, err = sess.OpenChannel(ctx, peerID)
	if err != nil {
		return "", fmt.Errorf("kvmobile: open channel: %w", err)
	}
	startChannelWatch(channelID, cb)
	return channelID, nil
}

var (
	listenMu     sync.Mutex
	listenCancel context.CancelFunc
)

// ListenChannel blocks until another peer opens an incoming channel to
// this device, then starts the same background pump loop OpenChannel
// does -- the callee-side counterpart. Blocks internally polling; call
// StopListenChannel (e.g. from a Kotlin lifecycle callback like
// onPause) to cancel a still-pending call. Only one ListenChannel call
// may usefully be pending at a time -- a second call replaces the
// first's cancellation handle, mirroring WatchExecute's own "replace,
// don't stack" shape (see stopWatchExecuteLocked).
// channelAcceptResult is ListenChannel's JSON return shape -- gomobile
// bindings only support one non-error return value (see PollExecute's
// identical pollExecuteResult envelope in kvmobile.go), so channelID and
// remotePeerID can't be two separate Go return values the way
// pkg/shmclient.Session.ListenChannel's own signature has them.
type channelAcceptResult struct {
	ChannelID    string `json:"channel_id"`
	RemotePeerID string `json:"remote_peer_id"`
}

func ListenChannel(cb ChannelCallback) (string, error) {
	if cb == nil {
		return "", fmt.Errorf("kvmobile: ListenChannel: cb must not be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	listenMu.Lock()
	listenCancel = cancel
	listenMu.Unlock()
	defer func() {
		listenMu.Lock()
		if listenCancel != nil {
			listenCancel()
			listenCancel = nil
		}
		listenMu.Unlock()
	}()

	for {
		if ctx.Err() != nil {
			return "", fmt.Errorf("kvmobile: ListenChannel: cancelled")
		}

		sess, sessErr := currentSession()
		if sessErr != nil {
			if sleepOrCancelled(ctx, channelPollInterval) {
				return "", fmt.Errorf("kvmobile: ListenChannel: cancelled")
			}
			continue
		}

		pollCtx, pollCancel := context.WithTimeout(ctx, channelPollCallTimeout)
		id, remote, ok, listenErr := sess.ListenChannel(pollCtx)
		pollCancel()
		if listenErr != nil || !ok {
			if sleepOrCancelled(ctx, channelPollInterval) {
				return "", fmt.Errorf("kvmobile: ListenChannel: cancelled")
			}
			continue
		}

		startChannelWatch(id, cb)
		out, err := json.Marshal(channelAcceptResult{ChannelID: id, RemotePeerID: remote})
		if err != nil {
			return "", fmt.Errorf("kvmobile: encode listen channel result: %w", err)
		}
		return string(out), nil
	}
}

// StopListenChannel cancels a currently-blocked ListenChannel call, if
// any. Safe to call when nothing is pending (a no-op); does not affect
// already-open channels or their running pump loops.
func StopListenChannel() {
	listenMu.Lock()
	defer listenMu.Unlock()
	if listenCancel != nil {
		listenCancel()
	}
}

// SendChannelData writes one chunk of raw bytes (base64-encoded, the
// inverse of ChannelCallback.OnData) to channelID -- see
// shmevent.EventChannelSend's doc comment.
func SendChannelData(channelID, base64Chunk string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	chunk, err := base64.StdEncoding.DecodeString(base64Chunk)
	if err != nil {
		return fmt.Errorf("kvmobile: send channel data: decode base64: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.SendChannel(ctx, channelID, chunk); err != nil {
		return fmt.Errorf("kvmobile: send channel data: %w", err)
	}
	return nil
}

// CloseChannel ends channelID outright -- see shmevent.EventChannelClose's
// doc comment -- and also stops this device's own local pump loop for it
// (see StopChannel), since there's nothing left for it to watch.
func CloseChannel(channelID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	closeErr := sess.CloseChannel(ctx, channelID)
	StopChannel(channelID)
	if closeErr != nil {
		return fmt.Errorf("kvmobile: close channel: %w", closeErr)
	}
	return nil
}

// StopChannel stops channelID's background pump loop, if any, and waits
// for it to actually exit before returning -- a local-only action: it
// does NOT itself close the channel server-side (see CloseChannel for
// that), the same "stop watching, don't necessarily end the thing being
// watched" scope StopWatchExecute/StopCommandDispatcher already have.
// Safe to call when nothing is running for it (a no-op).
func StopChannel(channelID string) {
	channelMu.Lock()
	defer channelMu.Unlock()
	stopChannelWatchLocked(channelID)
}

// startChannelWatch registers and starts channelID's background pump
// loop -- shared by OpenChannel/ListenChannel once a channelID is in
// hand. Replaces any previous watch already running for the same
// channelID first (shouldn't normally happen, a channelID is freshly
// minted per call, but mirrors RunCommandDispatcher's own defensive
// "replace, don't stack" precedent).
func startChannelWatch(channelID string, cb ChannelCallback) {
	channelMu.Lock()
	defer channelMu.Unlock()
	stopChannelWatchLocked(channelID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	channelWatches[channelID] = channelWatch{cancel: cancel, done: done}
	go runChannelWatch(ctx, done, channelID, cb)
}

// stopChannelWatchLocked requires channelMu already held.
func stopChannelWatchLocked(channelID string) {
	w, ok := channelWatches[channelID]
	if !ok {
		return
	}
	w.cancel()
	<-w.done
	delete(channelWatches, channelID)
}

// runChannelWatch is startChannelWatch's background loop body: polls
// PollChannel and invokes cb.OnData for each received chunk (base64-
// encoded), until the channel reports closed (calls cb.OnClosed once
// and returns) or ctx is cancelled (StopChannel, or a replacing
// startChannelWatch call -- returns without calling OnClosed, since the
// channel itself hasn't necessarily ended, just this device's local
// delivery of it). Recovers a panicking callback the same way
// dispatch.go's runCommandHandlerSafely does, so one misbehaving Kotlin
// implementation can't take down this loop -- a deliberate improvement
// over WatchExecute's own callback loop in kvmobile.go, which has no
// such recovery.
func runChannelWatch(ctx context.Context, done chan struct{}, channelID string, cb ChannelCallback) {
	defer close(done)

	for {
		if ctx.Err() != nil {
			return
		}

		sess, err := currentSession()
		if err != nil {
			if sleepOrCancelled(ctx, channelPollInterval) {
				return
			}
			continue
		}

		pollCtx, cancel := context.WithTimeout(ctx, channelPollCallTimeout)
		chunk, status, pollErr := sess.PollChannel(pollCtx, channelID)
		cancel()
		if pollErr != nil {
			if sleepOrCancelled(ctx, channelPollInterval) {
				return
			}
			continue
		}

		switch status {
		case shmclient.ChannelChunk:
			callOnDataSafely(cb, base64.StdEncoding.EncodeToString(chunk))
			// Loop again immediately (no wait) to drain any backlog
			// quickly -- mirrors runExecuteWatch's identical shape.
		case shmclient.ChannelClosed:
			callOnClosedSafely(cb, "")
			return
		default:
			if sleepOrCancelled(ctx, channelPollInterval) {
				return
			}
		}
	}
}

func callOnDataSafely(cb ChannelCallback, chunk string) {
	defer func() { recover() }()
	cb.OnData(chunk)
}

func callOnClosedSafely(cb ChannelCallback, reason string) {
	defer func() { recover() }()
	cb.OnClosed(reason)
}

// sleepOrCancelled sleeps for d, returning true early if ctx is
// cancelled first -- the shared wait shape every poll loop in this file
// uses.
func sleepOrCancelled(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}
