package shmclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// channelPipe is one open channel's pkg/chandata data-plane handles from
// this local caller's own side: up is this side's producer ring (this
// process created it -- see chandata.ChunkWriter.CloseStorage's doc
// comment on why this side also releases its storage), down is this
// side's consumer ring (the daemon created it -- this side only ever
// Close()s its own mapping of it). See setupChannelData.
//
// A shmring Writer/Reader documents itself as usable from only a single
// goroutine at a time, but this package's own callers make no such
// promise back: mobile/kvmobile in particular hands SendChannelData/
// PollChannel-driving code and CloseChannel out to Kotlin, which is free
// to call them from different threads concurrently -- unlike the old
// design, where every call was a self-contained IPC round trip with
// nothing left to race afterward, up/down are now long-lived objects a
// concurrent Close could free out from under an in-flight
// WriteChunk/ReadChunk. ctx/cancel/mu/closed/wg below exist purely to
// make that safe: every Send/Poll call registers itself (enter) before
// touching up/down and unregisters (leave) after, while
// close/closeUpload cancel ctx first (promptly unblocking anything
// currently inside WriteChunk/ReadChunk, which both respect context
// cancellation) and only then wait for every registered call to actually
// finish before touching the underlying rings themselves.
type channelPipe struct {
	up   *chandata.ChunkWriter
	down *chandata.ChunkReader

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func newChannelPipe(up *chandata.ChunkWriter, down *chandata.ChunkReader) *channelPipe {
	ctx, cancel := context.WithCancel(context.Background())
	return &channelPipe{up: up, down: down, ctx: ctx, cancel: cancel}
}

// enter registers one in-flight Send/Poll/closeUpload call against p,
// returning false (caller must not proceed) if p has already started
// closing. Every successful enter must be matched by exactly one leave.
func (p *channelPipe) enter() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.wg.Add(1)
	return true
}

func (p *channelPipe) leave() { p.wg.Done() }

// closeUpload safely closes p's upload ring writer (CloseChannelWrite's
// implementation) -- serialized against any in-flight SendChannel the
// same way close (below) is, but without tearing the whole pipe down:
// PollChannel keeps working against down afterward.
func (p *channelPipe) closeUpload() {
	if !p.enter() {
		return
	}
	defer p.leave()
	p.up.Close()
}

// close marks p closed (any Send/Poll call that hasn't already entered
// will now fail fast rather than touch up/down), cancels ctx to unblock
// whichever calls are currently in flight, waits for them to actually
// return, and only then releases both rings -- see this type's own doc
// comment for why each step matters.
func (p *channelPipe) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cancel()
	p.wg.Wait()
	p.up.CloseStorage()
	p.down.Close()
}

// mergeCancel returns a context cancelled when either a or b is --
// SendChannel/PollChannel bound their WriteChunk/ReadChunk call by both
// the caller's own ctx and the pipe's ctx (cancelled by channelPipe.close/
// closeUpload), so a call blocked on a full/empty ring is unblocked
// promptly by either the caller giving up or this channel being closed
// from a different goroutine, whichever comes first. The returned cancel
// must always be called to release the association context.AfterFunc
// makes internally.
func mergeCancel(a, b context.Context) (context.Context, context.CancelFunc) {
	merged, cancel := context.WithCancel(a)
	stop := context.AfterFunc(b, cancel)
	return merged, func() { stop(); cancel() }
}

// pollWaitCap bounds how long a single PollChannel call blocks inside
// ReadChunk waiting for the next chunk before reporting ChannelNoData --
// deliberately short (not ctx's own, often much longer, deadline): the
// existing callers in pkg/kvctl/mobile/kvmobile expect a poll call to
// return promptly so their own loop can re-check for a signal to stop
// (SIGINT, a caller-cancelled context) between calls, the same
// responsiveness the old per-chunk IPC round trip had by virtue of never
// blocking server-side in the first place. This still gets the ring's own
// efficient backoff wait (see pkg/chandata) instead of returning
// instantly and relying entirely on the caller's own sleep-then-retry, so
// a chunk that arrives mid-wait is delivered with lower latency than
// before, not higher.
const pollWaitCap = 150 * time.Millisecond

// setupChannelData creates/opens channelID's pkg/chandata ring pair and
// registers it in s.channels, then sends channelDataReady so the daemon
// opens its own end and starts forwarding -- shared by
// OpenChannel/ListenChannel once each has a channelID in hand. On any
// failure it tears down whatever it already created/opened before
// returning the error, so a caller that gives up on a failed Open/Listen
// doesn't leak a half-set-up ring.
func (s *Session) setupChannelData(ctx context.Context, channelID string) (err error) {
	up, err := chandata.Create(s.peerID, channelID, chandata.DirUp)
	if err != nil {
		return fmt.Errorf("shmclient: create upload ring: %w", err)
	}
	defer func() {
		if err != nil {
			up.CloseStorage()
		}
	}()

	down, err := chandata.Open(ctx, s.peerID, channelID, chandata.DirDown)
	if err != nil {
		return fmt.Errorf("shmclient: open download ring: %w", err)
	}
	defer func() {
		if err != nil {
			down.Close()
		}
	}()

	m, err := shmevent.NewChannelDataReady(channelID)
	if err != nil {
		return fmt.Errorf("shmclient: channel_data_ready: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: channel_data_ready: %w", err)
	}
	if err := respErr("channel_data_ready", resp); err != nil {
		return err
	}

	s.channelsMu.Lock()
	s.channels[channelID] = newChannelPipe(up, down)
	s.channelsMu.Unlock()
	return nil
}

// OpenChannel opens a raw, persistent, bidirectional byte pipe from the
// session's own node to destPeerID -- see channelOpen's doc comment in
// api/shmevent.capnp. Unlike Execute, this needs no prior setKey
// registration: channelOpen's peerId field is just destPeerID directly.
// Returns the freshly minted channelID every subsequent
// SendChannel/PollChannel/CloseChannel call on this channel needs. Also
// sets up channelID's pkg/chandata data-plane ring pair
// (setupChannelData) before returning, so every channelID this method
// ever hands back is immediately usable with SendChannel/PollChannel's
// high-throughput ring path.
func (s *Session) OpenChannel(ctx context.Context, destPeerID string) (channelID string, err error) {
	m, err := shmevent.NewChannelOpen(destPeerID)
	if err != nil {
		return "", fmt.Errorf("shmclient: open_channel: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: open_channel: %w", err)
	}
	if err := respErr("open_channel", resp); err != nil {
		return "", err
	}
	channelID, err = resp.ChannelOpen().PeerId()
	if err != nil {
		return "", fmt.Errorf("shmclient: open_channel: %w", err)
	}
	if err := s.setupChannelData(ctx, channelID); err != nil {
		return "", fmt.Errorf("shmclient: open_channel: %w", err)
	}
	return channelID, nil
}

// SendChannel writes one chunk of bytes to channelID, tagged with purpose
// (see shmevent.ChannelPurposeData/Control/Video) -- unlike before, this
// is a pkg/chandata ring write (see OpenChannel/ListenChannel's own doc
// comments), not a per-chunk IPC round trip: it returns once chunk has
// been copied into the ring, which may be before the daemon has actually
// forwarded it onto the wire (see channelDataReady's doc comment in
// api/shmevent.capnp on why CloseChannelWrite, not this call, is where
// that distinction matters).
func (s *Session) SendChannel(ctx context.Context, channelID string, purpose byte, chunk []byte) error {
	pipe, ok := s.channelPipe(channelID)
	if !ok {
		return fmt.Errorf("shmclient: send_channel: no such channel %q", channelID)
	}
	if !pipe.enter() {
		return fmt.Errorf("shmclient: send_channel: channel %q is closing", channelID)
	}
	defer pipe.leave()
	workCtx, cancel := mergeCancel(ctx, pipe.ctx)
	defer cancel()
	if err := pipe.up.WriteChunk(workCtx, purpose, chunk); err != nil {
		return fmt.Errorf("shmclient: send_channel: %w", err)
	}
	return nil
}

// ChannelStatus is PollChannel's three-way result -- see channelPoll's
// doc comment in api/shmevent.capnp.
type ChannelStatus byte

const (
	ChannelNoData ChannelStatus = iota
	ChannelChunk
	ChannelClosed
)

// PollChannel drains one buffered chunk received on channelID since the
// last poll, if any -- a caller loops this (with a short sleep between
// empty polls) to observe a channel's incoming traffic, the same "no push
// transport" shape PollExecute already uses. purpose (see
// shmevent.ChannelPurposeData/Control/Video) is only meaningful when
// status is ChannelChunk. Reads from channelID's pkg/chandata download
// ring (see OpenChannel/ListenChannel), blocking briefly (pollWaitCap) for
// a chunk to arrive rather than returning ChannelNoData instantly.
func (s *Session) PollChannel(ctx context.Context, channelID string) (chunk []byte, purpose byte, status ChannelStatus, err error) {
	pipe, ok := s.channelPipe(channelID)
	if !ok {
		return nil, 0, ChannelNoData, fmt.Errorf("shmclient: poll_channel: no such channel %q", channelID)
	}
	if !pipe.enter() {
		// Closed by a concurrent CloseChannel -- same terminal state a
		// still-open pipe eventually reports via io.EOF below.
		return nil, 0, ChannelClosed, nil
	}
	defer pipe.leave()
	workCtx, wcancel := mergeCancel(ctx, pipe.ctx)
	defer wcancel()
	waitCtx, cancel := context.WithTimeout(workCtx, pollWaitCap)
	defer cancel()
	purpose, chunk, err = pipe.down.ReadChunk(waitCtx)
	if err != nil {
		if err == io.EOF {
			return nil, 0, ChannelClosed, nil
		}
		if ctx.Err() != nil {
			// The caller's own ctx is what actually ran out, not just this
			// call's internal pollWaitCap -- a real error, not "no data
			// yet."
			return nil, 0, ChannelNoData, fmt.Errorf("shmclient: poll_channel: %w", err)
		}
		if pipe.ctx.Err() != nil {
			// Closed by a concurrent CloseChannel mid-wait.
			return nil, 0, ChannelClosed, nil
		}
		return nil, 0, ChannelNoData, nil
	}
	return chunk, purpose, ChannelChunk, nil
}

// ListenChannel claims one pending incoming channel -- see channelListen's
// doc comment in api/shmevent.capnp. ok is false if none are currently
// pending; a caller loops this the same way PollChannel loops for
// incoming traffic. Also sets up channelID's pkg/chandata data-plane ring
// pair (setupChannelData) before returning, same as OpenChannel.
func (s *Session) ListenChannel(ctx context.Context) (channelID, remotePeerID string, ok bool, err error) {
	m, err := shmevent.NewChannelListen()
	if err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %w", err)
	}
	if err := respErr("listen_channel", resp); err != nil {
		return "", "", false, err
	}
	grp := resp.ChannelListen()
	id, err := grp.ChannelId()
	if err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %w", err)
	}
	if id == "" {
		return "", "", false, nil
	}
	remote, err := grp.RemotePeerId()
	if err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %w", err)
	}
	if err := s.setupChannelData(ctx, id); err != nil {
		return "", "", false, fmt.Errorf("shmclient: listen_channel: %w", err)
	}
	return id, remote, true, nil
}

// CloseChannel ends channelID outright -- see channelClose's doc comment
// in api/shmevent.capnp. Also releases channelID's pkg/chandata ring
// pair, if this session ever set one up for it (OpenChannel/
// ListenChannel) -- this side created the upload ring, so it releases its
// storage outright (ChunkWriter.CloseStorage); it only ever opened the
// download ring as a reader, so it just releases its own mapping
// (ChunkReader.Close). Best-effort regardless of whether the wire call
// itself succeeds, so a daemon that's already gone doesn't leak this
// side's own ring storage.
func (s *Session) CloseChannel(ctx context.Context, channelID string) error {
	m, err := shmevent.NewChannelClose(channelID)
	var resp shmevent.Msg
	if err == nil {
		resp, err = s.call(ctx, m)
	}

	s.channelsMu.Lock()
	pipe, ok := s.channels[channelID]
	delete(s.channels, channelID)
	s.channelsMu.Unlock()
	if ok {
		pipe.close()
	}

	if err != nil {
		return fmt.Errorf("shmclient: close_channel: %w", err)
	}
	return respErr("close_channel", resp)
}

// CloseChannelWrite half-closes channelID's outgoing direction only --
// "I have nothing more to send," not "end the channel outright" (that's
// CloseChannel). A caller whose own local input source (e.g. os.Stdin)
// reaches a clean EOF should call this rather than CloseChannel, then
// keep polling for whatever the remote peer still has left to send. First
// closes (not releases -- see CloseChannel) this side's own upload ring
// writer, then sends channelCloseWrite -- see that variant's doc comment
// in api/shmevent.capnp for why the daemon deliberately delays its
// response until every chunk this call's Close just made visible has
// actually been forwarded onto the wire, so this call returning is still
// a genuine "everything I sent already reached the network" guarantee,
// the same one the old per-chunk-synchronous design had for free.
func (s *Session) CloseChannelWrite(ctx context.Context, channelID string) error {
	if pipe, ok := s.channelPipe(channelID); ok {
		pipe.closeUpload()
	}

	m, err := shmevent.NewChannelCloseWrite(channelID)
	if err != nil {
		return fmt.Errorf("shmclient: close_channel_write: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: close_channel_write: %w", err)
	}
	return respErr("close_channel_write", resp)
}

// channelPipe looks up channelID's data-plane ring pair, set up by
// OpenChannel/ListenChannel.
func (s *Session) channelPipe(channelID string) (*channelPipe, bool) {
	s.channelsMu.Lock()
	defer s.channelsMu.Unlock()
	p, ok := s.channels[channelID]
	return p, ok
}

// OpenChannel is the one-shot convenience wrapper around
// Open+Session.OpenChannel.
func OpenChannel(ctx context.Context, peerID, destPeerID string) (channelID string, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.OpenChannel(ctx, destPeerID)
}

// Unlike OpenChannel/ListenChannel/CloseChannel above, SendChannel and
// PollChannel have no one-shot convenience wrapper here: both now read
// from/write to a pkg/chandata ring pair that only the Session which
// itself called OpenChannel/ListenChannel for that channelID ever set up
// (see setupChannelData) -- a fresh Open()'d Session has no way to
// rediscover it, so a one-shot wrapper could never do anything but fail.
// A caller needs the same *Session across a channel's Open/Listen,
// Send/Poll, and Close calls regardless, which pkg/kvctl and
// mobile/kvmobile both already do.

// ListenChannel is the one-shot convenience wrapper around
// Open+Session.ListenChannel.
func ListenChannel(ctx context.Context, peerID string) (channelID, remotePeerID string, ok bool, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", "", false, err
	}
	return s.ListenChannel(ctx)
}

// CloseChannel is the one-shot convenience wrapper around
// Open+Session.CloseChannel.
func CloseChannel(ctx context.Context, peerID, channelID string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.CloseChannel(ctx, channelID)
}
