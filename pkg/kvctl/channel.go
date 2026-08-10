package kvctl

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/chandata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// channelPollInterval bounds how often pumpChannelRecv/ListenChannel's
// poll loops sleep between empty results -- shorter than dispatch.go's
// own defaultDispatchPollInterval, since this is an interactive terminal
// pipe where latency matters more than a command-dispatch poll.
const channelPollInterval = 100 * time.Millisecond

// channelSendChunkSize bounds how many bytes pumpChannelSend reads from
// stdin per SendChannel call -- set to chandata.MaxChunkSize (the most a
// single SendChannel call may ever carry, see that constant's doc
// comment) rather than the old, much smaller shmevent.ChannelValueSize:
// SendChannel is a pkg/chandata ring write now, not a per-chunk IPC round
// trip, so there's no longer any reason to read stdin in small pieces --
// bigger reads here mean fewer, larger signed wire frames end to end,
// which is the single biggest lever on this pipe's real throughput (see
// pkg/chandata's doc comment).
const channelSendChunkSize = chandata.MaxChunkSize

// OpenChannel implements `mage openchannel <peerID> <purpose>`: opens a
// persistent, bidirectional stream from the current node to peerID,
// tagging everything this process sends with purpose (resolved via
// shmevent.ChannelPurposeFromName -- "" for the default data purpose,
// "control"/"video"/a numeric string for anything else), and pumps this
// process's own stdin/stdout through it -- everything read from stdin is
// sent to peerID, everything peerID sends back is written to stdout --
// until stdin reaches EOF, the remote side closes the channel, or this
// process receives SIGINT/SIGTERM. See shmevent.EventChannelOpen's doc
// comment for the wire design.
func OpenChannel(peerID, purposeName string) error {
	purpose, ok := shmevent.ChannelPurposeFromName(purposeName)
	if !ok {
		return fmt.Errorf("open channel: unknown purpose %q", purposeName)
	}

	reg, err := registry.Open()
	if err != nil {
		return err
	}
	ownPeerID, err := reg.Current()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	sess, err := shmclient.Open(ctx, ownPeerID)
	cancel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	openCtx, openCancel := context.WithTimeout(context.Background(), ipcTimeout)
	channelID, err := sess.OpenChannel(openCtx, peerID)
	openCancel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	return pumpChannel(sess, channelID, purpose)
}

// ListenChannel implements `mage listenchannel <purpose>`: blocks until
// another peer opens an incoming channel to the current node, then pumps
// stdin/stdout through it exactly like OpenChannel does -- the
// callee-side counterpart. purpose tags this process's own outgoing data
// the same way OpenChannel's does; the incoming side's purpose is
// whatever the remote peer tagged it with, per chunk (see
// pumpChannelRecv). Prints the remote peer id to stderr once claimed;
// stdout is reserved for the pipe itself.
func ListenChannel(purposeName string) error {
	purpose, ok := shmevent.ChannelPurposeFromName(purposeName)
	if !ok {
		return fmt.Errorf("listen channel: unknown purpose %q", purposeName)
	}

	reg, err := registry.Open()
	if err != nil {
		return err
	}
	ownPeerID, err := reg.Current()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	sess, err := shmclient.Open(ctx, ownPeerID)
	cancel()
	if err != nil {
		return fmt.Errorf("listen channel: %w", err)
	}

	var channelID, remotePeerID string
	for {
		listenCtx, listenCancel := context.WithTimeout(context.Background(), ipcTimeout)
		id, remote, ok, err := sess.ListenChannel(listenCtx)
		listenCancel()
		if err != nil {
			return fmt.Errorf("listen channel: %w", err)
		}
		if ok {
			channelID, remotePeerID = id, remote
			break
		}
		time.Sleep(channelPollInterval)
	}
	fmt.Fprintf(os.Stderr, "channel from %s\n", remotePeerID)

	return pumpChannel(sess, channelID, purpose)
}

// pumpChannel is OpenChannel/ListenChannel's shared body once a
// channelID is in hand: pumpChannelSend chunks os.Stdin into SendChannel
// calls tagged with purpose (in its own goroutine) until EOF, then
// half-closes (see pumpChannelSend's own doc comment for why a half-
// rather than full close matters here); pumpChannelRecv polls
// PollChannel and writes chunks to os.Stdout (also in its own goroutine)
// until the channel reports closed. Waits for *both* directions to
// finish -- one side reaching EOF must not cut off whatever the peer
// still has left to send -- then fully closes the channel. Returns
// immediately on SIGINT/SIGTERM instead, abandoning whichever
// goroutine(s) are still blocked (Go's stdlib has no way to interrupt a
// blocking os.Stdin.Read or an in-flight ipc.Call, but that's fine: this
// process is about to exit either way).
func pumpChannel(sess *shmclient.Session, channelID string, purpose byte) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	sendDone := make(chan error, 1)
	go func() { sendDone <- pumpChannelSend(sess, channelID, purpose) }()

	recvDone := make(chan error, 1)
	go func() { recvDone <- pumpChannelRecv(sess, channelID) }()

	var sendErr, recvErr error
	sendFinished, recvFinished := false, false
	for !sendFinished || !recvFinished {
		select {
		case <-sigCh:
			ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
			sess.CloseChannel(ctx, channelID)
			cancel()
			return nil
		case sendErr = <-sendDone:
			sendFinished = true
		case recvErr = <-recvDone:
			recvFinished = true
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	sess.CloseChannel(ctx, channelID)
	cancel()

	if sendErr != nil {
		return fmt.Errorf("channel: %w", sendErr)
	}
	return recvErr
}

// pumpChannelSend chunks os.Stdin into SendChannel calls tagged with
// purpose until EOF, then half-closes (CloseChannelWrite, not
// CloseChannel) -- reaching EOF only means this side has nothing more to
// *send*; the remote peer may still have data in flight the other
// direction, which a full close would cut off before pumpChannelRecv
// ever saw it.
func pumpChannelSend(sess *shmclient.Session, channelID string, purpose byte) error {
	buf := make([]byte, channelSendChunkSize)
	for {
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
			sendErr := sess.SendChannel(ctx, channelID, purpose, buf[:n])
			cancel()
			if sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
			closeErr := sess.CloseChannelWrite(ctx, channelID)
			cancel()
			return closeErr
		}
	}
}

// pumpChannelRecv discards each chunk's purpose -- stdout piping stays
// purpose-agnostic; an operator piping raw bytes through `mage
// openchannel`/`listenchannel` doesn't need the tag echoed back.
func pumpChannelRecv(sess *shmclient.Session, channelID string) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
		chunk, _, status, err := sess.PollChannel(ctx, channelID)
		cancel()
		if err != nil {
			return err
		}
		switch status {
		case shmclient.ChannelChunk:
			if _, err := os.Stdout.Write(chunk); err != nil {
				return err
			}
		case shmclient.ChannelClosed:
			return nil
		default:
			time.Sleep(channelPollInterval)
		}
	}
}
