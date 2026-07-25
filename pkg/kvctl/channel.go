package kvctl

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// channelPollInterval bounds how often pumpChannelRecv/ListenChannel's
// poll loops sleep between empty results -- shorter than dispatch.go's
// own defaultDispatchPollInterval, since this is an interactive terminal
// pipe where latency matters more than a command-dispatch poll.
const channelPollInterval = 100 * time.Millisecond

// channelSendChunkSize bounds how many bytes pumpChannelSend reads from
// stdin per SendChannel call -- comfortably under shmevent.ValueSize
// (512) once EncodeChannelSendPayload's own length-prefixed channelID
// field is accounted for.
const channelSendChunkSize = 400

// OpenChannel implements `mage openchannel <peerID>`: opens a raw,
// bidirectional byte pipe from the current node to peerID and pumps this
// process's own stdin/stdout through it -- everything read from stdin is
// sent to peerID, everything peerID sends back is written to stdout --
// until stdin reaches EOF, the remote side closes the channel, or this
// process receives SIGINT/SIGTERM. See shmevent.EventChannelOpen's doc
// comment for the wire design.
func OpenChannel(peerID string) error {
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

	return pumpChannel(sess, channelID)
}

// ListenChannel implements `mage listenchannel`: blocks until another
// peer opens an incoming channel to the current node, then pumps stdin/
// stdout through it exactly like OpenChannel does -- the callee-side
// counterpart. Prints the remote peer id to stderr once claimed; stdout
// is reserved for the raw pipe itself.
func ListenChannel() error {
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

	return pumpChannel(sess, channelID)
}

// pumpChannel is OpenChannel/ListenChannel's shared body once a
// channelID is in hand: pumpChannelSend chunks os.Stdin into SendChannel
// calls (in its own goroutine) until EOF, then half-closes (see
// pumpChannelSend's own doc comment for why a half- rather than full
// close matters here); pumpChannelRecv polls PollChannel and writes
// chunks to os.Stdout (also in its own goroutine) until the channel
// reports closed. Waits for *both* directions to finish -- one side
// reaching EOF must not cut off whatever the peer still has left to send
// -- then fully closes the channel. Returns immediately on SIGINT/
// SIGTERM instead, abandoning whichever goroutine(s) are still blocked
// (Go's stdlib has no way to interrupt a blocking os.Stdin.Read or an
// in-flight ipc.Call, but that's fine: this process is about to exit
// either way).
func pumpChannel(sess *shmclient.Session, channelID string) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	sendDone := make(chan error, 1)
	go func() { sendDone <- pumpChannelSend(sess, channelID) }()

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

// pumpChannelSend chunks os.Stdin into SendChannel calls until EOF, then
// half-closes (CloseChannelWrite, not CloseChannel) -- reaching EOF only
// means this side has nothing more to *send*; the remote peer may still
// have data in flight the other direction, which a full close would cut
// off before pumpChannelRecv ever saw it.
func pumpChannelSend(sess *shmclient.Session, channelID string) error {
	buf := make([]byte, channelSendChunkSize)
	for {
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
			sendErr := sess.SendChannel(ctx, channelID, buf[:n])
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

func pumpChannelRecv(sess *shmclient.Session, channelID string) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
		chunk, status, err := sess.PollChannel(ctx, channelID)
		cancel()
		if err != nil {
			return err
		}
		switch status {
		case shmclient.ChannelChunk:
			os.Stdout.Write(chunk)
		case shmclient.ChannelClosed:
			return nil
		default:
			time.Sleep(channelPollInterval)
		}
	}
}
