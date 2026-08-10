package daemon

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// maxExecuteInbox bounds executeInbox: a queue nothing ever drains (no
// local caller ever polls) would otherwise grow without limit as long as
// other nodes keep sending EventExecute notifications. Past this many
// pending entries, the oldest is dropped to make room for the newest --
// same trade-off a best-effort notification queue with no persistence
// already implies (see executeInbox's doc comment).
const maxExecuteInbox = 256

// executeNotification is one queued EventExecute delivery: senderPeerID is
// the string pkg/shmevent.DecodeExecuteNotification returned (the sending
// node's own peer id, already signature-verified against it by
// handleExecuteStream before queuing), payload is that same call's payload.
type executeNotification struct {
	senderPeerID []byte
	payload      []byte
}

// executeInbox is a bounded FIFO queue of executeNotification, guarded by
// a mutex -- deliberately the simplest thing that could work rather than
// a channel, since EventPollExecute needs a non-blocking "is anything
// there" drain (a closed/empty channel read blocks or needs a select,
// where a plain slice-under-a-mutex just returns ok=false).
type executeInbox struct {
	mu      sync.Mutex
	entries []executeNotification
}

func newExecuteInbox() *executeInbox {
	return &executeInbox{}
}

func (q *executeInbox) push(senderPeerID, payload []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) >= maxExecuteInbox {
		q.entries = q.entries[1:]
	}
	q.entries = append(q.entries, executeNotification{senderPeerID: senderPeerID, payload: payload})
}

func (q *executeInbox) pop() (executeNotification, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return executeNotification{}, false
	}
	n := q.entries[0]
	q.entries = q.entries[1:]
	return n, true
}

// dispatchExecute implements EventExecute (see that event's doc comment
// in pkg/shmevent): resolves SourceID/DestinationID against this node's
// own registry, confirms the caller isn't claiming some other node as the
// sender (this node can only ever relay the peer-to-peer hop below under
// its own identity, since that's the key it signs with), then delivers
// it. Never touches n.store or raft.
func (n *Node) dispatchExecute(ctx context.Context, m shmevent.Msg) error {
	grp := m.Execute()
	senderKey, ok := n.registry.Lookup(grp.SourceId())
	if !ok {
		return fmt.Errorf("execute: no peer id registered under source id %d -- send SetKey first", grp.SourceId())
	}
	if string(senderKey) != n.peerID {
		return fmt.Errorf("execute: source %q is not this node's own peer id (%s)", senderKey, n.peerID)
	}
	destKey, ok := n.registry.Lookup(grp.DestinationId())
	if !ok {
		return fmt.Errorf("execute: no peer id registered under destination id %d -- send SetKey first", grp.DestinationId())
	}
	destPeerID, err := peer.Decode(string(destKey))
	if err != nil {
		return fmt.Errorf("execute: invalid destination peer id %q: %w", destKey, err)
	}
	value, err := grp.Value()
	if err != nil {
		return fmt.Errorf("execute: value: %w", err)
	}
	return n.sendExecute(ctx, destPeerID, value)
}

// sendExecute dials dest directly over ExecuteProtocolID -- a fresh
// peer-to-peer libp2p stream between two raft node processes, entirely
// outside raft consensus -- and hands it an EventExecute message carrying
// EncodeExecuteNotification(this node's own peer id, payload), signed
// with this node's own key. See handleExecuteStream for the receiving
// side.
func (n *Node) sendExecute(ctx context.Context, dest peer.ID, payload []byte) error {
	// dest is resolved from this node's own registry (dispatchExecute), not
	// necessarily a raft member -- same reasoning as join/recruit/
	// exec-invite-redeem's own relayCtx: without this, a dest reachable only
	// through a /p2p-circuit address hangs until ctx's deadline instead of
	// using the relayed connection.
	relayCtx := network.WithAllowLimitedConn(ctx, "execute")
	s, err := n.host.NewStream(relayCtx, dest, ExecuteProtocolID)
	if err != nil {
		return fmt.Errorf("execute: open stream to %s: %w", dest, err)
	}
	defer s.Close()

	notif, err := shmevent.NewExecuteNotification(n.peerID, payload)
	if err != nil {
		return fmt.Errorf("execute: build notification: %w", err)
	}
	buf, err := shmevent.Encode(notif, n.ed25519Priv)
	if err != nil {
		return fmt.Errorf("execute: encode message: %w", err)
	}
	if _, err := s.Write(buf); err != nil {
		return fmt.Errorf("execute: write to %s: %w", dest, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("execute: close write to %s: %w", dest, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("execute: read response from %s: %w", dest, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("execute: %s", respBuf)
	}
	return nil
}

// handleExecuteStream is the receiving side of ExecuteProtocolID: it
// decodes the message (Decode itself checks crc32), extracts the claimed
// sender peer id from the notification payload (see
// EncodeExecuteNotification), and verifies the signature against *that*
// peer id's own Ed25519 public key -- embedded in the peer id itself for
// this project's identities, the same extraction
// pkg/daemon.recordClusterMember uses -- rather than trusting whichever
// address dialed in. That's what makes the signature self-contained
// (matches EventExecute's doc comment: authenticity doesn't depend on the
// stream's own connection identity), unlike handleForwardConfirmStream's
// check, which deliberately does the opposite for a different reason (see
// its own doc comment). Once the signature checks out, applies an
// unconditional current-raft-membership gate (shmevent.ReservedGroupCluster,
// kept in lockstep with actual raft membership by syncMemberGroups) against
// that same verified sender peer id -- unlike Channel/relay/remote, this is
// deliberately NOT routed through isAuthorizedForGatedAccess: there is no
// operator-granted execute-group or personal-grant carve-out here, only a
// current voter/learner may push an execute notification -- then queues the
// notification for EventPollExecute. Never writes to n.store or raft either
// way.
func (n *Node) handleExecuteStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "execute: read: %v", err)
		return
	}
	m, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		fmt.Fprintf(s, "execute: decode: %v", err)
		return
	}
	if m.Which() != shmevent.Event_Which_execute {
		fmt.Fprintf(s, "execute: expected execute, got %s", shmevent.EventName(m.Which()))
		return
	}
	grp := m.Execute()
	senderPeerID, err := grp.SenderPeerId()
	if err != nil {
		fmt.Fprintf(s, "execute: sender peer id: %v", err)
		return
	}
	payload, err := grp.Value()
	if err != nil {
		fmt.Fprintf(s, "execute: value: %v", err)
		return
	}
	senderPeer, err := peer.Decode(senderPeerID)
	if err != nil {
		fmt.Fprintf(s, "execute: invalid sender peer id %q: %v", senderPeerID, err)
		return
	}
	senderPub, err := senderPeer.ExtractPublicKey()
	if err != nil {
		fmt.Fprintf(s, "execute: extract sender public key: %v", err)
		return
	}
	rawSenderPub, err := senderPub.Raw()
	if err != nil {
		fmt.Fprintf(s, "execute: sender public key raw bytes: %v", err)
		return
	}
	if err := shmevent.Verify(shmevent.PublicKey(rawSenderPub), m, crc, sig); err != nil {
		fmt.Fprintf(s, "execute: %v", err)
		return
	}
	// Authorization, checked only now that senderPeer is proven authentic
	// (see this function's doc comment on why that's the peer id this
	// gate must check, never s.Conn().RemotePeer()). Deliberately just
	// current raft cluster membership -- not isAuthorizedForGatedAccess --
	// so an execute-group grant or personal grant can no longer substitute.
	if !isInGroupSt(n.store, senderPeer, shmevent.ReservedGroupCluster) {
		fmt.Fprintf(s, "execute: %s is not a current cluster member", senderPeer)
		return
	}
	n.executeInbox.push([]byte(senderPeerID), payload)
}
