// Package rafttransport adapts a go-libp2p host into a hashicorp/raft
// StreamLayer, so raft RPCs travel over libp2p streams instead of raw TCP.
//
// raft.ServerAddress values used with this transport must be full libp2p
// multiaddrs that include a /p2p/<peer-id> component (e.g. what
// peer.AddrInfo.Addrs()/p2p-circuit resolves to). Callers are responsible for
// resolving a peer id to such an address (pkg/registry does this) before
// calling raft.AddVoter or otherwise referencing a ServerAddress.
package rafttransport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/multiformats/go-multiaddr"
)

// ProtocolID is the libp2p protocol used for raft RPC streams.
const ProtocolID = protocol.ID("/libp2p-kv-raft/raft/1.0.0")

// streamLayer implements raft.StreamLayer over a libp2p host.
type streamLayer struct {
	host host.Host
	pid  protocol.ID

	mu       sync.Mutex
	closed   bool
	closeCh  chan struct{}
	incoming chan network.Stream
}

// NewStreamLayer registers a stream handler on h for the raft protocol and
// returns a raft.StreamLayer that Accept()s incoming raft streams and Dial()s
// outgoing ones.
func NewStreamLayer(h host.Host) raft.StreamLayer {
	sl := &streamLayer{
		host:     h,
		pid:      ProtocolID,
		closeCh:  make(chan struct{}),
		incoming: make(chan network.Stream, 16),
	}
	h.SetStreamHandler(sl.pid, func(s network.Stream) {
		select {
		case sl.incoming <- s:
		case <-sl.closeCh:
			s.Reset()
		}
	})
	return sl
}

// Accept implements net.Listener.
func (sl *streamLayer) Accept() (net.Conn, error) {
	select {
	case s := <-sl.incoming:
		return newConn(s), nil
	case <-sl.closeCh:
		return nil, fmt.Errorf("rafttransport: listener closed")
	}
}

// Close implements net.Listener.
func (sl *streamLayer) Close() error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if sl.closed {
		return nil
	}
	sl.closed = true
	sl.host.RemoveStreamHandler(sl.pid)
	close(sl.closeCh)
	return nil
}

// Addr implements net.Listener.
func (sl *streamLayer) Addr() net.Addr {
	return peerAddr(sl.host.ID().String())
}

// Dial implements raft.StreamLayer. address must be a multiaddr string
// containing a /p2p/<peer-id> component.
func (sl *streamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	maddr, err := multiaddr.NewMultiaddr(string(address))
	if err != nil {
		return nil, fmt.Errorf("rafttransport: invalid address %q: %w", address, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("rafttransport: address %q missing peer id: %w", address, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Allow a limited (relay) connection for the connect too, not just for
	// the stream below: without it an existing circuit connection does not
	// count as "connected", so this re-dials one on every single call.
	ctx = network.WithAllowLimitedConn(ctx, "raft")

	// A circuit address whose relay hop is this host itself cannot be
	// dialed: the first thing dialing it does is open a connection to the
	// relay, which here is us, and libp2p refuses to dial itself. Every
	// attempt fails with a bare "all dials failed" naming only the
	// destination, which reads like the peer being unreachable rather than
	// like an address we can never use.
	//
	// This is the ordinary shape for a peer behind *our own* relay
	// service -- a browser tab or phone that reserved a slot here and was
	// then enrolled at the resulting /p2p-circuit address, which is what
	// advertisedAddrs hands out. Nothing is wrong with such a peer: by
	// definition it holds a reservation, and a reservation only exists
	// while its holder keeps a connection open to us. So the connection
	// needed is already there, inbound, and the right move is to use it
	// rather than to dial. Found on this project's own deployed leader,
	// whose raft.log was full of "failed to heartbeat to"/"failed to
	// appendEntries to" for exactly one peer: the browser learner reserved
	// on that same node, which consequently never received a single entry
	// while every write it made committed fine.
	if relay, ok := circuitRelay(maddr); ok && relay == sl.host.ID() {
		s, err := sl.host.NewStream(network.WithNoDial(ctx, "relayed by this host"), info.ID, sl.pid)
		if err != nil {
			return nil, fmt.Errorf("rafttransport: no open connection to %s, which is reachable only through this host's own relay (it must reconnect to be reached): %w", info.ID, err)
		}
		return newConn(s), nil
	}

	if len(info.Addrs) > 0 {
		sl.host.Peerstore().AddAddrs(info.ID, info.Addrs, time.Hour)
	}
	// Clear libp2p's per-peer dial backoff before every attempt. Raft
	// re-dials a peer it cannot reach on its own heartbeat schedule, which
	// is faster than the backoff expires, so each attempt re-arms the
	// latch the previous one set and the peer becomes permanently
	// undialable *by this node* -- while remaining perfectly reachable to
	// anyone else. Seen on this project's own e2e leader: 35,056 log lines
	// of "failed to heartbeat to ... dial backoff" against one browser
	// learner, which consequently never received a single entry even
	// though every write it made committed fine and nothing reported an
	// error to it.
	//
	// This is the same reasoning as pkg/daemon's clearDialBackoff, which
	// covers the explicitly-requested dials (public-access, join, recruit,
	// exec-invite). Raft's own transport is the one that dials on a timer,
	// so it is the one the latch hurts most.
	clearDialBackoff(sl.host, *info)
	if err := sl.host.Connect(ctx, *info); err != nil {
		return nil, fmt.Errorf("rafttransport: connect to %s: %w", info.ID, err)
	}

	s, err := sl.host.NewStream(ctx, info.ID, sl.pid)
	if err != nil {
		return nil, fmt.Errorf("rafttransport: new stream to %s: %w", info.ID, err)
	}
	return newConn(s), nil
}

// circuitRelay returns the peer id of the relay named in addr's
// /p2p-circuit hop -- the /p2p/<id> component to the *left* of
// /p2p-circuit, which is the relay, as opposed to the one on the right,
// which is the destination. ok is false for an address that isn't a
// circuit address at all.
func circuitRelay(addr multiaddr.Multiaddr) (peer.ID, bool) {
	var relay peer.ID
	var found bool
	multiaddr.ForEach(addr, func(c multiaddr.Component) bool {
		switch c.Protocol().Code {
		case multiaddr.P_P2P:
			if !found {
				if id, err := peer.IDFromBytes(c.RawValue()); err == nil {
					relay = id
				}
			}
		case multiaddr.P_CIRCUIT:
			found = true
			return false // everything past here names the destination
		}
		return true
	})
	return relay, found && relay != ""
}

// peerAddr is a minimal net.Addr wrapping a libp2p peer id / multiaddr string.
type peerAddr string

func (a peerAddr) Network() string { return "libp2p" }
func (a peerAddr) String() string  { return string(a) }

// conn adapts a network.Stream to net.Conn.
type conn struct {
	network.Stream
}

func newConn(s network.Stream) *conn { return &conn{Stream: s} }

func (c *conn) LocalAddr() net.Addr {
	return peerAddr(c.Stream.Conn().LocalPeer().String())
}

func (c *conn) RemoteAddr() net.Addr {
	return peerAddr(c.Stream.Conn().RemotePeer().String())
}

// NewTransport builds a raft.NetworkTransport that runs over h using the
// raft protocol stream handler registered by NewStreamLayer.
func NewTransport(h host.Host, timeout time.Duration) *raft.NetworkTransport {
	return raft.NewNetworkTransport(NewStreamLayer(h), 4, timeout, nil)
}

// clearDialBackoff drops go-libp2p's per-peer dial backoff for info's peer
// and for the relay named in any /p2p-circuit address it carries -- a
// relayed dial goes through that hop first, so its own backoff entry stops
// the dial just as effectively as the destination's. See Dial's call site
// for what leaving it armed cost.
func clearDialBackoff(h host.Host, info peer.AddrInfo) {
	type backoffClearer interface{ Backoff() *swarm.DialBackoff }
	sw, ok := h.Network().(backoffClearer)
	if !ok {
		return
	}
	sw.Backoff().Clear(info.ID)
	for _, addr := range info.Addrs {
		multiaddr.ForEach(addr, func(c multiaddr.Component) bool {
			if c.Protocol().Code != multiaddr.P_P2P {
				return true
			}
			if hop, err := peer.IDFromBytes(c.RawValue()); err == nil && hop != info.ID {
				sw.Backoff().Clear(hop)
			}
			return true
		})
	}
}
