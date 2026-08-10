package rafttransport

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/multiformats/go-multiaddr"
)

// mustMultiaddr builds addr, failing the test on a malformed string --
// every case below is a fixed literal, so a parse failure means the test
// itself is wrong, not the code under test.
func mustMultiaddr(t *testing.T, addr string) multiaddr.Multiaddr {
	t.Helper()
	m, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		t.Fatalf("multiaddr.NewMultiaddr(%q): %v", addr, err)
	}
	return m
}

// TestCircuitRelay checks the address-shape parsing Dial's own doc
// comment leans on to detect "this address's relay hop is myself":
// circuitRelay must name the /p2p/<id> component to the *left* of
// /p2p-circuit (the relay), never the destination on the right, and must
// report ok=false for any address that isn't a circuit address at all --
// including one with a /p2p component but no /p2p-circuit component, and
// one with /p2p-circuit but no preceding /p2p component (an ok=false
// "relay unknown" case Dial's own doc comment doesn't have to handle
// today, but circuitRelay's contract still has to answer correctly rather
// than panicking or misreporting a peer id).
func TestCircuitRelay(t *testing.T) {
	relay := test.RandPeerIDFatal(t)
	dest := test.RandPeerIDFatal(t)

	tests := []struct {
		name      string
		addr      string
		wantOK    bool
		wantRelay peer.ID
	}{
		{
			name:   "plain direct address, no circuit",
			addr:   fmt.Sprintf("/ip4/1.2.3.4/tcp/4001/p2p/%s", dest),
			wantOK: false,
		},
		{
			name:      "circuit address: relay then destination",
			addr:      fmt.Sprintf("/ip4/1.2.3.4/tcp/4001/p2p/%s/p2p-circuit/p2p/%s", relay, dest),
			wantOK:    true,
			wantRelay: relay,
		},
		{
			name:   "circuit address with no relay peer id component",
			addr:   fmt.Sprintf("/ip4/1.2.3.4/tcp/4001/p2p-circuit/p2p/%s", dest),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := mustMultiaddr(t, tt.addr)
			gotRelay, gotOK := circuitRelay(m)
			if gotOK != tt.wantOK {
				t.Fatalf("circuitRelay(%q) ok = %v, want %v", tt.addr, gotOK, tt.wantOK)
			}
			if tt.wantOK && gotRelay != tt.wantRelay {
				t.Fatalf("circuitRelay(%q) relay = %s, want %s", tt.addr, gotRelay, tt.wantRelay)
			}
		})
	}
}

// dialableAddr returns the first full (address + /p2p/<id>) multiaddr for
// h, suitable for another host's Dial.
func dialableAddr(t *testing.T, h interface{ Addrs() []multiaddr.Multiaddr }, id peer.ID) string {
	t.Helper()
	info := peer.AddrInfo{ID: id, Addrs: h.Addrs()}
	full, err := peer.AddrInfoToP2pAddrs(&info)
	if err != nil || len(full) == 0 {
		t.Fatalf("no dialable address for %s: %v", id, err)
	}
	return full[0].String()
}

// A live Dial-to-Accept round trip between two bare libp2p hosts is
// deliberately not exercised at this package's own test level. Direct
// experimentation while writing this file found that two freshly created,
// otherwise-idle bare hosts (host.Host via plain libp2p.New(), no other
// activity) can leave a newly registered SetStreamHandler callback
// unfired indefinitely in this environment -- confirmed via a raw,
// rafttransport-independent host.SetStreamHandler probe that never fired
// within a clean 90-second wait, yet fires reliably and fast (under a
// second) once literally anything else perturbs the connection (e.g. a
// Close). That behavior reproduces with plain go-libp2p APIs with no
// rafttransport code involved, so it isn't a bug in this package; it also
// never reproduces against a real pkg/daemon.Node (a bare host dialing
// into one, e.g. pkg/daemon's own TestExecuteStreamRejectsForgedSignature,
// resolves in well under a second) -- and this exact transport's Dial/
// Accept/circuitRelay/clearDialBackoff already run on every real
// AppendEntries/RequestVote in pkg/daemon's own extensive multi-node raft
// cluster test suite. So the actual wire round trip has real coverage;
// what's tested directly here is this package's own logic that isn't
// already exercised as a side effect of those cluster tests: circuitRelay
// (above), Dial's input validation (below), and Close's contract
// (below) -- none of which need a live successful stream negotiation to
// verify.

// TestStreamLayerDialRejectsInvalidAddress checks Dial's two validation
// steps happen, and happen in the right order, before anything ever
// touches the network: a string that isn't a multiaddr at all must be
// rejected by the multiaddr parse, and a syntactically valid multiaddr
// missing a /p2p/<peer-id> component -- the package doc comment's stated
// precondition for every raft.ServerAddress this transport accepts --
// must be rejected by the peer-id extraction, with an error naming the
// actual problem rather than attempting a dial that could only ever fail
// confusingly later.
func TestStreamLayerDialRejectsInvalidAddress(t *testing.T) {
	hA, err := libp2p.New()
	if err != nil {
		t.Fatalf("start host A: %v", err)
	}
	t.Cleanup(func() { hA.Close() })

	slA := NewStreamLayer(hA)
	defer slA.Close()

	tests := []struct {
		name string
		addr string
	}{
		{name: "not a multiaddr at all", addr: "not-a-multiaddr"},
		{name: "empty string", addr: ""},
		{name: "well-formed multiaddr, no /p2p component", addr: "/ip4/1.2.3.4/tcp/4001"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := slA.Dial(raft.ServerAddress(tt.addr), 200*time.Millisecond); err == nil {
				t.Fatalf("Dial(%q) succeeded, want a validation error", tt.addr)
			}
		})
	}
}

// TestStreamLayerDialUnreachablePeerFails checks Dial actually returns
// (bounded by the timeout it was given, not hanging past it) when the
// address is well-formed but nothing is listening there -- the ordinary
// "peer is down" case raft's own retry loop has to be able to move past.
func TestStreamLayerDialUnreachablePeerFails(t *testing.T) {
	hA, err := libp2p.New()
	if err != nil {
		t.Fatalf("start host A: %v", err)
	}
	t.Cleanup(func() { hA.Close() })

	slA := NewStreamLayer(hA)
	defer slA.Close()

	unreachable := dialableAddr(t, hA, test.RandPeerIDFatal(t))

	done := make(chan error, 1)
	go func() {
		_, err := slA.Dial(raft.ServerAddress(unreachable), time.Second)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Dial to an unreachable peer id succeeded, want an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dial did not respect its own timeout for an unreachable peer")
	}
}

// TestStreamLayerCloseStopsAccepting checks Close's two documented
// effects: Accept, whether already blocked or called afterward, returns
// promptly with an error instead of hanging forever, and Close itself is
// idempotent (raft.NetworkTransport's own shutdown path may call Close
// more than once). It also checks that a peer dialing in after Close sees
// the protocol as genuinely gone (RemoveStreamHandler ran), not just
// silently dropped on this side.
func TestStreamLayerCloseStopsAccepting(t *testing.T) {
	hA, err := libp2p.New()
	if err != nil {
		t.Fatalf("start host A: %v", err)
	}
	t.Cleanup(func() { hA.Close() })

	hB, err := libp2p.New()
	if err != nil {
		t.Fatalf("start host B: %v", err)
	}
	t.Cleanup(func() { hB.Close() })

	slB := NewStreamLayer(hB)

	acceptErrCh := make(chan error, 1)
	go func() {
		_, err := slB.Accept()
		acceptErrCh <- err
	}()

	// Give Accept a moment to actually start blocking before Close, so
	// this also covers "already waiting when Close happens," not just
	// "Close happened first."
	time.Sleep(20 * time.Millisecond)

	if err := slB.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := slB.Close(); err != nil {
		t.Fatalf("second Close should be a no-op, got: %v", err)
	}

	select {
	case err := <-acceptErrCh:
		if err == nil {
			t.Fatal("Accept should return an error once the listener is closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return after Close")
	}

	// A dial in from another host after Close must fail outright: Close
	// already removed the stream handler for this protocol on hB.
	slA := NewStreamLayer(hA)
	defer slA.Close()
	addr := dialableAddr(t, hB, hB.ID())
	if _, err := slA.Dial(raft.ServerAddress(addr), 2*time.Second); err == nil {
		t.Fatal("Dial succeeded against a peer whose stream handler was already removed by Close")
	}
}
