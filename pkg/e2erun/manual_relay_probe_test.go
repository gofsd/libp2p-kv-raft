package e2erun

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	lp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// loopbackTCPAddrRe matches GetOwnAddr's own last-resort fallback shape,
// "/ip4/127.0.0.1/tcp/<port>/p2p/<peerid>" -- what a device falls back to
// advertising if its relay reservation hasn't completed yet. A match here
// means the device's own address isn't real evidence of relay reachability
// yet, not something to work around.
var loopbackTCPAddrRe = regexp.MustCompile(`^/ip4/127\.0\.0\.1/tcp/(\d+)/p2p/(.+)$`)

// TestManualRelayProbe is a throwaway, manually-invoked probe answering a
// question still relevant to the optical suite's own join-ticket recruiting
// (pkg/e2erun/android_optical.go): once a device has relay standing, does it
// *stay* reachable through the relay, or does the reservation lapse while
// the device is still up and still advertising that address? From this
// machine, an entirely separate peer, it repeatedly dials the device's own
// /p2p-circuit address through the relay over a window and reports every
// attempt.
//
// Unlike an earlier version of this test, it no longer drives the device
// into that state itself -- that used android_pair.go's uiOp/
// runUIStepsBackgroundPeek, built on UiCommandE2ETest's Run-button catalog
// sweep, removed along with every other execute-without-scanning path (see
// the android-app RunCode rewrite). Bring the device up by hand first
// (Cluster: StartPending -> RequestRelayAccess -> GetOwnAddr, e.g. via a
// real camera scan from a second device or the optical suite's own
// generateAndHold/awaitAndVerifyScan) and pass its own reported address as
// MANUAL_RELAY_PROBE_DEVICE_ADDR.
//
// Set MANUAL_RELAY_PROBE_BOOTSTRAP=<relay multiaddr> and
// MANUAL_RELAY_PROBE_DEVICE_ADDR=<device's own /p2p-circuit address, from
// its own GetOwnAddr>.
func TestManualRelayProbe(t *testing.T) {
	relayAddr := os.Getenv("MANUAL_RELAY_PROBE_BOOTSTRAP")
	deviceCircuitAddr := os.Getenv("MANUAL_RELAY_PROBE_DEVICE_ADDR")
	if relayAddr == "" || deviceCircuitAddr == "" {
		t.Skip("set MANUAL_RELAY_PROBE_BOOTSTRAP=<relay multiaddr> and MANUAL_RELAY_PROBE_DEVICE_ADDR=<device's own GetOwnAddr> to run this manually")
	}
	if loopbackTCPAddrRe.MatchString(deviceCircuitAddr) {
		t.Fatalf("MANUAL_RELAY_PROBE_DEVICE_ADDR is a bare loopback address: %q", deviceCircuitAddr)
	}

	// Dial repeatedly rather than once: the question is not only "is it
	// reachable" but "for how long does it stay reachable" -- the device is
	// assumed to already be up and holding its own daemon alive for the
	// whole window.
	if err := probeReachabilityOverTime(t, relayAddr, deviceCircuitAddr, 110*time.Second, 10*time.Second); err != nil {
		t.Fatalf("device stopped being dialable through the relay: %v", err)
	}
}

// probeReachabilityOverTime stands in for the *other* device in a real
// pairing: a peer with its own standing on the same relay, dialing the
// probe device's circuit address every interval for the whole window and
// reporting each attempt. Any failure after a first success is the
// interesting result -- it means the device's reservation lapsed while it
// was still up and still advertising that address.
func probeReachabilityOverTime(t *testing.T, relayAddr, deviceCircuitAddr string, window, interval time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window+60*time.Second)
	defer cancel()

	_, rawPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	lp2pPriv, err := lp2pcrypto.UnmarshalEd25519PrivateKey(rawPriv)
	if err != nil {
		return err
	}
	h, err := libp2p.New(libp2p.Identity(lp2pPriv), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"), libp2p.EnableRelay())
	if err != nil {
		return err
	}
	defer h.Close()

	relayMaddr, err := multiaddr.NewMultiaddr(relayAddr)
	if err != nil {
		return err
	}
	relayInfo, err := peer.AddrInfoFromP2pAddr(relayMaddr)
	if err != nil {
		return err
	}
	if err := h.Connect(ctx, *relayInfo); err != nil {
		return fmt.Errorf("connect to relay: %w", err)
	}
	// This dialer needs its own standing too -- relayACL gates the
	// connecting side as well as the reserving one.
	if err := submitPublicAccess(ctx, h, *relayInfo, shmevent.PrivateKey(rawPriv)); err != nil {
		return fmt.Errorf("dialer standing: %w", err)
	}

	deviceMaddr, err := multiaddr.NewMultiaddr(deviceCircuitAddr)
	if err != nil {
		return err
	}
	deviceInfo, err := peer.AddrInfoFromP2pAddr(deviceMaddr)
	if err != nil {
		return err
	}

	start := time.Now()
	var everReached bool
	var lastErr error
	for time.Since(start) < window {
		// A fresh connection each round: a held-open one would keep
		// answering long after the relay stopped honouring new circuits.
		_ = h.Network().ClosePeer(deviceInfo.ID)
		dialCtx, dialCancel := context.WithTimeout(network.WithAllowLimitedConn(ctx, "probe"), 15*time.Second)
		err := h.Connect(dialCtx, *deviceInfo)
		dialCancel()
		if err == nil {
			everReached = true
			t.Logf("t+%-4s reachable", time.Since(start).Round(time.Second))
			lastErr = nil
		} else {
			short := err.Error()
			if i := strings.Index(short, "\n"); i > 0 {
				short = short[i+1:]
			}
			t.Logf("t+%-4s NOT reachable: %s", time.Since(start).Round(time.Second), strings.TrimSpace(short))
			lastErr = err
		}
		time.Sleep(interval)
	}
	if !everReached {
		return fmt.Errorf("never reachable: %w", lastErr)
	}
	if lastErr != nil {
		return fmt.Errorf("was reachable, then stopped: %w", lastErr)
	}
	return nil
}

// submitPublicAccess is pkg/daemon.dialAndSubmitPublicAccess's shape,
// hand-rolled here for a plain libp2p host with no daemon behind it.
func submitPublicAccess(ctx context.Context, h lp2phost.Host, info peer.AddrInfo, priv shmevent.PrivateKey) error {
	rnd, err := logrecord.NewRand()
	if err != nil {
		return err
	}
	instanceID := "relay-probe-dialer"
	ts := time.Now().UTC()
	kind := shmevent.CommandRequestLogKind(shmevent.DefaultPublicCommandID)
	key, err := logrecord.BuildKey(kind, instanceID, ts, rnd)
	if err != nil {
		return err
	}
	value, err := logrecord.Record{
		Kind: kind, UnitID: instanceID, Timestamp: ts,
		AuthorPeerID: h.ID().String(),
		Fields:       map[string]string{"note": "relay probe dialer"},
	}.Encode()
	if err != nil {
		return err
	}
	logMsg, err := shmevent.NewLogAppend(key, value)
	if err != nil {
		return err
	}
	logMsg.SetId(1)
	buf, err := shmevent.Encode(logMsg, priv)
	if err != nil {
		return err
	}
	s, err := h.NewStream(network.WithAllowLimitedConn(ctx, "probe"), info.ID, daemon.ClientProtocolID)
	if err != nil {
		return err
	}
	defer s.Close()
	if _, err := s.Write(buf); err != nil {
		return err
	}
	if err := s.CloseWrite(); err != nil {
		return err
	}
	respBuf, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	resp, _, _, err := shmevent.Decode(respBuf)
	if err != nil {
		return err
	}
	if resp.Which() == shmevent.Event_Which_error {
		errMsg, _ := resp.Error().Message_()
		return fmt.Errorf("%s", errMsg)
	}
	return nil
}
