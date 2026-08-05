package e2erun

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
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

// TestManualRelayProbe is a throwaway, manually-invoked probe answering the
// one question the whole pair scenario rests on: can a device go from "no
// standing at all" to "reachable through the relay" inside a single
// process, with no restart? It starts a pending daemon on one device, asks
// the baked-in relay for standing, holds that process alive, and -- from
// this machine, an entirely separate peer -- repeatedly dials the device's
// own /p2p-circuit address through that same relay. A successful dial is
// proof the reservation landed, independent of whatever the device's own
// GetOwnAddr happens to report.
//
// Set MANUAL_RELAY_PROBE_SERIAL=<serial> and
// MANUAL_RELAY_PROBE_BOOTSTRAP=<relay multiaddr>. Assumes the app is
// already installed on that device.
func TestManualRelayProbe(t *testing.T) {
	serial := os.Getenv("MANUAL_RELAY_PROBE_SERIAL")
	relayAddr := os.Getenv("MANUAL_RELAY_PROBE_BOOTSTRAP")
	if serial == "" || relayAddr == "" {
		t.Skip("set MANUAL_RELAY_PROBE_SERIAL=<serial> and MANUAL_RELAY_PROBE_BOOTSTRAP=<relay multiaddr> to run this manually")
	}

	// A key this test keeps, so it knows the device's peer id -- and
	// therefore its circuit address -- before the device ever reports it.
	_, rawPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	lp2pPriv, err := lp2pcrypto.UnmarshalEd25519PrivateKey(rawPriv)
	if err != nil {
		t.Fatalf("unmarshal identity: %v", err)
	}
	marshaled, err := lp2pcrypto.MarshalPrivateKey(lp2pPriv)
	if err != nil {
		t.Fatalf("marshal identity: %v", err)
	}
	keyHex := hex.EncodeToString(marshaled)
	devicePeerID, err := peer.IDFromPrivateKey(lp2pPriv)
	if err != nil {
		t.Fatalf("derive peer id: %v", err)
	}
	deviceCircuitAddr := relayAddr + "/p2p-circuit/p2p/" + devicePeerID.String()
	t.Logf("device peer id: %s", devicePeerID)

	// The device grants itself standing and holds its daemon alive; this
	// process then dials it through the relay from outside, after an
	// optional delay standing in for how long the *other* device's own
	// instrumentation invocation takes to get as far as dialing.
	ops := []uiOp{
		{label: "Cluster: StartPendingWithKeyAndPort", inputs: []string{keyHex, "47155"}},
		{label: "Cluster: RequestRelayAccess", inputs: []string{"relay probe"}},
		{label: "Cluster: GetOwnAddr"},
		{label: "Test: SleepMillis", inputs: []string{"120000"}},
	}
	start := time.Now()
	addr, wait, err := runUIStepsBackgroundPeek(serial, ops, "Cluster: GetOwnAddr", 150*time.Second)
	if err != nil {
		t.Fatalf("device never reported an address: %v", err)
	}
	t.Logf("device reported its own address after %s: %s", time.Since(start).Round(time.Second), addr)
	if loopbackTCPAddrRe.MatchString(addr) {
		t.Fatalf("device reported a bare loopback address: %q", addr)
	}

	// Dial repeatedly rather than once: the question is not only "is it
	// reachable" but "for how long does it stay reachable", which is what
	// the pair scenario actually depends on (the other device needs tens
	// of seconds of its own start-up before it can dial at all).
	dialErr := probeReachabilityOverTime(t, relayAddr, deviceCircuitAddr, 110*time.Second, 10*time.Second)
	if waitErr := wait(); waitErr != nil {
		t.Logf("device hold ended with: %v", waitErr)
	}
	if dialErr != nil {
		t.Fatalf("device stopped being dialable through the relay: %v", dialErr)
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
	payload, err := shmevent.EncodeSetPayload(key, value)
	if err != nil {
		return err
	}
	buf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventLogAppend, Value: payload, ID: 1}, priv)
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
	if resp.EventType == shmevent.EventError {
		return fmt.Errorf("%s", resp.Value)
	}
	return nil
}
