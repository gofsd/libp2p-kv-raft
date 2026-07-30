// Command relaytool is a single binary for everything relay-related that
// doesn't fit a `mage` target: running an actual circuit-relay v2 service
// node (the same daemon.Config.RelayService capability cmd/kvnode's own
// -relay-service flag exposes) for real production use or local/offline
// testing, and reproducing the join-ticket-over-relay bug fixed in
// pkg/registry.ExtractPeerID (see that function's doc comment) end to end
// against a genuine relay circuit. -mode picks which of the three it runs;
// -verbose controls how much step-by-step progress gets printed on top of
// each mode's final result.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// prodRelayAddr is configs/bootstrap-nodes.json's "primary" entry -- the
// real, already-deployed -relay-service node. -mode=debug joins/reserves
// through this by default; -relay overrides it (e.g. to point at a
// -mode=local instance's own printed address instead).
const prodRelayAddr = "/ip4/63.250.47.155/tcp/4001/p2p/12D3KooWRKy6WzDdgruaFjgH7LMXCwmhd6wYSLvRYU7LhMMY73n8"

var verbose bool

func vlogf(format string, args ...any) {
	if verbose {
		log.Printf(format, args...)
	}
}

func main() {
	mode := flag.String("mode", "debug", `"prod": run this process as a real, deployable circuit-relay-v2 service node (fixed port 4001 by default, identity persisted so restarts keep the same peer id); "local": the same relay-service capability tuned for local/offline testing (ephemeral port, throwaway identity each run); "debug": reproduce the join-ticket-over-relay flow (leader+follower daemons, a follower Join against a "<relayAddr>#<tokenHex>" ticket) through a relay named by -relay`)
	verboseFlag := flag.Bool("verbose", false, "print step-by-step progress (relay reservation polling, daemon readiness waits, etc.) instead of just each mode's final result")
	relayAddr := flag.String("relay", "", "mode=debug only: multiaddr of the circuit-relay-v2 node to join/reserve through (default: the real deployed production relay, "+prodRelayAddr+")")
	port := flag.Int("port", 0, "mode=prod/local only: listen port for the relay-service daemon (0 = ephemeral; mode=prod defaults to 4001 when left unset)")
	dataDir := flag.String("data-dir", "", "mode=prod/local only: identity/state directory for the relay-service daemon (default: a stable path under the OS temp dir for mode=prod so restarts keep the same peer id; a fresh temp dir each run for mode=local)")
	flag.Parse()
	verbose = *verboseFlag

	switch *mode {
	case "prod":
		p := *port
		if p == 0 {
			p = 4001
		}
		runRelayService(p, *dataDir, true)
	case "local":
		runRelayService(*port, *dataDir, false)
	case "debug":
		addr := *relayAddr
		if addr == "" {
			addr = prodRelayAddr
		}
		runDebugRepro(addr)
	default:
		log.Fatalf("relaytool: unknown -mode %q (want prod, local, or debug)", *mode)
	}
}

func setupSignalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()
	return ctx, cancel
}

// ensureIdentityKeyFile generates and persists a fresh identity under
// dataDir/identity.key if one isn't already there -- daemon.Config.KeyPath
// requires the key to already exist (see loadKey), unlike a mobile/kvmobile
// or kvctl-managed node, which always provision one before ever calling
// daemon.Run. Reusing an existing key file (mode=prod's stable dataDir
// across restarts) is what keeps that node's peer id stable, matching the
// pinned peer_id configs/bootstrap-nodes.json expects of a real relay.
func ensureIdentityKeyFile(dataDir string) (keyPath string, err error) {
	keyPath = filepath.Join(dataDir, "identity.key")
	if _, err := os.Stat(keyPath); err == nil {
		return keyPath, nil
	}
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		return "", fmt.Errorf("generate identity: %w", err)
	}
	if err := e2edata.WriteDesktopKeyFile(e2edata.Node{PrivateKey: hex.EncodeToString(priv)}, keyPath); err != nil {
		return "", fmt.Errorf("write key file: %w", err)
	}
	return keyPath, nil
}

// runRelayService runs this process as a circuit-relay v2 service node
// (daemon.Config.RelayService=true) until Ctrl+C -- the same capability a
// real deployed node gets from cmd/kvnode's -relay-service flag, just
// packaged standalone here for -mode=prod/-mode=local. persistent selects
// mode=prod's stable-dataDir/stable-identity defaults vs. mode=local's
// fresh-temp-dir-per-run ones.
func runRelayService(port int, dataDir string, persistent bool) {
	ctx, cancel := setupSignalContext()
	defer cancel()

	if dataDir == "" {
		if persistent {
			dataDir = filepath.Join(os.TempDir(), "relaytool-prod-relay")
			if err := os.MkdirAll(dataDir, 0o700); err != nil {
				log.Fatalf("relaytool: MkdirAll(%s): %v", dataDir, err)
			}
		} else {
			d, err := os.MkdirTemp("", "relaytool-local-relay-*")
			if err != nil {
				log.Fatalf("relaytool: MkdirTemp: %v", err)
			}
			dataDir = d
		}
	}
	keyPath, err := ensureIdentityKeyFile(dataDir)
	if err != nil {
		log.Fatalf("relaytool: %v", err)
	}

	fmt.Printf("starting relay-service node (data dir: %s, port: %d)...\n", dataDir, port)
	go func() {
		err := daemon.Run(ctx, daemon.Config{
			DataDir:      dataDir,
			KeyPath:      keyPath,
			ListenPort:   port,
			RelayService: true,
		})
		if err != nil && ctx.Err() == nil {
			log.Fatalf("relaytool: daemon.Run: %v", err)
		}
	}()

	deadline := time.Now().Add(60 * time.Second)
	var ready daemon.ReadyInfo
	for time.Now().Before(deadline) {
		if info, err := daemon.ReadReadyFile(dataDir); err == nil {
			ready = info
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ready.PeerID == "" {
		log.Fatal("relaytool: relay-service daemon never became ready")
	}

	fmt.Println("=======================================================")
	fmt.Printf("RELAY IS RUNNING\nPeerID: %s\nAddresses:\n", ready.PeerID)
	for _, a := range ready.ListenAddrs {
		fmt.Printf("  %s\n", a)
	}
	fmt.Println("=======================================================")
	fmt.Println("Press Ctrl+C to stop...")

	<-ctx.Done()
	fmt.Println("stopping relay-service node...")
}

func spawnNode(name, dataDir, relayAddr string) (peerID string, listenAddrs []string) {
	keyPath, err := ensureIdentityKeyFile(dataDir)
	if err != nil {
		log.Fatalf("%s: %v", name, err)
	}

	ctx := context.Background()
	go func() {
		err := daemon.Run(ctx, daemon.Config{
			DataDir:            dataDir,
			KeyPath:            keyPath,
			RelayPeers:         []string{relayAddr},
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
		})
		vlogf("%s: daemon.Run exited: %v", name, err)
	}()

	deadline := time.Now().Add(180 * time.Second)
	var ready daemon.ReadyInfo
	for time.Now().Before(deadline) {
		if info, err := daemon.ReadReadyFile(dataDir); err == nil {
			ready = info
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ready.PeerID == "" {
		log.Fatalf("%s: daemon never became ready", name)
	}
	vlogf("%s: ready, peerID=%s listenAddrs=%v", name, ready.PeerID, ready.ListenAddrs)
	return ready.PeerID, ready.ListenAddrs
}

// waitForCircuitAddr polls GetOwnAddr live (same as the real app's
// KvNodeClient.getOwnAddr, per that call's own doc comment: "relay
// reservation completes asynchronously ... call again if you get back a
// private/loopback address") -- NOT the ready file, which daemon.go's
// writeReadyFile only ever writes once, at startup, well before AutoRelay's
// async reservation could possibly have completed.
func waitForCircuitAddr(name string, sess *shmclient.Session, timeout time.Duration) string {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var lastAddr string
	for time.Now().Before(deadline) {
		addr, err := sess.GetOwnAddr(ctx)
		if err != nil {
			vlogf("%s: GetOwnAddr error: %v", name, err)
		} else {
			lastAddr = addr
			if strings.Contains(addr, "/p2p-circuit") {
				vlogf("%s: circuit addr ready: %s", name, addr)
				return addr
			}
		}
		time.Sleep(1 * time.Second)
	}
	log.Printf("%s: WARNING no circuit addr appeared within %s (last GetOwnAddr: %s)", name, timeout, lastAddr)
	return ""
}

// runDebugRepro reproduces the join-ticket-over-relay bug fixed in
// pkg/registry.ExtractPeerID: it spawns a leader daemon relayed through
// relayAddr, bootstraps it solo (mirrors kvmobile.StartSolo), waits for its
// real /p2p-circuit address, mints a join-invite ticket in the exact
// "<circuitAddr>#<tokenHex>" format kvmobile.Join's real callers use, then
// has a second, follower daemon redeem it.
func runDebugRepro(relayAddr string) {
	leaderDir, err := os.MkdirTemp("", "relaytool-debug-leader-*")
	if err != nil {
		log.Fatal(err)
	}
	followerDir, err := os.MkdirTemp("", "relaytool-debug-follower-*")
	if err != nil {
		log.Fatal(err)
	}
	vlogf("leaderDir: %s", leaderDir)
	vlogf("followerDir: %s", followerDir)
	vlogf("relay: %s", relayAddr)

	leaderPeerID, _ := spawnNode("leader", leaderDir, relayAddr)
	ctx := context.Background()
	leaderSess, err := shmclient.Open(ctx, leaderPeerID)
	if err != nil {
		log.Fatalf("shmclient.Open(leader): %v", err)
	}
	// Mirror Kvmobile.StartSolo: bootstrap this node as the sole leader of
	// its own single-node cluster (a bare daemon.Run has no cluster at all
	// yet, exactly like a real app's KvNodeClient.init before AppBootstrap).
	if _, err := leaderSess.Add(ctx, ""); err != nil {
		log.Fatalf("bootstrap solo cluster: %v", err)
	}
	circuitAddr := waitForCircuitAddr("leader", leaderSess, 150*time.Second)
	if circuitAddr == "" {
		log.Fatal("leader never got a circuit addr; aborting")
	}

	// follower node too (mirrors both real devices running the same relay config)
	followerPeerID, _ := spawnNode("follower-daemon", followerDir, relayAddr)

	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		log.Fatalf("rand: %v", err)
	}
	if err := leaderSess.CreateJoinInvite(ctx, token, shmevent.SuffrageVoter); err != nil {
		log.Fatalf("CreateJoinInvite: %v", err)
	}
	ticket := circuitAddr + "#" + hex.EncodeToString(token)
	fmt.Println("ticket:", ticket)

	followerSess, err := shmclient.Open(ctx, followerPeerID)
	if err != nil {
		log.Fatalf("shmclient.Open(follower): %v", err)
	}

	start := time.Now()
	addCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	result, err := followerSess.Add(addCtx, ticket)
	elapsed := time.Since(start)
	fmt.Printf("follower Add(ticket) after %s: result=%q err=%v\n", elapsed, result, err)
}
