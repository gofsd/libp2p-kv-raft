// Command relaytool is a single binary for everything relay-related that
// doesn't fit a `mage` target: running an actual circuit-relay v2 service
// node (the same daemon.Config.RelayService capability cmd/kvnode's own
// -relay-service flag exposes) for real production use or local/offline
// testing, reproducing the join-ticket-over-relay bug fixed in
// pkg/registry.ExtractPeerID (see that function's doc comment) end to end
// against a genuine relay circuit, and driving the full two-device pairing
// flow (-mode=pair) the same way pkg/e2erun/android_pair.go drives it
// across two phones, just with desktop daemons. -mode picks which of the
// four it runs; -verbose controls how much step-by-step progress gets
// printed on top of each mode's final result.
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

	"github.com/gofsd/libp2p-kv-raft/pkg/bootstrapnodes"
	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// prodRelayAddr is loaded from configs/bootstrap-nodes.json's first
// relay_service entry (pkg/bootstrapnodes.PrimaryRelayAddr) at startup,
// not hardcoded -- a hardcoded copy of that address only ever a literal
// copy-paste away from drifting out of sync with the catalog it's meant to
// mirror. -mode=debug/pair join/reserve through this by default; -relay
// overrides it (e.g. to point at a -mode=local instance's own printed
// address instead). Left empty if the catalog can't be found or parsed --
// e.g. running a compiled relaytool binary outside the repo checkout, or a
// checkout with no bootstrap nodes configured yet -- in which case
// -mode=debug/pair require -relay to be passed explicitly instead of
// silently falling back to a stale address.
var prodRelayAddr string

func init() {
	root, err := bootstrapnodes.FindRepoRoot("")
	if err != nil {
		return
	}
	f, err := bootstrapnodes.Load(filepath.Join(root, bootstrapnodes.RelativePath))
	if err != nil {
		return
	}
	addr, err := f.PrimaryRelayAddr()
	if err != nil {
		return
	}
	prodRelayAddr = addr
}

var verbose bool

func vlogf(format string, args ...any) {
	if verbose {
		log.Printf(format, args...)
	}
}

func main() {
	relayDefaultDesc := prodRelayAddr
	if relayDefaultDesc == "" {
		relayDefaultDesc = "none found -- -relay is required"
	}
	mode := flag.String("mode", "debug", `"prod": run this process as a real, deployable circuit-relay-v2 service node (fixed port 4001 by default, identity persisted so restarts keep the same peer id); "local": the same relay-service capability tuned for local/offline testing (ephemeral port, throwaway identity each run); "debug": reproduce the join-ticket-over-relay flow (leader+follower daemons, a follower Join against a "<relayAddr>#<tokenHex>" ticket) through a relay named by -relay; "pair": the full two-device pairing flow (both devices self-service relay standing, then recruit/join each other over the relay)`)
	verboseFlag := flag.Bool("verbose", false, "print step-by-step progress (relay reservation polling, daemon readiness waits, etc.) instead of just each mode's final result")
	relayAddr := flag.String("relay", "", "mode=debug/pair only: multiaddr of the circuit-relay-v2 node to join/reserve through (default: the deployed production relay from configs/bootstrap-nodes.json, "+relayDefaultDesc+")")
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
		addr := resolveRelayAddr(*relayAddr)
		runDebugRepro(addr)
	case "pair":
		addr := resolveRelayAddr(*relayAddr)
		runPairRepro(addr)
	default:
		log.Fatalf("relaytool: unknown -mode %q (want prod, local, debug, or pair)", *mode)
	}
}

// resolveRelayAddr returns explicit (an operator-supplied -relay) unchanged
// when set; otherwise falls back to prodRelayAddr, the address
// pkg/bootstrapnodes loaded from configs/bootstrap-nodes.json at startup --
// failing loudly rather than silently proceeding with an empty address if
// that load never found a catalog entry to fall back to.
func resolveRelayAddr(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if prodRelayAddr == "" {
		log.Fatal("relaytool: no -relay given and configs/bootstrap-nodes.json has no relay_service entry to default to (run from within the repo checkout, or pass -relay explicitly)")
	}
	return prodRelayAddr
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

// raftTiming is the subset of daemon.Config a spawned node's raft
// behavior is tuned with. Two profiles exist because the two things this
// tool spawns nodes for want opposite tradeoffs -- see fastLocalRaft and
// deviceRaft.
type raftTiming struct {
	heartbeat, election, commit, leaderLease time.Duration
}

// fastLocalRaft makes a single-node bootstrap settle as fast as possible,
// which is all -mode=debug needs of it (it never forms a multi-node
// cluster whose members have to keep heartbeating each other).
var fastLocalRaft = raftTiming{200 * time.Millisecond, 200 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}

// deviceRaft mirrors mobile/kvmobile's own raft timings exactly, and is
// what any cluster whose members reach each other *through a relay* has
// to use: a relayed round trip to a real VPS is comfortably longer than
// fastLocalRaft's whole 200ms election timeout, so a leader would lose
// leadership faster than it could ever replicate anything. Caught live --
// -mode=pair's recruit succeeded and then every write failed with
// "leadership lost while committing log" until these were widened.
var deviceRaft = raftTiming{4 * time.Second, 4 * time.Second, 200 * time.Millisecond, 2500 * time.Millisecond}

func spawnNode(name, dataDir, relayAddr string, timing raftTiming) (peerID string, listenAddrs []string) {
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
			HeartbeatTimeout:   timing.heartbeat,
			ElectionTimeout:    timing.election,
			CommitTimeout:      timing.commit,
			LeaderLeaseTimeout: timing.leaderLease,
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
	// shmclient.Open resolves a peer id to its data dir (and therefore its
	// shmring token) through this machine's local registry -- the same
	// bookkeeping pkg/kvctl does when it spawns a node. daemon.Run itself
	// never writes it, so a node spawned in-process here has to be
	// registered explicitly or every later session call fails with "no
	// registered node for peer id".
	reg, err := registry.Open()
	if err != nil {
		log.Fatalf("%s: registry.Open: %v", name, err)
	}
	if err := reg.Put(registry.NodeInfo{PeerID: ready.PeerID, DataDir: dataDir}); err != nil {
		log.Fatalf("%s: registry.Put: %v", name, err)
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

	leaderPeerID, _ := spawnNode("leader", leaderDir, relayAddr, fastLocalRaft)
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
	followerPeerID, _ := spawnNode("follower-daemon", followerDir, relayAddr, fastLocalRaft)

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

// runPairRepro drives the whole two-device pairing flow against a real
// relay -- the desktop equivalent of what pkg/e2erun/android_pair.go runs
// across two phones, and the only way to exercise it end to end without
// two emulators. Both devices are throwaway daemons that start out with
// no standing anywhere at all, exactly like a fresh install:
//
//  1. each asks the relay's cluster for standing over the always-public
//     command (shmevent.DefaultPublicCommandID, via Session.PublicAccess
//     -- `mage requestpublicaccess` / kvmobile's RequestRelayAccess), the
//     only thing that gets a stranger past relayACL;
//  2. each then has to actually end up with a /p2p-circuit address, in
//     this same process, with no restart -- see daemon.relayReserveBackoff
//     for why that used to be impossible;
//  3. a bootstraps its own solo cluster and b (still raft-less, exactly
//     AddPending's state) mints a join-request ticket carrying its circuit
//     address;
//  4. a redeems that ticket (Session.Recruit), which dials b through the
//     relay and hands it an invite, which b redeems by joining a back
//     through the relay -- both hops over limited connections;
//  5. a writes a key and b must observe it, proving raft replication runs
//     over the same circuit afterwards.
//
// Every step prints its own elapsed time: which one stalls is the whole
// diagnostic value of running this by hand.
func runPairRepro(relayAddr string) {
	ctx := context.Background()

	aDir, err := os.MkdirTemp("", "relaytool-pair-a-*")
	if err != nil {
		log.Fatal(err)
	}
	bDir, err := os.MkdirTemp("", "relaytool-pair-b-*")
	if err != nil {
		log.Fatal(err)
	}
	vlogf("device a dir: %s", aDir)
	vlogf("device b dir: %s", bDir)
	fmt.Println("relay:", relayAddr)

	aPeerID, _ := spawnNode("device-a", aDir, relayAddr, deviceRaft)
	bPeerID, _ := spawnNode("device-b", bDir, relayAddr, deviceRaft)
	aSess, err := shmclient.Open(ctx, aPeerID)
	if err != nil {
		log.Fatalf("shmclient.Open(a): %v", err)
	}
	bSess, err := shmclient.Open(ctx, bPeerID)
	if err != nil {
		log.Fatalf("shmclient.Open(b): %v", err)
	}
	fmt.Printf("device a: %s\ndevice b: %s\n", aPeerID, bPeerID)

	// Step 1: standing. Without this the relay refuses both reservations
	// outright and every later step has nothing dialable to work with.
	for name, sess := range map[string]*shmclient.Session{"a": aSess, "b": bSess} {
		start := time.Now()
		accessCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		instanceID, err := sess.PublicAccess(accessCtx, relayAddr, "relaytool pair")
		cancel()
		if err != nil {
			log.Fatalf("device %s: request public access: %v", name, err)
		}
		fmt.Printf("device %s: relay standing granted after %s (instance %s)\n", name, time.Since(start).Round(time.Millisecond), instanceID)
	}

	// Step 2: a real circuit address each, in this same process.
	start := time.Now()
	aCircuit := waitForCircuitAddr("device-a", aSess, 120*time.Second)
	if aCircuit == "" {
		log.Fatal("device a never obtained a relay reservation after being granted standing")
	}
	fmt.Printf("device a circuit addr after %s: %s\n", time.Since(start).Round(time.Millisecond), aCircuit)
	start = time.Now()
	bCircuit := waitForCircuitAddr("device-b", bSess, 120*time.Second)
	if bCircuit == "" {
		log.Fatal("device b never obtained a relay reservation after being granted standing")
	}
	fmt.Printf("device b circuit addr after %s: %s\n", time.Since(start).Round(time.Millisecond), bCircuit)

	// Step 3: a is a cluster, b is a fresh device offering itself.
	if _, err := aSess.Add(ctx, ""); err != nil {
		log.Fatalf("device a: bootstrap solo cluster: %v", err)
	}
	token, err := bSess.CreateJoinRequest(ctx)
	if err != nil {
		log.Fatalf("device b: create join request: %v", err)
	}
	ticket := bCircuit + "#" + hex.EncodeToString(token)
	fmt.Println("pairing ticket:", ticket)

	// Step 4: the pairing itself -- a dials b over the relay, b joins a
	// back over the relay.
	start = time.Now()
	recruitCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	result, err := aSess.Recruit(recruitCtx, ticket, shmevent.SuffrageVoter)
	cancel()
	if err != nil {
		log.Fatalf("device a: recruit b after %s: %v", time.Since(start).Round(time.Millisecond), err)
	}
	fmt.Printf("recruit succeeded after %s: %s\n", time.Since(start).Round(time.Millisecond), result)

	// Step 5: replication over the same circuit.
	if err := aSess.Set(ctx, "relaytool-pair", "yes"); err != nil {
		log.Fatalf("device a: set: %v", err)
	}
	start = time.Now()
	deadline := time.Now().Add(60 * time.Second)
	for {
		value, err := bSess.Get(ctx, "relaytool-pair")
		if err == nil && value == "yes" {
			fmt.Printf("device b observed a's replicated write after %s\n", time.Since(start).Round(time.Millisecond))
			break
		}
		if time.Now().After(deadline) {
			log.Fatalf("device b never observed a's replicated write: value=%q err=%v", value, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("PAIRED OK")
}
