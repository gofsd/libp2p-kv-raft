package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

const prodRelayAddr = "/ip4/63.250.47.155/tcp/4001/p2p/12D3KooWRKy6WzDdgruaFjgH7LMXCwmhd6wYSLvRYU7LhMMY73n8"

func spawnNode(name, dataDir string) (peerID string, listenAddrs []string) {
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		log.Fatalf("%s: GenerateIdentity: %v", name, err)
	}
	keyPath := filepath.Join(dataDir, "identity.key")
	if err := e2edata.WriteDesktopKeyFile(e2edata.Node{PrivateKey: hex.EncodeToString(priv)}, keyPath); err != nil {
		log.Fatalf("%s: WriteDesktopKeyFile: %v", name, err)
	}
	pid, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		log.Fatalf("%s: PeerIDFromPrivateKey: %v", name, err)
	}

	ctx := context.Background()
	go func() {
		err := daemon.Run(ctx, daemon.Config{
			DataDir:            dataDir,
			KeyPath:            keyPath,
			RelayPeers:         []string{prodRelayAddr},
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
		})
		log.Printf("%s: daemon.Run exited: %v", name, err)
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
	log.Printf("%s: ready, peerID=%s listenAddrs=%v", name, ready.PeerID, ready.ListenAddrs)
	return pid, ready.ListenAddrs
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
			log.Printf("%s: GetOwnAddr error: %v", name, err)
		} else {
			lastAddr = addr
			if contains(addr, "/p2p-circuit") {
				log.Printf("%s: circuit addr ready: %s", name, addr)
				return addr
			}
		}
		time.Sleep(1 * time.Second)
	}
	log.Printf("%s: WARNING no circuit addr appeared within %s (last GetOwnAddr: %s)", name, timeout, lastAddr)
	return ""
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (func() bool {
		for i := 0; i+len(substr) <= len(s); i++ {
			if s[i:i+len(substr)] == substr {
				return true
			}
		}
		return false
	})()
}

func main() {
	leaderDir, err := os.MkdirTemp("", "relayrepro-leader-*")
	if err != nil {
		log.Fatal(err)
	}
	followerDir, err := os.MkdirTemp("", "relayrepro-follower-*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("leaderDir:", leaderDir)
	fmt.Println("followerDir:", followerDir)

	leaderPeerID, _ := spawnNode("leader", leaderDir)
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
	spawnNode("follower-daemon", followerDir)

	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		log.Fatalf("rand: %v", err)
	}
	if err := leaderSess.CreateJoinInvite(ctx, token, shmevent.SuffrageVoter); err != nil {
		log.Fatalf("CreateJoinInvite: %v", err)
	}
	ticket := circuitAddr + "#" + hex.EncodeToString(token)
	fmt.Println("ticket:", ticket)

	followerSess, err := shmclient.Open(ctx, mustExtractPeerID(followerDir))
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

func mustExtractPeerID(dataDir string) string {
	info, err := daemon.ReadReadyFile(dataDir)
	if err != nil {
		log.Fatalf("ReadReadyFile(%s): %v", dataDir, err)
	}
	return info.PeerID
}

var _ = registry.IsMultiaddr
