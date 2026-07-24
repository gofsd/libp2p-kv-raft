// Command kvnode is the long-running node daemon spawned by `mage addnode`.
// It has no notion of leader/follower at startup; that is decided later by
// a pkg/shmevent EventAdd request delivered over pkg/ipc (see pkg/daemon).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
)

func main() {
	dataDir := flag.String("data-dir", "", "node data directory (identity key, sqlite, raft)")
	keyPath := flag.String("key-path", "", "path to this node's libp2p identity key")
	listenPort := flag.Int("listen-port", 0, "TCP/QUIC port to listen on (0 = ephemeral; pin this for publicly reachable deployments)")
	relayService := flag.Bool("relay-service", false, "act as a circuit-relay v2 point for other nodes and force public reachability (only for nodes with a real public address)")
	relayPeer := flag.String("relay-peer", "", "a known circuit-relay v2 server's multiaddr (a node running with -relay-service) to proactively reserve a relay slot through -- required for any node that isn't reliably directly dialable by the rest of the cluster (see Config.RelayPeer's doc comment and README's Node connectivity policy)")
	relayMaxCircuitsPerPeer := flag.Int("relay-max-circuits-per-peer", 0, "only used alongside -relay-service: concurrent open relayed circuits a single peer may hold through this node (0 = shmevent.DefaultRelayMaxCircuitsPerPeer, 1)")
	relayLimitDataBytes := flag.Int64("relay-limit-data-bytes", 0, "only used alongside -relay-service: bytes relayed, each direction, before a circuit is reset (0 = shmevent.DefaultRelayLimitData, 1GB)")
	relayLimitDuration := flag.Duration("relay-limit-duration", 0, "only used alongside -relay-service: wall-clock lifetime of a relayed circuit before it's reset (0 = shmevent.DefaultRelayLimitDuration, 720h/30 days)")
	relayMaxReservationsPerIP := flag.Int("relay-max-reservations-per-ip", 0, "only used alongside -relay-service: active relay-slot reservations allowed from one IP address (0 = shmevent.DefaultRelayMaxReservationsPerIP, 5)")
	relayMaxReservationsPerPeer := flag.Int("relay-max-reservations-per-peer", 0, "only used alongside -relay-service: active relay-slot reservations allowed from one peer (0 = shmevent.DefaultRelayMaxReservationsPerPeer, 1)")
	requirePermitForRelay := flag.Bool("require-permit-for-relay", false, "only used alongside -relay-service: only let peers with a confirmed permit (mage requestpermit/confirmpermit, kind \"peer\") reserve a relay slot or open a relayed circuit through this node")
	requirePermitForExecute := flag.Bool("require-permit-for-execute", false, "only deliver EventExecute notifications (mage execute/pollexecute) from a current raft voter/learner or a peer with a confirmed permit (mage requestpermit/confirmpermit, kind \"peer\")")
	requirePermitForLog := flag.Bool("require-permit-for-log", false, "only let a remote peer logappend/logquery a given pkg/logrecord kind if it holds a confirmed per-kind permit for it (mage requestlogpermit/confirmlogpermit)")
	requireConfirmForJoin := flag.Bool("require-confirm-for-join", false, "gate join requests (mage addfollower/rejoinnode/join) on a separate confirmation from a current raft voter (mage confirmpermit cluster-join <peerID>) instead of admitting them immediately")
	heartbeatTimeout := flag.Duration("raft-heartbeat-timeout", 0, "raft heartbeat timeout (0 = hashicorp/raft's own default, 1s -- safe for real networks)")
	electionTimeout := flag.Duration("raft-election-timeout", 0, "raft election timeout (0 = default, 1s)")
	commitTimeout := flag.Duration("raft-commit-timeout", 0, "raft commit timeout (0 = default, 50ms)")
	leaderLeaseTimeout := flag.Duration("raft-leader-lease-timeout", 0, "raft leader lease timeout (0 = default, 500ms)")
	snapshotThreshold := flag.Uint64("raft-snapshot-threshold", 0, "raft log entries since last snapshot before a new one is taken (0 = hashicorp/raft's own default, 8192 -- large for a long-lived leader that new non-voters periodically join, since a join replays the whole log from index 1 up to the last snapshot)")
	snapshotInterval := flag.Duration("raft-snapshot-interval", 0, "how often raft checks whether a snapshot is due (0 = default, 120s)")
	trailingLogs := flag.Uint64("raft-trailing-logs", 0, "log entries a snapshot keeps instead of compacting away (0 = hashicorp/raft's own default, 10240 -- set this alongside -raft-snapshot-threshold, not instead of it: a log smaller than this has nothing eligible for compaction regardless of how often it snapshots)")
	flag.Parse()

	if *dataDir == "" || *keyPath == "" {
		fmt.Fprintln(os.Stderr, "kvnode: -data-dir and -key-path are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	err := daemon.Run(ctx, daemon.Config{
		DataDir:                     *dataDir,
		KeyPath:                     *keyPath,
		ListenPort:                  *listenPort,
		RelayService:                *relayService,
		RelayPeer:                   *relayPeer,
		RelayMaxCircuitsPerPeer:     *relayMaxCircuitsPerPeer,
		RelayLimitData:              *relayLimitDataBytes,
		RelayLimitDuration:          *relayLimitDuration,
		RelayMaxReservationsPerIP:   *relayMaxReservationsPerIP,
		RelayMaxReservationsPerPeer: *relayMaxReservationsPerPeer,
		RequirePermitForRelay:       *requirePermitForRelay,
		RequirePermitForExecute:     *requirePermitForExecute,
		RequirePermitForLog:         *requirePermitForLog,
		RequireConfirmForJoin:       *requireConfirmForJoin,
		HeartbeatTimeout:            *heartbeatTimeout,
		ElectionTimeout:             *electionTimeout,
		CommitTimeout:               *commitTimeout,
		LeaderLeaseTimeout:          *leaderLeaseTimeout,
		SnapshotThreshold:           *snapshotThreshold,
		SnapshotInterval:            *snapshotInterval,
		TrailingLogs:                *trailingLogs,
	})
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "kvnode: %v\n", err)
		os.Exit(1)
	}
}
