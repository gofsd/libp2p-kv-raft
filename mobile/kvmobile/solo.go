package kvmobile

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// isAlreadyBootstrappedErr reports whether err is hashicorp/raft's
// ErrCantBootstrap ("bootstrap only works on new clusters"), as returned
// (string-wrapped across the shmclient IPC boundary, so a sentinel
// errors.Is check isn't available here) by an Add(ctx, "") call against a
// node that already has persisted raft state. See startSolo's call site.
func isAlreadyBootstrappedErr(err error) bool {
	return strings.Contains(err.Error(), "bootstrap only works on new clusters")
}

// StartSolo brings up the daemon in-process under dataDir (see Start) and
// bootstraps it as the sole leader of its own single-node raft cluster,
// instead of joining leaderMultiaddr -- the mobile counterpart of desktop's
// `mage addnode` with no leaderPeerID (pkg/kvctl's addNew(..., leaderPeerID
// ="")). This is the right default for a device with no externally-reachable
// leader baked in at build time: it still gets a working, durable KV/raft
// node of its own, reachable from other devices through relayMultiaddr
// (see relayPeers) exactly like a joined follower would be. A later Join/
// JoinWithKey call still works as always, switching this identity onto an
// externally-led cluster without disturbing this solo state (see startSolo's
// soloDir, kept separate from any clusterDir a Join lands in).
//
// Like Start, calling this again once already running (solo or otherwise)
// is a no-op returning the existing peer id.
func StartSolo(dataDir string) (string, error) {
	return startSolo(dataDir, ensureIdentity)
}

// StartSoloWithKey is StartSolo plus StartWithKey's identity provisioning
// (see StartWithKey) instead of always falling back to ensureIdentity's
// persisted-or-generated-or-build-seeded key.
func StartSoloWithKey(dataDir, keyHex string) (string, error) {
	return startSolo(dataDir, func(dataDir string) (keyPath, peerID string, err error) {
		return importIdentity(dataDir, keyHex)
	})
}

// startSolo is startAgainst's solo-bootstrap counterpart: same in-process
// daemon.Run/waitForReady/shmclient.Open dance, but calls sess.Add with an
// empty leaderPeerID -- pkg/daemon's handleAdd treats that as "bootstrap a
// brand new single-node cluster with this node as its only, immediately-
// elected voter" rather than "join someone else's" (see BootstrapCluster
// there). Callers must not hold mu.
func startSolo(dataDirRoot string, resolveIdentity func(dataDir string) (keyPath, peerID string, err error)) (string, error) {
	mu.Lock()
	defer mu.Unlock()
	if started {
		return peerID, nil
	}

	keyPath, id, err := resolveIdentity(dataDirRoot)
	if err != nil {
		return "", err
	}

	// soloDir is a subdirectory of dataDirRoot dedicated to this identity's
	// own single-node cluster -- never dataDirRoot itself (where
	// identity.key lives, see ensureIdentity/importIdentity) and never
	// collides with a startAgainst clusterDir (always "<id>-<remotePeerID>",
	// which a bare peer id can never equal since libp2p peer ids are base58
	// and never contain "-"), so a later Join to an externally-led cluster
	// lands in its own directory without disturbing this solo state, exactly
	// like switching between two different joined clusters already doesn't.
	soloDir := filepath.Join(dataDirRoot, id)

	// See startAgainst's identical remove: drops a stale ready file an
	// earlier Start/Stop cycle against this same directory left behind, so
	// waitForReady below can't return early on it before this run has
	// written its own.
	_ = os.Remove(filepath.Join(soloDir, daemon.ReadyFileName))

	ctx, cancel := context.WithCancel(context.Background())
	errC := make(chan error, 1)
	go func() {
		errC <- daemon.Run(ctx, daemon.Config{
			DataDir:            soloDir,
			KeyPath:            keyPath,
			RelayPeers:         relayPeers(),
			HeartbeatTimeout:   raftHeartbeatTimeout,
			ElectionTimeout:    raftElectionTimeout,
			CommitTimeout:      raftCommitTimeout,
			LeaderLeaseTimeout: raftLeaderLeaseTimeout,
		})
	}()

	if err := waitForReady(soloDir, errC, callTimeout); err != nil {
		cancel()
		return "", fmt.Errorf("kvmobile: start solo: %w", err)
	}

	addCtx, addCancel := context.WithTimeout(ctx, callTimeout)
	defer addCancel()
	sess, err := shmclient.Open(addCtx, id)
	if err != nil {
		cancel()
		return "", fmt.Errorf("kvmobile: fetch signing key: %w", err)
	}
	// A prior startSolo against this same soloDir (e.g. an earlier process
	// that already bootstrapped this device's single-node cluster, now
	// restarted) already has persisted raft state on disk by this point --
	// daemon.Run's own Run doc comment: a node with existing state resumes
	// via initRaft and is already fully operational (ready file written,
	// which waitForReady above already required) *without* this Add call,
	// which in that case only exists to bootstrap a genuinely new cluster
	// and correctly refuses ("bootstrap only works on new clusters") since
	// raft.HasExistingState is true. That refusal means "already resumed,
	// nothing to do" here, not a real failure -- do not cancel/tear down a
	// daemon that, by the time this error is possible, is already up.
	if _, err := sess.Add(addCtx, ""); err != nil && !isAlreadyBootstrappedErr(err) {
		cancel()
		return "", fmt.Errorf("kvmobile: bootstrap solo cluster: %w", err)
	}

	session = sess
	peerID = id
	curDataDir = soloDir
	curDataDirRoot = dataDirRoot
	curClusterID = ""
	runErrC = errC
	cancelRun = cancel
	started = true
	return peerID, nil
}
