package kvmobile

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// restartableLeader is a real in-process daemon whose lifetime this test
// controls, unlike spawnTestLeader's (which runs on context.Background and
// so can never be stopped). Restarting one on the same data directory is
// the whole point here: raft state that survives a restart is what makes a
// node come back as a *member* of a cluster rather than a blank node, and
// membership is what the behaviour under test turns on.
type restartableLeader struct {
	t        *testing.T
	dataDir  string
	keyPath  string
	peerID   string
	port     int
	addr     string
	cancel   context.CancelFunc
	exited   chan error
	bootstrp bool
}

// freePort picks a port nothing is listening on. The leader's port is
// pinned rather than ephemeral because it has to be the *same* port after a
// restart: a follower that still holds this node's old address in its own
// persisted raft configuration would otherwise be unable to reach it, and
// the test would be measuring a stale address rather than what it means to.
// (A real device does not need this -- its advertised address is a
// /p2p-circuit one, which names a relay and a peer id and no port at all,
// and so is already stable across restarts.)
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func newRestartableLeader(t *testing.T) *restartableLeader {
	t.Helper()
	dataDir := t.TempDir()
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	keyPath := filepath.Join(dataDir, "identity.key")
	if err := e2edata.WriteDesktopKeyFile(e2edata.Node{PrivateKey: hex.EncodeToString(priv)}, keyPath); err != nil {
		t.Fatalf("WriteDesktopKeyFile: %v", err)
	}
	peerID, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}
	leader := &restartableLeader{t: t, dataDir: dataDir, keyPath: keyPath, peerID: peerID, port: freePort(t)}
	t.Cleanup(leader.stop)
	return leader
}

// start runs the daemon and waits for its ready file, bootstrapping it into
// a single-node cluster the first time only -- a later start resumes the
// raft state the previous one left behind, which is exactly the situation
// this test needs.
func (l *restartableLeader) start() {
	l.t.Helper()
	// Drop the previous run's ready file, or the wait below would return on
	// it immediately and hand back a stale address before this daemon has
	// actually come up. Same reasoning as startAgainst's identical remove.
	_ = os.Remove(filepath.Join(l.dataDir, daemon.ReadyFileName))

	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.exited = make(chan error, 1)
	go func() {
		l.exited <- daemon.Run(ctx, daemon.Config{
			DataDir:            l.dataDir,
			KeyPath:            l.keyPath,
			ListenPort:         l.port,
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
		})
	}()

	deadline := time.Now().Add(20 * time.Second)
	var ready daemon.ReadyInfo
	for time.Now().Before(deadline) {
		if info, err := daemon.ReadReadyFile(l.dataDir); err == nil && len(info.ListenAddrs) > 0 {
			ready = info
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ready.PeerID == "" {
		l.t.Fatal("leader daemon never became ready")
	}
	l.addr = ready.ListenAddrs[0]

	reg, err := registry.Open()
	if err != nil {
		l.t.Fatalf("registry.Open: %v", err)
	}
	if err := reg.Put(registry.NodeInfo{PeerID: l.peerID, DataDir: l.dataDir}); err != nil {
		l.t.Fatalf("registry.Put: %v", err)
	}

	if !l.bootstrp {
		bootstrapCtx, bootstrapCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer bootstrapCancel()
		if _, err := shmclient.Add(bootstrapCtx, l.peerID, ""); err != nil {
			l.t.Fatalf("bootstrap leader: %v", err)
		}
		l.bootstrp = true
	}
}

// stop cancels the daemon and waits for Run to return, so the next start
// against the same directory does not race the previous one's store locks.
func (l *restartableLeader) stop() {
	if l.cancel == nil {
		return
	}
	l.cancel()
	select {
	case <-l.exited:
	case <-time.After(60 * time.Second):
		l.t.Error("leader daemon did not exit within a minute of being cancelled")
	}
	l.cancel = nil
}

// TestStartSurvivesARefusedRejoinOnceAlreadyAMember is the regression test
// for the deadlock that made this project's own two-device optical rig
// unusable: a pair that had once been up together could never come back up
// together again.
//
// The shape is the rig's exactly. The leader admits the device as a *voter*,
// so the cluster has two of them, and a majority of two is two -- the one
// cluster size with worse availability than running alone, as this repo's
// README says in as many words. Restart the leader while the device is
// down and it resumes that two-voter configuration alone, can elect nobody,
// and answers the device's join with "ERR: not leader".
//
// That answer used to be fatal on the device side: Start tore its daemon
// down and reported the failure. But that daemon had already resumed its
// own persisted raft state and was a member of the cluster before it ever
// sent the join -- it was the one participant whose return would have
// restored the quorum, and destroying it guaranteed neither device could
// ever come back. Every relaunch repeated it.
//
// So the assertion is that Start *succeeds* here, and the second half
// proves why that matters rather than merely asserting a return value: with
// both nodes up the cluster elects a leader on its own and commits a write,
// through no join at all.
func TestStartSurvivesARefusedRejoinOnceAlreadyAMember(t *testing.T) {
	leader := newRestartableLeader(t)
	leader.start()

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leader.addr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		_ = Stop()
	})

	// First launch: an ordinary join, which makes this device the cluster's
	// second voter.
	dataDirRoot := t.TempDir()
	id, err := Start(dataDirRoot)
	if err != nil {
		t.Fatalf("Start (first, joining): %v", err)
	}
	clusterDir := filepath.Join(dataDirRoot, registry.ClusterDirName(id, leader.peerID))
	if err := Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Restart the leader with the device still down: it comes back holding
	// a two-voter configuration it cannot form a majority of.
	leader.stop()
	leader.start()

	// The launch that used to be fatal.
	rejoinedID, err := Start(dataDirRoot)
	if err != nil {
		t.Fatalf("Start (second, after the leader lost quorum) failed instead of resuming: %v", err)
	}
	if rejoinedID != id {
		t.Fatalf("Start returned peer id %s, want the same identity %s", rejoinedID, id)
	}
	if info, err := daemon.ReadReadyFile(clusterDir); err != nil {
		t.Fatalf("read the device's ready file after resuming: %v", err)
	} else if !info.Resumed {
		t.Fatal("the device's second launch did not resume its persisted raft state -- the join it sent was not a re-announcement, so this test is not exercising what it claims to")
	}

	// Why it matters: both nodes are up, so the cluster has its majority
	// back and heals itself. Polled, since an election plus a commit takes
	// a moment and neither has been asked for explicitly.
	if err := awaitWritable(t, 60*time.Second); err != nil {
		t.Fatalf("the cluster never recovered once both voters were up again: %v", err)
	}
}

// awaitWritable polls a Submit/Get round trip until it succeeds, which is
// only possible once the cluster has elected a leader and can commit.
func awaitWritable(t *testing.T, within time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(within)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := Submit("rejoin-probe", "v"); err != nil {
			lastErr = fmt.Errorf("submit: %w", err)
			time.Sleep(250 * time.Millisecond)
			continue
		}
		got, err := Get("rejoin-probe")
		if err == nil && got == "v" {
			return nil
		}
		lastErr = fmt.Errorf("get: %v (value %q)", err, got)
		time.Sleep(250 * time.Millisecond)
	}
	return lastErr
}
