package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/rafttransport"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// initRaft lazily constructs the raft transport and raft.Raft instance. It
// must be called at most once, synchronously, from the EventAdd handler.
func (n *Node) initRaft() (*raft.Raft, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.raft != nil {
		return n.raft, nil
	}

	transport := rafttransport.NewTransport(n.host, 10*time.Second)

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(n.peerID)
	if n.cfg.HeartbeatTimeout > 0 {
		raftConf.HeartbeatTimeout = n.cfg.HeartbeatTimeout
	}
	if n.cfg.ElectionTimeout > 0 {
		raftConf.ElectionTimeout = n.cfg.ElectionTimeout
	}
	if n.cfg.CommitTimeout > 0 {
		raftConf.CommitTimeout = n.cfg.CommitTimeout
	}
	if n.cfg.LeaderLeaseTimeout > 0 {
		raftConf.LeaderLeaseTimeout = n.cfg.LeaderLeaseTimeout
	}
	if n.cfg.SnapshotThreshold > 0 {
		raftConf.SnapshotThreshold = n.cfg.SnapshotThreshold
	}
	if n.cfg.SnapshotInterval > 0 {
		raftConf.SnapshotInterval = n.cfg.SnapshotInterval
	}
	if n.cfg.TrailingLogs > 0 {
		raftConf.TrailingLogs = n.cfg.TrailingLogs
	}
	if logFile, err := os.OpenFile(filepath.Join(n.cfg.DataDir, "raft.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		raftConf.LogOutput = logFile
		n.raftLogFile = logFile
	}

	fsm := kvfsm.New(n.store)
	rf, err := raft.NewRaft(raftConf, fsm, n.logStore, n.logStore, n.snapStore, transport)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("daemon: create raft node: %w", err)
	}

	n.raft = rf
	n.transport = transport
	n.electionTimeout = raftConf.ElectionTimeout

	// Registered before this function returns to whichever caller then
	// calls BootstrapCluster (handleAdd's bootstrap branch) -- so this
	// node's very first self-election, not just later re-elections, is
	// caught too.
	obsCh := make(chan raft.Observation, 8)
	observer := raft.NewObserver(obsCh, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	rf.RegisterObserver(observer)
	n.leadershipObserver = observer
	n.leadershipObsCh = obsCh
	go n.watchLeadership(rf, obsCh)

	return rf, nil
}

// watchLeadership reacts to every leadership-change notification (see
// initRaft's Observer registration) by re-asserting this node's own
// current truth in its KindClusterMember record -- not tracking "who used
// to be leader": deliberately stateless/idempotent, so a redundant
// identical write is harmless and a missed one self-corrects on the next
// transition. Returns once ch is closed (shutdown).
func (n *Node) watchLeadership(rf *raft.Raft, ch chan raft.Observation) {
	for range ch {
		role, ok := n.ownCurrentRole(rf)
		if !ok {
			continue
		}
		if err := n.recordClusterMember(context.Background(), n.peerID, role); err != nil {
			fmt.Fprintf(os.Stderr, "daemon: record own cluster member status: %v\n", err)
		}
		// Only a voter (or the leader) syncs its own reserved groups. The
		// sync ends in a delete, which is voter-gated at the leader (see
		// deletePeerGroup), so a learner attempting it would fail every
		// time -- and pointlessly: the leader already ran this exact sync
		// for this peer when it admitted or demoted it (see
		// addServerLine's own call). A learner re-running it adds nothing
		// but a failed forward per leadership observation.
		if role != shmevent.RoleLearner {
			if err := n.syncMemberGroups(context.Background(), n.peerID, role); err != nil {
				fmt.Fprintf(os.Stderr, "daemon: sync own reserved groups: %v\n", err)
			}
		}
		if role == shmevent.RoleLeader {
			n.transferLeadershipToStableVoter(rf)
		}
	}
}

// transferLeadershipToStableVoter implements CLAUDE.md's connectivity
// policy that leadership belongs on a node with a real, stable address
// rather than one only reachable through a relay circuit: direct dials are
// cheaper and faster, and it keeps the cluster's single point of forwarding
// off a device (phone, laptop behind NAT) that can vanish at any time. It's
// an offer, not a requirement -- see (*raft.Raft).LeadershipTransferToServer
// -- so an unreachable or uncooperative target just leaves this node
// leading, same as today.
func (n *Node) transferLeadershipToStableVoter(rf *raft.Raft) {
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return
	}
	id, addr, ok := preferredLeaderTransferTarget(cfgFuture.Configuration(), raft.ServerID(n.peerID))
	if !ok {
		return
	}
	if err := rf.LeadershipTransferToServer(id, addr).Error(); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: transfer leadership to stable voter %s: %v\n", id, err)
	}
}

// preferredLeaderTransferTarget picks a voter to hand leadership to when
// self currently holds it but self's own address only routes through a
// relay circuit (see isRelayServerAddress) while some other voter's
// doesn't. ok is false when self is already stable, or when no other voter
// is any more stable than self -- nothing would be gained by moving.
func preferredLeaderTransferTarget(cfg raft.Configuration, self raft.ServerID) (id raft.ServerID, addr raft.ServerAddress, ok bool) {
	var selfAddr raft.ServerAddress
	for _, srv := range cfg.Servers {
		if srv.ID == self {
			selfAddr = srv.Address
			break
		}
	}
	if !isRelayServerAddress(selfAddr) {
		return "", "", false
	}
	for _, srv := range cfg.Servers {
		if srv.ID == self || srv.Suffrage != raft.Voter {
			continue
		}
		if !isRelayServerAddress(srv.Address) {
			return srv.ID, srv.Address, true
		}
	}
	return "", "", false
}

// isRelayServerAddress reports whether a raft.ServerAddress (always a full
// libp2p multiaddr over this project's transport, see
// pkg/rafttransport.Transport's doc comment) is a /p2p-circuit relay
// address rather than a direct one -- the same check advertisedAddrs and
// awaitRelayAddr use for this node's own addresses.
func isRelayServerAddress(addr raft.ServerAddress) bool {
	return strings.Contains(string(addr), "/p2p-circuit")
}

// ownCurrentRole determines this node's own current role: RoleLeader if
// it's currently raft.Leader, else RoleVoter/RoleLearner per its own
// suffrage in the current configuration. ok is false if this node isn't
// (yet) present in the configuration at all.
func (n *Node) ownCurrentRole(rf *raft.Raft) (role byte, ok bool) {
	if rf.State() == raft.Leader {
		return shmevent.RoleLeader, true
	}
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return 0, false
	}
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(n.peerID) {
			if srv.Suffrage == raft.Nonvoter {
				return shmevent.RoleLearner, true
			}
			return shmevent.RoleVoter, true
		}
	}
	return 0, false
}

// getRaft returns the raft instance if initRaft has already run.
func (n *Node) getRaft() *raft.Raft {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft
}
