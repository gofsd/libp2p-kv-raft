package kvctl

import (
	"context"
	"fmt"
	"sort"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// ClusterInfo is one raft cluster known to this machine's registry, as
// returned by ListClusters: the cluster's identifying peer id (see
// clusterID) and every locally-created identity that's currently part of
// it, whether or not that identity's daemon happens to be running right
// now.
type ClusterInfo struct {
	ClusterID string              `json:"cluster_id"`
	Members   []ClusterListMember `json:"members"`
}

// ClusterListMember is one locally-registered identity belonging to a
// ClusterInfo -- its registry.NodeInfo plus whether its daemon process is
// currently alive (isAlive is unexported, so this is computed here rather
// than making every caller, e.g. magefile.go, re-derive it itself).
type ClusterListMember struct {
	registry.NodeInfo
	Running bool `json:"running"`
}

// clusterID returns the peer id that identifies the raft cluster info
// belongs to: this project has no cluster identifier distinct from the
// peer id of whoever originally bootstrapped it (see
// registry.NodeInfo.ClusterPeerID's doc comment and registry.ClusterDirName)
// -- info.ClusterPeerID if it's currently joined to a remote cluster,
// otherwise info.PeerID itself, since an unjoined identity is the sole
// member of its own default single-node cluster.
func clusterID(info registry.NodeInfo) string {
	if info.ClusterPeerID != "" {
		return info.ClusterPeerID
	}
	return info.PeerID
}

// ListClusters groups every node identity this machine has ever created
// (registry.Registry.List -- no running daemon required, this is a pure
// local registry read) by the raft cluster it belongs to (see clusterID),
// so an identity that bootstrapped a cluster and any other local
// identities that later joined it (registry.NodeInfo.ClusterPeerID
// pointing back at the bootstrapper's peer id) are reported together under
// one ClusterInfo -- "available" here means "known to this machine's
// registry", the same scope every other kvctl operation is already
// limited to; there is no cross-machine cluster discovery. Feed one
// member's PeerID from the result into ListClusterMembers to see that
// cluster's full *live* raft membership, including peers this machine
// never created and so has no registry entry for at all.
func ListClusters() ([]ClusterInfo, error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, err
	}
	nodes, err := reg.List()
	if err != nil {
		return nil, err
	}

	grouped := make(map[string][]ClusterListMember)
	for _, n := range nodes {
		id := clusterID(n)
		grouped[id] = append(grouped[id], ClusterListMember{
			NodeInfo: n,
			Running:  n.PID != 0 && isAlive(n.PID),
		})
	}

	clusters := make([]ClusterInfo, 0, len(grouped))
	for id, members := range grouped {
		sort.Slice(members, func(i, j int) bool { return members[i].PeerID < members[j].PeerID })
		clusters = append(clusters, ClusterInfo{ClusterID: id, Members: members})
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].ClusterID < clusters[j].ClusterID })
	return clusters, nil
}

// ClusterMember is one entry in a raft cluster's live membership, as
// returned by ListClusterMembers.
type ClusterMember struct {
	PeerID string `json:"peer_id"`
	Role   string `json:"role"` // "leader", "voter", or "learner" -- see shmevent.RoleName
}

// ListClusterMembers returns every peer id currently in the raft cluster
// that the already-running local node localPeerID belongs to, read from
// that node's own locally-replicated shmevent.KindClusterMember records
// (kept current on every member -- leader and follower alike -- whenever a
// peer joins/leaves or this node's own leadership status changes; see
// pkg/daemon's recordClusterMember/watchLeadership/removeServerLine).
//
// localPeerID must name a node this machine's registry knows about and
// that is currently running: unlike raft's own AppendEntries/
// InstallSnapshot RPCs (which travel over libp2p directly, peer to peer),
// every kvctl/shmclient call -- this one included -- only ever reaches a
// daemon over local shmring IPC (see pkg/shmclient's package doc comment),
// never a remote peer directly. This is exactly why ListClusters (a pure
// registry read, no daemon needed) and ListClusterMembers (a live query
// against one specific running daemon) are two separate functions: the
// former is available offline, the latter isn't.
func ListClusterMembers(localPeerID string) ([]ClusterMember, error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, err
	}
	info, ok, err := reg.Get(localPeerID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("listnodes: unknown peer id %s (not created on this machine)", localPeerID)
	}
	if info.PID == 0 || !isAlive(info.PID) {
		return nil, fmt.Errorf("listnodes: node %s does not appear to be running; start it first", localPeerID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()

	lo, hi := shmevent.ClusterMemberKeyBounds()
	var members []ClusterMember
	for {
		key, value, ok, err := shmclient.ListRange(ctx, localPeerID, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("listnodes: %w", err)
		}
		if !ok {
			break
		}
		if len(key) < 3 {
			return nil, fmt.Errorf("listnodes: malformed cluster-member key %x", key)
		}
		peerID := string(key[3:])
		_, role, err := shmevent.DecodeClusterMemberPayload(value)
		if err != nil {
			return nil, fmt.Errorf("listnodes: decode member record for %s: %w", peerID, err)
		}
		members = append(members, ClusterMember{PeerID: peerID, Role: shmevent.RoleName(role)})
		lo = append(append([]byte{}, key...), 0x00)
	}

	sort.Slice(members, func(i, j int) bool { return members[i].PeerID < members[j].PeerID })
	return members, nil
}
