// Package registry tracks, on the local filesystem, the KV-store nodes that
// have been created on this machine: their peer id, role, data directory,
// and listen addresses. It also tracks which node is "current" for the
// mage set/get commands to target.
//
// It exists because a node is identified everywhere else (shmring channel
// names, raft ServerID, on-disk data directory) by its libp2p peer id, but a
// human operator needs to go from "the leader I just created" to that peer
// id, and a freshly-spawned follower needs to resolve "the leader's peer id"
// to a dialable multiaddr. Both directions are answered by this file.
//
// This package is meant for a single operator driving commands sequentially
// from a CLI, not concurrent writers; it does not implement cross-process
// locking beyond atomic file replacement.
package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// Role identifies whether a node was created as the initial cluster leader
// or joined later as a follower. It is informational only: raft itself
// decides and can change leadership at runtime.
type Role string

const (
	RoleLeader   Role = "leader"
	RoleFollower Role = "follower"
)

// NodeInfo is everything the CLI/daemon need to know about a node without
// having to ask the (possibly not-running) daemon itself.
type NodeInfo struct {
	PeerID       string   `json:"peer_id"`
	Role         Role     `json:"role"`
	DataDir      string   `json:"data_dir"`
	KeyPath      string   `json:"key_path"`
	ListenAddrs  []string `json:"listen_addrs"`
	LeaderPeerID string   `json:"leader_peer_id,omitempty"`
	PID          int      `json:"pid"`

	// ClusterPeerID is the remote leader peer id this identity is currently
	// joined through, i.e. which "nodes/<peerID>-<ClusterPeerID>" data
	// directory DataDir points at (see ClusterDataDir) -- empty means
	// DataDir is the plain solo "nodes/<peerID>" dir, this identity's own
	// default single-node cluster. Threaded through by AddNode's join path
	// so a later rejoin can tell "the same cluster this identity already
	// joined" (reuse DataDir as-is) apart from "a different cluster"
	// (switch to a different, possibly brand new, ClusterDataDir) -- see
	// pkg/kvctl's rejoin.
	ClusterPeerID string `json:"cluster_peer_id,omitempty"`
}

// file is the on-disk shape of registry.json.
type file struct {
	Nodes map[string]NodeInfo `json:"nodes"`
}

// Registry is a handle on the on-disk node registry rooted at Dir.
type Registry struct {
	Dir string
}

// EnvHome, when set, overrides the default registry root. Tests use this to
// isolate their state from the operator's real registry.
const EnvHome = "KVSTORE_HOME"

// Open resolves the registry root (EnvHome, or ~/.libp2p-kv-raft),
// creates it if necessary, and returns a handle to it.
func Open() (*Registry, error) {
	dir := os.Getenv(EnvHome)
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("registry: resolve home dir: %w", err)
		}
		dir = filepath.Join(home, ".libp2p-kv-raft")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("registry: create %s: %w", dir, err)
	}
	return &Registry{Dir: dir}, nil
}

func (r *Registry) jsonPath() string    { return filepath.Join(r.Dir, "registry.json") }
func (r *Registry) currentPath() string { return filepath.Join(r.Dir, "current") }

// NodeDataDir returns the directory a node identified by peerID should store
// its identity key, sqlite data, and raft log under. Callers use this before
// a NodeInfo even exists in the registry, when provisioning a brand new (or
// resuming an existing) node. This is peerID's *solo* data dir -- its own
// default single-node cluster, used when it has never joined another
// cluster -- as opposed to ClusterDataDir, used while joined to a specific
// remote cluster.
func (r *Registry) NodeDataDir(peerID string) string {
	return filepath.Join(r.Dir, "nodes", peerID)
}

// ClusterDirName returns the directory name (not a full path) peerID uses
// while joined to the cluster led through remotePeerID -- a pure naming
// function taking no filesystem root, so mobile/kvmobile can reuse the exact
// same convention under its own app-private root instead of duplicating it.
// Distinct remotePeerID values naturally get distinct directories, which is
// what lets AddNode/RejoinNode tell "rejoining the same cluster this
// identity already joined" (the directory already exists, reuse it as-is)
// apart from "joining a different cluster" (a fresh directory) with no
// separate replicated cluster-id concept -- see NodeInfo.ClusterPeerID's
// doc comment.
func ClusterDirName(peerID, remotePeerID string) string {
	return peerID + "-" + remotePeerID
}

// ClusterDataDir is ClusterDirName's desktop counterpart: the data directory
// peerID should use while joined to the cluster led through remotePeerID,
// rooted under this registry -- see NodeDataDir (the solo/default
// counterpart) and NodeInfo.ClusterPeerID's doc comment.
func (r *Registry) ClusterDataDir(peerID, remotePeerID string) string {
	return filepath.Join(r.Dir, "nodes", ClusterDirName(peerID, remotePeerID))
}

func (r *Registry) load() (file, error) {
	var f file
	data, err := os.ReadFile(r.jsonPath())
	if os.IsNotExist(err) {
		f.Nodes = map[string]NodeInfo{}
		return f, nil
	}
	if err != nil {
		return f, err
	}
	if len(data) == 0 {
		f.Nodes = map[string]NodeInfo{}
		return f, nil
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return f, fmt.Errorf("registry: parse %s: %w", r.jsonPath(), err)
	}
	if f.Nodes == nil {
		f.Nodes = map[string]NodeInfo{}
	}
	return f, nil
}

func (r *Registry) save(f file) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.jsonPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.jsonPath())
}

// Put persists (creating or overwriting) info for info.PeerID.
func (r *Registry) Put(info NodeInfo) error {
	f, err := r.load()
	if err != nil {
		return err
	}
	f.Nodes[info.PeerID] = info
	return r.save(f)
}

// Get returns the registered info for peerID, if any.
func (r *Registry) Get(peerID string) (NodeInfo, bool, error) {
	f, err := r.load()
	if err != nil {
		return NodeInfo{}, false, err
	}
	info, ok := f.Nodes[peerID]
	return info, ok, nil
}

// List returns every registered node.
func (r *Registry) List() ([]NodeInfo, error) {
	f, err := r.load()
	if err != nil {
		return nil, err
	}
	out := make([]NodeInfo, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		out = append(out, n)
	}
	return out, nil
}

// Delete removes peerID's entry from the registry. If peerID was the
// current Set/Get target, that selection is cleared too, so a deleted
// node's peer id doesn't linger as the current target (Current would keep
// returning it, and every subsequent Set/Get would fail resolving it).
// Deleting an unknown peerID is a no-op, not an error -- callers that need
// to distinguish "didn't exist" should check Get first.
func (r *Registry) Delete(peerID string) error {
	f, err := r.load()
	if err != nil {
		return err
	}
	delete(f.Nodes, peerID)
	if err := r.save(f); err != nil {
		return err
	}
	if cur, err := r.Current(); err == nil && cur == peerID {
		if err := os.Remove(r.currentPath()); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// ResolveAddress returns a dialable multiaddr (including /p2p/<peerID>) for
// peerID, looked up from the local registry. It is used to turn a bare peer
// id (what a human, or `mage addnode`, provides) into a raft.ServerAddress.
//
// It only works for nodes created on *this* machine, since the registry is
// a local file. A leader on another machine (e.g. a remote deployment
// joined over SSH) has no shared registry to resolve from -- callers should
// check IsMultiaddr first and, if the caller-supplied string is already a
// full multiaddr, use it directly instead of calling ResolveAddress.
func (r *Registry) ResolveAddress(peerID string) (string, error) {
	info, ok, err := r.Get(peerID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("registry: unknown peer id %s (not created on this machine)", peerID)
	}
	if len(info.ListenAddrs) == 0 {
		return "", fmt.Errorf("registry: peer id %s has no known listen address", peerID)
	}
	return info.ListenAddrs[0], nil
}

// ExtractPeerID returns the bare peer id leaderPeerIDOrMultiaddr identifies:
// leaderPeerIDOrMultiaddr itself if it's already a bare peer id, or the
// /p2p/<peer-id> component parsed out of it if it's a full multiaddr (a
// leader on another machine, resolved the same way daemon.join() already
// does). This is the one place that turns "whatever the operator typed" into
// the bare peer id ClusterDataDir's naming scheme and the join-confirm
// gate's bootstrap-peer allowlist both key on.
func ExtractPeerID(leaderPeerIDOrMultiaddr string) (string, error) {
	if !IsMultiaddr(leaderPeerIDOrMultiaddr) {
		return leaderPeerIDOrMultiaddr, nil
	}
	maddr, err := multiaddr.NewMultiaddr(leaderPeerIDOrMultiaddr)
	if err != nil {
		return "", fmt.Errorf("registry: invalid multiaddr %q: %w", leaderPeerIDOrMultiaddr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("registry: multiaddr %q missing peer id: %w", leaderPeerIDOrMultiaddr, err)
	}
	return info.ID.String(), nil
}

// IsMultiaddr reports whether s looks like a multiaddr (e.g.
// "/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...") rather than a bare peer id (e.g.
// "12D3Koo..."). Multiaddrs always start with "/"; peer ids never do. Used
// to decide whether a leader identifier can be dialed directly or needs
// resolving through the local registry.
func IsMultiaddr(s string) bool {
	return strings.HasPrefix(s, "/")
}

// Current returns the peer id of the "active" node that set/get target, or
// an error if none has been selected yet.
func (r *Registry) Current() (string, error) {
	data, err := os.ReadFile(r.currentPath())
	if os.IsNotExist(err) {
		return "", fmt.Errorf("registry: no current node selected; run `mage addnode` or `mage use <peer-id>` first")
	}
	if err != nil {
		return "", err
	}
	peerID := string(data)
	return peerID, nil
}

// SetCurrent records peerID as the active node for set/get.
func (r *Registry) SetCurrent(peerID string) error {
	tmp := r.currentPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(peerID), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.currentPath())
}
