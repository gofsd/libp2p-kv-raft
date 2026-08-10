//go:build mage

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// AddRelayNode implements `mage addrelaynode <multiaddr> <priority>`:
// lodges a pending shmevent.KindBootstrapNode record for multiaddr's own
// embedded /p2p/<peerID> -- the relay list pkg/daemon's relayCandidates
// draws its EnableAutoRelayWithStaticRelays candidate set from (merged
// with -relay-peer's own seed value, see Config.RelayPeers). Lower
// priority values are tried first. Any raft node may originate a request;
// it needs `mage confirmrelaynode` from a current voter before it's
// actually picked up.
// Usage: mage addrelaynode <multiaddr> <priority>
func AddRelayNode(multiaddr, priority string) error {
	p, err := strconv.ParseUint(priority, 10, 8)
	if err != nil {
		return fmt.Errorf("priority: %w", err)
	}
	if err := kvctl.AddRelayNode(multiaddr, uint8(p)); err != nil {
		return err
	}
	fmt.Println("✅ relay node requested (pending confirmrelaynode)")
	return nil
}

// ConfirmRelayNode implements `mage confirmrelaynode <multiaddr>`:
// promotes a pending relay-node record to confirmed, the point at which
// every cluster member's relayCandidates actually starts considering it.
// Only takes effect if the current node is itself a raft voter.
// Usage: mage confirmrelaynode <multiaddr>
func ConfirmRelayNode(multiaddr string) error {
	if err := kvctl.ConfirmRelayNode(multiaddr); err != nil {
		return err
	}
	fmt.Println("✅ relay node confirmed")
	return nil
}

// RemoveRelayNode implements `mage removerelaynode <multiaddr>`: deletes
// a confirmed relay-node record outright. Only takes effect if the
// current node is itself a raft voter.
// Usage: mage removerelaynode <multiaddr>
func RemoveRelayNode(multiaddr string) error {
	if err := kvctl.RemoveRelayNode(multiaddr); err != nil {
		return err
	}
	fmt.Println("✅ relay node removed")
	return nil
}

// GetRelayNode implements `mage getrelaynode <multiaddr>`: prints
// multiaddr's current confirmed relay-node record as one JSON object.
// Usage: mage getrelaynode <multiaddr>
func GetRelayNode(multiaddr string) error {
	node, err := kvctl.GetRelayNode(multiaddr)
	if err != nil {
		return err
	}
	out, err := json.Marshal(node)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListRelayNodes implements `mage listrelaynodes`: prints every confirmed
// relay-node record, one JSON object per line, in the same ascending-
// priority order relayCandidates prefers them in.
// Usage: mage listrelaynodes
func ListRelayNodes() error {
	nodes, err := kvctl.ListRelayNodes()
	if err != nil {
		return err
	}
	for _, n := range nodes {
		out, err := json.Marshal(n)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// bootstrapNodesConfigPath is configs/bootstrap-nodes.json, relative to the
// repo root -- a human-maintained record of already-deployed leader nodes
// (ssh host, install dir, port, peer id/multiaddrs). Nothing in this repo
// deploys to it automatically; it's updated by hand (or by whoever ran the
// SSH bootstrap in README's "Leader on a remote machine" section) after the
// fact, same as registry.json on the node itself.
const bootstrapNodesConfigPath = "configs/bootstrap-nodes.json"

// bootstrapNodesFile is configs/bootstrap-nodes.json's shape.
type bootstrapNodesFile struct {
	BootstrapNodes []bootstrapNodeConfig `json:"bootstrap_nodes"`
}

type bootstrapNodeConfig struct {
	Name         string   `json:"name"`
	SSHHost      string   `json:"ssh_host"`
	InstallDir   string   `json:"install_dir"`
	ListenPort   int      `json:"listen_port"`
	RelayService bool     `json:"relay_service"`
	PeerID       string   `json:"peer_id"`
	DataDir      string   `json:"data_dir"`
	KeyPath      string   `json:"key_path"`
	ListenAddrs  []string `json:"listen_addrs"`
}

// BootstrapNodes prints the leader nodes recorded in
// configs/bootstrap-nodes.json -- each one an already-deployed kvnode
// leader on a remote host, not something this target deploys itself (see
// bootstrapNodesConfigPath's doc comment).
//
// Usage: mage bootstrapnodes
func BootstrapNodes() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path := filepath.Join(root, bootstrapNodesConfigPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("bootstrapnodes: read %s: %w", path, err)
	}
	var cfg bootstrapNodesFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("bootstrapnodes: parse %s: %w", path, err)
	}
	if len(cfg.BootstrapNodes) == 0 {
		fmt.Printf("no bootstrap nodes configured in %s\n", bootstrapNodesConfigPath)
		return nil
	}
	for _, n := range cfg.BootstrapNodes {
		fmt.Printf("%s\n", n.Name)
		fmt.Printf("  ssh:     %s\n", n.SSHHost)
		fmt.Printf("  install: %s (port %d, relay-service=%v)\n", n.InstallDir, n.ListenPort, n.RelayService)
		fmt.Printf("  peer id: %s\n", n.PeerID)
		for _, addr := range n.ListenAddrs {
			fmt.Printf("  addr:    %s\n", addr)
		}
	}
	return nil
}
