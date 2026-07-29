package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file is the gomobile-bound relay-node CRUD surface, mirroring
// desktop's pkg/kvctl/relaynode.go one-for-one: shmevent.KindBootstrapNode
// records, reusing RequestPermit/ConfirmPermit/RevokePermit (kind
// "bootstrap", above) as create/activate/delete, plus the read side
// (GetRelayNode/ListRelayNodes) neither side had before. pkg/daemon's
// relayCandidates draws its EnableAutoRelayWithStaticRelays failover
// candidate list from every currently-confirmed record here, merged with
// this device's own build-time relayMultiaddr seed (see kvmobile.go's
// relayPeers) -- so a device's relay options can grow across the
// cluster's lifetime with no rebuild needed.

// RelayNode is a confirmed shmevent.KindBootstrapNode record. PeerID is
// derived from Multiaddr's own trailing /p2p/<peerID> component (the
// record's storage key), not tracked as an independent field.
type RelayNode struct {
	PeerID    string `json:"peer_id"`
	Multiaddr string `json:"multiaddr"`
	Priority  int    `json:"priority"`
}

// relayNodePeerID extracts the peer id embedded in a dialable multiaddr's
// trailing /p2p/<peerID> component -- the same validation pkg/daemon's
// newHost/relayCandidates already require of every relay address.
func relayNodePeerID(addr string) (peer.ID, error) {
	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return "", fmt.Errorf("kvmobile: invalid relay node address %q: %w", addr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("kvmobile: relay node address %q missing peer id: %w", addr, err)
	}
	return info.ID, nil
}

// AddRelayNode lodges a pending relay-node record for multiaddr's own
// embedded /p2p/<peerID> -- the "create" half of this record's two-stage
// lifecycle, see ConfirmRelayNode for "activate". priority (0-255; lower
// values are tried first) is clamped into that range. Any raft node may
// originate one, so this needs no special standing of its own.
func AddRelayNode(multiaddr string, priority int) error {
	pid, err := relayNodePeerID(multiaddr)
	if err != nil {
		return err
	}
	if priority < 0 {
		priority = 0
	}
	if priority > 255 {
		priority = 255
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	metadata := shmevent.EncodeBootstrapNodeMetadata(multiaddr, uint8(priority))
	if err := sess.RequestPermit(ctx, shmevent.KindBootstrapNode, []byte(pid.String()), metadata); err != nil {
		return fmt.Errorf("kvmobile: add relay node: %w", err)
	}
	return nil
}

// ConfirmRelayNode promotes a pending relay-node record for multiaddr to
// confirmed -- the "activate" half, see AddRelayNode for "create". Only
// takes effect if this device is itself a raft voter.
func ConfirmRelayNode(multiaddr string) error {
	pid, err := relayNodePeerID(multiaddr)
	if err != nil {
		return err
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.ConfirmPermit(ctx, shmevent.KindBootstrapNode, []byte(pid.String())); err != nil {
		return fmt.Errorf("kvmobile: confirm relay node: %w", err)
	}
	return nil
}

// RemoveRelayNode deletes a confirmed relay-node record for multiaddr
// outright -- "delete", see AddRelayNode/ConfirmRelayNode for
// "create"/"activate". Only takes effect if this device is itself a raft
// voter.
func RemoveRelayNode(multiaddr string) error {
	pid, err := relayNodePeerID(multiaddr)
	if err != nil {
		return err
	}
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RevokePermit(ctx, shmevent.KindBootstrapNode, []byte(pid.String())); err != nil {
		return fmt.Errorf("kvmobile: remove relay node: %w", err)
	}
	return nil
}

// GetRelayNode returns multiaddr's current confirmed relay-node record as
// a JSON RelayNode, or an error if it doesn't exist.
func GetRelayNode(multiaddr string) (string, error) {
	pid, err := relayNodePeerID(multiaddr)
	if err != nil {
		return "", err
	}
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	key := shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusConfirmed, []byte(pid.String()))
	value, err := sess.Get(ctx, string(key))
	if err != nil {
		return "", fmt.Errorf("kvmobile: relay node %s not found", multiaddr)
	}
	addr, priority, err := shmevent.DecodeBootstrapNodeMetadata([]byte(value))
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode relay node %s: %w", multiaddr, err)
	}
	out, err := json.Marshal(RelayNode{PeerID: pid.String(), Multiaddr: addr, Priority: int(priority)})
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode relay node: %w", err)
	}
	return string(out), nil
}

// ListRelayNodes returns every confirmed relay-node record as a JSON
// array (`"[]"` when none exist), sorted by ascending priority -- the
// same preference order pkg/daemon's relayCandidates applies.
func ListRelayNodes() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	lo, hi := shmevent.BootstrapNodeKeyBounds()
	nodes := []RelayNode{}
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list relay nodes: %w", err)
		}
		if !ok {
			break
		}
		if len(key) < systemKeyIDOffset {
			return "", fmt.Errorf("kvmobile: malformed relay node key %x", key)
		}
		addr, priority, err := shmevent.DecodeBootstrapNodeMetadata(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: decode relay node %x: %w", key, err)
		}
		nodes = append(nodes, RelayNode{PeerID: string(key[systemKeyIDOffset:]), Multiaddr: addr, Priority: int(priority)})
		lo = append(append([]byte{}, key...), 0x00)
	}
	sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].Priority < nodes[j].Priority })
	out, err := json.Marshal(nodes)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode relay nodes: %w", err)
	}
	return string(out), nil
}
