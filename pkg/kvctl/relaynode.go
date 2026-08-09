package kvctl

import (
	"context"
	"fmt"
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file implements CRUD for shmevent.KindBootstrapNode -- the relay
// list pkg/daemon's relayCandidates draws its EnableAutoRelayWithStaticRelays
// candidate set from (see that function's own doc comment). Unlike
// Group/Command (catalog.go), this reuses KindPermitPeer's existing
// two-stage request/confirm/revoke lifecycle (RequestPermit/ConfirmPermit/
// RevokePermit in kvctl.go) as create/activate/delete, rather than a new
// single-step Put/Delete pair -- KindBootstrapNode already had that
// lifecycle wired end to end before this file existed; what was missing
// was a read side (ListRelayNodes/GetRelayNode) and the actual consumption
// in pkg/daemon.

// RelayNode is a confirmed shmevent.KindBootstrapNode record. PeerID is
// derived from Multiaddr's own trailing /p2p/<peerID> component (the
// record's storage key), not tracked as an independent field.
type RelayNode struct {
	PeerID    string `json:"peer_id"`
	Multiaddr string `json:"multiaddr"`
	Priority  uint8  `json:"priority"`
}

// relayNodePeerID extracts the peer id embedded in a dialable multiaddr's
// trailing /p2p/<peerID> component -- the same validation pkg/daemon's
// newHost/relayCandidates already require of every relay address, so a
// malformed one is caught here rather than silently dropped later.
func relayNodePeerID(addr string) (peer.ID, error) {
	maddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return "", fmt.Errorf("kvctl: invalid relay node address %q: %w", addr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return "", fmt.Errorf("kvctl: relay node address %q missing peer id: %w", addr, err)
	}
	return info.ID, nil
}

// AddRelayNode implements `mage addrelaynode <multiaddr> <priority>`:
// lodges a pending shmevent.KindBootstrapNode record for addr's own peer
// id -- the "create" half of this record's two-stage lifecycle, see
// ConfirmRelayNode for "activate". Any raft node may originate one, so
// this needs no special standing of its own (mirrors RequestPermit).
func AddRelayNode(addr string, priority uint8) error {
	pid, err := relayNodePeerID(addr)
	if err != nil {
		return err
	}
	metadata := shmevent.EncodeBootstrapNodeMetadata(addr, priority)
	if err := RequestPermit(shmevent.KindBootstrapNode, []byte(pid.String()), metadata); err != nil {
		return fmt.Errorf("kvctl: add relay node: %w", err)
	}
	return nil
}

// ConfirmRelayNode implements `mage confirmrelaynode <multiaddr>`:
// promotes a pending relay-node record to confirmed -- the "activate"
// half, see AddRelayNode for "create". Only takes effect if the current
// node is itself a raft voter. Once confirmed, every cluster member picks
// it up via pkg/daemon's relayCandidates on their own next (re)start or
// join -- ordinary FSM replication already carries the record itself, no
// separate broadcast needed.
func ConfirmRelayNode(addr string) error {
	pid, err := relayNodePeerID(addr)
	if err != nil {
		return err
	}
	if err := ConfirmPermit(shmevent.KindBootstrapNode, []byte(pid.String())); err != nil {
		return fmt.Errorf("kvctl: confirm relay node: %w", err)
	}
	return nil
}

// RemoveRelayNode implements `mage removerelaynode <multiaddr>`: deletes
// a confirmed relay-node record outright -- "delete", see AddRelayNode/
// ConfirmRelayNode for "create"/"activate". Only takes effect if the
// current node is itself a raft voter.
func RemoveRelayNode(addr string) error {
	pid, err := relayNodePeerID(addr)
	if err != nil {
		return err
	}
	if err := RevokePermit(shmevent.KindBootstrapNode, []byte(pid.String())); err != nil {
		return fmt.Errorf("kvctl: remove relay node: %w", err)
	}
	return nil
}

// GetRelayNode implements `mage getrelaynode <multiaddr>`: reads back
// addr's own confirmed relay-node record.
func GetRelayNode(addr string) (RelayNode, error) {
	pid, err := relayNodePeerID(addr)
	if err != nil {
		return RelayNode{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return RelayNode{}, err
	}
	key := shmevent.SystemKey(shmevent.KindBootstrapNode, shmevent.StatusConfirmed, []byte(pid.String()))
	value, err := sess.Get(ctx, string(key))
	if err != nil {
		return RelayNode{}, fmt.Errorf("kvctl: relay node %s not found", addr)
	}
	multiaddrStr, priority, err := shmevent.DecodeBootstrapNodeMetadata(value)
	if err != nil {
		return RelayNode{}, fmt.Errorf("kvctl: decode relay node %s: %w", addr, err)
	}
	return RelayNode{PeerID: pid.String(), Multiaddr: multiaddrStr, Priority: priority}, nil
}

// ListRelayNodes implements `mage listrelaynodes`: returns every
// confirmed relay-node record, sorted by ascending priority -- the same
// preference order pkg/daemon's relayCandidates applies -- nil, not an
// error, when none exist.
func ListRelayNodes() ([]RelayNode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}
	lo, hi := shmevent.BootstrapNodeKeyBounds()
	var nodes []RelayNode
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list relay nodes: %w", err)
		}
		if !ok {
			break
		}
		if len(key) < systemKeyIDOffset {
			return nil, fmt.Errorf("kvctl: malformed relay node key %x", key)
		}
		multiaddrStr, priority, err := shmevent.DecodeBootstrapNodeMetadata(string(value))
		if err != nil {
			return nil, fmt.Errorf("kvctl: decode relay node %x: %w", key, err)
		}
		nodes = append(nodes, RelayNode{PeerID: string(key[systemKeyIDOffset:]), Multiaddr: multiaddrStr, Priority: priority})
		lo = append(append([]byte{}, key...), 0x00)
	}
	sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].Priority < nodes[j].Priority })
	return nodes, nil
}
