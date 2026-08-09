package shmevent

import "encoding/json"

// This file turns KindBootstrapNode (see system.go's doc comment: "a
// known circuit-relay v2 server's multiaddr this node can dial for a
// relay reservation") from a registry nothing ever reads back into the
// source pkg/daemon's newHost draws its relay candidate list from --
// see that package's relayCandidates. Request/confirm/revoke (the same
// EventPermitRequest/EventPermitConfirm/EventPermitRevoke lifecycle
// KindPermitPeer already uses) is this record's create/activate/delete;
// this file adds the read side (BootstrapNodeKeyBounds, for a
// pkg/shmclient.Session.ListRange scan) and a priority-aware payload
// codec, since a plain multiaddr string alone -- KindBootstrapNode's
// original metadata shape -- has no room to express failover ordering
// among more than one registered relay.

// bootstrapNodeStatusPlaceholder would be StatusConfirmed for the only
// records callers ever list (ListRelayNodes-style reads only care about
// confirmed, promoted-by-a-voter entries -- a still-pending request has
// no standing yet, exactly like KindPermitPeer). Kept as a named constant
// purely for BootstrapNodeKeyBounds' own clarity; SystemKey itself takes
// StatusConfirmed directly everywhere else this kind is touched.
const bootstrapNodeStatusPlaceholder = StatusConfirmed

// BootstrapNodeKeyBounds returns the [lo, hi] key range covering every
// currently-*confirmed* KindBootstrapNode record -- the enumeration
// counterpart to a single SystemKey(KindBootstrapNode, StatusConfirmed,
// peerID) lookup, for a raw range scan (see
// pkg/shmclient.Session.ListRange/EventListRange). Mirrors
// ClusterMemberKeyBounds' shape exactly.
func BootstrapNodeKeyBounds() (lo, hi []byte) {
	prefix := SystemKey(KindBootstrapNode, bootstrapNodeStatusPlaceholder, nil)
	lo = prefix
	hi = make([]byte, len(prefix)+64)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi
}

// bootstrapNodeMetadata is EncodeBootstrapNodeMetadata/
// DecodeBootstrapNodeMetadata's JSON shape.
type bootstrapNodeMetadata struct {
	Addr     string `json:"addr"`
	Priority uint8  `json:"priority"`
}

// EncodeBootstrapNodeMetadata packs a dialable multiaddr (including its
// trailing /p2p/<peerID> component -- the same full form
// Config.RelayPeers/-relay-peer/configs/bootstrap-nodes.json entries
// already use, since pkg/daemon's relayCandidates parses it straight back
// into a peer.AddrInfo via peer.AddrInfoFromP2pAddr) and a failover
// priority into the metadata argument NewPermitRequest expects for a
// KindBootstrapNode request. Lower priority values are preferred first --
// priority 0 is tried before priority 1 -- mirroring common "0 is
// highest" routing-preference convention.
func EncodeBootstrapNodeMetadata(addr string, priority uint8) string {
	b, _ := json.Marshal(bootstrapNodeMetadata{Addr: addr, Priority: priority})
	return string(b)
}

// DecodeBootstrapNodeMetadata is the inverse of EncodeBootstrapNodeMetadata.
func DecodeBootstrapNodeMetadata(metadata string) (addr string, priority uint8, err error) {
	var m bootstrapNodeMetadata
	if err := json.Unmarshal([]byte(metadata), &m); err != nil {
		return "", 0, err
	}
	return m.Addr, m.Priority, nil
}
