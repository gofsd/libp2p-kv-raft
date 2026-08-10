package daemon

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// handleShmEvent dispatches one decoded pkg/shmevent.Msg to the appropriate
// callerIdentity is who handleShmEvent should treat as the sender of a
// request: the public key its signature must verify against, and -- for a
// genuinely remote, libp2p-authenticated caller -- the peer id that
// identity resolves to. The zero value means "local pkg/ipc caller":
// verify against this node's own shared key, exactly as before (see
// pkg/shmevent's doc comment on the shmring same-machine trust boundary).
// remotePeer being non-empty is also what lets handleShmEvent tell a
// genuinely remote caller apart from a local one for the
// remote-only restrictions below (no key-fetch bootstrap, optional permit
// gate) -- it is never derived from anything the message itself claims.
type callerIdentity struct {
	verifyPub  shmevent.PublicKey
	remotePeer peer.ID // "" for a local caller
}

// localCaller is every pkg/ipc (shmring) request's identity: this node's
// own shared key, the same one it hands out via EventGetPrivateKey to a
// same-machine caller with no key yet -- see pkg/shmevent's doc comment.
func (n *Node) localCaller() callerIdentity {
	return callerIdentity{verifyPub: n.ed25519Pub}
}

// remoteCaller derives a callerIdentity from s's own libp2p-authenticated
// identity (the Noise/TLS handshake's remote public key/peer id) rather
// than anything the message itself claims -- mirroring the RemotePeer()
// pattern handleForwardConfirmStream already uses for its voter-only
// confirm check.
func remoteCaller(s network.Stream) (callerIdentity, error) {
	pub := s.Conn().RemotePublicKey()
	if pub == nil {
		return callerIdentity{}, fmt.Errorf("remote caller: connection has no authenticated public key")
	}
	raw, err := pub.Raw()
	if err != nil {
		return callerIdentity{}, fmt.Errorf("remote caller: %w", err)
	}
	return callerIdentity{verifyPub: raw, remotePeer: s.Conn().RemotePeer()}, nil
}

// forgetTransientPeer removes id's peerstore entry (addresses, public key,
// supported-protocol bookkeeping) once every connection to it has closed,
// unless id is a *current* raft voter/learner (checked live against
// n.getRaft's configuration, not isClusterMember's persistent-forever
// KindClusterMember record just below -- that would keep every peer this
// node has ever admitted, exactly the growth this exists to stop). The
// default libp2p peerstore this project constructs (see newHost) never
// evicts protobook/keybook/metadata entries on its own; only the addrbook
// has any TTL, and only for addresses added with one. A long-lived node
// dialed by hundreds of distinct, never-returning peers over weeks (e.g.
// this project's own e2e pipeline, which mints a fresh identity per test
// run) would otherwise keep every single one's peerstore entry forever --
// one of two confirmed root causes (the other: streamRequestTimeout) of a
// real production node found running at 4.95GB RSS against ~265MB of
// real on-disk data after 14 days of uptime.
//
// A current cluster member's entry must survive a disconnect -- raft
// needs to redial it, and dialForward already re-adds its address on
// every forwarded call regardless, so removing it here costs nothing
// except a redial's address lookup that would otherwise be a cache hit.
//
// So must a relay this node reserves through, and for a much less
// forgiving reason: nothing re-adds its address. AutoRelay refreshes a
// reservation by peer id alone (relayFinder.refreshRelayReservation
// passes a bare peer.AddrInfo{ID}), so it reads the addresses straight
// out of the peerstore -- wipe them and the refresh cannot dial, the
// relay drops out of the finder's set, this node's /p2p-circuit address
// disappears from host.Addrs(), and the relay itself drops the
// reservation as soon as the connection is gone. The device is then
// unreachable while still believing it asked for and received relay
// standing. That is exactly what the two-device pair scenario kept
// hitting: a device reported a real circuit address one moment and a bare
// loopback one seconds later, and any peer dialing the address it had
// handed out got NO_RESERVATION from the relay. Relay candidates are a
// small configured set (Config.RelayPeers plus confirmed
// KindBootstrapNode records), not the unbounded stream of strangers this
// function exists to forget, so exempting them costs nothing this was
// built to save.
func (n *Node) forgetTransientPeer(id peer.ID) {
	if n.host.Network().Connectedness(id) == network.Connected {
		// Another connection to this same peer is still open (or one
		// raced back in right as this one closed) -- not actually gone.
		return
	}
	if rf := n.getRaft(); rf != nil {
		if cfgFuture := rf.GetConfiguration(); cfgFuture.Error() == nil {
			for _, srv := range cfgFuture.Configuration().Servers {
				if srv.ID == raft.ServerID(id.String()) {
					return
				}
			}
		}
	}
	if n.isRelayCandidate(id) {
		return
	}
	n.host.Peerstore().RemovePeer(id)
}

// isInGroupSt is isInGroup's *store.Store-only twin, usable from relayACL
// (constructed inside newHost, before a *Node exists -- see start) as
// well as from (*Node).isInGroup itself, so the two checks never drift.
// A malformed groupID (longer than PeerGroupKey allows) simply reports
// false rather than erroring.
func isInGroupSt(st *store.Store, id peer.ID, groupID string) bool {
	key, err := shmevent.PeerGroupKey([]byte(id.String()), []byte(groupID))
	if err != nil {
		return false
	}
	_, err = st.Get(key)
	return err == nil
}

// isAuthorizedForGatedAccessSt is isAuthorizedForGatedAccess's
// *store.Store-only twin, usable from relayACL the same way isInGroupSt
// is -- see that function's doc comment for why. selfPeerID is the
// gating node's own peer id string (relayACL's own identity, standing in
// for (*Node).peerID).
func isAuthorizedForGatedAccessSt(st *store.Store, selfPeerID string, id peer.ID, groupID string) bool {
	return isInGroupSt(st, id, shmevent.ReservedGroupCluster) ||
		isInGroupSt(st, id, groupID) ||
		isInGroupSt(st, id, selfPeerID)
}

// isAuthorizedForGatedAccess reports whether id may use a resource gated
// the way channel and relay both are: current cluster membership
// (ReservedGroupCluster), operator-granted membership in groupID (e.g.
// ReservedGroupChannel/ReservedGroupRelay), or a pairwise personal grant
// into this node's own peer-id group (see isPeerIdentityGroupID's doc
// comment on the pairwise-grant mechanism this last check enables).
func (n *Node) isAuthorizedForGatedAccess(id peer.ID, groupID string) bool {
	return isAuthorizedForGatedAccessSt(n.store, n.peerID, id, groupID)
}

// isPeerIdentityGroupID reports whether id parses as a valid libp2p peer
// id -- pkg/daemon's own personal-group naming convention (see
// ensurePersonalGroup): every peer that ever becomes a raft member, or
// solo-bootstraps its own one-node cluster, gets its own reserved Group
// record whose id is that peer's own peer id string, auto-created the
// same way shmevent.ReservedGroupCluster/Voter/Learner/Channel are. Its
// own Group record is equally protected -- EventGroupPut/EventGroupDelete
// reject any id shaped like a peer id, the same as the seven fixed
// reserved names -- but unlike cluster/voter/learner (and like channel),
// its PeerGroup *membership* stays an ordinary operator grant: the
// mechanism for one specific peer to let one specific other peer open a
// channel to it, independent of cluster/channel-group standing entirely.
// To make a bidirectional channel between peer A and peer B: A (or any
// voter in A's own cluster) runs `addpeertogroup B A`, and B (or any
// voter in B's own cluster) runs `addpeertogroup A B` -- each grant lives
// in the granting peer's own store, so this works between any two peers
// regardless of whether they share a raft cluster at all.
func isPeerIdentityGroupID(id string) bool {
	_, err := peer.Decode(id)
	return err == nil
}

// rejectReservedKey refuses an ordinary EventSet/EventSetField write
// whose key starts with a byte this codebase reserves for its own
// internal bookkeeping -- shmevent.SystemKeyPrefix (permits/bootstrap
// nodes/cluster membership) or logrecord.LogKeyPrefix (log records) --
// so a caller can never collide with (or corrupt) either namespace by
// chance or malice. Both prefixes are single reserved bytes checked the
// same way; kept as one helper so a third reserved namespace only needs
// one new case here, not a change at every call site.
func rejectReservedKey(key []byte) error {
	if len(key) == 0 {
		return nil
	}
	switch key[0] {
	case shmevent.SystemKeyPrefix:
		return fmt.Errorf("key namespace starting with 0x%02x is reserved for system use", shmevent.SystemKeyPrefix)
	case logrecord.LogKeyPrefix:
		return fmt.Errorf("key namespace starting with 0x%02x is reserved for logrecord use", logrecord.LogKeyPrefix)
	default:
		return nil
	}
}

// relayACL is the v2relay.ACLFilter unconditionally wired into newHost
// (via v2relay.WithACL) whenever Config.RelayService is on -- the relay
// counterpart to handleChannelStream's gate, using the identical
// group-ACL mechanism (isAuthorizedForGatedAccessSt against
// shmevent.ReservedGroupRelay) instead of the confirmed-KindPermitPeer
// check this used to be. Both reservation (AllowReserve) and
// outgoing-connect (AllowConnect) check the peer that's trying to use
// *this* node's relay service -- the destination in AllowConnect is who
// src is dialing through the relay, not itself requesting anything, so
// it's never checked here.
//
// relayACL holds *store.Store rather than *Node because it's constructed
// inside newHost, before any *Node exists yet (see that function's own
// doc comment) -- selfPeerID/quota stand in for the two Node fields
// (peerID, relayQuota) the group-ACL and quota checks would otherwise
// read off n directly.
//
// Every peer this gate admits still shares the same node-wide
// shmevent.RelayLimits (EventPermitRequest stamps this node's current
// defaults onto a KindPermitPeer record purely for legacy/informational
// purposes at request time -- see relayLimits/EncodePermitPeerPayload):
// go-libp2p's circuitv2 relay hands one v2relay.Resources value to every
// admitted peer alike (see newHost), with no hook for a per-individual-
// peer override of those static per-circuit/per-reservation caps -- quota
// (below) is this package's own answer to that gap, a cumulative
// event-rate budget go-libp2p has no equivalent of.
type relayACL struct {
	store      *store.Store
	selfPeerID string
	quota      *quotaTracker
}

func (a relayACL) AllowReserve(p peer.ID, ra multiaddr.Multiaddr) bool {
	return a.allow(p, ra)
}

func (a relayACL) AllowConnect(src peer.ID, ra multiaddr.Multiaddr, _ peer.ID) bool {
	return a.allow(src, ra)
}

// allow is AllowReserve/AllowConnect's shared gate: group-ACL admission
// first (cheap, no allocation), then a 1-event debit against both the
// peer- and IP-keyed relay quota bucket. ra is go-libp2p's own multiaddr
// for the candidate peer's connection -- extractRemoteIP turns it into
// quotaTracker's IP-bucket key, degrading to peer-only quota (IP bucket
// skipped) if ra is nil or carries no resolvable IP.
func (a relayACL) allow(p peer.ID, ra multiaddr.Multiaddr) bool {
	if !isAuthorizedForGatedAccessSt(a.store, a.selfPeerID, p, shmevent.ReservedGroupRelay) {
		return false
	}
	return a.quota.allow(p.String(), extractRemoteIP(ra), 1)
}

// raft/store/registry operation and returns the Msg to send back -- the
// single entry point both pkg/ipc.Serve (local shared memory) and
// handleClientStream (ClientProtocolID, the remote equivalent for a
// browser learner) call into. See pkg/shmevent's doc comment for the
// overall protocol design and api/shmevent.capnp for the wire struct.
//
// caller identifies who's asking (see callerIdentity) -- a remote caller
// gets several restrictions a local one doesn't: EventGetPrivateKey/
// EventGetPublicKey are refused outright (a remote caller always has its
// own key already -- see web-app's do_connect -- so the bootstrap
// exception that exists for a same-machine caller with no key yet has no
// legitimate remote use, and serving it remotely would just hand out this
// node's own private key to anyone able to dial it), and every event but
// EventPermitRequest/EventPermitConfirm/EventSetKey/EventGetKey/EventAdd
// additionally requires either isAuthorizedForGatedAccess(caller,
// shmevent.ReservedGroupRemote) (a current cluster member, an explicit
// "remote" group grant, or a pairwise personal grant) or
// isCommandLogCarveOut's narrow exception -- see that function's doc
// comment. EventSetKey/EventGetKey are exempt because they never touch
// the store (just a per-connection relay of a value the caller itself
// supplied); EventAdd is exempt because it's the actual join door for a
// brand-new peer with no standing at all (a browser learner's first-ever
// call on this exact surface) -- handleAddDispatch self-authenticates
// that case against the stream's own libp2p identity instead.

// logKindOfBound extracts the fixed-format kind field directly out of a
// raw logrecord-namespaced key or ListRange bound, tolerating both a bound
// that ends right after the kind field (logrecord.KindPrefix's own shape,
// which appends no unitID/timestamp -- see e.g. kvctl's kindPrefixBounds,
// used by ListCommandRequests/ListExecutionsByPeer) and a fully-formed
// key/ScanBounds bound that continues past it (logrecord.BuildKey's own
// shape, used by scanRevisions) -- isCommandLogCarveOut needs to recognize
// both real wire shapes a legitimate caller sends, not just one.
func logKindOfBound(key []byte) (kind string, ok bool) {
	if len(key) < 3 || key[0] != logrecord.LogKeyPrefix {
		return "", false
	}
	kindLen := int(key[1])<<8 | int(key[2])
	if 3+kindLen > len(key) {
		return "", false
	}
	return string(key[3 : 3+kindLen]), true
}

// isCommandLogCarveOut is handleShmEvent's sole exception to the
// ReservedGroupRemote gate for a caller that isn't a cluster member and
// holds no "remote" group grant: it lets such a peer submit a command
// linked to a public shmevent Group (EventLogAppend targeting a
// shmevent.CommandRequestLogKind key -- the actual write this admits
// structurally here is still separately, raft-authoritatively enforced by
// kvfsm.OpAppendCommandRequest's IsPermittedForCommand, unchanged by this
// function) and read back that same dispatch's own records: its
// CommandRequestLogKind request queue (gated by the identical
// IsPermittedForCommand check, evaluated here too since a read has no
// Apply step of its own to enforce it otherwise), its own
// CommandExecIndexKind execution index (self only -- the peer id embedded
// in the kind must match the caller's own), and shmevent.CommandExecLogKind
// itself (any instance id -- "possessing the instance id is the
// credential" was already this kind's design, see pkg/kvctl's
// GetCommandRequest doc comment, so no further check is layered on top of
// recognizing the kind). This -- together with Join/Recruit, which are
// their own separate, already-gated protocols entirely outside this
// function's remote surface -- is deliberately the only door a peer with
// no other standing has into an otherwise closed cluster.
func (n *Node) isCommandLogCarveOut(m shmevent.Msg, callerID peer.ID) bool {
	switch m.Which() {
	case shmevent.Event_Which_logAppend:
		key, err := m.LogAppend().Key()
		if err != nil {
			return false
		}
		kind, ok := logKindOfBound(key)
		if !ok {
			return false
		}
		_, ok = shmevent.ParseCommandRequestLogKind(kind)
		return ok
	case shmevent.Event_Which_getFieldByKey:
		key, err := m.GetFieldByKey().Key()
		if err != nil {
			return false
		}
		kind, ok := logKindOfBound(key)
		if !ok {
			return false
		}
		return n.isCommandLogReadableKind(kind, callerID)
	case shmevent.Event_Which_listRange:
		grp := m.ListRange()
		start, err := grp.Start()
		if err != nil {
			return false
		}
		end, err := grp.End()
		if err != nil {
			return false
		}
		startKind, ok := logKindOfBound(start)
		if !ok {
			return false
		}
		endKind, ok := logKindOfBound(end)
		if !ok || startKind != endKind {
			return false
		}
		return n.isCommandLogReadableKind(startKind, callerID)
	default:
		return false
	}
}

// isCommandLogReadableKind is isCommandLogCarveOut's read-side kind check
// -- see that function's doc comment for what each case admits and why.
func (n *Node) isCommandLogReadableKind(kind string, callerID peer.ID) bool {
	if kind == shmevent.CommandExecLogKind {
		return true
	}
	if commandID, ok := shmevent.ParseCommandRequestLogKind(kind); ok {
		permitted, err := kvfsm.IsPermittedForCommand(n.store, []byte(commandID), []byte(callerID.String()))
		return err == nil && permitted
	}
	if peerID, ok := shmevent.ParseCommandExecIndexKind(kind); ok {
		return peerID == callerID.String()
	}
	return false
}

// requireVoter enforces "only a raft voter may act" for a remote caller
// -- skipped for a local caller (trusted implicitly). Shared by every
// catalog/permit case above that needs it.
func requireVoter(n *Node, caller callerIdentity) error {
	if caller.remotePeer == "" {
		return nil
	}
	rf := n.getRaft()
	if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
		return fmt.Errorf("%s is not a current raft voter", caller.remotePeer)
	}
	return nil
}

// errorMsg builds the response for a failed request -- see the error
// variant's doc comment in api/shmevent.capnp for why this exists even
// though it isn't part of the fields the protocol was originally
// specified with.
func errorMsg(id uint16, err error) shmevent.Msg {
	resp, buildErr := shmevent.NewError(err.Error())
	if buildErr != nil {
		resp, _ = shmevent.NewError("")
	}
	resp.SetId(id)
	return resp
}
