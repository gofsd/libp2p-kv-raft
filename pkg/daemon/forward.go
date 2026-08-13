package daemon

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// handleSetForward is the entry point for a Set (EventSetField, or
// handleForwardSetStream's leader-side answer to an already-forwarded
// request): it applies directly if this node is the leader, or forwards
// to the leader (one hop only) if not. allowForward is false on the
// already-forwarded path so a request can be forwarded at most once: if
// the node it lands on then *also* turns out not to be leader (a
// leadership change mid-flight), it fails outward with a clear error
// instead of forwarding again, which rules out any forwarding cycle
// regardless of how leadership bounces around. A thin kvfsm.OpSet
// wrapper around handleOpForward -- see that doc comment for why
// OpAppendCommandRequest also goes through here rather than through
// handleConfirmForward.
func (n *Node) handleSetForward(ctx context.Context, key, value []byte, allowForward bool) error {
	_, err := n.handleOpForward(ctx, kvfsm.OpSet, key, value, allowForward)
	return err
}

// handleOpForward is handleSetForward generalized to any op that -- like
// OpSet, and unlike OpConfirm/OpDel/OpCascadeDelete/the catalog's OpSet
// puts -- needs no sender-is-a-raft-voter check at the forwarding hop:
// OpAppendCommandRequest's actual authorization (the submitting peer's
// Group/GroupCommand/PeerGroup/Public ACL standing, checked against
// authorPeerID inside kvfsm.Apply itself) doesn't depend at all on
// whether the *node relaying the write* happens to be a voter -- a
// non-voting learner (every web-app browser tab, or a phone joined with
// suffrage "learner") must still be able to submit a command it's
// permitted for and have it forwarded on to the leader. Routing it
// through the voter-gated ForwardConfirmProtocolID instead would reject
// exactly that legitimate case with "not a current raft voter", even
// though Apply's own ACL check is what's actually supposed to gate it.
// handleOpForward returns the raft log index the write ended up applied
// at (this node's own index if it's the leader, the leader's index if
// forwarded) alongside the usual error -- see forwardOp/waitForLocalApply
// for why a forwarding follower needs that index before it can safely
// report success to its own caller.
func (n *Node) handleOpForward(ctx context.Context, op kvfsm.OpType, key, value []byte, allowForward bool) (uint64, error) {
	// Both wait windows below scale off the actual configured election
	// timeout (not a fixed constant) so a WAN-tuned longer timeout still
	// gets a comfortable margin: Apply itself can legitimately take a full
	// election cycle if the leader steps down and a new one is elected
	// mid-call.
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return 0, err
	}
	if isLeader {
		return n.applyOp(rf, op, key, value)
	}
	if !allowForward {
		return 0, fmt.Errorf("not leader; current leader is %s (already forwarded once)", leaderID)
	}
	index, err := n.forwardOp(ctx, op, leaderID, key, value)
	if err != nil {
		return 0, err
	}
	// The leader's ack only proves the write is durable+applied on the
	// leader -- this node's own copy (which is what any immediately
	// following local Get on this node will read) catches up
	// asynchronously via ordinary AppendEntries replication, on no fixed
	// schedule relative to the ack. Waiting here for our own applied index
	// to reach the leader's closes that read-your-own-writes race for the
	// common case (healthy replication, which is at least as fast as the
	// round trip that already happened to get this ack) without turning a
	// write that already genuinely succeeded into a reported failure: a
	// timeout falls through to returning success anyway, same as before
	// this wait existed, just without the improved freshness guarantee.
	n.waitForLocalApply(rf, index, 10*n.electionTimeout)
	return index, nil
}

func (n *Node) applySet(rf *raft.Raft, key, value []byte) error {
	_, err := n.applyOp(rf, kvfsm.OpSet, key, value)
	return err
}

func (n *Node) applyOp(rf *raft.Raft, op kvfsm.OpType, key, value []byte) (uint64, error) {
	cmd := kvfsm.EncodeCommand(op, key, value)
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return 0, err
	}
	if res, ok := future.Response().(kvfsm.ApplyResult); ok && res.Err != nil {
		return 0, res.Err
	}
	return future.Index(), nil
}

// waitForLocalApply blocks (up to timeout) until this node's own raft FSM
// has applied index -- see handleOpForward's doc comment for why a
// forwarding follower needs this before its own immediately-following
// local reads can be trusted to see the write it just forwarded.
// Best-effort: exceeding timeout is not reported as an error, since the
// write already succeeded on the leader by this point regardless of
// whether this node's own copy has caught up yet.
func (n *Node) waitForLocalApply(rf *raft.Raft, index uint64, timeout time.Duration) {
	if rf.AppliedIndex() >= index {
		return
	}
	deadline := time.After(timeout)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return
		case <-ticker.C:
			if rf.AppliedIndex() >= index {
				return
			}
		}
	}
}

// forwardStatusOK/forwardStatusErr are forwardOp/handleForwardSetStream's
// response framing: a single leading status byte, followed by either an
// 8-byte big-endian raft log index (OK -- the index the write was applied
// at on the leader, which the forwarding follower needs, see
// waitForLocalApply) or a UTF-8 error message (Err). Deliberately
// unambiguous about success -- see handleForwardSetStream's doc comment
// for the failure mode (a relay-adjacent stream reporting every Set as
// successful while nothing was ever persisted) this framing exists to
// rule out: decodeForwardResponse treats anything that isn't exactly this
// shape as an error, never as silent success.
const (
	forwardStatusOK  byte = 0x00
	forwardStatusErr byte = 0x01
)

// dialForward opens a stream to leaderID for one of the forward-*
// protocols below. Every forward-* function used to assume "the libp2p
// host already has an open connection/known address for leaderID -- it's
// the peer this node's own raft transport talks to for AppendEntries --
// so no address resolution is needed beyond the peer id itself", and
// dialed with a bare pid and no relay allowance. That assumption breaks
// exactly when it matters most: a leader only reachable via a relay
// circuit address (see CLAUDE.md's "Known gap" note, found running a
// real 3-node cluster) left every forward-* protocol failing with
// "context deadline exceeded" even though raft's own AppendEntries kept
// replicating fine. The difference is what rafttransport.Dial does that
// this didn't: look up a real address for the peer and allow a
// limited/relay connection for the resulting stream. dialForward closes
// that gap the same way -- pulling leaderID's address from this node's
// own current raft configuration (the same raft.ServerAddress
// rafttransport.Dial already dials successfully for AppendEntries, since
// it's this node's own up-to-date view of the cluster, not a guess) and
// seeding the peerstore with it before dialing, with
// network.WithAllowLimitedConn so a relay/transient connection is
// actually usable for the stream. If no address is on file (e.g. this
// node isn't itself a raft member yet, or a racing configuration read),
// it falls back to the old bare pid dial -- so an already-reachable peer
// never regresses.
func (n *Node) dialForward(ctx context.Context, leaderID raft.ServerID, protoID protocol.ID) (network.Stream, error) {
	pid, err := peer.Decode(string(leaderID))
	if err != nil {
		return nil, fmt.Errorf("invalid leader id %s: %w", leaderID, err)
	}

	if rf := n.getRaft(); rf != nil {
		if cfgFuture := rf.GetConfiguration(); cfgFuture.Error() == nil {
			for _, srv := range cfgFuture.Configuration().Servers {
				if srv.ID != leaderID || srv.Address == "" {
					continue
				}
				maddr, err := multiaddr.NewMultiaddr(string(srv.Address))
				if err != nil {
					break
				}
				info, err := peer.AddrInfoFromP2pAddr(maddr)
				if err != nil || info.ID != pid {
					break
				}
				n.host.Peerstore().AddAddrs(info.ID, info.Addrs, time.Hour)
				connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				n.host.Connect(connectCtx, *info) // best-effort -- NewStream below surfaces the real failure
				cancel()
				break
			}
		}
	}

	return n.host.NewStream(network.WithAllowLimitedConn(ctx, "forward"), pid, protoID)
}

// forwardOp relays op(key, value) to leaderID over ForwardProtocolID and
// returns the raft log index it was applied at -- generalizes what used
// to be a Set-only forwardSet to also carry OpAppendCommandRequest (see
// handleOpForward's doc comment). This is purely internal node-to-node
// machinery (not something a "user" ever speaks -- see pkg/shmevent's doc
// comment), so it reuses kvfsm's own log-command framing directly rather
// than pkg/shmevent's user-facing relational protocol for the request;
// see forwardStatusOK's doc comment for the response framing. See
// dialForward for how leaderID is actually reached.
func (n *Node) forwardOp(ctx context.Context, op kvfsm.OpType, leaderID raft.ServerID, key, value []byte) (uint64, error) {
	s, err := n.dialForward(ctx, leaderID, ForwardProtocolID)
	if err != nil {
		return 0, fmt.Errorf("forward set to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	cmd := kvfsm.EncodeCommand(op, key, value)
	if _, err := s.Write(cmd); err != nil {
		return 0, fmt.Errorf("forward set: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return 0, fmt.Errorf("forward set: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return 0, fmt.Errorf("forward set: read response from leader %s: %w", leaderID, err)
	}
	return decodeForwardResponse(respBuf)
}

// decodeForwardResponse parses forwardOp's response framing -- see
// forwardStatusOK's doc comment for the wire shape and why an
// unrecognized/truncated/empty buf is treated as an error rather than
// success.
func decodeForwardResponse(buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return 0, fmt.Errorf("forward set: empty response from leader")
	}
	switch buf[0] {
	case forwardStatusOK:
		if len(buf) != 9 {
			return 0, fmt.Errorf("forward set: malformed success response (%d bytes)", len(buf))
		}
		return binary.BigEndian.Uint64(buf[1:9]), nil
	case forwardStatusErr:
		return 0, fmt.Errorf("forward set: %s", buf[1:])
	default:
		return 0, fmt.Errorf("forward set: unrecognized response status byte 0x%02x", buf[0])
	}
}

// handleForwardSetStream is the leader-side handler for ForwardProtocolID:
// it decodes a kvfsm-framed command and answers it exactly like a local
// Set/OpAppendCommandRequest would, with forwarding disabled (see
// handleSetForward's allowForward doc). See forwardStatusOK's doc comment
// for the response wire format -- every early return here must write a
// well-formed error frame, never just close the stream: an empty response
// from a read/decode failure would otherwise be indistinguishable from
// genuine success, silently dropping the write while the forwarding
// follower reports it as applied. Found exactly this way -- a follower
// over a relay-adjacent connection (a phone) reporting every Set as
// successful while nothing was ever persisted, anywhere.
// OpAppendCommandRequest and OpTxn are accepted here (not just OpSet) for
// the same reason handleOpForward's doc comment gives: both run their own
// validation/ACL check raft-authoritatively inside kvfsm.Apply (OpTxn's own
// doc comment: "discipline OpAppendCommandRequest's ACL check already
// follows"), so this hop needs no separate sender-is-a-voter gate the way
// OpConfirm/OpDel/OpCascadeDelete do. OpTxn's own omission here (found live
// running the full android_optical_cases catalog against a real
// leader+learner pair) meant every CAS/Txn call from any non-leader node
// failed outright with "expected OpSet or OpAppendCommandRequest, got op 8"
// regardless of whether the operation itself was valid.
func (n *Node) handleForwardSetStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		writeForwardError(s, fmt.Errorf("forward set: read command: %w", err))
		return
	}
	op, key, value, err := kvfsm.DecodeCommand(buf)
	if err != nil {
		writeForwardError(s, fmt.Errorf("forward set: decode command: %w", err))
		return
	}
	if op != kvfsm.OpSet && op != kvfsm.OpAppendCommandRequest && op != kvfsm.OpTxn {
		writeForwardError(s, fmt.Errorf("forward set: expected OpSet, OpAppendCommandRequest, or OpTxn, got op %d", op))
		return
	}

	index, err := n.handleOpForward(context.Background(), op, key, value, false)
	if err != nil {
		writeForwardError(s, err)
		return
	}
	writeForwardSuccess(s, index)
}

// writeForwardError/writeForwardSuccess write forwardStatusOK/
// forwardStatusErr-framed responses -- see forwardStatusOK's doc comment.
// Errors from s.Write itself are deliberately ignored: the stream is
// already being torn down (deferred s.Close() in the caller) and there's
// no further response channel to report a write failure through.
func writeForwardError(s network.Stream, err error) {
	s.Write(append([]byte{forwardStatusErr}, []byte(err.Error())...))
}

func writeForwardSuccess(s network.Stream, index uint64) {
	buf := make([]byte, 9)
	buf[0] = forwardStatusOK
	binary.BigEndian.PutUint64(buf[1:], index)
	s.Write(buf)
}

// handleConfirmForward is EventPermitConfirm/EventPermitRevoke's shared
// counterpart to handleSetForward -- and, since it's exactly "apply
// directly if I'm the voter-implying leader, else forward to a voter-
// checked leader," also the group-based ACL catalog's Put/Delete events'
// (see shmevent.KindGroup's doc comment): applies directly if this node
// is the leader, or forwards to the leader (one hop only, same
// allowForward-guarded pattern) if not. op is kvfsm.OpConfirm (key1 = the
// pending record's key, key2 = the confirmed record's key it's promoted
// to), kvfsm.OpDel (key1 = the confirmed record's key being revoked/
// deleted outright, key2 unused), kvfsm.OpSet (key1 = key, key2 = value,
// a direct single-step write -- used by EventGroupPut et al., never by
// EventPermitConfirm/EventPermitRevoke), or kvfsm.OpCascadeDelete (key1 =
// the Group/Command record's own key, key2 unused -- see kvfsm.Apply's
// respective cases). When this node *is* the leader, no separate voter
// check is needed here -- hashicorp/raft guarantees only a Voter can ever
// hold leader state, so isLeader==true already implies the caller is a
// voter. The forwarded path's voter check happens in
// handleForwardConfirmStream instead, against the authenticated identity
// of whichever node actually opened the stream -- it applies identically
// regardless of which op is being forwarded.
func (n *Node) handleConfirmForward(ctx context.Context, op kvfsm.OpType, key1, key2 []byte, allowForward bool) error {
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		return n.applyConfirm(ctx, rf, op, key1, key2)
	}
	if !allowForward {
		return fmt.Errorf("not leader; current leader is %s (already forwarded once)", leaderID)
	}
	return n.forwardConfirm(ctx, leaderID, op, key1, key2)
}

func (n *Node) applyConfirm(ctx context.Context, rf *raft.Raft, op kvfsm.OpType, key1, key2 []byte) error {
	cmd := kvfsm.EncodeCommand(op, key1, key2)
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	if res, ok := future.Response().(kvfsm.ApplyResult); ok && res.Err != nil {
		return res.Err
	}
	if op == kvfsm.OpConfirm {
		if err := n.admitClusterJoinIfConfirmed(ctx, rf, key2); err != nil {
			return err
		}
	}
	return nil
}

// admitClusterJoinIfConfirmed is applyConfirm's one special case: every
// other kind's OpConfirm is just a replicated status flip (see
// kvfsm.Apply's OpConfirm case), but promoting a pending
// shmevent.KindClusterJoin record to confirmed must also actually admit
// the joining peer into the raft configuration -- this is the only place
// EventPermitConfirm's workflow ever triggers raft.AddVoter/AddNonvoter,
// as opposed to handleJoinStream's immediate (Config.RequireConfirmForJoin
// == false) path. No-op for every other kind. Called only from
// applyConfirm, always with rf already established as this node's own
// *leader* raft handle (guaranteed by applyConfirm's two call sites in
// handleConfirmForward), so addServerLine can run directly here with no
// further leader resolution or forwarding needed.
func (n *Node) admitClusterJoinIfConfirmed(ctx context.Context, rf *raft.Raft, confirmedKey []byte) error {
	if len(confirmedKey) < 3 || confirmedKey[0] != shmevent.SystemKeyPrefix || confirmedKey[1] != shmevent.KindClusterJoin {
		return nil
	}
	peerID := string(confirmedKey[3:])
	value, err := n.store.Get(confirmedKey)
	if err != nil {
		return fmt.Errorf("cluster join: read confirmed record for %s: %w", peerID, err)
	}
	addr, sf, err := shmevent.DecodeClusterJoinMetadata(string(value))
	if err != nil {
		return fmt.Errorf("cluster join: decode confirmed record for %s: %w", peerID, err)
	}
	suffrage := raft.Voter
	if sf == shmevent.SuffrageLearner {
		suffrage = raft.Nonvoter
	}
	if line := n.addServerLine(ctx, rf, peerID, addr, suffrage); strings.HasPrefix(line, "ERR:") {
		return fmt.Errorf("cluster join: %s", strings.TrimPrefix(line, "ERR: "))
	}
	return nil
}

// forwardConfirm relays op(key1, key2) to leaderID over
// ForwardConfirmProtocolID, mirroring forwardSet's wire convention
// exactly (kvfsm's own command framing; empty response = success,
// non-empty = the leader's error message). See dialForward for how
// leaderID is actually reached.
func (n *Node) forwardConfirm(ctx context.Context, leaderID raft.ServerID, op kvfsm.OpType, key1, key2 []byte) error {
	s, err := n.dialForward(ctx, leaderID, ForwardConfirmProtocolID)
	if err != nil {
		return fmt.Errorf("forward confirm to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	cmd := kvfsm.EncodeCommand(op, key1, key2)
	if _, err := s.Write(cmd); err != nil {
		return fmt.Errorf("forward confirm: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward confirm: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward confirm: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward confirm: %s", respBuf)
	}
	return nil
}

// handleForwardConfirmStream is the leader-side handler for
// ForwardConfirmProtocolID, shared by EventPermitConfirm (OpConfirm),
// EventPermitRevoke (OpDel), and -- reusing this same machinery wholesale
// rather than building a parallel set of protocols/handlers -- the
// group-based ACL catalog's single-step Put (OpSet)/Delete (OpDel)/
// cascading Delete (OpCascadeDelete) events (EventGroupPut,
// EventGroupCommandDelete, etc.; see shmevent.KindGroup's doc comment).
// Unlike handleForwardSetStream, it checks the stream's
// libp2p-authenticated remote peer -- s.Conn().RemotePeer(), established
// by the connection's own handshake and so unforgeable by whatever a
// caller puts in the message itself -- against the leader's live raft
// configuration before applying anything, rejecting unless that peer is
// currently a Voter. This is the actual enforcement of "only a raft voter
// may confirm/revoke/put/delete" for every op it accepts: the generic
// per-message Ed25519 signature check every event type already gets (see
// handleShmEvent) only proves the message wasn't corrupted and was signed
// with whoever's key it was checked against -- for local same-machine
// shmring IPC that's inherently this same node's own key (see
// pkg/shmevent's doc comment), which doesn't by itself say anything about
// cluster membership. The RemotePeer check here is what does, uniformly
// for every op.
func (n *Node) handleForwardConfirmStream(s network.Stream) {
	defer s.Close()

	remote := s.Conn().RemotePeer()
	rf := n.getRaft()
	if rf == nil || !isVoter(rf, raft.ServerID(remote.String())) {
		fmt.Fprintf(s, "forward confirm: %s is not a current raft voter", remote)
		return
	}

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "forward confirm: read command: %v", err)
		return
	}
	op, key1, key2, err := kvfsm.DecodeCommand(buf)
	if err != nil {
		fmt.Fprintf(s, "forward confirm: decode command: %v", err)
		return
	}
	if op != kvfsm.OpConfirm && op != kvfsm.OpDel && op != kvfsm.OpSet && op != kvfsm.OpCascadeDelete {
		fmt.Fprintf(s, "forward confirm: unsupported op %d", op)
		return
	}

	if err := n.handleConfirmForward(context.Background(), op, key1, key2, false); err != nil {
		s.Write([]byte(err.Error()))
	}
}

// isVoter reports whether id is currently a Voter in rf's configuration.
func isVoter(rf *raft.Raft, id raft.ServerID) bool {
	cfg := rf.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return false
	}
	for _, srv := range cfg.Configuration().Servers {
		if srv.ID == id && srv.Suffrage == raft.Voter {
			return true
		}
	}
	return false
}

// removeServerLine runs raft.RemoveServer for peerID and returns the
// response line: "OK" or "ERR: <reason>" -- addServerLine's counterpart
// for leaving instead of joining. Every call site already only reaches
// this once rf.State()==Leader is confirmed. On success it also deletes
// peerID's KindClusterMember record (reusing applyConfirm's OpDel path,
// the same one EventPermitRevoke uses) -- membership no longer holds, so
// the mirror shouldn't keep claiming it does. Shrinking the configuration
// this way is graceful: the remaining voters keep operating normally,
// exactly like hashicorp/raft already tolerates any minority of members
// being unreachable.
func (n *Node) removeServerLine(ctx context.Context, rf *raft.Raft, peerID string) string {
	future := rf.RemoveServer(raft.ServerID(peerID), 0, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}
	if err := n.applyConfirm(ctx, rf, kvfsm.OpDel, shmevent.ClusterMemberKey([]byte(peerID)), nil); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: remove cluster member record %s: %v\n", peerID, err)
	}
	if err := n.clearMemberGroups(ctx, peerID); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: clear reserved groups for %s: %v\n", peerID, err)
	}
	return "OK"
}

// leaveCluster asks the raft cluster this node currently belongs to
// (identified purely by this node's own live raft handle -- there's
// nothing else to specify) to remove it via raft.RemoveServer: applies
// directly if this node is already the leader, or forwards one hop over
// ForwardLeaveProtocolID otherwise. Mirrors handleConfirmForward's shape
// rather than join's public-facing JoinProtocolID dance, since a leaving
// node -- unlike a brand new joiner -- is already a member with its own
// live raft handle and leader-tracking; it never needs to ask an
// arbitrary stranger node to introduce it.
func (n *Node) leaveCluster(ctx context.Context) error {
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		if line := n.removeServerLine(ctx, rf, n.peerID); strings.HasPrefix(line, "ERR:") {
			return fmt.Errorf("%s", strings.TrimPrefix(line, "ERR: "))
		}
		return nil
	}
	return n.forwardLeave(ctx, leaderID)
}

// forwardLeave relays a leave request to leaderID over
// ForwardLeaveProtocolID, mirroring forwardConfirm's wire convention
// (empty response = success, non-empty = the leader's error message) --
// but with no payload at all: the peer to remove is whichever identity
// the stream itself authenticates as, established by
// handleForwardLeaveStream reading s.Conn().RemotePeer(), not anything
// this side writes.
func (n *Node) forwardLeave(ctx context.Context, leaderID raft.ServerID) error {
	s, err := n.dialForward(ctx, leaderID, ForwardLeaveProtocolID)
	if err != nil {
		return fmt.Errorf("forward leave to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward leave: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward leave: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward leave: %s", respBuf)
	}
	return nil
}

// handleForwardLeaveStream is the leader-side handler for
// ForwardLeaveProtocolID: it removes whichever peer the stream's own
// libp2p-authenticated connection identity names -- s.Conn().RemotePeer(),
// unforgeable by anything a caller could put in a payload -- from the
// raft configuration. There is no payload to parse at all: unlike
// handleForwardConfirmStream (which acts on an arbitrary peerID named in
// the forwarded command, restricted to voters only), a leave request can
// only ever remove the requester itself.
func (n *Node) handleForwardLeaveStream(s network.Stream) {
	defer s.Close()

	remote := s.Conn().RemotePeer()
	rf := n.getRaft()
	if rf == nil || rf.State() != raft.Leader {
		var leaderID raft.ServerID
		if rf != nil {
			_, leaderID = rf.LeaderWithID()
		}
		fmt.Fprintf(s, "not leader; current leader is %s (already forwarded once)", leaderID)
		return
	}
	if line := n.removeServerLine(context.Background(), rf, remote.String()); strings.HasPrefix(line, "ERR:") {
		s.Write([]byte(strings.TrimPrefix(line, "ERR: ")))
	}
}

// kickPeer implements EventKick (see that event's doc comment): removes
// targetPeerID from the raft cluster this node currently belongs to via
// raft.RemoveServer, applying directly if this node is already the
// leader, or forwarding one hop over ForwardKickProtocolID otherwise --
// leaveCluster's shape exactly, generalized from "remove myself" to
// "remove whoever the caller names." Authorization (only a raft voter may
// call this) is checked by handleShmEvent before this is ever reached for
// a remote caller, and re-checked at the forwarding hop by
// handleForwardKickStream -- kickPeer itself does no identity check, the
// same division of responsibility handleConfirmForward/leaveCluster
// already have.
func (n *Node) kickPeer(ctx context.Context, targetPeerID string) error {
	if targetPeerID == "" {
		return fmt.Errorf("kick: missing target peer id")
	}
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		if line := n.removeServerLine(ctx, rf, targetPeerID); strings.HasPrefix(line, "ERR:") {
			return fmt.Errorf("%s", strings.TrimPrefix(line, "ERR: "))
		}
		return nil
	}
	return n.forwardKick(ctx, leaderID, targetPeerID)
}

// forwardKick relays a kick request for targetPeerID to leaderID over
// ForwardKickProtocolID, mirroring forwardLeave/forwardConfirm's wire
// convention (empty response = success, non-empty = the leader's error
// message) -- unlike forwardLeave, targetPeerID genuinely is the payload
// here (see ForwardKickProtocolID's doc comment for why).
func (n *Node) forwardKick(ctx context.Context, leaderID raft.ServerID, targetPeerID string) error {
	s, err := n.dialForward(ctx, leaderID, ForwardKickProtocolID)
	if err != nil {
		return fmt.Errorf("forward kick to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	if _, err := s.Write([]byte(targetPeerID)); err != nil {
		return fmt.Errorf("forward kick: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward kick: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward kick: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward kick: %s", respBuf)
	}
	return nil
}

// handleForwardKickStream is the leader-side handler for
// ForwardKickProtocolID: it reads the target peer id to remove from the
// stream body (unlike handleForwardLeaveStream, which has no payload at
// all -- see ForwardKickProtocolID's doc comment), checks the *forwarding*
// peer's own libp2p-authenticated identity against the leader's voter
// list (handleForwardConfirmStream's same reasoning: the per-message
// signature check alone doesn't establish cluster membership), and
// removes targetPeerID via removeServerLine if authorized.
func (n *Node) handleForwardKickStream(s network.Stream) {
	defer s.Close()

	remote := s.Conn().RemotePeer()
	rf := n.getRaft()
	if rf == nil || !isVoter(rf, raft.ServerID(remote.String())) {
		fmt.Fprintf(s, "forward kick: %s is not a current raft voter", remote)
		return
	}
	if rf.State() != raft.Leader {
		_, leaderID := rf.LeaderWithID()
		fmt.Fprintf(s, "not leader; current leader is %s (already forwarded once)", leaderID)
		return
	}

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "forward kick: read target: %v", err)
		return
	}
	targetPeerID := string(buf)
	if targetPeerID == "" {
		fmt.Fprintf(s, "forward kick: missing target peer id")
		return
	}
	if line := n.removeServerLine(context.Background(), rf, targetPeerID); strings.HasPrefix(line, "ERR:") {
		s.Write([]byte(strings.TrimPrefix(line, "ERR: ")))
	}
}
