package kvctl

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// AddPending implements `mage addpending`: spawns a brand new node, exactly
// like AddNode's 0-arg (bootstrap) case, except it never sends the
// bootstrap-or-join EventAdd that every other AddNode/Join path sends
// immediately after spawn -- it's left running (host + IPC alive) but with
// no raft instance at all, registered under registry.RolePending. This is
// the prerequisite for the reverse-invite "join-request" flow (see
// CreateJoinRequest/RecruitPeer): a device auto-admitted by some other
// cluster's EventRecruit push (pkg/daemon's handleRecruitStream) needs to
// still be in this pristine, never-bootstrapped state when that push
// arrives, since handleAdd's non-bootstrap join path only works cleanly
// for a node with no existing raft instance to conflict with -- see
// pkg/daemon's RecruitProtocolID doc comment for why this feature doesn't
// support an already-clustered node.
//
// It builds the kvnode daemon binary from source; see AddPendingWithBinary
// for a machine with no Go toolchain.
func AddPending(repoRoot string) (string, error) {
	return AddPendingWithArgs(repoRoot, nil)
}

// AddPendingWithArgs is like AddPending but appends extraDaemonArgs to the
// spawned kvnode's command line -- the AddPending equivalent of
// AddNodeWithArgs.
func AddPendingWithArgs(repoRoot string, extraDaemonArgs []string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	binPath, err := ensureDaemonBinary(reg, repoRoot)
	if err != nil {
		return "", err
	}
	return addPending(reg, binPath, extraDaemonArgs)
}

// AddPendingWithBinary is the AddNodeWithBinary-equivalent of AddPending,
// for a machine with no Go toolchain (a remote deployment target).
func AddPendingWithBinary(binPath string, extraDaemonArgs []string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	return addPending(reg, binPath, extraDaemonArgs)
}

func addPending(reg *registry.Registry, binPath string, extraDaemonArgs []string) (string, error) {
	peerID, dataDir, keyPath, err := generateIdentity(reg.NodeDataDir)
	if err != nil {
		return "", err
	}
	return bootUp(reg, binPath, extraDaemonArgs, peerID, dataDir, keyPath, registry.RolePending, "", true)
}

// CreateJoinRequest implements `mage createjoinrequest`: mints a fresh
// one-time join-request ticket on the current node (see
// shmevent.EventJoinRequestCreate) and returns it hex-encoded -- the form
// an operator combines with this node's own advertised address, as
// "<ownAddr>#<tokenHex>", when calling `mage printjoinrequestdatamatrix`
// and later hands to some other cluster's operator to redeem via `mage
// recruitpeer` (see RecruitPeer).
func CreateJoinRequest() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	token, err := sess.CreateJoinRequest(ctx)
	if err != nil {
		return "", fmt.Errorf("create join request: %w", err)
	}
	return hex.EncodeToString(token), nil
}

// CancelJoinRequest implements `mage cancelJoinrequest <tokenHex>`: clears
// the current node's pending join-request ticket before it's ever
// redeemed (a no-op if it no longer matches -- already consumed or
// superseded by a later CreateJoinRequest).
func CancelJoinRequest(tokenHex string) error {
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return fmt.Errorf("invalid join request token %q: %w", tokenHex, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.CancelJoinRequest(ctx, token); err != nil {
		return fmt.Errorf("cancel join request: %w", err)
	}
	return nil
}

// RecruitPeer implements `mage recruitpeer <ticket> <voter|learner>`: asks
// the current node (an existing raft voter) to mint a normal join invite
// on its own cluster and hand-deliver it directly to the device named in
// ticket ("<device's own multiaddr>#<tokenHex>", from that device's own
// CreateJoinRequest) -- see shmevent.EventRecruit's doc comment. Returns
// the recruited device's own join result ("<peerID> ok"/"<peerID>
// pending") on success.
func RecruitPeer(ticket string, suffrage byte) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), recruitTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	result, err := sess.Recruit(ctx, ticket, suffrage)
	if err != nil {
		return "", fmt.Errorf("recruit peer: %w", err)
	}
	return result, nil
}

// recruitTimeout bounds RecruitPeer's IPC round trip: unlike every other
// ipcTimeout-bounded call in this package, the current node's daemon
// doesn't just touch its own local state -- it dials out to the recruited
// device and waits for that device's own handleAdd call to complete (see
// pkg/daemon's recruitJoinTimeout), so this needs real headroom beyond the
// usual same-machine IPC budget.
const recruitTimeout = 100 * time.Second

// CreateJoinRequestTicket implements `mage createjoinrequestticket`: does
// everything CreateJoinRequest does (mints a one-time join-request token
// on the current node), then wraps this node's own current address and
// that token into a signed, self-contained ticket -- an
// EventJoinRequestTicket Msg built and signed with shmevent.Encode
// entirely client-side, the same way CreateExecInviteTicket/
// CreateJoinInviteTicket do (see CreateExecInviteTicket's doc comment for
// why no daemon changes are needed). Returns the ticket base64-encoded --
// the string a DataMatrix encodes; RedeemJoinRequestTicket is its
// inverse.
//
// Unlike those two, this ticket is minted by the *requesting* device, not
// an already-established voter -- see EventJoinRequestTicket's doc
// comment for why the direction of proof is reversed here: the signature
// proves the ticket really came from the device asking to join, not from
// the cluster admitting it.
func CreateJoinRequestTicket() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, selfPeerID, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	token, err := sess.CreateJoinRequest(ctx)
	if err != nil {
		return "", fmt.Errorf("create join request ticket: %w", err)
	}

	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("create join request ticket: get own addr: %w", err)
	}

	value, err := shmevent.EncodeJoinTicketPayload(addr, token)
	if err != nil {
		return "", fmt.Errorf("create join request ticket: %w", err)
	}
	priv, err := shmclient.GetPrivateKey(ctx, selfPeerID)
	if err != nil {
		return "", fmt.Errorf("create join request ticket: fetch signing key: %w", err)
	}
	wire, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventJoinRequestTicket, Value: value}, priv)
	if err != nil {
		return "", fmt.Errorf("create join request ticket: sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(wire), nil
}

// RedeemJoinRequestTicket implements `mage redeemjoinrequestticket
// <ticketB64> <voter|learner>`: the CreateJoinRequestTicket counterpart
// to RecruitPeer -- decodes ticketB64, extracts the requesting device's
// peer id from its own embedded address (self-certifying, via the same
// addr->peer.ID extraction RedeemExecInviteTicket/VerifyJoinInviteTicket
// use) and verifies the signature against that peer id's own public key,
// rejecting the ticket outright if it doesn't check out -- only then
// calls RecruitPeer exactly like it always has (dials the requesting
// device and hand-delivers a normal join invite on this node's own
// cluster). Returns the recruited device's own join result
// ("<peerID> ok"/"<peerID> pending") on success.
func RedeemJoinRequestTicket(ticketB64 string, suffrage byte) (string, error) {
	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		return "", fmt.Errorf("invalid join request ticket: %w", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		return "", fmt.Errorf("decode join request ticket: %w", err)
	}
	if m.EventType != shmevent.EventJoinRequestTicket {
		return "", fmt.Errorf("not a join request ticket (event %s)", shmevent.EventName(m.EventType))
	}
	addr, token, err := shmevent.DecodeJoinTicketPayload(m.Value)
	if err != nil {
		return "", fmt.Errorf("decode join request ticket: %w", err)
	}

	requesterID, err := relayNodePeerID(addr)
	if err != nil {
		return "", fmt.Errorf("join request ticket: %w", err)
	}
	requesterPub, err := requesterID.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("join request ticket: extract requester public key: %w", err)
	}
	requesterPubRaw, err := requesterPub.Raw()
	if err != nil {
		return "", fmt.Errorf("join request ticket: requester public key: %w", err)
	}
	if err := shmevent.Verify(requesterPubRaw, m, crc, sig); err != nil {
		return "", fmt.Errorf("join request ticket: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), recruitTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	result, err := sess.Recruit(ctx, addr+"#"+hex.EncodeToString(token), suffrage)
	if err != nil {
		return "", fmt.Errorf("recruit peer: %w", err)
	}
	return result, nil
}
