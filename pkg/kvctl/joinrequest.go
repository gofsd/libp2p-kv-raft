package kvctl

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
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
