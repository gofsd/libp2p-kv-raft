package kvctl

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// RequestPublicAccess implements `mage requestpublicaccess <targetAddr>
// [note]`: asks the *current* node (mage use) to submit
// shmevent.DefaultPublicCommandID -- the always-public "public-access"
// front door every cluster bootstraps -- to the cluster at targetAddr,
// which grants this node Channel and circuit-relay standing there.
// Returns the resulting dispatch's instance id.
//
// This is the desktop counterpart of kvmobile's RequestPublicAccess, and
// the self-service alternative to an operator on the *target* cluster
// running `mage addpeertogroup <thisPeerID> relay` by hand for every node
// that needs its relay. See shmevent.EventPublicAccess's doc comment for
// the design, and pkg/daemon's relayACL for why a node with no standing in
// a relay's own cluster can't reserve a circuit through it otherwise.
func RequestPublicAccess(targetAddr, note string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), publicAccessTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	instanceID, err := sess.PublicAccess(ctx, targetAddr, note)
	if err != nil {
		return "", fmt.Errorf("request public access: %w", err)
	}
	return instanceID, nil
}

// EnablePublicAccess implements `mage enablepublicaccess`: creates the
// shmevent.DefaultPublicGroupID Group (public), the
// shmevent.DefaultPublicCommandID Command, and the link between them, on
// the cluster the *current* node (mage use) belongs to -- making
// RequestPublicAccess submissions from strangers succeed against it.
//
// pkg/daemon seeds exactly these three records itself
// (ensureDefaultPublicCommand), but only at the moment a brand new cluster
// is first bootstrapped, and deliberately not on every leadership
// transition afterwards: they're ordinary mutable records, so an operator
// who closed self-service enrollment (deletegroup public, or updategroup
// public=false) must not have that decision silently undone by the next
// election. The consequence is that a cluster bootstrapped by a build
// predating that seeding never gains them, and a stranger's public-access
// submission is refused by kvfsm's IsPermittedForCommand with "is not
// permitted to submit command public-access". This is the one-time,
// deliberate, operator-run repair for that -- and equally the way to
// re-open enrollment on a cluster where it was closed on purpose.
//
// Idempotent: each write is a Put (create and update are the same
// operation here, see PutGroup/PutCommand), so re-running it on a cluster
// that already has all three records changes nothing. Requires the current
// node to be a raft voter of the cluster being changed -- pkg/daemon
// rejects catalog writes otherwise.
func EnablePublicAccess() error {
	ctx, cancel := context.WithTimeout(context.Background(), publicAccessTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}

	if err := sess.PutGroup(ctx, shmevent.DefaultPublicGroupID, "Public", true); err != nil {
		return fmt.Errorf("enable public access: create public group: %w", err)
	}
	// Driven through the session directly rather than through PutCommand,
	// which rejects an empty peer id. That guard is right for every command
	// a person creates -- one with no executor would simply never run -- but
	// this command deliberately has none, matching pkg/daemon's own seeding:
	// its whole point is the FSM side effect, not a dispatched executor, and
	// naming a real peer would spend that peer's single
	// checkCommandPeerIDUnique slot on it forever. See
	// ensureDefaultPublicCommand's doc comment.
	if err := sess.PutCommand(ctx, shmevent.DefaultPublicCommandID, "Public Access", nil); err != nil {
		return fmt.Errorf("enable public access: create public command: %w", err)
	}
	if err := sess.PutGroupCommand(ctx, []byte(shmevent.DefaultPublicCommandID), []byte(shmevent.DefaultPublicGroupID)); err != nil {
		return fmt.Errorf("enable public access: link command to group: %w", err)
	}
	return nil
}

// publicAccessTimeout bounds RequestPublicAccess's IPC round trip. Like
// recruitTimeout (see joinrequest.go), the daemon does real outbound
// network work inside this call -- connect to the target, then a full
// ClientProtocolID request whose response only comes back once the
// target's raft has committed the write -- so it needs headroom well past
// the usual same-machine IPC budget.
const publicAccessTimeout = 60 * time.Second
