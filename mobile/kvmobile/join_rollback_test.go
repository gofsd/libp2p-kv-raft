package kvmobile

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// unreachableLeaderAddr is a well-formed multiaddr naming a real (freshly
// generated) peer id at a port nothing is listening on, so a join against it
// fails inside the daemon's own Add rather than while parsing the address --
// which is the distinction that matters here: a malformed address fails before
// the daemon is started at all, while this one fails after.
func unreachableLeaderAddr(t *testing.T) string {
	t.Helper()
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	id, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}
	return fmt.Sprintf("/ip4/127.0.0.1/tcp/1/p2p/%s", id)
}

// TestJoinFailureLeavesThePreviousNodeRunning pins the rollback Join owes its
// caller: Join stops whatever is running before it can know the new cluster is
// reachable, so a join that fails must put the old node back rather than leave
// the process with no daemon at all.
//
// Without it, `started` stays false for the life of the process and *every*
// later binding answers "kvmobile: Start has not completed successfully yet" --
// nothing restarts it, because Start is a no-op once it believes it has run and
// the only other door, StartSolo, is called once at app startup.
//
// Measured on the object-history-app rig, 2026-09-30: a phone whose
// files/joined_cluster named a generator identity that had been destroyed and
// reinstalled spent every launch in exactly this state. The app logged
// "could not re-enter the joined cluster -- staying solo for now", which was the
// intention and not the outcome: it was not solo, it had nothing. The relay grant
// then failed one millisecond after the join did, the device advertised no
// address, and it was unreachable for the whole launch.
func TestJoinFailureLeavesThePreviousNodeRunning(t *testing.T) {
	prevLeader := leaderMultiaddr
	leaderMultiaddr = ""
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	dir := t.TempDir()
	soloPeerID, err := StartSolo(dir)
	if err != nil {
		t.Fatalf("StartSolo: %v", err)
	}
	if err := Submit("before", "join"); err != nil {
		t.Fatalf("Submit before the join: %v", err)
	}

	// The join has to fail, and it has to fail for the ordinary reason: the
	// cluster it names is not there.
	if _, err := JoinAs(dir, unreachableLeaderAddr(t), "learner"); err == nil {
		t.Fatalf("JoinAs against an unreachable leader: want an error, got none")
	} else if !strings.Contains(err.Error(), "join cluster") {
		t.Fatalf("JoinAs failed with %v, want a join-cluster failure", err)
	}

	// Everything below is the actual subject: the node that was running before
	// the join is running after it.
	if got := PeerID(); got != soloPeerID {
		t.Fatalf("PeerID() = %q after a failed join, want the solo node's %q", got, soloPeerID)
	}
	if got, err := Get("before"); err != nil {
		t.Fatalf("Get after a failed join: %v", err)
	} else if got != "join" {
		t.Fatalf("Get(before) = %q, want %q", got, "join")
	}
	if err := Submit("after", "join"); err != nil {
		t.Fatalf("Submit after a failed join: %v", err)
	}
	if _, err := GetOwnAddr(); err != nil {
		t.Fatalf("GetOwnAddr after a failed join: %v", err)
	}
}
