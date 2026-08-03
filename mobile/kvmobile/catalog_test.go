package kvmobile

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// isReservedGroupID/filterReservedGroupIDs let every test below that
// predates pkg/daemon's reserved-group feature (see
// shmevent.ReservedGroupCluster's doc comment) keep asserting on just its
// own explicitly-created fixtures: every fresh cluster now auto-creates
// seven fixed daemon-managed groups (cluster/voter/learner/channel/relay/
// remote/execute, see shmevent.IsReservedGroupID) plus
// shmevent.DefaultPublicGroupID (not daemon-managed/reserved, just a
// bootstrapped default -- see that constant's doc comment -- but equally
// not something these tests asked for), any peer that actually joins the
// cluster (every kvmobile Start call below does) is itself automatically
// a member of "cluster" and one of "voter"/"learner" the moment the join
// completes, and every peer that ever joins/bootstraps also gets its own
// personal group (id == its own peer id, see pkg/daemon's
// isPeerIdentityGroupID doc comment) -- all of that would otherwise show
// up unexpectedly in a plain ListGroups/ListGroupsForPeer call these
// tests didn't ask for.
func isReservedGroupID(id string) bool {
	if shmevent.IsReservedGroupID(id) || id == shmevent.DefaultPublicGroupID {
		return true
	}
	_, err := peer.Decode(id)
	return err == nil
}

func filterReservedGroupIDs(ids []string) []string {
	var out []string
	for _, id := range ids {
		if !isReservedGroupID(id) {
			out = append(out, id)
		}
	}
	return out
}

// filterDefaultCommands drops shmevent.DefaultPublicCommandID -- the
// Command ensureDefaultPublicCommand bootstraps alongside
// DefaultPublicGroupID -- from a ListCommands result, the Command-side
// counterpart to filterReservedGroupIDs above.
func filterDefaultCommands(commands []Command) []Command {
	var out []Command
	for _, c := range commands {
		if c.ID != shmevent.DefaultPublicCommandID {
			out = append(out, c)
		}
	}
	return out
}

// pollUntilTrue retries check until it reports true, or fails the test
// after timeout -- the shared retry shape every catalog test below needs
// since a write forwarded through raft becomes locally readable
// asynchronously, same reason pkg/kvctl's own cross-node tests poll.
func pollUntilTrue(t *testing.T, timeout time.Duration, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s (last error: %v)", timeout, lastErr)
}

// TestGroupCRUD drives Create/Get/List/Update/Delete against a real
// leader -- a kvmobile follower always joins as a full raft voter (see
// pkg/daemon's join path), so every write here succeeds unconditionally,
// no participation gate exists anymore (see catalog.go's doc comment).
func TestGroupCRUD(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const groupID = "grp-1"
	if err := CreateGroup(groupID, "Group One", false); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	var g Group
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetGroup(groupID)
		if err != nil {
			return false, err
		}
		return true, json.Unmarshal([]byte(out), &g)
	})
	if g.ID != groupID || g.Name != "Group One" || g.Public {
		t.Fatalf("GetGroup = %+v, unexpected", g)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroups()
		if err != nil {
			return false, err
		}
		var groups []Group
		if err := json.Unmarshal([]byte(out), &groups); err != nil {
			return false, err
		}
		for _, gr := range groups {
			if gr.ID == groupID {
				return true, nil
			}
		}
		return false, nil
	})

	if err := UpdateGroup(groupID, "Renamed", true); err != nil {
		t.Fatalf("UpdateGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetGroup(groupID)
		if err != nil {
			return false, err
		}
		if err := json.Unmarshal([]byte(out), &g); err != nil {
			return false, err
		}
		return g.Name == "Renamed" && g.Public, nil
	})

	if err := DeleteGroup(groupID); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetGroup(groupID)
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroups()
		if err != nil {
			return false, err
		}
		var groups []Group
		if err := json.Unmarshal([]byte(out), &groups); err != nil {
			return false, err
		}
		for _, gr := range groups {
			if gr.ID == groupID {
				return false, nil
			}
		}
		return true, nil
	})
}

// TestCommandCRUD drives Create/Get/List/Update/Delete for Commands.
func TestCommandCRUD(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CreateCommand("cmd-1", "Reboot", followerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}

	var cmd Command
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetCommand("cmd-1")
		if err != nil {
			return false, err
		}
		return true, json.Unmarshal([]byte(out), &cmd)
	})
	if cmd.Name != "Reboot" || cmd.TargetPeerID != followerID {
		t.Fatalf("GetCommand = %+v, unexpected", cmd)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListCommands()
		if err != nil {
			return false, err
		}
		var cmds []Command
		if err := json.Unmarshal([]byte(out), &cmds); err != nil {
			return false, err
		}
		for _, c := range cmds {
			if c.ID == "cmd-1" {
				return true, nil
			}
		}
		return false, nil
	})

	if err := UpdateCommand("cmd-1", "Reboot Now", followerID); err != nil {
		t.Fatalf("UpdateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetCommand("cmd-1")
		if err != nil {
			return false, err
		}
		var fresh Command
		if err := json.Unmarshal([]byte(out), &fresh); err != nil {
			return false, err
		}
		cmd = fresh
		return cmd.Name == "Reboot Now", nil
	})

	if err := DeleteCommand("cmd-1"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand("cmd-1")
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListCommands()
		if err != nil {
			return false, err
		}
		var cmds []Command
		if err := json.Unmarshal([]byte(out), &cmds); err != nil {
			return false, err
		}
		return len(filterDefaultCommands(cmds)) == 0, nil
	})
}

// TestGroupCommandAndPeerGroupLinkingGatesSubmitCommand drives the full
// group-based ACL chain end to end: a peer with no PeerGroup membership at
// all must be refused by SubmitCommand; linking commandID to a group
// (AddCommandToGroup) alone still isn't enough; adding the peer to that
// group (AddPeerToGroup) is what finally permits it; removing the peer
// from the group revokes access again. Mirrors
// pkg/kvctl/catalog_test.go's identical test.
func TestGroupCommandAndPeerGroupLinkingGatesSubmitCommand(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CreateGroup("grp-1", "Group One", false); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := CreateCommand("cmd-1", "Reboot", followerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand("cmd-1")
		return err == nil, nil
	})

	if _, err := SubmitCommand("cmd-1", ""); err == nil {
		t.Fatalf("SubmitCommand before any group link: want error, got none")
	}

	if err := AddCommandToGroup("cmd-1", "grp-1"); err != nil {
		t.Fatalf("AddCommandToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForCommand("cmd-1")
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		return len(groupIDs) == 1 && groupIDs[0] == "grp-1", nil
	})

	// Linked to a group, but followerID isn't a member of it yet.
	if _, err := SubmitCommand("cmd-1", ""); err == nil {
		t.Fatalf("SubmitCommand before peer joined the group: want error, got none")
	}

	if err := AddPeerToGroup(followerID, "grp-1"); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForPeer(followerID)
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		groupIDs = filterReservedGroupIDs(groupIDs)
		return len(groupIDs) == 1 && groupIDs[0] == "grp-1", nil
	})

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = SubmitCommand("cmd-1", `{"delay":5}`)
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatalf("SubmitCommand returned empty instance id")
	}

	if err := RemovePeerFromGroup(followerID, "grp-1"); err != nil {
		t.Fatalf("RemovePeerFromGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := SubmitCommand("cmd-1", "")
		return err != nil, nil
	})
}

// TestPublicGroupExemptsPeerGroupMembership proves a Group created with
// public=true grants SubmitCommand access to a command linked to it with
// no AddPeerToGroup membership at all. Mirrors pkg/kvctl/catalog_test.go's
// identical test.
func TestPublicGroupExemptsPeerGroupMembership(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CreateGroup("grp-public", "Public Group", true); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := CreateCommand("cmd-public", "Reboot", followerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand("cmd-public")
		return err == nil, nil
	})

	if _, err := SubmitCommand("cmd-public", ""); err == nil {
		t.Fatalf("SubmitCommand before any group link: want error, got none")
	}

	if err := AddCommandToGroup("cmd-public", "grp-public"); err != nil {
		t.Fatalf("AddCommandToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForCommand("cmd-public")
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		return len(groupIDs) == 1 && groupIDs[0] == "grp-public", nil
	})

	// Linked to a public group -- permitted with no AddPeerToGroup call at
	// all, unlike the private-group case above.
	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = SubmitCommand("cmd-public", `{"delay":5}`)
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatalf("SubmitCommand returned empty instance id")
	}

	out, err := ListGroupsForPeer(followerID)
	if err != nil {
		t.Fatalf("ListGroupsForPeer: %v", err)
	}
	var groupIDs []string
	if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	groupIDs = filterReservedGroupIDs(groupIDs)
	if len(groupIDs) != 0 {
		t.Fatalf("followerID unexpectedly has non-reserved PeerGroup membership: %v", groupIDs)
	}
}

// TestDeleteGroupCascadesToRelations checks DeleteGroup removes every
// GroupCommand/PeerGroup record referencing it (pkg/kvfsm.OpCascadeDelete),
// so a peer that was only permitted via the deleted group loses access,
// and ListGroupsForCommand/ListGroupsForPeer no longer mention it.
func TestDeleteGroupCascadesToRelations(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CreateGroup("grp-cascade", "Cascade Group", false); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := CreateCommand("cmd-cascade", "Reboot", followerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand("cmd-cascade")
		return err == nil, nil
	})
	if err := AddCommandToGroup("cmd-cascade", "grp-cascade"); err != nil {
		t.Fatalf("AddCommandToGroup: %v", err)
	}
	if err := AddPeerToGroup(followerID, "grp-cascade"); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := SubmitCommand("cmd-cascade", "")
		return err == nil, err
	})

	if err := DeleteGroup("grp-cascade"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForCommand("cmd-cascade")
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		return len(groupIDs) == 0, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForPeer(followerID)
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		return len(filterReservedGroupIDs(groupIDs)) == 0, nil
	})
	if _, err := SubmitCommand("cmd-cascade", ""); err == nil {
		t.Fatalf("SubmitCommand after group cascade-deleted: want error, got none")
	}
}

// TestDeleteCommandCascadesToGroupCommand checks DeleteCommand removes
// every GroupCommand record referencing it.
func TestDeleteCommandCascadesToGroupCommand(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CreateGroup("grp-cmd-cascade", "Group", false); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := CreateCommand("cmd-to-delete", "Reboot", followerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand("cmd-to-delete")
		return err == nil, nil
	})
	if err := AddCommandToGroup("cmd-to-delete", "grp-cmd-cascade"); err != nil {
		t.Fatalf("AddCommandToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForCommand("cmd-to-delete")
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		return len(groupIDs) == 1, nil
	})

	if err := DeleteCommand("cmd-to-delete"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForCommand("cmd-to-delete")
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		return len(groupIDs) == 0, nil
	})
}

// TestCatalogEmptyListsAreEmptyArrays checks ListGroups/ListCommands
// return "[]", never "null", when nothing matches -- same convention
// LogQuery already established. ListGroups itself is no longer literally
// "[]" on a fresh cluster (see filterReservedGroupIDs' doc comment: four
// reserved groups always exist), so this checks the parsed, filtered
// result instead of the raw string; TestReservedGroupsCreatedAndProtected
// below checks the reserved groups themselves directly.
func TestCatalogEmptyListsAreEmptyArrays(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	out, err := ListGroups()
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	var groups []Group
	if err := json.Unmarshal([]byte(out), &groups); err != nil {
		t.Fatalf("unmarshal ListGroups: %v", err)
	}
	var nonReserved []Group
	for _, g := range groups {
		if !isReservedGroupID(g.ID) {
			nonReserved = append(nonReserved, g)
		}
	}
	if len(nonReserved) != 0 {
		t.Fatalf("ListGroups (empty) = %+v, want none", nonReserved)
	}

	out, err = ListCommands()
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	var commands []Command
	if err := json.Unmarshal([]byte(out), &commands); err != nil {
		t.Fatalf("unmarshal ListCommands: %v", err)
	}
	commands = filterDefaultCommands(commands)
	if len(commands) != 0 {
		t.Fatalf("ListCommands (empty) = %+v, want none", commands)
	}
}

// TestCatalogIDValidation checks CreateGroup rejects an empty or oversized
// id before ever touching the daemon.
func TestCatalogIDValidation(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CreateGroup("", "x", false); err == nil {
		t.Fatalf("CreateGroup with empty id: want error, got none")
	}
	if err := CreateGroup(strings.Repeat("a", maxCatalogIDLen+1), "x", false); err == nil {
		t.Fatalf("CreateGroup with oversized id: want error, got none")
	}
}

// TestReservedGroupsCreatedAndProtected mirrors pkg/kvctl/catalog_test.go's
// identical test: a fresh bootstrap auto-creates the seven fixed reserved
// groups (shmevent.ReservedGroupCluster/Voter/Learner/Channel/Relay/
// Remote/Execute) plus one personal group per peer that ever
// joins/bootstraps (id == that peer's own peer id, see pkg/daemon's
// isPeerIdentityGroupID doc comment) -- both the leader spawnTestLeader
// bootstraps and the follower Start joins get one. A follower that joins
// as a voter (this package's default joinSuffrage, see kvmobile.go) is
// automatically a member of "cluster" and "voter" (never "learner"), and
// every reserved group is protected -- CreateGroup/UpdateGroup/DeleteGroup
// against any of them is rejected, and AddPeerToGroup/RemovePeerFromGroup
// against cluster/voter/learner specifically is rejected too, while the
// same calls against "channel"/"relay"/"remote"/"execute" and the
// follower's own personal group all succeed (ordinary operator grants,
// not daemon-managed membership).
func TestReservedGroupsCreatedAndProtected(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	fixedIDs := []string{
		shmevent.ReservedGroupChannel, shmevent.ReservedGroupCluster, shmevent.ReservedGroupLearner,
		shmevent.ReservedGroupRelay, shmevent.ReservedGroupVoter, shmevent.ReservedGroupRemote, shmevent.ReservedGroupExecute,
	}

	// Every group present is either one of the seven fixed reserved names
	// or a valid peer id (someone's personal group) -- and followerID's
	// own personal group specifically must be among them.
	var groups []Group
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroups()
		if err != nil {
			return false, err
		}
		groups = nil
		if err := json.Unmarshal([]byte(out), &groups); err != nil {
			return false, err
		}
		sawFollower := false
		for _, g := range groups {
			if !isReservedGroupID(g.ID) {
				return false, nil
			}
			if g.ID == followerID {
				sawFollower = true
			}
		}
		return sawFollower, nil
	})
	for _, g := range groups {
		wantPublic := g.ID == shmevent.DefaultPublicGroupID
		if g.Public != wantPublic {
			t.Fatalf("group %q public = %v, want %v", g.ID, g.Public, wantPublic)
		}
	}
	if len(groups) < len(fixedIDs)+1 {
		t.Fatalf("ListGroups = %+v, want at least the %d fixed groups plus followerID's own", groups, len(fixedIDs))
	}

	// A voter-joined follower is a member of "cluster" and "voter", never
	// "learner".
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForPeer(followerID)
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		sort.Strings(groupIDs)
		return len(groupIDs) == 2 && groupIDs[0] == shmevent.ReservedGroupCluster && groupIDs[1] == shmevent.ReservedGroupVoter, nil
	})

	// Every reserved group's own record is protected, regardless of
	// which one it is -- one of the seven fixed names, or followerID's own
	// dynamic peer-identity-shaped one.
	for _, id := range append(append([]string{}, fixedIDs...), followerID) {
		if err := CreateGroup(id, "renamed", false); err == nil {
			t.Fatalf("CreateGroup against reserved group %q: want error, got none", id)
		}
		if err := DeleteGroup(id); err == nil {
			t.Fatalf("DeleteGroup against reserved group %q: want error, got none", id)
		}
	}

	// cluster/voter/learner membership is daemon-managed only.
	for _, id := range []string{shmevent.ReservedGroupCluster, shmevent.ReservedGroupVoter, shmevent.ReservedGroupLearner} {
		if err := AddPeerToGroup(followerID, id); err == nil {
			t.Fatalf("AddPeerToGroup against reserved group %q: want error, got none", id)
		}
		if err := RemovePeerFromGroup(followerID, id); err == nil {
			t.Fatalf("RemovePeerFromGroup against reserved group %q: want error, got none", id)
		}
	}

	// "channel"/"relay"/"remote"/"execute" and the follower's own personal
	// group are all the deliberate exception: their membership is an
	// ordinary operator grant, not tied to actual cluster membership.
	if err := AddPeerToGroup(followerID, shmevent.ReservedGroupChannel); err != nil {
		t.Fatalf("AddPeerToGroup against the channel group: %v", err)
	}
	if err := AddPeerToGroup(followerID, shmevent.ReservedGroupRelay); err != nil {
		t.Fatalf("AddPeerToGroup against the relay group: %v", err)
	}
	if err := AddPeerToGroup(followerID, shmevent.ReservedGroupRemote); err != nil {
		t.Fatalf("AddPeerToGroup against the remote group: %v", err)
	}
	if err := AddPeerToGroup(followerID, shmevent.ReservedGroupExecute); err != nil {
		t.Fatalf("AddPeerToGroup against the execute group: %v", err)
	}
	if err := AddPeerToGroup(followerID, followerID); err != nil {
		t.Fatalf("AddPeerToGroup against the follower's own personal group: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroupsForPeer(followerID)
		if err != nil {
			return false, err
		}
		var groupIDs []string
		if err := json.Unmarshal([]byte(out), &groupIDs); err != nil {
			return false, err
		}
		sawChannel, sawRelay, sawRemote, sawExecute, sawSelf := false, false, false, false, false
		for _, g := range groupIDs {
			if g == shmevent.ReservedGroupChannel {
				sawChannel = true
			}
			if g == shmevent.ReservedGroupRelay {
				sawRelay = true
			}
			if g == shmevent.ReservedGroupRemote {
				sawRemote = true
			}
			if g == shmevent.ReservedGroupExecute {
				sawExecute = true
			}
			if g == followerID {
				sawSelf = true
			}
		}
		return sawChannel && sawRelay && sawRemote && sawExecute && sawSelf, nil
	})
	if err := RemovePeerFromGroup(followerID, followerID); err != nil {
		t.Fatalf("RemovePeerFromGroup against the follower's own personal group: %v", err)
	}
	if err := RemovePeerFromGroup(followerID, shmevent.ReservedGroupChannel); err != nil {
		t.Fatalf("RemovePeerFromGroup against the channel group: %v", err)
	}
	if err := RemovePeerFromGroup(followerID, shmevent.ReservedGroupRelay); err != nil {
		t.Fatalf("RemovePeerFromGroup against the relay group: %v", err)
	}
	if err := RemovePeerFromGroup(followerID, shmevent.ReservedGroupRemote); err != nil {
		t.Fatalf("RemovePeerFromGroup against the remote group: %v", err)
	}
	if err := RemovePeerFromGroup(followerID, shmevent.ReservedGroupExecute); err != nil {
		t.Fatalf("RemovePeerFromGroup against the execute group: %v", err)
	}
}
