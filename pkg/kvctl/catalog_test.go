package kvctl_test

import (
	"sort"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// isReservedGroupID/filterReservedGroupIDs/filterReservedGroups let every
// test below that predates pkg/daemon's reserved-group feature (see
// shmevent.ReservedGroupCluster's doc comment) keep asserting on just its
// own explicitly-created fixtures: every fresh cluster now auto-creates
// seven daemon-managed groups (cluster/voter/learner/channel/relay/remote/
// execute, see shmevent.IsReservedGroupID) plus shmevent.DefaultPublicGroupID
// (not daemon-managed/reserved, just a bootstrapped default -- see that
// constant's doc comment -- but equally not something these tests asked
// for), a solo bootstrap leader is itself automatically a member of
// "cluster" and "voter" the moment AddNodeWithArgs returns, and every peer
// that ever joins/bootstraps also gets its own personal group (id == its
// own peer id, see pkg/daemon's isPeerIdentityGroupID doc comment) -- all
// of that would otherwise show up unexpectedly in a plain ListGroups/
// ListGroupsForPeer call these tests didn't ask for.
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

func filterReservedGroups(groups []kvctl.Group) []kvctl.Group {
	var out []kvctl.Group
	for _, g := range groups {
		if !isReservedGroupID(g.ID) {
			out = append(out, g)
		}
	}
	return out
}

// filterDefaultCommands drops shmevent.DefaultPublicCommandID -- the
// Command ensureDefaultPublicCommand bootstraps alongside
// DefaultPublicGroupID -- from a ListCommands result, the Command-side
// counterpart to filterReservedGroups above.
func filterDefaultCommands(commands []kvctl.Command) []kvctl.Command {
	var out []kvctl.Command
	for _, c := range commands {
		if c.ID != shmevent.DefaultPublicCommandID {
			out = append(out, c)
		}
	}
	return out
}

// fastRaftArgs shortens hashicorp/raft's WAN-appropriate default timeouts
// for a same-machine test -- see TestAddSetGetAcrossNodes's identical
// constant for why.
var fastRaftArgs = []string{
	"-raft-heartbeat-timeout", "300ms",
	"-raft-election-timeout", "300ms",
	"-raft-leader-lease-timeout", "250ms",
}

// pollUntilTrue retries check until it reports true, or fails the test
// after timeout -- every group-based ACL catalog write is a raft commit,
// asynchronously visible to a subsequent local read.
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

// TestGroupCRUD drives PutGroup/GetGroup/ListGroups/DeleteGroup against a
// real, single-node leader -- always a raft voter, so every write here
// succeeds unconditionally (no participation gate exists anymore, unlike
// the old logrecord-based scheme -- see catalog.go's doc comment).
func TestGroupCRUD(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const groupID = "grp-1"
	if err := kvctl.PutGroup(groupID, "Group One", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}

	var g kvctl.Group
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		g, err = kvctl.GetGroup(groupID)
		return err == nil, err
	})
	if g.ID != groupID || g.Name != "Group One" || g.Public {
		t.Fatalf("GetGroup = %+v, unexpected", g)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groups, err := kvctl.ListGroups()
		if err != nil {
			return false, err
		}
		for _, gr := range groups {
			if gr.ID == groupID {
				return true, nil
			}
		}
		return false, nil
	})

	if err := kvctl.PutGroup(groupID, "Renamed", true); err != nil {
		t.Fatalf("PutGroup (update): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		g, err = kvctl.GetGroup(groupID)
		if err != nil {
			return false, err
		}
		return g.Name == "Renamed" && g.Public, nil
	})

	if err := kvctl.DeleteGroup(groupID); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetGroup(groupID)
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groups, err := kvctl.ListGroups()
		if err != nil {
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

// TestCommandCRUD drives PutCommand/GetCommand/ListCommands/DeleteCommand.
func TestCommandCRUD(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutCommand("cmd-1", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}

	var cmd kvctl.Command
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		cmd, err = kvctl.GetCommand("cmd-1")
		return err == nil, err
	})
	if cmd.Name != "Reboot" || cmd.PeerID != leaderID {
		t.Fatalf("GetCommand = %+v, unexpected", cmd)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		commands, err := kvctl.ListCommands()
		if err != nil {
			return false, err
		}
		for _, c := range commands {
			if c.ID == "cmd-1" {
				return true, nil
			}
		}
		return false, nil
	})

	if err := kvctl.PutCommand("cmd-1", "Reboot Now", leaderID); err != nil {
		t.Fatalf("PutCommand (update): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		fresh, err := kvctl.GetCommand("cmd-1")
		if err != nil {
			return false, err
		}
		cmd = fresh
		return cmd.Name == "Reboot Now", nil
	})

	if err := kvctl.DeleteCommand("cmd-1"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-1")
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		commands, err := kvctl.ListCommands()
		if err != nil {
			return false, err
		}
		return len(filterDefaultCommands(commands)) == 0, nil
	})
}

// TestGroupCommandAndPeerGroupLinkingGatesSubmitCommand drives the full
// group-based ACL chain end to end: a peer with no PeerGroup membership
// at all must be refused by SubmitCommand; linking commandID to a group
// (CreateGroupCommand) alone still isn't enough; adding the peer to that
// group (AddPeerToGroup) is what finally permits it; removing the peer
// from the group revokes access again.
func TestGroupCommandAndPeerGroupLinkingGatesSubmitCommand(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("grp-1", "Group One", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-1", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-1")
		return err == nil, nil
	})

	if _, err := kvctl.SubmitCommand("cmd-1", ""); err == nil {
		t.Fatalf("SubmitCommand before any group link: want error, got none")
	}

	if err := kvctl.CreateGroupCommand("cmd-1", "grp-1"); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-1")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 1 && groupIDs[0] == "grp-1", nil
	})

	// Linked to a group, but leaderID isn't a member of it yet.
	if _, err := kvctl.SubmitCommand("cmd-1", ""); err == nil {
		t.Fatalf("SubmitCommand before peer joined the group: want error, got none")
	}

	if err := kvctl.AddPeerToGroup(leaderID, "grp-1"); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForPeer(leaderID)
		if err != nil {
			return false, err
		}
		groupIDs = filterReservedGroupIDs(groupIDs)
		return len(groupIDs) == 1 && groupIDs[0] == "grp-1", nil
	})

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.SubmitCommand("cmd-1", `{"delay":5}`)
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatalf("SubmitCommand returned empty instance id")
	}

	if err := kvctl.RemovePeerFromGroup(leaderID, "grp-1"); err != nil {
		t.Fatalf("RemovePeerFromGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.SubmitCommand("cmd-1", "")
		return err != nil, nil
	})
}

// TestPublicGroupExemptsPeerGroupMembership proves a Group created with
// public=true grants SubmitCommand access to a command linked to it with
// no AddPeerToGroup membership at all -- the reverse of
// TestGroupCommandAndPeerGroupLinkingGatesSubmitCommand's "linked but not
// a member" refusal case above.
func TestPublicGroupExemptsPeerGroupMembership(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("grp-public", "Public Group", true); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-public", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-public")
		return err == nil, nil
	})

	// Not yet linked to any group at all -- still refused.
	if _, err := kvctl.SubmitCommand("cmd-public", ""); err == nil {
		t.Fatalf("SubmitCommand before any group link: want error, got none")
	}

	if err := kvctl.CreateGroupCommand("cmd-public", "grp-public"); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-public")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 1 && groupIDs[0] == "grp-public", nil
	})

	// Linked to a public group -- permitted with no AddPeerToGroup call at
	// all, unlike the private-group case above.
	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.SubmitCommand("cmd-public", `{"delay":5}`)
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatalf("SubmitCommand returned empty instance id")
	}

	groupIDs, err := kvctl.ListGroupsForPeer(leaderID)
	if err != nil {
		t.Fatalf("ListGroupsForPeer: %v", err)
	}
	groupIDs = filterReservedGroupIDs(groupIDs)
	if len(groupIDs) != 0 {
		t.Fatalf("leaderID unexpectedly has non-reserved PeerGroup membership: %v", groupIDs)
	}
}

// TestDeleteGroupCascadesToRelations checks DeleteGroup removes every
// GroupCommand/PeerGroup record referencing it (kvfsm.OpCascadeDelete),
// so a peer that was only permitted via the deleted group loses access,
// and ListGroupsForCommand/ListGroupsForPeer no longer mention it.
func TestDeleteGroupCascadesToRelations(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("grp-cascade", "Cascade Group", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-cascade", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-cascade")
		return err == nil, nil
	})
	if err := kvctl.CreateGroupCommand("cmd-cascade", "grp-cascade"); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, "grp-cascade"); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.SubmitCommand("cmd-cascade", "")
		return err == nil, err
	})

	if err := kvctl.DeleteGroup("grp-cascade"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-cascade")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 0, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForPeer(leaderID)
		if err != nil {
			return false, err
		}
		return len(filterReservedGroupIDs(groupIDs)) == 0, nil
	})
	if _, err := kvctl.SubmitCommand("cmd-cascade", ""); err == nil {
		t.Fatalf("SubmitCommand after group cascade-deleted: want error, got none")
	}
}

// TestDeleteCommandCascadesToGroupCommand checks DeleteCommand removes
// every GroupCommand record referencing it.
func TestDeleteCommandCascadesToGroupCommand(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("grp-cmd-cascade", "Group", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-to-delete", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-to-delete")
		return err == nil, nil
	})
	if err := kvctl.CreateGroupCommand("cmd-to-delete", "grp-cmd-cascade"); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-to-delete")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 1, nil
	})

	if err := kvctl.DeleteCommand("cmd-to-delete"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-to-delete")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 0, nil
	})
}

// TestCatalogEmptyListsAreEmptyArrays checks ListGroups/ListCommands
// return a zero-length (nil) slice, not an error, when nothing matches --
// after filtering out the seven reserved groups plus the default public
// group/command ensureReservedGroups/ensureDefaultPublicCommand
// auto-create at bootstrap (see shmevent.ReservedGroupCluster's/
// DefaultPublicGroupID's doc comments), which
// TestReservedGroupsCreatedAndProtected below checks directly.
func TestCatalogEmptyListsAreEmptyArrays(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	groups, err := kvctl.ListGroups()
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	groups = filterReservedGroups(groups)
	if len(groups) != 0 {
		t.Fatalf("ListGroups (empty) = %+v, want none", groups)
	}

	commands, err := kvctl.ListCommands()
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	commands = filterDefaultCommands(commands)
	if len(commands) != 0 {
		t.Fatalf("ListCommands (empty) = %+v, want none", commands)
	}
}

// TestCatalogIDValidation checks PutGroup rejects an empty or oversized
// id before ever touching the daemon.
func TestCatalogIDValidation(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("", "x", false); err == nil {
		t.Fatalf("PutGroup with empty id: want error, got none")
	}
	oversized := make([]byte, 257)
	for i := range oversized {
		oversized[i] = 'a'
	}
	if err := kvctl.PutGroup(string(oversized), "x", false); err == nil {
		t.Fatalf("PutGroup with oversized id: want error, got none")
	}
}

// TestReservedGroupsCreatedAndProtected proves pkg/daemon's
// ensureReservedGroups/syncMemberGroups feature end to end through
// pkg/kvctl: a fresh bootstrap auto-creates the seven fixed reserved
// groups (shmevent.ReservedGroupCluster/Voter/Learner/Channel/Relay/
// Remote/Execute), the default public group (shmevent.DefaultPublicGroupID,
// see that constant's doc comment -- the only one of these that's
// actually Public), and the bootstrap leader's own personal group (id ==
// its own peer id, see pkg/daemon's isPeerIdentityGroupID doc comment).
// The leader is automatically a member of "cluster" and "voter" (never
// "learner"), and every *reserved* group is protected the way its own doc
// comment describes -- PutGroup/DeleteGroup against any of the eight is
// rejected, and AddPeerToGroup/RemovePeerFromGroup against
// cluster/voter/learner specifically is rejected too, while the same
// calls against "channel"/"relay"/"remote"/"execute" and the personal
// group all succeed (ordinary operator grants, not daemon-managed
// membership).
func TestReservedGroupsCreatedAndProtected(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	reservedIDs := []string{
		shmevent.ReservedGroupChannel, shmevent.ReservedGroupCluster, shmevent.ReservedGroupLearner,
		shmevent.ReservedGroupRelay, shmevent.ReservedGroupVoter, shmevent.ReservedGroupRemote, shmevent.ReservedGroupExecute,
	}
	fixedIDs := append(append([]string{}, reservedIDs...), shmevent.DefaultPublicGroupID)
	wantIDs := append(append([]string{}, fixedIDs...), leaderID)
	sort.Strings(wantIDs)

	var groups []kvctl.Group
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		groups, err = kvctl.ListGroups()
		return len(groups) == len(wantIDs), err
	})
	gotIDs := make([]string, len(groups))
	for i, g := range groups {
		gotIDs[i] = g.ID
		wantPublic := g.ID == shmevent.DefaultPublicGroupID
		if g.Public != wantPublic {
			t.Fatalf("group %q public = %v, want %v", g.ID, g.Public, wantPublic)
		}
	}
	sort.Strings(gotIDs)
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("ListGroups = %v, want %v", gotIDs, wantIDs)
		}
	}

	// The solo bootstrap leader is a voter, never a learner.
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForPeer(leaderID)
		if err != nil {
			return false, err
		}
		sort.Strings(groupIDs)
		return len(groupIDs) == 2 && groupIDs[0] == shmevent.ReservedGroupCluster && groupIDs[1] == shmevent.ReservedGroupVoter, nil
	})

	// Every *reserved* group's own record is protected, regardless of
	// which one it is -- the seven fixed names or the dynamic
	// peer-identity-shaped one. DefaultPublicGroupID is deliberately
	// excluded: unlike the reserved seven, it's just a mutable starting
	// default (see that constant's doc comment), not daemon-protected.
	protectedIDs := append(append([]string{}, reservedIDs...), leaderID)
	for _, id := range protectedIDs {
		if err := kvctl.PutGroup(id, "renamed", false); err == nil {
			t.Fatalf("PutGroup against reserved group %q: want error, got none", id)
		}
		if err := kvctl.DeleteGroup(id); err == nil {
			t.Fatalf("DeleteGroup against reserved group %q: want error, got none", id)
		}
	}
	if err := kvctl.PutGroup(shmevent.DefaultPublicGroupID, "renamed", false); err != nil {
		t.Fatalf("PutGroup against the default public group: %v", err)
	}
	if err := kvctl.PutGroup(shmevent.DefaultPublicGroupID, shmevent.DefaultPublicGroupID, true); err != nil {
		t.Fatalf("restore the default public group's original name/public flag: %v", err)
	}

	// cluster/voter/learner membership is daemon-managed only.
	for _, id := range []string{shmevent.ReservedGroupCluster, shmevent.ReservedGroupVoter, shmevent.ReservedGroupLearner} {
		if err := kvctl.AddPeerToGroup(leaderID, id); err == nil {
			t.Fatalf("AddPeerToGroup against reserved group %q: want error, got none", id)
		}
		if err := kvctl.RemovePeerFromGroup(leaderID, id); err == nil {
			t.Fatalf("RemovePeerFromGroup against reserved group %q: want error, got none", id)
		}
	}

	// "channel"/"relay"/"remote"/"execute" and the leader's own personal
	// group are all the deliberate exception: their membership is an
	// ordinary operator grant, not tied to actual cluster membership.
	for _, id := range []string{shmevent.ReservedGroupChannel, shmevent.ReservedGroupRelay, shmevent.ReservedGroupRemote, shmevent.ReservedGroupExecute, leaderID} {
		if err := kvctl.AddPeerToGroup(leaderID, id); err != nil {
			t.Fatalf("AddPeerToGroup against group %q: %v", id, err)
		}
		pollUntilTrue(t, 10*time.Second, func() (bool, error) {
			groupIDs, err := kvctl.ListGroupsForPeer(leaderID)
			if err != nil {
				return false, err
			}
			for _, g := range groupIDs {
				if g == id {
					return true, nil
				}
			}
			return false, nil
		})
		if err := kvctl.RemovePeerFromGroup(leaderID, id); err != nil {
			t.Fatalf("RemovePeerFromGroup against group %q: %v", id, err)
		}
	}
}
