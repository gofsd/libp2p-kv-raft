package kvmobile

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

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
	if err := CreateGroup(groupID, "Group One"); err != nil {
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
	if g.ID != groupID || g.Name != "Group One" {
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

	if err := UpdateGroup(groupID, "Renamed"); err != nil {
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
		return g.Name == "Renamed", nil
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
		return len(cmds) == 0, nil
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

	if err := CreateGroup("grp-1", "Group One"); err != nil {
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

	if err := CreateGroup("grp-cascade", "Cascade Group"); err != nil {
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
		return len(groupIDs) == 0, nil
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

	if err := CreateGroup("grp-cmd-cascade", "Group"); err != nil {
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
// LogQuery already established.
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
	if out != "[]" {
		t.Fatalf("ListGroups (empty) = %q, want %q", out, "[]")
	}

	out, err = ListCommands()
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	if out != "[]" {
		t.Fatalf("ListCommands (empty) = %q, want %q", out, "[]")
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

	if err := CreateGroup("", "x"); err == nil {
		t.Fatalf("CreateGroup with empty id: want error, got none")
	}
	if err := CreateGroup(strings.Repeat("a", maxCatalogIDLen+1), "x"); err == nil {
		t.Fatalf("CreateGroup with oversized id: want error, got none")
	}
}
