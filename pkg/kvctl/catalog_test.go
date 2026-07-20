package kvctl_test

import (
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// fastRaftArgs shortens hashicorp/raft's WAN-appropriate default timeouts
// for a same-machine test -- see TestAddSetGetAcrossNodes's identical
// constant for why.
var fastRaftArgs = []string{
	"-raft-heartbeat-timeout", "300ms",
	"-raft-election-timeout", "300ms",
	"-raft-leader-lease-timeout", "250ms",
}

// pollUntilTrue retries check until it reports true, or fails the test
// after timeout -- a write forwarded through raft (LogAppend, permit
// request/confirm) becomes locally readable asynchronously, same reason
// TestRequestConfirmPermitAcrossNodes already polls by hand.
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

// grantSelfParticipation makes leaderID (a single-node bootstrap leader,
// hence always a raft voter) a confirmed participant of groupID --
// request-then-confirm against itself, mirroring
// mobile/kvmobile/catalog_test.go's identical helper.
func grantSelfParticipation(t *testing.T, groupID, leaderID string) {
	t.Helper()
	if err := kvctl.RequestGroupParticipation(groupID, leaderID, ""); err != nil {
		t.Fatalf("RequestGroupParticipation: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		return true, kvctl.ConfirmGroupParticipation(groupID, leaderID)
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		return kvctl.IsGroupParticipant(groupID)
	})
}

// TestGroupCRUD drives Create/Get/List/Update/Delete against a real,
// single-node leader: Update/Delete must refuse before this node is a
// confirmed participant of the group and succeed after, and Delete's
// tombstone must exclude the group from both Get and List afterward.
func TestGroupCRUD(t *testing.T) {
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

	const groupID = "grp-1"
	if err := kvctl.CreateGroup(groupID, "Group One", "first group"); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	var g kvctl.Group
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		g, err = kvctl.GetGroup(groupID)
		return err == nil, err
	})
	if g.ID != groupID || g.Name != "Group One" || g.Description != "first group" {
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

	if err := kvctl.UpdateGroup(groupID, "Renamed", "updated desc"); err == nil {
		t.Fatalf("UpdateGroup before participation: want error, got none")
	}

	grantSelfParticipation(t, groupID, leaderID)

	if err := kvctl.UpdateGroup(groupID, "Renamed", "updated desc"); err != nil {
		t.Fatalf("UpdateGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		g, err = kvctl.GetGroup(groupID)
		if err != nil {
			return false, err
		}
		return g.Name == "Renamed", nil
	})
	if g.Description != "updated desc" {
		t.Fatalf("GetGroup after update Description = %q, want %q", g.Description, "updated desc")
	}

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

// TestGroupParticipationLifecycle drives IsGroupParticipant/
// RequestGroupParticipation/ConfirmGroupParticipation/
// RevokeGroupParticipation end to end.
func TestGroupParticipationLifecycle(t *testing.T) {
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

	const groupID = "grp-participation"

	ok, err := kvctl.IsGroupParticipant(groupID)
	if err != nil {
		t.Fatalf("IsGroupParticipant (before): %v", err)
	}
	if ok {
		t.Fatalf("IsGroupParticipant (before) = true, want false")
	}

	grantSelfParticipation(t, groupID, leaderID)

	if err := kvctl.RevokeGroupParticipation(groupID, leaderID); err != nil {
		t.Fatalf("RevokeGroupParticipation: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		ok, err := kvctl.IsGroupParticipant(groupID)
		if err != nil {
			return false, err
		}
		return !ok, nil
	})
}

// TestCommandCRUD drives Create/Get/List/Update/Delete for Commands,
// including the participation gate (unlike Group, every Command
// operation -- reads included -- requires it) and the FormSchema
// round-trip.
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

	const groupID = "grp-cmds"

	if err := kvctl.CreateCommand("cmd-1", groupID, leaderID, "Reboot", "restart the device", nil); err == nil {
		t.Fatalf("CreateCommand before participation: want error, got none")
	}

	grantSelfParticipation(t, groupID, leaderID)

	schema := []kvctl.FormField{{Name: "delay_seconds", Label: "Delay (seconds)", Type: "number", Required: true}}
	if err := kvctl.CreateCommand("cmd-1", groupID, leaderID, "Reboot", "restart the device", schema); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}

	var cmd kvctl.Command
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		cmd, err = kvctl.GetCommand(groupID, "cmd-1")
		return err == nil, err
	})
	if cmd.Name != "Reboot" || cmd.TargetPeerID != leaderID || cmd.GroupID != groupID {
		t.Fatalf("GetCommand = %+v, unexpected", cmd)
	}
	if len(cmd.FormSchema) != 1 || cmd.FormSchema[0].Name != "delay_seconds" {
		t.Fatalf("GetCommand FormSchema = %+v, want one field named delay_seconds", cmd.FormSchema)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		commands, err := kvctl.ListCommands(groupID)
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

	if err := kvctl.UpdateCommand("cmd-1", groupID, leaderID, "Reboot Now", "restart immediately", nil); err != nil {
		t.Fatalf("UpdateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		fresh, err := kvctl.GetCommand(groupID, "cmd-1")
		if err != nil {
			return false, err
		}
		cmd = fresh
		return cmd.Name == "Reboot Now", nil
	})
	if len(cmd.FormSchema) != 0 {
		t.Fatalf("GetCommand after update FormSchema = %+v, want empty (update passed no schema)", cmd.FormSchema)
	}

	if err := kvctl.DeleteCommand(groupID, "cmd-1"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand(groupID, "cmd-1")
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		commands, err := kvctl.ListCommands(groupID)
		if err != nil {
			return false, err
		}
		return len(commands) == 0, nil
	})

	if _, err := kvctl.ListCommands("some-other-group-never-joined"); err == nil {
		t.Fatalf("ListCommands for non-participant group: want error, got none")
	}
}

// TestCatalogEmptyListsAreEmptyArrays checks ListGroups/ListCommands
// return a zero-length (nil) slice, not an error, when nothing matches.
func TestCatalogEmptyListsAreEmptyArrays(t *testing.T) {
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

	groups, err := kvctl.ListGroups()
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("ListGroups (empty) = %+v, want none", groups)
	}

	grantSelfParticipation(t, "empty-group", leaderID)
	commands, err := kvctl.ListCommands("empty-group")
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("ListCommands (empty) = %+v, want none", commands)
	}
}

// TestCatalogIDValidation checks CreateGroup rejects an empty or
// oversized id before ever touching the daemon.
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

	if err := kvctl.CreateGroup("", "x", "y"); err == nil {
		t.Fatalf("CreateGroup with empty id: want error, got none")
	}
	oversized := make([]byte, 257)
	for i := range oversized {
		oversized[i] = 'a'
	}
	if err := kvctl.CreateGroup(string(oversized), "x", "y"); err == nil {
		t.Fatalf("CreateGroup with oversized id: want error, got none")
	}
}
