package kvctl_test

import (
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestDialSubmitCommandAndQueryCommandLogAcrossClusters drives
// DialSubmitCommand/DialQueryCommandLog end to end through two
// independently-spawned, never-joined solo nodes -- the CLI-facing path
// behind `mage dialsubmitcommand`/`mage dialquerycommandlog`, previously
// untested at any layer: unlike RequestPublicAccess/PublicAccess (which
// pkg/daemon covers in-process), this pair had no test anywhere in the
// module. A stranger with no raft membership or prior relationship with
// the target cluster submits a command linked to a public group there,
// then reads back a log entry appended against that same instance, using
// only the instance id DialSubmitCommand handed back as its credential
// (see shmevent.EventDialQueryCommandLog's doc comment).
func TestDialSubmitCommandAndQueryCommandLogAcrossClusters(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	targetID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (target): %v", err)
	}
	strangerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (stranger, separate solo cluster): %v", err)
	}

	const groupID = "grp-dial-public"
	const commandID = "cmd-dial-public"

	if err := kvctl.Use(targetID); err != nil {
		t.Fatalf("Use(target): %v", err)
	}
	if err := kvctl.PutGroup(groupID, "Dial Public Group", true); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand(commandID, "Dial Public Command", targetID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(commandID, groupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand(commandID)
		return err == nil, nil
	})
	targetAddr, err := kvctl.GetOwnAddr()
	if err != nil {
		t.Fatalf("GetOwnAddr(target): %v", err)
	}

	if err := kvctl.Use(strangerID); err != nil {
		t.Fatalf("Use(stranger): %v", err)
	}
	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.DialSubmitCommand(targetAddr, commandID, `{"k":"v"}`, "kvctl-live-test")
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatal("DialSubmitCommand returned an empty instance id")
	}

	// Nothing has appended a log entry for this instance yet -- confirm
	// DialQueryCommandLog reports that cleanly (no error, no records)
	// before anything exists to read back.
	before, err := kvctl.DialQueryCommandLog(targetAddr, instanceID, time.Unix(0, 0), time.Now(), 0)
	if err != nil {
		t.Fatalf("DialQueryCommandLog before any log entry: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("DialQueryCommandLog before any log entry = %+v, want none", before)
	}

	if err := kvctl.Use(targetID); err != nil {
		t.Fatalf("Use(target): %v", err)
	}
	if err := kvctl.AppendCommandLog("", instanceID, map[string]string{"status": "done"}, "finished"); err != nil {
		t.Fatalf("AppendCommandLog: %v", err)
	}

	if err := kvctl.Use(strangerID); err != nil {
		t.Fatalf("Use(stranger): %v", err)
	}
	var after []logrecord.Record
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		records, err := kvctl.DialQueryCommandLog(targetAddr, instanceID, time.Unix(0, 0), time.Now(), 0)
		if err != nil {
			return false, err
		}
		after = records
		return len(records) == 1, nil
	})
	if len(after) != 1 || after[0].Narrative != "finished" {
		t.Fatalf("DialQueryCommandLog after AppendCommandLog = %+v, want one record with narrative %q", after, "finished")
	}
}
