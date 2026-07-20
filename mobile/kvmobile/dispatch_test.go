package kvmobile

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestSubmitCommandIndexesExecutionsByPeer drives SubmitCommand and checks
// ListExecutionsByPeer surfaces the resulting dispatch under both the
// requester's and the target's peer id, with the right Role each time --
// the two-index-write behavior appendCommandExecIndex adds to
// SubmitCommand.
func TestSubmitCommandIndexesExecutionsByPeer(t *testing.T) {
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

	const groupID = "grp-exec"
	const targetPeerID = "some-target-peer-id"
	requesterPeerID := PeerID()

	grantSelfParticipation(t, groupID)
	if err := CreateCommand("cmd-1", groupID, targetPeerID, "Reboot", "restart", ""); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(groupID, "cmd-1")
		return err == nil, nil
	})

	instanceID, err := SubmitCommand(groupID, "cmd-1", `{"delay":5}`)
	if err != nil {
		t.Fatalf("SubmitCommand: %v", err)
	}
	if instanceID == "" {
		t.Fatalf("SubmitCommand returned empty instance id")
	}

	findExecution := func(view []CommandExecution) (CommandExecution, bool) {
		for _, e := range view {
			if e.InstanceID == instanceID {
				return e, true
			}
		}
		return CommandExecution{}, false
	}

	var requesterEntry CommandExecution
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListExecutionsByPeer(requesterPeerID)
		if err != nil {
			return false, err
		}
		var view []CommandExecution
		if err := json.Unmarshal([]byte(out), &view); err != nil {
			return false, err
		}
		e, ok := findExecution(view)
		requesterEntry = e
		return ok, nil
	})
	if requesterEntry.Role != "requester" || requesterEntry.RequestedBy != requesterPeerID ||
		requesterEntry.TargetPeerID != targetPeerID || requesterEntry.GroupID != groupID || requesterEntry.CommandID != "cmd-1" {
		t.Fatalf("ListExecutionsByPeer(requester) entry = %+v, unexpected", requesterEntry)
	}

	var targetEntry CommandExecution
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListExecutionsByPeer(targetPeerID)
		if err != nil {
			return false, err
		}
		var view []CommandExecution
		if err := json.Unmarshal([]byte(out), &view); err != nil {
			return false, err
		}
		e, ok := findExecution(view)
		targetEntry = e
		return ok, nil
	})
	if targetEntry.Role != "target" {
		t.Fatalf("ListExecutionsByPeer(target) entry role = %q, want %q", targetEntry.Role, "target")
	}

	if maxExecutionsByPeer != 200 {
		t.Fatalf("maxExecutionsByPeer = %d, want 200", maxExecutionsByPeer)
	}
}

// TestSubmitCommandSelfTargetWritesOneIndexEntry checks SubmitCommand
// doesn't double-index a dispatch when the requester is also the
// command's target -- appendCommandExecIndex's role-collapse case.
func TestSubmitCommandSelfTargetWritesOneIndexEntry(t *testing.T) {
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

	const groupID = "grp-self-target"
	selfPeerID := PeerID()

	grantSelfParticipation(t, groupID)
	if err := CreateCommand("cmd-self", groupID, selfPeerID, "Self", "", ""); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(groupID, "cmd-self")
		return err == nil, nil
	})
	instanceID, err := SubmitCommand(groupID, "cmd-self", "")
	if err != nil {
		t.Fatalf("SubmitCommand: %v", err)
	}

	var view []CommandExecution
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListExecutionsByPeer(selfPeerID)
		if err != nil {
			return false, err
		}
		return true, json.Unmarshal([]byte(out), &view)
	})

	count := 0
	for _, e := range view {
		if e.InstanceID == instanceID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("ListExecutionsByPeer(self) has %d entries for instance %s, want exactly 1", count, instanceID)
	}
}

// TestLatestCommandLog drives AppendCommandLog + LatestCommandLog: it
// must error before any entry exists for an instance, and always reflect
// whichever entry was appended most recently.
func TestLatestCommandLog(t *testing.T) {
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

	const instanceID = "instance-latest-1"

	if _, err := LatestCommandLog(instanceID); err == nil {
		t.Fatalf("LatestCommandLog before any entry: want error, got none")
	}

	if err := AppendCommandLog("", instanceID, `{"status":"running"}`, "starting up"); err != nil {
		t.Fatalf("AppendCommandLog (1): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(instanceID)
		if err != nil {
			return false, err
		}
		var rec logrecord.Record
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			return false, err
		}
		return rec.Narrative == "starting up", nil
	})

	if err := AppendCommandLog("", instanceID, `{"status":"done"}`, "finished ok"); err != nil {
		t.Fatalf("AppendCommandLog (2): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(instanceID)
		if err != nil {
			return false, err
		}
		var rec logrecord.Record
		if err := json.Unmarshal([]byte(out), &rec); err != nil {
			return false, err
		}
		return rec.Narrative == "finished ok" && rec.Fields["status"] == "done", nil
	})
}

// TestLatestCommandLogReturnsOutputIntact checks a normal-sized
// AppendCommandLog entry round-trips through LatestCommandLog byte-for-
// byte, and that the whole thing is comfortably inside
// shmevent.ValueSize -- LatestCommandLog itself does no truncation (see
// its doc comment), since AppendCommandLog already can't store anything
// over that limit in the first place.
func TestLatestCommandLogReturnsOutputIntact(t *testing.T) {
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

	const instanceID = "instance-latest-normal"
	output := "exit code 0: " + strings.Repeat("ok ", 50)
	if err := AppendCommandLog("", instanceID, `{"status":"done"}`, output); err != nil {
		t.Fatalf("AppendCommandLog: %v", err)
	}

	var rec logrecord.Record
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := LatestCommandLog(instanceID)
		if err != nil {
			return false, err
		}
		if len(out) > shmevent.ValueSize {
			t.Fatalf("LatestCommandLog output is %d bytes, want <= %d (shmevent.ValueSize)", len(out), shmevent.ValueSize)
		}
		return true, json.Unmarshal([]byte(out), &rec)
	})
	if rec.Narrative != output {
		t.Fatalf("LatestCommandLog narrative = %q, want %q (unmodified)", rec.Narrative, output)
	}
}

// TestAppendCommandLogRejectsOversizedEntry checks AppendCommandLog
// surfaces a clear error for a narrative too large for
// shmevent.ValueSize, rather than silently accepting or corrupting it --
// the write-time half of the guarantee LatestCommandLog's doc comment
// relies on.
func TestAppendCommandLogRejectsOversizedEntry(t *testing.T) {
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

	hugeOutput := strings.Repeat("x", shmevent.ValueSize*4)
	if err := AppendCommandLog("", "instance-oversized", "", hugeOutput); err == nil {
		t.Fatalf("AppendCommandLog with oversized narrative: want error, got none")
	}
}
