package kvmobile

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/logref"
)

// TestEncodeDecodeLogRefNeedsNoSession pins the property android-app's
// scan dispatch depends on: a device tries the log-reference decoder on
// every code it sees, including before its own daemon has finished
// starting, so neither direction may touch a session.
func TestEncodeDecodeLogRefNeedsNoSession(t *testing.T) {
	data, err := EncodeLogRef(4417)
	if err != nil {
		t.Fatalf("EncodeLogRef: %v", err)
	}
	got, err := DecodeLogRef(data)
	if err != nil {
		t.Fatalf("DecodeLogRef: %v", err)
	}
	if got != 4417 {
		t.Fatalf("DecodeLogRef = %d, want 4417", got)
	}
}

func TestDecodeLogRefRejectsOtherCodes(t *testing.T) {
	// The two other kinds of code this app's scanner sees, neither of
	// which is a log reference.
	for _, payload := range []string{
		"com.gofsd.kvdemo.nav.group:KV",
		"com.gofsd.kvdemo.run:WyJLViIsIkdldCJd",
	} {
		if _, err := DecodeLogRef([]byte(payload)); !errors.Is(err, logref.ErrNotLogRef) {
			t.Fatalf("DecodeLogRef(%q) error = %v, want ErrNotLogRef", payload, err)
		}
	}
}

func TestEncodeLogRefRejectsNegativeID(t *testing.T) {
	if _, err := EncodeLogRef(-1); err == nil {
		t.Fatal("EncodeLogRef accepted a negative id")
	}
	if err := PutLogRef(-1, "KV", "", ""); err == nil {
		t.Fatal("PutLogRef accepted a negative id")
	}
}

// TestPutGetLogRefThroughKvmobile drives the registration half against a
// real leader: register an id, read it back, and confirm the group and
// the extra fields both survived -- and that the record landed under the
// convention pkg/logref documents rather than under some private key,
// which is what makes it resolvable by any device in the cluster.
func TestPutGetLogRefThroughKvmobile(t *testing.T) {
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

	if err := PutLogRef(4417, "Log records", `{"label":"crate A"}`, "received at gate 3"); err != nil {
		t.Fatalf("PutLogRef: %v", err)
	}

	group, err := LogRefGroup(4417)
	if err != nil {
		t.Fatalf("LogRefGroup: %v", err)
	}
	if group != "Log records" {
		t.Fatalf("LogRefGroup = %q, want %q", group, "Log records")
	}

	recJSON, err := GetLogRef(4417)
	if err != nil {
		t.Fatalf("GetLogRef: %v", err)
	}
	var rec logrecord.Record
	if err := json.Unmarshal([]byte(recJSON), &rec); err != nil {
		t.Fatalf("parse GetLogRef result %q: %v", recJSON, err)
	}
	if rec.Kind != logref.Kind {
		t.Errorf("Kind = %q, want %q", rec.Kind, logref.Kind)
	}
	if rec.UnitID != logref.UnitID(4417) {
		t.Errorf("UnitID = %q, want %q", rec.UnitID, logref.UnitID(4417))
	}
	if rec.Fields["label"] != "crate A" {
		t.Errorf("Fields[label] = %q, want %q", rec.Fields["label"], "crate A")
	}
	if rec.Narrative != "received at gate 3" {
		t.Errorf("Narrative = %q", rec.Narrative)
	}

	// The record is an ordinary log record, so the generic reader must
	// see exactly the same thing -- this is what makes a registration
	// inspectable (and correctable) with the tools that already exist,
	// rather than only through this file.
	viaLogQuery, err := LogQuery(logref.Kind, logref.UnitID(4417), "", "", "")
	if err != nil {
		t.Fatalf("LogQuery: %v", err)
	}
	if !strings.Contains(viaLogQuery, "crate A") {
		t.Fatalf("LogQuery over the log-reference kind did not return the registration: %s", viaLogQuery)
	}
}

// A group assignment is a correction away from being wrong: labels get
// printed before anyone decides what they are for. Registering an id
// again must re-point it rather than fail or be ignored.
func TestPutLogRefReRegistrationWins(t *testing.T) {
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

	if err := PutLogRef(9001, "KV", "", ""); err != nil {
		t.Fatalf("PutLogRef: %v", err)
	}
	if err := PutLogRef(9001, "Log records", "", ""); err != nil {
		t.Fatalf("PutLogRef (re-register): %v", err)
	}

	group, err := LogRefGroup(9001)
	if err != nil {
		t.Fatalf("LogRefGroup: %v", err)
	}
	if group != "Log records" {
		t.Fatalf("LogRefGroup = %q, want the newer registration %q", group, "Log records")
	}
}

// An id nobody registered is a real answer, not an empty one: a person
// holding a phone at an unknown label needs to be told, and the UI can
// only say so if this reports it.
func TestGetLogRefUnknownIDIsAnError(t *testing.T) {
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

	if _, err := GetLogRef(123456); err == nil {
		t.Fatal("GetLogRef returned an unregistered id without an error")
	}
}
