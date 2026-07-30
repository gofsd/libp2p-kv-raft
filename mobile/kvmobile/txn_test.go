package kvmobile

import (
	"testing"
)

// TestTxnAppliesAllOpsAtomically mirrors
// TestStartSoloBootstrapsSingleNodeClusterAndServesWrites' solo setup, then
// drives Txn instead of Submit: two Sets and a Delete of a
// pre-existing key, all in one space-separated ops string, verified
// readable/gone afterward via Get exactly like Submit's own round trip.
func TestTxnAppliesAllOpsAtomically(t *testing.T) {
	prevLeader := leaderMultiaddr
	leaderMultiaddr = ""
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := StartSolo(t.TempDir()); err != nil {
		t.Fatalf("StartSolo: %v", err)
	}

	if err := Submit("to-delete", "gone-soon"); err != nil {
		t.Fatalf("seed Submit: %v", err)
	}

	if err := Txn("k1=v1 k2=v2 del:to-delete"); err != nil {
		t.Fatalf("Txn: %v", err)
	}

	if got, err := Get("k1"); err != nil || got != "v1" {
		t.Fatalf("Get(k1) = %q, %v, want v1, nil", got, err)
	}
	if got, err := Get("k2"); err != nil || got != "v2" {
		t.Fatalf("Get(k2) = %q, %v, want v2, nil", got, err)
	}
	if _, err := Get("to-delete"); err == nil {
		t.Fatal("to-delete still readable after Txn deleted it")
	}
}

// TestTxnRejectsMalformedOpsString proves a malformed ops token (neither
// <key>=<value> nor del:<key>) is rejected before ever reaching the daemon,
// with no partial effect on the other, well-formed op in the same string.
func TestTxnRejectsMalformedOpsString(t *testing.T) {
	prevLeader := leaderMultiaddr
	leaderMultiaddr = ""
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := StartSolo(t.TempDir()); err != nil {
		t.Fatalf("StartSolo: %v", err)
	}

	if err := Txn("k1=v1 not-a-valid-op"); err == nil {
		t.Fatal("Txn with a malformed op unexpectedly succeeded")
	}
	if _, err := Get("k1"); err == nil {
		t.Fatal("k1 was written despite the whole Txn being rejected for a malformed op")
	}
}
