package kvfsm_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// newExecInviteTestFSM opens a fresh, empty store-backed FSM for a test.
func newExecInviteTestFSM(t *testing.T) (*kvfsm.FSM, *store.Store) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return kvfsm.New(s), s
}

func applyExecInvite(t *testing.T, f *kvfsm.FSM, op kvfsm.OpType, key, value []byte) kvfsm.ApplyResult {
	t.Helper()
	res, ok := f.Apply(&raft.Log{Data: kvfsm.EncodeCommand(op, key, value)}).(kvfsm.ApplyResult)
	if !ok {
		t.Fatalf("Apply did not return kvfsm.ApplyResult")
	}
	return res
}

// grantCommandAccess links commandID to groupID and groupID to peerID, the
// two records isPermittedForCommand (both pkg/kvctl's client-side check and
// kvfsm's own authoritative one) requires to consider peerID permitted for
// commandID.
func grantCommandAccess(t *testing.T, f *kvfsm.FSM, commandID, groupID, peerID string) {
	t.Helper()
	gcKey, err := shmevent.GroupCommandKey([]byte(commandID), []byte(groupID))
	if err != nil {
		t.Fatalf("GroupCommandKey: %v", err)
	}
	if res := applyExecInvite(t, f, kvfsm.OpSet, gcKey, nil); res.Err != nil {
		t.Fatalf("Apply OpSet GroupCommand: %v", res.Err)
	}
	pgKey, err := shmevent.PeerGroupKey([]byte(peerID), []byte(groupID))
	if err != nil {
		t.Fatalf("PeerGroupKey: %v", err)
	}
	if res := applyExecInvite(t, f, kvfsm.OpSet, pgKey, nil); res.Err != nil {
		t.Fatalf("Apply OpSet PeerGroup: %v", res.Err)
	}
}

func setExecInvite(t *testing.T, f *kvfsm.FSM, token []byte, commandID, inputsJSON string) []byte {
	t.Helper()
	key := shmevent.ExecInviteKey(token)
	if res := applyExecInvite(t, f, kvfsm.OpSet, key, shmevent.EncodeExecInviteRecord(commandID, inputsJSON)); res.Err != nil {
		t.Fatalf("Apply OpSet ExecInvite: %v", res.Err)
	}
	return key
}

// TestApplyOpConsumeExecInvitePermittedRedeemDeletesAndReturnsRecord checks
// OpConsumeExecInvite's success path: a redeeming peer that *is* permitted
// for the invite's commandID gets the record's value back via
// ApplyResult.Value, and the invite is gone afterward.
func TestApplyOpConsumeExecInvitePermittedRedeemDeletesAndReturnsRecord(t *testing.T) {
	f, s := newExecInviteTestFSM(t)

	grantCommandAccess(t, f, "cmd-1", "group-1", "peerA")
	token := []byte("0123456789abcdef")
	key := setExecInvite(t, f, token, "cmd-1", `{"x":1}`)

	res := applyExecInvite(t, f, kvfsm.OpConsumeExecInvite, key, []byte("peerA"))
	if res.Err != nil {
		t.Fatalf("Apply OpConsumeExecInvite: %v", res.Err)
	}
	gotCommandID, gotInputs, err := shmevent.DecodeExecInviteRecord(res.Value)
	if err != nil {
		t.Fatalf("DecodeExecInviteRecord: %v", err)
	}
	if gotCommandID != "cmd-1" || gotInputs != `{"x":1}` {
		t.Fatalf("got commandID=%q inputs=%q, want commandID=%q inputs=%q", gotCommandID, gotInputs, "cmd-1", `{"x":1}`)
	}

	if _, err := s.Get(key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("invite key: got %v, want ErrNotFound (should be deleted after a permitted consume)", err)
	}
}

// TestApplyOpConsumeExecInviteUnpermittedRedeemFailsAndKeepsInvite checks
// the ACL half of OpConsumeExecInvite's contract: a redeeming peer with no
// Group/Command standing for the invite's commandID is rejected, and --
// unlike a permitted redemption -- the invite record is left in place, so
// a legitimate peer can still redeem it later.
func TestApplyOpConsumeExecInviteUnpermittedRedeemFailsAndKeepsInvite(t *testing.T) {
	f, s := newExecInviteTestFSM(t)

	// Deliberately no grantCommandAccess call: no peer is permitted for
	// cmd-1 at all.
	token := []byte("fedcba9876543210")
	key := setExecInvite(t, f, token, "cmd-1", "")

	res := applyExecInvite(t, f, kvfsm.OpConsumeExecInvite, key, []byte("peerB"))
	if res.Err == nil {
		t.Fatal("Apply OpConsumeExecInvite by an unpermitted peer unexpectedly succeeded")
	}

	if _, err := s.Get(key); err != nil {
		t.Fatalf("invite key: got %v, want it to still be present (an unauthorized attempt must not consume the ticket)", err)
	}
}

// TestApplyOpConsumeExecInviteTwiceFailsSecondTime is what actually makes
// an exec invite "one time": redeeming the same token again after a
// successful, permitted redemption must fail, not silently succeed (or
// silently re-run the ACL check and succeed again).
func TestApplyOpConsumeExecInviteTwiceFailsSecondTime(t *testing.T) {
	f, _ := newExecInviteTestFSM(t)

	grantCommandAccess(t, f, "cmd-1", "group-1", "peerA")
	token := []byte("aaaaaaaaaaaaaaaa")
	key := setExecInvite(t, f, token, "cmd-1", "")

	if res := applyExecInvite(t, f, kvfsm.OpConsumeExecInvite, key, []byte("peerA")); res.Err != nil {
		t.Fatalf("first Apply OpConsumeExecInvite: %v", res.Err)
	}

	res := applyExecInvite(t, f, kvfsm.OpConsumeExecInvite, key, []byte("peerA"))
	if res.Err == nil {
		t.Fatal("second Apply OpConsumeExecInvite unexpectedly succeeded -- invite should already be consumed")
	}
}

// TestApplyOpConsumeExecInviteNoSuchInviteFails exercises the "invalid
// token" branch directly, with no invite ever having existed under this
// key.
func TestApplyOpConsumeExecInviteNoSuchInviteFails(t *testing.T) {
	f, _ := newExecInviteTestFSM(t)

	key := shmevent.ExecInviteKey([]byte("ghostghostghostg"))
	res := applyExecInvite(t, f, kvfsm.OpConsumeExecInvite, key, []byte("peerA"))
	if res.Err == nil {
		t.Fatal("Apply OpConsumeExecInvite on a never-created invite unexpectedly succeeded")
	}
}
