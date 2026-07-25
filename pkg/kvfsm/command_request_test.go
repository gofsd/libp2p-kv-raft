package kvfsm_test

import (
	"errors"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// buildCommandRequestKeyValue mirrors what pkg/kvctl/mobile/kvmobile's
// SubmitCommand actually builds and hands to EventLogAppend, minus the
// client's own (now-redundant-for-security, still-kept-for-a-fast-local-
// failure) isPermittedForCommand pre-check.
func buildCommandRequestKeyValue(t *testing.T, commandID, instanceID, authorPeerID string) (key, value []byte) {
	t.Helper()
	rec := logrecord.Record{
		Kind:         shmevent.CommandRequestLogKind(commandID),
		UnitID:       instanceID,
		Timestamp:    time.Now(),
		AuthorPeerID: authorPeerID,
		Fields:       map[string]string{"command_id": commandID},
	}
	value, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	key, err = logrecord.BuildKey(rec.Kind, rec.UnitID, rec.Timestamp, [logrecord.RandSize]byte{})
	if err != nil {
		t.Fatalf("logrecord.BuildKey: %v", err)
	}
	return key, value
}

// TestApplyOpAppendCommandRequestPermittedSucceeds checks the success
// path: a submitting peer with real Group/GroupCommand/PeerGroup standing
// for commandID gets its CommandRequest written.
func TestApplyOpAppendCommandRequestPermittedSucceeds(t *testing.T) {
	f, s := newExecInviteTestFSM(t)

	grantCommandAccess(t, f, "cmd-1", "group-1", "peerA")
	key, recordValue := buildCommandRequestKeyValue(t, "cmd-1", "inst-1", "peerA")
	payload, err := shmevent.EncodeCommandRequestApplyPayload("peerA", recordValue)
	if err != nil {
		t.Fatalf("EncodeCommandRequestApplyPayload: %v", err)
	}

	res := applyExecInvite(t, f, kvfsm.OpAppendCommandRequest, key, payload)
	if res.Err != nil {
		t.Fatalf("Apply OpAppendCommandRequest: %v", res.Err)
	}

	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get key: %v", err)
	}
	rec, err := logrecord.Decode(got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if rec.Fields["command_id"] != "cmd-1" {
		t.Fatalf("got fields %+v, want command_id=cmd-1", rec.Fields)
	}
}

// TestApplyOpAppendCommandRequestPublicGroupSucceedsWithNoPeerGroupRecord
// proves the same public-group bypass OpConsumeExecInvite already has
// applies here too: no PeerGroup record needed at all.
func TestApplyOpAppendCommandRequestPublicGroupSucceedsWithNoPeerGroupRecord(t *testing.T) {
	f, s := newExecInviteTestFSM(t)

	grantPublicCommandAccess(t, f, "cmd-1", "group-public")
	key, recordValue := buildCommandRequestKeyValue(t, "cmd-1", "inst-1", "peerA")
	payload, err := shmevent.EncodeCommandRequestApplyPayload("peerA", recordValue)
	if err != nil {
		t.Fatalf("EncodeCommandRequestApplyPayload: %v", err)
	}

	res := applyExecInvite(t, f, kvfsm.OpAppendCommandRequest, key, payload)
	if res.Err != nil {
		t.Fatalf("Apply OpAppendCommandRequest: %v", res.Err)
	}
	if _, err := s.Get(key); err != nil {
		t.Fatalf("Get key: %v", err)
	}
}

// TestApplyOpAppendCommandRequestUnpermittedRejectedAndNotWritten is the
// actual gap this feature closes: a peer with no Group/GroupCommand/
// PeerGroup standing at all for commandID -- exactly what a remote caller
// forging this write directly (bypassing SubmitCommand's own client-side
// pre-check) would look like -- must be rejected, and nothing written.
func TestApplyOpAppendCommandRequestUnpermittedRejectedAndNotWritten(t *testing.T) {
	f, s := newExecInviteTestFSM(t)

	// Deliberately no grantCommandAccess/grantPublicCommandAccess call --
	// no peer is permitted for cmd-1 at all.
	key, recordValue := buildCommandRequestKeyValue(t, "cmd-1", "inst-1", "peerB")
	payload, err := shmevent.EncodeCommandRequestApplyPayload("peerB", recordValue)
	if err != nil {
		t.Fatalf("EncodeCommandRequestApplyPayload: %v", err)
	}

	res := applyExecInvite(t, f, kvfsm.OpAppendCommandRequest, key, payload)
	if res.Err == nil {
		t.Fatal("Apply OpAppendCommandRequest by an unpermitted peer unexpectedly succeeded")
	}

	if _, err := s.Get(key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("key: got %v, want ErrNotFound (nothing should have been written)", err)
	}
}

// TestApplyOpAppendCommandRequestIgnoresSelfDeclaredAuthorPeerID proves
// the ACL check runs against the payload's authorPeerID field (which
// pkg/daemon populates from the connection-authenticated caller identity,
// never anything the message itself claims) rather than the record's own
// AuthorPeerID -- a forged record claiming to be a permitted peer while
// the payload's actual authorPeerID is someone else must still be
// rejected.
func TestApplyOpAppendCommandRequestIgnoresSelfDeclaredAuthorPeerID(t *testing.T) {
	f, _ := newExecInviteTestFSM(t)

	grantCommandAccess(t, f, "cmd-1", "group-1", "peerA")
	// The logrecord.Record itself claims AuthorPeerID "peerA" (permitted),
	// but the actual authenticated payload field says "peerB" (not
	// permitted) -- the latter is what must govern.
	key, recordValue := buildCommandRequestKeyValue(t, "cmd-1", "inst-1", "peerA")
	payload, err := shmevent.EncodeCommandRequestApplyPayload("peerB", recordValue)
	if err != nil {
		t.Fatalf("EncodeCommandRequestApplyPayload: %v", err)
	}

	res := applyExecInvite(t, f, kvfsm.OpAppendCommandRequest, key, payload)
	if res.Err == nil {
		t.Fatal("Apply OpAppendCommandRequest with a forged self-declared AuthorPeerID unexpectedly succeeded")
	}
}
