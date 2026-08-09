package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestCatalogEnvelopePutDeleteRoundTrip used to prove EventCatalogPut/
// EventCatalogDelete -- a generic kind-byte envelope that dispatched to
// catalogPutSpecs/catalogDeleteSpecs -- reached the same store key/value its
// kind-specific predecessor (EventGroupPut/Delete) did. That generic
// envelope, and the entity-kind byte it switched on, no longer exist: the
// capnp rewrite gives every catalog kind its own top-level union variant
// (groupPut/groupDelete/commandPut/...) instead of one shared envelope, so
// there is no longer a way to submit "an unrecognized entity kind" at
// all -- that sub-case has no surviving translation and is dropped here
// rather than faked. What does still apply -- shmevent.IsReservedGroupID's
// validation hook and a plain put/get/delete round trip -- is exercised
// directly against groupPut/groupDelete below.
func TestCatalogEnvelopePutDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "leader.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	leader, err := start(Config{
		DataDir:            filepath.Join(tmpDir, "leader"),
		KeyPath:            key,
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}

	call := func(m shmevent.Msg) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, leader, m, leader.ed25519Priv)
	}

	// groupPut must land at the same GroupKey EventGroupPut always did, and
	// be readable there.
	putMsg, err := shmevent.NewGroupPut("grp-via-envelope", "Envelope Group", false)
	if err != nil {
		t.Fatalf("NewGroupPut: %v", err)
	}
	putMsg.SetId(2)
	resp := call(putMsg)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("group_put rejected: %s", mustErrMessage(t, resp))
	}

	groupKey := shmevent.GroupKey([]byte("grp-via-envelope"))
	getMsg, err := shmevent.NewGetFieldByKey(groupKey)
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	getMsg.SetId(3)
	getResp := call(getMsg)
	if getResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("group put did not land at GroupKey: %s", mustErrMessage(t, getResp))
	}
	gotValue, err := getResp.GetFieldByKey().Value()
	if err != nil {
		t.Fatalf("GetFieldByKey value: %v", err)
	}
	if name, _, err := shmevent.DecodeGroupPayload(gotValue); err != nil || name != "Envelope Group" {
		t.Fatalf("got name=%q err=%v, want name=%q", name, err, "Envelope Group")
	}

	// The reserved-group-id validation hook must still apply.
	reservedMsg, err := shmevent.NewGroupPut(shmevent.ReservedGroupCluster, "renamed", false)
	if err != nil {
		t.Fatalf("NewGroupPut: %v", err)
	}
	reservedMsg.SetId(4)
	resp = call(reservedMsg)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("group_put against a reserved group id unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "reserved") {
		t.Fatalf("reserved-group-id rejection had the wrong message: %s", mustErrMessage(t, resp))
	}

	// Deleting must remove the same record.
	deleteMsg, err := shmevent.NewGroupDelete("grp-via-envelope")
	if err != nil {
		t.Fatalf("NewGroupDelete: %v", err)
	}
	deleteMsg.SetId(5)
	resp = call(deleteMsg)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("group_delete rejected: %s", mustErrMessage(t, resp))
	}
	getMsg2, err := shmevent.NewGetFieldByKey(groupKey)
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	getMsg2.SetId(6)
	getResp = call(getMsg2)
	if getResp.Which() != shmevent.Event_Which_error {
		t.Fatal("group deleted via envelope is still readable")
	}
}

// TestCatalogEnvelopeCommandPutAcceptsSpecOverPlainValueSize used to prove
// EventCatalogPut carried EventCommandPut's own KVValueSize (4KB) ceiling,
// not the generic 512-byte ValueSize -- a command's form spec is
// caller-authored content routinely bigger than 512 bytes. Both ceilings
// (shmevent.ValueSize/KVValueSize) are gone entirely in the capnp rewrite --
// there is no artificial per-event size ceiling anymore, generic or
// per-kind -- so the "which ceiling applies" question this test asked no
// longer has an answer to check. What's still worth keeping is the
// underlying behavior: a spec well over the old 512-byte ValueSize (1500
// bytes, mirroring this test's original fixture) must still round-trip
// intact through commandPut.
func TestCatalogEnvelopeCommandPutAcceptsSpecOverPlainValueSize(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "leader.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	leader, err := start(Config{
		DataDir:            filepath.Join(tmpDir, "leader"),
		KeyPath:            key,
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}

	call := func(m shmevent.Msg) shmevent.Msg {
		t.Helper()
		return callLocal(t, ctx, leader, m, leader.ed25519Priv)
	}

	spec := make([]byte, 1500)
	for i := range spec {
		spec[i] = byte('a' + i%26)
	}
	putMsg, err := shmevent.NewCommandPutWithSpec("cmd-big-spec", "Big Spec Command", "peer1", string(spec))
	if err != nil {
		t.Fatalf("NewCommandPutWithSpec: %v", err)
	}
	putMsg.SetId(1)
	resp := call(putMsg)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("command_put with a %d-byte spec rejected: %s", len(spec), mustErrMessage(t, resp))
	}

	commandKey := shmevent.CommandKey([]byte("cmd-big-spec"))
	deadline := time.Now().Add(10 * time.Second)
	for {
		getMsg, err := shmevent.NewGetFieldByKey(commandKey)
		if err != nil {
			t.Fatalf("NewGetFieldByKey: %v", err)
		}
		getMsg.SetId(2)
		getResp := call(getMsg)
		if getResp.Which() != shmevent.Event_Which_error {
			gotValue, err := getResp.GetFieldByKey().Value()
			if err == nil {
				if _, _, gotSpec, err := shmevent.DecodeCommandPayloadFull(gotValue); err == nil && string(gotSpec) == string(spec) {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("command with a large spec never became readable with an intact spec")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
