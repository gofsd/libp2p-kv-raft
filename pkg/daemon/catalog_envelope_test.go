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

// TestCatalogEnvelopePutDeleteRoundTrip proves EventCatalogPut/
// EventCatalogDelete -- the generic envelope added alongside (not instead
// of) EventGroupPut/Delete et al. -- reach the exact same store key/value
// their kind-specific predecessor does, that an unrecognized entityKind is
// rejected rather than silently ignored, and that the per-kind validation
// hook (IsReservedGroupID, moved into catalogPutSpecs/catalogDeleteSpecs)
// still applies when reached through the envelope.
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
		buf, err := shmevent.Encode(m, leader.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return leader.handleShmEvent(ctx, decoded, crc, sig, leader.localCaller())
	}

	// An unrecognized entity kind must be rejected outright, not silently
	// dropped or misrouted to the wrong table entry.
	resp := call(shmevent.Msg{EventType: shmevent.EventCatalogPut, Value: shmevent.EncodeCatalogPayload(0xEE, []byte("junk")), ID: 1})
	if resp.EventType != shmevent.EventError {
		t.Fatal("catalog_put with an unknown entity kind unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "unrecognized entity kind") {
		t.Fatalf("catalog_put with an unknown kind rejected for the wrong reason: %s", resp.Value)
	}

	// KindGroup through the envelope must land at the identical GroupKey a
	// direct EventGroupPut would, and be readable there.
	putPayload, err := shmevent.EncodeGroupPutPayload("grp-via-envelope", "Envelope Group", false)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}
	resp = call(shmevent.Msg{
		EventType: shmevent.EventCatalogPut,
		Value:     shmevent.EncodeCatalogPayload(shmevent.KindGroup, putPayload),
		ID:        2,
	})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("catalog_put(KindGroup) rejected: %s", resp.Value)
	}

	groupKey := shmevent.GroupKey([]byte("grp-via-envelope"))
	getResp := call(shmevent.Msg{EventType: shmevent.EventGetField, Value: groupKey, ID: 3})
	if getResp.EventType == shmevent.EventError {
		t.Fatalf("group put via envelope did not land at GroupKey: %s", getResp.Value)
	}
	if name, _, err := shmevent.DecodeGroupPayload(getResp.Value); err != nil || name != "Envelope Group" {
		t.Fatalf("got name=%q err=%v, want name=%q", name, err, "Envelope Group")
	}

	// The reserved-group-id validation hook (previously inline in
	// EventGroupPut's own case) must still apply when reached through the
	// generic envelope.
	reservedPayload, err := shmevent.EncodeGroupPutPayload(shmevent.ReservedGroupCluster, "renamed", false)
	if err != nil {
		t.Fatalf("EncodeGroupPutPayload: %v", err)
	}
	resp = call(shmevent.Msg{
		EventType: shmevent.EventCatalogPut,
		Value:     shmevent.EncodeCatalogPayload(shmevent.KindGroup, reservedPayload),
		ID:        4,
	})
	if resp.EventType != shmevent.EventError {
		t.Fatal("catalog_put against a reserved group id unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "reserved") {
		t.Fatalf("reserved-group-id rejection had the wrong message: %s", resp.Value)
	}

	// Deleting through the envelope must remove the same record.
	resp = call(shmevent.Msg{
		EventType: shmevent.EventCatalogDelete,
		Value:     shmevent.EncodeCatalogPayload(shmevent.KindGroup, []byte("grp-via-envelope")),
		ID:        5,
	})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("catalog_delete(KindGroup) rejected: %s", resp.Value)
	}
	getResp = call(shmevent.Msg{EventType: shmevent.EventGetField, Value: groupKey, ID: 6})
	if getResp.EventType != shmevent.EventError {
		t.Fatal("group deleted via envelope is still readable")
	}
}

// TestCatalogEnvelopeCommandPutAcceptsSpecOverPlainValueSize proves
// EventCatalogPut carries EventCommandPut's own KVValueSize (4KB) ceiling,
// not the generic 512-byte ValueSize -- a command's form spec is
// caller-authored content routinely bigger than 512 bytes, and this
// envelope defaulting to the narrower ceiling would silently reject a
// spec that fit fine through the direct EventCommandPut it replaces (see
// valueSizeFor's EventCatalogPut case in pkg/shmevent/event.go).
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
		buf, err := shmevent.Encode(m, leader.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return leader.handleShmEvent(ctx, decoded, crc, sig, leader.localCaller())
	}

	// Bigger than plain ValueSize (512) but within KVValueSize (4096) --
	// exactly the range that only a correct per-kind ceiling accepts.
	spec := make([]byte, 1500)
	for i := range spec {
		spec[i] = byte('a' + i%26)
	}
	putPayload, err := shmevent.EncodeCommandPutPayloadWithSpec("cmd-big-spec", "Big Spec Command", []byte("peer1"), spec)
	if err != nil {
		t.Fatalf("EncodeCommandPutPayloadWithSpec: %v", err)
	}
	resp := call(shmevent.Msg{
		EventType: shmevent.EventCatalogPut,
		Value:     shmevent.EncodeCatalogPayload(shmevent.KindCommand, putPayload),
		ID:        1,
	})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("catalog_put(KindCommand) with a %d-byte spec rejected: %s", len(spec), resp.Value)
	}

	commandKey := shmevent.CommandKey([]byte("cmd-big-spec"))
	deadline := time.Now().Add(10 * time.Second)
	for {
		getResp := call(shmevent.Msg{EventType: shmevent.EventGetField, Value: commandKey, ID: 2})
		if getResp.EventType != shmevent.EventError {
			if _, _, gotSpec, err := shmevent.DecodeCommandPayloadFull(getResp.Value); err == nil && string(gotSpec) == string(spec) {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("command with a large spec, put via the catalog envelope, never became readable with an intact spec")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
