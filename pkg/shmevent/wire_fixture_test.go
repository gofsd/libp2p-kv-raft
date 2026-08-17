package shmevent_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// updateWireFixture rewrites the committed fixture from whatever this build
// produces, for when api/shmevent.capnp legitimately changes:
//
//	go test ./pkg/shmevent -run TestWireFixture -update-wire-fixture
var updateWireFixture = flag.Bool("update-wire-fixture", false,
	"rewrite api/shmevent_wire_fixture.json from what this build produces")

// wireFixturePath is deliberately next to the schema rather than under a
// testdata/ directory: both this package and web-app/src/shmevent compile
// from api/shmevent.capnp, and the fixture is the artifact that proves they
// agree about it, so it belongs where a reader of either side will find it.
const wireFixturePath = "../../api/shmevent_wire_fixture.json"

// wireFixture is the committed cross-language wire fixture -- see
// TestWireFixture's doc comment.
type wireFixture struct {
	Doc               []string          `json:"_doc"`
	SigningKeySeedHex string            `json:"signing_key_seed_hex"`
	PublicKeyHex      string            `json:"public_key_hex"`
	Cases             []wireFixtureCase `json:"cases"`
}

type wireFixtureCase struct {
	Name string `json:"name"`
	// JSON is what EventFromMsg records for this message -- the same
	// {"event":...,"id":...,"fields":{...}} shape kvctl-cli's sendevent and
	// web-app's msg_to_json produce.
	JSON string `json:"json"`
	// EncodedHex is Encode's output, signed with the fixture's own key.
	EncodedHex string `json:"encoded_hex"`
	Crc32      uint32 `json:"crc32"`
	// Signed is false for the two variants a node accepts unsigned, whose
	// signature field is therefore 64 zero bytes rather than a real one.
	Signed bool `json:"signed"`
}

// fixtureSeed is a fixed, arbitrary Ed25519 seed -- a test fixture, not a
// key anything is protected with.
func fixtureSeed() []byte {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	return seed
}

// wireFixtureCases builds the messages the fixture records. Most come from
// e2edata.Event.ToMsg, which already dispatches over every union variant by
// name, so a case is just the JSON a caller would write; the handful that
// end in "_direct" are built through this package's own constructors,
// because they pin pointer states ToMsg's JSON cannot express (a
// present-but-empty Data field, an explicitly-cleared command spec).
func wireFixtureCases(t *testing.T) []struct {
	name string
	msg  shmevent.Msg
} {
	t.Helper()

	fromEvent := func(op string, fields map[string]string) shmevent.Msg {
		t.Helper()
		m, err := e2edata.Event{Op: op, ID: 7, Fields: fields}.ToMsg()
		if err != nil {
			t.Fatalf("%s: ToMsg: %v", op, err)
		}
		return m
	}
	// Takes a constructor's own (Msg, error) pair directly -- Go only
	// spreads a multi-value call into a call with no other arguments, which
	// is why this carries no name parameter.
	direct := func(m shmevent.Msg, err error) shmevent.Msg {
		t.Helper()
		if err != nil {
			t.Fatalf("build fixture message: %v", err)
		}
		m.SetId(7)
		return m
	}

	return []struct {
		name string
		msg  shmevent.Msg
	}{
		{"set_key", fromEvent("set_key", map[string]string{"value": "hello"})},
		{"set_field", fromEvent("set_field", map[string]string{"source_id": "42", "value": "world"})},
		{"get_key", fromEvent("get_key", map[string]string{"source_id": "42"})},
		{"get_field_by_registry", fromEvent("get_field_by_registry", map[string]string{"source_id": "9"})},
		{"get_field_by_key", fromEvent("get_field_by_key", map[string]string{"key": "some-key", "value": "some-value"})},
		{"get_public_key", fromEvent("get_public_key", nil)},
		{"get_private_key", fromEvent("get_private_key", nil)},
		{"bootstrap_or_join_cluster", fromEvent("bootstrap_or_join_cluster", map[string]string{
			"leader_addr": "/ip4/203.0.113.7/tcp/4001/p2p/12D3KooWLeaderExample",
		})},
		{"add_learner", fromEvent("add_learner", map[string]string{
			"claimed_peer_id": "3", "addr": "/ip4/198.51.100.9/tcp/4001",
		})},
		{"set", fromEvent("set", map[string]string{"key": "k", "value": "v"})},
		{"execute", fromEvent("execute", map[string]string{
			"source_id": "1", "destination_id": "2", "value": "payload",
		})},
		{"poll_execute", fromEvent("poll_execute", map[string]string{
			"sender_peer_id": "12D3KooWSenderExample", "value": "payload",
		})},
		{"list_range", fromEvent("list_range", map[string]string{"start": "aaa", "end": "zzz"})},
		{"log_append", fromEvent("log_append", map[string]string{
			"key": "0x01636d647265713a78", "value": "record-bytes",
		})},
		{"leave", fromEvent("leave", nil)},
		{"recruit", fromEvent("recruit", map[string]string{
			"ticket": "/ip4/203.0.113.7/tcp/4001#abcdef", "suffrage": "1",
		})},
		{"get_own_addr", fromEvent("get_own_addr", map[string]string{"addr": "/ip4/203.0.113.7/tcp/4001"})},
		{"channel_send", fromEvent("channel_send", map[string]string{
			"channel_id": "chan-1", "purpose": "2", "chunk": "0xdeadbeef",
		})},
		{"kick", fromEvent("kick", map[string]string{"peer_id": "12D3KooWGoneExample"})},
		{"get_version", fromEvent("get_version", map[string]string{
			"commit": "abc1234", "dirty": "true", "build_time": "2026-08-17T00:00:00Z",
			"go_version": "go1.25.13", "libp2p_version": "v0.38.0",
		})},
		{"public_access", fromEvent("public_access", map[string]string{
			"target_peer": "12D3KooWTargetExample", "note": "hello",
		})},
		{"dial_submit_command", fromEvent("dial_submit_command", map[string]string{
			"target_peer": "12D3KooWTargetExample", "command_id": "cmd-1",
			"inputs_json": `{"a":"b"}`, "note": "n",
		})},
		// Signed integers, which no other case covers.
		{"dial_query_command_log", fromEvent("dial_query_command_log", map[string]string{
			"target_peer": "12D3KooWTargetExample", "instance_id": "beef",
			"since": "1700000000000000000", "until": "1800000000000000000", "limit": "25",
		})},
		{"error", fromEvent("error", map[string]string{"message": "something went wrong"})},
		{"group_put", fromEvent("group_put", map[string]string{"id": "grp", "name": "Group", "public": "true"})},
		{"command_put", fromEvent("command_put", map[string]string{
			"id": "cmd", "name": "Command", "peer_id": "12D3KooWOwnerExample",
		})},
		{"station_put", fromEvent("station_put", map[string]string{
			"peer_id": "12D3KooWStationExample", "name": "Station", "attrs": `{"line":2}`,
		})},
		{"group_command_put", fromEvent("group_command_put", map[string]string{"command_id": "cmd", "group_id": "grp"})},
		{"peer_group_put", fromEvent("peer_group_put", map[string]string{"peer_id": "12D3KooWPeerExample", "group_id": "grp"})},
		{"permit_request", fromEvent("permit_request", map[string]string{
			"kind": "2", "peer_id": "12D3KooWPeerExample", "metadata": `{"addr":"/ip4/203.0.113.7/tcp/4001"}`,
		})},
		{"join_invite_create", fromEvent("join_invite_create", map[string]string{"token": "0x0102030405", "suffrage": "1"})},
		{"exec_invite_create", fromEvent("exec_invite_create", map[string]string{
			"token": "0x0a0b0c", "command_id": "cmd", "inputs_json": `{"a":"b"}`, "ttl_seconds": "3600",
		})},

		// Pointer states the JSON shape cannot express, built directly.
		//
		// A present-but-empty Data field: setting a key to an empty value.
		// Its canonical encoding differs from the same message with the
		// field absent, so both sides must keep them apart -- see
		// web-app/src/shmevent/mod.rs's own doc comment on this.
		{"set_empty_value_direct", direct(shmevent.NewSet([]byte("k"), []byte{}))},
		// A command_put that explicitly clears its stored spec, i.e. a
		// present-but-empty *Text* field.
		{"command_put_clearing_spec_direct", direct(
			shmevent.NewCommandPutWithSpec("cmd", "Command", "12D3KooWOwnerExample", ""))},
		{"command_put_with_spec_direct", direct(
			shmevent.NewCommandPutWithSpec("cmd", "Command", "12D3KooWOwnerExample", `{"fields":[]}`))},
		// A struct list, whose canonical encoding is the least obvious part
		// of the whole format -- if either side's canonicalization mishandles
		// element sizing, this is the case that catches it. Built directly
		// because e2edata's JSON shape deliberately cannot express a list
		// ("not representable as a generic event"), which is also why the
		// recorded json for this case carries no ops.
		{"txn_direct", direct(shmevent.NewTxn([]shmevent.TxnOpSpec{
			{Op: shmevent.TxnOpSet, Key: []byte("a"), Value: []byte("1")},
			{Op: shmevent.TxnOpCompareAbsent, Key: []byte("b")},
		}))},
		// The other list-carrying variant: List(Data) rather than a struct
		// list, and equally invisible to the JSON shape.
		{"dial_query_command_log_records_direct", direct(shmevent.NewDialQueryCommandLogResponse(
			[]logrecord.Record{
				{Kind: "cmdexec", UnitID: "beef", Timestamp: time.Unix(1700000000, 0).UTC(), AuthorPeerID: "12D3KooWAuthorExample"},
			}))},
	}
}

// TestWireFixture builds one message per interesting union variant, encodes
// each with a fixed key, and checks the result against the committed
// api/shmevent_wire_fixture.json -- the same file
// web-app/src/shmevent/tests.rs reads to prove its own codec agrees with
// this one byte-for-byte.
//
// Two implementations of the same wire format drift silently: both keep
// round-tripping against themselves while producing bytes the other rejects
// as forged, since the CRC and the signature cover the *canonical* encoding
// (see sign.go's marshalWithCrcAndEmptySig). A committed fixture is what
// turns that into a test failure on whichever side changed. This test fails
// if Go's output moves; the Rust side's fails if its own does.
//
// Regenerate deliberately, never to make a red test green without knowing
// why:
//
//	go test ./pkg/shmevent -run TestWireFixture -update-wire-fixture
func TestWireFixture(t *testing.T) {
	seed := fixtureSeed()
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)

	got := wireFixture{
		Doc: []string{
			"Cross-language wire fixture for api/shmevent.capnp: one encoded message per",
			"interesting union variant, produced by pkg/shmevent and read back by both",
			"pkg/shmevent (TestWireFixture) and web-app/src/shmevent (tests::go_fixture).",
			"Regenerate with: go test ./pkg/shmevent -run TestWireFixture -update-wire-fixture",
		},
		SigningKeySeedHex: hex.EncodeToString(seed),
		PublicKeyHex:      hex.EncodeToString(pub),
	}

	for _, c := range wireFixtureCases(t) {
		signed := shmevent.RequiresSignature(c.msg.Which())
		signingKey := priv
		if !signed {
			signingKey = nil
		}
		encoded, err := shmevent.Encode(c.msg, signingKey)
		if err != nil {
			t.Fatalf("%s: Encode: %v", c.name, err)
		}
		decoded, crc, sig, err := shmevent.Decode(encoded)
		if err != nil {
			t.Fatalf("%s: Decode: %v", c.name, err)
		}
		if signed {
			if err := shmevent.Verify(pub, decoded, crc, sig); err != nil {
				t.Fatalf("%s: Verify its own signature: %v", c.name, err)
			}
		}
		ev, err := e2edata.EventFromMsg(decoded)
		if err != nil {
			t.Fatalf("%s: EventFromMsg: %v", c.name, err)
		}
		evJSON, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("%s: marshal event: %v", c.name, err)
		}
		got.Cases = append(got.Cases, wireFixtureCase{
			Name:       c.name,
			JSON:       string(evJSON),
			EncodedHex: hex.EncodeToString(encoded),
			Crc32:      crc,
			Signed:     signed,
		})
	}

	rendered, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	rendered = append(rendered, '\n')

	path := filepath.Clean(wireFixturePath)
	if *updateWireFixture {
		if err := os.WriteFile(path, rendered, 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		t.Logf("wrote %s (%d cases)", path, len(got.Cases))
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture (regenerate with -update-wire-fixture): %v", err)
	}
	if string(want) != string(rendered) {
		t.Fatalf("this build's wire encoding no longer matches %s.\n"+
			"If api/shmevent.capnp or the encoding legitimately changed, regenerate with\n"+
			"  go test ./pkg/shmevent -run TestWireFixture -update-wire-fixture\n"+
			"and expect web-app/src/shmevent's own fixture test to need re-checking too.",
			path)
	}
}
