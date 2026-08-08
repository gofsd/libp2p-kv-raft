package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestDialSubmitCommandGrantsGenericCommand proves EventDialSubmitCommand's
// whole point end to end: EventPublicAccess generalized from one hardcoded
// command (DefaultPublicCommandID) to any commandID a target cluster has
// linked to a public group (see that event's doc comment). A stranger node
// -- never a member of leader's cluster, never granted anything -- dials
// leader directly, naming an arbitrary custom command plus its own
// inputsJSON/note, and the resulting CommandRequestLogKind record must land
// in leader's own log looking exactly like a local SubmitCommand would have
// written it.
func TestDialSubmitCommandGrantsGenericCommand(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	stranger := startExecuteTestNode(t, filepath.Join(t.TempDir(), "stranger"))
	connectPeers(t, ctx, stranger, leader)

	putGroup(t, ctx, leader, "grp-dial-public", "Public Group", true)
	putCommand(t, ctx, leader, "cmd-dial-public", "Reboot", leader.peerID)
	putGroupCommand(t, ctx, leader, "cmd-dial-public", "grp-dial-public")

	target := leader.advertisedAddrs()[0]
	inputsJSON := `{"foo":"bar"}`
	reqPayload, err := shmevent.EncodeDialSubmitCommandRequest(target, "cmd-dial-public", inputsJSON, "e2e-note")
	if err != nil {
		t.Fatalf("EncodeDialSubmitCommandRequest: %v", err)
	}

	resp := callLocal(t, ctx, stranger, shmevent.Msg{EventType: shmevent.EventDialSubmitCommand, Value: reqPayload, ID: 1}, stranger.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("dial_submit_command was rejected: %s", resp.Value)
	}
	instanceID := string(resp.Value)
	if instanceID == "" {
		t.Fatal("dial_submit_command returned no instance id")
	}

	// Checking success log: leader's own local (never-gated) view of its
	// command request queue must have exactly the record dialAndSubmitCommand
	// claims to have written, attributed to the stranger's own identity.
	base := time.Now()
	lo, hi := logrecord.ScanBounds(shmevent.CommandRequestLogKind("cmd-dial-public"), instanceID, base.Add(-time.Hour), base.Add(time.Hour))
	query, err := shmevent.EncodeListRangeQuery(lo, hi)
	if err != nil {
		t.Fatalf("EncodeListRangeQuery: %v", err)
	}
	queryResp := callLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventListRange, Value: query, ID: 2}, leader.ed25519Priv)
	if queryResp.EventType == shmevent.EventError {
		t.Fatalf("reading back the submitted command's log entry was rejected: %s", queryResp.Value)
	}
	if len(queryResp.Value) == 0 {
		t.Fatal("no log entry found for the submitted command -- dial_submit_command did not actually land")
	}
	_, value, err := shmevent.DecodeListRangeQuery(queryResp.Value)
	if err != nil {
		t.Fatalf("DecodeListRangeQuery: %v", err)
	}
	rec, err := logrecord.Decode(value)
	if err != nil {
		t.Fatalf("logrecord.Decode: %v", err)
	}
	if rec.Fields["command_id"] != "cmd-dial-public" {
		t.Fatalf("record command_id = %q, want cmd-dial-public", rec.Fields["command_id"])
	}
	if rec.Fields["inputs"] != inputsJSON {
		t.Fatalf("record inputs = %q, want %q", rec.Fields["inputs"], inputsJSON)
	}
	if rec.Fields["note"] != "e2e-note" {
		t.Fatalf("record note = %q, want e2e-note", rec.Fields["note"])
	}
	if rec.AuthorPeerID != stranger.peerID {
		t.Fatalf("record author = %q, want stranger's own peer id %q", rec.AuthorPeerID, stranger.peerID)
	}
}

// TestDialSubmitCommandRejectsNonPublicCommand proves the carve-out
// EventDialSubmitCommand rides is exactly as narrow as a local
// SubmitCommand's own IsPermittedForCommand check: a commandID with no
// public group linked to it must be rejected for a stranger caller the
// same way TestCommandLogCarveOutSubmitPublicCommand proves for a bare
// EventLogAppend.
func TestDialSubmitCommandRejectsNonPublicCommand(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	stranger := startExecuteTestNode(t, filepath.Join(t.TempDir(), "stranger"))
	connectPeers(t, ctx, stranger, leader)

	putGroup(t, ctx, leader, "grp-dial-private", "Private Group", false)
	putCommand(t, ctx, leader, "cmd-dial-private", "Shutdown", "some-other-target-peer-id")
	putGroupCommand(t, ctx, leader, "cmd-dial-private", "grp-dial-private")

	target := leader.advertisedAddrs()[0]
	reqPayload, err := shmevent.EncodeDialSubmitCommandRequest(target, "cmd-dial-private", "", "")
	if err != nil {
		t.Fatalf("EncodeDialSubmitCommandRequest: %v", err)
	}

	resp := callLocal(t, ctx, stranger, shmevent.Msg{EventType: shmevent.EventDialSubmitCommand, Value: reqPayload, ID: 1}, stranger.ed25519Priv)
	if resp.EventType != shmevent.EventError {
		t.Fatal("dial_submit_command against a command with no public group unexpectedly succeeded")
	}
}

// TestDialQueryCommandLogNeedsNoStanding proves EventDialQueryCommandLog's
// bearer-token design: a node with zero standing in leader's cluster --
// never granted anything, no relation at all to whatever command produced
// the instance id -- can still dial leader and read back a
// CommandExecLogKind(instanceID) entry, because "possessing the instance id
// is the credential" (see that event's doc comment). The exec-log entry is
// written directly into leader's store here, standing in for what a real
// command dispatcher would have appended after actually executing the
// dispatch -- this test is about the read-side carve-out, not the
// dispatcher.
func TestDialQueryCommandLogNeedsNoStanding(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	reader := startExecuteTestNode(t, filepath.Join(t.TempDir(), "reader"))
	connectPeers(t, ctx, reader, leader)

	instanceID := "dial-query-instance"
	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	key, err := logrecord.BuildKey(shmevent.CommandExecLogKind, instanceID, base, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	value, err := logrecord.Record{
		Kind:      shmevent.CommandExecLogKind,
		UnitID:    instanceID,
		Timestamp: base,
		Narrative: "done",
	}.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	if err := leader.store.Set(key, value); err != nil {
		t.Fatalf("seed exec log entry: %v", err)
	}

	target := leader.advertisedAddrs()[0]
	reqPayload, err := shmevent.EncodeDialQueryCommandLogRequest(target, instanceID, base.Add(-time.Hour), base.Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("EncodeDialQueryCommandLogRequest: %v", err)
	}

	resp := callLocal(t, ctx, reader, shmevent.Msg{EventType: shmevent.EventDialQueryCommandLog, Value: reqPayload, ID: 1}, reader.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("dial_query_command_log was rejected despite no standing being required: %s", resp.Value)
	}
	records, err := shmevent.DecodeLogRecordList(resp.Value)
	if err != nil {
		t.Fatalf("DecodeLogRecordList: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0].UnitID != instanceID {
		t.Fatalf("record unit id = %q, want %q", records[0].UnitID, instanceID)
	}
	if records[0].Narrative != "done" {
		t.Fatalf("record narrative = %q, want %q", records[0].Narrative, "done")
	}
}

// TestDialSubmitCommandRejectsRemoteCaller and
// TestDialQueryCommandLogRejectsRemoteCaller pin EventDialSubmitCommand/
// EventDialQueryCommandLog's local-only gate, mirroring
// TestPublicAccessRejectsRemoteCaller: both make the receiving node act as
// a client under its own identity, so a remote caller triggering either
// would spend that node's own standing on a stranger's behalf.
func TestDialSubmitCommandRejectsRemoteCaller(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	reqPayload, err := shmevent.EncodeDialSubmitCommandRequest("/ip4/127.0.0.1/tcp/1/p2p/"+leader.peerID, "cmd-x", "", "")
	if err != nil {
		t.Fatalf("EncodeDialSubmitCommandRequest: %v", err)
	}
	resp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventDialSubmitCommand,
		Value:     reqPayload,
		ID:        1,
	}, remotePriv)
	if err != nil {
		t.Fatalf("dial_submit_command call: %v", err)
	}
	if resp.EventType != shmevent.EventError {
		t.Fatal("dial_submit_command from a remote caller unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not available to a remote caller") {
		t.Fatalf("dial_submit_command rejected for the wrong reason: %s", resp.Value)
	}
}

func TestDialQueryCommandLogRejectsRemoteCaller(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	reqPayload, err := shmevent.EncodeDialQueryCommandLogRequest("/ip4/127.0.0.1/tcp/1/p2p/"+leader.peerID, "some-instance", time.Time{}, time.Time{}, 0)
	if err != nil {
		t.Fatalf("EncodeDialQueryCommandLogRequest: %v", err)
	}
	resp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventDialQueryCommandLog,
		Value:     reqPayload,
		ID:        1,
	}, remotePriv)
	if err != nil {
		t.Fatalf("dial_query_command_log call: %v", err)
	}
	if resp.EventType != shmevent.EventError {
		t.Fatal("dial_query_command_log from a remote caller unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not available to a remote caller") {
		t.Fatalf("dial_query_command_log rejected for the wrong reason: %s", resp.Value)
	}
}

// TestDialSubmitCommandRejectsSelfDial proves dialAndSubmitCommand's early
// self-dial guard: a node cannot use EventDialSubmitCommand to target
// itself -- it already has every standing in its own cluster, so the
// dial-and-submit carve-out (which exists only to reach a cluster the
// caller has no standing in) is a non sequitur against itself.
func TestDialSubmitCommandRejectsSelfDial(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})

	reqPayload, err := shmevent.EncodeDialSubmitCommandRequest(leader.advertisedAddrs()[0], "cmd-x", "", "")
	if err != nil {
		t.Fatalf("EncodeDialSubmitCommandRequest: %v", err)
	}
	resp := callLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventDialSubmitCommand, Value: reqPayload, ID: 1}, leader.ed25519Priv)
	if resp.EventType != shmevent.EventError {
		t.Fatal("dial_submit_command targeting the caller's own node unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "is this node itself") {
		t.Fatalf("dial_submit_command self-dial rejected for the wrong reason: %s", resp.Value)
	}
}
