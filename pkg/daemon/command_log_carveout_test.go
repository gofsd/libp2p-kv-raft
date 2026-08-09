package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// remoteAppend builds a pkg/logrecord key/value for kind/unitID/ts and
// sends it as EventLogAppend from remote, exactly the way a genuine
// non-local caller would over ClientProtocolID.
func remoteAppend(t *testing.T, ctx context.Context, remote lp2phost.Host, remotePriv shmevent.PrivateKey, target peer.ID, kind, unitID string, ts time.Time) shmevent.Msg {
	t.Helper()
	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	rec := logrecord.Record{Kind: kind, UnitID: unitID, Timestamp: ts, Narrative: "x"}
	value, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	m, err := shmevent.NewLogAppend(key, value)
	if err != nil {
		t.Fatalf("NewLogAppend: %v", err)
	}
	m.SetId(1)
	resp, err := callClientProtocol(ctx, remote, target, m, remotePriv)
	if err != nil {
		t.Fatalf("log_append: %v", err)
	}
	return resp
}

// remoteQuery drives listRange once (not in a loop -- these tests only
// need to know whether the first match is granted or rejected) from remote
// against target for [start, end].
func remoteQuery(t *testing.T, ctx context.Context, remote lp2phost.Host, remotePriv shmevent.PrivateKey, target peer.ID, start, end []byte) shmevent.Msg {
	t.Helper()
	m, err := shmevent.NewListRange(start, end)
	if err != nil {
		t.Fatalf("NewListRange: %v", err)
	}
	m.SetId(2)
	resp, err := callClientProtocol(ctx, remote, target, m, remotePriv)
	if err != nil {
		t.Fatalf("list_range: %v", err)
	}
	return resp
}

// putGroup/putCommand/putGroupCommand apply the group-based ACL catalog's
// Put locally against leader (its own sole-voter standing needs no remote
// identity check -- see groupPut/commandPut/groupCommandPut's cases in
// handleShmEvent), building the fixtures TestCommandLogCarveOut* below
// submit/read against.
func putGroup(t *testing.T, ctx context.Context, leader *Node, id, name string, public bool) {
	t.Helper()
	m, err := shmevent.NewGroupPut(id, name, public)
	if err != nil {
		t.Fatalf("NewGroupPut: %v", err)
	}
	resp := callLocal(t, ctx, leader, m, leader.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("group_put %q rejected: %s", id, mustErrMessage(t, resp))
	}
}

func putCommand(t *testing.T, ctx context.Context, leader *Node, id, name, peerID string) {
	t.Helper()
	m, err := shmevent.NewCommandPut(id, name, peerID)
	if err != nil {
		t.Fatalf("NewCommandPut: %v", err)
	}
	resp := callLocal(t, ctx, leader, m, leader.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("command_put %q rejected: %s", id, mustErrMessage(t, resp))
	}
}

func putGroupCommand(t *testing.T, ctx context.Context, leader *Node, commandID, groupID string) {
	t.Helper()
	m, err := shmevent.NewGroupCommandPut(commandID, groupID)
	if err != nil {
		t.Fatalf("NewGroupCommandPut: %v", err)
	}
	resp := callLocal(t, ctx, leader, m, leader.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("group_command_put (%q, %q) rejected: %s", commandID, groupID, mustErrMessage(t, resp))
	}
}

// TestCommandLogCarveOutSubmitPublicCommand proves isCommandLogCarveOut's
// write-side exception: a totally unknown remote caller (no cluster
// membership, no "remote" group grant) can submit a command linked to a
// public group (SubmitCommand's actual write, EventLogAppend targeting a
// shmevent.CommandRequestLogKind), but the identical submission against a
// command with no public group at all is still rejected -- the narrow
// carve-out only ever reaches as far as kvfsm.OpAppendCommandRequest's own
// raft-authoritative IsPermittedForCommand check already reached before
// this session's changes; it doesn't loosen that check itself.
func TestCommandLogCarveOutSubmitPublicCommand(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	putGroup(t, ctx, leader, "grp-public", "Public Group", true)
	putGroup(t, ctx, leader, "grp-private", "Private Group", false)
	putCommand(t, ctx, leader, "cmd-public", "Reboot", leader.peerID)
	putCommand(t, ctx, leader, "cmd-private", "Shutdown", "some-other-target-peer-id")
	putGroupCommand(t, ctx, leader, "cmd-public", "grp-public")
	putGroupCommand(t, ctx, leader, "cmd-private", "grp-private")

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	instanceID := "inst-1"

	publicResp := remoteAppend(t, ctx, remote, remotePriv, leaderPeerID, shmevent.CommandRequestLogKind("cmd-public"), instanceID, base)
	if publicResp.Which() == shmevent.Event_Which_error {
		t.Fatalf("submitting a command linked to a public group was rejected: %s", mustErrMessage(t, publicResp))
	}

	privateResp := remoteAppend(t, ctx, remote, remotePriv, leaderPeerID, shmevent.CommandRequestLogKind("cmd-private"), instanceID, base)
	if privateResp.Which() != shmevent.Event_Which_error {
		t.Fatal("submitting a command with no public group unexpectedly succeeded for an unauthorized remote caller")
	}
}

// TestCommandLogCarveOutReadOwnRecords proves isCommandLogCarveOut's
// read-side exceptions: an unauthorized remote caller may read back a
// public command's own CommandRequestLogKind queue (the same
// IsPermittedForCommand standing that let it submit in the first place),
// its own CommandExecIndexKind entry (self-scoped by peer id, regardless
// of command standing), and shmevent.CommandExecLogKind (bearer-token
// design, any instance id) -- but not a non-public command's queue or
// another peer's exec index.
func TestCommandLogCarveOutReadOwnRecords(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)
	remoteIDStr := remote.ID().String()

	putGroup(t, ctx, leader, "grp-public", "Public Group", true)
	putGroup(t, ctx, leader, "grp-private", "Private Group", false)
	putCommand(t, ctx, leader, "cmd-public", "Reboot", leader.peerID)
	putCommand(t, ctx, leader, "cmd-private", "Shutdown", "some-other-target-peer-id")
	putGroupCommand(t, ctx, leader, "cmd-public", "grp-public")
	putGroupCommand(t, ctx, leader, "cmd-private", "grp-private")

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)

	// A public command's own request queue is readable.
	lo, hi := logrecord.ScanBounds(shmevent.CommandRequestLogKind("cmd-public"), "inst-1", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo, hi); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("reading a public command's own request queue was rejected: %s", mustErrMessage(t, resp))
	}

	// A non-public command's request queue is not.
	lo2, hi2 := logrecord.ScanBounds(shmevent.CommandRequestLogKind("cmd-private"), "inst-1", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo2, hi2); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("reading a non-public command's request queue unexpectedly succeeded")
	}

	// The caller's own execution index is readable, regardless of any
	// command standing at all.
	loSelf, hiSelf := logrecord.ScanBounds(shmevent.CommandExecIndexKind(remoteIDStr), "inst-1", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, loSelf, hiSelf); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("reading the caller's own execution index was rejected: %s", mustErrMessage(t, resp))
	}

	// Another peer's execution index is not.
	loOther, hiOther := logrecord.ScanBounds(shmevent.CommandExecIndexKind("some-other-peer-id"), "inst-1", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, loOther, hiOther); resp.Which() != shmevent.Event_Which_error {
		t.Fatal("reading another peer's execution index unexpectedly succeeded")
	}

	// shmevent.CommandExecLogKind is readable for any instance id at all --
	// "possessing the instance id is the credential" (see pkg/kvctl's
	// GetCommandRequest doc comment), unrelated to any command/group
	// standing.
	loLog, hiLog := logrecord.ScanBounds(shmevent.CommandExecLogKind, "some-random-instance-id", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, loLog, hiLog); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("reading shmevent.CommandExecLogKind was rejected: %s", mustErrMessage(t, resp))
	}
}

// TestCommandLogCarveOutRejectsOrdinaryLogKind proves an unauthorized
// remote caller gets no access at all to an ordinary (non-command) log
// kind, on both EventLogAppend and EventListRange -- the command/log
// carve-out is deliberately narrow, not a general log-kind permit system
// (that entire mechanism, KindLogPermit/Config.RequirePermitForLog, was
// removed in favor of this narrower, always-on exception).
func TestCommandLogCarveOutRejectsOrdinaryLogKind(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	resp := remoteAppend(t, ctx, remote, remotePriv, leaderPeerID, "sitrep", "1BCT", base)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("log_append of an ordinary log kind from an unauthorized remote peer unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "not permitted") {
		t.Fatalf("log_append rejection = %q, want it to mention not being permitted", mustErrMessage(t, resp))
	}

	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	qresp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo, hi)
	if qresp.Which() != shmevent.Event_Which_error {
		t.Fatal("list_range of an ordinary log kind from an unauthorized remote peer unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, qresp), "not permitted") {
		t.Fatalf("list_range rejection = %q, want it to mention not being permitted", mustErrMessage(t, qresp))
	}
}

// TestCommandLogCarveOutRangeSingleKindOnly proves the read-side carve-out
// requires both the start and end of a ListRange query to resolve to the
// identical kind: a query whose start lands in a permitted command's
// CommandRequestLogKind range but whose end reaches into an unrelated
// kind's range must be rejected outright, not silently answered -- a raw
// byte range has no logical confinement to one kind just because start
// happens to be inside it.
func TestCommandLogCarveOutRangeSingleKindOnly(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	putGroup(t, ctx, leader, "grp-public", "Public Group", true)
	putCommand(t, ctx, leader, "cmd-public", "Reboot", leader.peerID)
	putGroupCommand(t, ctx, leader, "cmd-public", "grp-public")

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	lo, _ := logrecord.ScanBounds(shmevent.CommandRequestLogKind("cmd-public"), "inst-1", base.Add(-time.Hour), base.Add(time.Hour))
	_, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))

	resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo, hi)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("list_range with start in a permitted command's queue but end reaching into an unrelated kind unexpectedly succeeded")
	}
}

// TestLogAccessUnconditionalForClusterMembers proves a current raft
// cluster member gets full log append/query access with no group grant
// and no command/public-group standing at all -- "all other events by
// default allowed to any raft cluster members" applies to the log
// namespace exactly like every other event this package gates.
func TestLogAccessUnconditionalForClusterMembers(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	// Grant remote "cluster" group membership directly -- the same record
	// syncMemberGroups writes on a real join, without needing a full raft
	// join round trip for this test.
	remoteID, err := peer.Decode(remote.ID().String())
	if err != nil {
		t.Fatalf("decode remote peer id: %v", err)
	}
	clusterGroupKey, err := shmevent.PeerGroupKey([]byte(remoteID.String()), []byte(shmevent.ReservedGroupCluster))
	if err != nil {
		t.Fatalf("PeerGroupKey: %v", err)
	}
	if err := leader.store.Set(clusterGroupKey, nil); err != nil {
		t.Fatalf("grant cluster group: %v", err)
	}

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	if resp := remoteAppend(t, ctx, remote, remotePriv, leaderPeerID, "sitrep", "1BCT", base); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("log_append rejected for a cluster member: %s", mustErrMessage(t, resp))
	}

	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo, hi); resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("list_range rejected for a cluster member: %s", mustErrMessage(t, resp))
	}
}

// TestLogAccessLocalCallerNeverGated proves a local caller (no
// caller.remotePeer at all) is never restricted by the command/log
// carve-out or the "remote" group gate, regardless of raft membership or
// any group grant -- mirrors the same guarantee TestRemoteGateUnconditional
// documents for the general remote gate.
func TestLogAccessLocalCallerNeverGated(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{})

	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	key, err := logrecord.BuildKey("sitrep", "1BCT", base, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	rec := logrecord.Record{Kind: "sitrep", UnitID: "1BCT", Timestamp: base, Narrative: "x"}
	value, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	appendMsg, err := shmevent.NewLogAppend(key, value)
	if err != nil {
		t.Fatalf("NewLogAppend: %v", err)
	}
	appendMsg.SetId(1)

	resp := callLocal(t, ctx, leader, appendMsg, leader.ed25519Priv)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("local log_append rejected despite no group standing: %s", mustErrMessage(t, resp))
	}

	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	queryMsg, err := shmevent.NewListRange(lo, hi)
	if err != nil {
		t.Fatalf("NewListRange: %v", err)
	}
	queryMsg.SetId(2)
	qresp := callLocal(t, ctx, leader, queryMsg, leader.ed25519Priv)
	if qresp.Which() == shmevent.Event_Which_error {
		t.Fatalf("local list_range rejected despite no group standing: %s", mustErrMessage(t, qresp))
	}
	qrespKey, err := qresp.ListRange().Key()
	if err != nil {
		t.Fatalf("ListRange key: %v", err)
	}
	if len(qrespKey) == 0 {
		t.Fatal("local list_range found nothing -- append above must not have landed")
	}
}
