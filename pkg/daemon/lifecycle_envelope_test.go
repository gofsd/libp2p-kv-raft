package daemon

import (
	"context"
	"crypto/ed25519"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestLifecycleWriteRemoteGateExemption used to prove EventLifecycleWrite
// carried EventPermitRequest/EventPermitConfirm's own top-level
// remote-caller exemption only for the exact same cases the old, separate
// Event bytes were exempt for -- Permit-style Request/Confirm from a total
// stranger with no cluster standing at all -- while Permit-style Revoke and
// every JoinInvite/ExecInvite action still hit the ordinary "not permitted"
// gate. The generic EventLifecycleWrite envelope (one Event byte carrying a
// kind+action pair, dispatched via lookupLifecycleWriteSpec/
// isExemptPermitLifecycleWrite) is gone in the capnp rewrite -- permitRequest/
// permitConfirm/permitRevoke/joinInviteCreate are each their own top-level
// union variant now, and handleShmEvent's top-of-function remote-caller gate
// switches on m.Which() directly instead of peeking inside a payload. The
// underlying security behavior this test actually cares about --exactly
// which variants are exempt from the generic gate-- is identical and fully
// exercisable through those variants directly, so this translates cleanly.
func TestLifecycleWriteRemoteGateExemption(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "n.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	n, err := start(Config{
		DataDir:            filepath.Join(tmpDir, "n"),
		KeyPath:            key,
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer n.shutdown()
	if _, err := n.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// A total stranger: a fresh keypair with no join, no permit, no group
	// standing of any kind on n -- the exact class of caller
	// permitRequest/permitConfirm must stay reachable by.
	strangerPub, strangerPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate stranger key: %v", err)
	}
	strangerCaller := callerIdentity{remotePeer: peer.ID("stranger-peer-id"), verifyPub: strangerPub}

	call := func(m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, strangerPriv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return n.handleShmEvent(ctx, decoded, crc, sig, strangerCaller)
	}

	// permitRequest: exempt, and (unlike Confirm/Revoke) carries no voter
	// check either -- a total stranger's request must succeed outright.
	reqMsg, err := shmevent.NewPermitRequest(shmevent.KindBootstrapNode, "some-peer", "")
	if err != nil {
		t.Fatalf("NewPermitRequest: %v", err)
	}
	reqMsg.SetId(1)
	resp := call(reqMsg)
	if resp.Which() == shmevent.Event_Which_error {
		t.Fatalf("permit_request from a stranger was rejected: %s", mustErrMessage(t, resp))
	}

	// permitConfirm: exempt from the generic gate too, but still
	// voter-gated inside the case body -- a stranger must be rejected for
	// "not a current raft voter", not "not permitted", proving it reached
	// past the gate this test is really about.
	confirmMsg, err := shmevent.NewPermitConfirm(shmevent.KindBootstrapNode, "some-peer")
	if err != nil {
		t.Fatalf("NewPermitConfirm: %v", err)
	}
	confirmMsg.SetId(2)
	resp = call(confirmMsg)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("permit_confirm from a non-voter stranger unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "not a current raft voter") {
		t.Fatalf("permit_confirm rejected for the wrong reason (expected a voter-check failure, proving it passed the remote gate): %s", mustErrMessage(t, resp))
	}

	// permitRevoke: never exempt -- a stranger must be rejected at the
	// generic gate itself ("not permitted"), never reaching the case
	// body's own voter check.
	revokeMsg, err := shmevent.NewPermitRevoke(shmevent.KindBootstrapNode, "some-peer")
	if err != nil {
		t.Fatalf("NewPermitRevoke: %v", err)
	}
	revokeMsg.SetId(3)
	resp = call(revokeMsg)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("permit_revoke from a stranger unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "not permitted") {
		t.Fatalf("permit_revoke rejected for the wrong reason (expected the generic remote-caller gate): %s", mustErrMessage(t, resp))
	}

	// joinInviteCreate: never exempt, even though it shares the same
	// "create/request" shape permitRequest has -- the exemption is specific
	// to the permitRequest/permitConfirm variants, not to any general
	// "create" label.
	joinInviteMsg, err := shmevent.NewJoinInviteCreate(make([]byte, shmevent.JoinInviteTokenSize), shmevent.SuffrageVoter)
	if err != nil {
		t.Fatalf("NewJoinInviteCreate: %v", err)
	}
	joinInviteMsg.SetId(4)
	resp = call(joinInviteMsg)
	if resp.Which() != shmevent.Event_Which_error {
		t.Fatal("join_invite_create from a stranger unexpectedly succeeded")
	}
	if !strings.Contains(mustErrMessage(t, resp), "not permitted") {
		t.Fatalf("join_invite_create rejected for the wrong reason (expected the generic remote-caller gate): %s", mustErrMessage(t, resp))
	}
}

// TestLifecycleWriteUnrecognizedActionRejected used to prove an invalid
// (kind, action) combination fed into the generic EventLifecycleWrite
// envelope -- one lookupLifecycleWriteSpec had no table entry for, e.g.
// Confirm against KindJoinInvite, which has no pending/confirmed stage --
// was rejected outright rather than silently misrouted. That envelope, and
// the free-standing (kind, action) pair it validated at dispatch time, no
// longer exist: joinInviteCreate/joinInviteRevoke (and every other
// lifecycle variant) are now fixed top-level union variants, each with
// exactly one behavior baked in by the capnp schema itself -- there is no
// longer any way to *construct* an "unrecognized (kind, action)" message at
// all, so this test has no surviving translation. The type system now
// provides, at compile/decode time, the guarantee this test used to check
// at runtime.
func TestLifecycleWriteUnrecognizedActionRejected(t *testing.T) {
	t.Skip("no longer applicable: the generic (kind, action) envelope this test validated no longer exists -- see this test's doc comment")
}
