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

// TestLifecycleWriteRemoteGateExemption proves EventLifecycleWrite carries
// EventPermitRequest/EventPermitConfirm's own top-level remote-caller
// exemption (see isExemptPermitLifecycleWrite's doc comment) only for the
// exact same cases the old, separate Event bytes were exempt for --
// Permit-style Request/Confirm from a total stranger with no cluster
// standing at all -- while Permit-style Revoke and every JoinInvite/
// ExecInvite action (never exempt, even though JoinInvite's own action is
// also "Request"/Create) still hit the ordinary "not permitted" gate. This
// is the one behavior genuinely new to the envelope (collapsing seven
// Event bytes into one meant the pre-switch gate, which used to switch on
// EventType directly, needed to learn to peek inside the payload) rather
// than a reused existing code path, so it's the one thing this refactor
// could plausibly have gotten wrong silently.
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
	// EventPermitRequest/EventPermitConfirm must stay reachable by.
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

	// Permit-style Request: exempt, and (unlike Confirm/Revoke) carries no
	// voter check either -- a total stranger's request must succeed
	// outright, exactly as a direct EventPermitRequest always has.
	permitReqInner, err := shmevent.EncodePermitRequestPayload(shmevent.KindBootstrapNode, []byte("some-peer"), nil)
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload: %v", err)
	}
	resp := call(shmevent.Msg{
		EventType: shmevent.EventLifecycleWrite,
		Value:     shmevent.EncodeLifecycleWritePayload(shmevent.KindBootstrapNode, shmevent.LifecycleActionRequest, permitReqInner),
		ID:        1,
	})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("permit request (lifecycle_write) from a stranger was rejected: %s", resp.Value)
	}

	// Permit-style Confirm: exempt from the generic gate too, but still
	// voter-gated inside the case body -- a stranger must be rejected for
	// "not a current raft voter", not "not permitted", proving it reached
	// past the gate this test is really about.
	confirmInner := shmevent.EncodePermitConfirmPayload(shmevent.KindBootstrapNode, []byte("some-peer"))
	resp = call(shmevent.Msg{
		EventType: shmevent.EventLifecycleWrite,
		Value:     shmevent.EncodeLifecycleWritePayload(shmevent.KindBootstrapNode, shmevent.LifecycleActionConfirm, confirmInner),
		ID:        2,
	})
	if resp.EventType != shmevent.EventError {
		t.Fatal("permit confirm (lifecycle_write) from a non-voter stranger unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not a current raft voter") {
		t.Fatalf("permit confirm rejected for the wrong reason (expected a voter-check failure, proving it passed the remote gate): %s", resp.Value)
	}

	// Permit-style Revoke: never exempt -- a stranger must be rejected at
	// the generic gate itself ("not permitted"), never reaching the case
	// body's own voter check.
	resp = call(shmevent.Msg{
		EventType: shmevent.EventLifecycleWrite,
		Value:     shmevent.EncodeLifecycleWritePayload(shmevent.KindBootstrapNode, shmevent.LifecycleActionRevoke, confirmInner),
		ID:        3,
	})
	if resp.EventType != shmevent.EventError {
		t.Fatal("permit revoke (lifecycle_write) from a stranger unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not permitted") {
		t.Fatalf("permit revoke rejected for the wrong reason (expected the generic remote-caller gate): %s", resp.Value)
	}

	// JoinInvite Create (action=Request, but kind=KindJoinInvite): never
	// exempt, even though its action byte is the same LifecycleActionRequest
	// Permit-style Request uses -- the exemption is specific to Permit-style
	// kinds, not to the action label.
	joinInviteInner, err := shmevent.EncodeJoinInviteCreatePayload(make([]byte, shmevent.JoinInviteTokenSize), shmevent.SuffrageVoter)
	if err != nil {
		t.Fatalf("EncodeJoinInviteCreatePayload: %v", err)
	}
	resp = call(shmevent.Msg{
		EventType: shmevent.EventLifecycleWrite,
		Value:     shmevent.EncodeLifecycleWritePayload(shmevent.KindJoinInvite, shmevent.LifecycleActionRequest, joinInviteInner),
		ID:        4,
	})
	if resp.EventType != shmevent.EventError {
		t.Fatal("join invite create (lifecycle_write) from a stranger unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not permitted") {
		t.Fatalf("join invite create rejected for the wrong reason (expected the generic remote-caller gate): %s", resp.Value)
	}
}

// TestLifecycleWriteUnrecognizedActionRejected proves an invalid (kind,
// action) combination -- one lookupLifecycleWriteSpec has no table entry
// for, e.g. Confirm against KindJoinInvite, which has no pending/confirmed
// stage -- is rejected outright rather than silently misrouted.
func TestLifecycleWriteUnrecognizedActionRejected(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "n.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	n, err := start(Config{DataDir: filepath.Join(tmpDir, "n"), KeyPath: key})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer n.shutdown()
	if _, err := n.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	buf, err := shmevent.Encode(shmevent.Msg{
		EventType: shmevent.EventLifecycleWrite,
		Value:     shmevent.EncodeLifecycleWritePayload(shmevent.KindJoinInvite, shmevent.LifecycleActionConfirm, nil),
		ID:        1,
	}, n.ed25519Priv)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp := n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
	if resp.EventType != shmevent.EventError {
		t.Fatal("lifecycle_write with an unrecognized (kind, action) pair unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not valid for entity kind") {
		t.Fatalf("rejected for the wrong reason: %s", resp.Value)
	}
}
