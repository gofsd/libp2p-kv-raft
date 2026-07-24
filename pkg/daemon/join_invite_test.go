package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

func newInviteToken(t *testing.T) []byte {
	t.Helper()
	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

// TestJoinInviteAdmitsBrandNewNodeWithoutConfirmation is this feature's
// central claim, exercised against a real cluster (no mocks): a leader
// with -require-confirm-for-join *on* still admits a device it has never
// seen before, immediately, the moment that device's join request
// presents a still-valid invite token -- no live voter runs confirmpermit
// at that moment, unlike every other KindClusterJoin request. Also proves
// the token is genuinely one-time: a second node presenting the identical
// token afterward is rejected outright, not silently downgraded to the
// slower pending-confirm path.
func TestJoinInviteAdmitsBrandNewNodeWithoutConfirmation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:      200 * time.Millisecond,
		ElectionTimeout:       200 * time.Millisecond,
		CommitTimeout:         20 * time.Millisecond,
		LeaderLeaseTimeout:    100 * time.Millisecond,
		RequireConfirmForJoin: true,
	}

	startNode := func(name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg := fastRaft
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	leader := startNode("leader")
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, n.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
	}

	token := newInviteToken(t)
	createPayload, err := shmevent.EncodeJoinInviteCreatePayload(token, shmevent.SuffrageVoter)
	if err != nil {
		t.Fatalf("EncodeJoinInviteCreatePayload: %v", err)
	}
	resp := call(leader, shmevent.Msg{EventType: shmevent.EventJoinInviteCreate, Value: createPayload, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("join_invite_create rejected: %s", resp.Value)
	}

	newcomer := startNode("newcomer")
	defer newcomer.shutdown()
	if _, err := newcomer.initRaft(); err != nil {
		t.Fatalf("init newcomer raft: %v", err)
	}

	tokenHex := hex.EncodeToString(token)
	status, err := newcomer.handleAdd(ctx, leaderAddr+"#"+tokenHex)
	if err != nil {
		t.Fatalf("newcomer join with invite: %v", err)
	}
	// handleAdd returns "<peerID> ok" (admitted immediately) or
	// "<peerID> pending" -- with RequireConfirmForJoin true and no
	// invite, this would end in "pending"; the whole point of a valid
	// invite is that it ends in "ok" instead, with no separate confirm
	// ever happening.
	if !strings.HasSuffix(status, " ok") {
		t.Fatalf("got join status %q, want it to end in %q (invite should bypass RequireConfirmForJoin)", status, " ok")
	}

	cfgFuture := leader.getRaft().GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get leader configuration: %v", err)
	}
	var found bool
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(newcomer.peerID) {
			found = true
			if srv.Suffrage != raft.Voter {
				t.Fatalf("newcomer joined with suffrage %v, want Voter", srv.Suffrage)
			}
		}
	}
	if !found {
		t.Fatal("newcomer not present in leader's raft configuration after invite-based join")
	}

	// The token must now be consumed: a second, different node presenting
	// the identical token must be rejected outright.
	secondComer := startNode("second-comer")
	defer secondComer.shutdown()
	if _, err := secondComer.initRaft(); err != nil {
		t.Fatalf("init second-comer raft: %v", err)
	}
	_, err = secondComer.handleAdd(ctx, leaderAddr+"#"+tokenHex)
	if err == nil {
		t.Fatal("second node redeeming the already-consumed invite token unexpectedly succeeded")
	}
}

// TestJoinInviteRevokeInvalidatesBeforeRedemption proves
// EventJoinInviteRevoke actually takes effect: a revoked token must be
// rejected exactly like one that was never created.
func TestJoinInviteRevokeInvalidatesBeforeRedemption(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:      200 * time.Millisecond,
		ElectionTimeout:       200 * time.Millisecond,
		CommitTimeout:         20 * time.Millisecond,
		LeaderLeaseTimeout:    100 * time.Millisecond,
		RequireConfirmForJoin: true,
	}

	key := filepath.Join(tmpDir, "leader.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate leader key: %v", err)
	}
	cfg := fastRaft
	cfg.DataDir = filepath.Join(tmpDir, "leader")
	cfg.KeyPath = key
	leader, err := start(cfg)
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, n.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
	}

	token := newInviteToken(t)
	createPayload, err := shmevent.EncodeJoinInviteCreatePayload(token, shmevent.SuffrageLearner)
	if err != nil {
		t.Fatalf("EncodeJoinInviteCreatePayload: %v", err)
	}
	if resp := call(leader, shmevent.Msg{EventType: shmevent.EventJoinInviteCreate, Value: createPayload, ID: 1}); resp.EventType == shmevent.EventError {
		t.Fatalf("join_invite_create rejected: %s", resp.Value)
	}

	revokePayload := shmevent.EncodeJoinInviteRevokePayload(token)
	if resp := call(leader, shmevent.Msg{EventType: shmevent.EventJoinInviteRevoke, Value: revokePayload, ID: 2}); resp.EventType == shmevent.EventError {
		t.Fatalf("join_invite_revoke rejected: %s", resp.Value)
	}

	newcomerKey := filepath.Join(tmpDir, "newcomer.key")
	if _, err := p2praft.LoadOrGenerateKey(newcomerKey); err != nil {
		t.Fatalf("generate newcomer key: %v", err)
	}
	newcomerCfg := fastRaft
	newcomerCfg.DataDir = filepath.Join(tmpDir, "newcomer")
	newcomerCfg.KeyPath = newcomerKey
	newcomer, err := start(newcomerCfg)
	if err != nil {
		t.Fatalf("start newcomer: %v", err)
	}
	defer newcomer.shutdown()
	if _, err := newcomer.initRaft(); err != nil {
		t.Fatalf("init newcomer raft: %v", err)
	}

	tokenHex := hex.EncodeToString(token)
	if _, err := newcomer.handleAdd(ctx, leaderAddr+"#"+tokenHex); err == nil {
		t.Fatal("join with a revoked invite token unexpectedly succeeded")
	}
}

// TestJoinInviteUnknownTokenRejected checks a token that was never
// created at all is rejected the same way a consumed/revoked one is, not
// silently treated as "no token supplied" and downgraded to the pending
// path.
func TestJoinInviteUnknownTokenRejected(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:      200 * time.Millisecond,
		ElectionTimeout:       200 * time.Millisecond,
		CommitTimeout:         20 * time.Millisecond,
		LeaderLeaseTimeout:    100 * time.Millisecond,
		RequireConfirmForJoin: true,
	}

	key := filepath.Join(tmpDir, "leader.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate leader key: %v", err)
	}
	cfg := fastRaft
	cfg.DataDir = filepath.Join(tmpDir, "leader")
	cfg.KeyPath = key
	leader, err := start(cfg)
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	newcomerKey := filepath.Join(tmpDir, "newcomer.key")
	if _, err := p2praft.LoadOrGenerateKey(newcomerKey); err != nil {
		t.Fatalf("generate newcomer key: %v", err)
	}
	newcomerCfg := fastRaft
	newcomerCfg.DataDir = filepath.Join(tmpDir, "newcomer")
	newcomerCfg.KeyPath = newcomerKey
	newcomer, err := start(newcomerCfg)
	if err != nil {
		t.Fatalf("start newcomer: %v", err)
	}
	defer newcomer.shutdown()
	if _, err := newcomer.initRaft(); err != nil {
		t.Fatalf("init newcomer raft: %v", err)
	}

	tokenHex := hex.EncodeToString(newInviteToken(t))
	if _, err := newcomer.handleAdd(ctx, leaderAddr+"#"+tokenHex); err == nil {
		t.Fatal("join with a never-created invite token unexpectedly succeeded")
	}
}
