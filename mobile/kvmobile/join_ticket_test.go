package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// buildJoinTicket signs sourceAddr+token as eventType (EventJoinTicket or
// EventJoinRequestTicket -- both share EncodeJoinTicketPayload's wire
// shape, see that function's doc comment) with priv, exactly the way
// CreateJoinInviteTicket/CreateJoinRequestTicket do internally. Needed the
// same way buildExecInviteTicket is: to mint a ticket "as" a specific
// device other than whichever one is currently started via kvmobile in
// this process (kvmobile runs exactly one daemon per process).
func buildJoinTicket(t *testing.T, eventType uint8, priv shmevent.PrivateKey, sourceAddr string, token []byte) string {
	t.Helper()
	value, err := shmevent.EncodeJoinTicketPayload(sourceAddr, token)
	if err != nil {
		t.Fatalf("EncodeJoinTicketPayload: %v", err)
	}
	wire, err := shmevent.Encode(shmevent.Msg{EventType: eventType, Value: value}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return base64.StdEncoding.EncodeToString(wire)
}

// spawnTestPendingDevice starts a real pkg/daemon node in dataDir and
// registers it, exactly like spawnTestLeader, but deliberately skips the
// bootstrap shmclient.Add call -- the device is left with no raft
// instance at all, exactly AddPending/StartPending's state, the
// prerequisite a join-request ticket's own device needs to be in. Unlike
// spawnTestLeader, this can't return a cluster multiaddr (there's no
// cluster yet); callers that need this device's own dialable address use
// shmclient.GetOwnAddr against the returned peerID instead.
func spawnTestPendingDevice(t *testing.T, dataDir string) (peerID string) {
	t.Helper()
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	keyPath := filepath.Join(dataDir, "identity.key")
	if err := e2edata.WriteDesktopKeyFile(e2edata.Node{PrivateKey: hex.EncodeToString(priv)}, keyPath); err != nil {
		t.Fatalf("WriteDesktopKeyFile: %v", err)
	}
	peerID, err = e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}

	ctx := context.Background()
	go func() {
		_ = daemon.Run(ctx, daemon.Config{
			DataDir:            dataDir,
			KeyPath:            keyPath,
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
		})
	}()

	deadline := time.Now().Add(10 * time.Second)
	var ready daemon.ReadyInfo
	for time.Now().Before(deadline) {
		if info, err := daemon.ReadReadyFile(dataDir); err == nil {
			ready = info
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ready.PeerID == "" {
		t.Fatal("pending daemon never became ready")
	}

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	if err := reg.Put(registry.NodeInfo{PeerID: peerID, DataDir: dataDir}); err != nil {
		t.Fatalf("registry.Put: %v", err)
	}
	return peerID
}

// TestCreateJoinInviteTicketIsVerifiable mirrors
// TestCreateExecInviteTicketIsVerifiable for the join-invite path: drives
// CreateJoinInviteTicket through kvmobile's own gomobile-bindable API (on
// a device that joined a real leader as a full voter -- see
// TestCreateExecInviteRevokeExecInvite's own comment on why that's
// already voter-permitted), then independently decodes and verifies the
// result the way a recipient would.
func TestCreateJoinInviteTicketIsVerifiable(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	selfPeerID := PeerID()

	ticketB64, err := CreateJoinInviteTicket("voter")
	if err != nil {
		t.Fatalf("CreateJoinInviteTicket: %v", err)
	}
	if ticketB64 == "" {
		t.Fatal("CreateJoinInviteTicket returned an empty ticket")
	}

	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		t.Fatalf("decode ticket base64: %v", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		t.Fatalf("shmevent.Decode: %v", err)
	}
	if m.EventType != shmevent.EventJoinTicket {
		t.Fatalf("got event type %d, want %d", m.EventType, shmevent.EventJoinTicket)
	}
	addr, token, err := shmevent.DecodeJoinTicketPayload(m.Value)
	if err != nil {
		t.Fatalf("DecodeJoinTicketPayload: %v", err)
	}
	if len(token) != shmevent.JoinInviteTokenSize {
		t.Fatalf("got token length %d, want %d", len(token), shmevent.JoinInviteTokenSize)
	}

	issuerID, err := relayNodePeerID(addr)
	if err != nil {
		t.Fatalf("relayNodePeerID(%q): %v", addr, err)
	}
	if issuerID.String() != selfPeerID {
		t.Fatalf("ticket's embedded address names peer %s, want this device's own %s", issuerID, selfPeerID)
	}
	issuerPub, err := issuerID.ExtractPublicKey()
	if err != nil {
		t.Fatalf("ExtractPublicKey: %v", err)
	}
	issuerPubRaw, err := issuerPub.Raw()
	if err != nil {
		t.Fatalf("public key Raw: %v", err)
	}
	if err := shmevent.Verify(issuerPubRaw, m, crc, sig); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// TestVerifyJoinInviteTicketThenJoinSucceeds is
// TestJoinRedeemsInviteTicketWithToken's signed-ticket counterpart: a
// ticket signed by the leader's own key is verified (proving
// VerifyJoinInviteTicket's signature check accepts a genuine ticket and
// returns the right plain string), then that string is fed to the real
// Join exactly the way the plain-ticket test already proves works end to
// end -- confirming the ticket path and the plain path converge on the
// identical redemption mechanics.
func TestVerifyJoinInviteTicketThenJoinSucceeds(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leaderSess, err := shmclient.Open(ctx, leaderPeerID)
	if err != nil {
		t.Fatalf("shmclient.Open(leader): %v", err)
	}
	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		t.Fatalf("generate invite token: %v", err)
	}
	if err := leaderSess.CreateJoinInvite(ctx, token, shmevent.SuffrageVoter); err != nil {
		t.Fatalf("CreateJoinInvite: %v", err)
	}
	leaderPriv, err := shmclient.GetPrivateKey(ctx, leaderPeerID)
	if err != nil {
		t.Fatalf("GetPrivateKey(leader): %v", err)
	}
	ticket := buildJoinTicket(t, shmevent.EventJoinTicket, leaderPriv, leaderAddr, token)

	addrAndToken, err := VerifyJoinInviteTicket(ticket)
	if err != nil {
		t.Fatalf("VerifyJoinInviteTicket: %v", err)
	}
	if want := leaderAddr + "#" + hex.EncodeToString(token); addrAndToken != want {
		t.Fatalf("VerifyJoinInviteTicket returned %q, want %q", addrAndToken, want)
	}

	t.Cleanup(func() { _ = Stop() })
	id, err := Join(t.TempDir(), addrAndToken)
	if err != nil {
		t.Fatalf("Join(verified ticket): %v", err)
	}

	memberKey := string(shmevent.ClusterMemberKey([]byte(id)))
	if _, err := shmclient.Get(ctx, leaderPeerID, memberKey); err != nil {
		t.Fatalf("follower not a cluster member after Join(verified ticket): %v", err)
	}

	if err := Submit("k", "v"); err != nil {
		t.Fatalf("Submit after Join: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := Get("k")
		if err == nil && got == "v" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Get after Join never observed replicated value: err=%v value=%q", err, got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestVerifyJoinInviteTicketWithForgedSignatureFails isolates the new
// property a signed join-invite ticket adds: a ticket claiming to be from
// the leader but actually signed by a different key must be rejected by
// VerifyJoinInviteTicket itself, before any Join attempt is even
// possible.
func TestVerifyJoinInviteTicketWithForgedSignatureFails(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leaderSess, err := shmclient.Open(ctx, leaderPeerID)
	if err != nil {
		t.Fatalf("shmclient.Open(leader): %v", err)
	}
	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		t.Fatalf("generate invite token: %v", err)
	}
	if err := leaderSess.CreateJoinInvite(ctx, token, shmevent.SuffrageVoter); err != nil {
		t.Fatalf("CreateJoinInvite: %v", err)
	}

	// Sign with some unrelated key instead of the leader's -- structurally
	// valid, just not something the leader ever produced.
	_, forgerPriv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	forgedTicket := buildJoinTicket(t, shmevent.EventJoinTicket, forgerPriv, leaderAddr, token)

	if _, err := VerifyJoinInviteTicket(forgedTicket); err == nil {
		t.Fatal("VerifyJoinInviteTicket with a forged signature unexpectedly succeeded")
	}
}

// TestCreateJoinRequestTicketIsVerifiable mirrors
// TestCreateJoinInviteTicketIsVerifiable for the reverse-invite path:
// drives CreateJoinRequestTicket through kvmobile's own API on a
// StartPending device (never joined, never bootstrapped -- exactly the
// state a real join-request device is in), then independently verifies
// the result -- proving the signature really is this pending device's
// own, the opposite direction of trust from join-invite (see
// shmevent.EventJoinRequestTicket's doc comment).
func TestCreateJoinRequestTicketIsVerifiable(t *testing.T) {
	t.Cleanup(func() {
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	selfPeerID, err := StartPending(t.TempDir())
	if err != nil {
		t.Fatalf("StartPending: %v", err)
	}

	ticketB64, err := CreateJoinRequestTicket()
	if err != nil {
		t.Fatalf("CreateJoinRequestTicket: %v", err)
	}
	if ticketB64 == "" {
		t.Fatal("CreateJoinRequestTicket returned an empty ticket")
	}

	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		t.Fatalf("decode ticket base64: %v", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		t.Fatalf("shmevent.Decode: %v", err)
	}
	if m.EventType != shmevent.EventJoinRequestTicket {
		t.Fatalf("got event type %d, want %d", m.EventType, shmevent.EventJoinRequestTicket)
	}
	addr, token, err := shmevent.DecodeJoinTicketPayload(m.Value)
	if err != nil {
		t.Fatalf("DecodeJoinTicketPayload: %v", err)
	}
	if len(token) != shmevent.JoinInviteTokenSize {
		t.Fatalf("got token length %d, want %d", len(token), shmevent.JoinInviteTokenSize)
	}

	issuerID, err := relayNodePeerID(addr)
	if err != nil {
		t.Fatalf("relayNodePeerID(%q): %v", addr, err)
	}
	if issuerID.String() != selfPeerID {
		t.Fatalf("ticket's embedded address names peer %s, want this device's own %s", issuerID, selfPeerID)
	}
	issuerPub, err := issuerID.ExtractPublicKey()
	if err != nil {
		t.Fatalf("ExtractPublicKey: %v", err)
	}
	issuerPubRaw, err := issuerPub.Raw()
	if err != nil {
		t.Fatalf("public key Raw: %v", err)
	}
	if err := shmevent.Verify(issuerPubRaw, m, crc, sig); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// TestRedeemJoinRequestTicketAdmitsPendingDevice mirrors
// TestRecruitAdmitsPendingNodeAndTicketIsSingleUse (pkg/daemon), but
// through kvmobile's real, signed-ticket API on the recruiting side: a
// pending device (raw, no raft instance -- spawnTestPendingDevice) mints
// its own join-request and signs a ticket over it with its own key; a
// separately-clustered kvmobile device (joined to a real leader as a
// voter) redeems that ticket via RedeemJoinRequestTicket, which must
// verify the signature, then dial the pending device directly and admit
// it -- with no action of the pending device's own beyond minting the
// ticket. Also proves the ticket is genuinely one-time.
func TestRedeemJoinRequestTicketAdmitsPendingDevice(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	pendingPeerID := spawnTestPendingDevice(t, t.TempDir())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pendingSess, err := shmclient.Open(ctx, pendingPeerID)
	if err != nil {
		t.Fatalf("shmclient.Open(pending): %v", err)
	}
	token, err := pendingSess.CreateJoinRequest(ctx)
	if err != nil {
		t.Fatalf("CreateJoinRequest: %v", err)
	}
	pendingAddr, err := pendingSess.GetOwnAddr(ctx)
	if err != nil {
		t.Fatalf("GetOwnAddr(pending): %v", err)
	}
	pendingPriv, err := shmclient.GetPrivateKey(ctx, pendingPeerID)
	if err != nil {
		t.Fatalf("GetPrivateKey(pending): %v", err)
	}
	ticket := buildJoinTicket(t, shmevent.EventJoinRequestTicket, pendingPriv, pendingAddr, token)

	var result string
	pollUntilTrue(t, 20*time.Second, func() (bool, error) {
		var err error
		result, err = RedeemJoinRequestTicket(ticket, "voter")
		return err == nil, err
	})
	if result == "" {
		t.Fatal("RedeemJoinRequestTicket succeeded but returned an empty result")
	}

	memberKey := string(shmevent.ClusterMemberKey([]byte(pendingPeerID)))
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := shmclient.Get(ctx, pendingPeerID, memberKey)
		return err == nil, err
	})

	// The identical ticket again must fail: the underlying join-request
	// token is one-time, same as the plain RecruitPeer path.
	if _, err := RedeemJoinRequestTicket(ticket, "voter"); err == nil {
		t.Fatal("second RedeemJoinRequestTicket with the already-consumed ticket unexpectedly succeeded")
	}
}

// TestRedeemJoinRequestTicketWithForgedSignatureFails isolates the new
// property a signed join-request ticket adds: a ticket claiming to be
// from the pending device but actually signed by a different key must be
// rejected by RedeemJoinRequestTicket's own signature check, before it
// ever dials anything.
func TestRedeemJoinRequestTicketWithForgedSignatureFails(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	pendingPeerID := spawnTestPendingDevice(t, t.TempDir())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pendingSess, err := shmclient.Open(ctx, pendingPeerID)
	if err != nil {
		t.Fatalf("shmclient.Open(pending): %v", err)
	}
	token, err := pendingSess.CreateJoinRequest(ctx)
	if err != nil {
		t.Fatalf("CreateJoinRequest: %v", err)
	}
	pendingAddr, err := pendingSess.GetOwnAddr(ctx)
	if err != nil {
		t.Fatalf("GetOwnAddr(pending): %v", err)
	}

	// Sign with some unrelated key instead of the pending device's own.
	_, forgerPriv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	forgedTicket := buildJoinTicket(t, shmevent.EventJoinRequestTicket, forgerPriv, pendingAddr, token)

	if _, err := RedeemJoinRequestTicket(forgedTicket, "voter"); err == nil {
		t.Fatal("RedeemJoinRequestTicket with a forged signature unexpectedly succeeded")
	}
}
