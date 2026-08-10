package kvmobile

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// buildExecInviteTicket signs sourceAddr+token with priv exactly the way
// CreateExecInviteTicket does internally (see that function's body in
// execinvite.go), for a test that needs to mint a ticket "as" a specific
// device (the leader) other than whichever device is currently started
// via kvmobile in this process -- kvmobile runs exactly one daemon per
// process (see Stop's doc comment), so CreateExecInviteTicket itself can
// only ever sign on behalf of that one device, never a separately spawned
// raw leader like spawnTestLeader's.
func buildExecInviteTicket(t *testing.T, priv shmevent.PrivateKey, sourceAddr string, token []byte) string {
	t.Helper()
	m, err := shmevent.NewExecTicket(sourceAddr, token)
	if err != nil {
		t.Fatalf("NewExecTicket: %v", err)
	}
	wire, err := shmevent.Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return base64.StdEncoding.EncodeToString(wire)
}

// TestCreateExecInviteTicketIsVerifiable drives CreateExecInviteTicket
// directly through kvmobile's own gomobile-bindable API (on a lone,
// solo-bootstrapped device -- no second device needed to prove the
// *create* side works), then independently decodes and verifies the
// result the same way a redeeming device would: proving the ticket
// CreateExecInviteTicket produces really is signed by this device's own
// key over its own current address and the freshly minted token, not
// just that the call didn't error.
func TestCreateExecInviteTicketIsVerifiable(t *testing.T) {
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

	ticketB64, err := CreateExecInviteTicket("cmd-self-ticket", `{"a":1}`, 0)
	if err != nil {
		t.Fatalf("CreateExecInviteTicket: %v", err)
	}
	if ticketB64 == "" {
		t.Fatal("CreateExecInviteTicket returned an empty ticket")
	}

	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		t.Fatalf("decode ticket base64: %v", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		t.Fatalf("shmevent.Decode: %v", err)
	}
	if m.Which() != shmevent.Event_Which_execTicket {
		t.Fatalf("got event %s, want %s", shmevent.EventName(m.Which()), shmevent.EventName(shmevent.Event_Which_execTicket))
	}
	addr, err := m.ExecTicket().SourceAddr()
	if err != nil {
		t.Fatalf("SourceAddr: %v", err)
	}
	token, err := m.ExecTicket().Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if len(token) != shmevent.ExecInviteTokenSize {
		t.Fatalf("got token length %d, want %d", len(token), shmevent.ExecInviteTokenSize)
	}
	if addr == "" {
		t.Fatal("decoded ticket has an empty source address")
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

// TestRedeemExecInviteTicketByPermittedDeviceSucceedsAndIsOneTime mirrors
// TestRedeemExecInviteByPermittedDeviceSucceedsAndIsOneTime, but for the
// signed-ticket path: proves RedeemExecInviteTicket, driven through
// kvmobile's real public API, actually verifies the leader's signature
// and completes the redemption, and that the same ticket can't be
// replayed afterward.
func TestRedeemExecInviteTicketByPermittedDeviceSucceedsAndIsOneTime(t *testing.T) {
	dataDir := t.TempDir()
	leaderAddr := spawnTestLeader(t, dataDir)
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

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
	redeemerPeerID := PeerID()

	const commandID = "cmd-mobile-exec-ticket"
	const groupID = "grp-mobile-exec-ticket"
	token := newMobileExecInviteToken(t)
	setUpExecInviteOnLeader(t, leaderPeerID, commandID, groupID, redeemerPeerID, token, `{"x":1}`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leaderPriv, err := shmclient.GetPrivateKey(ctx, leaderPeerID)
	if err != nil {
		t.Fatalf("GetPrivateKey(leader): %v", err)
	}
	ticket := buildExecInviteTicket(t, leaderPriv, leaderAddr, token)

	var instanceID string
	pollUntilTrue(t, 20*time.Second, func() (bool, error) {
		var err error
		instanceID, err = RedeemExecInviteTicket(ticket)
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatal("RedeemExecInviteTicket succeeded but returned an empty instance id")
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommandRequest(commandID, instanceID)
		return err == nil, nil
	})

	// The identical ticket again must fail: the underlying invite token is
	// consumed, the same way a plain RedeemExecInvite retry does.
	if _, err := RedeemExecInviteTicket(ticket); err == nil {
		t.Fatal("second RedeemExecInviteTicket with the already-consumed ticket unexpectedly succeeded")
	}
}

// TestRedeemExecInviteTicketByUnpermittedDeviceFails mirrors
// TestRedeemExecInviteByUnpermittedDeviceFails for the signed-ticket path
// -- the signature checks out (the leader really did issue it), but this
// device still has no Group/Command ACL standing, so redemption must
// still fail at that separate, existing daemon-side check.
func TestRedeemExecInviteTicketByUnpermittedDeviceFails(t *testing.T) {
	dataDir := t.TempDir()
	leaderAddr := spawnTestLeader(t, dataDir)
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

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

	const commandID = "cmd-mobile-exec-ticket-unpermitted"
	const groupID = "grp-mobile-exec-ticket-unpermitted"
	token := newMobileExecInviteToken(t)
	// Deliberately pass "" for redeemerPeerID: no PeerGroup grant for this
	// device at all.
	setUpExecInviteOnLeader(t, leaderPeerID, commandID, groupID, "", token, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leaderPriv, err := shmclient.GetPrivateKey(ctx, leaderPeerID)
	if err != nil {
		t.Fatalf("GetPrivateKey(leader): %v", err)
	}
	ticket := buildExecInviteTicket(t, leaderPriv, leaderAddr, token)

	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := RedeemExecInviteTicket(ticket)
		if err != nil {
			return // rejected, as expected
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("RedeemExecInviteTicket by an unpermitted device unexpectedly succeeded (lastErr=%v)", lastErr)
}

// TestRedeemExecInviteTicketWithForgedSignatureFails is the test that
// actually isolates the new property a signed ticket adds over a plain
// one: a ticket claiming to be from the leader, but actually signed by
// some other key, must be rejected by signature verification -- entirely
// client-side, before RedeemExecInviteTicket ever dials the leader at
// all. The redeemer *is* fully permitted here (unlike
// TestRedeemExecInviteTicketByUnpermittedDeviceFails), isolating that
// this specific failure comes from the signature check, not the
// separate, pre-existing ACL check.
func TestRedeemExecInviteTicketWithForgedSignatureFails(t *testing.T) {
	dataDir := t.TempDir()
	leaderAddr := spawnTestLeader(t, dataDir)
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

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
	redeemerPeerID := PeerID()

	const commandID = "cmd-mobile-exec-ticket-forged"
	const groupID = "grp-mobile-exec-ticket-forged"
	token := newMobileExecInviteToken(t)
	setUpExecInviteOnLeader(t, leaderPeerID, commandID, groupID, redeemerPeerID, token, "")

	// Sign with this redeeming device's own key instead of the leader's --
	// structurally a perfectly valid ticket, just not one the leader ever
	// produced.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	forgerPriv, err := shmclient.GetPrivateKey(ctx, redeemerPeerID)
	if err != nil {
		t.Fatalf("GetPrivateKey(redeemer): %v", err)
	}
	forgedTicket := buildExecInviteTicket(t, forgerPriv, leaderAddr, token)

	if _, err := RedeemExecInviteTicket(forgedTicket); err == nil {
		t.Fatal("RedeemExecInviteTicket with a forged signature unexpectedly succeeded")
	}
}
