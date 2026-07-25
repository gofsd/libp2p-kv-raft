package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

func newMobileExecInviteToken(t *testing.T) []byte {
	t.Helper()
	token := make([]byte, shmevent.ExecInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return token
}

// setUpExecInviteOnLeader opens a direct pkg/shmclient session against the
// raw (non-kvmobile) leader spawnTestLeader started -- it's already a sole
// voter (spawnTestLeader's own bootstrap), so this can write Group/
// Command/GroupCommand/PeerGroup and the exec invite itself directly,
// mirroring what `mage creategroup`/`createcommand`/... would do against
// it. redeemerPeerID is granted access via PeerGroup so kvmobile's own
// device (started separately, joined to this same leader) can redeem.
func setUpExecInviteOnLeader(t *testing.T, leaderPeerID, commandID, groupID, redeemerPeerID string, token []byte, inputsJSON string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, err := shmclient.Open(ctx, leaderPeerID)
	if err != nil {
		t.Fatalf("shmclient.Open(leader): %v", err)
	}
	if err := sess.PutGroup(ctx, groupID, groupID, false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := sess.PutCommand(ctx, commandID, commandID, []byte(leaderPeerID)); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := sess.PutGroupCommand(ctx, []byte(commandID), []byte(groupID)); err != nil {
		t.Fatalf("PutGroupCommand: %v", err)
	}
	if redeemerPeerID != "" {
		if err := sess.PutPeerGroup(ctx, []byte(redeemerPeerID), []byte(groupID)); err != nil {
			t.Fatalf("PutPeerGroup: %v", err)
		}
	}
	if err := sess.CreateExecInvite(ctx, token, commandID, inputsJSON); err != nil {
		t.Fatalf("CreateExecInvite: %v", err)
	}
}

// TestRedeemExecInviteByPermittedDeviceSucceedsAndIsOneTime drives
// RedeemExecInvite end to end against a real (non-kvmobile) leader,
// proving three things through kvmobile's own gomobile-bindable API: a
// device with real Group/Command ACL standing can redeem a one-time
// execution invite it was handed out-of-band, the resulting instance id
// is real and trackable (GetCommandRequest), and the identical token
// presented again afterward is rejected. The daemon-side security
// guarantees themselves (atomic ACL-check+consume, invite surviving an
// unauthorized attempt) are already covered end to end in
// pkg/daemon/execinvite_test.go -- this only needs to prove kvmobile's
// thin client wrapper actually plumbs through to that correctly.
func TestRedeemExecInviteByPermittedDeviceSucceedsAndIsOneTime(t *testing.T) {
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

	const commandID = "cmd-mobile-exec"
	const groupID = "grp-mobile-exec"
	token := newMobileExecInviteToken(t)
	setUpExecInviteOnLeader(t, leaderPeerID, commandID, groupID, redeemerPeerID, token, `{"x":1}`)

	var instanceID string
	pollUntilTrue(t, 20*time.Second, func() (bool, error) {
		var err error
		instanceID, err = RedeemExecInvite(leaderAddr + "#" + hex.EncodeToString(token))
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatal("RedeemExecInvite succeeded but returned an empty instance id")
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommandRequest(commandID, instanceID)
		return err == nil, nil
	})

	// The identical token again -- even from this same, still-permitted
	// device -- must fail: the invite is gone.
	if _, err := RedeemExecInvite(leaderAddr + "#" + hex.EncodeToString(token)); err == nil {
		t.Fatal("second RedeemExecInvite with the already-consumed token unexpectedly succeeded")
	}
}

// TestRedeemExecInviteByUnpermittedDeviceFails checks that kvmobile's
// wrapper correctly surfaces a rejection when this device has no
// Group/Command ACL standing for the invite's command -- the daemon-side
// "unauthorized attempt doesn't consume the invite" guarantee itself is
// covered in pkg/daemon/execinvite_test.go; this just proves the
// rejection reaches the caller through RedeemExecInvite rather than being
// swallowed or misreported as success.
func TestRedeemExecInviteByUnpermittedDeviceFails(t *testing.T) {
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

	const commandID = "cmd-mobile-exec-unpermitted"
	const groupID = "grp-mobile-exec-unpermitted"
	token := newMobileExecInviteToken(t)
	// Deliberately pass "" for redeemerPeerID: no PeerGroup grant for this
	// device at all.
	setUpExecInviteOnLeader(t, leaderPeerID, commandID, groupID, "", token, "")

	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := RedeemExecInvite(leaderAddr + "#" + hex.EncodeToString(token))
		if err != nil {
			return // rejected, as expected
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("RedeemExecInvite by an unpermitted device unexpectedly succeeded (lastErr=%v)", lastErr)
}

// TestCreateExecInviteRevokeExecInviteRequireVoter exercises
// CreateExecInvite/RevokeExecInvite directly against kvmobile's own
// device -- a kvmobile follower always joins as a full raft voter (see
// catalog_test.go's TestGroupCRUD), so both calls succeed unconditionally
// here, and the token is gone (no longer redeemable) after RevokeExecInvite.
func TestCreateExecInviteRevokeExecInvite(t *testing.T) {
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

	tokenHex, err := CreateExecInvite("cmd-self", `{"a":1}`)
	if err != nil {
		t.Fatalf("CreateExecInvite: %v", err)
	}
	if tokenHex == "" {
		t.Fatal("CreateExecInvite returned an empty token")
	}

	if err := RevokeExecInvite(tokenHex); err != nil {
		t.Fatalf("RevokeExecInvite: %v", err)
	}

	// A revoked token must not be redeemable. The invite record is
	// raft-replicated (kvfsm.OpSet, forwarded to the leader like any other
	// catalog write), so dialing the real leader's address redeems
	// against the same replicated state regardless of which node's
	// CreateExecInvite call originally wrote it.
	if _, err := RedeemExecInvite(leaderAddr + "#" + tokenHex); err == nil {
		t.Fatal("RedeemExecInvite with a revoked token unexpectedly succeeded")
	}
}
