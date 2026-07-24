package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file is the mobile counterpart of desktop's
// pkg/kvctl/execinvite.go: a one-time execution invite lets a raft voter
// hand a specific "run this command with these inputs" ticket to another
// peer out-of-band (e.g. a Data Matrix barcode), redeemable exactly once,
// with the redeeming peer's real Group/Command ACL standing re-checked
// atomically at redemption time -- see shmevent.KindExecInvite's doc
// comment for the full design, already implemented daemon-side; this file
// is purely the gomobile-bindable client wrapper, same as every other file
// here. Unlike desktop's kvctl-cli, there is no printexecinvitedatamatrix
// equivalent: CreateExecInvite just returns the raw tokenHex, and an
// Android app combines it with its own advertised multiaddr and renders
// the barcode itself (e.g. via a Kotlin QR/Data-Matrix library) -- the
// same reasoning kvmobile's catalog.go doc comment gives for why
// ResolveQRGroup wasn't carried over either: this Go layer hands back
// data, presentation is the app's job.

// CreateExecInvite generates a fresh, cryptographically random one-time
// execution-invite token and lodges it as a shmevent.KindExecInvite
// record on this device's own daemon, binding commandID+inputsJSON
// (inputsJSON may be ""). Returns the token hex-encoded -- append it to
// this device's own advertised multiaddr as "<multiaddr>#<tokenHex>" for
// a redeeming peer to scan and pass to RedeemExecInvite. Only takes
// effect if this device is itself a raft voter -- see
// shmevent.EventExecInviteCreate's doc comment.
func CreateExecInvite(commandID, inputsJSON string) (string, error) {
	token := make([]byte, shmevent.ExecInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("kvmobile: generate exec invite token: %w", err)
	}

	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.CreateExecInvite(ctx, token, commandID, inputsJSON); err != nil {
		return "", fmt.Errorf("kvmobile: create exec invite: %w", err)
	}
	return hex.EncodeToString(token), nil
}

// RevokeExecInvite deletes a KindExecInvite record outright before it's
// ever redeemed. Only takes effect if this device is itself a raft voter.
func RevokeExecInvite(tokenHex string) error {
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return fmt.Errorf("kvmobile: invalid exec invite token %q: %w", tokenHex, err)
	}

	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RevokeExecInvite(ctx, token); err != nil {
		return fmt.Errorf("kvmobile: revoke exec invite: %w", err)
	}
	return nil
}

// RedeemExecInvite tells this device's own daemon to dial sourceAddr and
// redeem token there on this device's own behalf -- the daemon signs the
// redemption with this device's own key (see
// shmevent.EventExecInviteRedeem's doc comment), so this is the actual
// "peer signs it" step, not a formality. sourceAddrAndToken is exactly the
// string a scanned execution-invite barcode decodes to
// ("<sourceAddr>#<tokenHex>"). The receiving cluster's raft leader
// atomically re-checks this device's real Group/Command ACL standing and
// consumes the token in one step, so this only succeeds if this device is
// authorized *and* the invite hasn't already been redeemed. Returns the
// new instance id on success -- track it with
// GetCommandRequest/QueryCommandLog/LatestCommandLog against the target
// device's own node.
func RedeemExecInvite(sourceAddrAndToken string) (string, error) {
	sourceAddr, tokenHex, ok := strings.Cut(sourceAddrAndToken, "#")
	if !ok {
		return "", fmt.Errorf("kvmobile: expected \"<sourceAddr>#<tokenHex>\", got %q", sourceAddrAndToken)
	}
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return "", fmt.Errorf("kvmobile: invalid exec invite token %q: %w", tokenHex, err)
	}

	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	instanceID, err := sess.RedeemExecInvite(ctx, sourceAddr, token)
	if err != nil {
		return "", fmt.Errorf("kvmobile: redeem exec invite: %w", err)
	}
	return instanceID, nil
}
