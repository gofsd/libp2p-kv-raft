package kvctl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// CreateExecInvite implements `mage createexecinvite <commandID>
// <inputsJSON>`: generates a fresh, cryptographically random one-time
// execution-invite token and lodges it as a shmevent.KindExecInvite record
// on the current node, binding commandID+inputsJSON (inputsJSON may be "").
// Returns the token hex-encoded -- append it to this node's own advertised
// multiaddr as "<multiaddr>#<tokenHex>" (see mage printexecinvitedatamatrix)
// for a redeeming peer to scan and pass to `mage redeemexecinvite`. Only
// takes effect if the current node is itself a raft voter -- see
// shmevent.EventExecInviteCreate's doc comment.
func CreateExecInvite(commandID, inputsJSON string) (string, error) {
	token := make([]byte, shmevent.ExecInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("generate exec invite token: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	if err := sess.CreateExecInvite(ctx, token, commandID, inputsJSON); err != nil {
		return "", fmt.Errorf("create exec invite: %w", err)
	}
	return hex.EncodeToString(token), nil
}

// RevokeExecInvite implements `mage revokeexecinvite <tokenHex>`: deletes a
// KindExecInvite record outright before it's ever redeemed. Only takes
// effect if the current node is itself a raft voter.
func RevokeExecInvite(tokenHex string) error {
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return fmt.Errorf("invalid exec invite token %q: %w", tokenHex, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.RevokeExecInvite(ctx, token); err != nil {
		return fmt.Errorf("revoke exec invite: %w", err)
	}
	return nil
}

// RedeemExecInvite implements `mage redeemexecinvite <sourceAddr#tokenHex>`:
// tells the current node's own daemon to dial sourceAddr and redeem token
// there on this node's own behalf (its own daemon signs the redeem message
// with its own key -- see shmevent.EventExecInviteRedeem's doc comment).
// sourceAddrAndToken is exactly the string `mage printexecinvitedatamatrix`
// barcodes ("<sourceMultiaddr>#<tokenHex>"), split here the same way
// pkg/daemon's splitInviteToken splits a join-invite's combined string.
// Returns the new instance id on success -- track it with
// `mage getcommandrequest`/`mage querycommandlog`/`mage latestcommandlog`.
func RedeemExecInvite(sourceAddrAndToken string) (string, error) {
	sourceAddr, tokenHex, ok := strings.Cut(sourceAddrAndToken, "#")
	if !ok {
		return "", fmt.Errorf("expected \"<sourceAddr>#<tokenHex>\", got %q", sourceAddrAndToken)
	}
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return "", fmt.Errorf("invalid exec invite token %q: %w", tokenHex, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	instanceID, err := sess.RedeemExecInvite(ctx, sourceAddr, token)
	if err != nil {
		return "", fmt.Errorf("redeem exec invite: %w", err)
	}
	return instanceID, nil
}
