package kvctl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// CreateJoinInvite implements `mage createjoininvite <voter|learner>`:
// generates a fresh, cryptographically random one-time join-invite token
// and lodges it as a shmevent.KindJoinInvite record on the current node,
// granting suffrage. Returns the token hex-encoded -- that's the form an
// operator appends to a leader multiaddr, as "<multiaddr>#<tokenHex>",
// when calling mage addfollower/addnode (see pkg/daemon's
// splitInviteToken doc comment) to have that join admitted immediately
// even if the target has -require-confirm-for-join on. Only takes effect
// if the current node is itself a raft voter -- see
// shmevent.EventJoinInviteCreate's doc comment.
func CreateJoinInvite(suffrage byte) (string, error) {
	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("generate invite token: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	if err := sess.CreateJoinInvite(ctx, token, suffrage); err != nil {
		return "", fmt.Errorf("create join invite: %w", err)
	}
	return hex.EncodeToString(token), nil
}

// RevokeJoinInvite implements `mage revokejoininvite <tokenHex>`: deletes
// a KindJoinInvite record outright before it's ever redeemed. Only takes
// effect if the current node is itself a raft voter.
func RevokeJoinInvite(tokenHex string) error {
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return fmt.Errorf("invalid invite token %q: %w", tokenHex, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.RevokeJoinInvite(ctx, token); err != nil {
		return fmt.Errorf("revoke join invite: %w", err)
	}
	return nil
}
