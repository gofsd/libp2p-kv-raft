package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// CreateJoinInvite generates a fresh, cryptographically random one-time
// join-invite token and lodges it as a shmevent.KindJoinInvite record on
// this device, granting suffrage ("voter" or "learner", the kvmobile
// string-enum convention -- see RecruitPeer). Returns the token
// hex-encoded, the same "<multiaddr>#<tokenHex>" form desktop's
// pkg/kvctl.CreateJoinInvite/`mage createjoininvite` produces -- append it
// to this device's own advertised multiaddr (GetOwnAddr) for another
// device's Join/JoinWithKey/Start/StartWithKey to redeem, admitting that
// join immediately even if this cluster's leader has
// -require-confirm-for-join on. Unlike RecruitPeer (which both mints the
// invite and hand-delivers it directly to a named device), this only
// mints the token -- handing it to the joining device is left to the
// caller (typed in, QR code, etc.), mirroring desktop's separate
// createjoininvite vs. recruitpeer targets. Only takes effect if this
// device is itself a raft voter -- see shmevent.EventJoinInviteCreate's
// doc comment.
func CreateJoinInvite(suffrage string) (string, error) {
	var sf byte
	switch suffrage {
	case "voter":
		sf = shmevent.SuffrageVoter
	case "learner":
		sf = shmevent.SuffrageLearner
	default:
		return "", fmt.Errorf("kvmobile: unknown suffrage %q (want \"voter\" or \"learner\")", suffrage)
	}

	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("kvmobile: generate invite token: %w", err)
	}

	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.CreateJoinInvite(ctx, token, sf); err != nil {
		return "", fmt.Errorf("kvmobile: create join invite: %w", err)
	}
	return hex.EncodeToString(token), nil
}

// RevokeJoinInvite deletes a KindJoinInvite record outright before it's
// ever redeemed -- desktop's pkg/kvctl.RevokeJoinInvite/`mage
// revokejoininvite` counterpart. Only takes effect if this device is
// itself a raft voter.
func RevokeJoinInvite(tokenHex string) error {
	token, err := hex.DecodeString(tokenHex)
	if err != nil {
		return fmt.Errorf("kvmobile: invalid invite token %q: %w", tokenHex, err)
	}

	sess, err := currentSession()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RevokeJoinInvite(ctx, token); err != nil {
		return fmt.Errorf("kvmobile: revoke join invite: %w", err)
	}
	return nil
}
