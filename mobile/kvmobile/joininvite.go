package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
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

// CreateJoinInviteTicket is CreateJoinInvite's signed-ticket counterpart
// -- the mobile equivalent of desktop's pkg/kvctl.CreateJoinInviteTicket.
// It does everything CreateJoinInvite does (mints a token, lodges the
// KindJoinInvite record), then wraps this device's own current address
// and that token into a single signed, self-contained ticket, base64
// -encoded, for an app to render as a Data Matrix directly. A recipient
// can verify it really came from this device before ever using it -- see
// shmevent.EventJoinTicket's doc comment.
func CreateJoinInviteTicket(suffrage string) (string, error) {
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
		return "", fmt.Errorf("kvmobile: create join invite ticket: %w", err)
	}

	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join invite ticket: get own addr: %w", err)
	}

	m, err := shmevent.NewJoinTicket(addr, token)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join invite ticket: %w", err)
	}
	priv, err := shmclient.GetPrivateKey(ctx, PeerID())
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join invite ticket: fetch signing key: %w", err)
	}
	wire, err := shmevent.Encode(m, priv)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create join invite ticket: sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(wire), nil
}

// VerifyJoinInviteTicket is CreateJoinInviteTicket's counterpart -- the
// mobile equivalent of desktop's pkg/kvctl.VerifyJoinInviteTicket.
// Decodes ticketB64, extracts the issuing peer id from its own embedded
// address (self-certifying, via relayNodePeerID) and verifies the
// signature against that peer id's own public key, rejecting the ticket
// outright if it doesn't check out. Unlike RedeemExecInviteTicket, this
// doesn't dial or join anything itself -- join-invite redemption is
// baked into Join/JoinWithKey/Start/StartWithKey's own daemon-startup
// flow, which only ever wants the plain "<addr>#<tokenHex>" string
// CreateJoinInvite's caller has always hand-assembled -- so this returns
// exactly that string, ready to pass to any of those unchanged.
func VerifyJoinInviteTicket(ticketB64 string) (string, error) {
	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		return "", fmt.Errorf("kvmobile: invalid join invite ticket: %w", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode join invite ticket: %w", err)
	}
	if m.Which() != shmevent.Event_Which_joinTicket {
		return "", fmt.Errorf("kvmobile: not a join invite ticket (event %s)", shmevent.EventName(m.Which()))
	}
	grp := m.JoinTicket()
	addr, err := grp.SourceAddr()
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode join invite ticket: %w", err)
	}
	token, err := grp.Token()
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode join invite ticket: %w", err)
	}

	issuerID, err := relayNodePeerID(addr)
	if err != nil {
		return "", fmt.Errorf("kvmobile: join invite ticket: %w", err)
	}
	issuerPub, err := issuerID.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("kvmobile: join invite ticket: extract issuer public key: %w", err)
	}
	issuerPubRaw, err := issuerPub.Raw()
	if err != nil {
		return "", fmt.Errorf("kvmobile: join invite ticket: issuer public key: %w", err)
	}
	if err := shmevent.Verify(issuerPubRaw, m, crc, sig); err != nil {
		return "", fmt.Errorf("kvmobile: join invite ticket: %w", err)
	}

	return addr + "#" + hex.EncodeToString(token), nil
}
