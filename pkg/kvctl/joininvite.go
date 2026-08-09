package kvctl

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
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

// CreateJoinInviteTicket implements `mage createjoininviteticket
// <voter|learner>`: does everything CreateJoinInvite does (mints a token,
// lodges the KindJoinInvite record), then wraps this node's own current
// address and that token into a signed, self-contained ticket -- an
// EventJoinTicket Msg built and signed with shmevent.Encode entirely
// client-side, the same way CreateExecInviteTicket does for exec-invite
// (see that function's doc comment for why no daemon changes are
// needed). Returns the ticket base64-encoded -- the string a DataMatrix
// encodes; VerifyJoinInviteTicket is its inverse.
func CreateJoinInviteTicket(suffrage byte) (string, error) {
	token := make([]byte, shmevent.JoinInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("generate invite token: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, selfPeerID, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	if err := sess.CreateJoinInvite(ctx, token, suffrage); err != nil {
		return "", fmt.Errorf("create join invite ticket: %w", err)
	}

	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("create join invite ticket: get own addr: %w", err)
	}

	m, err := shmevent.NewJoinTicket(addr, token)
	if err != nil {
		return "", fmt.Errorf("create join invite ticket: %w", err)
	}
	priv, err := shmclient.GetPrivateKey(ctx, selfPeerID)
	if err != nil {
		return "", fmt.Errorf("create join invite ticket: fetch signing key: %w", err)
	}
	wire, err := shmevent.Encode(m, priv)
	if err != nil {
		return "", fmt.Errorf("create join invite ticket: sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(wire), nil
}

// VerifyJoinInviteTicket implements `mage verifyjoininviteticket
// <ticketB64>`: the CreateJoinInviteTicket counterpart -- decodes
// ticketB64, extracts the issuing peer id from its own embedded address
// (self-certifying, via the same addr->peer.ID extraction
// RedeemExecInviteTicket uses) and verifies the signature against that
// peer id's own public key, rejecting the ticket outright if it doesn't
// check out. Unlike RedeemExecInviteTicket, this doesn't dial or join
// anything itself -- join-invite redemption is baked into `mage
// addfollower`/`mage addnode`/`mage rejoinnode`/mobile/kvmobile.Join's
// own daemon-startup flow (pkg/daemon's splitInviteToken), which only
// ever wants the plain "<addr>#<tokenHex>" string CreateJoinInvite's
// caller has always hand-assembled -- so this returns exactly that
// string, ready to pass to any of those unchanged.
func VerifyJoinInviteTicket(ticketB64 string) (string, error) {
	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		return "", fmt.Errorf("invalid join invite ticket: %w", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		return "", fmt.Errorf("decode join invite ticket: %w", err)
	}
	if m.Which() != shmevent.Event_Which_joinTicket {
		return "", fmt.Errorf("not a join invite ticket (event %s)", shmevent.EventName(m.Which()))
	}
	grp := m.JoinTicket()
	addr, err := grp.SourceAddr()
	if err != nil {
		return "", fmt.Errorf("decode join invite ticket: %w", err)
	}
	token, err := grp.Token()
	if err != nil {
		return "", fmt.Errorf("decode join invite ticket: %w", err)
	}

	issuerID, err := relayNodePeerID(addr)
	if err != nil {
		return "", fmt.Errorf("join invite ticket: %w", err)
	}
	issuerPub, err := issuerID.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("join invite ticket: extract issuer public key: %w", err)
	}
	issuerPubRaw, err := issuerPub.Raw()
	if err != nil {
		return "", fmt.Errorf("join invite ticket: issuer public key: %w", err)
	}
	if err := shmevent.Verify(issuerPubRaw, m, crc, sig); err != nil {
		return "", fmt.Errorf("join invite ticket: %w", err)
	}

	return addr + "#" + hex.EncodeToString(token), nil
}
