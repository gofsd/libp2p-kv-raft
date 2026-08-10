package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
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
// (inputsJSON may be ""). ttlSeconds is how long the invite stays
// redeemable, 0 meaning no expiry (the default). Returns the token
// hex-encoded -- append it to this device's own advertised multiaddr as
// "<multiaddr>#<tokenHex>" for a redeeming peer to scan and pass to
// RedeemExecInvite. Only takes effect if this device is itself a raft
// voter -- see shmevent.EventExecInviteCreate's doc comment.
func CreateExecInvite(commandID, inputsJSON string, ttlSeconds int) (string, error) {
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
	if err := sess.CreateExecInvite(ctx, token, commandID, inputsJSON, uint64(ttlSeconds)); err != nil {
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

// CreateExecInviteTicket is CreateExecInvite's signed-ticket counterpart --
// the mobile equivalent of desktop's pkg/kvctl.CreateExecInviteTicket. It
// does everything CreateExecInvite does (mints a token, lodges the
// KindExecInvite record), then wraps this device's own current address
// and that token into a single signed, self-contained ticket, base64
// -encoded -- an app renders that string as a Data Matrix directly (see
// this file's doc comment on why kvmobile hands back data rather than a
// rendered barcode itself). Unlike the bare tokenHex CreateExecInvite
// returns, a redeeming peer can verify this ticket really came from this
// device before ever dialing it -- see shmevent.EventExecTicket's doc
// comment. ttlSeconds is how long the invite stays redeemable, 0 meaning
// no expiry (the default), same as CreateExecInvite.
func CreateExecInviteTicket(commandID, inputsJSON string, ttlSeconds int) (string, error) {
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
	if err := sess.CreateExecInvite(ctx, token, commandID, inputsJSON, uint64(ttlSeconds)); err != nil {
		return "", fmt.Errorf("kvmobile: create exec invite ticket: %w", err)
	}

	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create exec invite ticket: get own addr: %w", err)
	}

	m, err := shmevent.NewExecTicket(addr, token)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create exec invite ticket: %w", err)
	}
	priv, err := shmclient.GetPrivateKey(ctx, PeerID())
	if err != nil {
		return "", fmt.Errorf("kvmobile: create exec invite ticket: fetch signing key: %w", err)
	}
	wire, err := shmevent.Encode(m, priv)
	if err != nil {
		return "", fmt.Errorf("kvmobile: create exec invite ticket: sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(wire), nil
}

// RedeemExecInviteTicket is RedeemExecInvite's signed-ticket counterpart
// -- the mobile equivalent of desktop's pkg/kvctl.RedeemExecInviteTicket.
// Decodes ticketB64, extracts the issuing peer id from its own embedded
// address (self-certifying, via relayNodePeerID) and verifies the
// signature against that peer id's own public key, rejecting the ticket
// outright if it doesn't check out -- only then redeems it exactly like
// RedeemExecInvite does.
func RedeemExecInviteTicket(ticketB64 string) (string, error) {
	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		return "", fmt.Errorf("kvmobile: invalid exec invite ticket: %w", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode exec invite ticket: %w", err)
	}
	if m.Which() != shmevent.Event_Which_execTicket {
		return "", fmt.Errorf("kvmobile: not an exec invite ticket (event %s)", shmevent.EventName(m.Which()))
	}
	grp := m.ExecTicket()
	sourceAddr, err := grp.SourceAddr()
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode exec invite ticket: %w", err)
	}
	token, err := grp.Token()
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode exec invite ticket: %w", err)
	}

	issuerID, err := relayNodePeerID(sourceAddr)
	if err != nil {
		return "", fmt.Errorf("kvmobile: exec invite ticket: %w", err)
	}
	issuerPub, err := issuerID.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("kvmobile: exec invite ticket: extract issuer public key: %w", err)
	}
	issuerPubRaw, err := issuerPub.Raw()
	if err != nil {
		return "", fmt.Errorf("kvmobile: exec invite ticket: issuer public key: %w", err)
	}
	if err := shmevent.Verify(issuerPubRaw, m, crc, sig); err != nil {
		return "", fmt.Errorf("kvmobile: exec invite ticket: %w", err)
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
