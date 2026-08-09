package kvctl

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

// CreateExecInviteTicket implements `mage createexecinviteticket
// <commandID> <inputsJSON>`: does everything CreateExecInvite does (mints
// a token, lodges the KindExecInvite record), then wraps this node's own
// current address and that token into a signed, self-contained ticket --
// an EventExecTicket Msg built and signed with shmevent.Encode entirely
// client-side (no daemon changes needed: a local caller already holds the
// same signing key the node's own identity uses, fetched here via
// shmclient.GetPrivateKey exactly the way Session.Open does internally
// for every other signed call). Returns the ticket base64-encoded -- the
// string a DataMatrix encodes; RedeemExecInviteTicket is its inverse.
//
// Unlike the plain "<addr>#<tokenHex>" CreateExecInvite/RedeemExecInvite
// pair, a redeeming peer can verify this ticket really was produced by
// the peer id embedded in its own sourceAddr before ever dialing
// anything -- see EventExecTicket's doc comment for why that's a
// meaningfully different property than token possession alone, and why
// it doesn't change how redemption itself is authorized (kvfsm's
// existing ACL re-check on consume is unaffected either way).
func CreateExecInviteTicket(commandID, inputsJSON string) (string, error) {
	token := make([]byte, shmevent.ExecInviteTokenSize)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("generate exec invite token: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, selfPeerID, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	if err := sess.CreateExecInvite(ctx, token, commandID, inputsJSON); err != nil {
		return "", fmt.Errorf("create exec invite ticket: %w", err)
	}

	addr, err := sess.GetOwnAddr(ctx)
	if err != nil {
		return "", fmt.Errorf("create exec invite ticket: get own addr: %w", err)
	}

	m, err := shmevent.NewExecTicket(addr, token)
	if err != nil {
		return "", fmt.Errorf("create exec invite ticket: %w", err)
	}
	priv, err := shmclient.GetPrivateKey(ctx, selfPeerID)
	if err != nil {
		return "", fmt.Errorf("create exec invite ticket: fetch signing key: %w", err)
	}
	wire, err := shmevent.Encode(m, priv)
	if err != nil {
		return "", fmt.Errorf("create exec invite ticket: sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(wire), nil
}

// RedeemExecInviteTicket implements `mage redeemexecinviteticket
// <ticketB64>`: the CreateExecInviteTicket counterpart to
// RedeemExecInvite -- decodes ticketB64, extracts the issuing peer id
// from its own embedded sourceAddr (self-certifying, via the same
// addr->peer.ID extraction relayNodePeerID already uses for
// KindBootstrapNode addresses) and verifies the signature against that
// peer id's own public key, rejecting the ticket outright if it doesn't
// check out -- only then redeems it exactly like RedeemExecInvite does.
// Returns the new instance id on success -- track it with
// `mage getcommandrequest`/`mage querycommandlog`/`mage latestcommandlog`.
func RedeemExecInviteTicket(ticketB64 string) (string, error) {
	wire, err := base64.StdEncoding.DecodeString(ticketB64)
	if err != nil {
		return "", fmt.Errorf("invalid exec invite ticket: %w", err)
	}
	m, crc, sig, err := shmevent.Decode(wire)
	if err != nil {
		return "", fmt.Errorf("decode exec invite ticket: %w", err)
	}
	if m.Which() != shmevent.Event_Which_execTicket {
		return "", fmt.Errorf("not an exec invite ticket (event %s)", shmevent.EventName(m.Which()))
	}
	grp := m.ExecTicket()
	sourceAddr, err := grp.SourceAddr()
	if err != nil {
		return "", fmt.Errorf("decode exec invite ticket: %w", err)
	}
	token, err := grp.Token()
	if err != nil {
		return "", fmt.Errorf("decode exec invite ticket: %w", err)
	}

	issuerID, err := relayNodePeerID(sourceAddr)
	if err != nil {
		return "", fmt.Errorf("exec invite ticket: %w", err)
	}
	issuerPub, err := issuerID.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("exec invite ticket: extract issuer public key: %w", err)
	}
	issuerPubRaw, err := issuerPub.Raw()
	if err != nil {
		return "", fmt.Errorf("exec invite ticket: issuer public key: %w", err)
	}
	if err := shmevent.Verify(issuerPubRaw, m, crc, sig); err != nil {
		return "", fmt.Errorf("exec invite ticket: %w", err)
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
