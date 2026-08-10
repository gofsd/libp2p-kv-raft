//go:build mage

package main

import (
	"fmt"
	"strconv"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// CreateJoinInvite generates a fresh one-time join-invite token and
// prints it -- append it to a leader multiaddr as
// "<multiaddr>#<tokenHex>" and pass that to mage addfollower/addnode to
// have that join admitted immediately even with -require-confirm-for-join
// on, with no separate confirmpermit step. Only takes effect if the
// current node is itself a raft voter.
// Usage: mage createjoininvite <voter|learner>
func CreateJoinInvite(suffrage string) error {
	var sf byte
	switch suffrage {
	case "voter":
		sf = shmevent.SuffrageVoter
	case "learner":
		sf = shmevent.SuffrageLearner
	default:
		return fmt.Errorf("unknown suffrage %q (want \"voter\" or \"learner\")", suffrage)
	}
	tokenHex, err := kvctl.CreateJoinInvite(sf)
	if err != nil {
		return err
	}
	fmt.Println(tokenHex)
	return nil
}

// RevokeJoinInvite deletes a join-invite token outright before it's ever
// redeemed. Only takes effect if the current node is itself a raft voter.
// Usage: mage revokejoininvite <tokenHex>
func RevokeJoinInvite(tokenHex string) error {
	if err := kvctl.RevokeJoinInvite(tokenHex); err != nil {
		return err
	}
	fmt.Println("✅ join invite revoked")
	return nil
}

// CreateJoinInviteTicket is CreateJoinInvite's signed-ticket counterpart:
// mints the same one-time join-invite token and lodges the same
// KindJoinInvite record, but prints a single self-contained, base64
// ticket (this node's own address + the token, signed with this node's
// own key) instead of a bare token to hand-combine with an address
// yourself. A recipient verifies it really came from this peer id before
// ever using it -- see verifyjoininviteticket. Only takes effect if the
// current node is itself a raft voter.
// Usage: mage createjoininviteticket <voter|learner>
func CreateJoinInviteTicket(suffrage string) error {
	var sf byte
	switch suffrage {
	case "voter":
		sf = shmevent.SuffrageVoter
	case "learner":
		sf = shmevent.SuffrageLearner
	default:
		return fmt.Errorf("unknown suffrage %q (want \"voter\" or \"learner\")", suffrage)
	}
	ticket, err := kvctl.CreateJoinInviteTicket(sf)
	if err != nil {
		return err
	}
	fmt.Println(ticket)
	return nil
}

// VerifyJoinInviteTicket verifies ticketB64 (as printed by
// createjoininviteticket) against the peer id embedded in its own
// address, rejecting it outright if the signature doesn't check out, and
// prints the plain "<addr>#<tokenHex>" string -- pass that straight to
// addfollower/addnode/rejoinnode exactly as you would today's bare
// createjoininvite output combined with an address by hand.
// Usage: mage verifyjoininviteticket <ticketB64>
func VerifyJoinInviteTicket(ticketB64 string) error {
	addrAndToken, err := kvctl.VerifyJoinInviteTicket(ticketB64)
	if err != nil {
		return err
	}
	fmt.Println(addrAndToken)
	return nil
}

// CreateExecInvite generates a fresh one-time execution-invite token
// binding commandID+inputsJSON and prints it -- append it to this node's
// own advertised multiaddr as "<multiaddr>#<tokenHex>" (see
// printexecinvitedatamatrix) for a redeeming peer to scan and pass to
// `mage redeemexecinvite`. Only takes effect if the current node is itself
// a raft voter. ttlSeconds is a count or "" (no expiry -- the default),
// same convention as rangescan's limit.
// Usage: mage createexecinvite <commandID> <inputsJSON> [ttlSeconds|""]
func CreateExecInvite(commandID, inputsJSON, ttlSeconds string) error {
	ttl, err := parseExecInviteTTL(ttlSeconds)
	if err != nil {
		return err
	}
	tokenHex, err := kvctl.CreateExecInvite(commandID, inputsJSON, ttl)
	if err != nil {
		return err
	}
	fmt.Println(tokenHex)
	return nil
}

// parseExecInviteTTL parses CreateExecInvite/CreateExecInviteTicket's
// ttlSeconds arg -- "" means 0 (no expiry).
func parseExecInviteTTL(ttlSeconds string) (uint64, error) {
	if ttlSeconds == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(ttlSeconds, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid ttlSeconds %q: %w", ttlSeconds, err)
	}
	return v, nil
}

// RevokeExecInvite deletes an execution-invite token outright before it's
// ever redeemed. Only takes effect if the current node is itself a raft
// voter.
// Usage: mage revokeexecinvite <tokenHex>
func RevokeExecInvite(tokenHex string) error {
	if err := kvctl.RevokeExecInvite(tokenHex); err != nil {
		return err
	}
	fmt.Println("✅ exec invite revoked")
	return nil
}

// RedeemExecInvite tells the current node's own daemon to dial sourceAddr
// and redeem token there on this node's own behalf, signing the
// redemption with this node's own key -- the receiving cluster's raft
// leader atomically re-checks this node's real Group/Command ACL standing
// and consumes the token in one step. sourceAddrAndToken is exactly the
// string printexecinvitedatamatrix barcodes
// ("<sourceMultiaddr>#<tokenHex>"). Prints the new instance id on success;
// track it with getcommandrequest/querycommandlog/latestcommandlog against
// the target's own node.
// Usage: mage redeemexecinvite <sourceAddr#tokenHex>
func RedeemExecInvite(sourceAddrAndToken string) error {
	instanceID, err := kvctl.RedeemExecInvite(sourceAddrAndToken)
	if err != nil {
		return err
	}
	fmt.Println(instanceID)
	return nil
}

// CreateExecInviteTicket is CreateExecInvite's signed-ticket counterpart:
// mints the same one-time execution-invite token and lodges the same
// KindExecInvite record, but prints a single self-contained, base64
// ticket (this node's own address + the token, signed with this node's
// own key) instead of a bare token to hand-combine with an address
// yourself. That ticket is what a DataMatrix code encodes; a redeeming
// peer verifies it really came from the peer id embedded in its own
// address before ever dialing anything -- see redeemexecinviteticket.
// Only takes effect if the current node is itself a raft voter. ttlSeconds
// is a count or "" (no expiry -- the default), same as createexecinvite's.
// Usage: mage createexecinviteticket <commandID> <inputsJSON> [ttlSeconds|""]
func CreateExecInviteTicket(commandID, inputsJSON, ttlSeconds string) error {
	ttl, err := parseExecInviteTTL(ttlSeconds)
	if err != nil {
		return err
	}
	ticket, err := kvctl.CreateExecInviteTicket(commandID, inputsJSON, ttl)
	if err != nil {
		return err
	}
	fmt.Println(ticket)
	return nil
}

// RedeemExecInviteTicket is RedeemExecInvite's signed-ticket counterpart:
// verifies ticketB64 (as printed by createexecinviteticket) against the
// peer id embedded in its own address, rejecting it outright if the
// signature doesn't check out, then redeems it exactly like
// redeemexecinvite does. Prints the new instance id on success; track it
// with getcommandrequest/querycommandlog/latestcommandlog against the
// target's own node.
// Usage: mage redeemexecinviteticket <ticketB64>
func RedeemExecInviteTicket(ticketB64 string) error {
	instanceID, err := kvctl.RedeemExecInviteTicket(ticketB64)
	if err != nil {
		return err
	}
	fmt.Println(instanceID)
	return nil
}
