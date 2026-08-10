//go:build mage

package main

import (
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// AddPending spawns a brand new node, like addnode's bootstrap case,
// except it never bootstraps or joins anything -- it's left running but
// with no raft instance at all, ready for the reverse-invite "join-request"
// flow (createjoinrequest, then some other cluster's `mage recruitpeer`)
// to admit it into some other cluster with no restart in between. See
// pkg/daemon's RecruitProtocolID doc comment for why this only works on a
// node this fresh -- an already-bootstrapped or already-joined node needs
// `mage leave`/`rm` first.
//
// Usage: mage addpending
func AddPending() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.AddPending(root)
	if err != nil {
		return err
	}
	fmt.Printf("✅ node %s is up (pending, no cluster yet) and selected as current\n", peerID)
	return nil
}

// CreateJoinRequest mints a fresh one-time join-request ticket on the
// current node and prints it -- append it to this node's own advertised
// multiaddr as "<multiaddr>#<tokenHex>" (see printjoinrequestdatamatrix)
// for some other cluster's operator to scan and pass to `mage recruitpeer`,
// admitting this node into their cluster with no further action here.
//
// Usage: mage createjoinrequest
func CreateJoinRequest() error {
	tokenHex, err := kvctl.CreateJoinRequest()
	if err != nil {
		return err
	}
	fmt.Println(tokenHex)
	return nil
}

// CancelJoinRequest clears the current node's pending join-request ticket
// before it's ever redeemed.
//
// Usage: mage canceljoinrequest <tokenHex>
func CancelJoinRequest(tokenHex string) error {
	if err := kvctl.CancelJoinRequest(tokenHex); err != nil {
		return err
	}
	fmt.Println("✅ join request cancelled")
	return nil
}

// RecruitPeer asks the current node (an existing raft voter) to mint a
// normal join invite on its own cluster and hand-deliver it directly to
// the device named in ticket -- the reverse of addfollower/join-invite:
// instead of the device dialing in to join, this node dials out to the
// device and the device admits *itself* on receipt, with no further
// action from that device's own operator. ticket is exactly the string
// printjoinrequestdatamatrix barcodes ("<device's own multiaddr>#<tokenHex>").
// Prints the recruited device's own join result ("<peerID> ok"/"<peerID>
// pending") on success.
//
// Usage: mage recruitpeer <ticket> <voter|learner>
func RecruitPeer(ticket, suffrage string) error {
	var sf byte
	switch suffrage {
	case "voter":
		sf = shmevent.SuffrageVoter
	case "learner":
		sf = shmevent.SuffrageLearner
	default:
		return fmt.Errorf("unknown suffrage %q (want \"voter\" or \"learner\")", suffrage)
	}
	result, err := kvctl.RecruitPeer(ticket, sf)
	if err != nil {
		return err
	}
	fmt.Println(result)
	return nil
}

// CreateJoinRequestTicket is CreateJoinRequest's signed-ticket
// counterpart: mints the same one-time join-request token, but prints a
// single self-contained, base64 ticket (this node's own address + the
// token, signed with this node's own key) instead of a bare token to
// hand-combine with an address yourself. A recruiting peer verifies it
// really came from this device before ever dialing it -- see
// redeemjoinrequestticket.
//
// Usage: mage createjoinrequestticket
func CreateJoinRequestTicket() error {
	ticket, err := kvctl.CreateJoinRequestTicket()
	if err != nil {
		return err
	}
	fmt.Println(ticket)
	return nil
}

// RedeemJoinRequestTicket is RecruitPeer's signed-ticket counterpart:
// verifies ticketB64 (as printed by createjoinrequestticket) against the
// peer id embedded in its own address, rejecting it outright if the
// signature doesn't check out, then recruits the device exactly like
// recruitpeer does. Prints the recruited device's own join result
// ("<peerID> ok"/"<peerID> pending") on success.
//
// Usage: mage redeemjoinrequestticket <ticketB64> <voter|learner>
func RedeemJoinRequestTicket(ticketB64, suffrage string) error {
	var sf byte
	switch suffrage {
	case "voter":
		sf = shmevent.SuffrageVoter
	case "learner":
		sf = shmevent.SuffrageLearner
	default:
		return fmt.Errorf("unknown suffrage %q (want \"voter\" or \"learner\")", suffrage)
	}
	result, err := kvctl.RedeemJoinRequestTicket(ticketB64, sf)
	if err != nil {
		return err
	}
	fmt.Println(result)
	return nil
}
