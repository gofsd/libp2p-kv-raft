//go:build mage

package main

import (
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// RequestPublicAccess implements `mage requestpublicaccess <targetAddr>
// [note]`: asks the current node to submit the always-public
// "public-access" command to the cluster at targetAddr, which grants this
// node Channel and circuit-relay standing there. This is the self-service
// alternative to an operator on the *target* cluster running `mage
// addpeertogroup <thisPeerID> relay` for every node needing its relay --
// see shmevent.EventPublicAccess for the design.
// Usage: mage requestpublicaccess <targetAddr> [note]
func RequestPublicAccess(targetAddr string, note string) error {
	instanceID, err := kvctl.RequestPublicAccess(targetAddr, note)
	if err != nil {
		return err
	}
	fmt.Printf("✅ public access requested (instance %s)\n", instanceID)
	return nil
}

// EnablePublicAccess implements `mage enablepublicaccess`: seeds the
// "public" Group + "public-access" Command + their link on the current
// node's cluster, so strangers' `requestpublicaccess` submissions against
// it succeed. Needed on any cluster bootstrapped before pkg/daemon started
// seeding these itself, and the way to re-open enrollment on one where it
// was deliberately closed. Idempotent; the current node must be a voter.
// Usage: mage enablepublicaccess
func EnablePublicAccess() error {
	if err := kvctl.EnablePublicAccess(); err != nil {
		return err
	}
	fmt.Println("✅ public access enabled -- strangers may now submit the 'public-access' command")
	return nil
}

// DialSubmitCommand implements `mage dialsubmitcommand <targetAddr>
// <commandID> [inputsJSON]`: asks the current node to dial targetAddr
// directly and submit commandID there as a write into *that* cluster's own
// log -- RequestPublicAccess generalized from one hardcoded command to
// any commandID, no raft membership in targetAddr's cluster required,
// provided commandID is linked to a public group there. See
// shmevent.EventDialSubmitCommand for the design.
// Usage: mage dialsubmitcommand <targetAddr> <commandID> [inputsJSON]
func DialSubmitCommand(targetAddr, commandID, inputsJSON string) error {
	instanceID, err := kvctl.DialSubmitCommand(targetAddr, commandID, inputsJSON, "")
	if err != nil {
		return err
	}
	fmt.Printf("✅ command %q submitted to %s (instance %s)\n", commandID, targetAddr, instanceID)
	return nil
}

// DialQueryCommandLog implements `mage dialquerycommandlog <targetAddr>
// <instanceID>`: asks the current node to dial targetAddr directly and
// print back instanceID's own command-log entries -- DialSubmitCommand's
// read-back counterpart. Needs no standing in targetAddr's cluster at all:
// possessing instanceID is the credential.
// Usage: mage dialquerycommandlog <targetAddr> <instanceID>
func DialQueryCommandLog(targetAddr, instanceID string) error {
	records, err := kvctl.DialQueryCommandLog(targetAddr, instanceID, time.Time{}, time.Now(), 0)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		fmt.Println("(no records yet)")
		return nil
	}
	for _, r := range records {
		fmt.Printf("[%s] fields=%v narrative=%q\n", r.Timestamp.Format(time.RFC3339), r.Fields, r.Narrative)
	}
	return nil
}
