//go:build mage

package main

import (
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// RequestPermit lodges a pending permit record for peerID on the current
// node, forwarded to the leader like any other Set. metadata may be "" (a
// dialable multiaddr for kind "bootstrap" -- see
// shmevent.EncodePermitRequestPayload).
// Usage: mage requestpermit <kind: bootstrap> <peerID> <metadata>
func RequestPermit(kind, peerID, metadata string) error {
	k, ok := shmevent.KindFromName(kind)
	if !ok {
		return fmt.Errorf("unknown permit kind %q (want \"bootstrap\")", kind)
	}
	if err := kvctl.RequestPermit(k, []byte(peerID), metadata); err != nil {
		return err
	}
	fmt.Println("✅ permit requested")
	return nil
}

// ConfirmPermit promotes a pending permit record for peerID to confirmed.
// Only takes effect if the current node is itself a raft voter.
// Usage: mage confirmpermit <kind: bootstrap|cluster-join> <peerID>
func ConfirmPermit(kind, peerID string) error {
	k, ok := shmevent.KindFromName(kind)
	if !ok {
		return fmt.Errorf("unknown permit kind %q (want \"bootstrap\" or \"cluster-join\")", kind)
	}
	if err := kvctl.ConfirmPermit(k, []byte(peerID)); err != nil {
		return err
	}
	fmt.Println("✅ permit confirmed")
	return nil
}

// RevokePermit deletes a confirmed permit record for peerID outright.
// Only takes effect if the current node is itself a raft voter.
// Usage: mage revokepermit <kind: bootstrap> <peerID>
func RevokePermit(kind, peerID string) error {
	k, ok := shmevent.KindFromName(kind)
	if !ok {
		return fmt.Errorf("unknown permit kind %q (want \"bootstrap\")", kind)
	}
	if err := kvctl.RevokePermit(k, []byte(peerID)); err != nil {
		return err
	}
	fmt.Println("✅ permit revoked")
	return nil
}
