package kvmobile

import (
	"context"
	"fmt"
)

// RequestPublicAccess submits shmevent.DefaultPublicCommandID -- the
// always-public "public-access" front door every cluster bootstraps -- to
// the cluster at targetAddr (a multiaddr), which grants this device
// Channel and circuit-relay standing there. Returns the new dispatch's
// instance id.
//
// This is the call a phone makes once at startup against whichever relay
// its build baked in (see relayMultiaddr / relayPeers in kvmobile.go). A
// relay node's circuit-relay v2 service is ACL-gated against its own
// cluster's shmevent.ReservedGroupRelay membership (pkg/daemon's
// relayACL), and a freshly installed app is a total stranger to that
// cluster -- so without this, every reservation attempt is denied and the
// device only ever advertises addresses nobody else can dial. There is no
// opt-out flag on that gate by design; self-service escalation through the
// public command is the intended door, and this is its client side. See
// shmevent.EventPublicAccess and pkg/daemon's dialAndSubmitPublicAccess.
//
// Safe and cheap to call on every launch: re-granting a membership the
// cluster already recorded is a no-op write, and the grant does not
// survive that cluster deleting the record, so re-asserting it is also how
// a device recovers if it ever was revoked. Idempotent in effect, not in
// storage -- each call does append one more CommandRequest record to the
// target's log, so call it once per start, not per operation.
//
// note is optional free text stored on the request (e.g. an app/device
// label) purely so a relay operator reading its own request log can tell
// who asked; it never affects whether the grant happens. It must not
// contain "#".
func RequestPublicAccess(targetAddr, note string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	instanceID, err := sess.PublicAccess(ctx, targetAddr, note)
	if err != nil {
		return "", fmt.Errorf("kvmobile: request public access: %w", err)
	}
	return instanceID, nil
}

// RequestRelayAccess asks the relay this device's build baked in (see
// relayPeers) for the same public-access grant RequestPublicAccess makes
// against an explicitly named target -- the common case, so an app doesn't
// have to carry the relay multiaddr in its own code just to repeat what
// the AAR was already built with. Returns an error if no relay was baked
// in at build time (the generic prebuilt AAR, "Option A"), in which case
// there is nothing to ask.
func RequestRelayAccess(note string) (string, error) {
	peers := relayPeers()
	if len(peers) == 0 {
		return "", fmt.Errorf("kvmobile: no relay multiaddr baked in at build time")
	}
	return RequestPublicAccess(peers[0], note)
}
