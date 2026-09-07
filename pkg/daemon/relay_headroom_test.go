package daemon

import (
	"testing"

	v2relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// The numbers below are what a single ordinary client does through one relay,
// and they are the reason these defaults were raised on 2026-09-07. They are
// deliberately spelled out here rather than derived from the constants: a test
// that recomputes the value it is checking cannot fail when the value moves.
const (
	// A phone holds a circuit to its backend for the whole of a session and
	// opens another to whichever peer it is talking to -- the device it is
	// signalling, or the peer whose label it just scanned. Two concurrent
	// circuits is the ordinary case, not the pathological one.
	circuitsOneClientNeeds = 2

	// A test rig is a laptop, an emulator and a phone behind one NAT, each
	// reinstalling and restarting through a session; a reservation outlives
	// the peer that made it by ReservationTTL, so the slots in use at any
	// moment are several times the devices in the room.
	reservationsOneRigNeeds = 12

	// One optical batch dials the backend for each of its cases, back to back.
	dialsOneBatchMakes = 32
)

// TestRelayDefaultsLeaveRoomForOneOrdinaryClient pins the headroom rather than
// the numbers: what matters is not that MaxCircuitsPerPeer is 16, but that it
// is not below what a single client legitimately opens at once. It was 1 until
// 2026-09-07, which is under that floor, and the symptom was the second dial
// failing as `all dials failed` with nothing in the relay's own logs to say a
// limit had been reached.
func TestRelayDefaultsLeaveRoomForOneOrdinaryClient(t *testing.T) {
	limits := shmevent.DefaultRelayLimits()

	if got := int(limits.MaxCircuitsPerPeer); got < circuitsOneClientNeeds {
		t.Errorf("MaxCircuitsPerPeer = %d, which is below the %d concurrent circuits one client opens "+
			"(a backend dial and a peer dial at the same time); the second is refused", got, circuitsOneClientNeeds)
	}
	// go-libp2p picked 16 for this after its own experience of what clients do.
	// Sitting below the library's own default is worth noticing on its own.
	if got, want := int(limits.MaxCircuitsPerPeer), v2relay.DefaultResources().MaxCircuits; got < want {
		t.Errorf("MaxCircuitsPerPeer = %d, below go-libp2p's own default of %d", got, want)
	}
	if got := int(limits.MaxReservationsPerIP); got < reservationsOneRigNeeds {
		t.Errorf("MaxReservationsPerIP = %d, which is below the %d a handful of devices behind one NAT "+
			"hold across restarts (a reservation outlives its peer by ReservationTTL)", got, reservationsOneRigNeeds)
	}
}

// TestRelayQuotaAdmitsABatchOfDials pins the other half, and the one that reads
// least like a limit: relayACL.allow debits the quota bucket from AllowConnect
// as well as AllowReserve, so a client that dials a service over the relay for
// each operation spends a token per operation. The defaults were 1/sec with a
// burst of 5, which admitted five dials and then metered the rest at one a
// second -- diagnosed for months as the relay "throttling this source IP" and
// "recovering" after the rig was left alone, which is a bucket refilling.
func TestRelayQuotaAdmitsABatchOfDials(t *testing.T) {
	peerPerSec, peerBurst, ipPerSec, ipBurst := relayQuotaLimits(Config{})
	q := newQuotaTracker(peerPerSec, peerBurst, ipPerSec, ipBurst)

	for i := 0; i < dialsOneBatchMakes; i++ {
		if !q.allow("peer-a", "1.2.3.4", 1) {
			t.Fatalf("dial %d of %d denied by the default relay quota -- one client's back-to-back "+
				"dials must not be metered; peerPerSec=%v peerBurst=%d", i+1, dialsOneBatchMakes, peerPerSec, peerBurst)
		}
	}

	// Three devices behind one NAT dialling at once still share the IP bucket.
	for i := 0; i < dialsOneBatchMakes; i++ {
		for _, p := range []string{"peer-b", "peer-c", "peer-d"} {
			if !q.allow(p, "5.6.7.8", 1) {
				t.Fatalf("dial %d from %s denied by the default per-IP relay quota -- several devices "+
					"behind one gateway is the ordinary rig; ipPerSec=%v ipBurst=%d", i+1, p, ipPerSec, ipBurst)
			}
		}
	}
}
