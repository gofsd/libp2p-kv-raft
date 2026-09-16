//go:build !android

package ipc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// capture runs report and returns the line it wrote, by pointing os.Stderr at a pipe.
func capture(t *testing.T, tm *callTiming, err error) string {
	t.Helper()
	return captureStderr(t, func() { tm.report("peer-1", 7, "listRange", err) })
}

// Off by default. These sit in the hot path of every IPC round trip, and a backend where every
// call is slow would otherwise write one line per call.
func TestCallTimingIsSilentUnlessAskedFor(t *testing.T) {
	resetCallTiming(t, "")
	tm := startCallTiming()
	tm.started = time.Now().Add(-time.Minute)
	tm.done(phaseAwaitResponse)
	if got := capture(t, &tm, nil); got != "" {
		t.Fatalf("reported without the env var set: %q", got)
	}
}

// The whole point: a call that blew its budget must say which phase took it. Before this, every
// phase failed as "waiting for response channel ... context deadline exceeded".
func TestASlowCallNamesThePhaseThatDominatedIt(t *testing.T) {
	resetCallTiming(t, "1s")
	tm := callTiming{started: time.Now().Add(-10 * time.Second)}
	tm.spans[phaseLock] = 9500 * time.Millisecond
	tm.spans[phaseOpenRequest] = 2 * time.Millisecond
	tm.spans[phaseAwaitResponse] = 400 * time.Millisecond

	line := capture(t, &tm, nil)
	if !strings.Contains(line, "caller-lock dominated") {
		t.Errorf("did not name the dominating phase: %s", line)
	}
	for _, want := range []string{"listRange", "id=7", "peer-1", "caller-lock=9.5s", "await-response=400ms", "ok"} {
		if !strings.Contains(line, want) {
			t.Errorf("missing %q: %s", want, line)
		}
	}
}

// A failing call is reported on the same terms as a slow one -- the failure this exists for is a
// call that blocks until its deadline, so reporting only successes would miss every case of it.
func TestAFailedCallIsReportedToo(t *testing.T) {
	resetCallTiming(t, "1s")
	tm := callTiming{started: time.Now().Add(-10 * time.Second)}
	tm.spans[phaseAwaitResponse] = 10 * time.Second

	line := capture(t, &tm, errors.New("context deadline exceeded"))
	if !strings.Contains(line, "failed") {
		t.Errorf("a failed call was not marked as one: %s", line)
	}
	if !strings.Contains(line, "await-response dominated") {
		t.Errorf("did not localise the failure: %s", line)
	}
}

// A quick call says nothing even when reporting is on, or a measuring run drowns in its own output.
func TestAQuickCallIsStillSilent(t *testing.T) {
	resetCallTiming(t, "1s")
	tm := startCallTiming()
	tm.done(phaseAwaitResponse)
	if got := capture(t, &tm, nil); got != "" {
		t.Fatalf("a fast call reported: %q", got)
	}
}

// "0" means report everything, which is what a deliberate measurement wants.
func TestZeroReportsEveryCall(t *testing.T) {
	resetCallTiming(t, "0")
	tm := startCallTiming()
	tm.done(phaseLock)
	if got := capture(t, &tm, nil); got == "" {
		t.Fatal("KVRAFT_IPC_CALL_LOG=0 should report every call")
	}
}

// A typo must not change behaviour and must never be fatal.
func TestAnUnparseableCallThresholdStaysOff(t *testing.T) {
	for _, raw := range []string{"soon", "-1s", "10", "3,5s"} {
		resetCallTiming(t, raw)
		if _, on := callTimingThreshold(); on {
			t.Errorf("%q switched reporting on", raw)
		}
	}
}

// End to end: a real Call that blocks because another caller holds the lock must report
// caller-lock, not await-response. This is the exact misattribution the rig has been showing --
// every phase failing as "waiting for response channel ...: context deadline exceeded" -- so the
// reporting type being right is not enough; the wiring has to be right too.
func TestARealCallBlockedOnTheLockReportsTheLock(t *testing.T) {
	resetCallTiming(t, "100ms")
	peerID := "phasewiring-test"
	dataDir := t.TempDir()
	registerTestNode(t, peerID, dataDir)

	release := holdForPeer(t, peerID, 2*time.Second)
	defer release()

	req, err := shmevent.NewGetFieldByKey([]byte("k"))
	if err != nil {
		t.Fatalf("NewGetFieldByKey: %v", err)
	}
	req.SetId(99)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	line := captureStderr(t, func() {
		_, _ = Call(ctx, peerID, req, nil)
	})

	if !strings.Contains(line, "caller-lock dominated") {
		t.Fatalf("a call that spent its whole budget queued did not say so:\n%s", line)
	}
	// The event type is what turns "the daemon was slow" into "the daemon was slow answering
	// *this*", which is the difference between a finding and a lead.
	if !strings.Contains(line, "getFieldByKey") {
		t.Errorf("the line does not say which event was waiting:\n%s", line)
	}
	if !strings.Contains(line, "failed") {
		t.Errorf("the call failed but was not reported as failed:\n%s", line)
	}
	if strings.Contains(line, "await-response dominated") {
		t.Errorf("still blaming the response wait for time spent on the lock:\n%s", line)
	}
}
