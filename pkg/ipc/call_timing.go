package ipc

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Where one Call's time went, phase by phase.
//
// **Every phase of Call currently fails as the same sentence.** A call that blocks for the
// caller's whole budget reports `waiting for response channel ...: context deadline exceeded`
// whether it spent that budget queued behind another caller, writing its request, or waiting on a
// daemon that never answered -- and on the optical rig it is always exactly kvctl's 10s ipcTimeout,
// which says the time went somewhere that blocks rather than somewhere that works. Four different
// bugs present identically, so three hypotheses have now been eliminated by measurement and none
// replaced them (see mes's docs/verification-log.md, 2026-09-16).
//
// This says which phase, and which event. The event type is on the line because the phases alone
// got as far as "the daemon took 45s to answer" and no further: what a caller is waiting *for*
// decides whether that is a read behind a raft commit, a commit of its own, or something else
// entirely, and those are different bugs.
//
// Off unless KVRAFT_IPC_CALL_LOG names a duration: these are the hot path, one line per slow call
// is still one line per call on a backend where every call is slow, and the default has to be
// silence. "0" reports every call, which is what a measuring run wants.
const CallTimingEnvVar = "KVRAFT_IPC_CALL_LOG"

// callPhase names one span of Call, in the order they happen.
type callPhase int

const (
	phaseLock callPhase = iota
	phaseOpenRequest
	phaseWriteRequest
	phaseAwaitResponse
	phaseReadResponse
	phaseCount
)

func (p callPhase) String() string {
	switch p {
	case phaseLock:
		return "caller-lock"
	case phaseOpenRequest:
		return "open-request"
	case phaseWriteRequest:
		return "write-request"
	case phaseAwaitResponse:
		return "await-response"
	case phaseReadResponse:
		return "read-response"
	}
	return "unknown"
}

// callTiming accumulates one Call's spans. The zero value is usable and costs one time.Now per
// phase boundary whether or not anything is ever reported.
type callTiming struct {
	started time.Time
	last    time.Time
	spans   [phaseCount]time.Duration
}

func startCallTiming() callTiming {
	now := time.Now()
	return callTiming{started: now, last: now}
}

// done closes phase p and opens the next.
func (t *callTiming) done(p callPhase) {
	now := time.Now()
	t.spans[p] = now.Sub(t.last)
	t.last = now
}

// report writes one line if the call was slow enough to be worth it, naming the phase that
// dominated. outcome is the error the call is returning, or nil -- **a failing call is reported on
// the same terms as a slow one**, because the failure this exists for is a call that blocks until
// its deadline and then blames whichever phase happened to notice.
func (t *callTiming) report(peerID string, id uint16, which string, outcome error) {
	threshold, on := callTimingThreshold()
	if !on {
		return
	}
	elapsed := time.Since(t.started)
	if elapsed < threshold {
		return
	}

	worst, worstAt := time.Duration(0), phaseLock
	var parts string
	for p := callPhase(0); p < phaseCount; p++ {
		if t.spans[p] > worst {
			worst, worstAt = t.spans[p], p
		}
		if t.spans[p] > 0 {
			parts += fmt.Sprintf(" %s=%s", p, t.spans[p].Round(time.Millisecond))
		}
	}
	status := "ok"
	if outcome != nil {
		status = "failed"
	}
	fmt.Fprintf(os.Stderr,
		"ipc timing: %s id=%d to %s %s in %s -- %s dominated (%s);%s\n",
		which, id, peerID, status, elapsed.Round(time.Millisecond),
		worstAt, worst.Round(time.Millisecond), parts)
}

var (
	callTimingOnce sync.Once
	callTimingDur  time.Duration
	callTimingOn   bool
)

// callTimingThreshold reads the env var once. Once rather than per call, unlike pkg/dispatch's
// equivalent, because this sits in the hot path itself: a getenv per IPC round trip is a cost paid
// by every caller forever to serve a diagnostic almost nobody has switched on.
func callTimingThreshold() (time.Duration, bool) {
	callTimingOnce.Do(func() {
		raw := os.Getenv(CallTimingEnvVar)
		if raw == "" {
			return
		}
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			// Ignored rather than fatal: a typo in a diagnostic must not take down a node that
			// was otherwise serving.
			return
		}
		callTimingDur, callTimingOn = d, true
	})
	return callTimingDur, callTimingOn
}
