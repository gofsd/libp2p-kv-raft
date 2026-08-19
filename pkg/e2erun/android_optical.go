package e2erun

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// opticalDeviceVerifyTimeoutMs is the default budget for one verifyLogContains pass -- see
// OpticalExpectSpec.VerifyOnDeviceA/VerifyOnDeviceB's own doc comments. Whatever these checks
// wait on runs the moment the effect actually lands (device A's RunCommandDispatcher handler when
// B's dial or a scheduled fire arrives, device B's own scheduler callback when it fires), which
// unlike B's camera-decode wait never depends on real-world lighting/focus, so this can stay much
// tighter than TimeoutMs. It is also generous in practice: both passes run after *both* devices'
// batch invocations have finished, against an OutputLog already persisted to disk, so the entry
// is normally sitting there before the poll even starts.
const opticalDeviceVerifyTimeoutMs = 30_000

// opticalCrashRetries is how many extra times runOpticalScanSuite re-runs the whole batch after an
// app process on one of the two devices *crashed* mid-run -- not after a case failed, which is a
// real result and never retried.
//
// A crash is categorically different from a failing case: it takes down the instrumentation with
// it, so every case after it reports "device A never produced a result" regardless of whether it
// would have passed, and the run says nothing about the 90 cases it was meant to measure. Both
// devices have been seen to do it, once each in roughly ten runs, in ways nothing on this side can
// prevent or catch: device B's app died on Compose's own main thread (an uncaught
// IllegalStateException out of a layout pass), and device A's died inside the Go runtime
// ("bulkBarrierPreWrite: unaligned arguments", the gomobile/JNI hazard UiCommandE2ETest.kt's own
// signal-listener doc comment describes). Re-running costs one batch and turns a result that
// measured nothing into one that measures what it was asked to.
const opticalCrashRetries = 1

// How long stopAppProcesses waits for a force-stopped app process to actually disappear, and how
// often it re-checks.
const (
	appStopTimeout      = 30 * time.Second
	appStopPollInterval = 500 * time.Millisecond
)

func runOpticalScanSuite(cases []e2edata.OpticalScanCase, serialA, serialB string) *e2edata.OpticalScanResult {
	for attempt := 0; ; attempt++ {
		result, crashed := runOpticalScanBatch(cases, serialA, serialB)
		// Attempts counts batches spent, not batches retried, so a clean run records 1 rather than
		// omitting the field -- see OpticalScanResult.Attempts for why a retried pass and a
		// first-try pass must not persist identically.
		result.Attempts = attempt + 1
		if !crashed || attempt >= opticalCrashRetries {
			return result
		}
		fmt.Fprintf(os.Stderr, "e2erun: optical: an app process crashed mid-batch, so this run measured nothing -- re-running the whole batch (attempt %d of %d)\n", attempt+2, opticalCrashRetries+1)
	}
}

// runOpticalScanBatch drives every test/e2e/testdata.json android_optical_cases entry against a
// real two-device optical rig -- serialA the code-generating device (an emulator or any device,
// screen visible to serialB's camera), serialB the physically scanning device (its camera must
// already be aimed at serialA's screen -- this package has no way to verify that itself, the same
// hardware-setup assumption pkg/e2erun/android_pair.go's TestManualPairVerify used to make about
// USB connections already being in place). Never touches raft cluster membership or reinstalls
// anything -- every case only depends on both devices' apps already being installed and usable
// (see TestManualOpticalScan's own doc comment), so cases run in file order with no cross-case
// state threading and no per-run rebuild/reinstall step.
//
// Runs the *whole* case list as one long-lived instrumentation invocation per device --
// generateAndHoldAll on serialA, awaitAndVerifyScanAll on serialB -- rather than one
// generateAndHold/awaitAndVerifyScan pair per case. An earlier version launched a fresh
// instrumentation process (so a fresh Android app process, a fresh Kvmobile session bootstrap)
// per case; confirmed live that cost real wall-clock time on top of the actual navigation/scan
// work, multiplied by every case in the run. Batching both loops into one session each removes
// that relaunch overhead entirely -- what stays per-case is only the UI work and the camera scan
// itself.
//
// serialA is started first, not serialB: an earlier version of this function started B first and
// held A back until B signalled ready, reasoning that B's own RequestRelayAccess (re-establishing
// a relay reservation from scratch every process launch) can take 20s+ and A would otherwise race
// ahead. That direction was backwards -- confirmed live: B's own automatic join is *to A*, so
// starting B before A is even running just makes B's join fail outright ("all dials failed", A's
// identity unreachable), the same failure this project hit provisioning the rig by hand in the
// first place. A's own StartSolo bootstrap needs no one else already running and is fast (~2s
// live); B's subsequent join, once A is actually up, is what can take a while. The real
// synchronization need is the other way around: give device B's slower one-time setup a wide
// window to land on the *first* case specifically (UiCommandE2ETest.kt's generateAndHoldAll holds
// case index 0 far longer than the rest for exactly this), rather than gating either device's
// startup on the other's readiness signal.
func runOpticalScanBatch(cases []e2edata.OpticalScanCase, serialA, serialB string) (*e2edata.OpticalScanResult, bool) {
	result := &e2edata.OpticalScanResult{RanAt: time.Now()}
	if len(cases) == 0 {
		return result, false
	}

	// HoldMillis/TimeoutMs travel with every case, so a case whose code is genuinely harder to
	// read optically can buy itself more time without slowing the other 89 down (see
	// OpticalScanCase's own doc comments, and UiCommandE2ETest.kt's BATCH_HOLD_MILLIS for the
	// defaults a case that sets neither falls back to). The hold >= timeout invariant is checked
	// here rather than on the device: a violation makes device A move on to the next case while
	// B is still waiting for this one, which desynchronizes the whole batch and reports as a
	// cascade of unrelated per-case timeouts -- far cheaper to reject up front than to diagnose
	// from two devices' logs after a ten-minute run.
	type genSpecEntry struct {
		e2edata.OpticalGenerateSpec
		CaseID     string `json:"case_id"`
		HoldMillis int64  `json:"hold_millis,omitempty"`
	}
	type expSpecEntry struct {
		e2edata.OpticalExpectSpec
		CaseID    string `json:"case_id"`
		TimeoutMs int64  `json:"timeout_ms,omitempty"`
		// Category/Name name the command whose OutputLog entry is this case's own result,
		// copied from Generate rather than described separately -- they are how
		// UiCommandE2ETest's awaitOneCase picks *its* entry out of the log instead of
		// reading whatever landed last. Device B needs them because something other than
		// the case under test can write to that log while the case runs: a Cron: Serve
		// case leaves a scheduler running, and its fire/skip notifications go through
		// OutputLog.append, which records a bare INFO line with no category/name at all.
		// Measured on the rig: a Test: SleepMillis 20000 case took the scheduler's entry
		// as its own after 0.45s, and the sleep's real entry, arriving 20s later, was then
		// read by whichever case happened to be waiting -- three cases downstream, which
		// failed on a result belonging to a command it never ran.
		Category string `json:"category,omitempty"`
		Name     string `json:"name,omitempty"`
	}
	genSpecs := make([]genSpecEntry, len(cases))
	expSpecs := make([]expSpecEntry, len(cases))
	for i, c := range cases {
		if (c.HoldMillis == 0) != (c.TimeoutMs == 0) {
			result.Status = e2edata.StatusFail
			result.Error = fmt.Sprintf("case %q sets only one of hold_millis/timeout_ms (%d/%d) -- set both or neither", c.CaseID, c.HoldMillis, c.TimeoutMs)
			return result, false
		}
		if c.TimeoutMs > c.HoldMillis {
			result.Status = e2edata.StatusFail
			result.Error = fmt.Sprintf("case %q has timeout_ms (%d) > hold_millis (%d) -- device B would still be waiting for this case's code after device A stopped showing it", c.CaseID, c.TimeoutMs, c.HoldMillis)
			return result, false
		}
		genSpecs[i] = genSpecEntry{c.Generate, c.CaseID, c.HoldMillis}
		expSpecs[i] = expSpecEntry{c.Expect, c.CaseID, c.TimeoutMs, c.Generate.Category, c.Generate.Name}
	}
	genArg, err := json.Marshal(map[string]any{"specs": genSpecs})
	if err != nil {
		result.Status, result.Error = e2edata.StatusFail, fmt.Sprintf("encode generate specs: %v", err)
		return result, false
	}
	expArg, err := json.Marshal(map[string]any{"specs": expSpecs})
	if err != nil {
		result.Status, result.Error = e2edata.StatusFail, fmt.Sprintf("encode expect specs: %v", err)
		return result, false
	}

	genDone := make(chan opticalMethodOutcome, 1)
	go func() {
		results, _, crashed, err := runOpticalMethod(serialA, "generateAndHoldAll", "opticalSpecs", genArg)
		genDone <- opticalMethodOutcome{results, err, crashed}
	}()

	// A fixed head start rather than a peeked readiness signal: there's nothing meaningful to
	// peek for here the way B's own join has an "OpticalReady" moment worth waiting on -- A's
	// first observable output (its case-0 "OpticalGenerate" entry) is written well before the
	// thing B actually needs, so waiting on it would prove nothing.
	//
	// What B needs is not merely that A's process is up, but that A's raft has *won its
	// election*: B's app launches into an automatic Kvmobile.start() join, and a join reaching A
	// while it's still a Follower is refused outright ("leader rejected join: ERR: not leader").
	// That failure is fatal to the entire run, not just to B's first case -- with no session, B
	// can't open the case-signal channel either, so A hits its own no-signal ceiling and aborts
	// the batch (see UiCommandE2ETest.kt's generateAndHoldAll).
	//
	// This was 2 seconds, justified as "A's own StartSolo bootstrap is fast (~2s live)". That
	// measured a *fresh* bootstrap, which becomes leader immediately. Once A has persisted raft
	// state (true from its second run onward, and always true for a rig that isn't wiped between
	// runs), A instead *resumes* that configuration and has to wait out a heartbeat timeout and
	// run a real election first -- with the widened mobile raft timeouts (kvmobile's own
	// raftHeartbeatTimeout/raftElectionTimeout, 4s each) that measured 7.5s live, from
	// "initial configuration" to "entering leader state". So the old 2s raced A's election on
	// every run against an already-provisioned rig. 25s leaves real margin over that 7.5s for a
	// slower device, and costs nothing meaningful against a run whose first case already holds
	// for FIRST_CASE_HOLD_MILLIS.
	time.Sleep(25 * time.Second)

	bResults, bLog, bCrashed, bErr := runOpticalMethod(serialB, "awaitAndVerifyScanAll", "opticalExpects", expArg)

	// Block until A's own batch invocation has genuinely finished before proceeding to the
	// VerifyOnDeviceA pass below, which reuses serialA for a fresh, separate call -- the same
	// "don't start a new invocation against A while the last one is still winding down" reasoning
	// the old per-case handoff needed, just amortized over the whole run instead of every case.
	genRes := <-genDone

	result.Stalls = strings.Count(bLog, opticalStallMarker)

	var failed []string
	for _, c := range cases {
		entry := e2edata.OpticalScanCaseResult{CaseID: c.CaseID, Pass: true}

		genResult := findResult(genRes.results, "OpticalGenerate: "+c.CaseID)
		scanResult := findResult(bResults, "OpticalScan: "+c.CaseID)
		switch {
		case genResult == nil && genRes.err != nil:
			entry.Pass, entry.Error = false, fmt.Sprintf("device A: %v", genRes.err)
		case genResult == nil:
			entry.Pass, entry.Error = false, "device A never produced a result (batch may have stopped early)"
		case !genResult.Pass:
			entry.Pass, entry.Error = false, "device A: "+genResult.Error
		case scanResult == nil && bErr != nil:
			entry.Pass, entry.Error = false, fmt.Sprintf("device B: %v", bErr)
		case scanResult == nil:
			entry.Pass, entry.Error = false, "device B never produced a result (batch may have stopped early)"
		case !scanResult.Pass:
			entry.Pass, entry.Error = false, "device B: "+scanResult.Error
		case c.Expect.VerifyOnDeviceA != "" || c.Expect.VerifyOnDeviceB != "":
			// A case may name both, and then both have to pass. A scheduled dispatch is the
			// reason: it is recorded on the device whose scheduler fired *and* on the device
			// that owns the command and ran it, and checking only one of the two would leave
			// half the chain unmeasured.
			if err := verifyDeviceLogs(serialA, serialB, c, scanResult); err != nil {
				entry.Pass, entry.Error = false, err.Error()
			}
		}

		if !entry.Pass {
			failed = append(failed, fmt.Sprintf("%s: %s", c.CaseID, entry.Error))
		}
		result.Cases = append(result.Cases, entry)
	}
	if len(failed) > 0 {
		result.Status = e2edata.StatusFail
		result.Error = fmt.Sprintf("%d of %d optical scan case(s) failed:\n%s", len(failed), len(result.Cases), strings.Join(failed, "\n"))
	}
	// Only worth retrying while something actually went wrong: a batch that passed despite an app
	// dying on its way out (the instrumentation's own teardown, after the last case) has already
	// measured everything it was asked to.
	unmeasured := genRes.crashed || bCrashed || strings.Contains(result.Error, opticalCollectorGoneMarker)
	return result, unmeasured && result.Status != e2edata.StatusPass
}

// verifyDeviceLogs runs OpticalScanCase c's own VerifyOnDeviceA/VerifyOnDeviceB checks -- short,
// separate verifyLogContains invocations back on each named device, for a case whose scanned
// effect is not fully described by device B's own immediate result.
//
// Two shapes need this, for opposite reasons. A "Dispatch: DialSubmitCommand" case is observable
// only on A, since B dialing back and submitting against A's own RunCommandDispatcher leaves
// nothing behind on B. A scheduled dispatch is observable on neither at the time the case that
// started it returns: starting a scheduler looks exactly like starting one that will never fire,
// and what distinguishes them arrives asynchronously afterwards -- on B as its own scheduler
// callback, on A as the dispatch that callback triggered. Both are found by searching each
// device's whole Activity Log, which is also what makes them the reliable half of a Cron case
// (see examples/croncmd/README.md).
//
// Both substitute "{{instance_id}}" with the real instance id scanResult.Output carries (device
// B's own CommandExecutor result for this case).
func verifyDeviceLogs(serialA, serialB string, c e2edata.OpticalScanCase, scanResult *e2edata.UICaseResult) error {
	instanceID, err := extractInstanceID(scanResult.Output)
	if err != nil {
		return fmt.Errorf("device B result had no instance_id to verify against: %w", err)
	}
	if want := c.Expect.VerifyOnDeviceA; want != "" {
		if err := verifyLogOnDevice(serialA, "A", want, instanceID); err != nil {
			return err
		}
	}
	if want := c.Expect.VerifyOnDeviceB; want != "" {
		if err := verifyLogOnDevice(serialB, "B", want, instanceID); err != nil {
			return err
		}
	}
	return nil
}

// verifyLogOnDevice polls one device's own Activity Log for want, naming it as device in whatever
// it reports back so a failure says which of the two came up empty.
func verifyLogOnDevice(serial, device, want, instanceID string) error {
	want = strings.ReplaceAll(want, "{{instance_id}}", instanceID)
	verifyArg, err := json.Marshal(map[string]any{"contains": want, "timeoutMs": opticalDeviceVerifyTimeoutMs})
	if err != nil {
		return fmt.Errorf("encode verify spec: %w", err)
	}
	vResults, _, _, vErr := runOpticalMethod(serial, "verifyLogContains", "opticalVerify", verifyArg)
	vResult := findResult(vResults, "OpticalVerify")
	if vResult == nil {
		if vErr != nil {
			return fmt.Errorf("device %s verify: %w", device, vErr)
		}
		return fmt.Errorf("device %s verify: no result produced", device)
	}
	if !vResult.Pass {
		return fmt.Errorf("device %s verify: %s", device, vResult.Error)
	}
	return nil
}

func findResult(results []e2edata.UICaseResult, label string) *e2edata.UICaseResult {
	for i := range results {
		if results[i].Command == label {
			return &results[i]
		}
	}
	return nil
}

// extractInstanceID recovers the instance id DialSubmitCommand's own CommandExecutor result
// carries (device B's own awaitAndVerifyScan "run" output) -- unlike the pre-RunCode raw-event
// path, mobile/kvmobile/dialcommand.go's DialSubmitCommand returns the plain instance id string
// directly, not a JSON envelope with a fields.instance_id key, so this only needs to reject an
// empty result, not parse anything.
func extractInstanceID(commandExecutorOutput string) (string, error) {
	id := strings.TrimSpace(commandExecutorOutput)
	if id == "" {
		return "", fmt.Errorf("CommandExecutor result had no instance id: %q", commandExecutorOutput)
	}
	return id, nil
}

// opticalMethodOutcome is runOpticalMethod's own return shape, packaged so a background
// invocation's eventual result can travel over a channel -- named (rather than an anonymous
// struct literal) so its channel type can be shared between the goroutine that produces it
// (runOpticalScanSuite's generateAndHoldAll call) and the code that reads it back off the
// channel: Go channel types are only identical when their element types are identical, and a
// named type is never identical to a structurally-equal anonymous one.
type opticalMethodOutcome struct {
	results []e2edata.UICaseResult
	err     error
	crashed bool
}

// opticalCrashMarker is what `am instrument` reports when the app process under test died rather
// than the test failing -- "INSTRUMENTATION_RESULT: shortMsg=Process crashed." Matching the
// substring rather than the whole line because the runner's exact framing differs between a crash
// during a test and one during setup, and both mean the same thing here (see opticalCrashRetries).
const opticalCrashMarker = "Process crashed"

// opticalCollectorGoneMarker is what device B reports when its app can no longer receive scans at
// all -- AppRoot dispatches every scan from a single collector, and once that is gone nothing
// decoded reaches the app again for the life of that process (see UiCommandE2ETest.kt's
// SCAN_COLLECTOR_GONE, which this must stay in step with). Like a crash, and unlike a failing case,
// it says nothing about the cases it cut short, and only a fresh process can undo it -- recreating
// the Activity from inside the run was tried and does not rebuild the composition.
const opticalCollectorGoneMarker = "AppRoot's scan collector is gone"

// opticalStallMarker is what device B logs each time a code device A is already holding on screen
// failed to produce its expected UI within one look, and B re-armed for another (see
// UiCommandE2ETest.kt's awaitScannedTag, whose log line this must stay in step with). Counted into
// OpticalScanResult.Stalls: every one of these is a rebind and a lost ~10 seconds that a healthy
// camera path would not have needed, and counting them turns "the suite still passes" into a number
// that says whether it is passing comfortably or barely.
const opticalStallMarker = "not shown on look"

// stopAppProcesses force-stops the app under test and its instrumentation on serial, then waits for
// the app's process to actually be gone before returning.
//
// `am instrument` force-stops the target package itself, so this looks redundant -- it is not. The
// scanning device in this rig runs HyperOS, which *freezes* background processes rather than
// killing them, and a frozen process still holds its file locks. Measured live: a batch retry
// launched seconds after the previous batch ended, and the new daemon died in startup with
// "open store: database is locked (5) (SQLITE_BUSY)" against its own SQLite store, so the retry --
// the whole point of which is to salvage a run -- was guaranteed to fail. Waiting for the pid to
// disappear is what makes the store's lock actually available to the next process.
//
// Best-effort throughout: a device that reports no pid, or an adb that fails, just means there is
// nothing to wait for, and the invocation that follows reports any real problem itself.
func stopAppProcesses(serial string) {
	for _, pkg := range []string{androidTestAppID, androidAppID} {
		stop := exec.Command("adb", "shell", "am", "force-stop", pkg)
		withSerial(stop, serial)
		_ = stop.Run()
	}
	deadline := time.Now().Add(appStopTimeout)
	for time.Now().Before(deadline) {
		pidof := exec.Command("adb", "shell", "pidof", androidAppID)
		withSerial(pidof, serial)
		out, _ := pidof.Output()
		if strings.TrimSpace(string(out)) == "" {
			return
		}
		time.Sleep(appStopPollInterval)
	}
	fmt.Fprintf(os.Stderr, "e2erun: optical: %s's app process was still alive after %s of waiting for it to stop -- continuing anyway\n", serial, appStopTimeout)
}

// runOpticalMethod invokes exactly one @Test method in UiCommandE2ETest (via `-e class
// ClassName#method`, not the whole class) on serial, passing argJSON -- base64-encoded, same
// reasoning as runUICommandTest's own casesJSON -- as the argName instrumentation arg, and
// returns whatever that single method itself wrote to ui_e2e_results.json (see
// UiCommandE2ETest.kt's writeResults). Reuses that exact on-device path/pull/parse plumbing
// runUICommandTest already established, just targeting one specific method instead of the whole
// class.
func runOpticalMethod(serial, method, argName string, argJSON []byte) ([]e2edata.UICaseResult, string, bool, error) {
	stopAppProcesses(serial)

	deviceResultsPath := fmt.Sprintf("/sdcard/Android/data/%s/files/ui_e2e_results.json", androidAppID)
	rm := exec.Command("adb", "shell", "rm", "-f", deviceResultsPath)
	withSerial(rm, serial)
	_ = rm.Run()

	// Cleared right before the invocation, pulled right after (see below) -- this is what makes
	// every generateAndHold/awaitAndVerifyScan/verifyLogContains OPTICAL: log line (and AppRoot's
	// own AUTO:/ACTION_REQUIRED: scan-dispatch lines) visible per-invocation instead of only ever
	// showing up buried in the device's full, unbounded logcat history. Added specifically because
	// earlier runs' only failure evidence was a bare timeout or a JUnit stack trace -- neither
	// says whether a scan even happened, what it decoded, or how far generateAndHold/
	// awaitAndVerifyScan actually got before failing.
	clearLogcat := exec.Command("adb", "logcat", "-c")
	withSerial(clearLogcat, serial)
	_ = clearLogcat.Run()

	fmt.Fprintf(os.Stderr, "e2erun: optical: %s#%s starting on %s\n", androidUITestClass, method, serial)

	encodedArg := base64.StdEncoding.EncodeToString(argJSON)
	cmd := exec.Command("adb", "shell", "am", "instrument", "-w",
		"-e", "class", androidUITestClass+"#"+method,
		"-e", argName, encodedArg,
		androidTestRunner,
	)
	withSerial(cmd, serial)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	instrumentErr := cmd.Run()

	deviceLog, logErr := pullKVDemoLogcat(serial)
	if logErr != nil {
		deviceLog = fmt.Sprintf("(failed to capture device logcat: %v)", logErr)
	}
	fmt.Fprintf(os.Stderr, "e2erun: optical: %s#%s on %s finished (instrument err: %v)\n--- device KVDemo logcat ---\n%s\n--- end device logcat ---\n", androidUITestClass, method, serial, instrumentErr, deviceLog)

	localResultsPath := filepath.Join(os.TempDir(), fmt.Sprintf("kvraft-e2e-optical-%s-%s.json", method, strings.ReplaceAll(serial, ":", "_")))
	defer os.Remove(localResultsPath)
	pull := exec.Command("adb", "pull", deviceResultsPath, localResultsPath)
	withSerial(pull, serial)
	var pullOut bytes.Buffer
	pull.Stdout = &pullOut
	pull.Stderr = &pullOut
	crashed := strings.Contains(out.String(), opticalCrashMarker)
	if crashed {
		fmt.Fprintf(os.Stderr, "e2erun: optical: %s's app process crashed during %s -- whatever cases had not run yet measured nothing\n", serial, method)
	}
	if err := pull.Run(); err != nil {
		return nil, deviceLog, crashed, fmt.Errorf("pull results for %s (instrument err: %v, output: %s): %w: %s\ndevice KVDemo logcat:\n%s", method, instrumentErr, out.String(), err, pullOut.String(), deviceLog)
	}
	data, err := os.ReadFile(localResultsPath)
	if err != nil {
		return nil, deviceLog, crashed, fmt.Errorf("read results for %s: %w", method, err)
	}
	var results []e2edata.UICaseResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, deviceLog, crashed, fmt.Errorf("parse results for %s: %w (raw: %s)", method, err, data)
	}
	return results, deviceLog, crashed, nil
}

// pullKVDemoLogcat dumps serial's current KVDemo-tagged logcat buffer (see runOpticalMethod's own
// clear-before/dump-after pairing) -- everything AppRoot.kt's scan dispatch and this test's own
// generateAndHold/awaitAndVerifyScan/verifyLogContains (OPTICAL_LOG_TAG-prefixed) logged during
// the invocation that just ran.
func pullKVDemoLogcat(serial string) (string, error) {
	cmd := exec.Command("adb", "logcat", "-d", "-v", "time", "-s", "KVDemo:*")
	withSerial(cmd, serial)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("adb logcat -d: %w", err)
	}
	return string(out), nil
}
