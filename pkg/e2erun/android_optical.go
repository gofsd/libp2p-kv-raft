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

// opticalDeviceAVerifyTimeoutMs is the default budget for verifyLogContains -- see
// OpticalExpectSpec.VerifyOnDeviceA's own doc comment. Device A's own RunCommandDispatcher
// handler runs the moment B's dial actually lands, which (unlike B's own camera-decode wait)
// never depends on real-world lighting/focus, so this can stay much tighter than TimeoutMs.
const opticalDeviceAVerifyTimeoutMs = 30_000

// runOpticalScanSuite drives every test/e2e/testdata.json android_optical_cases entry against a
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
func runOpticalScanSuite(cases []e2edata.OpticalScanCase, serialA, serialB string) *e2edata.OpticalScanResult {
	result := &e2edata.OpticalScanResult{RanAt: time.Now()}
	if len(cases) == 0 {
		return result
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
	}
	genSpecs := make([]genSpecEntry, len(cases))
	expSpecs := make([]expSpecEntry, len(cases))
	for i, c := range cases {
		if (c.HoldMillis == 0) != (c.TimeoutMs == 0) {
			result.Status = e2edata.StatusFail
			result.Error = fmt.Sprintf("case %q sets only one of hold_millis/timeout_ms (%d/%d) -- set both or neither", c.CaseID, c.HoldMillis, c.TimeoutMs)
			return result
		}
		if c.TimeoutMs > c.HoldMillis {
			result.Status = e2edata.StatusFail
			result.Error = fmt.Sprintf("case %q has timeout_ms (%d) > hold_millis (%d) -- device B would still be waiting for this case's code after device A stopped showing it", c.CaseID, c.TimeoutMs, c.HoldMillis)
			return result
		}
		genSpecs[i] = genSpecEntry{c.Generate, c.CaseID, c.HoldMillis}
		expSpecs[i] = expSpecEntry{c.Expect, c.CaseID, c.TimeoutMs}
	}
	genArg, err := json.Marshal(map[string]any{"specs": genSpecs})
	if err != nil {
		result.Status, result.Error = e2edata.StatusFail, fmt.Sprintf("encode generate specs: %v", err)
		return result
	}
	expArg, err := json.Marshal(map[string]any{"specs": expSpecs})
	if err != nil {
		result.Status, result.Error = e2edata.StatusFail, fmt.Sprintf("encode expect specs: %v", err)
		return result
	}

	genDone := make(chan opticalMethodOutcome, 1)
	go func() {
		results, err := runOpticalMethod(serialA, "generateAndHoldAll", "opticalSpecs", genArg)
		genDone <- opticalMethodOutcome{results, err}
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

	bResults, bErr := runOpticalMethod(serialB, "awaitAndVerifyScanAll", "opticalExpects", expArg)

	// Block until A's own batch invocation has genuinely finished before proceeding to the
	// VerifyOnDeviceA pass below, which reuses serialA for a fresh, separate call -- the same
	// "don't start a new invocation against A while the last one is still winding down" reasoning
	// the old per-case handoff needed, just amortized over the whole run instead of every case.
	genRes := <-genDone

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
		case c.Expect.VerifyOnDeviceA != "":
			if err := verifyOnDeviceA(serialA, c, scanResult); err != nil {
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
	return result
}

// verifyOnDeviceA runs OpticalScanCase c's own VerifyOnDeviceA check -- a short, separate
// verifyLogContains invocation back on serialA (device A), for a case whose scanned effect (B
// dialing back and submitting against A's own RunCommandDispatcher) is only ever observable in
// A's own Activity Log, never B's. Substitutes "{{instance_id}}" with the real instance id
// scanResult.Output carries (device B's own CommandExecutor result for this case).
func verifyOnDeviceA(serialA string, c e2edata.OpticalScanCase, scanResult *e2edata.UICaseResult) error {
	instanceID, err := extractInstanceID(scanResult.Output)
	if err != nil {
		return fmt.Errorf("device B result had no instance_id to verify on device A: %w", err)
	}
	want := strings.ReplaceAll(c.Expect.VerifyOnDeviceA, "{{instance_id}}", instanceID)
	verifyArg, err := json.Marshal(map[string]any{"contains": want, "timeoutMs": opticalDeviceAVerifyTimeoutMs})
	if err != nil {
		return fmt.Errorf("encode verify spec: %w", err)
	}
	vResults, vErr := runOpticalMethod(serialA, "verifyLogContains", "opticalVerify", verifyArg)
	vResult := findResult(vResults, "OpticalVerify")
	if vResult == nil {
		if vErr != nil {
			return fmt.Errorf("device A verify: %w", vErr)
		}
		return fmt.Errorf("device A verify: no result produced")
	}
	if !vResult.Pass {
		return fmt.Errorf("device A verify: %s", vResult.Error)
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
}

// runOpticalMethod invokes exactly one @Test method in UiCommandE2ETest (via `-e class
// ClassName#method`, not the whole class) on serial, passing argJSON -- base64-encoded, same
// reasoning as runUICommandTest's own casesJSON -- as the argName instrumentation arg, and
// returns whatever that single method itself wrote to ui_e2e_results.json (see
// UiCommandE2ETest.kt's writeResults). Reuses that exact on-device path/pull/parse plumbing
// runUICommandTest already established, just targeting one specific method instead of the whole
// class.
func runOpticalMethod(serial, method, argName string, argJSON []byte) ([]e2edata.UICaseResult, error) {
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
	if err := pull.Run(); err != nil {
		return nil, fmt.Errorf("pull results for %s (instrument err: %v, output: %s): %w: %s\ndevice KVDemo logcat:\n%s", method, instrumentErr, out.String(), err, pullOut.String(), deviceLog)
	}
	data, err := os.ReadFile(localResultsPath)
	if err != nil {
		return nil, fmt.Errorf("read results for %s: %w", method, err)
	}
	var results []e2edata.UICaseResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("parse results for %s: %w (raw: %s)", method, err, data)
	}
	return results, nil
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
