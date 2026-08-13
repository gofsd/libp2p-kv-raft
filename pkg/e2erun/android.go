package e2erun

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// androidGoPackage is mobile/kvmobile's import path, for the -X ldflags
// gomobile bind needs to bake an identity/leader into the AAR.
const androidGoPackage = "github.com/gofsd/libp2p-kv-raft/mobile/kvmobile"

// androidAppID/androidTestRunner match android-app/app/build.gradle.kts's
// applicationId and testInstrumentationRunner -- AGP's default test APK id
// is "<applicationId>.test" (no testApplicationId override is set there).
//
// androidUITestClass names UiCommandE2ETest -- no longer driven from this automated pipeline (see
// runAndroidRows' doc comment), but still the class pkg/e2erun/android_optical.go's
// runOpticalMethod invokes one @Test method of at a time, from the separate, manual/
// hardware-gated TestManualOpticalScan.
const (
	androidAppID       = "com.gofsd.kvdemo"
	androidTestAppID   = androidAppID + ".test"
	androidTestClass   = androidAppID + ".E2ETest"
	androidUITestClass = androidAppID + ".UiCommandE2ETest"
	androidTestRunner  = androidTestAppID + "/androidx.test.runner.AndroidJUnitRunner"
)

// androidUnavailable checks that gomobile, adb, and a connected
// device/emulator are all present, returning a human-readable reason if
// not -- so a pipeline run on a machine without Android tooling degrades
// to a clear Skipped status instead of failing hard (see runRow's android
// case) or, worse, hanging on a tool that was never going to answer.
func androidUnavailable() string {
	if _, err := exec.LookPath("gomobile"); err != nil {
		return "gomobile not found on PATH"
	}
	if _, err := exec.LookPath("adb"); err != nil {
		return "adb not found on PATH"
	}
	out, err := exec.Command("adb", "devices").Output()
	if err != nil {
		return fmt.Sprintf("adb devices: %v", err)
	}
	if !hasConnectedDevice(string(out)) {
		return "no adb device/emulator connected (see `adb devices` / start an AVD)"
	}
	return ""
}

// hasConnectedDevice parses `adb devices` output for at least one line
// ending in "\tdevice" (as opposed to "\toffline"/"\tunauthorized", or the
// header line).
func hasConnectedDevice(adbDevicesOutput string) bool {
	return len(parseConnectedSerials(adbDevicesOutput)) > 0
}

// parseConnectedSerials returns every serial in adbDevicesOutput whose
// state line ends "\tdevice" (as opposed to "\toffline"/"\tunauthorized",
// or the header line) -- hasConnectedDevice's list-returning counterpart,
// for a caller that needs to address more than one connected
// device/emulator by serial rather than just checking "is there at least
// one."
func parseConnectedSerials(adbDevicesOutput string) []string {
	var serials []string
	for line := range strings.SplitSeq(adbDevicesOutput, "\n") {
		line = strings.TrimRight(line, "\r")
		if rest, ok := strings.CutSuffix(line, "\tdevice"); ok {
			serials = append(serials, rest)
		}
	}
	return serials
}

// connectedAndroidSerials runs `adb devices` and returns every currently
// connected device/emulator's serial (see parseConnectedSerials) -- unlike
// androidUnavailable/hasConnectedDevice, which only ever answer "is there
// at least one," this is what lets runAndroidRows deterministically pick
// one to address when more than one is connected (see its own doc
// comment).
func connectedAndroidSerials() ([]string, error) {
	out, err := exec.Command("adb", "devices").Output()
	if err != nil {
		return nil, fmt.Errorf("adb devices: %w", err)
	}
	return parseConnectedSerials(string(out)), nil
}

// withSerial sets cmd.Env so every adb/gradlew invocation this package
// makes routes to a specific device/emulator when more than one might be
// connected (ANDROID_SERIAL is honored by both adb directly and by
// gradlew's own shelled-out adb calls). serial == "" (this file's own call
// sites, which only ever address one device at a time) leaves cmd.Env
// untouched -- exactly the behavior of letting adb/gradlew fall back to
// whatever single device is connected. pkg/e2erun/android_optical.go's own
// two-device orchestration always passes an explicit serial instead, since
// it addresses two devices independently in the same run.
func withSerial(cmd *exec.Cmd, serial string) {
	if serial == "" {
		return
	}
	cmd.Env = append(os.Environ(), "ANDROID_SERIAL="+serial)
}

// runAndroidRows runs every PlatformAndroid row in rowIndices, grouped by
// node: one gomobile bind + gradle install + instrumented test run per
// distinct android node (covering every row that node has, in order),
// since rebuilding/reinstalling per row would be prohibitively slow --
// unlike desktop/remote rows, which each get their own real dispatch, or
// web rows, which share one Playwright-driven verdict per whole run (see
// web.go). Returns a result for every android row index in rowIndices.
//
// This only ever drives E2ETest's raw sendEvent row replay now -- the same
// cross-platform File.Rows protocol-parity check desktop/web rows also run,
// proving the wire protocol behaves identically on every platform's own
// binding. UiCommandE2ETest's catalog-driven UI walk used to also run from
// here (see git history); it's been replaced by the real-camera two-device
// optical suite (pkg/e2erun/android_optical.go), which is manual/hardware-
// gated and so deliberately isn't part of this automated pipeline at all
// (see TestManualOpticalScan's own doc comment).
func runAndroidRows(repoRoot string, f *e2edata.File, rowIndices []int, bootstrapMultiaddr string) (map[int]rowOutcome, error) {
	results := map[int]rowOutcome{}

	byNode := map[int][]int{}
	for _, idx := range rowIndices {
		row := f.Rows[idx]
		node, ok := f.Nodes[row.Node]
		if !ok || node.Platform != e2edata.PlatformAndroid {
			continue
		}
		byNode[row.Node] = append(byNode[row.Node], idx)
	}
	if len(byNode) == 0 {
		return results, nil
	}

	if reason := androidUnavailable(); reason != "" {
		for _, idxs := range byNode {
			for _, idx := range idxs {
				results[idx] = rowOutcome{status: e2edata.StatusSkipped, errMsg: "android e2e skipped: " + reason}
			}
		}
		return results, nil
	}

	// Pick one serial explicitly, even though androidUnavailable already
	// confirmed at least one is connected: leaving every adb/gradlew call
	// unqualified (serial "") only ever worked because exactly one device
	// was ever connected in practice, and a second device left connected
	// for other reasons (e.g. an optical-suite rig) would otherwise make an
	// unqualified adb call fail outright with "more than one
	// device/emulator". Deterministically using the first one keeps this
	// existing single-device path's own behavior/target unchanged whenever
	// there's still only one connected, the common case.
	serials, err := connectedAndroidSerials()
	if err != nil {
		return results, fmt.Errorf("e2erun: %w", err)
	}
	serial := ""
	if len(serials) > 0 {
		serial = serials[0]
	}

	for nodeID, idxs := range byNode {
		node := f.Nodes[nodeID]
		out := runAndroidNode(repoRoot, node, bootstrapMultiaddr, f, idxs, serial)
		maps.Copy(results, out)
	}
	return results, nil
}

// runAndroidNode builds an AAR baked with node's identity and
// bootstrapMultiaddr as the leader to join, installs the app + its
// instrumented test APK, and runs every row in rowIdxs (in order) in one
// E2ETest instrumentation invocation.
//
// A build/install/instrument-invocation-level failure marks every row in
// this batch failed with that same error, since none of them can be
// trusted to have run against a working app.
func runAndroidNode(repoRoot string, node e2edata.Node, bootstrapMultiaddr string, f *e2edata.File, rowIdxs []int, serial string) map[int]rowOutcome {
	fail := func(err error) map[int]rowOutcome {
		out := make(map[int]rowOutcome, len(rowIdxs))
		for _, idx := range rowIdxs {
			out[idx] = rowOutcome{status: e2edata.StatusFail, errMsg: err.Error()}
		}
		return out
	}

	if err := buildAndroidAAR(repoRoot, node, bootstrapMultiaddr, serial); err != nil {
		return fail(err)
	}
	if err := gradleInstall(repoRoot, serial); err != nil {
		return fail(err)
	}

	eventJSONs := make([]string, len(rowIdxs))
	for i, idx := range rowIdxs {
		ev := f.Rows[idx].Event
		if ev.Op == "bootstrap_or_join_cluster" && ev.Fields["leader_addr"] == BootstrapToken {
			// This row's own join is actually a no-op by the time it
			// runs: Kvmobile.start (E2ETest.kt, called once before any
			// row) already joined this device via buildAndroidAAR's
			// joinSuffrage=learner ldflag, which is what really matters
			// -- see that function's doc comment for why the device must
			// join as a non-voting learner rather than a voter against
			// this long-lived, shared, never-torn-down leader (quorum
			// loss when an ephemeral voter disconnects, confirmed
			// directly). Marked the same way here too regardless, purely
			// so this recorded row keeps meaning what it says (a learner
			// join, matching how the device actually joined) rather than
			// silently mismatching it.
			resolved := ResolveBootstrapPlaceholder(ev.Fields["leader_addr"], bootstrapMultiaddr) + " learner"
			ev = withField(ev, "leader_addr", resolved)
		}
		ev = expandEventFields(ev, ExpandRowValue)
		data, err := json.Marshal(ev)
		if err != nil {
			return fail(fmt.Errorf("e2erun: encode android row event: %w", err))
		}
		eventJSONs[i] = string(data)
	}
	rowsArg, err := json.Marshal(eventJSONs)
	if err != nil {
		return fail(fmt.Errorf("e2erun: encode android rows argument: %w", err))
	}

	resultsJSON, err := runInstrumentedTest(string(rowsArg), serial)
	if err != nil {
		return fail(err)
	}
	out, err := parseRowResults(resultsJSON, rowIdxs, "android instrumented test")
	if err != nil {
		return fail(err)
	}
	return out
}

// buildAndroidAAR runs `gomobile bind` for mobile/kvmobile, baking node's
// identity and bootstrapMultiaddr as the leader to join, and writes the
// result to android-app/app/libs/kvmobile.aar -- the exact path
// android-app/app/build.gradle.kts's `implementation(files("libs/kvmobile.aar"))`
// expects (see README.md's "Follower on Android" section for the manual
// equivalent of this same command). serial is passed through to withSerial
// -- gomobile bind itself never touches a device, but see runAndroidNode's
// callers for why every step of a build+install+run cycle takes it anyway
// (a consistent signature makes it obvious at each call site whether that
// step is device-specific).
func buildAndroidAAR(repoRoot string, node e2edata.Node, bootstrapMultiaddr string, serial string) error {
	aarPath := filepath.Join(repoRoot, "android-app", "app", "libs", "kvmobile.aar")
	if err := os.MkdirAll(filepath.Dir(aarPath), 0o755); err != nil {
		return err
	}
	// joinSuffrage=learner (see mobile/kvmobile's own doc comment on that
	// var) makes Kvmobile.start's automatic join -- which is what actually
	// admits this device, before any row ever runs (see runAndroidNode's
	// doc comment on why the row-level "add" event's own suffrage marker
	// below is otherwise redundant) -- a Nonvoter instead of a Voter.
	ldflags := fmt.Sprintf(
		"-X %[1]s.leaderMultiaddr=%[2]s -X %[1]s.relayMultiaddr=%[2]s -X %[1]s.identitySeedHex=%[3]s -X %[1]s.joinSuffrage=learner",
		androidGoPackage, bootstrapMultiaddr, node.PrivateKey,
	)
	cmd := exec.Command("gomobile", "bind", "-target=android", "-androidapi", "26",
		"-ldflags", ldflags, "-o", aarPath, "./mobile/kvmobile")
	cmd.Dir = repoRoot
	withSerial(cmd, serial)
	if err := runCaptured(cmd, "gomobile bind"); err != nil {
		return err
	}
	return nil
}

// gradleInstall builds and installs both the app and its instrumented
// test APK onto whatever adb device/emulator is connected, or -- if
// serial is non-empty -- specifically that device (see withSerial).
func gradleInstall(repoRoot string, serial string) error {
	androidDir := filepath.Join(repoRoot, "android-app")
	gradlew := filepath.Join(androidDir, "gradlew")
	cmd := exec.Command(gradlew, "installDebug", "installDebugAndroidTest")
	cmd.Dir = androidDir
	withSerial(cmd, serial)
	return runCaptured(cmd, "gradlew installDebug")
}

// capturedOutputLimit bounds how much of a failing command's combined
// output gets embedded in the returned error (and so recorded into
// testdata.json) -- long enough to carry the actual failure (e.g. a real
// device's package-manager rejection reason), short enough not to dump an
// entire multi-thousand-line Gradle build log into the test row.
const capturedOutputLimit = 2000

// runCaptured runs cmd, tee-ing its combined output to this process's
// stderr (for live visibility exactly like every other exec.Command in
// this package) while also capturing it, so a failure's returned error
// carries the actual diagnostic content instead of just "exit status 1" --
// a real bug caught by this producing exactly that useless message when
// android-app/app/build.gradle.kts's installDebugAndroidTest failed with a
// real, specific, otherwise-nowhere-recorded package-manager error. See
// summarizeFailure for why this takes the *front* of Gradle's "FAILURE:"
// section rather than just the tail of the whole log.
func runCaptured(cmd *exec.Cmd, label string) error {
	var buf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stderr, &buf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &buf)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("e2erun: %s: %w: %s", label, err, summarizeFailure(buf.String()))
	}
	return nil
}

// summarizeFailure extracts the useful part of a failing command's output
// for embedding in an error message. Gradle's own failures put the actual
// one-line cause (e.g. "INSTALL_FAILED_USER_RESTRICTED: Install canceled
// by user") in a short "FAILURE: Build failed..." / "* What went wrong:"
// section near the *start* of its error report, followed by a multi-
// thousand-line Java stack trace and "* Try:"/"* Get more help" boilerplate
// at the end -- so blindly taking the last N bytes of the full output (an
// earlier version of this function did exactly that) captures only stack
// trace noise and cuts off before ever reaching the real cause. Falls back
// to the last capturedOutputLimit bytes of the whole output for commands
// (e.g. gomobile bind) that don't follow this convention.
func summarizeFailure(out string) string {
	if idx := strings.Index(out, "FAILURE: "); idx >= 0 {
		out = out[idx:]
		if end := strings.Index(out, "\n* Try:"); end >= 0 {
			out = out[:end]
		}
		if len(out) > capturedOutputLimit {
			out = out[:capturedOutputLimit] + "..."
		}
		return strings.TrimSpace(out)
	}
	if len(out) > capturedOutputLimit {
		out = "..." + out[len(out)-capturedOutputLimit:]
	}
	return strings.TrimSpace(out)
}

// runInstrumentedTest triggers E2ETest via `adb shell am instrument`,
// passing rowsArgJSON as its "rows" instrumentation argument, then pulls
// and returns the results file it writes to the app's external files dir
// (see E2ETest.kt's doc comment on why external, not private, storage).
// The instrumentation run itself is allowed to report a JUnit failure
// (E2ETest.fail()s if any row failed) without that being treated as a Go
// error here -- the results file, not the instrumentation exit status, is
// authoritative for per-row pass/fail.
//
// rowsArgJSON is base64-encoded for the same reason runUICommandTest
// base64s its own "cases" value (see that function's doc comment): raw
// JSON's quotes and braces get mangled somewhere between `adb shell` and
// the device's `am` argument parser. That went unnoticed here for as long
// as every recorded row carried a short value -- a row carrying a value at
// shmevent.KVValueSize (see LargeValueToken) arrives truncated instead,
// and E2ETest fails parsing it with a JSONException naming a stray
// fragment rather than anything about size.
func runInstrumentedTest(rowsArgJSON string, serial string) ([]byte, error) {
	cmd := exec.Command("adb", "shell", "am", "instrument", "-w",
		"-e", "class", androidTestClass,
		"-e", "rows", base64.StdEncoding.EncodeToString([]byte(rowsArgJSON)),
		androidTestRunner,
	)
	withSerial(cmd, serial)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	_ = cmd.Run()

	deviceResultsPath := fmt.Sprintf("/sdcard/Android/data/%s/files/e2e_results.json", androidAppID)
	localResultsPath := filepath.Join(os.TempDir(), "kvraft-e2e-android-results.json")
	defer os.Remove(localResultsPath)
	pull := exec.Command("adb", "pull", deviceResultsPath, localResultsPath)
	withSerial(pull, serial)
	var pullOut bytes.Buffer
	pull.Stdout = &pullOut
	pull.Stderr = &pullOut
	if err := pull.Run(); err != nil {
		return nil, fmt.Errorf("e2erun: pull android results (instrument output: %s): %w: %s", out.String(), err, pullOut.String())
	}
	data, err := os.ReadFile(localResultsPath)
	if err != nil {
		return nil, fmt.Errorf("e2erun: read pulled android results: %w", err)
	}
	return data, nil
}
