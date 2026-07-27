package e2erun

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// androidGoPackage is mobile/kvmobile's import path, for the -X ldflags
// gomobile bind needs to bake an identity/leader into the AAR.
const androidGoPackage = "github.com/gofsd/libp2p-kv-raft/mobile/kvmobile"

// androidAppID/androidTestRunner match android-app/app/build.gradle.kts's
// applicationId and testInstrumentationRunner -- AGP's default test APK id
// is "<applicationId>.test" (no testApplicationId override is set there).
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
	for line := range strings.SplitSeq(adbDevicesOutput, "\n") {
		if strings.HasSuffix(strings.TrimRight(line, "\r"), "\tdevice") {
			return true
		}
	}
	return false
}

// runAndroidRows runs every PlatformAndroid row in rowIndices, grouped by
// node: one gomobile bind + gradle install + instrumented test run per
// distinct android node (covering every row that node has, in order),
// since rebuilding/reinstalling per row would be prohibitively slow --
// unlike desktop/remote rows, which each get their own real dispatch, or
// web rows, which share one Playwright-driven verdict per whole run (see
// web.go). Returns a result for every android row index in rowIndices.
//
// runUI additionally requests UiCommandE2ETest's own catalog-driven UI walk
// (see runAndroidNode's doc comment) using uiCases as its test plan (see
// e2edata.File.UICases) -- independent of rowIndices, since it isn't
// row-driven at all: when runUI is set, every PlatformAndroid node in f is
// visited (even one with zero rows in rowIndices, e.g. an
// E2E_TYPES=androidui-only run) purely to run it. Its outcome has no
// f.Rows entry of its own to attach to, so it comes back as a separate
// error rather than folded into the row-outcome map -- nil if runUI is
// false or every android node's UI walk passed.
func runAndroidRows(repoRoot string, f *e2edata.File, rowIndices []int, bootstrapMultiaddr string, runUI bool, uiCases map[string]e2edata.UICase) (map[int]rowOutcome, error) {
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
	if runUI {
		for nodeID, node := range f.Nodes {
			if node.Platform != e2edata.PlatformAndroid {
				continue
			}
			if _, ok := byNode[nodeID]; !ok {
				byNode[nodeID] = nil
			}
		}
	}
	if len(byNode) == 0 {
		return results, nil
	}

	if reason := androidUnavailable(); reason != "" {
		var uiErrs []error
		for nodeID, idxs := range byNode {
			if len(idxs) == 0 {
				uiErrs = append(uiErrs, fmt.Errorf("node %d: android e2e skipped: %s", nodeID, reason))
				continue
			}
			for _, idx := range idxs {
				results[idx] = rowOutcome{status: e2edata.StatusSkipped, errMsg: "android e2e skipped: " + reason}
			}
		}
		return results, errors.Join(uiErrs...)
	}

	uiCasesJSON, err := json.Marshal(uiCases)
	if err != nil {
		return results, fmt.Errorf("e2erun: encode android UI cases: %w", err)
	}

	var uiErrs []error
	for nodeID, idxs := range byNode {
		node := f.Nodes[nodeID]
		out, uiErr := runAndroidNode(repoRoot, node, bootstrapMultiaddr, f, idxs, runUI, string(uiCasesJSON))
		maps.Copy(results, out)
		if uiErr != nil {
			uiErrs = append(uiErrs, fmt.Errorf("node %s: %w", node.PeerID, uiErr))
		}
	}
	return results, errors.Join(uiErrs...)
}

// runAndroidNode builds an AAR baked with node's identity and
// bootstrapMultiaddr as the leader to join, installs the app + its
// instrumented test APK, runs every row in rowIdxs (in order, if any -- may
// be empty for a node visited purely to run UiCommandE2ETest, see
// runAndroidRows' doc comment) in one instrumentation invocation, then --
// if runUI is set -- runs UiCommandE2ETest -- a second, separate
// instrumentation invocation that clicks through every real screen
// (MainActivity/CommandListActivity/CommandDetailActivity) for every
// CommandCatalog.kt entry using uiCasesJSON as its test plan (see
// e2edata.File.UICases), proving the whole command surface is actually
// reachable/operable through the UI a real user taps, not just reachable
// via the raw sendEvent path E2ETest itself drives.
//
// A build/install/instrument-invocation-level failure marks every row in
// this batch failed with that same error (or, if rowIdxs is empty, is
// returned as this call's own error instead, since there's nothing to
// attach it to), since none of them can be trusted to have run against a
// working app. UiCommandE2ETest's own outcome is deliberately kept
// separate from rowIdxs' results, not folded in the way a build/install
// failure is: it's independent evidence (the catalog UI surface, not the
// raw wire rows this batch already verified on its own), returned as this
// call's own error.
func runAndroidNode(repoRoot string, node e2edata.Node, bootstrapMultiaddr string, f *e2edata.File, rowIdxs []int, runUI bool, uiCasesJSON string) (map[int]rowOutcome, error) {
	fail := func(err error) (map[int]rowOutcome, error) {
		if len(rowIdxs) == 0 {
			return map[int]rowOutcome{}, err
		}
		out := make(map[int]rowOutcome, len(rowIdxs))
		for _, idx := range rowIdxs {
			out[idx] = rowOutcome{status: e2edata.StatusFail, errMsg: err.Error()}
		}
		return out, nil
	}

	if err := buildAndroidAAR(repoRoot, node, bootstrapMultiaddr); err != nil {
		return fail(err)
	}
	if err := gradleInstall(repoRoot); err != nil {
		return fail(err)
	}

	out := map[int]rowOutcome{}
	if len(rowIdxs) > 0 {
		eventJSONs := make([]string, len(rowIdxs))
		for i, idx := range rowIdxs {
			ev := f.Rows[idx].Event
			if ev.EventType == shmevent.EventAdd && string(ev.Value()) == BootstrapToken {
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
				resolved := ResolveBootstrapPlaceholder(string(ev.Value()), bootstrapMultiaddr) + " learner"
				ev = e2edata.NewEvent(ev.EventType, ev.SourceID, ev.DestinationID, []byte(resolved), ev.ID)
			}
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

		resultsJSON, err := runInstrumentedTest(string(rowsArg))
		if err != nil {
			return fail(err)
		}
		out, err = parseRowResults(resultsJSON, rowIdxs, "android instrumented test")
		if err != nil {
			return fail(err)
		}
	}

	if runUI {
		if err := runUICommandTest(uiCasesJSON); err != nil {
			return out, err
		}
	}
	return out, nil
}

// buildAndroidAAR runs `gomobile bind` for mobile/kvmobile, baking node's
// identity and bootstrapMultiaddr as the leader to join, and writes the
// result to android-app/app/libs/kvmobile.aar -- the exact path
// android-app/app/build.gradle.kts's `implementation(files("libs/kvmobile.aar"))`
// expects (see README.md's "Follower on Android" section for the manual
// equivalent of this same command).
func buildAndroidAAR(repoRoot string, node e2edata.Node, bootstrapMultiaddr string) error {
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
	if err := runCaptured(cmd, "gomobile bind"); err != nil {
		return err
	}
	return nil
}

// gradleInstall builds and installs both the app and its instrumented
// test APK onto whatever adb device/emulator is connected.
func gradleInstall(repoRoot string) error {
	androidDir := filepath.Join(repoRoot, "android-app")
	gradlew := filepath.Join(androidDir, "gradlew")
	cmd := exec.Command(gradlew, "installDebug", "installDebugAndroidTest")
	cmd.Dir = androidDir
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
func runInstrumentedTest(rowsArgJSON string) ([]byte, error) {
	cmd := exec.Command("adb", "shell", "am", "instrument", "-w",
		"-e", "class", androidTestClass,
		"-e", "rows", rowsArgJSON,
		androidTestRunner,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	_ = cmd.Run()

	deviceResultsPath := fmt.Sprintf("/sdcard/Android/data/%s/files/e2e_results.json", androidAppID)
	localResultsPath := filepath.Join(os.TempDir(), "kvraft-e2e-android-results.json")
	defer os.Remove(localResultsPath)
	pull := exec.Command("adb", "pull", deviceResultsPath, localResultsPath)
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

// uiCommandResult is one entry of UiCommandE2ETest's own results file
// (ui_e2e_results.json) -- keyed by command label rather than by row
// index, since this test isn't driven by testdata.json rows at all: it
// walks the live CommandCatalog.kt list itself (see that test's own doc
// comment).
type uiCommandResult struct {
	Command string `json:"command"`
	Pass    bool   `json:"pass"`
	Error   string `json:"error"`
}

// runUICommandTest triggers UiCommandE2ETest via `adb shell am
// instrument`, passing casesJSON as its "cases" instrumentation argument
// (the JSON form of e2edata.File.UICases -- see UiCommandE2ETest.kt's own
// doc comment for how it parses this and substitutes runtime-only tokens
// like "{{selfPeerID}}"). Unlike runInstrumentedTest, this doesn't replay
// testdata.json Rows; it clicks through every CommandCatalog.kt entry's
// own screen instead, using UICases only to decide inputs/execute/expect
// per command. Pulls and parses its separate ui_e2e_results.json results
// file, returning a summarizing error if any command's UI case failed or
// if the instrumentation run never produced a results file at all (e.g. a
// crash before it could write one).
func runUICommandTest(casesJSON string) error {
	cmd := exec.Command("adb", "shell", "am", "instrument", "-w",
		"-e", "class", androidUITestClass,
		"-e", "cases", casesJSON,
		androidTestRunner,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	_ = cmd.Run()

	deviceResultsPath := fmt.Sprintf("/sdcard/Android/data/%s/files/ui_e2e_results.json", androidAppID)
	localResultsPath := filepath.Join(os.TempDir(), "kvraft-e2e-android-ui-results.json")
	defer os.Remove(localResultsPath)
	pull := exec.Command("adb", "pull", deviceResultsPath, localResultsPath)
	var pullOut bytes.Buffer
	pull.Stdout = &pullOut
	pull.Stderr = &pullOut
	if err := pull.Run(); err != nil {
		return fmt.Errorf("e2erun: pull android UI command results (instrument output: %s): %w: %s", out.String(), err, pullOut.String())
	}
	data, err := os.ReadFile(localResultsPath)
	if err != nil {
		return fmt.Errorf("e2erun: read pulled android UI command results: %w", err)
	}

	var results []uiCommandResult
	if err := json.Unmarshal(data, &results); err != nil {
		return fmt.Errorf("e2erun: parse android UI command results: %w (raw: %s)", err, data)
	}
	var failed []string
	for _, r := range results {
		if !r.Pass {
			failed = append(failed, fmt.Sprintf("%s: %s", r.Command, r.Error))
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("e2erun: %d of %d android UI command(s) failed:\n%s", len(failed), len(results), strings.Join(failed, "\n"))
	}
	return nil
}
