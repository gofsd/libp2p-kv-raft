package e2erun

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// TestManualOpticalScan is a throwaway, manually-invoked driver for live verification of
// runOpticalScanSuite against a real two-device optical rig: one device (serialA) whose screen a
// real camera is physically pointed at, and a second device (serialB) holding that camera. Not
// part of mage e2e:current/all's automatic pipeline -- it needs real hardware (specifically a
// camera aimed at a screen) no CI runner has, and this is now the *only* way any android command
// executes at all (see android-app's RunCode rewrite), not a supplementary check alongside an
// automated sweep.
//
// This never rebuilds or reinstalls anything, and never touches raft cluster membership: every
// OpticalScanCase only depends on both devices' apps already being installed and usable, not on a
// fresh identity -- run it against whatever the rig's two devices already have set up. The
// "dial_submit_command_ping" seed case in particular additionally assumes device A already has a
// "ping" command linked to a public group with RunCommandDispatcher registered for it (Dispatch:
// RunCommandDispatcher); this harness does not provision it.
//
// Persists its own result to test/e2e/testdata.json's android_optical_scan_result on every run
// (pass or fail), so "did the optical suite pass last time" has an answer on disk instead of only
// ever being visible in that one run's console output -- see OpticalScanResult's own doc comment.
//
//	MANUAL_OPTICAL_SCAN_SERIALS=<generatingDeviceSerial>,<scanningDeviceSerial> \
//	  go test ./pkg/e2erun -run TestManualOpticalScan -v
//
// Cases come from test/e2e/testdata.json's android_optical_cases -- see OpticalScanCase's own
// doc comment.
//
// MANUAL_OPTICAL_SCAN_CASES=<caseID>[,<caseID>...] narrows the run to those cases, in file order,
// for the mini-batch that is the cheap way to check a rig-level or scanner-level change before
// committing a full ~6-minute 90-case run to it (see the "3-case mini-batch" habit this harness's
// history is full of). A filtered run deliberately does *not* persist its result: overwriting
// android_optical_scan_result -- the on-disk answer to "did the whole suite pass last time" -- with
// a 3-case subset would silently destroy that answer, and a filtered run is a diagnostic, never a
// measurement of the suite.
func TestManualOpticalScan(t *testing.T) {
	spec := os.Getenv("MANUAL_OPTICAL_SCAN_SERIALS")
	if spec == "" {
		t.Skip("set MANUAL_OPTICAL_SCAN_SERIALS=generatingSerial,scanningSerial to run this manually")
	}
	parts := strings.Split(spec, ",")
	if len(parts) != 2 {
		t.Fatalf("MANUAL_OPTICAL_SCAN_SERIALS must be \"serialA,serialB\", got %q", spec)
	}
	serialA, serialB := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	repoRoot += "/../.."
	testdataPath := filepath.Join(repoRoot, e2edata.DefaultPath)

	file, err := e2edata.Load(testdataPath)
	if err != nil {
		t.Fatalf("load testdata: %v", err)
	}
	if len(file.OpticalScanCases) == 0 {
		t.Fatal("test/e2e/testdata.json has no android_optical_cases entries")
	}

	cases := file.OpticalScanCases
	only := os.Getenv("MANUAL_OPTICAL_SCAN_CASES")
	if only != "" {
		wanted := map[string]bool{}
		for _, id := range strings.Split(only, ",") {
			if id = strings.TrimSpace(id); id != "" {
				wanted[id] = true
			}
		}
		cases = nil
		for _, c := range file.OpticalScanCases {
			if wanted[c.CaseID] {
				cases = append(cases, c)
				delete(wanted, c.CaseID)
			}
		}
		if len(wanted) > 0 {
			t.Fatalf("MANUAL_OPTICAL_SCAN_CASES names case(s) android_optical_cases has no entry for: %v", slices.Sorted(maps.Keys(wanted)))
		}
		t.Logf("running %d of %d case(s) (MANUAL_OPTICAL_SCAN_CASES=%s) -- this result will NOT be persisted", len(cases), len(file.OpticalScanCases), only)
	}

	result := runOpticalScanSuite(cases, serialA, serialB)
	data, _ := json.MarshalIndent(result, "", "  ")
	t.Logf("result:\n%s", data)

	if only == "" {
		file.OpticalScanResult = result
		if err := file.Save(testdataPath); err != nil {
			t.Fatalf("save testdata (result was: %s): %v", result.Error, err)
		}
	}

	if result.Status != e2edata.StatusPass {
		t.Fatalf("optical scan suite failed: %s", result.Error)
	}
}
