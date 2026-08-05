package e2erun

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestManualPairVerify is a throwaway, manually-invoked driver for live
// verification of runAndroidPairScenarioOn against two real emulators.
// Not part of the normal test suite -- skipped unless
// MANUAL_PAIR_VERIFY_SERIALS="serialA,serialB" is set.
// MANUAL_PAIR_VERIFY_BOOTSTRAP names the live VPS relay multiaddr to
// request standing from (see runAndroidPairScenarioOn's own doc comment on
// why relayMultiaddr needs a real target) -- required now that this
// scenario routes through the relay instead of adb-forward bridging alone.
// Delete this file once verification is done.
func TestManualPairVerify(t *testing.T) {
	spec := os.Getenv("MANUAL_PAIR_VERIFY_SERIALS")
	if spec == "" {
		t.Skip("set MANUAL_PAIR_VERIFY_SERIALS=serialA,serialB to run this manually")
	}
	bootstrapMultiaddr := os.Getenv("MANUAL_PAIR_VERIFY_BOOTSTRAP")
	if bootstrapMultiaddr == "" {
		t.Skip("set MANUAL_PAIR_VERIFY_BOOTSTRAP=<live bootstrap multiaddr> to run this manually")
	}
	parts := strings.Split(spec, ",")
	if len(parts) != 2 {
		t.Fatalf("MANUAL_PAIR_VERIFY_SERIALS must be \"serialA,serialB\", got %q", spec)
	}
	serialA, serialB := parts[0], parts[1]

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	repoRoot = repoRoot + "/../.."

	result := runAndroidPairScenarioOn(repoRoot, bootstrapMultiaddr, serialA, serialB)
	data, _ := json.MarshalIndent(result, "", "  ")
	t.Logf("result:\n%s", data)
	if result.Status != 0 {
		t.Fatalf("pair scenario failed: %s", result.Error)
	}
}
