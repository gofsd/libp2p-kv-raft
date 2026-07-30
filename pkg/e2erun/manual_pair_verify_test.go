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
// MANUAL_PAIR_VERIFY_SERIALS="serialA,serialB" is set. Delete this file
// once verification is done.
func TestManualPairVerify(t *testing.T) {
	spec := os.Getenv("MANUAL_PAIR_VERIFY_SERIALS")
	if spec == "" {
		t.Skip("set MANUAL_PAIR_VERIFY_SERIALS=serialA,serialB to run this manually")
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

	result := runAndroidPairScenarioOn(repoRoot, serialA, serialB)
	data, _ := json.MarshalIndent(result, "", "  ")
	t.Logf("result:\n%s", data)
	if result.Status != 0 {
		t.Fatalf("pair scenario failed: %s", result.Error)
	}
}
