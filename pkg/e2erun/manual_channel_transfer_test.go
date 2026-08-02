package e2erun

import (
	"os"
	"strconv"
	"testing"
)

// TestManualChannelFileTransfer is a throwaway, manually-invoked driver for
// live verification of RunChannelFileTransferScenario against a real
// desktop node + a real Android emulator/device -- mirrors
// manual_pair_verify_test.go's own reasoning. Not part of the normal test
// suite -- skipped unless MANUAL_CHANNEL_TRANSFER=1 is set.
// MANUAL_CHANNEL_TRANSFER_BYTES optionally overrides the default 1GiB
// payload size (e.g. to something small for a quick sanity run before
// committing to the real thing). Delete this file once verification is
// done.
func TestManualChannelFileTransfer(t *testing.T) {
	if os.Getenv("MANUAL_CHANNEL_TRANSFER") == "" {
		t.Skip("set MANUAL_CHANNEL_TRANSFER=1 (and optionally MANUAL_CHANNEL_TRANSFER_BYTES) to run this manually")
	}

	sizeBytes := int64(1 << 30) // 1GiB
	if v := os.Getenv("MANUAL_CHANNEL_TRANSFER_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("MANUAL_CHANNEL_TRANSFER_BYTES: %v", err)
		}
		sizeBytes = n
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	repoRoot = repoRoot + "/../.."

	if err := RunChannelFileTransferScenario(repoRoot, sizeBytes); err != nil {
		t.Fatalf("channel file transfer scenario failed: %v", err)
	}
}
