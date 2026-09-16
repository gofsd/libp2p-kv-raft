package logrecord_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// What a scan of one kind costs, walked the two ways, against a real store.
//
// pkg/daemon serves a range scan one pair per request (pkg/store.ScanRange with limit 1), so every
// step of this walk is a full IPC round trip in production -- the round trips are the cost, not the
// SQLite. Stepping key-by-key therefore costs one per *record*, and a log is append-only: a unit
// that is written to repeatedly makes every later listing of that kind slower, forever. Skipping
// costs one per *unit*.
//
// This is the measurement behind logrecord.AfterUnit, made where it can be reproduced in
// milliseconds rather than on a rig.
func TestSkippingCostsOneScanPerUnitInsteadOfOnePerRecord(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const (
		kind      = "cmdreq-cmd-inventoryDictionary"
		units     = 12
		revisions = 9
	)
	want := map[string]bool{}
	for u := 0; u < units; u++ {
		unitID := string(rune('a'+u)) + "-instance"
		want[unitID] = true
		for r := 0; r < revisions; r++ {
			var rnd [logrecord.RandSize]byte
			rnd[0] = byte(r)
			k, err := logrecord.BuildKey(kind, unitID, time.Unix(0, int64(r+1)), rnd)
			if err != nil {
				t.Fatalf("BuildKey: %v", err)
			}
			if err := s.Set(k, []byte("v")); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
	}

	// walk enumerates distinct unitIDs the way pkg/kvctl.listUnitIDs does, counting the scans a
	// daemon would have served. skip selects the strategy under test.
	walk := func(skip bool) (ids []string, scans int) {
		lo, hi := logrecord.KindPrefix(kind), kindHi(kind)
		seen := map[string]bool{}
		for {
			matches, err := s.ScanRange(lo, hi, 1)
			if err != nil {
				t.Fatalf("ScanRange: %v", err)
			}
			scans++
			if len(matches) == 0 {
				return ids, scans
			}
			key := matches[0].Key
			_, unitID, _, err := logrecord.ParseKey(key)
			if err != nil {
				t.Fatalf("ParseKey: %v", err)
			}
			if !seen[unitID] {
				seen[unitID] = true
				ids = append(ids, unitID)
			}
			if skip {
				if next := logrecord.AfterUnit(key); next != nil {
					lo = next
					continue
				}
			}
			lo = append(append([]byte{}, key...), 0x00)
		}
	}

	stepped, steppedScans := walk(false)
	skipped, skippedScans := walk(true)

	// Same answer, which is the part that must not be traded away.
	if len(skipped) != len(want) {
		t.Fatalf("skipping found %d unit(s), want %d", len(skipped), len(want))
	}
	if len(stepped) != len(skipped) {
		t.Fatalf("the two walks disagree: stepped %d, skipped %d", len(stepped), len(skipped))
	}
	for i := range stepped {
		if stepped[i] != skipped[i] {
			t.Fatalf("order differs at %d: stepped %q, skipped %q", i, stepped[i], skipped[i])
		}
	}
	for _, id := range skipped {
		if !want[id] {
			t.Fatalf("unexpected unit %q", id)
		}
	}

	// And the cost, which is the point.
	if steppedScans != units*revisions+1 {
		t.Fatalf("stepping cost %d scans, expected one per record plus the empty one (%d)",
			steppedScans, units*revisions+1)
	}
	if skippedScans != units+1 {
		t.Fatalf("skipping cost %d scans, expected one per unit plus the empty one (%d)",
			skippedScans, units+1)
	}
	t.Logf("stepped %d scans, skipped %d -- %.1fx fewer round trips at %d revisions per unit",
		steppedScans, skippedScans, float64(steppedScans)/float64(skippedScans), revisions)
}

// kindHi mirrors kvctl.kindPrefixBounds' upper bound, which is unexported there.
func kindHi(kind string) []byte {
	prefix := logrecord.KindPrefix(kind)
	hi := make([]byte, len(prefix)+2+256+8+8)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return hi
}
