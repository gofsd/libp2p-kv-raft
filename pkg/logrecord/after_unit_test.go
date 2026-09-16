package logrecord

import (
	"bytes"
	"testing"
	"time"
)

func key(t *testing.T, kind, unitID string, tsNano int64, fill byte) []byte {
	t.Helper()
	var rnd [RandSize]byte
	for i := range rnd {
		rnd[i] = fill
	}
	k, err := BuildKey(kind, unitID, time.Unix(0, tsNano), rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	return k
}

// The skip must land after every record of the unit it was derived from, whatever the timestamp
// and tiebreaker -- including the largest either can be.
func TestAfterUnitClearsEveryRecordOfThatUnit(t *testing.T) {
	first := key(t, "cmdreq-cmd-inventoryDictionary", "inst-1", 1, 0x00)
	skip := AfterUnit(first)
	if skip == nil {
		t.Fatal("AfterUnit refused a well-formed key")
	}
	for _, k := range [][]byte{
		first,
		key(t, "cmdreq-cmd-inventoryDictionary", "inst-1", 1<<62, 0x7F),
		key(t, "cmdreq-cmd-inventoryDictionary", "inst-1", (1<<63)-1, 0xFF),
	} {
		if bytes.Compare(skip, k) <= 0 {
			t.Errorf("skip does not clear a record of the same unit:\n skip=%x\n key =%x", skip, k)
		}
	}
}

// And it must not run past a neighbouring unit -- the whole risk of skipping rather than stepping.
// The awkward pair is a unitID that *extends* another: "inst-1" and "inst-10" share a prefix, and
// only the length bytes BuildKey writes keep their ranges apart.
func TestAfterUnitStopsShortOfTheNextUnit(t *testing.T) {
	skip := AfterUnit(key(t, "cmdreq-x", "inst-1", 5, 0x11))
	for _, next := range []string{"inst-10", "inst-2", "inst-1a", "inst-1\x00"} {
		first := key(t, "cmdreq-x", next, 0, 0x00)
		if bytes.Compare(skip, first) > 0 {
			t.Errorf("skip runs past the first record of %q:\n skip =%x\n first=%x", next, skip, first)
		}
	}
}

// A unitID that sorts *before* the one we skipped is behind us either way; the guarantee that
// matters is that we never move backwards.
func TestAfterUnitOnlyEverMovesForward(t *testing.T) {
	k := key(t, "cmdreq-x", "inst-5", 99, 0x42)
	if bytes.Compare(AfterUnit(k), k) <= 0 {
		t.Fatal("AfterUnit did not advance past the key it was given")
	}
}

// A malformed or truncated key means "do not skip" rather than a bad bound, because the caller's
// fallback (step one key at a time) is always correct and a wrong skip silently loses records.
func TestAfterUnitRefusesAKeyTooShortToHoldATail(t *testing.T) {
	for _, k := range [][]byte{nil, {}, make([]byte, 8+RandSize-1)} {
		if got := AfterUnit(k); got != nil {
			t.Errorf("AfterUnit(%x) = %x, want nil so the caller steps instead", k, got)
		}
	}
}
