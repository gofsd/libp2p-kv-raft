package kvmobile

import (
	"encoding/json"
	"testing"
)

// TestRangeScan drives RangeScan against a real (in-process) leader: sets
// a handful of keys sharing a prefix plus one deliberately outside it, then
// checks a scan over just that prefix's range returns exactly the matching
// keys in ascending order, and that limit/skip/order page over them the way
// their doc comments claim -- including the composition that is easiest to
// get wrong, skip counted from the *requested* order rather than always
// from the start of the range.
func TestRangeScan(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		_ = Stop()
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for _, kv := range [][2]string{
		{"scan:a", "1"},
		{"scan:b", "2"},
		{"scan:c", "3"},
		{"zzz-outside-the-range", "should not appear"},
	} {
		if err := Submit(kv[0], kv[1]); err != nil {
			t.Fatalf("Submit(%s): %v", kv[0], err)
		}
	}

	resultsJSON, err := RangeScan("scan:", "scan:\xff", 0, 0, "")
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	var results []KV
	if err := json.Unmarshal([]byte(resultsJSON), &results); err != nil {
		t.Fatalf("unmarshal RangeScan result %s: %v", resultsJSON, err)
	}
	want := []KV{
		{Key: "scan:a", Value: "1"},
		{Key: "scan:b", Value: "2"},
		{Key: "scan:c", Value: "3"},
	}
	if len(results) != len(want) {
		t.Fatalf("RangeScan returned %d results, want %d: %+v", len(results), len(want), results)
	}
	for i, w := range want {
		if results[i] != w {
			t.Fatalf("RangeScan result[%d] = %+v, want %+v", i, results[i], w)
		}
	}

	// Every paging combination worth naming, against the same three keys.
	scan := func(t *testing.T, limit, skip int, order string) []KV {
		t.Helper()
		raw, err := RangeScan("scan:", "scan:\xff", limit, skip, order)
		if err != nil {
			t.Fatalf("RangeScan(limit=%d, skip=%d, order=%q): %v", limit, skip, order, err)
		}
		var got []KV
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		return got
	}
	a, b, c := want[0], want[1], want[2]
	for _, tc := range []struct {
		name  string
		limit int
		skip  int
		order string
		want  []KV
	}{
		{"limit caps from the front", 2, 0, "", []KV{a, b}},
		{"skip drops from the front", 0, 1, "", []KV{b, c}},
		{"skip then limit", 1, 1, "", []KV{b}},
		{"desc reverses", 0, 0, "desc", []KV{c, b, a}},
		{"desc limit takes the last keys", 2, 0, "desc", []KV{c, b}},
		{"desc skip counts from the end", 1, 1, "desc", []KV{b}},
		{"skip past the end is empty, not an error", 0, 99, "", []KV{}},
		{"explicit asc matches the default", 0, 0, "asc", []KV{a, b, c}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := scan(t, tc.limit, tc.skip, tc.order)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d results, want %d: %+v", len(got), len(tc.want), got)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("result[%d] = %+v, want %+v (full: %+v)", i, got[i], tc.want[i], got)
				}
			}
		})
	}

	// A bad order must be refused rather than silently treated as ascending,
	// which is the failure mode a caller would never notice.
	if _, err := RangeScan("scan:", "scan:\xff", 0, 0, "sideways"); err == nil {
		t.Fatal("RangeScan with an unknown order: want error, got none")
	}
	if _, err := RangeScan("scan:", "scan:\xff", 0, -1, ""); err == nil {
		t.Fatal("RangeScan with a negative skip: want error, got none")
	}
}

// TestRangeScanBeforeStartRefuses drives RangeScan with no daemon running
// -- it must refuse outright, same as Submit/Get do (currentSession's
// guard), rather than hang or panic.
func TestRangeScanBeforeStartRefuses(t *testing.T) {
	if _, err := RangeScan("a", "z", 0, 0, ""); err == nil {
		t.Fatal("RangeScan before Start: want error, got none")
	}
}
