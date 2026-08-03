package daemon

import "testing"

func TestQuotaTrackerUnlimitedByDefault(t *testing.T) {
	q := newQuotaTracker(0, 0, 0, 0)
	for i := 0; i < 1000; i++ {
		if !q.allow("peer-a", "1.2.3.4", 1<<20) {
			t.Fatalf("call %d: zero-configured tracker unexpectedly denied", i)
		}
	}
}

func TestQuotaTrackerPeerBucketBurstExhaustion(t *testing.T) {
	const burst = 3
	q := newQuotaTracker(1, burst, 0, 0)
	for i := 0; i < burst; i++ {
		if !q.allow("peer-a", "", 1) {
			t.Fatalf("call %d: denied within burst budget", i)
		}
	}
	if q.allow("peer-a", "", 1) {
		t.Fatal("call past burst budget unexpectedly allowed")
	}
}

func TestQuotaTrackerIPBucketBurstExhaustion(t *testing.T) {
	const burst = 3
	q := newQuotaTracker(0, 0, 1, burst)
	for i := 0; i < burst; i++ {
		if !q.allow("", "9.9.9.9", 1) {
			t.Fatalf("call %d: denied within burst budget", i)
		}
	}
	if q.allow("", "9.9.9.9", 1) {
		t.Fatal("call past burst budget unexpectedly allowed")
	}
}

// TestQuotaTrackerPeerAndIPBucketsIndependent confirms exhausting one
// peer's bucket doesn't affect a different peer sharing the same IP, and
// vice versa -- each key-space is keyed and consumed independently.
func TestQuotaTrackerPeerAndIPBucketsIndependent(t *testing.T) {
	const burst = 2
	q := newQuotaTracker(1, burst, 1, burst)

	for i := 0; i < burst; i++ {
		if !q.allow("peer-a", "1.1.1.1", 1) {
			t.Fatalf("peer-a call %d: denied within burst budget", i)
		}
	}
	if q.allow("peer-a", "1.1.1.1", 1) {
		t.Fatal("peer-a exceeded its own burst budget but was still allowed")
	}

	// A different peer id behind the same IP still has its own peer
	// bucket, but shares the now-exhausted IP bucket.
	if q.allow("peer-b", "1.1.1.1", 1) {
		t.Fatal("peer-b allowed despite the shared IP bucket already being exhausted")
	}

	// The same peer id from a different IP has its own peer bucket
	// already exhausted above, so it's denied regardless of IP.
	if q.allow("peer-a", "2.2.2.2", 1) {
		t.Fatal("peer-a allowed from a new IP despite its own peer bucket already being exhausted")
	}

	// A genuinely fresh peer id and IP pair is unaffected by either.
	if !q.allow("peer-c", "3.3.3.3", 1) {
		t.Fatal("a fresh peer/IP pair was unexpectedly denied")
	}
}

func TestQuotaTrackerCostConsumesMultipleTokens(t *testing.T) {
	q := newQuotaTracker(0, 0, 100, 100)
	if !q.allow("", "1.2.3.4", 60) {
		t.Fatal("first 60-byte debit unexpectedly denied")
	}
	if q.allow("", "1.2.3.4", 60) {
		t.Fatal("second 60-byte debit unexpectedly allowed against a 100-token burst")
	}
}

func TestQuotaTrackerEmptyKeysSkipped(t *testing.T) {
	const burst = 1
	q := newQuotaTracker(1, burst, 1, burst)
	if !q.allow("", "", 1) {
		t.Fatal("empty peer/IP keys should never allocate a limiter or be denied")
	}
	if !q.allow("", "", 1) {
		t.Fatal("repeated calls with empty peer/IP keys should stay unlimited")
	}
}
