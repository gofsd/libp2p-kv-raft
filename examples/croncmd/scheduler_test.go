package croncmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// The whole scheduler runs against Memory() here, which is what the Store
// interface is for: every property below -- including the claim exclusion
// that is the package's reason to exist -- is a property of the logic, and
// testing it needs a compare-and-swap, not a cluster. What a real node adds
// is the *scope* of that compare (see memoryStore's own comment); the
// scheduler cannot tell the two apart, and that is the point.

// recorder is a Submitter that remembers what it was asked to dispatch, and
// can be told to refuse.
type recorder struct {
	mu    sync.Mutex
	calls []recorded
	fail  error
	// id is appended to each returned instance id, so a test can tell two
	// recorders apart when two schedulers share a store.
	id string
}

type recorded struct {
	commandID string
	inputs    string
}

func (r *recorder) Submit(_ context.Context, commandID, inputs string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return "", r.fail
	}
	r.calls = append(r.calls, recorded{commandID: commandID, inputs: inputs})
	return fmt.Sprintf("instance-%s%d", r.id, len(r.calls)), nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recorder) at(i int) recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[i]
}

// base is the instant every test below builds its clock from: a Sunday, in
// UTC, far enough from any month or year boundary to keep the arithmetic in
// the tests themselves obvious.
var base = time.Date(2026, 3, 1, 0, 0, 30, 0, time.UTC)

// harness is one store, one scheduler and one recorder wired together, with
// a clock the test moves by hand.
type harness struct {
	store     Store
	catalog   *Catalog
	scheduler *Scheduler
	submitted *recorder
	now       time.Time
	errs      []error
	skips     []string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{store: Memory(), submitted: &recorder{}, now: base}
	h.catalog = NewCatalog(h.store)
	h.scheduler = &Scheduler{
		Store:     h.store,
		Submitter: h.submitted,
		Now:       func() time.Time { return h.now },
		OnError:   func(err error) { h.errs = append(h.errs, err) },
		OnSkip: func(s Schedule, fire time.Time, reason string) {
			h.skips = append(h.skips, fmt.Sprintf("%s %s %s", s.ID, fire.UTC().Format(time.RFC3339), reason))
		},
	}
	return h
}

func (h *harness) tick(t *testing.T) {
	t.Helper()
	if err := h.scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	for _, err := range h.errs {
		t.Errorf("scheduler reported: %v", err)
	}
	h.errs = nil
}

func (h *harness) put(t *testing.T, s Schedule) {
	t.Helper()
	if err := h.catalog.Put(context.Background(), s); err != nil {
		t.Fatalf("Put(%s): %v", s.ID, err)
	}
}

func (h *harness) advance(d time.Duration) { h.now = h.now.Add(d) }

func TestCatalogRoundTripsASchedule(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	want := Schedule{ID: "nightly", Spec: "@daily", CommandID: "backup", Inputs: `{"op":"run"}`, Comment: "the nightly backup"}
	h.put(t, want)

	got, err := h.catalog.Get(ctx, "nightly")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != want {
		t.Fatalf("Get = %+v, want %+v", got, want)
	}

	schedules, skipped, err := h.catalog.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("List skipped %v", skipped)
	}
	if len(schedules) != 1 || schedules[0] != want {
		t.Fatalf("List = %+v, want just %+v", schedules, want)
	}
}

func TestCatalogRefusesAScheduleItCannotHonour(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		s    Schedule
	}{
		{"no id", Schedule{Spec: "@daily", CommandID: "backup"}},
		{"a slash in the id would collide with a claim key", Schedule{ID: "a/b", Spec: "@daily", CommandID: "backup"}},
		{"no command", Schedule{ID: "nightly", Spec: "@daily"}},
		{"an expression nothing can read", Schedule{ID: "nightly", Spec: "not a cron expression", CommandID: "backup"}},
		{"a zone that does not exist", Schedule{ID: "nightly", Spec: "@daily", CommandID: "backup", Location: "Mars/Olympus"}},
	} {
		if err := h.catalog.Put(ctx, tc.s); err == nil {
			t.Errorf("Put(%s) was accepted, want a refusal", tc.name)
		}
	}
}

// TestListSkipsWhatItCannotReadRatherThanFailing is the property that keeps
// one bad row from stopping every other schedule in the cluster: a record
// written by a hand-rolled client, or by a build that knew an expression
// this one does not, is reported and passed over.
func TestListSkipsWhatItCannotReadRatherThanFailing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	good := Schedule{ID: "good", Spec: "@hourly", CommandID: "report"}
	h.put(t, good)

	// Straight past the Catalog, the way another writer would arrive.
	if err := h.store.Put(ctx, DefaultKeyPrefix+schedulePart+"garbage", "{not json"); err != nil {
		t.Fatalf("Put garbage: %v", err)
	}
	unreadable, err := json.Marshal(Schedule{ID: "future", Spec: "0 0 * * * seconds-field", CommandID: "report"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := h.store.Put(ctx, DefaultKeyPrefix+schedulePart+"future", string(unreadable)); err != nil {
		t.Fatalf("Put future: %v", err)
	}

	schedules, skipped, err := h.catalog.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(schedules) != 1 || schedules[0].ID != "good" {
		t.Fatalf("List = %+v, want just the readable schedule", schedules)
	}
	if len(skipped) != 2 {
		t.Fatalf("List skipped %d records, want 2: %v", len(skipped), skipped)
	}
}

func TestDeleteRemovesTheWatermarkWithTheSchedule(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.put(t, Schedule{ID: "nightly", Spec: "@hourly", CommandID: "backup"})
	h.tick(t) // establishes the watermark

	if _, ok, err := h.store.Get(ctx, DefaultKeyPrefix+watermarkPart+"nightly"); err != nil || !ok {
		t.Fatalf("watermark after first tick: ok=%v err=%v", ok, err)
	}
	if err := h.catalog.Delete(ctx, "nightly"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := h.store.Get(ctx, DefaultKeyPrefix+watermarkPart+"nightly"); err != nil || ok {
		t.Fatalf("watermark survived Delete: ok=%v err=%v", ok, err)
	}
	if _, err := h.catalog.Get(ctx, "nightly"); err == nil {
		t.Fatal("Get found a deleted schedule")
	}
}

func TestSetEnabledFlipsWithoutLosingTheSchedule(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.put(t, Schedule{ID: "nightly", Spec: "@hourly", CommandID: "backup", Comment: "keep me"})
	if err := h.catalog.SetEnabled(ctx, "nightly", false); err != nil {
		t.Fatalf("SetEnabled(false): %v", err)
	}
	got, err := h.catalog.Get(ctx, "nightly")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Disabled || got.Comment != "keep me" {
		t.Fatalf("Get = %+v, want it disabled with the rest intact", got)
	}
	if err := h.catalog.SetEnabled(ctx, "nightly", true); err != nil {
		t.Fatalf("SetEnabled(true): %v", err)
	}
	if got, _ := h.catalog.Get(ctx, "nightly"); got.Disabled {
		t.Fatal("SetEnabled(true) left it disabled")
	}
}

// TestAScheduleStartsFromWhenItWasCreated pins the choice window() makes
// for a schedule it has never seen: no watermark means "start now", not
// "start at the epoch". The alternative would fire a new schedule's whole
// back catalogue the moment it is written.
func TestAScheduleStartsFromWhenItWasCreated(t *testing.T) {
	h := newHarness(t)

	h.put(t, Schedule{ID: "everyminute", Spec: "* * * * *", CommandID: "noisy"})
	h.tick(t)

	if got := h.submitted.count(); got != 0 {
		t.Fatalf("the first tick submitted %d fires, want 0", got)
	}
	if _, ok, err := h.store.Get(context.Background(), DefaultKeyPrefix+watermarkPart+"everyminute"); err != nil || !ok {
		t.Fatalf("the first tick left no watermark: ok=%v err=%v", ok, err)
	}
}

func TestTickSubmitsWhatIsDueAndNothingElse(t *testing.T) {
	h := newHarness(t)

	h.put(t, Schedule{ID: "quarterly", Spec: "*/15 * * * *", CommandID: "sweep", Inputs: `{"op":"sweep"}`})
	h.tick(t) // watermark at 00:00:30

	// 00:15 and 00:30 have passed; 00:45 has not.
	h.advance(35 * time.Minute) // 00:35:30
	h.tick(t)

	if got := h.submitted.count(); got != 2 {
		t.Fatalf("submitted %d fires, want 2 (00:15 and 00:30)", got)
	}
	for i := 0; i < 2; i++ {
		if call := h.submitted.at(i); call.commandID != "sweep" || call.inputs != `{"op":"sweep"}` {
			t.Fatalf("call %d = %+v, want the schedule's command and inputs verbatim", i, call)
		}
	}

	// Ticking again with no time passed submits nothing: the watermark has
	// already moved past both.
	h.tick(t)
	if got := h.submitted.count(); got != 2 {
		t.Fatalf("a second tick submitted again: %d fires, want 2", got)
	}
}

// TestOnlyOneOfTwoSchedulersSubmitsAFire is the headline property, and the
// reason this package claims a fire before submitting it rather than just
// consulting a clock.
//
// There are two layers to that, and this exercises the lower one. In the
// ordinary case the *watermark* already settles it: whichever scheduler
// ticks first advances it past the fire, and the other reads the advanced
// value and finds nothing due. The claim is what covers the case the
// watermark cannot -- two schedulers that both read the watermark before
// either wrote it, which is what a barrier on the watermark read forces
// here.
func TestOnlyOneOfTwoSchedulersSubmitsAFire(t *testing.T) {
	store := Memory()
	now := base
	clock := func() time.Time { return now }

	first, second := &recorder{id: "a-"}, &recorder{id: "b-"}
	var mu sync.Mutex
	var claimedElsewhere int
	newScheduler := func(sub Submitter) *Scheduler {
		return &Scheduler{
			Store:     store,
			Submitter: sub,
			Now:       clock,
			OnError:   func(err error) { t.Errorf("scheduler reported: %v", err) },
			OnSkip: func(_ Schedule, _ time.Time, reason string) {
				mu.Lock()
				defer mu.Unlock()
				if reason == SkipClaimed {
					claimedElsewhere++
				}
			},
		}
	}
	a, b := newScheduler(first), newScheduler(second)

	if err := NewCatalog(store).Put(context.Background(), Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Both start from the same place.
	for _, s := range []*Scheduler{a, b} {
		if err := s.Tick(context.Background()); err != nil {
			t.Fatalf("first Tick: %v", err)
		}
	}

	// Now hold each of them at its watermark read until the other has got
	// there too, so both go on to consider the same fire.
	held := &barrierStore{Store: store, key: DefaultKeyPrefix + watermarkPart + "hourly", want: 2, ready: make(chan struct{})}
	a.Store, b.Store = held, held

	now = now.Add(90 * time.Minute) // past 01:00
	var wg sync.WaitGroup
	for _, s := range []*Scheduler{a, b} {
		wg.Add(1)
		go func(s *Scheduler) {
			defer wg.Done()
			if err := s.Tick(context.Background()); err != nil {
				t.Errorf("second Tick: %v", err)
			}
		}(s)
	}
	wg.Wait()

	if total := first.count() + second.count(); total != 1 {
		t.Fatalf("two schedulers submitted %d dispatches for one fire, want 1", total)
	}
	mu.Lock()
	defer mu.Unlock()
	if claimedElsewhere != 1 {
		t.Fatalf("the losing scheduler reported %d claimed-elsewhere skips, want 1", claimedElsewhere)
	}
}

// TestTheWatermarkAlreadySettlesTheOrdinaryCase is the layer above the
// claim: two schedulers ticking one after the other, which is what
// staggered tickers on different nodes usually amount to. The second finds
// the watermark already past the fire and never reaches a claim at all.
func TestTheWatermarkAlreadySettlesTheOrdinaryCase(t *testing.T) {
	store := Memory()
	now := base
	clock := func() time.Time { return now }

	first, second := &recorder{id: "a-"}, &recorder{id: "b-"}
	build := func(sub Submitter) *Scheduler {
		return &Scheduler{Store: store, Submitter: sub, Now: clock,
			OnError: func(err error) { t.Errorf("scheduler reported: %v", err) }}
	}
	a, b := build(first), build(second)

	if err := NewCatalog(store).Put(context.Background(), Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for _, s := range []*Scheduler{a, b} {
		if err := s.Tick(context.Background()); err != nil {
			t.Fatalf("first Tick: %v", err)
		}
	}
	now = now.Add(90 * time.Minute)
	for _, s := range []*Scheduler{a, b} {
		if err := s.Tick(context.Background()); err != nil {
			t.Fatalf("second Tick: %v", err)
		}
	}

	if first.count() != 1 || second.count() != 0 {
		t.Fatalf("dispatches were %d and %d, want 1 from the scheduler that ticked first and 0 from the other", first.count(), second.count())
	}
}

// barrierStore holds every reader of one key until `want` of them have
// arrived, which is how a test forces an interleaving that is otherwise a
// race to reproduce.
type barrierStore struct {
	Store
	key   string
	want  int
	mu    sync.Mutex
	seen  int
	ready chan struct{}
}

func (b *barrierStore) Get(ctx context.Context, key string) (string, bool, error) {
	value, ok, err := b.Store.Get(ctx, key)
	if key != b.key {
		return value, ok, err
	}
	b.mu.Lock()
	b.seen++
	if b.seen == b.want {
		close(b.ready)
	}
	b.mu.Unlock()
	<-b.ready
	return value, ok, err
}

// TestFiresOlderThanTheCatchUpWindowAreSkipped is the other half of a
// scheduler that was away: it comes back and does the recent work, not a
// day of it at once.
func TestFiresOlderThanTheCatchUpWindowAreSkipped(t *testing.T) {
	h := newHarness(t)
	h.scheduler.CatchUp = time.Hour

	h.put(t, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"})
	h.tick(t)

	h.advance(24 * time.Hour)
	h.tick(t)

	if got := h.submitted.count(); got != 1 {
		t.Fatalf("a day away produced %d dispatches, want 1 (only the last hour)", got)
	}
	var stale int
	for _, skip := range h.skips {
		if strings.HasSuffix(skip, SkipStale) {
			stale++
		}
	}
	if stale != 1 {
		t.Fatalf("reported %d stale skips, want 1: %v", stale, h.skips)
	}
}

func TestADisabledScheduleDoesNotFire(t *testing.T) {
	h := newHarness(t)

	h.put(t, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report", Disabled: true})
	h.tick(t)
	h.advance(90 * time.Minute)
	h.tick(t)

	if got := h.submitted.count(); got != 0 {
		t.Fatalf("a disabled schedule submitted %d fires, want 0", got)
	}
}

// TestAFailedSubmitIsNotRetriedByDefault pins the safer half of
// RetryFailedSubmit: the claim stays, so a submission that may in fact have
// landed is not made a second time.
func TestAFailedSubmitIsNotRetriedByDefault(t *testing.T) {
	h := newHarness(t)
	h.scheduler.OnError = func(err error) { h.errs = append(h.errs, err) }

	h.put(t, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"})
	h.tick(t)

	h.submitted.fail = fmt.Errorf("the daemon is not answering")
	h.advance(90 * time.Minute)
	if err := h.scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(h.errs) == 0 {
		t.Fatal("a failed submission was not reported")
	}
	h.errs = nil

	// The claim survives, so the recovered scheduler leaves that fire alone.
	h.submitted.fail = nil
	h.tick(t)
	if got := h.submitted.count(); got != 0 {
		t.Fatalf("the failed fire was retried: %d dispatches, want 0", got)
	}
}

func TestAFailedSubmitIsRetriedWhenAsked(t *testing.T) {
	h := newHarness(t)
	h.scheduler.RetryFailedSubmit = true

	h.put(t, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"})
	h.tick(t)

	h.submitted.fail = fmt.Errorf("the daemon is not answering")
	h.advance(90 * time.Minute)
	if err := h.scheduler.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	h.errs = nil

	h.submitted.fail = nil
	h.tick(t)
	if got := h.submitted.count(); got != 1 {
		t.Fatalf("the released fire produced %d dispatches, want 1", got)
	}
}

// TestAClaimRecordsTheDispatchItProduced is what makes a claim key an audit
// trail rather than only a lock.
func TestAClaimRecordsTheDispatchItProduced(t *testing.T) {
	h := newHarness(t)

	h.put(t, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"})
	h.tick(t)
	h.advance(90 * time.Minute)
	h.tick(t)

	pairs, err := h.store.Scan(context.Background(), DefaultKeyPrefix+claimPart)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("found %d claims, want 1", len(pairs))
	}
	var claim Fire
	if err := json.Unmarshal([]byte(pairs[0].Value), &claim); err != nil {
		t.Fatalf("claim does not parse: %v", err)
	}
	if claim.ScheduleID != "hourly" || claim.CommandID != "report" || claim.InstanceID == "" {
		t.Fatalf("claim = %+v, want it to name the schedule, the command and the instance", claim)
	}
	if want := base.Add(time.Hour).Truncate(time.Hour); !claim.At.Equal(want) {
		t.Fatalf("claim fire = %s, want %s", claim.At, want)
	}
	// The key carries the fire time too, which is what lets pruning read it
	// without decoding a value.
	if !strings.HasSuffix(pairs[0].Key, claimStamp(claim.At)) {
		t.Fatalf("claim key %q does not end in its own fire time", pairs[0].Key)
	}
}

// claimStamp is how a fire time appears in its own claim key.
func claimStamp(fire time.Time) string { return fire.UTC().Format(claimTimeLayout) }

func TestPruningRemovesOldClaimsAndLeavesRecentOnes(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.scheduler.CatchUp = time.Hour
	h.scheduler.Retain = 4 * time.Hour

	old := h.now.Add(-24 * time.Hour)
	recent := h.now.Add(-2 * time.Hour)
	for _, at := range []time.Time{old, recent} {
		if err := h.store.Put(ctx, h.scheduler.claimKey("hourly", at), "{}"); err != nil {
			t.Fatalf("Put claim: %v", err)
		}
	}
	// A key under the claim prefix that is not a claim at all must survive:
	// pruning reads the key, and what it cannot read it leaves alone.
	if err := h.store.Put(ctx, DefaultKeyPrefix+claimPart+"stray", "{}"); err != nil {
		t.Fatalf("Put stray: %v", err)
	}

	if err := h.scheduler.pruneClaims(ctx, h.now); err != nil {
		t.Fatalf("pruneClaims: %v", err)
	}

	pairs, err := h.store.Scan(ctx, DefaultKeyPrefix+claimPart)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(pairs) != 2 {
		t.Fatalf("after pruning %d keys remain, want 2 (the recent claim and the stray): %v", len(pairs), pairs)
	}
	for _, pair := range pairs {
		if strings.HasSuffix(pair.Key, claimStamp(old)) {
			t.Fatalf("the day-old claim survived pruning: %s", pair.Key)
		}
	}
}

// TestRetainIsHeldAboveTheCatchUpWindow pins the floor retain() enforces.
// A claim pruned while its fire is still inside the catch-up window would
// be re-claimed and re-submitted, which is the one thing this package is
// for.
func TestRetainIsHeldAboveTheCatchUpWindow(t *testing.T) {
	s := &Scheduler{CatchUp: 6 * time.Hour, Retain: time.Minute}
	if got, floor := s.retain(), 12*time.Hour; got != floor {
		t.Fatalf("retain() = %s with a 6h catch-up, want the %s floor", got, floor)
	}
	generous := &Scheduler{CatchUp: time.Hour, Retain: 30 * 24 * time.Hour}
	if got := generous.retain(); got != 30*24*time.Hour {
		t.Fatalf("retain() = %s, want the configured 30 days left alone", got)
	}
}

// TestOneTickIsBoundedHoweverFarBehindItIs keeps a misconfiguration -- a
// per-minute schedule with a long catch-up window -- costing a bounded
// amount of work per pass, with the remainder picked up next time rather
// than dropped.
func TestOneTickIsBoundedHoweverFarBehindItIs(t *testing.T) {
	h := newHarness(t)
	h.scheduler.CatchUp = 48 * time.Hour
	h.scheduler.OnSkip = nil

	h.put(t, Schedule{ID: "everyminute", Spec: "* * * * *", CommandID: "noisy"})
	h.tick(t)

	h.advance(48 * time.Hour)
	h.tick(t)

	if got := h.submitted.count(); got != maxFiresPerSchedulePerTick {
		t.Fatalf("one tick submitted %d fires, want it capped at %d", got, maxFiresPerSchedulePerTick)
	}

	// The next pass resumes from where this one stopped rather than from
	// now, so nothing inside the window is silently skipped.
	h.tick(t)
	if got := h.submitted.count(); got != 2*maxFiresPerSchedulePerTick {
		t.Fatalf("the second tick brought the total to %d, want %d", got, 2*maxFiresPerSchedulePerTick)
	}
}

// TestAScheduleFiresOnItsOwnZonesWallClock is why Schedule carries a
// Location at all: "02:30 every day" means 02:30 where the operator is.
func TestAScheduleFiresOnItsOwnZonesWallClock(t *testing.T) {
	// A fixed zone rather than a named one, so this holds on a machine with
	// no tzdata. Location resolution itself is covered by Put's refusal of
	// an unknown zone.
	spec := MustParseSpec("30 2 * * *")
	plusFive := time.FixedZone("plusfive", 5*60*60)

	// 02:30 in +05:00 is 21:30 the previous day in UTC, and that is the
	// instant the scheduler must compare against.
	next, ok := spec.Next(base.In(plusFive))
	if !ok {
		t.Fatal("Next found nothing")
	}
	if got := next.UTC().Format("2006-01-02 15:04"); got != "2026-03-01 21:30" {
		t.Fatalf("02:30 in +05:00 resolved to %s UTC, want 2026-03-01 21:30", got)
	}
}

// TestAWatermarkNobodyCanReadDoesNotStopTheSchedule: a corrupt watermark
// costs one window, not a schedule that never fires again.
func TestAWatermarkNobodyCanReadDoesNotStopTheSchedule(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.put(t, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"})
	if err := h.store.Put(ctx, DefaultKeyPrefix+watermarkPart+"hourly", "not a timestamp"); err != nil {
		t.Fatalf("Put watermark: %v", err)
	}

	if err := h.scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(h.errs) != 1 {
		t.Fatalf("reported %d errors, want exactly the unreadable watermark: %v", len(h.errs), h.errs)
	}
	h.errs = nil

	// It restarted from now, so the next hour still fires.
	h.advance(90 * time.Minute)
	h.tick(t)
	if got := h.submitted.count(); got != 1 {
		t.Fatalf("submitted %d fires after a corrupt watermark, want 1", got)
	}
}

// TestOneFailingScheduleDoesNotStopTheOthers: schedules are independent,
// and a tick that hits a problem on one carries on with the rest.
func TestOneFailingScheduleDoesNotStopTheOthers(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.put(t, Schedule{ID: "aaa", Spec: "@hourly", CommandID: "first"})
	h.put(t, Schedule{ID: "zzz", Spec: "@hourly", CommandID: "second"})
	h.tick(t)

	// Break the first schedule's watermark into something Get can return
	// but nothing can advance past, by way of a store that refuses writes
	// to that one key.
	h.scheduler.Store = refusingStore{Store: h.store, refuse: DefaultKeyPrefix + watermarkPart + "aaa"}
	h.advance(90 * time.Minute)
	if err := h.scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(h.errs) == 0 {
		t.Fatal("the failing schedule was not reported")
	}

	// The second schedule still fired.
	var sawSecond bool
	for i := 0; i < h.submitted.count(); i++ {
		if h.submitted.at(i).commandID == "second" {
			sawSecond = true
		}
	}
	if !sawSecond {
		t.Fatal("a failure on one schedule stopped another from firing")
	}
}

// refusingStore fails writes to one key and passes everything else through.
type refusingStore struct {
	Store
	refuse string
}

func (r refusingStore) Put(ctx context.Context, key, value string) error {
	if key == r.refuse {
		return fmt.Errorf("this key is not writable")
	}
	return r.Store.Put(ctx, key, value)
}

func TestAnIndependentPrefixKeepsTwoSchedulersApart(t *testing.T) {
	store := Memory()
	ctx := context.Background()

	mine := &Catalog{Store: store, KeyPrefix: "mine/"}
	yours := &Catalog{Store: store, KeyPrefix: "yours/"}
	if err := mine.Put(ctx, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	theirs, _, err := yours.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(theirs) != 0 {
		t.Fatalf("the other prefix saw %d schedules, want 0", len(theirs))
	}
	ours, _, err := mine.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ours) != 1 {
		t.Fatalf("own prefix saw %d schedules, want 1", len(ours))
	}
}
