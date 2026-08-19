package croncmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Defaults for the knobs below. Interval is well under a minute because
// cron's resolution is a minute and a poll at exactly that period drifts
// into missing one; CatchUp is an hour because "the scheduler was down
// over lunch" should still produce the fires and "it was down over the
// weekend" should not; Retain is a fortnight so that claim keys outlive
// any plausible catch-up window by a wide margin (see pruneClaims).
const (
	DefaultInterval      = 20 * time.Second
	DefaultCatchUp       = time.Hour
	DefaultRetain        = 14 * 24 * time.Hour
	DefaultPruneInterval = time.Hour
)

// maxFiresPerSchedulePerTick bounds how many fires one schedule may
// produce in a single pass, so that a misconfiguration -- a per-minute
// schedule whose watermark is somehow far behind -- costs a bounded amount
// of work per tick rather than blocking every other schedule behind it.
// The remainder is picked up on the next tick, from where this one
// stopped.
const maxFiresPerSchedulePerTick = 1000

// claimTimeLayout formats a fire's minute into its claim key. It is UTC
// and sorts lexicographically in time order, which is what lets
// pruneClaims decide what is old by reading the key rather than its value.
const claimTimeLayout = "20060102T1504Z"

// Scheduler submits commands when their schedules say to.
//
// Run one on as many nodes as you like. Every fire is claimed before it is
// submitted (see Store.Claim and the package doc), so more schedulers mean
// more redundancy rather than more dispatches -- which is the property
// that makes it safe to simply run one everywhere instead of maintaining a
// notion of which node is "the" scheduler.
type Scheduler struct {
	// Store is where schedules, watermarks and claims live.
	Store Store
	// Submitter dispatches a fire. Defaults to Kvctl().
	Submitter Submitter
	// KeyPrefix defaults to DefaultKeyPrefix, and must match the Catalog's.
	KeyPrefix string

	// Interval is how often to look for work. Defaults to DefaultInterval.
	Interval time.Duration
	// CatchUp is how far back a missed fire is still submitted. Fires
	// older than this are skipped, reported through OnSkip, and never
	// submitted. Defaults to DefaultCatchUp; set it at or below Interval
	// to effectively disable catching up.
	CatchUp time.Duration
	// Retain is how long a claim key is kept before pruneClaims removes
	// it. Defaults to DefaultRetain. It is held to at least twice CatchUp
	// however it is set: pruning a claim inside the catch-up window would
	// let the fire it recorded be claimed and submitted a second time,
	// which is the one thing this package exists to prevent.
	Retain time.Duration
	// PruneInterval is how often to look for expired claim keys.
	// Defaults to DefaultPruneInterval.
	PruneInterval time.Duration

	// RetryFailedSubmit decides what happens when the claim succeeded but
	// the submission itself failed.
	//
	// False (the default) keeps the claim, so that fire is never retried:
	// a submission that failed *after* the daemon accepted the write --
	// which a client cannot tell apart from one that failed before --
	// would otherwise run the command twice. True releases the claim so
	// some scheduler retries the fire while it is still inside CatchUp.
	// The choice is between possibly missing a fire and possibly
	// duplicating one, and no default is right for every command; this
	// one matches what the rest of the package is for.
	RetryFailedSubmit bool

	// Now is the clock, for tests. Defaults to time.Now.
	Now func() time.Time

	// OnFire, if set, is called after each successful submission with the
	// schedule, the fire's own time, and the instance id it produced.
	OnFire func(s Schedule, fire time.Time, instanceID string)
	// OnSkip, if set, is called for each fire that was not submitted, with
	// the reason -- a fire older than CatchUp, or one another scheduler
	// claimed first.
	OnSkip func(s Schedule, fire time.Time, reason string)
	// OnError, if set, is called with every error the loop hits. All of
	// them are treated as transient: none stops the loop.
	OnError func(error)

	// lastPrune is when pruneClaims last ran; zero means never.
	lastPrune time.Time
}

// SkipStale and SkipClaimed are OnSkip's two reasons.
const (
	SkipStale   = "older than the catch-up window"
	SkipClaimed = "claimed by another scheduler"
)

func (s *Scheduler) prefix() string {
	if s.KeyPrefix == "" {
		return DefaultKeyPrefix
	}
	return s.KeyPrefix
}

func (s *Scheduler) now() time.Time {
	if s.Now == nil {
		return time.Now()
	}
	return s.Now()
}

func (s *Scheduler) interval() time.Duration {
	if s.Interval <= 0 {
		return DefaultInterval
	}
	return s.Interval
}

func (s *Scheduler) catchUp() time.Duration {
	if s.CatchUp <= 0 {
		return DefaultCatchUp
	}
	return s.CatchUp
}

// retain enforces the "at least twice the catch-up window" floor its own
// field documents.
func (s *Scheduler) retain() time.Duration {
	retain := s.Retain
	if retain <= 0 {
		retain = DefaultRetain
	}
	if floor := 2 * s.catchUp(); retain < floor {
		retain = floor
	}
	return retain
}

func (s *Scheduler) pruneInterval() time.Duration {
	if s.PruneInterval <= 0 {
		return DefaultPruneInterval
	}
	return s.PruneInterval
}

func (s *Scheduler) submitter() Submitter {
	if s.Submitter == nil {
		return Kvctl()
	}
	return s.Submitter
}

func (s *Scheduler) catalog() *Catalog {
	return &Catalog{Store: s.Store, KeyPrefix: s.prefix()}
}

func (s *Scheduler) report(err error) {
	if err != nil && s.OnError != nil {
		s.OnError(err)
	}
}

func (s *Scheduler) watermarkKey(id string) string {
	return s.prefix() + watermarkPart + id
}

func (s *Scheduler) claimKey(id string, fire time.Time) string {
	return s.prefix() + claimPart + id + "/" + fire.UTC().Format(claimTimeLayout)
}

// Run ticks until stop is closed, running one pass immediately so that a
// scheduler restarted just after a fire was due does not hold it for a
// whole interval.
func (s *Scheduler) Run(ctx context.Context, stop <-chan struct{}) {
	s.report(s.Tick(ctx))

	ticker := time.NewTicker(s.interval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			s.report(s.Tick(ctx))
		}
	}
}

// Tick is one pass: read every schedule, submit whatever is due, and
// prune expired claims if it is time to.
//
// It is exported because it is the whole of the scheduler's behaviour, and
// a caller driving its own clock -- a test, or a process that would rather
// tick from its own loop -- should not have to start a goroutine to get
// at it.
func (s *Scheduler) Tick(ctx context.Context) error {
	schedules, skipped, err := s.catalog().List(ctx)
	if err != nil {
		return fmt.Errorf("croncmd: tick: %w", err)
	}
	for _, bad := range skipped {
		s.report(bad)
	}

	now := s.now()
	for _, schedule := range schedules {
		if schedule.Disabled {
			continue
		}
		if err := s.runSchedule(ctx, schedule, now); err != nil {
			// One schedule's failure must not stop the others: report it
			// and carry on, the same way a bad record is skipped rather
			// than fatal.
			s.report(err)
		}
	}

	if s.lastPrune.IsZero() || now.Sub(s.lastPrune) >= s.pruneInterval() {
		s.lastPrune = now
		if err := s.pruneClaims(ctx, now); err != nil {
			s.report(err)
		}
	}
	return nil
}

// runSchedule submits every fire of one schedule that falls in the window
// ending at now, and advances that schedule's watermark past them.
func (s *Scheduler) runSchedule(ctx context.Context, schedule Schedule, now time.Time) error {
	spec, loc, err := schedule.parsed()
	if err != nil {
		return err
	}

	from, first, err := s.window(ctx, schedule, now)
	if err != nil {
		return err
	}
	if first {
		// A schedule seen for the first time starts from now. The
		// alternative -- treating "no watermark" as "the epoch" -- would
		// make creating a schedule fire its entire back catalogue at
		// once, bounded only by CatchUp.
		return s.putWatermark(ctx, schedule.ID, now)
	}

	// Never look further back than the catch-up window, so the number of
	// candidate fires is bounded by that window rather than by how long
	// this scheduler was away.
	stale := now.Add(-s.catchUp())
	if from.Before(stale) {
		if s.OnSkip != nil {
			s.OnSkip(schedule, from, SkipStale)
		}
		from = stale
	}

	cursor := from.In(loc)
	advanceTo := now
	for fired := 0; fired < maxFiresPerSchedulePerTick; fired++ {
		fire, ok := spec.Next(cursor)
		if !ok || fire.After(now) {
			break
		}
		cursor = fire

		if err := s.fire(ctx, schedule, fire); err != nil {
			// Advance the watermark only past fires that were actually
			// dealt with, so one that failed here is reconsidered next
			// tick while it is still inside the catch-up window.
			if wErr := s.putWatermark(ctx, schedule.ID, fire.Add(-time.Minute)); wErr != nil {
				s.report(wErr)
			}
			return err
		}

		if fired == maxFiresPerSchedulePerTick-1 {
			advanceTo = cursor
		}
	}

	return s.putWatermark(ctx, schedule.ID, advanceTo)
}

// fire claims one (schedule, minute) pair and submits it if the claim was
// won. A claim lost to another scheduler is an ordinary outcome, not an
// error.
func (s *Scheduler) fire(ctx context.Context, schedule Schedule, at time.Time) error {
	key := s.claimKey(schedule.ID, at)
	claim := Fire{ScheduleID: schedule.ID, CommandID: schedule.CommandID, At: at.UTC()}

	encoded, err := json.Marshal(claim)
	if err != nil {
		return fmt.Errorf("croncmd: encode claim for %s: %w", schedule.ID, err)
	}

	claimed, err := s.Store.Claim(ctx, key, string(encoded))
	if err != nil {
		return err
	}
	if !claimed {
		if s.OnSkip != nil {
			s.OnSkip(schedule, at, SkipClaimed)
		}
		return nil
	}

	instanceID, err := s.submitter().Submit(ctx, schedule.CommandID, schedule.Inputs)
	if err != nil {
		if s.RetryFailedSubmit {
			// Release the claim so this fire is reconsidered. See the
			// field's own doc comment for what this trades away.
			if delErr := s.Store.Delete(ctx, key); delErr != nil {
				s.report(delErr)
			}
		}
		return fmt.Errorf("croncmd: submit %s for schedule %s: %w", schedule.CommandID, schedule.ID, err)
	}

	// Record which dispatch this fire produced, so the claim key is not
	// just a lock but the scheduler's own audit trail: "this minute of
	// this schedule became that instance id".
	claim.InstanceID = instanceID
	if encoded, err := json.Marshal(claim); err == nil {
		if err := s.Store.Put(ctx, key, string(encoded)); err != nil {
			s.report(err)
		}
	}

	if s.OnFire != nil {
		s.OnFire(schedule, at, instanceID)
	}
	return nil
}

// window returns where this schedule's next pass should start, and whether
// this is the first time the schedule has been seen at all.
func (s *Scheduler) window(ctx context.Context, schedule Schedule, now time.Time) (time.Time, bool, error) {
	value, ok, err := s.Store.Get(ctx, s.watermarkKey(schedule.ID))
	if err != nil {
		return time.Time{}, false, err
	}
	if !ok {
		return time.Time{}, true, nil
	}

	mark, err := time.Parse(time.RFC3339, value)
	if err != nil {
		// A watermark nobody can read is treated as "start from now"
		// rather than as a reason to stop: the worst case is one missed
		// window, against a schedule that would otherwise never fire
		// again.
		s.report(fmt.Errorf("croncmd: watermark for %s does not parse (%q), starting from now: %w", schedule.ID, value, err))
		return now, false, nil
	}
	if mark.After(now) {
		// A watermark in the future means somebody else's clock is ahead,
		// or this one is behind. Honour it: firing everything between the
		// two would be a burst of duplicates the claims would then have to
		// absorb.
		return now, false, nil
	}
	return mark, false, nil
}

func (s *Scheduler) putWatermark(ctx context.Context, id string, at time.Time) error {
	return s.Store.Put(ctx, s.watermarkKey(id), at.UTC().Format(time.RFC3339))
}

// pruneClaims removes claim keys for fires older than Retain.
//
// Claims are what make a fire happen once, so they cannot simply be
// dropped after use -- a deleted claim is an unclaimed fire. What bounds
// them is time: once a fire is far enough in the past that no scheduler
// would consider it again (Retain, held to at least twice CatchUp), its
// claim can go. The fire time is in the key, so this costs one scan and no
// value decoding.
func (s *Scheduler) pruneClaims(ctx context.Context, now time.Time) error {
	prefix := s.prefix() + claimPart
	pairs, err := s.Store.Scan(ctx, prefix)
	if err != nil {
		return fmt.Errorf("croncmd: prune claims: %w", err)
	}

	cutoff := now.Add(-s.retain())
	for _, pair := range pairs {
		fire, ok := claimKeyTime(pair.Key)
		if !ok {
			continue
		}
		if fire.Before(cutoff) {
			if err := s.Store.Delete(ctx, pair.Key); err != nil {
				s.report(err)
			}
		}
	}
	return nil
}

// claimKeyTime reads the fire time back out of a claim key, which is
// everything after the last "/". Returns false for a key that does not
// carry one, so an unrelated key sharing the prefix is left alone rather
// than deleted.
func claimKeyTime(key string) (time.Time, bool) {
	idx := strings.LastIndex(key, "/")
	if idx < 0 || idx == len(key)-1 {
		return time.Time{}, false
	}
	fire, err := time.Parse(claimTimeLayout, key[idx+1:])
	if err != nil {
		return time.Time{}, false
	}
	return fire, true
}
