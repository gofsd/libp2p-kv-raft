package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/croncmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// This file is the mobile counterpart of the cron commands on kvctl-cli:
// a schedule says "submit command C at these times", and a scheduler
// turns that into ordinary SubmitCommand dispatches. It is the same
// examples/croncmd package on both sides -- what differs is only how a
// dispatch is submitted (this package's own SubmitCommand rather than
// kvctl's) and where the session comes from.
//
// # Why running one on a phone is not a duplication bug
//
// It is the property the whole package is built on: every fire is
// claimed through a raft-committed compare-and-swap before it is
// submitted, so a phone, a laptop and three bootstrap nodes all running
// schedulers produce one dispatch per fire between them. That in turn is
// what makes a phone a reasonable place for one despite being the worst
// possible clock-keeper -- it sleeps, it loses its network, it is killed
// by the OS. A phone that misses a fire loses nothing if any other
// scheduler is up, and if none is, it catches the fire up itself within
// Scheduler.CatchUp when it wakes.
//
// What it does *not* get from anywhere else is standing: a scheduler
// submits under this device's own peer id, and the FSM checks that peer
// against the command's groups exactly as it would check a human's
// (catalog.go). A device that may not submit a command may not put it on
// a timer either.
//
// # Lifetime
//
// The scheduler outlives Stop, deliberately, the same way this package's
// other watch loops do (see RunCommandDispatcher): it reads the session
// afresh on every tick, so a torn-down daemon makes a tick fail and be
// reported rather than making the loop exit, and a later Start picks it
// straight back up. StopCronServe is what actually ends it.
//
// Everything else here is JSON in and JSON out, gomobile's only real
// option: no maps, no structs, no slices across the binding.

var (
	cronMu sync.Mutex
	// cronCancel and cronDone belong to the running scheduler, if any.
	cronCancel context.CancelFunc
	cronDone   chan struct{}
)

// CronListener receives what a running scheduler does, for a UI that
// wants to show it live. Every method is called from the scheduler's own
// goroutine, so a Kotlin implementation must post to the main thread
// itself rather than touching views directly.
//
// This is the live view. The durable one is CronFires, which reads back
// what was actually dispatched and survives the app being killed.
type CronListener interface {
	// OnFire reports a submitted dispatch. fire is the minute the
	// schedule was due, RFC3339 in UTC -- not when the submission
	// happened, which may be up to one interval later.
	OnFire(scheduleID, commandID, fire, instanceID string)
	// OnSkip reports a fire that was not submitted: one older than the
	// catch-up window, or one another scheduler claimed first.
	OnSkip(scheduleID, fire, reason string)
	// OnError reports a transient failure. None of them stops the
	// scheduler.
	OnError(message string)
}

// CronServe starts the scheduler on this device, submitting commands as
// their schedules fall due, until StopCronServe.
//
// intervalSeconds is how often to look for work (0 means the package
// default, 20s -- well under cron's one-minute resolution, so a poll does
// not drift into missing a minute). catchUpSeconds is how far back a fire
// missed while this device was asleep is still submitted (0 means 1h);
// everything older is skipped and reported, which is what stops a phone
// that was off overnight from dispatching a night of stale work when it
// wakes.
//
// Safe to run alongside schedulers on other nodes -- see the note above.
// Calling it again replaces any scheduler already running.
func CronServe(intervalSeconds, catchUpSeconds int) error {
	return cronServe(intervalSeconds, catchUpSeconds, nil)
}

// CronServeWithListener is CronServe with a listener attached, for a UI
// that wants to show each fire as it happens.
func CronServeWithListener(intervalSeconds, catchUpSeconds int, listener CronListener) error {
	if listener == nil {
		return fmt.Errorf("kvmobile: CronServeWithListener: listener must not be nil -- use CronServe")
	}
	return cronServe(intervalSeconds, catchUpSeconds, listener)
}

func cronServe(intervalSeconds, catchUpSeconds int, listener CronListener) error {
	interval, err := cronDuration(intervalSeconds, croncmd.DefaultInterval, "interval")
	if err != nil {
		return err
	}
	catchUp, err := cronDuration(catchUpSeconds, croncmd.DefaultCatchUp, "catchUp")
	if err != nil {
		return err
	}

	scheduler := &croncmd.Scheduler{
		Store:     cronStore(),
		Submitter: cronSubmitter{},
		Interval:  interval,
		CatchUp:   catchUp,
	}
	if listener != nil {
		scheduler.OnFire = func(s croncmd.Schedule, fire time.Time, instanceID string) {
			listener.OnFire(s.ID, s.CommandID, fire.UTC().Format(time.RFC3339), instanceID)
		}
		scheduler.OnSkip = func(s croncmd.Schedule, fire time.Time, reason string) {
			listener.OnSkip(s.ID, fire.UTC().Format(time.RFC3339), reason)
		}
		scheduler.OnError = func(err error) { listener.OnError(err.Error()) }
	}

	cronMu.Lock()
	defer cronMu.Unlock()
	stopCronLocked()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	cronCancel, cronDone = cancel, done
	go func() {
		defer close(done)
		scheduler.Run(ctx, nil)
	}()
	return nil
}

// StopCronServe stops the scheduler and waits for it to actually exit.
// Safe to call when nothing is running (a no-op).
func StopCronServe() {
	cronMu.Lock()
	defer cronMu.Unlock()
	stopCronLocked()
}

// CronServing reports whether a scheduler is running on this device --
// what a settings screen shows a switch from.
func CronServing() bool {
	cronMu.Lock()
	defer cronMu.Unlock()
	return cronDone != nil
}

// stopCronLocked requires cronMu already held.
func stopCronLocked() {
	if cronDone == nil {
		return
	}
	cronCancel()
	<-cronDone
	cronCancel, cronDone = nil, nil
}

// CronPut creates or replaces a schedule.
//
// spec is a five-field cron expression (minute hour day-of-month month
// day-of-week) or an @-shortcut (@daily, @hourly, ...). inputsJSON is
// handed to the command verbatim, so it is whatever that command's own
// request shape is. location is an IANA zone the expression is read in,
// empty meaning UTC -- deliberately, since a wall-clock schedule in a
// zone with daylight saving has two days a year on which an hour does not
// happen or happens twice. Note that this is the *schedule's* zone, not
// the phone's: a device that travels does not move its schedules.
//
// An expression that does not parse is refused here, where a form can
// still show the error, rather than silently at the next tick.
func CronPut(id, spec, commandID, inputsJSON, location string) error {
	ctx, cancel := cronContext()
	defer cancel()
	return cronCatalog().Put(ctx, croncmd.Schedule{
		ID: id, Spec: spec, CommandID: commandID, Inputs: inputsJSON, Location: location,
	})
}

// CronList returns every schedule as a JSON array, ascending by id. A
// record nobody can read is left out rather than failing the call.
func CronList() (string, error) {
	ctx, cancel := cronContext()
	defer cancel()
	schedules, _, err := cronCatalog().List(ctx)
	if err != nil {
		return "", err
	}
	return cronJSON(schedules)
}

// CronGet returns one schedule as JSON.
func CronGet(id string) (string, error) {
	ctx, cancel := cronContext()
	defer cancel()
	schedule, err := cronCatalog().Get(ctx, id)
	if err != nil {
		return "", err
	}
	return cronJSON(schedule)
}

// CronDelete removes a schedule and the watermark that tracked it. Claim
// keys are left alone: they are what stops a re-created schedule of the
// same id from re-firing what it already fired.
func CronDelete(id string) error {
	ctx, cancel := cronContext()
	defer cancel()
	return cronCatalog().Delete(ctx, id)
}

// CronSetEnabled stops a schedule firing without deleting it, or starts it
// again -- what a switch beside each row in a list is bound to.
func CronSetEnabled(id string, enabled bool) error {
	ctx, cancel := cronContext()
	defer cancel()
	return cronCatalog().SetEnabled(ctx, id, enabled)
}

// CronFires returns what the cluster's schedulers actually dispatched,
// most recent first, as a JSON array; scheduleID empty means every
// schedule's, and limit 0 means all of them.
//
// This is read back out of the claim keys rather than from a log of its
// own, so it reaches back exactly as far as those are retained -- a
// fortnight by default -- and it shows fires claimed by *any* node, not
// only this one. A row with no instance id is a fire that was claimed but
// whose submission then failed.
func CronFires(scheduleID string, limit int) (string, error) {
	ctx, cancel := cronContext()
	defer cancel()
	fires, err := cronCatalog().Fires(ctx, scheduleID, limit)
	if err != nil {
		return "", err
	}
	return cronJSON(fires)
}

// CronNext returns the next count times an expression would fire, as a
// JSON array of RFC3339 timestamps in the named zone. It reads no state
// and needs no daemon -- it is a question about the expression, which is
// what a form validates against before writing anything.
func CronNext(spec string, count int, location string) (string, error) {
	if count < 1 {
		return "", fmt.Errorf("kvmobile: cron: count must be at least 1, got %d", count)
	}
	if _, err := croncmd.ParseSpec(spec); err != nil {
		return "", err
	}
	fires, err := croncmd.NextFires(
		croncmd.Schedule{ID: "preview", Spec: spec, CommandID: "preview", Location: location},
		time.Now(), count)
	if err != nil {
		return "", err
	}
	stamps := make([]string, 0, len(fires))
	for _, fire := range fires {
		stamps = append(stamps, fire.Format(time.RFC3339))
	}
	return cronJSON(stamps)
}

// cronSubmitter dispatches through this package's own SubmitCommand,
// which is what makes the scheduler work here at all: kvctl's goes
// through a desktop registry this device has none of.
type cronSubmitter struct{}

func (cronSubmitter) Submit(ctx context.Context, commandID, inputs string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return SubmitCommand(commandID, inputs)
}

// cronStore resolves the session per call rather than holding one, so a
// Stop/Start cycle underneath a running scheduler is a failed tick rather
// than a scheduler wedged against a dead session.
func cronStore() croncmd.Store {
	return croncmd.Sessions(func(context.Context) (*shmclient.Session, error) {
		return currentSession()
	})
}

func cronCatalog() *croncmd.Catalog { return croncmd.NewCatalog(cronStore()) }

func cronContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), callTimeout)
}

// cronDuration reads a seconds argument, treating 0 as "the package's own
// default" and refusing a negative one outright rather than letting it
// become a default silently.
func cronDuration(seconds int, fallback time.Duration, name string) (time.Duration, error) {
	if seconds < 0 {
		return 0, fmt.Errorf("kvmobile: cron: %s must not be negative, got %d", name, seconds)
	}
	if seconds == 0 {
		return fallback, nil
	}
	return time.Duration(seconds) * time.Second, nil
}

func cronJSON(v any) (string, error) {
	out, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("kvmobile: cron: encode result: %w", err)
	}
	return string(out), nil
}
