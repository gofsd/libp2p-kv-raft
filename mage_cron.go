//go:build mage

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/croncmd"
)

// These wrap examples/croncmd -- a worked example rather than part of
// this repo's library (see its package doc), exposed here so a command
// can be put on a clock from a shell without writing a program first.
//
// What makes it worth an example is not the calendar. It is that a
// cluster has more than one node, any of them may be running a
// scheduler, and two schedulers noticing the same due minute must still
// produce one dispatch. croncmd claims each fire through a
// compare-and-swap that commits via raft before submitting it, so
// `mage cronserve` on every voter is a redundancy decision rather than a
// duplication bug.
//
// A scheduler submits under its *own* peer id, and the FSM checks that
// peer against the command's groups exactly as it would check a human's.
// So the node running cronserve needs catalog standing for the commands
// its schedules name (`mage addpeertogroup`/`addcommandtogroup`);
// putting a command on a timer grants nothing.
//
// The same operational note as the journal targets applies: every `mage`
// invocation builds before it runs, so for anything scripted use the
// identical commands on the kvctl-cli binary instead.

// cronTimeout bounds the one-shot targets' own reads and writes.
// CronServe is not bounded by it -- it runs until interrupted.
const cronTimeout = 30 * time.Second

// CronServe implements `mage cronserve [interval|""] [catchUp|""]`: runs
// the scheduler in the foreground against the current node, submitting
// commands as their schedules fall due. Stops on Ctrl-C.
//
// interval is how often to look for work (default 20s -- well under
// cron's one-minute resolution, so a poll does not drift into missing a
// minute). catchUp is how far back a fire missed while this was down is
// still submitted (default 1h); everything older is reported and
// skipped, which is what keeps a scheduler that was away for a week from
// dispatching a week of stale work at start-up.
//
// Safe to run on several nodes at once -- see the note above.
// Usage: mage cronserve [interval|""] [catchUp|""]
func CronServe(interval, catchUp string) error {
	every, err := parseCronDuration(interval, croncmd.DefaultInterval)
	if err != nil {
		return err
	}
	behind, err := parseCronDuration(catchUp, croncmd.DefaultCatchUp)
	if err != nil {
		return err
	}
	store, err := croncmd.CurrentNode()
	if err != nil {
		return err
	}

	scheduler := &croncmd.Scheduler{
		Store:     store,
		Submitter: croncmd.Kvctl(),
		Interval:  every,
		CatchUp:   behind,
		OnFire: func(s croncmd.Schedule, fire time.Time, instanceID string) {
			fmt.Printf("%s  %s -> %s  %s\n", fire.UTC().Format(time.RFC3339), s.ID, s.CommandID, instanceID)
		},
		OnSkip: func(s croncmd.Schedule, fire time.Time, reason string) {
			fmt.Printf("%s  %s skipped: %s\n", fire.UTC().Format(time.RFC3339), s.ID, reason)
		},
		OnError: func(err error) { fmt.Fprintf(os.Stderr, "cron: %v\n", err) },
	}

	stop := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		close(stop)
	}()

	fmt.Printf("scheduling every %s, catching up %s -- Ctrl-C to stop\n", every, behind)
	scheduler.Run(context.Background(), stop)
	return nil
}

// CronPut implements `mage cronput <id> <spec> <commandID> [inputsJSON|""]
// [location|""]`: creates or replaces a schedule.
//
// spec is a five-field cron expression (minute hour day-of-month month
// day-of-week) or an @-shortcut (@daily, @hourly, ...). inputsJSON is
// handed to the command verbatim, so it is whatever that command's own
// request shape is. location is an IANA zone the expression is read in,
// defaulting to UTC -- deliberately, since a wall-clock schedule in a
// zone with daylight saving has two days a year on which an hour does
// not happen or happens twice.
//
// The next few fire times are printed back, because a cron expression's
// usual failure is parsing cleanly and meaning something else.
// Usage: mage cronput <id> <spec> <commandID> [inputsJSON|""] [location|""]
func CronPut(id, spec, commandID, inputsJSON, location string) error {
	schedule := croncmd.Schedule{ID: id, Spec: spec, CommandID: commandID, Inputs: inputsJSON, Location: location}
	catalog, err := cronCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := cronContext()
	defer cancel()
	if err := catalog.Put(ctx, schedule); err != nil {
		return err
	}
	fmt.Printf("%s %s -> %s\n", schedule.ID, schedule.Spec, schedule.CommandID)
	return printCronNext(schedule, 3)
}

// CronList implements `mage cronlist`: prints every schedule as JSON.
// A record nobody can read is reported on stderr and passed over, so one
// bad row does not hide the rest.
// Usage: mage cronlist
func CronList() error {
	catalog, err := cronCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := cronContext()
	defer cancel()

	schedules, skipped, err := catalog.List(ctx)
	if err != nil {
		return err
	}
	for _, bad := range skipped {
		fmt.Fprintf(os.Stderr, "cronlist: %v\n", bad)
	}
	return printJSON(schedules)
}

// CronGet implements `mage cronget <id>`: prints one schedule as JSON.
// Usage: mage cronget <id>
func CronGet(id string) error {
	catalog, err := cronCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := cronContext()
	defer cancel()

	schedule, err := catalog.Get(ctx, id)
	if err != nil {
		return err
	}
	return printJSON(schedule)
}

// CronDelete implements `mage crondelete <id>`: removes a schedule and
// the watermark that tracked it. Claim keys are left alone -- they are
// what stops a re-created schedule of the same id from re-firing what it
// already fired, and they expire on their own.
// Usage: mage crondelete <id>
func CronDelete(id string) error {
	catalog, err := cronCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := cronContext()
	defer cancel()

	if err := catalog.Delete(ctx, id); err != nil {
		return err
	}
	fmt.Printf("%s deleted\n", id)
	return nil
}

// CronEnable implements `mage cronenable <id>`.
// Usage: mage cronenable <id>
func CronEnable(id string) error { return setCronEnabled(id, true) }

// CronDisable implements `mage crondisable <id>`: stops a schedule
// firing without deleting it, which is what silencing something for an
// afternoon actually wants.
// Usage: mage crondisable <id>
func CronDisable(id string) error { return setCronEnabled(id, false) }

func setCronEnabled(id string, enabled bool) error {
	catalog, err := cronCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := cronContext()
	defer cancel()

	if err := catalog.SetEnabled(ctx, id, enabled); err != nil {
		return err
	}
	state := "disabled"
	if enabled {
		state = "enabled"
	}
	fmt.Printf("%s %s\n", id, state)
	return nil
}

// CronFires implements `mage cronfires [scheduleID|""] [limit|""]`:
// prints what the schedulers actually dispatched, most recent first.
//
// This is read back out of the claim keys rather than from a log of its
// own (see croncmd.Catalog.Fires), so it reaches back exactly as far as
// those are retained -- a fortnight by default -- and a row with no
// instance id is a fire that was claimed but whose submission then
// failed.
// Usage: mage cronfires [scheduleID|""] [limit|""]
func CronFires(scheduleID, limit string) error {
	count := 0
	if limit != "" {
		parsed, err := strconv.Atoi(limit)
		if err != nil {
			return fmt.Errorf("cronfires: limit must be a number: %w", err)
		}
		count = parsed
	}
	catalog, err := cronCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := cronContext()
	defer cancel()

	fires, err := catalog.Fires(ctx, scheduleID, count)
	if err != nil {
		return err
	}
	return printJSON(fires)
}

// CronNext implements `mage cronnext <spec> [count|""] [location|""]`:
// prints when an expression would fire, without writing anything and
// without needing a daemon at all. The one target here that is purely a
// question about the expression.
// Usage: mage cronnext <spec> [count|""] [location|""]
func CronNext(spec, count, location string) error {
	howMany := 5
	if count != "" {
		parsed, err := strconv.Atoi(count)
		if err != nil || parsed < 1 {
			return fmt.Errorf("cronnext: count must be a number of at least 1")
		}
		howMany = parsed
	}
	if _, err := croncmd.ParseSpec(spec); err != nil {
		return err
	}
	return printCronNext(croncmd.Schedule{ID: "preview", Spec: spec, CommandID: "preview", Location: location}, howMany)
}

// printCronNext prints a schedule's next few fires in its own zone and in
// UTC beside it -- both, because those are the two clocks an operator is
// checking against: the one they wrote the schedule in, and the one every
// other node reasons in.
func printCronNext(schedule croncmd.Schedule, count int) error {
	fires, err := croncmd.NextFires(schedule, time.Now(), count)
	if err != nil {
		return err
	}
	if len(fires) == 0 {
		fmt.Println("  never: nothing within the search horizon matches this expression")
		return nil
	}
	for _, fire := range fires {
		fmt.Printf("  next %s  (%s UTC)\n", fire.Format("2006-01-02 15:04 MST"), fire.UTC().Format("2006-01-02 15:04"))
	}
	return nil
}

func cronCatalog() (*croncmd.Catalog, error) {
	store, err := croncmd.CurrentNode()
	if err != nil {
		return nil, err
	}
	return croncmd.NewCatalog(store), nil
}

func cronContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), cronTimeout)
}

// parseCronDuration reads an optional duration argument, falling back to
// the package's own default when it is empty.
func parseCronDuration(text string, fallback time.Duration) (time.Duration, error) {
	if text == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return 0, fmt.Errorf("%q is not a duration (try 20s, 1h): %w", text, err)
	}
	return parsed, nil
}
