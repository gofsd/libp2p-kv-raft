package main

// The cron commands drive examples/croncmd: a schedule says "submit
// command C at these times", and `cronserve` is a process that turns that
// into ordinary SubmitCommand dispatches.
//
// They live here for the same reason the journal commands do -- this is
// the binary that runs on a deployment target reached over SSH, and a
// scheduler has to run beside a daemon. It is also the binary to use for
// anything scripted: every `mage` invocation builds before it runs, which
// makes a loop of them heavy enough to starve the local daemon's IPC.
//
// The split is worth keeping in mind when deciding where to run what:
//
//   - cronserve is the only long-running one, and the only one that
//     submits anything. Run it on as many nodes as you like: fires are
//     claimed through raft before they are submitted, so more schedulers
//     mean more redundancy rather than more dispatches;
//   - cronput/list/get/delete/enable/disable edit replicated schedule
//     records, and can be run from anywhere with a daemon;
//   - cronfires reads back what the schedulers actually dispatched;
//   - cronnext needs no daemon at all -- it answers "when would this
//     expression fire" before anything is written.
//
// A scheduler submits under its *own* peer id, so the node running
// cronserve must have catalog standing for the commands its schedules
// name (`kvctl-cli addpeertogroup`/`addcommandtogroup`), exactly as a
// human submitter would. Putting a command on a timer grants nothing.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/croncmd"
)

// cronTimeout bounds the one-shot commands' own reads and writes. The
// scheduler itself is not bounded by it: it runs until interrupted.
const cronTimeout = 30 * time.Second

// cmdCronServe runs the scheduler in the foreground until interrupted.
func cmdCronServe(args []string) {
	if len(args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli cronserve [interval] [catchUp]")
		os.Exit(2)
	}
	interval := mustCronDuration(args, 0, croncmd.DefaultInterval, "cronserve")
	catchUp := mustCronDuration(args, 1, croncmd.DefaultCatchUp, "cronserve")

	store, err := croncmd.CurrentNode()
	if err != nil {
		cronFail("cronserve", err)
	}
	scheduler := &croncmd.Scheduler{
		Store:     store,
		Submitter: croncmd.Kvctl(),
		Interval:  interval,
		CatchUp:   catchUp,
		OnFire: func(s croncmd.Schedule, fire time.Time, instanceID string) {
			fmt.Printf("%s  %s -> %s  %s\n", fire.UTC().Format(time.RFC3339), s.ID, s.CommandID, instanceID)
		},
		OnSkip: func(s croncmd.Schedule, fire time.Time, reason string) {
			fmt.Printf("%s  %s skipped: %s\n", fire.UTC().Format(time.RFC3339), s.ID, reason)
		},
		OnError: func(err error) {
			fmt.Fprintf(os.Stderr, "cron: %v\n", err)
		},
	}

	stop := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		close(stop)
	}()

	fmt.Printf("scheduling every %s, catching up %s -- Ctrl-C to stop\n", interval, catchUp)
	scheduler.Run(context.Background(), stop)
}

// cmdCronPut creates or replaces a schedule.
func cmdCronPut(args []string) {
	if len(args) < 3 || len(args) > 5 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli cronput <id> <spec> <commandID> [inputsJSON] [location]`)
		os.Exit(2)
	}
	schedule := croncmd.Schedule{ID: args[0], Spec: args[1], CommandID: args[2]}
	if len(args) > 3 {
		schedule.Inputs = args[3]
	}
	if len(args) > 4 {
		schedule.Location = args[4]
	}

	catalog, ctx, cancel := mustCronCatalog("cronput")
	defer cancel()
	if err := catalog.Put(ctx, schedule); err != nil {
		cronFail("cronput", err)
	}

	// Show what it will actually do, since the point of failure with a
	// cron expression is usually that it parses and means something else.
	fmt.Printf("%s %s -> %s\n", schedule.ID, schedule.Spec, schedule.CommandID)
	printCronNext(schedule, 3)
}

// cmdCronList prints every schedule as JSON.
func cmdCronList(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli cronlist")
		os.Exit(2)
	}
	catalog, ctx, cancel := mustCronCatalog("cronlist")
	defer cancel()

	schedules, skipped, err := catalog.List(ctx)
	if err != nil {
		cronFail("cronlist", err)
	}
	// A record nobody can read is reported rather than hidden, but on
	// stderr: stdout stays a clean JSON document for whatever reads it.
	for _, bad := range skipped {
		fmt.Fprintf(os.Stderr, "cronlist: %v\n", bad)
	}
	cronPrintJSON("cronlist", schedules)
}

// cmdCronGet prints one schedule as JSON.
func cmdCronGet(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli cronget <id>")
		os.Exit(2)
	}
	catalog, ctx, cancel := mustCronCatalog("cronget")
	defer cancel()

	schedule, err := catalog.Get(ctx, args[0])
	if err != nil {
		cronFail("cronget", err)
	}
	cronPrintJSON("cronget", schedule)
}

// cmdCronDelete removes a schedule.
func cmdCronDelete(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli crondelete <id>")
		os.Exit(2)
	}
	catalog, ctx, cancel := mustCronCatalog("crondelete")
	defer cancel()

	if err := catalog.Delete(ctx, args[0]); err != nil {
		cronFail("crondelete", err)
	}
	fmt.Printf("%s deleted\n", args[0])
}

// cmdCronEnable and cmdCronDisable flip a schedule without deleting it,
// which is what an operator silencing something for an afternoon wants.
func cmdCronEnable(args []string)  { setCronEnabled(args, true, "cronenable") }
func cmdCronDisable(args []string) { setCronEnabled(args, false, "crondisable") }

func setCronEnabled(args []string, enabled bool, command string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: kvctl-cli %s <id>\n", command)
		os.Exit(2)
	}
	catalog, ctx, cancel := mustCronCatalog(command)
	defer cancel()

	if err := catalog.SetEnabled(ctx, args[0], enabled); err != nil {
		cronFail(command, err)
	}
	state := "disabled"
	if enabled {
		state = "enabled"
	}
	fmt.Printf("%s %s\n", args[0], state)
}

// cmdCronFires prints what the schedulers actually dispatched, most recent
// first -- read back out of the claim keys rather than from a separate log
// (see croncmd.Catalog.Fires), so it only goes back as far as those are
// retained.
func cmdCronFires(args []string) {
	if len(args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli cronfires [scheduleID] [limit]")
		os.Exit(2)
	}
	scheduleID := ""
	if len(args) > 0 {
		scheduleID = args[0]
	}
	limit := 0
	if len(args) > 1 {
		parsed, err := strconv.Atoi(args[1])
		if err != nil {
			cronFail("cronfires", fmt.Errorf("limit must be a number: %w", err))
		}
		limit = parsed
	}

	catalog, ctx, cancel := mustCronCatalog("cronfires")
	defer cancel()

	fires, err := catalog.Fires(ctx, scheduleID, limit)
	if err != nil {
		cronFail("cronfires", err)
	}
	cronPrintJSON("cronfires", fires)
}

// cmdCronNext answers "when would this fire" without writing anything, and
// without needing a daemon at all -- the one command here that is purely a
// question about the expression.
func cmdCronNext(args []string) {
	if len(args) < 1 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli cronnext <spec> [count] [location]`)
		os.Exit(2)
	}
	count := 5
	if len(args) > 1 {
		parsed, err := strconv.Atoi(args[1])
		if err != nil || parsed < 1 {
			cronFail("cronnext", fmt.Errorf("count must be a number of at least 1"))
		}
		count = parsed
	}
	schedule := croncmd.Schedule{ID: "preview", Spec: args[0], CommandID: "preview"}
	if len(args) > 2 {
		schedule.Location = args[2]
	}
	if _, err := croncmd.ParseSpec(schedule.Spec); err != nil {
		cronFail("cronnext", err)
	}
	printCronNext(schedule, count)
}

// printCronNext prints the next few times a schedule would fire, in its own
// zone and in UTC beside it. Both, because the two are what an operator is
// actually checking against: the wall clock they wrote the schedule in, and
// the one every other node reasons in.
func printCronNext(schedule croncmd.Schedule, count int) {
	fires, err := croncmd.NextFires(schedule, time.Now(), count)
	if err != nil {
		cronFail("cronnext", err)
	}
	for _, fire := range fires {
		fmt.Printf("  next %s  (%s UTC)\n", fire.Format("2006-01-02 15:04 MST"), fire.UTC().Format("2006-01-02 15:04"))
	}
	if len(fires) == 0 {
		fmt.Println("  never: nothing within the search horizon matches this expression")
	}
}

func mustCronCatalog(command string) (*croncmd.Catalog, context.Context, context.CancelFunc) {
	store, err := croncmd.CurrentNode()
	if err != nil {
		cronFail(command, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cronTimeout)
	return croncmd.NewCatalog(store), ctx, cancel
}

// mustCronDuration reads an optional positional duration, falling back to
// the package's own default.
func mustCronDuration(args []string, i int, fallback time.Duration, command string) time.Duration {
	if len(args) <= i || args[i] == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(args[i])
	if err != nil {
		cronFail(command, fmt.Errorf("%q is not a duration (try 20s, 1h): %w", args[i], err))
	}
	return parsed
}

func cronPrintJSON(command string, v any) {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		cronFail(command, err)
	}
	fmt.Println(string(out))
}

func cronFail(command string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
	os.Exit(1)
}
