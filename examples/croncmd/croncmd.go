// Package croncmd runs this repo's Group/Command catalog on a clock: a
// schedule says "submit command C with these inputs at these times", and a
// scheduler process turns that into ordinary SubmitCommand dispatches.
//
// A worked example, like examples/relations beside it. It adds nothing to
// the core library and needs no daemon, kvfsm or wire change -- schedules
// are ordinary user-namespace keys, and a fire is an ordinary
// kvctl.SubmitCommand call, which means everything the catalog already
// enforces still applies unchanged: the FSM checks the *scheduler's* own
// peer id against the command's groups (isPermittedForCommand) exactly as
// it would check a human's, and a scheduler with no standing for a command
// cannot make one run by putting it on a timer.
//
// # Why this is not just time.AfterFunc
//
// The interesting problem is not the calendar, it is that this cluster has
// more than one node and any of them may be running a scheduler. Two
// schedulers that both notice "02:00 has arrived" both submit, and the
// command runs twice -- which for a backup or a report is merely untidy
// and for anything that moves something is a bug.
//
// The fix is the primitive this repo already has and a cron package does
// not: a compare-and-swap against replicated state. Before submitting,
// a scheduler *claims* the fire by writing a key naming that exact
// (schedule, minute) pair, under a precondition that the key does not
// already exist (Store.Claim, backed by shmevent's OpCompareAbsent). Raft
// serialises those claims, so exactly one scheduler wins each fire and the
// losers skip it and move on. Run a scheduler on every voter if you like;
// the command still runs once.
//
// That also makes the answer to "what happens while a node is down"
// uninteresting, which is the point. The claim keys and the watermark are
// replicated, so a scheduler that starts up on a different node picks up
// where the cluster -- not that process -- left off.
//
// # Catching up, and refusing to
//
// A scheduler that was down for a week and then starts finds a week of
// missed fires. Submitting all of them is almost never what anybody wants
// (see Scheduler.CatchUp): the default is to submit only fires missed
// within the last hour and to skip the rest, recording that it did. The
// alternative -- a thundering herd of stale dispatches at start-up -- is
// the failure mode this kind of scheduler is usually written into.
//
// # Driving it
//
// Desktop: `mage cronput`/`cronserve`/`cronlist`/... , and the identical
// commands on the kvctl-cli binary, which is what a deployment target
// reached over SSH has. Android: mobile/kvmobile's CronPut/CronServe/...
// bindings, driving this same package against the same replicated
// records. See README.md in this directory for the walkthrough of both,
// and for the two cron behaviours worth knowing before writing a schedule
// (the day-of-month/day-of-week union, and why UTC is the default zone).
//
// # What it does not do
//
// It does not run commands; it submits them. Whoever owns the command
// (Command.PeerID, and the kvctl.RunCommandDispatcher process behind it --
// see examples/relations/journalcmd for one) still does the work and still
// writes the execution log. A scheduler is just another submitter, which
// is exactly why it needs no privileges of its own.
package croncmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// DefaultKeyPrefix is where a scheduler keeps its own records, in the
// ordinary user keyspace. It is a plain text prefix rather than a reserved
// namespace byte because that is all an example needs: 0x00
// (shmevent.SystemKeyPrefix) and pkg/logrecord's prefix are the two a
// daemon actually refuses, and this is neither. Change it (Scheduler.
// KeyPrefix) to run more than one independent scheduler on one cluster.
const DefaultKeyPrefix = "cron/"

// The three kinds of record a scheduler keeps, as key infixes under the
// prefix. Keeping them under distinct infixes is what lets List scan
// schedules alone without walking a claim key per fire per schedule.
const (
	schedulePart  = "schedule/"
	watermarkPart = "watermark/"
	claimPart     = "claim/"
)

// Schedule is one "submit this command at these times" rule, stored as
// JSON under a schedule key.
//
// Inputs is passed to SubmitCommand verbatim and is opaque here exactly as
// it is there -- croncmd never looks inside it, so a schedule can drive
// any command, including one whose request shape did not exist when this
// package was written (examples/relations/journalcmd.Request, say).
type Schedule struct {
	// ID names this schedule, and is what a claim key is built from. It
	// must not contain "/" -- see validate.
	ID string `json:"id"`
	// Spec is the cron expression, as written. See ParseSpec.
	Spec string `json:"spec"`
	// CommandID is the catalog Command to submit.
	CommandID string `json:"command_id"`
	// Inputs is the caller-defined JSON handed to SubmitCommand.
	Inputs string `json:"inputs,omitempty"`
	// Location is the IANA zone the expression is read in ("" means UTC).
	//
	// UTC is the default deliberately. A wall-clock schedule in a zone
	// with daylight saving has two days a year on which an hour does not
	// happen or happens twice, and a scheduler that claims fires by
	// wall-clock minute would skip or double them; keeping the default
	// zone one that never jumps means an operator opts into that only by
	// naming a zone.
	Location string `json:"location,omitempty"`
	// Disabled stops this schedule firing without deleting it.
	Disabled bool `json:"disabled,omitempty"`
	// Comment is free text for whoever reads the schedule list.
	Comment string `json:"comment,omitempty"`
}

// validate reports whether s is usable, and is called both on the way in
// (Put) and on the way out (List): a record written by an older or
// hand-rolled client should not be able to stop a scheduler by being
// unparseable, so List skips what this refuses rather than failing whole.
func (s Schedule) validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return fmt.Errorf("croncmd: a schedule needs an id")
	}
	// The id is a key component, so a "/" in it would let one schedule's
	// claim keys collide with another's infix.
	if strings.Contains(s.ID, "/") {
		return fmt.Errorf("croncmd: schedule id %q must not contain '/'", s.ID)
	}
	if strings.TrimSpace(s.CommandID) == "" {
		return fmt.Errorf("croncmd: schedule %s needs a command id", s.ID)
	}
	if _, err := ParseSpec(s.Spec); err != nil {
		return err
	}
	if _, err := s.location(); err != nil {
		return err
	}
	return nil
}

// location resolves Location, defaulting to UTC.
func (s Schedule) location() (*time.Location, error) {
	if strings.TrimSpace(s.Location) == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(s.Location)
	if err != nil {
		return nil, fmt.Errorf("croncmd: schedule %s: unknown location %q: %w", s.ID, s.Location, err)
	}
	return loc, nil
}

// parsed returns the schedule's compiled spec and location together, which
// is what every caller that needs either actually wants.
func (s Schedule) parsed() (Spec, *time.Location, error) {
	spec, err := ParseSpec(s.Spec)
	if err != nil {
		return Spec{}, nil, err
	}
	loc, err := s.location()
	if err != nil {
		return Spec{}, nil, err
	}
	return spec, loc, nil
}

// Pair is one key/value pair from a Store scan.
type Pair struct {
	Key   string
	Value string
}

// Store is the whole of what a scheduler needs from the key/value store:
// read a key, list a prefix, write, delete, and -- the one that carries
// the design -- create a key only if it does not already exist.
//
// It is an interface for the usual reason an example makes one: Memory()
// runs the scheduler's own logic in a test with no cluster at all, while
// CurrentNode() is the real, raft-replicated implementation. Claim is only
// as good as its backing: against Memory it excludes goroutines in one
// process, against a node it excludes every scheduler in the cluster.
type Store interface {
	// Get reads key, reporting whether it exists at all.
	Get(ctx context.Context, key string) (string, bool, error)
	// Scan returns every pair whose key starts with prefix, ascending.
	Scan(ctx context.Context, prefix string) ([]Pair, error)
	// Put writes value at key unconditionally.
	Put(ctx context.Context, key, value string) error
	// Delete removes key. Deleting a key that does not exist is not an
	// error.
	Delete(ctx context.Context, key string) error
	// Claim writes value at key only if key does not exist, and reports
	// whether this caller was the one that created it. A false return is
	// an ordinary outcome -- somebody else got there first -- not a
	// failure.
	Claim(ctx context.Context, key, value string) (bool, error)
}

// Submitter is how a scheduler dispatches a fire. Kvctl() is the real one;
// a test supplies its own to observe what would have been submitted
// without needing a catalog, a command or a daemon.
type Submitter interface {
	// Submit dispatches commandID with inputs and returns the instance id
	// the dispatch was recorded under.
	Submit(ctx context.Context, commandID, inputs string) (string, error)
}

// kvctlSubmitter dispatches through the ordinary catalog path.
type kvctlSubmitter struct{}

// Kvctl returns the Submitter that actually dispatches commands, via
// kvctl.SubmitCommand against whichever node `mage use` selected.
//
// The scheduler's own standing is what is checked: SubmitCommand signs as
// the local node, and the FSM re-evaluates IsPermittedForCommand against
// that peer id on every replica. So the peer running the scheduler must be
// in a group the command is linked to -- see README's "Group/command ACL".
func Kvctl() Submitter { return kvctlSubmitter{} }

func (kvctlSubmitter) Submit(ctx context.Context, commandID, inputs string) (string, error) {
	// kvctl.SubmitCommand applies its own IPC timeout internally and takes
	// no context; honour cancellation up to the point of the call rather
	// than pretending to interrupt it.
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return kvctl.SubmitCommand(commandID, inputs)
}

// Catalog is the schedule list as an operator edits it: the CRUD half of
// the package, with no clock in it. A Scheduler reads the same records.
type Catalog struct {
	// Store is where schedules live.
	Store Store
	// KeyPrefix defaults to DefaultKeyPrefix.
	KeyPrefix string
}

// NewCatalog returns a Catalog over store with the default key prefix.
func NewCatalog(store Store) *Catalog { return &Catalog{Store: store} }

func (c *Catalog) prefix() string {
	if c.KeyPrefix == "" {
		return DefaultKeyPrefix
	}
	return c.KeyPrefix
}

func (c *Catalog) scheduleKey(id string) string { return c.prefix() + schedulePart + id }

// Put creates or replaces a schedule, refusing one that is not usable --
// an unparseable expression is worth catching where an operator can still
// read the error, not silently at the next tick.
func (c *Catalog) Put(ctx context.Context, s Schedule) error {
	if err := s.validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("croncmd: encode schedule %s: %w", s.ID, err)
	}
	return c.Store.Put(ctx, c.scheduleKey(s.ID), string(encoded))
}

// Get returns one schedule by id.
func (c *Catalog) Get(ctx context.Context, id string) (Schedule, error) {
	value, ok, err := c.Store.Get(ctx, c.scheduleKey(id))
	if err != nil {
		return Schedule{}, err
	}
	if !ok {
		return Schedule{}, fmt.Errorf("croncmd: no schedule %s", id)
	}
	var s Schedule
	if err := json.Unmarshal([]byte(value), &s); err != nil {
		return Schedule{}, fmt.Errorf("croncmd: schedule %s does not parse: %w", id, err)
	}
	return s, nil
}

// List returns every schedule, ascending by id.
//
// A record that does not parse, or that names an expression this build
// cannot read, is skipped and reported in the second return value rather
// than failing the call: one bad row must not stop every other schedule in
// the cluster from firing.
func (c *Catalog) List(ctx context.Context) ([]Schedule, []error, error) {
	pairs, err := c.Store.Scan(ctx, c.prefix()+schedulePart)
	if err != nil {
		return nil, nil, err
	}

	schedules := make([]Schedule, 0, len(pairs))
	var skipped []error
	for _, pair := range pairs {
		var s Schedule
		if err := json.Unmarshal([]byte(pair.Value), &s); err != nil {
			skipped = append(skipped, fmt.Errorf("croncmd: schedule at %s does not parse: %w", pair.Key, err))
			continue
		}
		if err := s.validate(); err != nil {
			skipped = append(skipped, err)
			continue
		}
		schedules = append(schedules, s)
	}
	return schedules, skipped, nil
}

// Delete removes a schedule and the watermark that tracked it. Claim keys
// are left alone: they are what stops a re-created schedule of the same id
// from re-firing everything it already fired, and they expire on their own
// (see Scheduler.Retain).
func (c *Catalog) Delete(ctx context.Context, id string) error {
	if err := c.Store.Delete(ctx, c.scheduleKey(id)); err != nil {
		return err
	}
	return c.Store.Delete(ctx, c.prefix()+watermarkPart+id)
}

// SetEnabled flips a schedule's Disabled flag.
func (c *Catalog) SetEnabled(ctx context.Context, id string, enabled bool) error {
	s, err := c.Get(ctx, id)
	if err != nil {
		return err
	}
	s.Disabled = !enabled
	return c.Put(ctx, s)
}

// NextFires returns the next count times schedule would fire after from,
// in the schedule's own location.
//
// It answers a question about the expression, not about the cluster: it
// reads no state, honours no watermark, and deliberately ignores Disabled,
// because what an operator is checking with it is whether the expression
// they wrote means what they think -- which is the failure mode a cron
// expression actually has. A short result means the search ran out of
// horizon (see Spec.Next), and an empty one that the expression matches no
// instant that will ever exist.
func NextFires(schedule Schedule, from time.Time, count int) ([]time.Time, error) {
	spec, loc, err := schedule.parsed()
	if err != nil {
		return nil, err
	}
	fires := make([]time.Time, 0, count)
	cursor := from.In(loc)
	for len(fires) < count {
		fire, ok := spec.Next(cursor)
		if !ok {
			break
		}
		fires = append(fires, fire)
		cursor = fire
	}
	return fires, nil
}
