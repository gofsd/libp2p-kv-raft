# examples/croncmd — a command scheduler for a cluster, not for a machine

A worked example, like [`examples/relations`](../relations) beside it: it adds nothing to the core
library and needs no daemon, `kvfsm`, or wire change. A schedule is an ordinary user-namespace key;
a fire is an ordinary `SubmitCommand` dispatch through the
[Group/Command catalog](../../README.md#groupcommand-acl). Everything the catalog already enforces
still applies unchanged.

It runs on desktop (`mage cron*`, `kvctl-cli cron*`) and on Android (`kvmobile`'s `Cron*` bindings),
against the same replicated records.

---

## Why this is not `cron`, and not `time.AfterFunc`

The calendar is the easy half, and this package writes it out in
[`spec.go`](spec.go) in about three hundred lines: five bitmasks and a stepping search.

The interesting problem is that a cluster has more than one node. Point `cron` at a three-node
cluster and you get one of two failure modes:

* run the scheduler on **one** node, and it is a single point of failure with no failover — the
  nightly backup silently stops happening when that box is down;
* run it on **all** of them, and every schedule fires once per node. For a report that is untidy.
  For anything that moves something, it is a bug.

The fix is a primitive this repo has and a cron package does not: **a compare-and-swap against
replicated state**. Before submitting, a scheduler *claims* the fire — writes a key naming that
exact `(schedule, minute)` pair, under a precondition that the key does not already exist. Raft
serialises those claims, so exactly one scheduler wins each fire and the losers skip it.

```
                 02:00 arrives
                       |
      +----------------+----------------+
      |                |                |
  scheduler A      scheduler B      scheduler C        (one per node, all awake)
      |                |                |
      +-- claim cron/claim/nightly/20260819T0200Z -----+
      |                |                |
   created         refused          refused            <- raft decided the order
      |                |                |
   SubmitCommand     skip             skip
```

Run a scheduler on every voter if you like. The command still runs once. That is the whole design;
the rest of this README is consequences of it.

---

## What is stored

Three kinds of record, all under one plain-text prefix (`cron/` by default — a *user* namespace
byte, not one of the two the daemon reserves). Distinct infixes are what let a scheduler list
schedules without walking a claim key per fire per schedule.

```
cron/schedule/<id>                     the rule, as JSON
cron/watermark/<id>                    how far this schedule has been considered, RFC3339
cron/claim/<id>/20260819T0200Z         one fire: won by exactly one scheduler
```

A schedule:

```json
{
  "id": "nightly",
  "spec": "0 2 * * *",
  "command_id": "cmd-backup",
  "inputs": "{\"op\":\"run\"}",
  "location": "Europe/Berlin",
  "disabled": false,
  "comment": "the nightly backup"
}
```

`inputs` is opaque here and passed to `SubmitCommand` verbatim, which is why a schedule can drive
any command — including one whose request shape did not exist when this package was written
(`journalcmd.Request`, say).

### The watermark and the claim are two different mechanisms

Worth separating, because they look redundant and are not.

The **watermark** is what makes the ordinary case cheap. A scheduler asks "what has been considered
already", enumerates the fires between there and now, and moves it forward. Two schedulers ticking
one after the other — which is what staggered tickers usually amount to — never reach a claim at
all: the second reads a watermark the first already advanced and finds nothing due.

The **claim** is what covers the case the watermark cannot: two schedulers that both read the
watermark *before* either wrote it. That is a genuine race, it is the one that duplicates a
dispatch, and it is the only thing raft-ordered state can settle.

### The claim is also the audit trail

A claim key is not just a lock. Once the dispatch succeeds, the scheduler writes the instance id
back into it, so the key answers "this minute of this schedule became that dispatch" — which is the
only durable answer to *did the nightly backup actually run last night*. `Catalog.Fires`
(`mage cronfires`, `kvctl-cli cronfires`, `CronFires`) reads it back, most recent first.

It is a by-product, not a second log: nothing writes it separately, and it reaches back exactly as
far as claims are retained (a fortnight by default). A row with **no instance id** is a fire that
was claimed but whose submission then failed — see *Retrying* below.

---

## Catching up, and refusing to

A scheduler that was down for a week and then starts finds a week of missed fires. Submitting all of
them is almost never what anybody wants, and a thundering herd of stale dispatches at start-up is
the failure mode this kind of scheduler is usually written into.

So `CatchUp` (default **1h**) bounds how far back a missed fire is still submitted. Everything older
is skipped and *reported* as skipped, never submitted. "The scheduler was down over lunch" still
produces the fires; "it was down over the weekend" does not.

This is also why claim keys are retained far longer than they are useful (`Retain`, default a
fortnight) and why `Retain` is held to **at least twice `CatchUp`** however it is set: pruning a
claim while its fire is still inside the catch-up window would let that fire be claimed and
submitted a second time, which is the one thing this package exists to prevent.

### Retrying

When the claim succeeded but the submission itself then failed, there is no right answer, only a
choice:

* `RetryFailedSubmit: false` (the default) keeps the claim, so that fire is **never retried**. A
  submission that failed *after* the daemon accepted the write — which a client cannot tell apart
  from one that failed before — would otherwise run the command twice.
* `RetryFailedSubmit: true` releases the claim, so some scheduler retries the fire while it is still
  inside `CatchUp`.

Possibly missing a fire, or possibly duplicating one. The default matches what the rest of the
package is for.

---

## The expression

Five fields — minute, hour, day-of-month, month, day-of-week — or an `@`-shortcut (`@yearly`,
`@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`).

Each field is a comma-separated list of terms, where a term is `*`, a single value, or an inclusive
`lo-hi` range, any of which may carry a `/step`. A bare `value/step` reads as "from value to the end
of the field, every step". Months and weekdays accept three-letter names (`jan`, `mon`), weekday `7`
is accepted for Sunday.

**Seconds are not a field.** This scheduler's resolution is one minute, so admitting one would let
an operator write a schedule that silently could not be honoured.

Two behaviours are worth knowing before writing a schedule, and are the reason this package parses
expressions itself rather than inheriting some other package's opinion about them:

**Day-of-month and day-of-week are a union, not an intersection.** When *both* are restricted, a day
matches if *either* does. `0 0 1 * MON` is "the first of the month, **and also** every Monday", not
"Mondays that fall on the first". When only one is restricted, that one decides alone. This is Vixie
cron's behaviour and what most schedules are written against.

**UTC is the default zone, deliberately.** A wall-clock schedule in a zone with daylight saving has
two days a year on which an hour does not happen or happens twice, and a scheduler that claims fires
by wall-clock minute would skip or double them. Naming a `location` opts into that knowingly; the
consequence is that a schedule fixed to an hour inside a skipped window does not fire on that day in
that zone.

`cronnext` answers "when would this actually fire" without writing anything, and without needing a
daemon at all — a cron expression's usual failure is parsing cleanly and meaning something else:

```bash
$ mage cronnext "0 3 * * mon" 3 "Europe/Berlin"
  next 2026-08-24 03:00 CEST  (2026-08-24 01:00 UTC)
  next 2026-08-31 03:00 CEST  (2026-08-31 01:00 UTC)
  next 2026-09-07 03:00 CEST  (2026-09-07 01:00 UTC)
```

---

## Permissions: a timer grants nothing

A scheduler submits under **its own peer id**, and `kvfsm` re-evaluates `isPermittedForCommand`
against that peer on every replica, exactly as it would for a human submitter. So the node running
the scheduler must be in a group the command is linked to.

A scheduler with no standing for a command cannot make one run by putting it on a timer. This is
also why the package needs no privileges of its own: it is just another submitter.

It does not *run* commands, either — it submits them. Whoever owns the command (`Command.PeerID`,
and the dispatcher process behind it — see [`journalcmd`](../relations/journalcmd)) still does the
work and still writes the execution log.

---

## Running it: desktop

```bash
# The command must exist and this node must be allowed to submit it.
mage createcommand cmd-backup "Nightly backup" <peerID-that-does-the-work>
mage creategroup grp-ops "Operations" false
mage addcommandtogroup cmd-backup grp-ops
mage addpeertogroup <peerID-running-the-scheduler> grp-ops

# Write the schedule. The next few fires are printed back as a check.
mage cronput nightly "0 2 * * *" cmd-backup '{"op":"run"}' "Europe/Berlin"

# Run the scheduler in the foreground. Safe to do this on every voter.
mage cronserve
```

The rest:

```bash
mage cronlist                            # every schedule, as JSON
mage cronget <id>
mage crondisable <id>                    # silence it without deleting it
mage cronenable <id>
mage crondelete <id>                     # drops the schedule and its watermark, keeps the claims
mage cronfires <scheduleID|""> <limit|""> # what was actually dispatched, most recent first
mage cronnext <spec> [count|""] [location|""]
```

Every `mage` invocation builds before it runs, so for anything scripted — and on a deployment target
reached over SSH, which has `kvnode` and `kvctl-cli` on it and nothing else — use the identical
commands on the binary: `kvctl-cli cronserve`, `kvctl-cli cronput ...`, and so on.

---

## Running it: Android

The same package, through `mobile/kvmobile`'s gomobile bindings. The only differences are that a
dispatch goes through `kvmobile`'s own `SubmitCommand` rather than `kvctl`'s, and that the session
is resolved per call — so a `Stop`/`Start` cycle underneath a running scheduler is a failed tick
that gets reported, not a scheduler wedged against a dead session.

```kotlin
// Check the expression before writing anything -- needs no daemon.
val preview = Kvmobile.cronNext("0 2 * * *", 3, "Europe/Berlin")   // JSON array of RFC3339 times

Kvmobile.cronPut("nightly", "0 2 * * *", "cmd-backup", """{"op":"run"}""", "Europe/Berlin")

// Live view of what this device's scheduler does. Called from the scheduler's
// own goroutine -- post to the main thread yourself.
Kvmobile.cronServeWithListener(0, 0, object : CronListener {      // 0, 0 = package defaults
    override fun onFire(scheduleId: String, commandId: String, fire: String, instanceId: String) { ... }
    override fun onSkip(scheduleId: String, fire: String, reason: String) { ... }
    override fun onError(message: String) { ... }
})

// Durable view: what the cluster's schedulers dispatched, this device or not.
val fires = Kvmobile.cronFires("nightly", 20)                      // JSON array
Kvmobile.cronServing()                                             // for a settings switch
Kvmobile.stopCronServe()
```

A phone is the worst clock-keeper in any cluster — it sleeps, it loses its network, the OS kills it.
That is fine here, and is the point of claiming fires rather than owning them:

* if any other scheduler is up, a phone that misses a fire loses nothing — someone else claimed it;
* if none is, the phone catches the fire up itself when it wakes, within `CatchUp`;
* and a phone that wakes after a week does **not** dispatch a week of stale work, because
  everything past `CatchUp` is skipped and reported instead.

The scheduler deliberately outlives `Stop`, the way this package's other watch loops do: a
torn-down daemon makes a tick fail and be reported rather than making the loop exit, and a later
`Start` picks it straight back up. `stopCronServe` is what actually ends it.

---

## Testing it

The scheduler's own logic is written against a `Store` interface and tested against `Memory()`, so
`go test ./examples/croncmd/` needs no cluster at all — including the claim exclusion, which a
barrier on the watermark read forces into the interleaving that is otherwise a race to reproduce.
What a real node adds is the *scope* of the compare (one process vs. the whole cluster); the
scheduler cannot tell the two apart.

The real, raft-backed store is covered end to end by
`go test ./mobile/kvmobile/ -run TestCronScheduleDispatchesACommandFromADevice`: an in-process
daemon joined to a real leader, a real catalog grant, a real dispatch. It seeds the schedule's
watermark in the past rather than waiting for a minute boundary — which is the same code path a
phone that was asleep takes when it wakes, so it is worth exercising in its own right.

On two real devices, thirteen `android_optical_cases` in `test/e2e/testdata.json` (`cron_next_*`
through `cron_delete`) drive the whole loop through the app's own camera pipeline — every command
this package exposes to the app, read and toggled as well as scheduled. **Verified live on the
rig** (an emulator generating DataMatrix codes, a phone reading them with its real camera): the
nine scheduling cases first at 10/10 in ~81s, then the four catalog ones (`Get` twice, `SetEnabled`,
`Serving`) at 7/7 in ~72s, and the whole 115-case suite they belong to green in one pass.

The split across the two devices is the interesting part, and is forced by what is observable
where:

* **device B** scans `Cron: Put` and then `Cron: Serve`, and both return the instant they run — a
  scheduler that has *started* looks exactly like one that will never fire, so B's own results
  cannot prove anything about the schedule actually working;
* **device A** owns the command, and its dispatcher records a `Dispatching optical-cron/… (inputs:
  …)` line when the fire arrives. So the run carries a `verify_on_device_a` looking for the marker
  inside the schedule's own inputs — a string that could only have got into A's log as the inputs
  of a request the scheduler submitted, which makes finding it proof of the whole chain rather
  than of one link;
* and a `verify_on_device_b` for `Cron fired optical-cron-sweep`, the line B's own scheduler
  callback records when it fires. Both must pass, so the pair spans the whole chain — B scheduled
  it and B saw it fire, A ran it — rather than either end alone.

`cron_seed_watermark` writes the schedule's watermark into the past with an ordinary `KV: Submit`,
for the same reason the Go test does: otherwise the first fire waits for a minute boundary up to a
minute away. `cron_fires_records_the_dispatch` then reads the claim back on B, so both halves —
the fire as the cluster recorded it, and the execution as the owning device saw it — are checked
independently.

### Why four of those cases only assert `no_crash`

Worth stating plainly, because it looks like weak testing and is a deliberate consequence of what
a scheduler *is*. A running scheduler writes its own `Cron fired …` / `Cron skipped …` entries
into the scanning device's Activity Log at times no case controls, so that log stops being a
transcript of the cases and becomes a transcript of two writers interleaved.

The harness used to take that badly: it waited for *any* new entry after tapping Execute and read
the **last** one. Measured on the rig, a `Test: SleepMillis 20000` case therefore "finished" in
0.45s having read a scheduler line — and its own entry, landing 20s later, was read as the result
of whichever case was waiting by then, three cases downstream. `awaitOneCase` now waits for the
entry belonging to *this case's* command instead (`LogEntry.category`/`name`, which
`CommandExecutor` sets on every command and `OutputLog.append` leaves blank on every scheduler
callback), keyed off the monotonic entry id. That removes the misattribution, and makes a long
case honestly block for its own duration.

What it does not remove is the scheduler writing entries at unpredictable times, which still makes
a `result` assertion on a case running alongside it a statement about timing as much as about the
command.

So the four cases that execute *while the scheduler is live* (`cron_serve_starts_the_scheduler`,
`cron_seed_watermark`, `cron_settle_for_the_first_fire`, `cron_stop_serve`) assert only
`no_crash`, and every strict assertion is made where the log is quiet: before the scheduler starts
(`cron_next`/`cron_put`/`cron_get_reads_the_schedule_back`/`cron_list`) or after
`cron_stop_serve` has stopped it (`cron_serving_is_false_after_stop`,
`cron_fires_records_the_dispatch`, `cron_disable_the_schedule`, `cron_get_shows_it_disabled`,
`cron_delete`). That placement is also what lets the two catalog-state checks be exact rather than
approximate — `"command_id":"optical-cron"` read back out of `Get` after `Put`, and
`"disabled":true` read back after `SetEnabled false`, so the toggle is measured by its effect and
not by its own return value. The two `verify_on_device_*` checks are unaffected either way, since
they search the whole log for a substring rather than reading any one entry — which is why the
load-bearing assertions live there.

---

# Improving this

### 1. One minute is the floor

The resolution is a minute because the claim key names a minute. Anything finer needs a different
key layout and a different `Interval`, and would want revisiting whether a claim per fire is still
the right granularity.

### 2. Fires are not globally ordered

`Catalog.Fires` with no schedule id returns each schedule's fires in time order, but does not
interleave schedules — claim keys sort by schedule first, and re-sorting the whole set for a view
nobody reads that way would cost a comparison per pair. If a merged timeline is ever wanted, that is
where to add it.

### 3. Nothing observes the dispatch's outcome

A scheduler records the instance id it produced and stops there. Whether the command *succeeded* is
in the execution log, and a schedule that wants "retry until it works" needs to read that back and
decide — which is deliberately not this package's business, since what counts as failure is the
command's own question.

### 4. A schedule cannot depend on another

There is no "run B after A succeeded". That is a different shape of thing (a dependency graph, not a
calendar) and putting it here would mean this package knowing what a command's result means.

### 5. Skips are reported, not recorded

`OnSkip` tells a live listener that a fire was dropped for being too old, but nothing durable
records it — so "why did nothing run on the 3rd" is answerable from the *absence* of a claim rather
than from a record of the decision. A stale-skip marker under its own infix would fix that at the
cost of a write per skipped fire.
