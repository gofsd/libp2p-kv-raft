# examples/luacmd — a command you can write without rebuilding anything

A worked example, like [`examples/croncmd`](../croncmd) and [`examples/relations`](../relations)
beside it: it adds nothing to the core library and needs no daemon, `kvfsm`, or wire change. A
script is an ordinary [`pkg/logrecord`](../../pkg/logrecord) append, a Lua command is an ordinary
[catalog Command](../../README.md#groupcommand-acl), a run is an ordinary `SubmitCommand`
dispatch, and every line a script writes is an ordinary `AppendCommandLog` entry. Everything the
catalog already enforces still applies unchanged.

It runs on desktop (`mage lua*`, `kvctl-cli lua*`) and on Android (`kvmobile`'s `Lua*` bindings),
against the same replicated records.

---

## What problem it actually solves

Every other command in this repo is Go: to add one you edit `pkg/kvctl`, or write a dispatcher
process, and then you rebuild and redeploy the device that runs it. That is fine for the dozen
commands the repo ships and wrong for the hundred-and-first, especially when the device is a phone
in somebody's pocket and the command is "when this arrives, do that, then tell me what happened".

A Lua command is a command whose body is data. You write it, it replicates, and the device that
owns it runs it — no build, no install, no redeploy.

The interesting part is not embedding an interpreter, which is thirty lines. It is that a script
is *untrusted code arriving over the network at a device holding a raft voter's private key*, and
that the scripts worth writing are the ones that dispatch other commands and wait for them. Those
two facts decide almost everything below.

---

## What is stored

Three records, none of them new kinds:

```
journal   lua-script / <id> / <ts>        the source, one record per revision
catalog   Command <id>                    {"runtime":"lua","script_id":…,"sha256":…}
catalog   GroupCommand <id> ↔ <group>      who may run it
```

The source lives in the journal rather than under a key because a script is edited, and an
append-only log answers "what did this used to say, and who changed it" for free. `mage luahistory`
reads it back; a delete is a tombstone revision, not an erasure.

### Why the Command pins a hash

Journal appends are open to any local caller of the node they land on. Catalog writes are
voter-gated. If the Command named only `script_id`, then anyone able to reach a node's IPC socket
could rewrite what an ACL'd command does without touching the catalog at all — the permission
would be real and the code behind it would not.

So the Command pins `sha256` as well, and the runner refuses to execute bytes that do not match:

```
$ mage lualogs <instance>
[error] luacmd: script outer is 4f2c… but its command pins 875c… --
        refusing to run code the catalog did not register
```

Changing what a command runs therefore means a voter re-registering it. `mage luaput` does both
halves; `LuaPut` (mobile) does only the journal half, which is what a learner device can manage on
its own.

---

## Permissions: exactly the ordinary group model

A Lua command carries no permission concept of its own. Link it to a group, and that group decides
who may run it: a public group admits any peer, a private group admits its members. That check
happens inside the raft FSM against the same `Group`/`Command`/`GroupCommand`/`PeerGroup` records
every other command is checked against.

Two consequences worth stating plainly:

* **Writing a command in Lua grants nothing.** The author of a script has whatever standing they
  had before.
* **A script submits under the *running device's* peer id**, not its submitter's. So a script can
  reach any command that device may run — including ones the person who submitted it may not. That
  is the same property `examples/croncmd` has for schedules, and the same reason both packages say
  so out loud: a script is a program you gave a device, and it acts as that device.

---

## The sandbox, and what it does not cover

The VM opens four libraries — base, table, string, math — and nothing else. `io`, `os`, `package`,
`debug`, `coroutine` are never opened, so those names do not exist.

`require` and `module` are the surprising ones. They *look* like they belong to the package
library, which is never opened, but gopher-lua installs them from its **base** library, so a
sandbox that skips `package` and stops there still hands a script both of them, and with them the
filesystem. They are removed by name. A test asserts each blocked name is nil, which is how that
was found.

`pcall` and `xpcall` are deliberately **kept**, which is the opposite of the obvious call. The
reasoning that says to remove them is: a deadline arrives as a catchable Lua error, so a script
could wrap a loop in `pcall` and outlive it. That turns out to be false — gopher-lua *returns out
of its interpreter loop* when the context is spent rather than only raising, so there is no next
instruction to catch anything with, at any nesting depth. Four escape shapes were tried (an
unprotected outer loop, recursive re-entry, a fully `pcall`-wrapped nest, `repeat…until false`) and
all four stopped on a 200 ms deadline. `TestPcallCannotOutliveTheDeadline` keeps one of them
running against the real sandbox.

What is enforced per run: a wall-clock deadline (checked between instructions), a cap on how many
commands one run may dispatch, a cap on log lines, a cap on result size, and a chain-depth limit
carried in the inputs. What is **not**:

* **Memory.** gopher-lua has no allocation cap. `string.rep("x", 1e9)` is bounded by the device's
  RAM and nothing else, and the deadline does not help because allocating is fast.
* **Instruction count.** There is no public hook to count them, so "how long may this run" is
  answered in wall-clock time only. For work that spends most of its time waiting on other
  commands that is the more useful bound, but it is not the same promise.

Both are listed here rather than glossed because they decide whether you should let arbitrary
people register scripts on a device that matters.

---

## Why this package has its own dispatch loop

`pkg/kvctl.RunCommandDispatcher` and `mobile/kvmobile.RunCommandDispatcher` both call their handler
**synchronously, inside the loop that scans for pending requests**. That is free for every handler
written before this one, because none of them dispatched work back into their own device.

A Lua script doing `kv.run("inner", …)` against a command the same device serves is exactly that
case:

```
scan loop ──▶ handler(outer) ──▶ kv.run("inner") ──▶ waits for inner's log
    ▲                                                        │
    └──────────── cannot scan for "inner" until this returns ─┘
```

The wait blocks the loop that has to notice the child, and the two sit there until the deadline.
Not a theory: changing this package's runner to dispatch synchronously makes
`TestAScriptCanDispatchIntoItsOwnRunner` fail with `outer` recorded as
`script stopped after 5s: context deadline exceeded`.

So the runner here claims each request with an immediate `running` progress entry and then runs the
script in its own goroutine, behind a concurrency cap, leaving the scan loop free.

The claim has a second consequence worth knowing. "Latest entry is still `running`" is how every
dispatcher in this repo recognises a run whose process died, so that it gets retried — but this
runner's own claim looks identical to a dead process's. Without also remembering what it is
currently running, its very next pass would start the same script again. Both halves are tested.

---

## What a script sees

One global table, `kv`. Everything is synchronous; the runner owns the deadline.

| Call | Returns | What it does |
| --- | --- | --- |
| `kv.inputs` | table | The submitter's inputs JSON, decoded. |
| `kv.instance_id`, `kv.command_id`, `kv.requested_by`, `kv.script_id`, `kv.depth` | | This dispatch's own identity. |
| `kv.log(text, fields?)` | — | A live line in this command's log. `print` is an alias. |
| `kv.submit(id, inputs?)` | instance id | Dispatches another command, and records the child in this run's own log. |
| `kv.wait(instance_id, secs?)` | record | Polls until the child records a terminal entry: `{done, status, narrative, fields, result}`. |
| `kv.run(id, inputs?, secs?)` | instance id, record | Submit and wait, the common case. |
| `kv.logs(instance_id, n?)` | array | Every line the child wrote. |
| `kv.sleep(secs)` | — | Deadline-aware sleep. |
| `kv.json_decode(s)` | value, or nil + why | For a command that answers with JSON somewhere other than its `result` field. |
| `kv.json_encode(v)` | string | The inverse. Raises on a value that cannot be encoded — that is the script's own mistake. |

A returned table becomes the final log entry (`{result = ..., fields = {...}, narrative = "..."}`,
any combination); a returned string becomes its narrative; an error becomes a failed terminal entry
with the Lua message as the narrative and the traceback in a field.

`result` is the structured half. It is JSON-encoded into one reserved field — the same field
`examples/relations/journalcmd` already writes its own answer into — and a record's `result` is that
field decoded, or `nil` when the command wrote something that is not JSON. So a script can index
what an ordinary Go command returned, rather than parsing its sentence:

```lua
local id, res = kv.run("shift-log", {op = "form"}, 30)
if res.result then
  for _, column in ipairs(res.result.form.columns) do kv.log(column.name) end
end
```

Fields stay flat strings deliberately — they are what a log list renders — and `result` is where
structure goes. Three edges matter, because a script indexing a result depends on them: `null`
becomes `nil` and the key disappears, an empty table encodes as `{}` and never `[]`, and numbers are
float64, so a 64-bit id has to travel as a string or lose its low bits.

**A failed child is a value, not an error.** `kv.wait` hands back the child's terminal record
whatever its status, and the script decides. If a failing child unwound the parent instead, "the
child failed and I handled it" and "my own script is broken" would be indistinguishable in the log
— and one script could not produce both a clean success and a clean failure depending only on its
inputs, which is exactly what the tests and the optical rig need it to do.

```lua
-- outer
kv.log("hello from outer begin")

local id, res = kv.run("inner", {who = kv.inputs.who, mode = kv.inputs.mode}, 60)
for _, r in ipairs(kv.logs(id)) do
  kv.log("inner[" .. id .. "] " .. (r.narrative or ""), {child_instance = id})
end

if res.status ~= "ok" then
  return {fields = {status = "error", child_instance = id},
          narrative = "hello from outer failed: " .. (res.narrative or "")}
end
return {fields = {status = "ok", child_instance = id}, narrative = "hello from outer end"}
```

### How an ordinary Go command opts in

Nothing about the producing side is Lua-specific, and no interface changes: a handler already
returns `(fields, narrative)`, so it opts in by writing one field.

```go
encoded, _ := json.Marshal(result)
fields["result"] = string(encoded)
```

That is exactly what [`examples/relations/journalcmd`](../relations/journalcmd) has always done —
which is why the convention is that field rather than a new one, and why a script can read
journalcmd's answers without journalcmd knowing this package exists.

### Chain depth

`kv.submit` stamps a reserved `_lua_depth` key into the child's inputs and refuses past
`MaxDepth` (3 by default); a runner also refuses to start a dispatch already deeper than that. The
key is stripped before a script sees its own inputs, so it can be neither read as data nor forged.
Without it, a script that submits itself is an unbounded dispatch amplifier — which, on a phone
acting under a voter's identity, is the failure worth designing against.

---

## Running it: desktop

```bash
# The group is what decides who may run the command.
mage creategroup lua-ops "Lua operators" false
mage addpeertogroup <peerID> lua-ops

# Store the script and register it as a command on this node, in that group.
mage luaput inner "Inner" lua-ops ./inner.lua
mage luaput outer "Outer" lua-ops ./outer.lua

# The runner belongs on the device the commands target. Foreground; Ctrl-C stops it.
mage luaserve

# From another device (see the warning below), submit and follow:
mage luarun outer '{"who":"ops","mode":"ok"}'
mage lualastlog outer     # the durable read-back, no instance id needed
mage luahistory outer     # every revision this script has ever had
```

> **One process at a time, per node.** `pkg/ipc`'s request channel is single-in-flight *across
> processes* — its caller lock is Go-level only, and that package's own doc says two OS processes
> calling one daemon at once "was never safe and still isn't". Running `luaserve` and `luarun`
> against the same node starves both: everything reports `waiting for response channel … context
> deadline exceeded`, and a run can record that as its result. It is not the normal shape (a
> command names one target device; submitters are elsewhere and arrive over libp2p), but on one
> machine, submit first and start the runner after.

`kvctl-cli` has the identical commands, and is what a deployment target reached over SSH has. Use
it rather than `mage` for anything scripted: every `mage` invocation builds before it runs.

---

## Running it: Android

`mobile/kvmobile` exposes the same operations as gomobile bindings, and `android-app` drives them:

* **Writing one** — the Lua group's "New Lua command…" opens an editor: id, name, the group, and
  the script in a tall monospace field. This is the one screen in the app with a button that
  executes something directly. Everything else runs by generating a DataMatrix, scanning it on
  another device, and confirming — and a script does not fit in a code a camera can read across a
  desk, so authoring is exempt and execution is not. Create still routes through the same
  `CommandExecutor` as a scanned run, so it lands in the same Activity Log.
* **Running one** — `Lua: Run` like any other command: generate, scan, confirm. It starts a watch
  on the instance it submitted, so the script's lines appear in the log list as they happen, with
  any child dispatch named inline (`[child inner/abc123]`).
* **Hosting** — `Lua: Serve` is explicit and does not start with the app. Running an interpreter
  for whatever anyone submits should be a decision somebody made, not a side effect of opening an
  app. Until something starts it, commands targeting that device stay pending.

A phone is a *better* host than the desktop in one specific way: kvmobile's daemon is in-process,
so `pkg/ipc`'s caller lock covers the runner, a script's waiting, and the UI together — the
starvation described above cannot arise.

---

## Testing it

```bash
go test ./examples/luacmd/                     # everything, including the live ones
go test ./examples/luacmd/ -run TestLive       # a real node, the whole chain both ways
go test ./mobile/kvmobile/ -run TestLua        # the same chain through the mobile bindings
```

The unit tests run against a memory journal and a fake cluster, so most of this package is
exercised without a daemon: the sandbox's blocked names, the deadline, the caps, the conversion
rules, the dedup, the concurrency cap, the hash refusal.

Three tests are worth knowing about specifically, because they encode findings rather than
behaviour:

* `TestPcallCannotOutliveTheDeadline` — the escape that would break every other limit if it worked.
* `TestAScriptCanDispatchIntoItsOwnRunner` — the deadlock this package's own runner exists to
  avoid; confirmed to fail if the runner is made synchronous.
* `TestLiveRunnerRefusesASubstitutedScript` — the hash pin, against a real cluster: journal access
  is not enough to change what an approved command does.

---

# Improving this

### 1. A script can exhaust memory

The deadline bounds time and nothing bounds allocation. A script that builds a huge string takes
the device down with it, and on a phone that is the app. Fixing it properly means an allocator
hook gopher-lua does not expose; fixing it partially means capping the obvious offenders
(`string.rep`, table growth) inside the sandbox, which is the kind of guard that looks complete and
is not.

### 2. Standing is the device's, not the submitter's

A script reaches whatever the running device may reach, so a low-privilege submitter can cause a
high-privilege dispatch by running a script that makes one. The catalog can express "who may run
this script" but not "and only with these onward calls". An allowlist of dispatchable command ids
in the spec would close it, at the cost of a second place to keep permissions in step.

### 3. Nothing schedules or retries a failed run

A run that fails is recorded and left. There is no retry policy, no backoff, and no way to say
"try three times then give up" without writing it into the script. `examples/croncmd` can put a
Lua command on a timer, which covers the periodic case and nothing else.

### 4. One runner per command, enforced by convention only

A command names one target peer, and the runner filters on it — but if two devices somehow share
an identity, or someone points two runners at one peer id, both will decide a request is unhandled
and run it. The claim entry is a progress note, not a lock. `examples/croncmd`'s compare-and-swap
claim is the shape that would fix it.

### 5. The log is the only output

A script's result is `(fields, narrative)`, and everything else it wants to leave behind has to go
through another command. There is deliberately no `kv.set` — commands are the front door, and a
script that could write keys directly would walk around every ACL the catalog enforces. It does
mean a script that wants durable state of its own needs a command that owns it.

### 6. Editing a script needs a voter

The hash pin means every edit is a catalog write, which on a learner device (a phone, usually) is
a refusal. The script still stores, so nothing is lost — but "fix the typo and run it again" is a
two-device operation. A spec flag that pins only the script id would make it a one-device one, at
exactly the cost the pin exists to prevent.
