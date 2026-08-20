// Package luacmd lets a command be written as a Lua script instead of as
// Go: the script is stored in the journal, registered in the Group/Command
// catalog like any other command, and executed by a runner on the device
// that owns it -- reporting progress as it goes, and able to dispatch
// other commands and fold their execution ids and logs back into its own.
//
// A worked example, like examples/croncmd and examples/relations beside
// it. It adds nothing to the core library and needs no daemon, kvfsm or
// wire change: a script is an ordinary pkg/logrecord append, a Lua command
// is an ordinary catalog Command, a run is an ordinary SubmitCommand
// dispatch, and progress is an ordinary AppendCommandLog entry. Everything
// the catalog already enforces still applies unchanged.
//
// # Permissions are the ordinary group model
//
// A Lua command carries no permission concept of its own. Link it to a
// group and that group decides who may run it: a public group admits any
// peer, a private group admits its members. That check happens inside the
// raft FSM against the same Group/Command/PeerGroup records every other
// command is checked against, so client code cannot forge it, and a script
// author gains nothing by writing their command in Lua rather than in Go.
//
// # Why the script lives in the journal, and the hash in the catalog
//
// The source is a pkg/logrecord record (ScriptKind), which gives it an
// author, a timestamp and a full revision history for free -- editing a
// script never destroys what it used to say, which for something that runs
// on other people's devices is worth more than saving a row.
//
// But journal appends are open to any local caller of the node they land
// on, while catalog writes are voter-gated. If the Command named only the
// script id, anyone able to reach that node's IPC could rewrite what a
// group-gated command does without ever touching the catalog. So the
// Command's spec pins the script's SHA-256 as well as its id, and the
// runner refuses to execute bytes that don't match: changing what a
// command runs means a voter rewriting the Command, and every superseded
// revision stays readable.
//
// # Why this package runs its own dispatch loop
//
// pkg/kvctl.RunCommandDispatcher and mobile/kvmobile.RunCommandDispatcher
// both call their handler synchronously, inside the loop that scans for
// pending requests. No handler written before this one ever dispatched
// work back into its own device, so that was free. A Lua script that
// submits another command served by the same device and waits for it is
// exactly that case: the wait would block the loop that has to notice the
// child, and the two would sit there until the deadline.
//
// The runner here claims each request with an immediate "running" progress
// entry -- which the existing dedup check already reads as live-but-
// unhandled, so a request whose process dies mid-run is still retried --
// and then runs the script in its own goroutine, bounded by a concurrency
// cap.
//
// # What a script may do
//
// The VM is closed by default: no io, no os, no package, no debug, no
// require, no loading precompiled bytecode. What a script gets instead is
// a kv table -- write a log line, submit another command, wait for it,
// read its log -- plus a wall-clock deadline, an instruction guard, and a
// cap on how many commands one run may dispatch.
//
// That last set of limits is the point rather than the paperwork. A script
// runs on a device holding a raft voter's identity and its private key,
// and submits under that device's own peer id. The interesting failure is
// not a script reading the filesystem; it is a script submitting itself in
// a loop and turning a phone into a dispatch amplifier.
//
// # What it does not do
//
// It does not decide who may run what -- the catalog does, as above. It
// does not give a script any access this repo doesn't already expose as a
// command: a script that needs storage dispatches a command that owns it,
// rather than writing keys directly. And it grants nothing by virtue of
// being a script: a device that may not submit a command may not submit it
// from Lua either.
package luacmd

import (
	"fmt"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// ScriptKind is the pkg/logrecord Kind every script revision is stored
// under, keyed by script id as the unit id. A plain kind string, not a
// reserved namespace byte: pkg/shmevent.SystemKeyPrefix and
// pkg/logrecord's own prefix are the two a daemon actually refuses, and
// this is a record inside the second, exactly like every other log record.
const ScriptKind = "lua-script"

// SpecRuntime is the value a catalog Command's spec carries in its
// "runtime" field to mark it as one this package executes. The runner
// discovers its own work by scanning for commands whose spec says this and
// whose target peer id is the device it runs on, rather than by being
// handed a list -- so a command created while it is running is picked up
// on the next refresh with no restart.
const SpecRuntime = "lua"

// Check reports whether code compiles as a Lua chunk, without running any
// of it. Callers store a script only if this passes, so that a syntax
// error is caught by whoever wrote it, at the moment they wrote it,
// rather than by whichever device eventually runs the command -- where it
// would surface as a failed dispatch on a machine its author may not be
// holding.
//
// Compiles in the same closed state the runner will later use
// (SkipOpenLibs), which is what makes this a real precheck rather than an
// approximation of one: a chunk that compiles here compiles there. It is
// still only a compile, though -- nothing about a script that calls a
// command it has no standing for, or loops forever, is visible until it
// runs.
func Check(code string) error {
	if strings.TrimSpace(code) == "" {
		return fmt.Errorf("luacmd: script is empty")
	}
	state := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer state.Close()
	if _, err := state.LoadString(code); err != nil {
		return fmt.Errorf("luacmd: compile script: %w", err)
	}
	return nil
}
