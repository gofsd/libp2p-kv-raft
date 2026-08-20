//go:build mage

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
)

// These wrap examples/luacmd -- a worked example rather than part of this
// repo's library (see its package doc), exposed here so a command can be
// written in Lua from a shell without writing a Go program first.
//
// A Lua command is an ordinary catalog Command whose spec names a script
// in the journal and pins that script's hash. So:
//
//   - luaput writes the script and registers the command. Registering is
//     voter-gated, like every other catalog write, and the group given is
//     what decides who may run it -- public group, anybody; private group,
//     its members;
//   - luaserve is the process that actually runs them, and belongs on the
//     device the commands target. Unlike cronserve, running it on several
//     nodes is not redundancy: a command has one target peer, and two
//     runners for it would race;
//   - luarun submits one and follows its log to the end, which is what
//     makes the live lines a script writes visible from a terminal.
//
// The same operational note as the cron and journal targets applies: every
// `mage` invocation builds before it runs, so for anything scripted use
// the identical commands on the kvctl-cli binary instead.
//
// # One process at a time, per node
//
// pkg/ipc's request channel is single-in-flight *across processes*: its
// caller lock is Go-level only, and its own doc says two separate OS
// processes calling one daemon at once "was never safe and still isn't".
// So `luaserve` and any other lua/mage/kvctl-cli command aimed at the
// *same* node must not overlap. Running them together does not fail
// cleanly -- confirmed live: the runner, the running script and the
// following client all start reporting "waiting for response channel ...
// context deadline exceeded", and a run dies mid-way with that as its
// recorded result.
//
// It matters less than it sounds, because it is not the normal shape. A
// Lua command names one target device, and the runner belongs on that
// device; submitters are usually somewhere else, and reach it over libp2p
// rather than through its IPC. The case to avoid is one machine both
// serving and submitting from two separate processes. To drive a run
// against a node that is serving locally, either submit first and start
// the runner after, or watch the runner's own stdout, which prints every
// start and finish anyway.

// luaTimeout bounds the one-shot targets' own reads and writes. LuaServe
// and LuaRun are not bounded by it -- one runs until interrupted, the
// other until the run it is following finishes.
const luaTimeout = 30 * time.Second

// LuaPut implements `mage luaput <id> <name> <groupID|""> <file.lua>`:
// stores the script in the journal and registers it as a Lua command
// targeting the current node, linked to groupID if one is given.
//
// The script is read from a file rather than typed as an argument because
// a shell argument is the wrong shape for source code, and because the
// file is what an author edits between revisions -- every luaput of the
// same id adds a revision, keeping the old ones readable (`mage
// luahistory`).
//
// Only a raft voter may register a command. On a node that is not one, the
// script is stored and the daemon refuses the registration, which is
// recoverable: run the same luaput from a voter later.
//
// Usage: mage luaput <id> <name> <groupID|""> <file.lua>
func LuaPut(id, name, groupID, file string) error {
	code, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("luaput: read %s: %w", file, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()
	scripts, peerID, err := luacmd.CurrentCatalog()
	if err != nil {
		return err
	}

	stored, err := luacmd.Register(ctx, scripts, luacmd.Kvctl(), peerID,
		luacmd.Script{ID: id, Name: name, Code: string(code)}, groupID)
	if err != nil {
		return err
	}
	fmt.Printf("stored %s (%s), %d bytes, sha256 %s\n", stored.ID, stored.Name, len(stored.Code), stored.SHA256)
	fmt.Printf("registered as a Lua command on %s", peerID)
	if groupID != "" {
		fmt.Printf(", in group %s", groupID)
	}
	fmt.Println()
	return nil
}

// LuaGet implements `mage luaget <id>`: prints the current revision of a
// script -- its metadata as JSON, then its source.
// Usage: mage luaget <id>
func LuaGet(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()
	scripts, _, err := luacmd.CurrentCatalog()
	if err != nil {
		return err
	}
	script, err := scripts.Get(ctx, id)
	if err != nil {
		return err
	}
	return printScript(script, true)
}

// LuaList implements `mage lualist`: every script that currently exists,
// latest revision each, deleted ones left out.
// Usage: mage lualist
func LuaList() error {
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()
	scripts, _, err := luacmd.CurrentCatalog()
	if err != nil {
		return err
	}
	found, err := scripts.List(ctx)
	if err != nil {
		return err
	}
	if len(found) == 0 {
		fmt.Println("no scripts")
		return nil
	}
	for _, script := range found {
		if err := printScript(script, false); err != nil {
			return err
		}
	}
	return nil
}

// LuaHistory implements `mage luahistory <id>`: every revision ever
// written for a script, oldest first, tombstones included -- what the
// journal is for.
// Usage: mage luahistory <id>
func LuaHistory(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()
	scripts, _, err := luacmd.CurrentCatalog()
	if err != nil {
		return err
	}
	revisions, err := scripts.History(ctx, id)
	if err != nil {
		return err
	}
	if len(revisions) == 0 {
		return fmt.Errorf("luahistory: nothing was ever written under %s", id)
	}
	for _, revision := range revisions {
		if err := printScript(revision, false); err != nil {
			return err
		}
	}
	return nil
}

// LuaDelete implements `mage luadelete <id>`: writes a tombstone revision,
// after which the script no longer exists to a reader. Its source stays
// readable through luahistory, and the catalog Command pointing at it is
// left alone -- delete that separately with `mage deletecommand` if it
// should go too.
// Usage: mage luadelete <id>
func LuaDelete(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()
	scripts, _, err := luacmd.CurrentCatalog()
	if err != nil {
		return err
	}
	if err := scripts.Delete(ctx, id); err != nil {
		return err
	}
	fmt.Printf("deleted %s (its revisions remain readable with `mage luahistory %s`)\n", id, id)
	return nil
}

// LuaRun implements `mage luarun <commandID> [inputsJSON]`: submits the
// command and prints its log lines as they arrive, until it finishes.
//
// Submits under this node's own identity, so this node needs standing for
// the command exactly as any other submitter would -- being the author of
// the script grants nothing.
//
// Exits non-zero if the run itself reported failure, so a script driving
// this can branch on it.
//
// Do not run this against a node that is running luaserve locally in
// another process -- see the note at the top of this file.
// Usage: mage luarun <commandID> [inputsJSON]
func LuaRun(commandID, inputsJSON string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cluster := luacmd.Kvctl()
	instanceID, err := cluster.Submit(ctx, commandID, inputsJSON)
	if err != nil {
		return err
	}
	fmt.Printf("submitted %s as %s\n", commandID, instanceID)

	last, err := luacmd.Follow(ctx, cluster, instanceID, 0, func(entry luacmd.LogEntry) {
		fmt.Println(luacmd.FormatEntry(entry))
	})
	if err != nil {
		return err
	}
	if last.Status() == "error" {
		return fmt.Errorf("luarun: %s failed: %s", commandID, last.Narrative)
	}
	return nil
}

// LuaLogs implements `mage lualogs <instanceID>`: the durable read-back of
// one run, whether or not anything was watching while it happened.
// Usage: mage lualogs <instanceID>
func LuaLogs(instanceID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()
	entries, err := luacmd.Kvctl().QueryLog(ctx, instanceID)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return fmt.Errorf("lualogs: no entries for %s", instanceID)
	}
	for _, entry := range entries {
		printEntry(entry)
	}
	return nil
}

// LuaLastLog implements `mage lualastlog <commandID>`: the same read-back
// for whichever run of a command was submitted most recently -- for when
// the instance id went to whoever submitted it and not to you.
// Usage: mage lualastlog <commandID>
func LuaLastLog(commandID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()
	instanceID, entries, err := luacmd.LastRun(ctx, luacmd.Kvctl(), commandID)
	if err != nil {
		return err
	}
	fmt.Printf("%s, most recently run as %s\n", commandID, instanceID)
	for _, entry := range entries {
		printEntry(entry)
	}
	return nil
}

// printEntry writes one log entry: its one-line summary, then its
// structured result indented underneath if it has one. Kept out of
// FormatEntry itself so that a caller rendering a list (a log row, a UI)
// still gets a single scannable line -- see FormatResult.
func printEntry(entry luacmd.LogEntry) {
	fmt.Println(luacmd.FormatEntry(entry))
	if block, ok := luacmd.FormatResult(entry); ok {
		fmt.Println(block)
	}
}

// LuaServe implements `mage luaserve [interval|""] [concurrency|""]`: runs
// this node's Lua commands in the foreground, until Ctrl-C.
//
// Belongs on the device the commands target, and only there. This is not
// cronserve: a Lua command names one peer, and two runners serving the
// same one can both decide a request is unhandled and run it twice.
//
// interval is how often to look for new requests (default 1.5s), and
// concurrency how many scripts may run at once (default 4).
// Usage: mage luaserve [interval|""] [concurrency|""]
func LuaServe(interval, concurrency string) error {
	opts := luacmd.ServeOptions{Listener: luaStdoutListener{}}
	if interval != "" {
		parsed, err := time.ParseDuration(interval)
		if err != nil {
			return fmt.Errorf("luaserve: interval %q: %w", interval, err)
		}
		opts.Interval = parsed
	}
	if concurrency != "" {
		parsed, err := strconv.Atoi(concurrency)
		if err != nil {
			return fmt.Errorf("luaserve: concurrency %q: %w", concurrency, err)
		}
		opts.Concurrency = parsed
	}

	scripts, peerID, err := luacmd.CurrentCatalog()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("serving Lua commands targeting %s -- Ctrl-C to stop\n", peerID)
	luacmd.NewRunner(luacmd.Kvctl(), scripts, opts).Serve(ctx)
	fmt.Println("stopped")
	return nil
}

// luaStdoutListener prints what the runner is doing, which is the whole
// point of running it in the foreground.
type luaStdoutListener struct{}

func (luaStdoutListener) OnStart(commandID, instanceID string) {
	fmt.Printf("%s  start   %s %s\n", time.Now().Format("15:04:05"), commandID, instanceID)
}

func (luaStdoutListener) OnLog(commandID, instanceID, narrative string) {
	fmt.Printf("%s  log     %s %s: %s\n", time.Now().Format("15:04:05"), commandID, instanceID, narrative)
}

func (luaStdoutListener) OnFinish(commandID, instanceID, status, narrative string) {
	fmt.Printf("%s  %-7s %s %s: %s\n", time.Now().Format("15:04:05"), status, commandID, instanceID, narrative)
}

func (luaStdoutListener) OnError(message string) {
	fmt.Fprintf(os.Stderr, "lua: %s\n", message)
}

// printScript renders one revision: a JSON header line, and optionally the
// source under it. The header is JSON so that scripted callers can read it
// without parsing prose, matching what the other catalog targets print.
func printScript(script luacmd.Script, withCode bool) error {
	header := struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		SHA256  string `json:"sha256"`
		Author  string `json:"author,omitempty"`
		Rev     string `json:"rev"`
		Bytes   int    `json:"bytes"`
		Deleted bool   `json:"deleted,omitempty"`
	}{
		ID: script.ID, Name: script.Name, SHA256: script.SHA256, Author: script.Author,
		Rev: script.Rev.Format(time.RFC3339Nano), Bytes: len(script.Code), Deleted: script.Deleted,
	}
	encoded, err := json.Marshal(header)
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	if withCode && script.Code != "" {
		fmt.Println(script.Code)
	}
	return nil
}
