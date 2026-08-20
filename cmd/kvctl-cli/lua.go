package main

// The lua commands drive examples/luacmd: a command whose body is a Lua
// script, stored in the journal and registered in the catalog like any
// other command.
//
// They live here for the same reason the cron and journal commands do --
// this is the binary that runs on a deployment target reached over SSH,
// and `luaserve` has to run beside a daemon. It is also the binary to use
// for anything scripted: every `mage` invocation builds before it runs.
//
// Where each belongs:
//
//   - luaserve is the long-running one, and belongs on the device the
//     commands target, only there. Unlike cronserve, running it on more
//     nodes is not redundancy: a command names one target peer, and two
//     runners for it can both decide a request is unhandled;
//   - luaput registers a command, which is voter-gated, and takes the
//     group that decides who may run it;
//   - luarun submits one and follows its log; lualogs/lualastlog read a
//     run back afterwards;
//   - luaget/lualist/luahistory/luadelete edit the scripts themselves and
//     can be run from anywhere with a daemon.
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

// luaTimeout bounds the one-shot commands. luaserve and luarun are not
// bounded by it: one runs until interrupted, the other until the run it is
// following finishes.
const luaTimeout = 30 * time.Second

func luaFail(command string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
	os.Exit(1)
}

// mustLuaCatalog opens the script catalog for the current node.
func mustLuaCatalog(command string) (*luacmd.Catalog, string, context.Context, context.CancelFunc) {
	scripts, peerID, err := luacmd.CurrentCatalog()
	if err != nil {
		luaFail(command, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	return scripts, peerID, ctx, cancel
}

func cmdLuaPut(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli luaput <id> <name> <groupID|""> <file.lua>`)
		os.Exit(2)
	}
	code, err := os.ReadFile(args[3])
	if err != nil {
		luaFail("luaput", err)
	}

	scripts, peerID, ctx, cancel := mustLuaCatalog("luaput")
	defer cancel()

	stored, err := luacmd.Register(ctx, scripts, luacmd.Kvctl(), peerID,
		luacmd.Script{ID: args[0], Name: args[1], Code: string(code)}, args[2])
	if err != nil {
		luaFail("luaput", err)
	}
	fmt.Printf("stored %s (%s), %d bytes, sha256 %s\n", stored.ID, stored.Name, len(stored.Code), stored.SHA256)
	fmt.Printf("registered as a Lua command on %s", peerID)
	if args[2] != "" {
		fmt.Printf(", in group %s", args[2])
	}
	fmt.Println()
}

func cmdLuaGet(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli luaget <id>")
		os.Exit(2)
	}
	scripts, _, ctx, cancel := mustLuaCatalog("luaget")
	defer cancel()

	script, err := scripts.Get(ctx, args[0])
	if err != nil {
		luaFail("luaget", err)
	}
	luaPrintScript("luaget", script, true)
}

func cmdLuaList(args []string) {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli lualist")
		os.Exit(2)
	}
	scripts, _, ctx, cancel := mustLuaCatalog("lualist")
	defer cancel()

	found, err := scripts.List(ctx)
	if err != nil {
		luaFail("lualist", err)
	}
	for _, script := range found {
		luaPrintScript("lualist", script, false)
	}
}

func cmdLuaHistory(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli luahistory <id>")
		os.Exit(2)
	}
	scripts, _, ctx, cancel := mustLuaCatalog("luahistory")
	defer cancel()

	revisions, err := scripts.History(ctx, args[0])
	if err != nil {
		luaFail("luahistory", err)
	}
	if len(revisions) == 0 {
		luaFail("luahistory", fmt.Errorf("nothing was ever written under %s", args[0]))
	}
	for _, revision := range revisions {
		luaPrintScript("luahistory", revision, false)
	}
}

func cmdLuaDelete(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli luadelete <id>")
		os.Exit(2)
	}
	scripts, _, ctx, cancel := mustLuaCatalog("luadelete")
	defer cancel()

	if err := scripts.Delete(ctx, args[0]); err != nil {
		luaFail("luadelete", err)
	}
	fmt.Printf("deleted %s (its revisions remain readable with `kvctl-cli luahistory %s`)\n", args[0], args[0])
}

func cmdLuaRun(args []string) {
	if len(args) < 1 || len(args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli luarun <commandID> [inputsJSON]")
		os.Exit(2)
	}
	inputs := ""
	if len(args) == 2 {
		inputs = args[1]
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cluster := luacmd.Kvctl()
	instanceID, err := cluster.Submit(ctx, args[0], inputs)
	if err != nil {
		luaFail("luarun", err)
	}
	fmt.Printf("submitted %s as %s\n", args[0], instanceID)

	last, err := luacmd.Follow(ctx, cluster, instanceID, 0, func(entry luacmd.LogEntry) {
		fmt.Println(luacmd.FormatEntry(entry))
	})
	if err != nil {
		luaFail("luarun", err)
	}
	if last.Status() == "error" {
		luaFail("luarun", fmt.Errorf("%s failed: %s", args[0], last.Narrative))
	}
}

func cmdLuaLogs(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli lualogs <instanceID>")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()

	entries, err := luacmd.Kvctl().QueryLog(ctx, args[0])
	if err != nil {
		luaFail("lualogs", err)
	}
	if len(entries) == 0 {
		luaFail("lualogs", fmt.Errorf("no entries for %s", args[0]))
	}
	for _, entry := range entries {
		luaPrintEntry(entry)
	}
}

func cmdLuaLastLog(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli lualastlog <commandID>")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), luaTimeout)
	defer cancel()

	instanceID, entries, err := luacmd.LastRun(ctx, luacmd.Kvctl(), args[0])
	if err != nil {
		luaFail("lualastlog", err)
	}
	fmt.Printf("%s, most recently run as %s\n", args[0], instanceID)
	for _, entry := range entries {
		luaPrintEntry(entry)
	}
}

// luaPrintEntry writes one log entry: its one-line summary, then its
// structured result indented underneath if it has one. Kept out of
// FormatEntry itself so a caller rendering a list still gets a single
// scannable line -- see FormatResult.
func luaPrintEntry(entry luacmd.LogEntry) {
	fmt.Println(luacmd.FormatEntry(entry))
	if block, ok := luacmd.FormatResult(entry); ok {
		fmt.Println(block)
	}
}

func cmdLuaServe(args []string) {
	if len(args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli luaserve [interval] [concurrency]")
		os.Exit(2)
	}
	opts := luacmd.ServeOptions{Listener: luaStdoutListener{}}
	if len(args) > 0 && args[0] != "" {
		interval, err := time.ParseDuration(args[0])
		if err != nil {
			luaFail("luaserve", fmt.Errorf("interval %q: %w", args[0], err))
		}
		opts.Interval = interval
	}
	if len(args) > 1 && args[1] != "" {
		concurrency, err := strconv.Atoi(args[1])
		if err != nil {
			luaFail("luaserve", fmt.Errorf("concurrency %q: %w", args[1], err))
		}
		opts.Concurrency = concurrency
	}

	scripts, peerID, err := luacmd.CurrentCatalog()
	if err != nil {
		luaFail("luaserve", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("serving Lua commands targeting %s -- Ctrl-C to stop\n", peerID)
	luacmd.NewRunner(luacmd.Kvctl(), scripts, opts).Serve(ctx)
	fmt.Println("stopped")
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

func luaPrintScript(command string, script luacmd.Script, withCode bool) {
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
		luaFail(command, err)
	}
	fmt.Println(string(encoded))
	if withCode && script.Code != "" {
		fmt.Println(script.Code)
	}
}
