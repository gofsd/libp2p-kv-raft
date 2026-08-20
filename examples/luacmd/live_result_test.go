package luacmd_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/examples/relations/journalcmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// The claim worth testing about structured results is not that a Lua
// script can read a Lua script's output -- that is circular, since this
// package writes both ends. It is that structure crosses from an ordinary
// Go command.
//
// So this drives examples/relations/journalcmd, which was written long
// before this package existed, knows nothing about it, and is not modified
// here: it answers `{"op":"form"}` by writing its own Result struct as
// JSON into the log entry's "result" field, because that is what it has
// always done. A Lua script submits that command and indexes the answer.
//
// If this passes, adopting journalcmd's field name rather than inventing
// one (see FieldResult) is doing exactly what it was chosen to do.

const (
	liveJournalCommandID = "shift-log"
	liveReaderCommandID  = "form-reader"
	liveJournalLog       = 7
)

// liveReaderScript submits the journal command and indexes its structured
// answer -- the whole point of the test, written the way a person would.
const liveReaderScript = `
local id, res = kv.run("shift-log", {op = "form"}, 60)

if not res.result then
  return {fields = {status = "error"}, narrative = "no structured answer: " .. (res.narrative or "")}
end

local names, kinds = {}, {}
for _, column in ipairs(res.result.form.columns) do
  names[#names + 1] = column.name
  kinds[#kinds + 1] = column.input
end

return {
  result = {op = res.result.op, names = names, kinds = kinds, page = res.result.form.page},
  fields = {status = "ok", columns = #names, first = names[1], first_kind = kinds[1]},
  narrative = "read the form",
}
`

func TestLiveStructuredResultCrossesFromAGoCommand(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	t.Setenv(registry.EnvHome, t.TempDir())

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	peerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// One group, this node in it, both commands linked to it. Nothing
	// about the permission is Lua-specific or journal-specific.
	if err := kvctl.PutGroup(liveGroupID, "Lua live", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.AddPeerToGroup(peerID, liveGroupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	if err := kvctl.PutCommand(liveJournalCommandID, "Shift log", peerID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(liveJournalCommandID, liveGroupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}

	// The journal itself, and the service answering for it -- set up the
	// way journalcmd's own live test does, since that is the shape a real
	// deployment has.
	backend, err := relations.CurrentNode()
	if err != nil {
		t.Fatalf("relations.CurrentNode: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	actor := relations.Entity{Log: liveJournalLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 1}
	store := relations.New(backend, liveJournalLog, actor, priv)
	if err := store.DeclareActor(ctx, actor, "the log node", pub); err != nil {
		t.Fatalf("DeclareActor: %v", err)
	}
	journal := relations.NewJournal(store)
	for _, column := range []struct {
		name  string
		input relations.InputKind
	}{
		{"operator", relations.InputTerm},
		{"machine", relations.InputTerm},
		{"pieces", relations.InputNumber},
	} {
		if _, err := journal.DefineField(ctx, column.name, column.input); err != nil {
			t.Fatalf("DefineField(%s): %v", column.name, err)
		}
	}

	journalStop := make(chan struct{})
	defer close(journalStop)
	go journalcmd.New(journal).Run(liveJournalCommandID, journalStop, func(err error) {
		t.Logf("journalcmd: %v", err)
	})

	// The Lua command that reads it.
	scripts, _, err := luacmd.CurrentCatalog()
	if err != nil {
		t.Fatalf("CurrentCatalog: %v", err)
	}
	device := luacmd.Kvctl()
	if _, err := luacmd.Register(ctx, scripts, device, peerID,
		luacmd.Script{ID: liveReaderCommandID, Name: "Form reader", Code: liveReaderScript},
		liveGroupID); err != nil {
		t.Fatalf("Register: %v", err)
	}
	pollUntilTrue(t, 15*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand(liveReaderCommandID)
		return err == nil, nil
	})

	runnerCtx, stop := context.WithCancel(ctx)
	defer stop()
	runner := luacmd.NewRunner(device, scripts, luacmd.ServeOptions{
		Interval: 300 * time.Millisecond,
		Listener: testListener{t},
	})
	done := make(chan struct{})
	go func() { defer close(done); runner.Serve(runnerCtx) }()
	t.Cleanup(func() {
		stop()
		<-done
	})

	instanceID, err := device.Submit(ctx, liveReaderCommandID, "")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	followCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	last, err := luacmd.Follow(followCtx, device, instanceID, 300*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}

	if last.Fields["status"] != "ok" {
		t.Fatalf("the script could not read the form: %+v", last)
	}
	// The three columns declared above, in declaration order, reached the
	// script as an indexable list rather than a sentence it had to parse.
	if last.Fields["columns"] != "3" {
		t.Errorf("columns = %q, want 3", last.Fields["columns"])
	}
	if last.Fields["first"] != "operator" {
		t.Errorf("first column = %q, want operator", last.Fields["first"])
	}
	// The column's declared kind came through too, which is the part a
	// form-drawing client actually branches on.
	if last.Fields["first_kind"] != "term" {
		t.Errorf("first column kind = %q, want term", last.Fields["first_kind"])
	}

	// And the script's own structured answer round-tripped back out.
	block, ok := luacmd.FormatResult(last)
	if !ok {
		t.Fatalf("the run recorded no structured result: %+v", last)
	}
	for _, want := range []string{`"op": "form"`, `"operator"`, `"machine"`, `"pieces"`} {
		if !strings.Contains(block, want) {
			t.Errorf("the recorded result is missing %s:\n%s", want, block)
		}
	}
}

// The other half of D9: a command that answers in prose must leave a
// script able to notice and carry on, not fail it.
func TestLiveAProseAnsweringCommandLeavesResultNil(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	t.Setenv(registry.EnvHome, t.TempDir())

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	peerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := kvctl.PutGroup(liveGroupID, "Lua live", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.AddPeerToGroup(peerID, liveGroupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}

	// A plain Go handler that answers the way most commands do: a
	// sentence, no structured field at all.
	const proseCommandID = "prose"
	if err := kvctl.PutCommand(proseCommandID, "Prose", peerID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(proseCommandID, liveGroupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	proseStop := make(chan struct{})
	defer close(proseStop)
	go kvctl.RunCommandDispatcher(proseCommandID, func(kvctl.CommandRequest) (map[string]string, string) {
		return map[string]string{"status": "ok"}, "all quiet on the shop floor"
	}, proseStop, func(err error) { t.Logf("prose dispatcher: %v", err) })

	scripts, _, err := luacmd.CurrentCatalog()
	if err != nil {
		t.Fatalf("CurrentCatalog: %v", err)
	}
	device := luacmd.Kvctl()
	if _, err := luacmd.Register(ctx, scripts, device, peerID, luacmd.Script{
		ID: "prose-reader", Name: "Prose reader", Code: `
local id, res = kv.run("prose", {}, 60)
if res.result then
  return {fields = {status = "error"}, narrative = "expected no structured answer"}
end
return {fields = {status = "ok"}, narrative = "fell back to: " .. res.narrative}
`}, liveGroupID); err != nil {
		t.Fatalf("Register: %v", err)
	}
	pollUntilTrue(t, 15*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("prose-reader")
		return err == nil, nil
	})

	runnerCtx, stop := context.WithCancel(ctx)
	defer stop()
	runner := luacmd.NewRunner(device, scripts, luacmd.ServeOptions{
		Interval: 300 * time.Millisecond,
		Listener: testListener{t},
	})
	done := make(chan struct{})
	go func() { defer close(done); runner.Serve(runnerCtx) }()
	t.Cleanup(func() {
		stop()
		<-done
	})

	instanceID, err := device.Submit(ctx, "prose-reader", "")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	followCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	last, err := luacmd.Follow(followCtx, device, instanceID, 300*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if last.Fields["status"] != "ok" {
		t.Fatalf("terminal entry = %+v", last)
	}
	if !strings.Contains(last.Narrative, "all quiet on the shop floor") {
		t.Errorf("the script did not fall back to the narrative: %q", last.Narrative)
	}
}
