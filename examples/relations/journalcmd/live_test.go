package journalcmd_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/examples/relations/journalcmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// repoRoot/killAllRegistered/fastRaftArgs are the same local copies of
// pkg/kvctl's test helpers examples/genealogy and examples/relations
// each carry, duplicated for the same reason: a worked example is not a
// consumer of another package's test infrastructure.

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

func killAllRegistered(t *testing.T, reg *registry.Registry) {
	t.Helper()
	nodes, err := reg.List()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, info := range nodes {
		if info.PID == 0 {
			continue
		}
		proc, err := os.FindProcess(info.PID)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(proc *os.Process) {
			defer wg.Done()
			_ = proc.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() {
				proc.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = proc.Kill()
			}
		}(proc)
	}
	wg.Wait()
}

var fastRaftArgs = []string{
	"-raft-heartbeat-timeout", "300ms",
	"-raft-election-timeout", "300ms",
	"-raft-leader-lease-timeout", "250ms",
}

const (
	liveCommandID = "shift-log"
	liveGroupID   = "shift-operators"
	liveTimeout   = 20 * time.Second
)

// TestLiveCommandRoundTrip is the whole point of this package, end to
// end against a real node: a command exists in the catalog, a group is
// permitted to submit it, a service answers it, and a submitter that
// never touches a journal key gets a form, fills it in, and gets a line
// written.
func TestLiveCommandRoundTrip(t *testing.T) {
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

	// The catalog: a command bound to this node, and a group permitted
	// to submit it. This is the part the FSM enforces.
	if err := kvctl.PutGroup(liveGroupID, "Shift operators", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand(liveCommandID, "Shift log", peerID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(liveCommandID, liveGroupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(peerID, liveGroupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}

	// The service, on the node that owns the log.
	backend, err := relations.CurrentNode()
	if err != nil {
		t.Fatalf("CurrentNode: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	actor := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 1}
	store := relations.New(backend, testLog, actor, priv)
	if err := store.DeclareActor(ctx, actor, "the log node", pub); err != nil {
		t.Fatalf("DeclareActor: %v", err)
	}
	journal := relations.NewJournal(store)
	defineShiftLog(t, journal)
	operator, err := journal.Field(ctx, "operator")
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	for _, name := range []string{"Ivanova", "Petrov"} {
		if _, err := journal.Term(ctx, operator, name); err != nil {
			t.Fatalf("Term: %v", err)
		}
	}
	if err := journal.CloseField(ctx, operator); err != nil {
		t.Fatalf("CloseField: %v", err)
	}

	stop := make(chan struct{})
	defer close(stop)
	go journalcmd.New(journal).Run(liveCommandID, stop, func(err error) { t.Logf("dispatcher: %v", err) })

	// A submitter asks what the page looks like...
	form, err := journalcmd.FetchForm(liveCommandID, liveTimeout)
	if err != nil {
		t.Fatalf("FetchForm: %v", err)
	}
	byName := make(map[string]journalcmd.Column, len(form.Columns))
	for _, column := range form.Columns {
		byName[column.Name] = column
	}
	if !byName["operator"].Closed || len(byName["operator"].Options) != 2 {
		t.Fatalf("the operator column came back as %+v, want a closed vocabulary of two", byName["operator"])
	}
	if byName["pieces"].Input != "number" {
		t.Fatalf("pieces came back as %q", byName["pieces"].Input)
	}

	// ...fills it in, and gets a line.
	line, err := journalcmd.AppendLine(liveCommandID, map[string]string{
		"operator": "Ivanova",
		"machine":  "Lathe-2",
		"result":   "OK",
		"pieces":   "120",
	}, liveTimeout)
	if err != nil {
		t.Fatalf("AppendLine: %v", err)
	}
	entry, err := relations.ParseEntity(line)
	if err != nil {
		t.Fatalf("ParseEntity(%q): %v", line, err)
	}
	row, err := journal.Row(ctx, entry)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	got := make(map[string]relations.RowCell, len(row))
	for _, cell := range row {
		got[cell.FieldName] = cell
	}
	if got["pieces"].Number != 120 {
		t.Fatalf("the line records %d pieces", got["pieces"].Number)
	}
	if got["submitted_by"].Text != peerID {
		t.Fatalf("the line records the submitter as %q, want %s", got["submitted_by"].Text, peerID)
	}

	// A value the closed vocabulary does not admit is refused by the
	// service, not written.
	if _, err := journalcmd.AppendLine(liveCommandID, map[string]string{"operator": "Nobody"}, liveTimeout); err == nil {
		t.Fatal("a value outside the closed vocabulary was accepted")
	} else if !strings.Contains(err.Error(), "vocabulary is closed") {
		t.Fatalf("refusal = %v", err)
	}

	// The page reads back through the same command.
	page, err := journalcmd.RenderPage(liveCommandID, 0, liveTimeout)
	if err != nil {
		t.Fatalf("RenderPage: %v", err)
	}
	if !strings.Contains(page, "Ivanova") || !strings.Contains(page, "log 1, page 1") {
		t.Fatalf("the rendered page came back as:\n%s", page)
	}
	t.Logf("the page, fetched over a command:\n\n%s", page)

	if _, err := journal.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
}

// TestLiveSubmissionNeedsStanding is the assurance the command layer
// exists for: take the peer out of the permitted group, and the FSM
// refuses the submission before any of this package's code runs.
func TestLiveSubmissionNeedsStanding(t *testing.T) {
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
	if err := kvctl.PutGroup(liveGroupID, "Shift operators", false); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand(liveCommandID, "Shift log", peerID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand(liveCommandID, liveGroupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}

	// No membership yet: refused.
	if _, err := journalcmd.Submit(liveCommandID, journalcmd.Request{Op: journalcmd.OpForm}); err == nil {
		t.Fatal("a peer with no standing submitted a command")
	} else if !strings.Contains(err.Error(), "not permitted") {
		t.Fatalf("refusal = %v", err)
	}

	// Admitted: accepted.
	if err := kvctl.AddPeerToGroup(peerID, liveGroupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	if _, err := journalcmd.Submit(liveCommandID, journalcmd.Request{Op: journalcmd.OpForm}); err != nil {
		t.Fatalf("Submit after being admitted: %v", err)
	}

	// And removed again: refused again, with nothing this package could
	// have done about it either way.
	if err := kvctl.RemovePeerFromGroup(peerID, liveGroupID); err != nil {
		t.Fatalf("RemovePeerFromGroup: %v", err)
	}
	if _, err := journalcmd.Submit(liveCommandID, journalcmd.Request{Op: journalcmd.OpForm}); err == nil {
		t.Fatal("a peer removed from the group still submitted a command")
	}
}
