package relations_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/genealogy"
	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// repoRoot/killAllRegistered/fastRaftArgs are the same local copies of
// pkg/kvctl's test helpers examples/genealogy's own test file carries,
// duplicated for the same reason: a worked example is not a consumer of
// another package's test infrastructure.

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
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

// startNode boots a single-node cluster and returns a Backend bound to
// it.
func startNode(t *testing.T) relations.Backend {
	t.Helper()
	root := repoRoot(t)
	t.Setenv(registry.EnvHome, t.TempDir())

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	be, err := relations.CurrentNode()
	if err != nil {
		t.Fatalf("CurrentNode: %v", err)
	}
	return be
}

// TestLiveNodeCarriesTheBinaryKeys is what the in-memory tests cannot
// show: that nine raw bytes -- including the 0x00 bytes a declaration's
// Zero half is made of -- survive the whole path a real write takes.
// Key and value are capnp Data fields end to end (see
// api/shmevent.capnp), raft carries them as opaque bytes, and pkg/store
// keys a SQLite BLOB column, so nothing along the way treats a key as
// text; this test is what keeps that true.
func TestLiveNodeCarriesTheBinaryKeys(t *testing.T) {
	ctx := context.Background()
	be := startNode(t)

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	st, _, _ := newStoreOn(t, be, pub, priv)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	row, err := j.Row(ctx, entries[0])
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if len(row) != 6 {
		t.Fatalf("line 1 read back with %d columns, want 6", len(row))
	}
	for _, cell := range row {
		if cell.FieldName == "" {
			t.Fatalf("column %s came back without a heading", cell.Field)
		}
	}

	operator, err := j.Term(ctx, fields[fieldOperator], "Ivanova")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	withIvanova, err := j.EntriesWith(ctx, operator)
	if err != nil {
		t.Fatalf("EntriesWith: %v", err)
	}
	if len(withIvanova) != 2 {
		t.Fatalf("EntriesWith(Ivanova) returned %d lines, want 2", len(withIvanova))
	}

	// Signatures made before the round trip still verify after it, which
	// is only true if every byte came back exactly as written.
	for _, entry := range entries {
		decl, found, err := st.Declaration(ctx, entry)
		if err != nil || !found {
			t.Fatalf("Declaration(%s) = %v, %v", entry, found, err)
		}
		if err := st.Verify(ctx, decl); err != nil {
			t.Fatalf("Verify(%s) after a raft round trip: %v", entry, err)
		}
	}

	// The whole book verifies after a raft round trip: every digest
	// recomputed from records that went out over IPC, through raft, into
	// SQLite and back.
	if checked, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain against a live node: %v", err)
	} else if checked != len(entries) {
		t.Fatalf("verified %d lines, want %d", checked, len(entries))
	}

	// And the keys really are in the user namespace of the same store
	// `mage rangescan` reads, byte for byte.
	start, end := relations.NamespaceBounds(relations.NamespaceRelation)
	pairs, err := kvctl.RangeScan(string(start), string(end), 0, 0, kvctl.RangeOrderAsc)
	if err != nil {
		t.Fatalf("kvctl.RangeScan: %v", err)
	}
	if len(pairs) == 0 {
		t.Fatal("rangescan over the relation namespace found nothing")
	}
	for _, p := range pairs {
		if len(p.Key) != relations.KeyLen {
			t.Fatalf("stored key %x is %d bytes, want %d", p.Key, len(p.Key), relations.KeyLen)
		}
	}
}

// TestLiveGenealogyAgreesWithTheLogRecordExample runs the same
// transformation graph through both examples against the same live node
// -- examples/genealogy writing pkg/logrecord entries, this package
// writing relation edges -- and checks they answer identically. That is
// the compatibility claim: same questions, same answers, different
// storage.
func TestLiveGenealogyAgreesWithTheLogRecordExample(t *testing.T) {
	ctx := context.Background()
	be := startNode(t)

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	st, _, _ := newStoreOn(t, be, pub, priv)
	g, err := relations.NewGenealogy(ctx, relations.NewJournal(st))
	if err != nil {
		t.Fatalf("NewGenealogy: %v", err)
	}

	buildGenealogy(t, g)
	if err := genealogy.Record("instance-a", []string{"u1", "u2"}, []string{"u3"}, nil, ""); err != nil {
		t.Fatalf("genealogy.Record instance-a: %v", err)
	}
	if err := genealogy.Record("instance-b", []string{"u3", "u5"}, []string{"u4"}, nil, ""); err != nil {
		t.Fatalf("genealogy.Record instance-b: %v", err)
	}

	for _, tc := range []struct {
		name string
		unit string
		ours func() ([]string, error)
		logs func() ([]string, error)
	}{
		{
			name: "ancestors of u4",
			unit: "u4",
			ours: func() ([]string, error) { return g.Ancestors(ctx, "u4", 0) },
			logs: func() ([]string, error) { return genealogy.Ancestors("u4", 0) },
		},
		{
			name: "descendants of u1",
			unit: "u1",
			ours: func() ([]string, error) { return g.Descendants(ctx, "u1", 0) },
			logs: func() ([]string, error) { return genealogy.Descendants("u1", 0) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ours, err := tc.ours()
			if err != nil {
				t.Fatalf("relations: %v", err)
			}
			logs, err := tc.logs()
			if err != nil {
				t.Fatalf("genealogy: %v", err)
			}
			if !sameSet(ours, logs) {
				t.Fatalf("the two examples disagree about %s: relations %v, genealogy %v", tc.name, ours, logs)
			}
			if len(ours) == 0 {
				t.Fatalf("both examples returned nothing for %s", tc.name)
			}
		})
	}
}
