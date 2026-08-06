package genealogy_test

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/genealogy"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// repoRoot/killAllRegistered/fastRaftArgs/pollUntilTrue are local copies of
// the identical helpers pkg/kvctl's own tests use (see e.g.
// pkg/kvctl/kvctl_test.go, pkg/kvctl/catalog_test.go) -- duplicated on
// purpose rather than imported: this package is a standalone worked
// example (see genealogy.go's own doc comment), not a consumer of
// pkg/kvctl's internal test infrastructure.

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

func pollUntilTrue(t *testing.T, timeout time.Duration, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s (last error: %v)", timeout, lastErr)
}

func TestRecordRequiresAtLeastOneUnit(t *testing.T) {
	if err := genealogy.Record("instance-1", nil, nil, nil, ""); err == nil {
		t.Fatal("expected an error when both inputUnits and outputUnits are empty")
	}
}

func TestRecordRejectsCommaInUnitID(t *testing.T) {
	if err := genealogy.Record("instance-1", []string{"a,b"}, nil, nil, ""); err == nil {
		t.Fatal("expected an error for a unit id containing a comma")
	}
	if err := genealogy.Record("instance-1", nil, []string{"c,d"}, nil, ""); err == nil {
		t.Fatal("expected an error for a unit id containing a comma")
	}
}

func TestRecordRequiresInstanceID(t *testing.T) {
	if err := genealogy.Record("", []string{"u1"}, []string{"u2"}, nil, ""); err == nil {
		t.Fatal("expected an error for an empty instance id")
	}
}

// TestRecordAndQueryBothDirections drives one two-input, one-output
// execution (u1+u2 -> u3) and checks Query surfaces both directions: u3's
// own entry names u1/u2 as its inputs, and each of u1/u2's own entries
// names u3 as what it was consumed into.
func TestRecordAndQueryBothDirections(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const instanceID = "assembly-1"
	fields := map[string]string{"station": "line-1"}
	if err := genealogy.Record(instanceID, []string{"u1", "u2"}, []string{"u3"}, fields, "assembled"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	var outputEvents []genealogy.Event
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		events, err := genealogy.Query("u3", time.Unix(0, 0), time.Now(), 0)
		if err != nil {
			return false, err
		}
		outputEvents = events
		return len(events) == 1, nil
	})
	if outputEvents[0].Role != "output" {
		t.Fatalf("u3 role = %q, want %q", outputEvents[0].Role, "output")
	}
	if outputEvents[0].InstanceID != instanceID {
		t.Fatalf("u3 instance id = %q, want %q", outputEvents[0].InstanceID, instanceID)
	}
	gotRelated := append([]string{}, outputEvents[0].RelatedUnits...)
	sort.Strings(gotRelated)
	if len(gotRelated) != 2 || gotRelated[0] != "u1" || gotRelated[1] != "u2" {
		t.Fatalf("u3 related units = %v, want [u1 u2]", gotRelated)
	}
	if outputEvents[0].Narrative != "assembled" {
		t.Fatalf("u3 narrative = %q, want %q", outputEvents[0].Narrative, "assembled")
	}
	if outputEvents[0].Fields["station"] != "line-1" {
		t.Fatalf("u3 extra field station = %q, want %q", outputEvents[0].Fields["station"], "line-1")
	}

	for _, inputUnit := range []string{"u1", "u2"} {
		var inputEvents []genealogy.Event
		pollUntilTrue(t, 10*time.Second, func() (bool, error) {
			events, err := genealogy.Query(inputUnit, time.Unix(0, 0), time.Now(), 0)
			if err != nil {
				return false, err
			}
			inputEvents = events
			return len(events) == 1, nil
		})
		if inputEvents[0].Role != "input" {
			t.Fatalf("%s role = %q, want %q", inputUnit, inputEvents[0].Role, "input")
		}
		if len(inputEvents[0].RelatedUnits) != 1 || inputEvents[0].RelatedUnits[0] != "u3" {
			t.Fatalf("%s related units = %v, want [u3]", inputUnit, inputEvents[0].RelatedUnits)
		}
	}
}

// TestAncestorsAndDescendantsMultiHop chains two executions -- u1+u2 -> u3
// (instance A), then u3+u5 -> u4 (instance B) -- and checks
// Ancestors/Descendants walk both hops, not just the immediate one.
func TestAncestorsAndDescendantsMultiHop(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := genealogy.Record("instance-a", []string{"u1", "u2"}, []string{"u3"}, nil, ""); err != nil {
		t.Fatalf("Record instance-a: %v", err)
	}
	if err := genealogy.Record("instance-b", []string{"u3", "u5"}, []string{"u4"}, nil, ""); err != nil {
		t.Fatalf("Record instance-b: %v", err)
	}

	sortedCopy := func(ids []string) []string {
		out := append([]string{}, ids...)
		sort.Strings(out)
		return out
	}

	var ancestors []string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		ids, err := genealogy.Ancestors("u4", 0)
		if err != nil {
			return false, err
		}
		ancestors = sortedCopy(ids)
		return len(ancestors) == 4, nil
	})
	if want := []string{"u1", "u2", "u3", "u5"}; !equalAfterSort(ancestors, want) {
		t.Fatalf("Ancestors(u4) = %v, want %v (u5 is a direct parent too)", ancestors, want)
	}

	var descendants []string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		ids, err := genealogy.Descendants("u1", 0)
		if err != nil {
			return false, err
		}
		descendants = sortedCopy(ids)
		return len(descendants) == 2, nil
	})
	if want := []string{"u3", "u4"}; !equalAfterSort(descendants, want) {
		t.Fatalf("Descendants(u1) = %v, want %v", descendants, want)
	}

	// A leaf with no genealogy events at all has no ancestors and no
	// descendants -- Ancestors/Descendants return an empty result, not an
	// error, for a unit nothing was ever recorded against.
	noHistory, err := genealogy.Ancestors("never-recorded", 0)
	if err != nil {
		t.Fatalf("Ancestors(never-recorded): %v", err)
	}
	if len(noHistory) != 0 {
		t.Fatalf("Ancestors(never-recorded) = %v, want empty", noHistory)
	}
}

func equalAfterSort(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	g, w := append([]string{}, got...), append([]string{}, want...)
	sort.Strings(g)
	sort.Strings(w)
	for i := range g {
		if g[i] != w[i] {
			return false
		}
	}
	return true
}
