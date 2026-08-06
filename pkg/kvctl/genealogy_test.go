package kvctl_test

import (
	"sort"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestRecordGenealogyRequiresAtLeastOneUnit and
// TestRecordGenealogyRejectsCommaInUnitID exercise RecordGenealogy's
// argument validation, which runs before any registry/IPC access -- so
// these need no running node at all, unlike every other test in this file.
func TestRecordGenealogyRequiresAtLeastOneUnit(t *testing.T) {
	if err := kvctl.RecordGenealogy("instance-1", nil, nil, nil, ""); err == nil {
		t.Fatal("expected an error when both inputUnits and outputUnits are empty")
	}
}

func TestRecordGenealogyRejectsCommaInUnitID(t *testing.T) {
	if err := kvctl.RecordGenealogy("instance-1", []string{"a,b"}, nil, nil, ""); err == nil {
		t.Fatal("expected an error for a unit id containing a comma")
	}
	if err := kvctl.RecordGenealogy("instance-1", nil, []string{"c,d"}, nil, ""); err == nil {
		t.Fatal("expected an error for a unit id containing a comma")
	}
}

func TestRecordGenealogyRequiresInstanceID(t *testing.T) {
	if err := kvctl.RecordGenealogy("", []string{"u1"}, []string{"u2"}, nil, ""); err == nil {
		t.Fatal("expected an error for an empty instance id")
	}
}

// TestRecordGenealogyAndQueryBothDirections drives one two-input,
// one-output execution (u1+u2 -> u3) and checks QueryGenealogy surfaces
// both directions: u3's own entry names u1/u2 as its inputs, and each of
// u1/u2's own entries names u3 as what it was consumed into.
func TestRecordGenealogyAndQueryBothDirections(t *testing.T) {
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
	if err := kvctl.RecordGenealogy(instanceID, []string{"u1", "u2"}, []string{"u3"}, fields, "assembled"); err != nil {
		t.Fatalf("RecordGenealogy: %v", err)
	}

	var outputEvents []kvctl.GenealogyEvent
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		events, err := kvctl.QueryGenealogy("u3", time.Unix(0, 0), time.Now(), 0)
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
		var inputEvents []kvctl.GenealogyEvent
		pollUntilTrue(t, 10*time.Second, func() (bool, error) {
			events, err := kvctl.QueryGenealogy(inputUnit, time.Unix(0, 0), time.Now(), 0)
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

// TestGenealogyAncestorsAndDescendantsMultiHop chains two executions --
// u1+u2 -> u3 (instance A), then u3+u5 -> u4 (instance B) -- and checks
// Ancestors/Descendants walk both hops, not just the immediate one.
func TestGenealogyAncestorsAndDescendantsMultiHop(t *testing.T) {
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

	if err := kvctl.RecordGenealogy("instance-a", []string{"u1", "u2"}, []string{"u3"}, nil, ""); err != nil {
		t.Fatalf("RecordGenealogy instance-a: %v", err)
	}
	if err := kvctl.RecordGenealogy("instance-b", []string{"u3", "u5"}, []string{"u4"}, nil, ""); err != nil {
		t.Fatalf("RecordGenealogy instance-b: %v", err)
	}

	sortedCopy := func(ids []string) []string {
		out := append([]string{}, ids...)
		sort.Strings(out)
		return out
	}

	var ancestors []string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		ids, err := kvctl.Ancestors("u4", 0)
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
		ids, err := kvctl.Descendants("u1", 0)
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
	noHistory, err := kvctl.Ancestors("never-recorded", 0)
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
