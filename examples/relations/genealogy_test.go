package relations_test

import (
	"context"
	"sort"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// buildGenealogy records the same two-execution graph
// examples/genealogy's own multi-hop test uses -- u1+u2 -> u3 (instance
// a), then u3+u5 -> u4 (instance b) -- so the two examples can be
// compared claim for claim.
func buildGenealogy(t *testing.T, g *relations.Genealogy) {
	t.Helper()
	ctx := context.Background()
	if err := g.Record(ctx, "instance-a", []string{"u1", "u2"}, []string{"u3"}); err != nil {
		t.Fatalf("Record instance-a: %v", err)
	}
	if err := g.Record(ctx, "instance-b", []string{"u3", "u5"}, []string{"u4"}); err != nil {
		t.Fatalf("Record instance-b: %v", err)
	}
}

func newGenealogy(t *testing.T) (*relations.Genealogy, *relations.Store) {
	t.Helper()
	st, _, _ := newStore(t)
	g, err := relations.NewGenealogy(context.Background(), relations.NewJournal(st))
	if err != nil {
		t.Fatalf("NewGenealogy: %v", err)
	}
	return g, st
}

// TestGenealogyMatchesTheLogRecordExample asserts the same answers
// examples/genealogy's TestAncestorsAndDescendantsMultiHop asserts, from
// the same inputs -- the compatibility claim in NewGenealogy's doc
// comment, checked rather than described.
func TestGenealogyMatchesTheLogRecordExample(t *testing.T) {
	ctx := context.Background()
	g, _ := newGenealogy(t)
	buildGenealogy(t, g)

	ancestors, err := g.Ancestors(ctx, "u4", 0)
	if err != nil {
		t.Fatalf("Ancestors: %v", err)
	}
	if want := []string{"u1", "u2", "u3", "u5"}; !sameSet(ancestors, want) {
		t.Fatalf("Ancestors(u4) = %v, want %v (u5 is a direct parent too)", ancestors, want)
	}

	descendants, err := g.Descendants(ctx, "u1", 0)
	if err != nil {
		t.Fatalf("Descendants: %v", err)
	}
	if want := []string{"u3", "u4"}; !sameSet(descendants, want) {
		t.Fatalf("Descendants(u1) = %v, want %v", descendants, want)
	}

	// A unit nothing was ever recorded against traces to nothing, and is
	// not an error.
	none, err := g.Ancestors(ctx, "never-recorded", 0)
	if err != nil {
		t.Fatalf("Ancestors(never-recorded): %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("Ancestors(never-recorded) = %v, want empty", none)
	}

	// Depth is honoured: one hop back from u4 is its direct parents
	// only, not their parents.
	direct, err := g.Ancestors(ctx, "u4", 1)
	if err != nil {
		t.Fatalf("Ancestors(u4, 1): %v", err)
	}
	if want := []string{"u3", "u5"}; !sameSet(direct, want) {
		t.Fatalf("Ancestors(u4, 1) = %v, want %v", direct, want)
	}
}

// TestGenealogyEdgesCarryTheirExecution checks the part a bare
// parent/child edge would lose: which execution produced it, and who
// recorded that.
func TestGenealogyEdgesCarryTheirExecution(t *testing.T) {
	ctx := context.Background()
	g, st := newGenealogy(t)
	buildGenealogy(t, g)

	edges, err := g.Edges(ctx, "u3")
	if err != nil {
		t.Fatalf("Edges: %v", err)
	}
	if len(edges) != 3 {
		t.Fatalf("u3 has %d edges, want 3 (produced from u1 and u2, consumed into u4)", len(edges))
	}
	var produced, consumed int
	for _, e := range edges {
		switch {
		case e.Output == "u3":
			produced++
			if e.Instance != "instance-a" {
				t.Fatalf("edge %s <- %s names instance %q, want instance-a", e.Output, e.Input, e.Instance)
			}
		case e.Input == "u3":
			consumed++
			if e.Output != "u4" || e.Instance != "instance-b" {
				t.Fatalf("edge %s <- %s names instance %q, want u4 <- u3 in instance-b", e.Output, e.Input, e.Instance)
			}
		default:
			t.Fatalf("edge %s <- %s does not involve u3", e.Output, e.Input)
		}
		if e.At.IsZero() {
			t.Fatalf("edge %s <- %s has no timestamp", e.Output, e.Input)
		}
	}
	if produced != 2 || consumed != 1 {
		t.Fatalf("u3: %d producing edges and %d consuming, want 2 and 1", produced, consumed)
	}

	// Every edge -- both physical copies of it -- is signed and
	// verifiable, the same as any other record in the store.
	u3, err := g.Unit(ctx, "u3")
	if err != nil {
		t.Fatalf("Unit: %v", err)
	}
	rels, err := st.Relations(ctx, u3)
	if err != nil {
		t.Fatalf("Relations: %v", err)
	}
	for _, rel := range relations.OfKind(rels, relations.KindDerivedFrom) {
		if err := st.Verify(ctx, rel); err != nil {
			t.Fatalf("Verify(%s -> %s): %v", rel.A, rel.B, err)
		}
	}
}

// TestGenealogyInternsUnitIDs is the difference from the log-record
// example that motivated this one: there, every entry repeats the unit
// ids on the other side of the transformation as text, so "u3" is
// written into four separate records; here it exists once.
func TestGenealogyInternsUnitIDs(t *testing.T) {
	g, st := newGenealogy(t)
	buildGenealogy(t, g)

	pairs := scanAll(t, st)
	for _, id := range []string{"u1", "u2", "u3", "u4", "u5", "instance-a", "instance-b"} {
		if n := countRecordsNamed(t, pairs, id); n != 1 {
			t.Fatalf("%q is stored in %d records, want exactly 1", id, n)
		}
	}

	// The edges that reference them carry no text at all -- four bytes
	// of instance reference and nothing else.
	for _, p := range pairs {
		rec, _, err := relations.DecodeRecord(p.Value)
		if err != nil {
			t.Fatalf("DecodeRecord: %v", err)
		}
		if rec.Kind == relations.KindDerivedFrom {
			if rec.Name != "" {
				t.Fatalf("edge record carries the text %q", rec.Name)
			}
			if len(rec.Data) != relations.EntityLen {
				t.Fatalf("edge payload is %d bytes, want a %d-byte instance reference", len(rec.Data), relations.EntityLen)
			}
		}
	}
}

// TestGenealogyRecordIsAtomicAndValidated covers the input checks and
// the all-or-nothing write examples/genealogy explicitly does not have
// (see its Record's "Not atomic" note).
func TestGenealogyRecordIsAtomicAndValidated(t *testing.T) {
	ctx := context.Background()
	g, _ := newGenealogy(t)

	if err := g.Record(ctx, "", []string{"u1"}, []string{"u2"}); err == nil {
		t.Fatal("expected an error recording with no instance id")
	}
	if err := g.Record(ctx, "instance", nil, nil); err == nil {
		t.Fatal("expected an error recording with neither inputs nor outputs")
	}
	if err := g.Record(ctx, "instance", []string{"u1"}, []string{"u1"}); err == nil {
		t.Fatal("expected an error recording a unit as both input and output")
	}

	// One-sided is allowed: a unit that came from nowhere this log knows
	// about still gets interned, so a later execution can reference it.
	if err := g.Record(ctx, "goods-in", nil, []string{"raw-1"}); err != nil {
		t.Fatalf("Record with only outputs: %v", err)
	}
	if _, err := g.Unit(ctx, "raw-1"); err != nil {
		t.Fatalf("Unit(raw-1): %v", err)
	}
}

func sameSet(got, want []string) bool {
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
