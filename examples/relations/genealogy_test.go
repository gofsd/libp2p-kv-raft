package relations_test

import (
	"context"
	"errors"
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

// TestGenealogyInSharesAnExistingColumnsTerms is why NewGenealogyIn
// exists. Terms are interned per column, so a genealogy that declares
// its own "unit" column holds a *different* entity for id "T-1" than a
// log whose lines name "T-1" in a "thing" column -- one object, two
// entities, and neither the edges nor the id's own history reachable
// from the other. Naming the column the log already uses is what keeps
// one object one entity.
func TestGenealogyInSharesAnExistingColumnsTerms(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	// The log's own schema, declared before any genealogy exists.
	thing, err := j.DefineField(ctx, "thing", relations.InputTerm)
	if err != nil {
		t.Fatalf("DefineField(thing): %v", err)
	}
	if _, err := j.Append(ctx, relations.TermCell(thing, "T-1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	logTerm, err := j.Term(ctx, thing, "T-1")
	if err != nil {
		t.Fatalf("Term(thing, T-1): %v", err)
	}

	shared, err := relations.NewGenealogyIn(ctx, j, "thing", "instance")
	if err != nil {
		t.Fatalf("NewGenealogyIn: %v", err)
	}
	unit, err := shared.Unit(ctx, "T-1")
	if err != nil {
		t.Fatalf("Unit: %v", err)
	}
	if unit != logTerm {
		t.Fatalf("genealogy unit for T-1 is %s, but the log's own cell is %s -- they must be the same entity", unit, logTerm)
	}

	// The hazard this guards against, stated as a test: the default
	// constructor's own "unit" column is a separate term space, and the
	// same id in it is a different entity.
	separate, err := relations.NewGenealogy(ctx, j)
	if err != nil {
		t.Fatalf("NewGenealogy: %v", err)
	}
	other, err := separate.Unit(ctx, "T-1")
	if err != nil {
		t.Fatalf("Unit: %v", err)
	}
	if other == logTerm {
		t.Fatalf("a genealogy on its own %q column returned the log's %q term for T-1; terms are supposed to be per column",
			relations.UnitFieldName, "thing")
	}

	// Pointing a genealogy at a column that holds something other than
	// terms is a schema conflict, not a column to quietly intern into.
	if _, err := j.DefineField(ctx, "note", relations.InputText); err != nil {
		t.Fatalf("DefineField(note): %v", err)
	}
	if _, err := relations.NewGenealogyIn(ctx, j, "note", "instance"); err == nil {
		t.Fatal("NewGenealogyIn on a free-text column succeeded, want a schema conflict")
	}
}

// TestAppendWithWritesEdgesInTheLinesTransaction pins the seam a log
// keeping its own relations alongside its lines depends on: the line and
// the ops that describe it either both land or neither does.
func TestAppendWithWritesEdgesInTheLinesTransaction(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	thing, err := j.DefineField(ctx, "thing", relations.InputTerm)
	if err != nil {
		t.Fatalf("DefineField(thing): %v", err)
	}
	g, err := relations.NewGenealogyIn(ctx, j, "thing", "instance")
	if err != nil {
		t.Fatalf("NewGenealogyIn: %v", err)
	}

	// A failing extra abandons the line with it.
	before := len(scanAll(t, st))
	wantErr := errors.New("no")
	if _, err := j.AppendWith(ctx, func(relations.Entity) ([]relations.Op, error) {
		return nil, wantErr
	}, relations.TermCell(thing, "T-out")); !errors.Is(err, wantErr) {
		t.Fatalf("AppendWith with a failing extra: err = %v, want %v", err, wantErr)
	}
	// Terms interned along the way survive -- see AppendWith's own doc
	// comment -- so what must be absent is the line's cells and the
	// edges, not every trace of the attempt.
	if n := countRecordsOfKind(t, st, relations.KindCell); n != 0 {
		t.Fatalf("%d cell record(s) written after a failing extra, want none", n)
	}
	if n := countRecordsOfKind(t, st, relations.KindDerivedFrom); n != 0 {
		t.Fatalf("%d edge record(s) written after a failing extra, want none", n)
	}
	if len(scanAll(t, st)) < before {
		t.Fatal("the store shrank")
	}

	// The success path: one line, and its edges, in one transaction.
	entry, err := j.AppendWith(ctx, func(relations.Entity) ([]relations.Op, error) {
		return g.RecordOps(ctx, "wo-1", []string{"T-in-a", "T-in-b"}, []string{"T-out"})
	}, relations.TermCell(thing, "T-out"))
	if err != nil {
		t.Fatalf("AppendWith: %v", err)
	}
	if entry == relations.Zero {
		t.Fatal("AppendWith returned the zero entry")
	}
	row, err := j.Row(ctx, entry)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if len(row) != 1 || row[0].Field != thing || row[0].Text != "T-out" {
		t.Fatalf("the line reads back as %+v, want one thing cell holding T-out", row)
	}

	ancestors, err := g.Ancestors(ctx, "T-out", 0)
	if err != nil {
		t.Fatalf("Ancestors: %v", err)
	}
	if want := []string{"T-in-a", "T-in-b"}; !sameSet(ancestors, want) {
		t.Fatalf("Ancestors(T-out) = %v, want %v", ancestors, want)
	}

	// The edges hang off the same entity the line's own cell names, which
	// is the whole point of sharing the column.
	edges, err := g.Edges(ctx, "T-out")
	if err != nil {
		t.Fatalf("Edges: %v", err)
	}
	if len(edges) != 2 {
		t.Fatalf("Edges(T-out) = %d, want 2", len(edges))
	}
	for _, e := range edges {
		if e.Instance != "wo-1" {
			t.Fatalf("edge instance = %q, want %q", e.Instance, "wo-1")
		}
	}
}

// countRecordsOfKind reports how many records in the whole store are of
// the given kind, mirrors included.
func countRecordsOfKind(t *testing.T, st *relations.Store, kind byte) int {
	t.Helper()
	n := 0
	for _, p := range scanAll(t, st) {
		rec, _, err := relations.DecodeRecord(p.Value)
		if err != nil {
			t.Fatalf("DecodeRecord: %v", err)
		}
		if rec.Kind == kind {
			n++
		}
	}
	return n
}

// TestAppendWithChainsTheEdgesItWrites is the reason folding edges into
// a line's transaction is worth more than writing them beside it: the
// derivation claims land inside the book's own signed chain, so an edge
// added or removed afterwards is as visible as a tampered line. An edge
// written by Genealogy.Record's standalone Apply is not chained (nothing
// in the package chains a transaction it does not open), which is the
// standing every relation here had before the chain existed.
func TestAppendWithChainsTheEdgesItWrites(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	thing, err := j.DefineField(ctx, "thing", relations.InputTerm)
	if err != nil {
		t.Fatalf("DefineField(thing): %v", err)
	}
	g, err := relations.NewGenealogyIn(ctx, j, "thing", "instance")
	if err != nil {
		t.Fatalf("NewGenealogyIn: %v", err)
	}

	if _, err := j.AppendWith(ctx, func(relations.Entity) ([]relations.Op, error) {
		return g.RecordOps(ctx, "wo-1", []string{"T-in-a", "T-in-b"}, []string{"T-out"})
	}, relations.TermCell(thing, "T-out")); err != nil {
		t.Fatalf("AppendWith: %v", err)
	}

	// One line and one event per edge.
	checked, err := j.VerifyChain(ctx)
	if err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
	if checked != 3 {
		t.Fatalf("verified %d events, want 3 (the line, and one derivation per input)", checked)
	}

	events, err := j.Events(ctx, relations.Range{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	derivations := 0
	for _, e := range events {
		if e.Kind() == "derivation" {
			derivations++
		}
	}
	if derivations != 2 {
		t.Fatalf("%d derivation events chained, want 2", derivations)
	}

	// Edges written the standalone way are outside the chain, and the
	// book still verifies -- they are unchained, not invalid.
	if err := g.Record(ctx, "wo-2", []string{"T-out"}, []string{"T-final"}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	after, err := j.VerifyChain(ctx)
	if err != nil {
		t.Fatalf("VerifyChain after a standalone Record: %v", err)
	}
	if after != checked {
		t.Fatalf("verified %d events after a standalone Record, want the same %d", after, checked)
	}
}
