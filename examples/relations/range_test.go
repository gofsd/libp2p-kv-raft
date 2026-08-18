package relations_test

import (
	"context"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

func TestEntityOrderMatchesKeyOrder(t *testing.T) {
	e := relations.Entity{Log: 1, Page: 2, Type: 3, ID: 4}
	if got := relations.EntityFromValue(e.Value()); got != e {
		t.Fatalf("round trip through Value = %s, want %s", got, e)
	}
	next, ok := e.Next()
	if !ok || next != (relations.Entity{Log: 1, Page: 2, Type: 3, ID: 5}) {
		t.Fatalf("Next = %s, %v", next, ok)
	}
	// Next carries across the byte boundaries, because an entity is a
	// 32-bit number spelled most significant first -- the same order the
	// store compares its key bytes in.
	rollover := relations.Entity{Log: 1, Page: 2, Type: 3, ID: 0xFF}
	next, ok = rollover.Next()
	if !ok || next != (relations.Entity{Log: 1, Page: 2, Type: 4, ID: 0}) {
		t.Fatalf("Next across a byte boundary = %s, %v", next, ok)
	}
	if _, ok := (relations.Entity{Log: 0xFF, Page: 0xFF, Type: 0xFF, ID: 0xFF}).Next(); ok {
		t.Fatal("the last entity there is reported a successor")
	}
	if e.Compare(next) >= 0 || next.Compare(e) <= 0 || e.Compare(e) != 0 {
		t.Fatal("Compare does not order entities the way their keys sort")
	}
}

// TestRelationsRangePagesThrough walks one entity's relations a few at a
// time and checks the pages join up: every relation once, in order, with
// no repeats and nothing skipped.
func TestRelationsRangePagesThrough(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)

	source, err := st.Allocate(ctx, 0, 0x50, relations.KindDeclaration, "source", nil)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	var targets []relations.Entity
	for i := 0; i < 10; i++ {
		target, err := st.Allocate(ctx, 0, 0x51, relations.KindDeclaration, "", nil)
		if err != nil {
			t.Fatalf("Allocate target: %v", err)
		}
		if err := st.Link(ctx, source, target, 0x60, nil); err != nil {
			t.Fatalf("Link: %v", err)
		}
		targets = append(targets, target)
	}

	var got []relations.Entity
	pages := 0
	r := relations.Range{Limit: 3}
	for {
		batch, err := st.RelationsRange(ctx, source, r)
		if err != nil {
			t.Fatalf("RelationsRange: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		if len(batch) > r.Limit {
			t.Fatalf("a page returned %d relations, want at most %d", len(batch), r.Limit)
		}
		pages++
		for _, rel := range batch {
			got = append(got, rel.B)
		}
		next, ok := r.Resume(batch[len(batch)-1].B)
		if !ok {
			break
		}
		r = next
	}

	if pages != 4 {
		t.Fatalf("read %d pages, want 4 (10 relations, 3 at a time)", pages)
	}
	if len(got) != len(targets) {
		t.Fatalf("paging returned %d relations, want %d", len(got), len(targets))
	}
	for i := range targets {
		if got[i] != targets[i] {
			t.Fatalf("relation %d = %s, want %s (paging must not reorder or repeat)", i, got[i], targets[i])
		}
	}

	// A declaration is not a relation, whatever the lower bound says:
	// asking from the very beginning still starts past it.
	fromZero, err := st.RelationsRange(ctx, source, relations.Range{From: relations.Zero, Limit: 1})
	if err != nil {
		t.Fatalf("RelationsRange from Zero: %v", err)
	}
	if len(fromZero) != 1 || fromZero[0].B != targets[0] {
		t.Fatalf("scan from Zero = %+v, want the first relation", fromZero)
	}
}

func TestRangeResumeStopsAtItsUpperBound(t *testing.T) {
	r := relations.Range{
		From:  relations.Entity{ID: 1},
		To:    relations.Entity{Log: 1, Page: 0, Type: 0, ID: 10},
		Limit: 5,
	}
	next, ok := r.Resume(relations.Entity{Log: 1, ID: 9})
	if !ok {
		t.Fatal("Resume stopped before reaching the upper bound")
	}
	if next.From != (relations.Entity{Log: 1, ID: 10}) || next.To != r.To || next.Limit != r.Limit {
		t.Fatalf("Resume = %+v, want the same bound and limit one past the cursor", next)
	}
	if _, ok := r.Resume(r.To); ok {
		t.Fatal("Resume continued past its own upper bound")
	}
	if _, ok := (relations.Range{}).Resume(relations.Entity{Log: 0xFF, Page: 0xFF, Type: 0xFF, ID: 0xFF}); ok {
		t.Fatal("Resume continued past the last entity there is")
	}
}

// TestEntriesWithInReadsOnlyThePagesAsked is the property the key layout
// makes free: because the index namespace orders a term's backlinks by
// the line's own (page, id) bytes, a page window is a sub-range scan
// rather than a filter over the term's whole history -- and for lines,
// page order is the order they were written.
func TestEntriesWithInReadsOnlyThePagesAsked(t *testing.T) {
	ctx := context.Background()
	be := &countingBackend{Backend: relations.Memory()}
	pub, priv := newKey(t)
	st, _, _ := newStoreOn(t, be, pub, priv)
	j := relations.NewJournal(st)

	field, err := j.Field(ctx, fieldResult)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	term, err := j.Term(ctx, field, "OK")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}

	// 260 lines: page 1 fills at 255, the rest roll onto page 2.
	var lines []relations.Entity
	for i := 0; i < 260; i++ {
		entry, err := j.Append(ctx, relations.TermCell(field, "OK"))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		lines = append(lines, entry)
	}

	be.reset()
	all, err := j.EntriesWith(ctx, term)
	if err != nil {
		t.Fatalf("EntriesWith: %v", err)
	}
	if len(all) != len(lines) {
		t.Fatalf("EntriesWith returned %d lines, want %d", len(all), len(lines))
	}
	unbounded := be.pairs.Load()

	be.reset()
	secondPage, err := j.EntriesWithIn(ctx, term, j.EntryPages(relations.FirstEntryPage+1, relations.FirstEntryPage+1))
	if err != nil {
		t.Fatalf("EntriesWithIn: %v", err)
	}
	// The narrowing happens in the key bounds, so the store hands back
	// only the lines on that page -- it is not a filter over the term's
	// whole history.
	if bounded := be.pairs.Load(); bounded != int64(len(lines)-255) {
		t.Fatalf("a one-page scan read %d records, want %d (the unbounded scan read %d)",
			bounded, len(lines)-255, unbounded)
	}
	if len(secondPage) != len(lines)-255 {
		t.Fatalf("page 2 holds %d of the term's lines, want %d", len(secondPage), len(lines)-255)
	}
	for _, line := range secondPage {
		if line.Page != relations.FirstEntryPage+1 {
			t.Fatalf("line %s is not on the page asked for", line)
		}
	}

	// Bounded means bounded: a limit caps what comes back, and resuming
	// from the last line of one batch continues with the next.
	be.reset()
	first, err := j.EntriesWithIn(ctx, term, relations.Range{Limit: 4})
	if err != nil {
		t.Fatalf("EntriesWithIn(limit): %v", err)
	}
	if len(first) != 4 {
		t.Fatalf("a limited scan returned %d lines, want 4", len(first))
	}
	if read := be.pairs.Load(); read != 4 {
		t.Fatalf("a limit of 4 read %d records from the store, want 4", read)
	}
	r, ok := (relations.Range{Limit: 4}).Resume(first[len(first)-1])
	if !ok {
		t.Fatal("Resume refused to continue")
	}
	second, err := j.EntriesWithIn(ctx, term, r)
	if err != nil {
		t.Fatalf("EntriesWithIn(resumed): %v", err)
	}
	if len(second) != 4 || second[0] != lines[4] {
		t.Fatalf("the resumed batch starts at %s, want %s", second[0], lines[4])
	}
}
