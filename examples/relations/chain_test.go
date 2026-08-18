package relations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestChainVerifiesAWholeBook is the ordinary case: a book that has been
// written, corrected and struck through in the normal way verifies end
// to end.
func TestChainVerifiesAWholeBook(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	checked, err := j.VerifyChain(ctx)
	if err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
	if checked != len(entries) {
		t.Fatalf("verified %d events, want %d (one per line written)", checked, len(entries))
	}

	// A correction is two events: the replacement line, and the strike
	// it puts on the line it replaces. The strike must not disturb the
	// digest of the line it lands on -- a line's standing is written
	// after its own digest is fixed, so it is chained as its own event
	// instead.
	if _, err := j.Correct(ctx, entries[1], relations.QuantityCell(fields[fieldPieces], 5)); err != nil {
		t.Fatalf("Correct: %v", err)
	}
	checked, err = j.VerifyChain(ctx)
	if err != nil {
		t.Fatalf("VerifyChain after a correction: %v", err)
	}
	if checked != len(entries)+2 {
		t.Fatalf("verified %d events, want %d (a correction is a line and a strike)", checked, len(entries)+2)
	}

	// A strike with no replacement is one event.
	if err := j.Void(ctx, entries[2], relations.Zero); err != nil {
		t.Fatalf("Void: %v", err)
	}
	checked, err = j.VerifyChain(ctx)
	if err != nil {
		t.Fatalf("VerifyChain after a void: %v", err)
	}
	if checked != len(entries)+3 {
		t.Fatalf("verified %d events, want %d", checked, len(entries)+3)
	}

	// The event stream reads back in order, one event per change, each
	// with a digest of its own.
	events, err := j.Events(ctx, relations.Range{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != len(entries)+3 {
		t.Fatalf("the stream holds %d events, want %d", len(events), len(entries)+3)
	}
	seen := make(map[string]bool)
	for i, event := range events {
		if event.Seq != uint64(i)+1 {
			t.Fatalf("event %d is numbered %d", i+1, event.Seq)
		}
		if len(event.Digest) != relations.ChainDigestLen {
			t.Fatalf("event %d carries a %d-byte digest, want %d", event.Seq, len(event.Digest), relations.ChainDigestLen)
		}
		if seen[string(event.Digest)] {
			t.Fatalf("event %d repeats an earlier digest", event.Seq)
		}
		seen[string(event.Digest)] = true
	}
	if events[0].Kind() != "line" || events[3].Kind() != "line" ||
		events[4].Kind() != "strike" || events[5].Kind() != "strike" {
		t.Fatalf("event kinds are wrong: want three lines, then the correction's line and strike, then the void")
	}
}

// TestChainCatchesARemovedCell is the tamper a signature alone cannot
// catch: nothing is edited, so every surviving record still verifies --
// one is simply gone.
func TestChainCatchesARemovedCell(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	// Remove the "Scrap" cell from line 2, both directions, exactly as
	// the store itself would.
	scrap, err := j.Term(ctx, fields[fieldResult], "Scrap")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	if err := st.Unlink(ctx, entries[1], scrap); err != nil {
		t.Fatalf("Unlink: %v", err)
	}

	// The line still reads, and everything left in it still verifies:
	// this is precisely what per-record signing cannot see.
	row, err := j.Row(ctx, entries[1])
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	for _, cell := range row {
		if cell.Text == "Scrap" {
			t.Fatal("the cell was not actually removed")
		}
	}

	checked, err := j.VerifyChain(ctx)
	if err == nil {
		t.Fatal("VerifyChain accepted a book with a line's cell removed")
	}
	if checked != 1 {
		t.Fatalf("VerifyChain stopped after %d events, want 1 (the break is on line 2)", checked)
	}
	if !strings.Contains(err.Error(), entries[1].String()) {
		t.Fatalf("VerifyChain error does not name the broken line %s: %v", entries[1], err)
	}
}

// TestChainCatchesARemovedLine covers the two shapes of a whole line
// disappearing: from the middle, where the following digest breaks, and
// from the end, where there is no following digest and only the
// allocator's own record says the line was ever handed out.
func TestChainCatchesARemovedLine(t *testing.T) {
	ctx := context.Background()

	t.Run("from the middle", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		entries, _ := writeShiftLog(t, j)
		deleteLine(t, st, j, entries[1])

		_, err := j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a book with a line torn out of the middle")
		}
		if !strings.Contains(err.Error(), entries[1].String()) {
			t.Fatalf("error does not name the missing line %s: %v", entries[1], err)
		}
	})

	t.Run("from the end", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		entries, _ := writeShiftLog(t, j)
		last := entries[len(entries)-1]
		deleteLine(t, st, j, last)

		_, err := j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a book with its last line torn out")
		}
		if !strings.Contains(err.Error(), last.String()) {
			t.Fatalf("error does not name the missing line %s: %v", last, err)
		}
	})
}

// TestChainCatchesAnUnallocatedLine covers the other direction: a line
// spliced in that the allocator never handed out an id for.
func TestChainCatchesAnUnallocatedLine(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	writeShiftLog(t, j)

	// A well-formed link payload -- digest, tag, subject, party -- for a
	// line the allocator never handed out, spliced in at the next free
	// sequence number.
	forged := relations.Entity{Log: testLog, Page: relations.FirstEntryPage, Type: relations.TypeEntry, ID: 0x40}
	payload := make([]byte, relations.ChainDigestLen)
	payload = append(payload, 1) // the line tag
	subject := forged.Bytes()
	payload = append(payload, subject[:]...)
	payload = append(payload, 0, 0, 0, 0)
	if err := st.Link(ctx, j.EventAnchor(), relations.EntityFromValue(4), relations.KindChainLink, payload); err != nil {
		t.Fatalf("Link: %v", err)
	}

	_, err := j.VerifyChain(ctx)
	if err == nil {
		t.Fatal("VerifyChain accepted a chained line the allocator never handed out")
	}
	if !strings.Contains(err.Error(), forged.String()) {
		t.Fatalf("error does not name the spliced line %s: %v", forged, err)
	}
}

// TestChainCatchesAnEditedRecordBeforeItsDigest checks the two defences
// compose: a record rewritten by somebody without the signing key is
// reported as the forgery it is, rather than as a bare digest mismatch.
func TestChainCatchesAnEditedRecordBeforeItsDigest(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, _ := writeShiftLog(t, j)

	decl, found, err := st.Declaration(ctx, entries[1])
	if err != nil || !found {
		t.Fatalf("Declaration = %v, %v", found, err)
	}
	forged, err := relations.Record{
		Kind:    decl.Record.Kind,
		Author:  decl.Record.Author,
		Created: decl.Record.Created,
		Name:    "tampered",
	}.Encode(relations.DeclarationKey(entries[1]), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := st.Backend().Apply(ctx, []relations.Op{{
		Kind:  relations.OpSet,
		Key:   relations.DeclarationKey(entries[1]),
		Value: forged,
	}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	_, err = j.VerifyChain(ctx)
	if err == nil {
		t.Fatal("VerifyChain accepted a forged declaration")
	}
	if !strings.Contains(err.Error(), "unsigned") {
		t.Fatalf("error does not report the missing signature: %v", err)
	}
}

// TestChainSpansPages checks the chain does not restart at a page
// boundary: line 1 of page 2 chains onto the last line of page 1, so a
// whole page cannot be removed without breaking the page after it.
func TestChainSpansPages(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	field, err := j.Field(ctx, fieldResult)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	ok, err := j.Term(ctx, field, "OK")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}

	// Fill page 1 to its last line, then write one more so it rolls.
	var lines []relations.Entity
	for i := 0; i < 256; i++ {
		entry, err := j.Append(ctx, relations.TermCell(field, "OK"))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		lines = append(lines, entry)
	}
	rolled := lines[len(lines)-1]
	if rolled.Page != relations.FirstEntryPage+1 || rolled.ID != 1 {
		t.Fatalf("line 256 landed at %s, want the first line of the next page", rolled)
	}

	checked, err := j.VerifyChain(ctx)
	if err != nil {
		t.Fatalf("VerifyChain across a page boundary: %v", err)
	}
	if checked != len(lines) {
		t.Fatalf("verified %d events, want %d", checked, len(lines))
	}

	// Breaking the last line of page 1 must break the first line of
	// page 2, which is the whole point of not restarting the chain per
	// page.
	if err := st.Unlink(ctx, lines[254], ok); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	checked, err = j.VerifyChain(ctx)
	if err == nil {
		t.Fatal("VerifyChain accepted a book with the last line of a page altered")
	}
	if checked != 254 {
		t.Fatalf("VerifyChain stopped after %d events, want 254", checked)
	}
}

// deleteLine removes every record a line consists of -- its declaration,
// its cells and its chain link, in both namespaces -- the way somebody
// with write access to the raw store would.
func deleteLine(t *testing.T, st *relations.Store, j *relations.Journal, entry relations.Entity) {
	t.Helper()
	ctx := context.Background()

	rels, err := st.Relations(ctx, entry)
	if err != nil {
		t.Fatalf("Relations: %v", err)
	}
	ops := []relations.Op{{Kind: relations.OpDelete, Key: relations.DeclarationKey(entry)}}
	for _, rel := range rels {
		ops = append(ops,
			relations.Op{Kind: relations.OpDelete, Key: relations.RelationKey(rel.A, rel.B)},
			relations.Op{Kind: relations.OpDelete, Key: relations.IndexKey(rel.A, rel.B)})
	}
	if err := st.Backend().Apply(ctx, ops); err != nil {
		t.Fatalf("delete line %s: %v", entry, err)
	}
	if _, found, err := st.Declaration(ctx, entry); err != nil {
		t.Fatalf("Declaration: %v", err)
	} else if found {
		t.Fatalf("line %s survived deletion", entry)
	}
}

// deleteEvent removes one event from the chain -- what a forger has to
// do after removing whatever the event recorded.
func deleteEvent(t *testing.T, st *relations.Store, j *relations.Journal, seq uint32) {
	t.Helper()
	if err := st.Unlink(context.Background(), j.EventAnchor(), relations.EntityFromValue(seq)); err != nil {
		t.Fatalf("delete event %d: %v", seq, err)
	}
}

// TestChainCatchesARemovedStrike is what chaining the strikes buys, and
// the reason the chain keeps a running head rather than deriving each
// line's predecessor from its id. A strike has no line number of its
// own, so a per-line chain could never cover one -- and a voided line
// whose status record is quietly dropped reads as live again, which is
// exactly the kind of erasure a log book exists to prevent.
func TestChainCatchesARemovedStrike(t *testing.T) {
	ctx := context.Background()

	t.Run("status record alone", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		entries, _ := writeShiftLog(t, j)
		if err := j.Void(ctx, entries[1], relations.Zero); err != nil {
			t.Fatalf("Void: %v", err)
		}
		if err := st.Unlink(ctx, entries[1], j.StatusMarker()); err != nil {
			t.Fatalf("Unlink: %v", err)
		}

		// The line reads as live again -- the tamper worked, as far as
		// the store itself is concerned.
		status, err := j.Status(ctx, entries[1])
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if !status.Live() {
			t.Fatal("the status record was not actually removed")
		}

		_, err = j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a book with a strike removed")
		}
		if !strings.Contains(err.Error(), entries[1].String()) {
			t.Fatalf("error does not name the line whose strike went missing %s: %v", entries[1], err)
		}
	})

	t.Run("strike and its chain link together", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		entries, _ := writeShiftLog(t, j)
		if err := j.Void(ctx, entries[1], relations.Zero); err != nil {
			t.Fatalf("Void: %v", err)
		}
		// A thorough forger removes the evidence of the evidence too:
		// the strike is event 4, after the three lines.
		if err := st.Unlink(ctx, entries[1], j.StatusMarker()); err != nil {
			t.Fatalf("Unlink(status): %v", err)
		}
		deleteEvent(t, st, j, 4)

		// Nothing is left to break a digest: the surviving events are a
		// contiguous, correctly chained prefix. Only the head still says
		// how many events there should have been.
		_, err := j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a book with a strike and its chain link both removed")
		}
		if !strings.Contains(err.Error(), "head counts") {
			t.Fatalf("error does not come from the head's event count: %v", err)
		}
	})

	t.Run("the strike half of a correction", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		entries, fields := writeShiftLog(t, j)
		if _, err := j.Correct(ctx, entries[0], relations.QuantityCell(fields[fieldPieces], 121)); err != nil {
			t.Fatalf("Correct: %v", err)
		}
		if err := st.Unlink(ctx, entries[0], j.StatusMarker()); err != nil {
			t.Fatalf("Unlink: %v", err)
		}

		_, err := j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a correction whose strike was removed")
		}
	})
}

// TestChainCatchesAForgedHead checks the head is not a free pass: it is
// signed like every other record, and rewriting it to match a shortened
// chain needs the key.
func TestChainCatchesAForgedHead(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, _ := writeShiftLog(t, j)

	head, found, err := st.Declaration(ctx, j.ChainHead())
	if err != nil || !found {
		t.Fatalf("Declaration(head) = %v, %v", found, err)
	}
	if err := st.Verify(ctx, head); err != nil {
		t.Fatalf("Verify(head): %v", err)
	}

	// Drop the last line, then rewrite the head to claim the book only
	// ever had two events -- the tamper a bare event count would fall for.
	deleteLine(t, st, j, entries[len(entries)-1])
	shortened := append([]byte(nil), head.Record.Data...)
	shortened[len(shortened)-1] = byte(len(entries) - 1)
	forged, err := relations.Record{
		Kind:    head.Record.Kind,
		Author:  head.Record.Author,
		Created: head.Record.Created,
		Data:    shortened,
	}.Encode(relations.DeclarationKey(j.ChainHead()), nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := st.Backend().Apply(ctx, []relations.Op{{
		Kind:  relations.OpSet,
		Key:   relations.DeclarationKey(j.ChainHead()),
		Value: forged,
	}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if _, err := j.VerifyChain(ctx); err == nil {
		t.Fatal("VerifyChain accepted a book whose head was rewritten to hide a missing line")
	}
	head, found, err = st.Declaration(ctx, j.ChainHead())
	if err != nil || !found {
		t.Fatalf("Declaration(head) = %v, %v", found, err)
	}
	if err := st.Verify(ctx, head); err == nil {
		t.Fatal("the rewritten head still verifies")
	}
}
