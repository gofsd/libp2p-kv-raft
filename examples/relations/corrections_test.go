package relations_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestCorrectionLeavesTheOriginalStanding is the discipline this whole
// file exists for: correcting line 2 of the shift log writes a new line
// and marks the old one superseded, and does not touch a byte of what
// the old line said.
func TestCorrectionLeavesTheOriginalStanding(t *testing.T) {
	ctx := context.Background()
	st, actor, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)
	wrong := entries[1] // "Day / Ivanova / Lathe-2 / Turning / Scrap / 3"

	before, err := j.Row(ctx, wrong)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}

	right, err := j.Correct(ctx, wrong,
		relations.TermCell(fields[fieldShift], "Day"),
		relations.TermCell(fields[fieldOperator], "Ivanova"),
		relations.TermCell(fields[fieldMachine], "Lathe-2"),
		relations.TermCell(fields[fieldOperation], "Turning"),
		relations.TermCell(fields[fieldResult], "Scrap"),
		relations.QuantityCell(fields[fieldPieces], 5),
	)
	if err != nil {
		t.Fatalf("Correct: %v", err)
	}

	// The original still reads exactly as it was written.
	after, err := j.Row(ctx, wrong)
	if err != nil {
		t.Fatalf("Row after correction: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("the corrected line now has %d columns, want the %d it was written with", len(after), len(before))
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("the corrected line changed: column %s was %+v, now %+v", before[i].FieldName, before[i], after[i])
		}
	}

	// Its standing changed, and only that.
	status, err := j.Status(ctx, wrong)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != relations.StateSuperseded {
		t.Fatalf("status of the corrected line = %s, want superseded", status.State)
	}
	if status.Replacement != right {
		t.Fatalf("superseded by %s, want %s", status.Replacement, right)
	}
	if status.Author != actor {
		t.Fatalf("the strike was recorded by %s, want %s", status.Author, actor)
	}
	if status.At.IsZero() {
		t.Fatal("the strike has no timestamp")
	}
	if live, err := j.Status(ctx, right); err != nil {
		t.Fatalf("Status(replacement): %v", err)
	} else if !live.Live() {
		t.Fatalf("the replacement is already %s", live.State)
	}

	// The strike is attributable the same way every other record is.
	marker, found, err := st.Lookup(ctx, wrong, j.StatusMarker())
	if err != nil || !found {
		t.Fatalf("Lookup(marker) = %v, %v", found, err)
	}
	if err := st.Verify(ctx, marker); err != nil {
		t.Fatalf("Verify(strike): %v", err)
	}

	// The page still shows both lines -- struck-through, then re-entered
	// -- while the book as it now stands shows only the good one.
	page, err := j.Page(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(page) != len(entries)+1 {
		t.Fatalf("page holds %d lines, want %d (nothing is ever removed)", len(page), len(entries)+1)
	}
	live, err := j.LivePage(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("LivePage: %v", err)
	}
	if len(live) != len(entries) {
		t.Fatalf("the live page holds %d lines, want %d", len(live), len(entries))
	}
	for _, line := range live {
		if line == wrong {
			t.Fatal("the superseded line is still shown as current")
		}
	}

	// The correction log, in one scan.
	corrections, err := j.Corrections(ctx)
	if err != nil {
		t.Fatalf("Corrections: %v", err)
	}
	if len(corrections) != 1 || corrections[0].Entry != wrong || corrections[0].Replacement != right {
		t.Fatalf("Corrections = %+v, want the one strike on %s", corrections, wrong)
	}

	// And the replacement reads the way it was meant to.
	fixed, err := j.Row(ctx, right)
	if err != nil {
		t.Fatalf("Row(replacement): %v", err)
	}
	var pieces int64
	for _, cell := range fixed {
		if cell.FieldName == fieldPieces {
			pieces = cell.Number
		}
	}
	if pieces != 5 {
		t.Fatalf("the replacement records %d pieces, want 5", pieces)
	}
}

func TestVoidStrikesALineThrough(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)
	duplicate := entries[2]

	// The reason is a value like any other, so it comes from a
	// dictionary rather than being written out.
	reasonField, err := j.Field(ctx, "void_reason")
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	reason, err := j.Term(ctx, reasonField, "duplicate")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}

	if err := j.Void(ctx, duplicate, reason); err != nil {
		t.Fatalf("Void: %v", err)
	}

	status, err := j.Status(ctx, duplicate)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != relations.StateVoided {
		t.Fatalf("status = %s, want voided", status.State)
	}
	if status.Reason != reason {
		t.Fatalf("reason = %s, want %s", status.Reason, reason)
	}
	if !status.Replacement.IsZero() {
		t.Fatalf("a voided line names %s as its replacement, want none", status.Replacement)
	}

	// Still readable, still on the page, just no longer standing.
	if _, err := j.Row(ctx, duplicate); err != nil {
		t.Fatalf("Row(voided): %v", err)
	}
	live, err := j.LivePage(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("LivePage: %v", err)
	}
	if len(live) != len(entries)-1 {
		t.Fatalf("live page holds %d lines, want %d", len(live), len(entries)-1)
	}
	chain, err := j.History(ctx, duplicate)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(chain) != 1 || chain[0] != duplicate {
		t.Fatalf("History(voided) = %v, want just the line itself", chain)
	}

	// Voiding without a reason is allowed; voiding with something that
	// is not a declared term is not.
	if err := j.Void(ctx, entries[0], relations.Zero); err != nil {
		t.Fatalf("Void without a reason: %v", err)
	}
	if err := j.Void(ctx, entries[1], fields[fieldShift]); err == nil {
		t.Fatal("expected an error voiding with a column as the reason")
	}
	undeclared := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeTerm, ID: 0xFE}
	if err := j.Void(ctx, entries[1], undeclared); err == nil {
		t.Fatal("expected an error voiding with an undeclared reason")
	}
}

func TestStrikingIsOnceOnly(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)
	line := entries[0]

	if _, err := j.Correct(ctx, line, relations.TermCell(fields[fieldResult], "OK")); err != nil {
		t.Fatalf("Correct: %v", err)
	}
	if _, err := j.Correct(ctx, line, relations.TermCell(fields[fieldResult], "Scrap")); !errors.Is(err, relations.ErrAlreadyStruck) {
		t.Fatalf("second Correct = %v, want ErrAlreadyStruck", err)
	}
	if err := j.Void(ctx, line, relations.Zero); !errors.Is(err, relations.ErrAlreadyStruck) {
		t.Fatalf("Void after Correct = %v, want ErrAlreadyStruck", err)
	}

	// The refused strikes wrote nothing: one correction, one replacement.
	corrections, err := j.Corrections(ctx)
	if err != nil {
		t.Fatalf("Corrections: %v", err)
	}
	if len(corrections) != 1 {
		t.Fatalf("%d strikes recorded, want 1", len(corrections))
	}

	// Neither is a line something else already is.
	if err := j.Void(ctx, fields[fieldShift], relations.Zero); err == nil {
		t.Fatal("expected an error voiding a column")
	}
	unwritten := relations.Entity{Log: testLog, Page: relations.FirstEntryPage, Type: relations.TypeEntry, ID: 0xFE}
	if _, err := j.Correct(ctx, unwritten, relations.TermCell(fields[fieldResult], "OK")); err == nil {
		t.Fatal("expected an error correcting a line that was never written")
	}
	if err := j.Void(ctx, j.StatusMarker(), relations.Zero); err == nil {
		t.Fatal("expected an error striking the status marker itself")
	}
	_ = st
}

func TestHistoryFollowsTheWholeChain(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)
	first := entries[0]

	second, err := j.Correct(ctx, first, relations.QuantityCell(fields[fieldPieces], 121))
	if err != nil {
		t.Fatalf("Correct: %v", err)
	}
	third, err := j.Correct(ctx, second, relations.QuantityCell(fields[fieldPieces], 122))
	if err != nil {
		t.Fatalf("Correct again: %v", err)
	}

	want := []relations.Entity{first, second, third}
	for _, from := range want {
		chain, err := j.History(ctx, from)
		if err != nil {
			t.Fatalf("History(%s): %v", from, err)
		}
		if len(chain) != len(want) {
			t.Fatalf("History(%s) = %v, want %v", from, chain, want)
		}
		for i := range want {
			if chain[i] != want[i] {
				t.Fatalf("History(%s) = %v, want %v", from, chain, want)
			}
		}
		latest, err := j.Latest(ctx, from)
		if err != nil {
			t.Fatalf("Latest(%s): %v", from, err)
		}
		if latest != third {
			t.Fatalf("Latest(%s) = %s, want %s", from, latest, third)
		}
	}

	// Every version is still on the page and still readable; only the
	// last one stands.
	live, err := j.LivePage(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("LivePage: %v", err)
	}
	for _, line := range live {
		if line == first || line == second {
			t.Fatalf("superseded line %s is still shown as current", line)
		}
	}
	if _, err := j.Row(ctx, first); err != nil {
		t.Fatalf("Row(oldest): %v", err)
	}

	// Two strikes, both attributable.
	corrections, err := j.Corrections(ctx)
	if err != nil {
		t.Fatalf("Corrections: %v", err)
	}
	if len(corrections) != 2 {
		t.Fatalf("%d strikes recorded, want 2", len(corrections))
	}
	for _, c := range corrections {
		if c.State != relations.StateSuperseded || c.Replacement.IsZero() {
			t.Fatalf("strike %+v is not a supersession", c)
		}
	}
	_ = st
}

// TestCorrectionRaceStrikesOnce drives two writers correcting the same
// line at the same moment. Exactly one correction may stand: a line with
// two replacements is a forked record, which is the thing a compare
// precondition on the marker exists to prevent.
func TestCorrectionRaceStrikesOnce(t *testing.T) {
	ctx := context.Background()
	inner := relations.Memory()
	be := &racingBackend{Backend: inner}
	pub, priv := newKey(t)
	st, actor, _ := newStoreOn(t, be, pub, priv)

	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)
	line := entries[0]

	var winner relations.Entity
	be.target = relations.RelationKey(line, j.StatusMarker())
	be.competes = func() {
		w := relations.NewJournal(relations.New(inner, testLog, actor, priv))
		field, err := w.Field(ctx, fieldResult)
		if err != nil {
			t.Errorf("competing Field: %v", err)
			return
		}
		winner, err = w.Correct(ctx, line, relations.TermCell(field, "OK"))
		if err != nil {
			t.Errorf("competing Correct: %v", err)
		}
	}

	_, err := j.Correct(ctx, line, relations.TermCell(fields[fieldResult], "Scrap"))
	if !errors.Is(err, relations.ErrAlreadyStruck) {
		t.Fatalf("losing Correct = %v, want ErrAlreadyStruck", err)
	}
	if winner.IsZero() {
		t.Fatal("the competing writer never ran -- the race was not exercised")
	}

	status, err := j.Status(ctx, line)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Replacement != winner {
		t.Fatalf("the line is superseded by %s, want the winner's %s", status.Replacement, winner)
	}
	corrections, err := j.Corrections(ctx)
	if err != nil {
		t.Fatalf("Corrections: %v", err)
	}
	if len(corrections) != 1 {
		t.Fatalf("%d strikes recorded, want 1", len(corrections))
	}

	// The loser's line was never written: the book holds the three
	// original lines plus the winner's correction, and nothing else.
	page, err := j.Page(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(page) != len(entries)+1 {
		t.Fatalf("page holds %d lines, want %d (a refused correction must leave nothing behind)", len(page), len(entries)+1)
	}
}
