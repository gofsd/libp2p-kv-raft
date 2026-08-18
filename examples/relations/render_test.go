package relations_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestRenderPageReadsLikeThePage is the last thing a log book has to be
// able to do: be handed to somebody. The whole shift -- a correction, a
// strike, a remark, a countersignature and the sign-off at the foot --
// has to be legible without reading any of the code that wrote it.
func TestRenderPageReadsLikeThePage(t *testing.T) {
	ctx := context.Background()
	be := relations.Memory()
	operatorStore, _ := newActor(t, be, 1, "Ivanova")
	supervisorStore, _ := newActor(t, be, 2, "Petrov")

	// A fixed clock, so the rendered timestamps are the same on every
	// run and the test can assert on them.
	clock := time.Date(2026, 8, 18, 8, 12, 0, 0, time.UTC)
	operatorStore.Now = func() time.Time { return clock }
	supervisorStore.Now = func() time.Time { return clock.Add(9 * time.Hour) }

	j := relations.NewJournal(operatorStore)
	entries, fields := writeShiftLog(t, j)
	remarks, err := j.DefineField(ctx, fieldRemarks, relations.InputText)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}

	// Line 2 was wrong: correct it, with a remark saying why.
	if _, err := j.Correct(ctx, entries[1],
		relations.TermCell(fields[fieldShift], "Day"),
		relations.TermCell(fields[fieldOperator], "Ivanova"),
		relations.TermCell(fields[fieldMachine], "Lathe-2"),
		relations.TermCell(fields[fieldOperation], "Turning"),
		relations.TermCell(fields[fieldResult], "Scrap"),
		relations.QuantityCell(fields[fieldPieces], 5),
		relations.RemarkCell(remarks, "recount after the shift"),
	); err != nil {
		t.Fatalf("Correct: %v", err)
	}
	// Line 3 should never have been written at all.
	reasonField, err := j.Field(ctx, "void_reason")
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	reason, err := j.Term(ctx, reasonField, "duplicate")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	if err := j.Void(ctx, entries[2], reason); err != nil {
		t.Fatalf("Void: %v", err)
	}
	// The supervisor endorses line 1 and closes the page.
	supervisor := relations.NewJournal(supervisorStore)
	if err := supervisor.Countersign(ctx, entries[0]); err != nil {
		t.Fatalf("Countersign: %v", err)
	}
	if err := supervisor.SignOffPage(ctx, relations.FirstEntryPage); err != nil {
		t.Fatalf("SignOffPage: %v", err)
	}

	page, err := j.RenderPage(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("RenderPage: %v", err)
	}
	t.Logf("the page, as an inspector would be handed it:\n\n%s", page)

	for _, want := range []string{
		"log 1, page 1 -- 4 lines",
		"shift", "operator", "machine", "operation", "result", "pieces", "remarks",
		"signed", "at",
		"Ivanova", "Lathe-2", "Turning",
		"2026-08-18 08:12",
		"(2)",                     // the superseded line, ruled out but still shown
		"superseded by line 4",    // and what replaced it
		"(3)",                     // the voided line
		"voided (duplicate)",      // with the reason, from the dictionary
		"replaces line 2",         // read from the correction's side
		"countersigned by Petrov", // the second signature
		"recount after the shift", // the remark
		"page closed by Petrov at 2026-08-18 17:12, 4 lines",
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("the rendered page does not contain %q:\n%s", want, page)
		}
	}

	// The columns line up: every row has the same number of separators
	// as the heading row, and the rule under the heading is as wide as
	// the heading itself.
	lines := strings.Split(strings.TrimRight(page, "\n"), "\n")
	heading, rule := lines[2], lines[3]
	if len(rule) != len(heading) {
		t.Fatalf("the rule is %d wide and the heading %d:\n%s", len(rule), len(heading), page)
	}
	bars := strings.Count(heading, "|")
	for _, row := range lines[4:8] {
		if got := strings.Count(row, "|"); got != bars {
			t.Fatalf("row %q has %d column separators, heading has %d", row, got, bars)
		}
	}

	// Quantities are right-aligned, so a column of numbers reads as one.
	columns := strings.Split(heading, "|")
	pieces := -1
	for i, column := range columns {
		if strings.TrimSpace(column) == fieldPieces {
			pieces = i
		}
	}
	if pieces < 0 {
		t.Fatalf("no pieces column in the heading %q", heading)
	}
	cell := strings.Split(lines[4], "|")[pieces]
	if !strings.HasPrefix(cell, "  ") || strings.HasSuffix(cell, "  ") {
		t.Fatalf("the pieces cell %q is not right-aligned under a %q heading:\n%s", cell, columns[pieces], page)
	}
}

func TestRenderAnOpenAndAnEmptyPage(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	empty, err := j.RenderPage(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("RenderPage: %v", err)
	}
	if !strings.Contains(empty, "(no lines)") || !strings.Contains(empty, "page open") {
		t.Fatalf("an empty page renders as:\n%s", empty)
	}

	writeShiftLog(t, j)
	open, err := j.RenderPage(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("RenderPage: %v", err)
	}
	if !strings.Contains(open, "page open") {
		t.Fatalf("a page nobody closed does not say so:\n%s", open)
	}
}

// TestRenderBookEndsWithTheChainsWord is the difference between a
// printout and a record: the book says, at the foot, whether it still
// adds up.
func TestRenderBookEndsWithTheChainsWord(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	st.Now = func() time.Time { return time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC) }
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	book, err := j.RenderBook(ctx)
	if err != nil {
		t.Fatalf("RenderBook: %v", err)
	}
	if !strings.Contains(book, "chain: 3 events, verified 2026-08-18 12:00") {
		t.Fatalf("the book does not end with a verified chain:\n%s", book)
	}

	// Two pages render in order.
	if err := j.SignOffPage(ctx, relations.FirstEntryPage); err != nil {
		t.Fatalf("SignOffPage: %v", err)
	}
	if _, err := j.Append(ctx, relations.TermCell(fields[fieldResult], "OK")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	book, err = j.RenderBook(ctx)
	if err != nil {
		t.Fatalf("RenderBook: %v", err)
	}
	if strings.Index(book, "page 1") > strings.Index(book, "page 2") {
		t.Fatalf("the pages are out of order:\n%s", book)
	}

	// And when it does not add up, it says that instead -- loudly. The
	// tamper is the cheap one: remove a cell from a line, leaving every
	// surviving record perfectly signed.
	ok, err := j.Term(ctx, fields[fieldResult], "OK")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	if err := st.Unlink(ctx, entries[0], ok); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	book, err = j.RenderBook(ctx)
	if err != nil {
		t.Fatalf("RenderBook: %v", err)
	}
	if !strings.Contains(book, "chain: BROKEN") {
		t.Fatalf("a tampered book renders as sound:\n%s", book)
	}
	t.Logf("a tampered book:\n\n%s", book)
}
