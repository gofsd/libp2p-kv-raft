package relations_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestSignOffClosesThePage is the foot-of-the-page signature: who closed
// the page, when, how many lines it held -- and no more lines on it
// afterwards.
func TestSignOffClosesThePage(t *testing.T) {
	ctx := context.Background()
	st, actor, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	if _, found, err := j.PageStatus(ctx, relations.FirstEntryPage); err != nil {
		t.Fatalf("PageStatus: %v", err)
	} else if found {
		t.Fatal("a page nobody signed off reports a sign-off")
	}

	if err := j.SignOffPage(ctx, relations.FirstEntryPage); err != nil {
		t.Fatalf("SignOffPage: %v", err)
	}

	signoff, found, err := j.PageStatus(ctx, relations.FirstEntryPage)
	if err != nil || !found {
		t.Fatalf("PageStatus = %v, %v", found, err)
	}
	if signoff.By != actor || signoff.Name != "Ivanov" {
		t.Fatalf("page closed by %s (%q), want %s (Ivanov)", signoff.By, signoff.Name, actor)
	}
	if signoff.Lines != uint8(len(entries)) {
		t.Fatalf("the sign-off records %d lines, want %d", signoff.Lines, len(entries))
	}
	if signoff.At.IsZero() {
		t.Fatal("the sign-off has no timestamp")
	}

	// The next line goes on the next page -- and not because this layer
	// checks, but because the allocator now finds the page closed.
	next, err := j.Append(ctx, relations.TermCell(fields[fieldResult], "OK"))
	if err != nil {
		t.Fatalf("Append after sign-off: %v", err)
	}
	if next.Page != relations.FirstEntryPage+1 || next.ID != 1 {
		t.Fatalf("the line after a sign-off landed at %s, want the first line of the next page", next)
	}
	page, err := j.Page(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(page) != len(entries) {
		t.Fatalf("the closed page holds %d lines, want the %d it was closed with", len(page), len(entries))
	}

	// A correction to a line on the closed page is still possible -- it
	// is a new line, and new lines go where new lines go.
	replacement, err := j.Correct(ctx, entries[0], relations.TermCell(fields[fieldResult], "Scrap"))
	if err != nil {
		t.Fatalf("Correct: %v", err)
	}
	if replacement.Page != relations.FirstEntryPage+1 {
		t.Fatalf("the correction landed at %s, want the open page", replacement)
	}

	// Ruling a page off is once-only, like striking a line.
	if err := j.SignOffPage(ctx, relations.FirstEntryPage); !errors.Is(err, relations.ErrPageSignedOff) {
		t.Fatalf("second sign-off = %v, want ErrPageSignedOff", err)
	}
	if err := j.SignOffPage(ctx, relations.SchemaPage); err == nil {
		t.Fatal("expected an error signing off the schema page")
	}

	closed, err := j.SignedOffPages(ctx)
	if err != nil {
		t.Fatalf("SignedOffPages: %v", err)
	}
	if len(closed) != 1 || closed[0].Page != relations.FirstEntryPage {
		t.Fatalf("SignedOffPages = %+v, want just page %d", closed, relations.FirstEntryPage)
	}

	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
}

// TestSignOffIsChained checks a sign-off cannot be quietly removed --
// which, if it could, would reopen a closed page and let a line be
// written into a shift that was already closed out.
func TestSignOffIsChained(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, _ := writeShiftLog(t, j)

	if err := j.SignOffPage(ctx, relations.FirstEntryPage); err != nil {
		t.Fatalf("SignOffPage: %v", err)
	}
	events, err := j.Events(ctx, relations.Range{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != len(entries)+1 {
		t.Fatalf("the stream holds %d events, want %d", len(events), len(entries)+1)
	}
	last := events[len(events)-1]
	if last.Kind() != "signoff" || last.Subject != j.PageEntity(relations.FirstEntryPage) {
		t.Fatalf("the last event is a %s on %s, want a signoff on page %d",
			last.Kind(), last.Subject, relations.FirstEntryPage)
	}

	if err := st.Unlink(ctx, j.PageEntity(relations.FirstEntryPage), j.StatusMarker()); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	if _, found, err := j.PageStatus(ctx, relations.FirstEntryPage); err != nil {
		t.Fatalf("PageStatus: %v", err)
	} else if found {
		t.Fatal("the sign-off was not actually removed")
	}

	_, err = j.VerifyChain(ctx)
	if err == nil {
		t.Fatal("VerifyChain accepted a book with a page sign-off removed")
	}
	if !strings.Contains(err.Error(), "signoff") {
		t.Fatalf("error does not say what went missing: %v", err)
	}
}

// TestSignOffAnEmptyPage covers the case a shift that produced no work
// still has to close: the page had no lines, so it has no counter until
// the sign-off makes one.
func TestSignOffAnEmptyPage(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	field, err := j.Field(ctx, fieldResult)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	if err := j.SignOffPage(ctx, relations.FirstEntryPage); err != nil {
		t.Fatalf("SignOffPage: %v", err)
	}
	signoff, found, err := j.PageStatus(ctx, relations.FirstEntryPage)
	if err != nil || !found {
		t.Fatalf("PageStatus = %v, %v", found, err)
	}
	if signoff.Lines != 0 {
		t.Fatalf("the sign-off records %d lines on an empty page", signoff.Lines)
	}

	entry, err := j.Append(ctx, relations.TermCell(field, "OK"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if entry.Page != relations.FirstEntryPage+1 {
		t.Fatalf("the first line landed at %s, want the page after the closed one", entry)
	}
	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
}
