package relations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

const fieldRemarks = "remarks"

// TestRemarkRoundTrip writes the thing no vocabulary could have
// anticipated, and reads it back off the line.
func TestRemarkRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	result, err := j.Field(ctx, fieldResult)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	remarks, err := j.DefineField(ctx, fieldRemarks, relations.InputText)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}

	const note = "bearing sounded rough on the third pass; flagged to maintenance"
	entry, err := j.Append(ctx,
		relations.TermCell(result, "OK"),
		relations.RemarkCell(remarks, note),
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	row, err := j.Row(ctx, entry)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	var got relations.RowCell
	for _, cell := range row {
		if cell.FieldName == fieldRemarks {
			got = cell
		}
	}
	if !got.Free {
		t.Fatalf("the remarks column came back as %+v, want a free-text cell", got)
	}
	if got.Text != note {
		t.Fatalf("remark = %q, want %q", got.Text, note)
	}
	if !got.Term.IsZero() {
		t.Fatalf("remark names term %s; free text has no dictionary entry behind it", got.Term)
	}

	// It is signed with the line and covered by the line's digest, like
	// any other cell.
	rel, found, err := st.Lookup(ctx, entry, remarks)
	if err != nil || !found {
		t.Fatalf("Lookup(remark) = %v, %v", found, err)
	}
	if err := st.Verify(ctx, rel); err != nil {
		t.Fatalf("Verify(remark): %v", err)
	}
	if checked, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	} else if checked != 1 {
		t.Fatalf("verified %d events, want 1", checked)
	}
}

// TestRemarkIsCoveredByTheChain is why the text goes on the relation
// rather than into a record of its own off to one side: deleting a
// remark is deleting part of the line, and the line's digest says so.
func TestRemarkIsCoveredByTheChain(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	result, err := j.Field(ctx, fieldResult)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	remarks, err := j.DefineField(ctx, fieldRemarks, relations.InputText)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	if _, err := j.Append(ctx, relations.TermCell(result, "OK")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	entry, err := j.Append(ctx,
		relations.TermCell(result, "Scrap"),
		relations.RemarkCell(remarks, "operator disputes the reading"),
	)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := st.Unlink(ctx, entry, remarks); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	checked, err := j.VerifyChain(ctx)
	if err == nil {
		t.Fatal("VerifyChain accepted a line whose remark was deleted")
	}
	if checked != 1 {
		t.Fatalf("VerifyChain stopped after %d events, want 1 (the break is on line 2)", checked)
	}
	if !strings.Contains(err.Error(), entry.String()) {
		t.Fatalf("error does not name the line whose remark went missing %s: %v", entry, err)
	}
}

// TestRemarksAreNotADictionary states the exception plainly, because it
// is the one place this package stores the same text twice on purpose.
func TestRemarksAreNotADictionary(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	result, err := j.Field(ctx, fieldResult)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	remarks, err := j.DefineField(ctx, fieldRemarks, relations.InputText)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}

	const note = "same words on two lines"
	for i := 0; i < 2; i++ {
		if _, err := j.Append(ctx,
			relations.TermCell(result, "OK"),
			relations.RemarkCell(remarks, note),
		); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Twice on the wire, once per line -- and not as a name, so the
	// dictionary's own "stored exactly once" audit is untouched.
	stored := 0
	for _, p := range scanAll(t, st) {
		rec, _, err := relations.DecodeRecord(p.Value)
		if err != nil {
			t.Fatalf("DecodeRecord: %v", err)
		}
		if rec.Kind == relations.KindRemark && string(rec.Data) == note {
			stored++
		}
		if rec.Name == note {
			t.Fatal("a remark was stored as a declaration name; free text is not interned")
		}
	}
	// Two lines, and each remark is written twice -- once forward, once
	// in the index that makes "which lines mention this column" a scan.
	if stored != 4 {
		t.Fatalf("the remark is stored in %d records, want 4 (two lines, both directions)", stored)
	}

	// A vocabulary value in the same log is still interned exactly once,
	// so the exception really is confined to remarks.
	if n := countRecordsNamed(t, scanAll(t, st), "OK"); n != 1 {
		t.Fatalf("%q is stored in %d records, want exactly 1", "OK", n)
	}
}

func TestAppendRejectsAMalformedRemark(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	remarks, err := j.DefineField(ctx, fieldRemarks, relations.InputText)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	if _, err := j.Append(ctx, relations.RemarkCell(remarks, "")); err == nil {
		t.Fatal("expected an error appending an empty remark")
	}
	both := relations.RemarkCell(remarks, "text")
	both.Numeric = true
	if _, err := j.Append(ctx, both); err == nil {
		t.Fatal("expected an error appending a cell that is both a number and free text")
	}
}
