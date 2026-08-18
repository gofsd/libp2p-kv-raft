package relations_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestClosingAVocabularyMakesItASet is the difference between a column
// whose values happen to be shared and a column with a fixed set of
// admissible entries.
func TestClosingAVocabularyMakesItASet(t *testing.T) {
	ctx := context.Background()
	st, actor, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)
	machine := fields[fieldMachine]

	if closed, err := j.FieldIsClosed(ctx, machine); err != nil {
		t.Fatalf("FieldIsClosed: %v", err)
	} else if closed {
		t.Fatal("a column nobody closed reports itself closed")
	}

	if err := j.CloseField(ctx, machine); err != nil {
		t.Fatalf("CloseField: %v", err)
	}
	if closed, err := j.FieldIsClosed(ctx, machine); err != nil {
		t.Fatalf("FieldIsClosed: %v", err)
	} else if !closed {
		t.Fatal("the column did not close")
	}

	// The words it already has go on working, in a fresh journal with no
	// cache to help it.
	cold := relations.NewJournal(st)
	existing, err := cold.Term(ctx, machine, "Lathe-2")
	if err != nil {
		t.Fatalf("Term(existing value on a closed column): %v", err)
	}
	if _, err := cold.Append(ctx, relations.TermCell(machine, "Lathe-2")); err != nil {
		t.Fatalf("Append with an existing value: %v", err)
	}

	// A new one does not.
	if _, err := cold.Term(ctx, machine, "Lathe-3"); !errors.Is(err, relations.ErrFieldClosed) {
		t.Fatalf("Term(new value on a closed column) = %v, want ErrFieldClosed", err)
	}
	if _, err := cold.Append(ctx, relations.TermCell(machine, "Lathe-3")); !errors.Is(err, relations.ErrFieldClosed) {
		t.Fatalf("Append(new value on a closed column) = %v, want ErrFieldClosed", err)
	}
	// And nothing was written on the way to refusing.
	vocab, err := cold.Vocabulary(ctx, machine)
	if err != nil {
		t.Fatalf("Vocabulary: %v", err)
	}
	if len(vocab) != 2 {
		t.Fatalf("the closed column holds %d terms, want the 2 it was closed with", len(vocab))
	}

	// Other columns are unaffected, and the closure is attributable.
	if _, err := cold.Term(ctx, fields[fieldResult], "Rework"); err != nil {
		t.Fatalf("a new value on an open column: %v", err)
	}
	closedFields, err := j.ClosedFields(ctx)
	if err != nil {
		t.Fatalf("ClosedFields: %v", err)
	}
	if len(closedFields) != 1 || closedFields[0].Field != machine || closedFields[0].Name != fieldMachine {
		t.Fatalf("ClosedFields = %+v, want just %s", closedFields, fieldMachine)
	}
	if closedFields[0].By != actor || closedFields[0].At.IsZero() {
		t.Fatalf("the closure is not attributable: %+v", closedFields[0])
	}

	// Reopening is an ordinary act, and recorded as one.
	if err := j.CloseField(ctx, machine); err == nil {
		t.Fatal("expected an error closing an already-closed column")
	}
	if err := j.ReopenField(ctx, machine); err != nil {
		t.Fatalf("ReopenField: %v", err)
	}
	if _, err := relations.NewJournal(st).Term(ctx, machine, "Lathe-3"); err != nil {
		t.Fatalf("Term after reopening: %v", err)
	}
	if err := j.ReopenField(ctx, machine); err == nil {
		t.Fatal("expected an error reopening an open column")
	}

	// Both acts are in the chain, and the book still adds up.
	events, err := j.Events(ctx, relations.Range{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	var states int
	for _, event := range events {
		if event.Kind() == "vocabulary" {
			states++
			if event.Subject != machine {
				t.Fatalf("a vocabulary event is about %s, want %s", event.Subject, machine)
			}
		}
	}
	if states != 2 {
		t.Fatalf("%d vocabulary events recorded, want 2 (a closing and a reopening)", states)
	}
	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}

	if existing.Type != relations.TypeTerm {
		t.Fatalf("Term returned %s, which is not a term", existing)
	}
	if len(entries) == 0 {
		t.Fatal("the fixture wrote no lines")
	}

	// Neither call takes anything that is not a column.
	if err := j.CloseField(ctx, entries[0]); err == nil {
		t.Fatal("expected an error closing a line")
	}
	undeclared := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeField, ID: 0xFE}
	if err := j.CloseField(ctx, undeclared); err == nil {
		t.Fatal("expected an error closing a column that was never declared")
	}
}

// TestVocabularyStateIsChained checks a closure cannot be flipped or
// dropped behind the chain's back -- which, if it could, would let a
// value be added to a set that was supposed to be fixed and then leave
// no sign of it.
func TestVocabularyStateIsChained(t *testing.T) {
	ctx := context.Background()

	t.Run("flipped without an event", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		_, fields := writeShiftLog(t, j)
		machine := fields[fieldMachine]
		if err := j.CloseField(ctx, machine); err != nil {
			t.Fatalf("CloseField: %v", err)
		}

		// Reopen it by hand, signed with the same key -- the tamper a
		// digest over the record could not catch, because the record is
		// meant to change.
		if err := st.Link(ctx, machine, j.StatusMarker(), relations.KindFieldState, []byte{0}); err != nil {
			t.Fatalf("Link: %v", err)
		}
		if closed, err := j.FieldIsClosed(ctx, machine); err != nil {
			t.Fatalf("FieldIsClosed: %v", err)
		} else if closed {
			t.Fatal("the state was not actually flipped")
		}

		_, err := j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a vocabulary reopened with no record of it")
		}
		if !strings.Contains(err.Error(), "vocabulary") {
			t.Fatalf("error does not name what changed: %v", err)
		}
	})

	t.Run("removed outright", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		_, fields := writeShiftLog(t, j)
		machine := fields[fieldMachine]
		if err := j.CloseField(ctx, machine); err != nil {
			t.Fatalf("CloseField: %v", err)
		}
		if err := st.Unlink(ctx, machine, j.StatusMarker()); err != nil {
			t.Fatalf("Unlink: %v", err)
		}

		_, err := j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a book with a closure removed")
		}
		if !strings.Contains(err.Error(), "gone") {
			t.Fatalf("error does not say the record went missing: %v", err)
		}
	})
}
