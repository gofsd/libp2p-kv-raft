package relations_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestTwoBooksOnOneStore pins what the log byte is for: several
// independent log books in one store, told apart by that byte alone.
//
// Everything a book consists of hangs off it -- its columns, its
// vocabularies, its actors, its pages, its allocator counters, and its
// chain -- so two books share a cluster and a keyspace and nothing else.
// That is worth a test rather than a paragraph, because an application
// keeping a book per line, per shop or per year is relying on it.
func TestTwoBooksOnOneStore(t *testing.T) {
	ctx := context.Background()
	backend := relations.Memory()

	first := openBook(t, backend, 1)
	second := openBook(t, backend, 2)

	// The same column heading in both books is two different columns,
	// each with its own vocabulary.
	firstField, err := first.DefineField(ctx, "operator", relations.InputTerm)
	if err != nil {
		t.Fatalf("DefineField(book 1): %v", err)
	}
	secondField, err := second.DefineField(ctx, "operator", relations.InputTerm)
	if err != nil {
		t.Fatalf("DefineField(book 2): %v", err)
	}
	if firstField == secondField {
		t.Fatalf("both books resolved \"operator\" to %s", firstField)
	}
	if firstField.Log != 1 || secondField.Log != 2 {
		t.Fatalf("columns are %s and %s, want one in each book", firstField, secondField)
	}

	if _, err := first.Term(ctx, firstField, "Ivanova"); err != nil {
		t.Fatalf("Term(book 1): %v", err)
	}
	if _, err := second.Term(ctx, secondField, "Petrov"); err != nil {
		t.Fatalf("Term(book 2): %v", err)
	}

	// Closing one book's vocabulary says nothing about the other's.
	if err := first.CloseField(ctx, firstField); err != nil {
		t.Fatalf("CloseField(book 1): %v", err)
	}
	if _, err := first.Term(ctx, firstField, "Nobody"); err == nil {
		t.Fatal("book 1 accepted a value after its vocabulary was closed")
	}
	if _, err := second.Term(ctx, secondField, "Nobody"); err != nil {
		t.Fatalf("book 2's vocabulary was closed by book 1's closure: %v", err)
	}

	// Lines are numbered per book: both start at page 1, line 1.
	firstLine, err := first.Append(ctx, relations.TermCell(firstField, "Ivanova"))
	if err != nil {
		t.Fatalf("Append(book 1): %v", err)
	}
	secondLine, err := second.Append(ctx, relations.TermCell(secondField, "Petrov"))
	if err != nil {
		t.Fatalf("Append(book 2): %v", err)
	}
	if firstLine.Page != relations.FirstEntryPage || firstLine.ID != 1 ||
		secondLine.Page != relations.FirstEntryPage || secondLine.ID != 1 {
		t.Fatalf("lines landed at %s and %s, want line 1 of page 1 in each", firstLine, secondLine)
	}
	if firstLine.Log != 1 || secondLine.Log != 2 {
		t.Fatalf("lines are %s and %s, want one in each book", firstLine, secondLine)
	}

	// Neither book can see the other's page.
	for _, book := range []struct {
		name    string
		journal *relations.Journal
		want    relations.Entity
	}{{"book 1", first, firstLine}, {"book 2", second, secondLine}} {
		page, err := book.journal.Page(ctx, relations.FirstEntryPage)
		if err != nil {
			t.Fatalf("Page(%s): %v", book.name, err)
		}
		if len(page) != 1 || page[0] != book.want {
			t.Fatalf("%s's page holds %v, want just %s", book.name, page, book.want)
		}
	}

	// And each keeps its own chain: separate heads, separate event
	// streams, verified separately -- so a book is also the unit two
	// writers can work in without serialising against each other.
	if first.ChainHead() == second.ChainHead() || first.EventAnchor() == second.EventAnchor() {
		t.Fatal("the two books share a chain head or event anchor")
	}
	//
	// The counts differ, and that asymmetry is the evidence: book 1
	// recorded a closure and a line, book 2 only a line, so book 1's
	// closure left no trace in book 2's chain.
	for _, book := range []struct {
		name    string
		journal *relations.Journal
		want    int
	}{{"book 1", first, 2}, {"book 2", second, 1}} {
		events, err := book.journal.VerifyChain(ctx)
		if err != nil {
			t.Fatalf("VerifyChain(%s): %v", book.name, err)
		}
		if events != book.want {
			t.Fatalf("%s replayed %d events, want %d", book.name, events, book.want)
		}
	}
}

// openBook is one device opening one log book: its own actor, its own
// key, its own everything.
func openBook(t *testing.T, backend relations.Backend, log uint8) *relations.Journal {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	actor := relations.Entity{Log: log, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 1}
	store := relations.New(backend, log, actor, priv)
	if err := store.DeclareActor(context.Background(), actor, "the writer", pub); err != nil {
		t.Fatalf("DeclareActor(log %d): %v", log, err)
	}
	return relations.NewJournal(store)
}

// TestVocabularyOutgrowsOnePage pins the size of a dictionary. Terms are
// allocated from the schema page upward and spill onto the next page when
// one fills, so a vocabulary is not capped at a page's 255 ids -- and the
// interning that keeps a term single has to keep working across that
// boundary, since a presence bucket is keyed by owner and text and knows
// nothing about which page the term ended up on.
func TestVocabularyOutgrowsOnePage(t *testing.T) {
	ctx := context.Background()
	book := openBook(t, relations.Memory(), 1)

	field, err := book.DefineField(ctx, "part", relations.InputTerm)
	if err != nil {
		t.Fatalf("DefineField: %v", err)
	}

	const want = 300 // one page's 255 and then some
	seen := make(map[relations.Entity]string, want)
	spilled := 0
	for i := 0; i < want; i++ {
		text := fmt.Sprintf("part-%03d", i)
		term, err := book.Term(ctx, field, text)
		if err != nil {
			t.Fatalf("Term(%q): %v", text, err)
		}
		if other, dup := seen[term]; dup {
			t.Fatalf("%q and %q were both handed %s", other, text, term)
		}
		seen[term] = text
		if term.Page != relations.SchemaPage {
			spilled++
		}
	}
	if spilled == 0 {
		t.Fatal("300 terms all fit on the schema page; they should have spilled")
	}

	// Interning still recognises a term that landed on a spilled page:
	// asking for it again returns the entity already there, not a second
	// one -- which is the whole point of a dictionary.
	for term, text := range seen {
		again, err := book.Term(ctx, field, text)
		if err != nil {
			t.Fatalf("re-Term(%q): %v", text, err)
		}
		if again != term {
			t.Fatalf("%q interned as %s and then again as %s", text, term, again)
		}
	}

	vocab, err := book.Vocabulary(ctx, field)
	if err != nil {
		t.Fatalf("Vocabulary: %v", err)
	}
	if len(vocab) != want {
		t.Fatalf("vocabulary lists %d terms, want %d", len(vocab), want)
	}
}
