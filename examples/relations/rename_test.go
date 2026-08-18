package relations_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestRenameMovesThePresenceBucket is the point of Rename: a typo fixed
// once is fixed for every line that referenced it, the new text is
// findable, and the old text is not.
func TestRenameMovesThePresenceBucket(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	field, err := j.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	term, err := j.Term(ctx, field, "Ivanvoa")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	entry, err := j.Append(ctx, relations.TermCell(field, "Ivanvoa"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	before, found, err := st.Declaration(ctx, term)
	if err != nil || !found {
		t.Fatalf("Declaration = %v, %v", found, err)
	}

	if err := j.Rename(ctx, term, "Ivanova"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// The entity is the same one, still says when it was first written,
	// and now reads the right way.
	after, found, err := st.Declaration(ctx, term)
	if err != nil || !found {
		t.Fatalf("Declaration after rename = %v, %v", found, err)
	}
	if after.Record.Name != "Ivanova" {
		t.Fatalf("declaration reads %q, want %q", after.Record.Name, "Ivanova")
	}
	if !after.Record.Created.Equal(before.Record.Created) {
		t.Fatalf("Created moved from %v to %v; a rename is not a re-creation", before.Record.Created, after.Record.Created)
	}
	if err := st.Verify(ctx, after); err != nil {
		t.Fatalf("Verify after rename: %v", err)
	}

	// The line that referenced it reads the new way -- it holds four
	// bytes, not text.
	row, err := j.Row(ctx, entry)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if len(row) != 1 || row[0].Term != term || row[0].Text != "Ivanova" {
		t.Fatalf("row = %+v, want the same term reading %q", row, "Ivanova")
	}

	// A cold reader -- no cache to help it -- finds it by the new text
	// through the presence index.
	cold := relations.NewJournal(st)
	coldField, err := cold.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("cold Field: %v", err)
	}
	again, err := cold.Term(ctx, coldField, "Ivanova")
	if err != nil {
		t.Fatalf("cold Term: %v", err)
	}
	if again != term {
		t.Fatalf("cold lookup of the new text = %s, want %s", again, term)
	}

	// And the old text is gone: nothing is stored under it, and asking
	// for it mints a new term rather than resurrecting this one.
	if n := countRecordsNamed(t, scanAll(t, st), "Ivanvoa"); n != 0 {
		t.Fatalf("the old text is still stored in %d records", n)
	}
	stale, err := relations.NewJournal(st).Term(ctx, field, "Ivanvoa")
	if err != nil {
		t.Fatalf("Term(old text): %v", err)
	}
	if stale == term {
		t.Fatal("the old text still resolves to the renamed term")
	}

	// One text, one record, either way round.
	pairs := scanAll(t, st)
	if n := countRecordsNamed(t, pairs, "Ivanova"); n != 1 {
		t.Fatalf("%q is stored in %d records, want exactly 1", "Ivanova", n)
	}
}

func TestRenameAColumn(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	field, err := j.Field(ctx, "operater")
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	if _, err := j.Term(ctx, field, "Ivanova"); err != nil {
		t.Fatalf("Term: %v", err)
	}
	if err := j.Rename(ctx, field, fieldOperator); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	cold := relations.NewJournal(st)
	again, err := cold.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("cold Field: %v", err)
	}
	if again != field {
		t.Fatalf("cold lookup of the renamed column = %s, want %s", again, field)
	}
	fields, err := cold.Fields(ctx)
	if err != nil {
		t.Fatalf("Fields: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("log holds %d columns after renaming one, want 1", len(fields))
	}

	// The column's vocabulary is untouched: a rename moves a name, not a
	// relation.
	vocab, err := cold.Vocabulary(ctx, field)
	if err != nil {
		t.Fatalf("Vocabulary: %v", err)
	}
	if len(vocab) != 1 || vocab[0].Text != "Ivanova" {
		t.Fatalf("vocabulary after renaming the column = %+v, want the one term it had", vocab)
	}
}

func TestRenameRefusesAnOccupiedText(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	field, err := j.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	ivanova, err := j.Term(ctx, field, "Ivanova")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	petrov, err := j.Term(ctx, field, "Petrov")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}

	if err := j.Rename(ctx, petrov, "Ivanova"); !errors.Is(err, relations.ErrTextAlreadyInterned) {
		t.Fatalf("Rename onto an occupied text = %v, want ErrTextAlreadyInterned", err)
	}
	// Nothing moved.
	decl, _, err := st.Declaration(ctx, petrov)
	if err != nil {
		t.Fatalf("Declaration: %v", err)
	}
	if decl.Record.Name != "Petrov" {
		t.Fatalf("the refused rename still landed: %q", decl.Record.Name)
	}
	if got, err := relations.NewJournal(st).Term(ctx, field, "Ivanova"); err != nil {
		t.Fatalf("Term: %v", err)
	} else if got != ivanova {
		t.Fatalf("Ivanova now resolves to %s, want %s", got, ivanova)
	}

	// Renaming to the text it already has is a no-op, not a conflict
	// with itself.
	if err := j.Rename(ctx, petrov, "Petrov"); err != nil {
		t.Fatalf("Rename to the same text: %v", err)
	}
	if err := j.Rename(ctx, petrov, ""); err == nil {
		t.Fatal("expected an error renaming to an empty text")
	}

	// An actor is declared at a fixed id and has no bucket, so it is not
	// renameable through the journal.
	actor := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 1}
	if err := j.Rename(ctx, actor, "Someone"); err == nil {
		t.Fatal("expected an error renaming a non-interned entity")
	}
}

// TestRenameWithinOneBucket exercises the branch where the old and new
// texts hash into the *same* presence bucket -- one key, so the removal
// and the insertion have to be a single write. Two writes would leave
// the second overwriting the first, putting the term back into the
// bucket it was just taken out of.
//
// The colliding pair is found by brute force rather than hard-coded, so
// the test stays honest if TextBucket's hash ever changes.
func TestRenameWithinOneBucket(t *testing.T) {
	ctx := context.Background()
	first, second := bucketCollision(t)
	t.Logf("colliding texts %q and %q both hash to bucket %s", first, second, relations.TextBucket(first))

	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	field, err := j.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	term, err := j.Term(ctx, field, first)
	if err != nil {
		t.Fatalf("Term: %v", err)
	}

	if err := j.Rename(ctx, term, second); err != nil {
		t.Fatalf("Rename within one bucket: %v", err)
	}

	cold := relations.NewJournal(st)
	got, err := cold.Term(ctx, field, second)
	if err != nil {
		t.Fatalf("cold Term(new text): %v", err)
	}
	if got != term {
		t.Fatalf("cold lookup of the new text = %s, want %s", got, term)
	}
	stale, err := relations.NewJournal(st).Term(ctx, field, first)
	if err != nil {
		t.Fatalf("cold Term(old text): %v", err)
	}
	if stale == term {
		t.Fatal("the renamed term is still reachable by its old text -- the removal was overwritten")
	}

	// The bucket now names both: the renamed term and the fresh one the
	// old text just minted, in that order, and nothing twice.
	bucketKey := relations.PresenceKey(field, second)
	pairs, err := st.Backend().Scan(ctx, bucketKey, bucketKey, 1)
	if err != nil {
		t.Fatalf("Scan bucket: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("bucket holds %d records, want 1", len(pairs))
	}
	bucket, _, err := relations.DecodeRecord(pairs[0].Value)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if len(bucket.Data) != 2*relations.EntityLen {
		t.Fatalf("bucket lists %d bytes, want two entities", len(bucket.Data))
	}
}

// TestRenameLosesARaceCleanly checks the precondition on the buckets
// does its job: an intern that lands between Rename's reads and its
// write must not be silently dropped from the bucket Rename rewrites.
func TestRenameLosesARaceCleanly(t *testing.T) {
	ctx := context.Background()
	inner := relations.Memory()
	be := &racingBackend{Backend: inner}
	pub, priv := newKey(t)
	st, actor, _ := newStoreOn(t, be, pub, priv)

	j := relations.NewJournal(st)
	field, err := j.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	term, err := j.Term(ctx, field, "Ivanvoa")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}

	// While the rename is in flight, another writer interns a different
	// text into the bucket the rename is about to claim.
	var competitor relations.Entity
	be.target = relations.PresenceKey(field, "Ivanova")
	be.competes = func() {
		w := relations.NewJournal(relations.New(inner, testLog, actor, priv))
		wf, err := w.Field(ctx, fieldOperator)
		if err != nil {
			t.Errorf("competing Field: %v", err)
			return
		}
		// Seed the target bucket directly with an unrelated entity, as a
		// colliding intern would.
		other, err := w.Term(ctx, wf, "Sidorova")
		if err != nil {
			t.Errorf("competing Term: %v", err)
			return
		}
		ref := other.Bytes()
		value, err := relations.Record{Kind: relations.KindPresence, Data: ref[:]}.Encode(be.target, nil)
		if err != nil {
			t.Errorf("encode competing bucket: %v", err)
			return
		}
		if err := inner.Apply(ctx, []relations.Op{{Kind: relations.OpSet, Key: be.target, Value: value}}); err != nil {
			t.Errorf("competing Apply: %v", err)
			return
		}
		competitor = other
	}

	if err := j.Rename(ctx, term, "Ivanova"); err != nil {
		t.Fatalf("Rename racing an intern: %v", err)
	}
	if competitor.IsZero() {
		t.Fatal("the competing writer never ran -- the race was not exercised")
	}

	// The rename retried and both entities are in the bucket: the
	// competitor was not dropped, and the renamed term is findable.
	cold := relations.NewJournal(st)
	got, err := cold.Term(ctx, field, "Ivanova")
	if err != nil {
		t.Fatalf("cold Term: %v", err)
	}
	if got != term {
		t.Fatalf("renamed term resolves to %s, want %s", got, term)
	}
	bucketKey := relations.PresenceKey(field, "Ivanova")
	pairs, err := st.Backend().Scan(ctx, bucketKey, bucketKey, 1)
	if err != nil {
		t.Fatalf("Scan bucket: %v", err)
	}
	bucket, _, err := relations.DecodeRecord(pairs[0].Value)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if len(bucket.Data) != 2*relations.EntityLen {
		t.Fatalf("bucket lists %d bytes, want the competitor and the renamed term", len(bucket.Data))
	}
}

// bucketCollision finds two texts whose TextBucket is the same. Four
// bytes of hash means a birthday collision turns up within a few tens of
// thousands of candidates, so this is milliseconds of work -- cheap
// enough to do honestly rather than hard-code a pair that would go stale
// the moment the hash changed.
var (
	collisionOnce   sync.Once
	collisionA      string
	collisionB      string
	collisionFailed bool
)

func bucketCollision(t *testing.T) (string, string) {
	t.Helper()
	collisionOnce.Do(func() {
		const limit = 4_000_000
		seen := make(map[relations.Entity]string, 1<<20)
		for i := 0; i < limit; i++ {
			text := fmt.Sprintf("collision-candidate-%d", i)
			bucket := relations.TextBucket(text)
			if other, ok := seen[bucket]; ok {
				collisionA, collisionB = other, text
				return
			}
			seen[bucket] = text
		}
		collisionFailed = true
	})
	if collisionFailed {
		t.Fatal("found no 4-byte bucket collision -- TextBucket is not behaving like a hash")
	}
	return collisionA, collisionB
}

// TestRenameIsChained covers the hole a per-record digest leaves: a line
// holds four bytes, not text, so renaming the term those bytes point at
// changes what every line using it says. The rename is an event, with
// both names on the record, and renaming twice does not invalidate the
// first event -- which is exactly what digesting the declaration itself
// would have done.
func TestRenameIsChained(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	machine, err := j.Term(ctx, fields[fieldMachine], "Lathe-2")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	if err := j.Rename(ctx, machine, "Lathe-2A"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	events, err := j.Events(ctx, relations.Range{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind() != "rename" || last.Subject != machine {
		t.Fatalf("the last event is a %s on %s, want a rename on %s", last.Kind(), last.Subject, machine)
	}
	from, to, ok := last.Rename()
	if !ok || from != "Lathe-2" || to != "Lathe-2A" {
		t.Fatalf("the rename event records %q -> %q (ok=%v), want Lathe-2 -> Lathe-2A", from, to, ok)
	}
	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain after a rename: %v", err)
	}

	// Renaming again must not invalidate the first event. This is the
	// whole reason a rename digests what the link carries rather than
	// the record it wrote.
	if err := j.Rename(ctx, machine, "Lathe-2B"); err != nil {
		t.Fatalf("second Rename: %v", err)
	}
	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain after renaming twice: %v", err)
	}

	// And the line still reads the current name, which is the point of
	// chaining this at all.
	row, err := j.Row(ctx, entries[0])
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	var reads string
	for _, cell := range row {
		if cell.FieldName == fieldMachine {
			reads = cell.Text
		}
	}
	if reads != "Lathe-2B" {
		t.Fatalf("line 1 reads %q in the machine column, want Lathe-2B", reads)
	}
}

// TestRenameBehindTheChainsBack is the tamper the event exists to catch:
// rewriting a declaration directly -- with a perfectly good signature,
// because the key is right there -- and no event to say it happened.
func TestRenameBehindTheChainsBack(t *testing.T) {
	ctx := context.Background()

	t.Run("rewritten with no event", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		_, fields := writeShiftLog(t, j)
		machine, err := j.Term(ctx, fields[fieldMachine], "Lathe-2")
		if err != nil {
			t.Fatalf("Term: %v", err)
		}
		if err := j.Rename(ctx, machine, "Lathe-2A"); err != nil {
			t.Fatalf("Rename: %v", err)
		}

		// Straight past the journal: a signed declaration with a
		// different name and no event behind it.
		if err := st.Declare(ctx, machine, relations.KindDeclaration, "Mill-9", nil); err != nil {
			t.Fatalf("Declare: %v", err)
		}
		_, err = j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a declaration rewritten with no event")
		}
		if !strings.Contains(err.Error(), "renamed with no record of it") {
			t.Fatalf("error does not say what happened: %v", err)
		}
	})

	t.Run("removed outright", func(t *testing.T) {
		st, _, _ := newStore(t)
		j := relations.NewJournal(st)
		_, fields := writeShiftLog(t, j)
		machine, err := j.Term(ctx, fields[fieldMachine], "Lathe-2")
		if err != nil {
			t.Fatalf("Term: %v", err)
		}
		if err := j.Rename(ctx, machine, "Lathe-2A"); err != nil {
			t.Fatalf("Rename: %v", err)
		}
		if err := st.Backend().Apply(ctx, []relations.Op{{
			Kind: relations.OpDelete,
			Key:  relations.DeclarationKey(machine),
		}}); err != nil {
			t.Fatalf("Apply: %v", err)
		}

		_, err = j.VerifyChain(ctx)
		if err == nil {
			t.Fatal("VerifyChain accepted a book with a renamed declaration removed")
		}
		if !strings.Contains(err.Error(), "gone") {
			t.Fatalf("error does not say the record went missing: %v", err)
		}
	})
}
