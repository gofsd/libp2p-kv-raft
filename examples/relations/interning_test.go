package relations_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// TestInterningIsRaceFree is the property the whole dictionary
// discipline rests on: several writers interning the same new value at
// the same moment must end up with one entity, not one each. Each
// goroutine gets its own Journal, so none of them can win by reading
// another's cache -- they have to agree through the store.
func TestInterningIsRaceFree(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)

	const writers = 8
	var (
		start   sync.WaitGroup
		done    sync.WaitGroup
		mu      sync.Mutex
		gotFld  []relations.Entity
		gotTerm []relations.Entity
		errs    []error
	)
	start.Add(1)
	for i := 0; i < writers; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			j := relations.NewJournal(st)
			start.Wait()

			field, err := j.Field(ctx, fieldOperator)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			term, err := j.Term(ctx, field, "Ivanova")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			gotFld = append(gotFld, field)
			gotTerm = append(gotTerm, term)
		}()
	}
	start.Done()
	done.Wait()

	for _, err := range errs {
		t.Fatalf("concurrent intern: %v", err)
	}
	if len(gotTerm) != writers {
		t.Fatalf("%d writers returned a term, want %d", len(gotTerm), writers)
	}
	for i := range gotTerm {
		if gotFld[i] != gotFld[0] {
			t.Fatalf("writers disagree about the field: %s vs %s", gotFld[i], gotFld[0])
		}
		if gotTerm[i] != gotTerm[0] {
			t.Fatalf("writers disagree about the term: %s vs %s", gotTerm[i], gotTerm[0])
		}
	}

	// And the store agrees with them: one column, one term in it, one
	// record carrying the text.
	fields, err := relations.NewJournal(st).Fields(ctx)
	if err != nil {
		t.Fatalf("Fields: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("store holds %d columns after a concurrent intern, want 1", len(fields))
	}
	vocab, err := relations.NewJournal(st).Vocabulary(ctx, gotFld[0])
	if err != nil {
		t.Fatalf("Vocabulary: %v", err)
	}
	if len(vocab) != 1 || vocab[0].Term != gotTerm[0] || vocab[0].Text != "Ivanova" {
		t.Fatalf("vocabulary = %+v, want exactly the one term the writers agreed on", vocab)
	}
	pairs := scanAll(t, st)
	for _, text := range []string{fieldOperator, "Ivanova"} {
		if n := countRecordsNamed(t, pairs, text); n != 1 {
			t.Fatalf("%q is stored in %d records after a concurrent intern, want exactly 1", text, n)
		}
	}
}

// racingBackend loses the interning race on purpose: the first time it
// sees a transaction claiming a presence bucket, it lets a competing
// writer claim that bucket first, so the transaction it was handed is
// guaranteed to fail its precondition.
type racingBackend struct {
	relations.Backend
	once sync.Once
	// target is the one presence bucket to race for, armed once the
	// test is ready; competes is what claims it first.
	target   []byte
	competes func()
}

func (r *racingBackend) Apply(ctx context.Context, ops []relations.Op) error {
	if r.target != nil {
		for _, op := range ops {
			if op.Kind == relations.OpCompareAbsent && bytes.Equal(op.Key, r.target) {
				r.once.Do(r.competes)
				break
			}
		}
	}
	return r.Backend.Apply(ctx, ops)
}

// TestInterningLoserAdoptsTheWinnersTerm drives the race deterministically
// rather than hoping goroutines interleave: the loser's precondition is
// made to fail, and what has to happen next is that it adopts the
// winner's entity instead of allocating a second one for the same text.
func TestInterningLoserAdoptsTheWinnersTerm(t *testing.T) {
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

	// The competing writer works straight against the inner backend, so
	// its own write does not re-enter the interposer. It is armed only
	// now, so that creating the column above raced with nobody.
	winnerStore := relations.New(inner, testLog, actor, priv)
	var winner relations.Entity
	be.target = relations.PresenceKey(field, "Ivanova")
	be.competes = func() {
		w := relations.NewJournal(winnerStore)
		wf, err := w.Field(ctx, fieldOperator)
		if err != nil {
			t.Errorf("competing Field: %v", err)
			return
		}
		winner, err = w.Term(ctx, wf, "Ivanova")
		if err != nil {
			t.Errorf("competing Term: %v", err)
		}
	}

	term, err := j.Term(ctx, field, "Ivanova")
	if err != nil {
		t.Fatalf("Term after losing the race: %v", err)
	}
	if winner.IsZero() {
		t.Fatal("the competing writer never ran -- the race was not exercised")
	}
	if term != winner {
		t.Fatalf("loser interned %s, want the winner's %s", term, winner)
	}

	vocab, err := relations.NewJournal(st).Vocabulary(ctx, field)
	if err != nil {
		t.Fatalf("Vocabulary: %v", err)
	}
	if len(vocab) != 1 {
		t.Fatalf("column holds %d terms after the race, want 1", len(vocab))
	}
	if n := countRecordsNamed(t, scanAll(t, st), "Ivanova"); n != 1 {
		t.Fatalf("%q is stored in %d records, want exactly 1", "Ivanova", n)
	}

	// The loser consumed no id: the next term allocated is the one right
	// after the winner's.
	next, err := j.Term(ctx, field, "Petrov")
	if err != nil {
		t.Fatalf("Term(Petrov): %v", err)
	}
	if next.Page != winner.Page || next.ID != winner.ID+1 {
		t.Fatalf("next term = %s, want the id right after the winner's %s (a refused intern must not burn an id)", next, winner)
	}
}

// TestInterningSurvivesABucketCollision drives the path a real 4-byte
// hash collision would take. Finding one takes ~2^16 texts by birthday,
// which is too slow for a test, so the bucket is seeded by hand with an
// entity whose declared name is *not* what is being looked up -- exactly
// what a collision looks like from the lookup's side.
//
// The requirement is that the collision costs a read, not correctness:
// the second text gets its own entity, both stay findable, and neither
// is ever returned for the other.
func TestInterningSurvivesABucketCollision(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	field, err := j.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	squatter, err := j.Term(ctx, field, "Petrov")
	if err != nil {
		t.Fatalf("Term(Petrov): %v", err)
	}

	// Move Petrov's entity into Ivanova's bucket, as a collision would.
	bucketKey := relations.PresenceKey(field, "Ivanova")
	ref := squatter.Bytes()
	seeded, err := relations.Record{Kind: relations.KindPresence, Data: ref[:]}.Encode(bucketKey, nil)
	if err != nil {
		t.Fatalf("Encode bucket: %v", err)
	}
	if err := st.Backend().Apply(ctx, []relations.Op{{Kind: relations.OpSet, Key: bucketKey, Value: seeded}}); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}

	term, err := j.Term(ctx, field, "Ivanova")
	if err != nil {
		t.Fatalf("Term(Ivanova) with a colliding bucket: %v", err)
	}
	if term == squatter {
		t.Fatal("interning returned the colliding entity -- the name check did not happen")
	}
	decl, found, err := st.Declaration(ctx, term)
	if err != nil || !found {
		t.Fatalf("Declaration(%s) = %v, %v", term, found, err)
	}
	if decl.Record.Name != "Ivanova" {
		t.Fatalf("interned entity is named %q, want %q", decl.Record.Name, "Ivanova")
	}

	// The bucket now lists both, and a cold reader resolves each to the
	// right one.
	pairs, err := st.Backend().Scan(ctx, bucketKey, bucketKey, 1)
	if err != nil {
		t.Fatalf("Scan bucket: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("bucket read back %d records, want 1", len(pairs))
	}
	bucket, _, err := relations.DecodeRecord(pairs[0].Value)
	if err != nil {
		t.Fatalf("DecodeRecord(bucket): %v", err)
	}
	squatterRef, termRef := squatter.Bytes(), term.Bytes()
	if want := append(append([]byte{}, squatterRef[:]...), termRef[:]...); !bytes.Equal(bucket.Data, want) {
		t.Fatalf("bucket lists %x, want both entities %x", bucket.Data, want)
	}
	if bucket.Name != "" {
		t.Fatalf("bucket record carries the text %q; the index must not copy values", bucket.Name)
	}

	cold := relations.NewJournal(st)
	again, err := cold.Term(ctx, field, "Ivanova")
	if err != nil {
		t.Fatalf("cold Term(Ivanova): %v", err)
	}
	if again != term {
		t.Fatalf("cold lookup of a collided bucket = %s, want %s", again, term)
	}
	stillPetrov, err := relations.NewJournal(st).Term(ctx, field, "Petrov")
	if err != nil {
		t.Fatalf("cold Term(Petrov): %v", err)
	}
	if stillPetrov != squatter {
		t.Fatalf("Petrov now resolves to %s, want %s", stillPetrov, squatter)
	}
}

// countingBackend records how many reads are point reads (start == end)
// and how many are range scans, and how many pairs those reads actually
// pulled out of the store.
type countingBackend struct {
	relations.Backend
	points atomic.Int64
	ranges atomic.Int64
	pairs  atomic.Int64
}

func (c *countingBackend) Scan(ctx context.Context, start, end []byte, limit int) ([]relations.Pair, error) {
	if bytes.Equal(start, end) {
		c.points.Add(1)
	} else {
		c.ranges.Add(1)
	}
	pairs, err := c.Backend.Scan(ctx, start, end, limit)
	c.pairs.Add(int64(len(pairs)))
	return pairs, err
}

func (c *countingBackend) reset() {
	c.points.Store(0)
	c.ranges.Store(0)
	c.pairs.Store(0)
}

// TestInternedLookupIsAPointRead pins the cost of the presence index.
// Looking a value up used to mean scanning its whole column vocabulary
// -- which over the real IPC path is one round trip per term in it,
// because listRange answers with a single pair at a time. With the
// bucket it is a point read of the bucket plus one confirming read of
// the candidate's declaration, whatever the vocabulary's size.
func TestInternedLookupIsAPointRead(t *testing.T) {
	ctx := context.Background()
	be := &countingBackend{Backend: relations.Memory()}
	pub, priv := newKey(t)
	st, _, _ := newStoreOn(t, be, pub, priv)

	warm := relations.NewJournal(st)
	field, err := warm.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	for _, name := range []string{"Ivanova", "Petrov", "Sidorova", "Kim", "Novak"} {
		if _, err := warm.Term(ctx, field, name); err != nil {
			t.Fatalf("Term(%s): %v", name, err)
		}
	}

	be.reset()

	cold := relations.NewJournal(st)
	coldField, err := cold.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("cold Field: %v", err)
	}
	if coldField != field {
		t.Fatalf("cold lookup found column %s, want %s", coldField, field)
	}
	term, err := cold.Term(ctx, coldField, "Novak")
	if err != nil {
		t.Fatalf("cold Term: %v", err)
	}
	if term.Type != relations.TypeTerm {
		t.Fatalf("cold Term returned %s, which is not a term", term)
	}

	if got := be.ranges.Load(); got != 0 {
		t.Fatalf("a cold lookup of an existing column and term made %d range scans, want 0", got)
	}
	if got := be.points.Load(); got > 6 {
		t.Fatalf("a cold lookup made %d point reads, want a small constant", got)
	}
	t.Logf("cold lookup of an existing column and term: %d point reads, %d range scans (vocabulary of 5)",
		be.points.Load(), be.ranges.Load())
}
