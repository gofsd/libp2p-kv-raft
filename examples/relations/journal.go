package relations

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// The entity types this example's paper-log replacement uses. Only
// TypeAllocator (see store.go) is reserved by the package itself; these
// four are application vocabulary, and an application with different
// nouns should define its own.
const (
	// TypeField is a column of the log: "operator", "machine", "shift".
	// Its declaration's Name is the column heading.
	TypeField uint8 = 0x01
	// TypeTerm is one admissible value of one field -- a dictionary
	// entry. Its declaration's Name is the only place that text is ever
	// stored; every use of it is a 4-byte reference.
	TypeTerm uint8 = 0x02
	// TypeEntry is one line of the log. Its declaration carries no name
	// at all: a line has no text of its own, only references to terms.
	TypeEntry uint8 = 0x03
	// TypeActor is a person or device that writes entries. Its
	// declaration's Data holds their Ed25519 public key (see
	// Store.DeclareActor).
	TypeActor uint8 = 0x04
)

// The relation kinds this example uses. Kind lives in the record, not
// the key -- see Record.Kind.
const (
	// KindDeclaration is the kind of an (e, Zero) record. Zero so that a
	// caller who never thinks about kinds still gets sensible
	// declarations.
	KindDeclaration byte = 0x00
	// KindTermOf relates a term to the field whose vocabulary it belongs
	// to: term -> field. Its mirror is what makes "list this field's
	// vocabulary" a prefix scan.
	KindTermOf byte = 0x01
	// KindCell relates a log entry to one term it uses: entry -> term.
	// Its mirror is what makes "every entry that named this term" a
	// prefix scan -- the query a paper log cannot answer at all without
	// reading every page.
	KindCell byte = 0x02
	// KindQuantity relates a log entry directly to a *field* rather than
	// to a term, with the number in the record's Data: entry -> field.
	// Quantities are the one thing a dictionary is wrong for -- there is
	// no useful vocabulary of every number that might be written in a
	// column -- so they are carried as an 8-byte payload on the relation
	// instead of interned.
	KindQuantity byte = 0x03
	// 0x04 is KindDerivedFrom, declared in genealogy.go beside the layer
	// that uses it.
	//
	// KindPresence is the kind of a presence-index record: the
	// content-addressed bucket that makes interning a value a point read
	// and a race-free write (see PresenceKey and Journal.intern). Its
	// Data is a list of 4-byte entities and it carries no name -- the
	// text it indexes stays in the one declaration that owns it.
	KindPresence byte = 0x05
	// 0x06..0x0B are the correction and chain kinds, declared in
	// corrections.go and chain.go beside the code that uses them.
	//
	// KindRemark relates a log entry directly to a *field*, like
	// KindQuantity, with free text in the record's Data: entry -> field.
	// It is the one deliberate exception to the dictionary discipline --
	// a remark is prose about one line, not a value from a vocabulary,
	// so it is stored verbatim on the relation and repeated text really
	// is repeated. See RemarkCell.
	KindRemark byte = 0x0C
)

// SchemaPage is where a Journal puts the things that describe the log --
// fields, dictionary terms, actors. FirstEntryPage is where the entries
// themselves start, so page 1 of the store is page 1 of the log book.
const (
	SchemaPage     uint8 = 0
	FirstEntryPage uint8 = 1
)

// Journal is the paper-log replacement built on Store: a book of
// numbered pages whose every line is composed of dictionary terms rather
// than handwriting.
//
// The rules it enforces are the ones a paper log only asks for politely:
// a value must come from its column's vocabulary (Term interns it, and
// an entry can only reference an interned term), a line is written whole
// or not at all (one atomic Apply per Append), every line carries who
// wrote it, when, and their signature (Store stamps those onto every
// record), and nothing is ever erased -- a wrong line is superseded or
// struck through, never overwritten or deleted (see corrections.go).
//
// A Journal caches the dictionary it has seen, so repeated Appends do
// not re-read it. The cache is additive: another writer's new term is
// picked up on the next miss. Its own Rename updates it, but a rename
// performed by *another* process is not noticed -- this Journal goes on
// answering with the text it last read until it is rebuilt. That is
// acceptable because renaming is a schema correction rather than routine
// traffic; a process that must see them promptly should build a fresh
// Journal per unit of work, which costs nothing but the cache.
type Journal struct {
	st *Store

	mu         sync.Mutex
	fields     map[string]Entity            // field name -> field entity
	fieldNames map[Entity]string            // field entity -> name
	vocab      map[Entity]map[string]Entity // field -> term text -> term entity
	terms      map[Entity]TermInfo          // term entity -> its field and text
}

// TermInfo is a dictionary entry: which field's vocabulary it belongs to
// and the text it stands for.
type TermInfo struct {
	Term  Entity
	Field Entity
	Text  string
}

// NewJournal returns a Journal writing through st.
func NewJournal(st *Store) *Journal {
	return &Journal{
		st:         st,
		fields:     make(map[string]Entity),
		fieldNames: make(map[Entity]string),
		vocab:      make(map[Entity]map[string]Entity),
		terms:      make(map[Entity]TermInfo),
	}
}

// Store exposes the underlying Store, for queries this layer does not
// wrap.
func (j *Journal) Store() *Store { return j.st }

// Field returns the field (column) named name, declaring it if this log
// does not have it yet. Field names are the log's schema, so this is
// find-or-create rather than create: calling it on every startup is the
// intended use, and two processes starting at once still end up with one
// column (see intern).
func (j *Journal) Field(ctx context.Context, name string) (Entity, error) {
	if name == "" {
		return Zero, fmt.Errorf("relations: field name must not be empty")
	}
	if e, ok := j.cachedField(name); ok {
		return e, nil
	}
	e, err := j.intern(ctx, j.fieldOwner(), TypeField, name, nil)
	if err != nil {
		return Zero, err
	}
	j.mu.Lock()
	j.fields[name] = e
	j.fieldNames[e] = name
	j.mu.Unlock()
	return e, nil
}

// Term returns the dictionary term of field whose text is text,
// declaring it and linking it into field's vocabulary if it is new. The
// declaration, the term -> field link and the presence-index update go
// out as one atomic Apply, so a term that exists is always both
// reachable from its field and findable by its text.
//
// Interning is race-free: two processes interning the same new text at
// the same moment agree on one term, because the write is guarded by a
// compare precondition on the text's presence bucket and the loser
// adopts the winner's entity (see intern). This is the property the
// whole dictionary discipline rests on -- a duplicated term would mean
// the same value stored twice, which is exactly what the format exists
// to prevent.
func (j *Journal) Term(ctx context.Context, field Entity, text string) (Entity, error) {
	if field.Type != TypeField {
		return Zero, fmt.Errorf("relations: term: %s is not a field", field)
	}
	if text == "" {
		return Zero, fmt.Errorf("relations: term text must not be empty")
	}
	if e, ok := j.cachedTerm(field, text); ok {
		return e, nil
	}
	term, err := j.intern(ctx, field, TypeTerm, text, func(term Entity) ([]Op, error) {
		ops, err := j.st.LinkOps(term, field, KindTermOf, nil)
		if err != nil {
			return nil, err
		}
		// This callback runs only when the term is about to be created,
		// which is the only moment a closed vocabulary has anything to
		// say -- and it runs inside the transaction, so the closure is a
		// precondition rather than a check that can be raced.
		guard, err := j.vocabularyGuardOps(ctx, field)
		if err != nil {
			return nil, err
		}
		return append(ops, guard...), nil
	})
	if err != nil {
		return Zero, err
	}
	j.rememberTerm(TermInfo{Term: term, Field: field, Text: text})
	return term, nil
}

// ErrTextAlreadyInterned is what Rename returns when the text asked for
// is already held by some other entity in the same vocabulary. Renaming
// into it would leave two entities standing for one value, which is the
// one thing the dictionary exists to prevent -- so the rename is refused
// rather than performed and apologised for.
var ErrTextAlreadyInterned = errors.New("relations: another entity already holds this text")

// Rename changes the text an interned entity -- a column or a dictionary
// term -- stands for, moving it between presence buckets so that
// lookups by the new text find it and lookups by the old one do not.
// The bucket removal, the bucket insertion and the rewritten
// declaration are one atomic Apply, so the index is never out of step
// with the declaration it indexes.
//
// The declaration keeps its original Created stamp and takes this
// store's actor as its author: a rename records *who last stated what
// this entity is called*, not a new creation. It does not keep the old
// text anywhere.
//
// Two things a caller has to mean before calling this:
//
//   - Renaming a term rewrites what every line referencing it says. That
//     is inherent to a dictionary -- the lines hold a 4-byte reference,
//     not the text -- and it is why a rename is for fixing a name
//     (a typo, a machine relabelled), never for correcting what a
//     particular line recorded. A correction to a line is a new line
//     superseding the old one; see the README.
//   - The old text becomes free. A later intern of it allocates a new
//     entity, unrelated to this one and to the lines that used to read
//     that way.
//
// Returns ErrTextAlreadyInterned if some other entity in the same
// vocabulary already holds text, and does nothing if e already reads
// that way.
func (j *Journal) Rename(ctx context.Context, e Entity, text string) error {
	if text == "" {
		return fmt.Errorf("relations: rename: text must not be empty")
	}
	owner, err := j.internOwner(ctx, e)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < allocRetries; attempt++ {
		decl, found, err := j.st.Declaration(ctx, e)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("relations: rename: %s is not declared", e)
		}
		old := decl.Record.Name
		if old == text {
			return nil
		}

		ops, err := j.renameOps(ctx, e, owner, old, text, decl.Record)
		if err != nil {
			return err
		}
		// A rename changes what every line referencing this entity says,
		// so it is an event like any other -- carrying both names,
		// because neither survives in the store afterwards (see
		// mutableEvent).
		tail, err := encodeRenameTail(old, text)
		if err != nil {
			return err
		}
		chain, err := j.chainEventOps(ctx, Zero, ops, mutableSpec{tag: eventDeclare, subject: e, tail: tail})
		if err != nil {
			return err
		}
		err = j.st.Backend().Apply(ctx, append(ops, chain...))
		if err == nil {
			j.renameCached(e, owner, old, text)
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
		// Somebody else moved one of the two buckets (or the
		// declaration) between the reads above and the write -- re-read
		// everything and try again.
	}
	return fmt.Errorf("relations: rename: %s lost %d races in a row", e, allocRetries)
}

// renameOps builds the one transaction Rename applies: drop e from the
// old text's bucket, add it to the new text's, and rewrite the
// declaration. Every bucket write is guarded by a compare against the
// exact bytes it was read as, so a concurrent intern into either bucket
// makes the whole rename fail rather than silently drop an entry.
func (j *Journal) renameOps(ctx context.Context, e, owner Entity, old, text string, rec Record) ([]Op, error) {
	oldKey, newKey := PresenceKey(owner, old), PresenceKey(owner, text)

	newRaw, newCandidates, err := j.bucket(ctx, owner, text)
	if err != nil {
		return nil, err
	}
	if holder, found, err := j.resolveBucket(ctx, owner, text, newCandidates); err != nil {
		return nil, err
	} else if found && holder != e {
		return nil, fmt.Errorf("%w: %q is %s", ErrTextAlreadyInterned, text, holder)
	}

	oldRaw, oldCandidates, err := j.bucket(ctx, owner, old)
	if err != nil {
		return nil, err
	}
	remaining := make([]Entity, 0, len(oldCandidates))
	for _, candidate := range oldCandidates {
		if candidate != e {
			remaining = append(remaining, candidate)
		}
	}

	var ops []Op
	if bytes.Equal(oldKey, newKey) {
		// Both texts hash into the same bucket. One key, therefore one
		// precondition and one write: two of each would leave the
		// second overwriting the first's result, putting e back in the
		// bucket it was just removed from.
		value, err := j.st.encode(newKey, KindPresence, "", packEntities(append(remaining, e)))
		if err != nil {
			return nil, err
		}
		ops = append(ops,
			Op{Kind: OpCompare, Key: oldKey, Value: oldRaw},
			Op{Kind: OpSet, Key: newKey, Value: value})
	} else {
		if len(remaining) != len(oldCandidates) {
			ops = append(ops, Op{Kind: OpCompare, Key: oldKey, Value: oldRaw})
			if len(remaining) == 0 {
				ops = append(ops, Op{Kind: OpDelete, Key: oldKey})
			} else {
				value, err := j.st.encode(oldKey, KindPresence, "", packEntities(remaining))
				if err != nil {
					return nil, err
				}
				ops = append(ops, Op{Kind: OpSet, Key: oldKey, Value: value})
			}
		}
		pre := Op{Kind: OpCompareAbsent, Key: newKey}
		if newRaw != nil {
			pre = Op{Kind: OpCompare, Key: newKey, Value: newRaw}
		}
		value, err := j.st.encode(newKey, KindPresence, "", packEntities(append(newCandidates, e)))
		if err != nil {
			return nil, err
		}
		ops = append(ops, pre, Op{Kind: OpSet, Key: newKey, Value: value})
	}

	declOps, err := j.st.DeclareOps(e, rec.Kind, text, rec.Data, rec.Created)
	if err != nil {
		return nil, err
	}
	return append(ops, declOps...), nil
}

// internOwner returns the vocabulary an interned entity belongs to: its
// field for a term, the synthetic field-name owner for a column. Nothing
// else in this package is interned, so nothing else can be renamed
// through it -- an actor, for instance, is declared at a fixed id and
// has no bucket to move.
func (j *Journal) internOwner(ctx context.Context, e Entity) (Entity, error) {
	switch e.Type {
	case TypeField:
		return j.fieldOwner(), nil
	case TypeTerm:
		rels, err := j.st.Relations(ctx, e)
		if err != nil {
			return Zero, err
		}
		owners := OfKind(rels, KindTermOf)
		if len(owners) == 0 {
			return Zero, fmt.Errorf("relations: %s belongs to no field", e)
		}
		return owners[0].B, nil
	default:
		return Zero, fmt.Errorf("relations: only columns and terms are interned; %s is neither", e)
	}
}

// packEntities lays entities out as a presence bucket's payload: four
// bytes each, no separators, no text.
func packEntities(entities []Entity) []byte {
	data := make([]byte, 0, len(entities)*EntityLen)
	for _, e := range entities {
		b := e.Bytes()
		data = append(data, b[:]...)
	}
	return data
}

// renameCached moves e between the caches Field/Term read, so the
// renaming process does not go on answering with the old text.
func (j *Journal) renameCached(e, owner Entity, old, text string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	switch e.Type {
	case TypeField:
		delete(j.fields, old)
		j.fields[text] = e
		j.fieldNames[e] = text
	case TypeTerm:
		if j.vocab[owner] != nil {
			delete(j.vocab[owner], old)
			j.vocab[owner][text] = e
		}
		j.terms[e] = TermInfo{Term: e, Field: owner, Text: text}
	}
}

// fieldOwner is the presence-index owner the column *names* are interned
// under -- {log, SchemaPage, TypeField, 0}. Id 0 is never allocated to a
// real entity (Allocate starts at 1), so this is a free, self-describing
// slot rather than a reserved constant: it reads as "the vocabulary of
// field names in this log".
func (j *Journal) fieldOwner() Entity {
	return Entity{Log: j.st.Log, Page: SchemaPage, Type: TypeField, ID: 0}
}

// errAlreadyInterned is how the allocation callback below reports that
// somebody else interned this text first -- returned instead of ops, so
// no entity is allocated and no counter is bumped.
type errAlreadyInterned struct{ entity Entity }

func (e errAlreadyInterned) Error() string {
	return fmt.Sprintf("relations: this text is already interned as %s", e.entity)
}

// bucket reads text's presence bucket under owner: the raw stored value
// (needed verbatim as a compare precondition when appending to it) and
// the entities it currently lists.
func (j *Journal) bucket(ctx context.Context, owner Entity, text string) (raw []byte, candidates []Entity, err error) {
	raw, found, err := j.st.get(ctx, PresenceKey(owner, text))
	if err != nil || !found {
		return nil, nil, err
	}
	rec, _, err := DecodeRecord(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("relations: presence bucket for %q under %s: %w", text, owner, err)
	}
	if len(rec.Data)%EntityLen != 0 {
		return nil, nil, fmt.Errorf("relations: presence bucket for %q under %s holds %d bytes, not a whole number of entities",
			text, owner, len(rec.Data))
	}
	for off := 0; off < len(rec.Data); off += EntityLen {
		e, err := DecodeEntity(rec.Data[off:])
		if err != nil {
			return nil, nil, err
		}
		candidates = append(candidates, e)
	}
	return raw, candidates, nil
}

// resolveBucket returns the entity in text's bucket whose declared name
// really is text. A bucket is four bytes of hash, so it can list more
// than one entity; confirming the name is what keeps a collision from
// silently returning the wrong value. Every candidate it reads is
// cached along the way, since resolving one is exactly the read
// resolveTerm would otherwise pay for later.
func (j *Journal) resolveBucket(ctx context.Context, owner Entity, text string, candidates []Entity) (Entity, bool, error) {
	for _, candidate := range candidates {
		decl, found, err := j.st.Declaration(ctx, candidate)
		if err != nil {
			return Zero, false, err
		}
		if !found {
			return Zero, false, fmt.Errorf("relations: presence index names %s, which is not declared", candidate)
		}
		if candidate.Type == TypeTerm {
			j.rememberTerm(TermInfo{Term: candidate, Field: owner, Text: decl.Record.Name})
		}
		if decl.Record.Name == text {
			return candidate, true, nil
		}
	}
	return Zero, false, nil
}

// intern is find-or-create by text, and the reason a value in this store
// exists exactly once.
//
// The lookup is a point read of text's presence bucket (plus one read
// per candidate in it to confirm the name), not a scan of owner's whole
// vocabulary. The create is one atomic Apply holding the entity's
// declaration, whatever link the caller adds, the id-counter bump, *and*
// a precondition on the bucket's current bytes -- absent if the bucket
// is new, exact-match if this text is being added to an existing one. So
// two processes interning the same new text cannot both succeed: the
// loser's precondition fails, Store.AllocateWith retries, and on the
// retry the callback finds the winner's entity and reports it via
// errAlreadyInterned rather than allocating a second one.
//
// The retry is what makes the loser cheap: it costs one refused
// transaction and one re-read, and no id is consumed, because the
// callback runs before anything is applied.
func (j *Journal) intern(ctx context.Context, owner Entity, typ uint8, text string, link func(Entity) ([]Op, error)) (Entity, error) {
	_, candidates, err := j.bucket(ctx, owner, text)
	if err != nil {
		return Zero, err
	}
	if e, found, err := j.resolveBucket(ctx, owner, text, candidates); err != nil {
		return Zero, err
	} else if found {
		return e, nil
	}

	e, err := j.st.AllocateWith(ctx, SchemaPage, typ, KindDeclaration, text, nil,
		func(e Entity) ([]Op, error) {
			raw, candidates, err := j.bucket(ctx, owner, text)
			if err != nil {
				return nil, err
			}
			if existing, found, err := j.resolveBucket(ctx, owner, text, candidates); err != nil {
				return nil, err
			} else if found {
				return nil, errAlreadyInterned{entity: existing}
			}

			key := PresenceKey(owner, text)
			pre := Op{Kind: OpCompareAbsent, Key: key}
			if raw != nil {
				pre = Op{Kind: OpCompare, Key: key, Value: raw}
			}
			value, err := j.st.encode(key, KindPresence, "", packEntities(append(candidates, e)))
			if err != nil {
				return nil, err
			}

			ops := []Op{pre, {Kind: OpSet, Key: key, Value: value}}
			if link != nil {
				more, err := link(e)
				if err != nil {
					return nil, err
				}
				ops = append(ops, more...)
			}
			return ops, nil
		})
	if err != nil {
		var interned errAlreadyInterned
		if errors.As(err, &interned) {
			return interned.entity, nil
		}
		return Zero, err
	}
	return e, nil
}

// Cell is one column of one entry, as handed to Append. Build one with
// TermCell (a dictionary value) or QuantityCell (a number).
type Cell struct {
	Field  Entity
	Text   string
	Number int64
	// Numeric and Free say what kind of cell this is: a number, free
	// text, or -- with neither set -- a value from the field's
	// vocabulary. At most one may be set.
	Numeric  bool
	Free     bool
	resolved Entity // the interned term, filled in by Append
}

// TermCell is a cell whose value comes from its field's vocabulary --
// the normal case, and the one that costs four bytes per line no matter
// how long the text is.
func TermCell(field Entity, text string) Cell {
	return Cell{Field: field, Text: text}
}

// QuantityCell is a cell holding a number rather than a term. See
// KindQuantity for why numbers are not interned.
func QuantityCell(field Entity, n int64) Cell {
	return Cell{Field: field, Number: n, Numeric: true}
}

// RemarkCell is a cell holding free text: the remarks column every real
// log book has, where somebody writes "bearing sounded rough" and no
// vocabulary could have anticipated it.
//
// It is the one place this package stores text without interning it, and
// that is the point rather than an oversight: a remark is prose about
// one line, not a value shared between lines, so two lines that happen
// to say the same thing store it twice. Everything else holds -- the
// text is signed with the line, covered by the line's chain digest, and
// never rewritten -- but a remarks column is not a dictionary and the
// "stored exactly once" property does not apply to it.
//
// Use a term (TermCell) whenever the thing being written down is
// actually one of a known set. Reach for this only for the genuinely
// unforeseen.
func RemarkCell(field Entity, text string) Cell {
	return Cell{Field: field, Text: text, Free: true}
}

// Append writes one line of the log: it interns every term cell (each in
// its own Apply, since the dictionary outlives the line), then allocates
// the entry and writes its declaration and every cell -- forward records
// and index mirrors alike -- in a single atomic Apply. A reader
// therefore never sees a half-written line, only a line or nothing.
//
// Entries are allocated from FirstEntryPage upward, 255 to a page, so
// the entity's page and id bytes read as the page and line number of the
// book.
func (j *Journal) Append(ctx context.Context, cells ...Cell) (Entity, error) {
	return j.appendLine(ctx, cells, nil)
}

// appendLine is Append with room for more ops in the same transaction --
// what Correct adds its supersedes link and status marker through, so a
// correction and the line it corrects can never half-exist.
func (j *Journal) appendLine(ctx context.Context, cells []Cell, extra func(Entity) ([]Op, error)) (Entity, error) {
	if len(cells) == 0 {
		return Zero, fmt.Errorf("relations: append: an entry needs at least one cell")
	}
	seen := make(map[Entity]bool, len(cells))
	for i := range cells {
		c := &cells[i]
		if c.Field.Type != TypeField {
			return Zero, fmt.Errorf("relations: append: %s is not a field", c.Field)
		}
		if seen[c.Field] {
			return Zero, fmt.Errorf("relations: append: field %s given twice in one entry", c.Field)
		}
		seen[c.Field] = true
		if c.Numeric && c.Free {
			return Zero, fmt.Errorf("relations: append: cell for field %s is both a number and free text", c.Field)
		}
		if c.Numeric {
			continue
		}
		if c.Free {
			if c.Text == "" {
				return Zero, fmt.Errorf("relations: append: remark for field %s must not be empty", c.Field)
			}
			continue
		}
		term, err := j.Term(ctx, c.Field, c.Text)
		if err != nil {
			return Zero, err
		}
		c.resolved = term
	}

	return j.st.AllocateWith(ctx, FirstEntryPage, TypeEntry, KindDeclaration, "", nil,
		func(entry Entity) ([]Op, error) {
			ops, err := j.cellOps(entry, cells)
			if err != nil {
				return nil, err
			}
			if extra != nil {
				more, err := extra(entry)
				if err != nil {
					return nil, err
				}
				ops = append(ops, more...)
			}
			// Last, and in the same transaction: the chain events
			// this line makes -- the line itself, plus the strike a
			// correction puts on the line it replaces (see chain.go).
			chain, err := j.chainEventOps(ctx, entry, ops)
			if err != nil {
				return nil, err
			}
			return append(ops, chain...), nil
		})
}

// cellOps builds the relations that make up one line: a KindCell link
// per dictionary value and a KindQuantity link per number.
func (j *Journal) cellOps(entry Entity, cells []Cell) ([]Op, error) {
	var ops []Op
	for _, c := range cells {
		var (
			target Entity
			kind   byte
			data   []byte
		)
		switch {
		case c.Numeric:
			target, kind = c.Field, KindQuantity
			data = binary.BigEndian.AppendUint64(nil, uint64(c.Number))
		case c.Free:
			target, kind = c.Field, KindRemark
			data = []byte(c.Text)
		default:
			target, kind = c.resolved, KindCell
		}
		cellOps, err := j.st.LinkOps(entry, target, kind, data)
		if err != nil {
			return nil, err
		}
		ops = append(ops, cellOps...)
	}
	return ops, nil
}

// RowCell is one column of an entry as read back: what column, what
// value, and the term entity behind it (Zero for a quantity).
type RowCell struct {
	Field     Entity
	FieldName string
	Term      Entity
	Text      string
	Number    int64
	Numeric   bool
	// Free marks a remark: Text is what was written, and Term is Zero
	// because no dictionary entry stands behind it.
	Free bool
}

// Row reads one entry back into resolved columns, following each cell's
// reference to the term that holds its text and each term's reference to
// the field it belongs to. Everything it resolves is cached, so reading
// a whole page costs one scan per entry plus one read per *newly seen*
// term, not per cell.
func (j *Journal) Row(ctx context.Context, entry Entity) ([]RowCell, error) {
	if entry.Type != TypeEntry {
		return nil, fmt.Errorf("relations: row: %s is not an entry", entry)
	}
	rels, err := j.st.Relations(ctx, entry)
	if err != nil {
		return nil, err
	}
	row := make([]RowCell, 0, len(rels))
	for _, rel := range rels {
		switch rel.Record.Kind {
		case KindCell:
			info, err := j.resolveTerm(ctx, rel.B)
			if err != nil {
				return nil, err
			}
			name, err := j.fieldName(ctx, info.Field)
			if err != nil {
				return nil, err
			}
			row = append(row, RowCell{Field: info.Field, FieldName: name, Term: info.Term, Text: info.Text})
		case KindRemark:
			name, err := j.fieldName(ctx, rel.B)
			if err != nil {
				return nil, err
			}
			row = append(row, RowCell{
				Field:     rel.B,
				FieldName: name,
				Text:      string(rel.Record.Data),
				Free:      true,
			})
		case KindQuantity:
			if len(rel.Record.Data) != 8 {
				return nil, fmt.Errorf("relations: row: quantity on %s has %d bytes, want 8", rel.B, len(rel.Record.Data))
			}
			name, err := j.fieldName(ctx, rel.B)
			if err != nil {
				return nil, err
			}
			row = append(row, RowCell{
				Field:     rel.B,
				FieldName: name,
				Number:    int64(binary.BigEndian.Uint64(rel.Record.Data)),
				Numeric:   true,
			})
		}
	}
	return row, nil
}

// EntriesWith returns every entry that used term, newest last -- one
// scan of the index namespace, no matter how many pages the log has.
// This is the question a paper log answers by reading the whole book.
//
// A term used on every line of a large book has a correspondingly large
// answer; EntriesWithIn bounds it.
func (j *Journal) EntriesWith(ctx context.Context, term Entity) ([]Entity, error) {
	return j.EntriesWithIn(ctx, term, Range{})
}

// EntriesWithIn is EntriesWith bounded by r, whose From/To are entry
// entities -- so it reads only the pages asked for rather than filtering
// the whole book. Pair it with EntryPages for a page window, or with
// Range.Resume to page through a term's lines a batch at a time:
//
//	r := relations.Range{Limit: 100}
//	for {
//		lines, err := j.EntriesWithIn(ctx, term, r)
//		// ... use lines ...
//		next, ok := r.Resume(lines[len(lines)-1])
//		if !ok || len(lines) < r.Limit {
//			break
//		}
//		r = next
//	}
//
// The Limit bounds relations read, not entries returned: a bounded scan
// yields at most Limit records, of which only the cell relations become
// entries. In a journal every backlink of a term is a cell, so the two
// coincide -- but a caller that links its own relations to a term should
// expect a short page rather than a wrong one.
func (j *Journal) EntriesWithIn(ctx context.Context, term Entity, r Range) ([]Entity, error) {
	rels, err := j.st.BacklinksRange(ctx, term, r)
	if err != nil {
		return nil, err
	}
	var entries []Entity
	for _, rel := range OfKind(rels, KindCell) {
		entries = append(entries, rel.A)
	}
	return entries, nil
}

// EntryPages is the Range covering the lines on pages [from, to] of this
// log. For entries, page order is the order they were written, so this
// is also how a time window is expressed: "what this term was used for
// while the book was on pages 5 to 7".
func (j *Journal) EntryPages(from, to uint8) Range {
	return Range{
		From: Entity{Log: j.st.Log, Page: from, Type: TypeEntry, ID: 0x00},
		To:   Entity{Log: j.st.Log, Page: to, Type: TypeEntry, ID: 0xFF},
	}
}

// Vocabulary returns every term declared for field, in allocation order.
func (j *Journal) Vocabulary(ctx context.Context, field Entity) ([]TermInfo, error) {
	rels, err := j.st.Backlinks(ctx, field)
	if err != nil {
		return nil, err
	}
	terms := OfKind(rels, KindTermOf)
	out := make([]TermInfo, 0, len(terms))
	for _, rel := range terms {
		decl, found, err := j.st.Declaration(ctx, rel.A)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("relations: vocabulary: term %s is linked to %s but not declared", rel.A, field)
		}
		info := TermInfo{Term: rel.A, Field: field, Text: decl.Record.Name}
		j.rememberTerm(info)
		out = append(out, info)
	}
	return out, nil
}

// Page returns the entries on one page of the log, in line order.
func (j *Journal) Page(ctx context.Context, page uint8) ([]Entity, error) {
	decls, err := j.st.ListPage(ctx, page, TypeEntry)
	if err != nil {
		return nil, err
	}
	entries := make([]Entity, 0, len(decls))
	for _, d := range decls {
		entries = append(entries, d.A)
	}
	return entries, nil
}

// Fields returns every column this log has declared.
func (j *Journal) Fields(ctx context.Context) ([]Entity, error) {
	decls, err := j.st.List(ctx, SchemaPage, TypeField)
	if err != nil {
		return nil, err
	}
	out := make([]Entity, 0, len(decls))
	for _, d := range decls {
		out = append(out, d.A)
		j.mu.Lock()
		j.fields[d.Record.Name] = d.A
		j.fieldNames[d.A] = d.Record.Name
		j.mu.Unlock()
	}
	return out, nil
}

func (j *Journal) cachedField(name string) (Entity, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e, ok := j.fields[name]
	return e, ok
}

func (j *Journal) cachedTerm(field Entity, text string) (Entity, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e, ok := j.vocab[field][text]
	return e, ok
}

func (j *Journal) rememberTerm(info TermInfo) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.vocab[info.Field] == nil {
		j.vocab[info.Field] = make(map[string]Entity)
	}
	j.vocab[info.Field][info.Text] = info.Term
	j.terms[info.Term] = info
}

// resolveTerm reads a term's text and its field, caching both. A term is
// two reads the first time it is seen (its declaration, and the
// KindTermOf relation naming its field) and free thereafter.
func (j *Journal) resolveTerm(ctx context.Context, term Entity) (TermInfo, error) {
	j.mu.Lock()
	info, ok := j.terms[term]
	j.mu.Unlock()
	if ok {
		return info, nil
	}

	decl, found, err := j.st.Declaration(ctx, term)
	if err != nil {
		return TermInfo{}, err
	}
	if !found {
		return TermInfo{}, fmt.Errorf("relations: term %s is referenced but not declared", term)
	}
	rels, err := j.st.Relations(ctx, term)
	if err != nil {
		return TermInfo{}, err
	}
	owners := OfKind(rels, KindTermOf)
	if len(owners) == 0 {
		return TermInfo{}, fmt.Errorf("relations: term %s belongs to no field", term)
	}
	info = TermInfo{Term: term, Field: owners[0].B, Text: decl.Record.Name}
	j.rememberTerm(info)
	return info, nil
}

func (j *Journal) fieldName(ctx context.Context, field Entity) (string, error) {
	j.mu.Lock()
	name, ok := j.fieldNames[field]
	j.mu.Unlock()
	if ok {
		return name, nil
	}
	decl, found, err := j.st.Declaration(ctx, field)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("relations: field %s is referenced but not declared", field)
	}
	j.mu.Lock()
	j.fields[decl.Record.Name] = field
	j.fieldNames[field] = decl.Record.Name
	j.mu.Unlock()
	return decl.Record.Name, nil
}
