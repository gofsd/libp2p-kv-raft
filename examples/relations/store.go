package relations

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"
)

// TypeAllocator is the one entity type this package reserves for itself:
// an allocator counter (see Store.Allocate) lives at
// {Log, Page, TypeAllocator, <the type it counts>}, so the bookkeeping
// for a page's id space is itself an ordinary declaration in the same
// 9-byte format as everything else -- nothing outside this key layout
// has to exist for allocation to work. Applications may use any other
// type byte.
const TypeAllocator uint8 = 0xFF

// allocRetries bounds how many times Allocate re-reads and retries after
// losing an allocation race. Losing more than a handful in a row means
// real contention on one page, which the caller should hear about rather
// than have hidden behind an unbounded loop.
const allocRetries = 8

// ErrTypeSpaceFull is returned when every page from the requested one up
// to 255 has exhausted its 255 ids for a type. Within one log byte that
// is ~65k entities of that type; see the README's "Improving this" for
// what to do about it (the short version: it is what the Log byte is
// for).
var ErrTypeSpaceFull = errors.New("relations: no free id left for this type")

// Store is the entity/relation store: it turns Declare/Link/Relations
// calls into the 9-byte keys entity.go defines and the signed records
// record.go defines, over any Backend.
//
// It is safe for concurrent use to exactly the degree the Backend is:
// nothing here caches, and the one read-modify-write it performs
// (Allocate) is guarded by a compare precondition rather than a lock, so
// two processes racing on the same page produce two distinct entities,
// not one lost write.
type Store struct {
	be Backend

	// Log is the log byte stamped into every entity this store
	// allocates -- which log book it is writing.
	Log uint8
	// Author is the entity recorded as the writer of every record this
	// store creates. Its own declaration should carry the matching
	// public key (see DeclareActor), which is what Verify resolves.
	Author Entity
	// Now, if set, replaces time.Now as the source of every record's
	// Created stamp -- the seam a test uses to get deterministic
	// timestamps.
	Now func() time.Time

	priv ed25519.PrivateKey
}

// New returns a Store writing into be, stamping log into every entity it
// allocates and author plus a signature from priv into every record it
// writes. priv may be nil, in which case records are written unsigned
// (Verify then reports ErrUnsigned) -- useful when working out a schema,
// not when replacing a log people sign.
func New(be Backend, log uint8, author Entity, priv ed25519.PrivateKey) *Store {
	return &Store{be: be, Log: log, Author: author, priv: priv}
}

// Backend exposes the underlying store, for a caller that wants to audit
// raw keys (see journal_test.go) or share one Backend across several
// Stores.
func (s *Store) Backend() Backend { return s.be }

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Relation is one stored record together with the two entities it
// relates. A is always the source and B the target, whichever namespace
// the record was read from -- Backlinks flips the pair back so a caller
// never has to think about which physical key it came off.
//
// A declaration reads back as a Relation with B == Zero.
type Relation struct {
	A      Entity
	B      Entity
	Record Record

	// key/unsigned are what this record's signature covers -- kept so
	// Verify needs no re-encode and no second read. key is the physical
	// key the record was read from, which for a backlink is the index
	// key, not the forward one. value is the whole stored value, which
	// the hash chain digests verbatim (see chain.go).
	key      []byte
	unsigned []byte
	value    []byte
}

// Key returns the physical store key this relation was read from.
func (r Relation) Key() []byte { return append([]byte(nil), r.key...) }

// Value returns the record exactly as stored -- what a digest over this
// relation covers, and what a caller re-encoding it would have to
// reproduce byte for byte.
func (r Relation) Value() []byte { return append([]byte(nil), r.value...) }

// Declare writes e's own record: its name, this store's author and
// signature, and the current time, at (e, Zero). It overwrites any
// existing declaration of e -- an entity is declared once in practice,
// and a re-Declare is how a name is corrected.
func (s *Store) Declare(ctx context.Context, e Entity, kind byte, name string, data []byte) error {
	ops, err := s.DeclareOps(e, kind, name, data, time.Time{})
	if err != nil {
		return err
	}
	return s.be.Apply(ctx, ops)
}

// DeclareOps builds the op Declare would apply, without applying it --
// for a caller folding a declaration into a larger transaction (see
// Journal.Rename).
//
// created replaces the store's clock when it is non-zero, which is how a
// record can be rewritten without losing when it was first written. Use
// it only for that: an ordinary declaration should be stamped by the
// store, not by its caller.
func (s *Store) DeclareOps(e Entity, kind byte, name string, data []byte, created time.Time) ([]Op, error) {
	if e.IsZero() {
		return nil, fmt.Errorf("relations: declare: the zero entity is the declaration placeholder, not an entity")
	}
	if created.IsZero() {
		created = s.now()
	}
	key := DeclarationKey(e)
	value, err := s.encodeAt(key, kind, name, data, created)
	if err != nil {
		return nil, err
	}
	return []Op{{Kind: OpSet, Key: key, Value: value}}, nil
}

// DeclareActor declares e as a writer of records, with pub as the public
// key every record naming e as its Author is verified against. Storing
// the key once, here, is why a Record carries a 4-byte author reference
// instead of a 32-byte key copied onto every line of the log.
func (s *Store) DeclareActor(ctx context.Context, e Entity, name string, pub ed25519.PublicKey) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("relations: declare actor: public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	return s.Declare(ctx, e, KindDeclaration, name, append([]byte(nil), pub...))
}

// Allocate declares a new entity of type typ on the first page at or
// after page that still has a free id, and returns it. The id counter
// for (page, typ) is itself a declaration at
// {Log, page, TypeAllocator, typ}, and the whole allocation -- read the
// counter, bump it, write the new entity's declaration -- goes out as
// one atomic Apply guarded by a compare precondition on the counter's
// current bytes. Two processes allocating at once therefore cannot get
// the same id: the loser's compare fails, it re-reads and takes the next
// one.
//
// A page whose counter has reached 255 is skipped, so a type spills onto
// page+1, page+2 and so on; when everything up to page 255 is full this
// returns ErrTypeSpaceFull.
func (s *Store) Allocate(ctx context.Context, page, typ uint8, kind byte, name string, data []byte) (Entity, error) {
	return s.AllocateWith(ctx, page, typ, kind, name, data, nil)
}

// AllocateWith is Allocate with more work folded into the same atomic
// Apply: extra is called with the entity about to be allocated and
// returns further ops -- typically the LinkOps of everything that
// entity is born relating to -- which land in the same transaction as
// its declaration and the counter bump, or not at all.
//
// This is what lets Journal.Append write a whole line of the log
// atomically: allocating the line, declaring it, and attaching every one
// of its cells is one transaction, so no reader ever sees a half-written
// line. extra may be nil.
func (s *Store) AllocateWith(ctx context.Context, page, typ uint8, kind byte, name string, data []byte, extra func(Entity) ([]Op, error)) (Entity, error) {
	if typ == 0 {
		return Zero, fmt.Errorf("relations: allocate: type 0 is not usable (it would collide with the zero entity)")
	}
	if typ == TypeAllocator {
		return Zero, fmt.Errorf("relations: allocate: type %#x is reserved for allocator counters", TypeAllocator)
	}

	for attempt := 0; attempt < allocRetries; attempt++ {
		conflicted := false
		for p := int(page); p <= 0xFF; p++ {
			counter := Entity{Log: s.Log, Page: uint8(p), Type: TypeAllocator, ID: typ}
			counterKey := DeclarationKey(counter)
			raw, found, err := s.get(ctx, counterKey)
			if err != nil {
				return Zero, err
			}

			next := uint8(1)
			pre := Op{Kind: OpCompareAbsent, Key: counterKey}
			if found {
				used, flags, err := decodeCounter(counter, raw)
				if err != nil {
					return Zero, err
				}
				if flags&counterClosed != 0 {
					continue // this page was closed off; write on the next one
				}
				next = used
				if next == 0 {
					continue // this page is full for this type
				}
				pre = Op{Kind: OpCompare, Key: counterKey, Value: raw}
			}

			// An id whose declaration already exists was placed by hand
			// rather than handed out here (Declare and DeclareActor both
			// take an entity the caller chose), so the counter knows
			// nothing about it. Step over it: allocation must never
			// overwrite something already there, which would leave two
			// different things claiming one entity -- and if those were
			// actors, signatures would start verifying against the wrong
			// key.
			taken := false
			for ; next != 0; next++ {
				candidate := DeclarationKey(Entity{Log: s.Log, Page: uint8(p), Type: typ, ID: next})
				if _, exists, err := s.get(ctx, candidate); err != nil {
					return Zero, err
				} else if !exists {
					break
				}
				taken = true
			}
			if next == 0 {
				// Every id from here to 255 is spoken for. The counter is
				// left where it is; the page loop moves on.
				if taken {
					continue
				}
			}

			// 255 is the last usable id, so bumping past it stores 0 --
			// the "full" marker the read above skips on.
			var after uint8
			if next < 0xFF {
				after = next + 1
			}
			counterValue, err := s.encode(counterKey, KindDeclaration, "", []byte{after, 0})
			if err != nil {
				return Zero, err
			}

			e := Entity{Log: s.Log, Page: uint8(p), Type: typ, ID: next}
			declKey := DeclarationKey(e)
			declValue, err := s.encode(declKey, kind, name, data)
			if err != nil {
				return Zero, err
			}

			ops := []Op{
				pre,
				// ...and the id itself must still be free at the moment
				// of writing, so the skip above cannot be raced by
				// somebody declaring that entity by hand in between.
				{Kind: OpCompareAbsent, Key: declKey},
				{Kind: OpSet, Key: counterKey, Value: counterValue},
				{Kind: OpSet, Key: declKey, Value: declValue},
			}
			if extra != nil {
				more, err := extra(e)
				if err != nil {
					return Zero, err
				}
				ops = append(ops, more...)
			}

			err = s.be.Apply(ctx, ops)
			if err == nil {
				return e, nil
			}
			if !errors.Is(err, ErrConflict) {
				return Zero, err
			}
			conflicted = true
			break // somebody else moved the counter -- re-read from scratch
		}
		if !conflicted {
			return Zero, fmt.Errorf("%w: type %#x, pages %d..255 of log %d", ErrTypeSpaceFull, typ, page, s.Log)
		}
	}
	return Zero, fmt.Errorf("relations: allocate: lost %d allocation races in a row on type %#x", allocRetries, typ)
}

// Link records the relation a -> b, writing it under (a, b) and its
// mirror under (b, a) in one atomic Apply -- so a half-written relation
// visible in only one direction is not a state this store can be in.
// Each copy is signed for its own key (see Record.Encode), so neither
// can be replayed as the other.
//
// It overwrites an existing a -> b relation. That is deliberate: a
// relation is identified by its endpoints, and re-linking with a
// different kind or payload is how it gets corrected.
func (s *Store) Link(ctx context.Context, a, b Entity, kind byte, data []byte) error {
	ops, err := s.LinkOps(a, b, kind, data)
	if err != nil {
		return err
	}
	return s.be.Apply(ctx, ops)
}

// LinkOps builds the two ops Link would apply, without applying them --
// for a caller assembling a larger transaction (see AllocateWith).
func (s *Store) LinkOps(a, b Entity, kind byte, data []byte) ([]Op, error) {
	if a.IsZero() || b.IsZero() {
		return nil, fmt.Errorf("relations: link: neither side may be the zero entity (use Declare for %s)", a)
	}
	if a == b {
		return nil, fmt.Errorf("relations: link: %s cannot relate to itself", a)
	}
	fwdKey, revKey := RelationKey(a, b), IndexKey(a, b)
	fwd, err := s.encode(fwdKey, kind, "", data)
	if err != nil {
		return nil, err
	}
	rev, err := s.encode(revKey, kind, "", data)
	if err != nil {
		return nil, err
	}
	return []Op{
		{Kind: OpSet, Key: fwdKey, Value: fwd},
		{Kind: OpSet, Key: revKey, Value: rev},
	}, nil
}

// Unlink removes the relation a -> b and its mirror, again atomically.
func (s *Store) Unlink(ctx context.Context, a, b Entity) error {
	return s.be.Apply(ctx, []Op{
		{Kind: OpDelete, Key: RelationKey(a, b)},
		{Kind: OpDelete, Key: IndexKey(a, b)},
	})
}

// Declaration reads e's own record, reporting whether it exists at all.
func (s *Store) Declaration(ctx context.Context, e Entity) (Relation, bool, error) {
	return s.read(ctx, DeclarationKey(e), e, Zero)
}

// Lookup reads the single relation a -> b.
func (s *Store) Lookup(ctx context.Context, a, b Entity) (Relation, bool, error) {
	return s.read(ctx, RelationKey(a, b), a, b)
}

// Range bounds a relation scan: which far entities to include, and how
// many to return.
//
// The zero Range means everything, unbounded -- which is what Relations
// and Backlinks pass. To is inclusive like From, and the zero entity as
// To means "no upper bound" rather than "up to Zero": Zero is the
// declaration placeholder and never a relation's endpoint, so there is
// no real range it could otherwise express.
//
// Two scans this is for, beyond simple paging. Narrowing the far entity
// is a *sub-range scan*, not a filter over everything, because both
// namespaces order relations by the far entity's own (log, page, type,
// id) bytes -- so "this term's lines on pages 5 to 7" reads only those
// pages (see Journal.EntryPages), and "a's relations to entities of one
// type on one page" is likewise contiguous.
type Range struct {
	// From is the inclusive lower bound on the far entity. The zero
	// value starts at the beginning.
	From Entity
	// To is the inclusive upper bound. The zero value means no upper
	// bound.
	To Entity
	// Limit caps how many relations come back; 0 means unlimited.
	Limit int
}

// upper is To with the zero value resolved to "everything".
func (r Range) upper() Entity {
	if r.To.IsZero() {
		return maxEntity
	}
	return r.To
}

// Resume returns the Range that continues just past last -- the far
// entity of the final relation a page returned (Relation.B for a
// forward scan, Relation.A for a backlink scan) -- and false when there
// is nothing left to read, either because last is the last entity there
// is or because it already reached this Range's upper bound.
//
// Resuming from an entity rather than a raw key is what makes a cursor
// four bytes a caller can store anywhere, and what makes a resumed scan
// pick up correctly even if relations were added or removed in between.
func (r Range) Resume(last Entity) (Range, bool) {
	next, ok := last.Next()
	if !ok {
		return Range{}, false
	}
	if next.Compare(r.upper()) > 0 {
		return Range{}, false
	}
	r.From = next
	return r, true
}

// Relations returns every relation whose source is a, in ascending order
// of the target entity -- one contiguous range scan, a's own declaration
// excluded. Ordering is by the target's (log, page, type, id) bytes, so
// all of a's relations to entities of one type sit together within a
// page.
//
// This reads the whole set. Where that set can grow without bound -- a
// dictionary term used by every line of a large book, say -- use
// RelationsRange or BacklinksRange and page through it.
func (s *Store) Relations(ctx context.Context, a Entity) ([]Relation, error) {
	return s.RelationsRange(ctx, a, Range{})
}

// RelationsRange is Relations bounded by r.
func (s *Store) RelationsRange(ctx context.Context, a Entity, r Range) ([]Relation, error) {
	start, end := RelationBoundsIn(a, r.From, r.upper())
	pairs, err := s.be.Scan(ctx, start, end, r.Limit)
	if err != nil {
		return nil, err
	}
	return s.decodePairs(pairs, false)
}

// Backlinks returns every relation whose *target* is b -- the same
// records, read out of the index namespace, with A and B put back the
// way they were written. This is the query the mirrored write exists
// for: "every log entry that used this dictionary term", "every unit
// produced from this one", in one prefix scan instead of a sweep.
func (s *Store) Backlinks(ctx context.Context, b Entity) ([]Relation, error) {
	return s.BacklinksRange(ctx, b, Range{})
}

// BacklinksRange is Backlinks bounded by r, where r's bounds are on the
// *source* entity -- see Range for why narrowing that is a sub-range
// scan rather than a filter.
func (s *Store) BacklinksRange(ctx context.Context, b Entity, r Range) ([]Relation, error) {
	start, end := BacklinkBoundsIn(b, r.From, r.upper())
	pairs, err := s.be.Scan(ctx, start, end, r.Limit)
	if err != nil {
		return nil, err
	}
	return s.decodePairs(pairs, true)
}

// ListPage returns the declarations of every entity of type typ on one
// page, in id order. This is the scan the entity byte order exists for:
// because Type sits above ID, one type's entities are a contiguous run
// inside a page, so listing them needs no index and no filter beyond
// dropping the relation records that share the same range (the ones
// whose B is not Zero).
func (s *Store) ListPage(ctx context.Context, page, typ uint8) ([]Relation, error) {
	start, end := TypeBounds(s.Log, page, typ)
	pairs, err := s.be.Scan(ctx, start, end, 0)
	if err != nil {
		return nil, err
	}
	rels, err := s.decodePairs(pairs, false)
	if err != nil {
		return nil, err
	}
	decls := make([]Relation, 0, len(rels))
	for _, rel := range rels {
		if rel.B.IsZero() {
			decls = append(decls, rel)
		}
	}
	return decls, nil
}

// List is ListPage across every page from fromPage upward that holds
// entities of this type. It stops at the first page with no allocator
// counter for typ, which is exactly the first unused one: Allocate fills
// pages in order, so a gap cannot appear behind it.
func (s *Store) List(ctx context.Context, fromPage, typ uint8) ([]Relation, error) {
	var all []Relation
	for p := int(fromPage); p <= 0xFF; p++ {
		counter := Entity{Log: s.Log, Page: uint8(p), Type: TypeAllocator, ID: typ}
		if _, found, err := s.get(ctx, DeclarationKey(counter)); err != nil {
			return nil, err
		} else if !found {
			break
		}
		page, err := s.ListPage(ctx, uint8(p), typ)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
	}
	return all, nil
}

// counterClosed is the allocator counter's one flag: this page takes no
// more entities of this type, even though it has room. Closing a page
// early is how a log book is ruled off (see Journal.SignOffPage) -- the
// flag lives here, rather than being a journal-level check, so that
// Allocate skips a closed page without knowing what a sign-off is.
const counterClosed byte = 1 << 0

// decodeCounter reads an allocator counter's payload: the next free id
// (0 meaning the page is full) and its flags. One-byte payloads are read
// as flagless, which is what every counter written before this flag
// existed looks like.
func decodeCounter(counter Entity, raw []byte) (next, flags byte, err error) {
	rec, _, err := DecodeRecord(raw)
	if err != nil {
		return 0, 0, fmt.Errorf("relations: counter %s: %w", counter, err)
	}
	switch len(rec.Data) {
	case 1:
		return rec.Data[0], 0, nil
	case 2:
		return rec.Data[0], rec.Data[1], nil
	default:
		return 0, 0, fmt.Errorf("relations: counter %s has a %d-byte payload, want 1 or 2", counter, len(rec.Data))
	}
}

// LastAllocated returns the highest id Allocate has handed out for typ
// on page -- 0 if it has never allocated one there. It reads the same
// counter record Allocate itself keeps, so it needs no scan of the
// entities themselves.
//
// This is what lets a caller walk a type's entities by id without
// scanning for them (see Journal.VerifyChain, which has to know which
// lines *should* exist before it can notice one that no longer does).
// A page closed early still reports what it actually holds, which is why
// closing is a flag rather than a fake "full" marker.
func (s *Store) LastAllocated(ctx context.Context, page, typ uint8) (uint8, error) {
	last, _, err := s.PageAllocated(ctx, page, typ)
	return last, err
}

// PageAllocated is LastAllocated plus whether the page has a counter at
// all. The two are worth telling apart: a page can hold no entities and
// still have been reached -- an empty page closed off by a sign-off has
// a counter saying so -- so "no lines here" is not the same as "the log
// stops here", and a walk over the pages that confuses them stops early.
func (s *Store) PageAllocated(ctx context.Context, page, typ uint8) (last uint8, exists bool, err error) {
	counter := Entity{Log: s.Log, Page: page, Type: TypeAllocator, ID: typ}
	raw, found, err := s.get(ctx, DeclarationKey(counter))
	if err != nil || !found {
		return 0, false, err
	}
	next, _, err := decodeCounter(counter, raw)
	if err != nil {
		return 0, false, err
	}
	// The counter holds the *next* free id, and 0 is the "page full"
	// marker Allocate writes after handing out 255.
	if next == 0 {
		return 0xFF, true, nil
	}
	return next - 1, true, nil
}

// ClosePageOps builds the ops that close page to further entities of
// type typ: the counter keeps the id it was up to and gains the closed
// flag, under a compare on its current bytes so a concurrent allocation
// cannot slip in underneath. A page that has never been written to is
// closed by creating its counter outright.
//
// It returns ops rather than applying them because closing a page is
// never the whole of what a caller is doing -- see Journal.SignOffPage,
// which closes the page, records who closed it and chains the event in
// one transaction.
func (s *Store) ClosePageOps(ctx context.Context, page, typ uint8) ([]Op, error) {
	counter := Entity{Log: s.Log, Page: page, Type: TypeAllocator, ID: typ}
	counterKey := DeclarationKey(counter)
	raw, found, err := s.get(ctx, counterKey)
	if err != nil {
		return nil, err
	}

	next := uint8(1)
	pre := Op{Kind: OpCompareAbsent, Key: counterKey}
	if found {
		used, flags, err := decodeCounter(counter, raw)
		if err != nil {
			return nil, err
		}
		if flags&counterClosed != 0 {
			return nil, fmt.Errorf("relations: page %d of log %d is already closed for type %#x", page, s.Log, typ)
		}
		next = used
		pre = Op{Kind: OpCompare, Key: counterKey, Value: raw}
	}
	value, err := s.encode(counterKey, KindDeclaration, "", []byte{next, counterClosed})
	if err != nil {
		return nil, err
	}
	return []Op{pre, {Kind: OpSet, Key: counterKey, Value: value}}, nil
}

// PageIsClosed reports whether page takes no further entities of type
// typ.
func (s *Store) PageIsClosed(ctx context.Context, page, typ uint8) (bool, error) {
	counter := Entity{Log: s.Log, Page: page, Type: TypeAllocator, ID: typ}
	raw, found, err := s.get(ctx, DeclarationKey(counter))
	if err != nil || !found {
		return false, err
	}
	_, flags, err := decodeCounter(counter, raw)
	if err != nil {
		return false, err
	}
	return flags&counterClosed != 0, nil
}

// OfKind filters a Relations/Backlinks result to one relation kind. It
// is a plain slice filter, not a second query: a range scan already
// returned the values, so the kind byte is in hand and matching on it
// costs nothing.
func OfKind(rels []Relation, kind byte) []Relation {
	out := make([]Relation, 0, len(rels))
	for _, r := range rels {
		if r.Record.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// Verify checks rel's signature against the public key declared for its
// author, resolving that author's declaration through the store. Returns
// ErrUnsigned for a record written without a key, ErrBadSignature for
// one whose signature does not cover the key and body it is stored with.
func (s *Store) Verify(ctx context.Context, rel Relation) error {
	if len(rel.Record.Signature) == 0 {
		return ErrUnsigned
	}
	actor, found, err := s.Declaration(ctx, rel.Record.Author)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("relations: verify: author %s is not declared", rel.Record.Author)
	}
	return rel.Record.Verify(rel.key, rel.unsigned, actor.Record.Data)
}

// encode builds the stored value for key: the caller's kind/name/data
// plus this store's author, clock and signature.
func (s *Store) encode(key []byte, kind byte, name string, data []byte) ([]byte, error) {
	return s.encodeAt(key, kind, name, data, s.now())
}

// encodeAt is encode with an explicit timestamp -- see DeclareOps.
func (s *Store) encodeAt(key []byte, kind byte, name string, data []byte, created time.Time) ([]byte, error) {
	rec := Record{
		Kind:    kind,
		Author:  s.Author,
		Created: created,
		Name:    name,
		Data:    data,
	}
	return rec.Encode(key, s.priv)
}

// get reads one exact key, as a one-pair range scan. The read primitive
// this package is built on is a range scan, and a point read is just its
// degenerate case (start == end) -- which also means "missing" comes
// back as an empty result rather than an error string to match on.
func (s *Store) get(ctx context.Context, key []byte) ([]byte, bool, error) {
	pairs, err := s.be.Scan(ctx, key, key, 1)
	if err != nil {
		return nil, false, err
	}
	if len(pairs) == 0 {
		return nil, false, nil
	}
	return pairs[0].Value, true, nil
}

func (s *Store) read(ctx context.Context, key []byte, a, b Entity) (Relation, bool, error) {
	raw, found, err := s.get(ctx, key)
	if err != nil || !found {
		return Relation{}, false, err
	}
	rec, unsigned, err := DecodeRecord(raw)
	if err != nil {
		return Relation{}, false, fmt.Errorf("relations: read %x: %w", key, err)
	}
	return Relation{A: a, B: b, Record: rec, key: key, unsigned: unsigned, value: raw}, true, nil
}

// decodePairs turns a scan result into Relations. flip is set for an
// index-namespace scan, whose keys hold (target, source).
func (s *Store) decodePairs(pairs []Pair, flip bool) ([]Relation, error) {
	rels := make([]Relation, 0, len(pairs))
	for _, p := range pairs {
		_, first, second, err := ParseKey(p.Key)
		if err != nil {
			return nil, err
		}
		rec, unsigned, err := DecodeRecord(p.Value)
		if err != nil {
			return nil, fmt.Errorf("relations: decode %x: %w", p.Key, err)
		}
		a, b := first, second
		if flip {
			a, b = second, first
		}
		rels = append(rels, Relation{A: a, B: b, Record: rec, key: p.Key, unsigned: unsigned, value: p.Value})
	}
	return rels, nil
}
