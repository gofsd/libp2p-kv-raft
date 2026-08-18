// Package relations is a worked example, not part of this project's core
// library: it implements a general entity/relation store -- the shape a
// paper log book actually has -- on top of nothing but key/value
// primitives this repo already exposes (set, delete, compare, a bounded
// range scan, and the atomic multi-key txn that combines them). Nothing
// here needed a daemon/kvfsm/wire change to exist, which is why it lives
// beside examples/genealogy rather than in pkg/: every record it writes
// is an ordinary user-namespace Set, client-asserted at exactly the trust
// level examples/genealogy's own entries already have (see that package's
// doc comment). Copy the pattern into your own application rather than
// importing this as a dependency -- the type/kind vocabularies below are
// one opinionated shape among many, not a contract this project
// maintains.
//
// # The key
//
// Every record -- an entity's own declaration and every relation between
// two entities alike -- is one fixed-width 9-byte key:
//
//	[namespace 1][entity A 4][entity B 4]
//
// and every entity id is itself four bytes, most significant first:
//
//	[log][page][type][id]
//
// log is which log book (or which schema version of one) the entity
// belongs to, page is the page inside it, type is what kind of thing it
// is, and id is its ordinal on that page. Because pkg/store compares
// keys byte-wise (its SQLite kv table keys a BLOB column), that byte
// order is not cosmetic: it makes "everything on page P of log L",
// "every entity of type T on page P", and "every relation of entity A"
// each a single contiguous range scan, with no secondary index to
// maintain and no per-record length prefix to parse.
//
// An entity with all four bytes zero (Zero) is not an entity: it is the
// "nothing" placeholder that turns a relation key into a *declaration*
// of A itself -- Key(ns, A, Zero) is where A's name, author, timestamp
// and signature live. Declarations sort first inside A's own range
// (Zero is the smallest B), so one scan of A yields its declaration
// followed by all of its relations.
//
// # Both directions
//
// A relation is stored twice: (A, B) under NamespaceRelation and the
// mirrored (B, A) under NamespaceIndex, written in a single atomic txn
// so the pair can never half-exist. That is what makes both "what does A
// point at" and "what points at A" a prefix scan of the same fixed
// width, which is the whole reason to spend a second 9-byte key on it.
//
// # Why bytes instead of strings
//
// The point of the format is that a value is written once, in the
// declaration of the entity that holds it, and every later reference to
// it is four bytes. A paper log book repeats "Ivanov" on every line;
// this repeats a 4-byte id, and the operator's name exists in exactly
// one record in the store. See journal.go for that used as intended, and
// journal_test.go for the test that actually asserts no value is ever
// copied.
package relations

import (
	"crypto/sha256"
	"fmt"
)

// The namespace byte -- the first byte of every key this package writes.
//
// 0x00 (shmevent.SystemKeyPrefix) and 0x01 (logrecord.LogKeyPrefix) are
// reserved by the core and rejected outright for an ordinary Set (see
// pkg/daemon's rejectReservedKey), so an example may not use them. These
// two are ordinary user-namespace bytes: nothing in the daemon knows
// what they mean, which is exactly why this example needs no daemon
// change at all. A second, independent relation space would just be two
// more bytes.
const (
	// NamespaceRelation holds the records as written: A's declaration at
	// (A, Zero), and every relation A -> B at (A, B).
	NamespaceRelation byte = 0x10
	// NamespaceIndex holds the mirror of every relation, (B, A), so that
	// "who points at B" is a prefix scan instead of a full sweep. It
	// never holds declarations.
	NamespaceIndex byte = 0x11
	// NamespacePresence holds the dictionary's content-addressed index:
	// (owner, hash of the text) -> the entities declared with that text.
	// It is what makes interning a value both a point read and race-free
	// -- see PresenceKey and Journal.Term.
	NamespacePresence byte = 0x12
)

// EntityLen/KeyLen are the fixed widths this whole package is built
// around: no length prefixes, no separators, no escaping, and therefore
// no key that is ever a byte-prefix of another key.
const (
	EntityLen = 4
	KeyLen    = 1 + 2*EntityLen
)

// Entity is one four-byte entity id. The field order is the byte order:
// changing it changes what a range scan groups together (see the package
// doc comment), so treat it as wire format, not as a struct layout.
type Entity struct {
	// Log is which log book -- or, used the other way, which schema
	// version of one -- this entity belongs to. Bumping it gives a whole
	// new 24-bit id space whose keys sort entirely after the old one, so
	// an incompatible redefinition of what a type byte means never has
	// to rewrite a single existing record.
	Log uint8
	// Page is the page within the log. For entries it is literally the
	// page of the book; for everything else it is simply the high byte
	// of the id space, letting a type hold more than 255 entities by
	// spilling onto the next page (see Store.Allocate).
	Page uint8
	// Type is what kind of thing this is -- a field definition, a
	// dictionary term, a log entry (see journal.go's Type* constants).
	// It sits above ID so that one type's entities are contiguous within
	// a page.
	Type uint8
	// ID is the ordinal within (Log, Page, Type): the row number on the
	// page.
	ID uint8
}

// Zero is the all-zero entity: the second half of a declaration key, and
// never a real entity. A caller allocating entities through this package
// never gets it back (Store.Allocate starts at ID 1 and refuses a zero
// type), so "B is zero" unambiguously means "this record declares A".
var Zero = Entity{}

// IsZero reports whether e is the Zero placeholder.
func (e Entity) IsZero() bool { return e == Zero }

// Bytes returns e's four wire bytes, most significant first.
func (e Entity) Bytes() [EntityLen]byte {
	return [EntityLen]byte{e.Log, e.Page, e.Type, e.ID}
}

// String formats e as its four bytes in hex, log first -- the order they
// appear in a key, so a printed entity and a printed key line up.
func (e Entity) String() string {
	return fmt.Sprintf("%02x.%02x.%02x.%02x", e.Log, e.Page, e.Type, e.ID)
}

// Value is e as the 32-bit number its four bytes spell, most
// significant first -- the same order the store compares keys in, so
// comparing two entities' values and comparing their key bytes are the
// same thing.
func (e Entity) Value() uint32 {
	return uint32(e.Log)<<24 | uint32(e.Page)<<16 | uint32(e.Type)<<8 | uint32(e.ID)
}

// EntityFromValue is Value's inverse.
func EntityFromValue(v uint32) Entity {
	return Entity{Log: uint8(v >> 24), Page: uint8(v >> 16), Type: uint8(v >> 8), ID: uint8(v)}
}

// Compare orders e against o the way the store orders their keys:
// negative if e sorts first, zero if they are the same, positive if e
// sorts last.
func (e Entity) Compare(o Entity) int {
	switch a, b := e.Value(), o.Value(); {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// Next returns the entity immediately after e, and false if e is the
// last one there is. This is what makes a scan resumable without
// remembering a raw key: hand the last entity a page returned back as
// the next page's lower bound (see Range.Resume).
func (e Entity) Next() (Entity, bool) {
	v := e.Value()
	if v == ^uint32(0) {
		return Zero, false
	}
	return EntityFromValue(v + 1), true
}

// DecodeEntity reads the four bytes at the front of b.
func DecodeEntity(b []byte) (Entity, error) {
	if len(b) < EntityLen {
		return Zero, fmt.Errorf("relations: entity needs %d bytes, got %d", EntityLen, len(b))
	}
	return Entity{Log: b[0], Page: b[1], Type: b[2], ID: b[3]}, nil
}

// Key packs ns, a and b into the 9-byte key this package stores under.
func Key(ns byte, a, b Entity) []byte {
	k := make([]byte, KeyLen)
	k[0] = ns
	ab, bb := a.Bytes(), b.Bytes()
	copy(k[1:], ab[:])
	copy(k[1+EntityLen:], bb[:])
	return k
}

// ParseKey splits a stored key back into its namespace and its two
// entities. The two are returned in the order they appear in the key, so
// for a NamespaceIndex key the first is the relation's *target* -- see
// Store.Backlinks, which flips them back.
func ParseKey(k []byte) (ns byte, first, second Entity, err error) {
	if len(k) != KeyLen {
		return 0, Zero, Zero, fmt.Errorf("relations: key must be %d bytes, got %d", KeyLen, len(k))
	}
	first, err = DecodeEntity(k[1 : 1+EntityLen])
	if err != nil {
		return 0, Zero, Zero, err
	}
	second, err = DecodeEntity(k[1+EntityLen:])
	if err != nil {
		return 0, Zero, Zero, err
	}
	return k[0], first, second, nil
}

// DeclarationKey is where e's own record -- name, author, timestamp,
// signature -- lives: (e, Zero) in the relation namespace.
func DeclarationKey(e Entity) []byte { return Key(NamespaceRelation, e, Zero) }

// RelationKey is where the relation a -> b lives.
func RelationKey(a, b Entity) []byte { return Key(NamespaceRelation, a, b) }

// IndexKey is where the relation a -> b is mirrored for reverse lookup:
// the same record under (b, a) in the index namespace.
func IndexKey(a, b Entity) []byte { return Key(NamespaceIndex, b, a) }

// TextBucket is the four bytes a piece of text hashes to: the first four
// of its SHA-256. It is the second half of a PresenceKey, which is why
// it has to be exactly entity-width -- the presence index reuses the
// same 9-byte key shape as everything else rather than inventing a
// variable-length one, so that a key in this store is always nine bytes
// no matter which namespace it is in.
//
// Four bytes is a bucket, not an identity: two different texts can land
// in the same one (a birthday collision is likely somewhere past ~2^16
// distinct values in one owner's vocabulary). That is why a bucket holds
// a *list* of candidate entities and why every lookup confirms the
// candidate's declared name really is the text being looked for -- see
// Journal.Term. A cheaper scheme that trusted the hash would, at some
// point, silently return the wrong value for a line of a log.
//
// An all-zero hash is nudged to {0,0,0,1} so the Zero entity keeps its
// one meaning ("this record declares A") in every namespace.
func TextBucket(text string) Entity {
	sum := sha256.Sum256([]byte(text))
	e := Entity{Log: sum[0], Page: sum[1], Type: sum[2], ID: sum[3]}
	if e.IsZero() {
		e.ID = 1
	}
	return e
}

// PresenceKey is where the dictionary records which entities own a piece
// of text: (owner, TextBucket(text)) in the presence namespace. owner
// scopes the vocabulary -- a field entity for its terms, and the
// synthetic {log, SchemaPage, TypeField, 0} owner for the field names
// themselves (see Journal.fieldOwner), so two columns may each have a
// term reading "OK" without colliding.
func PresenceKey(owner Entity, text string) []byte {
	return Key(NamespacePresence, owner, TextBucket(text))
}

// maxEntity is the largest entity id there is -- the upper bound of
// every scan range below, since both bounds are inclusive (that is what
// the underlying listRange primitive takes, see
// shmclient.Session.ScanRange).
var maxEntity = Entity{Log: 0xFF, Page: 0xFF, Type: 0xFF, ID: 0xFF}

// firstTarget is the smallest entity a relation can point at: Zero is
// the declaration placeholder, so a's relations start one past it.
var firstTarget = Entity{ID: 1}

// RelationBounds covers a's relations *without* its declaration: the
// smallest non-Zero B is {0,0,0,1}, so starting there skips (a, Zero)
// without needing to filter it out after the fact.
func RelationBounds(a Entity) (start, end []byte) {
	return RelationBoundsIn(a, firstTarget, maxEntity)
}

// RelationBoundsIn narrows RelationBounds to targets in [from, to],
// both inclusive. from is clamped past the declaration placeholder, so
// passing Zero means "from the first relation" rather than "include the
// declaration".
func RelationBoundsIn(a, from, to Entity) (start, end []byte) {
	if from.Compare(firstTarget) < 0 {
		from = firstTarget
	}
	return Key(NamespaceRelation, a, from), Key(NamespaceRelation, a, to)
}

// BacklinkBounds covers every relation pointing at b, in the index
// namespace. There are no declarations in that namespace, so unlike
// RelationBounds this needs no lower-bound trick.
func BacklinkBounds(b Entity) (start, end []byte) {
	return BacklinkBoundsIn(b, Zero, maxEntity)
}

// BacklinkBoundsIn narrows BacklinkBounds to sources in [from, to],
// both inclusive.
//
// This narrowing is more useful than it looks: the index namespace
// orders b's backlinks by the *source* entity's own (log, page, type,
// id) bytes, so bounding them to a page range is a sub-range scan rather
// than a filter over everything -- and for log entries, page order is
// the order they were written. "Every line on pages 5 to 7 that used
// this term" is therefore one bounded scan (see Journal.EntryPages).
func BacklinkBoundsIn(b, from, to Entity) (start, end []byte) {
	return Key(NamespaceIndex, b, from), Key(NamespaceIndex, b, to)
}

// TypeBounds covers every key whose first entity is of type typ on page
// page of log log -- declarations and relations interleaved, since both
// share the range. This is the scan that only works because Type sits
// above ID in the entity byte order.
func TypeBounds(log, page, typ uint8) (start, end []byte) {
	lo := Entity{Log: log, Page: page, Type: typ, ID: 0x00}
	hi := Entity{Log: log, Page: page, Type: typ, ID: 0xFF}
	return Key(NamespaceRelation, lo, Zero), Key(NamespaceRelation, hi, maxEntity)
}

// NamespaceBounds covers an entire namespace -- every record this
// package ever wrote under ns. Used by tests and by any whole-store
// audit (see journal_test.go's no-copied-values check).
func NamespaceBounds(ns byte) (start, end []byte) {
	return Key(ns, Zero, Zero), Key(ns, maxEntity, maxEntity)
}
