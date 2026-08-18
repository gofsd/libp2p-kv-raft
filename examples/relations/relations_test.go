package relations_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

func TestKeyLayoutIsNineBytes(t *testing.T) {
	a := relations.Entity{Log: 1, Page: 2, Type: 3, ID: 4}
	b := relations.Entity{Log: 5, Page: 6, Type: 7, ID: 8}

	key := relations.RelationKey(a, b)
	want := []byte{relations.NamespaceRelation, 1, 2, 3, 4, 5, 6, 7, 8}
	if !bytes.Equal(key, want) {
		t.Fatalf("relation key = %x, want %x", key, want)
	}
	if len(key) != relations.KeyLen || relations.KeyLen != 9 {
		t.Fatalf("key length = %d, want 9", len(key))
	}

	decl := relations.DeclarationKey(a)
	if !bytes.Equal(decl, []byte{relations.NamespaceRelation, 1, 2, 3, 4, 0, 0, 0, 0}) {
		t.Fatalf("declaration key = %x, want the entity followed by four zero bytes", decl)
	}

	index := relations.IndexKey(a, b)
	if !bytes.Equal(index, []byte{relations.NamespaceIndex, 5, 6, 7, 8, 1, 2, 3, 4}) {
		t.Fatalf("index key = %x, want the mirrored pair in the index namespace", index)
	}

	ns, first, second, err := relations.ParseKey(key)
	if err != nil {
		t.Fatalf("ParseKey: %v", err)
	}
	if ns != relations.NamespaceRelation || first != a || second != b {
		t.Fatalf("ParseKey = %#x/%s/%s, want %#x/%s/%s", ns, first, second, relations.NamespaceRelation, a, b)
	}
	if _, _, _, err := relations.ParseKey(key[:8]); err == nil {
		t.Fatal("expected an error parsing an 8-byte key")
	}
}

// TestKeyOrdering pins the three range-scan properties the byte order
// exists for: a declaration sorts before its entity's relations, one
// type's entities are contiguous inside a page, and pages sort in order.
// Every query in this package is one of these three shapes, so if this
// test breaks, the scans silently start returning the wrong rows rather
// than failing.
func TestKeyOrdering(t *testing.T) {
	e := relations.Entity{Log: 1, Page: 1, Type: relations.TypeEntry, ID: 7}
	target := relations.Entity{Log: 1, Page: 0, Type: relations.TypeTerm, ID: 1}

	decl := relations.DeclarationKey(e)
	rel := relations.RelationKey(e, target)
	if bytes.Compare(decl, rel) >= 0 {
		t.Fatalf("declaration key %x should sort before relation key %x", decl, rel)
	}

	relStart, relEnd := relations.RelationBounds(e)
	if bytes.Compare(relStart, decl) <= 0 {
		t.Fatalf("relation scan lower bound %x must exclude the declaration at %x", relStart, decl)
	}
	if bytes.Compare(rel, relStart) < 0 || bytes.Compare(rel, relEnd) > 0 {
		t.Fatalf("relation key %x falls outside its own scan bounds %x..%x", rel, relStart, relEnd)
	}

	start, end := relations.TypeBounds(1, 1, relations.TypeEntry)
	for _, id := range []uint8{0x00, 0x01, 0x80, 0xFF} {
		k := relations.DeclarationKey(relations.Entity{Log: 1, Page: 1, Type: relations.TypeEntry, ID: id})
		if bytes.Compare(k, start) < 0 || bytes.Compare(k, end) > 0 {
			t.Fatalf("entity id %d falls outside its type's bounds", id)
		}
	}
	otherType := relations.DeclarationKey(relations.Entity{Log: 1, Page: 1, Type: relations.TypeTerm, ID: 1})
	if bytes.Compare(otherType, start) >= 0 && bytes.Compare(otherType, end) <= 0 {
		t.Fatal("a different type's entity landed inside the type bounds")
	}
	nextPage := relations.DeclarationKey(relations.Entity{Log: 1, Page: 2, Type: relations.TypeEntry, ID: 1})
	if bytes.Compare(nextPage, end) <= 0 {
		t.Fatal("page 2 should sort entirely after page 1's entries")
	}
}

func TestRecordRoundTripAndSignature(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	key := relations.DeclarationKey(relations.Entity{Log: 1, Page: 0, Type: relations.TypeTerm, ID: 9})
	original := relations.Record{
		Kind:    relations.KindDeclaration,
		Author:  relations.Entity{Log: 1, Type: relations.TypeActor, ID: 1},
		Created: time.Unix(1_700_000_000, 123).UTC(),
		Name:    "Ivanov",
		Data:    []byte{0xDE, 0xAD},
	}

	encoded, err := original.Encode(key, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, unsigned, err := relations.DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if decoded.Kind != original.Kind || decoded.Author != original.Author || decoded.Name != original.Name {
		t.Fatalf("round trip = %+v, want %+v", decoded, original)
	}
	if !decoded.Created.Equal(original.Created) {
		t.Fatalf("created = %v, want %v", decoded.Created, original.Created)
	}
	if !bytes.Equal(decoded.Data, original.Data) {
		t.Fatalf("data = %x, want %x", decoded.Data, original.Data)
	}
	if err := decoded.Verify(key, unsigned, pub); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	// The signature covers the key, so the same record stored under a
	// different key does not verify -- a relation cannot be lifted out
	// from under one pair of entities and replayed under another.
	otherKey := relations.DeclarationKey(relations.Entity{Log: 1, Page: 0, Type: relations.TypeTerm, ID: 10})
	if err := decoded.Verify(otherKey, unsigned, pub); !errors.Is(err, relations.ErrBadSignature) {
		t.Fatalf("Verify under a different key = %v, want ErrBadSignature", err)
	}

	// Tampering with the body is caught for the same reason.
	tampered := append([]byte(nil), encoded...)
	nameAt := bytes.Index(tampered, []byte("Ivanov"))
	if nameAt < 0 {
		t.Fatal("encoded record does not contain the name it was built with")
	}
	tampered[nameAt] = 'P'
	rec, unsignedTampered, err := relations.DecodeRecord(tampered)
	if err != nil {
		t.Fatalf("DecodeRecord(tampered): %v", err)
	}
	if err := rec.Verify(key, unsignedTampered, pub); !errors.Is(err, relations.ErrBadSignature) {
		t.Fatalf("Verify(tampered) = %v, want ErrBadSignature", err)
	}

	unsignedRec, err := original.Encode(key, nil)
	if err != nil {
		t.Fatalf("Encode unsigned: %v", err)
	}
	rec, u, err := relations.DecodeRecord(unsignedRec)
	if err != nil {
		t.Fatalf("DecodeRecord(unsigned): %v", err)
	}
	if err := rec.Verify(key, u, pub); !errors.Is(err, relations.ErrUnsigned) {
		t.Fatalf("Verify(unsigned) = %v, want ErrUnsigned", err)
	}
}

func TestDecodeRecordRejectsGarbage(t *testing.T) {
	if _, _, err := relations.DecodeRecord(nil); err == nil {
		t.Fatal("expected an error decoding an empty value")
	}
	key := relations.DeclarationKey(relations.Entity{Log: 1, ID: 1, Type: 1})
	good, err := relations.Record{Name: "x"}.Encode(key, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	future := append([]byte(nil), good...)
	future[0] = relations.RecordVersion + 1
	if _, _, err := relations.DecodeRecord(future); err == nil {
		t.Fatal("expected an error decoding a record written by a newer version")
	}
	if _, _, err := relations.DecodeRecord(good[:len(good)-1]); err == nil {
		t.Fatal("expected an error decoding a truncated record")
	}
	if _, _, err := relations.DecodeRecord(append(good, 0x00)); err == nil {
		t.Fatal("expected an error decoding a record with trailing bytes")
	}
}
