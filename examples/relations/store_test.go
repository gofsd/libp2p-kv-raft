package relations_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// testLog is the log byte every test in this package writes under -- one
// log book. See relations.Entity.Log.
const testLog uint8 = 1

// newStore returns a Store over an in-memory backend, with an actor
// entity already declared and its public key stored. The actor's own
// declaration is self-authored and self-signed, which is the ordinary
// case: a device writing a log signs its own identity record with the
// key that record publishes.
func newStore(t *testing.T) (*relations.Store, relations.Entity, ed25519.PublicKey) {
	t.Helper()
	pub, priv := newKey(t)
	return newStoreOn(t, relations.Memory(), pub, priv)
}

// newKey mints the Ed25519 identity a Store signs with.
func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

func newStoreOn(t *testing.T, be relations.Backend, pub ed25519.PublicKey, priv ed25519.PrivateKey) (*relations.Store, relations.Entity, ed25519.PublicKey) {
	t.Helper()
	actor := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 1}
	st := relations.New(be, testLog, actor, priv)
	if err := st.DeclareActor(context.Background(), actor, "Ivanov", pub); err != nil {
		t.Fatalf("DeclareActor: %v", err)
	}
	return st, actor, pub
}

// TestAllocateSpillsOntoNextPage walks a type's whole 255-id page and
// checks the 256th allocation lands on the next page rather than failing
// or reusing an id -- the behaviour that makes "page" mean "capacity
// page" for everything except entries, where it also means the page of
// the book.
func TestAllocateSpillsOntoNextPage(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)

	const typ uint8 = 0x20
	var last relations.Entity
	for i := 1; i <= 255; i++ {
		e, err := st.Allocate(ctx, 0, typ, relations.KindDeclaration, "", nil)
		if err != nil {
			t.Fatalf("Allocate #%d: %v", i, err)
		}
		if e.Page != 0 || int(e.ID) != i {
			t.Fatalf("Allocate #%d = %s, want page 0 id %d", i, e, i)
		}
		last = e
	}
	if last.ID != 255 {
		t.Fatalf("last id on page 0 = %d, want 255", last.ID)
	}

	spilled, err := st.Allocate(ctx, 0, typ, relations.KindDeclaration, "", nil)
	if err != nil {
		t.Fatalf("Allocate after a full page: %v", err)
	}
	if spilled.Page != 1 || spilled.ID != 1 {
		t.Fatalf("first allocation after a full page = %s, want page 1 id 1", spilled)
	}

	if _, err := st.Allocate(ctx, 0, relations.TypeAllocator, relations.KindDeclaration, "", nil); err == nil {
		t.Fatal("expected an error allocating the reserved allocator type")
	}
	if _, err := st.Allocate(ctx, 0, 0, relations.KindDeclaration, "", nil); err == nil {
		t.Fatal("expected an error allocating type 0")
	}
}

// TestLinkWritesBothDirections is the core claim of the key layout: one
// Link makes the relation findable from either end, and Unlink removes
// both halves.
func TestLinkWritesBothDirections(t *testing.T) {
	ctx := context.Background()
	st, actor, pub := newStore(t)
	_ = pub

	a, err := st.Allocate(ctx, 0, 0x30, relations.KindDeclaration, "part", nil)
	if err != nil {
		t.Fatalf("Allocate a: %v", err)
	}
	b, err := st.Allocate(ctx, 0, 0x31, relations.KindDeclaration, "batch", nil)
	if err != nil {
		t.Fatalf("Allocate b: %v", err)
	}
	if err := st.Link(ctx, a, b, 0x42, []byte("payload")); err != nil {
		t.Fatalf("Link: %v", err)
	}

	forward, err := st.Relations(ctx, a)
	if err != nil {
		t.Fatalf("Relations: %v", err)
	}
	if len(forward) != 1 || forward[0].A != a || forward[0].B != b || forward[0].Record.Kind != 0x42 {
		t.Fatalf("Relations(a) = %+v, want one a->b relation of kind 0x42", forward)
	}
	if string(forward[0].Record.Data) != "payload" {
		t.Fatalf("relation payload = %q, want %q", forward[0].Record.Data, "payload")
	}
	if forward[0].Record.Author != actor {
		t.Fatalf("relation author = %s, want %s", forward[0].Record.Author, actor)
	}

	backward, err := st.Backlinks(ctx, b)
	if err != nil {
		t.Fatalf("Backlinks: %v", err)
	}
	if len(backward) != 1 || backward[0].A != a || backward[0].B != b {
		t.Fatalf("Backlinks(b) = %+v, want the same a->b relation with its endpoints unflipped", backward)
	}

	// Both physical copies are signed for their own key, so both verify
	// where they are and neither would verify where the other is.
	for _, rel := range []relations.Relation{forward[0], backward[0]} {
		if err := st.Verify(ctx, rel); err != nil {
			t.Fatalf("Verify(%x): %v", rel.Key(), err)
		}
	}

	// A relation is not a declaration: scanning a's relations skips the
	// (a, Zero) record entirely.
	decl, found, err := st.Declaration(ctx, a)
	if err != nil || !found {
		t.Fatalf("Declaration(a) = %v, %v", found, err)
	}
	if decl.Record.Name != "part" {
		t.Fatalf("declaration name = %q, want %q", decl.Record.Name, "part")
	}

	if err := st.Unlink(ctx, a, b); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	forward, err = st.Relations(ctx, a)
	if err != nil {
		t.Fatalf("Relations after unlink: %v", err)
	}
	backward, err = st.Backlinks(ctx, b)
	if err != nil {
		t.Fatalf("Backlinks after unlink: %v", err)
	}
	if len(forward) != 0 || len(backward) != 0 {
		t.Fatalf("after Unlink: %d forward, %d backward relations remain, want none", len(forward), len(backward))
	}

	if err := st.Link(ctx, a, a, 1, nil); err == nil {
		t.Fatal("expected an error linking an entity to itself")
	}
	if err := st.Link(ctx, a, relations.Zero, 1, nil); err == nil {
		t.Fatal("expected an error linking to the zero entity")
	}
}

// TestVerifyCatchesAnEditedStoredRecord edits a value behind the Store's
// back, the way anyone with write access to the underlying key/value
// store could, and checks the signature catches it. This is the property
// a paper log gets from ink and a countersignature.
func TestVerifyCatchesAnEditedStoredRecord(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)

	e, err := st.Allocate(ctx, 0, 0x40, relations.KindDeclaration, "Lathe-2", nil)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	decl, found, err := st.Declaration(ctx, e)
	if err != nil || !found {
		t.Fatalf("Declaration = %v, %v", found, err)
	}
	if err := st.Verify(ctx, decl); err != nil {
		t.Fatalf("Verify before tampering: %v", err)
	}

	// Re-encode the same record with a different name and no signing key
	// -- a forger who cannot sign but can write.
	forged, err := relations.Record{
		Kind:    decl.Record.Kind,
		Author:  decl.Record.Author,
		Created: decl.Record.Created,
		Name:    "Lathe-9",
	}.Encode(relations.DeclarationKey(e), nil)
	if err != nil {
		t.Fatalf("Encode forgery: %v", err)
	}
	if err := st.Backend().Apply(ctx, []relations.Op{{
		Kind:  relations.OpSet,
		Key:   relations.DeclarationKey(e),
		Value: forged,
	}}); err != nil {
		t.Fatalf("Apply forgery: %v", err)
	}

	decl, found, err = st.Declaration(ctx, e)
	if err != nil || !found {
		t.Fatalf("Declaration after forgery = %v, %v", found, err)
	}
	if decl.Record.Name != "Lathe-9" {
		t.Fatalf("forgery did not land: name = %q", decl.Record.Name)
	}
	if err := st.Verify(ctx, decl); !errors.Is(err, relations.ErrUnsigned) {
		t.Fatalf("Verify(unsigned forgery) = %v, want ErrUnsigned", err)
	}

	// And one that keeps the original signature but changes the name.
	tampered, err := relations.Record{
		Kind:      decl.Record.Kind,
		Author:    decl.Record.Author,
		Created:   decl.Record.Created,
		Name:      "Lathe-9",
		Signature: mustSignature(t, st, e),
	}.Encode(relations.DeclarationKey(e), nil)
	if err != nil {
		t.Fatalf("Encode tampered: %v", err)
	}
	if err := st.Backend().Apply(ctx, []relations.Op{{
		Kind:  relations.OpSet,
		Key:   relations.DeclarationKey(e),
		Value: tampered,
	}}); err != nil {
		t.Fatalf("Apply tampered: %v", err)
	}
	decl, _, err = st.Declaration(ctx, e)
	if err != nil {
		t.Fatalf("Declaration after tampering: %v", err)
	}
	if err := st.Verify(ctx, decl); !errors.Is(err, relations.ErrBadSignature) {
		t.Fatalf("Verify(tampered) = %v, want ErrBadSignature", err)
	}
}

// mustSignature re-creates a genuine signature over a fresh declaration
// of e, so the tampering test can attach a real signature to a record
// whose body it then changes.
func mustSignature(t *testing.T, st *relations.Store, e relations.Entity) []byte {
	t.Helper()
	ctx := context.Background()
	other := relations.Entity{Log: e.Log, Page: e.Page, Type: e.Type, ID: e.ID + 1}
	if err := st.Declare(ctx, other, relations.KindDeclaration, "signed", nil); err != nil {
		t.Fatalf("Declare: %v", err)
	}
	decl, found, err := st.Declaration(ctx, other)
	if err != nil || !found {
		t.Fatalf("Declaration = %v, %v", found, err)
	}
	return decl.Record.Signature
}

// TestAllocateStepsOverAHandPlacedID pins the bug this check exists for:
// Declare and DeclareActor take an entity the caller chose, so an id can
// exist without the counter knowing. Allocating over it would leave two
// different things claiming one entity -- and when those are actors,
// signatures start verifying against the wrong key.
func TestAllocateStepsOverAHandPlacedID(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)

	const typ uint8 = 0x70
	placed := relations.Entity{Log: testLog, Page: 0, Type: typ, ID: 1}
	if err := st.Declare(ctx, placed, relations.KindDeclaration, "placed by hand", nil); err != nil {
		t.Fatalf("Declare: %v", err)
	}

	allocated, err := st.Allocate(ctx, 0, typ, relations.KindDeclaration, "allocated", nil)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if allocated == placed {
		t.Fatal("the allocator handed out an id that was already taken")
	}
	if allocated.ID != 2 {
		t.Fatalf("the allocator handed out id %d, want 2 (stepping over the one already there)", allocated.ID)
	}

	// And the one placed by hand is untouched.
	decl, found, err := st.Declaration(ctx, placed)
	if err != nil || !found {
		t.Fatalf("Declaration = %v, %v", found, err)
	}
	if decl.Record.Name != "placed by hand" {
		t.Fatalf("the hand-placed entity now reads %q", decl.Record.Name)
	}
}
