package relations

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"time"
)

// Almost everything in this package is written by whoever holds the
// Store: the store's actor is the author and the store's key makes the
// signature. That is right for a device writing its own log, and wrong
// for the two acts that are somebody's *personal* signature -- a
// countersignature and a page sign-off -- as soon as the writing happens
// somewhere else.
//
// The case that forces it: a log driven over this repo's Group/Command
// catalog, where devices submit commands and one node does the writing
// (see examples/relations/journalcmd). A countersignature written by
// that node would carry the node's signature under the endorser's name,
// which is precisely the assurance a countersignature exists to give.
//
// So those records can be built and signed by the endorser, wherever
// they are, and handed over as bytes. The node that owns the log checks
// the signature against the key that endorser declared, checks the
// record is exactly the one it claims to be, and writes it verbatim. It
// never signs on anybody's behalf, and it cannot: it does not have the
// key.

// SignedLink is a relation somebody signed elsewhere, ready to be
// written: both directions of it, each signed for its own key, because a
// signature covers the key it is stored under (see Record.Encode).
type SignedLink struct {
	A Entity `json:"a"`
	B Entity `json:"b"`
	// Forward is the record as stored at RelationKey(A, B); Index is the
	// same record as stored at IndexKey(A, B).
	Forward []byte `json:"forward"`
	Index   []byte `json:"index"`
}

// SignLink builds both halves of a relation signed by priv on author's
// behalf. It needs no Store: a device holding its own key can produce
// one with nothing but this package, which is the point -- it does not
// have, and does not need, write access to the log.
func SignLink(a, b Entity, kind byte, data []byte, author Entity, priv ed25519.PrivateKey, created time.Time) (SignedLink, error) {
	if a.IsZero() || b.IsZero() {
		return SignedLink{}, fmt.Errorf("relations: sign link: neither side may be the zero entity")
	}
	if len(priv) != ed25519.PrivateKeySize {
		return SignedLink{}, fmt.Errorf("relations: sign link: a private key is %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	if created.IsZero() {
		created = time.Now()
	}
	rec := Record{Kind: kind, Author: author, Created: created, Data: data}

	forwardKey, indexKey := RelationKey(a, b), IndexKey(a, b)
	forward, err := rec.Encode(forwardKey, priv)
	if err != nil {
		return SignedLink{}, err
	}
	index, err := rec.Encode(indexKey, priv)
	if err != nil {
		return SignedLink{}, err
	}
	return SignedLink{A: a, B: b, Forward: forward, Index: index}, nil
}

// Verify checks both halves against pub and returns the record they
// agree on. Both are checked, not just the one a reader happens to look
// at first: they are stored under different keys and must be the same
// record signed twice, or the pair would disagree depending on which
// direction it was read from.
func (l SignedLink) Verify(pub ed25519.PublicKey) (Record, error) {
	if l.A.IsZero() || l.B.IsZero() {
		return Record{}, fmt.Errorf("relations: signed link: neither side may be the zero entity")
	}
	forward, forwardUnsigned, err := DecodeRecord(l.Forward)
	if err != nil {
		return Record{}, fmt.Errorf("relations: signed link: forward record: %w", err)
	}
	index, indexUnsigned, err := DecodeRecord(l.Index)
	if err != nil {
		return Record{}, fmt.Errorf("relations: signed link: index record: %w", err)
	}
	if forward.Kind != index.Kind || forward.Author != index.Author ||
		!forward.Created.Equal(index.Created) || forward.Name != index.Name ||
		!bytes.Equal(forward.Data, index.Data) {
		return Record{}, fmt.Errorf("relations: signed link: the two directions are not the same record")
	}
	if err := forward.Verify(RelationKey(l.A, l.B), forwardUnsigned, pub); err != nil {
		return Record{}, fmt.Errorf("relations: signed link: forward record: %w", err)
	}
	if err := index.Verify(IndexKey(l.A, l.B), indexUnsigned, pub); err != nil {
		return Record{}, fmt.Errorf("relations: signed link: index record: %w", err)
	}
	return forward, nil
}

// Ops are the two writes this link consists of, verbatim -- nothing
// re-encodes them, because re-encoding would mean re-signing.
func (l SignedLink) Ops() []Op {
	return []Op{
		{Kind: OpSet, Key: RelationKey(l.A, l.B), Value: l.Forward},
		{Kind: OpSet, Key: IndexKey(l.A, l.B), Value: l.Index},
	}
}

// Actor finds or declares the actor named name with public key pub --
// the registry a device's signature is checked against.
//
// Find-or-create by name, interned like any other value, so the same
// name always resolves to the same entity and two processes declaring it
// at once agree. It refuses to hand back an actor whose declared key is
// not pub: an actor is a name bound to a key, and quietly rebinding one
// would invalidate every signature already made under it.
func (j *Journal) Actor(ctx context.Context, name string, pub ed25519.PublicKey) (Entity, error) {
	if name == "" {
		return Zero, fmt.Errorf("relations: actor name must not be empty")
	}
	if len(pub) != ed25519.PublicKeySize {
		return Zero, fmt.Errorf("relations: actor: public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	actor, err := j.intern(ctx, j.actorOwner(), TypeActor, name, append([]byte(nil), pub...), nil)
	if err != nil {
		return Zero, err
	}
	decl, found, err := j.st.Declaration(ctx, actor)
	if err != nil {
		return Zero, err
	}
	if !found {
		return Zero, fmt.Errorf("relations: actor %s is not declared", actor)
	}
	if !bytes.Equal(decl.Record.Data, pub) {
		return Zero, fmt.Errorf("relations: actor %q is already declared with a different key", name)
	}
	return actor, nil
}

// actorOwner is the presence-index owner actor names are interned under
// -- id 0 of TypeActor, which Allocate never hands out, the same trick
// fieldOwner uses for column names.
func (j *Journal) actorOwner() Entity {
	return Entity{Log: j.st.Log, Page: SchemaPage, Type: TypeActor, ID: 0}
}

// actorKey reads the public key an actor was declared with.
func (j *Journal) actorKey(ctx context.Context, actor Entity) (ed25519.PublicKey, error) {
	if actor.Type != TypeActor {
		return nil, fmt.Errorf("relations: %s is not an actor", actor)
	}
	decl, found, err := j.st.Declaration(ctx, actor)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("relations: actor %s is not declared, so nothing could check its signature", actor)
	}
	if len(decl.Record.Data) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("relations: actor %s is declared without a public key", actor)
	}
	return ed25519.PublicKey(decl.Record.Data), nil
}

// CountersignWith records an endorsement somebody else signed: the same
// act as Countersign, except that the signature on the record is the
// endorser's own and this node merely checks it and writes it down.
//
// It refuses anything that is not exactly the endorsement it claims to
// be -- wrong endpoints, wrong kind, an author that is not the endorser,
// a signature that does not check out against the key that endorser
// declared -- and then applies the same rules Countersign does: not your
// own line, not twice, not one that no longer stands.
func (j *Journal) CountersignWith(ctx context.Context, entry Entity, link SignedLink) error {
	if link.A != entry {
		return fmt.Errorf("relations: countersign: this endorsement is for %s, not %s", link.A, entry)
	}
	actor := link.B
	pub, err := j.actorKey(ctx, actor)
	if err != nil {
		return err
	}
	rec, err := link.Verify(pub)
	if err != nil {
		return err
	}
	if rec.Kind != KindCountersign {
		return fmt.Errorf("relations: countersign: this record is kind %#x, not an endorsement", rec.Kind)
	}
	if rec.Author != actor {
		return fmt.Errorf("relations: countersign: the record is authored by %s, not by the endorser %s", rec.Author, actor)
	}
	return j.countersign(ctx, entry, actor, link.Ops())
}

// SignOffPageWith is SignOffPage with the signature made elsewhere -- see
// CountersignWith. The line count the signer put in the record is checked
// against the page as it actually stands: signing "I close this page with
// four lines on it" means nothing if a fifth arrived in between, so a
// stale signature is refused rather than quietly recorded as something
// its signer did not say.
func (j *Journal) SignOffPageWith(ctx context.Context, page uint8, link SignedLink) error {
	if page < FirstEntryPage {
		return fmt.Errorf("relations: sign off: page %d is not a page of entries", page)
	}
	if link.A != j.PageEntity(page) || link.B != j.StatusMarker() {
		return fmt.Errorf("relations: sign off: this record is not a sign-off of page %d", page)
	}
	forward, _, err := DecodeRecord(link.Forward)
	if err != nil {
		return err
	}
	pub, err := j.actorKey(ctx, forward.Author)
	if err != nil {
		return err
	}
	rec, err := link.Verify(pub)
	if err != nil {
		return err
	}
	if rec.Kind != KindPageSignoff {
		return fmt.Errorf("relations: sign off: this record is kind %#x, not a sign-off", rec.Kind)
	}
	if len(rec.Data) != 1 {
		return fmt.Errorf("relations: sign off: the record carries %d bytes, want the line count", len(rec.Data))
	}
	lines, err := j.st.LastAllocated(ctx, page, TypeEntry)
	if err != nil {
		return err
	}
	if rec.Data[0] != lines {
		return fmt.Errorf("relations: sign off: this was signed for a page of %d lines, and it now holds %d -- sign it again",
			rec.Data[0], lines)
	}
	return j.signOffPage(ctx, page, link.Ops())
}
