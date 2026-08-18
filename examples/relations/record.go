package relations

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// RecordVersion is the first byte of every stored value. It exists for
// the same reason Entity.Log does, one level down: a value layout change
// that older readers cannot understand must be detectable, not guessed
// at. A reader that meets a version it does not know refuses the record
// rather than misparsing it.
const RecordVersion byte = 1

// Record is the value half of every key this package writes -- what the
// paper original had printed on the line: what this is (Name), who wrote
// it (Author), when (Created), and their signature over the whole thing.
//
// Name is only ever set on a *declaration* (see the package doc): it is
// the one place a human-readable string is stored, and every later use
// of that string is a 4-byte reference to the entity that owns it. A
// relation record normally carries no name at all -- its meaning is its
// two endpoints plus Kind.
type Record struct {
	// Kind is what this record means to the application: 0
	// (KindDeclaration) for a declaration, and any application-defined
	// byte for a relation (see journal.go's Kind* constants). It is
	// deliberately in the value and not in the key -- a range scan
	// returns keys and values together, so filtering on it is free,
	// while spending a key byte on it would cost a byte of id space
	// forever.
	Kind byte
	// Author is the entity of whoever wrote the record -- itself an
	// ordinary declared entity (see Store.DeclareActor) whose own record
	// carries their public key. Four bytes here instead of a 32-byte
	// key copied onto every record is the same dictionary discipline the
	// rest of the package applies to names.
	Author Entity
	// Created is when the record was written, to nanosecond precision.
	Created time.Time
	// Name is the human-readable value this entity stands for, on a
	// declaration; empty on a relation.
	Name string
	// Data is an optional application payload -- a public key on an
	// actor declaration, a quantity on a measurement relation, a
	// reference to a third entity on an edge. Bounded to 64KiB by the
	// length prefix.
	Data []byte
	// Signature is Ed25519 over the record's key and body (see
	// SignedPayload), or empty if the record was written without a
	// signing key.
	Signature []byte
}

// maxFieldLen bounds Name and Data: both get a 2-byte length prefix, the
// same bound and the same reason as pkg/logrecord.BuildKey's own fields.
const maxFieldLen = 0xFFFF

// Fixed offsets inside an encoded record, before the two
// variable-length fields start.
const (
	offKind    = 1
	offAuthor  = 2
	offCreated = offAuthor + EntityLen
	headerLen  = offCreated + 8
)

// ErrUnsigned is what Verify returns for a record written without a
// signing key. It is distinct from a *bad* signature on purpose: an
// unsigned record is a policy decision the writer made, a bad one is
// evidence of tampering, and a caller auditing a log wants to tell those
// two apart.
var ErrUnsigned = errors.New("relations: record is unsigned")

// ErrBadSignature is what Verify returns when a signature is present but
// does not check out against the key and body it was supposed to cover.
var ErrBadSignature = errors.New("relations: signature does not verify")

// Encode serializes r as the stored value, signing it with priv. If priv
// is nil the record keeps r.Signature as it stands (normally empty, i.e.
// unsigned). The layout is:
//
//	[version 1][kind 1][author 4][created 8BE][nameLen 2BE][name]
//	[dataLen 2BE][data][sigLen 2BE][signature]
//
// key is the 9-byte key the value will be stored under. It is not part
// of the encoding -- it is already the key -- but it *is* part of what
// gets signed (see SignedPayload), so a signed record cannot be lifted
// out from under one key and replayed under another. That matters here
// specifically because the same record is written twice, once per
// direction (Store.Link): each copy is signed for its own key.
func (r Record) Encode(key []byte, priv ed25519.PrivateKey) ([]byte, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("relations: encode: key must be %d bytes, got %d", KeyLen, len(key))
	}
	if len(r.Name) > maxFieldLen {
		return nil, fmt.Errorf("relations: encode: name too long: %d bytes", len(r.Name))
	}
	if len(r.Data) > maxFieldLen {
		return nil, fmt.Errorf("relations: encode: data too long: %d bytes", len(r.Data))
	}

	body := make([]byte, 0, headerLen+2+len(r.Name)+2+len(r.Data)+2+ed25519.SignatureSize)
	body = append(body, RecordVersion, r.Kind)
	author := r.Author.Bytes()
	body = append(body, author[:]...)
	body = binary.BigEndian.AppendUint64(body, uint64(r.Created.UTC().UnixNano()))
	body = binary.BigEndian.AppendUint16(body, uint16(len(r.Name)))
	body = append(body, r.Name...)
	body = binary.BigEndian.AppendUint16(body, uint16(len(r.Data)))
	body = append(body, r.Data...)

	// With a key, sign; without one, keep whatever signature r already
	// carries -- which is normally none, and is how an unsigned record
	// is written, but also lets a record read back out of the store be
	// re-encoded verbatim by a reader holding no key at all.
	sig := r.Signature
	if priv != nil {
		sig = ed25519.Sign(priv, SignedPayload(key, body))
	}
	body = binary.BigEndian.AppendUint16(body, uint16(len(sig)))
	body = append(body, sig...)
	return body, nil
}

// SignedPayload is what a record's signature actually covers: the key it
// is stored under, followed by the encoded body up to (but not
// including) the signature's own length prefix. Passing the unsigned
// portion in, rather than re-encoding here, keeps signing and verifying
// on one definition of "the bytes" -- the same reasoning
// pkg/shmevent.marshalWithCrcAndEmptySig applies to a capnp message.
func SignedPayload(key, unsigned []byte) []byte {
	payload := make([]byte, 0, len(key)+len(unsigned))
	payload = append(payload, key...)
	payload = append(payload, unsigned...)
	return payload
}

// DecodeRecord parses a stored value back into a Record, and also
// returns the unsigned portion of it, which Verify needs to re-derive
// what the signature covered. A caller that only wants the fields can
// ignore that second return.
func DecodeRecord(b []byte) (Record, []byte, error) {
	if len(b) < headerLen+2 {
		return Record{}, nil, fmt.Errorf("relations: record too short: %d bytes", len(b))
	}
	if b[0] != RecordVersion {
		return Record{}, nil, fmt.Errorf("relations: unknown record version %d (this build understands %d)", b[0], RecordVersion)
	}
	var r Record
	r.Kind = b[offKind]
	author, err := DecodeEntity(b[offAuthor:offCreated])
	if err != nil {
		return Record{}, nil, err
	}
	r.Author = author
	r.Created = time.Unix(0, int64(binary.BigEndian.Uint64(b[offCreated:headerLen]))).UTC()

	off := headerLen
	name, off, err := takeField(b, off, "name")
	if err != nil {
		return Record{}, nil, err
	}
	r.Name = string(name)
	data, off, err := takeField(b, off, "data")
	if err != nil {
		return Record{}, nil, err
	}
	if len(data) > 0 {
		r.Data = data
	}
	unsigned := b[:off]
	sig, off, err := takeField(b, off, "signature")
	if err != nil {
		return Record{}, nil, err
	}
	if len(sig) > 0 {
		r.Signature = sig
	}
	if off != len(b) {
		return Record{}, nil, fmt.Errorf("relations: record has %d trailing bytes", len(b)-off)
	}
	return r, unsigned, nil
}

// takeField reads one 2-byte-length-prefixed field starting at off,
// returning it and the offset just past it. label only names the field
// in the error.
func takeField(b []byte, off int, label string) ([]byte, int, error) {
	if off+2 > len(b) {
		return nil, 0, fmt.Errorf("relations: record truncated before %s length", label)
	}
	n := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	if off+n > len(b) {
		return nil, 0, fmt.Errorf("relations: record truncated inside %s: want %d bytes, have %d", label, n, len(b)-off)
	}
	return b[off : off+n], off + n, nil
}

// Verify checks r's signature against pub, given the key it was stored
// under and the unsigned portion DecodeRecord returned alongside it.
// Returns ErrUnsigned if there is no signature at all, ErrBadSignature
// if there is one and it does not check out.
func (r Record) Verify(key, unsigned []byte, pub ed25519.PublicKey) error {
	if len(r.Signature) == 0 {
		return ErrUnsigned
	}
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("relations: verify: public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	if !ed25519.Verify(pub, SignedPayload(key, unsigned), r.Signature) {
		return ErrBadSignature
	}
	return nil
}
