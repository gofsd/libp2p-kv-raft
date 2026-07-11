package shmevent

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// PrivateKey and PublicKey are stdlib crypto/ed25519's types, not
// go-libp2p's crypto.PrivKey/PubKey wrapper: GetPublicKey/GetPrivateKey
// hand these bytes to callers on every platform (including Rust, via
// ed25519-dalek), so the wire format is the portable Ed25519-native one,
// not go-libp2p's protobuf-wrapped key encoding. pkg/daemon derives these
// from the node's existing libp2p identity key via PrivKeyToEd25519 /
// PubKeyToEd25519.
type (
	PrivateKey = ed25519.PrivateKey
	PublicKey  = ed25519.PublicKey
)

// canonicalPayload returns the fixed-width byte sequence CRC32 and the
// Ed25519 signature are computed over: event(1) || sourceId_BE(2) ||
// destinationId_BE(2) || value, zero-padded/truncated to ValueSize (512)
// || id_BE(2) -- see api/shmevent.capnp's doc comment. This is the
// *logical* field values, deliberately not capnp's own encoded bytes:
// signing the transport encoding directly would make the signature
// fragile to encoding-level changes (segment layout, padding) that don't
// change the message's meaning.
func canonicalPayload(m Msg) []byte {
	buf := make([]byte, 1+2+2+ValueSize+2)
	buf[0] = m.EventType
	binary.BigEndian.PutUint16(buf[1:3], m.SourceID)
	binary.BigEndian.PutUint16(buf[3:5], m.DestinationID)
	copy(buf[5:5+ValueSize], m.Value) // zero-padded; copy truncates if longer, but Encode already rejects that
	binary.BigEndian.PutUint16(buf[5+ValueSize:], m.ID)
	return buf
}

func crc32Of(m Msg) uint32 {
	return crc32.ChecksumIEEE(canonicalPayload(m))
}

// signedPayload is what Sign/Verify actually operate on: the CRC-covered
// payload plus the CRC itself, big-endian -- see api/shmevent.capnp's doc
// comment ("a real Ed25519 signature over the same payload, checked
// against the sender's public key... plus the crc32 value itself").
func signedPayload(m Msg, crc uint32) []byte {
	payload := canonicalPayload(m)
	out := make([]byte, len(payload)+4)
	copy(out, payload)
	binary.BigEndian.PutUint32(out[len(payload):], crc)
	return out
}

// Sign signs ev (whose Crc32 must already be crc) with priv, returning the
// 64-byte signature to place in Event.signature. priv may be nil only for
// EventGetPublicKey/EventGetPrivateKey requests -- the two bootstrap
// events a node accepts unsigned (see this package's doc comment) -- in
// which case Sign returns a zero-filled signature rather than an error, so
// Encode's call site doesn't need a special case.
func Sign(priv PrivateKey, m Msg, crc uint32) ([]byte, error) {
	if priv == nil {
		if m.EventType == EventGetPublicKey || m.EventType == EventGetPrivateKey {
			return make([]byte, SignatureSize), nil
		}
		return nil, fmt.Errorf("shmevent: signing key required for event %s", EventName(m.EventType))
	}
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("shmevent: private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	return ed25519.Sign(priv, signedPayload(m, crc)), nil
}

// Verify checks sig against m/crc and pub. Returns an error describing the
// mismatch on failure.
func Verify(pub PublicKey, m Msg, crc uint32, sig []byte) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("shmevent: public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	if len(sig) != SignatureSize {
		return fmt.Errorf("shmevent: signature must be %d bytes, got %d", SignatureSize, len(sig))
	}
	if !ed25519.Verify(pub, signedPayload(m, crc), sig) {
		return fmt.Errorf("shmevent: signature verification failed for event %s (id %d)", EventName(m.EventType), m.ID)
	}
	return nil
}

// RequiresSignature reports whether e is one of the two bootstrap events a
// node accepts unsigned -- see this package's doc comment.
func RequiresSignature(e uint8) bool {
	return e != EventGetPublicKey && e != EventGetPrivateKey
}
