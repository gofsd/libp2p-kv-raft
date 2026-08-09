package shmevent

import (
	"crypto/ed25519"
	"fmt"
	"hash/crc32"

	capnp "capnproto.org/go/capnp/v3"
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

// marshalWithCrcAndEmptySig returns m's canonical capnp encoding
// (capnp.Canonicalize, not a plain Message.Marshal) with crc32
// temporarily set to crc and signature temporarily cleared, then
// restores both fields to whatever they held before returning -- the one
// primitive crc32Of/signedPayload below share. Canonicalization -- not
// signing/verifying the marshaled struct bytes directly, an earlier
// design here did -- matters because m's exact segment layout depends on
// *how* it was built (a fresh NewXxx constructor vs. Decode's
// unmarshal-then-copy, see event.go's writableCopy) even when the
// logical field values are identical; two differently-built messages
// with the same fields must still sign/verify identically, which only a
// canonical encoding guarantees ("The result will be identical for
// equivalent structs", see capnp.Canonicalize's own doc comment).
func marshalWithCrcAndEmptySig(m Msg, crc uint32) (raw []byte, err error) {
	savedCRC := m.Crc32()
	savedSig, err := m.Signature()
	if err != nil {
		return nil, fmt.Errorf("shmevent: signature: %w", err)
	}
	savedSigCopy := append([]byte(nil), savedSig...)
	defer func() {
		m.SetCrc32(savedCRC)
		if sigErr := m.SetSignature(savedSigCopy); sigErr != nil && err == nil {
			err = sigErr
			raw = nil
		}
	}()

	m.SetCrc32(crc)
	if err := m.SetSignature(nil); err != nil {
		return nil, fmt.Errorf("shmevent: clear signature: %w", err)
	}
	raw, err = capnp.Canonicalize(capnp.Struct(m))
	if err != nil {
		return nil, fmt.Errorf("shmevent: canonicalize: %w", err)
	}
	return raw, nil
}

// crc32Of computes the CRC-32 (IEEE polynomial) over m's marshaled bytes
// with crc32 zeroed and signature cleared -- see api/shmevent.capnp's doc
// comment ("crc32 covers every other field except itself and signature").
func crc32Of(m Msg) (uint32, error) {
	raw, err := marshalWithCrcAndEmptySig(m, 0)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(raw), nil
}

// signedPayload is what Sign/Verify actually operate on: m's marshaled
// bytes with crc32 set to crc and signature cleared -- see
// api/shmevent.capnp's doc comment ("a real Ed25519 signature ... plus
// the crc32 value itself").
func signedPayload(m Msg, crc uint32) ([]byte, error) {
	return marshalWithCrcAndEmptySig(m, crc)
}

// Sign signs m (whose Crc32 must already be crc) with priv, returning the
// 64-byte signature to place in Event.signature. priv may be nil only for
// getPublicKey/getPrivateKey requests -- the two bootstrap events a node
// accepts unsigned (see this package's doc comment) -- in which case Sign
// returns a zero-filled signature rather than an error, so Encode's call
// site doesn't need a special case.
func Sign(priv PrivateKey, m Msg, crc uint32) ([]byte, error) {
	if priv == nil {
		w := m.Which()
		if w == Event_Which_getPublicKey || w == Event_Which_getPrivateKey {
			return make([]byte, SignatureSize), nil
		}
		return nil, fmt.Errorf("shmevent: signing key required for event %s", EventName(w))
	}
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("shmevent: private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	payload, err := signedPayload(m, crc)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(priv, payload), nil
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
	payload, err := signedPayload(m, crc)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, payload, sig) {
		return fmt.Errorf("shmevent: signature verification failed for event %s (id %d)", EventName(m.Which()), m.Id())
	}
	return nil
}

// RequiresSignature reports whether w is one of the two bootstrap events a
// node accepts unsigned -- see this package's doc comment.
func RequiresSignature(w Event_Which) bool {
	return w != Event_Which_getPublicKey && w != Event_Which_getPrivateKey
}
