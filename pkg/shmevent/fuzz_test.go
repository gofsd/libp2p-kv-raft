package shmevent

import (
	"crypto/ed25519"
	"testing"
)

// FuzzDecode targets Decode -- the single most attacker-exposed parser in
// this codebase, since it's the first thing run on every byte string
// arriving over shmring IPC or a libp2p stream, before any signature
// verification or authorization ever happens (see this package's own doc
// comment on the same-machine/network trust boundaries). The goal is
// exclusively "never panics on malformed input", not "produces a
// meaningful Msg" -- garbage input is expected to return a non-nil error,
// never crash the process that called it.
func FuzzDecode(f *testing.F) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		f.Fatal(err)
	}

	seed := func(eventType uint8, sourceID, destID uint16, value []byte, id uint16) {
		buf, err := Encode(Msg{EventType: eventType, SourceID: sourceID, DestinationID: destID, Value: value, ID: id}, priv)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(buf)
	}
	seed(EventSetField, 42, 0, []byte("world"), 7)
	seed(EventGetField, 0, 0, []byte("hello"), 1)
	seed(EventChannelOpen, 0, 0, []byte("12D3KooWnotarealpeeridatall"), 0)
	seed(EventChannelSend, 0, 0, nil, 0)
	seed(EventGetPublicKey, 0, 0, nil, 0)

	// Non-capnp garbage and edge-length inputs -- not valid encodings at
	// all, exactly what a hostile or simply broken peer might actually
	// send.
	f.Add([]byte(nil))
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte("not capnp at all, just plain text"))

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Decode panicked on input %x: %v", data, r)
			}
		}()
		_, _, _, _ = Decode(data)
	})
}

// FuzzVerify targets Verify with a decodable-but-adversarial signature --
// complements FuzzDecode by covering the very next step every inbound
// message goes through, using a real (valid-shape) Msg/crc pair so the
// fuzzer's mutations land on the signature bytes and public key rather
// than being rejected by Decode before Verify is ever reached.
func FuzzVerify(f *testing.F) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		f.Fatal(err)
	}
	m := Msg{EventType: EventSetField, Value: []byte("world"), ID: 7}
	buf, err := Encode(m, priv)
	if err != nil {
		f.Fatal(err)
	}
	decoded, crc, sig, err := Decode(buf)
	if err != nil {
		f.Fatal(err)
	}

	f.Add([]byte(pub), sig)
	f.Add([]byte(pub), []byte(nil))
	f.Add([]byte(pub), []byte{})
	f.Add(make([]byte, 32), sig)

	f.Fuzz(func(t *testing.T, pubBytes, sigBytes []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Verify panicked on pub=%x sig=%x: %v", pubBytes, sigBytes, r)
			}
		}()
		_ = Verify(PublicKey(pubBytes), decoded, crc, sigBytes)
	})
}
