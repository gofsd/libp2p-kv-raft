// Package shmevent implements the single wire structure (see
// api/shmevent.capnp) used for every message exchanged between a raft node
// instance and a local "user" -- the desktop CLI, the in-process Android
// UI, or a browser tab's main thread -- over shmring shared memory, and
// (per the same struct's remote counterpart) over
// pkg/daemon.ClientProtocolID. It replaces pkg/ipcproto.
//
// See api/shmevent.capnp's doc comment for the full design rationale: why
// every message carries exactly one raw value plus two relational id
// fields instead of a fixed Key+Value pair, and how Set/Get decompose into
// short sequences of linked messages built around a server-side key
// registry (registry.go).
package shmevent

import (
	"fmt"

	capnp "capnproto.org/go/capnp/v3"
)

// Event type bytes -- the wire values of Msg.EventType. See
// api/shmevent.capnp and this package's doc comment for the
// SetKey/SetField/GetKey/GetField relational pattern.
const (
	// EventSetKey registers Value under this message's own ID in the
	// node's key registry (see registry.go) -- generic, not KV-specific:
	// used both for an actual KV key (ahead of EventSetField) and for a
	// peer id (ahead of EventAdd's learner-join case).
	EventSetKey uint8 = 1
	// EventSetField performs store.Set(registry[SourceID], Value).
	EventSetField uint8 = 2
	// EventGetKey returns registry[SourceID] as Value (reverse lookup).
	EventGetKey uint8 = 3
	// EventGetField performs store.Get(key), where key is
	// registry[SourceID] if SourceID != 0, or Value itself otherwise (a
	// one-shot read needing no prior registry entry).
	EventGetField uint8 = 4
	// EventGetPublicKey returns the node's Ed25519 public key (32 bytes)
	// as Value. Accepted unsigned -- see this package's doc comment.
	EventGetPublicKey uint8 = 5
	// EventGetPrivateKey returns the node's Ed25519 private key (64
	// bytes, stdlib crypto/ed25519 format) as Value. Accepted unsigned --
	// see this package's doc comment.
	EventGetPrivateKey uint8 = 6
	// EventAdd bootstraps this node as the cluster's sole leader (Value
	// empty, SourceID 0), joins as a voter (Value = leader multiaddr to
	// dial, SourceID 0 -- the daemon already knows its own identity, so
	// nothing needs registering first), or -- when SourceID references a
	// prior EventSetKey holding the caller's own peer id -- adds the
	// caller as a non-voting learner at Value (its own reachable
	// address), the shape pkg/daemon.ClientProtocolID's remote browser
	// callers need since the target daemon has no other way to learn a
	// remote caller's identity.
	EventAdd uint8 = 7
	// EventError is response-only: Value carries a UTF-8 error message,
	// ID echoes the failed request's ID. Not part of the fields the
	// protocol was specified with -- added because the struct has no
	// separate status field, and errors need some way to be reported;
	// see this package's doc comment.
	EventError uint8 = 255
)

// EventName returns a human-readable name for e, for logging -- "unknown"
// for anything not defined above.
func EventName(e uint8) string {
	switch e {
	case EventSetKey:
		return "set_key"
	case EventSetField:
		return "set_field"
	case EventGetKey:
		return "get_key"
	case EventGetField:
		return "get_field"
	case EventGetPublicKey:
		return "get_public_key"
	case EventGetPrivateKey:
		return "get_private_key"
	case EventAdd:
		return "add"
	case EventError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", e)
	}
}

// ValueSize is the maximum length of Msg.Value this package enforces (a
// convention, not a capnp schema constraint -- see api/shmevent.capnp's
// doc comment on Event.value).
const ValueSize = 512

// SignatureSize is the length of an Ed25519 signature.
const SignatureSize = 64

// PublicKeySize is the length of an Ed25519 public key.
const PublicKeySize = 32

// PrivateKeySize is the length of an Ed25519 private key in stdlib
// crypto/ed25519's format (32-byte seed + 32-byte public key).
const PrivateKeySize = 64

// Msg is the Go-friendly form of the capnp Event struct (named Msg, not
// Event, only to avoid colliding with the generated capnp type of that
// name in this same package -- see shmevent.capnp.go): Encode/Decode
// handle capnp (de)serialization plus CRC/signature computation and
// verification, so callers never touch the generated capnp API directly.
type Msg struct {
	EventType     uint8
	SourceID      uint16
	DestinationID uint16
	Value         []byte
	ID            uint16
}

// Encode serializes m to its capnp wire form, computing CRC32 and signing
// with priv. priv may be nil only for EventGetPublicKey/EventGetPrivateKey
// requests (see Sign's doc comment).
func Encode(m Msg, priv PrivateKey) ([]byte, error) {
	if len(m.Value) > ValueSize {
		return nil, fmt.Errorf("shmevent: value too long: %d bytes (max %d)", len(m.Value), ValueSize)
	}

	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, fmt.Errorf("shmevent: new message: %w", err)
	}
	root, err := NewRootEvent(seg)
	if err != nil {
		return nil, fmt.Errorf("shmevent: new root: %w", err)
	}
	root.SetEvent(m.EventType)
	root.SetSourceId(m.SourceID)
	root.SetDestinationId(m.DestinationID)
	if err := root.SetValue(m.Value); err != nil {
		return nil, fmt.Errorf("shmevent: set value: %w", err)
	}
	root.SetId(m.ID)

	crc := crc32Of(m)
	root.SetCrc32(crc)

	sig, err := Sign(priv, m, crc)
	if err != nil {
		return nil, fmt.Errorf("shmevent: sign: %w", err)
	}
	if err := root.SetSignature(sig); err != nil {
		return nil, fmt.Errorf("shmevent: set signature: %w", err)
	}

	return msg.Marshal()
}

// Decode parses buf as a capnp Event message and verifies its CRC32
// against the decoded fields. It does not verify the signature -- callers
// that need authenticity (anything but a bootstrap
// GetPublicKey/GetPrivateKey exchange) must call Verify explicitly once
// they know which public key to check against; see this package's doc
// comment on why those two events are the exception.
func Decode(buf []byte) (m Msg, crc uint32, signature []byte, err error) {
	msg, err := capnp.Unmarshal(buf)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: unmarshal: %w", err)
	}
	root, err := ReadRootEvent(msg)
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: read root: %w", err)
	}
	value, err := root.Value()
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: value: %w", err)
	}
	sig, err := root.Signature()
	if err != nil {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: signature: %w", err)
	}

	m = Msg{
		EventType:     root.Event(),
		SourceID:      root.SourceId(),
		DestinationID: root.DestinationId(),
		Value:         append([]byte(nil), value...),
		ID:            root.Id(),
	}
	wantCRC := root.Crc32()
	if gotCRC := crc32Of(m); gotCRC != wantCRC {
		return Msg{}, 0, nil, fmt.Errorf("shmevent: crc32 mismatch: got %#x, message says %#x", gotCRC, wantCRC)
	}
	return m, wantCRC, append([]byte(nil), sig...), nil
}
