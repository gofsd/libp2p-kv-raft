package shmevent

import (
	"crypto/ed25519"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	m := Msg{
		EventType:     EventSetField,
		SourceID:      42,
		DestinationID: 0,
		Value:         []byte("world"),
		ID:            7,
	}

	buf, err := Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, crc, sig, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.EventType != m.EventType || got.SourceID != m.SourceID || got.DestinationID != m.DestinationID ||
		string(got.Value) != string(m.Value) || got.ID != m.ID {
		t.Fatalf("decoded mismatch: got %+v, want %+v", got, m)
	}

	if err := Verify(pub, got, crc, sig); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	// Wrong key must fail.
	otherPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(otherPub, got, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded with the wrong public key")
	}
}

func TestDecodeDetectsCorruption(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("hello-corruption-marker")
	buf, err := Encode(Msg{EventType: EventGetField, Value: value, ID: 1}, priv)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit inside the encoded value bytes specifically (crc32 covers
	// Value; Decode's crc32 check should catch this regardless of where
	// capnp physically places the value's pointed-to content in the
	// buffer -- unlike flipping an arbitrary byte, which might land in the
	// signature instead, a field Decode deliberately does not check).
	idx := bytesIndex(buf, value)
	if idx < 0 {
		t.Fatal("could not locate value bytes in encoded message")
	}
	buf[idx] ^= 0xff
	if _, _, _, err := Decode(buf); err == nil {
		t.Fatal("Decode did not detect corruption via crc32 mismatch")
	}
}

func bytesIndex(haystack, needle []byte) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func TestSignVerifyTamperDetection(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	m := Msg{EventType: EventSetKey, Value: []byte("hello"), ID: 99}
	crc := crc32Of(m)
	sig, err := Sign(priv, m, crc)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(pub, m, crc, sig); err != nil {
		t.Fatalf("Verify of untampered message failed: %v", err)
	}

	tampered := m
	tampered.SourceID = m.SourceID + 1
	if err := Verify(pub, tampered, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded after tampering with SourceID")
	}
}

func TestGetPublicPrivateKeyEventsSignWithNilKey(t *testing.T) {
	buf, err := Encode(Msg{EventType: EventGetPublicKey, ID: 3}, nil)
	if err != nil {
		t.Fatalf("Encode with nil key for EventGetPublicKey: %v", err)
	}
	if _, _, _, err := Decode(buf); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if _, err := Encode(Msg{EventType: EventSetKey, ID: 3}, nil); err == nil {
		t.Fatal("Encode with nil key unexpectedly succeeded for a non-bootstrap event")
	}
}

func TestEventNameRoundTrip(t *testing.T) {
	for _, e := range []uint8{EventSetKey, EventSetField, EventGetKey, EventGetField, EventGetPublicKey, EventGetPrivateKey, EventAdd, EventSet, EventExecute, EventPollExecute, EventListRange, EventLogAppend, EventCatalogPut, EventCatalogDelete, EventLifecycleWrite, EventError} {
		name := EventName(e)
		got, ok := EventFromName(name)
		if !ok {
			t.Fatalf("EventFromName(%q): not recognized", name)
		}
		if got != e {
			t.Fatalf("EventFromName(EventName(%d)) = %d, want %d", e, got, e)
		}
	}
	if _, ok := EventFromName("not_a_real_event"); ok {
		t.Fatal("EventFromName unexpectedly recognized a bogus name")
	}
}

func TestSetPayloadRoundTrip(t *testing.T) {
	payload, err := EncodeSetPayload([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	key, value, err := DecodeSetPayload(payload)
	if err != nil {
		t.Fatalf("DecodeSetPayload: %v", err)
	}
	if string(key) != "hello" || string(value) != "world" {
		t.Fatalf("got key=%q value=%q, want key=%q value=%q", key, value, "hello", "world")
	}

	// Empty key and/or value must round-trip too.
	payload, err = EncodeSetPayload(nil, []byte("world"))
	if err != nil {
		t.Fatalf("EncodeSetPayload with empty key: %v", err)
	}
	key, value, err = DecodeSetPayload(payload)
	if err != nil {
		t.Fatalf("DecodeSetPayload with empty key: %v", err)
	}
	if len(key) != 0 || string(value) != "world" {
		t.Fatalf("got key=%q value=%q, want key=\"\" value=%q", key, value, "world")
	}

	if _, _, err := DecodeSetPayload([]byte{0}); err == nil {
		t.Fatal("DecodeSetPayload unexpectedly accepted a payload shorter than the length prefix")
	}
	if _, _, err := DecodeSetPayload([]byte{0, 10}); err == nil {
		t.Fatal("DecodeSetPayload unexpectedly accepted a key length exceeding the payload size")
	}
}

func TestExecuteNotificationRoundTrip(t *testing.T) {
	notif, err := EncodeExecuteNotification([]byte("12D3KooWSender"), []byte("payload bytes"))
	if err != nil {
		t.Fatalf("EncodeExecuteNotification: %v", err)
	}
	sender, payload, err := DecodeExecuteNotification(notif)
	if err != nil {
		t.Fatalf("DecodeExecuteNotification: %v", err)
	}
	if string(sender) != "12D3KooWSender" || string(payload) != "payload bytes" {
		t.Fatalf("got sender=%q payload=%q, want sender=%q payload=%q", sender, payload, "12D3KooWSender", "payload bytes")
	}

	// Empty sender and/or payload must round-trip too.
	notif, err = EncodeExecuteNotification(nil, []byte("payload bytes"))
	if err != nil {
		t.Fatalf("EncodeExecuteNotification with empty sender: %v", err)
	}
	sender, payload, err = DecodeExecuteNotification(notif)
	if err != nil {
		t.Fatalf("DecodeExecuteNotification with empty sender: %v", err)
	}
	if len(sender) != 0 || string(payload) != "payload bytes" {
		t.Fatalf("got sender=%q payload=%q, want sender=\"\" payload=%q", sender, payload, "payload bytes")
	}

	if _, _, err := DecodeExecuteNotification([]byte{0}); err == nil {
		t.Fatal("DecodeExecuteNotification unexpectedly accepted a payload shorter than the length prefix")
	}
	if _, _, err := DecodeExecuteNotification([]byte{0, 10}); err == nil {
		t.Fatal("DecodeExecuteNotification unexpectedly accepted a sender peer id length exceeding the payload size")
	}
}

func TestEventSetEncodeDecodeRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := EncodeSetPayload([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	buf, err := Encode(Msg{EventType: EventSet, Value: payload, ID: 5}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	key, value, err := DecodeSetPayload(got.Value)
	if err != nil {
		t.Fatalf("DecodeSetPayload: %v", err)
	}
	if string(key) != "hello" || string(value) != "world" {
		t.Fatalf("got key=%q value=%q, want key=%q value=%q", key, value, "hello", "world")
	}
}

// Three ceilings now exist, and which one applies is decided by event type
// alone (valueSizeFor). Each is pinned from both sides -- one byte under is
// accepted, one byte over is refused -- because a ceiling that silently
// applies the *wrong* tier is the failure mode worth catching: too small
// breaks a caller storing legitimate data, too large produces a message the
// shared-memory transport underneath (see pkg/ipc's capacity) can't carry.
func TestValueTooLongRejected(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		event uint8
		limit int
	}{
		{"an ordinary event keeps ValueSize", EventLifecycleWrite, ValueSize},
		{"a plain-KV data event gets KVValueSize", EventSetKey, KVValueSize},
		{"a set gets KVValueSize", EventSet, KVValueSize},
		{"a txn gets KVValueSize", EventTxn, KVValueSize},
		{"a channel chunk gets ChannelValueSize", EventChannelSend, ChannelValueSize},
		// EventCatalogPut's payload can be a Command-with-spec or a
		// Station's attrs, routed through one extra kind-byte wrapper.
		// Pinned here because it's exactly the kind of regression that's
		// silent until a real caller's payload is big enough to hit it:
		// EventCatalogPut defaulting to plain ValueSize would still encode
		// every *test* fixture (which tend to be small) without error.
		{"catalog put inherits its widest wrapped kind's KVValueSize", EventCatalogPut, KVValueSize},
		{"catalog delete keeps ValueSize (every wrapped kind's delete payload is small)", EventCatalogDelete, ValueSize},
		{"lifecycle write keeps ValueSize (none of its wrapped kinds needed more)", EventLifecycleWrite, ValueSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Encode(Msg{EventType: tc.event, Value: make([]byte, tc.limit), ID: 1}, priv); err != nil {
				t.Fatalf("Encode rejected a value at the limit (%d bytes): %v", tc.limit, err)
			}
			if _, err := Encode(Msg{EventType: tc.event, Value: make([]byte, tc.limit+1), ID: 1}, priv); err == nil {
				t.Fatalf("Encode accepted a value over the limit (%d bytes)", tc.limit+1)
			}
		})
	}
}
