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

	m, err := NewSetField(42, []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	m.SetId(7)

	buf, err := Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, crc, sig, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Which() != Event_Which_setField {
		t.Fatalf("Which = %v, want setField", got.Which())
	}
	gotValue, err := got.SetField().Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if got.SetField().SourceId() != 42 || string(gotValue) != "world" || got.Id() != 7 {
		t.Fatalf("decoded mismatch: sourceId=%d value=%q id=%d", got.SetField().SourceId(), gotValue, got.Id())
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
	m, err := NewSetKey(value)
	if err != nil {
		t.Fatal(err)
	}
	m.SetId(1)
	buf, err := Encode(m, priv)
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
	m, err := NewSetField(1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	m.SetId(99)
	crc, err := crc32Of(m)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := Sign(priv, m, crc)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(pub, m, crc, sig); err != nil {
		t.Fatalf("Verify of untampered message failed: %v", err)
	}

	// m is a capnp struct wrapping a pointer to shared segment storage, so
	// mutating a field in place (rather than building a separate message)
	// is what stands in for the old flat struct's field-copy tamper here.
	m.SetField().SetSourceId(2)
	if err := Verify(pub, m, crc, sig); err == nil {
		t.Fatal("Verify unexpectedly succeeded after tampering with SourceID")
	}
}

func TestGetPublicPrivateKeyEventsSignWithNilKey(t *testing.T) {
	getPub, err := NewGetPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	getPub.SetId(3)
	buf, err := Encode(getPub, nil)
	if err != nil {
		t.Fatalf("Encode with nil key for GetPublicKey: %v", err)
	}
	if _, _, _, err := Decode(buf); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	setKey, err := NewSetKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	setKey.SetId(3)
	if _, err := Encode(setKey, nil); err == nil {
		t.Fatal("Encode with nil key unexpectedly succeeded for a non-bootstrap event")
	}
}

func TestEventNameRoundTrip(t *testing.T) {
	for _, w := range []Event_Which{
		Event_Which_setKey,
		Event_Which_setField,
		Event_Which_getKey,
		Event_Which_getFieldByRegistry,
		Event_Which_getFieldByKey,
		Event_Which_getPublicKey,
		Event_Which_getPrivateKey,
		Event_Which_bootstrapOrJoinCluster,
		Event_Which_addLearner,
		Event_Which_set,
		Event_Which_execute,
		Event_Which_pollExecute,
		Event_Which_listRange,
		Event_Which_logAppend,
		Event_Which_leave,
		Event_Which_execInviteRedeem,
		Event_Which_joinRequestCreate,
		Event_Which_joinRequestCancel,
		Event_Which_recruit,
		Event_Which_getOwnAddr,
		Event_Which_channelOpen,
		Event_Which_channelSend,
		Event_Which_channelPoll,
		Event_Which_channelListen,
		Event_Which_channelClose,
		Event_Which_channelCloseWrite,
		Event_Which_channelDataReady,
		Event_Which_kick,
		Event_Which_txn,
		Event_Which_getVersion,
		Event_Which_publicAccess,
		Event_Which_execTicket,
		Event_Which_joinTicket,
		Event_Which_joinRequestTicket,
		Event_Which_dialSubmitCommand,
		Event_Which_dialQueryCommandLog,
		Event_Which_error,
		Event_Which_groupPut,
		Event_Which_groupDelete,
		Event_Which_commandPut,
		Event_Which_commandDelete,
		Event_Which_stationPut,
		Event_Which_stationDelete,
		Event_Which_groupCommandPut,
		Event_Which_groupCommandDelete,
		Event_Which_peerGroupPut,
		Event_Which_peerGroupDelete,
		Event_Which_permitRequest,
		Event_Which_permitConfirm,
		Event_Which_permitRevoke,
		Event_Which_joinInviteCreate,
		Event_Which_joinInviteRevoke,
		Event_Which_execInviteCreate,
		Event_Which_execInviteRevoke,
	} {
		name := EventName(w)
		got, ok := EventFromName(name)
		if !ok {
			t.Fatalf("EventFromName(%q): not recognized", name)
		}
		if got != w {
			t.Fatalf("EventFromName(EventName(%v)) = %v, want %v", w, got, w)
		}
	}
	if _, ok := EventFromName("not_a_real_event"); ok {
		t.Fatal("EventFromName unexpectedly recognized a bogus name")
	}
}

func TestSetPayloadRoundTrip(t *testing.T) {
	m, err := NewSet([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	key, err := m.Set().Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	value, err := m.Set().Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if string(key) != "hello" || string(value) != "world" {
		t.Fatalf("got key=%q value=%q, want key=%q value=%q", key, value, "hello", "world")
	}

	// Empty key and/or value must round-trip too.
	m, err = NewSet(nil, []byte("world"))
	if err != nil {
		t.Fatalf("NewSet with empty key: %v", err)
	}
	key, err = m.Set().Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	value, err = m.Set().Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if len(key) != 0 || string(value) != "world" {
		t.Fatalf("got key=%q value=%q, want key=\"\" value=%q", key, value, "world")
	}
}

func TestExecuteNotificationRoundTrip(t *testing.T) {
	notif, err := NewExecuteNotification("12D3KooWSender", []byte("payload bytes"))
	if err != nil {
		t.Fatalf("NewExecuteNotification: %v", err)
	}
	sender, err := notif.Execute().SenderPeerId()
	if err != nil {
		t.Fatalf("SenderPeerId: %v", err)
	}
	payload, err := notif.Execute().Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if sender != "12D3KooWSender" || string(payload) != "payload bytes" {
		t.Fatalf("got sender=%q payload=%q, want sender=%q payload=%q", sender, payload, "12D3KooWSender", "payload bytes")
	}

	// Empty sender and/or payload must round-trip too.
	notif, err = NewExecuteNotification("", []byte("payload bytes"))
	if err != nil {
		t.Fatalf("NewExecuteNotification with empty sender: %v", err)
	}
	sender, err = notif.Execute().SenderPeerId()
	if err != nil {
		t.Fatalf("SenderPeerId: %v", err)
	}
	payload, err = notif.Execute().Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if sender != "" || string(payload) != "payload bytes" {
		t.Fatalf("got sender=%q payload=%q, want sender=\"\" payload=%q", sender, payload, "payload bytes")
	}
}

func TestEventSetEncodeDecodeRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewSet([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	m.SetId(5)
	buf, err := Encode(m, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Which() != Event_Which_set {
		t.Fatalf("Which = %v, want set", got.Which())
	}
	key, err := got.Set().Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	value, err := got.Set().Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	if string(key) != "hello" || string(value) != "world" {
		t.Fatalf("got key=%q value=%q, want key=%q value=%q", key, value, "hello", "world")
	}
}

// The old wire format capped each event's Value at one of a few fixed
// ceilings (ValueSize/KVValueSize/ChannelValueSize) so cross-language CRC/
// signature computation stayed in sync over a fixed-width canonical
// payload. The capnp rewrite has no such ceiling -- each variant's fields
// are ordinary capnp Data/Text pointers with no artificial size cap this
// package enforces -- so TestValueTooLongRejected's assertion ("a value
// past the limit is rejected") has no equivalent behavior to pin anymore;
// not rejecting large values is the intended new behavior, not a gap.
// Dropped rather than adapted -- see this package's migration notes.
