package shmevent

import (
	"bytes"
	"crypto/ed25519"
	"testing"
)

func TestSystemKeyLayout(t *testing.T) {
	key := SystemKey(KindBootstrapNode, StatusPending, []byte("peer-123"))
	want := append([]byte{SystemKeyPrefix, KindBootstrapNode, StatusPending}, []byte("peer-123")...)
	if !bytes.Equal(key, want) {
		t.Fatalf("got key %x, want %x", key, want)
	}
	if key[0] != 0x00 {
		t.Fatalf("SystemKey's first byte = %#x, want 0x00", key[0])
	}
}

func TestPermitRequestPayloadRoundTrip(t *testing.T) {
	payload, err := EncodePermitRequestPayload(KindBootstrapNode, []byte("peer-123"), []byte("/ip4/1.2.3.4/tcp/4001"))
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload: %v", err)
	}
	kind, peerID, metadata, err := DecodePermitRequestPayload(payload)
	if err != nil {
		t.Fatalf("DecodePermitRequestPayload: %v", err)
	}
	if kind != KindBootstrapNode || string(peerID) != "peer-123" || string(metadata) != "/ip4/1.2.3.4/tcp/4001" {
		t.Fatalf("got kind=%d peerID=%q metadata=%q", kind, peerID, metadata)
	}

	// Empty metadata must round-trip too (the common case for a kind with
	// no metadata of its own).
	payload, err = EncodePermitRequestPayload(KindBootstrapNode, []byte("peer-456"), nil)
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload with empty metadata: %v", err)
	}
	kind, peerID, metadata, err = DecodePermitRequestPayload(payload)
	if err != nil {
		t.Fatalf("DecodePermitRequestPayload with empty metadata: %v", err)
	}
	if kind != KindBootstrapNode || string(peerID) != "peer-456" || len(metadata) != 0 {
		t.Fatalf("got kind=%d peerID=%q metadata=%q, want empty metadata", kind, peerID, metadata)
	}

	if _, _, _, err := DecodePermitRequestPayload([]byte{0, 0}); err == nil {
		t.Fatal("DecodePermitRequestPayload unexpectedly accepted a payload shorter than the header")
	}
	if _, _, _, err := DecodePermitRequestPayload([]byte{KindBootstrapNode, 0, 10}); err == nil {
		t.Fatal("DecodePermitRequestPayload unexpectedly accepted a peerID length exceeding the payload size")
	}
}

func TestClusterMemberKeyLayout(t *testing.T) {
	key := ClusterMemberKey([]byte("peer-123"))
	want := SystemKey(KindClusterMember, clusterMemberStatusPlaceholder, []byte("peer-123"))
	if !bytes.Equal(key, want) {
		t.Fatalf("got key %x, want %x", key, want)
	}
	if key[0] != SystemKeyPrefix || key[1] != KindClusterMember {
		t.Fatalf("ClusterMemberKey = %x, want SystemKeyPrefix/KindClusterMember prefix", key)
	}
}

func TestClusterMemberPayloadRoundTrip(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, role := range []byte{RoleVoter, RoleLearner, RoleLeader} {
		payload := EncodeClusterMemberPayload(PublicKey(pub), role)
		gotPub, gotRole, err := DecodeClusterMemberPayload(payload)
		if err != nil {
			t.Fatalf("DecodeClusterMemberPayload(role=%d): %v", role, err)
		}
		if !bytes.Equal(gotPub, pub) || gotRole != role {
			t.Fatalf("got pub=%x role=%d, want pub=%x role=%d", gotPub, gotRole, pub, role)
		}
	}

	if _, _, err := DecodeClusterMemberPayload(nil); err == nil {
		t.Fatal("DecodeClusterMemberPayload unexpectedly accepted an empty payload")
	}
	if _, _, err := DecodeClusterMemberPayload(make([]byte, PublicKeySize)); err == nil {
		t.Fatal("DecodeClusterMemberPayload unexpectedly accepted a payload missing the role byte")
	}
	if _, _, err := DecodeClusterMemberPayload(make([]byte, PublicKeySize+2)); err == nil {
		t.Fatal("DecodeClusterMemberPayload unexpectedly accepted a payload longer than pub+role")
	}
}

func TestPermitConfirmPayloadRoundTrip(t *testing.T) {
	payload := EncodePermitConfirmPayload(KindBootstrapNode, []byte("peer-123"))
	kind, peerID, err := DecodePermitConfirmPayload(payload)
	if err != nil {
		t.Fatalf("DecodePermitConfirmPayload: %v", err)
	}
	if kind != KindBootstrapNode || string(peerID) != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", kind, peerID)
	}

	if _, _, err := DecodePermitConfirmPayload(nil); err == nil {
		t.Fatal("DecodePermitConfirmPayload unexpectedly accepted an empty payload")
	}
}

// EventLifecycleWrite's whole point is carrying every existing
// per-(kind,action) payload unchanged behind two extra leading bytes --
// this pins that round trip, reusing the exact inner payloads
// EventPermitRequest/Confirm and EventJoinInviteCreate/Revoke already
// produce.
func TestLifecycleWritePayloadRoundTrip(t *testing.T) {
	permitInner, err := EncodePermitRequestPayload(KindBootstrapNode, []byte("peer-123"), []byte("meta"))
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload: %v", err)
	}
	joinInviteInner, err := EncodeJoinInviteCreatePayload(make([]byte, JoinInviteTokenSize), SuffrageVoter)
	if err != nil {
		t.Fatalf("EncodeJoinInviteCreatePayload: %v", err)
	}

	for _, tc := range []struct {
		name   string
		kind   byte
		action byte
		inner  []byte
	}{
		{"permit request", KindBootstrapNode, LifecycleActionRequest, permitInner},
		{"permit revoke", KindClusterJoin, LifecycleActionRevoke, EncodePermitConfirmPayload(KindClusterJoin, []byte("peer-9"))},
		{"join invite create", KindJoinInvite, LifecycleActionRequest, joinInviteInner},
		{"empty inner", KindExecInvite, LifecycleActionRevoke, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := EncodeLifecycleWritePayload(tc.kind, tc.action, tc.inner)
			gotKind, gotAction, gotInner, err := DecodeLifecycleWritePayload(wrapped)
			if err != nil {
				t.Fatalf("DecodeLifecycleWritePayload: %v", err)
			}
			if gotKind != tc.kind || gotAction != tc.action {
				t.Fatalf("got kind=%d action=%d, want kind=%d action=%d", gotKind, gotAction, tc.kind, tc.action)
			}
			if !bytes.Equal(gotInner, tc.inner) {
				t.Fatalf("got inner %x, want %x", gotInner, tc.inner)
			}
		})
	}

	if _, _, _, err := DecodeLifecycleWritePayload([]byte{1}); err == nil {
		t.Fatal("DecodeLifecycleWritePayload unexpectedly accepted a payload shorter than kind+action")
	}
}

func TestEventPermitRequestConfirmEncodeDecodeRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	reqPayload, err := EncodePermitRequestPayload(KindBootstrapNode, []byte("peer-123"), nil)
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload: %v", err)
	}
	buf, err := Encode(Msg{EventType: EventPermitRequest, Value: reqPayload, ID: 11}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	kind, peerID, _, err := DecodePermitRequestPayload(got.Value)
	if err != nil {
		t.Fatalf("DecodePermitRequestPayload: %v", err)
	}
	if kind != KindBootstrapNode || string(peerID) != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", kind, peerID)
	}

	confirmPayload := EncodePermitConfirmPayload(KindBootstrapNode, []byte("peer-123"))
	buf, err = Encode(Msg{EventType: EventPermitConfirm, Value: confirmPayload, ID: 12}, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err = Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	kind, peerID, err = DecodePermitConfirmPayload(got.Value)
	if err != nil {
		t.Fatalf("DecodePermitConfirmPayload: %v", err)
	}
	if kind != KindBootstrapNode || string(peerID) != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", kind, peerID)
	}
}
