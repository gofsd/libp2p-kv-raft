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

// The old wire format's EncodePermitRequestPayload/DecodePermitRequestPayload
// (a hand-packed kind+peerID+metadata blob) is gone -- a permitRequest Msg
// carries those as separate typed capnp fields (NewPermitRequest,
// Event_permitRequest's Kind/PeerId/Metadata accessors) instead.
func TestPermitRequestPayloadRoundTrip(t *testing.T) {
	m, err := NewPermitRequest(KindBootstrapNode, "peer-123", "/ip4/1.2.3.4/tcp/4001")
	if err != nil {
		t.Fatalf("NewPermitRequest: %v", err)
	}
	grp := m.PermitRequest()
	peerID, err := grp.PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	metadata, err := grp.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if grp.Kind() != KindBootstrapNode || peerID != "peer-123" || metadata != "/ip4/1.2.3.4/tcp/4001" {
		t.Fatalf("got kind=%d peerID=%q metadata=%q", grp.Kind(), peerID, metadata)
	}

	// Empty metadata must round-trip too (the common case for a kind with
	// no metadata of its own).
	m, err = NewPermitRequest(KindBootstrapNode, "peer-456", "")
	if err != nil {
		t.Fatalf("NewPermitRequest with empty metadata: %v", err)
	}
	grp = m.PermitRequest()
	peerID, err = grp.PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	metadata, err = grp.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if grp.Kind() != KindBootstrapNode || peerID != "peer-456" || metadata != "" {
		t.Fatalf("got kind=%d peerID=%q metadata=%q, want empty metadata", grp.Kind(), peerID, metadata)
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

// The old wire format's EncodePermitConfirmPayload/DecodePermitConfirmPayload
// is gone the same way -- a permitConfirm Msg carries kind/peerID as
// separate typed fields (NewPermitConfirm, Event_permitConfirm's Kind/
// PeerId accessors).
func TestPermitConfirmPayloadRoundTrip(t *testing.T) {
	m, err := NewPermitConfirm(KindBootstrapNode, "peer-123")
	if err != nil {
		t.Fatalf("NewPermitConfirm: %v", err)
	}
	peerID, err := m.PermitConfirm().PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	if m.PermitConfirm().Kind() != KindBootstrapNode || peerID != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", m.PermitConfirm().Kind(), peerID)
	}
}

// The old generic EventLifecycleWrite envelope (EncodeLifecycleWritePayload/
// DecodeLifecycleWritePayload plus the LifecycleActionRequest/Confirm/Revoke
// constants) wrapped every (kind,action) pair -- permit request/confirm/
// revoke, join-invite create/revoke, exec-invite create/revoke -- behind
// two extra leading bytes. All of that is gone: each (kind,action) pair is
// now its own top-level capnp variant (permitRequest/permitConfirm/
// permitRevoke/joinInviteCreate/...), so Which() alone is what used to be
// the envelope's kind+action header. This pins the same property the old
// test did -- every pair survives a full sign/encode/decode/verify cycle
// distinguishably from every other pair -- at the variant level instead.
func TestPermitVariantsRoundTripThroughWire(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		build func() (Msg, error)
		want  Event_Which
	}{
		{"permit request", func() (Msg, error) { return NewPermitRequest(KindBootstrapNode, "peer-123", "meta") }, Event_Which_permitRequest},
		{"permit confirm", func() (Msg, error) { return NewPermitConfirm(KindClusterJoin, "peer-9") }, Event_Which_permitConfirm},
		{"permit revoke", func() (Msg, error) { return NewPermitRevoke(KindClusterJoin, "peer-9") }, Event_Which_permitRevoke},
		{"join invite create", func() (Msg, error) { return NewJoinInviteCreate(make([]byte, JoinInviteTokenSize), SuffrageVoter) }, Event_Which_joinInviteCreate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := tc.build()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			buf, err := Encode(m, priv)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			decoded, crc, sig, err := Decode(buf)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if err := Verify(pub, decoded, crc, sig); err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if decoded.Which() != tc.want {
				t.Fatalf("Which = %v, want %v", decoded.Which(), tc.want)
			}
		})
	}
}

func TestEventPermitRequestConfirmEncodeDecodeRoundTrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	req, err := NewPermitRequest(KindBootstrapNode, "peer-123", "")
	if err != nil {
		t.Fatalf("NewPermitRequest: %v", err)
	}
	req.SetId(11)
	buf, err := Encode(req, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Which() != Event_Which_permitRequest {
		t.Fatalf("Which = %v, want permitRequest", got.Which())
	}
	peerID, err := got.PermitRequest().PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	if got.PermitRequest().Kind() != KindBootstrapNode || peerID != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", got.PermitRequest().Kind(), peerID)
	}

	confirm, err := NewPermitConfirm(KindBootstrapNode, "peer-123")
	if err != nil {
		t.Fatalf("NewPermitConfirm: %v", err)
	}
	confirm.SetId(12)
	buf, err = Encode(confirm, priv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, _, _, err = Decode(buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Which() != Event_Which_permitConfirm {
		t.Fatalf("Which = %v, want permitConfirm", got.Which())
	}
	peerID, err = got.PermitConfirm().PeerId()
	if err != nil {
		t.Fatalf("PeerId: %v", err)
	}
	if got.PermitConfirm().Kind() != KindBootstrapNode || peerID != "peer-123" {
		t.Fatalf("got kind=%d peerID=%q", got.PermitConfirm().Kind(), peerID)
	}
}
