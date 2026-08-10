package shmclient

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// RequestPermit lodges a pending permit request for peerID (of the given
// kind -- shmevent.KindBootstrapNode or shmevent.KindClusterJoin).
// metadata is opaque, kind-specific JSON (e.g. see
// shmevent.EncodeBootstrapNodeMetadata). See permitRequest's doc comment
// in api/shmevent.capnp: any raft node may receive and relay this.
func (s *Session) RequestPermit(ctx context.Context, kind byte, peerID []byte, metadata string) error {
	m, err := shmevent.NewPermitRequest(kind, string(peerID), metadata)
	if err != nil {
		return fmt.Errorf("shmclient: permit_request: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: permit_request: %w", err)
	}
	return respErr("permit_request", resp)
}

// ConfirmPermit promotes a pending permit request for peerID (of the
// given kind) from pending to confirmed. See permitConfirm's doc comment
// in api/shmevent.capnp: only a peer that is currently a raft voter may
// confirm -- the session's node will reject this (surfaced as an error
// here) if it forwards to a leader that determines the confirming node
// isn't one.
func (s *Session) ConfirmPermit(ctx context.Context, kind byte, peerID []byte) error {
	m, err := shmevent.NewPermitConfirm(kind, string(peerID))
	if err != nil {
		return fmt.Errorf("shmclient: permit_confirm: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: permit_confirm: %w", err)
	}
	return respErr("permit_confirm", resp)
}

// RevokePermit deletes a confirmed permit record for peerID (of the given
// kind) outright. See permitRevoke's doc comment in api/shmevent.capnp:
// only a peer that is currently a raft voter may revoke -- the session's
// node will reject this (surfaced as an error here) if it forwards to a
// leader that determines the revoking node isn't one, the same as
// ConfirmPermit.
func (s *Session) RevokePermit(ctx context.Context, kind byte, peerID []byte) error {
	m, err := shmevent.NewPermitRevoke(kind, string(peerID))
	if err != nil {
		return fmt.Errorf("shmclient: permit_revoke: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: permit_revoke: %w", err)
	}
	return respErr("permit_revoke", resp)
}

// RequestPermit is the one-shot convenience wrapper around
// Open+Session.RequestPermit.
func RequestPermit(ctx context.Context, peerID string, kind byte, targetPeerID []byte, metadata string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.RequestPermit(ctx, kind, targetPeerID, metadata)
}

// ConfirmPermit is the one-shot convenience wrapper around
// Open+Session.ConfirmPermit.
func ConfirmPermit(ctx context.Context, peerID string, kind byte, targetPeerID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.ConfirmPermit(ctx, kind, targetPeerID)
}

// RevokePermit is the one-shot convenience wrapper around
// Open+Session.RevokePermit.
func RevokePermit(ctx context.Context, peerID string, kind byte, targetPeerID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.RevokePermit(ctx, kind, targetPeerID)
}
