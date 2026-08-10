package shmclient

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// CreateJoinInvite lodges a one-time shmevent.KindJoinInvite record for
// token, granting suffrage, on the session's node. Only a current raft
// voter may do this -- see joinInviteCreate's doc comment in
// api/shmevent.capnp.
func (s *Session) CreateJoinInvite(ctx context.Context, token []byte, suffrage byte) error {
	m, err := shmevent.NewJoinInviteCreate(token, suffrage)
	if err != nil {
		return fmt.Errorf("shmclient: join_invite_create: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: join_invite_create: %w", err)
	}
	return respErr("join_invite_create", resp)
}

// RevokeJoinInvite deletes the KindJoinInvite record for token outright,
// before it's ever redeemed. Only a current raft voter may do this.
func (s *Session) RevokeJoinInvite(ctx context.Context, token []byte) error {
	m, err := shmevent.NewJoinInviteRevoke(token)
	if err != nil {
		return fmt.Errorf("shmclient: join_invite_revoke: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: join_invite_revoke: %w", err)
	}
	return respErr("join_invite_revoke", resp)
}

// CreateJoinRequest mints a fresh join-request ticket on the session's own
// node -- the reverse of CreateJoinInvite, for a node with no cluster of
// its own yet to hand some other cluster's voter (see joinRequestCreate's
// doc comment in api/shmevent.capnp). Returns the new token.
func (s *Session) CreateJoinRequest(ctx context.Context) ([]byte, error) {
	m, err := shmevent.NewJoinRequestCreate()
	if err != nil {
		return nil, fmt.Errorf("shmclient: join_request_create: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("shmclient: join_request_create: %w", err)
	}
	if err := respErr("join_request_create", resp); err != nil {
		return nil, err
	}
	token, err := resp.JoinRequestCreate().Token()
	if err != nil {
		return nil, fmt.Errorf("shmclient: join_request_create: %w", err)
	}
	return token, nil
}

// CancelJoinRequest clears the session's own pending join-request ticket
// (a no-op if token no longer matches -- already consumed or superseded).
func (s *Session) CancelJoinRequest(ctx context.Context, token []byte) error {
	m, err := shmevent.NewJoinRequestCancel(token)
	if err != nil {
		return fmt.Errorf("shmclient: join_request_cancel: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: join_request_cancel: %w", err)
	}
	return respErr("join_request_cancel", resp)
}

// Recruit tells the session's own node (an existing raft voter) to mint a
// normal join invite on its own cluster and hand-deliver it directly to
// the device named in ticket ("<device's own multiaddr>#<tokenHex>", from
// that device's own CreateJoinRequest) -- see recruit's doc comment in
// api/shmevent.capnp. Returns the recruited device's own join result
// ("<peerID> ok"/"<peerID> pending") on success.
func (s *Session) Recruit(ctx context.Context, ticket string, suffrage byte) (string, error) {
	m, err := shmevent.NewRecruit(ticket, suffrage)
	if err != nil {
		return "", fmt.Errorf("shmclient: recruit: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: recruit: %w", err)
	}
	if err := respErr("recruit", resp); err != nil {
		return "", err
	}
	result, err := resp.Recruit().Ticket()
	if err != nil {
		return "", fmt.Errorf("shmclient: recruit: %w", err)
	}
	return result, nil
}

// CreateExecInvite lodges a one-time shmevent.KindExecInvite record for
// token, binding commandID+inputsJSON, on the session's node. ttlSeconds is
// how long the invite stays redeemable, 0 meaning no expiry (the default).
// Only a current raft voter may do this -- see execInviteCreate's doc
// comment in api/shmevent.capnp.
func (s *Session) CreateExecInvite(ctx context.Context, token []byte, commandID, inputsJSON string, ttlSeconds uint64) error {
	m, err := shmevent.NewExecInviteCreate(token, commandID, inputsJSON, ttlSeconds)
	if err != nil {
		return fmt.Errorf("shmclient: exec_invite_create: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: exec_invite_create: %w", err)
	}
	return respErr("exec_invite_create", resp)
}

// RevokeExecInvite deletes the KindExecInvite record for token outright,
// before it's ever redeemed. Only a current raft voter may do this.
func (s *Session) RevokeExecInvite(ctx context.Context, token []byte) error {
	m, err := shmevent.NewExecInviteRevoke(token)
	if err != nil {
		return fmt.Errorf("shmclient: exec_invite_revoke: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: exec_invite_revoke: %w", err)
	}
	return respErr("exec_invite_revoke", resp)
}

// RedeemExecInvite tells the session's own node to dial sourceAddr and
// redeem token there on this node's own behalf -- see execInviteRedeem's
// doc comment in api/shmevent.capnp. Returns the new instance id on
// success.
func (s *Session) RedeemExecInvite(ctx context.Context, sourceAddr string, token []byte) (string, error) {
	m, err := shmevent.NewExecInviteRedeem(sourceAddr, token)
	if err != nil {
		return "", fmt.Errorf("shmclient: exec_invite_redeem: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: exec_invite_redeem: %w", err)
	}
	if err := respErr("exec_invite_redeem", resp); err != nil {
		return "", err
	}
	instanceID, err := resp.ExecInviteRedeem().InstanceId()
	if err != nil {
		return "", fmt.Errorf("shmclient: exec_invite_redeem: %w", err)
	}
	return instanceID, nil
}
