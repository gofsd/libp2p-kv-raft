package shmclient

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Add bootstraps this node as the cluster's sole leader (leaderPeerID ==
// "") or joins it to the cluster led by leaderPeerID as a voter. Returns
// this node's own peer id, mirroring the pre-shmevent
// ipcproto.ActionAdd response. See bootstrapOrJoinCluster's doc comment in
// api/shmevent.capnp for the (unused here) learner-join shape a remote
// browser caller uses instead (addLearner).
func (s *Session) Add(ctx context.Context, leaderPeerID string) (string, error) {
	m, err := shmevent.NewBootstrapOrJoinCluster(leaderPeerID)
	if err != nil {
		return "", fmt.Errorf("shmclient: add: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: add: %w", err)
	}
	if err := respErr("add", resp); err != nil {
		return "", err
	}
	result, err := resp.BootstrapOrJoinCluster().LeaderAddr()
	if err != nil {
		return "", fmt.Errorf("shmclient: add: %w", err)
	}
	return result, nil
}

// GetOwnAddr returns the session's node's own current best-advertised
// multiaddr (public first, then a relay reservation, then anything else,
// loopback last -- see pkg/daemon's advertisedAddrs) -- queried live, never
// cached, so a node whose Config.RelayPeers reservation completed after
// startup returns the up-to-date circuit address on a later call even if
// an earlier one didn't have it yet.
func (s *Session) GetOwnAddr(ctx context.Context) (string, error) {
	m, err := shmevent.NewGetOwnAddr()
	if err != nil {
		return "", fmt.Errorf("shmclient: get_own_addr: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: get_own_addr: %w", err)
	}
	if err := respErr("get_own_addr", resp); err != nil {
		return "", err
	}
	addr, err := resp.GetOwnAddr().Addr()
	if err != nil {
		return "", fmt.Errorf("shmclient: get_own_addr: %w", err)
	}
	return addr, nil
}

// GetVersion returns the session's node's own build/version info -- see
// getVersion's doc comment in api/shmevent.capnp. Queried live, never
// cached.
func (s *Session) GetVersion(ctx context.Context) (shmevent.VersionInfo, error) {
	m, err := shmevent.NewGetVersion()
	if err != nil {
		return shmevent.VersionInfo{}, fmt.Errorf("shmclient: get_version: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return shmevent.VersionInfo{}, fmt.Errorf("shmclient: get_version: %w", err)
	}
	if err := respErr("get_version", resp); err != nil {
		return shmevent.VersionInfo{}, err
	}
	v, err := shmevent.VersionInfoFrom(resp.GetVersion())
	if err != nil {
		return shmevent.VersionInfo{}, fmt.Errorf("shmclient: get_version: %w", err)
	}
	return v, nil
}

// Leave asks the raft cluster the session's node currently belongs to to
// remove it (raft.RemoveServer) -- see leave's doc comment in
// api/shmevent.capnp. Unlike Add, it takes no target: there's only ever
// one cluster this node's own live raft handle currently belongs to.
func (s *Session) Leave(ctx context.Context) error {
	m, err := shmevent.NewLeave()
	if err != nil {
		return fmt.Errorf("shmclient: leave: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: leave: %w", err)
	}
	return respErr("leave", resp)
}

// Kick asks the raft cluster the session's node currently belongs to to
// force-remove targetPeerID (raft.RemoveServer), without that peer's own
// cooperation -- see kick's doc comment in api/shmevent.capnp. Only takes
// effect if the session's node is itself a raft voter (or forwards to
// one), same restriction ConfirmPermit/RevokePermit have.
func (s *Session) Kick(ctx context.Context, targetPeerID string) error {
	m, err := shmevent.NewKick(targetPeerID)
	if err != nil {
		return fmt.Errorf("shmclient: kick: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: kick: %w", err)
	}
	return respErr("kick", resp)
}

// Add is the one-shot convenience wrapper around Open+Session.Add.
//
// Bootstrap/first-join is a special case: a brand new node has no signing
// key exchange to do beyond what Open already performs (GetPrivateKey is
// itself unsigned and always available), so this works uniformly whether
// or not the node has ever been added to a cluster before.
func Add(ctx context.Context, peerID, leaderPeerID string) (string, error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", err
	}
	return s.Add(ctx, leaderPeerID)
}

// Leave is the one-shot convenience wrapper around Open+Session.Leave.
func Leave(ctx context.Context, peerID string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Leave(ctx)
}

// Kick is the one-shot convenience wrapper around Open+Session.Kick.
func Kick(ctx context.Context, peerID, targetPeerID string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Kick(ctx, targetPeerID)
}
