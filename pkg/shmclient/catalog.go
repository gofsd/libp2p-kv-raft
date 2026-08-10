package shmclient

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// PutGroup creates or updates (single-step, no separate create/update --
// see shmevent.KindGroup's doc comment) the Group record id=name on the
// session's node, granting unconditional access to its linked commands to
// any peer if public is true (see pkg/kvfsm's isPermittedForCommand). Only
// a current raft voter may do this -- see groupPut's doc comment in
// api/shmevent.capnp.
func (s *Session) PutGroup(ctx context.Context, id, name string, public bool) error {
	m, err := shmevent.NewGroupPut(id, name, public)
	if err != nil {
		return fmt.Errorf("shmclient: group_put: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: group_put: %w", err)
	}
	return respErr("group_put", resp)
}

// DeleteGroup deletes the Group record id, cascading to every
// GroupCommand/PeerGroup record referencing it (see
// kvfsm.OpCascadeDelete). Only a current raft voter may do this.
func (s *Session) DeleteGroup(ctx context.Context, id string) error {
	m, err := shmevent.NewGroupDelete(id)
	if err != nil {
		return fmt.Errorf("shmclient: group_delete: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: group_delete: %w", err)
	}
	return respErr("group_delete", resp)
}

// PutCommand creates or updates the Command record id={name, peerID}
// (peerID is where the command may be executed) on the session's node,
// leaving any existing spec unchanged. Only a current raft voter may do
// this.
func (s *Session) PutCommand(ctx context.Context, id, name string, peerID []byte) error {
	m, err := shmevent.NewCommandPut(id, name, string(peerID))
	if err != nil {
		return fmt.Errorf("shmclient: command_put: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: command_put: %w", err)
	}
	return respErr("command_put", resp)
}

// PutCommandWithSpec is PutCommand carrying the command's spec as well --
// the form definition a client renders inputs from. Opaque to the daemon
// and the FSM, which store and replicate it without parsing it. Passing
// an empty spec is ClearCommandSpec's job, not this one's -- see that
// method.
//
// This is what lets a cluster gain a new command without any device
// gaining new code: the definition replicates through raft like any other
// catalog record, and every device renders it from the same bytes.
func (s *Session) PutCommandWithSpec(ctx context.Context, id, name string, peerID, spec []byte) error {
	m, err := shmevent.NewCommandPutWithSpec(id, name, string(peerID), string(spec))
	if err != nil {
		return fmt.Errorf("shmclient: command_put: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: command_put: %w", err)
	}
	return respErr("command_put", resp)
}

// ClearCommandSpec rewrites command id with an explicitly empty spec,
// removing whatever form definition it held. Needed as its own call because
// a plain PutCommand deliberately leaves an existing spec alone, so "send
// no spec" can no longer mean "delete it".
func (s *Session) ClearCommandSpec(ctx context.Context, id, name string, peerID []byte) error {
	return s.PutCommandWithSpec(ctx, id, name, peerID, []byte(""))
}

// PutStation creates or updates the KindStation record describing the device
// peerID -- a human-readable name plus opaque attrs (JSON, in practice).
// Only a current raft voter may do this, so a device cannot name itself.
func (s *Session) PutStation(ctx context.Context, peerID []byte, name string, attrs []byte) error {
	m, err := shmevent.NewStationPut(string(peerID), name, string(attrs))
	if err != nil {
		return fmt.Errorf("shmclient: station_put: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: station_put: %w", err)
	}
	return respErr("station_put", resp)
}

// DeleteStation removes the station description for peerID. The device's
// cluster membership and group memberships are untouched -- this deletes
// what it's *called*, not what it *is*.
func (s *Session) DeleteStation(ctx context.Context, peerID []byte) error {
	m, err := shmevent.NewStationDelete(string(peerID))
	if err != nil {
		return fmt.Errorf("shmclient: station_delete: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: station_delete: %w", err)
	}
	return respErr("station_delete", resp)
}

// DeleteCommand deletes the Command record id, cascading to every
// GroupCommand record referencing it. Only a current raft voter may do
// this.
func (s *Session) DeleteCommand(ctx context.Context, id string) error {
	m, err := shmevent.NewCommandDelete(id)
	if err != nil {
		return fmt.Errorf("shmclient: command_delete: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: command_delete: %w", err)
	}
	return respErr("command_delete", resp)
}

// PutGroupCommand links commandID to groupID -- peers in groupID (see
// PutPeerGroup) become permitted to submit/execute commandID. Only a
// current raft voter may do this.
func (s *Session) PutGroupCommand(ctx context.Context, commandID, groupID []byte) error {
	m, err := shmevent.NewGroupCommandPut(string(commandID), string(groupID))
	if err != nil {
		return fmt.Errorf("shmclient: group_command_put: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: group_command_put: %w", err)
	}
	return respErr("group_command_put", resp)
}

// DeleteGroupCommand unlinks commandID from groupID. Only a current raft
// voter may do this.
func (s *Session) DeleteGroupCommand(ctx context.Context, commandID, groupID []byte) error {
	m, err := shmevent.NewGroupCommandDelete(string(commandID), string(groupID))
	if err != nil {
		return fmt.Errorf("shmclient: group_command_delete: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: group_command_delete: %w", err)
	}
	return respErr("group_command_delete", resp)
}

// PutPeerGroup adds peerID as a member of groupID -- see PutGroupCommand
// for what that grants. Only a current raft voter may do this.
func (s *Session) PutPeerGroup(ctx context.Context, peerID, groupID []byte) error {
	m, err := shmevent.NewPeerGroupPut(string(peerID), string(groupID))
	if err != nil {
		return fmt.Errorf("shmclient: peer_group_put: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: peer_group_put: %w", err)
	}
	return respErr("peer_group_put", resp)
}

// DeletePeerGroup removes peerID from groupID. Only a current raft voter
// may do this.
func (s *Session) DeletePeerGroup(ctx context.Context, peerID, groupID []byte) error {
	m, err := shmevent.NewPeerGroupDelete(string(peerID), string(groupID))
	if err != nil {
		return fmt.Errorf("shmclient: peer_group_delete: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return fmt.Errorf("shmclient: peer_group_delete: %w", err)
	}
	return respErr("peer_group_delete", resp)
}

// PutGroup is the one-shot convenience wrapper around Open+Session.PutGroup.
func PutGroup(ctx context.Context, peerID, id, name string, public bool) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutGroup(ctx, id, name, public)
}

// DeleteGroup is the one-shot convenience wrapper around
// Open+Session.DeleteGroup.
func DeleteGroup(ctx context.Context, peerID, id string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeleteGroup(ctx, id)
}

// PutCommand is the one-shot convenience wrapper around
// Open+Session.PutCommand.
func PutCommand(ctx context.Context, peerID, id, name string, targetPeerID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutCommand(ctx, id, name, targetPeerID)
}

// DeleteCommand is the one-shot convenience wrapper around
// Open+Session.DeleteCommand.
func DeleteCommand(ctx context.Context, peerID, id string) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeleteCommand(ctx, id)
}

// PutGroupCommand is the one-shot convenience wrapper around
// Open+Session.PutGroupCommand.
func PutGroupCommand(ctx context.Context, peerID string, commandID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutGroupCommand(ctx, commandID, groupID)
}

// DeleteGroupCommand is the one-shot convenience wrapper around
// Open+Session.DeleteGroupCommand.
func DeleteGroupCommand(ctx context.Context, peerID string, commandID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeleteGroupCommand(ctx, commandID, groupID)
}

// PutPeerGroup is the one-shot convenience wrapper around
// Open+Session.PutPeerGroup.
func PutPeerGroup(ctx context.Context, peerID string, targetPeerID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.PutPeerGroup(ctx, targetPeerID, groupID)
}

// DeletePeerGroup is the one-shot convenience wrapper around
// Open+Session.DeletePeerGroup.
func DeletePeerGroup(ctx context.Context, peerID string, targetPeerID, groupID []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.DeletePeerGroup(ctx, targetPeerID, groupID)
}
