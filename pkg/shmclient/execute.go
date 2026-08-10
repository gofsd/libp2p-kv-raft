package shmclient

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Execute sends payload as a direct peer-to-peer execute notification
// from the session's own node to destPeerID -- bypassing raft and the
// store entirely, see execute's doc comment in api/shmevent.capnp. Needs
// two setKey round trips first (registering the session's own peer id and
// destPeerID under fresh ids) since dispatchExecute requires both
// sourceId and destinationId to reference prior registrations, unlike
// Set/Get's single-round-trip forms.
func (s *Session) Execute(ctx context.Context, destPeerID string, payload []byte) error {
	sourceID := newID()
	srcMsg, err := shmevent.NewSetKey([]byte(s.peerID))
	if err != nil {
		return fmt.Errorf("shmclient: execute: register source: %w", err)
	}
	srcMsg.SetId(sourceID)
	resp, err := ipc.Call(ctx, s.peerID, srcMsg, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: execute: register source: %w", err)
	}
	if err := respErr("execute: register source", resp); err != nil {
		return err
	}

	destID := newID()
	destMsg, err := shmevent.NewSetKey([]byte(destPeerID))
	if err != nil {
		return fmt.Errorf("shmclient: execute: register destination: %w", err)
	}
	destMsg.SetId(destID)
	resp, err = ipc.Call(ctx, s.peerID, destMsg, s.priv)
	if err != nil {
		return fmt.Errorf("shmclient: execute: register destination: %w", err)
	}
	if err := respErr("execute: register destination", resp); err != nil {
		return err
	}

	execMsg, err := shmevent.NewExecute(sourceID, destID, payload)
	if err != nil {
		return fmt.Errorf("shmclient: execute: %w", err)
	}
	resp, err = s.call(ctx, execMsg)
	if err != nil {
		return fmt.Errorf("shmclient: execute: %w", err)
	}
	return respErr("execute", resp)
}

// PollExecute drains one queued execute notification delivered to the
// session's node -- see pollExecute's doc comment in api/shmevent.capnp.
// ok is false if nothing is currently queued.
func (s *Session) PollExecute(ctx context.Context) (senderPeerID string, payload []byte, ok bool, err error) {
	m, err := shmevent.NewPollExecute()
	if err != nil {
		return "", nil, false, fmt.Errorf("shmclient: poll_execute: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", nil, false, fmt.Errorf("shmclient: poll_execute: %w", err)
	}
	if err := respErr("poll_execute", resp); err != nil {
		return "", nil, false, err
	}
	grp := resp.PollExecute()
	sender, err := grp.SenderPeerId()
	if err != nil {
		return "", nil, false, fmt.Errorf("shmclient: poll_execute: %w", err)
	}
	if sender == "" {
		return "", nil, false, nil
	}
	notifPayload, err := grp.Value()
	if err != nil {
		return "", nil, false, fmt.Errorf("shmclient: poll_execute: %w", err)
	}
	return sender, notifPayload, true, nil
}

// Execute is the one-shot convenience wrapper around Open+Session.Execute.
func Execute(ctx context.Context, peerID, destPeerID string, payload []byte) error {
	s, err := Open(ctx, peerID)
	if err != nil {
		return err
	}
	return s.Execute(ctx, destPeerID, payload)
}

// PollExecute is the one-shot convenience wrapper around
// Open+Session.PollExecute.
func PollExecute(ctx context.Context, peerID string) (senderPeerID string, payload []byte, ok bool, err error) {
	s, err := Open(ctx, peerID)
	if err != nil {
		return "", nil, false, err
	}
	return s.PollExecute(ctx)
}
