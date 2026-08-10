package shmclient

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// PublicAccess tells the session's own node to submit
// shmevent.DefaultPublicCommandID -- the always-public front door every
// cluster bootstraps -- to the cluster at targetPeer (a multiaddr or bare
// peer id), which grants this node Channel and relay standing there. note
// is an optional free-text tag stored on the request. See publicAccess's
// doc comment in api/shmevent.capnp for the full design; returns the new
// dispatch's instance id.
//
// This is what a device with no standing in a cluster it needs the relay
// service of (a phone reserving a circuit-relay v2 slot on one of
// configs/bootstrap-nodes.json's nodes) calls once, instead of an operator
// running mage addpeertogroup by hand for every such device.
func (s *Session) PublicAccess(ctx context.Context, targetPeer, note string) (string, error) {
	m, err := shmevent.NewPublicAccess(targetPeer, note)
	if err != nil {
		return "", fmt.Errorf("shmclient: public_access: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: public_access: %w", err)
	}
	if err := respErr("public_access", resp); err != nil {
		return "", err
	}
	instanceID, err := resp.PublicAccess().InstanceId()
	if err != nil {
		return "", fmt.Errorf("shmclient: public_access: %w", err)
	}
	return instanceID, nil
}

// DialSubmitCommand asks this session's own node to dial targetPeer (a
// multiaddr or bare peer id) directly and submit commandID/inputsJSON
// there as a CommandRequestLogKind write into *that* cluster's own log --
// PublicAccess generalized from one hardcoded command to any commandID,
// see dialSubmitCommand's doc comment in api/shmevent.capnp. Returns the
// new dispatch's instance id, usable with DialQueryCommandLog to read
// back its result.
func (s *Session) DialSubmitCommand(ctx context.Context, targetPeer, commandID, inputsJSON, note string) (string, error) {
	m, err := shmevent.NewDialSubmitCommand(targetPeer, commandID, inputsJSON, note)
	if err != nil {
		return "", fmt.Errorf("shmclient: dial_submit_command: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return "", fmt.Errorf("shmclient: dial_submit_command: %w", err)
	}
	if err := respErr("dial_submit_command", resp); err != nil {
		return "", err
	}
	instanceID, err := resp.DialSubmitCommand().InstanceId()
	if err != nil {
		return "", fmt.Errorf("shmclient: dial_submit_command: %w", err)
	}
	return instanceID, nil
}

// DialQueryCommandLog asks this session's own node to dial targetPeer
// directly and read back instanceID's own CommandExecLogKind entries in
// [since, until], up to limit records (0 meaning unbounded) --
// DialSubmitCommand's read-back counterpart, see dialQueryCommandLog's
// doc comment in api/shmevent.capnp.
func (s *Session) DialQueryCommandLog(ctx context.Context, targetPeer, instanceID string, since, until time.Time, limit int) ([]logrecord.Record, error) {
	m, err := shmevent.NewDialQueryCommandLog(targetPeer, instanceID, since, until, limit)
	if err != nil {
		return nil, fmt.Errorf("shmclient: dial_query_command_log: %w", err)
	}
	resp, err := s.call(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("shmclient: dial_query_command_log: %w", err)
	}
	if err := respErr("dial_query_command_log", resp); err != nil {
		return nil, err
	}
	records, err := shmevent.DialQueryCommandLogRecords(resp.DialQueryCommandLog())
	if err != nil {
		return nil, fmt.Errorf("shmclient: dial_query_command_log: decode response: %w", err)
	}
	return records, nil
}
