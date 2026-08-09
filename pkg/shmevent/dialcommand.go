package shmevent

import (
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// NewPublicAccess builds a publicAccess Msg: asks this node to dial
// targetPeer (a multiaddr, or a bare peer id -- see resolveDialAddr) and
// self-service this node's own public-group/relay standing there. Local-
// only. note may be "".
func NewPublicAccess(targetPeer, note string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetPublicAccess()
	grp := m.PublicAccess()
	if err := grp.SetTargetPeer(targetPeer); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_public_access: %w", err)
	}
	if err := grp.SetNote(note); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_public_access: %w", err)
	}
	return m, nil
}

// NewDialSubmitCommand builds a dialSubmitCommand Msg: asks this node to
// dial targetPeer (a multiaddr, or a bare peer id) and submit
// commandID/inputsJSON there as a CommandRequestLogKind write, provided
// targetPeer has linked commandID to a public group. Local-only.
// inputsJSON/note may be "".
func NewDialSubmitCommand(targetPeer, commandID, inputsJSON, note string) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetDialSubmitCommand()
	grp := m.DialSubmitCommand()
	if err := grp.SetTargetPeer(targetPeer); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_dial_submit_command: %w", err)
	}
	if err := grp.SetCommandId(commandID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_dial_submit_command: %w", err)
	}
	if err := grp.SetInputsJson(inputsJSON); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_dial_submit_command: %w", err)
	}
	if err := grp.SetNote(note); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_dial_submit_command: %w", err)
	}
	return m, nil
}

// NewDialQueryCommandLog builds a dialQueryCommandLog request Msg: asks
// this node to dial targetPeer and read back instanceID's own
// CommandExecLogKind entries in [since, until] (up to limit records, 0
// meaning unbounded) -- needs no standing at all, possessing instanceID
// is the credential. Local-only. A zero since/until is treated as "no
// bound".
func NewDialQueryCommandLog(targetPeer, instanceID string, since, until time.Time, limit int) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetDialQueryCommandLog()
	grp := m.DialQueryCommandLog()
	if err := grp.SetTargetPeer(targetPeer); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_dial_query_command_log: %w", err)
	}
	if err := grp.SetInstanceId(instanceID); err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_dial_query_command_log: %w", err)
	}
	grp.SetSince(since.UnixNano())
	grp.SetUntil(until.UnixNano())
	grp.SetLimit(int32(limit))
	return m, nil
}

// NewDialQueryCommandLogResponse builds a dialQueryCommandLog Msg carrying
// records -- the response side of NewDialQueryCommandLog's request, each
// record its own logrecord.Record.Encode() blob (capnp's List(Data)
// handles the aggregation natively, unlike the old design's hand-rolled
// EncodeLogRecordList).
func NewDialQueryCommandLogResponse(records []logrecord.Record) (Msg, error) {
	m, err := newMsg()
	if err != nil {
		return Msg{}, err
	}
	m.SetDialQueryCommandLog()
	grp := m.DialQueryCommandLog()
	list, err := grp.NewRecords(int32(len(records)))
	if err != nil {
		return Msg{}, fmt.Errorf("shmevent: new_dial_query_command_log_response: %w", err)
	}
	for i, r := range records {
		encoded, err := r.Encode()
		if err != nil {
			return Msg{}, fmt.Errorf("shmevent: new_dial_query_command_log_response: record %d: %w", i, err)
		}
		if err := list.Set(i, encoded); err != nil {
			return Msg{}, fmt.Errorf("shmevent: new_dial_query_command_log_response: record %d: %w", i, err)
		}
	}
	return m, nil
}

// DialQueryCommandLogRecords reads a dialQueryCommandLog response Msg's
// records back out as []logrecord.Record -- NewDialQueryCommandLogResponse's
// inverse.
func DialQueryCommandLogRecords(grp Event_dialQueryCommandLog) ([]logrecord.Record, error) {
	list, err := grp.Records()
	if err != nil {
		return nil, fmt.Errorf("shmevent: dial_query_command_log records: %w", err)
	}
	out := make([]logrecord.Record, list.Len())
	for i := range out {
		raw, err := list.At(i)
		if err != nil {
			return nil, fmt.Errorf("shmevent: dial_query_command_log record %d: %w", i, err)
		}
		rec, err := logrecord.Decode(raw)
		if err != nil {
			return nil, fmt.Errorf("shmevent: dial_query_command_log record %d: %w", i, err)
		}
		out[i] = rec
	}
	return out, nil
}
