//go:build mage

package main

import (
	"encoding/json"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// SubmitCommand implements `mage submitcommand <commandID> <inputsJSON>`:
// dispatches commandID as a durable, replicated request plus a
// low-latency Execute poke to its PeerID, and prints the new instance id
// -- see pkg/kvctl.SubmitCommand's doc comment. Requires the current
// node's own peer id to be permitted for commandID (some group both
// commandID is linked to and the current node is a member of).
// Usage: mage submitcommand <commandID> <inputsJSON>
func SubmitCommand(commandID, inputsJSON string) error {
	instanceID, err := kvctl.SubmitCommand(commandID, inputsJSON)
	if err != nil {
		return err
	}
	fmt.Println(instanceID)
	return nil
}

// GetCommandRequest implements `mage getcommandrequest <commandID>
// <instanceID>`: prints instanceID's dispatch record for commandID as one
// JSON object.
// Usage: mage getcommandrequest <commandID> <instanceID>
func GetCommandRequest(commandID, instanceID string) error {
	req, err := kvctl.GetCommandRequest(commandID, instanceID)
	if err != nil {
		return err
	}
	out, err := json.Marshal(req)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListCommandRequests implements `mage listcommandrequests <commandID>`:
// prints every dispatch request currently recorded for commandID, oldest
// first, one JSON object per line -- the target's catch-up path for a
// missed Execute poke.
// Usage: mage listcommandrequests <commandID>
func ListCommandRequests(commandID string) error {
	requests, err := kvctl.ListCommandRequests(commandID)
	if err != nil {
		return err
	}
	for _, r := range requests {
		out, err := json.Marshal(r)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// ListExecutions implements `mage listexecutions <peerID>`: prints up to
// the 200 most recent SubmitCommand dispatches touching peerID, as either
// requester or target, most recent first, one JSON object per line -- see
// pkg/kvctl.ListExecutionsByPeer's doc comment.
// Usage: mage listexecutions <peerID>
func ListExecutions(peerID string) error {
	executions, err := kvctl.ListExecutionsByPeer(peerID)
	if err != nil {
		return err
	}
	for _, e := range executions {
		out, err := json.Marshal(e)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// AppendCommandLog implements `mage appendcommandlog <requesterPeerID>
// <instanceID> <fieldsJSON> <narrative>`: writes one execution-log entry
// for instanceID and pokes requesterPeerID (pass "" to skip the poke) --
// see pkg/kvctl.AppendCommandLog's doc comment.
// Usage: mage appendcommandlog <requesterPeerID> <instanceID> <fieldsJSON> <narrative>
func AppendCommandLog(requesterPeerID, instanceID, fieldsJSON, narrative string) error {
	var fields map[string]string
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("decode fieldsJSON: %w", err)
		}
	}
	if err := kvctl.AppendCommandLog(requesterPeerID, instanceID, fields, narrative); err != nil {
		return err
	}
	fmt.Println("✅ command log appended")
	return nil
}

// ReportProgress implements `mage reportprogress <requesterPeerID>
// <instanceID> <fieldsJSON> <narrative>`: like appendcommandlog, but
// stamps fields["status"] = "running" first -- see
// pkg/kvctl.ReportProgress's doc comment on why that's what keeps
// RunCommandDispatcher's dedup check from treating instanceID as already
// handled. Mainly for a CommandHandler's own Go code to call directly;
// this mage target exists as the same manual escape hatch
// appendcommandlog already is.
// Usage: mage reportprogress <requesterPeerID> <instanceID> <fieldsJSON> <narrative>
func ReportProgress(requesterPeerID, instanceID, fieldsJSON, narrative string) error {
	var fields map[string]string
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("decode fieldsJSON: %w", err)
		}
	}
	if err := kvctl.ReportProgress(requesterPeerID, instanceID, fields, narrative); err != nil {
		return err
	}
	fmt.Println("✅ progress reported")
	return nil
}

// QueryCommandLog implements `mage querycommandlog <instanceID> <since>
// <until> <limit>`: lists every AppendCommandLog entry for instanceID
// whose timestamp falls in [since, until], oldest first, one JSON object
// per line -- see LogQuery's doc comment for the since/until/limit
// argument shape.
// Usage: mage querycommandlog <instanceID> <since|""> <until|""> <limit|"">
func QueryCommandLog(instanceID, since, until, limit string) error {
	start, end, n, err := parseTimeWindow(since, until, limit)
	if err != nil {
		return err
	}

	records, err := kvctl.QueryCommandLog(instanceID, start, end, n)
	if err != nil {
		return err
	}
	for _, rec := range records {
		out, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// LatestCommandLog implements `mage latestcommandlog <instanceID>`:
// prints instanceID's single most recent AppendCommandLog entry as one
// JSON object -- see pkg/kvctl.LatestCommandLog's doc comment.
// Usage: mage latestcommandlog <instanceID>
func LatestCommandLog(instanceID string) error {
	rec, err := kvctl.LatestCommandLog(instanceID)
	if err != nil {
		return err
	}
	out, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
