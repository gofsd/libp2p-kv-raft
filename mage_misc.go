//go:build mage

package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// GetOwnAddr prints the current node's own current best-advertised
// multiaddr -- the address to embed in a "<ownAddr>#<tokenHex>" ticket
// (printjoinrequestdatamatrix) or invite (printjoininvitedatamatrix/
// printexecinvitedatamatrix). Queried live: a node started with
// -relay-peer only gets its actual circuit-relay reservation asynchronously
// in the background after startup, so re-run this a moment later if an
// earlier call returned a private/loopback address instead.
//
// Usage: mage getownaddr
func GetOwnAddr() error {
	addr, err := kvctl.GetOwnAddr()
	if err != nil {
		return err
	}
	fmt.Println(addr)
	return nil
}

// Version prints the current node's own build/version info as one JSON
// object -- git commit, dirty flag, build time, Go version, and the
// go-libp2p version it was built against (see shmevent.EventGetVersion's
// doc comment). Queried live against the running daemon, never cached, so
// it always reflects whichever binary is actually running right now, not
// whatever's currently checked out in this source tree.
//
// Usage: mage version
func Version() error {
	info, err := kvctl.Version()
	if err != nil {
		return err
	}
	out, err := json.Marshal(info)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// Execute sends value as a direct peer-to-peer EventExecute notification
// from the current node to destPeerID -- bypassing raft and the store
// entirely, see shmevent.EventExecute's doc comment.
// Usage: mage execute <destPeerID> <value>
func Execute(destPeerID, value string) error {
	if err := kvctl.Execute(destPeerID, value); err != nil {
		return err
	}
	fmt.Println("✅ execute sent")
	return nil
}

// PollExecute drains one queued EventExecute notification delivered to the
// current node, if any, and prints its sender peer id and value.
// Usage: mage pollexecute
func PollExecute() error {
	senderPeerID, value, ok, err := kvctl.PollExecute()
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println("(no execute notification pending)")
		return nil
	}
	fmt.Printf("%s: %s\n", senderPeerID, value)
	return nil
}

// Openchannel opens a persistent, bidirectional, multipurpose stream
// from the current node to peerID and pumps this process's own
// stdin/stdout through it -- everything read from stdin is sent to
// peerID, everything peerID sends back is written to stdout -- until
// stdin reaches EOF, the remote side closes the channel, or this process
// receives SIGINT/SIGTERM. purpose tags every chunk this process sends
// (shmevent.ChannelPurposeName/FromName -- "data"/"control"/"video", or a
// plain decimal number for a custom purpose); may be "" for the default
// data purpose. See shmevent.EventChannelOpen's doc comment for the wire
// design, and Listenchannel for the callee-side counterpart.
// Usage: mage openchannel <peerID> <purpose|"">
func Openchannel(peerID, purpose string) error {
	return kvctl.OpenChannel(peerID, purpose)
}

// Listenchannel blocks until another peer opens an incoming channel to
// the current node, then pumps stdin/stdout through it exactly like
// Openchannel does. purpose tags this process's own outgoing chunks the
// same way Openchannel's does; may be "" for the default data purpose.
// Prints the remote peer id to stderr once claimed; stdout is reserved
// for the pipe itself.
// Usage: mage listenchannel <purpose|"">
func Listenchannel(purpose string) error {
	return kvctl.ListenChannel(purpose)
}

// LogAppend appends a pkg/logrecord.Record of the given kind/unit,
// timestamped now and attributed to the current node -- see
// pkg/logrecord's doc comment for the generic key/record scheme this
// project builds structured log/report records on top of. kind and
// unitID are entirely caller-chosen strings, not a fixed set. fieldsJSON
// may be "" (no structured fields).
// Usage: mage logappend <kind> <unitID> <fieldsJSON> <narrative>
func LogAppend(kind, unitID, fieldsJSON, narrative string) error {
	var fields map[string]string
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("decode fieldsJSON: %w", err)
		}
	}
	if err := kvctl.LogAppend(kind, unitID, fields, narrative); err != nil {
		return err
	}
	fmt.Println("✅ log appended")
	return nil
}

// LogQuery lists every pkg/logrecord.Record of the given kind/unit whose
// timestamp falls in [since, until], oldest first, one JSON object per
// line. since/until are RFC3339 or "" (since "" = unbounded, until "" =
// now). limit is a count or "" (unlimited).
// Usage: mage logquery <kind> <unitID> <since|""> <until|""> <limit|"">
func LogQuery(kind, unitID, since, until, limit string) error {
	start, end, n, err := parseTimeWindow(since, until, limit)
	if err != nil {
		return err
	}

	records, err := kvctl.LogQuery(kind, unitID, start, end, n)
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

// parseTimeWindow parses the since/until/limit string triple every
// *Query-style mage target (LogQuery, QueryCommandLog) takes on the
// command line into kvctl.LogQuery's native (start, end time.Time, limit
// int) shape: since/until are RFC3339 or "" (since "" = unbounded epoch,
// until "" = now); limit is a count or "" (0, meaning unlimited).
func parseTimeWindow(since, until, limit string) (start, end time.Time, n int, err error) {
	start = time.Unix(0, 0)
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return time.Time{}, time.Time{}, 0, fmt.Errorf("since: %w", err)
		}
		start = t
	}
	end = time.Now()
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return time.Time{}, time.Time{}, 0, fmt.Errorf("until: %w", err)
		}
		end = t
	}
	if limit != "" {
		v, err := strconv.Atoi(limit)
		if err != nil {
			return time.Time{}, time.Time{}, 0, fmt.Errorf("limit: %w", err)
		}
		n = v
	}
	return start, end, n, nil
}
