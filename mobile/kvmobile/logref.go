package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/logref"
)

// This file is the mobile half of the log-reference Data Matrix code
// (api/logref.capnp, pkg/logref): the code a person scans to say "this
// is the thing I am standing in front of", rather than to run anything.
//
// It is deliberately thin. Encoding and decoding are pkg/logref's, the
// record it resolves through is an ordinary pkg/logrecord one, and the
// lookup is the same bounded range scan LogQuery already performs -- the
// only thing added here is the addressing convention that ties the three
// together, so that android-app's Kotlin (which has no capnp and no
// store access of its own) can go from scanned bytes to a group name in
// two calls.
//
// Everything crossing the binding is a plain type gomobile supports.
// That is why every log id here is an int64 rather than pkg/logref's own
// uint64: gomobile does not bind uint64 at all. Ids above
// math.MaxInt64 are consequently not reachable from a phone -- which
// costs nothing real (an id is a number somebody assigns to a physical
// object, not a hash) and is refused explicitly rather than silently
// wrapping.

// logRefLookupRetries/logRefLookupBackoff pace resolving an id that was
// registered moments ago on *another* device. The registration is an
// ordinary raft-replicated write, so the scanning device sees it only
// once its own replica has caught up -- normally already true by the
// time a camera has decoded the symbol, but a device that has just
// joined can be a heartbeat or two behind (kvmobile widens raft timeouts
// to 4s). Retrying briefly turns that race into a slightly slower scan
// instead of an "unknown log id" the person has no way to act on.
const (
	logRefLookupRetries = 12
	logRefLookupBackoff = 500 * time.Millisecond
)

// EncodeLogRef builds the Data Matrix payload naming logID -- the bytes
// android-app renders into a symbol. Needs no session and no signing
// key: a log reference asks a node for nothing (see api/logref.capnp),
// so a device that has never started can still print one.
func EncodeLogRef(logID int64) ([]byte, error) {
	id, err := logRefID(logID)
	if err != nil {
		return nil, err
	}
	return logref.Encode(id)
}

// DecodeLogRef parses scanned bytes as a log reference and returns the
// log id they name, or an error if they are not one (somebody else's
// barcode) or did not survive the trip (a decode whose checksum
// disagrees with its id). Session-free for the same reason as
// EncodeLogRef, which matters here: the scanning device tries this
// decoder on every code it sees, including before its own daemon is up.
func DecodeLogRef(data []byte) (int64, error) {
	id, err := logref.Decode(data)
	if err != nil {
		return 0, err
	}
	if id > maxLogRefID {
		return 0, fmt.Errorf("kvmobile: log id %d does not fit a signed 64-bit value", id)
	}
	return int64(id), nil
}

// PutLogRef registers logID as belonging to group, by appending an
// ordinary pkg/logrecord.Record under the convention pkg/logref
// documents (kind "objcode", unitID the id's decimal form). fieldsJSON
// is a JSON object of any further fields to carry on the record, or ""
// for none; narrative is free text, or "".
//
// Registering an id that already has a record is not an error and does
// not replace anything: log records are append-only, so this writes a
// newer one and the newest wins (see LogRefGroup). That is what lets a
// group assignment be corrected after labels are already printed.
func PutLogRef(logID int64, group, fieldsJSON, narrative string) error {
	id, err := logRefID(logID)
	if err != nil {
		return err
	}
	if group == "" {
		return fmt.Errorf("kvmobile: log reference %d needs a group", id)
	}

	fields := map[string]string{}
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("kvmobile: decode fieldsJSON: %w", err)
		}
	}
	// Written last so a caller cannot accidentally shadow the one field
	// this record exists to carry with a stray entry in fieldsJSON.
	fields[logref.GroupField] = group

	sess, err := currentSession()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := appendRecord(ctx, sess, logref.Kind, logref.UnitID(id), PeerID(), fields, narrative); err != nil {
		return fmt.Errorf("kvmobile: put log reference: %w", err)
	}
	return nil
}

// GetLogRef returns the newest registration for logID as a JSON
// pkg/logrecord.Record -- the group plus whatever else was recorded
// alongside it. Returns an error if the id has never been registered,
// which is a real answer rather than an empty one: a scanned code naming
// an unknown object is exactly the case a person needs told about.
//
// Unlike LogRefGroup this does not retry: a caller asking about an id it
// already has on screen is inspecting, not racing a replication lag.
func GetLogRef(logID int64) (string, error) {
	id, err := logRefID(logID)
	if err != nil {
		return "", err
	}
	rec, err := latestLogRef(id)
	if err != nil {
		return "", err
	}
	out, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode log reference: %w", err)
	}
	return string(out), nil
}

// LogRefGroup returns just the group logID belongs to -- the one thing a
// scanning device needs in order to react to a code, and so the call
// android-app's scan dispatch makes on every log reference it decodes.
//
// Retries briefly while the id is unknown (see logRefLookupRetries): the
// device that printed the code is usually the device that registered it,
// moments earlier, and this device may not have replicated that write
// yet.
func LogRefGroup(logID int64) (string, error) {
	id, err := logRefID(logID)
	if err != nil {
		return "", err
	}
	var lastErr error
	for attempt := 0; attempt < logRefLookupRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(logRefLookupBackoff)
		}
		rec, err := latestLogRef(id)
		if err != nil {
			lastErr = err
			continue
		}
		group := rec.Fields[logref.GroupField]
		if group == "" {
			// A record exists but declares no group. Retrying cannot
			// help -- this is a malformed registration, not a stale
			// replica -- so say so immediately.
			return "", fmt.Errorf("kvmobile: log reference %d is registered without a %q field", id, logref.GroupField)
		}
		return group, nil
	}
	return "", lastErr
}

// maxLogRefID is the largest id reachable across the gomobile binding --
// see this file's own doc comment on why ids are int64 here.
const maxLogRefID = uint64(1)<<63 - 1

func logRefID(logID int64) (uint64, error) {
	if logID < 0 {
		return 0, fmt.Errorf("kvmobile: log id %d must not be negative", logID)
	}
	return uint64(logID), nil
}

// latestLogRef reads the newest registration for id. logQuery returns
// records oldest first (the timestamp sits in the key ahead of the
// tiebreaker, see pkg/logrecord.BuildKey), so the last one is the one in
// force.
func latestLogRef(id uint64) (rec logrecord.Record, err error) {
	sess, err := currentSession()
	if err != nil {
		return rec, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	records, err := logQuery(ctx, sess, logref.Kind, logref.UnitID(id), time.Unix(0, 0), time.Now(), 0)
	if err != nil {
		return rec, err
	}
	if len(records) == 0 {
		return rec, fmt.Errorf("kvmobile: no log reference registered for %d", id)
	}
	return records[len(records)-1], nil
}
