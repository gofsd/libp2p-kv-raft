package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// DialSubmitCommand asks this device's own node to dial targetAddr
// directly and submit commandID/inputsJSON there as a CommandRequestLogKind
// write into *that* cluster's own log -- SubmitCommand for a cluster this
// device is a total stranger to, provided commandID is linked to a public
// group there. RequestPublicAccess generalized from one hardcoded command
// to any commandID -- see shmevent.EventDialSubmitCommand's doc comment on
// the Go side for the full design.
//
// Unlike SubmitCommand, no local exec-index/Execute-poke bookkeeping is
// written here: this device has no local replica of targetAddr's cluster
// to index against, and the poke would have nowhere local to land anyway.
// Returns the new dispatch's instance id, usable with
// DialQueryCommandLog/DialWatchCommandLog to read back its result.
func DialSubmitCommand(targetAddr, commandID, inputsJSON, note string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	instanceID, err := sess.DialSubmitCommand(ctx, targetAddr, commandID, inputsJSON, note)
	if err != nil {
		return "", fmt.Errorf("kvmobile: dial submit command: %w", err)
	}
	return instanceID, nil
}

// DialQueryCommandLog asks this device's own node to dial targetAddr
// directly and read back instanceID's own CommandExecLogKind entries in
// [since, until], up to limit records -- DialSubmitCommand's read-back
// counterpart, and QueryCommandLog's remote equivalent for a cluster this
// device has no local replica of. since/until are RFC3339 or "" (since ""
// = unbounded, until "" = now); limit is a count or "" (no limit). Needs
// no standing in targetAddr's cluster at all: possessing instanceID is the
// credential (see shmevent.EventDialQueryCommandLog's doc comment).
func DialQueryCommandLog(targetAddr, instanceID, since, until, limit string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	start := time.Unix(0, 0)
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return "", fmt.Errorf("kvmobile: since: %w", err)
		}
		start = t
	}
	end := time.Now()
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return "", fmt.Errorf("kvmobile: until: %w", err)
		}
		end = t
	}
	n := 0
	if limit != "" {
		v, err := strconv.Atoi(limit)
		if err != nil {
			return "", fmt.Errorf("kvmobile: limit: %w", err)
		}
		n = v
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	records, err := sess.DialQueryCommandLog(ctx, targetAddr, instanceID, start, end, n)
	if err != nil {
		return "", fmt.Errorf("kvmobile: dial query command log: %w", err)
	}
	if records == nil {
		records = []logrecord.Record{}
	}
	out, err := json.Marshal(records)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode dial query command log result: %w", err)
	}
	return string(out), nil
}

// dialCommandLogWatch/dialCommandLogWatches mirror commandLogWatch/
// commandLogWatches exactly, kept as a separate map so a local
// WatchCommandLog and a remote DialWatchCommandLog on coincidentally-equal
// instance ids (the two are drawn from entirely different clusters' own id
// spaces) never collide or stop each other.
type dialCommandLogWatch struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var (
	dialCommandLogWatchMu sync.Mutex
	dialCommandLogWatches = map[string]dialCommandLogWatch{}
)

// DialWatchCommandLog is WatchCommandLog's direct-dial counterpart: polls
// DialQueryCommandLog(targetAddr, instanceID, ...) on a timer and invokes
// cb.OnRecords with whatever's new since the last poll, until
// StopDialWatchCommandLog(instanceID) is called. A second
// DialWatchCommandLog call for the same instanceID replaces the first
// (stopping it first); different instanceIDs run independently.
func DialWatchCommandLog(targetAddr, instanceID string, cb LogCallback) error {
	if cb == nil {
		return fmt.Errorf("kvmobile: DialWatchCommandLog: cb must not be nil")
	}
	if instanceID == "" {
		return fmt.Errorf("kvmobile: DialWatchCommandLog: instanceID must not be empty")
	}

	dialCommandLogWatchMu.Lock()
	defer dialCommandLogWatchMu.Unlock()
	stopDialCommandLogWatchLocked(instanceID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	dialCommandLogWatches[instanceID] = dialCommandLogWatch{cancel: cancel, done: done}

	go runDialCommandLogWatch(ctx, done, targetAddr, instanceID, cb)
	return nil
}

// StopDialWatchCommandLog stops instanceID's dial-watcher, if any, and
// waits for it to actually exit before returning. Safe to call when
// nothing is running for it (a no-op).
func StopDialWatchCommandLog(instanceID string) {
	dialCommandLogWatchMu.Lock()
	defer dialCommandLogWatchMu.Unlock()
	stopDialCommandLogWatchLocked(instanceID)
}

// stopDialCommandLogWatchLocked requires dialCommandLogWatchMu already held.
func stopDialCommandLogWatchLocked(instanceID string) {
	w, ok := dialCommandLogWatches[instanceID]
	if !ok {
		return
	}
	w.cancel()
	<-w.done
	delete(dialCommandLogWatches, instanceID)
}

// runDialCommandLogWatch is DialWatchCommandLog's background loop body --
// byte-for-byte runCommandLogWatch's loop, with its one local
// QueryCommandLog call replaced by a direct dial to targetAddr.
func runDialCommandLogWatch(ctx context.Context, done chan struct{}, targetAddr, instanceID string, cb LogCallback) {
	defer close(done)

	// since tracks the timestamp just past the newest record already
	// delivered to cb, so each round only asks for what's new.
	var since time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchCommandLogPollInterval):
		}

		sinceStr := ""
		if !since.IsZero() {
			sinceStr = since.Format(time.RFC3339Nano)
		}
		out, err := DialQueryCommandLog(targetAddr, instanceID, sinceStr, "", "")
		if err != nil {
			continue
		}
		var records []logrecord.Record
		if err := json.Unmarshal([]byte(out), &records); err != nil || len(records) == 0 {
			continue
		}

		cb.OnRecords(out)
		since = records[len(records)-1].Timestamp.Add(time.Nanosecond)
	}
}
