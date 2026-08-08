package kvctl

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// DialSubmitCommand implements `mage dialsubmitcommand <targetAddr>
// <commandID> <inputsJSON>`: asks the *current* node (mage use) to dial
// targetAddr directly and submit commandID/inputsJSON there as a
// CommandRequestLogKind write into *that* cluster's own log -- no raft
// membership in targetAddr's cluster required, provided commandID is
// linked to a public group there. RequestPublicAccess generalized from one
// hardcoded command to any commandID -- see
// shmevent.EventDialSubmitCommand's doc comment. Returns the new
// dispatch's instance id, usable with DialQueryCommandLog.
func DialSubmitCommand(targetAddr, commandID, inputsJSON, note string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialCommandTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return "", err
	}
	instanceID, err := sess.DialSubmitCommand(ctx, targetAddr, commandID, inputsJSON, note)
	if err != nil {
		return "", fmt.Errorf("dial submit command: %w", err)
	}
	return instanceID, nil
}

// DialQueryCommandLog implements `mage dialquerycommandlog <targetAddr>
// <instanceID>`: asks the *current* node to dial targetAddr directly and
// read back instanceID's own CommandExecLogKind entries in [since, until],
// up to limit records (0 meaning unbounded) -- DialSubmitCommand's
// read-back counterpart. Needs no standing in targetAddr's cluster at all:
// possessing instanceID is the credential (see
// shmevent.EventDialQueryCommandLog's doc comment).
func DialQueryCommandLog(targetAddr, instanceID string, since, until time.Time, limit int) ([]logrecord.Record, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialCommandTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}
	records, err := sess.DialQueryCommandLog(ctx, targetAddr, instanceID, since, until, limit)
	if err != nil {
		return nil, fmt.Errorf("dial query command log: %w", err)
	}
	return records, nil
}

// dialCommandTimeout bounds DialSubmitCommand/DialQueryCommandLog's IPC
// round trip. Like publicAccessTimeout, the daemon does real outbound
// network work inside this call -- connect to the target, then a full
// ClientProtocolID request whose response only comes back once the
// target's raft has committed the write (or, for a query, once it's
// scanned its own log) -- so it needs the same headroom past the usual
// same-machine IPC budget.
const dialCommandTimeout = 60 * time.Second
