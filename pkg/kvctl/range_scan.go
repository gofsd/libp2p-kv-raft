package kvctl

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// KV is one key/value pair, as returned by RangeScan/RangeScanFrom.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// The orders RangeScan accepts, re-exported from shmclient so a caller of
// this package (cmd/kvctl-cli, magefile.go) needn't import that one just to
// name an order.
const (
	RangeOrderAsc  = shmclient.RangeOrderAsc
	RangeOrderDesc = shmclient.RangeOrderDesc
)

// RangeScan implements `mage rangescan <start> <end> [limit] [skip]
// [order]`: returns every key/value pair in [start, end] (both inclusive,
// lexicographic byte order over the raw key bytes -- not numeric or any
// other ordering) on the current node, ordered by order ("asc", the
// default, or "desc"), with the first skip pairs of that order dropped and
// at most limit returned (0 = unlimited). This is a thin, generic
// counterpart to every internal caller already built on the same
// shmevent.EventListRange primitive (listUnitIDs, ListClusterMembers,
// logrecord.ScanBounds) -- those all narrow to one fixed namespace, this
// one doesn't: start/end are whatever the caller passes, covering ordinary
// Set/Get keys or anything else in the store, reserved namespaces
// (shmevent.SystemKeyPrefix, pkg/logrecord's own prefix) included. That's
// not a new privilege: every kvctl call only ever reaches the local
// daemon over shmring IPC, the same same-machine trust boundary Set/Get
// already operate under (see pkg/shmevent's package doc comment) -- a
// local caller already has unrestricted read access to its own node's
// entire store; this just exposes it conveniently instead of requiring a
// raw `sendevent` call.
//
// See shmclient.Session.ScanRange for how order/skip/limit compose, and
// why a descending scan reads the whole range rather than stopping early.
func RangeScan(start, end string, limit, skip int, order string) ([]KV, error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, err
	}
	peerID, err := reg.Current()
	if err != nil {
		return nil, err
	}
	return RangeScanFrom(peerID, start, end, limit, skip, order)
}

// RangeScanFrom is RangeScan against an explicit peerID, regardless of
// which node is currently selected -- the RangeScan equivalent of
// GetFrom, and how ListClusterMembers-style callers that already know
// which node they want reach this without disturbing "current".
//
// Opens one session for the whole scan rather than one per pair: the
// package-level shmclient.ListRange convenience wrapper this used to call
// in its loop re-Opened the node on every single pair, which for an
// n-pair range meant n handshakes to do one scan.
func RangeScanFrom(peerID, start, end string, limit, skip int, order string) ([]KV, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()

	sess, err := shmclient.Open(ctx, peerID)
	if err != nil {
		return nil, fmt.Errorf("rangescan: %w", err)
	}
	pairs, err := sess.ScanRange(ctx, []byte(start), []byte(end), limit, skip, order)
	if err != nil {
		return nil, fmt.Errorf("rangescan: %w", err)
	}
	results := make([]KV, 0, len(pairs))
	for _, p := range pairs {
		results = append(results, KV{Key: string(p.Key), Value: string(p.Value)})
	}
	return results, nil
}
