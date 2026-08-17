package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
)

// KV mirrors pkg/kvctl.KV (one key/value pair) for JSON marshaling across
// the gomobile boundary -- see RangeScan.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RangeScan returns the key/value pairs in [start, end] (both inclusive,
// lexicographic byte order over the raw key bytes) on this device's own
// locally replicated state, as a JSON array (`"[]"` if none) -- the
// kvmobile counterpart to desktop's kvctl.RangeScan, a generic complement
// to Submit/Get for inspecting a whole range of keys at once. Like
// Submit/Get it isn't restricted to any particular namespace -- see
// kvctl.RangeScan's doc comment for why that's not a new privilege: this
// device's own daemon is no more (and no less) trusted than it already is
// for Submit/Get.
//
// order is "asc" (the default, and what an empty string means) or "desc";
// skip drops that many pairs from the front of that order; limit caps how
// many come back (0 = unlimited). See shmclient.Session.ScanRange for how
// the three compose and what a descending scan costs.
func RangeScan(start, end string, limit, skip int, order string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	pairs, err := sess.ScanRange(ctx, []byte(start), []byte(end), limit, skip, order)
	if err != nil {
		return "", fmt.Errorf("kvmobile: range scan: %w", err)
	}
	results := []KV{}
	for _, p := range pairs {
		results = append(results, KV{Key: string(p.Key), Value: string(p.Value)})
	}

	out, err := json.Marshal(results)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode range scan results: %w", err)
	}
	return string(out), nil
}
