package croncmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// The read side of what a scheduler leaves behind. A claim key exists to
// stop a fire happening twice (see the package doc), but it also records
// what that fire became -- which schedule, which command, which dispatch --
// and that is the only durable answer to "did the nightly backup actually
// run last night, and under what instance id".
//
// It is a by-product rather than a second log: nothing here writes
// anything, and the records read back are exactly the ones Store.Claim
// created. That also bounds how far back it goes -- claims are pruned at
// Scheduler.Retain, so this is recent history, not an archive.

// Fire is one claimed dispatch, as a claim key holds it.
type Fire struct {
	// ScheduleID is the schedule that produced it.
	ScheduleID string `json:"schedule_id"`
	// CommandID is what was submitted.
	CommandID string `json:"command_id"`
	// At is the minute the schedule was due, in UTC -- not when the
	// submission happened, which may be up to one Interval later.
	At time.Time `json:"at"`
	// InstanceID is the dispatch it produced, and is empty when the claim
	// was won but the submission then failed. That combination is
	// deliberate and is the record of a fire that was consumed without
	// dispatching -- see Scheduler.RetryFailedSubmit.
	InstanceID string `json:"instance_id,omitempty"`
}

// Fires returns recent fires, most recent first, at most limit of them
// (limit <= 0 means all of them). scheduleID narrows it to one schedule;
// empty returns every schedule's, which is why the ordering is by time
// within a schedule rather than globally -- claim keys sort by schedule
// first, and re-sorting the whole set to interleave them would cost a
// comparison per pair for a view nobody reads that way.
//
// A claim key whose value does not decode is skipped: this is a report,
// and one unreadable row should not deny the rest.
func (c *Catalog) Fires(ctx context.Context, scheduleID string, limit int) ([]Fire, error) {
	prefix := c.prefix() + claimPart
	if scheduleID != "" {
		prefix += scheduleID + "/"
	}
	pairs, err := c.Store.Scan(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("croncmd: read fires: %w", err)
	}

	fires := make([]Fire, 0, len(pairs))
	// Scan is ascending and claim keys sort in time order within a
	// schedule, so walking backwards is "most recent first" without a sort.
	for i := len(pairs) - 1; i >= 0; i-- {
		var fire Fire
		if err := json.Unmarshal([]byte(pairs[i].Value), &fire); err != nil {
			continue
		}
		if fire.At.IsZero() {
			// Not a claim this package wrote -- see pruneClaims, which
			// leaves such a key alone for the same reason.
			continue
		}
		fires = append(fires, fire)
		if limit > 0 && len(fires) == limit {
			break
		}
	}
	return fires, nil
}
