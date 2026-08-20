package luacmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// The submitting side: dispatch a Lua command and watch its log, rather
// than serve one. Shared by the desktop CLI and (via its own adapter) the
// Android app, because "run it and show me the lines as they arrive" is
// the same problem on both.

// DefaultFollowInterval is how often Follow re-reads a run's log. Well
// under the runner's own scan interval, so the first line of a run shows
// up promptly once it starts rather than a poll later.
const DefaultFollowInterval = 500 * time.Millisecond

// followMaxReadFailures is how many consecutive failed log reads Follow
// rides out before giving up. Observed once: a whole-repo `go test ./...`
// runs packages in parallel, several of which spawn their own daemons, and
// under that load a single read can exceed its own IPC deadline while the
// run itself is perfectly healthy.
const followMaxReadFailures = 3

// Follow watches instanceID's log until a terminal entry lands or ctx is
// done, calling onEntry once for every entry as it appears -- including
// entries already written before the call, so a caller that starts
// watching late still sees the whole run.
//
// Returns the terminal entry. A context that ends first is an error, not a
// silent partial answer: "the run finished" and "I stopped watching" are
// different outcomes and a caller printing a result must not confuse them.
func Follow(ctx context.Context, cluster Cluster, instanceID string, interval time.Duration, onEntry func(LogEntry)) (LogEntry, error) {
	if instanceID == "" {
		return LogEntry{}, fmt.Errorf("luacmd: follow: an instance id is required")
	}
	if interval <= 0 {
		interval = DefaultFollowInterval
	}

	seen, failures := 0, 0
	for {
		entries, err := cluster.QueryLog(ctx, instanceID)
		if err != nil {
			// A read that fails is usually the daemon being busy, not the
			// run being lost -- the same transient every other polling
			// loop in this repo rides out rather than exiting on. Give up
			// only when it keeps failing, so a genuinely broken daemon
			// still reports rather than hanging until the caller's own
			// deadline.
			failures++
			if failures >= followMaxReadFailures {
				return LogEntry{}, fmt.Errorf("luacmd: reading %s failed %d times in a row: %w", instanceID, failures, err)
			}
			select {
			case <-ctx.Done():
				return LogEntry{}, fmt.Errorf("luacmd: stopped following %s: %w", instanceID, ctx.Err())
			case <-time.After(interval):
			}
			continue
		}
		failures = 0
		for ; seen < len(entries); seen++ {
			if onEntry != nil {
				onEntry(entries[seen])
			}
		}
		if len(entries) > 0 {
			if last := entries[len(entries)-1]; last.Done() {
				return last, nil
			}
		}

		select {
		case <-ctx.Done():
			return LogEntry{}, fmt.Errorf("luacmd: stopped following %s: %w", instanceID, ctx.Err())
		case <-time.After(interval):
		}
	}
}

// LastRun returns the most recently submitted instance of commandID and
// its log.
//
// This exists because an instance id is not always available to whoever
// wants the log: a request submitted from another device, or from a
// barcode, or by a scheduler, hands its id back to the submitter and
// nobody else. "The last run of this command" is the question a person
// actually has in front of them.
//
// Most recent by request time, which is the order the requests were
// recorded in; ties go to whichever the request list returns later.
func LastRun(ctx context.Context, cluster Cluster, commandID string) (string, []LogEntry, error) {
	requests, err := cluster.ListRequests(ctx, commandID)
	if err != nil {
		return "", nil, err
	}
	if len(requests) == 0 {
		return "", nil, fmt.Errorf("luacmd: %s has never been run", commandID)
	}

	latest := requests[0]
	for _, request := range requests[1:] {
		if !request.RequestedAt.Before(latest.RequestedAt) {
			latest = request
		}
	}
	entries, err := cluster.QueryLog(ctx, latest.InstanceID)
	if err != nil {
		return latest.InstanceID, nil, err
	}
	return latest.InstanceID, entries, nil
}

// FormatEntry renders one log entry as a line for a terminal or a log
// list: the time, the status if it has one, the narrative, and any fields
// beyond status.
func FormatEntry(entry LogEntry) string {
	line := ""
	if !entry.Timestamp.IsZero() {
		line += entry.Timestamp.Format("15:04:05") + "  "
	}
	if status := entry.Status(); status != "" {
		line += "[" + status + "] "
	}
	line += entry.Narrative

	// Sorted, not map order: this goes into terminal output and test
	// expectations, and a line that reorders itself between runs is
	// unreadable in both.
	keys := make([]string, 0, len(entry.Fields))
	for key := range entry.Fields {
		if key == "status" || key == "traceback" || key == FieldResult {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		line += fmt.Sprintf("  %s=%s", key, entry.Fields[key])
	}
	// A structured result is announced by size, not printed: it is
	// frequently kilobytes, and a log line that becomes a wall of JSON
	// stops being scannable, which is the only thing a one-liner is for.
	// FormatResult renders it for a caller that wants it.
	if payload := entry.Fields[FieldResult]; payload != "" {
		line += fmt.Sprintf("  result=%dB", len(payload))
	}
	return line
}

// FormatResult renders an entry's structured result as an indented block
// to print under its FormatEntry line, and reports whether there was one.
//
// Pretty-printed when it parses, verbatim when it does not: a command that
// wrote something other than JSON into that field still has its answer
// shown rather than swallowed, which is the same "not an error, just not
// structured" treatment a script gets from a record's own result.
func FormatResult(entry LogEntry) (string, bool) {
	payload := entry.Fields[FieldResult]
	if payload == "" {
		return "", false
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, []byte(payload), "    ", "  "); err != nil {
		return "    " + payload, true
	}
	return "    " + indented.String(), true
}
