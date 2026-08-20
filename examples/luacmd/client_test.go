package luacmd_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/luacmd"
)

func TestFollowReportsEveryLineIncludingOnesWrittenBeforeItStarted(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeCluster()
	instanceID := "inst-follow"

	// Two lines already written: a caller that starts watching late still
	// has to see the whole run, or a log printed from a terminal would
	// silently begin in the middle.
	if err := cluster.Progress(ctx, selfPeer, instanceID, nil, "hello from outer begin"); err != nil {
		t.Fatalf("Progress: %v", err)
	}
	if err := cluster.Progress(ctx, selfPeer, instanceID, nil, "still working"); err != nil {
		t.Fatalf("Progress: %v", err)
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = cluster.Progress(ctx, selfPeer, instanceID, nil, "nearly there")
		time.Sleep(30 * time.Millisecond)
		_ = cluster.Append(ctx, selfPeer, instanceID, map[string]string{"status": "ok"}, "hello from outer end")
	}()

	var seen []string
	last, err := luacmd.Follow(ctx, cluster, instanceID, 5*time.Millisecond, func(entry luacmd.LogEntry) {
		seen = append(seen, entry.Narrative)
	})
	if err != nil {
		t.Fatalf("Follow: %v", err)
	}
	want := []string{"hello from outer begin", "still working", "nearly there", "hello from outer end"}
	if strings.Join(seen, "|") != strings.Join(want, "|") {
		t.Errorf("saw %v, want %v", seen, want)
	}
	if last.Narrative != "hello from outer end" || last.Fields["status"] != "ok" {
		t.Errorf("terminal entry = %+v", last)
	}
}

// "The run finished" and "I stopped watching" are different outcomes, and
// a caller printing a result must not mistake one for the other.
func TestFollowReportsGivingUpRatherThanReturningAPartialAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	cluster := newFakeCluster()
	if err := cluster.Progress(context.Background(), selfPeer, "inst-slow", nil, "working"); err != nil {
		t.Fatalf("Progress: %v", err)
	}

	_, err := luacmd.Follow(ctx, cluster, "inst-slow", 5*time.Millisecond, nil)
	if err == nil {
		t.Fatal("Follow returned a result for a run that never finished")
	}
	if !strings.Contains(err.Error(), "stopped following") {
		t.Errorf("error = %q", err)
	}
}

func TestLastRunFindsTheMostRecentDispatch(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeCluster()

	first := cluster.request("outer", "")
	time.Sleep(2 * time.Millisecond)
	second := cluster.request("outer", "")
	if err := cluster.Append(ctx, selfPeer, first, map[string]string{"status": "ok"}, "the older run"); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := cluster.Append(ctx, selfPeer, second, map[string]string{"status": "ok"}, "hello from outer end"); err != nil {
		t.Fatalf("Append: %v", err)
	}

	instanceID, entries, err := luacmd.LastRun(ctx, cluster, "outer")
	if err != nil {
		t.Fatalf("LastRun: %v", err)
	}
	if instanceID != second {
		t.Errorf("LastRun returned %s, want the later dispatch %s", instanceID, second)
	}
	if len(entries) != 1 || entries[0].Narrative != "hello from outer end" {
		t.Errorf("entries = %+v", entries)
	}
}

func TestLastRunSaysSoWhenACommandHasNeverRun(t *testing.T) {
	_, _, err := luacmd.LastRun(context.Background(), newFakeCluster(), "outer")
	if err == nil {
		t.Fatal("LastRun answered for a command with no dispatches")
	}
	if !strings.Contains(err.Error(), "never been run") {
		t.Errorf("error = %q", err)
	}
}

func TestFormatEntryIsStableAndLeavesTheTracebackOut(t *testing.T) {
	entry := luacmd.LogEntry{
		Timestamp: time.Date(2026, 8, 20, 9, 30, 15, 0, time.UTC),
		Fields: map[string]string{
			"status":         "ok",
			"child_instance": "inst-2",
			"child_command":  "inner",
			"traceback":      "stack traceback:\n\t[G]: ?",
		},
		Narrative: "hello from outer end",
	}
	line := luacmd.FormatEntry(entry)

	const want = "09:30:15  [ok] hello from outer end  child_command=inner  child_instance=inst-2"
	if line != want {
		t.Errorf("FormatEntry =\n  %q\nwant\n  %q", line, want)
	}
	if strings.Contains(line, "traceback") {
		t.Error("the traceback belongs in the record, not in a one-line summary")
	}
}
