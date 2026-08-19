package kvmobile

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/croncmd"
)

// recordingCronListener is a Go-side CronListener for tests -- Kotlin
// would implement the same interface through gomobile's reverse binding;
// from Go it is an ordinary type to satisfy, the same way
// recordingDispatchHandler is.
type recordingCronListener struct {
	mu    sync.Mutex
	fires []string
	errs  []string
}

func (l *recordingCronListener) OnFire(scheduleID, commandID, fire, instanceID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fires = append(l.fires, scheduleID+" "+commandID+" "+instanceID)
}

func (l *recordingCronListener) OnSkip(string, string, string) {}

func (l *recordingCronListener) OnError(message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, message)
}

func (l *recordingCronListener) fired() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.fires)
}

// TestCronScheduleDispatchesACommandFromADevice drives the whole loop the
// way an app would: the device registers a schedule, runs a scheduler, and
// a command it is permitted to submit gets dispatched and handled -- all
// through the gomobile-shaped API.
//
// The one thing it arranges rather than waits for is the clock. A minute
// resolution schedule would otherwise leave this test waiting up to a
// minute for a boundary, so the schedule's watermark is seeded in the past
// and the scheduler catches it up on its first pass -- which is the same
// code path a phone that was asleep takes when it wakes, and so worth
// exercising in its own right.
func TestCronScheduleDispatchesACommandFromADevice(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	selfPeerID := PeerID()

	const commandID = "cmd-cron"
	const groupID = "grp-cron"
	if err := CreateCommand(commandID, "Nightly sweep", selfPeerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(commandID)
		return err == nil, nil
	})
	grantCommandAccess(t, commandID, groupID, selfPeerID)

	handler := &recordingDispatchHandler{}
	if err := RunCommandDispatcher(commandID, handler); err != nil {
		t.Fatalf("RunCommandDispatcher: %v", err)
	}
	t.Cleanup(func() { StopCommandDispatcher(commandID) })

	const scheduleID = "sweep"
	const inputs = `{"op":"sweep"}`
	if err := CronPut(scheduleID, "* * * * *", commandID, inputs, ""); err != nil {
		t.Fatalf("CronPut: %v", err)
	}
	// Seed the watermark two minutes back, so the first pass has something
	// due rather than waiting for the next minute boundary.
	watermark := time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339)
	if err := Submit(croncmd.DefaultKeyPrefix+"watermark/"+scheduleID, watermark); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}

	listener := &recordingCronListener{}
	if err := CronServeWithListener(1, 3600, listener); err != nil {
		t.Fatalf("CronServeWithListener: %v", err)
	}
	t.Cleanup(StopCronServe)
	if !CronServing() {
		t.Fatal("CronServing reported no scheduler after starting one")
	}

	pollUntilTrue(t, 30*time.Second, func() (bool, error) {
		return listener.fired() > 0, nil
	})
	// The command the schedule named actually ran, with the schedule's
	// inputs -- which is the whole claim this package makes.
	pollUntilTrue(t, 30*time.Second, func() (bool, error) {
		return handler.calls > 0, nil
	})

	StopCronServe()
	if CronServing() {
		t.Fatal("CronServing still reported a scheduler after StopCronServe")
	}

	// The claims are the durable record of what was dispatched, readable
	// by any node rather than only the one that fired.
	raw, err := CronFires(scheduleID, 0)
	if err != nil {
		t.Fatalf("CronFires: %v", err)
	}
	var fires []croncmd.Fire
	if err := json.Unmarshal([]byte(raw), &fires); err != nil {
		t.Fatalf("CronFires returned %q: %v", raw, err)
	}
	if len(fires) == 0 {
		t.Fatal("CronFires returned nothing after a dispatch")
	}
	for _, fire := range fires {
		if fire.ScheduleID != scheduleID || fire.CommandID != commandID || fire.InstanceID == "" {
			t.Fatalf("fire = %+v, want it to name the schedule, the command and its dispatch", fire)
		}
	}
}

// TestCronCatalogFromADevice covers the CRUD half, which needs a daemon
// but no scheduler and no clock.
func TestCronCatalogFromADevice(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CronPut("nightly", "@daily", "cmd-backup", `{"op":"backup"}`, "Asia/Tokyo"); err != nil {
		t.Fatalf("CronPut: %v", err)
	}
	// An expression nothing can read is refused where a form can still
	// show the error, not silently at the next tick.
	if err := CronPut("broken", "not a cron expression", "cmd-backup", "", ""); err == nil {
		t.Fatal("CronPut accepted an expression it cannot honour")
	}

	raw, err := CronGet("nightly")
	if err != nil {
		t.Fatalf("CronGet: %v", err)
	}
	var schedule croncmd.Schedule
	if err := json.Unmarshal([]byte(raw), &schedule); err != nil {
		t.Fatalf("CronGet returned %q: %v", raw, err)
	}
	if schedule.CommandID != "cmd-backup" || schedule.Location != "Asia/Tokyo" || schedule.Inputs != `{"op":"backup"}` {
		t.Fatalf("CronGet = %+v, want the schedule as written", schedule)
	}

	if err := CronSetEnabled("nightly", false); err != nil {
		t.Fatalf("CronSetEnabled: %v", err)
	}
	listed, err := CronList()
	if err != nil {
		t.Fatalf("CronList: %v", err)
	}
	var schedules []croncmd.Schedule
	if err := json.Unmarshal([]byte(listed), &schedules); err != nil {
		t.Fatalf("CronList returned %q: %v", listed, err)
	}
	if len(schedules) != 1 || !schedules[0].Disabled {
		t.Fatalf("CronList = %+v, want the one schedule, disabled", schedules)
	}

	if err := CronDelete("nightly"); err != nil {
		t.Fatalf("CronDelete: %v", err)
	}
	if _, err := CronGet("nightly"); err == nil {
		t.Fatal("CronGet found a deleted schedule")
	}
}

// TestCronNextNeedsNoDaemon is the one binding a form can call before
// anything exists: "when would this fire", answered from the expression
// alone.
func TestCronNextNeedsNoDaemon(t *testing.T) {
	raw, err := CronNext("0 3 * * mon", 3, "UTC")
	if err != nil {
		t.Fatalf("CronNext: %v", err)
	}
	var stamps []string
	if err := json.Unmarshal([]byte(raw), &stamps); err != nil {
		t.Fatalf("CronNext returned %q: %v", raw, err)
	}
	if len(stamps) != 3 {
		t.Fatalf("CronNext returned %d times, want 3", len(stamps))
	}
	for _, stamp := range stamps {
		at, err := time.Parse(time.RFC3339, stamp)
		if err != nil {
			t.Fatalf("CronNext returned %q, which is not a timestamp: %v", stamp, err)
		}
		if at.Weekday() != time.Monday || at.Hour() != 3 || at.Minute() != 0 {
			t.Fatalf("CronNext returned %s, want 03:00 on a Monday", stamp)
		}
	}

	if _, err := CronNext("not a cron expression", 1, ""); err == nil {
		t.Fatal("CronNext accepted an expression it cannot read")
	}
	if _, err := CronNext("@daily", 0, ""); err == nil {
		t.Fatal("CronNext accepted a count of 0")
	}
}
