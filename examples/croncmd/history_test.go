package croncmd

import (
	"context"
	"testing"
	"time"
)

// TestFiresReadsBackWhatTheClaimsRecorded is the audit trail working as
// one: the scheduler writes no separate log, and this is what "did it run,
// and as which dispatch" is answered from.
func TestFiresReadsBackWhatTheClaimsRecorded(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.put(t, Schedule{ID: "hourly", Spec: "@hourly", CommandID: "report"})
	h.put(t, Schedule{ID: "other", Spec: "@hourly", CommandID: "sweep"})
	h.tick(t)

	h.advance(3 * time.Hour)
	h.scheduler.CatchUp = 4 * time.Hour
	h.tick(t)

	all, err := h.catalog.Fires(ctx, "", 0)
	if err != nil {
		t.Fatalf("Fires: %v", err)
	}
	if len(all) != 6 {
		t.Fatalf("Fires returned %d, want 6 (three hours for each of two schedules)", len(all))
	}

	mine, err := h.catalog.Fires(ctx, "hourly", 0)
	if err != nil {
		t.Fatalf("Fires(hourly): %v", err)
	}
	if len(mine) != 3 {
		t.Fatalf("Fires(hourly) returned %d, want 3", len(mine))
	}
	for _, fire := range mine {
		if fire.ScheduleID != "hourly" || fire.CommandID != "report" || fire.InstanceID == "" {
			t.Fatalf("fire = %+v, want it to name the hourly schedule and its dispatch", fire)
		}
	}
	// Most recent first, which is the order a report wants.
	for i := 1; i < len(mine); i++ {
		if !mine[i].At.Before(mine[i-1].At) {
			t.Fatalf("fires came back %s then %s, want most recent first", mine[i-1].At, mine[i].At)
		}
	}

	limited, err := h.catalog.Fires(ctx, "hourly", 2)
	if err != nil {
		t.Fatalf("Fires(hourly, 2): %v", err)
	}
	if len(limited) != 2 || !limited[0].At.Equal(mine[0].At) {
		t.Fatalf("limit gave %d fires starting at %v, want the 2 most recent", len(limited), limited)
	}
}

// TestFiresIgnoresAKeyItDidNotWrite: the claim prefix is ordinary user
// keyspace, so anything may be sitting under it.
func TestFiresIgnoresAKeyItDidNotWrite(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	for _, value := range []string{"not json", `{"schedule_id":"x"}`} {
		if err := h.store.Put(ctx, DefaultKeyPrefix+claimPart+"stray/thing", value); err != nil {
			t.Fatalf("Put: %v", err)
		}
		fires, err := h.catalog.Fires(ctx, "", 0)
		if err != nil {
			t.Fatalf("Fires: %v", err)
		}
		if len(fires) != 0 {
			t.Fatalf("Fires returned %d rows for a key it did not write: %v", len(fires), fires)
		}
	}
}
