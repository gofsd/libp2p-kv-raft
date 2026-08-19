package croncmd

import (
	"testing"
	"time"
)

func TestParseSpecAcceptsTheStandardForms(t *testing.T) {
	for _, expr := range []string{
		"0 0 * * *",
		"*/15 * * * *",
		"5 4 * * sun",
		"0 0 1 1 *",
		"@daily",
		"@hourly",
		"@yearly",
		"0 0 * jan mon",
		"0 9-17 * * 1-5",
		"0 0-23/2 * * *",
		"0 5/10 * * *",
		"0 0 * * 7",
		"0,15,30,45 * * * *",
	} {
		if _, err := ParseSpec(expr); err != nil {
			t.Errorf("ParseSpec(%q): %v", expr, err)
		}
	}
}

func TestParseSpecRefusesWhatItCannotHonour(t *testing.T) {
	for _, expr := range []string{
		"",
		"* * * *",       // four fields
		"* * * * * *",   // six: seconds are not a field here
		"60 * * * *",    // minute out of range
		"* 24 * * *",    // hour out of range
		"* * 0 * *",     // day-of-month starts at 1
		"* * * 13 *",    // month out of range
		"* * * * 8",     // day-of-week out of range
		"5-1 * * * *",   // backwards range
		"*/0 * * * *",   // zero step
		"*/-1 * * * *",  // negative step
		"nope * * * *",  // not a number or a name
		"0 0 * bogus *", // not a month name
		"@bogus",        // not a shortcut
	} {
		if _, err := ParseSpec(expr); err == nil {
			t.Errorf("ParseSpec(%q) = nil error, want a refusal", expr)
		}
	}
}

func TestShortcutsExpandToTheirExpression(t *testing.T) {
	for _, tc := range []struct{ shortcut, equivalent string }{
		{"@daily", "0 0 * * *"},
		{"@midnight", "0 0 * * *"},
		{"@hourly", "0 * * * *"},
		{"@weekly", "0 0 * * 0"},
		{"@monthly", "0 0 1 * *"},
		{"@yearly", "0 0 1 1 *"},
		{"@annually", "0 0 1 1 *"},
	} {
		short := MustParseSpec(tc.shortcut)
		long := MustParseSpec(tc.equivalent)

		// Walk a year of candidate minutes at an hour granularity and
		// require the two to agree everywhere -- comparing masks would
		// only restate how they are stored.
		at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 24*365; i++ {
			if short.Match(at) != long.Match(at) {
				t.Fatalf("%s and %s disagree at %s", tc.shortcut, tc.equivalent, at)
			}
			at = at.Add(time.Hour)
		}
	}
}

func TestMatchHonoursEveryField(t *testing.T) {
	spec := MustParseSpec("*/15 9-17 * * 1-5")

	for _, tc := range []struct {
		at   time.Time
		want bool
	}{
		// Monday, in hours, on a quarter.
		{time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC), true},
		{time.Date(2026, 1, 5, 17, 45, 0, 0, time.UTC), true},
		// Monday, in hours, off a quarter.
		{time.Date(2026, 1, 5, 9, 7, 0, 0, time.UTC), false},
		// Monday, out of hours.
		{time.Date(2026, 1, 5, 8, 0, 0, 0, time.UTC), false},
		{time.Date(2026, 1, 5, 18, 0, 0, 0, time.UTC), false},
		// Saturday and Sunday.
		{time.Date(2026, 1, 3, 9, 0, 0, 0, time.UTC), false},
		{time.Date(2026, 1, 4, 9, 0, 0, 0, time.UTC), false},
	} {
		if got := spec.Match(tc.at); got != tc.want {
			t.Errorf("Match(%s) = %v, want %v", tc.at.Format(time.RFC3339), got, tc.want)
		}
	}
}

// TestDayOfMonthAndWeekAreAUnion pins cron's one genuinely surprising
// rule, which is the whole reason this package parses expressions itself
// rather than matching them field by field ad hoc: when both day fields
// are restricted, either one matching is enough.
func TestDayOfMonthAndWeekAreAUnion(t *testing.T) {
	spec := MustParseSpec("0 0 1 * mon")

	for _, tc := range []struct {
		at     time.Time
		want   bool
		reason string
	}{
		{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), true, "the 1st, a Thursday: day-of-month matches"},
		{time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC), true, "a Monday, not the 1st: day-of-week matches"},
		{time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC), false, "a Tuesday, not the 1st: neither matches"},
	} {
		if got := spec.Match(tc.at); got != tc.want {
			t.Errorf("Match(%s) = %v, want %v (%s)", tc.at.Format(time.RFC3339), got, tc.want, tc.reason)
		}
	}

	// With only one of the two restricted, that one decides alone.
	domOnly := MustParseSpec("0 0 1 * *")
	if domOnly.Match(time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)) {
		t.Error("day-of-month alone should not match a Monday that is not the 1st")
	}
	dowOnly := MustParseSpec("0 0 * * mon")
	if dowOnly.Match(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("day-of-week alone should not match the 1st when it is a Thursday")
	}
}

func TestNextIsStrictlyAfterAndInOrder(t *testing.T) {
	spec := MustParseSpec("0 0 * * *")
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	next, ok := spec.Next(at)
	if !ok {
		t.Fatal("Next found nothing for a daily schedule")
	}
	want := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("Next(%s) = %s, want %s", at, next, want)
	}

	// Called on a matching minute, Next moves on rather than returning it
	// again -- which is what stops a caller's enumeration loop.
	after, ok := spec.Next(next)
	if !ok {
		t.Fatal("Next found nothing on the second call")
	}
	if !after.After(next) {
		t.Fatalf("Next(%s) = %s, want something strictly later", next, after)
	}
}

func TestNextWalksEveryFireInOrder(t *testing.T) {
	spec := MustParseSpec("*/20 * * * *")
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	want := []string{"00:20", "00:40", "01:00", "01:20"}
	for _, expected := range want {
		next, ok := spec.Next(at)
		if !ok {
			t.Fatalf("Next found nothing after %s", at)
		}
		if got := next.Format("15:04"); got != expected {
			t.Fatalf("Next(%s) = %s, want %s", at.Format("15:04"), got, expected)
		}
		at = next
	}
}

// TestNextGivesUpOnAnImpossibleDate is the bound doing its job: the 30th of
// February matches no instant that will ever exist, and the search has to
// end rather than spin.
func TestNextGivesUpOnAnImpossibleDate(t *testing.T) {
	spec := MustParseSpec("0 0 30 2 *")
	if next, ok := spec.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); ok {
		t.Fatalf("Next returned %s for the 30th of February", next)
	}
}

func TestNextStaysInItsLocation(t *testing.T) {
	// A fixed zone rather than a named one: this must hold on a machine
	// with no tzdata installed.
	plusFive := time.FixedZone("plusfive", 5*60*60)
	spec := MustParseSpec("30 2 * * *")

	next, ok := spec.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, plusFive))
	if !ok {
		t.Fatal("Next found nothing")
	}
	if next.Location() != plusFive {
		t.Fatalf("Next returned a time in %s, want %s", next.Location(), plusFive)
	}
	if got := next.Format("2006-01-02 15:04"); got != "2026-01-01 02:30" {
		t.Fatalf("Next = %s, want 2026-01-01 02:30 in its own zone", got)
	}
}

func TestSpecKeepsTheExpressionAsWritten(t *testing.T) {
	spec := MustParseSpec("  */15 * * * *  ")
	if spec.String() != "*/15 * * * *" {
		t.Fatalf("String() = %q, want the trimmed expression as written", spec.String())
	}
}
