package croncmd

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// This file is the calendar half of the example, and it is deliberately
// dependency-free: five bitmasks and a stepping search, no imports beyond
// the standard library. A scheduler that reaches for a cron package
// inherits that package's own opinions about the two things below that
// actually decide when a command runs -- the day-of-month/day-of-week
// disjunction, and what "the next fire" means across a DST jump -- and an
// example whose whole point is *when a command is submitted* should state
// those itself rather than delegate them.

// The inclusive bounds of each cron field. Day-of-month and month start at
// 1; the rest start at 0. Day-of-week runs 0-6 from Sunday, with 7 also
// accepted for Sunday on the way in (see parseField) because both
// conventions are written in the wild and neither is worth refusing.
const (
	minuteMin, minuteMax = 0, 59
	hourMin, hourMax     = 0, 23
	domMin, domMax       = 1, 31
	monthMin, monthMax   = 1, 12
	dowMin, dowMax       = 0, 6
)

// monthNames and dowNames are the three-letter forms a field may use
// instead of a number. Matched case-insensitively.
var (
	monthNames = map[string]int{
		"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
		"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
	}
	dowNames = map[string]int{
		"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
	}
)

// shortcuts are the @-forms, expanded to the five-field expression they
// stand for before anything else parses them.
var shortcuts = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// Spec is a parsed cron expression: which minutes, hours, days, months and
// weekdays it admits, each as a bitmask over that field's own range.
//
// domRestricted/dowRestricted record whether the day-of-month and
// day-of-week fields were written as anything other than "*", because
// that -- not the masks themselves -- is what decides how the two combine.
// See dayMatches.
type Spec struct {
	// Text is the expression as it was written, kept so a Spec can be
	// round-tripped back into a schedule record and shown to an operator
	// as they typed it rather than as this package would render it.
	Text string

	minute uint64
	hour   uint64
	dom    uint64
	month  uint64
	dow    uint64

	domRestricted bool
	dowRestricted bool
}

// ParseSpec parses a standard five-field cron expression -- minute, hour,
// day-of-month, month, day-of-week -- or one of the @-shortcuts above.
//
// Each field is a comma-separated list of terms, where a term is "*", a
// single value, or an inclusive "lo-hi" range, any of which may carry a
// "/step". A bare "value/step" is read as "from value to that field's
// maximum, every step", which is the common Vixie-cron extension.
//
// Seconds are not a field. This scheduler's own resolution is one minute
// (see Scheduler.Interval), so admitting a seconds field would let an
// operator write a schedule that silently could not be honoured.
func ParseSpec(expr string) (Spec, error) {
	text := strings.TrimSpace(expr)
	if text == "" {
		return Spec{}, fmt.Errorf("croncmd: empty cron expression")
	}

	expanded := text
	if strings.HasPrefix(text, "@") {
		replacement, ok := shortcuts[strings.ToLower(text)]
		if !ok {
			return Spec{}, fmt.Errorf("croncmd: %q is not a known shortcut", text)
		}
		expanded = replacement
	}

	fields := strings.Fields(expanded)
	if len(fields) != 5 {
		return Spec{}, fmt.Errorf("croncmd: %q has %d fields, want 5 (minute hour day-of-month month day-of-week)", text, len(fields))
	}

	spec := Spec{Text: text}
	var err error
	if spec.minute, err = parseField(fields[0], minuteMin, minuteMax, nil); err != nil {
		return Spec{}, fmt.Errorf("croncmd: %q: minute: %w", text, err)
	}
	if spec.hour, err = parseField(fields[1], hourMin, hourMax, nil); err != nil {
		return Spec{}, fmt.Errorf("croncmd: %q: hour: %w", text, err)
	}
	if spec.dom, err = parseField(fields[2], domMin, domMax, nil); err != nil {
		return Spec{}, fmt.Errorf("croncmd: %q: day of month: %w", text, err)
	}
	if spec.month, err = parseField(fields[3], monthMin, monthMax, monthNames); err != nil {
		return Spec{}, fmt.Errorf("croncmd: %q: month: %w", text, err)
	}
	if spec.dow, err = parseField(fields[4], dowMin, dowMax, dowNames); err != nil {
		return Spec{}, fmt.Errorf("croncmd: %q: day of week: %w", text, err)
	}

	spec.domRestricted = strings.TrimSpace(fields[2]) != "*"
	spec.dowRestricted = strings.TrimSpace(fields[4]) != "*"
	return spec, nil
}

// MustParseSpec is ParseSpec for an expression fixed in source, where a
// failure is a programming error rather than bad input.
func MustParseSpec(expr string) Spec {
	spec, err := ParseSpec(expr)
	if err != nil {
		panic(err)
	}
	return spec
}

// String returns the expression as it was written.
func (s Spec) String() string { return s.Text }

// parseField turns one comma-separated cron field into a bitmask over
// [min, max]. names, if non-nil, is the field's own set of accepted
// three-letter aliases.
func parseField(field string, min, max int, names map[string]int) (uint64, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return 0, fmt.Errorf("empty field")
	}

	var mask uint64
	for _, term := range strings.Split(field, ",") {
		termMask, err := parseTerm(strings.TrimSpace(term), min, max, names)
		if err != nil {
			return 0, err
		}
		mask |= termMask
	}
	if mask == 0 {
		return 0, fmt.Errorf("%q matches nothing", field)
	}
	return mask, nil
}

// parseTerm handles one comma-free term: the "*", "lo-hi", "value" and
// "/step" forms described on ParseSpec.
func parseTerm(term string, min, max int, names map[string]int) (uint64, error) {
	if term == "" {
		return 0, fmt.Errorf("empty term")
	}

	rangePart, stepPart, hasStep := strings.Cut(term, "/")
	step := 1
	if hasStep {
		parsed, err := strconv.Atoi(strings.TrimSpace(stepPart))
		if err != nil {
			return 0, fmt.Errorf("step %q is not a number", stepPart)
		}
		if parsed < 1 {
			return 0, fmt.Errorf("step %d must be at least 1", parsed)
		}
		step = parsed
	}

	rangePart = strings.TrimSpace(rangePart)
	lo, hi := min, max
	switch {
	case rangePart == "*":
		// Whole range; lo/hi already are it.
	case strings.Contains(rangePart, "-"):
		loText, hiText, _ := strings.Cut(rangePart, "-")
		var err error
		if lo, err = parseValue(loText, min, max, names); err != nil {
			return 0, err
		}
		if hi, err = parseValue(hiText, min, max, names); err != nil {
			return 0, err
		}
		if lo > hi {
			return 0, fmt.Errorf("range %q runs backwards", rangePart)
		}
	default:
		value, err := parseValue(rangePart, min, max, names)
		if err != nil {
			return 0, err
		}
		lo = value
		// "value" alone is that value; "value/step" is every step-th one
		// from there to the end of the field.
		if hasStep {
			hi = max
		} else {
			hi = value
		}
	}

	var mask uint64
	for v := lo; v <= hi; v += step {
		mask |= 1 << uint(v)
	}
	return mask, nil
}

// parseValue reads one number or three-letter name, and bounds-checks it.
func parseValue(text string, min, max int, names map[string]int) (int, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, fmt.Errorf("empty value")
	}

	if names != nil {
		if value, ok := names[strings.ToLower(text)]; ok {
			return value, nil
		}
	}

	value, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number or a known name", text)
	}
	// Sunday is 0, but 7 is written for it often enough that refusing it
	// would be pedantry rather than safety.
	if names != nil && max == dowMax && value == 7 {
		value = 0
	}
	if value < min || value > max {
		return 0, fmt.Errorf("%d is out of range %d-%d", value, min, max)
	}
	return value, nil
}

// Match reports whether t falls on a minute this spec admits. Seconds and
// anything finer are ignored.
func (s Spec) Match(t time.Time) bool {
	return s.month&(1<<uint(int(t.Month()))) != 0 &&
		s.dayMatches(t) &&
		s.hour&(1<<uint(t.Hour())) != 0 &&
		s.minute&(1<<uint(t.Minute())) != 0
}

// dayMatches applies cron's one genuinely surprising rule: when *both* the
// day-of-month and day-of-week fields are restricted, a day matches if
// *either* does -- they are a union, not an intersection. So
// "0 0 1 * MON" is "the first of the month, and also every Monday", not
// "Mondays that fall on the first".
//
// When only one of the two is restricted, that one decides alone; when
// neither is, every day matches. This is Vixie cron's behaviour and the
// one most schedules are written against, but it is worth knowing before
// writing a schedule that names both fields.
func (s Spec) dayMatches(t time.Time) bool {
	domHit := s.dom&(1<<uint(t.Day())) != 0
	dowHit := s.dow&(1<<uint(int(t.Weekday()))) != 0

	switch {
	case s.domRestricted && s.dowRestricted:
		return domHit || dowHit
	case s.domRestricted:
		return domHit
	case s.dowRestricted:
		return dowHit
	default:
		return true
	}
}

// maxNextSearchYears bounds how far Next will look before giving up. A
// spec like "0 0 30 2 *" (the 30th of February) matches no instant that
// will ever exist, and the search has to end somewhere rather than spin.
const maxNextSearchYears = 5

// Next returns the first minute strictly after t that this spec admits,
// in t's own location, and whether one was found within
// maxNextSearchYears.
//
// The search steps by the largest unit that can be ruled out -- a month
// that does not match is skipped whole, then a day, then an hour -- so a
// yearly schedule costs a few dozen iterations rather than the half a
// million minutes between fires.
//
// Daylight saving is handled by construction rather than by a special
// case: every candidate is built with time.Date in t's location, which
// resolves a wall-clock time that a DST jump skipped or repeated to a real
// instant. The consequence worth knowing is that a schedule fixed to a
// wall-clock hour inside the skipped window does not fire on that day in
// that zone, which is why Scheduler treats UTC as the default (see
// Schedule.Location).
func (s Spec) Next(t time.Time) (time.Time, bool) {
	loc := t.Location()
	// Start at the top of the next minute: "strictly after" is what makes
	// a caller's loop terminate.
	next := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, loc).Add(time.Minute)
	deadline := next.AddDate(maxNextSearchYears, 0, 0)

	for next.Before(deadline) {
		if s.month&(1<<uint(int(next.Month()))) == 0 {
			// Roll to midnight on the first of the next month.
			next = time.Date(next.Year(), next.Month(), 1, 0, 0, 0, 0, loc).AddDate(0, 1, 0)
			continue
		}
		if !s.dayMatches(next) {
			next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1)
			continue
		}
		if s.hour&(1<<uint(next.Hour())) == 0 {
			next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), 0, 0, 0, loc).Add(time.Hour)
			continue
		}
		if s.minute&(1<<uint(next.Minute())) == 0 {
			next = next.Add(time.Minute)
			continue
		}
		return next, true
	}
	return time.Time{}, false
}
