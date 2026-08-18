package relations

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// A paper log is never erased. A wrong line is struck through, initialled,
// and re-entered below -- and the strike-through is part of the record,
// because what was written and then corrected is often the interesting
// part. This file is that discipline: nothing here deletes or rewrites a
// line, ever.
//
// A line's status is one relation from the line to a fixed marker entity
// (see Journal.StatusMarker), which gives three things at once:
//
//   - It is a single key, so claiming it with an OpCompareAbsent
//     precondition makes striking a line atomic and once-only. Two
//     writers correcting the same line cannot fork it into two competing
//     replacements, and a struck line can never be struck again.
//   - Its mirror in the index namespace makes "every correction in this
//     log" one prefix scan of the marker, however many pages the book
//     has (Journal.Corrections).
//   - It carries the usual author, timestamp and signature, so a
//     strike-through is attributable exactly like the line it strikes.

// The two ways a line stops being current. Both are relations from the
// line to the status marker; the kind says which, and the payload says
// what replaced it or why it went.
const (
	// KindSuperseded marks a line replaced by a later one. Its Data is
	// the 4-byte entity of the replacement.
	KindSuperseded byte = 0x06
	// KindVoided marks a line struck through with nothing put in its
	// place -- entered by mistake, a duplicate, a wrong page. Its Data
	// is the 4-byte entity of a dictionary term giving the reason, or
	// the zero entity if none was given.
	KindVoided byte = 0x07
	// KindSupersedes runs the other way, from the replacement to the
	// line it replaced, so a corrected line can be read back from the
	// correction without scanning for it.
	KindSupersedes byte = 0x08
)

// ErrAlreadyStruck is returned when a line has already been superseded
// or voided. A strike-through is permanent and singular: the fix for a
// bad correction is to correct the correction, which leaves both in the
// record, not to undo it.
var ErrAlreadyStruck = errors.New("relations: this line has already been superseded or voided")

// maxCorrectionChain bounds History's walk. A line corrected a thousand
// times is a data problem, not a workflow, and an unbounded walk over a
// cycle that should not exist is worse than an error.
const maxCorrectionChain = 1000

// EntryState is what History/Status report about one line.
type EntryState uint8

const (
	// StateLive means the line still stands as written.
	StateLive EntryState = iota
	// StateSuperseded means a later line replaced it.
	StateSuperseded
	// StateVoided means it was struck through with no replacement.
	StateVoided
)

func (s EntryState) String() string {
	switch s {
	case StateLive:
		return "live"
	case StateSuperseded:
		return "superseded"
	case StateVoided:
		return "voided"
	default:
		return fmt.Sprintf("state(%d)", uint8(s))
	}
}

// EntryStatus is one line's standing, as Status and Corrections report
// it.
type EntryStatus struct {
	// Entry is the line this is about.
	Entry Entity
	// State is whether it still stands.
	State EntryState
	// Replacement is the line that replaced it, when State is
	// StateSuperseded.
	Replacement Entity
	// Reason is the dictionary term explaining a StateVoided strike, or
	// Zero if none was given.
	Reason Entity
	// Author and At are who struck the line and when -- zero values for
	// a live line, which nobody struck.
	Author Entity
	At     time.Time
}

// Live reports whether the line still stands.
func (s EntryStatus) Live() bool { return s.State == StateLive }

// StatusMarker is the entity every struck line points at: id 0 of
// TypeEntry on the schema page, which Allocate never hands out (ids
// start at 1). It is a pure link anchor -- nothing declares it, and it
// holds no record of its own -- so it costs nothing and cannot collide
// with a real line.
//
// Scanning its backlinks is the audit query a paper book cannot answer
// without being read cover to cover: every correction ever made, in one
// pass. See Corrections.
func (j *Journal) StatusMarker() Entity {
	return Entity{Log: j.st.Log, Page: SchemaPage, Type: TypeEntry, ID: 0}
}

// Correct writes a replacement for an existing line: the new line is
// appended at the end of the book exactly like any other, and in the
// same atomic transaction it is linked to the line it replaces and that
// line is marked superseded.
//
// The old line is left exactly as it was written, still readable by Row
// and still on its own page -- that is the whole point. What changes is
// its standing (Status), and that later readers following the chain
// (History, Latest) arrive at the new line instead.
//
// Returns ErrAlreadyStruck if the line has already been corrected or
// voided; the guard is a compare precondition on the marker, so two
// writers racing to correct one line cannot both succeed.
func (j *Journal) Correct(ctx context.Context, superseded Entity, cells ...Cell) (Entity, error) {
	if err := j.checkStrikeable(ctx, superseded); err != nil {
		return Zero, err
	}

	replacement, err := j.appendLine(ctx, cells, func(entry Entity) ([]Op, error) {
		if err := j.checkStrikeable(ctx, superseded); err != nil {
			return nil, err
		}
		ref := entry.Bytes()
		ops, err := j.strikeOps(superseded, KindSuperseded, ref[:])
		if err != nil {
			return nil, err
		}
		back, err := j.st.LinkOps(entry, superseded, KindSupersedes, nil)
		if err != nil {
			return nil, err
		}
		return append(ops, back...), nil
	})
	if err != nil {
		return Zero, j.explainStrikeConflict(ctx, superseded, err)
	}
	return replacement, nil
}

// Void strikes a line through with nothing in its place -- a duplicate,
// a line on the wrong page, a job that never ran. reason is a dictionary
// term saying why, or Zero for none: a reason is a value like any other,
// so it is interned rather than written out (see Journal.Term).
//
// Like Correct, this leaves the line itself untouched and readable.
func (j *Journal) Void(ctx context.Context, entry Entity, reason Entity) error {
	if err := j.checkStrikeable(ctx, entry); err != nil {
		return err
	}
	if !reason.IsZero() {
		if reason.Type != TypeTerm {
			return fmt.Errorf("relations: void: reason %s is not a dictionary term", reason)
		}
		if _, found, err := j.st.Declaration(ctx, reason); err != nil {
			return err
		} else if !found {
			return fmt.Errorf("relations: void: reason %s is not declared", reason)
		}
	}

	// The strike is a chained event of its own (see chain.go), so it has
	// to read the running head and write under a compare on it -- which
	// means it can lose a race with any other write in the log, not just
	// with another strike on this line. Losing is cheap and the retry is
	// the whole handling: re-read the head, rebuild, apply.
	ref := reason.Bytes()
	for attempt := 0; attempt < allocRetries; attempt++ {
		ops, err := j.strikeOps(entry, KindVoided, ref[:])
		if err != nil {
			return err
		}
		chain, err := j.chainEventOps(ctx, Zero, ops)
		if err != nil {
			return err
		}
		err = j.st.Backend().Apply(ctx, append(ops, chain...))
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
		if err := j.checkStrikeable(ctx, entry); err != nil {
			return err
		}
	}
	return fmt.Errorf("relations: void: %s lost %d races in a row", entry, allocRetries)
}

// Status reads one line's standing: a point read of its marker relation,
// whether or not it was ever struck.
func (j *Journal) Status(ctx context.Context, entry Entity) (EntryStatus, error) {
	if entry.Type != TypeEntry {
		return EntryStatus{}, fmt.Errorf("relations: status: %s is not a line", entry)
	}
	rel, found, err := j.st.Lookup(ctx, entry, j.StatusMarker())
	if err != nil {
		return EntryStatus{}, err
	}
	if !found {
		return EntryStatus{Entry: entry, State: StateLive}, nil
	}
	return decodeStrike(rel)
}

// Corrections returns every struck line in this log -- superseded and
// voided alike -- from a single scan of the status marker's backlinks,
// in the order the lines were written. This is the correction log an
// auditor asks for, and it costs one scan no matter how large the book
// is.
func (j *Journal) Corrections(ctx context.Context) ([]EntryStatus, error) {
	rels, err := j.st.Backlinks(ctx, j.StatusMarker())
	if err != nil {
		return nil, err
	}
	out := make([]EntryStatus, 0, len(rels))
	for _, rel := range rels {
		switch rel.Record.Kind {
		case KindSuperseded, KindVoided:
			status, err := decodeStrike(rel)
			if err != nil {
				return nil, err
			}
			out = append(out, status)
		}
	}
	return out, nil
}

// LivePage is Page with the struck lines left out -- the book as it
// currently stands, rather than as it was written. It costs the page
// scan plus one scan of the marker, not a read per line.
func (j *Journal) LivePage(ctx context.Context, page uint8) ([]Entity, error) {
	lines, err := j.Page(ctx, page)
	if err != nil {
		return nil, err
	}
	struck, err := j.Corrections(ctx)
	if err != nil {
		return nil, err
	}
	gone := make(map[Entity]bool, len(struck))
	for _, s := range struck {
		gone[s.Entry] = true
	}
	live := make([]Entity, 0, len(lines))
	for _, line := range lines {
		if !gone[line] {
			live = append(live, line)
		}
	}
	return live, nil
}

// History returns the whole chain a line belongs to, oldest first: the
// lines it replaced, itself, and the lines that replaced it. Passing any
// version returns the same chain, so a caller holding a stale reference
// needs no special case.
//
// A voided line ends its chain -- nothing replaced it.
func (j *Journal) History(ctx context.Context, entry Entity) ([]Entity, error) {
	if entry.Type != TypeEntry {
		return nil, fmt.Errorf("relations: history: %s is not a line", entry)
	}

	// Walk back to the original through the KindSupersedes links, then
	// forward from it through the markers. Both walks are bounded and
	// remember where they have been: the data should be a chain, but
	// nothing in the store enforces that it is.
	seen := map[Entity]bool{entry: true}
	var older []Entity
	for cursor := entry; ; {
		rels, err := j.st.Relations(ctx, cursor)
		if err != nil {
			return nil, err
		}
		previous := OfKind(rels, KindSupersedes)
		if len(previous) == 0 {
			break
		}
		cursor = previous[0].B
		if seen[cursor] {
			return nil, fmt.Errorf("relations: history: %s supersedes a line already in its own chain", cursor)
		}
		if len(older) >= maxCorrectionChain {
			return nil, fmt.Errorf("relations: history: chain longer than %d lines", maxCorrectionChain)
		}
		seen[cursor] = true
		older = append(older, cursor)
	}

	chain := make([]Entity, 0, len(older)+1)
	for i := len(older) - 1; i >= 0; i-- {
		chain = append(chain, older[i])
	}
	chain = append(chain, entry)

	for cursor := entry; ; {
		status, err := j.Status(ctx, cursor)
		if err != nil {
			return nil, err
		}
		if status.State != StateSuperseded {
			break
		}
		cursor = status.Replacement
		if seen[cursor] {
			return nil, fmt.Errorf("relations: history: %s is replaced by a line already in its own chain", cursor)
		}
		if len(chain) >= maxCorrectionChain {
			return nil, fmt.Errorf("relations: history: chain longer than %d lines", maxCorrectionChain)
		}
		seen[cursor] = true
		chain = append(chain, cursor)
	}
	return chain, nil
}

// Latest returns the line that currently stands in entry's place --
// entry itself if it was never corrected, the end of its chain if it
// was. A voided line is its own latest: nothing replaced it, and the
// caller learns it no longer stands from Status.
func (j *Journal) Latest(ctx context.Context, entry Entity) (Entity, error) {
	chain, err := j.History(ctx, entry)
	if err != nil {
		return Zero, err
	}
	return chain[len(chain)-1], nil
}

// strikeOps builds the marker relation that strikes a line, guarded by
// the precondition that makes it once-only.
func (j *Journal) strikeOps(entry Entity, kind byte, payload []byte) ([]Op, error) {
	marker := j.StatusMarker()
	ops, err := j.st.LinkOps(entry, marker, kind, payload)
	if err != nil {
		return nil, err
	}
	return append([]Op{{Kind: OpCompareAbsent, Key: RelationKey(entry, marker)}}, ops...), nil
}

// checkStrikeable rejects a line that cannot be struck: one that is not
// a line at all, was never written, or has already been struck. The
// atomic guarantee comes from strikeOps' precondition; this is what
// turns the common case into a clear error instead of a lost race.
func (j *Journal) checkStrikeable(ctx context.Context, entry Entity) error {
	if entry.Type != TypeEntry || entry.ID == 0 {
		return fmt.Errorf("relations: %s is not a line", entry)
	}
	if _, found, err := j.st.Declaration(ctx, entry); err != nil {
		return err
	} else if !found {
		return fmt.Errorf("relations: %s is not a line this log has written", entry)
	}
	status, err := j.Status(ctx, entry)
	if err != nil {
		return err
	}
	if !status.Live() {
		return fmt.Errorf("%w: %s is %s", ErrAlreadyStruck, entry, status.State)
	}
	return nil
}

// explainStrikeConflict turns the precondition failure a lost race
// produces into ErrAlreadyStruck when that is what actually happened,
// and leaves any other error alone.
func (j *Journal) explainStrikeConflict(ctx context.Context, entry Entity, err error) error {
	if !errors.Is(err, ErrConflict) {
		return err
	}
	status, statusErr := j.Status(ctx, entry)
	if statusErr != nil {
		return err
	}
	if !status.Live() {
		return fmt.Errorf("%w: %s is %s", ErrAlreadyStruck, entry, status.State)
	}
	return err
}

// decodeStrike reads a marker relation into an EntryStatus.
func decodeStrike(rel Relation) (EntryStatus, error) {
	status := EntryStatus{
		Entry:  rel.A,
		Author: rel.Record.Author,
		At:     rel.Record.Created,
	}
	if len(rel.Record.Data) != EntityLen {
		return EntryStatus{}, fmt.Errorf("relations: status of %s carries a %d-byte payload, want %d",
			rel.A, len(rel.Record.Data), EntityLen)
	}
	payload, err := DecodeEntity(rel.Record.Data)
	if err != nil {
		return EntryStatus{}, err
	}
	switch rel.Record.Kind {
	case KindSuperseded:
		if payload.IsZero() {
			return EntryStatus{}, fmt.Errorf("relations: %s is superseded by no line", rel.A)
		}
		status.State = StateSuperseded
		status.Replacement = payload
	case KindVoided:
		status.State = StateVoided
		status.Reason = payload
	default:
		return EntryStatus{}, fmt.Errorf("relations: %s has an unknown status kind %#x", rel.A, rel.Record.Kind)
	}
	return status, nil
}
