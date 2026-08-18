package relations

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// KindFieldState relates a column to the status marker and says whether
// its vocabulary is closed: field -> status marker, with one byte of
// payload. Unlike a strike it is written more than once -- closing and
// reopening are both ordinary acts -- which is why it is chained as a
// mutable event (see mutableEvent).
const KindFieldState byte = 0x0F

// The two states a column's vocabulary can be in.
const (
	vocabularyOpen   byte = 0
	vocabularyClosed byte = 1
)

// ErrFieldClosed is returned when a value is asked for that a closed
// column does not already have. A closed vocabulary is the point at
// which "one of a known set" stops being a hope and becomes a rule.
var ErrFieldClosed = errors.New("relations: this column's vocabulary is closed")

// CloseField closes a column's vocabulary: its existing terms go on
// working, and no new one may be interned into it.
//
// Until a column is closed, "controlled vocabulary" means only that
// values are stored once and referenced -- anybody can still add a word,
// which for a column like a machine list or a result code is exactly
// what a log book's fixed set of admissible entries is meant to prevent.
// Closing it is what makes the set actual.
//
// The refusal is enforced inside the transaction that would create the
// term, not just by the check here, so a column closed at that same
// moment cannot have one slipped past it.
func (j *Journal) CloseField(ctx context.Context, field Entity) error {
	return j.setVocabulary(ctx, field, vocabularyClosed)
}

// ReopenField reopens a closed vocabulary -- because a shop really does
// buy a new machine, and the alternative to reopening is a second column
// meaning the same thing.
//
// Reopening is recorded exactly as closing is: an event of its own,
// attributable and in sequence, so a vocabulary that was closed while
// something was written and reopened afterwards reads that way forever.
// A closure nobody could see reversed would not be worth recording.
func (j *Journal) ReopenField(ctx context.Context, field Entity) error {
	return j.setVocabulary(ctx, field, vocabularyOpen)
}

// FieldIsClosed reports whether field's vocabulary is closed.
func (j *Journal) FieldIsClosed(ctx context.Context, field Entity) (bool, error) {
	state, _, found, err := j.vocabularyState(ctx, field)
	if err != nil || !found {
		return false, err
	}
	return state == vocabularyClosed, nil
}

// VocabularyState is a column's vocabulary as ClosedFields reports it.
type VocabularyState struct {
	Field  Entity
	Name   string
	Closed bool
	// By and At are who last changed the state and when.
	By Entity
	At time.Time
}

// ClosedFields returns every column whose vocabulary is currently
// closed -- one scan of the status marker, the same one corrections and
// page sign-offs come off.
func (j *Journal) ClosedFields(ctx context.Context) ([]VocabularyState, error) {
	rels, err := j.st.Backlinks(ctx, j.StatusMarker())
	if err != nil {
		return nil, err
	}
	var out []VocabularyState
	for _, rel := range OfKind(rels, KindFieldState) {
		if len(rel.Record.Data) != 1 || rel.Record.Data[0] != vocabularyClosed {
			continue
		}
		name, err := j.fieldName(ctx, rel.A)
		if err != nil {
			return nil, err
		}
		out = append(out, VocabularyState{
			Field:  rel.A,
			Name:   name,
			Closed: true,
			By:     rel.Record.Author,
			At:     rel.Record.Created,
		})
	}
	return out, nil
}

// setVocabulary writes a column's new vocabulary state and chains it.
func (j *Journal) setVocabulary(ctx context.Context, field Entity, want byte) error {
	if field.Type != TypeField {
		return fmt.Errorf("relations: vocabulary: %s is not a column", field)
	}
	if _, found, err := j.st.Declaration(ctx, field); err != nil {
		return err
	} else if !found {
		return fmt.Errorf("relations: vocabulary: %s is not a column this log has declared", field)
	}

	for attempt := 0; attempt < allocRetries; attempt++ {
		state, raw, found, err := j.vocabularyState(ctx, field)
		if err != nil {
			return err
		}
		if found && state == want {
			return fmt.Errorf("relations: vocabulary of %s is already %s", field, vocabularyName(want))
		}
		if !found && want == vocabularyOpen {
			return fmt.Errorf("relations: vocabulary of %s was never closed", field)
		}

		key := RelationKey(field, j.StatusMarker())
		pre := Op{Kind: OpCompareAbsent, Key: key}
		if found {
			pre = Op{Kind: OpCompare, Key: key, Value: raw}
		}
		ops, err := j.st.LinkOps(field, j.StatusMarker(), KindFieldState, []byte{want})
		if err != nil {
			return err
		}
		ops = append([]Op{pre}, ops...)

		chain, err := j.chainEventOps(ctx, Zero, ops, mutableSpec{
			tag:     eventFieldState,
			subject: field,
			party:   j.StatusMarker(),
			tail:    []byte{want},
		})
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
	}
	return fmt.Errorf("relations: vocabulary of %s lost %d races in a row", field, allocRetries)
}

// vocabularyState reads a column's state record, returning the state
// byte and the record exactly as stored -- which the compare
// precondition of the next write needs verbatim.
func (j *Journal) vocabularyState(ctx context.Context, field Entity) (state byte, raw []byte, found bool, err error) {
	key := RelationKey(field, j.StatusMarker())
	raw, found, err = j.st.get(ctx, key)
	if err != nil || !found {
		return vocabularyOpen, nil, false, err
	}
	rec, _, err := DecodeRecord(raw)
	if err != nil {
		return vocabularyOpen, nil, false, err
	}
	if rec.Kind != KindFieldState || len(rec.Data) != 1 {
		return vocabularyOpen, nil, false, fmt.Errorf("relations: %s has a status record of kind %#x, want a vocabulary state", field, rec.Kind)
	}
	return rec.Data[0], raw, true, nil
}

// vocabularyGuardOps is what makes the closure a rule rather than a
// check: the precondition every new term's transaction carries, saying
// the column's vocabulary is still exactly as open as it was read to be.
// A closure landing at the same moment turns the term's write into a
// conflict rather than letting it through.
func (j *Journal) vocabularyGuardOps(ctx context.Context, field Entity) ([]Op, error) {
	state, raw, found, err := j.vocabularyState(ctx, field)
	if err != nil {
		return nil, err
	}
	key := RelationKey(field, j.StatusMarker())
	if !found {
		return []Op{{Kind: OpCompareAbsent, Key: key}}, nil
	}
	if state == vocabularyClosed {
		return nil, fmt.Errorf("%w: %s", ErrFieldClosed, field)
	}
	return []Op{{Kind: OpCompare, Key: key, Value: raw}}, nil
}

func vocabularyName(state byte) string {
	if state == vocabularyClosed {
		return "closed"
	}
	return "open"
}
