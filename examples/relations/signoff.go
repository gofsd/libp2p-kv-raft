package relations

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// TypePage is the entity a page's own records hang off -- one per page,
// at id 0, which no line ever takes (line ids start at 1). A page is not
// a thing this format needed until somebody had to sign one off; now it
// is, it is an entity like everything else.
const TypePage uint8 = 0x07

// KindPageSignoff relates a page to the status marker, and is what "this
// page is closed" means: page -> status marker, authored and signed by
// whoever closed it, with the number of lines it held at that moment in
// the payload.
const KindPageSignoff byte = 0x0E

// ErrPageSignedOff is returned when a page has already been signed off.
// Ruling a page off is a once-only act, the same way striking a line is.
var ErrPageSignedOff = errors.New("relations: this page has already been signed off")

// PageEntity is the entity standing for one page of this log.
func (j *Journal) PageEntity(page uint8) Entity {
	return Entity{Log: j.st.Log, Page: page, Type: TypePage, ID: 0}
}

// SignOffPage rules a page off: it records who closed it and when, and
// closes the page to further lines, in one transaction.
//
// The closing is not a rule this layer enforces on the way in -- it is
// the allocator's own counter gaining a flag (Store.ClosePageOps), so
// Append simply finds the page full and rolls onto the next one, exactly
// as it would if the page had run out of line numbers. There is no
// second code path that could disagree with the first.
//
// This is the end-of-shift signature a paper log book has at the foot of
// every page, and it is chained like any other event, so removing it is
// as visible as removing a line.
func (j *Journal) SignOffPage(ctx context.Context, page uint8) error {
	if page < FirstEntryPage {
		return fmt.Errorf("relations: sign off: page %d is not a page of entries", page)
	}
	if err := j.checkSignable(ctx, page); err != nil {
		return err
	}

	subject := j.PageEntity(page)
	key := RelationKey(subject, j.StatusMarker())
	for attempt := 0; attempt < allocRetries; attempt++ {
		lines, err := j.st.LastAllocated(ctx, page, TypeEntry)
		if err != nil {
			return err
		}
		closeOps, err := j.st.ClosePageOps(ctx, page, TypeEntry)
		if err != nil {
			return err
		}
		signOps, err := j.st.LinkOps(subject, j.StatusMarker(), KindPageSignoff, []byte{lines})
		if err != nil {
			return err
		}

		ops := append([]Op{{Kind: OpCompareAbsent, Key: key}}, closeOps...)
		ops = append(ops, signOps...)
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
		// Either somebody wrote a line on this page, signed it off, or
		// moved the chain head between the reads above and the write.
		// The first is why the line count is re-read on the way round.
		if err := j.checkSignable(ctx, page); err != nil {
			return err
		}
	}
	return fmt.Errorf("relations: sign off: page %d lost %d races in a row", page, allocRetries)
}

// PageSignoff is a closed page, as PageStatus and SignedOffPages read it
// back.
type PageSignoff struct {
	Page uint8
	// By and Name are the actor who closed the page and what their
	// declaration calls them; At is when.
	By   Entity
	Name string
	At   time.Time
	// Lines is how many lines the page held when it was closed, which is
	// what makes "and then somebody added one" a contradiction rather
	// than a judgement call.
	Lines uint8
}

// PageStatus returns a page's sign-off, or false if it is still open.
func (j *Journal) PageStatus(ctx context.Context, page uint8) (PageSignoff, bool, error) {
	rel, found, err := j.st.Lookup(ctx, j.PageEntity(page), j.StatusMarker())
	if err != nil || !found {
		return PageSignoff{}, false, err
	}
	if rel.Record.Kind != KindPageSignoff {
		return PageSignoff{}, false, fmt.Errorf("relations: page %d has a status record of kind %#x, want a sign-off", page, rel.Record.Kind)
	}
	if len(rel.Record.Data) != 1 {
		return PageSignoff{}, false, fmt.Errorf("relations: page %d's sign-off carries %d bytes, want 1", page, len(rel.Record.Data))
	}
	name := ""
	if decl, ok, err := j.st.Declaration(ctx, rel.Record.Author); err != nil {
		return PageSignoff{}, false, err
	} else if ok {
		name = decl.Record.Name
	}
	return PageSignoff{
		Page:  page,
		By:    rel.Record.Author,
		Name:  name,
		At:    rel.Record.Created,
		Lines: rel.Record.Data[0],
	}, true, nil
}

// SignedOffPages returns every closed page of this log, in page order.
func (j *Journal) SignedOffPages(ctx context.Context) ([]PageSignoff, error) {
	rels, err := j.st.Backlinks(ctx, j.StatusMarker())
	if err != nil {
		return nil, err
	}
	var out []PageSignoff
	for _, rel := range OfKind(rels, KindPageSignoff) {
		signoff, found, err := j.PageStatus(ctx, rel.A.Page)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, signoff)
		}
	}
	return out, nil
}

// checkSignable turns the reasons a page cannot be signed off into
// errors that say which; the transaction's own preconditions are what
// make the refusal airtight.
func (j *Journal) checkSignable(ctx context.Context, page uint8) error {
	if _, found, err := j.PageStatus(ctx, page); err != nil {
		return err
	} else if found {
		return fmt.Errorf("%w: page %d", ErrPageSignedOff, page)
	}
	closed, err := j.st.PageIsClosed(ctx, page, TypeEntry)
	if err != nil {
		return err
	}
	if closed {
		return fmt.Errorf("%w: page %d is closed to further lines", ErrPageSignedOff, page)
	}
	return nil
}
