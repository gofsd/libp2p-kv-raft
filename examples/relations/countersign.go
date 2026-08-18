package relations

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"
)

// KindCountersign relates a line to somebody who endorsed it: entry ->
// actor. The record's own author *is* the countersigner, so the
// signature on it is the countersignature -- there is nothing else it
// could be, and nothing to keep in step.
const KindCountersign byte = 0x0D

// ErrAlreadyCountersigned is returned when this actor has already
// countersigned this line. Endorsing something twice says no more than
// endorsing it once.
var ErrAlreadyCountersigned = errors.New("relations: this actor has already countersigned this line")

// Countersign endorses a line under the store's own actor and key: the
// operator writes the line, the supervisor countersigns it, and both
// signatures are on the record afterwards.
//
// It is a separate call rather than a field on Append because it is a
// separate act, usually by a different person at a different time, and
// often on a different device -- which in this package means a different
// Store, carrying that person's key and actor entity. Whoever holds the
// key is who the countersignature is from.
//
// Several people may countersign one line (a supervisor and a QA
// inspector are different keys and different records), but nobody may
// countersign twice, countersign their own line, or countersign a line
// that no longer stands. All three are refused, and the last two are
// guarded by compare preconditions in the same transaction rather than
// by the check alone, so a line struck at the same moment cannot slip
// an endorsement past.
func (j *Journal) Countersign(ctx context.Context, entry Entity) error {
	actor := j.st.Author
	if err := j.checkCountersignable(ctx, entry, actor); err != nil {
		return err
	}

	key := RelationKey(entry, actor)
	for attempt := 0; attempt < allocRetries; attempt++ {
		ops, err := j.st.LinkOps(entry, actor, KindCountersign, nil)
		if err != nil {
			return err
		}
		ops = append([]Op{
			// Nobody may endorse this line twice...
			{Kind: OpCompareAbsent, Key: key},
			// ...and it has to still stand at the moment of endorsing.
			{Kind: OpCompareAbsent, Key: RelationKey(entry, j.StatusMarker())},
		}, ops...)

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
		// A precondition failed: either somebody struck or endorsed the
		// line under us -- which checkCountersignable turns into the
		// error that says which -- or the chain head moved, which is
		// just a retry.
		if err := j.checkCountersignable(ctx, entry, actor); err != nil {
			return err
		}
	}
	return fmt.Errorf("relations: countersign: %s lost %d races in a row", entry, allocRetries)
}

// Countersignature is one endorsement of a line, as Countersignatures
// reads them back.
type Countersignature struct {
	Entry Entity
	// Actor is who endorsed it, and Name what their declaration calls
	// them.
	Actor Entity
	Name  string
	At    time.Time
}

// Countersignatures returns every endorsement of a line, in the order
// the actors' entities sort -- one prefix scan of the line, filtered to
// the endorsements.
func (j *Journal) Countersignatures(ctx context.Context, entry Entity) ([]Countersignature, error) {
	if entry.Type != TypeEntry {
		return nil, fmt.Errorf("relations: countersignatures: %s is not a line", entry)
	}
	rels, err := j.st.Relations(ctx, entry)
	if err != nil {
		return nil, err
	}
	signed := OfKind(rels, KindCountersign)
	out := make([]Countersignature, 0, len(signed))
	for _, rel := range signed {
		decl, found, err := j.st.Declaration(ctx, rel.B)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("relations: countersignatures: %s endorsed %s but is not declared", rel.B, entry)
		}
		out = append(out, Countersignature{
			Entry: entry,
			Actor: rel.B,
			Name:  decl.Record.Name,
			At:    rel.Record.Created,
		})
	}
	return out, nil
}

// CountersignedBy reports whether actor has endorsed entry.
func (j *Journal) CountersignedBy(ctx context.Context, entry, actor Entity) (bool, error) {
	_, found, err := j.st.Lookup(ctx, entry, actor)
	return found, err
}

// checkCountersignable turns the reasons an endorsement is refused into
// errors that say which. The transaction's own preconditions are what
// make the refusal airtight; this is what makes it legible.
func (j *Journal) checkCountersignable(ctx context.Context, entry, actor Entity) error {
	if entry.Type != TypeEntry || entry.ID == 0 {
		return fmt.Errorf("relations: countersign: %s is not a line", entry)
	}
	if actor.IsZero() {
		return fmt.Errorf("relations: countersign: this store has no actor to sign as")
	}
	actorDecl, found, err := j.st.Declaration(ctx, actor)
	if err != nil {
		return err
	}
	if !found || len(actorDecl.Record.Data) != ed25519.PublicKeySize {
		return fmt.Errorf("relations: countersign: %s is not declared with a public key, so nobody could check the endorsement", actor)
	}

	decl, found, err := j.st.Declaration(ctx, entry)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("relations: countersign: %s is not a line this log has written", entry)
	}
	if decl.Record.Author == actor {
		return fmt.Errorf("relations: countersign: %s wrote this line; a second signature from the same actor endorses nothing", actor)
	}

	status, err := j.Status(ctx, entry)
	if err != nil {
		return err
	}
	if !status.Live() {
		return fmt.Errorf("%w: %s is %s", ErrAlreadyStruck, entry, status.State)
	}
	if signed, err := j.CountersignedBy(ctx, entry, actor); err != nil {
		return err
	} else if signed {
		return fmt.Errorf("%w: %s on %s", ErrAlreadyCountersigned, actor, entry)
	}
	return nil
}
