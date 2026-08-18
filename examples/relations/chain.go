package relations

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"time"
)

// A signature proves a record was not *edited*. It cannot prove one was
// not *removed*: delete a record outright and everything left still
// verifies perfectly, with nothing to say the missing one ever existed.
// In a log that replaces paper, that is the difference between evidence
// and decoration -- tearing a page out is the classic way a paper log
// gets falsified, and per-record signing does nothing about it.
//
// So every change to the book goes into one numbered event stream, and
// each event's digest covers the event before it. There are four kinds:
//
//   - a line being written (an Append, or the replacement half of a
//     Correct),
//   - a line being struck (a Void, or the strike half of a Correct),
//   - a line being countersigned, and
//   - a page being signed off.
//
// The log keeps a running head -- one record holding the last event's
// digest and how many events there have been -- advanced under a compare
// precondition in the same transaction as the event itself. So the chain
// cannot fork, cannot skip, and cannot half-exist.
//
// Everything after the first kind is an event *about* a line rather than
// a line, which is why events are keyed by sequence number rather than
// by their subject: a line has exactly one write, but it can be
// countersigned by several people, and no key derived from the subject
// alone would be unique. Keying by sequence also makes the whole stream
// one ordered prefix scan, and a missing event a visible gap rather than
// an inference.
//
// The cost is a real serialization point: every write reads and rewrites
// one record, so concurrent writers contend on it and the losers retry.
// A log book has one pen at a time, which is the same trade the paper
// original makes.

// TypeChain is the second entity type this package reserves for itself
// (TypeAllocator being the first). Two fixed entities live under it,
// neither of which Allocate ever hands out: the event anchor at id 0 and
// the running head at id 1. Applications may use any other type byte.
const TypeChain uint8 = 0xFE

// The chain's own record kinds.
const (
	// KindChainLink is one event: a relation from the event anchor to
	// the entity that spells its sequence number.
	KindChainLink byte = 0x09
	// KindChainHead is the declaration holding the running head.
	KindChainHead byte = 0x0B
)

// The event tags. They go into the digest, so one kind of event's digest
// can never be replayed as another's, and they tell a verifier how to
// recompute it.
const (
	eventLine        byte = 1
	eventStrike      byte = 2
	eventCountersign byte = 3
	eventSignoff     byte = 4
	// The two below are events about records that can change again -- a
	// name being rewritten, a vocabulary being closed and reopened. They
	// are digested differently from everything else; see mutableEvent.
	eventDeclare    byte = 5
	eventFieldState byte = 6
)

// mutableEvent reports whether an event is about a record that can be
// written again later.
//
// Every other event digests the record it wrote, which works precisely
// because that record never changes afterwards: a strike, an endorsement
// and a sign-off are each written once and stand. A rename is not --
// rename a term twice and the first event's digest would no longer match
// what is stored, through nobody's fault.
//
// So these events do not digest the record. They carry their own content
// in the chain link (Event.Tail), which is immutable and signed, and the
// digest covers that. What binds the chain to what is actually stored is
// then one check at the end: the *latest* event for a given record must
// agree with the record as it now stands. Together those say the whole
// sequence of changes happened, in that order, by those people -- and
// that the current state is the one the last of them left behind.
func mutableEvent(tag byte) bool {
	return tag == eventDeclare || tag == eventFieldState
}

// Widths: a digest, a link record's payload (digest, tag, and the two
// entities that locate what the event was about), and the head's payload
// (digest and event count).
const (
	ChainDigestLen = sha256.Size
	// chainLinkHeadLen is the fixed part of a link's payload; a mutable
	// event appends its own content after it (see Event.Tail).
	chainLinkHeadLen = ChainDigestLen + 1 + 2*EntityLen
	chainHeadLen     = ChainDigestLen + 8
)

// maxChainSeq is the last sequence number expressible as an entity. Four
// billion events is far beyond the ~65k lines a log byte can hold, so
// this is a bound worth stating rather than one worth designing around.
const maxChainSeq = uint64(^uint32(0))

// genesisDigest is what the first event chains onto: 32 zero bytes,
// which no SHA-256 output collides with in practice, so "this is the
// first event" is not forgeable as "this event follows one you cannot
// see".
var genesisDigest = make([]byte, ChainDigestLen)

// EventAnchor is what every event's chain link hangs off. Its relations
// are the whole event stream, in order, because the target entity spells
// the sequence number -- so reading the chain is one prefix scan, and it
// pages like any other (see Range).
func (j *Journal) EventAnchor() Entity {
	return Entity{Log: j.st.Log, Page: SchemaPage, Type: TypeChain, ID: 0}
}

// ChainHead is the entity whose declaration holds the running head: the
// last event's digest and how many events there have been. It is the one
// record every write in this log has to agree on, and the reason a
// deleted *last* event is detectable at all -- a chain alone can only
// notice a break in front of something that still exists.
func (j *Journal) ChainHead() Entity {
	return Entity{Log: j.st.Log, Page: SchemaPage, Type: TypeChain, ID: 1}
}

// chainBuilder accumulates the events of one transaction: it reads the
// head once, digests each event onto the last, and emits the link
// records plus the head update, all guarded so the whole transaction
// fails if another writer moved the head first.
type chainBuilder struct {
	journal *Journal
	// prev is the digest the next event chains onto, and seq the
	// sequence number it will take -- both advancing as events are
	// added. Sequence numbers start at 1, because 0 is the entity that
	// means "declaration" and so could never be a link's target.
	prev []byte
	seq  uint64
	// headRaw is the head record exactly as read, which the compare
	// precondition needs verbatim; nil when the log has no head yet.
	headRaw []byte
	ops     []Op
}

func (j *Journal) newChainBuilder(ctx context.Context) (*chainBuilder, error) {
	b := &chainBuilder{journal: j, prev: genesisDigest, seq: 1}
	raw, found, err := j.st.get(ctx, DeclarationKey(j.ChainHead()))
	if err != nil || !found {
		return b, err
	}
	digest, count, err := decodeHead(raw)
	if err != nil {
		return nil, err
	}
	b.prev = digest
	b.seq = count + 1
	b.headRaw = raw
	return b, nil
}

// addLine chains a written line, digesting the records its transaction
// writes under that line.
func (b *chainBuilder) addLine(entry Entity, content []Op) error {
	return b.add(eventLine, entry, Zero, lineDigest(b.prev, b.seq, entry, contentRecords(entry, content)), nil)
}

// addRecord chains an event that consists of exactly one record -- a
// strike, a countersignature, a page sign-off. subject and party are
// that record's two entities, which is all a verifier needs to find it
// again.
func (b *chainBuilder) addRecord(tag byte, subject, party Entity, key, value []byte) error {
	return b.add(tag, subject, party, recordDigest(b.prev, b.seq, tag, key, value), nil)
}

// addMutable chains an event about a record that may be written again:
// the digest covers the tail rather than the record, and the tail rides
// along in the link. See mutableEvent.
func (b *chainBuilder) addMutable(tag byte, subject, party Entity, tail []byte) error {
	key := RelationKey(subject, party)
	return b.add(tag, subject, party, recordDigest(b.prev, b.seq, tag, key, tail), tail)
}

func (b *chainBuilder) add(tag byte, subject, party Entity, digest, tail []byte) error {
	if b.seq > maxChainSeq {
		return fmt.Errorf("relations: chain: this log has run out of event sequence numbers (%d)", maxChainSeq)
	}
	payload := make([]byte, 0, chainLinkHeadLen+len(tail))
	payload = append(payload, digest...)
	payload = append(payload, tag)
	sb, pb := subject.Bytes(), party.Bytes()
	payload = append(payload, sb[:]...)
	payload = append(payload, pb[:]...)
	payload = append(payload, tail...)

	ops, err := b.journal.st.LinkOps(b.journal.EventAnchor(), EntityFromValue(uint32(b.seq)), KindChainLink, payload)
	if err != nil {
		return err
	}
	b.ops = append(b.ops, ops...)
	b.prev = digest
	b.seq++
	return nil
}

// finish returns the link records added so far plus the head update,
// preconditioned on the head not having moved. Applying anything less
// than all of it would leave the chain describing a book that is not
// there.
func (b *chainBuilder) finish() ([]Op, error) {
	if len(b.ops) == 0 {
		return nil, nil
	}
	headKey := DeclarationKey(b.journal.ChainHead())
	pre := Op{Kind: OpCompareAbsent, Key: headKey}
	if b.headRaw != nil {
		pre = Op{Kind: OpCompare, Key: headKey, Value: b.headRaw}
	}
	data := make([]byte, 0, chainHeadLen)
	data = append(data, b.prev...)
	data = binary.BigEndian.AppendUint64(data, b.seq-1)
	head, err := b.journal.st.DeclareOps(b.journal.ChainHead(), KindChainHead, "", data, time.Time{})
	if err != nil {
		return nil, err
	}
	return append(append(b.ops, pre), head...), nil
}

// chainEventOps is what every writing path calls with the ops it is
// about to apply, and returns the chain's own ops to append to them.
//
// It works the events out from the ops themselves rather than being told
// what they are: the records written under `line` are that line's own
// content, and *any other* relation the transaction writes is an event
// in its own right -- which is what a strike, a countersignature and a
// page sign-off each are. A relation kind with no event tag is an error
// rather than something quietly left out of the chain, so a new
// after-the-fact record added to this package cannot forget to chain
// itself.
func (j *Journal) chainEventOps(ctx context.Context, line Entity, ops []Op, mutable ...mutableSpec) ([]Op, error) {
	b, err := j.newChainBuilder(ctx)
	if err != nil {
		return nil, err
	}
	if !line.IsZero() {
		if err := b.addLine(line, ops); err != nil {
			return nil, err
		}
	}
	// A mutable event already accounts for the record it is about, so
	// the sweep below must not chain that record a second time.
	covered := make(map[string]bool, len(mutable))
	for _, spec := range mutable {
		if err := b.addMutable(spec.tag, spec.subject, spec.party, spec.tail); err != nil {
			return nil, err
		}
		covered[string(RelationKey(spec.subject, spec.party))] = true
	}
	for _, op := range ops {
		if covered[string(op.Key)] {
			continue
		}
		subject, party, rec, ok, err := standaloneRecord(line, op)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		tag, known := eventTagFor(rec.Kind)
		if !known {
			return nil, fmt.Errorf("relations: chain: record kind %#x on %s -> %s has no event tag; it would go unchained",
				rec.Kind, subject, party)
		}
		if err := b.addRecord(tag, subject, party, op.Key, op.Value); err != nil {
			return nil, err
		}
	}
	return b.finish()
}

// mutableSpec is one event about a record that can change again, which a
// caller has to describe rather than have inferred: what it attests to
// is not recoverable from the store afterwards, which is the whole
// reason it travels in the link.
type mutableSpec struct {
	tag     byte
	subject Entity
	party   Entity
	tail    []byte
}

// standaloneRecord reports whether op writes a relation that is an event
// of its own: a forward-namespace record that is neither part of `line`
// nor a chain link.
func standaloneRecord(line Entity, op Op) (subject, party Entity, rec Record, ok bool, err error) {
	if op.Kind != OpSet || len(op.Key) != KeyLen || op.Key[0] != NamespaceRelation {
		return Zero, Zero, Record{}, false, nil
	}
	_, a, b, err := ParseKey(op.Key)
	if err != nil {
		return Zero, Zero, Record{}, false, err
	}
	if b.IsZero() || a == line || a.Type == TypeChain || b.Type == TypeChain {
		return Zero, Zero, Record{}, false, nil
	}
	rec, _, err = DecodeRecord(op.Value)
	if err != nil {
		return Zero, Zero, Record{}, false, err
	}
	return a, b, rec, true, nil
}

// eventTagFor maps a record kind to the event it constitutes.
func eventTagFor(kind byte) (byte, bool) {
	switch kind {
	case KindSuperseded, KindVoided:
		return eventStrike, true
	case KindCountersign:
		return eventCountersign, true
	case KindPageSignoff:
		return eventSignoff, true
	default:
		return 0, false
	}
}

// contentRecords picks out the records a line's transaction writes that
// belong to the line itself: forward-namespace writes anchored at entry,
// minus the sentinels. The chain links are excluded because they carry
// the digest being computed, and the status marker because a line's
// standing is chained as its own event -- if it were in the line's
// digest too, correcting a line would break the line's own digest.
//
// A correction's transaction also writes the strike onto the line it
// supersedes; that record is anchored at *that* line, so this filter
// leaves it out without needing to name it.
func contentRecords(entry Entity, ops []Op) []Pair {
	var records []Pair
	for _, op := range ops {
		if op.Kind != OpSet || len(op.Key) != KeyLen || op.Key[0] != NamespaceRelation {
			continue
		}
		_, a, b, err := ParseKey(op.Key)
		if err != nil || a != entry || isSentinel(b) {
			continue
		}
		records = append(records, Pair{Key: op.Key, Value: op.Value})
	}
	return records
}

// isSentinel reports whether e is one of the fixed anchors a line points
// at for bookkeeping rather than content: the chain's own entities, or
// the status marker.
func isSentinel(e Entity) bool {
	return e.Type == TypeChain || (e.Type == TypeEntry && e.ID == 0)
}

// lineDigest and recordDigest are the two definitions of an event's
// digest, used both to write and to check it. Both start with the
// previous event's digest and this event's sequence number, then a tag
// saying which kind of event it is, then what the event actually did.
//
// Sorting the content by key rather than trusting the order it was built
// in is what makes the two sides agree: a scan returns records in key
// order, an Append does not.
func lineDigest(previous []byte, seq uint64, entry Entity, records []Pair) []byte {
	h := sha256.New()
	writeEventPreamble(h, previous, seq, eventLine)
	h.Write(DeclarationKey(entry))
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].Key, records[j].Key) < 0 })
	for _, rec := range records {
		h.Write(rec.Key)
		h.Write(rec.Value)
	}
	return h.Sum(nil)
}

func recordDigest(previous []byte, seq uint64, tag byte, key, value []byte) []byte {
	h := sha256.New()
	writeEventPreamble(h, previous, seq, tag)
	h.Write(key)
	h.Write(value)
	return h.Sum(nil)
}

func writeEventPreamble(h hashWriter, previous []byte, seq uint64, tag byte) {
	h.Write(previous)
	h.Write(binary.BigEndian.AppendUint64(nil, seq))
	h.Write([]byte{tag})
}

// hashWriter is the slice of hash.Hash the digests above need.
type hashWriter interface{ Write([]byte) (int, error) }

// Event is one entry of the chain as Events and VerifyChain read it
// back.
type Event struct {
	// Seq is the event's place in the stream, counting from 1.
	Seq uint64
	// Subject is the line the event is about -- or the page, for a
	// sign-off.
	Subject Entity
	// Party is the record's other end: the status marker for a strike or
	// a sign-off, the countersigner for a countersignature, and the zero
	// entity for a line.
	Party Entity
	// Digest is the event's own digest and Tag says which kind it is.
	Digest []byte
	Tag    byte
	// Tail is what a mutable event attests to -- the names a rename
	// moved between, the state a vocabulary was put into -- and is empty
	// for every other kind. See mutableEvent.
	Tail []byte

	link Relation
}

// Kind names the event for a reader: "line", "strike", "countersign" or
// "signoff".
func (e Event) Kind() string {
	switch e.Tag {
	case eventLine:
		return "line"
	case eventStrike:
		return "strike"
	case eventCountersign:
		return "countersign"
	case eventSignoff:
		return "signoff"
	case eventDeclare:
		return "rename"
	case eventFieldState:
		return "vocabulary"
	default:
		return fmt.Sprintf("event(%d)", e.Tag)
	}
}

// At and By are when the event was recorded and by whom, taken from the
// chain link's own record.
func (e Event) At() time.Time { return e.link.Record.Created }
func (e Event) By() Entity    { return e.link.Record.Author }

// Events returns the log's event stream in order -- one prefix scan of
// the anchor, bounded and resumable like any other (r's bounds are on
// the sequence entity, so Range{Limit: n} reads the first n).
func (j *Journal) Events(ctx context.Context, r Range) ([]Event, error) {
	rels, err := j.st.RelationsRange(ctx, j.EventAnchor(), r)
	if err != nil {
		return nil, err
	}
	events := make([]Event, 0, len(rels))
	for _, rel := range OfKind(rels, KindChainLink) {
		if len(rel.Record.Data) < chainLinkHeadLen {
			return nil, fmt.Errorf("relations: chain: event %d carries %d bytes, want at least %d",
				rel.B.Value(), len(rel.Record.Data), chainLinkHeadLen)
		}
		subject, err := DecodeEntity(rel.Record.Data[ChainDigestLen+1:])
		if err != nil {
			return nil, err
		}
		party, err := DecodeEntity(rel.Record.Data[ChainDigestLen+1+EntityLen:])
		if err != nil {
			return nil, err
		}
		events = append(events, Event{
			Seq:     uint64(rel.B.Value()),
			Subject: subject,
			Party:   party,
			Digest:  rel.Record.Data[:ChainDigestLen],
			Tag:     rel.Record.Data[ChainDigestLen],
			Tail:    rel.Record.Data[chainLinkHeadLen:],
			link:    rel,
		})
	}
	return events, nil
}

// VerifyChain replays the whole log and reports how many events it
// checked, or the first point at which the record stops adding up. A
// correction is two events -- the replacement line and the strike on the
// line it replaces -- so the count is not the number of lines.
//
// For each event it recomputes the digest from the previous event's and
// the event's own content, and checks every signature involved against
// the public key its author declared. Four things beyond the digests
// have to hold as well, and each closes a hole a chain alone leaves:
//
//   - Every line id the allocator ever handed out has a line event, so a
//     line cannot be dropped together with its link. This runs first,
//     because it names the line -- which a gap in the sequence cannot.
//   - The sequence numbers run 1..n with no gaps, so an event removed
//     from the middle is named directly.
//   - The head's count matches the events found, which catches an event
//     removed from the *end*, where no later digest is left to break.
//   - The replayed digest equals the stored head, so none of the above
//     can be hidden by rebuilding part of the chain. The head is signed
//     like everything else, so rebuilding it needs the key.
func (j *Journal) VerifyChain(ctx context.Context) (int, error) {
	events, err := j.Events(ctx, Range{})
	if err != nil {
		return 0, err
	}

	headDigest, headCount, found, err := j.readHead(ctx)
	if err != nil {
		return 0, err
	}
	if !found {
		if len(events) > 0 {
			return 0, fmt.Errorf("relations: chain: %d events are chained but the log has no head record", len(events))
		}
		return 0, nil
	}

	lines := make(map[Entity]bool, len(events))
	for _, event := range events {
		if event.Tag == eventLine {
			lines[event.Subject] = true
		}
	}
	if err := j.checkEveryLineIsChained(ctx, lines); err != nil {
		return 0, err
	}

	verify := j.signatureChecker(ctx)
	latest := make(map[string]Event)
	previous := genesisDigest
	checked := 0

	for i, event := range events {
		if event.Seq != uint64(i)+1 {
			return checked, fmt.Errorf("relations: chain: event %d is missing -- the next one recorded is %d, a %s on %s",
				i+1, event.Seq, event.Kind(), event.Subject)
		}
		if err := verify(event.link); err != nil {
			return checked, fmt.Errorf("relations: chain: event %d (%s on %s): chain link: %w",
				event.Seq, event.Kind(), event.Subject, err)
		}

		var want []byte
		switch {
		case event.Tag == eventLine:
			records, err := j.lineContent(ctx, event.Subject, verify)
			if err != nil {
				return checked, fmt.Errorf("relations: chain: event %d: line %s: %w", event.Seq, event.Subject, err)
			}
			want = lineDigest(previous, event.Seq, event.Subject, records)
		case mutableEvent(event.Tag):
			// The record this is about may have been written again
			// since, so the digest covers what the link itself carries.
			// Keeping the latest event per record is what ties the chain
			// back to what is stored -- see the check after the loop.
			key := RelationKey(event.Subject, event.Party)
			want = recordDigest(previous, event.Seq, event.Tag, key, event.Tail)
			latest[string(key)] = event
		default:
			record, found, err := j.st.Lookup(ctx, event.Subject, event.Party)
			if err != nil {
				return checked, err
			}
			if !found {
				return checked, fmt.Errorf("relations: chain: event %d records a %s on %s, but that record is gone -- it has been removed",
					event.Seq, event.Kind(), event.Subject)
			}
			if err := verify(record); err != nil {
				return checked, fmt.Errorf("relations: chain: event %d (%s on %s): %w",
					event.Seq, event.Kind(), event.Subject, err)
			}
			want = recordDigest(previous, event.Seq, event.Tag, record.key, record.value)
		}

		if !bytes.Equal(want, event.Digest) {
			return checked, fmt.Errorf("relations: chain: event %d (%s on %s) does not match its digest -- it, or something before it, has been changed or removed",
				event.Seq, event.Kind(), event.Subject)
		}
		previous = event.Digest
		checked++
	}

	if uint64(len(events)) != headCount {
		return checked, fmt.Errorf("relations: chain: the head counts %d events, %d are chained -- %d have been removed",
			headCount, len(events), int64(headCount)-int64(len(events)))
	}
	if !bytes.Equal(previous, headDigest) {
		return checked, fmt.Errorf("relations: chain: the replayed chain does not end at the head the log records")
	}
	if err := j.checkMutableState(ctx, latest, verify); err != nil {
		return checked, err
	}
	return checked, nil
}

// checkMutableState is the other half of how a mutable record is
// covered: its events say what it was set to and when, and this says the
// record actually holds what the last of them left. Without it a rename
// could be performed with no event at all -- every digest would still
// line up, because none of them covers the record.
func (j *Journal) checkMutableState(ctx context.Context, latest map[string]Event, verify func(Relation) error) error {
	for _, event := range latest {
		record, found, err := j.st.Lookup(ctx, event.Subject, event.Party)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("relations: chain: event %d records a %s on %s, but that record is gone -- it has been removed",
				event.Seq, event.Kind(), event.Subject)
		}
		if err := verify(record); err != nil {
			return fmt.Errorf("relations: chain: %s on %s: %w", event.Kind(), event.Subject, err)
		}
		if err := matchesMutableState(event, record); err != nil {
			return fmt.Errorf("relations: chain: %s", err)
		}
	}
	return nil
}

// matchesMutableState checks one record against the last event about it.
func matchesMutableState(event Event, record Relation) error {
	switch event.Tag {
	case eventDeclare:
		_, want, err := decodeRenameTail(event.Tail)
		if err != nil {
			return err
		}
		if record.Record.Name != want {
			return fmt.Errorf("%s is named %q, but the last rename (event %d) set it to %q -- it has been renamed with no record of it",
				event.Subject, record.Record.Name, event.Seq, want)
		}
	case eventFieldState:
		if len(event.Tail) != 1 || len(record.Record.Data) != 1 || record.Record.Data[0] != event.Tail[0] {
			return fmt.Errorf("the vocabulary of %s is in a state the chain does not record (event %d)", event.Subject, event.Seq)
		}
	}
	return nil
}

// encodeRenameTail and decodeRenameTail carry both names a rename moved
// between, because neither is recoverable from the store afterwards: the
// old one is gone, and the new one can be superseded by a later rename.
func encodeRenameTail(from, to string) ([]byte, error) {
	if len(from) > maxFieldLen || len(to) > maxFieldLen {
		return nil, fmt.Errorf("relations: rename: a name longer than %d bytes cannot be chained", maxFieldLen)
	}
	tail := binary.BigEndian.AppendUint16(nil, uint16(len(from)))
	tail = append(tail, from...)
	return append(tail, to...), nil
}

func decodeRenameTail(tail []byte) (from, to string, err error) {
	if len(tail) < 2 {
		return "", "", fmt.Errorf("relations: chain: a rename event carries %d bytes, want at least 2", len(tail))
	}
	n := int(binary.BigEndian.Uint16(tail))
	if len(tail) < 2+n {
		return "", "", fmt.Errorf("relations: chain: a rename event is truncated inside the old name")
	}
	return string(tail[2 : 2+n]), string(tail[2+n:]), nil
}

// Rename reports the names a rename event moved between, and whether
// this event is one.
func (e Event) Rename() (from, to string, ok bool) {
	if e.Tag != eventDeclare {
		return "", "", false
	}
	from, to, err := decodeRenameTail(e.Tail)
	if err != nil {
		return "", "", false
	}
	return from, to, true
}

// readHead returns the running head: the last event's digest and how
// many events there have been.
func (j *Journal) readHead(ctx context.Context) (digest []byte, count uint64, found bool, err error) {
	raw, found, err := j.st.get(ctx, DeclarationKey(j.ChainHead()))
	if err != nil || !found {
		return nil, 0, false, err
	}
	digest, count, err = decodeHead(raw)
	if err != nil {
		return nil, 0, false, err
	}
	return digest, count, true, nil
}

func decodeHead(raw []byte) (digest []byte, count uint64, err error) {
	rec, _, err := DecodeRecord(raw)
	if err != nil {
		return nil, 0, fmt.Errorf("relations: chain head: %w", err)
	}
	if len(rec.Data) != chainHeadLen {
		return nil, 0, fmt.Errorf("relations: chain head carries %d bytes, want %d", len(rec.Data), chainHeadLen)
	}
	return rec.Data[:ChainDigestLen], binary.BigEndian.Uint64(rec.Data[ChainDigestLen:]), nil
}

// lineContent reads back the records a line event's digest covers,
// verifying each one's signature on the way.
//
// Countersignatures are excluded for the same reason a strike is: they
// arrive after the line's digest is fixed, and are chained as events of
// their own.
func (j *Journal) lineContent(ctx context.Context, entry Entity, verify func(Relation) error) ([]Pair, error) {
	decl, found, err := j.st.Declaration(ctx, entry)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("it has a chain link but no declaration")
	}
	if err := verify(decl); err != nil {
		return nil, fmt.Errorf("declaration: %w", err)
	}
	rels, err := j.st.Relations(ctx, entry)
	if err != nil {
		return nil, err
	}
	var records []Pair
	for _, rel := range rels {
		if isSentinel(rel.B) || rel.Record.Kind == KindCountersign {
			continue
		}
		if err := verify(rel); err != nil {
			return nil, fmt.Errorf("relation to %s: %w", rel.B, err)
		}
		records = append(records, Pair{Key: rel.key, Value: rel.value})
	}
	return records, nil
}

// checkEveryLineIsChained makes sure no line the allocator handed out an
// id for has gone missing along with its chain link.
func (j *Journal) checkEveryLineIsChained(ctx context.Context, chained map[Entity]bool) error {
	for page := int(FirstEntryPage); page <= 0xFF; page++ {
		// A page with no counter is one the log never reached; a page
		// with a counter and no lines is one that was closed off empty,
		// and the pages after it still count.
		last, reached, err := j.st.PageAllocated(ctx, uint8(page), TypeEntry)
		if err != nil {
			return err
		}
		if !reached {
			return leftoverLines(chained)
		}
		for id := 1; id <= int(last); id++ {
			entry := Entity{Log: j.st.Log, Page: uint8(page), Type: TypeEntry, ID: uint8(id)}
			if !chained[entry] {
				return fmt.Errorf("relations: chain: line %s is missing -- the allocator handed out its id, but no event wrote it", entry)
			}
			delete(chained, entry)
		}
	}
	return leftoverLines(chained)
}

// leftoverLines reports a line event for a line the allocator never
// handed out an id for -- the opposite splice from a missing line, and
// the one a sequence of digests alone would happily accept.
func leftoverLines(chained map[Entity]bool) error {
	for entry := range chained {
		return fmt.Errorf("relations: chain: line %s is chained but was never allocated a line id", entry)
	}
	return nil
}

// signatureChecker returns a Verify that caches each author's public key,
// so a whole-book walk reads an actor's declaration once rather than
// once per record.
func (j *Journal) signatureChecker(ctx context.Context) func(Relation) error {
	keys := make(map[Entity]ed25519.PublicKey)
	return func(rel Relation) error {
		pub, ok := keys[rel.Record.Author]
		if !ok {
			actor, found, err := j.st.Declaration(ctx, rel.Record.Author)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("author %s is not declared", rel.Record.Author)
			}
			pub = actor.Record.Data
			keys[rel.Record.Author] = pub
		}
		return rel.Record.Verify(rel.key, rel.unsigned, pub)
	}
}
