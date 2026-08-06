// Package genealogy is a worked example, not part of this project's core
// library: it shows how an application (e.g. an MES built on top of this
// repo) can add unit traceability/genealogy tracking using nothing but
// pkg/kvctl's already-public, already-generic LogAppend/LogQuery -- the
// same primitive AppendCommandLog/QueryCommandLog themselves are built on
// (see pkg/kvctl/dispatch.go). Nothing here needed a daemon/kvfsm/wire
// change to exist, which is exactly why it doesn't live in pkg/kvctl: the
// things that genuinely have to be in the library (Group/Command/Station,
// dispatch ACL) are the ones whose correctness depends on being checked
// inside a raft Apply, where client code cannot forge them. A genealogy
// claim recorded this way is client-asserted, the same trust level
// AppendCommandLog's own entries already have -- good enough for an
// audit/traceability trail an operator reviews, not for anything that
// needs to survive an adversarial writer. Copy this file (or the pattern
// it demonstrates) into your own application rather than importing it as a
// dependency -- its field names, CSV encoding, and role vocabulary are one
// opinionated shape among many a real MES might want, not a contract this
// project maintains.
package genealogy

import (
	"fmt"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// logKind is the fixed pkg/logrecord Kind every Record entry is stored
// under, keyed by the traceable unit id itself (not an instance id the way
// AppendCommandLog's "cmdlog" kind is) -- see logrecord.BuildKey's (kind,
// unitID, timestamp) key scheme. Keying by unit id, not instance id, is
// what makes Query(unitID) a single prefix scan instead of needing a
// separate unit->instance index: everything Record ever wrote naming
// unitID -- as either an input or an output of some execution -- is
// already right there under its own key range.
const logKind = "genealogy-example"

// roleInput/roleOutput are an Event's Role values: which side of the
// transformation the entry's own UnitID played in the execution named by
// InstanceID.
const (
	roleInput  = "input"
	roleOutput = "output"
)

// fieldInstanceID/fieldRole/fieldRelated are the reserved
// pkg/logrecord.Record.Fields keys Record writes and Event parses back out.
// fieldRelated holds the *other* side of the transformation from this
// entry's own UnitID: for a roleOutput entry, the input units that
// produced it; for a roleInput entry, the output units it was consumed
// into. This single field, read in light of Role, is what gives Query both
// directions (who made this unit, and what this unit became) from one scan
// -- see Event's doc comment.
const (
	fieldInstanceID = "instance_id"
	fieldRole       = "role"
	fieldRelated    = "related_units"
)

// Event is one Record entry as Query/Ancestors/Descendants read it back:
// UnitID played Role in the execution InstanceID, and RelatedUnits names
// the other side of that transformation -- UnitID's immediate parents if
// Role is "output", or the units UnitID was consumed into if Role is
// "input".
type Event struct {
	UnitID       string            `json:"unit_id"`
	InstanceID   string            `json:"instance_id"`
	Role         string            `json:"role"`
	RelatedUnits []string          `json:"related_units,omitempty"`
	Fields       map[string]string `json:"fields,omitempty"`
	Narrative    string            `json:"narrative,omitempty"`
	RecordedBy   string            `json:"recorded_by"`
	Timestamp    time.Time         `json:"timestamp"`
}

// joinUnitIDs/splitUnitIDs pack a []string of unit ids into fieldRelated's
// single comma-separated field value and back. Comma is disallowed in a
// unit id (validateUnitID) specifically so this round-trips unambiguously
// with no escaping needed.
func joinUnitIDs(ids []string) string {
	return strings.Join(ids, ",")
}

func splitUnitIDs(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	ids := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			ids = append(ids, p)
		}
	}
	return ids
}

// validateUnitID rejects an empty id or one containing a comma -- see
// joinUnitIDs/splitUnitIDs.
func validateUnitID(id string) error {
	if id == "" {
		return fmt.Errorf("genealogy: unit id must not be empty")
	}
	if strings.Contains(id, ",") {
		return fmt.Errorf("genealogy: unit id %q must not contain a comma", id)
	}
	return nil
}

// Record links instanceID's execution to every unit it touched: inputUnits
// were consumed, outputUnits were produced. It writes one kvctl.LogAppend
// entry per unit named in either slice (an output-role entry under each
// output unit's own id, naming inputUnits as its fieldRelated; an
// input-role entry under each input unit's own id, naming outputUnits) --
// so a later Query(unitID) finds every event unitID ever played either
// role in with one prefix scan, in either direction.
//
// extraFields (may be nil) is merged into every entry written -- e.g. a
// station id, a lot/batch attribute -- alongside the reserved
// fieldInstanceID/fieldRole/fieldRelated keys, which extraFields must not
// set itself (Record overwrites them unconditionally). narrative is stored
// verbatim on every entry.
//
// Requires at least one of inputUnits/outputUnits to be non-empty --
// otherwise there is no transformation to record at all -- and rejects any
// unit id containing a comma (see joinUnitIDs) before writing anything.
//
// Not atomic: each unit gets its own independent kvctl.LogAppend call (the
// same per-key durability pkg/kvctl.SubmitCommand's own sequential writes
// have), so a failure partway through leaves some units' genealogy
// recorded and others not. A caller that needs all-or-nothing genealogy
// for one execution should retry Record with the same instanceID/units on
// error -- every write is naturally idempotent-in-effect (LogAppend never
// overwrites; a retried call just adds another, identically-shaped entry
// nothing dedups against, so prefer only retrying an observed failure, not
// calling this speculatively more than once per real execution).
func Record(instanceID string, inputUnits, outputUnits []string, extraFields map[string]string, narrative string) error {
	if instanceID == "" {
		return fmt.Errorf("genealogy: instance id must not be empty")
	}
	if len(inputUnits) == 0 && len(outputUnits) == 0 {
		return fmt.Errorf("genealogy: record needs at least one input or output unit")
	}
	for _, id := range inputUnits {
		if err := validateUnitID(id); err != nil {
			return err
		}
	}
	for _, id := range outputUnits {
		if err := validateUnitID(id); err != nil {
			return err
		}
	}

	writeOne := func(unitID, role string, related []string) error {
		fields := make(map[string]string, len(extraFields)+3)
		for k, v := range extraFields {
			fields[k] = v
		}
		fields[fieldInstanceID] = instanceID
		fields[fieldRole] = role
		if len(related) > 0 {
			fields[fieldRelated] = joinUnitIDs(related)
		}
		return kvctl.LogAppend(logKind, unitID, fields, narrative)
	}

	for _, unitID := range outputUnits {
		if err := writeOne(unitID, roleOutput, inputUnits); err != nil {
			return fmt.Errorf("genealogy: record: output unit %s: %w", unitID, err)
		}
	}
	for _, unitID := range inputUnits {
		if err := writeOne(unitID, roleInput, outputUnits); err != nil {
			return fmt.Errorf("genealogy: record: input unit %s: %w", unitID, err)
		}
	}
	return nil
}

// decodeEvent converts one raw pkg/logrecord.Record (already known to be
// under logKind) into an Event, splitting out the reserved fields Record
// wrote from whatever extraFields it also carried.
func decodeEvent(rec logrecord.Record) Event {
	fields := make(map[string]string, len(rec.Fields))
	for k, v := range rec.Fields {
		switch k {
		case fieldInstanceID, fieldRole, fieldRelated:
			// reserved -- surfaced through their own Event fields below
		default:
			fields[k] = v
		}
	}
	if len(fields) == 0 {
		fields = nil
	}
	return Event{
		UnitID:       rec.UnitID,
		InstanceID:   rec.Fields[fieldInstanceID],
		Role:         rec.Fields[fieldRole],
		RelatedUnits: splitUnitIDs(rec.Fields[fieldRelated]),
		Fields:       fields,
		Narrative:    rec.Narrative,
		RecordedBy:   rec.AuthorPeerID,
		Timestamp:    rec.Timestamp,
	}
}

// Query returns every Record entry naming unitID -- as either an input or
// an output of whatever execution touched it -- with a timestamp in
// [start, end], oldest first, up to limit records (limit <= 0 means
// unlimited). A thin decode over kvctl.LogQuery(logKind, unitID, ...).
func Query(unitID string, start, end time.Time, limit int) ([]Event, error) {
	if unitID == "" {
		return nil, fmt.Errorf("genealogy: Query: unitID must not be empty")
	}
	recs, err := kvctl.LogQuery(logKind, unitID, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("genealogy: query: %w", err)
	}
	events := make([]Event, len(recs))
	for i, rec := range recs {
		events[i] = decodeEvent(rec)
	}
	return events, nil
}

// allEvents fetches unitID's full genealogy history (no time bound) --
// Ancestors/Descendants' own building block, same "no reverse-scan
// primitive, pay for a full walk" tradeoff pkg/kvctl.LatestCommandLog's own
// doc comment already accepts for this stack.
func allEvents(unitID string) ([]Event, error) {
	return Query(unitID, time.Unix(0, 0), time.Now(), 0)
}

// defaultTraceDepth bounds Ancestors/Descendants when maxDepth<=0 is passed
// -- generous for any real assembly tree, while still bounding the walk
// against a cyclic or unexpectedly deep unit graph a caller passed by
// mistake (see maxTraceDepth's own comment).
const defaultTraceDepth = 50

// maxTraceDepth normalizes a caller-supplied maxDepth: <=0 becomes
// defaultTraceDepth, anything larger is left alone (a caller that
// genuinely wants to walk deeper than the default may).
func maxTraceDepth(maxDepth int) int {
	if maxDepth <= 0 {
		return defaultTraceDepth
	}
	return maxDepth
}

// trace is Ancestors/Descendants' shared BFS: starting from unitID, at
// each step gathers that unit's immediate neighbors in the direction being
// traced (RelatedUnits off every wantRole-matching event), and keeps
// expanding unvisited neighbors up to maxDepth hops. Cycle-safe (a unit
// already seen is never re-expanded, no matter how it's reached) -- real
// genealogy graphs are acyclic by construction, but nothing here assumes a
// caller's data actually is.
//
// There is no server-side recursive query in this KV store (pkg/store
// exposes ordered range scans, not joins), so an N-generation trace
// genuinely costs N rounds of Query, one per unit visited at each depth,
// not a single query.
func trace(unitID string, maxDepth int, wantRole string) ([]string, error) {
	if unitID == "" {
		return nil, fmt.Errorf("genealogy: trace: unitID must not be empty")
	}
	maxDepth = maxTraceDepth(maxDepth)

	visited := map[string]bool{unitID: true}
	var result []string
	frontier := []string{unitID}

	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []string
		for _, id := range frontier {
			events, err := allEvents(id)
			if err != nil {
				return nil, fmt.Errorf("genealogy: trace: %s: %w", id, err)
			}
			for _, ev := range events {
				if ev.Role != wantRole {
					continue
				}
				for _, related := range ev.RelatedUnits {
					if visited[related] {
						continue
					}
					visited[related] = true
					result = append(result, related)
					next = append(next, related)
				}
			}
		}
		frontier = next
	}
	return result, nil
}

// Ancestors returns every unit that fed, directly or transitively (up to
// maxDepth hops, <=0 meaning defaultTraceDepth), into unitID -- walking
// roleOutput events' RelatedUnits (the inputs that produced each unit
// visited) outward from unitID. Order is breadth-first (nearest ancestors
// first) but otherwise unspecified among units at the same depth; unitID
// itself is never included.
func Ancestors(unitID string, maxDepth int) ([]string, error) {
	return trace(unitID, maxDepth, roleOutput)
}

// Descendants is Ancestors' mirror image -- every unit that unitID fed
// into, directly or transitively, walking roleInput events' RelatedUnits
// (the outputs each unit visited was consumed into) outward from unitID.
func Descendants(unitID string, maxDepth int) ([]string, error) {
	return trace(unitID, maxDepth, roleInput)
}
