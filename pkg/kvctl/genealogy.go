package kvctl

import (
	"fmt"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// This file is traceability/genealogy's first cut, deliberately built on
// top of LogAppend/LogQuery (the same generic pkg/logrecord primitive
// AppendCommandLog/QueryCommandLog already use) rather than a new
// SystemKeyPrefix record kind: no daemon/kvfsm change, no new wire schema,
// no new mage-level ACL. A genealogy claim is therefore only as trustworthy
// as AppendCommandLog's own entries are today -- client-asserted, not
// raft-validated against the CommandExecution it claims to belong to. If
// that stops being good enough (e.g. once a caller needs to *trust*
// genealogy from a peer it doesn't otherwise trust), the natural next step
// is a real kvfsm.OpRecordGenealogy Apply case that checks the claim
// against an actual CommandExecution before committing it -- the same
// shape of work KindStation/KindGroupCommand were, not attempted here.

// genealogyLogKind is the fixed pkg/logrecord Kind every RecordGenealogy
// entry is stored under, keyed by the traceable unit id itself (not an
// instance id the way cmdlog is) -- see logrecord.BuildKey's (kind, unitID,
// timestamp) key scheme. Keying by unit id, not instance id, is what makes
// QueryGenealogy(unitID) a single prefix scan instead of needing a separate
// unit->instance index: everything RecordGenealogy ever wrote naming
// unitID -- as either an input or an output of some execution -- is
// already right there under its own key range.
const genealogyLogKind = "genealogy"

// genealogyRoleInput/genealogyRoleOutput are a GenealogyEvent's Role
// values: which side of the transformation the entry's own UnitID played
// in the execution named by InstanceID.
const (
	genealogyRoleInput  = "input"
	genealogyRoleOutput = "output"
)

// genealogyFieldInstanceID/genealogyFieldRole/genealogyFieldRelated are the
// reserved pkg/logrecord.Record.Fields keys RecordGenealogy writes and
// GenealogyEvent parses back out. genealogyFieldRelated holds the *other*
// side of the transformation from this entry's own UnitID: for a
// genealogyRoleOutput entry, the input units that produced it; for a
// genealogyRoleInput entry, the output units it was consumed into. This
// single field, read in light of Role, is what gives QueryGenealogy both
// directions (who made this unit, and what this unit became) from one scan
// -- see GenealogyEvent's doc comment.
const (
	genealogyFieldInstanceID = "instance_id"
	genealogyFieldRole       = "role"
	genealogyFieldRelated    = "related_units"
)

// GenealogyEvent is one RecordGenealogy entry as QueryGenealogy/Ancestors/
// Descendants read it back: UnitID played Role in the execution InstanceID,
// and RelatedUnits names the other side of that transformation -- UnitID's
// immediate parents if Role is "output", or the units UnitID was consumed
// into if Role is "input".
type GenealogyEvent struct {
	UnitID       string            `json:"unit_id"`
	InstanceID   string            `json:"instance_id"`
	Role         string            `json:"role"`
	RelatedUnits []string          `json:"related_units,omitempty"`
	Fields       map[string]string `json:"fields,omitempty"`
	Narrative    string            `json:"narrative,omitempty"`
	RecordedBy   string            `json:"recorded_by"`
	Timestamp    time.Time         `json:"timestamp"`
}

// joinUnitIDs/splitUnitIDs pack a []string of unit ids into
// genealogyFieldRelated's single comma-separated field value and back.
// Comma is disallowed in a unit id (validateGenealogyUnitID) specifically
// so this round-trips unambiguously with no escaping needed.
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

// validateGenealogyUnitID rejects an empty id or one containing a comma --
// see joinUnitIDs/splitUnitIDs.
func validateGenealogyUnitID(id string) error {
	if id == "" {
		return fmt.Errorf("kvctl: genealogy unit id must not be empty")
	}
	if strings.Contains(id, ",") {
		return fmt.Errorf("kvctl: genealogy unit id %q must not contain a comma", id)
	}
	return nil
}

// RecordGenealogy links instanceID's execution to every unit it touched:
// inputUnits were consumed, outputUnits were produced. It writes one
// LogAppend entry per unit named in either slice (an output-role entry
// under each output unit's own id, naming inputUnits as its
// genealogyFieldRelated; an input-role entry under each input unit's own
// id, naming outputUnits) -- so a later QueryGenealogy(unitID) finds every
// event unitID ever played either role in with one prefix scan, in either
// direction.
//
// extraFields (may be nil) is merged into every entry written -- e.g. a
// station id, a lot/batch attribute -- alongside the reserved
// genealogyFieldInstanceID/genealogyFieldRole/genealogyFieldRelated keys,
// which extraFields must not set itself (RecordGenealogy overwrites them
// unconditionally). narrative is stored verbatim on every entry.
//
// Requires at least one of inputUnits/outputUnits to be non-empty --
// otherwise there is no transformation to record at all -- and rejects any
// unit id containing a comma (see joinUnitIDs) before writing anything.
//
// Not atomic: each unit gets its own independent LogAppend call (the same
// per-key durability every other multi-write kvctl operation in this
// package has -- see SubmitCommand's own sequential
// commandRequestLogKind/commandExecIndexKind writes), so a failure partway
// through leaves some units' genealogy recorded and others not. A caller
// that needs all-or-nothing genealogy for one execution should retry
// RecordGenealogy with the same instanceID/units on error -- every write is
// naturally idempotent-in-effect (LogAppend never overwrites; a retried
// call just adds another, identically-shaped entry for the same instant a
// query already dedups nothing against, so prefer only retrying an
// observed failure, not calling this speculatively more than once per real
// execution).
func RecordGenealogy(instanceID string, inputUnits, outputUnits []string, extraFields map[string]string, narrative string) error {
	if instanceID == "" {
		return fmt.Errorf("kvctl: genealogy instance id must not be empty")
	}
	if len(inputUnits) == 0 && len(outputUnits) == 0 {
		return fmt.Errorf("kvctl: genealogy record needs at least one input or output unit")
	}
	for _, id := range inputUnits {
		if err := validateGenealogyUnitID(id); err != nil {
			return err
		}
	}
	for _, id := range outputUnits {
		if err := validateGenealogyUnitID(id); err != nil {
			return err
		}
	}

	writeOne := func(unitID, role string, related []string) error {
		fields := make(map[string]string, len(extraFields)+3)
		for k, v := range extraFields {
			fields[k] = v
		}
		fields[genealogyFieldInstanceID] = instanceID
		fields[genealogyFieldRole] = role
		if len(related) > 0 {
			fields[genealogyFieldRelated] = joinUnitIDs(related)
		}
		return LogAppend(genealogyLogKind, unitID, fields, narrative)
	}

	for _, unitID := range outputUnits {
		if err := writeOne(unitID, genealogyRoleOutput, inputUnits); err != nil {
			return fmt.Errorf("kvctl: record genealogy: output unit %s: %w", unitID, err)
		}
	}
	for _, unitID := range inputUnits {
		if err := writeOne(unitID, genealogyRoleInput, outputUnits); err != nil {
			return fmt.Errorf("kvctl: record genealogy: input unit %s: %w", unitID, err)
		}
	}
	return nil
}

// decodeGenealogyEvent converts one raw pkg/logrecord.Record (already known
// to be under genealogyLogKind) into a GenealogyEvent, splitting out the
// reserved fields RecordGenealogy wrote from whatever extraFields it also
// carried.
func decodeGenealogyEvent(rec logrecord.Record) GenealogyEvent {
	fields := make(map[string]string, len(rec.Fields))
	for k, v := range rec.Fields {
		switch k {
		case genealogyFieldInstanceID, genealogyFieldRole, genealogyFieldRelated:
			// reserved -- surfaced through their own GenealogyEvent fields below
		default:
			fields[k] = v
		}
	}
	if len(fields) == 0 {
		fields = nil
	}
	return GenealogyEvent{
		UnitID:       rec.UnitID,
		InstanceID:   rec.Fields[genealogyFieldInstanceID],
		Role:         rec.Fields[genealogyFieldRole],
		RelatedUnits: splitUnitIDs(rec.Fields[genealogyFieldRelated]),
		Fields:       fields,
		Narrative:    rec.Narrative,
		RecordedBy:   rec.AuthorPeerID,
		Timestamp:    rec.Timestamp,
	}
}

// QueryGenealogy implements `mage querygenealogy <unitID> <since> <until>
// <limit>`: returns every RecordGenealogy entry naming unitID -- as either
// an input or an output of whatever execution touched it -- with a
// timestamp in [start, end], oldest first, up to limit records (limit <= 0
// means unlimited). A thin decode over LogQuery(genealogyLogKind, unitID,
// ...), the same relationship QueryCommandLog has to LogQuery.
func QueryGenealogy(unitID string, start, end time.Time, limit int) ([]GenealogyEvent, error) {
	if unitID == "" {
		return nil, fmt.Errorf("kvctl: QueryGenealogy: unitID must not be empty")
	}
	recs, err := LogQuery(genealogyLogKind, unitID, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("kvctl: query genealogy: %w", err)
	}
	events := make([]GenealogyEvent, len(recs))
	for i, rec := range recs {
		events[i] = decodeGenealogyEvent(rec)
	}
	return events, nil
}

// allGenealogyEvents fetches unitID's full genealogy history (no time
// bound) -- Ancestors/Descendants' own building block, same "no
// reverse-scan primitive, pay for a full walk" tradeoff LatestCommandLog's
// doc comment already accepts for this stack.
func allGenealogyEvents(unitID string) ([]GenealogyEvent, error) {
	return QueryGenealogy(unitID, time.Unix(0, 0), time.Now(), 0)
}

// defaultGenealogyTraceDepth bounds Ancestors/Descendants when maxDepth<=0
// is passed -- generous for any real assembly tree, while still bounding
// the walk against a cyclic or unexpectedly deep unit graph a caller passed
// by mistake (see maxTraceDepth's own comment).
const defaultGenealogyTraceDepth = 50

// maxTraceDepth normalizes a caller-supplied maxDepth: <=0 becomes
// defaultGenealogyTraceDepth, anything larger is left alone (a caller that
// genuinely wants to walk deeper than the default may).
func maxTraceDepth(maxDepth int) int {
	if maxDepth <= 0 {
		return defaultGenealogyTraceDepth
	}
	return maxDepth
}

// traceGenealogy is Ancestors/Descendants' shared BFS: starting from
// unitID, at each step calls next(unit) to get that unit's immediate
// neighbors in the direction being traced (RelatedUnits off every
// wantRole-matching event), and keeps expanding unvisited neighbors up to
// maxDepth hops. Cycle-safe (a unit already seen is never re-expanded, no
// matter how it's reached) -- real genealogy graphs are acyclic by
// construction, but nothing here assumes a caller's data actually is.
//
// There is no server-side recursive query in this KV store (pkg/store
// exposes ordered range scans, not joins -- see pkg/store.Store's own
// interface), so an N-generation trace genuinely costs N rounds of
// QueryGenealogy, one per unit visited at each depth, not a single query.
func traceGenealogy(unitID string, maxDepth int, wantRole string) ([]string, error) {
	if unitID == "" {
		return nil, fmt.Errorf("kvctl: genealogy trace: unitID must not be empty")
	}
	maxDepth = maxTraceDepth(maxDepth)

	visited := map[string]bool{unitID: true}
	var result []string
	frontier := []string{unitID}

	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []string
		for _, id := range frontier {
			events, err := allGenealogyEvents(id)
			if err != nil {
				return nil, fmt.Errorf("kvctl: genealogy trace: %s: %w", id, err)
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

// Ancestors implements `mage genealogyancestors <unitID> <maxDepth>`:
// returns every unit that fed, directly or transitively (up to maxDepth
// hops, <=0 meaning defaultGenealogyTraceDepth), into unitID -- walking
// genealogyRoleOutput events' RelatedUnits (the inputs that produced each
// unit visited) outward from unitID. Order is breadth-first (nearest
// ancestors first) but otherwise unspecified among units at the same
// depth; unitID itself is never included.
func Ancestors(unitID string, maxDepth int) ([]string, error) {
	return traceGenealogy(unitID, maxDepth, genealogyRoleOutput)
}

// Descendants implements `mage genealogydescendants <unitID> <maxDepth>`:
// Ancestors' mirror image -- every unit that unitID fed into, directly or
// transitively, walking genealogyRoleInput events' RelatedUnits (the
// outputs each unit visited was consumed into) outward from unitID.
func Descendants(unitID string, maxDepth int) ([]string, error) {
	return traceGenealogy(unitID, maxDepth, genealogyRoleInput)
}
