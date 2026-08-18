package relations

import (
	"context"
	"fmt"
	"time"
)

// KindDerivedFrom relates a produced unit to a unit it was made from:
// output -> input. Its mirror -- input -> output, written by the same
// atomic Link -- is what makes tracing forward ("what did this part end
// up in") as cheap as tracing backward ("what was this part made of").
const KindDerivedFrom byte = 0x04

// UnitFieldName/InstanceFieldName are the two columns a Genealogy
// declares in the journal it shares: units and the executions that
// transform them. They are ordinary fields, so a unit id is an ordinary
// dictionary term -- which is the whole difference from
// examples/genealogy, where every entry repeats the unit ids it names as
// text (see that package's fieldRelated).
const (
	UnitFieldName     = "unit"
	InstanceFieldName = "instance"
)

// DefaultTraceDepth bounds Ancestors/Descendants when maxDepth <= 0,
// matching examples/genealogy's own default: generous for any real
// assembly tree, while still bounding a walk over data that turned out
// to be deeper or more cyclic than the caller expected.
const DefaultTraceDepth = 50

// Genealogy is examples/genealogy's model -- units, executions,
// ancestors, descendants -- expressed as relations rather than as log
// records, and answering the same questions with the same semantics:
// Record links inputs to outputs, Ancestors walks the derived-from edges
// backward and Descendants forward, both breadth-first, cycle-safe, and
// bounded by DefaultTraceDepth.
//
// What it does not share is the storage. examples/genealogy writes one
// pkg/logrecord entry per unit per execution, each repeating the unit
// ids on the other side of the transformation as a comma-joined string,
// and traces by re-reading every entry of every unit it visits. Here a
// unit id is interned once as a dictionary term and each transformation
// is one 9-byte edge (plus its 9-byte mirror), so a trace step is a
// prefix scan of one entity's edges rather than a scan-and-parse of its
// whole history -- and the same store is queryable as an ordinary
// journal, because it is one.
//
// Both models are client-asserted at the same trust level: a signature
// here says which declared actor wrote a record, not that the claim in
// it is true.
type Genealogy struct {
	j             *Journal
	unitField     Entity
	instanceField Entity
}

// NewGenealogy prepares (declaring them if needed) the unit and instance
// fields inside j, and returns a Genealogy writing through it. Because
// it is the same journal, genealogy edges and ordinary log entries
// coexist in one store and can reference the same terms -- the operator
// who signed a line and the unit that line was about are entities in the
// same space.
func NewGenealogy(ctx context.Context, j *Journal) (*Genealogy, error) {
	unitField, err := j.Field(ctx, UnitFieldName)
	if err != nil {
		return nil, err
	}
	instanceField, err := j.Field(ctx, InstanceFieldName)
	if err != nil {
		return nil, err
	}
	return &Genealogy{j: j, unitField: unitField, instanceField: instanceField}, nil
}

// Unit returns the entity standing for unit id, interning it on first
// use. Two mentions of the same unit id anywhere in the store are the
// same four bytes.
func (g *Genealogy) Unit(ctx context.Context, id string) (Entity, error) {
	if id == "" {
		return Zero, fmt.Errorf("relations: genealogy: unit id must not be empty")
	}
	return g.j.Term(ctx, g.unitField, id)
}

// Record links the execution instanceID's inputs to its outputs: one
// output -> input edge per pair, each carrying the instance's entity in
// its payload, all in a single atomic Apply. Unlike
// examples/genealogy.Record -- which writes one independent log entry
// per unit and documents that a failure partway through leaves some
// units recorded and others not -- either the whole transformation is
// recorded or none of it is.
//
// Requires at least one of inputs/outputs (otherwise there is no
// transformation), and tolerates a one-sided call: with no inputs or no
// outputs there are no edges to write, and the units named are still
// interned so later executions can reference them.
func (g *Genealogy) Record(ctx context.Context, instanceID string, inputs, outputs []string) error {
	if instanceID == "" {
		return fmt.Errorf("relations: genealogy: instance id must not be empty")
	}
	if len(inputs) == 0 && len(outputs) == 0 {
		return fmt.Errorf("relations: genealogy: record needs at least one input or output unit")
	}

	instance, err := g.j.Term(ctx, g.instanceField, instanceID)
	if err != nil {
		return err
	}
	instanceRef := instance.Bytes()

	inputEntities, err := g.units(ctx, inputs)
	if err != nil {
		return err
	}
	outputEntities, err := g.units(ctx, outputs)
	if err != nil {
		return err
	}

	var ops []Op
	for _, out := range outputEntities {
		for _, in := range inputEntities {
			if out == in {
				return fmt.Errorf("relations: genealogy: unit %s is both an input and an output of %s", in, instanceID)
			}
			edge, err := g.j.Store().LinkOps(out, in, KindDerivedFrom, instanceRef[:])
			if err != nil {
				return err
			}
			ops = append(ops, edge...)
		}
	}
	if len(ops) == 0 {
		return nil
	}
	return g.j.Store().Backend().Apply(ctx, ops)
}

// units interns every id in ids, preserving order.
func (g *Genealogy) units(ctx context.Context, ids []string) ([]Entity, error) {
	out := make([]Entity, 0, len(ids))
	for _, id := range ids {
		e, err := g.Unit(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// Edge is one recorded transformation as read back: Output was produced
// from Input by the execution Instance, recorded by Author at At.
type Edge struct {
	Output   string
	Input    string
	Instance string
	Author   Entity
	At       time.Time
}

// Edges returns every transformation unitID took part in, on either
// side: the ones that produced it (Output == unitID) and the ones it was
// consumed into (Input == unitID). Two prefix scans, one per direction.
func (g *Genealogy) Edges(ctx context.Context, unitID string) ([]Edge, error) {
	unit, err := g.Unit(ctx, unitID)
	if err != nil {
		return nil, err
	}
	produced, err := g.j.Store().Relations(ctx, unit)
	if err != nil {
		return nil, err
	}
	consumed, err := g.j.Store().Backlinks(ctx, unit)
	if err != nil {
		return nil, err
	}

	var edges []Edge
	for _, rel := range append(OfKind(produced, KindDerivedFrom), OfKind(consumed, KindDerivedFrom)...) {
		out, err := g.text(ctx, rel.A)
		if err != nil {
			return nil, err
		}
		in, err := g.text(ctx, rel.B)
		if err != nil {
			return nil, err
		}
		instance, err := g.instanceOf(ctx, rel)
		if err != nil {
			return nil, err
		}
		edges = append(edges, Edge{
			Output:   out,
			Input:    in,
			Instance: instance,
			Author:   rel.Record.Author,
			At:       rel.Record.Created,
		})
	}
	return edges, nil
}

// Ancestors returns every unit that fed, directly or transitively (up to
// maxDepth hops, <= 0 meaning DefaultTraceDepth), into unitID. Breadth
// first, nearest first, unitID itself never included -- the same
// contract examples/genealogy.Ancestors documents.
func (g *Genealogy) Ancestors(ctx context.Context, unitID string, maxDepth int) ([]string, error) {
	return g.trace(ctx, unitID, maxDepth, false)
}

// Descendants is Ancestors' mirror: every unit unitID fed into, directly
// or transitively.
func (g *Genealogy) Descendants(ctx context.Context, unitID string, maxDepth int) ([]string, error) {
	return g.trace(ctx, unitID, maxDepth, true)
}

// trace is the shared breadth-first walk. forward=false follows
// derived-from edges out of a unit (its inputs, i.e. its parents);
// forward=true follows their mirrors (the units it was consumed into).
// Visiting is idempotent, so a cycle in data that should not have one
// terminates rather than looping.
func (g *Genealogy) trace(ctx context.Context, unitID string, maxDepth int, forward bool) ([]string, error) {
	start, err := g.Unit(ctx, unitID)
	if err != nil {
		return nil, err
	}
	if maxDepth <= 0 {
		maxDepth = DefaultTraceDepth
	}

	visited := map[Entity]bool{start: true}
	frontier := []Entity{start}
	var result []string

	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []Entity
		for _, unit := range frontier {
			var (
				rels []Relation
				err  error
			)
			if forward {
				rels, err = g.j.Store().Backlinks(ctx, unit)
			} else {
				rels, err = g.j.Store().Relations(ctx, unit)
			}
			if err != nil {
				return nil, err
			}
			for _, rel := range OfKind(rels, KindDerivedFrom) {
				neighbor := rel.B
				if forward {
					neighbor = rel.A
				}
				if visited[neighbor] {
					continue
				}
				visited[neighbor] = true
				text, err := g.text(ctx, neighbor)
				if err != nil {
					return nil, err
				}
				result = append(result, text)
				next = append(next, neighbor)
			}
		}
		frontier = next
	}
	return result, nil
}

// text resolves a unit entity back to the id it was interned from.
func (g *Genealogy) text(ctx context.Context, unit Entity) (string, error) {
	info, err := g.j.resolveTerm(ctx, unit)
	if err != nil {
		return "", err
	}
	return info.Text, nil
}

// instanceOf reads the execution reference an edge carries in its
// payload.
func (g *Genealogy) instanceOf(ctx context.Context, rel Relation) (string, error) {
	if len(rel.Record.Data) != EntityLen {
		return "", fmt.Errorf("relations: genealogy: edge %s -> %s carries a %d-byte instance ref, want %d",
			rel.A, rel.B, len(rel.Record.Data), EntityLen)
	}
	instance, err := DecodeEntity(rel.Record.Data)
	if err != nil {
		return "", err
	}
	return g.text(ctx, instance)
}
