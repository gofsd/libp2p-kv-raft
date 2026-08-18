package relations_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// newActor returns a Store writing into be under its own declared
// identity -- a second person at a second device, which is what a
// countersignature is by definition.
func newActor(t *testing.T, be relations.Backend, id uint8, name string) (*relations.Store, relations.Entity) {
	t.Helper()
	pub, priv := newKey(t)
	actor := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: id}
	st := relations.New(be, testLog, actor, priv)
	if err := st.DeclareActor(context.Background(), actor, name, pub); err != nil {
		t.Fatalf("DeclareActor(%s): %v", name, err)
	}
	return st, actor
}

// TestCountersignAddsASecondSignature is the operator-signs,
// supervisor-countersigns control every regulated log book has.
func TestCountersignAddsASecondSignature(t *testing.T) {
	ctx := context.Background()
	be := relations.Memory()
	operatorStore, operator := newActor(t, be, 1, "Ivanova")
	supervisorStore, supervisor := newActor(t, be, 2, "Petrov")

	operatorJournal := relations.NewJournal(operatorStore)
	entries, _ := writeShiftLog(t, operatorJournal)
	line := entries[0]

	supervisorJournal := relations.NewJournal(supervisorStore)
	if err := supervisorJournal.Countersign(ctx, line); err != nil {
		t.Fatalf("Countersign: %v", err)
	}

	signatures, err := operatorJournal.Countersignatures(ctx, line)
	if err != nil {
		t.Fatalf("Countersignatures: %v", err)
	}
	if len(signatures) != 1 {
		t.Fatalf("line has %d countersignatures, want 1", len(signatures))
	}
	if signatures[0].Actor != supervisor || signatures[0].Name != "Petrov" {
		t.Fatalf("countersigned by %s (%q), want %s (Petrov)", signatures[0].Actor, signatures[0].Name, supervisor)
	}
	if signatures[0].At.IsZero() {
		t.Fatal("the countersignature has no timestamp")
	}

	// The endorsement is the supervisor's own signature, verifiable
	// against the key the supervisor declared -- not a note the operator
	// wrote about them.
	rel, found, err := operatorStore.Lookup(ctx, line, supervisor)
	if err != nil || !found {
		t.Fatalf("Lookup(countersignature) = %v, %v", found, err)
	}
	if rel.Record.Author != supervisor {
		t.Fatalf("the countersignature is authored by %s, want %s", rel.Record.Author, supervisor)
	}
	if err := operatorStore.Verify(ctx, rel); err != nil {
		t.Fatalf("Verify(countersignature): %v", err)
	}
	if operator == supervisor {
		t.Fatal("the test set up one actor, not two")
	}

	// Several people may endorse one line; nobody may endorse it twice.
	inspectorJournal, _ := newActor(t, be, 3, "Kim")
	if err := relations.NewJournal(inspectorJournal).Countersign(ctx, line); err != nil {
		t.Fatalf("second countersigner: %v", err)
	}
	if signatures, err = operatorJournal.Countersignatures(ctx, line); err != nil {
		t.Fatalf("Countersignatures: %v", err)
	} else if len(signatures) != 2 {
		t.Fatalf("line has %d countersignatures, want 2", len(signatures))
	}
	if err := supervisorJournal.Countersign(ctx, line); !errors.Is(err, relations.ErrAlreadyCountersigned) {
		t.Fatalf("second endorsement by the same actor = %v, want ErrAlreadyCountersigned", err)
	}

	// And the book still adds up: countersigning is an event of its own,
	// so it neither disturbs the line's digest nor goes unrecorded.
	checked, err := operatorJournal.VerifyChain(ctx)
	if err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
	if checked != len(entries)+2 {
		t.Fatalf("verified %d events, want %d (%d lines and 2 countersignatures)", checked, len(entries)+2, len(entries))
	}
}

func TestCountersignRefusals(t *testing.T) {
	ctx := context.Background()
	be := relations.Memory()
	operatorStore, _ := newActor(t, be, 1, "Ivanova")
	supervisorStore, _ := newActor(t, be, 2, "Petrov")

	operator := relations.NewJournal(operatorStore)
	supervisor := relations.NewJournal(supervisorStore)
	entries, fields := writeShiftLog(t, operator)

	// Your own signature is already on the line; a second one from the
	// same actor endorses nothing.
	if err := operator.Countersign(ctx, entries[0]); err == nil {
		t.Fatal("expected an error countersigning one's own line")
	}

	// A line that no longer stands cannot be endorsed.
	if err := operator.Void(ctx, entries[1], relations.Zero); err != nil {
		t.Fatalf("Void: %v", err)
	}
	if err := supervisor.Countersign(ctx, entries[1]); !errors.Is(err, relations.ErrAlreadyStruck) {
		t.Fatalf("countersigning a voided line = %v, want ErrAlreadyStruck", err)
	}

	// Neither is a line something else already is.
	if err := supervisor.Countersign(ctx, fields[fieldShift]); err == nil {
		t.Fatal("expected an error countersigning a column")
	}
	unwritten := relations.Entity{Log: testLog, Page: relations.FirstEntryPage, Type: relations.TypeEntry, ID: 0xFE}
	if err := supervisor.Countersign(ctx, unwritten); err == nil {
		t.Fatal("expected an error countersigning a line that was never written")
	}

	// An actor nobody can check is not an endorsement.
	strangerStore := relations.New(be, testLog, relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 9}, nil)
	if err := relations.NewJournal(strangerStore).Countersign(ctx, entries[2]); err == nil {
		t.Fatal("expected an error countersigning as an undeclared actor")
	}
}

// TestCountersignIsChained checks the endorsement cannot be quietly
// removed -- the reason it is a chained event rather than just another
// signed record.
func TestCountersignIsChained(t *testing.T) {
	ctx := context.Background()
	be := relations.Memory()
	operatorStore, _ := newActor(t, be, 1, "Ivanova")
	supervisorStore, supervisor := newActor(t, be, 2, "Petrov")

	operator := relations.NewJournal(operatorStore)
	entries, _ := writeShiftLog(t, operator)
	if err := relations.NewJournal(supervisorStore).Countersign(ctx, entries[0]); err != nil {
		t.Fatalf("Countersign: %v", err)
	}

	if err := operatorStore.Unlink(ctx, entries[0], supervisor); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	if signed, err := operator.CountersignedBy(ctx, entries[0], supervisor); err != nil {
		t.Fatalf("CountersignedBy: %v", err)
	} else if signed {
		t.Fatal("the endorsement was not actually removed")
	}

	_, err := operator.VerifyChain(ctx)
	if err == nil {
		t.Fatal("VerifyChain accepted a book with an endorsement removed")
	}
	if !strings.Contains(err.Error(), "countersign") {
		t.Fatalf("error does not say what went missing: %v", err)
	}
}
