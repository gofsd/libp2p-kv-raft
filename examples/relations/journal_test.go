package relations_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
)

// The paper original this test replaces -- a machine shop's shift log
// book, one line per job, the same six columns on every page:
//
//	| shift | operator | machine  | operation | result | pieces |
//	| Day   | Ivanova  | Lathe-2  | Turning   | OK     | 120    |
//	| Day   | Ivanova  | Lathe-2  | Turning   | Scrap  | 3      |
//	| Night | Petrov   | Mill-1   | Milling   | OK     | 80     |
//
// On paper every one of those words is written out by hand on every
// line: three lines repeat "Day"/"Ivanova"/"Lathe-2" twice over, and a
// month of them repeats each hundreds of times. That is what makes a
// paper log both bulky and unsearchable -- "every job Ivanova ran on
// Lathe-2" means reading the book -- and it is what this package's
// dictionary discipline removes.
const (
	fieldShift     = "shift"
	fieldOperator  = "operator"
	fieldMachine   = "machine"
	fieldOperation = "operation"
	fieldResult    = "result"
	fieldPieces    = "pieces"
)

type logLine struct {
	shift     string
	operator  string
	machine   string
	operation string
	result    string
	pieces    int64
}

var shiftLog = []logLine{
	{shift: "Day", operator: "Ivanova", machine: "Lathe-2", operation: "Turning", result: "OK", pieces: 120},
	{shift: "Day", operator: "Ivanova", machine: "Lathe-2", operation: "Turning", result: "Scrap", pieces: 3},
	{shift: "Night", operator: "Petrov", machine: "Mill-1", operation: "Milling", result: "OK", pieces: 80},
}

// writeShiftLog fills j with the three lines above and returns the
// entries in the order they were written, plus the field entities by
// name.
func writeShiftLog(t *testing.T, j *relations.Journal) ([]relations.Entity, map[string]relations.Entity) {
	t.Helper()
	ctx := context.Background()

	// The columns say what they hold, so a form can be generated from the
	// schema and a cell of the wrong kind is refused rather than written.
	columns := []struct {
		name  string
		input relations.InputKind
	}{
		{fieldShift, relations.InputTerm},
		{fieldOperator, relations.InputTerm},
		{fieldMachine, relations.InputTerm},
		{fieldOperation, relations.InputTerm},
		{fieldResult, relations.InputTerm},
		{fieldPieces, relations.InputNumber},
	}
	fields := make(map[string]relations.Entity)
	for _, column := range columns {
		e, err := j.DefineField(ctx, column.name, column.input)
		if err != nil {
			t.Fatalf("DefineField(%s): %v", column.name, err)
		}
		fields[column.name] = e
	}

	var entries []relations.Entity
	for i, line := range shiftLog {
		entry, err := j.Append(ctx,
			relations.TermCell(fields[fieldShift], line.shift),
			relations.TermCell(fields[fieldOperator], line.operator),
			relations.TermCell(fields[fieldMachine], line.machine),
			relations.TermCell(fields[fieldOperation], line.operation),
			relations.TermCell(fields[fieldResult], line.result),
			relations.QuantityCell(fields[fieldPieces], line.pieces),
		)
		if err != nil {
			t.Fatalf("Append line %d: %v", i+1, err)
		}
		entries = append(entries, entry)
	}
	return entries, fields
}

// TestPaperLogReplacement is the whole point of the example: the three
// lines above go in, and every question an operator, an auditor or an
// inspector would ask of the paper book comes back out.
func TestPaperLogReplacement(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, fields := writeShiftLog(t, j)

	// Lines are numbered: page 1 of the store is page 1 of the book, and
	// the ids are the line numbers on it.
	for i, entry := range entries {
		if entry.Page != relations.FirstEntryPage || int(entry.ID) != i+1 {
			t.Fatalf("line %d landed at %s, want page %d line %d", i+1, entry, relations.FirstEntryPage, i+1)
		}
	}

	// Reading a line back gives the columns with their headings, exactly
	// as written.
	row, err := j.Row(ctx, entries[1])
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	got := make(map[string]string, len(row))
	for _, cell := range row {
		if cell.Numeric {
			got[cell.FieldName] = fmt.Sprint(cell.Number)
			continue
		}
		got[cell.FieldName] = cell.Text
	}
	want := map[string]string{
		fieldShift: "Day", fieldOperator: "Ivanova", fieldMachine: "Lathe-2",
		fieldOperation: "Turning", fieldResult: "Scrap", fieldPieces: "3",
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("line 2 column %s = %q, want %q (full row: %v)", k, got[k], v, got)
		}
	}
	if len(row) != len(want) {
		t.Fatalf("line 2 has %d columns, want %d", len(row), len(want))
	}

	// The question a paper log cannot answer without reading the book:
	// every line Ivanova signed for. One prefix scan of the index
	// namespace.
	operatorTerm, err := j.Term(ctx, fields[fieldOperator], "Ivanova")
	if err != nil {
		t.Fatalf("Term: %v", err)
	}
	withIvanova, err := j.EntriesWith(ctx, operatorTerm)
	if err != nil {
		t.Fatalf("EntriesWith: %v", err)
	}
	if len(withIvanova) != 2 || withIvanova[0] != entries[0] || withIvanova[1] != entries[1] {
		t.Fatalf("EntriesWith(Ivanova) = %v, want the first two lines %v", withIvanova, entries[:2])
	}

	// The column's admissible values, which on paper exist only in
	// somebody's head.
	vocab, err := j.Vocabulary(ctx, fields[fieldOperator])
	if err != nil {
		t.Fatalf("Vocabulary: %v", err)
	}
	var names []string
	for _, term := range vocab {
		names = append(names, term.Text)
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "Ivanova" || names[1] != "Petrov" {
		t.Fatalf("operator vocabulary = %v, want [Ivanova Petrov]", names)
	}

	// The page, in line order.
	page, err := j.Page(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(page) != len(entries) {
		t.Fatalf("page 1 has %d lines, want %d", len(page), len(entries))
	}
	for i := range page {
		if page[i] != entries[i] {
			t.Fatalf("page 1 line %d = %s, want %s", i+1, page[i], entries[i])
		}
	}

	// Every column heading is declared, so the book describes itself.
	declared, err := j.Fields(ctx)
	if err != nil {
		t.Fatalf("Fields: %v", err)
	}
	if len(declared) != len(fields) {
		t.Fatalf("declared %d fields, want %d", len(declared), len(fields))
	}

	t.Logf("the whole book, as stored:\n%s", dumpStore(t, st))
}

// TestEveryValueIsStoredExactlyOnce is the "from dictionaries, to
// prevent copy values" requirement, checked the only way that really
// counts: by sweeping every byte in the store and confirming each piece
// of text occurs in exactly one record, no matter how many lines use it.
func TestEveryValueIsStoredExactlyOnce(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, _ := writeShiftLog(t, j)

	pairs := scanAll(t, st)

	// Every distinct text in the fixture -- column headings and values
	// alike -- must appear in exactly one stored value, even though
	// "Day", "Ivanova", "Lathe-2" and "Turning" are each used on two
	// lines and "OK" on two more.
	var texts []string
	texts = append(texts, fieldShift, fieldOperator, fieldMachine, fieldOperation, fieldResult, fieldPieces)
	for _, line := range shiftLog {
		texts = append(texts, line.shift, line.operator, line.machine, line.operation, line.result)
	}
	for _, text := range texts {
		if n := countRecordsNamed(t, pairs, text); n != 1 {
			t.Fatalf("%q is stored in %d records, want exactly 1 -- a value is being copied", text, n)
		}
	}

	// And the lines themselves hold no text at all: an entry's own
	// declaration is nameless, and each of its cells is a bare 4-byte
	// reference with an empty name.
	for _, entry := range entries {
		decl, found, err := st.Declaration(ctx, entry)
		if err != nil || !found {
			t.Fatalf("Declaration(%s) = %v, %v", entry, found, err)
		}
		if decl.Record.Name != "" {
			t.Fatalf("entry %s carries the text %q; a line should reference terms, not repeat them", entry, decl.Record.Name)
		}
		cells, err := st.Relations(ctx, entry)
		if err != nil {
			t.Fatalf("Relations(%s): %v", entry, err)
		}
		for _, cell := range cells {
			if cell.Record.Name != "" {
				t.Fatalf("cell %s -> %s carries the text %q", cell.A, cell.B, cell.Record.Name)
			}
		}
	}

	// For scale: what the same three lines cost written out as text, and
	// what the references that replaced them cost. The interesting
	// number is not the ratio on three lines (records carry an author,
	// timestamp and 64-byte signature that the paper never had) but that
	// the per-line text cost is a constant 4 bytes per column no matter
	// how long the value is.
	written := 0
	for _, line := range shiftLog {
		written += len(line.shift) + len(line.operator) + len(line.machine) + len(line.operation) + len(line.result)
	}
	t.Logf("three lines: %d bytes of repeated text on paper, %d bytes of references in the store (%d records total)",
		written, len(shiftLog)*5*relations.EntityLen, len(pairs))
}

// TestEveryRecordIsSigned checks the other half of what a paper log's
// signature column is for: not just that a name is written next to the
// line, but that it can be checked afterwards.
func TestEveryRecordIsSigned(t *testing.T) {
	ctx := context.Background()
	st, actor, _ := newStore(t)
	j := relations.NewJournal(st)
	entries, _ := writeShiftLog(t, j)

	checked := 0
	for _, entry := range entries {
		decl, found, err := st.Declaration(ctx, entry)
		if err != nil || !found {
			t.Fatalf("Declaration(%s) = %v, %v", entry, found, err)
		}
		if decl.Record.Author != actor {
			t.Fatalf("entry %s authored by %s, want %s", entry, decl.Record.Author, actor)
		}
		if err := st.Verify(ctx, decl); err != nil {
			t.Fatalf("Verify(%s): %v", entry, err)
		}
		checked++

		cells, err := st.Relations(ctx, entry)
		if err != nil {
			t.Fatalf("Relations(%s): %v", entry, err)
		}
		for _, cell := range cells {
			if err := st.Verify(ctx, cell); err != nil {
				t.Fatalf("Verify(cell %s -> %s): %v", cell.A, cell.B, err)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("verified nothing")
	}
}

// failingBackend fails one specific Apply -- the one containing a
// TypeEntry declaration -- so the atomicity of a whole written line can
// be checked without a cluster to crash.
type failingBackend struct {
	relations.Backend
	fail bool
}

func (f *failingBackend) Apply(ctx context.Context, ops []relations.Op) error {
	if f.fail {
		for _, op := range ops {
			if len(op.Key) == relations.KeyLen && op.Key[3] == relations.TypeEntry {
				return errors.New("backend refused this transaction")
			}
		}
	}
	return f.Backend.Apply(ctx, ops)
}

// TestAppendIsAllOrNothing checks that a line is written whole. On paper
// a half-written line is a scribble somebody has to interpret; here the
// declaration, every cell and both directions of every cell are one
// transaction, so a refused write leaves nothing behind -- not a
// nameless entry with three of its six columns attached.
func TestAppendIsAllOrNothing(t *testing.T) {
	ctx := context.Background()
	be := &failingBackend{Backend: relations.Memory()}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	st, _, _ := newStoreOn(t, be, pub, priv)
	j := relations.NewJournal(st)

	shift, err := j.Field(ctx, fieldShift)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	operator, err := j.Field(ctx, fieldOperator)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}

	be.fail = true
	if _, err := j.Append(ctx, relations.TermCell(shift, "Day"), relations.TermCell(operator, "Ivanova")); err == nil {
		t.Fatal("expected Append to fail while the backend is refusing entry transactions")
	}
	be.fail = false

	// The terms it interned along the way survive -- a dictionary entry
	// is written before, and independently of, the line that first used
	// it -- but no line, and no counter bump, does.
	page, err := j.Page(ctx, relations.FirstEntryPage)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(page) != 0 {
		t.Fatalf("page 1 has %d lines after a refused Append, want 0", len(page))
	}

	entry, err := j.Append(ctx, relations.TermCell(shift, "Day"), relations.TermCell(operator, "Ivanova"))
	if err != nil {
		t.Fatalf("Append after the backend recovered: %v", err)
	}
	if entry.ID != 1 {
		t.Fatalf("first successful line = %s, want line 1 (a refused write must not consume an id)", entry)
	}
	row, err := j.Row(ctx, entry)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if len(row) != 2 {
		t.Fatalf("line has %d columns, want 2", len(row))
	}
}

func TestAppendRejectsMalformedLines(t *testing.T) {
	ctx := context.Background()
	st, _, _ := newStore(t)
	j := relations.NewJournal(st)

	if _, err := j.Append(ctx); err == nil {
		t.Fatal("expected an error appending a line with no columns")
	}
	shift, err := j.Field(ctx, fieldShift)
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	if _, err := j.Append(ctx, relations.TermCell(shift, "Day"), relations.TermCell(shift, "Night")); err == nil {
		t.Fatal("expected an error giving the same column twice in one line")
	}
	if _, err := j.Term(ctx, shift, ""); err == nil {
		t.Fatal("expected an error interning an empty term")
	}
	notAField := relations.Entity{Log: testLog, Page: 1, Type: relations.TypeEntry, ID: 1}
	if _, err := j.Append(ctx, relations.TermCell(notAField, "x")); err == nil {
		t.Fatal("expected an error using a non-field entity as a column")
	}
}

// scanAll returns every record in both namespaces -- the whole store, as
// an auditor would read it.
func scanAll(t *testing.T, st *relations.Store) []relations.Pair {
	t.Helper()
	ctx := context.Background()
	var all []relations.Pair
	for _, ns := range []byte{relations.NamespaceRelation, relations.NamespaceIndex, relations.NamespacePresence} {
		start, end := relations.NamespaceBounds(ns)
		pairs, err := st.Backend().Scan(ctx, start, end, 0)
		if err != nil {
			t.Fatalf("scan namespace %#x: %v", ns, err)
		}
		all = append(all, pairs...)
	}
	return all
}

// countRecordsNamed reports how many of pairs carry text as their name.
// Counting decoded names rather than sweeping for the bytes is
// deliberate: a signature is 64 bytes of noise, and a short string like
// "OK" turns up inside one by chance often enough to make a byte sweep
// flaky rather than strict.
func countRecordsNamed(t *testing.T, pairs []relations.Pair, text string) int {
	t.Helper()
	n := 0
	for _, p := range pairs {
		rec, _, err := relations.DecodeRecord(p.Value)
		if err != nil {
			t.Fatalf("DecodeRecord: %v", err)
		}
		if rec.Name == text {
			n++
		}
	}
	return n
}

// dumpStore renders every record as a line, for eyeballing a failure.
// Unused by assertions on purpose -- it is here because the first thing
// anyone copying this example wants is to see what it actually wrote.
func dumpStore(t *testing.T, st *relations.Store) string {
	t.Helper()
	var b strings.Builder
	for _, p := range scanAll(t, st) {
		ns, first, second, err := relations.ParseKey(p.Key)
		if err != nil {
			t.Fatalf("ParseKey: %v", err)
		}
		rec, _, err := relations.DecodeRecord(p.Value)
		if err != nil {
			t.Fatalf("DecodeRecord: %v", err)
		}
		fmt.Fprintf(&b, "%#x %s -> %s kind=%d name=%q data=%x\n", ns, first, second, rec.Kind, rec.Name, rec.Data)
	}
	return b.String()
}
