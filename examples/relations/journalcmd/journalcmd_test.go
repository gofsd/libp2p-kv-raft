package journalcmd_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/examples/relations/journalcmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// The handler is a plain function of a request and a journal, so all of
// this runs against an in-memory store with no cluster anywhere -- which
// is also how an application would test its own command service.

const testLog uint8 = 1

func newJournal(t *testing.T) *relations.Journal {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	actor := relations.Entity{Log: testLog, Page: relations.SchemaPage, Type: relations.TypeActor, ID: 1}
	st := relations.New(relations.Memory(), testLog, actor, priv)
	if err := st.DeclareActor(context.Background(), actor, "the node", pub); err != nil {
		t.Fatalf("DeclareActor: %v", err)
	}
	return relations.NewJournal(st)
}

// defineShiftLog declares the columns a shift log has, the way an
// operator would before anybody submits anything.
func defineShiftLog(t *testing.T, j *relations.Journal) {
	t.Helper()
	ctx := context.Background()
	for _, column := range []struct {
		name  string
		input relations.InputKind
	}{
		{"operator", relations.InputTerm},
		{"machine", relations.InputTerm},
		{"result", relations.InputTerm},
		{"pieces", relations.InputNumber},
		{"remarks", relations.InputText},
	} {
		if _, err := j.DefineField(ctx, column.name, column.input); err != nil {
			t.Fatalf("DefineField(%s): %v", column.name, err)
		}
	}
}

// ask runs one request through the handler and returns the parsed
// result, as a submitter would read it back off the command log.
func ask(t *testing.T, s *journalcmd.Service, req journalcmd.Request) (journalcmd.Result, string) {
	t.Helper()
	inputs, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	fields, narrative := s.Handle(kvctl.CommandRequest{
		InstanceID:  "inst-1",
		CommandID:   "shift-log",
		RequestedBy: "12D3KooWSubmitter",
		Inputs:      string(inputs),
	})
	var result journalcmd.Result
	if raw := fields[journalcmd.FieldResult]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &result); err != nil {
			t.Fatalf("the answer does not parse: %v", err)
		}
	}
	if fields[journalcmd.FieldStatus] == "" {
		t.Fatal("the answer records no status")
	}
	return result, narrative
}

// TestFormComesFromTheSchema is the metadata half: a client asks what
// the page looks like, and gets the columns, their types and -- where a
// vocabulary is closed -- exactly what it will accept.
func TestFormComesFromTheSchema(t *testing.T) {
	ctx := context.Background()
	j := newJournal(t)
	defineShiftLog(t, j)
	s := journalcmd.New(j)

	// Two operators are known, and that list is closed; machines are
	// still open.
	operator, err := j.Field(ctx, "operator")
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	for _, name := range []string{"Ivanova", "Petrov"} {
		if _, err := j.Term(ctx, operator, name); err != nil {
			t.Fatalf("Term: %v", err)
		}
	}
	if err := j.CloseField(ctx, operator); err != nil {
		t.Fatalf("CloseField: %v", err)
	}
	machine, err := j.Field(ctx, "machine")
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	if _, err := j.Term(ctx, machine, "Lathe-2"); err != nil {
		t.Fatalf("Term: %v", err)
	}

	result, narrative := ask(t, s, journalcmd.Request{Op: journalcmd.OpForm})
	if result.Error != "" {
		t.Fatalf("form refused: %s", result.Error)
	}
	if result.Form == nil {
		t.Fatal("no form in the answer")
	}
	if result.Form.Log != testLog || result.Form.Page != relations.FirstEntryPage {
		t.Fatalf("the form is for log %d page %d, want %d/%d",
			result.Form.Log, result.Form.Page, testLog, relations.FirstEntryPage)
	}
	if !strings.Contains(narrative, "columns") {
		t.Fatalf("narrative = %q", narrative)
	}

	byName := make(map[string]journalcmd.Column)
	for _, column := range result.Form.Columns {
		byName[column.Name] = column
	}
	if len(byName) != 5 {
		t.Fatalf("the form has %d columns, want 5: %+v", len(byName), result.Form.Columns)
	}
	if got := byName["pieces"].Input; got != "number" {
		t.Fatalf("pieces is a %q column, want number", got)
	}
	if got := byName["remarks"].Input; got != "text" {
		t.Fatalf("remarks is a %q column, want text", got)
	}

	ops := byName["operator"]
	if !ops.Closed {
		t.Fatal("the operator column does not report its vocabulary as closed")
	}
	if len(ops.Options) != 2 {
		t.Fatalf("operator offers %d values, want 2: %+v", len(ops.Options), ops.Options)
	}
	if _, err := relations.ParseEntity(ops.Options[0].Term); err != nil {
		t.Fatalf("an option's term does not parse: %v", err)
	}
	if byName["machine"].Closed {
		t.Fatal("the machine column reports itself closed")
	}
	if len(byName["machine"].Options) != 1 {
		t.Fatalf("machine offers %+v, want the one value used so far", byName["machine"].Options)
	}

	// The columns the service fills in itself are not on the form.
	if _, ok := byName[s.SubmitterColumn]; ok {
		t.Fatal("the form asks the submitter to fill in who they are")
	}
}

// TestAppendWritesAFilledForm is the other half: the form comes back
// filled, and one line is written from it -- each value converted
// according to its own column's declared type.
func TestAppendWritesAFilledForm(t *testing.T) {
	ctx := context.Background()
	j := newJournal(t)
	defineShiftLog(t, j)
	s := journalcmd.New(j)

	result, narrative := ask(t, s, journalcmd.Request{
		Op: journalcmd.OpAppend,
		Cells: map[string]string{
			"operator": "Ivanova",
			"machine":  "Lathe-2",
			"result":   "OK",
			"pieces":   "120",
			"remarks":  "bearing sounded rough",
		},
	})
	if result.Error != "" {
		t.Fatalf("append refused: %s", result.Error)
	}
	line, err := relations.ParseEntity(result.Line)
	if err != nil {
		t.Fatalf("the answer's line does not parse: %v", err)
	}
	if !strings.Contains(narrative, "wrote line") {
		t.Fatalf("narrative = %q", narrative)
	}

	row, err := j.Row(ctx, line)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	got := make(map[string]relations.RowCell, len(row))
	for _, cell := range row {
		got[cell.FieldName] = cell
	}
	if got["pieces"].Number != 120 || !got["pieces"].Numeric {
		t.Fatalf("pieces came back as %+v, want the number 120", got["pieces"])
	}
	if got["operator"].Text != "Ivanova" || got["operator"].Term.IsZero() {
		t.Fatalf("operator came back as %+v, want the interned term Ivanova", got["operator"])
	}
	if got["remarks"].Text != "bearing sounded rough" || !got["remarks"].Free {
		t.Fatalf("remarks came back as %+v, want free text", got["remarks"])
	}

	// Provenance: the line says who asked for it and under which
	// dispatch, taken from the request rather than from the submitter's
	// own claim.
	if got[s.SubmitterColumn].Text != "12D3KooWSubmitter" {
		t.Fatalf("the line records the submitter as %q", got[s.SubmitterColumn].Text)
	}
	if got[s.RequestColumn].Text != "inst-1" {
		t.Fatalf("the line records the request as %q", got[s.RequestColumn].Text)
	}

	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
}

// TestRefusalsAreAnswers checks a refused request comes back as a
// result, not as silence -- a request nothing wrote a terminal entry for
// would be retried forever.
func TestRefusalsAreAnswers(t *testing.T) {
	ctx := context.Background()
	j := newJournal(t)
	defineShiftLog(t, j)
	s := journalcmd.New(j)

	for _, tc := range []struct {
		name string
		req  journalcmd.Request
		want string
	}{
		{"unknown op", journalcmd.Request{Op: "delete-everything"}, "is not an operation"},
		{"no cells", journalcmd.Request{Op: journalcmd.OpAppend}, "at least one filled column"},
		{
			"a word in a number column",
			journalcmd.Request{Op: journalcmd.OpAppend, Cells: map[string]string{"pieces": "lots"}},
			"holds numbers",
		},
		{
			"filling in the provenance",
			journalcmd.Request{Op: journalcmd.OpAppend, Cells: map[string]string{"submitted_by": "somebody else"}},
			"recorded by the service",
		},
		{
			"correcting a line that is not one",
			journalcmd.Request{Op: journalcmd.OpCorrect, Line: "not-an-entity", Cells: map[string]string{"result": "OK"}},
			"is not an entity",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, narrative := ask(t, s, tc.req)
			if result.Error == "" {
				t.Fatalf("the request was accepted: %+v", result)
			}
			if !strings.Contains(result.Error, tc.want) {
				t.Fatalf("refusal = %q, want it to mention %q", result.Error, tc.want)
			}
			if !strings.Contains(narrative, "refused") {
				t.Fatalf("narrative = %q", narrative)
			}
		})
	}

	// A closed vocabulary refuses a value it does not admit -- the rule
	// the form advertises, enforced where it actually matters.
	operator, err := j.Field(ctx, "operator")
	if err != nil {
		t.Fatalf("Field: %v", err)
	}
	if _, err := j.Term(ctx, operator, "Ivanova"); err != nil {
		t.Fatalf("Term: %v", err)
	}
	if err := j.CloseField(ctx, operator); err != nil {
		t.Fatalf("CloseField: %v", err)
	}
	result, _ := ask(t, s, journalcmd.Request{
		Op:    journalcmd.OpAppend,
		Cells: map[string]string{"operator": "Nobody"},
	})
	if !strings.Contains(result.Error, "vocabulary is closed") {
		t.Fatalf("a value outside a closed vocabulary was accepted: %+v", result)
	}
}

// TestCorrectVoidAndRender covers the rest of the surface a form-driven
// client needs: fixing a line, striking one, and reading the page back.
func TestCorrectVoidAndRender(t *testing.T) {
	ctx := context.Background()
	j := newJournal(t)
	defineShiftLog(t, j)
	s := journalcmd.New(j)

	first, _ := ask(t, s, journalcmd.Request{
		Op:    journalcmd.OpAppend,
		Cells: map[string]string{"result": "OK", "pieces": "120"},
	})
	if first.Error != "" {
		t.Fatalf("append refused: %s", first.Error)
	}

	corrected, _ := ask(t, s, journalcmd.Request{
		Op:    journalcmd.OpCorrect,
		Line:  first.Line,
		Cells: map[string]string{"result": "Scrap", "pieces": "3"},
	})
	if corrected.Error != "" {
		t.Fatalf("correct refused: %s", corrected.Error)
	}
	original, err := relations.ParseEntity(first.Line)
	if err != nil {
		t.Fatalf("ParseEntity: %v", err)
	}
	status, err := j.Status(ctx, original)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != relations.StateSuperseded {
		t.Fatalf("the corrected line is %s, want superseded", status.State)
	}

	voided, _ := ask(t, s, journalcmd.Request{
		Op:     journalcmd.OpVoid,
		Line:   corrected.Line,
		Reason: "duplicate",
	})
	if voided.Error != "" {
		t.Fatalf("void refused: %s", voided.Error)
	}
	replacement, err := relations.ParseEntity(corrected.Line)
	if err != nil {
		t.Fatalf("ParseEntity: %v", err)
	}
	if status, err = j.Status(ctx, replacement); err != nil {
		t.Fatalf("Status: %v", err)
	} else if status.State != relations.StateVoided {
		t.Fatalf("the voided line is %s, want voided", status.State)
	}

	rendered, _ := ask(t, s, journalcmd.Request{Op: journalcmd.OpRender})
	if rendered.Error != "" {
		t.Fatalf("render refused: %s", rendered.Error)
	}
	for _, want := range []string{"log 1, page 1", "(1)", "superseded by line 2", "voided (duplicate)"} {
		if !strings.Contains(rendered.Text, want) {
			t.Fatalf("the rendered page does not contain %q:\n%s", want, rendered.Text)
		}
	}
	t.Logf("the page as the command answered it:\n\n%s", rendered.Text)

	if _, err := j.VerifyChain(ctx); err != nil {
		t.Fatalf("VerifyChain: %v", err)
	}
}
