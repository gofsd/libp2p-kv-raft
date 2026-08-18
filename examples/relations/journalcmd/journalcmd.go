// Package journalcmd puts an examples/relations log book behind this
// repo's Group/Command catalog, so that a device writes to it by
// *submitting a command* rather than by writing keys.
//
// That distinction is the whole point. The journal's own records are
// ordinary user-namespace Sets, which means anything that can reach a
// daemon can write them: signatures and the hash chain make tampering
// evident, not impossible. A Command is different -- who may submit one
// is checked inside the raft FSM against the Group/Command catalog
// (pkg/kvctl/catalog.go's isPermittedForCommand, backed by
// KindGroup/KindCommand/KindGroupCommand/KindPeerGroup records that only
// a voter may write). So:
//
//   - grant devices command/group standing and nothing else, and the
//     catalog becomes the front door: "operators may submit append,
//     supervisors may submit sign-off" is enforced where client code
//     cannot forge it;
//   - keep the journal on one node -- the command's own target peer --
//     and that node is the only writer, which also suits a chain whose
//     head serialises every write anyway.
//
// Do not also give those devices the "remote" group: a peer with that
// standing can do forwarded Sets of arbitrary keys, which walks straight
// around this front door into the journal's namespace.
//
// # What a submitter gets
//
// Two things, which is what a form-driven client needs:
//
//   - "form" returns the log's schema -- its columns, what each holds,
//     and the admissible values of any closed vocabulary -- generated
//     from the declarations themselves, so the form a client draws and
//     the rules the log enforces cannot drift apart.
//   - "append" (and "correct", "void") submits a filled form, which the
//     service turns into cells of the right kind and writes as one line.
//
// # Who signed it
//
// The node running the dispatcher writes the line, so the line's own
// signature is that node's. The submitter's attestation is not lost,
// though -- it is the CommandRequest itself, which is authored by the
// submitting peer, replicated through raft, and ACL-checked by the FSM
// on the way in. This service records the requester and the instance id
// on every line it writes, so a line always points back at the signed,
// permission-checked request that asked for it.
//
// # Signing without writing
//
// Two acts are somebody's *personal* signature, and this service cannot
// make them: a countersignature it signed would carry the node's
// signature under somebody else's name, which is exactly the assurance a
// countersignature exists to give. Those go the other way round -- the
// device builds and signs the record where it is (relations.SignLink
// needs no Store and no access to the log), hands over the bytes, and
// this service checks them and writes them verbatim. See sign.go for the
// submitting side.
//
// Which actor a device signs as is derived from the peer id its request
// was authored under, whose Ed25519 key that peer id carries -- so
// nothing is trusted from the request except what the FSM already
// established by accepting it, and a device can only ever sign as
// itself.
package journalcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// The operations a submitter may ask for, in Request.Op.
const (
	// OpForm returns the schema to draw a form from. It writes nothing.
	OpForm = "form"
	// OpAppend writes one line from a filled form.
	OpAppend = "append"
	// OpCorrect writes a replacement for an existing line and marks the
	// original superseded.
	OpCorrect = "correct"
	// OpVoid strikes a line through with no replacement.
	OpVoid = "void"
	// OpRender returns a page as text. It writes nothing.
	OpRender = "render"
	// OpIdentity returns the actor entity this submitter signs as, and
	// what a signature would have to say about the page right now. It
	// writes nothing to the log itself, but it does declare the actor on
	// first use -- which is the one moment a peer id is bound to a key.
	OpIdentity = "identity"
	// OpCountersign records an endorsement the submitter signed itself,
	// and OpSignoff a page sign-off it signed itself. Both carry the
	// signed record in Request.Signed; see the package doc on why these
	// two cannot be written on a submitter's behalf.
	OpCountersign = "countersign"
	OpSignoff     = "signoff"
)

// Request is the inputs JSON a submitter passes to SubmitCommand.
type Request struct {
	Op string `json:"op"`
	// Cells maps a column heading to its value as text -- what a form
	// posts. A number column takes digits, a term column takes the text
	// of the value, a remarks column takes prose.
	Cells map[string]string `json:"cells,omitempty"`
	// Line is which line to correct or void, as relations.Entity.String
	// prints it.
	Line string `json:"line,omitempty"`
	// Reason is the term explaining a void.
	Reason string `json:"reason,omitempty"`
	// Page is which page to render or sign off; zero means the one being
	// written.
	Page uint8 `json:"page,omitempty"`
	// Signed carries a record the submitter signed with its own key, for
	// the operations that are somebody's personal signature rather than
	// a request for one.
	Signed *SignedLink `json:"signed,omitempty"`
}

// SignedLink is relations.SignedLink on the wire: the two entities the
// relation runs between, and both directions of the record, each signed
// for its own key.
type SignedLink struct {
	A       string `json:"a"`
	B       string `json:"b"`
	Forward []byte `json:"forward"`
	Index   []byte `json:"index"`
}

// Decode turns a wire link back into the one the journal checks.
func (l SignedLink) Decode() (relations.SignedLink, error) {
	a, err := relations.ParseEntity(l.A)
	if err != nil {
		return relations.SignedLink{}, err
	}
	b, err := relations.ParseEntity(l.B)
	if err != nil {
		return relations.SignedLink{}, err
	}
	return relations.SignedLink{A: a, B: b, Forward: l.Forward, Index: l.Index}, nil
}

// EncodeSignedLink is Decode's inverse, for a submitter building one.
func EncodeSignedLink(link relations.SignedLink) SignedLink {
	return SignedLink{A: link.A.String(), B: link.B.String(), Forward: link.Forward, Index: link.Index}
}

// Result is what the service records as the command's outcome, in the
// log entry's "result" field. A submitter reads it back with
// kvctl.LatestCommandLog (see Await).
type Result struct {
	Op string `json:"op"`
	// Line and Page are where a written line landed.
	Line string `json:"line,omitempty"`
	Page uint8  `json:"page,omitempty"`
	// Form is OpForm's answer; Text is OpRender's; Identity is
	// OpIdentity's.
	Form     *Form     `json:"form,omitempty"`
	Text     string    `json:"text,omitempty"`
	Identity *Identity `json:"identity,omitempty"`
	// Error is set instead of the rest when the request was refused.
	Error string `json:"error,omitempty"`
}

// The keys the service writes into the command log entry. "status" is
// pkg/kvctl's own convention (CommandStatusSuccess/CommandStatusError);
// "result" is this service's.
const (
	FieldStatus = "status"
	FieldResult = "result"
)

// handlerTimeout bounds one request's work. A request that cannot be
// answered in this long is answered with an error rather than left
// hanging, since a dispatcher handles requests one at a time.
const handlerTimeout = 30 * time.Second

// Service answers journal commands. Bind one to a Journal on the node a
// command names as its target, and run it (Run) for each command id it
// should serve.
type Service struct {
	// Journal is the log this service writes to.
	Journal *relations.Journal

	// SubmitterColumn and RequestColumn are where a written line records
	// which peer asked for it and under which dispatch. They are
	// declared on first use and left out of the form, since a submitter
	// does not fill them in. Set either to "" to record neither.
	//
	// The submitter is a term -- a log has a bounded set of devices, so
	// each peer id is stored once and referenced -- while the request is
	// free text, because an instance id is unique per dispatch and
	// interning one per line would fill the dictionary with values
	// nothing ever looks up twice.
	SubmitterColumn string
	RequestColumn   string
	// VoidReasonColumn is the vocabulary a void's reason is drawn from.
	VoidReasonColumn string
}

// New returns a Service writing to j, with the default provenance and
// reason columns.
func New(j *relations.Journal) *Service {
	return &Service{
		Journal:          j,
		SubmitterColumn:  "submitted_by",
		RequestColumn:    "request",
		VoidReasonColumn: "void_reason",
	}
}

// Run serves commandID until stop is closed. It is
// kvctl.RunCommandDispatcher with this service as the handler, and
// carries that function's single-dispatcher-per-command assumption:
// exactly one process per command id, which is the same thing the
// command's own target peer already says.
func (s *Service) Run(commandID string, stop <-chan struct{}, onError func(error)) {
	kvctl.RunCommandDispatcher(commandID, s.Handle, stop, onError)
}

// Handle answers one dispatched request. It is a kvctl.CommandHandler:
// whatever it returns is recorded against the instance id, so a refusal
// is reported as a result rather than dropped -- a request nothing ever
// wrote a terminal entry for would be retried forever.
func (s *Service) Handle(req kvctl.CommandRequest) (map[string]string, string) {
	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()

	result, err := s.answer(ctx, req)
	if err != nil {
		result = Result{Op: result.Op, Error: err.Error()}
		return s.fields(kvctl.CommandStatusError, result), fmt.Sprintf("refused: %v", err)
	}
	return s.fields(kvctl.CommandStatusSuccess, result), s.narrate(result)
}

func (s *Service) fields(status string, result Result) map[string]string {
	fields := map[string]string{FieldStatus: status}
	if encoded, err := json.Marshal(result); err == nil {
		fields[FieldResult] = string(encoded)
	} else {
		fields[FieldResult] = fmt.Sprintf(`{"op":%q,"error":%q}`, result.Op, err.Error())
	}
	return fields
}

func (s *Service) narrate(result Result) string {
	switch result.Op {
	case OpForm:
		return fmt.Sprintf("form for page %d: %d columns", result.Page, len(result.Form.Columns))
	case OpAppend:
		return "wrote line " + result.Line
	case OpCorrect:
		return "wrote line " + result.Line + ", superseding the one it corrects"
	case OpVoid:
		return "struck line " + result.Line + " through"
	case OpIdentity:
		return "identity " + result.Identity.Actor
	case OpCountersign:
		return "recorded an endorsement of line " + result.Line
	case OpSignoff:
		return fmt.Sprintf("closed page %d", result.Page)
	case OpRender:
		return fmt.Sprintf("rendered page %d", result.Page)
	default:
		return "done"
	}
}

// answer is Handle without the reporting: it parses the request and runs
// the operation.
func (s *Service) answer(ctx context.Context, req kvctl.CommandRequest) (Result, error) {
	var parsed Request
	if req.Inputs == "" {
		return Result{}, fmt.Errorf("journalcmd: this command takes a JSON request; see Request")
	}
	if err := json.Unmarshal([]byte(req.Inputs), &parsed); err != nil {
		return Result{}, fmt.Errorf("journalcmd: inputs are not a request: %w", err)
	}

	switch parsed.Op {
	case OpForm:
		form, err := s.Form(ctx)
		if err != nil {
			return Result{Op: OpForm}, err
		}
		return Result{Op: OpForm, Page: form.Page, Form: &form}, nil

	case OpAppend:
		line, err := s.appendLine(ctx, req, parsed)
		if err != nil {
			return Result{Op: OpAppend}, err
		}
		return Result{Op: OpAppend, Line: line.String(), Page: line.Page}, nil

	case OpCorrect:
		line, err := s.correctLine(ctx, req, parsed)
		if err != nil {
			return Result{Op: OpCorrect}, err
		}
		return Result{Op: OpCorrect, Line: line.String(), Page: line.Page}, nil

	case OpVoid:
		if err := s.voidLine(ctx, parsed); err != nil {
			return Result{Op: OpVoid}, err
		}
		return Result{Op: OpVoid, Line: parsed.Line}, nil

	case OpIdentity:
		identity, err := s.identity(ctx, req)
		if err != nil {
			return Result{Op: OpIdentity}, err
		}
		return Result{Op: OpIdentity, Page: identity.Page, Identity: &identity}, nil

	case OpCountersign:
		line, err := relations.ParseEntity(parsed.Line)
		if err != nil {
			return Result{Op: OpCountersign}, err
		}
		link, err := s.signedLink(ctx, req, parsed)
		if err != nil {
			return Result{Op: OpCountersign}, err
		}
		if err := s.Journal.CountersignWith(ctx, line, link); err != nil {
			return Result{Op: OpCountersign}, err
		}
		return Result{Op: OpCountersign, Line: parsed.Line}, nil

	case OpSignoff:
		page := parsed.Page
		if page == 0 {
			current, _, err := s.currentPage(ctx)
			if err != nil {
				return Result{Op: OpSignoff}, err
			}
			page = current
		}
		link, err := s.signedLink(ctx, req, parsed)
		if err != nil {
			return Result{Op: OpSignoff}, err
		}
		if err := s.Journal.SignOffPageWith(ctx, page, link); err != nil {
			return Result{Op: OpSignoff}, err
		}
		return Result{Op: OpSignoff, Page: page}, nil

	case OpRender:
		page := parsed.Page
		if page == 0 {
			current, _, err := s.currentPage(ctx)
			if err != nil {
				return Result{Op: OpRender}, err
			}
			page = current
		}
		text, err := s.Journal.RenderPage(ctx, page)
		if err != nil {
			return Result{Op: OpRender}, err
		}
		return Result{Op: OpRender, Page: page, Text: text}, nil

	default:
		return Result{}, fmt.Errorf("journalcmd: %q is not an operation (want %s, %s, %s, %s, %s, %s, %s or %s)",
			parsed.Op, OpForm, OpIdentity, OpAppend, OpCorrect, OpVoid, OpCountersign, OpSignoff, OpRender)
	}
}

func (s *Service) appendLine(ctx context.Context, req kvctl.CommandRequest, parsed Request) (relations.Entity, error) {
	cells, err := s.cells(ctx, req, parsed)
	if err != nil {
		return relations.Zero, err
	}
	return s.Journal.Append(ctx, cells...)
}

func (s *Service) correctLine(ctx context.Context, req kvctl.CommandRequest, parsed Request) (relations.Entity, error) {
	superseded, err := relations.ParseEntity(parsed.Line)
	if err != nil {
		return relations.Zero, err
	}
	cells, err := s.cells(ctx, req, parsed)
	if err != nil {
		return relations.Zero, err
	}
	return s.Journal.Correct(ctx, superseded, cells...)
}

func (s *Service) voidLine(ctx context.Context, parsed Request) error {
	line, err := relations.ParseEntity(parsed.Line)
	if err != nil {
		return err
	}
	reason := relations.Zero
	if parsed.Reason != "" {
		if s.VoidReasonColumn == "" {
			return fmt.Errorf("journalcmd: this service records no void reasons")
		}
		field, err := s.Journal.DefineField(ctx, s.VoidReasonColumn, relations.InputTerm)
		if err != nil {
			return err
		}
		if reason, err = s.Journal.Term(ctx, field, parsed.Reason); err != nil {
			return err
		}
	}
	return s.Journal.Void(ctx, line, reason)
}

// cells turns a filled form into the cells of a line: each value is
// converted according to the column's own declared type, so a number
// column takes a number and a closed vocabulary takes only what it
// admits. Provenance is added last, from the dispatch rather than from
// the submitter.
func (s *Service) cells(ctx context.Context, req kvctl.CommandRequest, parsed Request) ([]relations.Cell, error) {
	if len(parsed.Cells) == 0 {
		return nil, fmt.Errorf("journalcmd: a line needs at least one filled column")
	}
	j := s.Journal
	cells := make([]relations.Cell, 0, len(parsed.Cells)+2)
	for name, value := range parsed.Cells {
		if s.isProvenanceColumn(name) {
			return nil, fmt.Errorf("journalcmd: column %q is recorded by the service, not submitted", name)
		}
		field, err := j.Field(ctx, name)
		if err != nil {
			return nil, err
		}
		cell, err := s.cell(ctx, field, name, value)
		if err != nil {
			return nil, err
		}
		cells = append(cells, cell)
	}

	if s.SubmitterColumn != "" && req.RequestedBy != "" {
		field, err := j.DefineField(ctx, s.SubmitterColumn, relations.InputTerm)
		if err != nil {
			return nil, err
		}
		cells = append(cells, relations.TermCell(field, req.RequestedBy))
	}
	if s.RequestColumn != "" && req.InstanceID != "" {
		field, err := j.DefineField(ctx, s.RequestColumn, relations.InputText)
		if err != nil {
			return nil, err
		}
		cells = append(cells, relations.RemarkCell(field, req.InstanceID))
	}
	return cells, nil
}

// cell converts one submitted value according to its column's type.
func (s *Service) cell(ctx context.Context, field relations.Entity, name, value string) (relations.Cell, error) {
	input, err := s.Journal.FieldInput(ctx, field)
	if err != nil {
		return relations.Cell{}, err
	}
	switch input {
	case relations.InputNumber:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return relations.Cell{}, fmt.Errorf("journalcmd: column %q holds numbers: %w", name, err)
		}
		return relations.QuantityCell(field, n), nil
	case relations.InputText:
		return relations.RemarkCell(field, value), nil
	default:
		return relations.TermCell(field, value), nil
	}
}

// isProvenanceColumn reports a column this service fills in itself.
func (s *Service) isProvenanceColumn(name string) bool {
	return name != "" && (name == s.SubmitterColumn || name == s.RequestColumn)
}
