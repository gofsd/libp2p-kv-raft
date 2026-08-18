package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/examples/relations/journalcmd"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file is the mobile counterpart of the journal commands on
// kvctl-cli: a device fetches the schema of a log book, draws a form
// from it, and submits the filled form as a Command. It is the same
// service on the other end, driven through this package's own dispatch
// (dispatch.go) rather than desktop's -- see journalcmd.Service.Answer,
// which exists precisely so both dispatchers run one implementation
// instead of two that have to be kept saying the same thing.
//
// Which peers may submit is the Group/Command catalog's business,
// enforced inside the raft FSM (catalog.go's own CreateCommand/
// AddPeerToGroup wrap the same records). A device granted command
// standing and nothing else can write a line without being able to write
// a key, which is the whole reason to drive a log this way from a phone.
//
// Everything here is JSON in and JSON out, gomobile's only real option:
// no maps, no structs, no slices across the binding. A caller in Kotlin
// gets the same documents kvctl-cli prints.

// journalAwaitPoll paces waiting for a submitted command's answer. The
// dispatcher is poked as soon as a request lands, so the usual wait is
// one interval rather than anything like the timeout.
const journalAwaitPoll = 250 * time.Millisecond

var (
	journalMu sync.Mutex
	// journals caches one opened log book per log byte, since opening one
	// declares this device's actor and re-doing that per call would be
	// several IPC round trips for nothing.
	journals = map[uint8]*relations.Journal{}
)

// JournalServe answers a command's journal requests on this device, for
// as long as the app runs or until StopJournalServe -- the phone-side
// equivalent of `kvctl-cli journalserve`, for a device that owns a log
// book rather than only writing to somebody else's.
//
// log is the book (a number, 1 unless a device keeps several). Lines it
// writes are signed with this device's own identity, the same one the
// cluster knows it by.
func JournalServe(commandID string, log int) error {
	journal, err := journalFor(log)
	if err != nil {
		return err
	}
	return RunCommandDispatcher(commandID, &journalDispatchHandler{service: journalcmd.New(journal)})
}

// StopJournalServe stops answering commandID's requests. Safe to call
// when nothing is running for it.
func StopJournalServe(commandID string) { StopCommandDispatcher(commandID) }

// journalDispatchHandler adapts the journal service to this package's
// own CommandDispatchHandler -- the reverse binding Kotlin would
// otherwise implement, implemented in Go instead because the thing
// answering is a Go service.
type journalDispatchHandler struct{ service *journalcmd.Service }

func (h *journalDispatchHandler) Handle(instanceID, commandID, requestedBy, inputs string) string {
	fields, narrative := h.service.Answer(instanceID, commandID, requestedBy, inputs)
	out, err := json.Marshal(struct {
		Fields    map[string]string `json:"fields"`
		Narrative string            `json:"narrative"`
	}{Fields: fields, Narrative: narrative})
	if err != nil {
		return fmt.Sprintf(`{"fields":{"status":"error"},"narrative":%q}`, err.Error())
	}
	return string(out)
}

// JournalDefine declares the columns of a log book on this device and
// what each one holds, from a JSON object of name to type
// (`{"operator":"term","pieces":"number","remarks":"text"}`), and returns
// the declared columns as JSON. Local and operator-side: this writes the
// schema, which only the device that owns the log can do.
func JournalDefine(log int, columnsJSON string) (string, error) {
	var columns map[string]string
	if err := json.Unmarshal([]byte(columnsJSON), &columns); err != nil {
		return "", fmt.Errorf("kvmobile: journal define: columns must be a JSON object of name to type: %w", err)
	}
	journal, err := journalFor(log)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	declared := []map[string]string{}
	for name, kind := range columns {
		input, err := relations.ParseInputKind(kind)
		if err != nil {
			return "", fmt.Errorf("kvmobile: journal define: %w", err)
		}
		field, err := journal.DefineField(ctx, name, input)
		if err != nil {
			return "", fmt.Errorf("kvmobile: journal define: %w", err)
		}
		declared = append(declared, map[string]string{"field": field.String(), "name": name, "input": input.String()})
	}
	return journalJSON(declared)
}

// JournalVocabulary adds values to a column of a log book on this device
// (a JSON array of strings) and, if closeIt, closes that vocabulary so
// only those values are admissible afterwards. Returns the terms as
// JSON. Local and operator-side, like JournalDefine.
func JournalVocabulary(log int, column, valuesJSON string, closeIt bool) (string, error) {
	var values []string
	if err := json.Unmarshal([]byte(valuesJSON), &values); err != nil {
		return "", fmt.Errorf("kvmobile: journal vocabulary: values must be a JSON array: %w", err)
	}
	journal, err := journalFor(log)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	field, err := journal.Field(ctx, column)
	if err != nil {
		return "", fmt.Errorf("kvmobile: journal vocabulary: %w", err)
	}
	terms := []map[string]string{}
	for _, value := range values {
		term, err := journal.Term(ctx, field, value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: journal vocabulary: %w", err)
		}
		terms = append(terms, map[string]string{"term": term.String(), "text": value})
	}
	if closeIt {
		if err := journal.CloseField(ctx, field); err != nil {
			return "", fmt.Errorf("kvmobile: journal vocabulary: %w", err)
		}
	}
	return journalJSON(terms)
}

// JournalVerify replays a log book on this device and returns it as
// text, ending with whether the record still adds up -- every digest,
// every signature, and that nothing has been removed.
//
// Local, and deliberately so: checking a record you were handed is not
// something to ask the holder of that record to do for you.
func JournalVerify(log int) (string, error) {
	journal, err := journalFor(log)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	text, err := journal.RenderBook(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: journal verify: %w", err)
	}
	return text, nil
}

// JournalForm fetches the schema to draw a form from -- the columns of
// the log behind commandID, what each holds, and the values a closed
// vocabulary admits -- as JSON.
func JournalForm(commandID string) (string, error) {
	result, err := journalDo(commandID, journalcmd.Request{Op: journalcmd.OpForm})
	if err != nil {
		return "", err
	}
	if result.Form == nil {
		return "", fmt.Errorf("kvmobile: journal form: %s answered with no form", commandID)
	}
	return journalJSON(result.Form)
}

// JournalAppend submits one filled form -- a JSON object of column
// heading to value, all text, exactly what a form posts -- and returns
// the line it was written as.
func JournalAppend(commandID, cellsJSON string) (string, error) {
	cells, err := journalCells(cellsJSON)
	if err != nil {
		return "", err
	}
	result, err := journalDo(commandID, journalcmd.Request{Op: journalcmd.OpAppend, Cells: cells})
	if err != nil {
		return "", err
	}
	return result.Line, nil
}

// JournalCorrect submits a replacement for line and returns the new
// line. The original is left exactly as written and marked superseded.
func JournalCorrect(commandID, line, cellsJSON string) (string, error) {
	cells, err := journalCells(cellsJSON)
	if err != nil {
		return "", err
	}
	result, err := journalDo(commandID, journalcmd.Request{Op: journalcmd.OpCorrect, Line: line, Cells: cells})
	if err != nil {
		return "", err
	}
	return result.Line, nil
}

// JournalVoid strikes line through with nothing in its place. reason is
// a value from the log's void-reason vocabulary, or "" for none.
func JournalVoid(commandID, line, reason string) error {
	_, err := journalDo(commandID, journalcmd.Request{Op: journalcmd.OpVoid, Line: line, Reason: reason})
	return err
}

// JournalPage returns a page of the log behind commandID as text, laid
// out the way it would be handed to somebody -- struck lines still
// legible, with what happened to them. page 0 means the one being
// written.
func JournalPage(commandID string, page int) (string, error) {
	number, err := journalPage(page)
	if err != nil {
		return "", err
	}
	result, err := journalDo(commandID, journalcmd.Request{Op: journalcmd.OpRender, Page: number})
	if err != nil {
		return "", err
	}
	return result.Text, nil
}

// JournalIdentity returns, as JSON, the actor this device signs as in
// the log behind commandID and the page state a signature would commit
// to. Declares the actor there on first use.
func JournalIdentity(commandID string) (string, error) {
	identity, err := journalIdentity(commandID)
	if err != nil {
		return "", err
	}
	return journalJSON(identity)
}

// JournalCountersign endorses a line under this device's own signature.
//
// The record is built and signed here, with a key that never leaves the
// device, and the node that owns the log checks it and writes it
// verbatim -- it cannot produce this signature itself, which is exactly
// what makes an endorsement worth having. A device may not endorse a
// line it wrote, endorse one twice, or endorse one that no longer
// stands.
func JournalCountersign(commandID, line string) error {
	entry, err := relations.ParseEntity(line)
	if err != nil {
		return fmt.Errorf("kvmobile: journal countersign: %w", err)
	}
	identity, actor, key, err := journalSigningIdentity(commandID)
	if err != nil {
		return err
	}
	_ = identity

	link, err := relations.SignLink(entry, actor, relations.KindCountersign, nil, actor, key, time.Now())
	if err != nil {
		return fmt.Errorf("kvmobile: journal countersign: %w", err)
	}
	signed := journalcmd.EncodeSignedLink(link)
	_, err = journalDo(commandID, journalcmd.Request{Op: journalcmd.OpCountersign, Line: line, Signed: &signed})
	return err
}

// JournalSignOff rules a page off under this device's own signature --
// the end-of-shift signature at the foot of the page -- after which
// lines go on the next one. page 0 means the page being written.
//
// The signature commits to how many lines the page held, so a line
// landing between asking and signing makes it stale and the log says so;
// call this again.
func JournalSignOff(commandID string, page int) error {
	number, err := journalPage(page)
	if err != nil {
		return err
	}
	identity, actor, key, err := journalSigningIdentity(commandID)
	if err != nil {
		return err
	}
	if number == 0 {
		number = identity.Page
	}
	if number != identity.Page {
		return fmt.Errorf("kvmobile: journal sign off: this log is writing page %d, not page %d", identity.Page, number)
	}

	link, err := relations.SignLink(
		relations.PageEntityOf(identity.Log, number),
		relations.StatusMarkerOf(identity.Log),
		relations.KindPageSignoff,
		[]byte{identity.Lines},
		actor, key, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("kvmobile: journal sign off: %w", err)
	}
	signed := journalcmd.EncodeSignedLink(link)
	_, err = journalDo(commandID, journalcmd.Request{Op: journalcmd.OpSignoff, Page: number, Signed: &signed})
	return err
}

// journalFor opens (once) the log book with this log byte, writing as
// this device's own identity.
func journalFor(log int) (*relations.Journal, error) {
	book, err := journalLog(log)
	if err != nil {
		return nil, err
	}
	journalMu.Lock()
	defer journalMu.Unlock()
	if journal, ok := journals[book]; ok {
		return journal, nil
	}

	sess, err := currentSession()
	if err != nil {
		return nil, err
	}
	key, err := journalKey()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	journal, err := journalcmd.OpenJournalOn(ctx, relations.Session(sess), book, PeerID(), key)
	if err != nil {
		return nil, fmt.Errorf("kvmobile: open journal: %w", err)
	}
	journals[book] = journal
	return journal, nil
}

// journalKey reads this device's own Ed25519 signing key from its own
// daemon -- the same key every record this device signs is checked
// against, and one that never leaves the device (see pkg/shmevent's doc
// comment on this same-process trust boundary).
func journalKey() (shmevent.PrivateKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	key, err := shmclient.GetPrivateKey(ctx, PeerID())
	if err != nil {
		return nil, fmt.Errorf("kvmobile: read this device's signing key: %w", err)
	}
	return key, nil
}

// journalSigningIdentity gathers what signing needs: the log's view of
// this device, the actor entity to sign as, and the key to sign with.
func journalSigningIdentity(commandID string) (journalcmd.Identity, relations.Entity, shmevent.PrivateKey, error) {
	identity, err := journalIdentity(commandID)
	if err != nil {
		return journalcmd.Identity{}, relations.Zero, nil, err
	}
	if identity.Name != PeerID() {
		return journalcmd.Identity{}, relations.Zero, nil, fmt.Errorf(
			"kvmobile: the log knows this submitter as %s, not %s -- signing as somebody else is not possible", identity.Name, PeerID())
	}
	actor, err := relations.ParseEntity(identity.Actor)
	if err != nil {
		return journalcmd.Identity{}, relations.Zero, nil, err
	}
	key, err := journalKey()
	if err != nil {
		return journalcmd.Identity{}, relations.Zero, nil, err
	}
	return identity, actor, key, nil
}

func journalIdentity(commandID string) (journalcmd.Identity, error) {
	result, err := journalDo(commandID, journalcmd.Request{Op: journalcmd.OpIdentity})
	if err != nil {
		return journalcmd.Identity{}, err
	}
	if result.Identity == nil {
		return journalcmd.Identity{}, fmt.Errorf("kvmobile: %s answered with no identity", commandID)
	}
	return *result.Identity, nil
}

// journalDo submits one request and waits for the service's answer,
// through this package's own dispatch rather than desktop's.
//
// A refusal comes back as an error here rather than as a result: a
// caller in Kotlin wants an exception for "the vocabulary is closed",
// not a field to remember to check.
func journalDo(commandID string, req journalcmd.Request) (journalcmd.Result, error) {
	inputs, err := json.Marshal(req)
	if err != nil {
		return journalcmd.Result{}, fmt.Errorf("kvmobile: journal: encode request: %w", err)
	}
	instanceID, err := SubmitCommand(commandID, string(inputs))
	if err != nil {
		return journalcmd.Result{}, err
	}

	deadline := time.Now().Add(callTimeout)
	for {
		raw, err := LatestCommandLog(instanceID)
		if err == nil {
			var record logrecord.Record
			if err := json.Unmarshal([]byte(raw), &record); err != nil {
				return journalcmd.Result{}, fmt.Errorf("kvmobile: journal: answer to %s does not parse: %w", instanceID, err)
			}
			if record.Fields[journalcmd.FieldStatus] != CommandStatusRunning {
				return journalResult(instanceID, record)
			}
		}
		if time.Now().After(deadline) {
			return journalcmd.Result{}, fmt.Errorf("kvmobile: journal: no answer to %s within %s", instanceID, callTimeout)
		}
		time.Sleep(journalAwaitPoll)
	}
}

// journalResult reads the service's answer out of one log entry.
func journalResult(instanceID string, record logrecord.Record) (journalcmd.Result, error) {
	raw := record.Fields[journalcmd.FieldResult]
	if raw == "" {
		return journalcmd.Result{}, fmt.Errorf("kvmobile: journal: %s: %s", instanceID, record.Narrative)
	}
	var result journalcmd.Result
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return journalcmd.Result{}, fmt.Errorf("kvmobile: journal: answer to %s does not parse: %w", instanceID, err)
	}
	if result.Error != "" {
		return journalcmd.Result{}, fmt.Errorf("kvmobile: journal: %s", result.Error)
	}
	return result, nil
}

func journalCells(cellsJSON string) (map[string]string, error) {
	var cells map[string]string
	if err := json.Unmarshal([]byte(cellsJSON), &cells); err != nil {
		return nil, fmt.Errorf("kvmobile: journal: cells must be a JSON object of column to value: %w", err)
	}
	if len(cells) == 0 {
		return nil, fmt.Errorf("kvmobile: journal: a line needs at least one filled column")
	}
	return cells, nil
}

func journalLog(log int) (uint8, error) {
	if log < 0 || log > 0xFF {
		return 0, fmt.Errorf("kvmobile: journal: log book must be 0..255, got %d", log)
	}
	return uint8(log), nil
}

func journalPage(page int) (uint8, error) {
	if page < 0 || page > 0xFF {
		return 0, fmt.Errorf("kvmobile: journal: page must be 0..255, got %d", page)
	}
	return uint8(page), nil
}

func journalJSON(v any) (string, error) {
	out, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("kvmobile: journal: encode result: %w", err)
	}
	return string(out), nil
}

// resetJournals drops the cached log books, so a test (or an app that
// Stops and Starts against a different node) does not go on using a
// journal bound to a session that is gone.
func resetJournals() {
	journalMu.Lock()
	defer journalMu.Unlock()
	journals = map[uint8]*relations.Journal{}
}
