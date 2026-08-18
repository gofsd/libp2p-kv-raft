//go:build mage

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gofsd/libp2p-kv-raft/examples/relations"
	"github.com/gofsd/libp2p-kv-raft/examples/relations/journalcmd"
)

// These wrap examples/relations/journalcmd -- a worked example rather
// than part of this repo's library (see examples/relations/README.md),
// exposed here so the whole loop can be driven from a shell without
// writing a program first: one node serves a log book behind a Command,
// and anything permitted to submit that Command writes to it.
//
// The split matters more than it looks. `mage journalserve` runs on the
// node that owns the log and is the only thing here that touches journal
// keys; every other target below submits a Command and would work
// unchanged from a device with no write access to the log's namespace at
// all. Which peers may submit is the Group/Command catalog's business
// (`mage creategroup`/`createcommand`/`addcommandtogroup`/
// `addpeertogroup`), enforced inside the raft FSM.

// A caveat worth knowing before deploying this shape: a running
// dispatcher polls the local daemon every 250ms, and that contends with
// other processes' IPC calls to the same daemon. Measured on one node,
// roughly one local CLI read in ten fails with "context deadline
// exceeded" while `mage journalserve` is running, and none when it is
// not. Nothing is lost when it happens -- the call simply has to be made
// again -- but a script that submits in a loop should expect it, and a
// confusing symptom to recognise is "command <id> not found", which is a
// timed-out read of the command record rather than a missing command.
//
// journalTimeout bounds how long a submitted command waits for its
// answer. The service is poked the moment a request lands, so this is a
// ceiling for something having gone wrong, not a normal wait.
const journalTimeout = 30 * time.Second

// JournalServe implements `mage journalserve <commandID> <log>`: runs the
// journal service for commandID in the foreground, writing to log book
// `log` (a number, 1 unless you keep several books on one node) as this
// node's own identity. Stops on Ctrl-C.
//
// Run this on the node commandID names as its target -- one process per
// command id, which is the same single-owner assumption Command.PeerID
// already encodes.
// Usage: mage journalserve <commandID> <log>
func JournalServe(commandID, log string) error {
	book, err := parseLog(log)
	if err != nil {
		return err
	}
	journal, err := journalcmd.OpenLocalJournal(book)
	if err != nil {
		return err
	}

	stop := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		close(stop)
	}()

	fmt.Printf("serving %s over log %d -- Ctrl-C to stop\n", commandID, book)
	journalcmd.New(journal).Run(commandID, stop, func(err error) {
		fmt.Fprintf(os.Stderr, "journal: %v\n", err)
	})
	return nil
}

// JournalDefine implements `mage journaldefine <log> <columnsJSON>`:
// declares the columns of log book `log` and what each holds, e.g.
// '{"operator":"term","pieces":"number","remarks":"text"}'. Find-or-
// create, so running it again is how a column is added; a column that
// already exists as a different type is reported rather than redefined.
//
// Local and operator-side: this writes the schema directly, on the node
// that owns the log, which is also the only place it can be done.
// Usage: mage journaldefine <log> <columnsJSON>
func JournalDefine(log, columnsJSON string) error {
	book, err := parseLog(log)
	if err != nil {
		return err
	}
	var columns map[string]string
	if err := json.Unmarshal([]byte(columnsJSON), &columns); err != nil {
		return fmt.Errorf("journaldefine: columns must be a JSON object of name to type: %w", err)
	}
	journal, err := journalcmd.OpenLocalJournal(book)
	if err != nil {
		return err
	}
	ctx, cancel := journalContext()
	defer cancel()
	for name, kind := range columns {
		input, err := relations.ParseInputKind(kind)
		if err != nil {
			return err
		}
		field, err := journal.DefineField(ctx, name, input)
		if err != nil {
			return err
		}
		fmt.Printf("%s %s %s\n", field, name, input)
	}
	return nil
}

// JournalVocabulary implements `mage journalvocabulary <log> <column>
// <valuesJSON> <close: true|false>`: adds values to a column's
// vocabulary and optionally closes it, after which only those values are
// admissible. Pass "[]" to close a vocabulary without adding anything.
//
// Local and operator-side, like journaldefine: what a column may contain
// is the log's schema, not something a submitter decides.
// Usage: mage journalvocabulary <log> <column> <valuesJSON> <close: true|false>
func JournalVocabulary(log, column, valuesJSON, closeIt string) error {
	book, err := parseLog(log)
	if err != nil {
		return err
	}
	var values []string
	if err := json.Unmarshal([]byte(valuesJSON), &values); err != nil {
		return fmt.Errorf("journalvocabulary: values must be a JSON array: %w", err)
	}
	shut, err := strconv.ParseBool(closeIt)
	if err != nil {
		return fmt.Errorf("journalvocabulary: close must be true or false: %w", err)
	}

	journal, err := journalcmd.OpenLocalJournal(book)
	if err != nil {
		return err
	}
	ctx, cancel := journalContext()
	defer cancel()
	field, err := journal.Field(ctx, column)
	if err != nil {
		return err
	}
	for _, value := range values {
		term, err := journal.Term(ctx, field, value)
		if err != nil {
			return err
		}
		fmt.Printf("%s %s\n", term, value)
	}
	if shut {
		if err := journal.CloseField(ctx, field); err != nil {
			return err
		}
		fmt.Printf("%s closed\n", column)
	}
	return nil
}

// JournalForm implements `mage journalform <commandID>`: fetches the
// log's schema over commandID and prints it as JSON -- the columns, what
// each holds, and the values a closed vocabulary admits. This is what a
// client draws a form from.
// Usage: mage journalform <commandID>
func JournalForm(commandID string) error {
	form, err := journalcmd.FetchForm(commandID, journalTimeout)
	if err != nil {
		return err
	}
	return printJSON(form)
}

// JournalAppend implements `mage journalappend <commandID> <cellsJSON>`:
// submits one filled form, e.g. '{"operator":"Ivanova","pieces":"120"}',
// and prints the line it was written as. Every value is text; the service
// converts each according to its own column's declared type.
// Usage: mage journalappend <commandID> <cellsJSON>
func JournalAppend(commandID, cellsJSON string) error {
	cells, err := parseCells(cellsJSON)
	if err != nil {
		return err
	}
	line, err := journalcmd.AppendLine(commandID, cells, journalTimeout)
	if err != nil {
		return err
	}
	fmt.Println(line)
	return nil
}

// JournalCorrect implements `mage journalcorrect <commandID> <line>
// <cellsJSON>`: submits a replacement for line, and prints the new line.
// The original is left exactly as written and marked superseded -- see
// examples/relations' README on why nothing here erases.
// Usage: mage journalcorrect <commandID> <line> <cellsJSON>
func JournalCorrect(commandID, line, cellsJSON string) error {
	cells, err := parseCells(cellsJSON)
	if err != nil {
		return err
	}
	replacement, err := journalcmd.CorrectLine(commandID, line, cells, journalTimeout)
	if err != nil {
		return err
	}
	fmt.Println(replacement)
	return nil
}

// JournalVoid implements `mage journalvoid <commandID> <line> [reason|""]`:
// strikes line through with nothing in its place. reason is a value from
// the log's void-reason vocabulary, or "" for none.
// Usage: mage journalvoid <commandID> <line> [reason|""]
func JournalVoid(commandID, line, reason string) error {
	return journalcmd.VoidLine(commandID, line, reason, journalTimeout)
}

// JournalCountersign implements `mage journalcountersign <commandID>
// <line>`: endorses line under this node's own signature.
//
// The record is built and signed here, with a key that never leaves, and
// the node that owns the log checks it and writes it verbatim -- it
// cannot produce this signature itself, which is exactly what makes a
// countersignature worth having. You may not endorse a line you wrote,
// endorse one twice, or endorse one that no longer stands.
// Usage: mage journalcountersign <commandID> <line>
func JournalCountersign(commandID, line string) error {
	signer, err := journalcmd.LocalSigner()
	if err != nil {
		return err
	}
	return signer.Countersign(commandID, line, journalTimeout)
}

// JournalSignOff implements `mage journalsignoff <commandID> [page|""]`:
// rules a page off under this node's own signature -- the end-of-shift
// signature at the foot of the page -- after which lines go on the next
// one. Empty page means the page currently being written.
//
// The signature commits to how many lines the page held, so a line
// landing between asking and signing makes it stale and the log says so;
// run it again.
// Usage: mage journalsignoff <commandID> [page|""]
func JournalSignOff(commandID, page string) error {
	number, err := parsePage(page)
	if err != nil {
		return err
	}
	signer, err := journalcmd.LocalSigner()
	if err != nil {
		return err
	}
	return signer.SignOffPage(commandID, number, journalTimeout)
}

// JournalIdentity implements `mage journalidentity <commandID>`: prints
// the actor this node signs as in that log, and the page state a
// signature would commit to. Declares the actor there on first use.
// Usage: mage journalidentity <commandID>
func JournalIdentity(commandID string) error {
	signer, err := journalcmd.LocalSigner()
	if err != nil {
		return err
	}
	identity, err := signer.Identity(commandID, journalTimeout)
	if err != nil {
		return err
	}
	return printJSON(identity)
}

// JournalPage implements `mage journalpage <commandID> [page|""]`: prints
// a page the way it would be handed to somebody -- ruled columns, struck
// lines still legible with what happened to them, and the sign-off at the
// foot. Empty page means the one being written.
// Usage: mage journalpage <commandID> [page|""]
func JournalPage(commandID, page string) error {
	number, err := parsePage(page)
	if err != nil {
		return err
	}
	text, err := journalcmd.RenderPage(commandID, number, journalTimeout)
	if err != nil {
		return err
	}
	fmt.Print(text)
	return nil
}

// JournalVerify implements `mage journalverify <log>`: replays log book
// `log` on this node and reports whether it still adds up -- every
// digest, every signature, and that nothing has been removed. Prints the
// whole book first.
//
// Local, and deliberately so: verifying a record you were handed is not
// something to ask the holder of that record to do for you.
// Usage: mage journalverify <log>
func JournalVerify(log string) error {
	book, err := parseLog(log)
	if err != nil {
		return err
	}
	journal, err := journalcmd.OpenLocalJournal(book)
	if err != nil {
		return err
	}
	ctx, cancel := journalContext()
	defer cancel()
	text, err := journal.RenderBook(ctx)
	if err != nil {
		return err
	}
	fmt.Print(text)
	return nil
}

// journalContext bounds the local (non-command) targets' own reads and
// writes, which talk straight to this node rather than waiting on a
// dispatcher.
func journalContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), journalTimeout)
}

// parseLog reads a log book number.
func parseLog(log string) (uint8, error) {
	if log == "" {
		return 0, fmt.Errorf("which log book? pass a number, e.g. 1")
	}
	n, err := strconv.ParseUint(log, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("log book must be a number 0..255: %w", err)
	}
	return uint8(n), nil
}

// parsePage reads a page number, empty meaning "the one being written".
func parsePage(page string) (uint8, error) {
	if page == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(page, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("page must be a number 0..255: %w", err)
	}
	return uint8(n), nil
}

// parseCells reads a filled form: column heading to value, all text.
func parseCells(cellsJSON string) (map[string]string, error) {
	var cells map[string]string
	if err := json.Unmarshal([]byte(cellsJSON), &cells); err != nil {
		return nil, fmt.Errorf("cells must be a JSON object of column to value: %w", err)
	}
	if len(cells) == 0 {
		return nil, fmt.Errorf("a line needs at least one filled column")
	}
	return cells, nil
}

func printJSON(v any) error {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
