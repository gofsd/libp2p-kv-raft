package main

// The journal commands drive examples/relations/journalcmd: one node
// serves a log book behind a Command, and anything permitted to submit
// that Command writes to it. They live here, in the binary that needs no
// Go toolchain, because that is where the log actually runs -- a
// deployment target reached over SSH has kvnode and kvctl-cli on it and
// nothing else, and `journalserve` has to run beside the daemon that
// owns the log.
//
// This is the one place a shipped binary depends on an example rather
// than on pkg/. That is deliberate and worth stating: examples/relations
// is a worked example whose type bytes and vocabularies are one
// opinionated shape (see its README), and an application with different
// nouns should copy the pattern rather than inherit these commands.
//
// The split below is the deployment's own:
//
//   - journalserve runs on the node that owns the log, and is the only
//     command here that touches journal keys;
//   - journalform/append/correct/void/countersign/signoff/page/identity
//     submit Commands, and work unchanged from a device with no write
//     access to the log's namespace at all;
//   - journaldefine/vocabulary/verify are local and operator-side: a
//     log's schema, and the checking of a record you were handed.
//
// Measured on a real node: 30 consecutive local reads through this
// binary failed 0 times both with a dispatcher running and without, so
// the dispatcher's own 250ms poll costs nothing worth planning around.
// An IPC call can still time out on a machine short of memory (pkg/ipc
// allocates a fresh shmring segment pair per call); nothing is lost when
// it does, and "command <id> not found" is usually that timeout rather
// than a missing command.

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

// journalTimeout bounds how long a submitted command waits for its
// answer, and how long the local commands give their own reads. The
// service is poked the moment a request lands, so this is a ceiling for
// something having gone wrong rather than a normal wait.
const journalTimeout = 30 * time.Second

// cmdJournalServe runs the journal service for a command id in the
// foreground, until interrupted.
func cmdJournalServe(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalserve <commandID> <log>")
		os.Exit(2)
	}
	book := mustLog(args[1], "journalserve")
	journal, err := journalcmd.OpenLocalJournal(book)
	if err != nil {
		journalFail("journalserve", err)
	}

	stop := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		close(stop)
	}()

	fmt.Printf("serving %s over log %d -- Ctrl-C to stop\n", args[0], book)
	journalcmd.New(journal).Run(args[0], stop, func(err error) {
		fmt.Fprintf(os.Stderr, "journal: %v\n", err)
	})
}

// cmdJournalDefine declares a log's columns and what each one holds.
func cmdJournalDefine(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli journaldefine <log> <columnsJSON>`)
		os.Exit(2)
	}
	book := mustLog(args[0], "journaldefine")
	var columns map[string]string
	if err := json.Unmarshal([]byte(args[1]), &columns); err != nil {
		journalFail("journaldefine", fmt.Errorf("columns must be a JSON object of name to type: %w", err))
	}

	journal, err := journalcmd.OpenLocalJournal(book)
	if err != nil {
		journalFail("journaldefine", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTimeout)
	defer cancel()
	for name, kind := range columns {
		input, err := relations.ParseInputKind(kind)
		if err != nil {
			journalFail("journaldefine", err)
		}
		field, err := journal.DefineField(ctx, name, input)
		if err != nil {
			journalFail("journaldefine", err)
		}
		fmt.Printf("%s %s %s\n", field, name, input)
	}
}

// cmdJournalVocabulary adds values to a column and optionally closes it.
func cmdJournalVocabulary(args []string) {
	if len(args) != 4 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli journalvocabulary <log> <column> <valuesJSON> <close: true|false>`)
		os.Exit(2)
	}
	book := mustLog(args[0], "journalvocabulary")
	var values []string
	if err := json.Unmarshal([]byte(args[2]), &values); err != nil {
		journalFail("journalvocabulary", fmt.Errorf("values must be a JSON array: %w", err))
	}
	shut, err := strconv.ParseBool(args[3])
	if err != nil {
		journalFail("journalvocabulary", fmt.Errorf("close must be true or false: %w", err))
	}

	journal, err := journalcmd.OpenLocalJournal(book)
	if err != nil {
		journalFail("journalvocabulary", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTimeout)
	defer cancel()
	field, err := journal.Field(ctx, args[1])
	if err != nil {
		journalFail("journalvocabulary", err)
	}
	for _, value := range values {
		term, err := journal.Term(ctx, field, value)
		if err != nil {
			journalFail("journalvocabulary", err)
		}
		fmt.Printf("%s %s\n", term, value)
	}
	if shut {
		if err := journal.CloseField(ctx, field); err != nil {
			journalFail("journalvocabulary", err)
		}
		fmt.Printf("%s closed\n", args[1])
	}
}

// cmdJournalForm prints the schema a client draws its form from.
func cmdJournalForm(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalform <commandID>")
		os.Exit(2)
	}
	form, err := journalcmd.FetchForm(args[0], journalTimeout)
	if err != nil {
		journalFail("journalform", err)
	}
	journalPrintJSON("journalform", form)
}

// cmdJournalAppend submits one filled form.
func cmdJournalAppend(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli journalappend <commandID> <cellsJSON>`)
		os.Exit(2)
	}
	line, err := journalcmd.AppendLine(args[0], mustCells(args[1], "journalappend"), journalTimeout)
	if err != nil {
		journalFail("journalappend", err)
	}
	fmt.Println(line)
}

// cmdJournalCorrect submits a replacement for a line.
func cmdJournalCorrect(args []string) {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, `usage: kvctl-cli journalcorrect <commandID> <line> <cellsJSON>`)
		os.Exit(2)
	}
	line, err := journalcmd.CorrectLine(args[0], args[1], mustCells(args[2], "journalcorrect"), journalTimeout)
	if err != nil {
		journalFail("journalcorrect", err)
	}
	fmt.Println(line)
}

// cmdJournalVoid strikes a line through with nothing in its place.
func cmdJournalVoid(args []string) {
	if len(args) != 2 && len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalvoid <commandID> <line> [reason]")
		os.Exit(2)
	}
	reason := ""
	if len(args) == 3 {
		reason = args[2]
	}
	if err := journalcmd.VoidLine(args[0], args[1], reason, journalTimeout); err != nil {
		journalFail("journalvoid", err)
	}
}

// cmdJournalCountersign endorses a line under this node's own signature.
func cmdJournalCountersign(args []string) {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalcountersign <commandID> <line>")
		os.Exit(2)
	}
	signer, err := journalcmd.LocalSigner()
	if err != nil {
		journalFail("journalcountersign", err)
	}
	if err := signer.Countersign(args[0], args[1], journalTimeout); err != nil {
		journalFail("journalcountersign", err)
	}
}

// cmdJournalSignOff rules a page off under this node's own signature.
func cmdJournalSignOff(args []string) {
	if len(args) != 1 && len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalsignoff <commandID> [page]")
		os.Exit(2)
	}
	page := uint8(0)
	if len(args) == 2 {
		page = mustPage(args[1], "journalsignoff")
	}
	signer, err := journalcmd.LocalSigner()
	if err != nil {
		journalFail("journalsignoff", err)
	}
	if err := signer.SignOffPage(args[0], page, journalTimeout); err != nil {
		journalFail("journalsignoff", err)
	}
}

// cmdJournalIdentity prints the actor this node signs as in a log.
func cmdJournalIdentity(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalidentity <commandID>")
		os.Exit(2)
	}
	signer, err := journalcmd.LocalSigner()
	if err != nil {
		journalFail("journalidentity", err)
	}
	identity, err := signer.Identity(args[0], journalTimeout)
	if err != nil {
		journalFail("journalidentity", err)
	}
	journalPrintJSON("journalidentity", identity)
}

// cmdJournalPage prints a page the way it would be handed to somebody.
func cmdJournalPage(args []string) {
	if len(args) != 1 && len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalpage <commandID> [page]")
		os.Exit(2)
	}
	page := uint8(0)
	if len(args) == 2 {
		page = mustPage(args[1], "journalpage")
	}
	text, err := journalcmd.RenderPage(args[0], page, journalTimeout)
	if err != nil {
		journalFail("journalpage", err)
	}
	fmt.Print(text)
}

// cmdJournalVerify replays a log on this node and reports whether it
// still adds up.
func cmdJournalVerify(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl-cli journalverify <log>")
		os.Exit(2)
	}
	journal, err := journalcmd.OpenLocalJournal(mustLog(args[0], "journalverify"))
	if err != nil {
		journalFail("journalverify", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTimeout)
	defer cancel()
	text, err := journal.RenderBook(ctx)
	if err != nil {
		journalFail("journalverify", err)
	}
	fmt.Print(text)
}

func mustLog(log, command string) uint8 {
	n, err := strconv.ParseUint(log, 10, 8)
	if err != nil {
		journalFail(command, fmt.Errorf("log book must be a number 0..255: %w", err))
	}
	return uint8(n)
}

func mustPage(page, command string) uint8 {
	n, err := strconv.ParseUint(page, 10, 8)
	if err != nil {
		journalFail(command, fmt.Errorf("page must be a number 0..255: %w", err))
	}
	return uint8(n)
}

func mustCells(cellsJSON, command string) map[string]string {
	var cells map[string]string
	if err := json.Unmarshal([]byte(cellsJSON), &cells); err != nil {
		journalFail(command, fmt.Errorf("cells must be a JSON object of column to value: %w", err))
	}
	if len(cells) == 0 {
		journalFail(command, fmt.Errorf("a line needs at least one filled column"))
	}
	return cells
}

func journalPrintJSON(command string, v any) {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		journalFail(command, err)
	}
	fmt.Println(string(out))
}

func journalFail(command string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
	os.Exit(1)
}
