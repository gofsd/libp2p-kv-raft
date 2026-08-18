package kvmobile

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestJournalRoundTripFromADevice drives the whole loop the way an app
// would: the device serves a log book behind a Command, fetches the
// schema to draw a form from, submits the filled form, and reads the
// page back -- all through the gomobile-shaped API, which is JSON in and
// JSON out.
func TestJournalRoundTripFromADevice(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	selfPeerID := PeerID()

	const commandID = "cmd-journal"
	const groupID = "grp-journal"
	if err := CreateCommand(commandID, "Shift log", selfPeerID); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(commandID)
		return err == nil, nil
	})
	grantCommandAccess(t, commandID, groupID, selfPeerID)

	// The schema, written locally by the device that owns the log.
	if _, err := JournalDefine(1, `{"operator":"term","machine":"term","result":"term","pieces":"number","remarks":"text"}`); err != nil {
		t.Fatalf("JournalDefine: %v", err)
	}
	if _, err := JournalVocabulary(1, "operator", `["Ivanova","Petrov"]`, true); err != nil {
		t.Fatalf("JournalVocabulary: %v", err)
	}

	if err := JournalServe(commandID, 1); err != nil {
		t.Fatalf("JournalServe: %v", err)
	}
	t.Cleanup(func() { StopJournalServe(commandID) })

	// The form comes from the declarations, closed vocabulary and all.
	formJSON, err := JournalForm(commandID)
	if err != nil {
		t.Fatalf("JournalForm: %v", err)
	}
	var form struct {
		Log     int `json:"log"`
		Page    int `json:"page"`
		Columns []struct {
			Name    string `json:"name"`
			Input   string `json:"input"`
			Closed  bool   `json:"closed"`
			Options []struct {
				Text string `json:"text"`
			} `json:"options"`
		} `json:"columns"`
	}
	if err := json.Unmarshal([]byte(formJSON), &form); err != nil {
		t.Fatalf("the form does not parse: %v\n%s", err, formJSON)
	}
	if form.Log != 1 || form.Page != 1 {
		t.Fatalf("the form is for log %d page %d, want 1/1", form.Log, form.Page)
	}
	columns := map[string]int{}
	for i, column := range form.Columns {
		columns[column.Name] = i
	}
	if len(columns) != 5 {
		t.Fatalf("the form has %d columns, want 5: %s", len(columns), formJSON)
	}
	if operator := form.Columns[columns["operator"]]; !operator.Closed || len(operator.Options) != 2 {
		t.Fatalf("the operator column came back as %+v, want a closed vocabulary of two", operator)
	}
	if got := form.Columns[columns["pieces"]].Input; got != "number" {
		t.Fatalf("pieces is a %q column, want number", got)
	}

	// A filled form goes in as one line.
	line, err := JournalAppend(commandID, `{"operator":"Ivanova","machine":"Lathe-2","result":"OK","pieces":"120"}`)
	if err != nil {
		t.Fatalf("JournalAppend: %v", err)
	}
	if line == "" {
		t.Fatal("JournalAppend returned no line")
	}

	// A value the closed vocabulary does not admit is refused, as an
	// error rather than as something a caller has to remember to check.
	if _, err := JournalAppend(commandID, `{"operator":"Nobody"}`); err == nil {
		t.Fatal("a value outside the closed vocabulary was accepted")
	} else if !strings.Contains(err.Error(), "vocabulary is closed") {
		t.Fatalf("refusal = %v", err)
	}

	// A correction leaves the original standing and struck.
	corrected, err := JournalCorrect(commandID, line, `{"operator":"Ivanova","result":"Scrap","pieces":"3","remarks":"recount"}`)
	if err != nil {
		t.Fatalf("JournalCorrect: %v", err)
	}
	page, err := JournalPage(commandID, 1)
	if err != nil {
		t.Fatalf("JournalPage: %v", err)
	}
	for _, want := range []string{"Ivanova", "Lathe-2", "superseded by line", "replaces line", "recount"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the page does not contain %q:\n%s", want, page)
		}
	}
	t.Logf("the page, as a device would show it:\n\n%s", page)

	// This device signs as itself, and cannot endorse a line it wrote --
	// which on a one-device log is every line.
	identityJSON, err := JournalIdentity(commandID)
	if err != nil {
		t.Fatalf("JournalIdentity: %v", err)
	}
	var identity struct {
		Actor string `json:"actor"`
		Name  string `json:"name"`
	}
	if err := json.Unmarshal([]byte(identityJSON), &identity); err != nil {
		t.Fatalf("the identity does not parse: %v", err)
	}
	if identity.Name != selfPeerID || identity.Actor == "" {
		t.Fatalf("identity = %s, want this device's own peer id", identityJSON)
	}
	if err := JournalCountersign(commandID, corrected); err == nil {
		t.Fatal("a device endorsed a line it wrote itself")
	} else if !strings.Contains(err.Error(), "endorses nothing") {
		t.Fatalf("refusal = %v", err)
	}

	// Closing the page is this device's own signature, made here.
	if err := JournalSignOff(commandID, 0); err != nil {
		t.Fatalf("JournalSignOff: %v", err)
	}

	// And the book verifies locally, which is the point of holding it.
	book, err := JournalVerify(1)
	if err != nil {
		t.Fatalf("JournalVerify: %v", err)
	}
	if !strings.Contains(book, "page closed by") {
		t.Fatalf("the book does not show the page closed:\n%s", book)
	}
	if !strings.Contains(book, "chain:") || strings.Contains(book, "BROKEN") {
		t.Fatalf("the chain did not verify:\n%s", book)
	}
}

// TestJournalRefusesMalformedInput covers the shapes a Kotlin caller can
// get wrong, since every argument across the binding is a string.
func TestJournalRefusesMalformedInput(t *testing.T) {
	if _, err := JournalDefine(1, "not json"); err == nil {
		t.Fatal("expected an error defining columns from malformed JSON")
	}
	if _, err := JournalVocabulary(1, "operator", "{}", false); err == nil {
		t.Fatal("expected an error passing an object where an array of values belongs")
	}
	if _, err := JournalDefine(-1, "{}"); err == nil {
		t.Fatal("expected an error for a log book outside 0..255")
	}
	if _, err := JournalPage("cmd", 999); err == nil {
		t.Fatal("expected an error for a page outside 0..255")
	}
	if _, err := JournalAppend("cmd", "{}"); err == nil {
		t.Fatal("expected an error appending a line with no columns")
	}
	if err := JournalCountersign("cmd", "not-an-entity"); err == nil {
		t.Fatal("expected an error countersigning something that is not a line")
	}
}
