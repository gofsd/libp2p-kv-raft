package e2erun

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// withLedgerHome points setupLedgerPath at a temp HOME so these tests never read or write the
// developer's real ~/.libp2p-kv-raft/optical-setup-done.json -- a test that recorded a setup
// completion there would silently stop that machine's rig from ever running the link case again.
func withLedgerHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("MES_OPTICAL_SETUP_AGAIN", "")
	t.Setenv("MANUAL_OPTICAL_SCAN_CASES", "")
	return filepath.Join(home, ".libp2p-kv-raft", setupLedgerFile)
}

func writeLedger(t *testing.T, path string, ledger setupLedger) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func caseIDs(cases []e2edata.OpticalScanCase) []string {
	out := make([]string, 0, len(cases))
	for _, c := range cases {
		out = append(out, c.CaseID)
	}
	return out
}

var setupPlan = []e2edata.OpticalScanCase{
	{CaseID: "ordinary_case"},
	{CaseID: "the_setup", Setup: true},
	{CaseID: "another_ordinary"},
}

func TestResolveSetupCasesKeepsEverythingWhenNothingRecorded(t *testing.T) {
	withLedgerHome(t)

	got := caseIDs(resolveSetupCases(setupPlan))
	if len(got) != 3 {
		t.Fatalf("a host that has never run setup must run every case, got %v", got)
	}
}

func TestResolveSetupCasesDropsACompletedSetupCase(t *testing.T) {
	path := withLedgerHome(t)
	writeLedger(t, path, setupLedger{"the_setup": {RanAt: time.Now().UTC()}})

	got := caseIDs(resolveSetupCases(setupPlan))
	if len(got) != 2 || got[0] != "ordinary_case" || got[1] != "another_ordinary" {
		t.Fatalf("a recorded setup case must be dropped and the rest kept in order, got %v", got)
	}
}

// A ledger entry for a case that is not marked Setup must not drop it. The ledger is keyed by case
// id, so a stale entry left behind by a case that used to be setup would otherwise silently remove
// an ordinary case from every future batch.
func TestResolveSetupCasesIgnoresLedgerEntriesForNonSetupCases(t *testing.T) {
	path := withLedgerHome(t)
	writeLedger(t, path, setupLedger{"ordinary_case": {RanAt: time.Now().UTC()}})

	if got := caseIDs(resolveSetupCases(setupPlan)); len(got) != 3 {
		t.Fatalf("only Setup cases may be dropped by the ledger, got %v", got)
	}
}

func TestResolveSetupCasesHonoursSetupAgain(t *testing.T) {
	path := withLedgerHome(t)
	writeLedger(t, path, setupLedger{"the_setup": {RanAt: time.Now().UTC()}})
	t.Setenv(setupAgainEnvVar, "1")

	if got := caseIDs(resolveSetupCases(setupPlan)); len(got) != 3 {
		t.Fatalf("%s must run recorded setup cases again, got %v", setupAgainEnvVar, got)
	}
}

// Naming a case explicitly has to beat the ledger, or the mini-batch habit ("run exactly this one
// case") answers a request for one case with nothing at all.
func TestResolveSetupCasesLetsAnExplicitlyNamedCaseThrough(t *testing.T) {
	path := withLedgerHome(t)
	writeLedger(t, path, setupLedger{"the_setup": {RanAt: time.Now().UTC()}})
	t.Setenv("MANUAL_OPTICAL_SCAN_CASES", "the_setup")

	got := caseIDs(resolveSetupCases(setupPlan))
	if len(got) != 3 {
		t.Fatalf("an explicitly named setup case must run despite the ledger, got %v", got)
	}
}

// An unreadable ledger must fail *open*. Skipping a setup case we cannot prove was done strands
// the batch behind state nobody established; re-running one costs a person at the rig and nothing
// worse.
func TestResolveSetupCasesFailsOpenOnACorruptLedger(t *testing.T) {
	path := withLedgerHome(t)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := caseIDs(resolveSetupCases(setupPlan)); len(got) != 3 {
		t.Fatalf("a corrupt ledger must not skip anything, got %v", got)
	}
}

func TestRecordSetupCasesRecordsOnlyPassingSetupCases(t *testing.T) {
	path := withLedgerHome(t)

	recordSetupCases(setupPlan, &e2edata.OpticalScanResult{Cases: []e2edata.OpticalScanCaseResult{
		{CaseID: "ordinary_case", Pass: true},
		{CaseID: "the_setup", Pass: true},
	}})

	ledger := loadSetupLedger()
	if _, ok := ledger["the_setup"]; !ok {
		t.Fatalf("a passing setup case must be recorded, ledger is %v", ledger)
	}
	if _, ok := ledger["ordinary_case"]; ok {
		t.Fatalf("an ordinary case must never enter the ledger, ledger is %v", ledger)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("ledger file not written: %v", err)
	}
}

// A setup case that failed established nothing. Recording it would skip the retry that is the only
// thing able to fix the rig -- the single most damaging thing this ledger could do.
func TestRecordSetupCasesIgnoresAFailedSetupCase(t *testing.T) {
	withLedgerHome(t)

	recordSetupCases(setupPlan, &e2edata.OpticalScanResult{Cases: []e2edata.OpticalScanCaseResult{
		{CaseID: "the_setup", Pass: false, Error: "nobody scanned it"},
	}})

	if ledger := loadSetupLedger(); len(ledger) != 0 {
		t.Fatalf("a failed setup case must not be recorded, ledger is %v", ledger)
	}
}

// The recorded date is what the skip message shows a person deciding whether to trust the entry,
// so an existing record must not be refreshed by a later run that skipped the case.
func TestRecordSetupCasesKeepsTheOriginalDate(t *testing.T) {
	path := withLedgerHome(t)
	first := time.Now().UTC().Add(-90 * 24 * time.Hour)
	writeLedger(t, path, setupLedger{"the_setup": {RanAt: first}})

	recordSetupCases(setupPlan, &e2edata.OpticalScanResult{Cases: []e2edata.OpticalScanCaseResult{
		{CaseID: "the_setup", Pass: true},
	}})

	got := loadSetupLedger()["the_setup"].RanAt
	if !got.Equal(first) {
		t.Fatalf("an existing record must keep its original date, want %s got %s", first, got)
	}
}

// The real plan must actually carry the marker, and on the case this feature exists for.
func TestPlanMarksTheRealSignalLinkAsSetup(t *testing.T) {
	file, err := e2edata.Load(filepath.Join("..", "..", e2edata.DefaultPath))
	if err != nil {
		t.Fatalf("load testdata: %v", err)
	}
	var found bool
	for _, c := range file.OpticalScanCases {
		if !c.Setup {
			continue
		}
		found = true
		if c.CaseID != "mes_link_account_real_link_on_three_devices" {
			t.Errorf("unexpected setup case %q -- setup means irreversible or costs a person, so a new one deserves its own review", c.CaseID)
		}
		if !c.NeedsHuman {
			t.Errorf("%s is Setup but not NeedsHuman: an unattended batch would reach it", c.CaseID)
		}
	}
	if !found {
		t.Error("no case in the plan is marked Setup")
	}
}
