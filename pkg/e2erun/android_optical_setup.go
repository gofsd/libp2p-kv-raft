package e2erun

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// setupLedgerFile remembers which OpticalScanCase.Setup cases this *host* has already completed.
//
// Beside mes-optical-backend.addr for the same reason that lives there rather than in the repo: it
// describes this machine's rig, not the plan. test/e2e/testdata.json is committed and shared, and
// "the Signal account reachable from this host already has the rig linked into it" is true of one
// machine at a time.
const setupLedgerFile = "optical-setup-done.json"

// setupAgainEnvVar forces every Setup case to run again without touching the ledger, for the case
// where you know the state was wiped and do not want to hand-edit JSON to say so.
//
// Not the only way back on purpose -- naming a case in MANUAL_OPTICAL_SCAN_CASES overrides the
// ledger too (see resolveSetupCases). Two routes because they answer different questions: this one
// is "redo the setup", the other is "run exactly this case, whatever you think you know about it".
const setupAgainEnvVar = "MES_OPTICAL_SETUP_AGAIN"

// setupRecord is one completed setup case. RanAt is what the skip message prints, and it is the
// whole point of storing a struct rather than a bare bool: a run skipped because of something that
// happened twenty minutes ago and one skipped because of something from three months ago deserve
// different amounts of suspicion, and only the date can tell them apart.
type setupRecord struct {
	RanAt time.Time `json:"ran_at"`
}

type setupLedger map[string]setupRecord

func setupLedgerPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return setupLedgerFile
	}
	return filepath.Join(home, ".libp2p-kv-raft", setupLedgerFile)
}

// loadSetupLedger reads the ledger, treating every failure as "nothing has been set up yet".
//
// Failing open is the safe direction here and the opposite of what it usually is: an unreadable
// ledger that made us *skip* a setup case would strand the batch behind state nobody established,
// while an unreadable ledger that makes us re-run one costs a person at the rig and nothing else.
// A corrupt file is reported, not swallowed, because it also means completions are about to stop
// being recorded.
func loadSetupLedger() setupLedger {
	path := setupLedgerPath()
	body, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "e2erun: optical: cannot read the setup ledger %s (%v) -- treating every setup case as not yet run\n", path, err)
		}
		return setupLedger{}
	}
	var ledger setupLedger
	if err := json.Unmarshal(body, &ledger); err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: optical: setup ledger %s is not valid JSON (%v) -- treating every setup case as not yet run\n", path, err)
		return setupLedger{}
	}
	if ledger == nil {
		return setupLedger{}
	}
	return ledger
}

func saveSetupLedger(ledger setupLedger) {
	path := setupLedgerPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: optical: cannot create %s (%v) -- this run's setup completions are NOT recorded and will run again\n", filepath.Dir(path), err)
		return
	}
	body, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: optical: cannot encode the setup ledger (%v) -- this run's setup completions are NOT recorded\n", err)
		return
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: optical: cannot write %s (%v) -- this run's setup completions are NOT recorded and will run again\n", path, err)
	}
}

// explicitlyNamedCases is the set named by MANUAL_OPTICAL_SCAN_CASES, read here rather than
// threaded down from TestManualOpticalScan because resolveHumanCases/resolveMesCases already read
// their own env vars at this layer and a second convention would be worse than this one.
func explicitlyNamedCases() map[string]bool {
	named := map[string]bool{}
	for _, id := range strings.Split(os.Getenv("MANUAL_OPTICAL_SCAN_CASES"), ",") {
		if id = strings.TrimSpace(id); id != "" {
			named[id] = true
		}
	}
	return named
}

// resolveSetupCases drops every Setup case this host has already completed, and says so loudly.
//
// Loud is load-bearing rather than courteous. The ledger has no expiry -- see OpticalScanCase.Setup
// for why that was chosen and what it costs -- so the one protection against a stale entry is that
// a person reading the run's output can see the claim being made and the date behind it. A silent
// skip here would surface as an unrelated case failing much later.
func resolveSetupCases(cases []e2edata.OpticalScanCase) []e2edata.OpticalScanCase {
	if os.Getenv(setupAgainEnvVar) != "" {
		var forced []string
		for _, c := range cases {
			if c.Setup {
				forced = append(forced, c.CaseID)
			}
		}
		if len(forced) > 0 {
			fmt.Fprintf(os.Stderr,
				"e2erun: optical: %s is set -- running %d setup case(s) again regardless of the ledger: %s\n",
				setupAgainEnvVar, len(forced), strings.Join(forced, ", "))
		}
		return cases
	}

	ledger := loadSetupLedger()
	if len(ledger) == 0 {
		return cases
	}
	named := explicitlyNamedCases()

	out := make([]e2edata.OpticalScanCase, 0, len(cases))
	var skipped []string
	for _, c := range cases {
		rec, done := ledger[c.CaseID]
		// Naming a case explicitly beats the ledger: asking for exactly this case and being given
		// nothing at all would be the least useful answer available.
		if c.Setup && done && !named[c.CaseID] {
			skipped = append(skipped, fmt.Sprintf("%s (ran %s)", c.CaseID, rec.RanAt.Format(time.RFC3339)))
			continue
		}
		out = append(out, c)
	}

	if len(skipped) > 0 {
		fmt.Fprintf(os.Stderr,
			"e2erun: optical: skipping %d setup case(s) this host has already completed: %s\n",
			len(skipped), strings.Join(skipped, ", "))
		fmt.Fprintf(os.Stderr,
			"e2erun: optical: nothing expires that record. If the state it established is gone (restarting the mes backend discards signal-cli's account data with its temp home), set %s=1 or name the case in MANUAL_OPTICAL_SCAN_CASES. Ledger: %s\n",
			setupAgainEnvVar, setupLedgerPath())
	}
	return out
}

// recordSetupCases writes down every Setup case that passed in this run.
//
// Only on a pass, and that is the whole contract: a setup case that failed established nothing, so
// remembering it would skip the retry that is the only thing able to fix the rig. A case that was
// never in the batch has no result here and is left alone.
func recordSetupCases(cases []e2edata.OpticalScanCase, result *e2edata.OpticalScanResult) {
	if result == nil {
		return
	}
	isSetup := map[string]bool{}
	for _, c := range cases {
		if c.Setup {
			isSetup[c.CaseID] = true
		}
	}
	if len(isSetup) == 0 {
		return
	}

	var recorded []string
	ledger := loadSetupLedger()
	for _, r := range result.Cases {
		if !r.Pass || !isSetup[r.CaseID] {
			continue
		}
		if _, already := ledger[r.CaseID]; already {
			continue
		}
		ledger[r.CaseID] = setupRecord{RanAt: time.Now().UTC()}
		recorded = append(recorded, r.CaseID)
	}
	if len(recorded) == 0 {
		return
	}
	sort.Strings(recorded)
	saveSetupLedger(ledger)
	fmt.Fprintf(os.Stderr,
		"e2erun: optical: recorded %d completed setup case(s) -- later batches will skip them: %s (ledger: %s)\n",
		len(recorded), strings.Join(recorded, ", "), setupLedgerPath())
}
